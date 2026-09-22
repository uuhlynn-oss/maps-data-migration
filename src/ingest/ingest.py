import sys
import os
import re
from functools import reduce as functools_reduce
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

from src.config.settings import (
    UC_CATALOG,
    BRONZE_SCHEMA,
    BRONZE_PATH,  # abfss://data@stmapspoc01.dfs.core.windows.net/bronze_test
)

# 원천 데이터 디렉터리 루트 경로 설정
RAW_BASE_PATH = BRONZE_PATH

# ============================================================
# File Helper Functions
# ============================================================

def resolve_latest_partition_path(dbutils, base_path: str, partition_prefix: str = "ingest_date="):
    """base_path 하위 파티션 중 최신 경로 반환"""
    try:
        entries = dbutils.fs.ls(base_path)
        partition_paths = sorted(e.path for e in entries if e.name.startswith(partition_prefix))
        if not partition_paths:
            raise FileNotFoundError(f"{base_path} 아래 '{partition_prefix}*' 파티션이 없습니다.")
        return partition_paths[-1]
    except Exception as e:
        print(f"❌ [경로 탐색 실패] {base_path}: {e}")
        raise e


def find_file_in_dir(dbutils, dir_path: str, suffix: str = None):
    """디렉터리 내 대상 파일 경로 반환"""
    entries = dbutils.fs.ls(dir_path)
    files = sorted(e.path for e in entries if not e.path.endswith("/"))
    if suffix:
        files = [f for f in files if f.endswith(suffix)]
    if not files:
        raise FileNotFoundError(f"{dir_path} 내 대상 파일({suffix})이 존재하지 않습니다.")
    return files[0]


# ============================================================
# File Loaders
# ============================================================

def load_csv_source(spark, path: str, columns: list):
    """CSV Source를 전 컬럼 StringType으로 원본 유지하여 로드"""
    schema = StructType([StructField(c, StringType(), True) for c in columns])
    return spark.read.option("header", True).schema(schema).csv(path)


def load_chatbot_json_unified(spark, path: str):
    """Chatbot Nested JSON -> Flattened Spark DataFrame 변환"""
    df_raw = spark.read.option("multiline", "true").json(path)
    df_sessions = df_raw.select(F.explode("sessions").alias("s")) if "sessions" in df_raw.columns else df_raw.select(F.col("s"))

    s_schema = df_sessions.schema["s"].dataType
    s_fields = {f.name: f.dataType for f in s_schema.fields} if hasattr(s_schema, "fields") else {}

    def convert_complex_to_json(col_name):
        dtype = s_fields.get(col_name)
        if dtype is None:
            return F.lit(None).cast("string").alias(col_name)
        if isinstance(dtype, StringType):
            return F.col(f"s.{col_name}").cast("string").alias(col_name)
        return F.to_json(F.col(f"s.{col_name}")).alias(col_name)

    df_base = df_sessions.select(
        F.col("s.session_id").alias("session_id"),
        F.col("s.channel").alias("channel"),
        F.col("s.device_type").alias("device_type"),
        F.col("s.os").alias("os"),
        F.col("s.app_version").alias("app_version"),
        F.col("s.started_at").alias("started_at"),
        F.col("s.ended_at").alias("ended_at"),
        F.col("s.duration_seconds").alias("duration_seconds"),
        F.col("s.entry_login_status").alias("entry_login_status"),
        F.col("s.is_logged_in").cast("string").alias("is_logged_in"),
        F.col("s.customer_id").alias("customer_id"),
        F.col("s.consent_screen_shown").cast("string").alias("consent_screen_shown"),
        F.col("s.consent_privacy_accepted").cast("string").alias("consent_privacy_accepted"),
        F.col("s.consent_marketing_accepted").cast("string").alias("consent_marketing_accepted"),
        F.col("s.consent_thirdparty_accepted").cast("string").alias("consent_thirdparty_accepted"),
        F.col("s.consent_answered_at").alias("consent_answered_at"),
        convert_complex_to_json("product_interest"),
        F.col("s.login_gate_hit").cast("string").alias("login_gate_hit"),
        F.col("s.mobile_only_redirect").cast("string").alias("mobile_only_redirect"),
        F.col("s.external_site_redirect").cast("string").alias("external_site_redirect"),
        convert_complex_to_json("accident_claim"),
        convert_complex_to_json("contract_change_request"),
        convert_complex_to_json("benefit_event_interaction"),
        convert_complex_to_json("resolution"),
        F.col("s.intent_category_guess").alias("intent_category_guess"),
        F.explode_outer("s.navigation_path").alias("nav"),
        F.explode_outer("s.free_text_queries").alias("q")
    )

    return df_base.select(
        "session_id", "channel", "device_type", "os", "app_version",
        "started_at", "ended_at", "duration_seconds", "entry_login_status",
        "is_logged_in", "customer_id", "consent_screen_shown",
        "consent_privacy_accepted", "consent_marketing_accepted",
        "consent_thirdparty_accepted", "consent_answered_at", "product_interest",
        "login_gate_hit", "mobile_only_redirect", "external_site_redirect",
        "accident_claim", "contract_change_request", "benefit_event_interaction",
        "resolution", "intent_category_guess",
        F.col("nav.step").cast("string").alias("nav_step"),
        F.col("nav.menu_level").alias("nav_menu_level"),
        F.col("nav.menu_name").alias("nav_menu_name"),
        F.col("nav.full_path").alias("nav_full_path"),
        F.col("nav.timestamp").alias("nav_timestamp"),
        F.col("q.query").alias("user_query"),
        F.col("q.matched").cast("string").alias("query_matched"),
        F.to_json(F.col("q.matched_items")).alias("query_matched_items"),
        F.col("q.timestamp").alias("query_timestamp")
    )


# ============================================================
# Bronze Table Ingest Execution
# ============================================================

def ingest_to_bronze(spark, dbutils, source_name: str, config: dict):
    """
    원천 파일(CSV/JSON)을 읽어 경로에서 추출한 ingest_date를 컬럼에 주입 후 
    Unity Catalog Bronze Delta 관리형 테이블로 파티션 저장
    """
    fmt = config.get("format")
    raw_path = f"{RAW_BASE_PATH}/{config['raw_relative_path']}"
    latest_path = resolve_latest_partition_path(dbutils, raw_path)

    print(f"[{source_name}] 원천 파일 탐색 위치: {latest_path}")

    if fmt == "csv":
        df = load_csv_source(spark, latest_path, config["columns"])
    elif fmt == "json_unified":
        json_file = find_file_in_dir(dbutils, latest_path, suffix=".json")
        df = load_chatbot_json_unified(spark, json_file)
    else:
        raise ValueError(f"지원하지 않는 format 타입: {fmt}")

    # 1. 경로 문자열에서 ingest_date 값 정규식 추출 (예: ingest_date=2026-09-15 -> 2026-09-15)
    date_match = re.search(r"ingest_date=([0-9-]+)", latest_path)
    ingest_date_val = date_match.group(1) if date_match else "unknown"

    # 2. DataFrame에 명시적으로 ingest_date 컬럼 추가
    df = df.withColumn("ingest_date", F.lit(ingest_date_val))

    # 3. 스키마 존재 여부 확인 및 자동 생성 (SCHEMA_NOT_FOUND 방지)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {UC_CATALOG}.{BRONZE_SCHEMA}")

    # 4. Unity Catalog 브론즈 관리형 테이블명 지정
    target_table = f"{UC_CATALOG}.{BRONZE_SCHEMA}.{source_name}"
    
    # 5. Delta 테이블 저장 (Overwrite 모드 + ingest_date 파티션 적용)
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("ingest_date")
        .saveAsTable(target_table)
    )
    
    row_count = spark.table(target_table).count()
    print(f"✅ [Bronze 적재 완료] {source_name} -> {target_table} (파티션일자: {ingest_date_val}, 총 {row_count:,}건)")
    
    return target_table