import sys, os

# maps/ 디렉터리를 sys.path에 추가하여 src 패키지를 인식시킴
try:
    _maps_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
except NameError:
    _maps_root = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
if _maps_root not in sys.path:
    sys.path.insert(0, _maps_root)

from pyspark.sql import functions as F
from src.config.settings import(
    UC_CATALOG,
    META_SCHEMA
)
from src.profiling.profiling_config import (
    PROFILING_OUTPUT_BASE_PATH,
    get_all_table_names,
)
from src.profiling.profiling_functions import save_delta_and_csv, save_single_csv

TABLE_NAMES = get_all_table_names()
INTEGRATED_OUTPUT_PATH = f"{PROFILING_OUTPUT_BASE_PATH}/integrated"

META_SCHEMA = "meta"

def _read_table_result(spark, table_name: str, result_name: str):
    path = f"{PROFILING_OUTPUT_BASE_PATH}/{table_name}/{result_name}"
    try:
        return spark.read.format("delta").load(path)
    except Exception as e:
        print(f"[경고] {table_name}/{result_name} 읽기 실패: {path}")
        return None

def build_integrated_column_profile(spark, table_names: list):
    unioned = None
    for table_name in table_names:
        df = _read_table_result(spark, table_name, "column_profile")
        if df is None:
            continue
        df = df.withColumn("table_name", F.lit(table_name)).select(
            "table_name", "column_name", "column_role", "total_count", "null_count",
            "null_ratio", "blank_count", "blank_ratio", "distinct_count", "distinct_rate",
            "min_value", "max_value", "min_length", "max_length", "avg_length"
        )
        unioned = df if unioned is None else unioned.unionByName(df)
    return unioned.orderBy("table_name", "column_name") if unioned is not None else None

def build_integrated_categorical_distribution(spark, table_names: list):
    unioned = None
    for table_name in table_names:
        df = _read_table_result(spark, table_name, "categorical_distribution")
        if df is None:
            continue
        df = df.withColumn("table_name", F.lit(table_name)).select(
            "table_name", "column_name", "value", "count", "ratio", "value_category"
        )
        unioned = df if unioned is None else unioned.unionByName(df)
    return unioned.orderBy("table_name", "column_name", F.desc("count")) if unioned is not None else None

def build_integrated_summary(spark, table_names: list):
    summary_list = []
    for table_name in table_names:
        dup_df = _read_table_result(spark, table_name, "duplicate_summary")
        cross_df = _read_table_result(spark, table_name, "cross_column_check")

        row_count = dup_extra_count = key_null_count = seq_reversed_count = 0

        if dup_df is not None:
            try:
                pivoted = dup_df.groupBy().pivot("metric", ["row_count", "duplicate_extra_count", "key_null_count"]).agg(F.first("value"))
                stats = pivoted.collect()
                if stats:
                    row_dict = stats[0].asDict()
                    row_count = int(row_dict.get("row_count") or 0)
                    dup_extra_count = int(row_dict.get("duplicate_extra_count") or 0)
                    key_null_count = int(row_dict.get("key_null_count") or 0)
            except Exception as e:
                print(f"[알림] {table_name} 요약 정보 실패: {e}")

        if cross_df is not None:
            rev_row = cross_df.filter(F.col("sequence_status") == "REVERSED").agg(F.coalesce(F.sum("count"), F.lit(0)).alias("rev_cnt")).collect()
            if rev_row:
                seq_reversed_count = int(rev_row[0]["rev_cnt"] or 0)

        summary_list.append((table_name, row_count, dup_extra_count, key_null_count, seq_reversed_count))

    return spark.createDataFrame(
        summary_list,
        ["table_name", "row_count", "duplicate_key_extra_count", "key_null_count", "sequence_reversed_count"]
    )

def run_integrated_builder(spark, dbutils, ingest_date: str):
    # fallback 제거 및 명시적 검증 강화
    if not ingest_date:
        raise ValueError("❌ [오류] 통합 프로파일링 빌더에 Bronze ingest_date가 전달되지 않았습니다.")

    target_ingest_date = ingest_date
    print(f"📌 최종 적용된 Target Ingest Date: {target_ingest_date}")
    current_timestamp = F.current_timestamp()

    column_profile_df = build_integrated_column_profile(spark, TABLE_NAMES)
    categorical_dist_df = build_integrated_categorical_distribution(spark, TABLE_NAMES)
    summary_df = build_integrated_summary(spark, TABLE_NAMES)

    # 표준 메타데이터 컬럼(ingest_date, executed_at) 주입
    if column_profile_df is not None:
        column_profile_df = (
            column_profile_df
            .withColumn("ingest_date", F.lit(target_ingest_date))
            .withColumn("executed_at", current_timestamp)
        )
    if categorical_dist_df is not None:
        categorical_dist_df = (
            categorical_dist_df
            .withColumn("ingest_date", F.lit(target_ingest_date))
            .withColumn("executed_at", current_timestamp)
        )
    if summary_df is not None:
        summary_df = (
            summary_df
            .withColumn("ingest_date", F.lit(target_ingest_date))
            .withColumn("executed_at", current_timestamp)
        )

    # 1. 파일 시스템 저장 (Delta & 단일 CSV 변환)
    if column_profile_df is not None:
        save_delta_and_csv(column_profile_df, INTEGRATED_OUTPUT_PATH, "column_profile")
        save_single_csv(dbutils, column_profile_df, INTEGRATED_OUTPUT_PATH, "00_integrated_column_profile.csv")

    if categorical_dist_df is not None:
        save_delta_and_csv(categorical_dist_df, INTEGRATED_OUTPUT_PATH, "categorical_distribution")
        save_single_csv(dbutils, categorical_dist_df, INTEGRATED_OUTPUT_PATH, "01_integrated_categorical_distribution.csv")

    if summary_df is not None:
        save_delta_and_csv(summary_df, INTEGRATED_OUTPUT_PATH, "summary")
        save_single_csv(dbutils, summary_df, INTEGRATED_OUTPUT_PATH, "02_integrated_summary.csv")

    print(f"✅ 통합 파일 시스템 저장 완료 (ingest_date={target_ingest_date}): {INTEGRATED_OUTPUT_PATH}")

    # 2. Unity Catalog 'meta' 스키마 적재 (.saveAsTable 활용)
    target_catalog = UC_CATALOG
    target_schema = META_SCHEMA 

    print("=" * 60)
    print(f"Unity Catalog 통합 이력 테이블 적재 시작 (Catalog: {target_catalog}, Schema: {target_schema})")
    print("=" * 60)

    try:
        targets = [
            (column_profile_df, "integrated_column_profile"),
            (categorical_dist_df, "integrated_categorical_distribution"),
            (summary_df, "integrated_summary")
        ]

        for df, t_name in targets:
            if df is not None:
                full_table_name = f"{target_catalog}.{target_schema}.{t_name}"
                
                (
                    df.write.format("delta")
                    .mode("overwrite")
                    .option("replaceWhere", f"ingest_date = '{target_ingest_date}'")
                    .option("mergeSchema", "true")
                    .partitionBy("ingest_date")
                    .saveAsTable(full_table_name)
                )
                print(f"  - 카탈로그 이력 적재 완료: {full_table_name} (ingest_date={target_ingest_date})")

        print("✅ Unity Catalog 통합 이력 적재가 성공적으로 완료되었습니다.")

    except Exception as e:
        print(f"[오류] Unity Catalog 통합 테이블 적재 실패: {e}")