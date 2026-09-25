# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Mapping 실행: Silver → gold_candidate
# MAGIC
# MAGIC `silver_candidate.<source>`의 최신 배치를 Mapping Definition에 따라 Target(TO-BE) 모양으로 변환합니다.
# MAGIC
# MAGIC **실행 전 준비**
# MAGIC 1. `src/mapping/`에 `mapping_config.py`, `mapping_engine.py`를 올렸다 (파일을 바꿨다면 `%restart_python` 후 실행)
# MAGIC 2. `mapping_seed_loader.py`를 한 번 실행해 `meta.target_model`, `meta.mapping_definition`, `meta.code_mapping_asis_tobe`를 만들었다
# MAGIC 3. DQ 실행이 끝나 `silver_candidate.<source>`에 최신 배치가 있다
# MAGIC
# MAGIC **이 노트북이 하는 일**: 필요한 테이블 확인 → 변환 실행 → 요약 → 시각 변환 독립 검증 → NULL 비율과 분포 확인 → (선택) 저장
# MAGIC
# MAGIC **`TARGET_TABLE = "CUSTOMER"`인 경우**: `SOURCE_SYSTEM` 하나가 아니라, `mapping_definition`에서 CUSTOMER로
# MAGIC 들어오는 모든 Source를 자동 조회해 각각 `run()`+`save()`한 뒤, **그 전부가 끝나고 나서 `engine.integrate("CUSTOMER")`를
# MAGIC 정확히 1번** 실행합니다 (아래 "CUSTOMER 자동 실행" 섹션). `SOURCE_SYSTEM` 파라미터는 이때 무시됩니다.

# COMMAND ----------

# 파라미터 (여기만 바꿔서 실행)
SOURCE_SYSTEM = "OUTBOUND"     # 매핑 정의의 SOURCE_SYSTEM (TARGET_TABLE="CUSTOMER"일 때는 무시되고 자동 조회됨)
TARGET_TABLE = "CUSTOMER"      # 현재 지원 목록은 mapping_config.SUPPORTED_TARGET_TABLES 참고 (소스 1행 = Target 1행인 테이블만)
SOURCE_BATCH_ID = None        # None이면 silver_candidate의 최신 배치 (CUSTOMER는 Source마다 각자의 최신 배치를 씀)
SAVE_CANDIDATE = True         # gold_candidate.<target>에 저장 (후보를 뷰로 할지 물리 테이블로 할지 정하기 전의 확인용)
PROJECT_ROOT = None           # 예: "/Workspace/Users/<계정>/maps"  (src/mapping이 이 폴더 아래에 있을 때. 이미 import되면 None 그대로)

# COMMAND ----------

import datetime
import importlib
import os
import sys
from zoneinfo import ZoneInfo

from pyspark.sql import functions as F

if not PROJECT_ROOT:
    PROJECT_ROOT = os.path.normpath(os.path.join(os.getcwd(), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
try:
    import src.mapping.mapping_config as cfg
    import src.mapping.mapping_engine as engine_mod
except ModuleNotFoundError:
    import mapping_config as cfg
    import mapping_engine as engine_mod
for _m in (cfg, engine_mod):          # 파일을 교체한 뒤 이전 모듈이 남는 문제를 막는다
    importlib.reload(_m)


def _show(df, n=10):
    try:
        display(df.limit(n))          # Databricks
    except NameError:
        df.show(n, False)


engine = engine_mod.MappingEngine.from_tables(spark)
IS_CUSTOMER = TARGET_TABLE.upper() == "CUSTOMER"   # CUSTOMER는 아래에서 Source를 자동 조회해 여러 번 run()+save()한다


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. 필요한 테이블 확인

# COMMAND ----------

if IS_CUSTOMER:
    # SOURCE_SYSTEM 하나로 고정하지 않고, mapping_definition에서 CUSTOMER로 들어오는 모든 Source를 자동 조회한다.
    customer_defs_all = (
        spark.read.table(cfg.MAPPING_DEFINITION_TABLE)
        .filter(F.upper(F.trim("TARGET_TABLE")) == TARGET_TABLE.upper())
        .filter(F.upper(F.trim("FINAL_MIGRATION_APPLY_YN")) == "Y")
        .filter(F.upper(F.trim("REVIEW_STATUS")).isin(*cfg.APPLY_REVIEW_STATUSES))
    )
    customer_source_systems = sorted({r["SOURCE_SYSTEM"].upper() for r in
                                      customer_defs_all.select("SOURCE_SYSTEM").distinct().collect()})
    print(f"CUSTOMER로 들어오는 Source 자동 조회: {customer_source_systems}")

    required = {
        "TO-BE 모델 (target_model)": cfg.TARGET_MODEL_TABLE,
        "매핑 정의 (mapping_definition)": cfg.MAPPING_DEFINITION_TABLE,
        "코드 변환표 (code_mapping_asis_tobe)": cfg.CODE_MAPPING_TABLE,
        "Entity Integration 정의 (entity_integration_definition)": cfg.ENTITY_INTEGRATION_TABLE,
    }
    for src in customer_source_systems:
        required[f"Silver 입력 ({src})"] = cfg.silver_input_table(cfg.SOURCE_SYSTEMS[src]["silver"])
    missing = []
    for name, table in required.items():
        if spark.catalog.tableExists(table):
            print(f"✅ {name}: {table} ({spark.read.table(table).count()}행)")
        else:
            print(f"❌ {name}: {table} 없음")
            missing.append(table)
    if missing:
        raise RuntimeError(f"필요한 테이블이 없습니다: {missing}\n메타 테이블은 mapping_seed_loader.py를, Silver 입력은 DQ 실행을 먼저 하세요.")
else:
    silver_table = cfg.silver_input_table(cfg.SOURCE_SYSTEMS[SOURCE_SYSTEM.upper()]["silver"])
    required = {
        "TO-BE 모델 (target_model)": cfg.TARGET_MODEL_TABLE,
        "매핑 정의 (mapping_definition)": cfg.MAPPING_DEFINITION_TABLE,
        "코드 변환표 (code_mapping_asis_tobe)": cfg.CODE_MAPPING_TABLE,
        "Silver 입력": silver_table,
    }
    missing = []
    for name, table in required.items():
        if spark.catalog.tableExists(table):
            print(f"✅ {name}: {table} ({spark.read.table(table).count()}행)")
        else:
            print(f"❌ {name}: {table} 없음")
            missing.append(table)
    if missing:
        raise RuntimeError(f"필요한 테이블이 없습니다: {missing}\n메타 테이블은 mapping_seed_loader.py를, Silver 입력은 DQ 실행을 먼저 하세요.")

    defs = (
        spark.read.table(cfg.MAPPING_DEFINITION_TABLE)
        .filter(F.upper(F.trim("SOURCE_SYSTEM")) == SOURCE_SYSTEM.upper())
        .filter(F.upper(F.trim("TARGET_TABLE")) == TARGET_TABLE.upper())
        .filter(F.upper(F.trim("FINAL_MIGRATION_APPLY_YN")) == "Y")
        .filter(F.upper(F.trim("REVIEW_STATUS")).isin(*cfg.APPLY_REVIEW_STATUSES))
    )
    print(f"\n실행 대상 매핑 정의 ({SOURCE_SYSTEM} → {TARGET_TABLE}): {defs.count()}행")
    _show(defs.select("MAPPING_ID", "SOURCE_COLUMN", "TARGET_COLUMN", "MAPPING_TYPE", "PROCESS_TYPE", "REVIEW_STATUS"), 30)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 변환 실행
# MAGIC 정의가 잘못돼 있으면(없는 Source 컬럼, 같은 Target에 매핑 2개 등) 일부만 내보내지 않고 여기서 멈춥니다.

# COMMAND ----------

if IS_CUSTOMER:
    print("TARGET_TABLE=CUSTOMER: 이 셀은 스킵합니다 - 아래 'CUSTOMER 자동 실행' 섹션에서 Source별로 처리합니다.")
else:
    candidate, summary = engine.run(SOURCE_SYSTEM, TARGET_TABLE, source_batch_id=SOURCE_BATCH_ID)
    target_cols = [c for c in candidate.columns if not c.startswith("_")]   # Target 컬럼은 TARGET_TABLE마다 다르므로 매번 다시 뽑는다

    print("mapping_run_id  :", summary["mapping_run_id"])
    print("mapping_version :", summary["mapping_version"])
    print("입력 Silver     :", cfg.silver_input_table(summary["silver_source"]), "/ 배치", summary["source_batch_id"])
    match = summary["input_count"] == summary["output_count"]
    print(f"건수            : 입력 {summary['input_count']} → 출력 {summary['output_count']}  {'✅ 일치 (1:1)' if match else '❌ 불일치'}")
    print("\n적용 컬럼       :", ", ".join(summary["applied_columns"]))
    print("\n이번 단계 보류 컬럼 (NULL로 둠):")
    for col, reason in summary["deferred_columns"]:
        print(f"  - {col}: {reason}")
    print(f"\n값 변환 실패 행 : {summary['conversion_error_rows']}  {summary['conversion_errors_by_column'] or ''}")
    print(f"미매핑 코드 행  : {summary['unmapped_code_rows']}")
    for k, v in sorted(summary["unmapped_codes"].items(), key=lambda kv: -kv[1]):
        print(f"  - {k}: {v}건")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 결과 미리보기

# COMMAND ----------

if IS_CUSTOMER:
    print("TARGET_TABLE=CUSTOMER: 이 셀은 스킵합니다.")
else:
    _show(candidate.select(*target_cols, "_unmapped_codes", "_map_errors"), 10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Target 컬럼별 NULL 비율과 코드 분포
# MAGIC 보류 컬럼(CUST_ID, PRD_ID 등)은 100% NULL이 정상입니다. 적용 컬럼의 NULL이 예상보다 많으면 원인을 확인하세요.

# COMMAND ----------

if IS_CUSTOMER:
    print("TARGET_TABLE=CUSTOMER: 이 셀은 스킵합니다.")
else:
    total = summary["output_count"]
    null_counts = candidate.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in target_cols]).collect()[0].asDict()
    applied = set(summary["applied_columns"])
    print(f"{'컬럼':<14}{'NULL 건수':>10}{'비율':>9}   상태")
    for c in target_cols:
        n = null_counts[c] or 0
        print(f"{c:<14}{n:>10}{(n / total if total else 0):>9.1%}   {'적용' if c in applied else '보류(NULL 정상)'}")

    # 코드 분포를 볼 컬럼도 TARGET_TABLE마다 다르므로 하드코딩하지 않고, 1번에서 이미 읽어둔 defs(이 SOURCE_SYSTEM/TARGET_TABLE의
    # 매핑 정의)에서 CODE/LOOKUP으로 정의된 TARGET_COLUMN을 그대로 사용한다.
    code_cols = [r["TARGET_COLUMN"] for r in
                defs.filter(F.upper(F.trim("MAPPING_TYPE")) == "CODE").select("TARGET_COLUMN").distinct().collect()]
    for code_col in code_cols:
        if code_col in applied:
            print(f"\n{code_col} 분포")
            candidate.groupBy(code_col).count().orderBy(F.desc("count")).show(20, False)

# COMMAND ----------

# MAGIC %md ## 6. 저장
# MAGIC `gold_candidate.<target>`에 같은 소스·배치만 교체해서 저장합니다. (CUSTOMER는 아래 자동 실행 섹션에서 저장합니다)

# COMMAND ----------

if IS_CUSTOMER:
    print("TARGET_TABLE=CUSTOMER: 이 셀은 스킵합니다 - 아래 'CUSTOMER 자동 실행' 섹션에서 Source별로 저장합니다.")
elif SAVE_CANDIDATE:
    table = engine.save(candidate, summary)
    n = (spark.read.table(table)
         .filter((F.col("_source_system") == summary["silver_source"]) & (F.col("_source_batch_id") == summary["source_batch_id"]))
         .count())
    print(f"✅ {table} 저장 완료: 이번 소스·배치 {n}행")
else:
    print("SAVE_CANDIDATE=False: 저장하지 않았습니다.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. CUSTOMER 자동 실행 (Source별 Mapping+저장 → Entity Integration 1회)
# MAGIC `TARGET_TABLE="CUSTOMER"`일 때만 동작합니다. 1번 셀에서 조회한 `customer_source_systems`를 순서대로
# MAGIC `run()`+`save()`하고, **그 반복문이 전부 끝난 뒤에만** `engine.integrate("CUSTOMER")`를 정확히 1번 호출합니다
# MAGIC (반복문 안에서 호출하지 않습니다). COUNSEL/COMPLAINT 등 다른 Target에서는 이 셀 자체가 스킵됩니다.

# COMMAND ----------

if IS_CUSTOMER:
    print(f"[CUSTOMER Mapping]")
    for src in customer_source_systems:
        c_candidate, c_summary = engine.run(src, "CUSTOMER", source_batch_id=SOURCE_BATCH_ID)
        print(f"\n{src}")
        print(f"  입력: {c_summary['input_count']}")
        print(f"  출력: {c_summary['output_count']}")
        if SAVE_CANDIDATE:
            c_table = engine.save(c_candidate, c_summary)
            n = (spark.read.table(c_table)
                 .filter((F.col("_source_system") == c_summary["silver_source"])
                         & (F.col("_source_batch_id") == c_summary["source_batch_id"]))
                 .count())
            print(f"  저장: {n}")
        else:
            print("  저장: SAVE_CANDIDATE=False (저장 안 함)")

    print("\n[CUSTOMER Entity Integration]")
    if SAVE_CANDIDATE:
        # 모든 Source의 run()+save()가 끝난 뒤, 반복문 밖에서 정확히 1번만 호출한다.
        integration_result = engine.integrate("CUSTOMER")
        gold_candidate_n = spark.table(cfg.gold_candidate_table("CUSTOMER")).count()
        error_table = cfg.gold_mapping_error_table("CUSTOMER")
        error_n = spark.table(error_table).count() if spark.catalog.tableExists(error_table) else 0
        print(f"  Integration 실행: 1회")
        print(f"  최종 CUSTOMER: {gold_candidate_n}행")
        print(f"  gold_mapping_error.customer: {error_n}행")
        print(f"\n{integration_result}")
    else:
        print("  SAVE_CANDIDATE=False라 저장된 candidate가 없어 Integration을 실행하지 않았습니다.")
else:
    print(f"TARGET_TABLE={TARGET_TABLE}: CUSTOMER가 아니므로 이 셀은 스킵합니다 (6번 셀까지가 이 Target의 전체 흐름입니다).")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from maps_databricks.gold_mapping_error.customer;