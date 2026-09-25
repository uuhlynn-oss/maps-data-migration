# =============================================================================
# target_model / product_mapping 역할 분리 진단 (Databricks 노트북 셀에 붙여넣어 실행)
# 읽기 전용 - 아무 테이블도 수정하지 않는다.
# =============================================================================
from pyspark.sql import functions as F

try:
    import src.mapping.mapping_config as cfg
except ModuleNotFoundError:
    import mapping_config as cfg

# 1) target_model에 TABLE_NAME='PRODUCT_MAPPING' 행(크로스워크 스키마 정의)이 실제로 있는지 확인
pm_in_target_model = spark.table(cfg.TARGET_MODEL_TABLE).filter(
    F.upper(F.trim(F.col("TABLE_NAME"))) == "PRODUCT_MAPPING"
)
n = pm_in_target_model.count()
print(f"target_model 내 TABLE_NAME='PRODUCT_MAPPING' 행 수: {n}")
if n > 0:
    pm_in_target_model.orderBy("ORDINAL").show(50, False)

# 2) target_model에 등록된 전체 TABLE_NAME 중, 실제 run()이 가능한 Target 화이트리스트
#    (SUPPORTED_TARGET_TABLES)에는 없는 이름을 찾는다 - PRODUCT_MAPPING처럼 "구조만 정의돼 있고
#    Mapping Execution의 실행 대상은 아닌" 이름이 몇 개나 더 있는지 한눈에 본다.
all_tables = [r["t"] for r in spark.table(cfg.TARGET_MODEL_TABLE)
              .select(F.upper(F.trim(F.col("TABLE_NAME"))).alias("t")).distinct().collect()]
not_supported = sorted(set(all_tables) - set(cfg.SUPPORTED_TARGET_TABLES))
print(f"\ntarget_model에는 있지만 SUPPORTED_TARGET_TABLES에는 없는 TABLE_NAME: {not_supported}")

# 3) (PRODUCT_MAPPING 행이 있다면) 실제 gold_candidate.product_mapping 데이터의 컬럼과
#    target_model에 정의된 컬럼이 서로 일치하는지만 대조한다 - 문서로서 정확한지 확인용, 수정하지 않는다.
if n > 0 and spark.catalog.tableExists(cfg.PRODUCT_MAPPING_TABLE):
    actual_cols = spark.table(cfg.PRODUCT_MAPPING_TABLE).columns
    defined_cols = [r["COLUMN_NAME"] for r in pm_in_target_model.orderBy("ORDINAL").collect()]
    print(f"\ngold_candidate.product_mapping 실제 컬럼   : {actual_cols}")
    print(f"target_model에 정의된 PRODUCT_MAPPING 컬럼: {defined_cols}")
    print(f"일치 여부: {actual_cols == defined_cols}")
