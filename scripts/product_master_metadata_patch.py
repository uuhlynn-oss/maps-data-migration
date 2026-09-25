# =============================================================================
# 1회성 metadata 패치 (Databricks 노트북 셀에 붙여넣어 실행)
#
# mapping_orchestrator.run_target()이 PRODUCT를 MASTER로 판정하고 target_model에서 PK/UK를 동적으로
# 도출하는 데 필요한 두 가지 gap만 최소 범위로 메운다. 다른 파일(product_mapping_seed_data.py,
# product_master_seed_loader.py, mapping_engine.py, mapping_orchestrator.py)은 전혀 건드리지 않는다.
#
#   1) meta.target_model : PRODUCT.PRD_ID -> KEY='PK', PRODUCT.PRD_CD -> KEY='UK'
#      replaceWhere를 COLUMN_NAME까지 좁혀서 이 2행만 교체한다 - PRODUCT의 다른 컬럼(PRD_NM/PRD_CLS_CD/
#      PRD_CTGR_CD)이나 다른 Target(CUSTOMER/CONTRACT/COUNSEL 등)의 행은 전혀 건드리지 않는다.
#   2) meta.entity_integration_definition : TARGET_ENTITY=PRODUCT, INTEGRATION_TYPE=MASTER 행 1개 추가
#      replaceWhere TARGET_ENTITY='PRODUCT' - 다른 TARGET_ENTITY(CUSTOMER/CONTRACT) 행은 영향 없음.
#
# 멱등적이다 - 여러 번 실행해도 같은 결과가 된다. product_master_seed_loader.py를 먼저 실행해
# target_model/mapping_definition에 PRODUCT 행이 이미 있어야 한다.
# =============================================================================
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

try:
    import src.mapping.mapping_config as cfg
except ModuleNotFoundError:
    import mapping_config as cfg

# ---------------------------------------------------------------------------
# 1) target_model: PRODUCT.PRD_ID(PK) / PRD_CD(UK) 보강
# ---------------------------------------------------------------------------
if "KEY" not in spark.table(cfg.TARGET_MODEL_TABLE).columns:
    raise RuntimeError(
        "target_model에 KEY 컬럼이 없습니다. CUSTOMER/CONTRACT의 FK/PK 판정이 이미 이 컬럼에 의존하고 "
        "있으므로(mapping_engine.py의 _fk_reference_table/_target_uk_column), 이 컬럼 자체가 없다면 "
        "이 패치보다 먼저 원인을 확인해야 합니다."
    )

# 기존 PRD_ID/PRD_CD 행의 나머지 컬럼(DATA_TYPE, ORDINAL 등)은 그대로 두고 KEY만 갱신한다.
target_model_patched = (
    spark.table(cfg.TARGET_MODEL_TABLE)
    .filter((F.col("TABLE_NAME") == "PRODUCT") & (F.col("COLUMN_NAME").isin("PRD_ID", "PRD_CD")))
    .withColumn(
        "KEY",
        F.when(F.col("COLUMN_NAME") == "PRD_ID", F.lit("PK"))
         .when(F.col("COLUMN_NAME") == "PRD_CD", F.lit("UK"))
         .otherwise(F.col("KEY")),
    )
)
n_patched = target_model_patched.count()
if n_patched != 2:
    raise RuntimeError(
        f"target_model에서 PRODUCT.PRD_ID/PRD_CD 행을 정확히 2개 찾아야 하는데 {n_patched}개입니다. "
        f"product_master_seed_loader.py를 먼저 실행했는지 확인하세요."
    )

(target_model_patched.write.format("delta").mode("overwrite")
 .option("replaceWhere", "TABLE_NAME = 'PRODUCT' AND COLUMN_NAME IN ('PRD_ID','PRD_CD')")
 .option("mergeSchema", "true")
 .saveAsTable(cfg.TARGET_MODEL_TABLE))
print(f"✅ {cfg.TARGET_MODEL_TABLE}: PRODUCT.PRD_ID(KEY=PK)/PRD_CD(KEY=UK) 2행 갱신 완료 "
      f"(PRODUCT의 다른 컬럼, 다른 Target 행 변경 없음)")

# 반영 확인
spark.table(cfg.TARGET_MODEL_TABLE).filter(F.col("TABLE_NAME") == "PRODUCT") \
     .select("TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "KEY", "ORDINAL").orderBy("ORDINAL").show(20, False)

# ---------------------------------------------------------------------------
# 2) entity_integration_definition: TARGET_ENTITY=PRODUCT, INTEGRATION_TYPE=MASTER 행 추가
# ---------------------------------------------------------------------------
existing_cols = spark.table(cfg.ENTITY_INTEGRATION_TABLE).columns
product_row = {
    "TARGET_ENTITY": "PRODUCT",
    "INTEGRATION_TYPE": "MASTER",
    "MATCHING_RULE": None,
    "MATCHING_KEY_COLUMNS": None,
    "CONFLICT_RULE": None,
    "CONFLICT_REFERENCE": None,
    "REVIEW_STATUS": "APPROVED",
    "FINAL_MIGRATION_APPLY_YN": "Y",
}
dropped = set(product_row) - set(existing_cols)
if dropped:
    print(f"⚠️  entity_integration_definition 기존 스키마에 없어 반영 못한 컬럼: {sorted(dropped)}")

values = tuple(product_row.get(c) for c in existing_cols)
schema = StructType([StructField(c, StringType(), True) for c in existing_cols])
sdf = spark.createDataFrame([values], schema=schema)

(sdf.write.format("delta").mode("overwrite")
 .option("replaceWhere", "TARGET_ENTITY = 'PRODUCT'")
 .option("mergeSchema", "true")
 .saveAsTable(cfg.ENTITY_INTEGRATION_TABLE))
print(f"✅ {cfg.ENTITY_INTEGRATION_TABLE}: TARGET_ENTITY=PRODUCT, INTEGRATION_TYPE=MASTER 행 반영 완료 "
      f"(다른 TARGET_ENTITY 행 변경 없음)")

# 반영 확인
spark.table(cfg.ENTITY_INTEGRATION_TABLE).select(*existing_cols).show(20, False)
