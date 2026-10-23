# =============================================================================
# PRODUCT 메타데이터(product_mapping_seed_data.py, 하드코딩본) 적재 셀
# (Databricks 노트북에 셀 하나로 붙여넣어 실행 - product_master_run.py보다 먼저 한 번 실행해야 한다)
#
# mapping_seed_loader.py는 CSV 전체로 4개 메타 테이블을 통째로 교체(overwrite)하는 방식이라, 그걸 그대로
# 쓰면 COUNSEL/CUSTOMER/COMPLAINT 등 다른 Target의 기존 메타데이터가 날아간다. 이 스크립트는 PRODUCT /
# PRODUCT_MASTER에 해당하는 행만 replaceWhere로 selective하게 반영하고 나머지는 건드리지 않는다.
#
# AI Mapping이 실제로 구현되면 product_mapping_seed_data.py의 AI_MAPPING_RECOMMENDATIONS/
# MAPPING_DEFINITION_ROWS를 AI가 만든 CSV로 교체하면 되고(사용자 승인 단계는 그대로), 이 replaceWhere
# 반영 방식 자체는 계속 재사용할 수 있다.
# =============================================================================
from pyspark.sql import functions as F

try:
    import src.mapping.mapping_config as cfg
except ModuleNotFoundError:
    import mapping_config as cfg

from product_mapping_seed_data import TARGET_MODEL_ROWS, MAPPING_DEFINITION_ROWS, CODE_MAPPING_ROWS

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.UC_CATALOG}.{cfg.META_SCHEMA}")


def _load_rows(table: str, rows: list, replace_where: str) -> None:
    sdf = spark.createDataFrame(rows)

    if not spark.catalog.tableExists(table):
        # 이 메타 테이블이 아직 없다면(이 프로젝트에서 처음 만드는 경우) 그대로 생성한다.
        sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
        print(f"✅ {table}: 신규 생성, {sdf.count()}행 적재")
        return

    # 이미 mapping_seed_loader.py로 다른 Target(COUNSEL/CUSTOMER/...)의 행이 적재돼 있는 정식 테이블이다 -
    # 그 실제 스키마(컬럼 집합)에 맞춰 우리 쪽 행을 정렬한다: 기존 스키마에 없는 컬럼은 버리고(정의되지 않은
    # 컬럼을 함부로 새로 만들지 않는다), 우리 쪽에 없는 기존 컬럼은 NULL로 채운다.
    existing_cols = spark.table(table).columns
    dropped = set(sdf.columns) - set(existing_cols)
    if dropped:
        print(f"⚠️  {table}: 기존 스키마에 없어 반영하지 못한 컬럼 {sorted(dropped)} - 필요하면 기존 테이블에 "
              f"먼저 컬럼을 추가한 뒤 다시 실행하세요.")
    select_exprs = [(F.col(c) if c in sdf.columns else F.lit(None).cast("string")).alias(c) for c in existing_cols]
    aligned = sdf.select(*select_exprs)

    (aligned.write.format("delta").mode("overwrite")
     .option("replaceWhere", replace_where).option("mergeSchema", "true").saveAsTable(table))
    print(f"✅ {table}: PRODUCT 관련 {aligned.count()}행 반영 (replaceWhere: {replace_where})")


_load_rows(cfg.TARGET_MODEL_TABLE, TARGET_MODEL_ROWS, "TABLE_NAME = 'PRODUCT'")
_load_rows(cfg.MAPPING_DEFINITION_TABLE, MAPPING_DEFINITION_ROWS,
           "TARGET_TABLE = 'PRODUCT' AND SOURCE_SYSTEM = 'PRODUCT_MASTER'")
_load_rows(cfg.CODE_MAPPING_TABLE, CODE_MAPPING_ROWS, "SOURCE_SYSTEM = '상품마스터'")