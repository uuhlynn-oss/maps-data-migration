# =============================================================================
# PRODUCT_MAPPING 테스트 시드 적재 셀 (Databricks 노트북에 셀 하나로 붙여넣어 실행)
#
# ★★★ 실제 AI Mapping/사람 승인 데이터가 아니다 ★★★ (AI Mapping 알고리즘은 이번 범위 밖)
# CODE_MAPPING_RULE=="PRODUCT_MAPPING" FK Lookup 배관(mapping_engine.py, SRC_SYS+SRC_PRD_CD+APRV_YN='Y'
# -> TGT_PRD_ID)이 정상 동작하는지 검증하기 위한 최소 시드다. mapping_engine.py는 이 스크립트가 전혀
# 건드리지 않는다 - 기존 CODE_MAPPING_RULE=="PRODUCT_MAPPING" 로직을 그대로 쓴다.
#
# SRC_PRD_CD가 실제로 어느 표준상품(PRD_CD)을 가리키는지의 업무적 대응은 이 스크립트가 정하지 않는다 -
# TEST_PAIRS의 STD_PRD_CD는 "엔진 배관이 도는지"만 보려고 테스트 편의로 고른 실제 표준코드일 뿐이다.
#
# TGT_PRD_ID는 하드코딩하지 않는다 - PRODUCT Master Data Integration이 이미 완료돼 gold_candidate.product에
# 실제로 있는 PRD_ID를 그 자리에서 조회해서 채운다(재실행하지 않음, 이미 있는 결과만 읽는다). 그래서 이
# 스크립트는 어느 환경에서 돌려도(로컬 검증이든 실제 Databricks든) 그 환경의 실제 PRD_ID를 그대로 반영한다.
# =============================================================================
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

try:
    import src.mapping.mapping_config as cfg
except ModuleNotFoundError:
    import mapping_config as cfg

# (SRC_SYS, SRC_PRD_CD, 테스트로 고른 실제 STD_PRODUCT_CODE) - SRC_SYS/SRC_PRD_CD는 mapping_definition.csv의
# 실제 SAMPLE_VALUE를 그대로 썼다. 세 번째 값(표준코드)은 그 채널 코드가 실제로 이 상품이라는 업무적 확인이
# 아니라, 엔진 동작 검증용으로 존재가 확실한 실제 STD_PRODUCT_CODE 중 하나를 고른 것이다.
TEST_PAIRS = [
    ("INBOUND",   "LONG-ACC-001",   "ACC-001"),
    ("OUTBOUND",  "TRAVEL-03",      "TRV-001"),
    ("HOMEPAGE",  "AUTO-PERS-002",  "AUTO-001"),
]

product = spark.table(cfg.gold_candidate_table("PRODUCT")).select("PRD_CD", "PRD_ID").dropDuplicates(["PRD_CD"])
by_prd_cd = {r["PRD_CD"]: r["PRD_ID"] for r in product.collect()}

rows, missing = [], []
for i, (src_sys, src_cd, std_cd) in enumerate(TEST_PAIRS, start=1):
    tgt_prd_id = by_prd_cd.get(std_cd)
    if tgt_prd_id is None:
        missing.append(std_cd)
        continue
    rows.append({
        "PRD_MAP_ID": f"PM-TEST-{i:03d}", "SRC_SYS": src_sys, "SRC_PRD_CD": src_cd,
        "SRC_PRD_NM": f"(테스트) {src_cd}", "TGT_PRD_ID": tgt_prd_id,
        "MAP_ST_CD": "TEST", "CNFD_SCR": None, "APRV_YN": "Y",
        "APRV_BY": "TEST_SEED", "APRV_DTM": None,
    })

if missing:
    raise ValueError(f"gold_candidate.product에 없는 PRD_CD: {missing} - PRODUCT Master Data Integration이 "
                      f"이미 완료돼 있어야 한다 (재실행하지 않음, 확인만 필요).")

cols = list(rows[0].keys())
schema = StructType([StructField(c, StringType(), True) for c in cols])
sdf = spark.createDataFrame([tuple(r[c] for c in cols) for r in rows], schema=schema)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.UC_CATALOG}.{cfg.GOLD_CANDIDATE_SCHEMA}")
sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(cfg.PRODUCT_MAPPING_TABLE)

print(f"✅ {cfg.PRODUCT_MAPPING_TABLE}: {sdf.count()}행 (테스트 시드 - TGT_PRD_ID는 gold_candidate.product 실제 값)")
for r in rows:
    print(f"   {r['SRC_SYS']}.{r['SRC_PRD_CD']} -> {r['TGT_PRD_ID']}")
