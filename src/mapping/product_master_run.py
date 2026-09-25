# =============================================================================
# PRODUCT Mapping Execution 셀 (Master Data Integration)
#
# 먼저 product_master_seed_loader.py로 silver_candidate.product_master를 적재해 둔 뒤 이 셀을 실행한다.
# generic MappingEngine을 그대로 쓴다 - PRODUCT 전용 엔진을 따로 만들지 않는다.
#   run()               : 컬럼 매핑(RENAME/COPY) + 코드 변환(CODE/LOOKUP). PRD_ID(GENERATE_ID)는 보류(NULL).
#   load_master_data()  : 자연키(PRD_CD) 기준 PRD_ID 재사용/신규 발급 + "현재 전체 스냅샷"으로 gold_candidate 반영.
# =============================================================================
try:
    import src.mapping.mapping_config as cfg
    import src.mapping.mapping_engine as eng
except ModuleNotFoundError:
    import mapping_config as cfg
    import mapping_engine as eng

engine = eng.MappingEngine.from_tables(spark)

# silver_df 생략 -> silver_candidate.product_master의 최신 배치(=가장 최근 적재)를 자동으로 읽는다.
candidate, summary = engine.run("PRODUCT_MASTER", "PRODUCT")
print("[run] summary:", summary)

result = engine.load_master_data(
    "PRODUCT", candidate, summary,
    id_column="PRD_ID", key_column="PRD_CD", prefix="PRD",
)
print("[load_master_data] result:", result)
