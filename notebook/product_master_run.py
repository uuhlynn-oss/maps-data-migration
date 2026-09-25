# =============================================================================
# PRODUCT Mapping Execution 셀 (Master Data Integration)
#
# 이전 버전은 id_column="PRD_ID", key_column="PRD_CD", prefix="PRD"를 이 노트북에 직접 하드코딩했다.
# 이제 mapping_orchestrator.run_target()이 다음을 모두 metadata에서 동적으로 판정하므로 그 하드코딩을
# 제거한다 (mapping_engine.py의 run()/load_master_data() 로직 자체는 무수정):
#   - Integration Type: meta.entity_integration_definition (TARGET_ENTITY=PRODUCT, INTEGRATION_TYPE=MASTER)
#   - id_column/key_column/prefix: meta.target_model의 KEY='PK'(PRD_ID)/KEY='UK'(PRD_CD) 행에서 도출
#
# 이전(하드코딩) 버전과 결과가 동일함을 verify_run_target.py로 이미 검증했다
# (gold_candidate.product: row_count/schema/PK 중복/NULL count/값 diff 전부 189/189 완전 일치).
# =============================================================================
try:
    import src.mapping.mapping_config as cfg
    import src.mapping.mapping_engine as eng
    import src.mapping.mapping_orchestrator as orch
except ModuleNotFoundError:
    import mapping_config as cfg
    import mapping_engine as eng
    import mapping_orchestrator as orch

engine = eng.MappingEngine.from_tables(spark)

result = orch.run_target(engine, "PRODUCT")
print("[run_target] integration_type:", result["integration_type"])
print("[run_target] sources:", result["sources"])
print("[run_target] master_params (target_model에서 동적 도출):", result.get("master_params"))
print("[run_target] master_load_result:", result.get("master_load_result"))
