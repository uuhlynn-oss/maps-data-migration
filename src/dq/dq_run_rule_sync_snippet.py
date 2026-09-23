# =============================================================================
# dq_run 노트북의 맨 앞 셀에 그대로 붙여넣으세요.
#
# DQ를 실행하기 전에 Volume의 규칙 CSV를 항상 먼저 동기화합니다. sync_from_csv()는 "바뀐 것만" 반영하는
# 멱등 함수라 매번 호출해도 안전합니다(바뀐 게 없으면 아무것도 쓰지 않습니다). 다만 무엇이 바뀌었는지는
# 아래 출력에 항상 남으므로, 실행 로그를 보면 이번 DQ 실행이 어떤 규칙 버전으로 돌았는지 알 수 있습니다.
#
# CSV를 새로 편집한 뒤 먼저 확인하고 싶다면(예: 실수로 규칙이 빠지진 않았는지) dq_rule_loader.py를 따로
# 먼저 실행해 보세요. 이 셀은 그 확인 없이 바로 반영합니다.
# =============================================================================
try:
    import src.dq.dq_config as dq_config
    import src.dq.dq_engine as dq_engine
except ModuleNotFoundError:
    import dq_config
    import dq_engine

RULE_CSV = "/Volumes/maps_databricks/meta/files/dq_rule_def.csv"    # <- 실제 경로로 변경

dq_engine.RuleRepository(spark).sync_from_csv(RULE_CSV, change_reason="dq_run 자동 동기화")
