# =============================================================================
# DQ 규칙 적재 셀  (Databricks 노트북에 셀 하나로 붙여넣어 실행)
#
# 규칙을 새로 바꾼 뒤 "내용을 먼저 확인하고" 적재하고 싶을 때 이 셀을 씁니다.
# 평소 dq_run 실행에는 이 셀이 필요 없습니다 - dq_run이 매번 시작할 때 자동으로 같은 로직(sync_from_csv)을
# 호출합니다 (아래 "dq_run 맨 앞에 붙이는 코드" 참고). 이 파일은 그 자동 반영 전에 미리 무엇이 바뀌는지
# 사람이 확인하고 싶을 때 쓰는 보조 도구입니다.
#
#   - 처음에는 dq_rule_def.csv(현재 코드의 규칙 35개를 내보낸 파일)를 Volume에 올려 그대로 적재하면 모든 규칙이 버전 1이 됩니다.
#   - 이후 규칙을 바꿀 때는 CSV의 해당 행만 고쳐서 이 셀을 다시 실행합니다 (코드 배포 불필요).
#       * 내용이 바뀐 규칙 -> 버전 +1 (무엇이 바뀌었는지 출력에 표시)
#       * 안 바뀐 규칙    -> 그대로
#       * CSV에서 뺀 규칙  -> 비활성 버전 추가 (더 이상 실행되지 않음, 이력은 남음)
#   - APPLY = False로 바꾸면 미리보기만 하고 적재하지 않습니다.
#   - 검증에 하나라도 실패하면 아무것도 적재하지 않고 모든 오류를 한꺼번에 알려 줍니다.
#   - CSV는 반드시 "전체 목록"이어야 합니다 (일부만 올리면 나머지가 비활성 처리됩니다).
# =============================================================================
import importlib

try:
    import src.dq.dq_config as dq_config
    import src.dq.dq_engine as dq_engine
except ModuleNotFoundError:
    import dq_config
    import dq_engine
for _m in (dq_config, dq_engine):
    importlib.reload(_m)

RULE_CSV = "/Volumes/maps_databricks/meta/files/dq_rule_def.csv"    # <- 실제 경로로 변경
APPLY = True                                                        # False로 바꾸면 미리보기만

dq_engine.RuleRepository(spark).sync_from_csv(RULE_CSV, change_reason="수동 적재", apply=APPLY)
