import datetime

# 1. 실제 실행되는 규칙(meta.dq_rule_def, 활성 버전)에서 대상 테이블 목록을 뽑는다.
#    dq_config.DQ_RULES(코드 규칙)로는 안 된다 - DQRunner 기본값이 rule_source="table"이라 실제로는
#    meta.dq_rule_def를 실행하는데, 거기서 채널/규칙을 추가해도 DQ_RULES는 코드를 안 고치면 그대로라서
#    새로 추가한 소스가 이 목록에 영영 안 잡힌다.
try:
    import src.dq.dq_config as dq_config
    import src.dq.dq_engine as dq_engine
except ModuleNotFoundError:
    import dq_config
    import dq_engine

# 2. Volume의 규칙 CSV를 먼저 동기화한다 (dq_run_rule_sync_snippet.py와 같은 내용).
#    sync_from_csv()는 "바뀐 것만" 반영하는 멱등 함수라 매번 호출해도 안전하다.
RULE_CSV = "/Volumes/maps_databricks/meta/files/dq_rule_def.csv"    # <- 실제 경로로 변경
dq_engine.RuleRepository(spark).sync_from_csv(RULE_CSV, change_reason="dq_run 자동 동기화")

# 3. 지금 실제로 활성화된 규칙 기준으로 대상 테이블 목록을 뽑는다 (채널 추가 시 CSV만 바뀌면 자동 반영)
target_tables = sorted({r["target_table"] for r in dq_engine.RuleRepository(spark).load_current()})
print(f"📋 [자동 감지된 검증 대상 테이블 목록]: {target_tables}")

# 4. 러너 객체 생성 (rule_source 기본값이 "table"이라 방금 동기화한 meta.dq_rule_def를 그대로 읽는다)
runner = dq_engine.DQRunner(spark=spark, dbutils=dbutils)

# ⚠️ 아래 초기화(DELETE) 단계는 지금 dq_engine.py의 설계(dq_result는 append만 하고 이력을 전부 보존)와
# 반대 방향입니다. 이력을 계속 쌓는 게 맞는지, 하루 1번 최신 결과만 남기는 걸로 설계가 바뀐 건지 확인 후
# 다시 여쭤보고 정리하겠습니다 - 지금은 그대로 두었습니다.
current_date = datetime.date.today().strftime("%Y-%m-%d")
try:
    spark.sql(f"DELETE FROM {dq_config.DQ_RESULT_TABLE} WHERE ingest_date = '{current_date}'")
    print(f"🧹 [DQ 결과 초기화] {current_date} 일자 기준 기존 DQ 결과 정리를 완료했습니다.")
except Exception as e:
    print(f"ℹ️ [안내] 초기화 생략 (테이블이 아직 없거나 첫 실행): {e}")

# 5. 모든 채널/테이블을 자동으로 순회하며 DQ 검사 실행 및 결과 행(Row) 단위 누적(Append)
total_summaries = []

for target_table in target_tables:
    print(f"\n{'=' * 50}")
    print(f"🚀 [DQ 실행 중] 대상 테이블: {target_table}")
    print(f"{'=' * 50}")

    dq_results, dq_summary = runner.run_table_dq(target_table=target_table)

    total_summaries.append(dq_summary)
    print(f"✅ [완료] {target_table} 검증 완료")

print("\n🎉 [모든 채널 DQ 검사 완료] dq_result 테이블에 결과가 안전하게 누적되었습니다.")
