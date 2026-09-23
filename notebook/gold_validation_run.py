# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Target Validation 실행: gold_candidate → Gold / gold_quarantine / MIGRATION_TRACE
# MAGIC
# MAGIC **반드시 mapping_run.py로 필요한 소스를 전부 실행한 "같은 세션"에서 이어서 실행하세요.**
# MAGIC `gold_candidate_<target>`은 임시 뷰라 세션이 끝나면 사라집니다 (계보 보존 조건: mapping_run.py 상단 안내 참고).
# MAGIC
# MAGIC **이 노트북이 하는 일**: 통합 후보 확인 → 검증 실행(15개 규칙 + CTI_ID 중복 보정) → 요약 → 위반 규칙 분포 → 저장(Gold/격리/MIGRATION_TRACE) → 결과 확인

# COMMAND ----------

TARGET_TABLE = "COUNSEL"
SAVE = True          # False면 저장 없이 결과만 확인

# COMMAND ----------

import sys
from pyspark.sql import functions as F

if "PROJECT_ROOT" in dir() and PROJECT_ROOT and PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
try:
    import src.gold.gold_validation_config as gcfg
    import src.gold.gold_validation_runner as gv
except ModuleNotFoundError:
    import gold_validation_config as gcfg
    import gold_validation_runner as gv


def _show(df, n=10):
    try:
        display(df.limit(n))
    except NameError:
        df.show(n, False)


# COMMAND ----------

# MAGIC %md ## 1. 통합 후보 확인
# MAGIC mapping_run.py의 6번 셀이 만든 `gold_candidate_<target>`(소스 통합본)이 이 세션에 있는지 확인합니다.

# COMMAND ----------

view_name = gcfg.gold_candidate_view(TARGET_TABLE)
if view_name not in [t.name for t in spark.catalog.listTables() if t.isTemporary]:
    raise RuntimeError(f"{view_name} 임시 뷰가 없습니다. mapping_run.py를 이 세션에서 먼저 실행하세요 (여러 소스를 검증하려면 각 소스를 이 세션에서 순서대로 실행).")

candidate = spark.table(view_name)
print(f"{view_name}: {candidate.count()}행")
candidate.groupBy("_source_system").count().show()

# COMMAND ----------

# MAGIC %md ## 2. 검증 실행
# MAGIC TO-BE 모델에서 규칙을 다시 만들고(모델이 바뀌면 규칙도 같이 바뀝니다), 승인된 보정(CTI_ID 중복 → CNSL_ID 재발급, Review R3)을 적용한 뒤 재검증합니다.

# COMMAND ----------

validator = gv.TargetValidator.from_tables(spark)
passed, failed, summary = validator.run(TARGET_TABLE, candidate=candidate)

print("validation_run_id:", summary["validation_run_id"])
match = summary["input_count"] == summary["loaded_count"] + summary["quarantined_count"]
print(f"입력 {summary['input_count']} = 적재 {summary['loaded_count']} + 격리 {summary['quarantined_count']}  "
     f"{'✅ 대사 일치' if match else '❌ 불일치'}")
print(f"CTI_ID 중복 보정: {summary['pk_dedup_fixed']}건 (Review R3, 아웃바운드만 대상)")
print(f"\n적용된 규칙 {len(summary['rules_applied'])}개:", summary["rules_applied"])

# COMMAND ----------

# MAGIC %md ## 3. 위반 규칙 분포

# COMMAND ----------

if summary["violations_by_rule"]:
    for rule_id, cnt in sorted(summary["violations_by_rule"].items(), key=lambda kv: -kv[1]):
        print(f"  {rule_id:<28}{cnt}건")
    print("\n격리 사유 샘플 (최대 10건)")
    _show(failed.select("_source_system", "_source_record_key", "_violations"), 10)
else:
    print("위반 없음 (격리된 레코드가 없습니다)")

# COMMAND ----------

# MAGIC %md ## 4. 결과 미리보기 (Gold 형태)

# COMMAND ----------

_show(passed, 10)

# COMMAND ----------

# MAGIC %md ## 5. 저장
# MAGIC Gold Target 테이블(append), gold_quarantine(소스·배치 단위 교체), MIGRATION_TRACE(모든 원천 레코드, 추가 전용).

# COMMAND ----------

if SAVE:
    tables = validator.save(passed, failed, summary)
    for label, tbl in tables.items():
        print(f"✅ {label}: {tbl}")
    gold_n = spark.read.table(tables["gold_table"])
    print(f"\n{tables['gold_table']}: 이번 실행 적재분 {summary['loaded_count']}행 (테이블 전체 {gold_n.count()}행, 재실행 시 누적 - 알려진 제약)")
else:
    print("SAVE=False: 저장하지 않았습니다.")