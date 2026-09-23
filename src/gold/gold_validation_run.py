# Databricks notebook source
# MAGIC %md
# MAGIC # Target Validation만 다시 실행 (재검증용)
# MAGIC
# MAGIC **보통은 `mapping_run_all.py` 하나로 Mapping + Validation이 한 번에 끝납니다.** 이 노트북은 매핑은 그대로 두고
# MAGIC Validation만 다시 돌리고 싶을 때(예: `target_model`이나 규칙을 고친 뒤 재확인) 쓰는 보조 노트북입니다.
# MAGIC
# MAGIC `gold_candidate.<target>` **물리 테이블**(mapping_engine.save()가 저장한 것)을 읽습니다. 다른 노트북에서 저장한 것도 보입니다.

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

# MAGIC %md ## 1. 후보 확인
# MAGIC `mapping_run.py` 또는 `mapping_run_all.py`가 저장한 물리 테이블입니다.

# COMMAND ----------

candidate_table = gcfg.gold_candidate_table(TARGET_TABLE)
if not spark.catalog.tableExists(candidate_table):
    raise RuntimeError(f"{candidate_table}이(가) 없습니다. mapping_run.py 또는 mapping_run_all.py를 먼저 실행하세요.")

candidate = spark.table(candidate_table)
print(f"{candidate_table}: {candidate.count()}행")
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
