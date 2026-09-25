# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Target Validation 실행: target_model 기반 자동 실행
# MAGIC
# MAGIC 이전 버전은 `TARGET_TABLE`을 노트북 파라미터로 하나씩 지정했다. 이제
# MAGIC `mapping_orchestrator.discover_execution_targets()`(Mapping Execution과 동일한 함수 재사용 - "실행
# MAGIC 가능한 Target이 무엇인가"라는 질문 자체가 두 단계에서 같다)로 대상을 자동 조회해 순회한다.
# MAGIC `gold_validation_runner.py`의 `TargetValidator` 로직 자체는 수정하지 않았다.
# MAGIC
# MAGIC ⚠️ **FK 참조 순서 주의는 여전히 유효하다**: 참조 대상 Target이 먼저 검증·저장(`gold.<table>`)되어
# MAGIC 있어야 그 FK가 실제로 검증된다(`summary["relationship_rules_skipped"]`가 비어 있지 않으면 일부 FK가
# MAGIC 검증되지 않은 것). `discover_execution_targets()`가 돌려주는 순서(`SUPPORTED_TARGET_TABLES` 순서 -
# MAGIC CUSTOMER가 CONTRACT보다 앞)가 이 순서를 그대로 지켜준다.

# COMMAND ----------

SAVE = True  # False면 저장 없이 결과만 확인

# COMMAND ----------

import os
import sys

for _p in sys.path:
    _parent = os.path.dirname(_p)
    if os.path.isdir(os.path.join(_parent, "src")) and _parent not in sys.path:
        sys.path.insert(0, _parent)
        break

try:
    import src.gold.gold_validation_config as gcfg
    import src.gold.gold_validation_runner as gv
    import src.mapping.mapping_orchestrator as orch  # discover_execution_targets()만 재사용
except ModuleNotFoundError:
    import gold_validation_config as gcfg
    import gold_validation_runner as gv
    import mapping_orchestrator as orch

validator = gv.TargetValidator.from_tables(spark)

# COMMAND ----------

# MAGIC %md ## 1. 검증 대상 자동 조회
# MAGIC Mapping Execution과 동일한 목록(target_model ∩ SUPPORTED_TARGET_TABLES)을 그대로 쓴다.

# COMMAND ----------

targets = orch.discover_execution_targets(spark)
print(f"Validation 대상 (target_model ∩ SUPPORTED_TARGET_TABLES): {targets}")

# COMMAND ----------

# MAGIC %md ## 2. Target별 순차 검증

# COMMAND ----------

summaries = {}
for target_table in targets:
    print(f"\n{'=' * 60}\n[{target_table}] Validation 시작\n{'=' * 60}")

    candidate_table = gcfg.gold_candidate_table(target_table)
    if not spark.catalog.tableExists(candidate_table):
        print(f"⚠️  {candidate_table} 없음 - 03_mapping_run.py를 먼저 실행하세요. 이 Target은 건너뜁니다.")
        continue

    passed, failed, summary = validator.run(target_table)
    summaries[target_table] = summary

    match = summary["input_count"] == summary["loaded_count"] + summary["quarantined_count"]
    print(f"입력 {summary['input_count']} = 적재 {summary['loaded_count']} + 격리 {summary['quarantined_count']} "
          f"{'✅ 대사 일치' if match else '❌ 불일치'}")
    if summary["violations_by_rule"]:
        for rule_id, cnt in sorted(summary["violations_by_rule"].items(), key=lambda kv: -kv[1]):
            print(f"    위반 {rule_id}: {cnt}건")
    if summary["relationship_rules_skipped"]:
        print(f"⚠️  검증되지 않은 FK (참조 테이블이 아직 없음): {summary['relationship_rules_skipped']}")

    if SAVE:
        tables = validator.save(passed, failed, summary)
        for label, tbl in tables.items():
            print(f"✅ {label}: {tbl}")
    else:
        print("SAVE=False: 저장하지 않았습니다.")

# COMMAND ----------

# MAGIC %md ## 3. 전체 결과 요약

# COMMAND ----------

print("Validation 전체 완료:", list(summaries.keys()))
for target_table, summary in summaries.items():
    skipped = len(summary["relationship_rules_skipped"])
    print(f"  {target_table:10s} 적재={summary['loaded_count']:>6}  격리={summary['quarantined_count']:>6}  "
          f"미검증 FK={skipped}")