# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Mapping Execution: target_model 기반 자동 실행
# MAGIC
# MAGIC 이전 버전은 `TARGET_TABLE`을 노트북 파라미터로 하나씩 지정하고, `CUSTOMER`만 `IS_CUSTOMER` 분기로
# MAGIC 특수 처리했다. 이제 `meta.target_model`에 정의된 Target(`SUPPORTED_TARGET_TABLES`와의 교집합)을
# MAGIC `mapping_orchestrator.discover_execution_targets()`로 자동 조회해 순회하며,
# MAGIC 각 Target을 `mapping_orchestrator.run_target()` 하나로 실행한다 - Target별 하드코딩 분기가 없다.
# MAGIC
# MAGIC Target 유형(DIRECT/ENTITY/MASTER) 판정, 소스 자동 조회, `integrate()`/`load_master_data()` 호출
# MAGIC 시점은 모두 `run_target()` 내부(이전 단계에서 검증 완료)가 담당한다. 이 노트북은 "무엇을 순회할지"와
# MAGIC "결과를 어떻게 보여줄지"만 담당한다.

# COMMAND ----------

# MAGIC %md ## 0. Job Run ID 수신
# MAGIC Job 설정의 base_parameters(`job_run_id`: `{{job.run_id}}`)로 전달된 값을 읽는다. 대화형으로
# MAGIC 직접 실행하면(Job 없이) 빈 값이 들어오고, 아래에서 None으로 처리해 그대로 진행한다.

# COMMAND ----------

dbutils.widgets.text("job_run_id", "")
job_run_id = dbutils.widgets.get("job_run_id") or None

# COMMAND ----------

import os
import sys

for _p in sys.path:
    _parent = os.path.dirname(_p)
    if os.path.isdir(os.path.join(_parent, "src")) and _parent not in sys.path:
        sys.path.insert(0, _parent)
        break

try:
    import src.mapping.mapping_config as cfg
    import src.mapping.mapping_engine as engine_mod
    import src.mapping.mapping_orchestrator as orch
except ModuleNotFoundError:
    import mapping_config as cfg
    import mapping_engine as engine_mod
    import mapping_orchestrator as orch

engine = engine_mod.MappingEngine.from_tables(spark)

# COMMAND ----------

# MAGIC %md ## 0. 사전조건 검증: 전체 Target Model의 ACTIVE approved_rule 준비 여부
# MAGIC SUPPORTED_TARGET_TABLES 중 하나라도 준비되지 않았으면 여기서 멈춘다 - 일부 Target만
# MAGIC 실행하거나 건너뛰지 않는다.

# COMMAND ----------

orch.assert_all_targets_ready(spark)
print("✅ 전체 Target Model의 ACTIVE approved_rule이 준비되었습니다. Mapping Execution을 시작합니다.")

# COMMAND ----------

# MAGIC %md ## 1. 실행 대상 자동 조회

# COMMAND ----------

targets = orch.discover_execution_targets(spark)
print(f"Mapping Execution 대상 (target_model ∩ SUPPORTED_TARGET_TABLES): {targets}")
if not targets:
    raise RuntimeError(
        "실행 대상이 없습니다. target_model 정리(target_model_cleanup.py) 이후 SUPPORTED_TARGET_TABLES와 "
        "겹치는 TABLE_NAME이 하나도 없는지 확인하세요."
    )

# COMMAND ----------

# MAGIC %md ## 2. Target별 순차 실행

# COMMAND ----------

results = {}
for target_table in targets:
    print(f"\n{'=' * 60}\n[{target_table}] Mapping Execution 시작\n{'=' * 60}")
    result = orch.run_target(engine, target_table, job_run_id=job_run_id)
    results[target_table] = result

    print(f"integration_type = {result['integration_type']}")
    print(f"sources          = {result['sources']}")
    if "integration_result" in result:
        print(f"integrate 결과   = {result['integration_result']}")
    if "master_load_result" in result:
        print(f"master 적재 결과 = {result['master_load_result']}")
    if "note" in result:
        print(f"비고             = {result['note']}")

# COMMAND ----------

# MAGIC %md ## 3. 전체 결과 요약

# COMMAND ----------

print("Mapping Execution 전체 완료:", list(results.keys()))
for target_table, result in results.items():
    n_rows = None
    table = cfg.gold_candidate_table(target_table)
    if spark.catalog.tableExists(table):
        n_rows = spark.table(table).count()
    print(f"  {target_table:10s} integration_type={result['integration_type']:8s} "
          f"gold_candidate 행수={n_rows}")