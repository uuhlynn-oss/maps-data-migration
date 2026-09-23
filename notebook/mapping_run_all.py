# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Gold 파이프라인 실행: Mapping(전 소스) + Target Validation
# MAGIC
# MAGIC `dq_run`이 DQ 검사와 정제를 한 노트북에서 끝내는 것과 같은 방식입니다. 이 노트북은
# MAGIC **소스 목록 확정 → 소스별 Mapping(물리 저장) → Target Validation → Gold/격리/MIGRATION_TRACE 저장**까지 한 번에 합니다.
# MAGIC
# MAGIC 소스를 하드코딩하지 않습니다. "이 Target으로 승인된(FINAL_MIGRATION_APPLY_YN=Y, REVIEW_STATUS 승인) 매핑이 있는 소스"를
# MAGIC 매번 `meta.mapping_definition`에서 다시 찾으므로, 정의에 소스를 추가·제외하면 이 목록도 그대로 따라갑니다.
# MAGIC
# MAGIC 후보(`gold_candidate.<target>`)는 **물리 테이블**입니다(소스·배치 단위로만 교체). 노트북이 분리돼 있어도, 다른 세션에서
# MAGIC 저장해 둔 소스가 있으면 이 테이블에 그대로 남아 있습니다. 소스 하나만 깊게 디버깅할 때는 `mapping_run.py`를 따로 쓰세요.

# COMMAND ----------

TARGET_TABLE = "COUNSEL"
SAVE_GOLD = True       # False면 Validation 결과만 확인하고 Gold/격리/MIGRATION_TRACE에는 저장하지 않음

# COMMAND ----------

import sys
from pyspark.sql import functions as F

if "PROJECT_ROOT" in dir() and PROJECT_ROOT and PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
try:
    import src.mapping.mapping_config as mcfg
    import src.mapping.mapping_engine as engine_mod
    import src.gold.gold_validation_config as gcfg
    import src.gold.gold_validation_runner as gv
except ModuleNotFoundError:
    import mapping_config as mcfg
    import mapping_engine as engine_mod
    import gold_validation_config as gcfg
    import gold_validation_runner as gv


def _show(df, n=10):
    try:
        display(df.limit(n))
    except NameError:
        df.show(n, False)


# COMMAND ----------

# MAGIC %md ## 1. 소스 목록 확정

# COMMAND ----------

engine = engine_mod.MappingEngine.from_tables(spark)
sources = engine.discover_sources(TARGET_TABLE)
print(f"{TARGET_TABLE}로 승인된 매핑이 있는 소스: {sources}")
if not sources:
    raise RuntimeError(f"{TARGET_TABLE}로 가는 승인된 매핑 정의가 없습니다. meta.mapping_definition을 확인하세요.")

# COMMAND ----------

# MAGIC %md ## 2. 소스별 Mapping 실행 (물리 저장)
# MAGIC Silver에 아직 데이터가 없는 소스는 건너뜁니다. 각 소스는 `gold_candidate.<target>`에 자기 (소스, 배치) 부분만 교체해서 저장하므로,
# MAGIC 순서와 무관하게 서로의 결과를 지우지 않습니다.

# COMMAND ----------

mapping_results = []
for source in sources:
    silver_table = mcfg.silver_input_table(mcfg.SOURCE_SYSTEMS[source]["silver"])
    if not spark.catalog.tableExists(silver_table):
        print(f"⏭️  {source}: {silver_table} 없음 (건너뜀 - DQ가 아직 이 소스를 처리하지 않은 것으로 보임)")
        mapping_results.append({"source": source, "status": "SKIPPED (Silver 없음)"})
        continue
    try:
        candidate, summary = engine.run(source, TARGET_TABLE)
    except NotImplementedError as e:
        print(f"⏭️  {source}: {e}")
        mapping_results.append({"source": source, "status": "SKIPPED (미지원 Target)"})
        continue
    table = engine.save(candidate, summary)
    print(f"✅ {source}: {table}  (입력 {summary['input_count']} → 출력 {summary['output_count']}, "
         f"변환실패 {summary['conversion_error_rows']}, 미매핑 {summary['unmapped_code_rows']})")
    mapping_results.append({
        "source": source, "status": "OK", "table": table, "batch": summary["source_batch_id"],
        "input": summary["input_count"], "output": summary["output_count"],
        "conversion_errors": summary["conversion_error_rows"], "unmapped_codes": summary["unmapped_code_rows"],
        "deferred_columns": [c for c, _ in summary["deferred_columns"]],
    })

_show(spark.createDataFrame(mapping_results, samplingRatio=1.0), 10)

ok_sources = [r["source"] for r in mapping_results if r["status"] == "OK"]
skipped_sources = [r["source"] for r in mapping_results if r["status"] != "OK"]
if not ok_sources:
    raise RuntimeError("실행된 소스가 하나도 없습니다 (전부 건너뜀). Silver 데이터 상태를 확인하세요.")
if skipped_sources:
    print(f"\n⚠️  이번에 건너뛴 소스: {skipped_sources}. gold_candidate.{TARGET_TABLE.lower()}에는 이전에 저장된 값이 남아있을 수 있습니다"
         f"(그 소스를 한 번도 저장한 적이 없다면 애초에 없습니다).")

candidate_table = mcfg.gold_candidate_table(TARGET_TABLE)
merged = spark.read.table(candidate_table)
print(f"\ngold_candidate.{TARGET_TABLE.lower()} 현재 상태: {merged.count()}행")
merged.groupBy("_source_system").count().show()

# COMMAND ----------

# MAGIC %md ## 3. Target Validation 실행
# MAGIC 방금 저장한 물리 테이블(`gold_candidate.<target>`)을 읽어 검증합니다. TO-BE 모델에서 규칙을 다시 만들고,
# MAGIC 승인된 보정(아웃바운드 CTI_ID 중복 → CNSL_ID 재발급, Review R3)을 적용한 뒤 재검증합니다.

# COMMAND ----------

validator = gv.TargetValidator.from_tables(spark)
passed, failed, summary = validator.run(TARGET_TABLE)

print("validation_run_id:", summary["validation_run_id"])
match = summary["input_count"] == summary["loaded_count"] + summary["quarantined_count"]
print(f"입력 {summary['input_count']} = 적재 {summary['loaded_count']} + 격리 {summary['quarantined_count']}  "
     f"{'✅ 대사 일치' if match else '❌ 불일치'}")
print(f"CTI_ID 중복 보정: {summary['pk_dedup_fixed']}건 (Review R3, 아웃바운드만 대상)")

if summary["violations_by_rule"]:
    print("\n위반 규칙 분포")
    for rule_id, cnt in sorted(summary["violations_by_rule"].items(), key=lambda kv: -kv[1]):
        print(f"  {rule_id:<28}{cnt}건")
else:
    print("\n위반 없음 (격리된 레코드가 없습니다)")

_show(passed, 5)

# COMMAND ----------

# MAGIC %md ## 4. 저장 (Gold / gold_quarantine / MIGRATION_TRACE)

# COMMAND ----------

if SAVE_GOLD:
    tables = validator.save(passed, failed, summary)
    for label, tbl in tables.items():
        print(f"✅ {label}: {tbl}")
    gold_n = spark.read.table(tables["gold_table"])
    print(f"\n{tables['gold_table']}: 이번 실행 적재분 {summary['loaded_count']}행 (테이블 전체 {gold_n.count()}행, 재실행 시 누적 - 알려진 제약)")
else:
    print("SAVE_GOLD=False: 저장하지 않았습니다.")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from maps_databricks.silver_candidate.inbound limit 3