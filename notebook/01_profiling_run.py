# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
import sys, os

# maps/notebook → maps (project root containing src/)
for _p in sys.path:
    _parent = os.path.dirname(_p)
    if os.path.isdir(os.path.join(_parent, "src")) and _parent not in sys.path:
        sys.path.insert(0, _parent)
        break

from src.profiling.profiling_runner import run_all_sources
from pyspark.sql import functions as F

# # Spark 세션 시간대 설정 안함 - 기본 UTC 기준으로 진행
# spark.conf.set("spark.sql.session.timeZone", "Asia/Seoul")

# print(
#     "Spark Session Timezone:",
#     spark.conf.get("spark.sql.session.timeZone")
# )

# COMMAND ----------

all_results = run_all_sources(spark, dbutils)

print("\n" + "=" * 60)
print("Profiling 실행 완료")
print("=" * 60)

for result in all_results:
    print(
        f"- {result['table_name']:35s} "
        f"rows={result['row_count']:,} "
        f"ingest_date={result.get('ingest_date', 'N/A')}"
    )