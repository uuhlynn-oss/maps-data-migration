import os

# Project
PROJECT_NAME = "maps"

# Unity Catalog Defaults
UC_CATALOG = "maps_databricks"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
META_SCHEMA = "meta"


# ADLS Gen2
STORAGE_ACCOUNT = "stmapspoc01"
CONTAINER = "data"

# ADLS Base Path
ADLS_BASE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# Medallion Layers (Delta Table이 저장될 외부 스토리지 경로 필요 시)
BRONZE_PATH = f"{ADLS_BASE_PATH}/bronze_test"
SILVER_PATH = f"{ADLS_BASE_PATH}/silver"
GOLD_PATH = f"{ADLS_BASE_PATH}/gold"
REJECT_PATH = f"{ADLS_BASE_PATH}/reject"

# OUTPUT Base path - 프로파일링 및 메타 데이터 저장 경로
PROFILING_OUTPUT_BASE_PATH = f"{ADLS_BASE_PATH}/profiling"
DQ_OUTPUT_BASE_PATH = f"{ADLS_BASE_PATH}/dq"