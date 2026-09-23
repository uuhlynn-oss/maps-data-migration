# =============================================================================
# 매핑 메타데이터 적재 셀 (Databricks 노트북에 셀 하나로 붙여넣어 실행)
#
# CSV 3개를 읽어 엔진이 읽는 메타 테이블 3개를 (교체) 적재합니다.
#   TO-BE 물리 모델      -> meta.target_model             (컬럼 순서를 ORDINAL로 보존)
#   AS-IS -> TO-BE 코드  -> meta.code_mapping_asis_tobe
#   컬럼 단위 매핑 정의  -> meta.mapping_definition
# 아래 경로만 실제 위치(Volume 또는 Workspace 파일)로 바꾸세요. CSV는 UTF-8(BOM 가능)이어야 합니다.
# =============================================================================
import pandas as pd
from pyspark.sql.types import StringType, StructField, StructType

try:
    import src.mapping.mapping_config as cfg
except ModuleNotFoundError:
    import mapping_config as cfg

CSV_DIR = "/Volumes/maps_databricks/meta/files"      # <- 실제 경로로 변경
FILES = {
    "target_model":            ("target_model.csv",          cfg.TARGET_MODEL_TABLE),
    "code_mapping_asis_tobe":  ("code_mapping.csv",           cfg.CODE_MAPPING_TABLE),
    # 매핑 정의는 전 소스가 한 파일이다 (AI가 매핑을 생성할 때도 소스시스템 전체를 한 파일로 낸다).
    # ⚠ 지금은 매번 전체 교체(overwrite)한다. 사람이 검토(REVIEW_STATUS)한 행을 AI 재생성이 덮어쓰면 안 되는 시점이 오면
    #   dq_rule_repository.py처럼 버전 관리(diff 후 바뀐 행만 새 버전 추가, 사람 검토 필드는 보존)로 바꿔야 한다.
    "mapping_definition":      ("mapping_definition.csv",                       cfg.MAPPING_DEFINITION_TABLE),
}

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.UC_CATALOG}.{cfg.META_SCHEMA}")
for key, (fname, table) in FILES.items():
    pdf = pd.read_csv(f"{CSV_DIR}/{fname}", encoding="utf-8-sig", dtype=str, keep_default_na=False)
    pdf = pdf.replace({"": None})                       # 빈 칸은 NULL
    if key == "target_model":
        pdf.insert(0, "ORDINAL", [str(i + 1) for i in range(len(pdf))])   # 파일의 컬럼 순서를 보존
    if key == "mapping_definition":
        dup = pdf[pdf.MAPPING_ID.duplicated(keep=False)]
        if not dup.empty:
            raise ValueError(f"MAPPING_ID 중복 (적재하지 않음):\n{dup['MAPPING_ID'].to_string(index=False)}")
    # 전부 빈 컬럼이 있어도 타입을 추론하지 못해 실패하지 않도록 모든 컬럼을 STRING으로 명시한다
    schema = StructType([StructField(c, StringType(), True) for c in pdf.columns])
    records = [tuple(None if pd.isna(v) else v for v in row) for row in pdf.itertuples(index=False, name=None)]
    sdf = spark.createDataFrame(records, schema=schema)
    sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    print(f"✅ {table}: {sdf.count()}행")