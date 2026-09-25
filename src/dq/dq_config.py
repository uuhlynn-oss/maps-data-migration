from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, TimestampType,
    BooleanType, IntegerType,
)

# -------------------------------------------------------------------------
# Catalog & Schema Settings
# -------------------------------------------------------------------------
UC_CATALOG = "maps_databricks"
BRONZE_SCHEMA = "bronze"
META_SCHEMA = "meta"  # 메타(코드마스터, DQ결과 등) 관리용 스키마

# Meta Table Settings
CODE_MASTER_TABLE = f"{UC_CATALOG}.{META_SCHEMA}.code_master"
DQ_RESULT_TABLE = f"{UC_CATALOG}.{META_SCHEMA}.dq_result"
DQ_CLEANSING_DETAIL_TABLE = f"{UC_CATALOG}.{META_SCHEMA}.dq_cleansing_detail"  # 요청서 4장 - 행 단위 상세
DQ_RULE_DEF_TABLE = f"{UC_CATALOG}.{META_SCHEMA}.dq_rule_def"   # DQ 규칙 정의 (정본, 규칙 단위 버전 관리 - 추가 전용)
# DQ 기준서 8.7 - 승인된 Source -> Standard 코드 매핑표 (CLN-VAL-003 전용)
# 필수 컬럼: CODE_GROUP, SOURCE_CODE, STANDARD_CODE, APPROVED_YN('Y'만 사용)
CODE_MAPPING_TABLE = f"{UC_CATALOG}.{META_SCHEMA}.code_mapping"

# Silver Candidate는 소스 테이블마다 하나씩 생긴다 (요청서 5.1)
# 예: maps_databricks.silver_candidate.salesforce
SILVER_CANDIDATE_SCHEMA_NAME = "silver_candidate"


def silver_candidate_table(source_system: str) -> str:
    """maps_databricks.silver_candidate.<source_system> 전체 경로를 만들어준다."""
    return f"{UC_CATALOG}.{SILVER_CANDIDATE_SCHEMA_NAME}.{source_system}"


# DQ 기준서 §4/§12의 "오류 격리": 정제 후에도 BLOCK/REVIEW Rule 위반이 남은 레코드(UNRESOLVED)는
# silver_candidate가 아니라 소스별 격리 테이블로 보낸다. (DQ 단계에는 HITL이 없다 - AI+HITL은 Gold -> Target 단계)
# 예: maps_databricks.dq_quarantine.inbound
QUARANTINE_SCHEMA_NAME = "dq_quarantine"


def quarantine_table(source_system: str) -> str:
    """maps_databricks.dq_quarantine.<source_system> 전체 경로를 만들어준다."""
    return f"{UC_CATALOG}.{QUARANTINE_SCHEMA_NAME}.{source_system}"


# DQ 규칙의 정본은 meta.dq_rule_def 테이블이다 (규칙마다 rule_version이 있고, 내용이 바뀌면 자동으로 올라간다).

# 규칙 정의 검증용 허용값 (dq_rule_repository가 CSV/테이블 적재 전에 검사한다)
RULE_TYPES = ("NULL_CHECK", "PATTERN_CHECK", "RANGE_CHECK", "ORDER_CHECK", "CODE_EXISTS", "DUPLICATE_CHECK")
ERROR_GRADES = ("CRITICAL", "HIGH", "MEDIUM", "LOW", "REVIEW")

# dq_cleansing_detail/silver_candidate에서 "이 레코드가 무엇인지"를 가리키는 컬럼.
# 업무키가 애매한 outbound(CTI_ID로 사용 - LEAD_MGMT_NO는 DUPLICATE_CHECK 대상이라 다름)를
# 제외하면 대부분 COMPLETENESS/UNIQUENESS Rule이 이미 보고 있는 컬럼과 같다.
TABLE_RECORD_KEY_COLUMN = {
    f"{UC_CATALOG}.{BRONZE_SCHEMA}.inbound": "consultation_id",
    f"{UC_CATALOG}.{BRONZE_SCHEMA}.outbound": "CTI_ID",
    f"{UC_CATALOG}.{BRONZE_SCHEMA}.salesforce": "Consultation_No__c",
    f"{UC_CATALOG}.{BRONZE_SCHEMA}.homepage": "COMPLAINT_ID",
    f"{UC_CATALOG}.{BRONZE_SCHEMA}.chatbot": "session_id",
}

# -------------------------------------------------------------------------
# DQ Result Schema
# -------------------------------------------------------------------------
DQ_RESULT_SCHEMA = StructType([
    StructField("rule_id", StringType(), False),
    StructField("rule_name", StringType(), True),
    StructField("target_table", StringType(), False),
    StructField("target_column", StringType(), True),
    StructField("dimension", StringType(), True),
    StructField("check_count", LongType(), True),
    StructField("error_count", LongType(), True),        # 최종(자동 Cleansing+재-DQ 후) 잔여 오류 건수
    StructField("error_rate", DoubleType(), True),        # 최종 오류율 - action_type 판정 기준
    StructField("initial_error_count", LongType(), True),  # 자동 Cleansing 전 원본 오류 건수
    StructField("initial_error_rate", DoubleType(), True),  # 자동 Cleansing 전 원본 오류율
    StructField("threshold_rate", DoubleType(), True),
    StructField("result_status", StringType(), True),  # PASS / FAIL
    StructField("error_grade", StringType(), True),    # CRITICAL / HIGH / MEDIUM / LOW / REVIEW / INFO
    StructField("action_type", StringType(), True),    # BLOCK / WARN / REVIEW / ALLOW
    StructField("sample_values_json", StringType(), True),
    StructField("dq_reason", StringType(), True),
    StructField("executed_at", TimestampType(), True),
    StructField("ingest_date", StringType(), False),
    # ---- 종옥님 요청서(DQ/Cleansing 결과 구조 수정) 반영분 ----
    StructField("dq_run_id", StringType(), False),           # 한 번의 DQ 실행(run_table_dq 호출들의 묶음)을 식별하는 ID
    StructField("source_system", StringType(), True),        # target_table에서 유추한 원천 시스템명 (예: inbound, salesforce)
    StructField("source_batch_id", StringType(), True),      # 검사 대상 Bronze 배치 ID - 현재는 하루 1배치 전제라 ingest_date를 그대로 사용
    StructField("dq_rule_version", IntegerType(), True),      # 이번 실행이 어느 Rule 버전(meta.dq_rule_def)으로 돌았는지
    StructField("unique_error_record_count", LongType(), True),  # 이 Rule에서 오류난 고유 레코드 수 (dq_cleansing_detail의 source_record_key distinct count)
    StructField("cleansing_required_yn", BooleanType(), True),  # action_type 기준 Cleansing 필요 여부 (CLEANSING_REQUIRED_MAP 참고)
    # ---- DQ 기준서 8장(자동 Cleansing) 반영분 ----
    StructField("auto_cleansed_record_count", LongType(), True),  # 자동 Cleansing 후 재-DQ PASS로 정상화된 건수 (detail 행 기준, ALLOW Rule은 NULL)
    StructField("unresolved_record_count", LongType(), True),     # 정제 후에도 위반이 남아 격리 대상(UNRESOLVED)이 된 건수 = review_required_yn=true (ALLOW Rule은 NULL)
])

# -------------------------------------------------------------------------
# dq_cleansing_detail 스키마 (요청서 4.2) - Rule별 집계가 아니라 오류난 레코드 하나하나의 상세.
# FAIL로 판정된 Rule에서만 생성한다 (PASS/ALLOW는 Review 대상이 아니므로 여기 안 들어온다 - 요청서 6장).
# -------------------------------------------------------------------------
DQ_CLEANSING_DETAIL_SCHEMA = StructType([
    StructField("dq_detail_id", StringType(), False),      # 상세 오류 건 고유 ID (uuid)
    StructField("dq_run_id", StringType(), False),
    StructField("rule_id", StringType(), False),
    StructField("source_system", StringType(), True),
    StructField("source_table", StringType(), True),
    StructField("source_batch_id", StringType(), True),
    StructField("source_record_key", StringType(), True),  # TABLE_RECORD_KEY_COLUMN 기준 레코드 식별값
    StructField("target_column", StringType(), True),      # 오류 발생 컬럼 (복합 Rule이면 컬럼 조합)
    StructField("before_value", StringType(), True),       # Bronze 원본 값 (마스킹 적용됨 - 4.4)
    StructField("proposed_value", StringType(), True),     # 자동 정제 로직이 정해지면 채워질 추천값 - 지금은 전부 NULL
    StructField("final_value", StringType(), True),        # 사용자 승인/수정 후 최종값 - 지금은 전부 NULL
    StructField("cleansing_action", StringType(), True),   # 적용/제안한 정제 유형 - 자동정제 로직 미정이라 지금은 NULL
    StructField("cleansing_status", StringType(), True),   # AUTO_CLEANSED / UNRESOLVED / ALLOWED / ... (CLEANSING_STATUS_VALUES)
    StructField("dq_reason", StringType(), True),
    StructField("review_required_yn", BooleanType(), True),  # 이름은 요청서 그대로 유지 - DQ 단계에서는 "격리 대상(UNRESOLVED) 여부"를 뜻한다
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True),
    # ---- DQ 기준서 8.11(Cleansing 이력 관리) 반영분 ----
    # BEFORE_VALUE=before_value / AFTER_VALUE=proposed_value(=final_value, 재-DQ PASS 시) / EXECUTED_AT=updated_at
    StructField("cleansing_rule_id", StringType(), True),  # 실제 값이 바뀐 CLN Rule ID (콤마 구분, 예: "CLN-COM-001,CLN-VAL-001")
    StructField("re_dq_result", StringType(), True),       # PASS / FAIL - 자동 Cleansing Rule이 없는 건은 NULL
])

# 요청서 4.3 표준값 + DQ 단계 처리 결과. dq_cleansing_detail은 위반 이력 "전체"라서 (임계치와 무관) 아래 넷이 모두 들어온다.
#   - AUTO_CLEANSED : 자동 Cleansing 후 재-DQ PASS                        -> Silver 후보
#   - UNRESOLVED    : 정제 후에도 BLOCK/REVIEW Rule 위반이 남음            -> 격리 테이블 (ISOLATE_ACTIONS 참고)
#   - ALLOWED       : WARN(기준서 WARNING) Rule 위반이 정제되지 않은 채 남음 -> 허용, 로그만 남기고 통과
#   - NOT_REQUIRED  : 허용 오류율 이내(ALLOW) Rule의 위반 - 정제 없이 원본 유지, 기록만
# (재-DQ까지 갔다가 실패한 건은 re_dq_result='FAIL'로 구분한다)
# MANUAL_REVIEW_REQUIRED/PENDING/FAILED/REPROCESSED/NOT_REQUIRED는 값 정의만 유지한다 - DQ 단계에는 HITL이 없다.
CLEANSING_STATUS_VALUES = [
    "NOT_REQUIRED", "PENDING", "AUTO_CLEANSED",
    "MANUAL_REVIEW_REQUIRED", "FAILED", "REPROCESSED",
    "UNRESOLVED", "ALLOWED",
]

# -------------------------------------------------------------------------
# 처리 기준 (DQ 기준서 §4) - Rule이 임계치를 넘어 FAIL일 때, "정제 후에도 남은 위반 레코드"의 처리
#   BLOCK  (BLOCKER/ERROR) : 승인된 자동보정이 있으면 보정 후 재검사, 아니면 격리
#   REVIEW                 : 자동보정 금지 -> 격리
#   WARN   (WARNING)       : 허용 - 로그만 남기고 통과
#   ALLOW  (허용 오류율 이내) : 통과 (위반은 detail에 NOT_REQUIRED로 기록만 - 정제·격리 대상 아님)
# 레코드 상태: UNRESOLVED(격리) / CLEANSED(정제 후 통과) / CLEAN(격리·정제 대상 위반 없음, 허용된 위반은 포함될 수 있음)
# -------------------------------------------------------------------------
ISOLATE_ACTIONS = ("BLOCK", "REVIEW")

# -------------------------------------------------------------------------
# cleansing_required_yn 판정 기준 (요청서 3.3, action_type은 DQ 기준서/현재 judge_result 그대로 사용)
# -------------------------------------------------------------------------
CLEANSING_REQUIRED_MAP = {
    "ALLOW": False,   # 원본 사용 가능
    "WARN": True,     # 정제 또는 확인 필요
    "REVIEW": True,   # 사용자 판단 필요
    "BLOCK": True,    # 해결 전 Silver 확정 불가
}

# -------------------------------------------------------------------------
# 자동 Cleansing Rule 정의 (DQ 기준서 8장)
#
# 선정 기준(8.2) - 아래 4가지를 "모두" 만족하는 오류만 자동 Cleansing 대상이 된다.
#   1) 변환 규칙의 명확성   2) 변환 결과의 결정 가능성
#   3) 업무 의미의 불변성   4) Cleansing 후 DQ 재검증 가능성
# 흐름(8.10): DQ ERROR -> Cleansing Rule 확인 -> (없으면 UNRESOLVED)
#             -> 자동 Cleansing -> 재-DQ -> PASS: Silver 후보 / FAIL: UNRESOLVED
#   ※ UNRESOLVED의 처리는 위 ISOLATE_ACTIONS 기준: BLOCK/REVIEW Rule이면 격리, WARN Rule이면 허용(ALLOWED)
# -------------------------------------------------------------------------

# 8.3 자동 Cleansing Rule 목록
CLEANSING_RULES = {
    "CLN-COM-001": {"name": "공백 정규화",       "dimension": "Completeness", "action": "WHITESPACE_NORMALIZE", "auto_yn": True, "re_dq_yn": True},
    "CLN-VAL-001": {"name": "전화번호 표준화",   "dimension": "Validity",     "action": "PHONE_FORMAT",         "auto_yn": True, "re_dq_yn": True},
    "CLN-VAL-002": {"name": "날짜/시간 표준화",  "dimension": "Validity",     "action": "DATETIME_FORMAT",      "auto_yn": True, "re_dq_yn": True},
    # enabled=False: 이 단계에서는 코드 매핑을 하지 않는다 (보류). True로 바꾸면 code_mapping 테이블의 승인 매핑으로 정제한다.
    #   보류 중에는 이 단계가 통째로 건너뛰어져 code_mapping 테이블을 읽지도 않는다 (테이블이 없어도 됨).
    "CLN-VAL-003": {"name": "승인된 코드값 표준화", "dimension": "Validity",   "action": "CODE_MAPPING",         "auto_yn": True, "re_dq_yn": True, "enabled": False},
}

# 8.9 자동 Cleansing 제외 대상 - 아래 rule_type은 cleansing_steps를 등록할 수 없고 항상 UNRESOLVED 처리된다.
# (재-DQ가 가능한 유형은 PATTERN_CHECK / CODE_EXISTS 뿐이라 AUTO_CLEANSING_RULE_TYPES로 제한한다)
AUTO_CLEANSING_RULE_TYPES = ("PATTERN_CHECK", "CODE_EXISTS")
CLEANSING_EXCLUDED_REASONS = {
    "NULL_CHECK":      "필수값 NULL - 대체값을 임의 생성할 수 없음",
    "DUPLICATE_CHECK": "PK/BK 중복 - 어떤 레코드를 유지할지 업무 판단 필요 (임의 삭제 금지)",
    "ORDER_CHECK":     "시작일 > 종료일 등 - 어느 값이 잘못되었는지 판단 필요",
    "RANGE_CHECK":     "범위 이탈 - 올바른 값을 추정할 수 없음",
}

# CLN-VAL-001 전화번호 표준 형식 - DQ 기준서 기준 "하이픈 없는 숫자만"(01045683510)이 표준이다.
# DQ Rule의 pattern과 Cleansing 결과 형식이 어긋나면 재-DQ가 항상 FAIL 나므로, 둘 다 여기 한 곳에서 가져간다.
#   "DIGITS": 01045683510    (기본값 / 표준)
#   "HYPHEN": 010-4568-3510  (필요 시 전환용 - 바꾸면 PHONE_PATTERN을 쓰는 DQ Rule도 함께 바뀐다)
PHONE_STANDARD_FORMAT = "DIGITS"
_PHONE_PATTERNS = {
    "HYPHEN": r"^01[0-9]-[0-9]{3,4}-[0-9]{4}$",
    "DIGITS": r"^01[0-9][0-9]{7,8}$",
}
PHONE_PATTERN = _PHONE_PATTERNS[PHONE_STANDARD_FORMAT]
# 제거를 허용하는 구분자(공백/점/하이픈). 이 밖의 문자(영문, +82, 괄호 등)가 섞이면 변환하지 않는다 (8.5-4).
PHONE_SEPARATOR_REGEX = r"[\s.\-]"

# CLN-VAL-002 날짜/시간 - 해석이 하나로 결정되는 형식만 허용한다.
# dd/MM/yyyy vs MM/dd/yyyy 처럼 순서가 모호한 형식, 날짜만 있는 값을 TIMESTAMP로 바꾸는 것(시각 추정)은 제외 (8.2.2 / 8.2.3).
DATE_INPUT_FORMATS = ["yyyy-MM-dd", "yyyy/MM/dd", "yyyy.MM.dd", "yyyyMMdd"]
TIMESTAMP_INPUT_FORMATS = [
    "yyyy-MM-dd HH:mm:ss", "yyyy/MM/dd HH:mm:ss", "yyyy.MM.dd HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd HH:mm", "yyyy/MM/dd HH:mm", "yyyyMMddHHmmss",
]
DATE_STANDARD_FORMAT = "yyyy-MM-dd"
TIMESTAMP_STANDARD_FORMAT = "yyyy-MM-dd HH:mm:ss"


def cleansing_config_for(rule: dict):
    """Rule(meta.dq_rule_def에서 읽은 행)에 붙은 자동 Cleansing 설정을 돌려준다 (없으면 None)."""
    steps = list(rule.get("cleansing_steps") or [])
    if not steps:
        return None
    cfg = {"steps": steps}
    if rule.get("datetime_kind"):
        cfg["datetime_kind"] = rule["datetime_kind"]
    return cfg