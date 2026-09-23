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
# 이 값은 코드의 DQ_RULES로 실행하는 경우(DQRunner rule_source="code": 테스트·비교용)의 기본 버전일 뿐이다.
DQ_RULES_VERSION = 1

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
    StructField("error_count", LongType(), True),
    StructField("error_rate", DoubleType(), True),
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
    StructField("dq_rule_version", IntegerType(), True),      # DQ_RULES_VERSION - 이번 실행이 어느 Rule 버전으로 돌았는지
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

# 8.9 자동 Cleansing 제외 대상 - 아래 rule_type은 CLEANSING_RULE_MAPPING에 등록할 수 없고 항상 UNRESOLVED 처리된다.
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

# -------------------------------------------------------------------------
# DQ Rules Definition (실제 테이블 스키마 기반 반영)
#
# 이 리스트는 "초기 적재용 원본(seed)"이자 rule_source="code" 실행용이다. 운영 정본은 meta.dq_rule_def 테이블이며,
# 규칙을 바꿀 때는 Volume의 CSV를 고쳐 dq_rule_loader로 적재한다 (코드 배포 불필요, 버전 이력이 남는다).
# 이 파일의 DQ_RULES를 고쳐도 테이블은 바뀌지 않는다.
# -------------------------------------------------------------------------
DQ_RULES = [
    # =====================================================================
    # 1. INBOUND CALLS -> target_table: inbound
    # =====================================================================
    {
        "rule_id": "DQ-COM-IN-001",
        "rule_name": "Inbound 콜 ID 누락 검사",
        "target_table": f"{UC_CATALOG}.{BRONZE_SCHEMA}.inbound",
        "column": "consultation_id",
        "rule_type": "NULL_CHECK",
        "dimension": "결손성",
        "threshold_rate": 0.0,
        "error_grade": "CRITICAL"
    },
    {
        "rule_id": "DQ-COM-IN-002",
        "rule_name": "Inbound 고객 ID 누락 검사",
        "target_table": f"{UC_CATALOG}.{BRONZE_SCHEMA}.inbound",
        "column": "customer_id",
        "rule_type": "NULL_CHECK",
        "dimension": "결손성",
        "threshold_rate": 0.01,
        "error_grade": "HIGH"
    },
    {
        "rule_id": "DQ-VAL-IN-001",
        "rule_name": "Inbound 전화번호 정규식 검사",
        "target_table": f"{UC_CATALOG}.{BRONZE_SCHEMA}.inbound",
        "column": "phone_number",
        "rule_type": "PATTERN_CHECK",
        "pattern": PHONE_PATTERN,
        "dimension": "유효성",
        "threshold_rate": 0.005,
        "error_grade": "MEDIUM"
    },
    {
        "rule_id": "DQ-VAL-IN-002",
        "rule_name": "Inbound 상태 코드 유효성 검사",
        "target_table": f"{UC_CATALOG}.{BRONZE_SCHEMA}.inbound",
        "column": "status",
        "code_group": "CONSULTATION_STATUS",  # [수정] 마스터에 등록된 표준 그룹명으로 맞춤
        "rule_type": "CODE_EXISTS",
        "dimension": "유효성",
        "threshold_rate": 0.0,
        "error_grade": "HIGH"
    },
    {
        "rule_id": "DQ-VAL-IN-003",
        "rule_name": "Inbound 콜유형 코드 마스터 검증",
        "target_table": f"{UC_CATALOG}.{BRONZE_SCHEMA}.inbound",
        "column": "inbound_type",
        "code_group": "INBOUND_TYPE",
        "rule_type": "CODE_EXISTS",
        "dimension": "유효성",
        # Gold 이행 제외 컬럼(매핑 MAP-INB-CONSULTATION-002, PoC 범위 제외)이고 DQ 기준서 §7.1에도 이 컬럼 규칙이 없다.
        # 이 값 때문에 레코드가 격리되어 Gold에서 빠지면 안 되므로 기준서 §3.1 OPTIONAL(선택/참고 컬럼: 5%, WARNING)에 맞춰
        # 격리(BLOCK)하지 않고 기록만 남긴다. (위반은 dq_cleansing_detail에 ALLOWED/NOT_REQUIRED로 남아 Silver 분석에 쓸 수 있다)
        "threshold_rate": 0.05,
        "error_grade": "LOW"
    },
    {
        "rule_id": "DQ-CON-IN-001",
        "rule_name": "Inbound 통화시작/종료 시각 순서 검사",
        "target_table": f"{UC_CATALOG}.{BRONZE_SCHEMA}.inbound",
        "columns": ["started_at", "ended_at"],
        "rule_type": "ORDER_CHECK",
        "dimension": "일관성",
        "threshold_rate": 0.0,
        "error_grade": "HIGH"
    },
    {
        "rule_id": "DQ-UNI-IN-001",
        "rule_name": "Inbound consultation_id 중복 검사",
        "target_table": f"{UC_CATALOG}.{BRONZE_SCHEMA}.inbound",
        "columns": ["consultation_id"],
        "rule_type": "DUPLICATE_CHECK",
        "dimension": "유일성",
        "threshold_rate": 0.0,
        "error_grade": "CRITICAL"
    },
    {
        "rule_id": "DQ-VAL-IN-004",
        "rule_name": "Inbound 정책 상태 유효성",
        "target_table": f"{UC_CATALOG}.{BRONZE_SCHEMA}.inbound",
        "column": "policy_status",
        "code_group": "POLICY_STATUS",
        "rule_type": "CODE_EXISTS",
        "dimension": "유효성",
        "threshold_rate": 0.02,
        "error_grade": "MEDIUM"
    },
    {
        "rule_id": "DQ-COM-IN-003",
        "rule_name": "Inbound 상담원 ID 필수 검사",
        "target_table": f"{UC_CATALOG}.{BRONZE_SCHEMA}.inbound",
        "column": "agent_id",
        "rule_type": "NULL_CHECK",
        "dimension": "결손성",
        # Gold 이행 제외 컬럼(MAP-INB-CONSULTATION-019)이고 DQ 기준서에도 규칙이 없다 -> 위 inbound_type과 같은 이유로 격리하지 않는다.
        "threshold_rate": 0.05,
        "error_grade": "LOW"
    },

    # =====================================================================
    # 2. OUTBOUND CALLS -> target_table: outbound
    # =====================================================================
    {
        "rule_id": "DQ-COM-OUT-001",
        "rule_name": "Outbound CTI ID 누락 검사",
        "target_table": f"{UC_CATALOG}.{BRONZE_SCHEMA}.outbound",
        "column": "CTI_ID",
        "rule_type": "NULL_CHECK",
        "dimension": "결손성",
        "threshold_rate": 0.0,
        "error_grade": "CRITICAL"
    },
    {
        "rule_id": "DQ-CON-OUT-001",
        "rule_name": "Outbound 통화시작/연결 시각 순서 검사",
        "target_table": f"{UC_CATALOG}.{BRONZE_SCHEMA}.outbound",
        # 기준서 §7.2는 CALL_ST ≤ CONN ≤ END 체인이지만 ORDER_CHECK 구현(check_start_end_order, row_error_expr)은 앞의 2개 컬럼만 비교한다.
        # 이전에는 3개로 선언해 두고 CALL_END_DTM을 조용히 무시했으므로, 정의를 실제 검사와 일치시켰다 (검사 동작은 그대로).
        # "연결 ≤ 종료" 검사는 미연결 통화(CALL_CONN_DTM NULL)를 오류로 볼지 정한 뒤 별도 규칙으로 추가한다.
        "columns": ["CALL_ST_DTM", "CALL_CONN_DTM"],
        "rule_type": "ORDER_CHECK",
        "dimension": "일관성",
        "threshold_rate": 0.0,
        "error_grade": "HIGH"
    },
    {
        "rule_id": "DQ-VAL-OUT-001",
        "rule_name": "Outbound 수신번호 패턴 검사",
        "target_table": f"{UC_CATALOG}.{BRONZE_SCHEMA}.outbound",
        "column": "HP_NO",
        "rule_type": "PATTERN_CHECK",
        # 01로 시작하고, 뒤에 0~9 중 한 자리(보통 0, 1, 6, 7, 8, 9)와 숫자 7~8자리가 이어지는
        # 하이픈 없는 휴대전화번호 (형식은 PHONE_STANDARD_FORMAT 참고)
        "pattern": PHONE_PATTERN,
        "dimension": "유효성",
        "threshold_rate": 0.005,
        "error_grade": "MEDIUM"
    },
    {
        "rule_id": "DQ-VAL-OUT-002",
        "rule_name": "Outbound 결과 코드 검증",
        "target_table": f"{UC_CATALOG}.{BRONZE_SCHEMA}.outbound",
        "column": "CONN_RSLT_CD",
        "code_group": "CONN_RESULT",
        "rule_type": "CODE_EXISTS",
        "dimension": "유효성",
        "threshold_rate": 0.0,
        "error_grade": "HIGH"
    },
    {
        "rule_id": "DQ-UNI-OUT-001",
        "rule_name": "Outbound 리드관리번호 중복 검사",
        "target_table": f"{UC_CATALOG}.{BRONZE_SCHEMA}.outbound",
        "columns": ["LEAD_MGMT_NO"],
        "rule_type": "DUPLICATE_CHECK",
        "dimension": "유일성",
        "threshold_rate": 0.0,
        "error_grade": "CRITICAL"
    },
    {
        "rule_id": "DQ-COM-OUT-002",
        "rule_name": "Outbound 캠페인 코드 누락 검사",
        "target_table": f"{UC_CATALOG}.{BRONZE_SCHEMA}.outbound",
        "column": "CMPGN_CD",
        "rule_type": "NULL_CHECK",
        "dimension": "결손성",
        "threshold_rate": 0.01,
        "error_grade": "MEDIUM"
    },

    # =====================================================================
    # 3. SALESFORCE -> target_table: salesforce
    # =====================================================================
    {
        "rule_id": "DQ-COM-SF-001",
        "rule_name": "Salesforce ID 누락 검사",
        "target_table": f"{UC_CATALOG}.{BRONZE_SCHEMA}.salesforce",
        "column": "Id",
        "rule_type": "NULL_CHECK",
        "dimension": "결손성",
        "threshold_rate": 0.0,
        "error_grade": "CRITICAL"
    },
    {
        "rule_id": "DQ-COM-SF-002",
        "rule_name": "Salesforce 상담번호 누락 검사",
        "target_table": f"{UC_CATALOG}.{BRONZE_SCHEMA}.salesforce",
        "column": "Consultation_No__c",
        "rule_type": "NULL_CHECK",
        "dimension": "결손성",
        "threshold_rate": 0.01,
        "error_grade": "HIGH"
    },
    {
        "rule_id": "DQ-VAL-SF-001",
        "rule_name": "Salesforce 프로세스 상태 코드 검증",
        "target_table": f"{UC_CATALOG}.{BRONZE_SCHEMA}.salesforce",
        "column": "Process_Status__c",
        "code_group": "Process_Status__c",
        "rule_type": "CODE_EXISTS",
        "dimension": "유효성",
        "threshold_rate": 0.0,
        "error_grade": "HIGH"
    },
    {
        "rule_id": "DQ-VAL-SF-002",
        "rule_name": "Salesforce 고객 타입 검증",
        "target_table": f"{UC_CATALOG}.{BRONZE_SCHEMA}.salesforce",
        "column": "Customer_Type__c",
        "code_group": "Customer_Type__c",
        "rule_type": "CODE_EXISTS",
        "dimension": "유효성",
        "threshold_rate": 0.0,
        "error_grade": "MEDIUM"
    },
    {
        "rule_id": "DQ-VAL-SF-003",
        "rule_name": "Salesforce 신규 채널 코드 검토(REVIEW)",
        "target_table": f"{UC_CATALOG}.{BRONZE_SCHEMA}.salesforce",
        "column": "Referrer_Channel__c",
        "code_group": "Referrer_Channel__c",
        "rule_type": "CODE_EXISTS",
        "dimension": "유효성",
        "threshold_rate": 0.0,
        "error_grade": "REVIEW"
    },
    {
        "rule_id": "DQ-CON-SF-001",
        "rule_name": "Salesforce 생성/수정 일시 순서 검사",
        "target_table": f"{UC_CATALOG}.{BRONZE_SCHEMA}.salesforce",
        "columns": ["CreatedDate", "LastModifiedDate"],
        "rule_type": "ORDER_CHECK",
        "dimension": "일관성",
        "threshold_rate": 0.0,
        "error_grade": "HIGH"
    },
    {
        "rule_id": "DQ-UNI-SF-001",
        "rule_name": "Salesforce Consultation_No__c 중복 검사",
        "target_table": f"{UC_CATALOG}.{BRONZE_SCHEMA}.salesforce",
        "columns": ["Consultation_No__c"],
        "rule_type": "DUPLICATE_CHECK",
        "dimension": "유일성",
        "threshold_rate": 0.0,
        "error_grade": "CRITICAL"
    },
    {
        "rule_id": "DQ-COM-SF-003",
        "rule_name": "Salesforce 고객 ID 누락 검사",
        "target_table": f"{UC_CATALOG}.{BRONZE_SCHEMA}.salesforce",
        "column": "Customer_Id__c",
        "rule_type": "NULL_CHECK",
        "dimension": "결손성",
        "threshold_rate": 0.02,
        "error_grade": "MEDIUM"
    },

    # =====================================================================
    # 4. WEB COMPLAINTS -> target_table: homepage
    # =====================================================================
    {
        "rule_id": "DQ-COM-WEB-001",
        "rule_name": "홈페이지 민원 접수번호 누락 검사",
        "target_table": f"{UC_CATALOG}.{BRONZE_SCHEMA}.homepage",
        "column": "COMPLAINT_ID",
        "rule_type": "NULL_CHECK",
        "dimension": "결손성",
        "threshold_rate": 0.0,
        "error_grade": "CRITICAL"
    },
    {
        "rule_id": "DQ-VAL-WEB-001",
        "rule_name": "홈페이지 이메일 형식 검사",
        "target_table": f"{UC_CATALOG}.{BRONZE_SCHEMA}.homepage",
        "column": "CONTACT_EMAIL",
        "rule_type": "PATTERN_CHECK",
        "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
        "dimension": "유효성",
        "threshold_rate": 0.01,
        "error_grade": "MEDIUM"
    },
    {
        "rule_id": "DQ-VAL-WEB-002",
        "rule_name": "홈페이지 민원 유형 코드 검증",
        "target_table": f"{UC_CATALOG}.{BRONZE_SCHEMA}.homepage",
        "column": "COMPLAINT_TP",
        "code_group": "COMPLAINT_TYPE",
        "rule_type": "CODE_EXISTS",
        "dimension": "유효성",
        "threshold_rate": 0.0,
        "error_grade": "HIGH"
    },
    {
        "rule_id": "DQ-UNI-WEB-001",
        "rule_name": "홈페이지 COMPLAINT_ID 중복 검사",
        "target_table": f"{UC_CATALOG}.{BRONZE_SCHEMA}.homepage",
        "columns": ["COMPLAINT_ID"],
        "rule_type": "DUPLICATE_CHECK",
        "dimension": "유일성",
        "threshold_rate": 0.0,
        "error_grade": "CRITICAL"
    },
    {
        "rule_id": "DQ-VAL-WEB-003",
        "rule_name": "홈페이지 연락처 패턴 검사",
        "target_table": f"{UC_CATALOG}.{BRONZE_SCHEMA}.homepage",
        "column": "CONTACT_TEL",
        "rule_type": "PATTERN_CHECK",
        "pattern": PHONE_PATTERN,
        "dimension": "유효성",
        "threshold_rate": 0.005,
        "error_grade": "MEDIUM"
    },
    {
        "rule_id": "DQ-COM-WEB-002",
        "rule_name": "홈페이지 민원 제목 필수 입력 검사",
        "target_table": f"{UC_CATALOG}.{BRONZE_SCHEMA}.homepage",
        "column": "COMPLAINT_TITLE",
        "rule_type": "NULL_CHECK",
        "dimension": "결손성",
        "threshold_rate": 0.0,
        "error_grade": "HIGH"
    },
    {
        "rule_id": "DQ-COM-WEB-003",
        "rule_name": "홈페이지 민원 본문 필수 입력 검사",
        "target_table": f"{UC_CATALOG}.{BRONZE_SCHEMA}.homepage",
        "column": "COMPLAINT_CONTENT",
        "rule_type": "NULL_CHECK",
        "dimension": "결손성",
        "threshold_rate": 0.0,
        "error_grade": "HIGH"
    },

    # =====================================================================
    # 5. CHATBOT -> target_table: chatbot
    # =====================================================================
    {
        "rule_id": "DQ-COM-CHAT-001",
        "rule_name": "Chatbot 대화 세션 ID 누락 검사",
        "target_table": f"{UC_CATALOG}.{BRONZE_SCHEMA}.chatbot",
        "column": "session_id",
        "rule_type": "NULL_CHECK",
        "dimension": "결손성",
        "threshold_rate": 0.0,
        "error_grade": "CRITICAL"
    },
    {
        "rule_id": "DQ-VAL-CHAT-001",
        "rule_name": "Chatbot 의도 카테고리 검증",
        "target_table": f"{UC_CATALOG}.{BRONZE_SCHEMA}.chatbot",
        "column": "intent_category_guess",
        "code_group": "CHATBOT_INTENT",
        "rule_type": "CODE_EXISTS",
        "dimension": "유효성",
        "threshold_rate": 0.01,
        "error_grade": "MEDIUM"
    },
    {
        "rule_id": "DQ-CON-CHAT-001",
        "rule_name": "Chatbot 질의/응답 시각 순서 검사",
        "target_table": f"{UC_CATALOG}.{BRONZE_SCHEMA}.chatbot",
        "columns": ["started_at", "ended_at"],
        "rule_type": "ORDER_CHECK",
        "dimension": "일관성",
        "threshold_rate": 0.0,
        "error_grade": "HIGH"
    },
    {
        "rule_id": "DQ-VAL-CHAT-002",
        "rule_name": "Chatbot 응답 소요시간(초) 범위 검사",
        "target_table": f"{UC_CATALOG}.{BRONZE_SCHEMA}.chatbot",
        "column": "duration_seconds",
        "rule_type": "RANGE_CHECK",
        "min_value": 0,
        "max_value": 3600,
        "dimension": "유효성",
        "threshold_rate": 0.01,
        "error_grade": "LOW"
    },
    {
        "rule_id": "DQ-COM-CHAT-002",
        "rule_name": "Chatbot 사용자 쿼리 필수 누락 검사",
        "target_table": f"{UC_CATALOG}.{BRONZE_SCHEMA}.chatbot",
        "column": "user_query",
        "rule_type": "NULL_CHECK",
        "dimension": "결손성",
        "threshold_rate": 0.005,
        "error_grade": "MEDIUM"
    }
]


# -------------------------------------------------------------------------
# DQ Rule <-> Cleansing Rule 매핑 (DQ 기준서 8.8)
#
# steps: 위에서부터 순서대로 적용한다. CLN-COM-001(앞뒤 공백 제거)은 " 010-...", " M " 처럼
#        공백 때문에 패턴/코드 검증에 실패한 값을 먼저 정리하는 선행 단계로 붙인다 (8.4 예시).
# 여기 없는 Rule은 자동 Cleansing 대상이 아니며, 오류 건은 전부 UNRESOLVED로 분류된다 (8.9).
#
# ※ 기준서(8.8)의 Rule ID(DQ-VAL-INB-002 등)와 이 파일의 rule_id(DQ-VAL-IN-001 등) 체계가 달라서,
#    컬럼/의미 기준으로 매핑했다. Rule ID 체계를 통일하면 이 딕셔너리 키만 바꾸면 된다.
# -------------------------------------------------------------------------
_PHONE_STEPS = ["CLN-COM-001", "CLN-VAL-001"]
_CODE_STEPS = ["CLN-COM-001", "CLN-VAL-003"]   # CLN-VAL-003은 보류(enabled=False)라 지금은 공백 정규화(CLN-COM-001)만 적용된다

CLEANSING_RULE_MAPPING = {
    # ---- 전화번호 형식 오류 -> CLN-VAL-001 ----
    "DQ-VAL-IN-001":   {"steps": _PHONE_STEPS},   # inbound.phone_number
    "DQ-VAL-OUT-001":  {"steps": _PHONE_STEPS},   # outbound.HP_NO
    "DQ-VAL-WEB-003":  {"steps": _PHONE_STEPS},   # homepage.CONTACT_TEL

    # ---- 코드값 오류 -> CLN-VAL-003 (보류 중: 지금은 공백 제거 후에도 코드 마스터에 없으면 UNRESOLVED) ----
    "DQ-VAL-IN-002":   {"steps": _CODE_STEPS},    # inbound.status
    "DQ-VAL-IN-003":   {"steps": _CODE_STEPS},    # inbound.inbound_type
    "DQ-VAL-IN-004":   {"steps": _CODE_STEPS},    # inbound.policy_status
    "DQ-VAL-OUT-002":  {"steps": _CODE_STEPS},    # outbound.CONN_RSLT_CD
    "DQ-VAL-SF-001":   {"steps": _CODE_STEPS},    # salesforce.Process_Status__c
    "DQ-VAL-SF-002":   {"steps": _CODE_STEPS},    # salesforce.Customer_Type__c
    "DQ-VAL-WEB-002":  {"steps": _CODE_STEPS},    # homepage.COMPLAINT_TP
    "DQ-VAL-CHAT-001": {"steps": _CODE_STEPS},    # chatbot.intent_category_guess

    # ---- 의도적으로 제외 ----
    # DQ-VAL-SF-003 (Referrer_Channel__c): "신규 채널 코드 검토(REVIEW)" 목적의 Rule이라 자동 변환하지 않는다.
    # DQ-VAL-WEB-001 (이메일 형식): 기준서 8.8에 매핑이 없어 제외. 공백만 문제면 ["CLN-COM-001"]로 추가 가능.

    # ---- CLN-VAL-002(날짜/시간)는 현재 매핑할 DQ Rule이 없다 ----
    # 날짜/시간 형식 검증용 PATTERN_CHECK Rule이 DQ_RULES에 추가되면 아래처럼 등록한다.
    # "DQ-VAL-XXX-00N": {"steps": ["CLN-COM-001", "CLN-VAL-002"], "datetime_kind": "TIMESTAMP"},  # 또는 "DATE"
}


def validate_cleansing_config() -> None:
    """오타로 매핑이 조용히 무시되는 것을 막기 위해 import 시점에 한 번 검증한다."""
    rules_by_id = {r["rule_id"]: r for r in DQ_RULES}
    for rule_id, cfg in CLEANSING_RULE_MAPPING.items():
        rule = rules_by_id.get(rule_id)
        if rule is None:
            raise ValueError(f"CLEANSING_RULE_MAPPING의 '{rule_id}'가 DQ_RULES에 없습니다.")
        if rule["rule_type"] not in AUTO_CLEANSING_RULE_TYPES:
            reason = CLEANSING_EXCLUDED_REASONS.get(rule["rule_type"], "재-DQ 불가 유형")
            raise ValueError(f"'{rule_id}'({rule['rule_type']})는 자동 Cleansing 대상이 될 수 없습니다: {reason}")
        for step in cfg["steps"]:
            if step not in CLEANSING_RULES:
                raise ValueError(f"'{rule_id}'의 step '{step}'가 CLEANSING_RULES에 정의되어 있지 않습니다.")
        if cfg.get("datetime_kind", "TIMESTAMP") not in ("DATE", "TIMESTAMP"):
            raise ValueError(f"'{rule_id}'의 datetime_kind는 DATE/TIMESTAMP 중 하나여야 합니다.")


def cleansing_config_for(rule: dict):
    """
    Rule에 붙은 자동 Cleansing 설정을 돌려준다 (없으면 None).
      - 테이블(meta.dq_rule_def)에서 읽은 Rule: rule["cleansing_steps"] (리스트, 비어 있으면 정제 없음)
      - 코드(DQ_RULES)로 실행하는 Rule: CLEANSING_RULE_MAPPING
    """
    if "cleansing_steps" in rule:
        steps = list(rule["cleansing_steps"] or [])
        if not steps:
            return None
        cfg = {"steps": steps}
        if rule.get("datetime_kind"):
            cfg["datetime_kind"] = rule["datetime_kind"]
        return cfg
    return CLEANSING_RULE_MAPPING.get(rule["rule_id"])


validate_cleansing_config()
