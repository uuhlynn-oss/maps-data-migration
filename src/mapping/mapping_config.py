"""
Silver -> Gold Mapping Execution 설정 (슬라이스 1: 인바운드 -> COUNSEL)
"""

UC_CATALOG = "maps_databricks"
META_SCHEMA = "meta"
GOLD_CANDIDATE_SCHEMA = "gold_candidate"
SILVER_INPUT_SCHEMA = "silver_candidate"   # 정식 Silver 테이블이 생기면 여기만 바꾼다

# ---- 엔진이 읽는 메타데이터 테이블 (mapping_seed_loader.py로 CSV에서 적재) ----
TARGET_MODEL_TABLE = f"{UC_CATALOG}.{META_SCHEMA}.target_model"                    # TO-BE 물리 모델 (DA 확정본)
MAPPING_DEFINITION_TABLE = f"{UC_CATALOG}.{META_SCHEMA}.mapping_definition"        # 컬럼 단위 Source -> Target 매핑
CODE_MAPPING_TABLE = f"{UC_CATALOG}.{META_SCHEMA}.code_mapping_asis_tobe"          # AS-IS -> TO-BE 코드 변환표 (v1.1 최종)
# ※ DQ 단계의 meta.code_mapping(CLN-VAL-003, 보류 중)과는 다른 테이블이다. 코드 변환의 정본은 이쪽이다.


def silver_input_table(silver_source: str) -> str:
    return f"{UC_CATALOG}.{SILVER_INPUT_SCHEMA}.{silver_source}"


def gold_candidate_table(target_table: str) -> str:
    """Target 테이블 1개당 후보 테이블 1개. 여러 소스(inbound/outbound/...)가 같은 후보 테이블에 _source_system으로 구분되어 들어간다."""
    return f"{UC_CATALOG}.{GOLD_CANDIDATE_SCHEMA}.{target_table.lower()}"


# 소스 시스템 이름이 문서마다 달라서(매핑 정의 INBOUND / 코드 매핑표 인바운드 / Silver inbound) 한 곳에서 연결한다.
SOURCE_SYSTEMS = {
    "INBOUND":    {"silver": "inbound",    "code_mapping": "인바운드"},
    "OUTBOUND":   {"silver": "outbound",   "code_mapping": "아웃바운드"},
    "SALESFORCE": {"silver": "salesforce", "code_mapping": "세일즈포스"},
    "HOMEPAGE":   {"silver": "homepage",   "code_mapping": "민원"},
    "CHATBOT":    {"silver": "chatbot",    "code_mapping": "챗봇"},
}

# 이 엔진이 실행할 수 있는 Target 테이블 (소스 1행 -> Target 1행, 즉 1:1 변환만).
# CUSTOMER / CONTRACT처럼 여러 소스 행이 같은 개체로 합쳐지는 테이블은 중복 제거와 개체 통합(고객 매칭, ID 생성)이 필요해서
# 다음 단계에서 다룬다. 그때까지 실행하면 "상담 1건당 1행"이 나와 잘못된 결과가 되므로 막아 둔다.
SUPPORTED_TARGET_TABLES = ("COUNSEL",)

# 실행 대상 매핑 행 조건: FINAL_MIGRATION_APPLY_YN = 'Y' 이고 REVIEW_STATUS가 아래 중 하나
APPLY_REVIEW_STATUSES = ("APPROVED", "MODIFIED_APPROVED")

# 코드 매핑표에서 "소스 시스템 자체"를 뜻하는 행의 SOURCE_COLUMN 표기 (문서마다 괄호 모양이 다름)
CONSTANT_SOURCE_COLUMNS = ("(SOURCE_SYSTEM)", "<SOURCE_SYSTEM>")

# ---- 시간대 ----
# TO-BE 물리 정책: 시각은 UTC로 저장하고 조회/표시 때 KST로 변환한다.
# Silver의 시각 문자열이 어느 시간대인지는 소스마다 다를 수 있어 설정으로 분리한다.
# 상류(Bronze/Silver 적재) 코드가 시각을 UTC로 바꿔 저장하도록 바뀌면, 그 소스는 "UTC"로 바꿔야 이중 변환이 생기지 않는다.
DEFAULT_SOURCE_TIMEZONE = "Asia/Seoul"
SOURCE_TIMEZONE = {
    "INBOUND": "Asia/Seoul",
}
SOURCE_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"

# ---- 정책 (잠정) ----
# 승인된 코드 매핑이 없는 값(REVIEW 포함)은 격리하지 않고 NULL로 적재하고, 원천 값을 _unmapped_codes에 남긴다.
# (TO-BE의 해당 코드 컬럼이 모두 NULL 허용이고, 격리하면 레코드 손실이 커서 잠정으로 이렇게 둔다 - 확정 필요)
UNMAPPED_CODE_POLICY = "NULL_AND_RECORD"