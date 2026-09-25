"""
Silver -> Gold Mapping Execution 설정 (슬라이스 1: 인바운드 -> COUNSEL)
"""

UC_CATALOG = "maps_databricks"
META_SCHEMA = "meta"
GOLD_CANDIDATE_SCHEMA = "gold_candidate"
GOLD_MAPPING_ERROR_SCHEMA = "gold_mapping_error"   # _map_errors IS NOT NULL 행 전용 (Gold Validation 입력에서 제외)
SILVER_INPUT_SCHEMA = "silver_candidate"   # 정식 Silver 테이블이 생기면 여기만 바꾼다

# ---- 엔진이 읽는 메타데이터 테이블 (mapping_seed_loader.py로 CSV에서 적재) ----
TARGET_MODEL_TABLE = f"{UC_CATALOG}.{META_SCHEMA}.target_model"                    # TO-BE 물리 모델 (DA 확정본)
MAPPING_DEFINITION_TABLE = f"{UC_CATALOG}.{META_SCHEMA}.mapping_definition"        # 컬럼 단위 Source -> Target 매핑
CODE_MAPPING_TABLE = f"{UC_CATALOG}.{META_SCHEMA}.code_mapping_asis_tobe"          # AS-IS -> TO-BE 코드 변환표 (v1.1 최종)
# ※ DQ 단계의 meta.code_mapping(CLN-VAL-003, 보류 중)과는 다른 테이블이다. 코드 변환의 정본은 이쪽이다.


def silver_input_table(silver_source: str) -> str:
    return f"{UC_CATALOG}.{SILVER_INPUT_SCHEMA}.{silver_source}"


def gold_candidate_table(target_table: str) -> str:
    """Target 테이블 1개당 후보 테이블 1개. 여러 소스(inbound/outbound/...)가 같은 후보 테이블에 _source_system으로 구분되어 들어간다.
    _map_errors가 있는 행은 여기 들어가지 않는다 (gold_mapping_error_table 참고) - Gold Validation의 입력은 항상 Mapping 성공분뿐이다."""
    return f"{UC_CATALOG}.{GOLD_CANDIDATE_SCHEMA}.{target_table.lower()}"


def gold_mapping_error_table(target_table: str) -> str:
    """_map_errors IS NOT NULL인 행(값 변환 실패)만 모아두는 테이블. gold_candidate_table과 같은 naming 패턴."""
    return f"{UC_CATALOG}.{GOLD_MAPPING_ERROR_SCHEMA}.{target_table.lower()}"


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
# COMPLAINT(HOMEPAGE)는 target_model/mapping_definition 확인 결과 NOT NULL 컬럼(CMPL_ID/CMPL_CNTNT/REG_DTM)이
# 전부 이 엔진이 지원하는 매핑 타입(RENAME/COPY, TIMESTAMP FORMAT)으로 커버되어 1:1로 안전해 추가한다.
# CUSTOMER는 여러 소스 행이 하나로 합쳐지는 테이블이라 예전엔 막혀 있었지만, mapping_config.ENTITY_INTEGRATION_TABLE +
# MappingEngine.integrate()로 Entity Integration(MERGE)을 별도 단계로 실행하게 되어 추가한다 - integrate()를
# 반드시 모든 소스의 run()/save()가 끝난 뒤 한 번만 호출해야 한다 (03_mapping_run.py의 RUN_INTEGRATION 참고).
# CONTRACT(PK·FK가 DERIVED/GENERATE_ID·LOOKUP으로만 정의돼 엔진 미지원)/COUNSEL_DETAIL(PK 매핑 없음 + EXPLODE로
# 실제 1:N)/PRODUCT(mapping_definition 행 자체가 없음)는 계속 제외한다.
SUPPORTED_TARGET_TABLES = ("COUNSEL", "COMPLAINT", "CUSTOMER")

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
SOURCE_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"   # 표준 포맷 - DQ Cleansing(CLN-VAL-002)이 모든 소스를 이 형식으로 정규화한다.
SOURCE_DATE_FORMAT = "yyyy-MM-dd"   # 표준 포맷 - DQ Cleansing(CLN-VAL-002, DATE_STANDARD_FORMAT과 동일 값)이 정규화한다.

# ---- 정책 (잠정) ----
# 승인된 코드 매핑이 없는 값(REVIEW 포함)은 격리하지 않고 NULL로 적재하고, 원천 값을 _unmapped_codes에 남긴다.
# (TO-BE의 해당 코드 컬럼이 모두 NULL 허용이고, 격리하면 레코드 손실이 커서 잠정으로 이렇게 둔다 - 확정 필요)
UNMAPPED_CODE_POLICY = "NULL_AND_RECORD"

# ---------------------------------------------------------------------------
# Entity Integration Definition: 여러 소스가 같은 Target Entity로 합쳐질 때 "어떻게 통합할지"는
# Column Mapping(MAPPING_TYPE/PROCESS_TYPE)만으로 표현할 수 없어 별도 메타데이터로 관리한다.
# AI가 생성하고 HITL이 승인한 CSV(entity_integration_definition.csv, mapping_seed_loader.py 참고)를
# meta.entity_integration_definition 테이블로 적재해, Engine이 target_table마다 동적으로 해석한다.
# 코드에 특정 Target 이름이나 통합 방식을 하드코딩하지 않는다 - 이 테이블에 없는 target_table은 DIRECT로 본다.
# 컬럼: TARGET_ENTITY, INTEGRATION_TYPE(DIRECT/UNION/MERGE), MATCHING_RULE, MATCHING_KEY_COLUMNS(콤마구분,
#      Target 컬럼명 기준), CONFLICT_RULE, CONFLICT_REFERENCE("TARGET_TABLE.TARGET_COLUMN" 형식 - 예:
#      "COUNSEL.STRT_DTM". CONFLICT_RULE=LATEST_CONSULTATION일 때 어느 표준 컬럼을 "상담일시"로 볼지 가리킨다.
#      기존 mapping_definition에 이 (target_table,target_column)에 대한 (source_system,source_table)별
#      SOURCE_COLUMN 매핑이 이미 있다고 가정하고 그걸 그대로 재사용한다 - 새 정규화 로직을 만들지 않는다),
#      REVIEW_STATUS/FINAL_MIGRATION_APPLY_YN(기존 mapping_definition과 같은 HITL 승인 컬럼, 같은 값 재사용).
# ---------------------------------------------------------------------------
ENTITY_INTEGRATION_TABLE = f"{UC_CATALOG}.{META_SCHEMA}.entity_integration_definition"