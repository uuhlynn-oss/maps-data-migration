"""
Silver -> Gold Mapping Execution 설정 (슬라이스 1: 인바운드 -> COUNSEL)
"""

UC_CATALOG = "maps_databricks"
META_SCHEMA = "meta"
GOLD_CANDIDATE_SCHEMA = "gold_candidate"
GOLD_MAPPING_ERROR_SCHEMA = "gold_mapping_error"   # _map_errors IS NOT NULL 행 전용 (Gold Validation 입력에서 제외)
GOLD_ENTITY_LINEAGE_SCHEMA = "gold_entity_lineage"   # Entity MERGE로 대표 행에서 탈락한 원본 lineage -> 최종 PK crosswalk
SILVER_INPUT_SCHEMA = "silver_candidate"   # 정식 Silver 테이블이 생기면 여기만 바꾼다

# ---- 엔진이 읽는 메타데이터 테이블 (mapping_seed_loader.py로 CSV에서 적재) ----
TARGET_MODEL_TABLE = f"{UC_CATALOG}.{META_SCHEMA}.target_model"                    # TO-BE 물리 모델 (DA 확정본)
MAPPING_DEFINITION_TABLE = f"{UC_CATALOG}.{META_SCHEMA}.mapping_definition"        # 컬럼 단위 Source -> Target 매핑
CODE_MAPPING_TABLE = f"{UC_CATALOG}.{META_SCHEMA}.code_mapping_asis_tobe"          # AS-IS -> TO-BE 코드 변환표 (v1.1 최종)
# ※ DQ 단계의 meta.code_mapping(CLN-VAL-003, 보류 중)과는 다른 테이블이다. 코드 변환의 정본은 이쪽이다.

# ---- Master Data(비즈니스가 이미 확정한 원천 파일) Volume 경로 ----
# meta 메타데이터가 /Volumes/<catalog>/meta/files에 있는 것과 같은 컨벤션(<catalog>/<schema>/<volume>)을
# 따르되, DA/AI가 만드는 파이프라인 메타데이터(meta)와 비즈니스가 확정한 원천 파일은 스키마를 분리한다 -
# Master Data는 AI Mapping이나 이 파이프라인이 생성하는 데이터가 아니라 그대로 갖다 쓰는 authoritative
# source이기 때문이다. product_master_seed_loader.py가 이 CSV를 읽어 silver_input_table("product_master")로
# 적재한다 (SOURCE_SYSTEMS["PRODUCT_MASTER"] 참고).
MASTER_DATA_SCHEMA = "master_data"
MASTER_DATA_VOLUME_DIR = f"/Volumes/{UC_CATALOG}/{MASTER_DATA_SCHEMA}/files"
PRODUCT_MASTER_CSV = f"{MASTER_DATA_VOLUME_DIR}/product_master.csv"


def silver_input_table(silver_source: str) -> str:
    return f"{UC_CATALOG}.{SILVER_INPUT_SCHEMA}.{silver_source}"


def gold_candidate_table(target_table: str) -> str:
    """Target 테이블 1개당 후보 테이블 1개. 여러 소스(inbound/outbound/...)가 같은 후보 테이블에 _source_system으로 구분되어 들어간다.
    _map_errors가 있는 행은 여기 들어가지 않는다 (gold_mapping_error_table 참고) - Gold Validation의 입력은 항상 Mapping 성공분뿐이다."""
    return f"{UC_CATALOG}.{GOLD_CANDIDATE_SCHEMA}.{target_table.lower()}"


def gold_mapping_error_table(target_table: str) -> str:
    """_map_errors IS NOT NULL인 행(값 변환 실패)만 모아두는 테이블. gold_candidate_table과 같은 naming 패턴."""
    return f"{UC_CATALOG}.{GOLD_MAPPING_ERROR_SCHEMA}.{target_table.lower()}"


def gold_entity_lineage_table(target_table: str) -> str:
    """Entity Integration(MERGE)에서 대표 행으로 축약되며 사라지는 원본 lineage를 최종 PK와 이어주는 crosswalk.
    (source_system, source_batch_id, source_record_key) -> (target_table, target_pk). UK가 없는 Target(예:
    CUSTOMER)을 다른 Target의 FK Lookup(run())이 참조할 때, 병합 중 사라진 원본의 lineage도 찾을 수 있게 한다.
    gold_candidate_table과 같은 naming 패턴이며 target_table을 그대로 받으므로 특정 Entity에 종속되지 않는다."""
    return f"{UC_CATALOG}.{GOLD_ENTITY_LINEAGE_SCHEMA}.{target_table.lower()}"


# PRODUCT_MAPPING(채널별 상품코드 -> PRODUCT.PRD_ID 크로스워크, target_model.csv에 스키마가 이미 정의돼
# 있음: SRC_SYS/SRC_PRD_CD/TGT_PRD_ID/APRV_YN 등)은 "Master Data Integration의 관계 매핑"이라 다른 Master
# Data Integration Target(PRODUCT)과 같은 gold_candidate 스키마에 둔다 - 새 스키마를 만들지 않고 기존
# gold_candidate_table()을 그대로 재사용한다. AI Mapping + 사람 승인 결과가 최종적으로 쌓이는 곳이며, 지금은
# AI Mapping이 없어 사람이 확정한 값(또는 테스트용 seed)만 들어간다.
PRODUCT_MAPPING_TABLE = gold_candidate_table("PRODUCT_MAPPING")


# 소스 시스템 이름이 문서마다 달라서(매핑 정의 INBOUND / 코드 매핑표 인바운드 / Silver inbound) 한 곳에서 연결한다.
SOURCE_SYSTEMS = {
    "INBOUND":    {"silver": "inbound",    "code_mapping": "인바운드"},
    "OUTBOUND":   {"silver": "outbound",   "code_mapping": "아웃바운드"},
    "SALESFORCE": {"silver": "salesforce", "code_mapping": "세일즈포스"},
    "HOMEPAGE":   {"silver": "homepage",   "code_mapping": "민원"},
    "CHATBOT":    {"silver": "chatbot",    "code_mapping": "챗봇"},
    # 상담 채널이 아니라 PRODUCT의 소스인 상품마스터 참조 데이터다. source_system -> {silver, code_mapping}
    # 연결 구조를 그대로 재사용할 수 있어(엔진이 다른 소스로 안다는 차이만 있을 뿐 구조는 동일) 별도 매핑
    # 방식을 새로 만들지 않고 여기 추가한다.
    "PRODUCT_MASTER": {"silver": "product_master", "code_mapping": "상품마스터"},
}

# 이 엔진이 실행할 수 있는 Target 테이블. run() 자체는 항상 "소스 1행 -> Target 1행"만 하며(컬럼 매핑/코드
# 변환/PK 컬럼 보류), 그 결과를 gold_candidate에 어떻게 반영하는지는 Target의 유형에 따라 세 가지로 나뉜다.
#   A. Direct Mapping        run() -> save()만으로 끝난다. Source-Target이 사실상 1:1인 경우.
#                             예: COUNSEL, COMPLAINT.
#   B. Entity Integration     여러 소스 행이 하나의 개체로 합쳐져야 하는 경우. run()/save()를 소스마다 반복한
#                             뒤, MappingEngine.integrate()가 Record Matching(자연키가 아닌 매칭 규칙) +
#                             중복 제거/통합(MERGE) + ID 생성/재사용을 한 번에 한다 - mapping_config.
#                             ENTITY_INTEGRATION_TABLE의 메타데이터로 동적으로 해석한다 (integrate()는 모든
#                             소스의 run()/save()가 끝난 뒤 target_table당 한 번만 호출).
#                             예: CUSTOMER.
#   C. Master Data Integration 비즈니스가 이미 확정한 Master Data(예: 표준상품마스터)가 authoritative
#                             source인 경우. 여러 소스를 매칭/병합하는 게 아니라, 이미 완결된 1행=1개체
#                             목록을 "지금 이 순간의 전체 스냅샷"으로 gold_candidate에 반영하면 된다 - 그래서
#                             Record Matching이나 충돌 해소(CONFLICT_RULE)는 필요 없고, 자연키(예: PRD_CD)
#                             기준 ID 재사용/신규 발급만 하면 된다. MappingEngine.load_master_data()가 run()
#                             결과를 받아 이 반영(저장)까지 한 번에 한다 - Entity Integration과 달리 소스가
#                             하나뿐이라 run() 직후 target_table당 한 번만 호출하면 끝난다.
#                             예: PRODUCT (Target Model/표준상품마스터 자체는 AI Mapping이 만드는 게 아니다 -
#                             AI Mapping의 역할은 Source Column -> Target Column 추천뿐이다).
# COMPLAINT(HOMEPAGE)는 target_model/mapping_definition 확인 결과 NOT NULL 컬럼(CMPL_ID/CMPL_CNTNT/REG_DTM)이
# 전부 이 엔진이 지원하는 매핑 타입(RENAME/COPY, TIMESTAMP FORMAT)으로 커버되어 1:1로 안전해 추가한다.
# CONTRACT는 mapping_definition에 INBOUND -> CONTRACT 매핑(PLCY_NO/CNTR_ST_CD/CNTR_DT/EXPR_DT/CNTR_ID/
# CUST_ID/PRD_ID)이 확인되어 Entity Integration(B)으로 추가한다 - entity_integration_definition.csv의
# TARGET_ENTITY=CONTRACT 행(MATCHING_KEY_COLUMNS=PLCY_NO, CONFLICT_REFERENCE=CONTRACT.CNTR_DT)을
# integrate()가 그대로 읽어 처리한다. CUST_ID/PRD_ID는 MAPPING_TYPE=DERIVED/PROCESS_TYPE=LOOKUP으로
# 정의돼 있고, run()이 이제 이 조합을 처리한다(MappingEngine._fk_lookup - 참조 Target의 gold_candidate에서
# PK를 조회, target_model 기반으로 동적 판단이라 하드코딩 없음). CNTR_ID(GENERATE_ID)는 여전히 CUSTOMER의
# CUST_ID와 마찬가지로 run()에서 NULL+deferred, integrate()가 채번한다. 현재 OUTBOUND -> CONTRACT 매핑은
# 없어 INBOUND만 실행 대상이다(discover_sources()가 mapping_definition 기준으로 자동 판단하므로 코드
# 변경 불필요).
# COUNSEL_DETAIL(PK 매핑 없음 + EXPLODE로 실제 1:N)은 계속 제외한다.
# PRODUCT_MAPPING은 여기 없다 - Silver -> run()을 거치는 Target이 아니라, PRODUCT_MAPPING_TABLE에 직접
# 채워지는 크로스워크 참조 테이블이다(entity_integration_definition.csv/code_mapping_asis_tobe와 같은
# 성격 - AI Mapping+사람 승인 결과가 쌓이는 곳). CONTRACT/COUNSEL의 PRD_ID FK Lookup이 이걸 조회한다.
SUPPORTED_TARGET_TABLES = ("COUNSEL", "COMPLAINT", "CUSTOMER", "PRODUCT", "CONTRACT")

# 실행 대상 매핑 행 조건: FINAL_MIGRATION_APPLY_YN = 'Y' 이고 REVIEW_STATUS가 아래 중 하나
APPLY_REVIEW_STATUSES = ("APPROVED", "MODIFIED_APPROVED")

# integrate()가 허용하는 MATCHING_RULE 이름 화이트리스트. 실제 매칭 알고리즘은 이름과 무관하게
# MATCHING_KEY_COLUMNS 전체 컬럼의 완전일치(EXACT) 하나뿐이라, 이 튜플은 로직을 분기하지 않고 "정의가
# 오타/미승인 값으로 잘못 들어오는 것을 막는" 안전장치일 뿐이다. entity_integration_definition.csv에 새
# Target을 추가할 때 그 MATCHING_RULE 이름을 여기 추가해야 integrate()가 처리한다.
#   NAME_DOB_PHONE_EXACT  CUSTOMER (이름+생년월일 완전일치 - 이름은 유지되지만 TEL_NO는 실제로 안 씀)
#   PLCY_NO_EXACT         CONTRACT (증권/계약 업무번호 완전일치 - mapping_definition의 INBOUND policy_id
#                         -> PLCY_NO 매핑으로 확인된 Source 자연키)
SUPPORTED_MATCHING_RULES = ("NAME_DOB_PHONE_EXACT", "PLCY_NO_EXACT")

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