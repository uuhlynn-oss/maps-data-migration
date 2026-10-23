"""
PRODUCT 테스트용 매핑 메타데이터 (AI Mapping 알고리즘은 이번 작업 범위 밖 - 사람이 확정한 값을 하드코딩).
PRODUCT는 mapping_config.SUPPORTED_TARGET_TABLES 기준 "C. Master Data Integration" Target이다 - 여러 소스를
매칭/병합하는 게 아니라, 비즈니스가 이미 확정한 표준상품마스터(authoritative source)를 그대로 Target Model에
얹는다. AI Mapping은 그 확정된 마스터를 만들지 않는다.

책임 범위 (AI Mapping이 실제로 구현되면 이 구조는 그대로, AI_MAPPING_RECOMMENDATIONS만 알고리즘 출력으로 바뀐다):
    표준상품마스터(product_master.csv)  비즈니스가 이미 확정한 Master Data. AI Mapping이 만들지 않는다 -
                                        product_master_seed_loader.py가 Volume에서 그대로 읽어 Silver로 적재한다.
    Target Schema(target_model)  이미 존재하는 메타데이터. AI Mapping이 만드는 게 아니라 "참고"만 한다.
    code_mapping                 별도로 관리되는 코드 변환 규칙. 이 역시 AI Mapping이 만드는 게 아니라 "참고"만 한다.
    AI Mapping                   Source Column -> Target Column 추천만 한다 (PK/GENERATE_ID 제외).
    사용자 승인 -> Mapping Definition 확정   추천에 SOURCE_SYSTEM/MAPPING_TYPE/PROCESS_TYPE/REVIEW_STATUS 등
                                            Execution에 필요한 정보를 붙여 mapping_definition 행으로 굳힌다.
    Mapping Execution             확정된 Mapping Definition + target_model + code_mapping으로 Silver -> Gold 변환
                                   (generic MappingEngine.run()). PRD_ID 채번(GENERATE_ID)은 컬럼 매핑이 아니라
                                   이 단계 고유의 로직이다 (MappingEngine.load_master_data, 기존 PRD_CD 기준
                                   유지/신규 발급 - 상품 코드 체계나 채번 규칙 자체를 AI Mapping이 정하지 않는다).

AI Mapping 알고리즘이 아직 없어 아래 세 블록 모두 "결과가 이미 확정되어 있다고 가정"하고 하드코딩했지만,
표현하는 정보의 성격은 다르다:
    TARGET_MODEL_ROWS / CODE_MAPPING_ROWS  AI Mapping과 무관하게 이미 존재해야 하는 참고 메타데이터의 스냅샷
                                            (진짜 target_model/code_mapping 테이블이 생기면 그쪽을 쓰고 여기선 지운다)
    AI_MAPPING_RECOMMENDATIONS             AI Mapping이 "추천"했을 법한 산출물의 스냅샷 (SOURCE_COLUMN/TARGET_COLUMN만 -
                                            confidence/reason 등 추천 메타데이터를 붙일 수 있는 자리이지 Execution
                                            이 쓰는 필드가 아니다)
    MAPPING_DEFINITION_ROWS                위 추천에 "사용자 승인"을 가정해 Execution 필드를 채운 확정본 + PRD_ID
                                            채번 정의(추천 대상 아님, 별도 표시) - MappingEngine이 실제로 읽는 것은 이것뿐.
"""

# ---------------------------------------------------------------------------
# Target Schema(target_model) 스냅샷 - 이미 정의되어 있는 것으로 간주. AI Mapping이 생성하지 않고, 존재를
# 전제로 컬럼 매핑을 추천할 때 "어떤 Target 컬럼이 있는지" 참고만 한다.
# ---------------------------------------------------------------------------
# ORDINAL은 mapping_seed_loader.py가 target_model.csv를 적재할 때 파일 순서를 보존하려고 붙이는 것과 같은
# 관례(문자열 "1".."N")를 따른다 - 필수는 아니지만(엔진은 없어도 동작) 실제 target_model 테이블과 형식을 맞춘다.
TARGET_MODEL_ROWS = [
    {"TABLE_NAME": "PRODUCT", "COLUMN_NAME": "PRD_ID",       "DATA_TYPE": "VARCHAR(20)",  "ORDINAL": "1"},
    {"TABLE_NAME": "PRODUCT", "COLUMN_NAME": "PRD_CD",       "DATA_TYPE": "VARCHAR(20)",  "ORDINAL": "2"},
    {"TABLE_NAME": "PRODUCT", "COLUMN_NAME": "PRD_NM",       "DATA_TYPE": "VARCHAR(200)", "ORDINAL": "3"},
    {"TABLE_NAME": "PRODUCT", "COLUMN_NAME": "PRD_CLS_CD",   "DATA_TYPE": "VARCHAR(30)",  "ORDINAL": "4"},
    {"TABLE_NAME": "PRODUCT", "COLUMN_NAME": "PRD_CTGR_CD",  "DATA_TYPE": "VARCHAR(30)",  "ORDINAL": "5"},
]

# ---------------------------------------------------------------------------
# code_mapping 스냅샷 - 별도로 관리되는 코드 변환 규칙. AI Mapping이 생성하지 않고, 컬럼 매핑을 추천할 때
# (예: 값 목록이 코드성인지 판단하는 참고 자료로) 참고만 할 수 있다.
# ---------------------------------------------------------------------------
_PRD_CLS_CD = {"자동차": "AUTO", "장기": "LONG_TERM", "일반": "GENERAL"}
_PRD_CTGR_CD = {
    "건강": "HEALTH", "기타": "ETC", "레저": "LEISURE", "배상책임": "LIABILITY",
    "상해": "ACCIDENT", "어린이": "CHILD", "여행": "TRAVEL",
    "연금/저축": "PENSION_SAVINGS", "운전자": "DRIVER", "자동차": "AUTO",
    "퇴직연금": "RETIREMENT_PENSION", "펫": "PET", "화재/재물": "FIRE_PROPERTY",
}
CODE_MAPPING_ROWS = (
    [{"SOURCE_SYSTEM": "상품마스터", "SOURCE_COLUMN": "PRODUCT_CLASS", "TARGET_CODE_GROUP_ID": "PRD_CLS_CD",
      "SOURCE_CODE": s, "TARGET_CODE": t, "MAPPING_STATUS": "APPROVED"} for s, t in _PRD_CLS_CD.items()]
    + [{"SOURCE_SYSTEM": "상품마스터", "SOURCE_COLUMN": "STD_CATEGORY", "TARGET_CODE_GROUP_ID": "PRD_CTGR_CD",
        "SOURCE_CODE": s, "TARGET_CODE": t, "MAPPING_STATUS": "APPROVED"} for s, t in _PRD_CTGR_CD.items()]
)

# ---------------------------------------------------------------------------
# AI Mapping의 책임 범위: Source Column -> Target Column 추천만. TARGET_MODEL_ROWS(어떤 Target 컬럼이
# 있는지)와 CODE_MAPPING_ROWS(값이 코드 변환 대상인지)를 참고했을 수는 있지만, 그 둘을 만드는 것은
# AI Mapping이 아니다. PK인 PRD_ID는 어떤 Source 컬럼과도 매핑되지 않으므로 여기 없다 -
# "기존 PRD_CD 기준으로 기존 ID 유지/신규 발급"은 컬럼 매핑이 아니라 Execution 단계(load_master_data)의
# 로직이다. CONFIDENCE/REASON 같은 추천 메타데이터는 실제 알고리즘이 붙는 자리로 남겨 둔다
# (지금은 알고리즘이 없어 값 없이 둔다) - Execution은 이 필드들을 읽지 않는다.
# ---------------------------------------------------------------------------
AI_MAPPING_RECOMMENDATIONS = [
    {"SOURCE_COLUMN": "STD_PRODUCT_CODE", "TARGET_COLUMN": "PRD_CD",      "CONFIDENCE": None, "REASON": None},
    {"SOURCE_COLUMN": "STD_PRODUCT_NAME", "TARGET_COLUMN": "PRD_NM",      "CONFIDENCE": None, "REASON": None},
    {"SOURCE_COLUMN": "PRODUCT_CLASS",    "TARGET_COLUMN": "PRD_CLS_CD",  "CONFIDENCE": None, "REASON": None},
    {"SOURCE_COLUMN": "STD_CATEGORY",     "TARGET_COLUMN": "PRD_CTGR_CD", "CONFIDENCE": None, "REASON": None},
]

# 컬럼 매핑별로 Execution이 실제로 써야 하는 변환 방식(RENAME/COPY vs CODE/LOOKUP)은 AI Mapping의 추천
# 항목이 아니라, "사용자 승인 -> Mapping Definition 확정" 단계에서 정해지는 정보다. 지금은 그 확정 단계도
# 사람이 대신하므로 여기서 함께 하드코딩하지만, 실제로는 승인 UI에서 사용자가 각 추천 행에 대해 정하는 값이다.
_CONFIRMED_TRANSFORM = {
    "PRD_CD": ("RENAME", "COPY"),
    "PRD_NM": ("RENAME", "COPY"),
    "PRD_CLS_CD": ("CODE", "LOOKUP"),
    "PRD_CTGR_CD": ("CODE", "LOOKUP"),
}
_CONFIRMED_DATATYPE = {c["COLUMN_NAME"]: c["DATA_TYPE"] for c in TARGET_MODEL_ROWS}

# ---------------------------------------------------------------------------
# Mapping Definition 확정본 - MappingEngine(Execution)이 실제로 읽는 것은 이것뿐이다.
# AI_MAPPING_RECOMMENDATIONS 4건에 "사용자 승인"을 가정해 Execution 필드(SOURCE_SYSTEM/MAPPING_TYPE/
# PROCESS_TYPE/VERSION/REVIEW_STATUS/FINAL_MIGRATION_APPLY_YN/TARGET_LOAD_ORDER)를 채워 만든다.
# PRD_ID 행(MAP-PRD-001)은 AI Mapping 추천에서 온 게 아니라 여기서 직접 추가한다 - PK 채번은 Source 컬럼과
# 매핑되는 대상이 아니므로 AI_MAPPING_RECOMMENDATIONS에 없고, Execution이 "이 컬럼은 GENERATE_ID라 run()
# 에서 보류하고 load_master_data()로 채번해야 함"을 알 수 있도록 정의만 여기 존재한다. SOURCE_COLUMN은
# 실제로 값을 읽어오는 데 쓰이지 않지만(_validate가 "Silver 입력에 있는 컬럼인지"만 검사하므로), 채번 기준
# 자연키가 무엇인지 문서화하는 의미로 STD_PRODUCT_CODE를 넣는다.
# ---------------------------------------------------------------------------
MAPPING_DEFINITION_ROWS = [
    {"MAPPING_ID": "MAP-PRD-001", "SOURCE_SYSTEM": "PRODUCT_MASTER", "SOURCE_COLUMN": "STD_PRODUCT_CODE",
     "TARGET_TABLE": "PRODUCT", "TARGET_COLUMN": "PRD_ID", "TARGET_DATATYPE": _CONFIRMED_DATATYPE["PRD_ID"],
     "MAPPING_TYPE": "DERIVED", "PROCESS_TYPE": "GENERATE_ID", "VERSION": "1.0",
     "REVIEW_STATUS": "APPROVED", "FINAL_MIGRATION_APPLY_YN": "Y", "TARGET_LOAD_ORDER": "10"},
] + [
    {"MAPPING_ID": f"MAP-PRD-{i:03d}", "SOURCE_SYSTEM": "PRODUCT_MASTER",
     "SOURCE_COLUMN": rec["SOURCE_COLUMN"], "TARGET_TABLE": "PRODUCT", "TARGET_COLUMN": rec["TARGET_COLUMN"],
     "TARGET_DATATYPE": _CONFIRMED_DATATYPE[rec["TARGET_COLUMN"]],
     "MAPPING_TYPE": _CONFIRMED_TRANSFORM[rec["TARGET_COLUMN"]][0],
     "PROCESS_TYPE": _CONFIRMED_TRANSFORM[rec["TARGET_COLUMN"]][1],
     "VERSION": "1.0", "REVIEW_STATUS": "APPROVED", "FINAL_MIGRATION_APPLY_YN": "Y", "TARGET_LOAD_ORDER": "10"}
    for i, rec in enumerate(AI_MAPPING_RECOMMENDATIONS, start=2)
]