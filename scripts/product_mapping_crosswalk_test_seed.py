"""
PRODUCT_MAPPING(target_model.csv에 이미 정의된 크로스워크: SRC_SYS/SRC_PRD_CD/TGT_PRD_ID/APRV_YN 등)
테스트용 최소 시드.

★★★ 실제 AI Mapping/사람 승인 데이터가 아니다 ★★★ (AI Mapping 알고리즘은 이번 작업 범위 밖)
mapping_engine.py의 CODE_MAPPING_RULE=="PRODUCT_MAPPING" 분기(run()의 FK 사전 조인)가 정상 동작하는지
검증하기 위한 최소 시드일 뿐, SRC_PRD_CD -> TGT_PRD_ID 대응 자체의 업무적 정확성은 보장하지 않는다.

TGT_PRD_ID는 임의로 만든 값이 아니다: 실제 MappingEngine.load_master_data()를 표준상품마스터
189건(MAPS_TO-BE_표준_상품마스터_v1_1.csv) 전체에 대해 그대로 실행해서 나온 진짜 PRD_ID다(결정적 채번 -
PRD_CD 알파벳순, 같은 CSV로 다시 돌려도 같은 값이 나온다):
    ACC-001 -> PRD-000001, AUTO-001 -> PRD-000030, TRV-001 -> PRD-000181
(compute_real_product_ids.py로 재현 가능)

SRC_PRD_CD 값은 mapping_definition.csv의 실제 SAMPLE_VALUE를 그대로 썼다(LONG-ACC-001/TRAVEL-03/
AUTO-PERS-002) - 이 값들은 실제 표준상품마스터의 STD_PRODUCT_CODE와 문자 그대로 일치하지 않는다(확인됨,
채널마다 다른 코드 체계를 쓴다는 걸 보여주는 사례). 그래서 "LONG-ACC-001은 실제로 ACC-001(상해)이다" 같은
업무적 대응은 이 시드에서 정하지 않았고, 단지 엔진이 SRC_SYS+SRC_PRD_CD로 걸러서 APRV_YN='Y'인 TGT_PRD_ID를
정확히 조회해오는지(배관)만 검증할 수 있게 실존하는 PRD_ID 중 하나를 임시로 붙여 둔 것이다.

MAP-SAL-INSURANCECON-004(SALESFORCE)는 mapping_definition.csv의 SAMPLE_VALUE 자체가 비어 있어 시드를
만들지 못했다 - 실제 값 확인 필요.
"""

PRODUCT_MAPPING_ROWS = [
    {"PRD_MAP_ID": "PM-TEST-001", "SRC_SYS": "INBOUND", "SRC_PRD_CD": "LONG-ACC-001",
     "SRC_PRD_NM": "(테스트) 장기상해 원천 상품명", "TGT_PRD_ID": "PRD-000001",   # ACC-001의 실제 PRD_ID
     "MAP_ST_CD": "TEST", "CNFD_SCR": None, "APRV_YN": "Y", "APRV_BY": "TEST_SEED", "APRV_DTM": None},
    {"PRD_MAP_ID": "PM-TEST-002", "SRC_SYS": "OUTBOUND", "SRC_PRD_CD": "TRAVEL-03",
     "SRC_PRD_NM": "(테스트) 여행 원천 상품명", "TGT_PRD_ID": "PRD-000181",   # TRV-001의 실제 PRD_ID
     "MAP_ST_CD": "TEST", "CNFD_SCR": None, "APRV_YN": "Y", "APRV_BY": "TEST_SEED", "APRV_DTM": None},
    {"PRD_MAP_ID": "PM-TEST-003", "SRC_SYS": "HOMEPAGE", "SRC_PRD_CD": "AUTO-PERS-002",
     "SRC_PRD_NM": "(테스트) 개인용자동차 원천 상품명", "TGT_PRD_ID": "PRD-000030",   # AUTO-001의 실제 PRD_ID
     "MAP_ST_CD": "TEST", "CNFD_SCR": None, "APRV_YN": "Y", "APRV_BY": "TEST_SEED", "APRV_DTM": None},
]
