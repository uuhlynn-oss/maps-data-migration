"""
Gold Target Validation 설정.

VR-COUNSEL-001~024 (gold_validation_rule_counsel_v0_1.xlsx, IMPLEMENTABLE_NOW=Y 18개) 중
13개(SCHEMA/NOT_NULL/UNIQUE/LENGTH/TYPE/DOMAIN)는 meta.target_model에서 규칙을 매번 다시 만들어낸다
(모델이 바뀌면 규칙도 같이 바뀐다). 코드로 남기는 것은 두 가지뿐이다.
    BUSINESS_RULES   - 컬럼 간 업무 규칙 (모델만으로는 알 수 없음, VR-COUNSEL-022)
    PK_DEDUP_RULES   - 승인된 보정 규칙 (Mapping 평가 기준서 Review R3)
정책 규칙(VR-COUNSEL-023, 미매핑 코드는 NULL+기록)과 스키마 일치(VR-COUNSEL-001)는 Mapping Engine이
이미 만족시키므로 여기서 다시 검사하지 않는다. 건수 대사(VR-COUNSEL-024)는 러너의 summary에서 계산한다.
"""

UC_CATALOG = "maps_databricks"
META_SCHEMA = "meta"
GOLD_SCHEMA = "gold"                 # 확정 Gold Target 테이블
GOLD_QUARANTINE_SCHEMA = "gold_quarantine"
GOLD_CANDIDATE_SCHEMA = "gold_candidate"    # mapping_config.gold_candidate_table()과 같은 물리 테이블을 가리킨다 (임시 뷰 아님)

TARGET_MODEL_TABLE = f"{UC_CATALOG}.{META_SCHEMA}.target_model"
MIGRATION_TRACE_TABLE = f"{UC_CATALOG}.{GOLD_SCHEMA}.migration_trace"


def gold_table(target_table: str) -> str:
    return f"{UC_CATALOG}.{GOLD_SCHEMA}.{target_table.lower()}"


def gold_quarantine_table(target_table: str) -> str:
    return f"{UC_CATALOG}.{GOLD_QUARANTINE_SCHEMA}.{target_table.lower()}"


def gold_candidate_table(target_table: str) -> str:
    """Mapping Engine이 저장하는 물리 테이블 (mapping_config.gold_candidate_table과 동일한 이름 규칙).
    노트북이 분리돼 있어도 이 테이블을 통해 매핑 결과가 이어진다 (예전의 세션 한정 임시 뷰를 대체)."""
    return f"{UC_CATALOG}.{GOLD_CANDIDATE_SCHEMA}.{target_table.lower()}"


# ---------------------------------------------------------------------------
# 컬럼 간 업무 규칙 (모델의 NULLABLE/KEY/DATA_TYPE만으로는 도출되지 않는 것)
# VR-COUNSEL-022: DQ 기준서 DQ-CON-INB-001 등 (시작 ≤ 종료, 0% ERROR) 재사용. 저장이 UTC라 소스가 달라도 같은 기준.
# ---------------------------------------------------------------------------
BUSINESS_RULES = {
    "COUNSEL": [
        {"rule_id": "VR-COUNSEL-022", "type": "ORDER", "columns": ["STRT_DTM", "END_DTM"],
         "description": "STRT_DTM ≤ END_DTM (둘 중 하나가 NULL이면 검사하지 않음)"},
    ],
}

# ---------------------------------------------------------------------------
# 승인된 보정 규칙 (Mapping 평가 기준서 Review R3)
# "Outbound CTI_ID: 의도적 중복 테스트 가능성. DQ로 유일성 선검증; 유일하면 사용, 중복이면 Target CNSL_ID 생성"
# DQ의 아웃바운드 중복 검사(DQ-UNI-OUT-001)는 LEAD_MGMT_NO+TRY_CNT만 보고 CTI_ID 자체의 유일성은 보지 않으므로
# (DQ-UNI-OUT-002 미구현), Gold 쪽에서 중복이 그대로 들어올 수 있다 - 이 규칙이 그 간극을 메운다.
# 다른 소스(인바운드 consultation_id, Salesforce Consultation_No__c)는 DQ의 CRITICAL 유일성 검사(0%, BLOCK)가
# Silver 진입 전에 이미 걸러내므로 R3의 대상이 아니다. 적용 범위를 OUTBOUND로 한정해 그 경계를 지킨다.
# ---------------------------------------------------------------------------
PK_DEDUP_RULES = {
    "COUNSEL": {
        "pk_column": "CNSL_ID",
        "applies_to_source": "outbound",     # 이 소스에서 온 중복만 보정한다. 그 외 소스의 PK 중복은 격리한다(정의되지 않은 상황).
        "order_by": "_source_record_key",    # 재실행해도 같은 순서가 되도록 결정적 기준으로 정렬
        "suffix_format": "-DUP{n}",          # 그룹의 2번째 이후 행에 붙인다 (1번째는 원래 값 유지)
        "rule_id": "VR-COUNSEL-003-DEDUP",
    },
}
