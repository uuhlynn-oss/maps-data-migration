import sys
import os

# Project root(maps/)를 sys.path에 등록 — src 패키지 import 지원
_project_root = next((p for p in sys.path if os.path.isdir(os.path.join(p, "src"))), None)
if _project_root is None:
    _d = os.path.dirname(os.path.abspath(__file__)) if "__file__" in dir() else os.getcwd()
    while _d != "/" and not os.path.isdir(os.path.join(_d, "src")):
        _d = os.path.dirname(_d)
    if os.path.isdir(os.path.join(_d, "src")):
        _project_root = _d
if _project_root and _project_root not in sys.path:
    sys.path.insert(0, _project_root)

# 중앙 settings 모듈에서 설정값 Import
from src.config.settings import (
    UC_CATALOG,
    BRONZE_SCHEMA,
    PROFILING_OUTPUT_BASE_PATH,  # settings.py의 프로파일링 출력 경로 사용
)

# 카탈로그 및 경로 설정 중앙화
BRONZE_CATALOG = UC_CATALOG
BRONZE_SCHEMA = BRONZE_SCHEMA

# ============================================================
# Code Master Loader Helper
# ============================================================

def load_code_master_from_uc(spark, table_name: str, group_col="CODE_GROUP_ID", val_col="CODE_VALUE") -> dict:
    """Unity Catalog에 적재된 코드마스터 테이블 읽기 (동적 Catalog/Schema 참조)"""
    try:
        df = spark.table(f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.{table_name}")
        rows = df.collect()
        master = {}
        for r in rows:
            master.setdefault(r[group_col], set()).add(r[val_col])
        return master
    except Exception as e:
        print(f"[경고] 코드마스터 {table_name} 읽기 실패 ({e})")
        return {}

# ============================================================
# Source Profiling Configurations
# ============================================================

SOURCE_CONFIG = {
    "inbound": {
        "key_columns": ["consultation_id"],
        "column_roles": {
            "consultation_id": "BUSINESS_KEY",
            "inbound_type": "CODE",
            "ars_menu": "CODE",
            "consultation_type": "CODE",
            "started_at": "DATE_DATETIME",
            "ended_at": "DATE_DATETIME",
            "status": "CODE",
            "customer_id": "GENERAL",
            "customer_name": "TEXT",
            "birth_date": "DATE_DATETIME",
            "gender": "CODE",
            "phone_number": "PHONE",
            "address": "TEXT",
            "policy_id": "GENERAL",
            "policy_status": "CODE",
            "contract_date": "DATE_DATETIME",
            "expiration_date": "DATE_DATETIME",
            "product_code": "CODE",
            "agent_id": "GENERAL",
            "agent_name": "TEXT",
            "team_name": "CODE",
            "consultation_content": "TEXT",
        },
        "date_formats": {
            "started_at": ["yyyy-MM-dd HH:mm:ss", "yyyy.MM.dd HH:mm:ss", "yyyy/MM/dd HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss"],
            "ended_at": ["yyyy-MM-dd HH:mm:ss", "yyyy.MM.dd HH:mm:ss", "yyyy/MM/dd HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss"],
            "birth_date": ["yyyy-MM-dd", "yyyy/MM/dd", "yyyy.MM.dd"],
            "contract_date": ["yyyy-MM-dd", "yyyy/MM/dd", "yyyy.MM.dd"],
            "expiration_date": ["yyyy-MM-dd", "yyyy/MM/dd", "yyyy.MM.dd"],
        },
        "expected_patterns": {
            "inbound_type": r"^INB_[A-Za-z0-9_]+$",
            "ars_menu": r"^ARS_[A-Za-z0-9_]+$",
            "consultation_type": r"^CON_[A-Za-z0-9_]+$",
            "status": r"^CST_[A-Za-z0-9_]+$",
            "gender": r"^[MF]$",
            "policy_status": r"^POL_[A-Za-z0-9_]+$",
            "team_name": r"^TEAM_[A-Za-z0-9_]+$",
            "product_code": r"^[A-Z]+-[A-Z]+-[0-9]+$",
        },
        "semantic_variant_groups": [{"columns": ["status"], "values": ["보류", "PENDING"]}],
        "code_master_loader": lambda spark, dbutils: load_code_master_from_uc(spark, "inbound_code_master"),
        "cross_column_rules": [
            {"columns": ["started_at", "ended_at"]},
            {"columns": ["contract_date", "expiration_date"]},
        ],
    },
    "outbound": {
        "key_columns": ["CTI_ID"],
        "column_roles": {
            "CUST_NM": "TEXT", "BRTH_YMD": "DATE_DATETIME", "SX_DV_CD": "CODE",
            "HP_NO": "PHONE", "LEAD_MGMT_NO": "GENERAL", "DB_ACQ_PATH_CD": "CODE",
            "DB_ACQ_DTM": "DATE_DATETIME", "DB_ST_CD": "CODE", "CMPGN_CD": "CODE",
            "GD_CD": "CODE", "GD_NM": "TEXT", "CTI_ID": "BUSINESS_KEY",
            "TMR_ID": "GENERAL", "TRY_CNT": "GENERAL", "CALL_ST_DTM": "DATE_DATETIME",
            "CALL_CONN_DTM": "DATE_DATETIME", "CALL_END_DTM": "DATE_DATETIME",
            "CONN_RSLT_CD": "CODE", "TALK_TM": "GENERAL", "TM_RSLT_CD": "CODE",
            "CSLT_TP_CD": "CODE", "RJT_RSN_CD": "CODE", "CSLT_NOTE": "TEXT",
            "R_CALL_RSV_DTM": "DATE_DATETIME", "STT_TXT": "TEXT", "REC_URI": "TEXT",
            "MKT_AGR_YN": "CODE", "REC_AGR_YN": "CODE", "DNC_YN": "CODE",
            "APPL_NO": "GENERAL", "CNTR_PRGS_CD": "CODE", "MN_PREM": "GENERAL",
        },
        "date_formats": {
            "BRTH_YMD": ["yyyyMMdd"], "DB_ACQ_DTM": ["yyyy-MM-dd HH:mm:ss"],
            "CALL_ST_DTM": ["yyyy-MM-dd HH:mm:ss"], "CALL_CONN_DTM": ["yyyy-MM-dd HH:mm:ss"],
            "CALL_END_DTM": ["yyyy-MM-dd HH:mm:ss"], "R_CALL_RSV_DTM": ["yyyy-MM-dd HH:mm:ss"],
        },
        "expected_patterns": {
            "SX_DV_CD": r"^[12]$", "DB_ACQ_PATH_CD": r"^A[0-9]{2}$", "DB_ST_CD": r"^D[0-9]{2}$",
            "CMPGN_CD": r"^CMPG-[A-Z0-9-]+$", "GD_CD": r"^[A-Z]+-[0-9]{2}$",
            "CONN_RSLT_CD": r"^C[0-9]{2}$", "TM_RSLT_CD": r"^R[0-9]{2}$",
            "CSLT_TP_CD": r"^Q[0-9]{2}$", "RJT_RSN_CD": r"^Q[0-9]{2}$",
            "MKT_AGR_YN": r"^[YN]$", "REC_AGR_YN": r"^[YN]$", "DNC_YN": r"^[YN]$",
            "CNTR_PRGS_CD": r"^S[0-9]{2}$",
        },
        "semantic_variant_groups": [],
        "code_master_loader": lambda spark, dbutils: load_code_master_from_uc(spark, "outbound_code_master"),
        "cross_column_rules": [{"columns": ["CALL_ST_DTM", "CALL_CONN_DTM", "CALL_END_DTM"]}],
    },
    "salesforce": {
        "key_columns": ["Consultation_No__c"],
        "column_roles": {
            "Id": "TECHNICAL_PK", "Consultation_No__c": "BUSINESS_KEY", "Product_Code__c": "CODE",
            "Inbound_Product_Code__c": "CODE", "Inbound_Product_Match_Status__c": "CODE",
            "Customer_Type__c": "CODE", "Customer_Id__c": "GENERAL", "Consult_Category__c": "CODE",
            "Consult_Content__c": "TEXT", "Access_Path__c": "CODE", "Language_Code__c": "CODE",
            "Device_Type__c": "CODE", "Referrer_Channel__c": "CODE", "Session_Id__c": "GENERAL",
            "Country_Code__c": "CODE", "Process_Status__c": "CODE", "Consent_Yn__c": "CODE",
            "Consult_Datetime__c": "DATE_DATETIME", "CreatedDate": "DATE_DATETIME",
            "LastModifiedDate": "DATE_DATETIME",
        },
        "date_formats": {
            "Consult_Datetime__c": ["yyyy-MM-dd HH:mm:ss"],
            "CreatedDate": ["yyyy-MM-dd HH:mm:ss"],
            "LastModifiedDate": ["yyyy-MM-dd HH:mm:ss"],
        },
        "expected_patterns": {"Access_Path__c": r"^/[A-Za-z0-9/_-]*$"},
        "semantic_variant_groups": [],
        "code_master_loader": lambda spark, dbutils: load_code_master_from_uc(spark, "salesforce_code_master"),
        "cross_column_rules": [{"columns": ["CreatedDate", "LastModifiedDate"]}],
    },
    "homepage": {
        "key_columns": ["COMPLAINT_ID"],
        "column_roles": {
            "COMPLAINT_ID": "BUSINESS_KEY", "CUST_ID": "GENERAL", "PRODUCT_GB": "CODE",
            "INBOUND_PRODUCT_CODE": "CODE", "PRODUCT_CODE_MATCH_STATUS": "CODE",
            "COMPLAINT_TP": "CODE", "COMPLAINT_TITLE": "TEXT", "COMPLAINT_CONTENT": "TEXT",
            "LANG_CD": "CODE", "RCV_PATH": "CODE", "FSS_RELATED_YN": "CODE",
            "FSS_CASE_NO": "TEXT", "SENTIMENT_REF": "CODE", "PROCESS_ST": "CODE",
            "REG_DT": "DATE_DATETIME", "ANSWER_DUE_DT": "DATE_DATETIME",
            "CONTACT_EMAIL": "EMAIL", "CONTACT_TEL": "PHONE",
        },
        "date_formats": {"REG_DT": ["yyyy/MM/dd HH:mm"], "ANSWER_DUE_DT": ["yyyy/MM/dd"]},
        "expected_patterns": {
            "INBOUND_PRODUCT_CODE": r"^[A-Z]+-[A-Z]+-[0-9]+$",
            "PRODUCT_CODE_MATCH_STATUS": r"^(MATCHED|UNMAPPED_NO_EQUIVALENT|DQ_NULL|DQ_INVALID_CODE|DQ_FORMAT)$",
            "LANG_CD": r"^[A-Z]{2}$", "FSS_RELATED_YN": r"^[YN]$",
        },
        "semantic_variant_groups": [],
        "code_master_loader": lambda spark, dbutils: load_code_master_from_uc(spark, "homepage_code_master"),
        "cross_column_rules": [{"columns": ["REG_DT", "ANSWER_DUE_DT"]}],
    },
    "chatbot": {
        "key_columns": ["session_id"],
        "column_roles": {
            "session_id": "BUSINESS_KEY", "customer_id": "GENERAL", "channel": "CODE",
            "device_type": "CODE", "os": "CODE", "app_version": "GENERAL",
            "started_at": "DATE_DATETIME", "ended_at": "DATE_DATETIME", "duration_seconds": "GENERAL",
            "entry_login_status": "CODE", "is_logged_in": "CODE", "consent_screen_shown": "CODE",
            "consent_privacy_accepted": "CODE", "consent_marketing_accepted": "CODE",
            "consent_thirdparty_accepted": "CODE", "consent_answered_at": "DATE_DATETIME",
            "product_interest": "GENERAL", "login_gate_hit": "CODE", "mobile_only_redirect": "CODE",
            "external_site_redirect": "CODE", "accident_claim": "GENERAL",
            "contract_change_request": "GENERAL", "benefit_event_interaction": "GENERAL",
            "resolution": "GENERAL", "intent_category_guess": "CODE", "nav_step": "GENERAL",
            "nav_menu_level": "CODE", "nav_menu_name": "TEXT", "nav_full_path": "TEXT",
            "nav_timestamp": "DATE_DATETIME", "user_query": "TEXT", "query_matched": "CODE",
            "query_matched_items": "GENERAL", "query_timestamp": "DATE_DATETIME",
        },
        "date_formats": {
            "started_at": ["yyyy-MM-dd'T'HH:mm:ssXXX", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"],
            "ended_at": ["yyyy-MM-dd'T'HH:mm:ssXXX", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"],
            "consent_answered_at": ["yyyy-MM-dd'T'HH:mm:ssXXX", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"],
            "nav_timestamp": ["yyyy-MM-dd'T'HH:mm:ssXXX", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"],
            "query_timestamp": ["yyyy-MM-dd'T'HH:mm:ssXXX", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"],
        },
        "expected_patterns": {
            "is_logged_in": r"^(true|false)$",
            "query_matched": r"^(true|false)$",
            "nav_menu_level": r"^(대분류|중분류|소분류|액션)$",
        },
        "semantic_variant_groups": [],
        "code_master_loader": lambda spark, dbutils: load_code_master_from_uc(spark, "chatbot_code_master"),
        "cross_column_rules": [{"columns": ["started_at", "ended_at"]}],
    }
}

def get_all_table_names() -> list:
    """설정된 전체 테이블 명단 반환"""
    return list(SOURCE_CONFIG.keys())