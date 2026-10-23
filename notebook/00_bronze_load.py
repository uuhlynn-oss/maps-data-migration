# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
import sys, os

# maps/notebook → maps (project root containing src/)
for _p in sys.path:
    _parent = os.path.dirname(_p)
    if os.path.isdir(os.path.join(_parent, "src")) and _parent not in sys.path:
        sys.path.insert(0, _parent)
        break

from src.ingest.ingest import ingest_to_bronze

INGEST_TARGETS = {
    "inbound": {
        "format": "csv",
        "raw_relative_path": "inbound/consultation",
        "columns": [
            "consultation_id", "inbound_type", "ars_menu", "consultation_type",
            "started_at", "ended_at", "status", "customer_id", "customer_name",
            "birth_date", "gender", "phone_number", "address", "policy_id",
            "policy_status", "contract_date", "expiration_date", "product_code",
            "agent_id", "agent_name", "team_name", "consultation_content"
        ]
    },
    "outbound": {
        "format": "csv",
        "raw_relative_path": "outbound/consultation",
        "columns": [
            "CUST_NM", "BRTH_YMD", "SX_DV_CD", "HP_NO", "LEAD_MGMT_NO",
            "DB_ACQ_PATH_CD", "DB_ACQ_DTM", "DB_ST_CD", "CMPGN_CD", "GD_CD",
            "GD_NM", "CTI_ID", "TMR_ID", "TRY_CNT", "CALL_ST_DTM",
            "CALL_CONN_DTM", "CALL_END_DTM", "CONN_RSLT_CD", "TALK_TM",
            "TM_RSLT_CD", "CSLT_TP_CD", "RJT_RSN_CD", "CSLT_NOTE",
            "R_CALL_RSV_DTM", "STT_TXT", "REC_URI", "MKT_AGR_YN",
            "REC_AGR_YN", "DNC_YN", "APPL_NO", "CNTR_PRGS_CD", "MN_PREM"
        ]
    },
    "salesforce": {
        "format": "csv",
        "raw_relative_path": "salesforce/consultation",
        "columns": [
            "Id", "Consultation_No__c", "Product_Code__c", "Inbound_Product_Code__c",
            "Inbound_Product_Match_Status__c", "Customer_Type__c", "Customer_Id__c",
            "Consult_Category__c", "Consult_Content__c", "Access_Path__c",
            "Language_Code__c", "Device_Type__c", "Referrer_Channel__c",
            "Session_Id__c", "Country_Code__c", "Process_Status__c",
            "Consent_Yn__c", "Consult_Datetime__c", "CreatedDate", "LastModifiedDate"
        ]
    },
    "homepage": {
        "format": "csv",
        "raw_relative_path": "homepage_complaint/consultation",
        "columns": [
            "COMPLAINT_ID", "CUST_ID", "PRODUCT_GB", "INBOUND_PRODUCT_CODE",
            "PRODUCT_CODE_MATCH_STATUS", "COMPLAINT_TP", "COMPLAINT_TITLE",
            "COMPLAINT_CONTENT", "LANG_CD", "RCV_PATH", "FSS_RELATED_YN",
            "FSS_CASE_NO", "SENTIMENT_REF", "PROCESS_ST", "REG_DT",
            "ANSWER_DUE_DT", "CONTACT_EMAIL", "CONTACT_TEL"
        ]
    },
    "chatbot": {
        "format": "json_unified",
        "raw_relative_path": "chatbot/consultation",
    }
}

# COMMAND ----------

print("Starting Bronze Load Job...")
for source_name, config in INGEST_TARGETS.items():
    ingest_to_bronze(spark, dbutils, source_name, config)
print("All Bronze Ingestion Completed Successfully.")