# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "fad801d9-a180-4679-b139-c0aa4ab7c2be",
# META       "default_lakehouse_name": "LH_MAPS_Bronze",
# META       "default_lakehouse_workspace_id": "61328685-0422-4aff-b239-d862b0549a96",
# META       "known_lakehouses": [
# META         {
# META           "id": "fad801d9-a180-4679-b139-c0aa4ab7c2be"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "aed47628-e38b-4e7c-86fb-381fc91635dc",
# META       "known_warehouses": [
# META         {
# META           "id": "aed47628-e38b-4e7c-86fb-381fc91635dc",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import sys

# Notebook의 builtin 리소스 폴더를 Python import 경로에 추가
from builtin.src.ingest.ingest import ingest_to_bronze



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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM LH_MAPS_Bronze.dbo.bronze_homepage LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Starting Bronze Load Job...")

for source_name, config in INGEST_TARGETS.items():
    ingest_to_bronze(spark, notebookutils, source_name, config)

print("Bronze ingestion completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
