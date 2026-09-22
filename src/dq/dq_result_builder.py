import json
from datetime import datetime
from typing import List, Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from pyspark.sql import functions as F

import dq_config


class DQResultBuilder:
    """
    DQ 검사 결과 데이터(List[Dict])를 전달받아 다양한 형태의 리포트, DataFrame, 
    요약 통계 및 알림(Alert) 데이터 구조로 변환하는 Builder 클래스입니다.
    """

    def __init__(self, spark: SparkSession, results: List[Dict[str, Any]]):
        self.spark = spark
        self.results = results

    def to_dataframe(self, schema: Optional[StructType] = None) -> DataFrame:
        """
        DQ 검사 결과 리스트를 Spark DataFrame으로 변환합니다.
        """
        if not self.results:
            target_schema = schema or dq_config.DQ_RESULT_SCHEMA
            return self.spark.createDataFrame([], schema=target_schema)

        target_schema = schema or dq_config.DQ_RESULT_SCHEMA
        return self.spark.createDataFrame(self.results, schema=target_schema)

    def get_summary_metrics(self) -> Dict[str, Any]:
        """
        실행된 전체 DQ 결과를 바탕으로 요약 통계 지표를 계산합니다.
        """
        if not self.results:
            return {
                "total_rules_executed": 0,
                "pass_count": 0,
                "fail_count": 0,
                "critical_fail_count": 0,
                "high_fail_count": 0,
                "review_count": 0,
                "auto_cleansed_record_count": 0,
                "unresolved_record_count": 0,
                "overall_status": "PASS",
                "execution_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }

        total_rules = len(self.results)
        pass_count = sum(1 for r in self.results if r["result_status"] == "PASS")
        fail_count = sum(1 for r in self.results if r["result_status"] == "FAIL")
        
        critical_fail_count = sum(
            1 for r in self.results 
            if r["result_status"] == "FAIL" and r["error_grade"] == "CRITICAL"
        )
        high_fail_count = sum(
            1 for r in self.results 
            if r["result_status"] == "FAIL" and r["error_grade"] == "HIGH"
        )
        review_count = sum(
            1 for r in self.results 
            if r["action_type"] == "REVIEW" or r["error_grade"] == "REVIEW"
        )

        # DQ 기준서 8장: 자동 Cleansing + 재-DQ PASS로 정상화된 건은 미해결(UNRESOLVED)이 아니다.
        auto_cleansed_record_count = sum(r.get("auto_cleansed_record_count") or 0 for r in self.results)
        unresolved_record_count = sum(r.get("unresolved_record_count") or 0 for r in self.results)

        # CRITICAL/HIGH 결함 중 "정제 후에도 미해결(격리) 건이 남은" Rule이 있을 때만 BLOCK 판정
        # (Rule은 FAIL이어도 오류 건이 전부 자동 Cleansing으로 정상화됐다면 BLOCK하지 않는다)
        unresolved_blocking = sum(
            1 for r in self.results
            if r["result_status"] == "FAIL" and r["error_grade"] in ("CRITICAL", "HIGH")
            and self._is_unresolved(r)
        )
        overall_status = "BLOCK" if unresolved_blocking > 0 else "PASS"

        return {
            "total_rules_executed": total_rules,
            "pass_count": pass_count,
            "fail_count": fail_count,
            "critical_fail_count": critical_fail_count,
            "high_fail_count": high_fail_count,
            "review_count": review_count,
            "auto_cleansed_record_count": auto_cleansed_record_count,
            "unresolved_record_count": unresolved_record_count,
            "overall_status": overall_status,
            "execution_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

    @staticmethod
    def _is_unresolved(r: Dict[str, Any]) -> bool:
        """FAIL Rule에서 정제 후에도 미해결(격리)로 남은 건이 있는지. detail 집계가 없으면(None) 안전하게 미해결로 본다."""
        q = r.get("unresolved_record_count")
        return q is None or q > 0

    def build_summary_dataframe(self) -> DataFrame:
        """
        요약 지표를 단일 행의 Spark DataFrame 형태로 반환합니다. (대시보드 적재용)
        """
        summary = self.get_summary_metrics()
        return self.spark.createDataFrame([summary])

    def build_failure_report(self) -> List[Dict[str, Any]]:
        """
        실패(FAIL)하거나 검토(REVIEW)가 필요한 건들만 추출하여 리포트로 생성합니다.
        """
        failed_items = [
            {
                "rule_id": r["rule_id"],
                "rule_name": r.get("rule_name", ""),
                "target_table": r["target_table"],
                "target_column": r["target_column"],
                "error_grade": r["error_grade"],
                "action_type": r["action_type"],
                "check_count": r["check_count"],
                "error_count": r["error_count"],
                "error_rate_pct": f"{round(r['error_rate'] * 100, 2)}%",
                "threshold_rate_pct": f"{round(r['threshold_rate'] * 100, 2)}%",
                "auto_cleansed_count": r.get("auto_cleansed_record_count") or 0,
                "unresolved_count": r.get("unresolved_record_count") or 0,
                "dq_reason": r.get("dq_reason", "")
            }
            for r in self.results
            if r["result_status"] == "FAIL" or r["action_type"] in ["BLOCK", "WARN", "REVIEW"]
        ]
        return failed_items

    def build_slack_alert_payload(self, pipeline_name: str = "MAPS Data Pipeline") -> Dict[str, Any]:
        """
        Slack/TeamsWebhook 발송용 메시지 JSON 객체를 작성합니다.
        """
        summary = self.get_summary_metrics()
        failed_reports = self.build_failure_report()

        status_emoji = "🚨" if summary["overall_status"] == "BLOCK" else "⚠️" if summary["fail_count"] > 0 else "✅"

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{status_emoji} [{pipeline_name}] DQ 검사 리포트 - {summary['overall_status']}"
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*총 검사 룰:* {summary['total_rules_executed']}개"},
                    {"type": "mrkdwn", "text": f"*성공/실패:* {summary['pass_count']} / {summary['fail_count']}개"},
                    {"type": "mrkdwn", "text": f"*CRITICAL 결함:* {summary['critical_fail_count']}개"},
                    {"type": "mrkdwn", "text": f"*HIGH 결함:* {summary['high_fail_count']}개"},
                    {"type": "mrkdwn", "text": f"*자동 Cleansing 정상화:* {summary['auto_cleansed_record_count']}건"},
                    {"type": "mrkdwn", "text": f"*미해결(격리):* {summary['unresolved_record_count']}건"},
                ]
            }
        ]

        if failed_reports:
            fail_text_lines = []
            for item in failed_reports[:5]:  # 메시지 길이 제한을 위해 최대 5개 노출
                fail_text_lines.append(
                    f"• *[{item['error_grade']}] {item['rule_id']}*: {item['rule_name']} "
                    f"({item['target_column']}) -> 오류율 {item['error_rate_pct']} (건수: {item['error_count']}건, "
                    f"자동정제 {item['auto_cleansed_count']} / 미해결 {item['unresolved_count']})"
                )
            
            if len(failed_reports) > 5:
                fail_text_lines.append(f"...외 {len(failed_reports) - 5}건의 결함 추가 발생")

            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*[주요 결함 항목 목록]*\n" + "\n".join(fail_text_lines)
                }
            })

        return {"text": f"[{pipeline_name}] DQ 검사 결과: {summary['overall_status']}", "blocks": blocks}

    def print_console_report(self) -> None:
        """
        콘솔/노트북 실행창에 가독성 좋은 텍스트 리포트를 출력합니다.
        """
        summary = self.get_summary_metrics()
        print("=" * 80)
        print(f" MAPS DATA QUALITY EXECUTION REPORT ({summary['execution_time']})")
        print("=" * 80)
        print(f" Total Rules Executed : {summary['total_rules_executed']}")
        print(f" Passed               : {summary['pass_count']}")
        print(f" Failed               : {summary['fail_count']}")
        print(f" Critical / High Fail : {summary['critical_fail_count']} / {summary['high_fail_count']}")
        print(f" REVIEW-grade Rules   : {summary['review_count']}")
        print(f" Auto Cleansed / Unresolved : {summary['auto_cleansed_record_count']} / {summary['unresolved_record_count']}")
        print(f" Final Action Status  : {summary['overall_status']}")
        print("-" * 80)

        failures = self.build_failure_report()
        if failures:
            print(" [DETAILED FAILURES]")
            for f in failures:
                print(f" [{f['error_grade']}] {f['rule_id']} | {f['rule_name']}")
                print(f"   - Target Column : {f['target_column']}")
                print(f"   - Error Count   : {f['error_count']} / {f['check_count']} (Rate: {f['error_rate_pct']})")
                print(f"   - Threshold     : {f['threshold_rate_pct']}")
                print(f"   - Cleansing     : auto {f['auto_cleansed_count']} / unresolved {f['unresolved_count']}")
                print(f"   - DQ Reason     : {f['dq_reason']}")
                print(f"   - Action        : {f['action_type']}")
                print("-" * 40)
        else:
            print(" All Data Quality Rules Passed Successfully!")
        print("=" * 80)