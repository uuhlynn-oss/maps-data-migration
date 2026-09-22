import json
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# 1. 외부 모듈 Import (경로 명시)
try:
    import src.dq.dq_config as dq_config
    import src.dq.dq_functions as dq_functions
    import src.dq.dq_cleansing as dq_cleansing
except ModuleNotFoundError:
    import dq_config
    import dq_functions
    import dq_cleansing


class DQRunner:
    """MAPS DQ Rule을 실행하고 판정 및 결과 적재를 수행합니다."""

    def __init__(self, spark: SparkSession, dbutils: Any = None):
        self.spark = spark
        self.dbutils = dbutils
        self.code_master_df: Optional[DataFrame] = None
        # dq_run_id는 run_table_dq() 호출마다(= 소스 테이블 × 실행마다) 새로 발급한다 (요청서 §7-3).
        # 여기 값은 execute_rule()을 run_table_dq() 밖에서 직접 호출할 때를 위한 기본값이다.
        self.dq_run_id = self._new_run_id()
        # DQ 기준서 8장 - 자동 Cleansing 엔진 (코드 마스터는 DQ 실행과 같은 캐시를 공유한다)
        self.cleansing = dq_cleansing.CleansingEngine(spark, self._load_code_master)

    @staticmethod
    def _new_run_id(source_system: Optional[str] = None) -> str:
        """
        DQ 실행 ID를 새로 만든다. 형식: DQ-{yyyyMMddHHmmss}-{source}-{uuid6}  (예: DQ-20260920143012-inbound-a1b2c3)
        (요청서 예시의 "DQ-YYYYMMDD-001" 같은 일련번호 대신 타임스탬프+uuid로 충돌 없이 생성 - 간단함 우선)
        source를 넣는 건 사람이 ID만 보고 어느 소스의 실행인지 알아보기 위함일 뿐, ID를 파싱해서 쓰지는 않는다.
        """
        parts = ["DQ", f"{datetime.now():%Y%m%d%H%M%S}"]
        if source_system:
            parts.append(source_system)
        parts.append(uuid.uuid4().hex[:6])
        return "-".join(parts)

    def _load_code_master(self) -> DataFrame:
        """코드 마스터 테이블을 로드합니다. (서버리스 호환을 위해 cache 제거)"""
        if self.code_master_df is None:
            self.code_master_df = (
                self.spark.read.table(dq_config.CODE_MASTER_TABLE)
                .select("CODE_GROUP", "CODE")
                .distinct()
            )
        return self.code_master_df

    def judge_result(self, rule: Dict[str, Any], check_count: int, error_count: int) -> Dict[str, Any]:
        """오류율을 계산하고 PASS / FAIL / REVIEW 상태를 판정합니다."""
        error_rate = (error_count / check_count) if check_count > 0 else 0.0
        threshold_rate = rule.get("threshold_rate", 0.0)
        configured_grade = rule.get("error_grade", "INFO")

        # REVIEW 등급 예외 처리
        if configured_grade == "REVIEW":
            if error_count > 0:
                return {"error_rate": round(error_rate, 6), "result_status": "FAIL", "error_grade": "REVIEW", "action_type": "REVIEW"}
            return {"error_rate": 0.0, "result_status": "PASS", "error_grade": "INFO", "action_type": "ALLOW"}

        # 일반 임계치 기준 판정
        if error_rate > threshold_rate:
            result_status = "FAIL"
            error_grade = configured_grade
            action_type = "BLOCK" if configured_grade in ["CRITICAL", "HIGH"] else "WARN"
        else:
            result_status = "PASS"
            error_grade = "INFO"
            action_type = "ALLOW"

        return {
            "error_rate": round(error_rate, 6),
            "result_status": result_status,
            "error_grade": error_grade,
            "action_type": action_type
        }

    def execute_rule(self, df: DataFrame, rule: Dict[str, Any], source_batch_id: str) -> Dict[str, Any]:
        """룰 타입에 맞춰 검사 함수를 호출합니다."""
        rule_type = rule["rule_type"]
        target_table = rule["target_table"]

        # target_table = "maps_databricks.bronze.inbound" 형태라 마지막 조각이 곧 소스명이다.
        # DQ_RULES에 source_system 필드를 따로 안 넣어도 되게 여기서 바로 유추한다.
        source_system = target_table.split(".")[-1]
        # dq_cleansing_detail/silver_candidate에서 이 레코드를 가리킬 업무키 컬럼.
        # 정의 안 된 테이블이면 None -> _build_detail_df가 알아서 NULL로 채운다.
        record_key_col = dq_config.TABLE_RECORD_KEY_COLUMN.get(target_table)

        if rule_type == "NULL_CHECK":
            res = dq_functions.check_null(df, rule, record_key_col)
        elif rule_type == "PATTERN_CHECK":
            res = dq_functions.check_pattern(df, rule, record_key_col)
        elif rule_type == "RANGE_CHECK":
            res = dq_functions.check_range(df, rule, record_key_col)
        elif rule_type == "ORDER_CHECK":
            res = dq_functions.check_start_end_order(df, rule, record_key_col)
        elif rule_type == "CODE_EXISTS":
            code_master = self._load_code_master()
            res = dq_functions.check_code_exists(df, rule, code_master, record_key_col)
        elif rule_type == "DUPLICATE_CHECK":
            res = dq_functions.check_duplicate(df, rule, record_key_col)
        else:
            raise ValueError(f"지원하지 않는 Rule Type입니다: {rule_type}")

        judgment = self.judge_result(rule, res["check_count"], res["error_count"])
        target_col = rule.get("column") or ", ".join(rule.get("columns", []))
        action_type = judgment["action_type"]

        # ---- 요청서 4장: dq_cleansing_detail은 "위반 이력 전체"다 ----
        # 오류가 하나라도 있으면 임계치와 무관하게(ALLOW 포함) 위반 레코드를 전부 기록한다.
        # (요청서 §1/§3.4/§9: 규칙 집계에서 행 단위 상세로 연결, 규칙별/실행 전체 고유 오류 레코드 수 산출.
        #  처리 여부는 cleansing_status / review_required_yn 으로 구분한다.)
        # 정제·격리 대상은 여전히 ALLOW가 아닌 Rule뿐이다. ALLOW 위반은 원본 유지(요청서 §6) -> NOT_REQUIRED로 기록만 한다.
        # 오류가 없는 Rule은 detail_df를 아예 안 쓴다 (detail_df 자체는 res에서 lazy하게만 만들어져 있음).
        detail_df = None
        unique_error_record_count = None
        auto_cleansed_record_count = None
        unresolved_record_count = None
        if res["error_count"] > 0 and res.get("detail_df") is not None:
            now = datetime.now()
            cleansing_cfg = dq_config.CLEANSING_RULE_MAPPING.get(rule["rule_id"])

            if action_type != "ALLOW" and cleansing_cfg and res.get("error_df") is not None:
                # DQ 기준서 8.10: Cleansing Rule 있음 -> 자동 Cleansing -> 재-DQ
                #   PASS -> AUTO_CLEANSED / FAIL -> 남은 위반은 action_type에 따라 UNRESOLVED(격리) 또는 ALLOWED(허용)
                # 마스킹 전 원문이 필요하므로 res["detail_df"]가 아니라 error_df를 쓴다.
                applied_df = self.cleansing.apply(res["error_df"], rule, cleansing_cfg)
                base_df = self.cleansing.build_detail_df(applied_df, record_key_col, rule, action_type)
            else:
                # 정제하지 않는 경우 (Cleansing Rule 없음 - 기준서 8.9 / 허용 오류율 이내 ALLOW):
                #   BLOCK/REVIEW -> UNRESOLVED(격리) / WARN -> ALLOWED(허용, 로그만) / ALLOW -> NOT_REQUIRED(원본 유지)
                # 값을 추정해서 채우지 않으므로 proposed/final_value는 NULL로 남긴다.
                isolate = action_type in dq_config.ISOLATE_ACTIONS
                if isolate:
                    status_value = "UNRESOLVED"
                elif action_type == "ALLOW":
                    status_value = "NOT_REQUIRED"
                else:
                    status_value = "ALLOWED"
                base_df = (
                    res["detail_df"]
                    .withColumn("proposed_value", F.lit(None).cast("string"))
                    .withColumn("final_value", F.lit(None).cast("string"))
                    .withColumn("cleansing_action", F.lit(None).cast("string"))
                    .withColumn("cleansing_rule_id", F.lit(None).cast("string"))
                    .withColumn("cleansing_status", F.lit(status_value))
                    .withColumn("re_dq_result", F.lit(None).cast("string"))
                    .withColumn("review_required_yn", F.lit(isolate))
                )

            detail_df = (
                base_df
                .withColumn("dq_detail_id", F.expr("uuid()"))
                .withColumn("dq_run_id", F.lit(self.dq_run_id))
                .withColumn("rule_id", F.lit(rule["rule_id"]))
                .withColumn("source_system", F.lit(source_system))
                .withColumn("source_table", F.lit(target_table))
                .withColumn("source_batch_id", F.lit(source_batch_id))
                .withColumn("dq_reason", F.lit(res.get("dq_reason", "")))
                .withColumn("created_at", F.lit(now))
                .withColumn("updated_at", F.lit(now))
                .select([f.name for f in dq_config.DQ_CLEANSING_DETAIL_SCHEMA.fields])  # 스키마 순서 고정
            )
            # 집계는 action 1번으로 한꺼번에 뽑는다 (detail_df는 lazy라 action마다 다시 계산되므로 횟수를 줄인다).
            # unique_error_record_count: error_count(검사 조건 만족 "건수")와 달리
            # source_record_key 기준 "고유 레코드 수" - check_duplicate처럼 한 레코드가
            # 여러 컬럼에 걸쳐 잡힐 수 있는 경우 error_count보다 작거나 같을 수 있다.
            # (countDistinct는 NULL을 세지 않으므로, 업무키 자체가 NULL인 오류 - 예: consultation_id 누락 - 는 1건으로 따로 더한다)
            key = F.col("source_record_key")
            agg = detail_df.agg(
                (F.countDistinct(key) + F.max(F.when(key.isNull(), 1).otherwise(0))).alias("uniq"),
                F.sum(F.when(F.col("cleansing_status") == "AUTO_CLEANSED", 1).otherwise(0)).alias("auto"),
                F.sum(F.when(F.col("cleansing_status") == "UNRESOLVED", 1).otherwise(0)).alias("unresolved"),
            ).collect()[0]
            unique_error_record_count = int(agg["uniq"] or 0)
            auto_cleansed_record_count = int(agg["auto"] or 0)
            unresolved_record_count = int(agg["unresolved"] or 0)

        result_row = {
            "rule_id": rule["rule_id"],
            "rule_name": rule.get("rule_name", ""),
            "target_table": target_table,
            "target_column": target_col,
            "dimension": rule.get("dimension", ""),
            "check_count": res["check_count"],
            "error_count": res["error_count"],
            "error_rate": judgment["error_rate"],
            "threshold_rate": rule.get("threshold_rate", 0.0),
            "result_status": judgment["result_status"],
            "error_grade": judgment["error_grade"],
            "action_type": action_type,
            "sample_values_json": json.dumps(res.get("sample_values", []), ensure_ascii=False),
            "dq_reason": res.get("dq_reason", ""),
            "executed_at": datetime.now(),
            # ---- 요청서 반영분 ----
            "dq_run_id": self.dq_run_id,
            "source_system": source_system,
            "dq_rule_version": dq_config.DQ_RULES_VERSION,
            "unique_error_record_count": unique_error_record_count,
            # cleansing_required_yn: action_type 기준 매핑. 매핑표에 없는 값이 나오면(향후 등급 추가 등)
            # 조용히 넘어가지 않고 True(=검토 필요)로 안전하게 처리한다.
            "cleansing_required_yn": dq_config.CLEANSING_REQUIRED_MAP.get(action_type, True),
            # ---- DQ 기준서 8장 반영분 ----
            "auto_cleansed_record_count": auto_cleansed_record_count,
            "unresolved_record_count": unresolved_record_count,
        }
        # detail_df는 DQ_RESULT_SCHEMA에 없는 필드라 result_row에는 안 넣고 별도로 반환한다
        # (run_table_dq가 모아서 dq_cleansing_detail에 한 번에 적재).
        return result_row, detail_df

    def run_table_dq(self, target_table: str, ingest_date: Optional[str] = None) -> tuple:
        """관리형 Delta 테이블을 대상으로 DQ 검사를 실행합니다."""
        rules = [r for r in dq_config.DQ_RULES if r["target_table"] == target_table]
        
        if not rules:
            print(f"[{target_table}] 적용 대상 DQ Rule이 존재하지 않습니다. (DQ_RULES의 target_table명을 확인하세요)")
            empty_summary = {"target_table": target_table, "total_rules": 0, "pass_count": 0, "fail_count": 0}
            return [], empty_summary

        # ingest_date가 전달되지 않은 경우, 관리형 테이블에서 가장 최신 파티션 값 조회
        if not ingest_date:
            partitions = (
                self.spark.read.table(target_table)
                .select("ingest_date")
                .distinct()
                .collect()
            )
            ingest_date_list = sorted([row["ingest_date"] for row in partitions if row["ingest_date"]])
            if not ingest_date_list:
                raise FileNotFoundError(f"관리형 테이블 '{target_table}'에 유효한 ingest_date 값이 존재하지 않습니다.")
            ingest_date = ingest_date_list[-1]

        # 소스 테이블 × 실행마다 새 dq_run_id를 발급한다. 같은 러너로 여러 소스를 돌려도 소스별로 ID가 달라지고,
        # 같은 배치를 다시 실행해도 ID가 달라져 이전 결과와 섞이지 않는다 (요청서 §7-3).
        self.dq_run_id = self._new_run_id(target_table.split(".")[-1])

        # source_batch_id: 지금은 "하루 1배치" 전제라 ingest_date를 그대로 배치 식별자로 쓴다.
        # 하루 여러 배치가 필요해지면 Bronze 적재 구조 자체를 다시 설계해야 하므로 별도 논의 필요.
        source_batch_id = ingest_date

        # 💡 [서버리스 대응] .cache() 제거 및 일반 읽기로 변경
        df = self.spark.read.table(target_table).filter(F.col("ingest_date") == ingest_date)

        results = []
        detail_dfs = []
        for rule in rules:
            result_row, detail_df = self.execute_rule(df, rule, source_batch_id)
            result_row["ingest_date"] = ingest_date
            result_row["source_batch_id"] = source_batch_id
            results.append(result_row)
            if detail_df is not None:
                detail_dfs.append(detail_df)

        # 💡 [서버리스 대응] df.unpersist() 제거 (캐시를 안 했으므로 불필요)
        self._save_dq_results(results)
        self._save_cleansing_details(detail_dfs)

        # 정제·격리 대상은 허용 오류율을 넘긴(ALLOW가 아닌) Rule뿐이다. ALLOW Rule의 위반은 detail에 NOT_REQUIRED로
        # 기록만 되고 candidate에서는 원본 그대로 통과한다. (detail의 AUTO_CLEANSED/UNRESOLVED 건과 candidate/격리 대상은 항상 같은 Rule 집합)
        rules_by_id = {r["rule_id"]: r for r in rules}
        detail_rules = [r for r in results if r["action_type"] != "ALLOW" and (r["error_count"] or 0) > 0]
        # 자동 Cleansing은 Cleansing Rule이 매핑된 Rule에만 적용 (WARN 포함 - 결정적 규칙이라 보정해도 안전하다)
        cleansing_specs = [
            (rules_by_id[r["rule_id"]], dq_config.CLEANSING_RULE_MAPPING[r["rule_id"]])
            for r in detail_rules if r["rule_id"] in dq_config.CLEANSING_RULE_MAPPING
        ]
        # 격리 판정은 BLOCK/REVIEW Rule만 (WARN은 허용이라 위반이 남아도 레코드는 통과)
        isolate_rules = [rules_by_id[r["rule_id"]] for r in detail_rules if r["action_type"] in dq_config.ISOLATE_ACTIONS]
        candidate_row_count, quarantine_row_count = self._save_candidate_and_quarantine(
            df, target_table, source_batch_id, cleansing_specs, isolate_rules
        )

        # 요약 정보 생성
        pass_cnt = sum(1 for r in results if r["result_status"] == "PASS")
        fail_cnt = sum(1 for r in results if r["result_status"] == "FAIL")
        summary = {
            "target_table": target_table,
            "dq_run_id": self.dq_run_id,
            "source_batch_id": source_batch_id,
            "total_rules": len(results),
            "pass_count": pass_cnt,
            "fail_count": fail_cnt,
            # 위반 건 단위 (한 레코드가 여러 Rule을 위반하면 각각 센다)
            "auto_cleansed_record_count": sum(r.get("auto_cleansed_record_count") or 0 for r in results),
            "unresolved_record_count": sum(r.get("unresolved_record_count") or 0 for r in results),
            # 레코드 단위: silver_candidate로 간 행 / 격리 테이블로 간 행 (한 레코드는 둘 중 한 곳에만 들어간다)
            "candidate_row_count": candidate_row_count,
            "quarantine_row_count": quarantine_row_count,
            # 격리된 레코드 없이 전부 silver_candidate로 갔는가 (이번 실행 기준)
            "silver_ready": quarantine_row_count == 0,
        }

        return results, summary
    
    def _save_dq_results(self, results: List[Dict[str, Any]]) -> None:
        """
        결과를 메타(Meta) Delta 테이블에 적재합니다.
        요청서 7장: 재실행해도 이전 결과를 UPDATE/DELETE하지 않고 append만 한다
        (dq_run_id가 실행마다 새로 발급되므로 같은 배치를 다시 검사해도 행이 늘어날 뿐
        기존 이력은 그대로 남는다 - replaceWhere로 덮어쓰던 예전 방식에서 변경됨).
        """
        if not results:
            return

        # 명시적 스키마가 적용된 DataFrame 생성
        result_df = self.spark.createDataFrame(results, schema=dq_config.DQ_RESULT_SCHEMA)
        table_name = dq_config.DQ_RESULT_TABLE

        # 상위 카탈로그/스키마 존재 여부 확인 및 테이블 자동 생성 보완
        if not self.spark.catalog.tableExists(table_name):
            print(f"ℹ️ 메타 테이블 '{table_name}'이 존재하지 않아 새로 생성합니다.")
            (
                result_df.limit(0)
                .write
                .format("delta")
                .mode("overwrite")
                .partitionBy("ingest_date")  # 최신 배치 조회가 잦으므로 ingest_date로 파티셔닝
                .saveAsTable(table_name)
            )

        target_ingest_date = results[0].get("ingest_date")
        target_table_name = results[0].get("target_table")
        print(f"🔄 메타 테이블 '{table_name}' 적재 중 (append, dq_run_id='{self.dq_run_id}', "
              f"ingest_date='{target_ingest_date}', table='{target_table_name}')...")

        # mergeSchema: DQ 기준서 8장 반영으로 늘어난 컬럼(auto_cleansed_record_count 등)을 기존 테이블에 자동 추가
        result_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table_name)

        print(f"✅ DQ 결과가 '{table_name}' 테이블에 누적 저장되었습니다. (건수: {len(results)})")

    def _save_cleansing_details(self, detail_dfs: List[DataFrame]) -> None:
        """요청서 4장: 오류 행 단위 상세를 dq_cleansing_detail에 append로 쌓는다."""
        if not detail_dfs:
            return  # 이번 배치에 FAIL 건이 하나도 없으면 저장할 것도 없다

        # union은 셔플 없는 가벼운 연산이라 서버리스에서도 부담 적음
        combined_df = detail_dfs[0]
        for d in detail_dfs[1:]:
            combined_df = combined_df.unionByName(d)

        table_name = dq_config.DQ_CLEANSING_DETAIL_TABLE
        if not self.spark.catalog.tableExists(table_name):
            print(f"ℹ️ 메타 테이블 '{table_name}'이 존재하지 않아 새로 생성합니다.")
            (
                combined_df.limit(0)
                .write.format("delta").mode("overwrite")
                .partitionBy("source_batch_id")
                .saveAsTable(table_name)
            )

        # mergeSchema: 늘어난 컬럼(cleansing_rule_id, re_dq_result)을 기존 테이블에 자동 추가
        combined_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table_name)
        print(f"✅ 오류 상세가 '{table_name}'에 누적 저장되었습니다. (행 수는 action=ALLOW 제외 기준)")

    def _ensure_schema_exists(self, full_table_name: str) -> None:
        """
        saveAsTable은 카탈로그/스키마를 자동으로 만들어주지 않는다 (테이블만 없으면 만들어줌).
        silver_candidate처럼 아직 존재를 보장할 수 없는 스키마는 저장 직전에 먼저 만들어둔다.
        계정에 해당 카탈로그의 CREATE SCHEMA 권한이 없으면 여기서 실패하니, 권한 문제면
        이 호출이 아니라 Unity Catalog 권한 쪽을 확인해야 한다.
        """
        catalog, schema, _ = full_table_name.split(".")
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    def _save_candidate_and_quarantine(self, df: DataFrame, target_table: str, source_batch_id: str,
                                       cleansing_specs: Optional[List[tuple]] = None,
                                       isolate_rules: Optional[List[Dict[str, Any]]] = None) -> tuple:
        """
        요청서 5장 + DQ 기준서 §4/§12: 레코드를 상태별로 나눠 적재한다.
          CLEAN     격리·정제 대상 위반이 없음 (허용된 WARN/ALLOW 위반은 포함될 수 있음)   -> silver_candidate.<source>
          CLEANSED  자동 Cleansing + 재-DQ PASS로 정상화된 값이 있고 남은 위반이 없음   -> silver_candidate.<source>
          UNRESOLVED 정제 후에도 BLOCK/REVIEW Rule 위반이 남음                         -> dq_quarantine.<source> (격리)
        한 레코드가 여러 Rule을 위반할 수 있으므로, 정제 "이후" 값 기준으로 isolate_rules를 전부 다시 판정한다.
        (하나라도 남으면 격리 - 일부 컬럼만 정상이라고 부분 적재하지 않는다)
        DQ 단계에는 HITL이 없다. 격리 레코드의 후속 처리(룰 추천, 재처리, 검토)는 Gold -> Target 단계 몫이다.
        반환: (silver_candidate 행 수, 격리 행 수)
        """
        source_system = target_table.split(".")[-1]
        record_key_col = dq_config.TABLE_RECORD_KEY_COLUMN.get(target_table)
        now = datetime.now()

        key_expr = F.col(record_key_col).cast("string") if record_key_col else F.lit(None).cast("string")

        # 자동 Cleansing 반영 -> 정제 "이후" 값으로 남은 위반을 행 단위로 판정
        cleansed_df = self.cleansing.apply_to_candidate(df, cleansing_specs or [])
        classified_df = self.cleansing.classify_unresolved(cleansed_df, isolate_rules or [])

        unresolved = F.coalesce(F.col("__unresolved_flag"), F.lit(False))
        base_df = (
            classified_df
            .withColumn("_source_system", F.lit(source_system))
            .withColumn("_source_table", F.lit(target_table))
            .withColumn("_source_record_key", key_expr)
            .withColumn("_source_batch_id", F.lit(source_batch_id))
            .withColumn("_dq_run_id", F.lit(self.dq_run_id))
            .withColumn("_cleansed_yn", F.col("__cleansed_flag"))  # 자동 Cleansing으로 값이 바뀐 행만 True
        )
        tmp_cols = ["__cleansed_flag", "__unresolved_flag", "__unresolved_rule_ids"]

        candidate_df = (
            base_df.filter(~unresolved)
            .withColumn("_candidate_created_at", F.lit(now))
            # 신규 컬럼은 기존 candidate 테이블과 컬럼 순서가 어긋나지 않도록 맨 뒤에 둔다
            .withColumn("_dq_status", F.when(F.col("_cleansed_yn"), F.lit("CLEANSED")).otherwise(F.lit("CLEAN")))
            .drop(*tmp_cols)
        )
        quarantine_df = (
            base_df.filter(unresolved)
            .withColumn("_quarantined_at", F.lit(now))
            .withColumn("_dq_status", F.lit("UNRESOLVED"))
            .withColumn("_unresolved_rule_ids", F.col("__unresolved_rule_ids"))  # 남아 있는 위반 Rule ID (콤마 구분)
            .drop(*tmp_cols)
        )

        candidate_rows = self._write_batch_table(
            candidate_df, dq_config.silver_candidate_table(source_system), source_batch_id, "Silver Candidate")
        quarantine_rows = self._write_batch_table(
            quarantine_df, dq_config.quarantine_table(source_system), source_batch_id, "격리(UNRESOLVED) 레코드")
        return candidate_rows, quarantine_rows

    def _write_batch_table(self, df: DataFrame, table_name: str, source_batch_id: str, label: str) -> int:
        """배치(_source_batch_id) 단위로 교체 적재하고 그 배치의 행 수를 돌려준다."""
        self._ensure_schema_exists(table_name)  # 스키마가 없으면 자동 생성

        if not self.spark.catalog.tableExists(table_name):
            print(f"ℹ️ {label} 테이블 '{table_name}'이 존재하지 않아 새로 생성합니다.")
            (
                df.limit(0)
                .write.format("delta").mode("overwrite")
                .partitionBy("_source_batch_id")
                .saveAsTable(table_name)
            )

        # 같은 배치(_source_batch_id)를 재검사한 경우 중복으로 쌓지 않고 그 배치분만 교체한다.
        # (dq_result처럼 무한 이력을 쌓을 필요는 없다고 판단 - "재실행 시 이력으로 남길지"는 종옥님과 확인 필요)
        # 격리 대상이 0건이어도 이전 실행에서 격리됐던 그 배치의 행이 지워지도록 항상 실행한다.
        (
            df.write.format("delta").mode("overwrite")
            .option("replaceWhere", f"_source_batch_id = '{source_batch_id}'")
            .option("mergeSchema", "true")  # 신규 컬럼(_dq_status 등) 자동 추가
            .saveAsTable(table_name)
        )
        n = (
            self.spark.read.table(table_name)
            .filter(F.col("_source_batch_id") == source_batch_id)
            .count()
        )
        print(f"✅ {label}이(가) '{table_name}'에 적재되었습니다. (행 수: {n})")
        return n