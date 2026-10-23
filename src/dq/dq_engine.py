"""
DQ 실행 엔진 (자동 정제 실행 · 규칙 저장소 · 결과 리포트 · 실행기) - 검사·정제의 순수 로직은 dq_functions.py에 있고,
여기는 그걸 실제로 Spark 테이블에 대해 돌리는 클래스들이다.

구성 (아래 순서 그대로, 뒤 섹션이 앞 섹션을 쓴다)
    1. CleansingEngine  - dq_functions의 정제 함수들을 규칙에 맞게 적용하고 재-DQ 판정
    2. RuleRepository   - meta.dq_rule_def 버전 관리, CSV 동기화
    3. DQResultBuilder  - 콘솔/Slack 리포트
    4. DQRunner         - 1~3번과 dq_functions를 조립해 실제로 돌리는 최상위 클래스
"""
import hashlib
import json
import re
import uuid
from datetime import datetime
from functools import reduce
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, DoubleType, IntegerType, StringType, StructField, StructType,
                               TimestampType)

try:
    import src.dq.dq_config as dq_config
    import src.dq.dq_functions as dq_functions
except ModuleNotFoundError:
    import dq_config
    import dq_functions


# =============================================================================
# 1. 자동 정제 실행 (CleansingEngine)
# =============================================================================
# 내부 작업용 컬럼 접두어 (최종 결과 DataFrame에는 남기지 않는다)
_TMP = "__cln_"

STATUS_AUTO_CLEANSED = "AUTO_CLEANSED"
STATUS_UNRESOLVED = "UNRESOLVED"   # 정제 후에도 BLOCK/REVIEW Rule 위반이 남음 -> 격리
STATUS_ALLOWED = "ALLOWED"         # 정제 후에도 WARN Rule 위반이 남음 -> 허용(로그만)
class CleansingEngine:
    """DQ Rule 1개에 대해 오류 행을 자동 Cleansing하고 재-DQ 결과를 붙인다."""

    def __init__(self, spark: SparkSession, code_master_loader):
        """code_master_loader: DQRunner._load_code_master 같이 CODE_GROUP/CODE 컬럼을 가진 DataFrame을 돌려주는 callable"""
        self.spark = spark
        self._load_code_master = code_master_loader
        self._valid_codes_cache: Dict[str, List[str]] = {}
        self._mapping_cache: Dict[str, Dict[str, str]] = {}

    # ---------------------------------------------------------------
    # 코드 마스터 / 승인 매핑 (둘 다 소량이라 driver로 collect 후 Column 식에 박아 넣는다 - join/shuffle 없음)
    # ---------------------------------------------------------------
    def valid_codes(self, code_group: str) -> List[str]:
        if code_group not in self._valid_codes_cache:
            rows = (
                self._load_code_master()
                .filter(F.col("CODE_GROUP") == code_group)
                .select(F.col("CODE").cast("string").alias("CODE"))
                .distinct()
                .collect()
            )
            self._valid_codes_cache[code_group] = [r["CODE"] for r in rows if r["CODE"] is not None]
        return self._valid_codes_cache[code_group]

    def approved_mapping(self, code_group: str) -> Dict[str, str]:
        """
        8.7 처리 조건 1~5를 만족하는 Source -> Standard 매핑만 돌려준다.
          - 매핑표에 명시되어 있고, APPROVED_YN='Y' 이며,
          - 하나의 Source 값에 Standard 값이 하나로 결정되고,
          - Standard 코드가 code_master(같은 CODE_GROUP)에 존재한다.
        매핑 테이블이 아직 없으면 빈 매핑으로 처리한다 -> 코드값 오류는 전부 미해결.
        """
        if code_group in self._mapping_cache:
            return self._mapping_cache[code_group]

        mapping: Dict[str, str] = {}
        table = dq_config.CODE_MAPPING_TABLE
        if self.spark.catalog.tableExists(table):
            approved = (
                self.spark.read.table(table)
                .filter((F.col("CODE_GROUP") == code_group) & (F.col("APPROVED_YN") == "Y"))
                .filter(F.col("SOURCE_CODE").isNotNull() & F.col("STANDARD_CODE").isNotNull())
                .select(
                    F.col("SOURCE_CODE").cast("string").alias("SOURCE_CODE"),
                    F.col("STANDARD_CODE").cast("string").alias("STANDARD_CODE"),
                )
                .distinct()
            )
            # 하나의 Source에 Standard가 둘 이상이면 결정할 수 없으므로(8.2.2) 통째로 제외
            deterministic = (
                approved.groupBy("SOURCE_CODE")
                .agg(F.count("*").alias("n"), F.first("STANDARD_CODE").alias("STANDARD_CODE"))
                .filter(F.col("n") == 1)
                .select("SOURCE_CODE", "STANDARD_CODE")
            )
            exists_in_master = (
                self._load_code_master()
                .filter(F.col("CODE_GROUP") == code_group)
                .select(F.col("CODE").cast("string").alias("STANDARD_CODE"))
                .distinct()
            )
            rows = deterministic.join(exists_in_master, "STANDARD_CODE", "inner").collect()
            mapping = {r["SOURCE_CODE"]: r["STANDARD_CODE"] for r in rows}
        self._mapping_cache[code_group] = mapping
        return mapping

    # ---------------------------------------------------------------
    # 재-DQ: 원래 DQ Rule의 "정상" 조건과 동일하게 판정
    # ---------------------------------------------------------------
    def _re_dq_expr(self, c: Column, rule: Dict[str, Any]) -> Column:
        rule_type = rule["rule_type"]
        # 오류 조건은 dq_functions의 공용 조건(pattern_error / code_error)을 그대로 쓴다 -> "정상" = 오류가 아님
        if rule_type == "PATTERN_CHECK":
            return ~dq_functions.pattern_error(c, rule["pattern"])
        if rule_type == "CODE_EXISTS":
            return ~dq_functions.code_error(c, self.valid_codes(rule["code_group"]))
        raise ValueError(
            f"'{rule['rule_id']}'({rule_type})는 재-DQ를 지원하지 않아 자동 Cleansing 대상이 될 수 없습니다."
        )

    def _step(self, step_id: str, c: Column, rule: Dict[str, Any], cfg: Dict[str, Any]) -> Column:
        if step_id == "CLN-COM-001":
            return dq_functions.cln_com_001_whitespace(c)
        if step_id == "CLN-VAL-001":
            return dq_functions.cln_val_001_phone(c)
        if step_id == "CLN-VAL-002":
            return dq_functions.cln_val_002_datetime(c, cfg.get("datetime_kind", "TIMESTAMP"))
        if step_id == "CLN-VAL-003":
            return dq_functions.cln_val_003_code_mapping(c, self.approved_mapping(rule["code_group"]))
        raise ValueError(f"구현되지 않은 Cleansing Rule입니다: {step_id}")

    # ---------------------------------------------------------------
    # 핵심: Cleansing + 재-DQ 결과 컬럼 부착
    # ---------------------------------------------------------------
    def apply(self, df: DataFrame, rule: Dict[str, Any], cfg: Dict[str, Any]) -> DataFrame:
        """
        df에 `__cln_*` 작업 컬럼을 붙여 돌려준다. (df 전체를 대상으로 해도, 오류 행만 대상으로 해도 된다)
          __cln_before      원본 값(문자열)
          __cln_after       Cleansing 결과(문자열)
          __cln_changed     값이 실제로 바뀌었는지
          __cln_rule_ids    값을 바꾼 CLN Rule ID (콤마 구분, 없으면 NULL)
          __cln_actions     값을 바꾼 정제 유형 (CLEANSING_RULES[..]['action'])
          __cln_orig_valid  원본이 이미 DQ를 통과하는 값이었는지
          __cln_re_dq_pass  Cleansing 결과가 DQ를 통과하는지 (= 재-DQ)
          __cln_auto        자동 Cleansing 성공 여부 (값이 바뀜 + 재-DQ PASS)
        """
        col_name = rule["column"]
        src = F.col(col_name).cast("string")

        cur = src
        rule_id_parts, action_parts = [], []
        # enabled=False인 단계(현재 CLN-VAL-003 코드 매핑)는 건너뛴다 - 매핑 테이블도 읽지 않는다
        steps = [s for s in cfg["steps"] if dq_config.CLEANSING_RULES[s].get("enabled", True)]
        for step_id in steps:
            nxt = self._step(step_id, cur, rule, cfg)
            changed = ~cur.eqNullSafe(nxt)
            rule_id_parts.append(F.when(changed, F.lit(step_id)))
            action_parts.append(F.when(changed, F.lit(dq_config.CLEANSING_RULES[step_id]["action"])))
            cur = nxt

        rule_ids = F.concat_ws(",", *rule_id_parts)
        actions = F.concat_ws(",", *action_parts)
        changed_any = ~src.eqNullSafe(cur)
        re_dq_pass = self._re_dq_expr(cur, rule)

        return (
            df
            .withColumn(f"{_TMP}before", src)
            .withColumn(f"{_TMP}after", cur)
            .withColumn(f"{_TMP}changed", changed_any)
            .withColumn(f"{_TMP}rule_ids", F.when(F.length(rule_ids) > 0, rule_ids))
            .withColumn(f"{_TMP}actions", F.when(F.length(actions) > 0, actions))
            .withColumn(f"{_TMP}orig_valid", self._re_dq_expr(src, rule))
            .withColumn(f"{_TMP}re_dq_pass", re_dq_pass)
            .withColumn(f"{_TMP}auto", changed_any & re_dq_pass)
        )

    # ---------------------------------------------------------------
    # dq_cleansing_detail용: 오류 행 -> 상세 (8.11)
    # ---------------------------------------------------------------
    def build_detail_df(self, applied_df: DataFrame, record_key_col: Optional[str],
                        rule: Dict[str, Any], action_type: str) -> DataFrame:
        """
        apply()를 거친 오류 행에서 dq_cleansing_detail의 Cleansing 관련 컬럼을 만든다.
        (dq_detail_id, dq_run_id, rule_id 등 공통 컬럼은 dq_runner가 붙인다)
        정제되지 않은 채 남은 위반은 Rule의 action_type으로 나뉜다 (기준서 §4):
          BLOCK/REVIEW -> UNRESOLVED (격리)  /  WARN -> ALLOWED (허용)
        """
        if record_key_col and record_key_col in applied_df.columns:
            key_expr = F.col(record_key_col).cast("string")
        else:
            key_expr = F.lit(None).cast("string")

        mask = dq_functions._mask_udf
        auto = F.col(f"{_TMP}auto")
        remaining = STATUS_UNRESOLVED if action_type in dq_config.ISOLATE_ACTIONS else STATUS_ALLOWED
        status = F.when(auto, F.lit(STATUS_AUTO_CLEANSED)).otherwise(F.lit(remaining))
        changed = F.col(f"{_TMP}changed")
        after_masked = mask(F.col(f"{_TMP}after"))

        return applied_df.select(
            key_expr.alias("source_record_key"),
            F.lit(rule["column"]).alias("target_column"),
            mask(F.col(f"{_TMP}before")).alias("before_value"),                 # AFTER 전 값 (마스킹)
            F.when(changed, after_masked).alias("proposed_value"),               # AFTER_VALUE (시도한 값)
            F.when(auto, after_masked).alias("final_value"),                     # 재-DQ PASS로 확정된 값
            F.col(f"{_TMP}actions").alias("cleansing_action"),
            F.col(f"{_TMP}rule_ids").alias("cleansing_rule_id"),
            status.alias("cleansing_status"),
            F.when(F.col(f"{_TMP}re_dq_pass"), F.lit("PASS")).otherwise(F.lit("FAIL")).alias("re_dq_result"),
            (F.lit(remaining == STATUS_UNRESOLVED) & ~auto).alias("review_required_yn"),  # 격리 대상 여부
        )

    # ---------------------------------------------------------------
    # silver_candidate용: 재-DQ PASS 값만 원본 컬럼에 반영
    # ---------------------------------------------------------------
    def apply_to_candidate(self, df: DataFrame,
                           specs: List[Tuple[Dict[str, Any], Dict[str, Any]]]) -> DataFrame:
        """
        specs: [(rule, cleansing_cfg), ...] - 이번 실행에서 dq_cleansing_detail을 만든 Rule들.
        원본이 이미 DQ 오류였고(orig_valid=False) 재-DQ PASS(AUTO_CLEANSED)인 행만 값을 바꾼다.
        나머지(변환 불가/재-DQ FAIL)는 Bronze 원본 그대로 남는다 (BLOCK/REVIEW Rule이면 classify_unresolved가 격리 대상으로 분류).
        detail을 만든 Rule에만 적용하므로, 바뀐 값은 항상 dq_cleansing_detail에서 before/after를 추적할 수 있다.
        반환 DataFrame에는 `__cleansed_flag`(행 단위 Cleansing 여부) 컬럼이 추가된다.
        """
        out = df.withColumn("__cleansed_flag", F.lit(False))
        for rule, cfg in specs:
            col_name = rule["column"]
            dtype = out.schema[col_name].dataType
            tmp = self.apply(out, rule, cfg)
            hit = F.col(f"{_TMP}auto") & (~F.col(f"{_TMP}orig_valid"))
            out = (
                tmp
                .withColumn(col_name, F.when(hit, F.col(f"{_TMP}after").cast(dtype)).otherwise(F.col(col_name)))
                .withColumn("__cleansed_flag", F.col("__cleansed_flag") | hit)
                .drop(*[c for c in tmp.columns if c.startswith(_TMP)])
            )
        return out

    # ---------------------------------------------------------------
    # 레코드 단위 격리 판정
    # ---------------------------------------------------------------
    def classify_unresolved(self, df: DataFrame, rules: List[Dict[str, Any]]) -> DataFrame:
        """
        한 레코드가 여러 Rule을 위반할 수 있으므로(예: 전화번호는 자동 정제되지만 코드값은 미해결),
        "정제 후 값" 기준으로 격리 대상 Rule(BLOCK/REVIEW)을 전부 다시 판정해 행 단위로 표시한다.
        (apply_to_candidate 이후에 호출)
          __unresolved_flag     정제 후에도 위반하는 Rule이 하나라도 있으면 True -> 격리(UNRESOLVED)
          __unresolved_rule_ids 남아 있는 위반 Rule ID (콤마 구분)
        rules: 이번 실행에서 detail을 만든 Rule 중 action_type이 ISOLATE_ACTIONS인 것.
        WARN(허용) Rule은 포함하지 않는다 - 위반이 남아도 레코드는 통과한다.
        detail과 같은 Rule 집합·같은 오류 조건을 쓰므로, Rule별 표시 행 수 = unresolved_record_count 이다.
        키 조인이 아니라 행 자체에서 판정하므로 업무키가 NULL/중복인 행도 정확히 표시된다.
        """
        out = df
        err_cols = []
        for i, rule in enumerate(rules):
            codes = self.valid_codes(rule["code_group"]) if rule["rule_type"] == "CODE_EXISTS" else None
            name = f"{_TMP}err_{i}"
            out = out.withColumn(name, F.coalesce(dq_functions.row_error_expr(rule, codes), F.lit(False)))
            err_cols.append((name, rule["rule_id"]))

        if not err_cols:
            return (out.withColumn("__unresolved_flag", F.lit(False))
                       .withColumn("__unresolved_rule_ids", F.lit(None).cast("string")))

        flag = reduce(lambda a, b: a | b, [F.col(n) for n, _ in err_cols])
        ids = F.concat_ws(",", *[F.when(F.col(n), F.lit(rid)) for n, rid in err_cols])
        return (
            out.withColumn("__unresolved_flag", flag)
               .withColumn("__unresolved_rule_ids", F.when(F.length(ids) > 0, ids))
               .drop(*[n for n, _ in err_cols])
        )


# =============================================================================


# =============================================================================
# 2. 규칙 저장소
# =============================================================================
"""
DQ 규칙 저장소 - 규칙 정의(파라미터)를 코드가 아니라 버전 관리되는 meta 테이블(meta.dq_rule_def)에서 읽는다.

설계
    정본      meta.dq_rule_def  (추가 전용: 행을 고치거나 지우지 않고, 바뀔 때마다 새 버전 행을 덧붙인다)
    입력      Volume의 CSV -> dq_rule_loader가 검증한 뒤 이 모듈로 적재
    실행      DQRunner가 시작할 때 "규칙별 최신 버전 중 활성(is_active)"만 읽는다

버전
    규칙 내용(대상 테이블·유형·컬럼·패턴·임계치·등급·정제 단계 등)의 해시가 바뀌면 rule_version이 1 올라간다.
    이름·변경 사유만 바뀐 경우는 새 버전을 만들지 않는다. 규칙을 CSV에서 빼면 "비활성 버전"이 추가되어 더 이상 실행되지 않고,
    다시 넣으면 다시 활성 버전이 추가된다. dq_result.dq_rule_version에는 실행에 쓴 그 규칙의 버전이 찍힌다.

코드는 그대로 두는 것
    검사·정제 "로직"(NULL/PATTERN/RANGE/ORDER/CODE/DUPLICATE, 공백·전화번호·날짜 정제 단계)은 코드이고, 이 테이블에는 "파라미터"만 있다.
    새로운 종류의 검사를 추가하려면 코드가 필요하다.
"""

RULE_DEF_SCHEMA = StructType([
    StructField("rule_id", StringType(), False),
    StructField("rule_version", IntegerType(), False),
    StructField("is_active", BooleanType(), False),
    StructField("rule_name", StringType(), True),
    StructField("target_table", StringType(), False),
    StructField("rule_type", StringType(), False),
    StructField("target_columns", StringType(), False),       # 콤마로 구분 (단일 컬럼 규칙은 1개)
    StructField("pattern", StringType(), True),
    StructField("min_value", DoubleType(), True),
    StructField("max_value", DoubleType(), True),
    StructField("code_group", StringType(), True),
    StructField("dimension", StringType(), True),
    StructField("threshold_rate", DoubleType(), False),
    StructField("error_grade", StringType(), False),
    StructField("cleansing_steps", StringType(), True),      # 콤마로 구분, 없으면 NULL (자동 정제 없음)
    StructField("datetime_kind", StringType(), True),        # DATE / TIMESTAMP (CLN-VAL-002 사용 시)
    StructField("definition_hash", StringType(), False),
    StructField("valid_from", TimestampType(), False),
    StructField("change_reason", StringType(), True),
])

# 입력(CSV/딕셔너리)의 컬럼. 버전·해시·유효시작은 저장소가 붙인다.
INPUT_COLUMNS = ["rule_id", "rule_name", "target_table", "rule_type", "target_columns", "pattern", "min_value", "max_value",
                 "code_group", "dimension", "threshold_rate", "error_grade", "cleansing_steps", "datetime_kind", "change_reason"]

# 이 필드가 바뀌면 "규칙이 바뀐 것"이다 (이름·변경 사유는 제외)
_HASH_FIELDS = ["target_table", "rule_type", "target_columns", "pattern", "min_value", "max_value", "code_group",
                "dimension", "threshold_rate", "error_grade", "cleansing_steps", "datetime_kind"]
_MULTI_COLUMN_TYPES = ("ORDER_CHECK", "DUPLICATE_CHECK")


# ---------------------------------------------------------------------------
# 입력 정규화 / 변환
# ---------------------------------------------------------------------------
def _blank_to_none(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, float) and v != v:      # NaN
        return None
    s = str(v).strip() if not isinstance(v, (int, float, bool)) else v
    return None if s == "" else s


def _split_csv(s: Optional[str]) -> List[str]:
    return [p.strip() for p in str(s).split(",") if p.strip()] if s else []


def _to_float(v: Any, field: str, rule_id: str, errors: List[str]) -> Optional[float]:
    v = _blank_to_none(v)
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        errors.append(f"{rule_id}: {field} '{v}'는 숫자여야 합니다")
        return None


def normalize_input(raw: Dict[str, Any]) -> Dict[str, Any]:
    """CSV 한 행(딕셔너리)을 저장소 입력 형식으로 정리한다 (빈 칸은 None, 숫자·콤마 목록 표준화). 값 검사는 validate_inputs가 한다."""
    errors: List[str] = []
    item = {k: _blank_to_none(raw.get(k)) for k in INPUT_COLUMNS}
    rid = item["rule_id"] or "(rule_id 없음)"
    item["threshold_rate"] = _to_float(raw.get("threshold_rate"), "threshold_rate", rid, errors)
    item["min_value"] = _to_float(raw.get("min_value"), "min_value", rid, errors)
    item["max_value"] = _to_float(raw.get("max_value"), "max_value", rid, errors)
    item["target_columns"] = ",".join(_split_csv(item["target_columns"])) or None
    item["cleansing_steps"] = ",".join(_split_csv(item["cleansing_steps"])) or None
    for f in ("rule_type", "error_grade", "datetime_kind"):
        if item[f]:
            item[f] = str(item[f]).upper()
    if errors:
        raise ValueError("규칙 입력 오류:\n  - " + "\n  - ".join(errors))
    return item


# ---------------------------------------------------------------------------
# 검증 (하나라도 틀리면 아무것도 적재하지 않고 모든 오류를 한꺼번에 알려 준다)
# ---------------------------------------------------------------------------
def validate_inputs(items: List[Dict[str, Any]]) -> None:
    errors: List[str] = []
    seen = set()
    for it in items:
        rid = it.get("rule_id") or "(rule_id 없음)"
        if not it.get("rule_id"):
            errors.append("rule_id가 비어 있는 행이 있습니다")
        elif it["rule_id"] in seen:
            errors.append(f"{rid}: rule_id가 중복되었습니다")
        seen.add(it.get("rule_id"))

        for f in ("target_table", "rule_type", "target_columns", "error_grade"):
            if not it.get(f):
                errors.append(f"{rid}: {f}가 비어 있습니다")
        if it.get("threshold_rate") is None:
            errors.append(f"{rid}: threshold_rate가 비어 있습니다")
        elif not 0.0 <= it["threshold_rate"] <= 1.0:
            errors.append(f"{rid}: threshold_rate {it['threshold_rate']}는 0~1 사이여야 합니다")

        rt, cols = it.get("rule_type"), _split_csv(it.get("target_columns"))
        if rt and rt not in dq_config.RULE_TYPES:
            errors.append(f"{rid}: rule_type '{rt}'는 {list(dq_config.RULE_TYPES)} 중 하나여야 합니다")
        if it.get("error_grade") and it["error_grade"] not in dq_config.ERROR_GRADES:
            errors.append(f"{rid}: error_grade '{it['error_grade']}'는 {list(dq_config.ERROR_GRADES)} 중 하나여야 합니다")
        if rt == "ORDER_CHECK" and len(cols) != 2:
            errors.append(f"{rid}: ORDER_CHECK는 target_columns가 정확히 2개(시작,종료)여야 합니다")
        if rt == "DUPLICATE_CHECK" and len(cols) < 1:
            errors.append(f"{rid}: DUPLICATE_CHECK는 target_columns가 1개 이상이어야 합니다")
        if rt in ("NULL_CHECK", "PATTERN_CHECK", "RANGE_CHECK", "CODE_EXISTS") and len(cols) != 1:
            errors.append(f"{rid}: {rt}는 target_columns가 정확히 1개여야 합니다 (현재 {len(cols)}개)")
        if rt == "PATTERN_CHECK":
            if not it.get("pattern"):
                errors.append(f"{rid}: PATTERN_CHECK는 pattern이 필요합니다")
            else:
                try:
                    re.compile(it["pattern"])
                except re.error as e:
                    errors.append(f"{rid}: pattern 정규식이 올바르지 않습니다 ({e})")
        if rt == "RANGE_CHECK":
            if it.get("min_value") is None or it.get("max_value") is None:
                errors.append(f"{rid}: RANGE_CHECK는 min_value와 max_value가 필요합니다")
            elif it["min_value"] > it["max_value"]:
                errors.append(f"{rid}: min_value가 max_value보다 큽니다")
        if rt == "CODE_EXISTS" and not it.get("code_group"):
            errors.append(f"{rid}: CODE_EXISTS는 code_group이 필요합니다")

        steps = _split_csv(it.get("cleansing_steps"))
        if steps:
            if rt not in dq_config.AUTO_CLEANSING_RULE_TYPES:
                reason = dq_config.CLEANSING_EXCLUDED_REASONS.get(rt, "재-DQ 불가 유형")
                errors.append(f"{rid}: {rt}는 자동 Cleansing 대상이 될 수 없습니다 ({reason})")
            for st in steps:
                if st not in dq_config.CLEANSING_RULES:
                    errors.append(f"{rid}: cleansing step '{st}'가 CLEANSING_RULES에 정의되어 있지 않습니다")
            # 전화번호 정제 결과 형식과 규칙의 pattern이 다르면 재-DQ가 항상 FAIL이 된다 (코드의 PHONE_STANDARD_FORMAT과 어긋남 방지)
            if "CLN-VAL-001" in steps and it.get("pattern") != dq_config.PHONE_PATTERN:
                errors.append(f"{rid}: 전화번호 정제(CLN-VAL-001)를 쓰는 규칙의 pattern은 표준 형식 "
                              f"{dq_config.PHONE_STANDARD_FORMAT}({dq_config.PHONE_PATTERN})과 같아야 합니다")
        if it.get("datetime_kind") and it["datetime_kind"] not in ("DATE", "TIMESTAMP"):
            errors.append(f"{rid}: datetime_kind는 DATE 또는 TIMESTAMP여야 합니다")
    if errors:
        raise ValueError("DQ 규칙 검증 실패 (아무것도 적재하지 않았습니다):\n  - " + "\n  - ".join(errors))


def compute_hash(item: Dict[str, Any]) -> str:
    payload = {f: item.get(f) for f in _HASH_FIELDS}
    for f in ("threshold_rate", "min_value", "max_value"):
        if payload[f] is not None:
            payload[f] = round(float(payload[f]), 10)
    return hashlib.sha1(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()[:16]


def row_to_rule(row: Dict[str, Any]) -> Dict[str, Any]:
    """테이블 한 행을 러너가 쓰는 규칙 딕셔너리(rule_version, cleansing_steps 포함)로 바꾼다."""
    cols = _split_csv(row["target_columns"])
    rule = {
        "rule_id": row["rule_id"], "rule_name": row.get("rule_name") or "", "target_table": row["target_table"],
        "rule_type": row["rule_type"], "dimension": row.get("dimension"), "threshold_rate": row["threshold_rate"],
        "error_grade": row["error_grade"], "rule_version": int(row["rule_version"]),
        "cleansing_steps": _split_csv(row.get("cleansing_steps")), "datetime_kind": row.get("datetime_kind"),
    }
    if row["rule_type"] in _MULTI_COLUMN_TYPES:
        rule["columns"] = cols
    else:
        rule["column"] = cols[0]
    if row.get("pattern"):
        rule["pattern"] = row["pattern"]
    if row["rule_type"] == "RANGE_CHECK":
        rule["min_value"], rule["max_value"] = row["min_value"], row["max_value"]
    if row.get("code_group"):
        rule["code_group"] = row["code_group"]
    return rule


# ---------------------------------------------------------------------------
# 저장소
# ---------------------------------------------------------------------------
class RuleRepository:
    def __init__(self, spark: SparkSession, table: Optional[str] = None):
        self.spark = spark
        self.table = table or dq_config.DQ_RULE_DEF_TABLE

    def exists(self) -> bool:
        return self.spark.catalog.tableExists(self.table)

    def _latest_df(self) -> DataFrame:
        """규칙별 최신 버전 1행 (비활성 포함)."""
        w = Window.partitionBy("rule_id").orderBy(F.col("rule_version").desc())
        return (self.spark.read.table(self.table)
                .withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn"))

    def load_current(self, target_table: Optional[str] = None) -> List[Dict[str, Any]]:
        """실행에 쓸 규칙: 규칙별 최신 버전이 활성인 것만 (target_table을 주면 그 테이블 규칙만)."""
        if not self.exists():
            raise RuntimeError(f"DQ 규칙 테이블 {self.table}이(가) 없습니다. dq_rule_loader로 규칙을 먼저 적재하세요.")
        df = self._latest_df().filter(F.col("is_active"))
        if target_table:
            df = df.filter(F.col("target_table") == target_table)
        return [row_to_rule(r.asDict()) for r in df.orderBy("rule_id").collect()]

    def history(self, rule_id: str) -> DataFrame:
        return self.spark.read.table(self.table).filter(F.col("rule_id") == rule_id).orderBy("rule_version")

    # ---- 동기화 ------------------------------------------------------------------
    def plan_sync(self, incoming: List[Dict[str, Any]], deactivate_missing: bool = True,
                  change_reason: Optional[str] = None) -> Dict[str, Any]:
        """
        입력 규칙 목록과 현재 테이블을 비교해 "무엇이 신규/변경/재활성/비활성/그대로인지"와 덧붙일 행을 계산한다 (쓰기 없음).
        incoming은 반드시 전체 목록(스냅샷)이어야 한다 - 빠진 규칙은 비활성으로 처리한다 (deactivate_missing=True).
        """
        validate_inputs(incoming)
        existing: Dict[str, Dict[str, Any]] = {}
        if self.exists():
            existing = {r["rule_id"]: r for r in (x.asDict() for x in self._latest_df().collect())}
        now = datetime.now()
        plan = {"new": [], "changed": [], "reactivated": [], "deactivated": [], "unchanged": [], "rows": []}

        def _row(item, version, active, reason):
            return {**{k: item.get(k) for k in INPUT_COLUMNS if k not in ("change_reason",)},
                    "rule_version": version, "is_active": active, "definition_hash": compute_hash(item),
                    "valid_from": now, "change_reason": reason}

        incoming_ids = set()
        for it in incoming:
            rid = it["rule_id"]
            incoming_ids.add(rid)
            h = compute_hash(it)
            ex = existing.get(rid)
            reason = it.get("change_reason") or change_reason
            if ex is None:
                plan["new"].append(rid)
                plan["rows"].append(_row(it, 1, True, reason))
            elif not ex["is_active"]:
                plan["reactivated"].append(rid)
                plan["rows"].append(_row(it, ex["rule_version"] + 1, True, reason or "재활성"))
            elif ex["definition_hash"] != h:
                diffs = []
                for f in _HASH_FIELDS:
                    old, new = ex.get(f), it.get(f)
                    if f in ("threshold_rate", "min_value", "max_value"):
                        old = None if old is None else round(float(old), 10)
                        new = None if new is None else round(float(new), 10)
                    if old != new:
                        diffs.append(f"{f}: {old} → {new}")
                plan["changed"].append({"rule_id": rid, "from_version": ex["rule_version"], "to_version": ex["rule_version"] + 1, "diffs": diffs})
                plan["rows"].append(_row(it, ex["rule_version"] + 1, True, reason or "; ".join(diffs)))
            else:
                plan["unchanged"].append(rid)

        if deactivate_missing:
            for rid, ex in existing.items():
                if ex["is_active"] and rid not in incoming_ids:
                    plan["deactivated"].append(rid)
                    old_item = {k: ex.get(k) for k in INPUT_COLUMNS}
                    plan["rows"].append(_row(old_item, ex["rule_version"] + 1, False, "입력 목록에서 제외되어 비활성"))
        return plan

    def sync(self, incoming: List[Dict[str, Any]], change_reason: Optional[str] = None,
             deactivate_missing: bool = True, apply: bool = True) -> Dict[str, Any]:
        """검증 -> 비교 -> (apply=True일 때만) 새 버전 행 덧붙이기. plan(요약)을 돌려준다. 바뀐 것이 없으면 아무것도 쓰지 않는다."""
        plan = self.plan_sync(incoming, deactivate_missing, change_reason)
        plan["applied"] = False
        if apply and plan["rows"]:
            catalog_schema = self.table.rsplit(".", 1)[0]
            if "." in catalog_schema:
                self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_schema}")
            df = self.spark.createDataFrame(
                [tuple(r.get(f.name) for f in RULE_DEF_SCHEMA.fields) for r in plan["rows"]], schema=RULE_DEF_SCHEMA)
            (df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(self.table))
            plan["applied"] = True
        return plan

    def sync_from_csv(self, csv_path: str, change_reason: Optional[str] = None, apply: bool = True,
                      verbose: bool = True) -> Dict[str, Any]:
        """
        Volume의 규칙 CSV를 읽어 sync()까지 한 번에 한다 (dq_rule_loader.py 셀의 로직을 재사용 가능한 함수로 옮긴 것).
        dq_run 노트북 맨 앞에서 매번 호출해도 안전하다 - sync()가 "바뀐 것만" 반영하는 멱등 함수라, 바뀐 게 없으면
        아무것도 쓰지 않는다. apply=True가 기본값인 이유도 이 때문이다(그날의 DQ 실행에 최신 CSV가 곧바로 반영되어야
        의미가 있다). 다만 무엇이 바뀌었는지는 verbose=True일 때 항상 출력한다 - 조용히 바뀌는 일이 없게 하기 위해서다.
        CSV가 일부만 담고 있으면(전체 목록이 아니면) 나머지 규칙이 비활성 처리되므로, CSV는 항상 "전체 목록"이어야 한다.
        """
        import pandas as pd

        pdf = pd.read_csv(csv_path, encoding="utf-8-sig", dtype=str, keep_default_na=False)
        missing_cols = [c for c in INPUT_COLUMNS if c not in pdf.columns]
        if missing_cols:
            raise ValueError(f"CSV에 없는 컬럼: {missing_cols}")
        items = [normalize_input(r) for r in pdf.to_dict("records")]

        plan = self.sync(items, change_reason=change_reason, apply=apply)

        if verbose:
            print(f"[DQ 규칙 동기화] {csv_path}")
            print(f"입력 {len(items)}개 | 신규 {len(plan['new'])} · 변경 {len(plan['changed'])} · 재활성 {len(plan['reactivated'])} · "
                 f"비활성 {len(plan['deactivated'])} · 변경 없음 {len(plan['unchanged'])}")
            for c in plan["changed"]:
                print(f"  ✏️  {c['rule_id']}: v{c['from_version']} → v{c['to_version']}  ({'; '.join(c['diffs'])})")
            for rid in plan["reactivated"]:
                print(f"  ♻️  {rid}: 재활성")
            for rid in plan["deactivated"]:
                print(f"  🚫 {rid}: CSV에 없어 비활성")
            if plan["new"]:
                print(f"  ➕ 신규: {', '.join(plan['new'][:10])}{' ...' if len(plan['new']) > 10 else ''}")
            if not plan["rows"]:
                print("바뀐 것이 없어 규칙 테이블에 쓰지 않았습니다.")
            elif plan["applied"]:
                print(f"✅ {self.table}에 {len(plan['rows'])}행을 새 버전으로 반영했습니다.")
            else:
                print("(미리보기) apply=False라 아직 적재하지 않았습니다.")
        return plan


# =============================================================================


# =============================================================================
# 3. 결과 리포트
# =============================================================================
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

# =============================================================================


# =============================================================================
# 4. 실행기
# =============================================================================
class DQRunner:
    """MAPS DQ Rule을 실행하고 판정 및 결과 적재를 수행합니다."""

    def __init__(self, spark: SparkSession, dbutils: Any = None, rule_table: Optional[str] = None):
        """
        meta.dq_rule_def(정본)에서 규칙별 최신 활성 버전을 읽어 실행한다.
        rule_table: 규칙 테이블 경로를 바꿔 쓸 때만 지정 (검증 셀의 dq_test 스키마 등).
        """
        self.spark = spark
        self.dbutils = dbutils
        self.rule_repo = RuleRepository(spark, rule_table)
        self.code_master_df: Optional[DataFrame] = None
        # dq_run_id는 run_table_dq() 호출마다(= 소스 테이블 × 실행마다) 새로 발급한다 (요청서 §7-3).
        # 여기 값은 execute_rule()을 run_table_dq() 밖에서 직접 호출할 때를 위한 기본값이다.
        self.dq_run_id = self._new_run_id()
        # DQ 기준서 8장 - 자동 Cleansing 엔진 (코드 마스터는 DQ 실행과 같은 캐시를 공유한다)
        self.cleansing = CleansingEngine(spark, self._load_code_master)

    def _load_rules(self, target_table: str) -> List[Dict[str, Any]]:
        """이번 실행에 쓸 규칙을 meta.dq_rule_def에서 읽는다. 규칙이 없거나 테이블이 없으면 여기서 멈춘다."""
        rules = self.rule_repo.load_current(target_table)
        if rules:
            versions = sorted({r["rule_version"] for r in rules})
            print(f"[{target_table}] DQ 규칙 {len(rules)}개 로드 (meta 테이블, 규칙 버전 {versions[0]}~{versions[-1]})")
        return rules

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
        # 규칙에 source_system 필드를 따로 안 넣어도 되게 여기서 바로 유추한다.
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

        # 자동 Cleansing 수행 여부를 action_type(임계치 판정)에 종속시키지 않는다: 정제 가능하면 임계치와
        # 무관하게 먼저 정제+재-DQ를 수행하고, "정제 후 남은 오류"를 기준으로 최종 action_type을 판정한다.
        # (정제 전 오류율로 먼저 판정하면, 오류율이 임계치 이하일 때 정제 가능한 위반도 조용히 건너뛰게 된다.)
        initial_error_count = res["error_count"]
        initial_error_rate = round(initial_error_count / res["check_count"], 6) if res["check_count"] > 0 else 0.0
        cleansing_cfg = dq_config.cleansing_config_for(rule)
        applied_df = None
        post_error_count = initial_error_count
        if initial_error_count > 0 and cleansing_cfg and res.get("error_df") is not None:
            applied_df = self.cleansing.apply(res["error_df"], rule, cleansing_cfg)
            post_error_count = applied_df.filter(~F.col(f"{_TMP}re_dq_pass")).count()

        judgment = self.judge_result(rule, res["check_count"], post_error_count)
        target_col = rule.get("column") or ", ".join(rule.get("columns", []))
        action_type = judgment["action_type"]

        # ---- 요청서 4장: dq_cleansing_detail은 "위반 이력 전체"다 ----
        # 오류가 하나라도 있으면 임계치와 무관하게(ALLOW 포함) 위반 레코드를 전부 기록한다.
        # (요청서 §1/§3.4/§9: 규칙 집계에서 행 단위 상세로 연결, 규칙별/실행 전체 고유 오류 레코드 수 산출.
        #  처리 여부는 cleansing_status / review_required_yn 으로 구분한다.)
        # 오류가 없는 Rule은 detail_df를 아예 안 쓴다 (detail_df 자체는 res에서 lazy하게만 만들어져 있음).
        detail_df = None
        unique_error_record_count = None
        auto_cleansed_record_count = None
        unresolved_record_count = None
        if res["error_count"] > 0 and res.get("detail_df") is not None:
            now = datetime.now()

            if applied_df is not None:
                # DQ 기준서 8.10: Cleansing Rule 있음 -> 자동 Cleansing -> 재-DQ (임계치 무관하게 이미 위에서 수행함)
                #   PASS -> AUTO_CLEANSED / FAIL -> 남은 위반은 (정제 후 판정된) action_type에 따라
                #   UNRESOLVED(격리) 또는 ALLOWED(허용). build_detail_df가 행 단위로 정확히 채워준다.
                base_df = self.cleansing.build_detail_df(applied_df, record_key_col, rule, action_type)
            else:
                # 정제하지 않는 경우 (Cleansing Rule 없음 - 기준서 8.9 / 애초에 정제 대상 위반이 없었음):
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
            "error_count": post_error_count,   # 정제 후 잔여 오류 기준 (judgment의 error_rate와 정합성 유지)
            "error_rate": judgment["error_rate"],
            # ---- 정제 전/후 구분 (요청 반영분). DQ_RESULT_SCHEMA(dq_config.py)엔 없는 필드라 _save_dq_results가
            # 테이블에 저장할 땐 자동으로 무시되고, 이 함수의 반환값(run_table_dq의 cleansing_specs 계산 등)에서만 쓰인다.
            "initial_error_count": initial_error_count,
            "initial_error_rate": initial_error_rate,
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
            "dq_rule_version": rule["rule_version"],   # 이 규칙 자체의 버전 (meta.dq_rule_def 기준)
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
        rules = self._load_rules(target_table)
        
        if not rules:
            print(f"[{target_table}] 적용 대상 DQ Rule이 존재하지 않습니다. (규칙의 target_table명과 활성 여부를 확인하세요)")
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

        # execute_rule()이 이제 "정제 후" action_type을 돌려주므로, 격리 판단(detail_rules/isolate_rules)은
        # 기존처럼 최종 action_type 기준을 그대로 쓴다.
        rules_by_id = {r["rule_id"]: r for r in rules}
        detail_rules = [r for r in results if r["action_type"] != "ALLOW" and (r["error_count"] or 0) > 0]
        # 격리 판정은 "정제 후" action_type이 BLOCK/REVIEW인 Rule만 (WARN은 허용이라 위반이 남아도 레코드는 통과)
        isolate_rules = [rules_by_id[r["rule_id"]] for r in detail_rules if r["action_type"] in dq_config.ISOLATE_ACTIONS]

        # 자동 Cleansing 적용 대상(cleansing_specs)은 최종 action_type과 무관하게 "정제 전(initial) 위반이 있었고
        # Cleansing Rule이 매핑된 Rule 전부"여야 한다 - 그래야 정제로 완전히 해소돼 최종 action_type이 ALLOW가 된
        # Rule도 silver_candidate에 정제값이 반영된다 (안 그러면 dq_cleansing_detail엔 AUTO_CLEANSED로 찍히는데
        # 정작 candidate 값은 원본 그대로 남는 불일치가 생긴다). 자동 Cleansing 여부와 최종 격리 여부는 별개 게이트다.
        cleansing_specs = [
            (rules_by_id[r["rule_id"]], dq_config.cleansing_config_for(rules_by_id[r["rule_id"]]))
            for r in results if (r.get("initial_error_count") or 0) > 0
            and dq_config.cleansing_config_for(rules_by_id[r["rule_id"]]) is not None
        ]
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

        # 실행 결과를 사람이 보기 좋은 콘솔 리포트로 자동 출력 (총 Rule 수, 통과/실패, 자동정제/미해결, 실패 상세)
        DQResultBuilder(self.spark, results).print_console_report()

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