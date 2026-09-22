"""
자동 Cleansing 엔진 (DQ 기준서 8장 구현)

흐름 (8.10)
    DQ ERROR 행 -> Cleansing Rule 확인 -> 자동 Cleansing -> 재-DQ
        재-DQ PASS -> AUTO_CLEANSED (Silver 후보에 정제값 반영)
        재-DQ FAIL -> 정제 실패 (re_dq_result='FAIL'). 남은 위반의 처리는 Rule의 action_type이 정한다 (기준서 §4)
            BLOCK/REVIEW -> UNRESOLVED (격리)   /   WARN -> ALLOWED (허용, 로그만)
    Cleansing Rule이 없는 오류는 이 모듈을 거치지 않고 dq_runner에서 같은 기준으로 분류한다.
    DQ 단계에는 HITL이 없다. (AI+HITL은 Gold -> Target 단계)

설계 원칙
    - 변환 함수는 전부 "Column -> Column" 순수 함수다. 같은 입력은 항상 같은 결과가 나오고(8.2.1/8.2.2),
      변환할 수 없는 값은 "원본 그대로" 돌려준다 -> 재-DQ에서 자연스럽게 FAIL -> UNRESOLVED/ALLOWED.
    - 값을 새로 만들지 않는다. NULL을 채우거나, 모르는 코드를 ETC로 바꾸는 일은 하지 않는다 (8.2.3).
    - 재-DQ는 원래 DQ Rule과 "같은 오류 조건"으로 판정한다 (dq_functions.pattern_error / code_error 공용).
    - dq_cleansing_detail에 저장하는 before/proposed/final 값은 dq_functions._mask_udf로 마스킹한다 (요청서 4.4).
      마스킹 전 원문은 Silver 후보(silver_candidate) 정제에만 쓰인다.
"""
from functools import reduce
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

try:
    import src.dq.dq_config as dq_config
    import src.dq.dq_functions as dq_functions
except ModuleNotFoundError:
    import dq_config
    import dq_functions


# 내부 작업용 컬럼 접두어 (최종 결과 DataFrame에는 남기지 않는다)
_TMP = "__cln_"

STATUS_AUTO_CLEANSED = "AUTO_CLEANSED"
STATUS_UNRESOLVED = "UNRESOLVED"   # 정제 후에도 BLOCK/REVIEW Rule 위반이 남음 -> 격리
STATUS_ALLOWED = "ALLOWED"         # 정제 후에도 WARN Rule 위반이 남음 -> 허용(로그만)


# =========================================================================
# 개별 변환 함수 (Column -> Column, 문자열 기준)
# =========================================================================
def cln_com_001_whitespace(c: Column) -> Column:
    """CLN-COM-001 공백 정규화: 앞뒤 공백 제거, 공백-only 값은 NULL. NULL은 NULL 그대로 둔다(8.4)."""
    # Spark trim()은 일반 공백(0x20)만 지우므로 탭/줄바꿈/NBSP/전각공백까지 정규식으로 처리한다.
    trimmed = F.regexp_replace(c, r"^[\s\u00A0\u3000]+|[\s\u00A0\u3000]+$", "")
    return F.when(trimmed == "", F.lit(None).cast("string")).otherwise(trimmed)


def cln_val_001_phone(c: Column) -> Column:
    """
    CLN-VAL-001 전화번호 표준화.
    숫자 + 허용 구분자(공백/점/하이픈)만으로 이루어진 휴대전화번호(01X + 7~8자리)만 변환한다.
    영문이 섞인 값, +82 같은 국가번호 변환은 업무 의미가 바뀔 수 있으므로 손대지 않는다(8.5-4).
    """
    sep = dq_config.PHONE_SEPARATOR_REGEX
    only_digits_and_separators = c.rlike(rf"^(?:[0-9]|{sep})+$")
    digits = F.regexp_replace(c, sep, "")
    is_mobile = digits.rlike(r"^01[0-9][0-9]{7,8}$")

    if dq_config.PHONE_STANDARD_FORMAT == "DIGITS":
        formatted = digits  # 구분자만 제거 (표준)
    else:  # "HYPHEN": 11자리 -> 3-4-4, 10자리 -> 3-3-4 (greedy 뒤 backtrack 으로 자동 결정)
        formatted = F.regexp_replace(digits, r"^(01[0-9])([0-9]{3,4})([0-9]{4})$", "$1-$2-$3")

    return F.when(only_digits_and_separators & is_mobile, formatted).otherwise(c)


def cln_val_002_datetime(c: Column, kind: str = "TIMESTAMP") -> Column:
    """
    CLN-VAL-002 날짜/시간 표준화.
    dq_config.*_INPUT_FORMATS 중 하나로 "정확히" 파싱되는 값만 표준 형식 문자열로 바꾼다.
    2026/13/40 같은 잘못된 날짜, '알 수 없음' 같은 값은 파싱에 실패하므로 원본 그대로 남는다(8.6-4).
    (Databricks 서버리스는 ANSI 모드라 to_timestamp는 오류를 던지므로 try_to_timestamp를 쓴다)
    """
    if kind == "DATE":
        in_formats, out_format = dq_config.DATE_INPUT_FORMATS, dq_config.DATE_STANDARD_FORMAT
    else:
        in_formats, out_format = dq_config.TIMESTAMP_INPUT_FORMATS, dq_config.TIMESTAMP_STANDARD_FORMAT

    parsed = F.coalesce(*[F.try_to_timestamp(c, F.lit(fmt)) for fmt in in_formats])
    return F.when(parsed.isNotNull(), F.date_format(parsed, out_format)).otherwise(c)


def cln_val_003_code_mapping(c: Column, mapping: Dict[str, str]) -> Column:
    """
    CLN-VAL-003 승인된 코드값 표준화.
    mapping은 "승인됨 + Source 1개당 Standard 1개 + Standard가 code_master에 존재"를 모두 통과한 것만 들어온다.
    매핑에 없는 값(예: CON_099, PLAN, 미분류)은 원본 그대로 둔다 -> 재-DQ FAIL -> 미해결.
    """
    if not mapping:
        return c
    mapped = F.coalesce(*[F.when(c == F.lit(src), F.lit(std)) for src, std in mapping.items()])
    return F.coalesce(mapped, c)


# =========================================================================
# 엔진
# =========================================================================
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
            return cln_com_001_whitespace(c)
        if step_id == "CLN-VAL-001":
            return cln_val_001_phone(c)
        if step_id == "CLN-VAL-002":
            return cln_val_002_datetime(c, cfg.get("datetime_kind", "TIMESTAMP"))
        if step_id == "CLN-VAL-003":
            return cln_val_003_code_mapping(c, self.approved_mapping(rule["code_group"]))
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