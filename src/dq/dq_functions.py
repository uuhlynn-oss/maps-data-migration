"""
DQ 검사·정제 순수 함수 모음. Spark Column을 받아 Column을 돌려주는 함수들이고, 클래스/실행 상태가 없다.
    - 검사 함수: PII 마스킹, Rule 6종(NULL/PATTERN/RANGE/ORDER/CODE/DUPLICATE)의 행 단위 오류 조건
    - 정제 함수: CLN-* 규칙 각각의 "before Column -> after Column" 변환 (같은 입력은 항상 같은 결과, 부작용 없음)
실행(재-DQ 판정, 규칙 저장소, 결과 리포트, 전체 조립)은 dq_engine.py에 있다.
"""
import re
from typing import Any, Dict

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

try:
    import src.dq.dq_config as dq_config
except ModuleNotFoundError:
    import dq_config


# 1. 검사 함수
# =============================================================================
# -------------------------------------------------------------------------
# 종옥님 요청서 4.4: 개인정보/민감정보는 Review UI에 원문으로 노출하지 않는다.
# 컬럼별 화이트리스트를 따로 관리하지 않고, 샘플 문자열이 이메일/전화번호
# "형태"이면 그 자리에서 바로 마스킹한다 (어느 check 함수를 거치든 공통 적용).
# -------------------------------------------------------------------------
_EMAIL_RE = re.compile(r"^([^@\s]{1,2})[^@\s]*(@.+)$")
# 구분자는 하이픈/공백/점을 모두 허용한다 - 자동 Cleansing(CLN-VAL-001)의 before_value에는
# "010 4568 3510", "010.4568.3510" 처럼 비표준 구분자 값이 그대로 들어오기 때문이다.
_PHONE_RE = re.compile(r"(01[0-9])[\s.\-]?(\d{3,4})[\s.\-]?(\d{4})")


def _mask_sensitive(value: Any) -> str:
    """이메일/전화번호로 보이는 문자열만 마스킹. 그 외 값은 그대로 반환."""
    if value is None:
        return None
    s = str(value)

    email_match = _EMAIL_RE.match(s)
    if email_match:
        return f"{email_match.group(1)}***{email_match.group(2)}"

    if _PHONE_RE.search(s):
        return _PHONE_RE.sub(lambda m: f"{m.group(1)}-****-{m.group(3)}", s)

    return s


def _masked_sample_values(sample_rows, columns) -> list:
    """collect()로 뽑은 샘플 Row들에서 컬럼값을 꺼내며 _mask_sensitive를 일괄 적용."""
    if isinstance(columns, str):
        return [_mask_sensitive(row[columns]) for row in sample_rows]
    return [
        str({col: _mask_sensitive(row[col]) for col in columns})
        for row in sample_rows
    ]


# -------------------------------------------------------------------------
# 종옥님 요청서 4장: dq_cleansing_detail(오류 행 단위 상세)을 만들기 위한 공통 헬퍼.
# sample_values(5건 문자열 요약)와 별개로, 오류난 행 "전체"를 source_record_key +
# target_column + before_value 3컬럼짜리 DataFrame으로 뽑아낸다.
# check 함수마다 오류 조건은 다르지만 이 마지막 정리 단계는 동일해서 공용으로 뺐다.
# 마스킹은 컬럼이 1개든 여러 개든 문자열로 합친 뒤 한 번에 적용한다.
# -------------------------------------------------------------------------
_mask_udf = F.udf(_mask_sensitive, StringType())


def _build_detail_df(error_df: DataFrame, record_key_col: str,
                      target_column_label: str, value_cols: list) -> DataFrame:
    """
    error_df: 오류로 판정된 행 전체 (샘플 아님, 전량)
    record_key_col: 이 레코드를 가리키는 업무키 컬럼명 (dq_config.TABLE_RECORD_KEY_COLUMN 참고)
    target_column_label: dq_cleansing_detail.target_column에 넣을 라벨
    value_cols: before_value를 구성할 컬럼들 (1개면 값 그대로, 여러 개면 "col=값" 나열)
    """
    if record_key_col in error_df.columns:
        key_expr = F.col(record_key_col).cast("string")
    else:
        # 업무키 컬럼이 이 Rule의 대상 df에 없는 경우 - source_record_key를 NULL로 남기고
        # Review UI에서는 target_column/before_value만으로 원인을 파악하게 한다.
        key_expr = F.lit(None).cast("string")

    if len(value_cols) == 1:
        raw_value_expr = F.col(value_cols[0]).cast("string")
    else:
        parts = [
            F.concat(F.lit(f"{c}="), F.coalesce(F.col(c).cast("string"), F.lit("NULL")))
            for c in value_cols
        ]
        raw_value_expr = parts[0]
        for p in parts[1:]:
            raw_value_expr = F.concat(raw_value_expr, F.lit(", "), p)

    return error_df.select(
        key_expr.alias("source_record_key"),
        F.lit(target_column_label).alias("target_column"),
        _mask_udf(raw_value_expr).alias("before_value"),  # PII 마스킹 (4.4) - 저장 시점에 적용
    )


# -------------------------------------------------------------------------
# 오류 조건 (단일 출처). check_* 함수, 자동 Cleansing 재-DQ(dq_cleansing), silver_candidate의
# 행 단위 검토 플래그(row_error_expr)가 모두 같은 조건을 쓰도록 여기서만 정의한다.
# -------------------------------------------------------------------------
def null_error(c: Column) -> Column:
    return c.isNull() | (c == "")


def pattern_error(c: Column, pattern: str) -> Column:
    return c.isNull() | ~c.rlike(pattern)


def range_error(c: Column, min_val, max_val) -> Column:
    return c.isNull() | (c < min_val) | (c > max_val)


def order_error(start: Column, end: Column) -> Column:
    return start.isNull() | end.isNull() | (start > end)


def code_error(c: Column, codes) -> Column:
    """코드 마스터에 없으면 오류 (NULL도 오류). codes는 해당 code_group의 유효 코드 리스트."""
    if not codes:
        return F.lit(True)
    return ~F.coalesce(c.isin(codes), F.lit(False))


def row_error_expr(rule: Dict[str, Any], codes=None) -> Column:
    """Rule 1개에 대해 "이 행이 오류인가"를 행 단위 boolean Column으로 돌려준다. (CODE_EXISTS는 codes 필요)"""
    t = rule["rule_type"]
    if t == "NULL_CHECK":
        return null_error(F.col(rule["column"]))
    if t == "PATTERN_CHECK":
        return pattern_error(F.col(rule["column"]), rule["pattern"])
    if t == "RANGE_CHECK":
        return range_error(F.col(rule["column"]), rule["min_value"], rule["max_value"])
    if t == "ORDER_CHECK":
        return order_error(F.col(rule["columns"][0]), F.col(rule["columns"][1]))
    if t == "CODE_EXISTS":
        return code_error(F.col(rule["column"]), codes)
    if t == "DUPLICATE_CHECK":
        # check_duplicate와 동일하게 NULL 키도 하나의 그룹으로 본다 (groupBy / partitionBy 모두 NULL-safe)
        return F.count(F.lit(1)).over(Window.partitionBy(*rule["columns"])) > 1
    raise ValueError(f"지원하지 않는 Rule Type입니다: {t}")


def _summarize_check(df: DataFrame, error_df: DataFrame, col_name: str,
                     record_key_col: str, dq_reason: str) -> Dict[str, Any]:
    """단일 컬럼 검사(NULL/PATTERN/RANGE/CODE_EXISTS)가 공통으로 쓰는 결과 형태를 만든다.
    error_df는 각 check 함수가 자신의 오류 조건으로 이미 필터링해 넘긴다."""
    check_count = df.count()
    error_count = error_df.count()
    sample_rows = error_df.limit(5).select(col_name).collect()
    sample_values = _masked_sample_values(sample_rows, col_name)  # 원문 대신 마스킹된 값 저장
    return {
        "check_count": check_count,
        "error_count": error_count,
        "sample_values": sample_values,
        "dq_reason": dq_reason,
        "detail_df": _build_detail_df(error_df, record_key_col, col_name, [col_name]),
        "error_df": error_df,
    }


def check_null(df: DataFrame, rule: Dict[str, Any], record_key_col: str = None) -> Dict[str, Any]:
    """NULL 값 검사"""
    col_name = rule["column"]
    error_df = df.filter(null_error(F.col(col_name)))
    return _summarize_check(df, error_df, col_name, record_key_col,
                            f"Column '{col_name}' contains NULL or empty values.")


def check_pattern(df: DataFrame, rule: Dict[str, Any], record_key_col: str = None) -> Dict[str, Any]:
    """정규식 패턴 검사"""
    col_name, pattern = rule["column"], rule["pattern"]
    error_df = df.filter(pattern_error(F.col(col_name), pattern))
    return _summarize_check(df, error_df, col_name, record_key_col,
                            f"Column '{col_name}' does not match pattern '{pattern}'.")


def check_range(df: DataFrame, rule: Dict[str, Any], record_key_col: str = None) -> Dict[str, Any]:
    """숫자 범위 검사"""
    col_name, min_val, max_val = rule["column"], rule["min_value"], rule["max_value"]
    error_df = df.filter(range_error(F.col(col_name), min_val, max_val))
    return _summarize_check(df, error_df, col_name, record_key_col,
                            f"Column '{col_name}' is out of range [{min_val}, {max_val}].")


def check_start_end_order(df: DataFrame, rule: Dict[str, Any], record_key_col: str = None) -> Dict[str, Any]:
    """시작/종료 시각 순서 검사 (주어진 컬럼 순서가 거꾸로 된 경우 에러)"""
    columns = rule["columns"]  # 예: [started_at, ended_at]
    start_col = columns[0]
    end_col = columns[1]
    check_count = df.count()
    
    # 시작 시간이 종료 시간보다 나중이거나, 어느 하나라도 누락된 경우
    error_df = df.filter(order_error(F.col(start_col), F.col(end_col)))
    error_count = error_df.count()
    
    sample_rows = error_df.limit(5).select(start_col, end_col).collect()
    # 날짜/시각 컬럼이라 원칙적으로 PII는 아니지만, 다른 check 함수들과 마스킹 경로를 통일해둔다
    sample_values = [f"{_mask_sensitive(row[0])} ~ {_mask_sensitive(row[1])}" for row in sample_rows]
    
    return {
        "check_count": check_count,
        "error_count": error_count,
        "sample_values": sample_values,
        "dq_reason": f"Order violation: '{start_col}' is later than '{end_col}'.",
        "detail_df": _build_detail_df(error_df, record_key_col, f"{start_col},{end_col}", [start_col, end_col]),
        "error_df": error_df,
    }

def check_code_exists(df: DataFrame, rule: Dict[str, Any], code_master_df: DataFrame,
                       record_key_col: str = None) -> Dict[str, Any]:
    """코드 마스터 존재 여부 검사 (Anti-Join 활용)"""
    col_name, code_group = rule["column"], rule["code_group"]
    filtered_master = code_master_df.filter(F.col("CODE_GROUP") == code_group).select("CODE").distinct()
    # 마스터에 존재하지 않는 값들을 에러로 판단 (Left Anti Join)
    error_df = df.join(filtered_master, df[col_name] == filtered_master["CODE"], "left_anti")
    return _summarize_check(df, error_df, col_name, record_key_col,
                            f"Value in '{col_name}' does not exist in CODE_MASTER (Group: {code_group}).")

def check_duplicate(df: DataFrame, rule: Dict[str, Any], record_key_col: str = None) -> Dict[str, Any]:
    """중복 데이터 검사"""
    columns = rule["columns"]
    check_count = df.count()
    
    # 복합키 또는 단일키 기준으로 그룹바이하여 2번 이상 나타나는 데이터 색출
    grouped = df.groupBy(columns).count().filter(F.col("count") > 1)
    error_count = grouped.count()
    
    sample_rows = grouped.limit(5).select(columns).collect()
    sample_values = _masked_sample_values(sample_rows, columns)  # 업무키에 전화번호 등이 섞여도 안전하게 마스킹

    # detail_df는 "중복된 키 종류"가 아니라 "그 키 때문에 중복된 원본 행 전체"가 필요하다.
    # grouped(키 목록)를 df와 다시 join해서 실제로 중복에 걸린 행들을 복원한다.
    # 그룹 수는 보통 적으므로 broadcast join으로 셔플을 줄인다 (서버리스에서도 가벼움).
    affected_rows_df = df.join(F.broadcast(grouped.select(columns)), on=columns, how="inner")
    detail_df = _build_detail_df(affected_rows_df, record_key_col, ",".join(columns), columns)
    
    return {
        "check_count": check_count,
        "error_count": error_count,
        "sample_values": sample_values,
        "dq_reason": f"Duplicate records found for columns: {columns}.",
        "detail_df": detail_df,
        "error_df": affected_rows_df,
    }

# =============================================================================
# 2. 정제 변환 함수 (CleansingEngine이 dq_engine.py에서 이 함수들을 불러 쓴다)
# =============================================================================
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
    - 재-DQ는 원래 DQ Rule과 "같은 오류 조건"으로 판정한다 (pattern_error / code_error 공용).
    - dq_cleansing_detail에 저장하는 before/proposed/final 값은 _mask_udf로 마스킹한다 (요청서 4.4).
      마스킹 전 원문은 Silver 후보(silver_candidate) 정제에만 쓰인다.
"""


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

    슬래시 구분에 월/일이 한 자리인 값("1990/3/15")은 문자열 0-패딩으로 먼저 "1990/03/15"로 맞춘 뒤 아래
    2자리 포맷으로 파싱한다. try_to_timestamp에 "yyyy/M/d"(한 자리 허용) 같은 패턴을 직접 추가하면 Spark의
    신/구 파서 불일치 검사에 걸려 try_to_timestamp가 예외를 삼키지 않고 그대로 던져 배치 전체가 죽는다
    (실측 확인: SparkUpgradeException/PARSE_DATETIME_BY_NEW_PARSER) - 그래서 패턴을 늘리는 대신 문자열을 미리 맞춘다.
    """
    if kind == "DATE":
        in_formats, out_format = dq_config.DATE_INPUT_FORMATS, dq_config.DATE_STANDARD_FORMAT
    else:
        in_formats, out_format = dq_config.TIMESTAMP_INPUT_FORMATS, dq_config.TIMESTAMP_STANDARD_FORMAT

    slash_parts = F.split(c, "/")
    padded = F.when(
        F.size(slash_parts) == 3,
        F.concat_ws("/", slash_parts.getItem(0),
                   F.lpad(slash_parts.getItem(1), 2, "0"), F.lpad(slash_parts.getItem(2), 2, "0"))
    ).otherwise(c)

    parsed = F.coalesce(*[F.try_to_timestamp(padded, F.lit(fmt)) for fmt in in_formats])
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