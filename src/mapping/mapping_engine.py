"""
Silver -> Gold Mapping Execution 엔진.

역할
    Mapping Definition(메타데이터)을 읽어 Silver 데이터를 Target 모양(gold_candidate)으로 변환한다.
    Target Validation과 gold_quarantine(gold_validation_runner.py)이 이 다음 단계이며, 이 엔진은 그 입력이 될 표시만 남긴다.
        _map_errors      값 변환에 실패한 Target 컬럼 (예: 시각 형식이 맞지 않음)
        _unmapped_codes  승인된 코드 매핑이 없어 NULL이 된 값 (예: CNSL_TYPE_CD=CON_099)

지원하는 (MAPPING_TYPE, PROCESS_TYPE)
    (RENAME | DIRECT, COPY)   값 복사
    (TRANSFORM, FORMAT)       TIMESTAMP: 원천 시간대의 시각 문자열 -> UTC 시각 (TO-BE 물리 정책)
    (CODE, LOOKUP)            승인된 AS-IS -> TO-BE 코드 변환. 매핑이 없으면 NULL + _unmapped_codes 기록
    (DERIVED, CONSTANT)       고정값 (코드 매핑표의 (SOURCE_SYSTEM) 행)
    그 밖(CUSTOMER_MATCH, PRODUCT_MAPPING 조회, GENERATE_ID, EXPLODE ...)은 이번 단계에서 NULL로 두고
    summary["deferred_columns"]에 사유와 함께 기록한다. (정의가 잘못된 경우와 달리 실행을 막지 않는다)

정의(Mapping Definition, Target Model, 코드 매핑)가 잘못된 경우에는 일부만 변환해서 내보내지 않고 ValueError로 즉시 멈춘다.
"""
import hashlib
import uuid
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

try:
    import src.mapping.mapping_config as cfg
except ModuleNotFoundError:
    import mapping_config as cfg


_LINEAGE_COLS = ["_source_system", "_source_table", "_source_record_key", "_source_batch_id", "_dq_run_id"]


def _spark_type(data_type: str) -> str:
    t = data_type.strip().upper()
    if t.startswith("VARCHAR") or t.startswith("CHAR") or t == "TEXT":
        return "string"
    if t == "TIMESTAMP":
        return "timestamp"
    if t == "DATE":
        return "date"
    if t == "INTEGER":
        return "int"
    if t.startswith("DECIMAL"):
        return t.lower().replace(" ", "")
    raise ValueError(f"지원하지 않는 Target 타입입니다: {data_type}")


def _norm_type(data_type: str) -> str:
    return "".join(str(data_type).upper().split())


def _ver(v: Any) -> float:
    try:
        return float(str(v).strip())
    except ValueError:
        return 0.0


def _join_when(parts: List[Column]) -> Column:
    """when(...) 조각들을 콤마로 잇고, 하나도 없으면 NULL."""
    if not parts:
        return F.lit(None).cast("string")
    joined = F.concat_ws(",", *parts)
    return F.when(F.length(joined) > 0, joined)


class MappingEngine:
    def __init__(self, spark: SparkSession, target_model_df: DataFrame,
                 mapping_def_df: DataFrame, code_map_df: DataFrame):
        """세 메타데이터를 DataFrame으로 받는다 (테스트에서는 인라인 DataFrame, 운영에서는 from_tables 사용)."""
        self.spark = spark
        self._target_model = target_model_df
        self._defs = mapping_def_df
        self._code_map = code_map_df

    @classmethod
    def from_tables(cls, spark: SparkSession) -> "MappingEngine":
        return cls(
            spark,
            spark.read.table(cfg.TARGET_MODEL_TABLE),
            spark.read.table(cfg.MAPPING_DEFINITION_TABLE),
            spark.read.table(cfg.CODE_MAPPING_TABLE),
        )

    # ------------------------------------------------------------------
    # 메타데이터 로딩
    # ------------------------------------------------------------------
    def _target_columns(self, target_table: str) -> List[Dict[str, Any]]:
        df = self._target_model.filter(F.upper(F.trim(F.col("TABLE_NAME"))) == target_table.upper())
        if "ORDINAL" in df.columns:
            df = df.orderBy(F.col("ORDINAL").cast("int"))
        rows = [r.asDict() for r in df.collect()]
        if not rows:
            raise ValueError(f"TO-BE 모델에 Target 테이블 '{target_table}'이(가) 없습니다.")
        return rows

    def _load_definitions(self, source_system: str, target_table: str) -> List[Dict[str, Any]]:
        # mapping_seed_loader.py가 적재 시 MAPPING_ID 중복을 이미 거부하므로, 여기서는 버전 선택 없이 그대로 모은다.
        # (mapping_definition이 dq_rule_def처럼 진짜 버전 관리 테이블이 되면, 그때 "최신 버전만 선택"을 다시 넣는다.)
        df = (
            self._defs
            .filter(F.upper(F.trim(F.col("SOURCE_SYSTEM"))) == source_system.upper())
            .filter(F.upper(F.trim(F.col("TARGET_TABLE"))) == target_table.upper())
            .filter(F.upper(F.trim(F.col("FINAL_MIGRATION_APPLY_YN"))) == "Y")
            .filter(F.upper(F.trim(F.col("REVIEW_STATUS"))).isin(*cfg.APPLY_REVIEW_STATUSES))
        )
        defs = [{k: (v.strip() if isinstance(v, str) else v) for k, v in r.asDict().items()} for r in df.collect()]
        return sorted(defs, key=lambda d: d["MAPPING_ID"])

    def _code_entries(self, code_mapping_system: str, source_column: str, target_group: str) -> Dict[str, str]:
        """승인(APPROVED)되고 TARGET_CODE가 있는 AS-IS -> TO-BE 코드만. 한 코드가 서로 다른 Target으로 가면 정의 오류."""
        rows = (
            self._code_map
            .filter(F.trim(F.col("SOURCE_SYSTEM")) == code_mapping_system)
            .filter(F.trim(F.col("SOURCE_COLUMN")) == source_column)
            .filter(F.trim(F.col("TARGET_CODE_GROUP_ID")) == target_group)
            .filter(F.upper(F.trim(F.col("MAPPING_STATUS"))) == "APPROVED")
            .filter(F.col("TARGET_CODE").isNotNull() & (F.trim(F.col("TARGET_CODE")) != ""))
            .select(F.trim(F.col("SOURCE_CODE")).alias("s"), F.trim(F.col("TARGET_CODE")).alias("t"))
            .distinct().collect()
        )
        out: Dict[str, str] = {}
        for r in rows:
            if r["s"] in out and out[r["s"]] != r["t"]:
                raise ValueError(f"코드 매핑 충돌: {code_mapping_system}.{source_column}의 '{r['s']}'가 "
                                 f"'{out[r['s']]}'와 '{r['t']}'로 동시에 매핑됨")
            out[r["s"]] = r["t"]
        return out

    # ------------------------------------------------------------------
    # 정의 검증 (잘못되면 일부만 내보내지 않고 멈춘다)
    # ------------------------------------------------------------------
    def _validate(self, defs: List[Dict[str, Any]], target_cols: List[Dict[str, Any]], silver_df: DataFrame) -> None:
        errors: List[str] = []
        by_name = {c["COLUMN_NAME"]: c for c in target_cols}
        seen: Dict[str, str] = {}
        for d in defs:
            t = d["TARGET_COLUMN"]
            if t not in by_name:
                errors.append(f"{d['MAPPING_ID']}: Target 컬럼 '{t}'이(가) TO-BE 모델에 없음")
            elif _norm_type(d["TARGET_DATATYPE"]) != _norm_type(by_name[t]["DATA_TYPE"]):
                errors.append(f"{d['MAPPING_ID']}: 타입 불일치 (매핑 {d['TARGET_DATATYPE']} / 모델 {by_name[t]['DATA_TYPE']})")
            if t in seen:
                errors.append(f"Target 컬럼 '{t}'에 매핑이 둘 이상: {seen[t]}, {d['MAPPING_ID']}")
            seen[t] = d["MAPPING_ID"]
            src = d["SOURCE_COLUMN"]
            if src not in cfg.CONSTANT_SOURCE_COLUMNS and src not in silver_df.columns:
                errors.append(f"{d['MAPPING_ID']}: Source 컬럼 '{src}'이(가) Silver 입력에 없음")
        if errors:
            raise ValueError("Mapping Definition 오류:\n  - " + "\n  - ".join(errors))

    # ------------------------------------------------------------------
    # 변환 핸들러: (값 Column, 변환 실패 Column|None, 미매핑 코드 Column|None)
    # ------------------------------------------------------------------
    def _copy(self, d, name, stype, ctx):
        return F.col(d["SOURCE_COLUMN"]).cast(stype), None, None

    def _format_date(self, d, name, stype, ctx):
        s = F.trim(F.col(d["SOURCE_COLUMN"]))
        # DQ Cleansing(CLN-VAL-002)이 Silver 적재 전에 표준 포맷(yyyy-MM-dd)으로 정규화해주므로 Mapping은 그
        # 표준 포맷만 파싱한다 (새 정규화 로직을 만들지 않는다). plain cast(_copy)를 안 쓰는 이유: ANSI 모드에서
        # malformed 값을 cast하면 NULL이 아니라 예외가 나서 배치 전체가 죽는다 - try_to_timestamp는 예외 대신
        # NULL을 돌려주고(이 환경에 try_to_date가 없어 대신 사용, 결과는 동일), 2/30처럼 실존하지 않는 날짜도
        # 달력 유효성까지 포함해 정확히 NULL로 거부한다(실측 확인됨).
        parsed = F.try_to_timestamp(s, F.lit(cfg.SOURCE_DATE_FORMAT)).cast(stype)
        failed = s.isNotNull() & (s != "") & parsed.isNull()
        return parsed, failed, None

    def _format_timestamp(self, d, name, stype, ctx):
        s = F.trim(F.col(d["SOURCE_COLUMN"]))
        tz = F.lit(ctx["source_tz"])
        # 시간대를 문자열에 붙여 "명시적으로" 파싱한다 -> 세션 시간대와 무관하게 정확한 절대 시각(UTC 기준)이 저장된다.
        # (try_to_timestamp: 서버리스의 ANSI 모드에서도 형식 오류를 예외 대신 NULL로 돌려준다)
        # DQ Cleansing(CLN-VAL-002)이 모든 소스를 표준 포맷으로 정규화해주므로 원천 포맷별 예외 처리는 필요 없다.
        # 다만 날짜만 있는 값(DQ의 DATE_STANDARD_FORMAT "yyyy-MM-dd")은 DQ가 시각을 추정하지 않고 그대로 두므로,
        # Target이 TIMESTAMP인 컬럼에서는 그 값을 자정(00:00:00)으로 채우는 구조적 변환만 여기서 한다.
        attempts = [
            F.try_to_timestamp(F.concat_ws(" ", s, tz), F.lit(f"{cfg.SOURCE_TIMESTAMP_FORMAT} VV")),
            F.try_to_timestamp(F.concat_ws(" ", s, tz), F.lit("yyyy-MM-dd VV")),
        ]
        parsed = F.coalesce(*attempts).cast(stype)   # 세션의 timestamp/timestamp_ntz 설정과 무관하게 Target Model 타입으로 고정
        failed = s.isNotNull() & (s != "") & parsed.isNull()
        return parsed, failed, None

    def _code_lookup(self, d, name, stype, ctx):
        entries = self._code_entries(ctx["code_mapping_system"], d["SOURCE_COLUMN"], name)
        src = F.trim(F.col(d["SOURCE_COLUMN"]))
        if entries:
            m = F.create_map(*[F.lit(x) for kv in entries.items() for x in kv])
            mapped = F.try_element_at(m, src)
        else:
            mapped = F.lit(None).cast("string")
        unmapped = F.when(src.isNotNull() & (src != "") & mapped.isNull(), F.concat(F.lit(f"{name}="), src))
        return mapped.cast(stype), None, unmapped

    def _constant(self, d, name, stype, ctx):
        rows = (
            self._code_map
            .filter(F.trim(F.col("SOURCE_SYSTEM")) == ctx["code_mapping_system"])
            .filter(F.trim(F.col("SOURCE_COLUMN")).isin(*cfg.CONSTANT_SOURCE_COLUMNS))
            .filter(F.trim(F.col("TARGET_CODE_GROUP_ID")) == name)
            .filter(F.upper(F.trim(F.col("MAPPING_STATUS"))) == "APPROVED")
            .select(F.trim(F.col("TARGET_CODE")).alias("t")).distinct().collect()
        )
        if len(rows) != 1:
            raise ValueError(f"{d['MAPPING_ID']}: 고정값({name})을 코드 매핑표의 (SOURCE_SYSTEM) 행에서 "
                             f"정확히 1개 찾아야 하는데 {len(rows)}개임")
        return F.lit(rows[0]["t"]).cast(stype), None, None

    def _dispatch(self, d, stype) -> Optional[Callable]:
        key = (str(d["MAPPING_TYPE"]).upper(), str(d["PROCESS_TYPE"]).upper())
        # Target이 timestamp면 선언된 MAPPING_TYPE(TRANSFORM/FORMAT 뿐 아니라 DIRECT/COPY, RENAME/COPY도)과 무관하게
        # 항상 시간대를 명시해 파싱한다. _copy가 먼저 걸리면 F.col(...).cast("timestamp")로 세션 시간대에 의존하게 되어
        # KST 원본이 변환 없이 그대로 들어가는 사고(아웃바운드 CALL_ST_DTM/CALL_END_DTM이 DIRECT/COPY로 선언된 경우 실제 재현됨)가 난다.
        if stype == "timestamp" and key in (("RENAME", "COPY"), ("DIRECT", "COPY"), ("TRANSFORM", "FORMAT")):
            return self._format_timestamp
        # DATE도 timestamp와 같은 이유로 별도 처리한다: plain cast(_copy)는 ANSI 모드에서 malformed 값에
        # 예외를 던져 배치 전체를 죽인다 (이번에 실제로 재현된 문제).
        if stype == "date" and key in (("RENAME", "COPY"), ("DIRECT", "COPY"), ("TRANSFORM", "FORMAT")):
            return self._format_date
        # TRANSFORM/FORMAT이 timestamp/date가 아닌 VARCHAR 등이면 여기로 온다. DQ Cleansing이 Silver 적재 전에
        # 이미 표준 포맷으로 정규화해주므로(CLN-VAL-001 전화번호, CLN-VAL-002 날짜 - dq_config.py 참고), Mapping은
        # 별도 파싱 없이 캐스트만 하면 된다(_copy와 동일) - 예전엔 여기가 None(미지원)이라 BRTH_DT/TEL_NO가 항상
        # NULL로 나갔었다.
        if key in (("RENAME", "COPY"), ("DIRECT", "COPY"), ("TRANSFORM", "FORMAT")):
            return self._copy
        if key == ("CODE", "LOOKUP"):
            return self._code_lookup
        if key == ("DERIVED", "CONSTANT"):
            return self._constant
        return None

    # ------------------------------------------------------------------
    # 실행
    # ------------------------------------------------------------------
    def discover_target_tables(self) -> List[str]:
        """meta.mapping_definition에 승인된 매핑이 있는 Target 중, 이 엔진이 지금 실행 가능한 것만 돌려준다.
        메타데이터에 있는 Target을 그대로 다 내보내지 않고 cfg.SUPPORTED_TARGET_TABLES와 교집합을 취하는 게 핵심이다.
        1:1 변환만 지원하는 이 엔진으로 CUSTOMER/CONTRACT(여러 소스 행이 하나로 합쳐져야 하는 테이블)를 그냥 실행하면
        레코드가 중복되거나 빈 행이 생긴다(실제로 재현된 문제). SUPPORTED_TARGET_TABLES가 그 사고를 막는 장치이므로,
        자동 탐색이 이 장치를 우회하지 않도록 여기서 항상 교집합을 취한다.

        정렬은 알파벳순이 아니라 mapping_definition.TARGET_LOAD_ORDER 기준이다 (10 상품/코드, 20 CUSTOMER, 30 CONTRACT,
        40 COUNSEL, ...). CONTRACT가 CUSTOMER를 NOT NULL FK로 참조하는 등 Target 간 참조 관계가 있어서, 알파벳순으로
        돌리면(CONTRACT가 CUSTOMER보다 먼저) 부모가 없는 상태에서 자식을 실행하게 된다."""
        rows = (
            self._defs
            .filter(F.upper(F.trim(F.col("FINAL_MIGRATION_APPLY_YN"))) == "Y")
            .filter(F.upper(F.trim(F.col("REVIEW_STATUS"))).isin(*cfg.APPLY_REVIEW_STATUSES))
            .select(F.upper(F.trim(F.col("TARGET_TABLE"))).alias("t"), F.col("TARGET_LOAD_ORDER").alias("o"))
            .distinct().collect()
        )
        order_by_target: Dict[str, float] = {}
        for r in rows:
            o = _ver(r["o"])   # 같은 Target이 여러 행에 걸쳐 있어도 보통 값이 하나다. 혹시 다르면 더 이른(작은) 순서를 취한다.
            if r["t"] not in order_by_target or o < order_by_target[r["t"]]:
                order_by_target[r["t"]] = o
        found = set(order_by_target)
        supported = [t for t in cfg.SUPPORTED_TARGET_TABLES if t in found]
        return sorted(supported, key=lambda t: (order_by_target[t], t))

    def discover_sources(self, target_table: str) -> List[str]:
        """이 Target으로 승인된 매핑(FINAL_MIGRATION_APPLY_YN=Y, REVIEW_STATUS 승인)이 있는 소스 목록을 meta.mapping_definition에서
        확정한다. 노트북에 소스를 하드코딩하지 않고, 정의가 바뀌면(새 소스 추가·제외) 이 목록도 같이 바뀌게 하기 위함."""
        target_table = target_table.upper()
        rows = (
            self._defs
            .filter(F.upper(F.trim(F.col("TARGET_TABLE"))) == target_table)
            .filter(F.upper(F.trim(F.col("FINAL_MIGRATION_APPLY_YN"))) == "Y")
            .filter(F.upper(F.trim(F.col("REVIEW_STATUS"))).isin(*cfg.APPLY_REVIEW_STATUSES))
            .select(F.upper(F.trim(F.col("SOURCE_SYSTEM"))).alias("s")).distinct().collect()
        )
        found = {r["s"] for r in rows}
        return sorted(s for s in cfg.SOURCE_SYSTEMS if s in found)   # SOURCE_SYSTEMS에 등록된 순서를 따른다 (안정적인 실행 순서)

    def run(self, source_system: str, target_table: str, silver_df: Optional[DataFrame] = None,
            source_batch_id: Optional[str] = None) -> Tuple[DataFrame, Dict[str, Any]]:
        """
        Silver -> gold_candidate 변환 (저장하지 않고 DataFrame과 summary를 돌려준다. 저장은 save()).
        silver_df를 주지 않으면 silver_candidate.<source>에서 source_batch_id(미지정 시 최신 배치)를 읽는다.
        """
        source_system = source_system.upper()
        target_table = target_table.upper()
        ss = cfg.SOURCE_SYSTEMS[source_system]
        if target_table not in cfg.SUPPORTED_TARGET_TABLES:
            raise NotImplementedError(
                f"{target_table}은(는) 아직 실행할 수 없습니다 (지원: {cfg.SUPPORTED_TARGET_TABLES}). "
                f"여러 소스 행이 하나의 개체로 합쳐지는 테이블이라 중복 제거와 개체 통합이 먼저 필요합니다.")

        if silver_df is None:
            silver_df = self.spark.read.table(cfg.silver_input_table(ss["silver"]))
            if source_batch_id is None:
                source_batch_id = silver_df.agg(F.max("_source_batch_id")).collect()[0][0]
            silver_df = silver_df.filter(F.col("_source_batch_id") == source_batch_id)
        elif source_batch_id is not None:
            silver_df = silver_df.filter(F.col("_source_batch_id") == source_batch_id)

        target_cols = self._target_columns(target_table)
        defs = self._load_definitions(source_system, target_table)
        if not defs:
            raise ValueError(f"실행할 Mapping Definition이 없습니다 ({source_system} -> {target_table}). "
                             f"FINAL_MIGRATION_APPLY_YN = 'Y' 이고 REVIEW_STATUS가 {cfg.APPLY_REVIEW_STATUSES}인 행이 필요합니다.")
        self._validate(defs, target_cols, silver_df)

        ctx = {
            "code_mapping_system": ss["code_mapping"],
            "source_tz": cfg.SOURCE_TIMEZONE.get(source_system, cfg.DEFAULT_SOURCE_TIMEZONE),
        }
        by_target = {d["TARGET_COLUMN"]: d for d in defs}

        value_cols: List[Column] = []
        error_parts: List[Column] = []
        unmapped_parts: List[Column] = []
        applied: List[str] = []
        deferred: List[Tuple[str, str]] = []

        for col in target_cols:
            name, stype = col["COLUMN_NAME"], _spark_type(col["DATA_TYPE"])
            d = by_target.get(name)
            if d is None:
                value_cols.append(F.lit(None).cast(stype).alias(name))
                deferred.append((name, "매핑 행 없음"))
                continue
            handler = self._dispatch(d, stype)
            if handler is None:
                value_cols.append(F.lit(None).cast(stype).alias(name))
                deferred.append((name, f"이번 단계 미지원 ({d['MAPPING_TYPE']}/{d['PROCESS_TYPE']})"))
                continue
            value, failed, unmapped = handler(d, name, stype, ctx)
            value_cols.append(value.alias(name))
            applied.append(name)
            if failed is not None:
                error_parts.append(F.when(failed, F.lit(name)))
            if unmapped is not None:
                unmapped_parts.append(unmapped)

        run_id = f"MAP-{datetime.now():%Y%m%d%H%M%S}-{ss['silver']}-{target_table.lower()}-{uuid.uuid4().hex[:6]}"
        version_tokens = sorted(f"{d['MAPPING_ID']}@{d['VERSION']}" for d in defs)
        mapping_version = f"{max(_ver(d['VERSION']) for d in defs)}#{hashlib.sha1('|'.join(version_tokens).encode()).hexdigest()[:8]}"

        lineage = [
            (F.col(c) if c in silver_df.columns else F.lit(None).cast("string")).alias(c) for c in _LINEAGE_COLS
        ]
        candidate = silver_df.select(
            *value_cols,
            *lineage,
            F.lit(run_id).alias("_mapping_run_id"),
            F.lit(mapping_version).alias("_mapping_version"),
            F.lit(datetime.now()).alias("_mapped_at"),
            _join_when(error_parts).alias("_map_errors"),
            _join_when(unmapped_parts).alias("_unmapped_codes"),
        )

        summary = self._summarize(candidate, silver_df, run_id, mapping_version, source_system, target_table,
                                  ss["silver"], source_batch_id, applied, deferred)
        return candidate, summary

    def _summarize(self, candidate, silver_df, run_id, mapping_version, source_system, target_table,
                   silver_source, source_batch_id, applied, deferred) -> Dict[str, Any]:
        input_count = silver_df.count()
        agg = candidate.agg(
            F.count("*").alias("n"),
            F.sum(F.when(F.col("_map_errors").isNotNull(), 1).otherwise(0)).alias("err_rows"),
            F.sum(F.when(F.col("_unmapped_codes").isNotNull(), 1).otherwise(0)).alias("unmapped_rows"),
        ).collect()[0]
        err_by_col = {
            r["c"]: r["cnt"] for r in
            candidate.filter(F.col("_map_errors").isNotNull())
            .select(F.explode(F.split(F.col("_map_errors"), ",")).alias("c"))
            .groupBy("c").agg(F.count("*").alias("cnt")).collect()
        }
        unmapped_by_value = {
            r["v"]: r["cnt"] for r in
            candidate.filter(F.col("_unmapped_codes").isNotNull())
            .select(F.explode(F.split(F.col("_unmapped_codes"), ",")).alias("v"))
            .groupBy("v").agg(F.count("*").alias("cnt")).collect()
        }
        return {
            "mapping_run_id": run_id,
            "mapping_version": mapping_version,
            "source_system": source_system,
            "silver_source": silver_source,
            "target_table": target_table,
            "source_batch_id": source_batch_id,
            "input_count": input_count,
            "output_count": int(agg["n"]),                    # 1:1 변환이라 input_count와 같아야 한다
            "applied_columns": applied,
            "deferred_columns": deferred,                      # (Target 컬럼, 사유) - 이번 단계에서 NULL로 둔 컬럼
            "conversion_error_rows": int(agg["err_rows"] or 0),
            "conversion_errors_by_column": err_by_col,
            "unmapped_code_rows": int(agg["unmapped_rows"] or 0),
            "unmapped_codes": unmapped_by_value,               # {"CNSL_TYPE_CD=CON_099": 4, ...}
        }

    # 저장: gold_candidate.<target> 물리 테이블에 같은 소스·배치만 교체 (기본 흐름).
    # 서버리스는 전역 임시 뷰를 지원하지 않고 세션 간 데이터 전달에 물리 테이블/세션 임시 뷰를 권장하므로,
    # 노트북이 분리돼 있어도 이어지도록 처음부터 물리 테이블로 간다.
    #
    # _map_errors가 있는 행(값 변환 실패)은 gold_candidate에 넣지 않고 gold_mapping_error로 보낸다.
    # Gold Validation은 gold_candidate만 입력으로 쓰므로, 이렇게 하면 Mapping 실패가 Gold Validation에서
    # 다시 걸려 gold_quarantine으로 이중 격리되는 일이 생기지 않는다. (_unmapped_codes는 격리 대상이 아니라
    # 그대로 gold_candidate에 남는다 - mapping_config.UNMAPPED_CODE_POLICY 참고)
    # ------------------------------------------------------------------
    def save(self, candidate: DataFrame, summary: Dict[str, Any]) -> str:
        target_table = summary["target_table"]
        table = cfg.gold_candidate_table(target_table)
        error_table = cfg.gold_mapping_error_table(target_table)
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.UC_CATALOG}.{cfg.GOLD_CANDIDATE_SCHEMA}")
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.UC_CATALOG}.{cfg.GOLD_MAPPING_ERROR_SCHEMA}")

        clean = candidate.filter(F.col("_map_errors").isNull())
        errors = candidate.filter(F.col("_map_errors").isNotNull())

        # 여러 소스가 같은 테이블을 공유하므로 (소스, 배치) 단위로만 교체한다 - 두 테이블 모두 같은 정책
        predicate = f"_source_system = '{summary['silver_source']}' AND _source_batch_id = '{summary['source_batch_id']}'"
        for df, tbl in ((clean, table), (errors, error_table)):
            if not self.spark.catalog.tableExists(tbl):
                (df.limit(0).write.format("delta").mode("overwrite")
                 .partitionBy("_source_batch_id").saveAsTable(tbl))
            (df.write.format("delta").mode("overwrite")
             .option("replaceWhere", predicate).option("mergeSchema", "true").saveAsTable(tbl))

        summary["mapping_error_rows_excluded"] = errors.count()
        return table

    # ------------------------------------------------------------------
    # Entity Integration: Column Mapping(run/save)과 분리된 Entity 단위 통합.
    # meta.entity_integration_definition(AI 생성 + HITL 승인, mapping_seed_loader.py가 적재)을 target_table
    # 기준으로 찾아 동적으로 해석한다 - 특정 target_table이나 통합 방식을 코드에 하드코딩하지 않는다.
    # 모든 소스를 이 target으로 Mapping+save()한 뒤, 이 target에 대해 한 번 호출한다. DIRECT/UNION은 지금 저장
    # 구조(소스별 replaceWhere)가 이미 그 결과이므로 아무것도 하지 않는다(no-op). MERGE만 실제로 통합한다.
    # ------------------------------------------------------------------
    def _integration_spec(self, target_table: str) -> Optional[Dict[str, Any]]:
        if not self.spark.catalog.tableExists(cfg.ENTITY_INTEGRATION_TABLE):
            return None
        rows = (self.spark.read.table(cfg.ENTITY_INTEGRATION_TABLE)
               .filter(F.upper(F.trim("TARGET_ENTITY")) == target_table.upper())
               .filter(F.upper(F.trim("FINAL_MIGRATION_APPLY_YN")) == "Y")
               .filter(F.upper(F.trim("REVIEW_STATUS")).isin(*cfg.APPLY_REVIEW_STATUSES))
               .collect())
        return rows[0].asDict() if rows else None

    def _reference_timestamp(self, ref_table: str, ref_column: str) -> Optional[DataFrame]:
        """CONFLICT_REFERENCE("TARGET_TABLE.TARGET_COLUMN")가 가리키는 컬럼의 원본을, 그 컬럼에 이미 정의된
        (source_system별) mapping_definition을 그대로 재사용해 Silver에서 읽어온다 - 새 정규화 로직을 만들지
        않는다. (source_system, source_batch_id, source_record_key) 단위로 값을 돌려준다."""
        out = None
        for source_system in cfg.SOURCE_SYSTEMS:
            defs = [d for d in self._load_definitions(source_system, ref_table) if d["TARGET_COLUMN"] == ref_column]
            if not defs:
                continue
            d = defs[0]
            stype = _spark_type(next(c["DATA_TYPE"] for c in self._target_columns(ref_table)
                                     if c["COLUMN_NAME"] == ref_column))
            handler = self._dispatch(d, stype)
            if handler is None:
                continue
            silver_df = self.spark.read.table(cfg.silver_input_table(cfg.SOURCE_SYSTEMS[source_system]["silver"]))
            ctx = {"source_tz": cfg.SOURCE_TIMEZONE.get(source_system, cfg.DEFAULT_SOURCE_TIMEZONE)}
            value, _, _ = handler(d, ref_column, stype, ctx)
            part = silver_df.select("_source_system", "_source_batch_id", "_source_record_key",
                                    value.alias("_ref_ts"))
            out = part if out is None else out.unionByName(part)
        return out

    def _assign_customer_ids(self, df: DataFrame) -> DataFrame:
        """CUST_ID가 없는(=한 번도 배정 안 된 신규) 행에만 'CUST-000001' 형식으로 순번을 채운다 (PoC 수준).
        전체에 row_number()를 다시 매기지 않고, 기존 CUST_ID 중 최대 번호 다음부터 신규 행에만 이어서 부여한다
        - 이미 배정된 값이 있는 행은 이 함수에 오기 전에(fill_cols의 first(ignorenulls=True)) 이미 보존되어
        CUST_ID가 채워진 채로 들어오므로 여기서는 손대지 않는다."""
        if "CUST_ID" not in df.columns:
            return df
        existing_n = (df.filter(F.col("CUST_ID").isNotNull())
                     .select(F.regexp_extract(F.col("CUST_ID"), r"^CUST-(\d+)$", 1).cast("int").alias("n"))
                     .agg(F.max("n")).collect()[0][0]) or 0

        with_id = df.filter(F.col("CUST_ID").isNotNull())
        without_id = df.filter(F.col("CUST_ID").isNull())
        if without_id.limit(1).count() == 0:
            return with_id

        # 배치·레코드키 순으로 정렬해 채번 순서를 결정적으로 만든다 (재실행해도 같은 입력이면 같은 순서).
        w = Window.orderBy("_source_batch_id", "_source_record_key")
        new_ids = (without_id
                  .withColumn("_seq", F.row_number().over(w) + F.lit(existing_n))
                  .withColumn("CUST_ID", F.format_string("CUST-%06d", F.col("_seq")))
                  .drop("_seq"))
        return with_id.unionByName(new_ids)

    def integrate(self, target_table: str) -> Dict[str, Any]:
        target_table = target_table.upper()
        spec = self._integration_spec(target_table)
        integration_type = (spec or {}).get("INTEGRATION_TYPE", "DIRECT")
        result = {"target_table": target_table, "integration_type": integration_type}

        if integration_type in ("DIRECT", "UNION"):
            result["note"] = "저장 구조가 이미 이 결과와 같아 변경 없음 (no-op)"
            return result
        if integration_type != "MERGE":
            raise ValueError(f"지원하지 않는 INTEGRATION_TYPE입니다: {integration_type}")

        matching_rule = spec.get("MATCHING_RULE")
        if matching_rule != "NAME_DOB_PHONE_EXACT":
            raise ValueError(f"지원하지 않는 MATCHING_RULE입니다: {matching_rule} (현재 NAME_DOB_PHONE_EXACT만 구현됨)")
        conflict_rule = spec.get("CONFLICT_RULE")
        if conflict_rule != "LATEST_NON_NULL":
            raise ValueError(f"지원하지 않는 CONFLICT_RULE입니다: {conflict_rule} (현재 LATEST_NON_NULL만 구현됨)")

        match_cols = [c.strip() for c in (spec.get("MATCHING_KEY_COLUMNS") or "").split(",") if c.strip()]
        if not match_cols:
            raise ValueError("MATCHING_KEY_COLUMNS가 비어 있습니다.")
        ref = (spec.get("CONFLICT_REFERENCE") or "").split(".")
        if len(ref) != 2:
            raise ValueError(f"CONFLICT_REFERENCE 형식이 'TARGET_TABLE.TARGET_COLUMN'이 아닙니다: {spec.get('CONFLICT_REFERENCE')}")
        ref_table, ref_column = ref[0].strip(), ref[1].strip()

        table = cfg.gold_candidate_table(target_table)
        if not self.spark.catalog.tableExists(table):
            result["note"] = f"{table} 없음 (아직 Mapping 실행 전)"
            return result
        candidate = self.spark.table(table)
        before = candidate.count()
        target_cols = [c for c in candidate.columns if not c.startswith("_")]
        lineage_keys = ["_source_system", "_source_batch_id", "_source_record_key"]

        # 매칭 키가 전부 채워진 행만 "확실한 Matching" 대상으로 본다. 하나라도 NULL이면 동일 고객인지 확신할
        # 수 없어 자동으로 합치지 않는다 - gold_mapping_error(이 단계에서 확정 못 한 후보를 위한 기존 구조)로 보낸다.
        matchable = candidate
        for c in match_cols:
            matchable = matchable.filter(F.col(c).isNotNull())
        key_missing = candidate.join(matchable.select(*lineage_keys), on=lineage_keys, how="left_anti")

        ref_ts = self._reference_timestamp(ref_table, ref_column)
        if ref_ts is None:
            raise ValueError(f"CONFLICT_REFERENCE({ref_table}.{ref_column})에 대응하는 mapping_definition을 찾지 못했습니다.")
        # 소스가 재처리 등으로 같은 레코드를 중복으로 갖고 있을 가능성에 대비해 방어적으로 1건만 남긴다
        # (join fan-out으로 그룹 크기가 부풀어 정상 단독 레코드까지 충돌로 오판하는 걸 막는다).
        ref_ts = ref_ts.dropDuplicates(lineage_keys)
        matchable = matchable.join(ref_ts, on=lineage_keys, how="left")

        fill_cols = [c for c in target_cols if c not in match_cols]
        w_group = Window.partitionBy(*match_cols)

        # 매칭 키가 같은 레코드가 "몇 개"인지로 먼저 나눈다: 다른 Source(또는 같은 Source의 다른 레코드)에
        # 동일 키가 없으면(그룹 크기 1) 애초에 합칠 대상이 없으므로 conflict 검사 자체가 의미 없다 - 그대로
        # 정상 CUSTOMER 행이 된다. 그룹 크기가 2 이상인 경우에만 LATEST_NON_NULL/충돌 판단을 적용한다.
        matchable = matchable.withColumn("_grp_size", F.count(F.lit(1)).over(w_group))
        singles = matchable.filter(F.col("_grp_size") == 1).drop("_grp_size", "_ref_ts")
        groups = matchable.filter(F.col("_grp_size") > 1).drop("_grp_size")

        # 같은 시각(=최신 시각이 동률)에 서로 다른 non-null 값이 있으면 시간만으로는 어느 값이 맞는지 판단할 수 없다
        # (6번 요구사항) - 이런 컬럼이 하나라도 있는 그룹(2개 이상 레코드)은 자동 병합하지 않고 gold_mapping_error로
        # 보낸다 (새 승인 테이블을 만들지 않고 기존 "이 단계에서 확정 못 한 후보" 구조를 그대로 재사용).
        top_ts = F.max(F.col("_ref_ts")).over(w_group)
        at_top = (groups.withColumn("_top_ts", top_ts)
                 .filter((F.col("_ref_ts") == F.col("_top_ts"))
                         | (F.col("_ref_ts").isNull() & F.col("_top_ts").isNull()))
                 .drop("_top_ts"))
        conflict_groups = None
        for c in fill_cols:
            dc = (at_top.filter(F.col(c).isNotNull()).groupBy(*match_cols)
                 .agg(F.countDistinct(c).alias("_n")).filter(F.col("_n") > 1).select(*match_cols))
            conflict_groups = dc if conflict_groups is None else conflict_groups.unionByName(dc).distinct()

        if conflict_groups is not None and conflict_groups.limit(1).count() > 0:
            conflicting = groups.join(conflict_groups, on=match_cols, how="inner").drop("_ref_ts")
            clean_groups = groups.join(conflict_groups, on=match_cols, how="left_anti")
        else:
            conflicting = None
            clean_groups = groups

        unmatched = key_missing if conflicting is None else key_missing.unionByName(conflicting)

        # unmatched는 candidate/table을 나중에 덮어쓰기 전에 먼저 완전히 처리(count+저장)해서 물질화한다.
        # candidate가 여기서 쓰는 table을 아래에서 덮어쓰는데, Spark는 그 시점에 이 table을 가리키는 캐시되지
        # 않은 DataFrame(candidate/unmatched)을 무효화해서 다시 읽으므로, 늦게 평가하면 덮어쓴 결과를 다시
        # 읽어버려 틀린 값이 나온다.
        unmatched_n = unmatched.count()
        if unmatched_n > 0:
            error_table = cfg.gold_mapping_error_table(target_table)
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.UC_CATALOG}.{cfg.GOLD_MAPPING_ERROR_SCHEMA}")
            if not self.spark.catalog.tableExists(error_table):
                unmatched.limit(0).write.format("delta").mode("overwrite") \
                    .partitionBy("_source_batch_id").saveAsTable(error_table)
            unmatched.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(error_table)

        # 여러 레코드가 있는(충돌 없는) 그룹만 LATEST_NON_NULL로 컬럼별 채움 후 1행으로 축약한다.
        # 단독 레코드(singles)는 합칠 대상이 없으므로 그대로 쓴다 (충돌 판단/채움 로직 적용 안 함).
        w_latest_first = w_group.orderBy(F.desc_nulls_last("_ref_ts")).rowsBetween(
            Window.unboundedPreceding, Window.unboundedFollowing)
        filled = clean_groups
        for c in fill_cols:
            filled = filled.withColumn(c, F.first(F.col(c), ignorenulls=True).over(w_latest_first))
        w_rank = w_group.orderBy(F.desc_nulls_last("_ref_ts"))
        collapsed = (filled.withColumn("_conflict_rank", F.row_number().over(w_rank))
                    .filter(F.col("_conflict_rank") == 1)
                    .drop("_conflict_rank", "_ref_ts"))

        merged = singles.unionByName(collapsed)

        # CUST_ID 채번 (PoC 수준 - MAX+1, row_number 전체 재부여 아님). fill_cols 루프가 CUST_ID도 이미
        # first(ignorenulls=True)로 처리해서 "기존에 배정된 값 보존"은 여기 오기 전에 끝나 있다 - 여기서는
        # 그래도 NULL로 남은(=한 번도 배정 안 된 진짜 신규) 행에만 새 번호를 채운다.
        merged = self._assign_customer_ids(merged)
        after = merged.count()

        # gold_candidate.<target>을 읽어서 만든 결과를 같은 테이블에 바로 덮어쓸 수 없다(Spark가 self-overwrite를
        # 막는다) - 임시 테이블에 먼저 물질화한 뒤 그걸 읽어서 원본에 덮어쓴다. unmatched는 위에서 이미 다
        # 처리했으므로 이제 table을 덮어써도 안전하다.
        tmp_table = f"{table}__integrate_tmp"
        (merged.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").saveAsTable(tmp_table))
        (self.spark.table(tmp_table).write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").saveAsTable(table))
        self.spark.sql(f"DROP TABLE IF EXISTS {tmp_table}")


        result.update({
            "matching_rule": matching_rule, "matching_key_columns": match_cols,
            "conflict_rule": conflict_rule, "conflict_reference": f"{ref_table}.{ref_column}",
            "input_rows": before, "merged_rows": after,
            "unmatched_rows_to_mapping_error": unmatched_n,
        })
        return result