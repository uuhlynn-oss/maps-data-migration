"""
Silver -> Gold Mapping Execution 엔진 (슬라이스 1: 최소 구현)

역할
    Mapping Definition(메타데이터)을 읽어 Silver 데이터를 Target 모양(gold_candidate)으로 변환한다.
    Target Validation과 gold_quarantine은 이 다음 단계이며, 이 엔진은 그 입력이 될 표시만 남긴다.
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

from pyspark.sql import Column, DataFrame, SparkSession
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
        df = (
            self._defs
            .filter(F.upper(F.trim(F.col("SOURCE_SYSTEM"))) == source_system.upper())
            .filter(F.upper(F.trim(F.col("TARGET_TABLE"))) == target_table.upper())
            .filter(F.upper(F.trim(F.col("FINAL_MIGRATION_APPLY_YN"))) == "Y")
            .filter(F.upper(F.trim(F.col("REVIEW_STATUS"))).isin(*cfg.APPLY_REVIEW_STATUSES))
        )
        latest: Dict[str, Dict[str, Any]] = {}
        for r in df.collect():
            d = {k: (v.strip() if isinstance(v, str) else v) for k, v in r.asDict().items()}
            prev = latest.get(d["MAPPING_ID"])
            if prev is None or _ver(d["VERSION"]) > _ver(prev["VERSION"]):
                latest[d["MAPPING_ID"]] = d   # 같은 MAPPING_ID가 여러 버전이면 가장 높은 버전만 쓴다
        return sorted(latest.values(), key=lambda d: d["MAPPING_ID"])

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

    def _format_timestamp(self, d, name, stype, ctx):
        s = F.trim(F.col(d["SOURCE_COLUMN"]))
        # 시간대를 문자열에 붙여 "명시적으로" 파싱한다 -> 세션 시간대와 무관하게 정확한 절대 시각(UTC 기준)이 저장된다.
        # (try_to_timestamp: 서버리스의 ANSI 모드에서도 형식 오류를 예외 대신 NULL로 돌려준다)
        parsed = F.try_to_timestamp(F.concat_ws(" ", s, F.lit(ctx["source_tz"])), F.lit(f"{cfg.SOURCE_TIMESTAMP_FORMAT} VV"))
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
        if key in (("RENAME", "COPY"), ("DIRECT", "COPY")):
            return self._copy
        if key == ("TRANSFORM", "FORMAT") and stype == "timestamp":
            return self._format_timestamp
        if key == ("CODE", "LOOKUP"):
            return self._code_lookup
        if key == ("DERIVED", "CONSTANT"):
            return self._constant
        return None

    # ------------------------------------------------------------------
    # 실행
    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    # 저장 (gold_candidate.<target>, 같은 소스·배치만 교체)
    # ------------------------------------------------------------------
    def save(self, candidate: DataFrame, summary: Dict[str, Any]) -> str:
        table = cfg.gold_candidate_table(summary["target_table"])
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.UC_CATALOG}.{cfg.GOLD_CANDIDATE_SCHEMA}")
        if not self.spark.catalog.tableExists(table):
            (candidate.limit(0).write.format("delta").mode("overwrite")
             .partitionBy("_source_batch_id").saveAsTable(table))
        # 여러 소스가 같은 후보 테이블을 공유하므로 (소스, 배치) 단위로만 교체한다
        predicate = f"_source_system = '{summary['silver_source']}' AND _source_batch_id = '{summary['source_batch_id']}'"
        (candidate.write.format("delta").mode("overwrite")
         .option("replaceWhere", predicate).option("mergeSchema", "true").saveAsTable(table))
        return table
