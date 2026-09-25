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
    (DERIVED, LOOKUP)         FK: 다른 Target의 gold_candidate에서 PK 값 조회 (run()이 미리 join, 아래
                              "FK Lookup" 참고). CODE_MAPPING_RULE == "PRODUCT_MAPPING"인 행은 예외로,
                              PRODUCT를 직접 조회하지 않고 PRODUCT_MAPPING 크로스워크(SRC_SYS+SRC_PRD_CD,
                              APRV_YN='Y')를 거친다. 참조 Target/크로스워크를 못 찾거나 아직 실행 전이면
                              (그리고 물론 값 자체가 안 맞으면) NULL + _unmapped_codes 기록
    그 밖(CUSTOMER_MATCH, GENERATE_ID, EXPLODE ...)은 이번 단계에서 NULL로 두고
    summary["deferred_columns"]에 사유와 함께 기록한다. (정의가 잘못된 경우와 달리 실행을 막지 않는다)

정의(Mapping Definition, Target Model, 코드 매핑)가 잘못된 경우에는 일부만 변환해서 내보내지 않고 ValueError로 즉시 멈춘다.

FK Lookup (DERIVED, LOOKUP)
    run()이 메인 컬럼 루프 전에, 이 조합의 컬럼마다 참조 Target을 target_model에서 동적으로 찾아
    (_fk_reference_table - "이 FK 컬럼과 같은 이름의 PK를 가진 테이블") 그 gold_candidate와 미리 join한다.
    매칭 방식은 참조 Target에 자연키(KEY='UK')가 있는지로 갈린다:
        자연키 있음(예: PRODUCT.PRD_CD)   Silver 원천 값과 완전일치로 매칭
        자연키 없음(예: CUSTOMER)         gold_entity_lineage.<참조Target> crosswalk(있으면 우선)로 원본
                                         레코드(lineage: _source_system/_source_batch_id/_source_record_key)를
                                         최종 PK와 잇는다 - Entity Integration(MERGE)이 대표 행 하나로 축약해도
                                         crosswalk는 병합 전 전체 원본의 lineage를 보존해 두므로(integrate()가
                                         저장) 대표로 안 뽑힌 원본도 찾을 수 있다. crosswalk가 아직 없으면(과거
                                         integrate() 실행분) 참조 Target의 gold_candidate에서 lineage로 직접
                                         조회하는 기존 방식으로 폴백한다(이 경우 병합 중 사라진 쪽은 NULL)
    예외: mapping_definition.CODE_MAPPING_RULE == "PRODUCT_MAPPING"인 행(현재 CONTRACT/COUNSEL/COMPLAINT의
    PRD_ID)은 위 규칙을 타지 않는다. 채널별 상품코드(product_code/GD_CD 등)는 표준 PRD_CD와 체계가 달라
    PRODUCT를 직접 조회할 수 없기 때문 - 대신 PRODUCT_MAPPING(target_model에 이미 정의된 크로스워크:
    SRC_SYS/SRC_PRD_CD -> TGT_PRD_ID, APRV_YN)을 SRC_SYS=이 행의 SOURCE_SYSTEM, SRC_PRD_CD=Silver의
    SOURCE_COLUMN 값, APRV_YN='Y' 조건으로 조회해 TGT_PRD_ID를 그대로 가져온다(이미 최종 PK 값이라
    PRODUCT를 다시 조회할 필요 없음, 1-hop). PRODUCT_MAPPING_TABLE이 아직 없거나 매칭이 없으면 다른
    FK Lookup과 동일하게 NULL + _unmapped_codes.
    참조 Target/크로스워크를 못 찾거나 아직 실행 전이면 그 컬럼은 기존과 동일하게 deferred로 남는다(에러 아님).

run() 이후 gold_candidate 반영 방식 (Target 유형별) - mapping_config.SUPPORTED_TARGET_TABLES의 A/B/C 참고
    A. Direct Mapping           save()만으로 끝난다. (COUNSEL, COMPLAINT)
    B. Entity Integration        여러 소스의 run()/save() 후 integrate()가 Record Matching + 중복 제거/통합
                                 (MERGE) + ID 생성/재사용을 한 번에 한다. (CUSTOMER, CONTRACT)
    C. Master Data Integration    비즈니스가 이미 확정한 Master Data 기준이라 매칭/병합이 필요 없다.
                                 load_master_data()가 run() 결과를 받아 자연키 기준 ID 재사용/신규 발급과
                                 "현재 전체 스냅샷"으로의 저장을 한 번에 한다. (PRODUCT)
    B/C 모두 PK 채번의 실제 알고리즘(포맷·순번 부여)은 _assign_sequential_ids를 공유한다.
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

    def _fk_reference_table(self, fk_column: str) -> Optional[str]:
        """fk_column과 이름이 같은 PK(KEY='PK')를 가진 target_model의 TABLE_NAME을 찾는다 - FK Lookup이
        참조할 Target을 "CUST_ID -> CUSTOMER"처럼 하드코딩하지 않고 target_model에서 동적으로 찾기 위한
        장치다. target_model에 KEY 컬럼이 없거나(과거 테스트용 인라인 스키마 등), 못 찾거나, 같은 이름의
        PK가 여럿이면(정의 모순) None을 돌려준다 - 이 경우 그 FK는 run()에서 deferred로 남는다."""
        if "KEY" not in self._target_model.columns:
            return None
        rows = (self._target_model
               .filter(F.upper(F.trim(F.col("COLUMN_NAME"))) == fk_column.upper())
               .filter(F.upper(F.trim(F.col("KEY"))) == "PK")
               .select(F.trim(F.col("TABLE_NAME")).alias("t")).distinct().collect())
        return rows[0]["t"] if len(rows) == 1 else None

    def _target_uk_column(self, table_name: str) -> Optional[str]:
        """table_name의 target_model에서 자연키(KEY='UK')로 표시된 컬럼 1개를 찾는다 (예: PRODUCT.PRD_CD).
        없거나(예: CUSTOMER - NAME+DOB로만 통합되어 별도 업무키를 안 남김) 여럿이면 None."""
        if "KEY" not in self._target_model.columns:
            return None
        rows = (self._target_model
               .filter(F.upper(F.trim(F.col("TABLE_NAME"))) == table_name.upper())
               .filter(F.upper(F.trim(F.col("KEY"))) == "UK")
               .select(F.trim(F.col("COLUMN_NAME")).alias("c")).distinct().collect())
        return rows[0]["c"] if len(rows) == 1 else None

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

    def _fk_lookup(self, d, name, stype, ctx):
        """DERIVED/LOOKUP: 다른 Target의 gold_candidate에서 이 FK(PK) 값을 조회한다. 실제 조회는 run()이
        메인 루프 전에 미리 join해 둔 임시 컬럼(ctx["fk_lookup_columns"][name])을 참조만 한다 - 이 핸들러는
        (다른 핸들러와 같은 모양을 맞추려고) silver_df 자체 컬럼만으로 계산되는 Column 표현식이어야 하는데
        JOIN은 표현할 수 없기 때문이다. 매칭 실패(참조 Target에 없음)는 코드 LOOKUP과 동일하게 NULL +
        _unmapped_codes에 기록한다(제거하지 않음) - 근거 없는 값을 만들지 않는다는 정책과 같다."""
        tmp_col = (ctx.get("fk_lookup_columns") or {}).get(name)
        src = F.col(d["SOURCE_COLUMN"]).cast("string")
        val = F.col(tmp_col).cast(stype)
        unmapped = F.when(src.isNotNull() & (F.trim(src) != "") & val.isNull(),
                          F.concat(F.lit(f"{name}="), src))
        return val, None, unmapped

    def _dispatch(self, d, stype, ctx=None) -> Optional[Callable]:
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
        # DERIVED/LOOKUP(다른 Target의 gold_candidate 참조)은 run()이 미리 join을 성공시킨 컬럼에 대해서만
        # 처리한다(ctx["fk_lookup_columns"]에 있는 경우만) - 참조 Target을 못 찾았거나 아직 실행 전이면
        # 기존과 동일하게 여기서 None을 돌려줘 deferred로 남긴다(_reference_timestamp처럼 ctx에
        # fk_lookup_columns가 없는 호출도 안전하게 여기로 떨어진다).
        if key == ("DERIVED", "LOOKUP") and d["TARGET_COLUMN"] in ((ctx or {}).get("fk_lookup_columns") or {}):
            return self._fk_lookup
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

        # ---- FK Lookup(DERIVED/LOOKUP) 사전 조인 ----
        # 핸들러는 silver_df 자체 컬럼만으로 계산되는 Column 표현식이라 JOIN을 표현할 수 없다 - 그래서 이
        # 컬럼들만 미리 참조 Target의 gold_candidate와 join해 silver_df에 임시 컬럼(_fk_lookup_<컬럼명>)으로
        # 얹어 두고, 메인 루프의 _fk_lookup 핸들러는 그 임시 컬럼을 참조만 한다. 참조 Target은 하드코딩하지
        # 않고 target_model에서 "이 FK 컬럼과 같은 이름의 PK를 가진 테이블"로 동적으로 찾는다
        # (_fk_reference_table) - 예: CONTRACT.CUST_ID -> PK가 CUST_ID인 CUSTOMER.
        # 단, CODE_MAPPING_RULE == "PRODUCT_MAPPING"인 행은 이 규칙을 타지 않는다 - 채널별 상품코드는 표준
        # PRD_CD와 체계가 달라(예: LONG-ACC-001 vs AUTO-001) PRODUCT를 직접 조회할 수 없고, PRODUCT_MAPPING
        # 크로스워크(SRC_SYS+SRC_PRD_CD -> TGT_PRD_ID, APRV_YN='Y' 승인분만)를 거쳐야 한다 - target_model의
        # PRD_ID -> PRODUCT 직접 참조 규칙은 그대로 두고, mapping_definition에 이 표시가 있는 행만 예외로
        # 먼저 처리한다(기존 컬럼 CODE_MAPPING_RULE 재사용 - 이전까지 전 행에서 빈 값이었다).
        fk_lookup_columns: Dict[str, str] = {}
        for d in defs:
            key = (str(d["MAPPING_TYPE"]).upper(), str(d["PROCESS_TYPE"]).upper())
            if key != ("DERIVED", "LOOKUP"):
                continue
            fk_col = d["TARGET_COLUMN"]
            tmp_col = f"_fk_lookup_{fk_col}"

            code_mapping_rule = str(d.get("CODE_MAPPING_RULE") or "").strip().upper()
            if code_mapping_rule == "PRODUCT_MAPPING":
                if not self.spark.catalog.tableExists(cfg.PRODUCT_MAPPING_TABLE):
                    continue   # PRODUCT_MAPPING이 아직 없음 - deferred로 남긴다 (기존 정책과 동일)
                pm_df = self.spark.table(cfg.PRODUCT_MAPPING_TABLE)
                # TGT_PRD_ID가 이미 최종 PK 값이라 PRODUCT를 다시 조회할 필요가 없다(1-hop). SRC_SYS로
                # 스코프를 좁히는 이유는 같은 코드 문자열이 소스 시스템마다 다른 상품을 가리킬 수 있어서다.
                lookup = (pm_df
                         .filter(F.upper(F.trim(F.col("SRC_SYS"))) == source_system)
                         .filter(F.upper(F.trim(F.col("APRV_YN"))) == "Y")
                         .select(F.trim(F.col("SRC_PRD_CD")).alias("_src_cd"), F.col("TGT_PRD_ID").alias(tmp_col))
                         .dropDuplicates(["_src_cd"]))
                silver_df = (silver_df
                            .join(lookup, on=F.trim(F.col(d["SOURCE_COLUMN"])) == F.col("_src_cd"), how="left")
                            .drop("_src_cd"))
                fk_lookup_columns[fk_col] = tmp_col
                continue

            ref_table = self._fk_reference_table(fk_col)
            if ref_table is None:
                continue   # 참조 Target을 target_model에서 못 찾음 - 기존처럼 deferred로 남긴다
            ref_gold = cfg.gold_candidate_table(ref_table)
            if not self.spark.catalog.tableExists(ref_gold):
                continue   # 참조 Target을 아직 실행 안 함 - deferred로 남긴다
            ref_df = self.spark.table(ref_gold)
            uk_col = self._target_uk_column(ref_table)
            if uk_col:
                # 참조 Target에 자연키(UK)가 있으면(예: PRODUCT.PRD_CD) Silver 값과 완전일치로 매칭한다.
                lookup = (ref_df.select(F.trim(F.col(uk_col)).alias("_uk"), F.col(fk_col).alias(tmp_col))
                         .dropDuplicates(["_uk"]))
                silver_df = (silver_df
                            .join(lookup, on=F.trim(F.col(d["SOURCE_COLUMN"])) == F.col("_uk"), how="left")
                            .drop("_uk"))
            else:
                # 참조 Target에 자연키가 없으면(예: CUSTOMER - NAME+DOB 완전일치로만 통합되고 별도 업무키를
                # 남기지 않음) Entity Lineage Crosswalk(gold_entity_lineage.<ref_table>)를 우선 쓴다 - MERGE 중
                # 대표 행으로 축약되며 사라진 원본의 lineage도 여기서는 최종 PK와 이어져 있어 찾을 수 있다.
                # crosswalk가 아직 없으면(과거 integrate() 실행분, 이 기능 도입 전) 기존처럼 참조 Target의
                # gold_candidate에서 lineage로 직접 조회한다(대표 행만 남아있어 병합 중 사라진 쪽은 여전히 NULL).
                lineage_keys = ["_source_system", "_source_batch_id", "_source_record_key"]
                crosswalk_table = cfg.gold_entity_lineage_table(ref_table)
                if self.spark.catalog.tableExists(crosswalk_table):
                    lookup = (self.spark.table(crosswalk_table)
                             .filter(F.col("TARGET_ENTITY") == ref_table)
                             .select(*lineage_keys, F.col("TARGET_PK").alias(tmp_col))
                             .dropDuplicates(lineage_keys))
                else:
                    lookup = ref_df.select(*lineage_keys, F.col(fk_col).alias(tmp_col)).dropDuplicates(lineage_keys)
                silver_df = silver_df.join(lookup, on=lineage_keys, how="left")
            fk_lookup_columns[fk_col] = tmp_col
        ctx["fk_lookup_columns"] = fk_lookup_columns

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
            handler = self._dispatch(d, stype, ctx)
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

    def _assign_sequential_ids(self, df: DataFrame, id_column: str, prefix: str,
                                order_cols: List[str]) -> DataFrame:
        """<id_column>이 없는(=한 번도 배정 안 된 신규) 행에만 '<PREFIX>-000001' 형식으로 순번을 채운다 (PoC 수준).
        전체에 row_number()를 다시 매기지 않고, 기존 <id_column> 중 최대 번호 다음부터 신규 행에만 이어서
        부여한다 - 이미 배정된 값이 있는 행은 이 함수에 오기 전에 이미 채워진 채로 들어온다고 가정하고
        여기서는 손대지 않는다 (어떻게 채워 넣을지는 호출자 책임 - CUSTOMER는 integrate()의 fill_cols가,
        PRODUCT는 assign_generated_id()의 자연키 backfill이 담당).

        CUSTOMER(_assign_customer_ids)와 PRODUCT(assign_generated_id)가 공유하는 채번 핵심 로직이다.
        기존 CUST_ID 채번 동작(포맷 'CUST-%06d', 정렬 기준)은 그대로다 - 이 함수는 그 로직을 prefix/정렬
        기준만 바꿀 수 있게 일반화했을 뿐 계산 자체는 바뀌지 않았다."""
        if id_column not in df.columns:
            return df
        existing_n = (df.filter(F.col(id_column).isNotNull())
                     .select(F.regexp_extract(F.col(id_column), rf"^{prefix}-(\d+)$", 1).cast("int").alias("n"))
                     .agg(F.max("n")).collect()[0][0]) or 0

        with_id = df.filter(F.col(id_column).isNotNull())
        without_id = df.filter(F.col(id_column).isNull())
        if without_id.limit(1).count() == 0:
            return with_id

        # order_cols 순으로 정렬해 채번 순서를 결정적으로 만든다 (재실행해도 같은 입력이면 같은 순서).
        w = Window.orderBy(*order_cols)
        new_ids = (without_id
                  .withColumn("_seq", F.row_number().over(w) + F.lit(existing_n))
                  .withColumn(id_column, F.format_string(f"{prefix}-%06d", F.col("_seq")))
                  .drop("_seq"))
        return with_id.unionByName(new_ids)

    def _assign_customer_ids(self, df: DataFrame) -> DataFrame:
        """CUST_ID 채번. 기존 동작(포맷 'CUST-000001', 배치·레코드키 순 정렬) 그대로이며, 실제 계산은
        _assign_sequential_ids로 일반화해 PRODUCT의 load_master_data()와 공유한다. integrate()는 이제 이
        메서드를 직접 부르지 않고 target_model의 KEY='PK' 행에서 PK 컬럼/prefix를 동적으로 찾아 같은
        _assign_sequential_ids를 호출한다(CUSTOMER든 CONTRACT든 동일 경로) - CUSTOMER에 대해서는 결과가
        이 메서드를 호출한 것과 완전히 같다. 이 메서드는 외부에서 CUST_ID 채번만 필요할 때 쓰는 편의
        함수로 남겨둔다."""
        return self._assign_sequential_ids(df, "CUST_ID", "CUST",
                                            order_cols=["_source_batch_id", "_source_record_key"])

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
        if matching_rule not in cfg.SUPPORTED_MATCHING_RULES:
            raise ValueError(f"지원하지 않는 MATCHING_RULE입니다: {matching_rule} (현재 {cfg.SUPPORTED_MATCHING_RULES}만 구현됨)")
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

        # PK 채번 (PoC 수준 - MAX+1, row_number 전체 재부여 아님). fill_cols 루프가 PK 컬럼도 이미
        # first(ignorenulls=True)로 처리해서 "기존에 배정된 값 보존"은 여기 오기 전에 끝나 있다 - 여기서는
        # 그래도 NULL로 남은(=한 번도 배정 안 된 진짜 신규) 행에만 새 번호를 채운다.
        # PK 컬럼/prefix는 target_model의 KEY='PK' 행에서 동적으로 찾는다 - CUSTOMER 전용으로 CUST_ID를
        # 하드코딩하지 않아 CONTRACT(CNTR_ID) 등 다른 Entity Integration Target도 같은 코드로 처리된다.
        # 포맷/정렬 기준은 CUSTOMER 때(_assign_customer_ids)와 완전히 동일하다.
        pk_rows = [c for c in self._target_columns(target_table) if str(c.get("KEY") or "").strip().upper() == "PK"]
        if len(pk_rows) != 1:
            raise ValueError(f"{target_table}의 PK 컬럼을 target_model에서 정확히 1개 찾아야 하는데 "
                              f"{len(pk_rows)}개임 (KEY='PK' 행 필요).")
        pk_col = pk_rows[0]["COLUMN_NAME"]
        prefix = pk_col[:-3] if pk_col.upper().endswith("_ID") else pk_col
        merged = self._assign_sequential_ids(merged, pk_col, prefix,
                                              order_cols=["_source_batch_id", "_source_record_key"])
        after = merged.count()

        # --- Entity Lineage Crosswalk (요구사항 1-4): 대표 행으로 축약되며 사라지는 원본 lineage도 최종 PK와
        # 이어 별도 저장한다. singles(단독 레코드)는 lineage=자기 자신의 PK, 병합 그룹은 filled(collapse 전,
        # 그룹 전원이 살아있는 상태)에서 lineage+match_cols를 뽑아 merged의 최종 pk_col과 match_cols로 join한다.
        # unmatched(요구사항 3에서 제외)와 같은 이유로, table을 덮어쓰기 전에 먼저 물질화해서 저장한다.
        pk_by_match = merged.select(*match_cols, pk_col).dropDuplicates(match_cols)
        lineage_pre_collapse = (singles.select(*lineage_keys, *match_cols)
                                .unionByName(filled.select(*lineage_keys, *match_cols)))
        crosswalk = (lineage_pre_collapse.join(pk_by_match, on=match_cols, how="left")
                    .select(*lineage_keys, F.lit(target_table).alias("TARGET_ENTITY"),
                            F.col(pk_col).alias("TARGET_PK")))
        self._save_entity_lineage_crosswalk(target_table, crosswalk)

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

    def _save_entity_lineage_crosswalk(self, target_table: str, crosswalk: DataFrame) -> None:
        """gold_entity_lineage.<target_table>을 이번 integrate() 계산 기준 전체 스냅샷으로 통째로 덮어쓴다 -
        candidate 자체가 매번 gold_candidate 전체를 다시 읽어 재계산되므로(save()의 배치 단위 append와 달리),
        crosswalk도 같은 범위로 맞춰야 탈락한 lineage가 갱신 없이 남거나 지워진 PK를 가리키는 일이 없다.
        target_table을 그대로 받으므로 CUSTOMER 등 특정 Entity에 하드코딩되지 않는다."""
        table = cfg.gold_entity_lineage_table(target_table)
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.UC_CATALOG}.{cfg.GOLD_ENTITY_LINEAGE_SCHEMA}")
        tmp_table = f"{table}__crosswalk_tmp"
        (crosswalk.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").saveAsTable(tmp_table))
        (self.spark.table(tmp_table).write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").saveAsTable(table))
        self.spark.sql(f"DROP TABLE IF EXISTS {tmp_table}")

    # ------------------------------------------------------------------
    # Master Data Integration 전용 저장 (예: PRODUCT). run()이 만든 candidate(PK 컬럼은 GENERATE_ID라
    # NULL)를 gold_candidate.<target_table>에 반영하는데, save()(Direct Mapping/Entity Integration이 쓰는
    # "소스·배치 단위로 추가/치환")를 그대로 쓰지 않는다 - Master Data는 여러 배치가 누적되는 로그가 아니라
    # "지금 이 순간의 전체 목록"이라, save()를 그대로 쓰면 재적재할 때마다 같은 자연키(key_column) 행이
    # 배치별로 쌓여 중복이 생긴다. 그래서 이 메서드가 저장까지 함께 맡는다:
    #   1) 이미 저장된 값이 있으면 자연키(key_column) 기준으로 id_column을 그대로 이어받는다 (기존 ID 유지)
    #   2) 처음 보는 자연키에는 새 번호를 발급한다 (_assign_sequential_ids, CUSTOMER의 채번과 공용)
    #   3) 결과를 가공 없이(=이번 적재의 나머지 컬럼 값 그대로) "새 전체 스냅샷"으로 통째로 교체한다 - 여러
    #      소스를 매칭/충돌 해소할 필요가 없으니(Master Data가 이미 authoritative) integrate()의
    #      MATCHING_RULE/CONFLICT_RULE 같은 메타데이터가 필요 없다.
    # target_table/id_column/key_column/prefix를 호출자가 지정하는 범용 메서드다 - PRODUCT 전용 로직이
    # 아니라 앞으로 다른 Master Data Integration Target이 생겨도 그대로 재사용한다.
    # ------------------------------------------------------------------
    def load_master_data(self, target_table: str, candidate: DataFrame, summary: Dict[str, Any],
                          id_column: str, key_column: str, prefix: str) -> Dict[str, Any]:
        target_table = target_table.upper()
        table = cfg.gold_candidate_table(target_table)
        error_table = cfg.gold_mapping_error_table(target_table)
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.UC_CATALOG}.{cfg.GOLD_CANDIDATE_SCHEMA}")
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.UC_CATALOG}.{cfg.GOLD_MAPPING_ERROR_SCHEMA}")

        # save()와 동일하게, 값 변환에 실패한 행(_map_errors)은 gold_candidate에 넣지 않고 error_table로 뺀다.
        clean = candidate.filter(F.col("_map_errors").isNull())
        errors = candidate.filter(F.col("_map_errors").isNotNull())

        already_assigned = 0
        if self.spark.catalog.tableExists(table):
            existing = self.spark.table(table)
            already_assigned = existing.filter(F.col(id_column).isNotNull()).count()
            # 자연키(key_column) 기준으로 기존 id_column만 가져온다 - 같은 키에 값이 여러 번 있었을 리
            # 없지만(이 메서드 자체가 매번 스냅샷 전체를 교체하므로) 방어적으로 dropDuplicates한다.
            existing_ids = (existing.filter(F.col(id_column).isNotNull())
                           .select(key_column, id_column).dropDuplicates([key_column]))
            clean = clean.drop(id_column).join(existing_ids, on=key_column, how="left")
        # else: 최초 적재라 재사용할 기존 ID가 없다 - clean의 id_column은 run()이 이미 전부 NULL로 둔 상태.

        assigned = self._assign_sequential_ids(clean, id_column, prefix, order_cols=[key_column])

        # assigned는 (존재한다면) 기존 table을 읽어 만든 DataFrame이므로, 이 값에 기반한 집계는 table을
        # 덮어쓰기 전에 미리 물질화해 둔다 - integrate()의 unmatched_n과 같은 이유다: 늦게 평가하면 Spark가
        # 그때 가서 lazy plan을 다시 실행하며 이미 덮어써진(또는 삭제된) table의 예전 파일을 읽으려다 실패한다.
        after_assigned = assigned.filter(F.col(id_column).isNotNull()).count()
        error_rows = errors.count()

        # gold_candidate.<target>을 읽어서(존재하는 경우) 만든 결과를 같은 테이블에 바로 덮어쓸 수 없다
        # (Spark가 self-overwrite를 막는다) - integrate()와 동일하게 임시 테이블을 거쳐 원본에 덮어쓴다.
        tmp_table = f"{table}__master_load_tmp"
        (assigned.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").saveAsTable(tmp_table))
        (self.spark.table(tmp_table).write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").saveAsTable(table))
        self.spark.sql(f"DROP TABLE IF EXISTS {tmp_table}")

        # 변환 오류 행도 마스터 데이터답게 "이번 적재분"으로 통째로 교체한다 (배치 단위 append가 아니다).
        (errors.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").saveAsTable(error_table))

        summary["mapping_error_rows_excluded"] = error_rows
        return {
            "target_table": target_table, "id_column": id_column, "key_column": key_column,
            "output_rows": after_assigned,
            "already_assigned_rows": already_assigned,
            "newly_assigned_rows": after_assigned - already_assigned,
            "mapping_error_rows_excluded": summary["mapping_error_rows_excluded"],
        }