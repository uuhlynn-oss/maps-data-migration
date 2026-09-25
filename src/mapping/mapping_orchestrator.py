"""
Mapping Orchestrator: Target 하나를 처음부터 끝까지 실행하는 단일 진입점.

Notebook(03_mapping_run.py)에 흩어져 있던 "이 Target이 CUSTOMER인가?"류의 Target별 하드코딩 분기를
대체한다. Target 유형(DIRECT/ENTITY/MASTER)은 오직 meta.entity_integration_definition에서만 판정하며,
Target 이름으로 분기하지 않는다 (PRODUCT/CUSTOMER/CONTRACT를 코드에서 개별 취급하지 않음).

mapping_engine.py의 기존 run()/save()/integrate()/load_master_data() 메서드는 수정하지 않는다 - 이
모듈은 그 위에서 "무엇을 어떤 순서로 호출할지"만 결정하는 얇은 오케스트레이션 레이어다. mapping_engine.py의
private 메서드(_integration_spec, _target_uk_column 등)는 재사용하지 않고, 필요한 metadata 조회는 이
파일 내부 helper(_lookup_integration_type, _master_params)가 직접 한다 - 약간의 조회 로직 중복은
감수하되 mapping_engine.py는 한 글자도 건드리지 않는다(1차 리팩토링 제약).

Integration Type 판정 (meta.entity_integration_definition 기준, Target 이름 하드코딩 없음)
    행 없음                    -> DIRECT  (여러 소스가 각자 1:1로 save()만 하면 끝나는 Target)
    INTEGRATION_TYPE = MERGE   -> ENTITY  (여러 소스 run()/save() 후 integrate() 1회 - Record Matching)
    INTEGRATION_TYPE = MASTER  -> MASTER  (단일 authoritative 소스, load_master_data() 1회)
    그 외 값                    -> ValueError (정의되지 않은 유형은 조용히 넘어가지 않는다)

PRODUCT_MAPPING은 여기서 다루는 Integration Type이 아니다 - CONTRACT/COUNSEL 등 ENTITY(또는 DIRECT)
Target의 PRD_ID FK Lookup이 run() 내부에서 참조하는 크로스워크일 뿐, run_target()의 분기 대상이 아니다
(engine.run()이 기존 로직 그대로 처리하며 이 모듈은 그 사실을 몰라도 된다).

discover_execution_targets()는 "어떤 Target들을 실행해야 하는가"를 meta.target_model에서 조회한다.
target_model은 이제 "Mapping Execution 대상 Gold 테이블만" 관리하는 metadata로 정리되었으므로, 원칙적으로는
target_model에 있는 이름이 전부 실행 대상이어야 하지만, COUNSEL_DETAIL처럼 스키마는 정의돼 있어도 엔진이
아직 지원하지 못하는 구조가 남아있을 수 있어 SUPPORTED_TARGET_TABLES를 안전장치로 그대로 둔다. 이 함수는
03_mapping_run.py뿐 아니라 04_gold_validation_run.py도 재사용한다 - "실행 가능한 Target이 무엇인가"라는
질문 자체가 두 단계에서 동일하기 때문이다(같은 target_model, 같은 SUPPORTED_TARGET_TABLES 화이트리스트).

이번 단계 범위 밖: 여러 Target 간 실행 순서를 target_model/mapping_definition만으로 완전히 자동 계산하는
진짜 dependency graph, pipeline_orchestrator(Silver->Gold->Validation 전체 조합), Databricks Workflow
구조. discover_execution_targets()는 SUPPORTED_TARGET_TABLES에 이미 나열된 순서(CUSTOMER가 CONTRACT보다
앞에 오도록 기존에 정의돼 있음)를 그대로 물려받을 뿐, 새로운 순서 계산 로직을 추가하지 않는다.
"""
from typing import Any, Dict, List

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

try:
    import src.mapping.mapping_config as cfg
except ModuleNotFoundError:
    import mapping_config as cfg


SUPPORTED_INTEGRATION_TYPES = ("DIRECT", "ENTITY", "MASTER")


def discover_execution_targets(spark: SparkSession) -> List[str]:
    """meta.target_model에 정의된 Target(TABLE_NAME distinct) 중, 이 코드베이스가 실제로 실행 가능한
    것만 돌려준다 - target_model.TABLE_NAME 전체와 SUPPORTED_TARGET_TABLES의 교집합.

    target_model은 이제 "Mapping Execution 대상 Gold 테이블만" 관리하도록 정리되었으므로, 이 교집합은
    사실상 target_model의 내용을 그대로 따르되, COUNSEL_DETAIL처럼 아직 엔진이 지원하지 못하는 Target이
    남아 있을 경우를 대비한 안전장치다. 새로운 Target을 target_model+entity_integration_definition에
    등록하고 SUPPORTED_TARGET_TABLES에도 추가하면, 이 함수와 run_target()은 코드 수정 없이 그 Target을
    처리한다.

    순서는 SUPPORTED_TARGET_TABLES에 나열된 순서를 그대로 따른다 - 이 튜플은 이미 CUSTOMER가 CONTRACT
    보다 앞에 오도록 정의돼 있어(CONTRACT의 CUST_ID FK Lookup이 CUSTOMER의 gold_candidate를 참조하므로)
    기존 실행 순서를 바꾸지 않는다. 여러 Target 간 실행 순서를 target_model만으로 완전히 자동 계산하는
    것(진짜 dependency graph)은 이번 범위 밖이다."""
    rows = (
        spark.read.table(cfg.TARGET_MODEL_TABLE)
        .select(F.upper(F.trim(F.col("TABLE_NAME"))).alias("t"))
        .distinct().collect()
    )
    in_target_model = {r["t"] for r in rows}
    return [t for t in cfg.SUPPORTED_TARGET_TABLES if t in in_target_model]


def _lookup_integration_type(spark: SparkSession, target_table: str) -> str:
    """meta.entity_integration_definition만 보고 DIRECT/ENTITY/MASTER를 판정한다. Target 이름으로
    분기하지 않는다. 승인된(FINAL_MIGRATION_APPLY_YN=Y, REVIEW_STATUS 승인) 행이 없으면 DIRECT. 행이
    있으면 INTEGRATION_TYPE 값(MERGE/MASTER)을 그대로 읽어 반환하고, 그 외 값이면 정의되지 않은
    유형이므로 조용히 넘어가지 않고 멈춘다."""
    target_table = target_table.upper()
    if not spark.catalog.tableExists(cfg.ENTITY_INTEGRATION_TABLE):
        return "DIRECT"

    rows = (
        spark.read.table(cfg.ENTITY_INTEGRATION_TABLE)
        .filter(F.upper(F.trim(F.col("TARGET_ENTITY"))) == target_table)
        .filter(F.upper(F.trim(F.col("FINAL_MIGRATION_APPLY_YN"))) == "Y")
        .filter(F.upper(F.trim(F.col("REVIEW_STATUS"))).isin(*cfg.APPLY_REVIEW_STATUSES))
        .collect()
    )
    if not rows:
        return "DIRECT"
    if len(rows) > 1:
        raise ValueError(
            f"{target_table}: entity_integration_definition에 승인된 행이 {len(rows)}개입니다 "
            f"(TARGET_ENTITY당 정확히 1개여야 합니다)."
        )

    raw_type = str(rows[0]["INTEGRATION_TYPE"] or "").strip().upper()
    if raw_type == "MERGE":
        return "ENTITY"
    if raw_type == "MASTER":
        return "MASTER"
    raise ValueError(
        f"{target_table}: entity_integration_definition.INTEGRATION_TYPE='{raw_type}'은(는) 정의되지 "
        f"않은 값입니다 (행 없음=DIRECT, MERGE=ENTITY, MASTER=MASTER만 지원)."
    )


def _master_params(spark: SparkSession, target_table: str) -> Dict[str, str]:
    """MASTER Target의 load_master_data() 파라미터를 target_model에서 동적으로 도출한다.
    PK 컬럼 -> id_column, UK 컬럼 -> key_column, id_column에서 '_ID' 접미사를 뗀 값 -> prefix.
    target_model에 KEY='PK'/'UK' 표시가 정확히 1개씩 없으면(예: target_model 데이터가 아직 보강되지
    않은 경우) 임의로 추측하지 않고 명확한 오류로 멈춘다 - 이 경우 target_model 데이터 보강이 먼저
    필요하다 (mapping_engine.py의 integrate()/_target_uk_column()이 CUSTOMER/CONTRACT에 대해 쓰는
    것과 동일한 KEY 컬럼 표기를 그대로 따른다)."""
    target_table = target_table.upper()
    rows = spark.read.table(cfg.TARGET_MODEL_TABLE).filter(
        F.upper(F.trim(F.col("TABLE_NAME"))) == target_table
    )
    if "KEY" not in rows.columns:
        raise ValueError(f"{target_table}: target_model에 KEY 컬럼 자체가 없어 PK/UK를 도출할 수 없습니다.")

    pk_rows = (
        rows.filter(F.upper(F.trim(F.col("KEY"))) == "PK")
        .select(F.trim(F.col("COLUMN_NAME")).alias("c")).distinct().collect()
    )
    if len(pk_rows) != 1:
        raise ValueError(
            f"{target_table}: target_model에서 PK 컬럼을 정확히 1개 찾아야 하는데 {len(pk_rows)}개입니다 "
            f"(KEY='PK' 행 확인 필요 - target_model 데이터 보강이 필요할 수 있습니다)."
        )
    id_column = pk_rows[0]["c"]

    uk_rows = (
        rows.filter(F.upper(F.trim(F.col("KEY"))) == "UK")
        .select(F.trim(F.col("COLUMN_NAME")).alias("c")).distinct().collect()
    )
    if len(uk_rows) != 1:
        raise ValueError(
            f"{target_table}: target_model에서 UK(자연키) 컬럼을 정확히 1개 찾아야 하는데 {len(uk_rows)}개입니다 "
            f"(MASTER Integration은 자연키 기준 ID 재사용/신규발급이 필요합니다 - KEY='UK' 행 확인 필요)."
        )
    key_column = uk_rows[0]["c"]

    prefix = id_column[:-3] if id_column.upper().endswith("_ID") else id_column
    return {"id_column": id_column, "key_column": key_column, "prefix": prefix}


def run_target(engine, target_table: str) -> Dict[str, Any]:
    """
    Target 하나에 대해 discover_sources() -> source별 run() -> (DIRECT/ENTITY는 save()) ->
    (ENTITY는 전체 source 처리 후 integrate() 1회 / MASTER는 target_model 기반 load_master_data() 1회)
    까지 한 번에 실행한다.

    engine: mapping_engine.MappingEngine.from_tables(spark)로 만든 인스턴스 (수정 없이 그대로 사용).
    반환값: 어떤 유형으로 판정했고 어떤 소스를 처리했는지, 최종 integrate/load_master_data 결과를 담은 dict.
    """
    target_table = target_table.upper()
    integration_type = _lookup_integration_type(engine.spark, target_table)

    sources = engine.discover_sources(target_table)
    if not sources:
        raise ValueError(f"{target_table}: 승인된 매핑 정의(mapping_definition)를 가진 소스가 없습니다.")
    if integration_type == "MASTER" and len(sources) != 1:
        # Master Data Integration은 "이미 확정된 authoritative source 하나"가 전제다 (mapping_config.py
        # SUPPORTED_TARGET_TABLES 주석 C 참고). load_master_data()는 candidate 하나만 받으므로, 소스가
        # 여럿이면 마지막 소스로 조용히 덮어쓰는 사고를 막기 위해 여기서 명확히 멈춘다.
        raise ValueError(
            f"{target_table}: MASTER Integration인데 승인된 소스가 {len(sources)}개입니다 ({sources}). "
            f"MASTER는 소스가 정확히 1개여야 합니다."
        )

    run_results: List[Dict[str, Any]] = []
    last_candidate = None
    last_summary = None
    for source_system in sources:
        candidate, summary = engine.run(source_system, target_table)
        last_candidate, last_summary = candidate, summary

        if integration_type in ("DIRECT", "ENTITY"):
            table = engine.save(candidate, summary)
            run_results.append({"source_system": source_system, "gold_candidate_table": table, "summary": summary})
        else:  # MASTER - save()를 부르지 않는다. load_master_data()가 저장까지 겸한다 (아래 반복문 밖 1회 호출).
            run_results.append({"source_system": source_system, "summary": summary})

    result: Dict[str, Any] = {
        "target_table": target_table,
        "integration_type": integration_type,
        "sources": sources,
        "run_results": run_results,
    }

    if integration_type == "ENTITY":
        # 모든 source의 run()+save()가 끝난 뒤, 정확히 1번만 호출한다 (기존 03_mapping_run.py §7의
        # "반복문 밖에서 한 번" 원칙과 동일 - integrate() 자체는 수정하지 않았다).
        result["integration_result"] = engine.integrate(target_table)
    elif integration_type == "MASTER":
        params = _master_params(engine.spark, target_table)
        result["master_params"] = params
        result["master_load_result"] = engine.load_master_data(
            target_table, last_candidate, last_summary,
            id_column=params["id_column"], key_column=params["key_column"], prefix=params["prefix"],
        )
    else:
        result["note"] = "DIRECT: source별 save()로 이미 반영 완료, 추가 처리 없음"

    return result