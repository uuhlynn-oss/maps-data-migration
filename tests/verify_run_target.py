# =============================================================================
# run_target() 검증 하네스 (Databricks 노트북 셀에 붙여넣어 실행)
#
# 기존 경로(03_mapping_run.py / product_master_run.py의 Notebook 셀 로직을 함수로 "재현"한 것)와 신규
# mapping_orchestrator.run_target() 경로를 같은 데이터로 순차 실행해, 최종 gold_candidate(및 ENTITY
# Target의 gold_entity_lineage) 결과가 동일한지 비교한다.
#
# 03_mapping_run.py / product_master_run.py 자체, mapping_engine.py, mapping_orchestrator.py는 이
# 스크립트가 전혀 수정하지 않는다 - 여기서는 그 Notebook 셀들이 하는 일을 함수로 재현해서 같은
# MappingEngine 인스턴스로 실행할 뿐이다.
#
# 주의: 이 스크립트는 gold_candidate.<target>/gold_entity_lineage.<target> 물리 테이블을 실제로 두 번
# (기존 경로 1번, 신규 경로 1번) 덮어쓴다. 각 실행 직후 결과를 verify 스키마의 별도 테이블로 복제해
# 보존하므로, 최종적으로 라이브 테이블에는 "신규 경로 결과"가 남는다 - 두 경로 모두 같은
# mapping_engine.py 로직을 쓰므로 최종 상태는 동일해야 한다(그게 이 검증의 목적).
#
# 비교 항목: row count / schema / PK 중복 / 컬럼별 NULL count / 컬럼별 값 diff(레코드 단위) / 최종 결과
# 일치 여부. CONTRACT는 추가로 "소스마다 즉시 integrate() vs 전체 소스 처리 후 1회 integrate()" 호출
# 로그를 남긴다 - 단, 현재 CONTRACT는 승인된 소스가 1개(INBOUND)뿐이라 두 방식 모두 실질적으로 1회
# 호출로 귀결되므로, 이 검증만으로는 "두 방식이 실제로 다르게 동작하는 경우"까지는 확인할 수 없다
# (OUTBOUND -> CONTRACT 매핑이 추가된 뒤에만 의미 있게 재현 가능 - report에 소스 개수를 명시해 이 한계를
# 놓치지 않게 한다).
# =============================================================================
import sys, os
from pyspark.sql import functions as F

for _p in sys.path:
    _parent = os.path.dirname(_p)
    if os.path.isdir(os.path.join(_parent, "src")) and _parent not in sys.path:
        sys.path.insert(0, _parent)
        break

try:
    import src.mapping.mapping_config as cfg
    import src.mapping.mapping_engine as engine_mod
    import src.mapping.mapping_orchestrator as orch
except ModuleNotFoundError:
    import mapping_config as cfg
    import mapping_engine as engine_mod
    import mapping_orchestrator as orch

VERIFY_SCHEMA = "verify"   # gold_candidate/gold_entity_lineage와 같은 catalog 아래 검증 전용 스키마


def _snapshot(spark, source_table: str, snapshot_table: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.UC_CATALOG}.{VERIFY_SCHEMA}")
    (spark.table(source_table).write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").saveAsTable(snapshot_table))


def _pk_column(spark, target_table: str):
    rows = (spark.table(cfg.TARGET_MODEL_TABLE)
            .filter(F.upper(F.trim(F.col("TABLE_NAME"))) == target_table.upper())
            .filter(F.upper(F.trim(F.col("KEY"))) == "PK")
            .select(F.trim(F.col("COLUMN_NAME")).alias("c")).distinct().collect())
    return rows[0]["c"] if len(rows) == 1 else None


def _run_existing_path(engine, target_table: str) -> dict:
    """03_mapping_run.py 셀 로직 재현.
    CUSTOMER: §7 그대로 - 소스별 run()+save() 반복 후, 반복문 밖에서 integrate() 정확히 1회.
    그 외(CONTRACT/COUNSEL 등): §6 그대로 - 소스마다 run()+save() 직후 매번 integrate()를 호출한다
    (DIRECT/UNION Target은 no-op이라 결과에 영향 없음 - 기존 노트북 주석과 동일한 전제)."""
    target_table = target_table.upper()
    sources = engine.discover_sources(target_table)
    integrate_call_log = []
    if target_table == "CUSTOMER":
        for src in sources:
            candidate, summary = engine.run(src, target_table)
            engine.save(candidate, summary)
        result = engine.integrate(target_table)
        integrate_call_log.append({"after_source": "ALL_SOURCES", "result": result})
    else:
        for src in sources:
            candidate, summary = engine.run(src, target_table)
            engine.save(candidate, summary)
            result = engine.integrate(target_table)   # 기존 §6: 매 소스 처리 직후 항상 호출
            integrate_call_log.append({"after_source": src, "result": result})
    return {"sources": sources, "integrate_call_log": integrate_call_log}


def _run_existing_path_master(engine, target_table: str) -> dict:
    """product_master_run.py 재현. id_column/key_column/prefix는 그 Notebook과 동일하게 하드코딩된 값을
    그대로 쓴다 (target_model 동적 도출과 결과가 같은지 확인하는 게 목적이므로, 기존 경로는 의도적으로
    기존 하드코딩을 유지한다)."""
    hardcoded = {"PRODUCT": {"id_column": "PRD_ID", "key_column": "PRD_CD", "prefix": "PRD"}}
    params = hardcoded[target_table.upper()]
    candidate, summary = engine.run("PRODUCT_MASTER", target_table)
    result = engine.load_master_data(target_table, candidate, summary, **params)
    return {"sources": ["PRODUCT_MASTER"], "master_params": params, "load_result": result}


# run()이 매 호출마다 새로 발급하는 비결정적(non-deterministic) 컬럼. 기존 경로/신규 경로가 서로 다른
# engine.run() 호출이므로 이 값들은 실제 데이터가 완전히 같아도 항상 다르다 - 값 diff(항목 5)에서
# 제외해야 "진짜 데이터 차이"만 볼 수 있다. _mapping_version/_map_errors/_unmapped_codes/lineage 컬럼은
# 같은 입력이면 결정적으로 같은 값이 나오므로 제외하지 않는다(1차 실행에서 이미 실증됨).
_NON_DETERMINISTIC_COLS = {"_mapping_run_id", "_mapped_at"}


# mapping_engine.py의 run()이 매 호출마다 새로 만드는 비결정적 컬럼 (같은 데이터를 두 번 실행해도 값이
# 다른 게 정상). 값 diff(5번 항목)에서만 제외한다 - row count/schema/PK 중복/NULL count는 애초에 이
# 컬럼들의 "값"이 아니라 "존재 여부/개수"만 보므로 영향이 없었다(실제로 그 4개 항목은 모두 일치했었다).
#   _mapping_run_id : f"MAP-{datetime.now():%Y%m%d%H%M%S}-...-{uuid4().hex[:6]}" - 시각+uuid 포함
#   _mapped_at      : datetime.now() - 호출 시각 그 자체
# _mapping_version은 정의(mapping_definition) 해시 기반이라 결정적이므로 제외하지 않는다 - 여기에 diff가
# 남는다면 그건 진짜 의심해야 할 신호다.
NONDETERMINISTIC_COLS = {"_mapping_run_id", "_mapped_at"}


def _compare_tables(spark, label: str, existing_table: str, new_table: str, pk_column=None) -> dict:
    report = {"label": label}
    if not spark.catalog.tableExists(existing_table) or not spark.catalog.tableExists(new_table):
        report["error"] = (f"스냅샷 테이블 누락 (existing 존재={spark.catalog.tableExists(existing_table)}, "
                            f"new 존재={spark.catalog.tableExists(new_table)})")
        return report

    df_a, df_b = spark.table(existing_table), spark.table(new_table)

    # 1) row count
    n_a, n_b = df_a.count(), df_b.count()
    report["row_count"] = {"existing": n_a, "new": n_b, "match": n_a == n_b}

    # 2) schema
    schema_a = [(f.name, str(f.dataType)) for f in df_a.schema.fields]
    schema_b = [(f.name, str(f.dataType)) for f in df_b.schema.fields]
    report["schema"] = {
        "match": schema_a == schema_b,
        "existing_only": sorted(set(schema_a) - set(schema_b)),
        "new_only": sorted(set(schema_b) - set(schema_a)),
    }
    common_cols = [c for c in df_a.columns if c in df_b.columns]

    # 3) PK 중복
    if pk_column and pk_column in common_cols:
        dup_a = df_a.groupBy(pk_column).count().filter(F.col("count") > 1).count()
        dup_b = df_b.groupBy(pk_column).count().filter(F.col("count") > 1).count()
        report["pk_duplicates"] = {"pk_column": pk_column, "existing": dup_a, "new": dup_b, "match": dup_a == dup_b}
    else:
        report["pk_duplicates"] = {"pk_column": pk_column, "note": "PK 컬럼을 공통 컬럼에서 찾지 못해 생략"}

    # 4) 컬럼별 NULL count (lineage 등 내부 컬럼 '_'는 제외)
    null_cols = [c for c in common_cols if not c.startswith("_")]
    null_diff = {}
    if null_cols:
        null_a = df_a.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in null_cols]).collect()[0].asDict()
        null_b = df_b.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in null_cols]).collect()[0].asDict()
        null_diff = {c: {"existing": null_a[c], "new": null_b[c]} for c in null_cols if null_a[c] != null_b[c]}
    report["null_counts"] = {"mismatched_columns": null_diff, "match": len(null_diff) == 0}

    # 5) 컬럼별 값 diff (레코드 단위 - exceptAll 양방향). 호출마다 값이 바뀌는 게 정상인 컬럼
    # (_mapping_run_id/_mapped_at)은 제외한다 - 안 그러면 비즈니스 데이터가 완전히 같아도 매번 100%
    # 다르다고 나온다(1차 실행에서 실제로 발생한 문제).
    diff_cols = [c for c in common_cols if c not in NONDETERMINISTIC_COLS]
    only_in_existing = df_a.select(*diff_cols).exceptAll(df_b.select(*diff_cols)).count()
    only_in_new = df_b.select(*diff_cols).exceptAll(df_a.select(*diff_cols)).count()
    report["value_diff"] = {
        "compared_columns": diff_cols,
        "excluded_nondeterministic_columns": sorted(NONDETERMINISTIC_COLS & set(common_cols)),
        "rows_only_in_existing": only_in_existing,
        "rows_only_in_new": only_in_new,
        "match": only_in_existing == 0 and only_in_new == 0,
    }

    # 6) 종합 판정
    report["overall_match"] = (
        report["row_count"]["match"] and report["schema"]["match"]
        and report["pk_duplicates"].get("match", True)
        and report["null_counts"]["match"] and report["value_diff"]["match"]
    )
    return report


def verify_target(spark, engine, target_table: str, is_master: bool = False) -> dict:
    target_table = target_table.upper()
    gold_candidate = cfg.gold_candidate_table(target_table)
    entity_lineage = cfg.gold_entity_lineage_table(target_table)
    existing_gc = f"{cfg.UC_CATALOG}.{VERIFY_SCHEMA}.{target_table.lower()}__existing_gold_candidate"
    new_gc = f"{cfg.UC_CATALOG}.{VERIFY_SCHEMA}.{target_table.lower()}__new_gold_candidate"
    existing_el = f"{cfg.UC_CATALOG}.{VERIFY_SCHEMA}.{target_table.lower()}__existing_entity_lineage"
    new_el = f"{cfg.UC_CATALOG}.{VERIFY_SCHEMA}.{target_table.lower()}__new_entity_lineage"

    print(f"\n{'=' * 60}\n[{target_table}] 기존 경로 실행\n{'=' * 60}")
    existing_meta = _run_existing_path_master(engine, target_table) if is_master else _run_existing_path(engine, target_table)
    _snapshot(spark, gold_candidate, existing_gc)
    has_lineage = spark.catalog.tableExists(entity_lineage)
    if has_lineage:
        _snapshot(spark, entity_lineage, existing_el)
    print("기존 경로 실행 메타:", existing_meta.get("integrate_call_log", existing_meta.get("load_result")))

    print(f"\n{'=' * 60}\n[{target_table}] 신규 경로 실행 (mapping_orchestrator.run_target)\n{'=' * 60}")
    new_result = orch.run_target(engine, target_table)
    _snapshot(spark, gold_candidate, new_gc)
    if spark.catalog.tableExists(entity_lineage):
        _snapshot(spark, entity_lineage, new_el)
    print(f"신규 경로 결과: integration_type={new_result['integration_type']}, sources={new_result['sources']}")

    pk_column = _pk_column(spark, target_table)
    gc_report = _compare_tables(spark, f"{target_table}.gold_candidate", existing_gc, new_gc, pk_column=pk_column)
    el_report = _compare_tables(spark, f"{target_table}.gold_entity_lineage", existing_el, new_el) if has_lineage else None

    return {
        "target_table": target_table,
        "existing_path_meta": existing_meta,
        "new_path_result": new_result,
        "gold_candidate_diff": gc_report,
        "gold_entity_lineage_diff": el_report,
    }


def recompare_from_existing_snapshots(spark, target_tables: list) -> list:
    """1차 실행에서 이미 만들어 둔 verify 스키마 스냅샷을 재사용해 값 diff만 다시 계산한다 - 전체
    파이프라인(기존 경로+신규 경로)을 재실행할 필요가 없다. NONDETERMINISTIC_COLS 수정 반영 확인용."""
    reports = []
    for target_table in target_tables:
        target_table = target_table.upper()
        existing_gc = f"{cfg.UC_CATALOG}.{VERIFY_SCHEMA}.{target_table.lower()}__existing_gold_candidate"
        new_gc = f"{cfg.UC_CATALOG}.{VERIFY_SCHEMA}.{target_table.lower()}__new_gold_candidate"
        existing_el = f"{cfg.UC_CATALOG}.{VERIFY_SCHEMA}.{target_table.lower()}__existing_entity_lineage"
        new_el = f"{cfg.UC_CATALOG}.{VERIFY_SCHEMA}.{target_table.lower()}__new_entity_lineage"
        pk_column = _pk_column(spark, target_table)
        gc_report = _compare_tables(spark, f"{target_table}.gold_candidate", existing_gc, new_gc, pk_column=pk_column)
        el_report = None
        if spark.catalog.tableExists(existing_el):
            el_report = _compare_tables(spark, f"{target_table}.gold_entity_lineage", existing_el, new_el)
        reports.append({"target_table": target_table, "gold_candidate_diff": gc_report, "gold_entity_lineage_diff": el_report})

        gc_match = gc_report.get("overall_match")
        el_match = el_report["overall_match"] if el_report else "N/A"
        print(f"{target_table:10s} gold_candidate 일치={gc_match}  gold_entity_lineage 일치={el_match}")
        if not (gc_match and (el_report is None or el_match)):
            print(f"  ⚠️  gold_candidate_diff: {gc_report}")
            if el_report:
                print(f"  ⚠️  gold_entity_lineage_diff: {el_report}")
    return reports


# ---- 재비교만 실행 (1차 실행에서 만든 스냅샷을 그대로 재사용 - 전체 파이프라인 재실행 불필요) ----
# recompare_from_existing_snapshots(spark, ["COUNSEL", "CUSTOMER", "CONTRACT", "PRODUCT"])

# ---- 전체 재실행 (기존 경로+신규 경로를 처음부터 다시 실행) ----
engine = engine_mod.MappingEngine.from_tables(spark)

all_reports = []
for target in ["COUNSEL", "CUSTOMER", "CONTRACT"]:
    all_reports.append(verify_target(spark, engine, target))

# PRODUCT는 product_master_metadata_patch.py를 먼저 실행해 target_model KEY / entity_integration_definition
# MASTER 행이 반영된 뒤에만 의미 있게 동작한다.
try:
    all_reports.append(verify_target(spark, engine, "PRODUCT", is_master=True))
except Exception as e:
    print(f"\n⚠️  PRODUCT 검증 실패 (product_master_metadata_patch.py를 먼저 실행했는지 확인하세요): {e}")

print("\n" + "=" * 60)
print("종합 결과")
print("=" * 60)
for r in all_reports:
    gc_match = r["gold_candidate_diff"].get("overall_match")
    el_match = r["gold_entity_lineage_diff"]["overall_match"] if r["gold_entity_lineage_diff"] else "N/A (DIRECT/MASTER)"
    print(f"{r['target_table']:10s} gold_candidate 일치={gc_match}  gold_entity_lineage 일치={el_match}")
    if r["target_table"] == "CONTRACT":
        n_sources = len(r["existing_path_meta"]["sources"])
        print(f"  └─ CONTRACT 승인 소스 수={n_sources}개 → "
              f"{'1개뿐이라 두 경로 모두 실질적으로 integrate() 1회 호출로 귀결 (구조적 차이 재현 불가)' if n_sources <= 1 else '여러 소스 - 아래 integrate_call_log 비교 필요'}")
        print(f"     기존 경로 integrate_call_log: {r['existing_path_meta']['integrate_call_log']}")

    if not (r["gold_candidate_diff"].get("overall_match") and
            (r["gold_entity_lineage_diff"] is None or r["gold_entity_lineage_diff"]["overall_match"])):
        print(f"  ⚠️  {r['target_table']}: 불일치 발견 - 아래 report 상세 확인 필요")
        print(f"     gold_candidate_diff: {r['gold_candidate_diff']}")
        if r["gold_entity_lineage_diff"]:
            print(f"     gold_entity_lineage_diff: {r['gold_entity_lineage_diff']}")