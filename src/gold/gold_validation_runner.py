"""
Gold Target Validation 러너.

    gold_candidate.<target> (물리 Delta 테이블, Mapping Engine의 save()가 저장 - _map_errors가 있던 행은
    애초에 여기 없다. mapping_run.py와 이 노트북이 같은 세션일 필요가 없다)
        │
        ├─ 1) meta.target_model에서 검사 규칙을 매번 다시 만든다 (SCHEMA/NOT_NULL/UNIQUE/LENGTH/DOMAIN)
        │     + gold_validation_config.BUSINESS_RULES (컬럼 간 업무 규칙)
        ▼
    검사 (레코드별로 위반 규칙 목록을 만든다)
        │
        ├─ 2) 승인된 보정 규칙(PK_DEDUP_RULES)이 있는 위반이면 보정
        ▼
    재검사 (보정된 값으로 규칙을 다시 적용 - DQ의 자동 Cleansing과 같은 원칙)
        │
        ├─ 위반 없음 ─────────────→ Gold Target 테이블 (gold.<target>)
        └─ 위반 남음 ─────────────→ gold_quarantine.<target>
        │
        ▼
    3) 모든 원천 레코드를 MIGRATION_TRACE에 기록 (LOADED / GOLD_QUARANTINED)

DQ 단계와 다른 점: 이 모듈은 DQ 격리(dq_quarantine)나 Silver 확정(HITL)을 다루지 않는다. 입력은 이미
DQ와 Mapping을 통과한 gold_candidate뿐이라, 여기서 나오는 격리는 오직 "Target 제약 위반"이다.
"""
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

try:
    import src.gold.gold_validation_config as cfg
except ModuleNotFoundError:
    import gold_validation_config as cfg

_LINEAGE_COLS = ["_source_system", "_source_table", "_source_record_key", "_source_batch_id"]


def _spark_type(data_type: str) -> str:
    t = data_type.strip().upper()
    if t.startswith("VARCHAR") or t.startswith("CHAR") or t == "TEXT":
        return "string"
    if t == "TIMESTAMP":
        return "timestamp"
    if t == "DATE":
        return "date"
    return "string"


def derive_rules(target_model_df: DataFrame, target_table: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Optional[str]]:
    """target_model에서 (컬럼 정의 목록, 검사 규칙 목록, PK 컬럼명)을 만든다. 모델이 바뀌면 규칙도 그대로 따라간다."""
    rows = [r.asDict() for r in target_model_df.filter(F.upper(F.trim("TABLE_NAME")) == target_table.upper())
            .orderBy(F.col("ORDINAL").cast("int")).collect()]
    if not rows:
        raise ValueError(f"TO-BE 모델에 '{target_table}'이(가) 없습니다.")

    rules: List[Dict[str, Any]] = []
    pk_column = None
    seq = 0
    for r in rows:
        col, dtype, key, nul, desc = r["COLUMN_NAME"], r["DATA_TYPE"], (r["KEY"] or ""), r["NULLABLE"], (r["DESCRIPTION"] or "")
        keys = [k.strip() for k in key.split(",") if k.strip()]
        seq += 1
        if nul == "N":
            rules.append({"rule_id": f"VR-{target_table}-{seq:03d}-NOTNULL", "type": "NOT_NULL", "columns": [col],
                          "check": lambda c=col: F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == "")})
        if "PK" in keys or "UK" in keys:
            if "PK" in keys and pk_column is None:
                pk_column = col
            rules.append({"rule_id": f"VR-{target_table}-{seq:03d}-UNIQUE", "type": "UNIQUE", "columns": [col],
                          "check": lambda c=col: F.count(F.lit(1)).over(Window.partitionBy(c)) > 1,
                          "requires_key_notnull": col})   # NULL 키는 UNIQUE로 안 잡는다 (NOT_NULL이 이미 잡음)
        m = re.match(r"(VAR)?CHAR\((\d+)\)", dtype.upper())
        if m:
            n = int(m.group(2))
            rules.append({"rule_id": f"VR-{target_table}-{seq:03d}-LENGTH", "type": "LENGTH", "columns": [col],
                          "check": lambda c=col, n=n: F.length(F.col(c)) > F.lit(n)})
        # TYPE(_map_errors 재검사) 규칙은 없앴다: Mapping Engine이 _map_errors가 있는 행을 gold_candidate에
        # 넣지 않고 mapping_error 테이블로 따로 보내므로(mapping_engine.save() 참고), 여기서 다시 검사하면
        # 항상 위반 없음(dead code)일 뿐 아니라 이중 격리 구조를 다시 만들게 된다. Gold Validation의 실패는
        # 오직 "Mapping은 끝났지만 TO-BE 품질/업무 규칙을 만족 못한 경우"로 한정한다.
        enum = re.fullmatch(r"([A-Z]+(?:/[A-Z]+)+)", desc.strip())
        if enum:
            vals = enum.group(1).split("/")
            rules.append({"rule_id": f"VR-{target_table}-{seq:03d}-DOMAIN", "type": "DOMAIN", "columns": [col],
                          "check": lambda c=col, v=vals: F.col(c).isNotNull() & ~F.col(c).isin(v)})

    for br in cfg.BUSINESS_RULES.get(target_table, []):
        if br["type"] == "ORDER":
            a, b = br["columns"]
            rules.append({"rule_id": br["rule_id"], "type": "BUSINESS", "columns": [a, b],
                          "check": lambda a=a, b=b: F.col(a).isNotNull() & F.col(b).isNotNull() & (F.col(a) > F.col(b))})

    return rows, rules, pk_column


_FK_REF_PATTERN = re.compile(r"→\s*([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)")


def derive_relationship_specs(target_model_df: DataFrame, target_table: str) -> List[Dict[str, str]]:
    """target_model에서 KEY='FK'인 컬럼의 DESCRIPTION(예: '→ CUSTOMER.CUST_ID')을 파싱해 관계 검증
    스펙(참조 테이블/컬럼, rule_id)을 만든다. 실제 참조 테이블 조회는 run()에서 한다(여긴 순수 함수로 유지).
    derive_rules()와 같은 ordinal seq로 rule_id를 매겨 VR-{TARGET}-{seq:03d}-FK 컨벤션을 따른다."""
    rows = [r.asDict() for r in target_model_df.filter(F.upper(F.trim("TABLE_NAME")) == target_table.upper())
            .orderBy(F.col("ORDINAL").cast("int")).collect()]
    if not rows:
        raise ValueError(f"TO-BE 모델에 '{target_table}'이(가) 없습니다.")

    specs: List[Dict[str, str]] = []
    seq = 0
    for r in rows:
        col, key, desc = r["COLUMN_NAME"], (r["KEY"] or ""), (r["DESCRIPTION"] or "")
        keys = [k.strip() for k in key.split(",") if k.strip()]
        seq += 1
        if "FK" not in keys:
            continue
        m = _FK_REF_PATTERN.search(desc)
        if not m:
            raise ValueError(
                f"{target_table}.{col}은(는) KEY=FK이지만 DESCRIPTION에서 '→ TABLE.COLUMN' 참조를 "
                f"파싱하지 못했습니다 (DESCRIPTION='{desc}'). meta.target_model을 확인하세요."
            )
        specs.append({
            "rule_id": f"VR-{target_table}-{seq:03d}-FK",
            "fk_column": col,
            "ref_table": m.group(1).upper(),
            "ref_column": m.group(2).upper(),
        })
    return specs


def _apply_rules(df: DataFrame, rules: List[Dict[str, Any]]) -> DataFrame:
    """각 규칙의 위반 여부를 계산해 '_violations'(콤마 구분 rule_id 목록, 없으면 NULL) 컬럼을 붙인다."""
    parts = []
    for rule in rules:
        cond = rule["check"]()
        if rule.get("requires_key_notnull"):   # UNIQUE는 NULL 값끼리는 묶지 않는다 (NOT_NULL이 별도로 잡음)
            key_col = rule["requires_key_notnull"]
            cond = cond & F.col(key_col).isNotNull() & (F.trim(F.col(key_col).cast("string")) != "")
        parts.append(F.when(cond, F.lit(rule["rule_id"])))
    if not parts:
        return df.withColumn("_violations", F.lit(None).cast("string"))
    joined = F.concat_ws(",", *parts)
    return df.withColumn("_violations", F.when(F.length(joined) > 0, joined))


def _remediate_pk_dedup(df: DataFrame, target_table: str) -> Tuple[DataFrame, int]:
    """PK_DEDUP_RULES에 해당하는 소스의 PK 중복만 결정적으로 새 값을 만든다(Review R3). 다른 소스의 PK 중복은 손대지 않는다(격리 대상으로 남음)."""
    spec = cfg.PK_DEDUP_RULES.get(target_table)
    if spec is None:
        return df, 0
    pk = spec["pk_column"]
    w = Window.partitionBy(pk).orderBy(F.col(spec["order_by"]))
    ranked = df.withColumn("_pk_dup_rank", F.when(F.col("_source_system") == spec["applies_to_source"],
                                                   F.row_number().over(w)).otherwise(F.lit(1)))
    dup_count = (ranked.filter(F.col("_pk_dup_rank") > 1)
                 .filter(F.col("_source_system") == spec["applies_to_source"]).count())
    fixed_pk = F.when(F.col("_pk_dup_rank") > 1,
                      F.concat(F.col(pk), F.lit(spec["suffix_format"].format(n="")), F.col("_pk_dup_rank").cast("string"))
                      ).otherwise(F.col(pk))
    out = (ranked.withColumn(f"_remediated_{pk}", F.when(F.col("_pk_dup_rank") > 1, F.col(pk)))  # 보정 전 원래 값 보존
           .withColumn(pk, fixed_pk).drop("_pk_dup_rank"))
    return out, dup_count


class TargetValidator:
    def __init__(self, spark: SparkSession, target_model_df: DataFrame):
        self.spark = spark
        self._target_model = target_model_df

    @classmethod
    def from_tables(cls, spark: SparkSession) -> "TargetValidator":
        return cls(spark, spark.read.table(cfg.TARGET_MODEL_TABLE))

    def run(self, target_table: str, candidate: Optional[DataFrame] = None) -> Tuple[DataFrame, DataFrame, Dict[str, Any]]:
        """
        candidate를 주지 않으면 gold_candidate.<target> 물리 테이블을 읽는다 (mapping_engine.save()가 저장한 것 — 소스별
        (source, batch) 단위로 갱신되므로 이미 실행된 모든 소스가 합쳐진 상태로 보인다. 다른 노트북/세션에서 실행해도 이어진다).
        (검증 통과 → Gold 형태 DataFrame, 격리 → 원천 계보+위반 사유 DataFrame, summary)를 돌려준다. 저장은 save()에서 한다.
        """
        target_table = target_table.upper()
        if candidate is None:
            candidate = self.spark.table(cfg.gold_candidate_table(target_table))

        model_rows, rules, pk_column = derive_rules(self._target_model, target_table)
        target_cols = [r["COLUMN_NAME"] for r in model_rows]
        missing = [c for c in target_cols if c not in candidate.columns]
        if missing:
            raise ValueError(f"gold_candidate에 Target 컬럼이 없습니다: {missing} (Mapping Engine 결과가 아닌 것으로 보입니다)")

        # Relationship Validation: 기존 rules 리스트에 FK 규칙을 그대로 추가한다 (별도 실행 단계를 만들지 않음 -
        # _apply_rules()/_remediate_pk_dedup()는 무수정으로 아래에서 한 번에 처리된다).
        relationship_specs = derive_relationship_specs(self._target_model, target_table)
        relationship_rules_checked: List[str] = []
        relationship_rules_skipped: List[Dict[str, str]] = []
        for spec in relationship_specs:
            ref_gold_table = cfg.gold_table(spec["ref_table"])
            if not self.spark.catalog.tableExists(ref_gold_table):
                # 참조 테이블이 아직 없음 = 데이터 오류가 아니라 실행 순서 문제. FAIL로 만들지 않고 SKIP하되,
                # "검증 안 함"을 summary에 명시적으로 남겨 PASS로 오인되지 않게 한다.
                relationship_rules_skipped.append({
                    "rule_id": spec["rule_id"], "fk_column": spec["fk_column"],
                    "ref_table": spec["ref_table"], "ref_column": spec["ref_column"],
                    "reason": f"참조 테이블 {ref_gold_table}이(가) 아직 없습니다 (해당 테이블을 먼저 실행하세요)",
                })
                continue
            ref_ids = [row[0] for row in
                      self.spark.table(ref_gold_table).select(spec["ref_column"]).distinct().collect()]
            fk_col = spec["fk_column"]
            rules.append({"rule_id": spec["rule_id"], "type": "FK", "columns": [fk_col],
                          "check": lambda c=fk_col, ids=ref_ids: F.col(c).isNotNull() & ~F.col(c).isin(ids)})
            relationship_rules_checked.append(spec["rule_id"])

        checked = _apply_rules(candidate, rules)
        remediated, dedup_fixed = _remediate_pk_dedup(checked, target_table)
        # 보정된 값으로 규칙을 다시 적용한다 (DQ의 재-DQ와 같은 원칙: 보정 후 통과했는지 다시 확인)
        rechecked = _apply_rules(remediated.drop("_violations"), rules)

        run_id = f"VAL-{datetime.now():%Y%m%d%H%M%S}-{target_table.lower()}-{uuid4().hex[:6]}"

        passed = rechecked.filter(F.col("_violations").isNull()).select(
            *target_cols, *_LINEAGE_COLS, F.lit(run_id).alias("_validation_run_id"))
        failed = rechecked.filter(F.col("_violations").isNotNull()).select(
            *target_cols, *_LINEAGE_COLS, "_violations", F.lit(run_id).alias("_validation_run_id"))

        input_count = candidate.count()
        pass_count, fail_count = passed.count(), failed.count()
        viol_counts = {r["v"]: r["cnt"] for r in
                      failed.select(F.explode(F.split("_violations", ",")).alias("v"))
                      .groupBy("v").agg(F.count("*").alias("cnt")).collect()}
        summary = {
            "validation_run_id": run_id, "target_table": target_table, "pk_column": pk_column,
            "input_count": input_count, "loaded_count": pass_count, "quarantined_count": fail_count,
            "reconciled": input_count == pass_count + fail_count,   # VR-COUNSEL-024
            "violations_by_rule": viol_counts,
            "pk_dedup_fixed": dedup_fixed,
            "rules_applied": [r["rule_id"] for r in rules],
            "relationship_rules_checked": relationship_rules_checked,
            "relationship_rules_skipped": relationship_rules_skipped,   # 비어있지 않으면 일부 FK가 검증되지 않은 것
        }
        return passed, failed, summary

    def save(self, passed: DataFrame, failed: DataFrame, summary: Dict[str, Any]) -> Dict[str, str]:
        """Gold Target(append) + gold_quarantine(소스·배치 단위 replaceWhere) + MIGRATION_TRACE(추가 전용)."""
        target_table = summary["target_table"]
        gold_t, quarantine_t = cfg.gold_table(target_table), cfg.gold_quarantine_table(target_table)
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.UC_CATALOG}.{cfg.GOLD_SCHEMA}")
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.UC_CATALOG}.{cfg.GOLD_QUARANTINE_SCHEMA}")

        # Gold Target: TO-BE 모델 그대로, 원천 계보 컬럼 없음. 배치 식별자가 없어 replaceWhere를 못 쓰므로 추가만 한다
        # (같은 배치를 재실행하면 중복 적재된다 - 알려진 제약, 3차에서 MIGRATION_TRACE 기준 멱등 적재로 보완 예정).
        target_cols = [c for c in passed.columns if not c.startswith("_")]
        gold_out = passed.select(*target_cols)
        if not self.spark.catalog.tableExists(gold_t):
            gold_out.limit(0).write.format("delta").mode("overwrite").saveAsTable(gold_t)
        gold_out.write.format("delta").mode("append").saveAsTable(gold_t)

        # gold_quarantine: 원천 계보가 있으므로 (소스, 배치) 단위로 교체한다 (Mapping Engine의 save()와 같은 방식)
        if not self.spark.catalog.tableExists(quarantine_t):
            failed.limit(0).write.format("delta").mode("overwrite").saveAsTable(quarantine_t)
        batches = [(r["_source_system"], r["_source_batch_id"]) for r in
                  failed.select("_source_system", "_source_batch_id").distinct().collect()]
        for src, batch in batches:
            pred = f"_source_system = '{src}' AND _source_batch_id = '{batch}'"
            (failed.filter((F.col("_source_system") == src) & (F.col("_source_batch_id") == batch))
             .write.format("delta").mode("overwrite").option("replaceWhere", pred)
             .option("mergeSchema", "true").saveAsTable(quarantine_t))

        self._write_migration_trace(passed, failed, target_table)
        return {"gold_table": gold_t, "gold_quarantine_table": quarantine_t, "migration_trace_table": cfg.MIGRATION_TRACE_TABLE}

    def _write_migration_trace(self, passed: DataFrame, failed: DataFrame, target_table: str) -> None:
        """모든 원천 레코드(통과·격리 모두)를 MIGRATION_TRACE에 남긴다. MIG_ST_CD 값은 잠정안(DA 확인 대기,
        gold_validation_rule_counsel_v0_1.xlsx의 PM_확인_사항 Q4: LOADED/DQ_QUARANTINED/GOLD_QUARANTINED/EXCLUDED)."""
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.UC_CATALOG}.{cfg.GOLD_SCHEMA}")
        now = datetime.now()
        pk_col = next((c for c in passed.columns if c.endswith("_ID") and not c.startswith("_")), None)
        loaded = passed.select(
            F.expr("uuid()").alias("TRC_ID"), F.col("_source_batch_id").alias("MIG_BATCH_ID"),
            F.col("_source_system").alias("SRC_SYS"), F.col("_source_table").alias("SRC_TBL"),
            F.col("_source_record_key").alias("SRC_KEY"), F.lit(target_table).alias("TGT_TBL"),
            (F.col(pk_col) if pk_col else F.lit(None).cast("string")).alias("TGT_KEY"),
            F.lit("LOADED").alias("MIG_ST_CD"), F.lit(now).alias("LOAD_DTM"),
        )
        quarantined = failed.select(
            F.expr("uuid()").alias("TRC_ID"), F.col("_source_batch_id").alias("MIG_BATCH_ID"),
            F.col("_source_system").alias("SRC_SYS"), F.col("_source_table").alias("SRC_TBL"),
            F.col("_source_record_key").alias("SRC_KEY"), F.lit(target_table).alias("TGT_TBL"),
            F.lit(None).cast("string").alias("TGT_KEY"),
            F.lit("GOLD_QUARANTINED").alias("MIG_ST_CD"), F.lit(now).alias("LOAD_DTM"),
        )
        trace = loaded.unionByName(quarantined)
        if not self.spark.catalog.tableExists(cfg.MIGRATION_TRACE_TABLE):
            trace.limit(0).write.format("delta").mode("overwrite").saveAsTable(cfg.MIGRATION_TRACE_TABLE)
        trace.write.format("delta").mode("append").saveAsTable(cfg.MIGRATION_TRACE_TABLE)