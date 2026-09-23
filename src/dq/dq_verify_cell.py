# =============================================================================
# DQ / Cleansing 로직 검증 셀  (Databricks 노트북에 셀 하나로 붙여넣어 실행)
#
# - 16행짜리 테스트 데이터를 별도 스키마(maps_databricks.dq_test)에 만들고 "실제 DQRunner"를 돌립니다.
# - 행마다 "기대 결과"를 적어 두고, 실제 결과와 나란히 비교해 ✅/❌로 보여줍니다.
# - 규칙은 코드가 아니라 "규칙 테이블"(dq_test.dq_rule_def)에서 읽어 실행합니다 (운영과 같은 방식). 규칙 버전 관리도 함께 검증합니다.
# - 이 단계에서는 코드 매핑(CLN-VAL-003)을 하지 않습니다 (dq_config의 enabled=False). 코드값은 공백만 정리하고, 그래도 코드 마스터에 없으면 격리됩니다.
# - 운영 테이블은 건드리지 않습니다 (테이블 경로를 dq_test 쪽으로 바꿔서 실행, 실행할 때마다 스키마를 비우고 새로 만듭니다).
# - ❌가 나오면 그 행이 로직 문제이거나, 기대값(=제가 이해한 요구사항)이 틀린 것입니다. 어느 쪽인지 같이 확인해 주세요.
# =============================================================================
import importlib
import pandas as pd
from pyspark.sql import functions as F

try:
    import src.dq.dq_config as dq_config
    import src.dq.dq_engine as dq_engine
except ModuleNotFoundError:
    import dq_config, dq_engine

# 파일을 교체한 뒤 노트북 세션에 이전 모듈이 남아 있는 문제를 막기 위해 의존 순서대로 다시 불러온다
for _m in (dq_config, dq_engine):
    importlib.reload(_m)

# ---------------------------------------------------------------------------
# 0. 테스트 환경: 운영 테이블 대신 dq_test 스키마를 쓰도록 경로/Rule을 바꾼다 (이 노트북 세션에서만 유효)
# ---------------------------------------------------------------------------
CAT = "maps_databricks"
TS = f"{CAT}.dq_test"
assert TS.split(".")[-1] == "dq_test", "안전장치: 테스트 스키마 이름은 dq_test여야 합니다 (아래에서 CASCADE 삭제함)"
BRONZE = f"{TS}.inbound"          # 테이블 이름이 inbound로 끝나야 source_system이 'inbound'가 된다
ING = "2026-09-20"

dq_config.CODE_MASTER_TABLE = f"{TS}.code_master"
dq_config.DQ_RESULT_TABLE = f"{TS}.dq_result"
dq_config.DQ_CLEANSING_DETAIL_TABLE = f"{TS}.dq_cleansing_detail"
dq_config.silver_candidate_table = lambda s: f"{TS}.silver_candidate_{s}"
dq_config.quarantine_table = lambda s: f"{TS}.quarantine_{s}"
dq_config.TABLE_RECORD_KEY_COLUMN = {BRONZE: "consultation_id"}

_rules = []
for _r in dq_config.DQ_RULES:
    if _r["target_table"].endswith(".bronze.inbound"):
        _r = dict(_r)
        _r["target_table"] = BRONZE
        if _r["rule_id"] in ("DQ-COM-IN-002", "DQ-COM-IN-003"):
            _r["threshold_rate"] = 0.5   # 검증용: 16행 중 1건 오류가 "허용 오류율 이내(ALLOW)"가 되도록 임계치만 올림
        _rules.append(_r)
dq_config.DQ_RULES = _rules

spark.sql(f"DROP SCHEMA IF EXISTS {TS} CASCADE")
spark.sql(f"CREATE SCHEMA {TS}")

# 규칙 테이블 (테스트용): 인바운드 규칙 9개를 운영과 같은 적재 경로(검증 -> 버전 판정 -> 추가)로 넣는다
RULE_TABLE = f"{TS}.dq_rule_def"
rule_repo = dq_engine.RuleRepository(spark, RULE_TABLE)
_init_plan = rule_repo.sync(dq_engine.code_rules_as_inputs(_rules), change_reason="검증 셀 초기 적재")

# 코드 마스터 (테스트용)
spark.createDataFrame(
    [("CONSULTATION_STATUS", c) for c in ["DONE", "WAIT", "CLOSED", "OPEN"]]
    + [("INBOUND_TYPE", "GEN"), ("POLICY_STATUS", "ACTIVE")],
    "CODE_GROUP string, CODE string",
).write.saveAsTable(dq_config.CODE_MASTER_TABLE)

# ---------------------------------------------------------------------------
# 1. 테스트 데이터 (case_id는 결과 대조용 컬럼이며 어떤 Rule도 보지 않는다)
# ---------------------------------------------------------------------------
def R(case, key, cust="CU1", phone="01011112222", status="DONE", itype="GEN", agent="A1",
      st="2026-09-20 10:00:00", en="2026-09-20 10:05:00"):
    return (case, key, cust, phone, status, itype, "ACTIVE", agent, st, en, ING)

rows = [
    R("T01", "K01"),                                              # 정상
    R("T02", "K02", phone="010-1234-5678"),                       # 전화 하이픈 -> 자동 정제
    R("T03", "K03", phone=" 010 3333 4444 "),                     # 전화 공백 -> 자동 정제
    R("T04", "K04", phone="010-ABCD-3510"),                       # 전화 정제 불가 (WARN 규칙이라 허용)
    R("T05", "K05", status="완료"),                                # 코드 매핑 보류 -> 코드 마스터에 없어 미해결(BLOCK) 격리
    R("T06", "K06", status="종료"),                                # 코드 마스터에 없음 -> 미해결(BLOCK) 격리
    R("T07", "K07", status=" WAIT "),                             # 공백만 문제 -> 자동 정제
    R("T08", "K08", status="대기중"),                              # 코드 마스터에 없음 -> 미해결 격리
    R("T09", "K09", phone="010-6505-9808", itype="미분류"),        # 전화는 정제, inbound_type 코드 오류는 Gold 미사용 컬럼(LOW)이라 격리 안 함
    R("T10", "K10", st="2026-09-20 11:00:00"),                    # 시각 순서 오류 -> 격리
    R("T11", "K11"),                                              # consultation_id 중복 (1/2)
    R("T12", "K11", phone="010-9999-8888"),                       # consultation_id 중복 (2/2) + 전화 정제
    R("T13", None),                                               # 업무키 NULL -> 격리
    R("T14", "K14", cust=None),                                   # customer_id 누락 1건 -> 허용 오류율 이내(ALLOW)
    R("T15", "K15", phone="010-5555-6666", agent=None),           # agent_id 누락 1건(ALLOW) + 전화 정제
    R("T16", "K16", phone="010-7777-8888", status="종료", itype="미분류"),  # 여러 Rule 동시 위반 (격리 사유는 status만)
]
spark.createDataFrame(
    rows,
    "case_id string, consultation_id string, customer_id string, phone_number string, status string, "
    "inbound_type string, policy_status string, agent_id string, started_at string, ended_at string, ingest_date string",
).write.saveAsTable(BRONZE)

# ---------------------------------------------------------------------------
# 2. 실행
# ---------------------------------------------------------------------------
runner = dq_engine.DQRunner(spark, rule_source="table", rule_table=RULE_TABLE)
results, summary = runner.run_table_dq(BRONZE, ING)

cand = spark.read.table(dq_config.silver_candidate_table("inbound"))
quar = spark.read.table(dq_config.quarantine_table("inbound"))
det = spark.read.table(dq_config.DQ_CLEANSING_DETAIL_TABLE)

# ---------------------------------------------------------------------------
# 3. 기대 결과 (행마다 "어디로 가고 / 어떤 상태이고 / 남은 위반이 뭐고 / 값이 어떻게 됐는가")
#    dest: candidate(silver_candidate) 또는 quarantine(격리)
# ---------------------------------------------------------------------------
E = {
    "T01": ("정상",                             "candidate",  "CLEAN",      set(), {"phone_number": "01011112222"}),
    "T02": ("전화 하이픈 -> 정제",               "candidate",  "CLEANSED",   set(), {"phone_number": "01012345678"}),
    "T03": ("전화 공백 -> 정제",                 "candidate",  "CLEANSED",   set(), {"phone_number": "01033334444"}),
    "T04": ("전화 정제불가(WARN, 허용)",          "candidate",  "CLEAN",      set(), {"phone_number": "010-ABCD-3510"}),
    "T05": ("status 완료, 매핑 보류 -> 격리",     "quarantine", "UNRESOLVED", {"DQ-VAL-IN-002"}, {"status": "완료"}),
    "T06": ("status 종료, 목록에 없음 -> 격리",    "quarantine", "UNRESOLVED", {"DQ-VAL-IN-002"}, {"status": "종료"}),
    "T07": ("status 공백 -> 정제",               "candidate",  "CLEANSED",   set(), {"status": "WAIT"}),
    "T08": ("status 대기중, 목록에 없음 -> 격리",  "quarantine", "UNRESOLVED", {"DQ-VAL-IN-002"}, {"status": "대기중"}),
    "T09": ("전화 정제 + inbound_type 오류(LOW) -> 통과", "candidate", "CLEANSED", set(), {"phone_number": "01065059808", "inbound_type": "미분류"}),
    "T10": ("시각 순서 오류 -> 격리",             "quarantine", "UNRESOLVED", {"DQ-CON-IN-001"}, {}),
    "T11": ("업무키 중복 1/2 -> 격리",           "quarantine", "UNRESOLVED", {"DQ-UNI-IN-001"}, {"phone_number": "01011112222"}),
    "T12": ("업무키 중복 2/2 + 전화 정제 -> 격리", "quarantine", "UNRESOLVED", {"DQ-UNI-IN-001"}, {"phone_number": "01099998888"}),
    "T13": ("업무키 NULL -> 격리",               "quarantine", "UNRESOLVED", {"DQ-COM-IN-001"}, {}),
    "T14": ("customer_id 누락(ALLOW) -> 통과",   "candidate",  "CLEAN",      set(), {"customer_id": None}),
    "T15": ("agent_id 누락(ALLOW)+전화 정제",    "candidate",  "CLEANSED",   set(), {"phone_number": "01055556666", "agent_id": None}),
    "T16": ("여러 Rule 위반, 격리 사유는 BLOCK만", "quarantine", "UNRESOLVED", {"DQ-VAL-IN-002"},
            {"phone_number": "01077778888", "status": "종료", "inbound_type": "미분류"}),
}

def _v(x):
    return "NULL" if x is None else str(x)

def _fmt(dest, dq, rules, vals):
    parts = [dest, dq, "사유=" + (",".join(sorted(rules)) if rules else "-")]
    parts += [f"{k}={_v(v)}" for k, v in vals.items()]
    return " | ".join(parts)

actual = {}
for _dest, _sdf in (("candidate", cand), ("quarantine", quar)):
    for _r in _sdf.collect():
        _d = _r.asDict()
        _d["_dest"] = _dest
        actual.setdefault(_d["case_id"], []).append(_d)

case_rows = []
for cid, (desc, dest, dq, rules, vals) in E.items():
    exp = _fmt(dest, dq, rules, vals)
    got = actual.get(cid, [])
    if len(got) != 1:
        act = "없음" if not got else f"{len(got)}곳에 존재"
    else:
        g = got[0]
        g_rules = set((g.get("_unresolved_rule_ids") or "").split(",")) - {""}
        act = _fmt(g["_dest"], g["_dq_status"], g_rules, {k: g[k] for k in vals})
    case_rows.append({"사례": cid, "내용": desc, "기대": exp, "실제": act, "결과": "✅" if exp == act else "❌"})
t_cases = pd.DataFrame(case_rows)

# ---------------------------------------------------------------------------
# 4. 규칙 단위 (dq_result) : 판정 / 오류 건수 / 자동정제 건수 / 미해결 건수
# ---------------------------------------------------------------------------
ER = {   # rule_id: (action_type, error_count, auto_cleansed, unresolved)
    "DQ-COM-IN-001": ("BLOCK", 1, 0, 1),
    "DQ-COM-IN-002": ("ALLOW", 1, 0, 0),
    "DQ-COM-IN-003": ("ALLOW", 1, 0, 0),
    "DQ-VAL-IN-001": ("WARN",  7, 6, 0),
    "DQ-VAL-IN-002": ("BLOCK", 5, 1, 4),
    "DQ-VAL-IN-003": ("WARN",  2, 0, 0),   # Gold 미사용 컬럼 -> 등급 LOW, 격리하지 않음
    "DQ-CON-IN-001": ("BLOCK", 1, 0, 1),
    "DQ-UNI-IN-001": ("BLOCK", 1, 0, 2),   # 중복은 error_count=중복 그룹 수, 미해결은 그룹에 속한 "행" 수
    "DQ-VAL-IN-004": ("ALLOW", 0, None, None),  # 오류 없음
}
by = {r["rule_id"]: r for r in results}
rule_rows = []
for rid, exp in ER.items():
    r = by.get(rid)
    act = None if r is None else (r["action_type"], r["error_count"], r["auto_cleansed_record_count"], r["unresolved_record_count"])
    rule_rows.append({"규칙": rid, "기대(판정,오류,자동정제,미해결)": str(exp), "실제": str(act), "결과": "✅" if act == exp else "❌"})
t_rules = pd.DataFrame(rule_rows)

# ---------------------------------------------------------------------------
# 5. 상세 (dq_cleansing_detail) : 규칙별 처리 상태 건수 - 위반 이력 전체가 들어가는지 확인
# ---------------------------------------------------------------------------
ED = {
    ("DQ-COM-IN-001", "UNRESOLVED"): 1,
    ("DQ-COM-IN-002", "NOT_REQUIRED"): 1,     # ALLOW 규칙의 위반도 기록만 된다
    ("DQ-COM-IN-003", "NOT_REQUIRED"): 1,
    ("DQ-VAL-IN-001", "AUTO_CLEANSED"): 6,
    ("DQ-VAL-IN-001", "ALLOWED"): 1,          # WARN 규칙의 정제 실패 -> 허용
    ("DQ-VAL-IN-002", "AUTO_CLEANSED"): 1,     # T07 (공백 제거만)
    ("DQ-VAL-IN-002", "UNRESOLVED"): 4,        # T05, T06, T08, T16
    ("DQ-VAL-IN-003", "ALLOWED"): 2,           # WARN 규칙의 정제 불가 위반은 허용(통과), 이력만 기록
    ("DQ-CON-IN-001", "UNRESOLVED"): 1,
    ("DQ-UNI-IN-001", "UNRESOLVED"): 2,
}
AD = {(r["rule_id"], r["cleansing_status"]): r["c"]
      for r in det.groupBy("rule_id", "cleansing_status").agg(F.count("*").alias("c")).collect()}
detail_rows = []
for k in sorted(set(ED) | set(AD)):
    e, a = ED.get(k, 0), AD.get(k, 0)
    detail_rows.append({"규칙": k[0], "상태": k[1], "기대 건수": e, "실제 건수": a, "결과": "✅" if e == a else "❌"})
t_detail = pd.DataFrame(detail_rows)

# ---------------------------------------------------------------------------
# 6. 요약
# ---------------------------------------------------------------------------
S = [
    ("silver_candidate 행 수",      8,     summary["candidate_row_count"]),
    ("격리(quarantine) 행 수",       8,     summary["quarantine_row_count"]),
    ("전체 = 후보 + 격리 (한 레코드는 한 곳에만)", 16, summary["candidate_row_count"] + summary["quarantine_row_count"]),
    ("silver_ready (격리 없음 여부)", False, summary["silver_ready"]),
]
t_sum = pd.DataFrame([{"항목": n, "기대": e, "실제": a, "결과": "✅" if e == a else "❌"} for n, e, a in S])

det_view = (det.select("rule_id", "source_record_key", "target_column", "before_value", "proposed_value", "final_value",
                       "cleansing_rule_id", "cleansing_status", "re_dq_result", "review_required_yn")
            .orderBy("rule_id", "source_record_key").toPandas())     # 아래 버전 검증이 실행 이력을 더 쌓기 전에 확보

# ---------------------------------------------------------------------------
# 6-2. 규칙 버전 관리 검증 (위 결과를 모두 뽑아 둔 뒤에 실행 - 이 아래는 규칙 테이블과 dq_result에 이력을 더 쌓는다)
# ---------------------------------------------------------------------------
def _inputs():
    return dq_engine.code_rules_as_inputs(_rules)

_versions_run1 = {r["dq_rule_version"] for r in results}
_same = rule_repo.sync(_inputs())                                              # 같은 내용으로 다시 적재

_changed_inputs = _inputs()
for _it in _changed_inputs:
    if _it["rule_id"] == "DQ-COM-IN-002":
        _it["threshold_rate"] = 0.6                                            # 규칙 1개의 임계치만 변경
_p_changed = rule_repo.sync(_changed_inputs, change_reason="검증: 임계치 변경")
_results2, _ = runner.run_table_dq(BRONZE, ING)                                # 변경된 규칙으로 다시 실행
_ver_run2 = {r["rule_id"]: r["dq_rule_version"] for r in _results2}

_name_inputs = _inputs()
for _it in _name_inputs:
    if _it["rule_id"] == "DQ-COM-IN-002":
        _it["threshold_rate"] = 0.6
        _it["rule_name"] = "이름만 바꿈"                                          # 이름만 변경
_p_name = rule_repo.sync(_name_inputs)

_removed = [i for i in _name_inputs if i["rule_id"] != "DQ-VAL-IN-004"]         # 규칙 1개를 목록에서 뺌
_p_removed = rule_repo.sync(_removed)
_active_after_removal = len(rule_repo.load_current(BRONZE))
_p_readd = rule_repo.sync(_name_inputs)                                         # 다시 넣음
_readd_version = max(r["rule_version"] for r in rule_repo.history("DQ-VAL-IN-004").collect())

V = [
    ("최초 적재: 신규 규칙 수 = 인바운드 규칙 수",            len(_rules),                   len(_init_plan["new"])),
    ("첫 실행의 dq_rule_version은 전부 1",                    {1},                           _versions_run1),
    ("같은 내용으로 다시 적재하면 바뀐 행 없음",              0,                             len(_same["rows"])),
    ("임계치 1개 변경 -> 그 규칙만 새 버전(v1->v2)",          [("DQ-COM-IN-002", 1, 2)],     [(c["rule_id"], c["from_version"], c["to_version"]) for c in _p_changed["changed"]]),
    ("다시 실행하면 변경된 규칙만 dq_rule_version=2",         {"DQ-COM-IN-002": 2},          {k: v for k, v in _ver_run2.items() if v != 1}),
    ("규칙 이름만 바꾸면 새 버전을 만들지 않음",              0,                             len(_p_name["rows"])),
    ("목록에서 뺀 규칙은 비활성 처리되어 실행 대상에서 제외",  (["DQ-VAL-IN-004"], 8),        (_p_removed["deactivated"], _active_after_removal)),
    ("다시 넣으면 재활성 (버전은 v3)",                        (["DQ-VAL-IN-004"], 3),        (_p_readd["reactivated"], _readd_version)),
]
t_ver = pd.DataFrame([{"항목": n, "기대": e, "실제": a, "결과": "✅" if e == a else "❌"} for n, e, a in V])

# ---------------------------------------------------------------------------
# 7. 출력
# ---------------------------------------------------------------------------
def _show(df, title):
    print(f"\n{'=' * 8} {title} {'=' * 8}")
    try:
        display(df)                                   # Databricks 노트북
    except NameError:
        print(df.to_string(index=False))

_show(t_cases,  "① 레코드별 결과 (어디로 갔고, 어떤 상태이고, 남은 위반이 뭔가)")
_show(t_rules,  "② 규칙별 결과 (dq_result)")
_show(t_detail, "③ 위반 이력 (dq_cleansing_detail) 상태별 건수")
_show(t_sum,    "④ 요약")
_show(t_ver,    "⑤ 규칙 버전 관리 (규칙 테이블)")

all_tables = pd.concat([t_cases["결과"], t_rules["결과"], t_detail["결과"], t_sum["결과"], t_ver["결과"]])
ok, total = int((all_tables == "✅").sum()), len(all_tables)
print(f"\n검증 결과: {ok}/{total} 통과" + ("  ← 모두 기대와 일치" if ok == total else "  ← ❌ 항목을 확인하세요"))

# 눈으로 확인용: 마스킹된 before/proposed/final 값과 처리 상태 (위반 이력)
_show(det_view, "참고: dq_cleansing_detail 원본 (개인정보는 마스킹되어 저장, 첫 실행분)")
