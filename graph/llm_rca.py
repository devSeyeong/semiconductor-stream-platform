# -*- coding: utf-8 -*-
"""Stage 3 — LLM 레이어 (GraphRAG).

그래프(사실) + 온톨로지(의미) 위에 Claude 를 얹어:
  1) ask()            : 자연어 질문 → Cypher 생성/실행 → 자연어 답변
  2) explain_anomaly(): 이상 run 의 계보+온톨로지를 근거로 근본원인(RCA) 내러티브

핵심은 GraphRAG — Claude 가 지어내는 게 아니라, 읽기전용 run_cypher 도구로
Neo4j 에서 "검증된 사실"만 가져와 답한다. 온톨로지 어휘를 시스템 프롬프트에
넣어 존재하지 않는 관계/센서를 못 지어내게 막는다(환각 가드레일).

모델: Claude Opus 4.8 (claude-opus-4-8), adaptive thinking.
인증: ANTHROPIC_API_KEY 환경변수 또는 `ant auth login` 프로파일을 SDK 가 자동 사용.

실행:
    .venv/bin/python graph/llm_rca.py ask "L003 랏에서 fail이 제일 많은 장비는?"
    .venv/bin/python graph/llm_rca.py explain W00007
"""

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import anthropic  # noqa: E402
from anthropic import beta_tool  # noqa: E402

import config  # noqa: E402
from graph.ontology import FDCOntology  # noqa: E402

MODEL = "claude-opus-4-8"

# 쓰기 계열 Cypher 절 — 읽기전용 강제를 위해 차단
_WRITE_CLAUSES = (
    "CREATE", "MERGE", "DELETE", "SET", "REMOVE", "DROP",
    "DETACH", "CALL {", "LOAD CSV", "FOREACH",
)

# --------------------------------------------------------------------- #
# Neo4j 읽기전용 실행기 (모듈 전역 — tool 함수가 참조)
# --------------------------------------------------------------------- #

_driver = None


def _get_driver():
    global _driver
    if _driver is None:
        from neo4j import GraphDatabase

        _driver = GraphDatabase.driver(
            config.NEO4J_URI,
            auth=(config.NEO4J_USER, config.NEO4J_PASSWORD),
        )
    return _driver


def _run_readonly(cypher: str, limit: int = 100):
    """읽기전용 트랜잭션으로 Cypher 실행. 쓰기 절이 있으면 거부."""
    upper = cypher.upper()
    for clause in _WRITE_CLAUSES:
        if clause in upper:
            raise ValueError(f"읽기전용만 허용됩니다. 금지된 절: {clause.strip()}")

    driver = _get_driver()
    with driver.session() as session:
        # execute_read → 읽기 트랜잭션. 혹시 쓰기가 섞이면 드라이버가 막는다.
        records = session.execute_read(
            lambda tx: [dict(r) for r in tx.run(cypher).fetch(limit)]
        )
    return records


# --------------------------------------------------------------------- #
# Claude 도구 정의
# --------------------------------------------------------------------- #

# 온톨로지는 한 번만 로드 (도구/프롬프트가 공유)
_ONTO = None


def _onto():
    global _ONTO
    if _ONTO is None:
        _ONTO = FDCOntology()
    return _ONTO


@beta_tool
def run_cypher(query: str) -> str:
    """Run a READ-ONLY Cypher query against the Neo4j semiconductor graph and return rows as text.

    The graph schema:
      (:Lot {id})
      (:Wafer {id})-[:BELONGS_TO]->(:Lot)
      (:Equipment {id, step, health})-[:PERFORMS]->(:Step {name})
      (:Step)-[:NEXT]->(:Step)   process flow DEPOSITION->ETCH->LITHOGRAPHY->INSPECTION
      (:Wafer)-[:HAS_RUN]->(:Run {run_key, timestamp, label, t2, ucl, is_anomaly, defect_probability, top_contributor})
      (:Run)-[:AT_STEP]->(:Step)
      (:Run)-[:ON_EQUIPMENT]->(:Equipment)
      (:Run)-[:CAUSED_BY]->(:Sensor {name})   only present when the run is an anomaly

    Only MATCH/WITH/RETURN/UNWIND/ORDER BY/WHERE queries are allowed. Never write.

    Args:
        query: A read-only Cypher query string.
    """
    try:
        rows = _run_readonly(query)
    except Exception as exc:
        return f"ERROR: {exc}"
    if not rows:
        return "(0 rows)"
    lines = [str(r) for r in rows[:50]]
    more = "" if len(rows) <= 50 else f"\n... (+{len(rows) - 50} more rows)"
    return "\n".join(lines) + more


@beta_tool
def classify_sensor(sensor_name: str) -> str:
    """Translate a sensor name into its FDC failure mode, root cause, and recommended action using the ontology.

    Use this when a run's top_contributor sensor needs domain interpretation.

    Args:
        sensor_name: e.g. "temperature", "pressure", "vibration", "particle_count".
    """
    r = _onto().classify_sensor(sensor_name)
    if not r:
        return f"'{sensor_name}' 에 대한 온톨로지 매핑 없음"
    return (
        f"sensor={r['sensor']} → failure_mode={r['failure_mode']} "
        f"({r['failure_mode_label']}), category={r['failure_category']}, "
        f"root_cause={r['root_cause']}, action={r['recommended_action']}"
    )


# --------------------------------------------------------------------- #
# 시스템 프롬프트 (온톨로지 어휘 주입 = 환각 가드레일)
# --------------------------------------------------------------------- #

def _system_prompt() -> str:
    vocab = _onto().vocabulary()
    return f"""너는 반도체 FDC(Fault Detection & Classification) 분석 어시스턴트다.

너는 Neo4j 그래프(웨이퍼 계보·장비·이상 이벤트)와 FDC 온톨로지에 접근할 수 있다.
질문에 답할 때는 반드시 run_cypher 도구로 그래프에서 **실제 데이터**를 조회해 근거로 삼아라.
추측하지 말고, 그래프에 없는 관계·센서·컬럼을 지어내지 마라.

사용 가능한 어휘:
- 센서: {', '.join(vocab['sensors'])}
- 공정 스텝: {', '.join(vocab['steps'])}
- 고장모드: {', '.join(vocab['failure_modes'])}

이상(anomaly)의 원인 센서를 도메인 의미(고장모드/근본원인/권장조치)로 해석할 때는
classify_sensor 도구를 사용해라.

답변은 한국어로, 근거가 된 수치와 함께 간결하게. 마지막에 실행 가능한 권장조치가 있으면 제시."""


# --------------------------------------------------------------------- #
# 1) 자연어 질의 에이전트
# --------------------------------------------------------------------- #

def ask(question: str) -> str:
    """자연어 질문을 받아 그래프를 조회하고 답변한다 (tool_runner 자동 루프)."""
    client = anthropic.Anthropic()
    runner = client.beta.messages.tool_runner(
        model=MODEL,
        max_tokens=4096,
        thinking={"type": "adaptive"},
        system=_system_prompt(),
        tools=[run_cypher, classify_sensor],
        messages=[{"role": "user", "content": question}],
    )

    final = None
    for message in runner:
        final = message

    if final is None:
        return "(응답 없음)"
    return "\n".join(b.text for b in final.content if b.type == "text")


# --------------------------------------------------------------------- #
# 2) 이상 RCA 내러티브
# --------------------------------------------------------------------- #

def _gather_wafer_context(wafer_id: str) -> dict:
    """웨이퍼의 계보 + 이상 run + 온톨로지 분류를 모아 근거 팩트로 만든다."""
    runs = _run_readonly(
        f"""
        MATCH (w:Wafer {{id: '{wafer_id}'}})-[:HAS_RUN]->(r:Run)-[:AT_STEP]->(s:Step)
        MATCH (r)-[:ON_EQUIPMENT]->(e:Equipment)
        RETURN r.timestamp AS ts, s.name AS step, e.id AS equipment,
               r.label AS label, r.t2 AS t2, r.ucl AS ucl,
               r.is_anomaly AS is_anomaly, r.top_contributor AS top_contributor
        ORDER BY ts
        """
    )
    # 이상 run 의 원인 센서를 온톨로지로 분류
    classified = []
    for r in runs:
        if r.get("is_anomaly") and r.get("top_contributor"):
            c = _onto().classify_sensor(r["top_contributor"])
            if c:
                classified.append({**c, "step": r["step"], "equipment": r["equipment"]})
    return {"runs": runs, "ontology": classified}


def explain_anomaly(wafer_id: str) -> str:
    """웨이퍼 하나의 이상을 근본원인 내러티브로 설명한다 (grounded)."""
    ctx = _gather_wafer_context(wafer_id)
    if not ctx["runs"]:
        return f"그래프에 {wafer_id} 데이터가 없습니다. (backfill 또는 consumer 실행 필요)"

    client = anthropic.Anthropic()
    prompt = (
        f"다음은 웨이퍼 {wafer_id} 의 그래프 계보와 온톨로지 분류 결과다. "
        f"이 사실만 근거로, 이상이 있었는지, 어느 스텝/장비에서 무슨 고장모드로 "
        f"보이는지, 근본원인과 권장조치를 엔지니어에게 보고하듯 요약해라.\n\n"
        f"[계보 runs]\n{ctx['runs']}\n\n"
        f"[온톨로지 분류]\n{ctx['ontology']}"
    )
    resp = client.messages.create(
        model=MODEL,
        max_tokens=2048,
        thinking={"type": "adaptive"},
        system=_system_prompt(),
        messages=[{"role": "user", "content": prompt}],
    )
    return "\n".join(b.text for b in resp.content if b.type == "text")


# --------------------------------------------------------------------- #

def main():
    if len(sys.argv) < 3:
        print(__doc__)
        return
    cmd, arg = sys.argv[1], " ".join(sys.argv[2:])
    if cmd == "ask":
        print(ask(arg))
    elif cmd == "explain":
        print(explain_anomaly(arg))
    else:
        print(f"알 수 없는 명령: {cmd} (ask | explain)")


if __name__ == "__main__":
    main()
