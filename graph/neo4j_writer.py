# -*- coding: utf-8 -*-
"""Neo4j dual-write writer.

consumer 가 PostgreSQL 에 이벤트를 적재할 때, 같은 이벤트를 Neo4j 그래프에도
MERGE 로 반영한다. 관계형 테이블은 "행"을 쌓는 데 강하지만, 반도체 FDC 의
핵심 질문(어느 장비를 거친 웨이퍼가 fail 났나, 같은 이상은 어디서 또 났나)은
"관계 추적"이라 그래프가 훨씬 잘 맞는다.

그래프 모델
-----------
(:Lot {id})
(:Wafer {id})            -[:BELONGS_TO]->      (:Lot)
(:Equipment {id, step})  -[:PERFORMS]->        (:Step)
(:Step {name})           -[:NEXT]->            (:Step)     # 공정 흐름
(:Sensor {name})
(:Run {run_key, ...})    -[:AT_STEP]->         (:Step)
                         -[:ON_EQUIPMENT]->    (:Equipment)
                         -[:CAUSED_BY]->       (:Sensor)   # 이상일 때만
(:Wafer)                 -[:HAS_RUN]->         (:Run)

설계 원칙
---------
* **장애 격리**: Neo4j 가 죽어도 메인 파이프라인(PostgreSQL 적재)은 절대
  멈추면 안 된다. 모든 쓰기는 try/except 로 감싸고 경고만 남긴다.
* **선택적**: 환경변수 GRAPH_ENABLED=0 이면 아무 것도 하지 않는다.
* **멱등성**: run_key(wafer+timestamp+step+equipment)로 MERGE 하므로
  같은 이벤트를 두 번 넣어도 노드/관계가 중복되지 않는다 → backfill 안전.
"""

import os

# 공정 흐름 순서 (simulator 의 STEPS 와 동일)
STEPS = ["DEPOSITION", "ETCH", "LITHOGRAPHY", "INSPECTION"]


def graph_enabled() -> bool:
    """GRAPH_ENABLED 가 명시적으로 꺼져있지 않으면 활성."""
    return os.getenv("GRAPH_ENABLED", "1") not in ("0", "false", "False", "")


class GraphWriter:
    """이벤트 하나를 Neo4j 그래프로 dual-write 한다.

    사용:
        writer = GraphWriter()          # 연결 실패해도 예외 안 던짐
        writer.write_event(event, result)
        writer.close()
    """

    def __init__(self, uri=None, user=None, password=None):
        self.uri = uri or os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.user = user or os.getenv("NEO4J_USER", "neo4j")
        self.password = password or os.getenv("NEO4J_PASSWORD", "semiconductor")
        self._driver = None
        self.available = False

        if not graph_enabled():
            print("[graph] GRAPH_ENABLED=0 → Neo4j dual-write 비활성")
            return

        try:
            from neo4j import GraphDatabase

            self._driver = GraphDatabase.driver(
                self.uri, auth=(self.user, self.password)
            )
            self._driver.verify_connectivity()
            self._ensure_constraints()
            self._ensure_process_flow()
            self.available = True
            print(f"[graph] Neo4j 연결 성공: {self.uri}")
        except Exception as exc:  # 연결 실패는 치명적이지 않다
            print(f"[graph][WARN] Neo4j 연결 실패 → 그래프 쓰기 생략: {exc}")
            self._driver = None
            self.available = False

    # ------------------------------------------------------------------ #
    # 스키마 준비
    # ------------------------------------------------------------------ #

    def _ensure_constraints(self):
        """노드 고유성 제약 (MERGE 성능 + 중복 방지)."""
        stmts = [
            "CREATE CONSTRAINT lot_id IF NOT EXISTS "
            "FOR (l:Lot) REQUIRE l.id IS UNIQUE",
            "CREATE CONSTRAINT wafer_id IF NOT EXISTS "
            "FOR (w:Wafer) REQUIRE w.id IS UNIQUE",
            "CREATE CONSTRAINT equipment_id IF NOT EXISTS "
            "FOR (e:Equipment) REQUIRE e.id IS UNIQUE",
            "CREATE CONSTRAINT step_name IF NOT EXISTS "
            "FOR (s:Step) REQUIRE s.name IS UNIQUE",
            "CREATE CONSTRAINT sensor_name IF NOT EXISTS "
            "FOR (s:Sensor) REQUIRE s.name IS UNIQUE",
            "CREATE CONSTRAINT run_key IF NOT EXISTS "
            "FOR (r:Run) REQUIRE r.run_key IS UNIQUE",
        ]
        with self._driver.session() as session:
            for stmt in stmts:
                session.run(stmt)

    def _ensure_process_flow(self):
        """공정 흐름 DEPOSITION→ETCH→LITHOGRAPHY→INSPECTION 을 (Step)-[:NEXT]->(Step) 로."""
        with self._driver.session() as session:
            for i in range(len(STEPS) - 1):
                session.run(
                    """
                    MERGE (a:Step {name: $a})
                    MERGE (b:Step {name: $b})
                    MERGE (a)-[:NEXT]->(b)
                    """,
                    a=STEPS[i],
                    b=STEPS[i + 1],
                )

    # ------------------------------------------------------------------ #
    # 이벤트 쓰기
    # ------------------------------------------------------------------ #

    @staticmethod
    def _run_key(event) -> str:
        return (
            f"{event.get('wafer_id')}:{event.get('timestamp')}:"
            f"{event.get('step')}:{event.get('equipment_id')}"
        )

    def write_event(self, event: dict, result: dict = None):
        """이벤트 1건을 그래프로 반영. 실패해도 조용히 넘어간다.

        event  : simulator 가 만든 원본 이벤트 dict
        result : anomaly.pca_t2.score_one() 결과 dict (없으면 이상탐지 정보 생략)
        """
        if not self.available:
            return

        result = result or {}
        params = {
            "run_key": self._run_key(event),
            "wafer_id": event.get("wafer_id"),
            "lot_id": event.get("lot_id"),
            "step": event.get("step"),
            "equipment_id": event.get("equipment_id"),
            "equipment_health": event.get("equipment_health"),
            "timestamp": event.get("timestamp"),
            "label": event.get("label"),
            "defect_probability": event.get("defect_probability"),
            "t2": result.get("t2"),
            "ucl": result.get("ucl"),
            "is_anomaly": bool(result.get("is_anomaly")),
            "top_contributor": result.get("top_contributor"),
        }

        try:
            with self._driver.session() as session:
                session.execute_write(self._merge_event, params)
        except Exception as exc:
            print(f"[graph][WARN] 그래프 쓰기 실패(무시): {exc}")

    @staticmethod
    def _merge_event(tx, p):
        # Lot / Wafer / Equipment / Step / Run 과 관계 MERGE
        tx.run(
            """
            MERGE (lot:Lot {id: $lot_id})
            MERGE (w:Wafer {id: $wafer_id})
            MERGE (w)-[:BELONGS_TO]->(lot)

            MERGE (step:Step {name: $step})
            MERGE (eq:Equipment {id: $equipment_id})
              ON CREATE SET eq.step = $step
            SET eq.health = $equipment_health
            MERGE (eq)-[:PERFORMS]->(step)

            MERGE (r:Run {run_key: $run_key})
              SET r.timestamp          = $timestamp,
                  r.label              = $label,
                  r.defect_probability = $defect_probability,
                  r.t2                 = $t2,
                  r.ucl                = $ucl,
                  r.is_anomaly         = $is_anomaly,
                  r.top_contributor    = $top_contributor
            MERGE (w)-[:HAS_RUN]->(r)
            MERGE (r)-[:AT_STEP]->(step)
            MERGE (r)-[:ON_EQUIPMENT]->(eq)
            """,
            **p,
        )

        # 이상이고 기여 센서가 있으면 (Run)-[:CAUSED_BY]->(Sensor)
        if p["is_anomaly"] and p["top_contributor"]:
            tx.run(
                """
                MATCH (r:Run {run_key: $run_key})
                MERGE (s:Sensor {name: $top_contributor})
                MERGE (r)-[:CAUSED_BY]->(s)
                """,
                run_key=p["run_key"],
                top_contributor=p["top_contributor"],
            )

    # ------------------------------------------------------------------ #

    def close(self):
        if self._driver is not None:
            self._driver.close()