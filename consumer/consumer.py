# -*- coding: utf-8 -*-
"""Kafka consumer.

semiconductor-events 토픽을 구독해서:
  1) 스텝별 PCA + Hotelling's T2 모델로 실시간 이상 점수를 계산하고
  2) 원본 이벤트와 함께 PostgreSQL 에 적재한다.

대시보드(dashboard/app.py)가 이 테이블을 읽어 실시간으로 시각화한다.
"""

import os
import sys
import json

import psycopg2
from psycopg2.extras import Json
from kafka import KafkaConsumer

# 프로젝트 루트를 import 경로에 추가
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from anomaly.pca_t2 import PCAT2Model  # noqa: E402
from graph.neo4j_writer import GraphWriter  # noqa: E402

MODEL_DIR = os.path.join(ROOT, "anomaly", "models")
STEPS = ["DEPOSITION", "ETCH", "LITHOGRAPHY", "INSPECTION"]


# --------------------------------------------------------------------- #
# 모델 로드
# --------------------------------------------------------------------- #

def load_models():
    models = {}
    for step in STEPS:
        path = os.path.join(MODEL_DIR, f"{step}.pkl")
        if os.path.exists(path):
            models[step] = PCAT2Model.load(path)
        else:
            print(f"[WARN] 모델 없음: {path} (먼저 train_model.py 실행)")
    return models


# --------------------------------------------------------------------- #
# DB 준비 (테이블/컬럼 자동 생성)
# --------------------------------------------------------------------- #

def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS semiconductor_events (
                id                 SERIAL PRIMARY KEY,
                timestamp          BIGINT,
                wafer_id           TEXT,
                lot_id             TEXT,
                step               TEXT,
                equipment_id       TEXT,
                equipment_health   DOUBLE PRECISION,
                temperature        DOUBLE PRECISION,
                pressure           DOUBLE PRECISION,
                gas_flow           DOUBLE PRECISION,
                rf_power           DOUBLE PRECISION,
                vibration          DOUBLE PRECISION,
                particle_count     INTEGER,
                defect_probability DOUBLE PRECISION,
                label              TEXT
            );
            """
        )
        # 이상탐지 결과 컬럼 (기존 테이블에도 안전하게 추가)
        for ddl in [
            "ADD COLUMN IF NOT EXISTS pca_t2 DOUBLE PRECISION",
            "ADD COLUMN IF NOT EXISTS t2_ucl DOUBLE PRECISION",
            "ADD COLUMN IF NOT EXISTS is_anomaly BOOLEAN",
            "ADD COLUMN IF NOT EXISTS top_contributor TEXT",
            "ADD COLUMN IF NOT EXISTS contributions JSONB",
            "ADD COLUMN IF NOT EXISTS ingest_time TIMESTAMPTZ DEFAULT now()",
        ]:
            cur.execute(f"ALTER TABLE semiconductor_events {ddl};")
    conn.commit()


INSERT_SQL = """
    INSERT INTO semiconductor_events (
        timestamp, wafer_id, lot_id, step, equipment_id, equipment_health,
        temperature, pressure, gas_flow, rf_power, vibration, particle_count,
        defect_probability, label,
        pca_t2, t2_ucl, is_anomaly, top_contributor, contributions
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s)
"""


# --------------------------------------------------------------------- #
# 메인 루프
# --------------------------------------------------------------------- #

def main():
    models = load_models()

    consumer = KafkaConsumer(
        "semiconductor-events",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    conn = psycopg2.connect(
        host="localhost",
        database="semiconductor",
        user="admin",
        password="admin",
    )
    ensure_schema(conn)
    cursor = conn.cursor()

    # Neo4j dual-write (선택적, 장애 격리). 연결 실패해도 파이프라인은 계속.
    graph = GraphWriter()

    print("Consumer 시작: 실시간 PCA + T2 이상탐지 중...")

    for msg in consumer:
        event = msg.value
        step = event.get("step")

        # 해당 스텝 모델로 이상 점수 계산
        model = models.get(step)
        if model is not None:
            result = model.score_one(event)
        else:
            result = {
                "t2": None, "ucl": None, "is_anomaly": None,
                "top_contributor": None, "contributions": {},
            }

        cursor.execute(
            INSERT_SQL,
            (
                event.get("timestamp"),
                event.get("wafer_id"),
                event.get("lot_id"),
                event.get("step"),
                event.get("equipment_id"),
                event.get("equipment_health"),
                event.get("temperature"),
                event.get("pressure"),
                event.get("gas_flow"),
                event.get("rf_power"),
                event.get("vibration"),
                event.get("particle_count"),
                event.get("defect_probability"),
                event.get("label"),
                result["t2"],
                result["ucl"],
                result["is_anomaly"],
                result["top_contributor"],
                Json(result["contributions"]),
            ),
        )
        conn.commit()

        # 같은 이벤트를 그래프에도 반영 (실패해도 무시)
        graph.write_event(event, result)

        flag = "  <== ANOMALY" if result["is_anomaly"] else ""
        print(
            f"{event['wafer_id']} [{step}] "
            f"T2={result['t2']} (UCL={result['ucl']}){flag}"
        )


if __name__ == "__main__":
    main()
