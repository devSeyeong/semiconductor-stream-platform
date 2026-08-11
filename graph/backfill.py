# -*- coding: utf-8 -*-
"""PostgreSQL → Neo4j backfill.

이미 semiconductor_events 테이블에 쌓여있는 과거 이벤트를 그래프로 옮긴다.
GraphWriter 의 MERGE 는 멱등이므로 여러 번 돌려도 안전하다.

실행:
    .venv/bin/python graph/backfill.py
"""

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import psycopg2  # noqa: E402
from psycopg2.extras import RealDictCursor  # noqa: E402

import config  # noqa: E402
from graph.neo4j_writer import GraphWriter  # noqa: E402


def main():
    conn = psycopg2.connect(**config.pg_dsn())

    writer = GraphWriter()
    if not writer.available:
        print("[backfill] Neo4j 사용 불가 → 중단")
        return

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT timestamp, wafer_id, lot_id, step, equipment_id,
                   equipment_health, defect_probability, label,
                   pca_t2, t2_ucl, is_anomaly, top_contributor
            FROM semiconductor_events
            ORDER BY id
            """
        )
        rows = cur.fetchall()

    print(f"[backfill] {len(rows)} 건 그래프로 이관 시작...")
    for i, row in enumerate(rows, 1):
        event = {
            "timestamp": row["timestamp"],
            "wafer_id": row["wafer_id"],
            "lot_id": row["lot_id"],
            "step": row["step"],
            "equipment_id": row["equipment_id"],
            "equipment_health": row["equipment_health"],
            "defect_probability": row["defect_probability"],
            "label": row["label"],
        }
        result = {
            "t2": row["pca_t2"],
            "ucl": row["t2_ucl"],
            "is_anomaly": row["is_anomaly"],
            "top_contributor": row["top_contributor"],
        }
        writer.write_event(event, result)
        if i % 500 == 0:
            print(f"[backfill] {i}/{len(rows)}")

    writer.close()
    conn.close()
    print(f"[backfill] 완료: {len(rows)} 건")


if __name__ == "__main__":
    main()