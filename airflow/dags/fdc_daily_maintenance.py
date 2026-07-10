# -*- coding: utf-8 -*-
"""FDC 일일 유지보수 DAG.

이 플랫폼은 실시간 스트리밍(Kafka → consumer → PostgreSQL)이 본체다.
Airflow 는 그 옆에서 "주기적 배치 작업"을 스케줄링한다:

    data_quality_check          # 적재 데이터 신선도·센서 범위 검증 (품질 게이트)
          │
      ┌───┴───────────────┐
   retrain_models      sync_graph     # PCA+T² 재학습 / PostgreSQL→Neo4j 이관 (병렬)
      └───┬───────────────┘
          │
    anomaly_report              # 장비·스텝별 일일 이상률 요약 → anomaly_daily_report 테이블

설계 원칙
--------
* 무거운 import 는 태스크 함수 안에서만 (DAG 파싱 부하 방지).
* retrain/sync 는 기존 스크립트를 그대로 실행 → 단건/스트리밍과 로직 100% 동일.
* DB/Neo4j 접속 정보는 전부 환경변수(PG_HOST, NEO4J_URI ...)로 주입 (docker-compose).
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# 프로젝트 코드는 컨테이너에 /opt/project 로 마운트되고 PYTHONPATH 에 등록된다.
PROJECT_DIR = os.getenv("PROJECT_DIR", "/opt/project")

# 리포트/품질검사 lookback 창(일). 시뮬레이션 데이터가 오래됐을 수 있어 넉넉히.
LOOKBACK_DAYS = int(os.getenv("FDC_LOOKBACK_DAYS", "30"))


# --------------------------------------------------------------------- #
# 공통: PostgreSQL 연결 (앱 DB — Airflow 메타DB 아님)
# --------------------------------------------------------------------- #

def _connect_app_db():
    import psycopg2

    return psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        database=os.getenv("PG_DB", "semiconductor"),
        user=os.getenv("PG_USER", "admin"),
        password=os.getenv("PG_PASSWORD", "admin"),
    )


def _table_exists(cur, name: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (name,))
    return cur.fetchone()[0] is not None


# --------------------------------------------------------------------- #
# Task 1: 데이터 품질 검사 (품질 게이트)
# --------------------------------------------------------------------- #

SENSORS = ["temperature", "pressure", "gas_flow", "rf_power", "vibration", "particle_count"]

# 물리적으로 말이 되는 대략적 범위 (벗어나면 경고). 상·하한 None = 검사 안 함.
SENSOR_RANGES = {
    "temperature": (0, 500),
    "pressure": (0, 50),
    "gas_flow": (0, 1000),
    "rf_power": (0, 5000),
    "vibration": (0, 100),
    "particle_count": (0, 100000),
}


def data_quality_check(**_):
    """적재 데이터가 존재하고 센서 값이 정상 범위인지 검증.

    - 테이블이 없거나 전체 0건이면 실패(파이프라인이 안 돌고 있다는 신호).
    - 센서 값이 범위를 벗어나면 경고만 (하드 실패 X).
    - 신선도(최근 적재 시각)는 로그로 리포트.
    """
    conn = _connect_app_db()
    try:
        with conn.cursor() as cur:
            if not _table_exists(cur, "semiconductor_events"):
                raise ValueError(
                    "semiconductor_events 테이블 없음 — consumer 를 먼저 실행해 "
                    "데이터를 적재하세요."
                )

            cur.execute("SELECT COUNT(*) FROM semiconductor_events")
            total = cur.fetchone()[0]
            if total == 0:
                raise ValueError("semiconductor_events 가 비어있음 — 적재된 이벤트가 없습니다.")

            cur.execute(
                "SELECT MAX(ingest_time), COUNT(*) FILTER "
                "(WHERE ingest_time >= now() - make_interval(days => %s)) "
                "FROM semiconductor_events",
                (LOOKBACK_DAYS,),
            )
            last_ingest, recent = cur.fetchone()
            print(f"[quality] 전체 {total}건, 최근 {LOOKBACK_DAYS}일 {recent}건, "
                  f"마지막 적재: {last_ingest}")

            # 센서 범위 검사 (경고만)
            warnings = []
            for s in SENSORS:
                lo, hi = SENSOR_RANGES.get(s, (None, None))
                cur.execute(
                    f"SELECT COUNT(*) FROM semiconductor_events "
                    f"WHERE {s} IS NOT NULL AND ({s} < %s OR {s} > %s)",
                    (lo, hi),
                )
                out_of_range = cur.fetchone()[0]
                if out_of_range:
                    warnings.append(f"{s}: {out_of_range}건 범위밖([{lo},{hi}])")

            if warnings:
                print("[quality][WARN] " + " | ".join(warnings))
            else:
                print("[quality] 모든 센서 값 정상 범위")
    finally:
        conn.close()


# --------------------------------------------------------------------- #
# Task 4: 일일 이상률 리포트
# --------------------------------------------------------------------- #

def anomaly_report(**_):
    """장비·스텝별 이상률을 집계해 anomaly_daily_report 테이블에 적재."""
    conn = _connect_app_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS anomaly_daily_report (
                    report_date  DATE,
                    step         TEXT,
                    equipment_id TEXT,
                    runs         INTEGER,
                    anomalies    INTEGER,
                    anomaly_rate NUMERIC(5,2),
                    created_at   TIMESTAMPTZ DEFAULT now(),
                    PRIMARY KEY (report_date, step, equipment_id)
                )
                """
            )

            cur.execute(
                """
                SELECT step, equipment_id,
                       COUNT(*) AS runs,
                       COUNT(*) FILTER (WHERE is_anomaly) AS anomalies,
                       ROUND(100.0 * COUNT(*) FILTER (WHERE is_anomaly)
                             / NULLIF(COUNT(*), 0), 2) AS anomaly_rate
                FROM semiconductor_events
                WHERE ingest_time >= now() - make_interval(days => %s)
                GROUP BY step, equipment_id
                ORDER BY anomaly_rate DESC NULLS LAST, runs DESC
                """,
                (LOOKBACK_DAYS,),
            )
            rows = cur.fetchall()

            if not rows:
                print(f"[report] 최근 {LOOKBACK_DAYS}일 집계 대상 없음")
                return

            print(f"[report] === 장비·스텝별 이상률 (최근 {LOOKBACK_DAYS}일) ===")
            print(f"{'STEP':12} {'EQUIPMENT':14} {'RUNS':>6} {'ANOM':>6} {'RATE%':>7}")
            for step, eq, runs, anom, rate in rows:
                print(f"{step or '-':12} {eq or '-':14} {runs:>6} "
                      f"{anom or 0:>6} {rate if rate is not None else 0:>7}")

            # report_date = 실행일(논리적 데이터 날짜). 재실행 안전하도록 UPSERT.
            cur.execute("SELECT CURRENT_DATE")
            report_date = cur.fetchone()[0]
            cur.executemany(
                """
                INSERT INTO anomaly_daily_report
                    (report_date, step, equipment_id, runs, anomalies, anomaly_rate)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (report_date, step, equipment_id) DO UPDATE SET
                    runs = EXCLUDED.runs,
                    anomalies = EXCLUDED.anomalies,
                    anomaly_rate = EXCLUDED.anomaly_rate,
                    created_at = now()
                """,
                [(report_date, step, eq, runs, anom or 0, rate)
                 for step, eq, runs, anom, rate in rows],
            )
        conn.commit()
        print(f"[report] anomaly_daily_report 에 {len(rows)}행 적재 완료")
    finally:
        conn.close()


# --------------------------------------------------------------------- #
# DAG 정의
# --------------------------------------------------------------------- #

default_args = {
    "owner": "fdc",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="fdc_daily_maintenance",
    description="반도체 FDC 일일 유지보수: 품질검사 → 모델재학습·그래프동기화 → 이상률 리포트",
    default_args=default_args,
    schedule="0 2 * * *",          # 매일 새벽 2시
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["fdc", "semiconductor", "mlops"],
) as dag:

    quality = PythonOperator(
        task_id="data_quality_check",
        python_callable=data_quality_check,
    )

    retrain = BashOperator(
        task_id="retrain_models",
        bash_command=f"python {PROJECT_DIR}/anomaly/train_model.py",
    )

    sync = BashOperator(
        task_id="sync_graph",
        bash_command=f"python {PROJECT_DIR}/graph/backfill.py",
    )

    report = PythonOperator(
        task_id="anomaly_report",
        python_callable=anomaly_report,
    )

    quality >> [retrain, sync] >> report
