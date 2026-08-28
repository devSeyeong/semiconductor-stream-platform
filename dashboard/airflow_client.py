# -*- coding: utf-8 -*-
"""Airflow REST API(v1) 얇은 클라이언트.

대시보드의 "배치 오케스트레이션" 탭이 이 모듈로 DAG 상태를 읽고 수동 실행을
건다. Airflow 메타DB 에 직접 붙지 않고 REST API 를 쓰는 이유:

* 메타DB 스키마는 Airflow 버전마다 바뀌는 내부 구현이다. API 는 계약이다.
* 대시보드에 Airflow 메타DB 자격증명을 주지 않아도 된다.

Airflow 가 안 떠 있는 게 정상 상태(profile 로 분리돼 있다)이므로, 모든 함수는
예외를 던지지 않고 `AirflowError` 를 담은 결과나 None 을 돌려준다 — 대시보드는
"Airflow 미기동" 안내를 띄우고 나머지 탭은 그대로 동작해야 한다.
"""

from __future__ import annotations

import os
import sys
from typing import Any

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import config  # noqa: E402

# Airflow 웹서버 기동에는 시간이 걸리므로 넉넉하지 않게 — 대시보드가 3초마다
# 재실행되는 환경이라 빨리 실패하고 안내를 띄우는 편이 낫다.
TIMEOUT = 5


class AirflowError(RuntimeError):
    """Airflow 에 붙지 못했거나 API 가 에러를 돌려준 경우."""


def _auth() -> tuple[str, str]:
    return (config.AIRFLOW_USER, config.AIRFLOW_PASSWORD)


def _request(method: str, path: str, **kwargs) -> Any:
    url = f"{config.AIRFLOW_API_URL.rstrip('/')}/{path.lstrip('/')}"
    try:
        resp = requests.request(
            method, url, auth=_auth(), timeout=TIMEOUT, **kwargs
        )
    except requests.RequestException as e:
        raise AirflowError(f"Airflow 에 연결할 수 없습니다 ({url}): {e}") from e

    if resp.status_code == 401:
        raise AirflowError(
            "Airflow 인증 실패 — AIRFLOW_USER / AIRFLOW_PASSWORD 를 확인하세요."
        )
    if resp.status_code >= 400:
        raise AirflowError(f"Airflow API {resp.status_code}: {resp.text[:200]}")
    return resp.json() if resp.content else {}


# --------------------------------------------------------------------- #
# 조회
# --------------------------------------------------------------------- #

def health() -> dict:
    """스케줄러/메타DB 상태. /health 는 api/v1 하위가 아니라 루트에 있다."""
    base = config.AIRFLOW_API_URL.rstrip("/").removesuffix("/api/v1")
    try:
        resp = requests.get(f"{base}/health", timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        raise AirflowError(f"Airflow 헬스체크 실패 ({base}/health): {e}") from e


def get_dag(dag_id: str | None = None) -> dict:
    dag_id = dag_id or config.AIRFLOW_DAG_ID
    return _request("GET", f"/dags/{dag_id}")


def list_runs(dag_id: str | None = None, limit: int = 10) -> list[dict]:
    """최근 DAG Run 을 최신순으로."""
    dag_id = dag_id or config.AIRFLOW_DAG_ID
    data = _request(
        "GET",
        f"/dags/{dag_id}/dagRuns",
        params={
            "limit": limit,
            "order_by": "-execution_date",
        },
    )
    return data.get("dag_runs", [])


def list_task_instances(run_id: str, dag_id: str | None = None) -> list[dict]:
    """한 DAG Run 안의 태스크별 상태."""
    dag_id = dag_id or config.AIRFLOW_DAG_ID
    data = _request("GET", f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances")
    return data.get("task_instances", [])


def task_log(run_id: str, task_id: str, try_number: int = 1,
             dag_id: str | None = None) -> str:
    """태스크 로그 본문 (실패 원인을 대시보드에서 바로 보기 위함)."""
    dag_id = dag_id or config.AIRFLOW_DAG_ID
    data = _request(
        "GET",
        f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/{try_number}",
        params={"full_content": "true"},
        headers={"Accept": "application/json"},
    )
    if isinstance(data, dict):
        return data.get("content", "")
    return str(data)


# --------------------------------------------------------------------- #
# 실행 제어
# --------------------------------------------------------------------- #

def trigger(dag_id: str | None = None, conf: dict | None = None) -> dict:
    """DAG 수동 실행. run_id 는 Airflow 가 붙여준다(manual__<timestamp>)."""
    dag_id = dag_id or config.AIRFLOW_DAG_ID
    return _request("POST", f"/dags/{dag_id}/dagRuns", json={"conf": conf or {}})


def set_paused(paused: bool, dag_id: str | None = None) -> dict:
    dag_id = dag_id or config.AIRFLOW_DAG_ID
    return _request("PATCH", f"/dags/{dag_id}", json={"is_paused": paused})
