# -*- coding: utf-8 -*-
"""실시간 이상탐지 대시보드 (Streamlit).

consumer 가 PostgreSQL 에 적재한 PCA + T2 결과를 읽어
관리도 / 이상 이벤트 / 센서 기여도를 실시간으로 보여준다.

실행:
    .venv/bin/streamlit run dashboard/app.py
"""

import os
import sys

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine

# 프로젝트 루트를 import 경로에 추가 (streamlit run 은 dashboard/ 를 기준으로 실행)
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)
sys.path.insert(0, HERE)   # airflow_client 를 형제 모듈로 import

import config  # noqa: E402
import airflow_client  # noqa: E402
from airflow_client import AirflowError  # noqa: E402

DB_URL = config.sqlalchemy_url()
STEPS = ["DEPOSITION", "ETCH", "LITHOGRAPHY", "INSPECTION"]

st.set_page_config(page_title="반도체 이상탐지", layout="wide")


@st.cache_resource
def get_engine():
    return create_engine(DB_URL, pool_pre_ping=True)


def load_data(step, limit):
    """최근 이벤트를 step 필터와 함께 조회."""
    where = "" if step == "ALL" else f"WHERE step = '{step}'"
    query = f"""
        SELECT id, ingest_time, wafer_id, lot_id, step, equipment_id,
               equipment_health, pca_t2, t2_ucl, is_anomaly,
               top_contributor, contributions, label
        FROM semiconductor_events
        {where}
        ORDER BY id DESC
        LIMIT {limit}
    """
    df = pd.read_sql(query, get_engine())
    return df.iloc[::-1].reset_index(drop=True)  # 시간 오름차순


def control_chart(df, step):
    """T2 관리도: 선 + UCL + 이상점 강조."""
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["id"], y=df["pca_t2"], mode="lines+markers",
        name="T²", line=dict(color="#4C78A8", width=1),
        marker=dict(size=4),
    ))
    anom = df[df["is_anomaly"] == True]  # noqa: E712
    if not anom.empty:
        fig.add_trace(go.Scatter(
            x=anom["id"], y=anom["pca_t2"], mode="markers",
            name="이상", marker=dict(color="#E45756", size=10, symbol="x"),
        ))
    # UCL 선 (step 별 단일 값 기준)
    ucl = df["t2_ucl"].dropna()
    if not ucl.empty:
        fig.add_hline(
            y=float(ucl.iloc[-1]), line_dash="dash", line_color="#E45756",
            annotation_text="UCL", annotation_position="top left",
        )
    fig.update_layout(
        title=f"Hotelling's T² 관리도 — {step}",
        xaxis_title="event id", yaxis_title="T²",
        height=380, margin=dict(t=40, b=30), showlegend=True,
    )
    return fig


def contribution_chart(row):
    """선택된 이상 이벤트의 센서별 기여도 막대."""
    contrib = row.get("contributions") or {}
    if not contrib:
        return None
    items = sorted(contrib.items(), key=lambda kv: kv[1], reverse=True)
    keys = [k for k, _ in items]
    vals = [v for _, v in items]
    fig = go.Figure(go.Bar(
        x=vals, y=keys, orientation="h",
        marker=dict(color="#E45756"),
    ))
    fig.update_layout(
        title="센서 기여도 (T² 분해)",
        xaxis_title="기여 비율", height=300,
        margin=dict(t=40, b=30), yaxis=dict(autorange="reversed"),
    )
    return fig


# --------------------------------------------------------------------- #
# 사이드바
# --------------------------------------------------------------------- #

st.sidebar.title("⚙️ 설정")
st.sidebar.caption("아래 두 항목은 '실시간 모니터링' 탭에만 적용됩니다.")
sel_step = st.sidebar.selectbox("공정 스텝", ["ALL"] + STEPS)
limit = st.sidebar.slider("표시할 최근 이벤트 수", 50, 1000, 300, step=50)
st.sidebar.caption("실시간 탭은 3초마다 자동 갱신됩니다.")
st.sidebar.divider()
st.sidebar.markdown(f"[🌀 Airflow UI 열기]({config.AIRFLOW_UI_URL})")

st.title("🔬 반도체 공정 실시간 이상탐지")
st.caption("PCA + Hotelling's T² · 스텝별 다변량 이상탐지 · Airflow 일일 배치")


# --------------------------------------------------------------------- #
# 실시간 본문 (3초마다 재실행)
# --------------------------------------------------------------------- #

@st.fragment(run_every=3)
def live_view():
    try:
        df = load_data(sel_step, limit)
    except Exception as e:
        st.error(f"DB 조회 실패: {e}")
        return

    if df.empty:
        st.info("아직 데이터가 없습니다. simulator 와 consumer 를 실행하세요.")
        return

    # KPI
    total = len(df)
    n_anom = int((df["is_anomaly"] == True).sum())  # noqa: E712
    rate = (n_anom / total * 100) if total else 0
    latest_t2 = df["pca_t2"].dropna()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("표시 이벤트", f"{total:,}")
    c2.metric("이상 건수", f"{n_anom:,}")
    c3.metric("이상 비율", f"{rate:.1f}%")
    c4.metric("최근 T²", f"{latest_t2.iloc[-1]:.2f}" if not latest_t2.empty else "-")

    # 관리도
    st.plotly_chart(control_chart(df, sel_step), use_container_width=True)

    # 이상 이벤트 테이블 + 기여도
    left, right = st.columns([3, 2])
    with left:
        st.subheader("최근 이상 이벤트")
        anom = df[df["is_anomaly"] == True].iloc[::-1]  # noqa: E712
        show = anom[[
            "id", "wafer_id", "step", "equipment_id",
            "pca_t2", "t2_ucl", "top_contributor", "label",
        ]].head(20)
        st.dataframe(show, use_container_width=True, hide_index=True)

    with right:
        st.subheader("센서 기여도")
        anom = df[df["is_anomaly"] == True]  # noqa: E712
        if anom.empty:
            st.info("현재 이상 이벤트가 없습니다.")
        else:
            row = anom.iloc[-1]
            st.caption(
                f"가장 최근 이상: **{row['wafer_id']}** "
                f"[{row['step']}] T²={row['pca_t2']:.2f}"
            )
            fig = contribution_chart(row)
            if fig:
                st.plotly_chart(fig, use_container_width=True)

# --------------------------------------------------------------------- #
# 배치 오케스트레이션 (Airflow) 탭
# --------------------------------------------------------------------- #
#
# 실시간 탭과 달리 자동 갱신하지 않는다. DAG 는 하루 한 번 도는 배치라
# 3초마다 API 를 두드릴 이유가 없고, Airflow 웹서버에 부하만 준다.
# 캐시 TTL 10초 + 명시적 새로고침 버튼으로 충분하다.

# Airflow 의 태스크 상태 → 화면에 쓸 아이콘
STATE_ICON = {
    "success": "🟢",
    "running": "🔵",
    "failed": "🔴",
    "upstream_failed": "🟠",
    "skipped": "⚪",
    "queued": "🟡",
    "scheduled": "🟡",
    "up_for_retry": "🟡",
    "deferred": "🟡",
    "removed": "⚫",
    None: "⚪",
}

# DAG 안의 의존 순서 (Airflow 가 돌려주는 순서는 보장되지 않는다)
TASK_ORDER = ["data_quality_check", "retrain_models", "sync_graph", "anomaly_report"]


def _fmt_ts(value):
    """Airflow 가 주는 ISO8601 문자열을 'MM-DD HH:MM:SS' 로."""
    if not value:
        return "-"
    try:
        return pd.to_datetime(value).strftime("%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return str(value)


@st.cache_data(ttl=10, show_spinner=False)
def fetch_airflow_state():
    """DAG 메타 + 최근 실행 이력을 한 번에. 실패하면 예외 메시지를 담아 반환."""
    try:
        return {
            "ok": True,
            "health": airflow_client.health(),
            "dag": airflow_client.get_dag(),
            "runs": airflow_client.list_runs(limit=10),
        }
    except AirflowError as e:
        return {"ok": False, "error": str(e)}


@st.cache_data(ttl=10, show_spinner=False)
def fetch_task_instances(run_id):
    try:
        return airflow_client.list_task_instances(run_id), None
    except AirflowError as e:
        return [], str(e)


def load_daily_report():
    """배치 산출물(anomaly_daily_report). 테이블이 없으면 빈 DataFrame."""
    query = """
        SELECT report_date, step, equipment_id, runs, anomalies,
               anomaly_rate, created_at
        FROM anomaly_daily_report
        WHERE report_date = (SELECT MAX(report_date) FROM anomaly_daily_report)
        ORDER BY anomaly_rate DESC NULLS LAST, runs DESC
    """
    try:
        return pd.read_sql(query, get_engine())
    except Exception:
        # 테이블 미생성 = DAG 이 아직 한 번도 안 돈 상태. 정상 흐름이다.
        return pd.DataFrame()


def report_chart(df):
    """장비별 이상률 막대 (배치 리포트 시각화)."""
    top = df.head(15)
    labels = [f"{s} / {e}" for s, e in zip(top["step"], top["equipment_id"])]
    fig = go.Figure(go.Bar(
        x=top["anomaly_rate"].astype(float), y=labels, orientation="h",
        marker=dict(color="#4C78A8"),
        text=[f"{r:.1f}%" for r in top["anomaly_rate"].astype(float)],
        textposition="outside",
    ))
    fig.update_layout(
        title="스텝·장비별 이상률 (배치 집계)",
        xaxis_title="이상률 (%)", height=max(320, 28 * len(top)),
        margin=dict(t=40, b=30, l=10), yaxis=dict(autorange="reversed"),
    )
    return fig


def batch_view():
    st.subheader("🌀 Airflow — 일일 유지보수 DAG")
    st.caption(
        "품질검사 → (모델 재학습 ∥ 그래프 동기화) → 이상률 리포트. "
        f"DAG: `{config.AIRFLOW_DAG_ID}`"
    )

    ctl1, ctl2, ctl3 = st.columns([1, 1, 4])
    if ctl1.button("🔄 새로고침", use_container_width=True):
        fetch_airflow_state.clear()
        fetch_task_instances.clear()
    trigger_clicked = ctl2.button("▶️ 지금 실행", use_container_width=True)
    ctl3.markdown(
        f"<div style='padding-top:6px'>"
        f"<a href='{config.AIRFLOW_UI_URL}' target='_blank'>Airflow UI 에서 열기 ↗</a>"
        f"</div>",
        unsafe_allow_html=True,
    )

    if trigger_clicked:
        try:
            run = airflow_client.trigger()
            fetch_airflow_state.clear()
            st.success(f"DAG 실행 요청 완료 — run_id: `{run.get('dag_run_id')}`")
        except AirflowError as e:
            st.error(f"실행 요청 실패: {e}")

    state = fetch_airflow_state()

    if not state["ok"]:
        st.warning(
            "Airflow 에 연결할 수 없습니다. 배치 스택은 profile 로 분리돼 있어 "
            "기본 기동에는 포함되지 않습니다.\n\n"
            "```\ndocker compose --profile airflow up -d --build\n```"
        )
        st.caption(f"상세: {state['error']}")
        st.divider()
        _render_report_section()
        return

    dag = state["dag"]
    runs = state["runs"]
    health = state["health"]

    sched_status = health.get("scheduler", {}).get("status", "unknown")
    last_run = runs[0] if runs else None

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("스케줄러", "🟢 정상" if sched_status == "healthy" else f"🔴 {sched_status}")
    k2.metric("DAG 상태", "⏸ 일시정지" if dag.get("is_paused") else "▶️ 활성")
    k3.metric("스케줄", str(dag.get("schedule_interval", {}).get("value")
                          or dag.get("timetable_description") or "-"))
    k4.metric(
        "최근 실행",
        f"{STATE_ICON.get(last_run['state'], '⚪')} {last_run['state']}" if last_run else "없음",
    )

    st.divider()

    # --- 최근 실행 이력 --------------------------------------------------
    st.markdown("#### 최근 실행 이력")
    if not runs:
        st.info(
            "아직 실행 이력이 없습니다. 위의 **지금 실행** 버튼으로 첫 배치를 돌려보세요."
        )
    else:
        hist = pd.DataFrame([{
            "상태": f"{STATE_ICON.get(r['state'], '⚪')} {r['state']}",
            "run_id": r["dag_run_id"],
            "유형": r.get("run_type", "-"),
            "시작": _fmt_ts(r.get("start_date")),
            "종료": _fmt_ts(r.get("end_date")),
        } for r in runs])
        st.dataframe(hist, use_container_width=True, hide_index=True)

        # --- 선택한 실행의 태스크별 상태 ---------------------------------
        st.markdown("#### 태스크 상태")
        run_ids = [r["dag_run_id"] for r in runs]
        sel_run = st.selectbox("실행 선택", run_ids, index=0, key="af_run")

        tis, ti_err = fetch_task_instances(sel_run)
        if ti_err:
            st.error(ti_err)
        elif not tis:
            st.info("태스크가 아직 스케줄되지 않았습니다.")
        else:
            by_id = {t["task_id"]: t for t in tis}
            ordered = ([by_id[t] for t in TASK_ORDER if t in by_id]
                       + [t for t in tis if t["task_id"] not in TASK_ORDER])

            cols = st.columns(len(ordered))
            for col, ti in zip(cols, ordered):
                dur = ti.get("duration")
                col.metric(
                    ti["task_id"],
                    f"{STATE_ICON.get(ti['state'], '⚪')} {ti['state'] or '대기'}",
                    f"{dur:.1f}s" if dur else None,
                )

            # 실패한 태스크는 로그를 바로 펼쳐볼 수 있게 (Airflow UI 왕복 절약)
            failed = [t for t in ordered if t["state"] in ("failed", "upstream_failed")]
            for ti in failed:
                with st.expander(f"🔴 {ti['task_id']} 로그"):
                    try:
                        log = airflow_client.task_log(
                            sel_run, ti["task_id"],
                            try_number=max(ti.get("try_number") or 1, 1),
                        )
                        st.code(log[-4000:] or "(로그 없음)")
                    except AirflowError as e:
                        st.error(str(e))

    st.divider()
    _render_report_section()


def _render_report_section():
    """DAG 의 최종 산출물(anomaly_daily_report)을 보여준다.

    Airflow 가 안 떠 있어도 과거 배치 결과는 앱 DB 에 남아있으므로,
    연결 실패 경로에서도 이 섹션은 그대로 렌더한다.
    """
    st.markdown("#### 📋 일일 이상률 리포트 (`anomaly_daily_report`)")
    rep = load_daily_report()
    if rep.empty:
        st.info(
            "리포트 테이블이 비어있습니다 — `anomaly_report` 태스크가 성공하면 "
            "여기에 스텝·장비별 집계가 나타납니다."
        )
        return

    st.caption(f"기준일: **{rep['report_date'].iloc[0]}** · {len(rep)}개 조합")
    left, right = st.columns([2, 3])
    with left:
        st.dataframe(
            rep[["step", "equipment_id", "runs", "anomalies", "anomaly_rate"]],
            use_container_width=True, hide_index=True, height=420,
        )
    with right:
        st.plotly_chart(report_chart(rep), use_container_width=True)


# --------------------------------------------------------------------- #
# 탭 구성
# --------------------------------------------------------------------- #

tab_live, tab_batch = st.tabs(["📈 실시간 모니터링", "🌀 배치 오케스트레이션"])

with tab_live:
    live_view()

with tab_batch:
    batch_view()
