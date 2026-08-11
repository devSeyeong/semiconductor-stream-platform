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
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import config  # noqa: E402

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
sel_step = st.sidebar.selectbox("공정 스텝", ["ALL"] + STEPS)
limit = st.sidebar.slider("표시할 최근 이벤트 수", 50, 1000, 300, step=50)
st.sidebar.caption("3초마다 자동 갱신됩니다.")

st.title("🔬 반도체 공정 실시간 이상탐지")
st.caption("PCA + Hotelling's T² · 스텝별 다변량 이상탐지")


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


live_view()