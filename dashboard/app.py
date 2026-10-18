"""Live London Underground disruptions dashboard. Reads the Postgres serving layer."""

import altair as alt
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from tfl.common.config import Settings

st.set_page_config(page_title="Tube Disruptions", layout="wide")

LONDON = "Europe/London"
STATE_STYLE = {
    "good": "background-color: #e8f5e9; color: #1b5e20",
    "planned": "background-color: #fff8e1; color: #8a5a00",
    "unplanned": "background-color: #ffebee; color: #b71c1c",
}
STATE_COLOURS = alt.Scale(domain=["planned", "unplanned"], range=["#f9a825", "#c62828"])


@st.cache_resource
def get_engine():
    s = Settings()
    url = f"postgresql+psycopg://{s.postgres_user}:{s.postgres_password}@{s.postgres_host}:{s.postgres_port}/{s.postgres_db}"
    return create_engine(url, pool_pre_ping=True)


@st.cache_data(ttl=30)
def query(sql: str) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn)


def to_london(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], utc=True).dt.tz_convert(LONDON)
    return df


def style_state(df: pd.DataFrame):
    return df.style.apply(lambda row: [STATE_STYLE.get(row["state"], "")] * len(row), axis=1)


def fit_height(df: pd.DataFrame) -> int:
    """Show every row without an inner scrollbar."""
    return 35 * (len(df) + 1) + 3


st.title("London Underground disruptions")
st.caption("TfL API → Kafka → Spark Structured Streaming → Delta Lake → Postgres. Refreshes every minute.")


@st.fragment(run_every="60s")
def live_section():
    try:
        current = to_london(query("select * from current_status order by worst_severity, line_name"), ["polled_at"])
    except SQLAlchemyError:  # table not created yet / DB not ready
        st.info("Waiting for the gold stream to write its first batch...")
        return

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Good service", int((current["state"] == "good").sum()))
    c2.metric("Planned closures", int((current["state"] == "planned").sum()))
    c3.metric("Unplanned disruptions", int((current["state"] == "unplanned").sum()))
    c4.metric("Last poll (London)", current["polled_at"].max().strftime("%a %H:%M"))

    st.subheader("Live board")
    board = current[["line_name", "state", "status_description", "reason"]]
    st.dataframe(style_state(board), hide_index=True, use_container_width=True, height=fit_height(board))

    left, right = st.columns(2)

    with left:
        st.subheader("Recent status changes")
        changes = to_london(query("select * from status_changes order by changed_at desc limit 20"), ["changed_at"])
        cols = [c for c in ["changed_at", "line_name", "prev_state", "state", "status_description", "after_gap"] if c in changes.columns]
        st.dataframe(changes[cols], hide_index=True, use_container_width=True)
        st.caption("after_gap = first seen after a polling gap, so the real change time is unknown.")

    with right:
        st.subheader("Disruption episodes")
        episodes = to_london(
            query(
                "select line_name, state, started_at, last_seen_at, duration_minutes, is_open, ended_by_gap "
                "from disruption_episodes order by is_open desc, started_at desc limit 30"
            ),
            ["started_at", "last_seen_at"],
        )
        st.dataframe(style_state(episodes), hide_index=True, use_container_width=True)
        st.caption("ended_by_gap = polling stopped, so the episode's true end is unknown.")


live_section()

st.divider()
st.subheader("Last 24 hours")

hourly = to_london(
    query(
        "select line_name, hour_utc, planned_minutes, unplanned_minutes, minutes_observed "
        "from disruptions_by_hour where hour_utc >= now() - interval '24 hours'"
    ),
    ["hour_utc"],
)

if hourly.empty:
    st.info("No data in the last 24 hours yet.")
else:
    totals = (
        hourly.groupby("line_name")[["planned_minutes", "unplanned_minutes"]]
        .sum()
        .reset_index()
        .melt(id_vars="line_name", var_name="type", value_name="minutes")
    )
    totals["type"] = totals["type"].str.replace("_minutes", "")
    bars = (
        alt.Chart(totals)
        .mark_bar()
        .encode(
            x=alt.X("minutes:Q", title="Disrupted minutes"),
            y=alt.Y("line_name:N", sort="-x", title=None),
            color=alt.Color("type:N", scale=STATE_COLOURS, title=None),
            tooltip=["line_name", "type", "minutes"],
        )
        .properties(height=380)
    )

    hourly["hour"] = hourly["hour_utc"].dt.strftime("%a %H:00")
    hourly["unplanned_pct"] = (100 * hourly["unplanned_minutes"] / hourly["minutes_observed"]).round(0)
    heat = (
        alt.Chart(hourly)
        .mark_rect()
        .encode(
            x=alt.X("hour:N", sort=None, title="Hour (London)", axis=alt.Axis(labelAngle=0)),
            y=alt.Y("line_name:N", title=None, axis=alt.Axis(labelOverlap=False)),
            color=alt.Color("unplanned_pct:Q", scale=alt.Scale(scheme="reds", domain=[0, 100]), title="% unplanned"),
            tooltip=["line_name", "hour", "unplanned_minutes", "minutes_observed"],
        )
        .properties(height=380)
    )

    a, b = st.columns(2)
    a.altair_chart(bars, use_container_width=True)
    b.altair_chart(heat, use_container_width=True)

st.subheader("Reliability (all data collected)")
reliability = query(
    "select line_name, round(100.0 * (1 - sum(unplanned_minutes)::numeric / nullif(sum(minutes_observed), 0)), 1) as pct_no_unplanned, "
    "round(100.0 * sum(good_minutes) / nullif(sum(minutes_observed), 0), 1) as pct_good_service, "
    "sum(planned_minutes) as planned_minutes, sum(unplanned_minutes) as unplanned_minutes, "
    "sum(minutes_observed) as minutes_observed from disruptions_by_hour group by line_name order by pct_no_unplanned, pct_good_service"
)
st.dataframe(reliability, hide_index=True, use_container_width=True, height=fit_height(reliability))
st.caption("pct_no_unplanned excludes planned closures (engineering works, scheduled non-running), "
           "so it reflects unexpected problems only.")
