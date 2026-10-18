from datetime import datetime, timedelta

import pytest

pytest.importorskip("pyspark")

from chispa import assert_df_equality

from tfl.streams.gold import build_episodes, detect_changes, to_line_polls

T0 = datetime(2026, 10, 18, 10, 0)
POLLS_SCHEMA = (
    "line_id string, line_name string, polled_at timestamp, worst_severity int, "
    "status_description string, reason string, state string"
)


def t(minute):
    return T0 + timedelta(minutes=minute)


def polls(spark, sequence):
    """sequence: list of (minute, state, severity) for the Victoria line."""
    rows = [("victoria", "Victoria", t(m), sev, "desc", None, state) for m, state, sev in sequence]
    return spark.createDataFrame(rows, POLLS_SCHEMA)


def test_unplanned_beats_planned_when_a_line_has_both(spark):
    silver = spark.createDataFrame(
        [
            ("victoria", "Victoria", T0, 5, "Part Closure", None),
            ("victoria", "Victoria", T0, 9, "Minor Delays", None),
        ],
        "line_id string, line_name string, polled_at timestamp, status_severity int, status_description string, reason string",
    )
    expected = spark.createDataFrame([("victoria", 5, "unplanned")], "line_id string, worst_severity int, state string")
    result = to_line_polls(silver).select("line_id", "worst_severity", "state")
    assert_df_equality(result, expected, ignore_nullable=True)


def test_single_episode_between_good_service(spark):
    lp = polls(spark, [(0, "good", 10), (1, "unplanned", 6), (2, "unplanned", 6), (3, "good", 10)])
    eps = build_episodes(lp).collect()

    assert len(eps) == 1
    ep = eps[0]
    assert (ep.started_at, ep.last_seen_at) == (t(1), t(2))
    assert ep.duration_minutes == 2
    assert ep.is_open is False
    assert ep.ended_by_gap is False


def test_polling_gap_splits_episode_and_flags_it(spark):
    lp = polls(spark, [(0, "unplanned", 6), (1, "unplanned", 6), (20, "unplanned", 6)])
    eps = sorted(build_episodes(lp, gap_minutes=5).collect(), key=lambda e: e.started_at)

    assert len(eps) == 2
    assert eps[0].ended_by_gap is True and eps[0].is_open is False and eps[0].duration_minutes == 2
    assert eps[1].is_open is True and eps[1].duration_minutes == 1


def test_state_change_starts_new_episode(spark):
    lp = polls(spark, [(0, "planned", 5), (1, "unplanned", 6)])
    states = [e.state for e in sorted(build_episodes(lp).collect(), key=lambda e: e.started_at)]
    assert states == ["planned", "unplanned"]


def test_change_after_gap_is_flagged(spark):
    lp = polls(spark, [(0, "good", 10), (1, "unplanned", 6), (30, "good", 10)])
    changes = sorted(detect_changes(lp, gap_minutes=5).collect(), key=lambda c: c.changed_at)

    assert [c.changed_at for c in changes] == [t(0), t(1), t(30)]
    assert [c.after_gap for c in changes] == [False, False, True]
    assert changes[0].prev_state is None
