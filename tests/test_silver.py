import json
from datetime import datetime

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import Row  # noqa: E402

from tfl.streams.silver import parse_bronze, to_silver  # noqa: E402

GOOD = [{"severity": 10, "description": "Good Service", "reason": None}]


def event(statuses=None, polled_at="2026-10-18T10:00:00Z", line_id="victoria"):
    return json.dumps(
        {
            "event_id": f"{line_id}-20261018T100000Z",
            "schema_version": 1,
            "line_id": line_id,
            "line_name": line_id.title(),
            "polled_at": polled_at,
            "statuses": GOOD if statuses is None else statuses,
        }
    )


def bronze(spark, raws):
    ts = datetime(2026, 10, 18, 10, 0)
    rows = [
        Row(kafka_key="victoria", raw_value=raw, topic="tfl.line_status", partition=0,
            offset=i, kafka_timestamp=ts, _ingested_at=ts)
        for i, raw in enumerate(raws)
    ]
    return spark.createDataFrame(rows)


def test_valid_event_becomes_one_row_per_status(spark):
    raw = event(statuses=[
        {"severity": 6, "description": "Severe Delays", "reason": "Signal failure"},
        {"severity": 9, "description": "Minor Delays", "reason": None},
    ])
    rows = to_silver(parse_bronze(bronze(spark, [raw])).filter("is_valid")).orderBy("status_position").collect()

    assert [r.status_severity for r in rows] == [6, 9]
    assert all(r.is_disrupted for r in rows)
    assert rows[0].reason == "Signal failure"
    assert rows[0].polled_at == datetime(2026, 10, 18, 10, 0)


def test_good_service_is_not_disrupted(spark):
    rows = to_silver(parse_bronze(bronze(spark, [event()])).filter("is_valid")).collect()
    assert rows[0].is_disrupted is False


@pytest.mark.parametrize(
    "raw",
    [
        "not json at all",
        event(statuses=[]),
        event(statuses=[{"severity": 99, "description": "??", "reason": None}]),
        event(polled_at="yesterday"),
    ],
    ids=["malformed-json", "no-statuses", "severity-out-of-range", "bad-timestamp"],
)
def test_bad_events_are_flagged_not_dropped(spark, raw):
    parsed = parse_bronze(bronze(spark, [raw])).collect()
    assert len(parsed) == 1
    assert parsed[0].is_valid is False


def test_duplicate_events_are_removed(spark):
    raw = event()
    assert to_silver(parse_bronze(bronze(spark, [raw, raw])).filter("is_valid")).count() == 1
