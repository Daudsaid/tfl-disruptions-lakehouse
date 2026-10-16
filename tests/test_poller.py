from datetime import datetime, timezone

from tfl.producer.poller import to_events

SAMPLE = [
    {
        "id": "victoria",
        "name": "Victoria",
        "lineStatuses": [{"statusSeverity": 10, "statusSeverityDescription": "Good Service"}],
    },
    {
        "id": "central",
        "name": "Central",
        "lineStatuses": [
            {"statusSeverity": 6, "statusSeverityDescription": "Severe Delays", "reason": "Signal failure"},
            {"statusSeverity": 9, "statusSeverityDescription": "Minor Delays"},
        ],
    },
]
POLLED_AT = datetime(2026, 10, 16, 11, 0, tzinfo=timezone.utc)


def test_one_event_per_line_keyed_by_line_id():
    events = to_events(SAMPLE, POLLED_AT)
    assert [key for key, _ in events] == ["victoria", "central"]


def test_event_id_is_deterministic():
    _, event = to_events(SAMPLE, POLLED_AT)[0]
    assert event["event_id"] == "victoria-20261016T110000Z"
    assert event["polled_at"] == "2026-10-16T11:00:00Z"


def test_keeps_multiple_statuses_per_line():
    _, event = to_events(SAMPLE, POLLED_AT)[1]
    assert [s["severity"] for s in event["statuses"]] == [6, 9]
    assert event["statuses"][0]["reason"] == "Signal failure"
