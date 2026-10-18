"""Polls the TfL tube status API and produces one Kafka event per line per poll."""

import json
import logging
import signal
import time
from datetime import UTC, datetime

import requests
from confluent_kafka import KafkaError, Message, Producer

from tfl.common.config import Settings

SCHEMA_VERSION = 1
log = logging.getLogger("poller")


def to_events(lines: list[dict], polled_at: datetime) -> list[tuple[str, dict]]:
    """Turn the raw TfL response into (key, event) pairs. Pure function, easy to test."""
    ts = polled_at.strftime("%Y%m%dT%H%M%SZ")
    events = []
    for line in lines:
        line_id = line["id"]
        statuses = [
            {
                "severity": s.get("statusSeverity"),
                "description": s.get("statusSeverityDescription"),
                "reason": s.get("reason"),
            }
            for s in line.get("lineStatuses", [])
        ]
        event = {
            "event_id": f"{line_id}-{ts}",  # deterministic -> consumers can dedupe
            "schema_version": SCHEMA_VERSION,
            "line_id": line_id,
            "line_name": line.get("name"),
            "polled_at": polled_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "statuses": statuses,
        }
        events.append((line_id, event))
    return events


def fetch_line_statuses(session: requests.Session, settings: Settings) -> list[dict]:
    params = {"app_key": settings.tfl_app_key} if settings.tfl_app_key else {}
    for attempt in range(1, 4):
        try:
            resp = session.get(settings.tfl_url, params=params, timeout=settings.request_timeout_seconds)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            wait = 2**attempt
            log.warning("TfL request failed (attempt %d/3): %s - retrying in %ds", attempt, exc, wait)
            time.sleep(wait)
    raise RuntimeError("TfL API unavailable after 3 attempts")


def on_delivery(err: KafkaError | None, msg: Message) -> None:
    if err is not None:
        log.error("Delivery failed for key=%s: %s", msg.key(), err)


def current_slot(interval: int) -> datetime:
    """Align polls to fixed slots (e.g. every whole minute) so event_ids are stable on retry."""
    epoch = int(time.time()) // interval * interval
    return datetime.fromtimestamp(epoch, tz=UTC)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = Settings()
    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "client.id": "tfl-poller",
            "enable.idempotence": True,  # no duplicates when the producer retries
            "acks": "all",
            "compression.type": "zstd",
            "linger.ms": 50,
        }
    )
    session = requests.Session()

    running = True

    def stop(*_):
        nonlocal running
        log.info("Shutdown signal received")
        running = False

    signal.signal(signal.SIGTERM, stop)  # sent by `docker stop`
    signal.signal(signal.SIGINT, stop)  # Ctrl+C

    log.info("Polling %s every %ds -> %s", settings.tfl_url, settings.poll_interval_seconds, settings.topic_line_status)

    while running:
        polled_at = current_slot(settings.poll_interval_seconds)
        try:
            lines = fetch_line_statuses(session, settings)
            for key, event in to_events(lines, polled_at):
                producer.produce(settings.topic_line_status, key=key, value=json.dumps(event), on_delivery=on_delivery)
            producer.flush(10)
            log.info("Produced %d events for %s", len(lines), polled_at.isoformat())
        except Exception:
            log.exception("Poll failed for %s - skipping this slot", polled_at.isoformat())

        next_slot = polled_at.timestamp() + settings.poll_interval_seconds
        while running and time.time() < next_slot:
            time.sleep(1)

    producer.flush(10)
    log.info("Poller stopped cleanly")


if __name__ == "__main__":
    main()
