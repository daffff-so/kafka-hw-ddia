from __future__ import annotations

import collections
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, TopicPartition

from .common import decode_event, env
from .db import connect, initialize


def read_dlt() -> tuple[list[dict], int]:
    consumer = Consumer(
        {
            "bootstrap.servers": env("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:19092,kafka-2:19092,kafka-3:19092"),
            "group.id": f"verify-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "enable.partition.eof": True,
        }
    )
    partitions = [TopicPartition("events.dlt", number, 0) for number in range(3)]
    consumer.assign(partitions)
    eof = set()
    events: list[dict] = []
    invalid = 0
    deadline = time.monotonic() + 30
    try:
        while len(eof) < len(partitions) and time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    eof.add(message.partition())
                else:
                    raise RuntimeError(message.error())
                continue
            try:
                events.append(decode_event(message.value()))
            except Exception:
                invalid += 1
    finally:
        consumer.close()
    if len(eof) < len(partitions):
        raise RuntimeError("timed out while reading the DLT snapshot")
    return events, invalid


def main() -> int:
    print(f"{env('GITHUB_LOGIN', 'GITHUB_LOGIN_NOT_SET')} {datetime.now(timezone.utc).isoformat()}")
    database = connect()
    initialize(database)
    with database.cursor() as cursor:
        cursor.execute("SELECT user_id, seq, event_id FROM processed_events")
        sink_rows = cursor.fetchall()
        cursor.execute("SELECT value FROM counters WHERE name = 'duplicates_dropped'")
        duplicate_row = cursor.fetchone()
        duplicates = duplicate_row[0] if duplicate_row else 0
        cursor.execute(
            """
            SELECT count(*)
            FROM processed_events AS later
            WHERE EXISTS (
                SELECT 1 FROM processed_events AS earlier
                WHERE earlier.user_id = later.user_id
                  AND earlier.processed_at < later.processed_at
                  AND earlier.seq > later.seq
            )
            """
        )
        order_violations = cursor.fetchone()[0]
    database.close()

    dlt_events, invalid_dlt = read_dlt()
    sequences: dict[str, set[int]] = collections.defaultdict(set)
    for user_id, seq, _event_id in sink_rows:
        sequences[user_id].add(int(seq))
    for event in dlt_events:
        sequences[str(event["user_id"])].add(int(event["seq"]))

    gaps: dict[str, list[int]] = {}
    for user_id, observed in sorted(sequences.items()):
        if not observed:
            continue
        missing = sorted(set(range(1, max(observed) + 1)).difference(observed))
        if missing:
            gaps[user_id] = missing[:50]

    unique_dlt = len({event["event_id"] for event in dlt_events})
    result = {
        "sink_events": len(sink_rows),
        "dlt_records": len(dlt_events),
        "dlt_unique_events": unique_dlt,
        "dlt_invalid_records": invalid_dlt,
        "duplicates_dropped": duplicates,
        "order_violations": order_violations,
        "users_seen": len(sequences),
        "sequence_gaps": gaps,
        "status": "PASS" if not gaps and invalid_dlt == 0 else "FAIL",
    }
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())

