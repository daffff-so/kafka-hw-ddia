from __future__ import annotations

import json
import logging
import os
import random
import time
import uuid
from pathlib import Path

from confluent_kafka import Producer

from .common import (
    Shutdown,
    configure_logging,
    env,
    env_float,
    env_int,
    json_bytes,
    sleep_until,
    utc_now,
)


EVENT_TYPES = ("page_view", "add_to_cart", "purchase")


def load_sequences(path: Path, users: list[str]) -> dict[str, int]:
    try:
        stored = json.loads(path.read_text())
    except FileNotFoundError:
        stored = {}
    return {user: int(stored.get(user, 0)) for user in users}


def save_sequences(path: Path, sequences: dict[str, int]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(sequences, sort_keys=True))
    temporary.replace(path)


def main() -> None:
    instance_id = env("INSTANCE_ID", "producer-1")
    configure_logging(instance_id)
    shutdown = Shutdown.install()
    producer_number = env_int("PRODUCER_NUMBER", 1)
    users_per_producer = env_int("USERS_PER_PRODUCER", 10)
    users = [
        f"user-{producer_number:02d}-{number:03d}"
        for number in range(1, users_per_producer + 1)
    ]
    rate = env_float("EVENTS_PER_SECOND", 5.0)
    poison_rate = env_float("POISON_RATE", 0.02)
    key_mode = env("KEY_MODE", "user_id")
    if rate <= 0:
        raise ValueError("EVENTS_PER_SECOND must be positive")
    if key_mode not in {"user_id", "none"}:
        raise ValueError("KEY_MODE must be user_id or none")

    state_path = Path(env("STATE_FILE", "/state/sequences.json"))
    sequences = load_sequences(state_path, users)
    client = Producer(
        {
            "bootstrap.servers": env("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:19092,kafka-2:19092,kafka-3:19092"),
            "client.id": instance_id,
            "acks": "all",
            "enable.idempotence": True,
            "retries": 2_147_483_647,
            "max.in.flight.requests.per.connection": 5,
        }
    )

    delivered_error = None

    def delivery_report(error, message) -> None:
        nonlocal delivered_error
        delivered_error = error
        if error:
            logging.error("delivery failed: %s", error)
        else:
            logging.debug(
                "delivered event to %s[%d]@%d",
                message.topic(),
                message.partition(),
                message.offset(),
            )

    interval = 1.0 / rate
    deadline = time.monotonic()
    sent = 0
    logging.info("started with %d owned users, rate=%.2f/s, key_mode=%s", len(users), rate, key_mode)
    try:
        while not shutdown.requested:
            user_id = random.choice(users)
            seq = sequences[user_id] + 1
            poison = random.random() < poison_rate
            # Deterministic ID makes a resend after a crash safe for the sink.
            event_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{instance_id}:{user_id}:{seq}"))
            event = {
                "event_id": event_id,
                "user_id": user_id,
                "type": random.choice(EVENT_TYPES),
                "seq": seq,
                "produced_at": utc_now(),
                "payload": {"poison": poison, "producer_id": instance_id},
            }
            delivered_error = None
            while not shutdown.requested:
                try:
                    client.produce(
                        "events",
                        key=user_id.encode() if key_mode == "user_id" else None,
                        value=json_bytes(event),
                        on_delivery=delivery_report,
                    )
                    break
                except BufferError:
                    logging.warning("producer queue is full; waiting")
                    client.poll(1.0)
            if shutdown.requested:
                break
            remaining = client.flush(15.0)
            if remaining or delivered_error is not None:
                logging.error(
                    "event %s was not acknowledged; sequence is not advanced (remaining=%d)",
                    event_id,
                    remaining,
                )
                shutdown.wait(1.0)
                continue
            sequences[user_id] = seq
            save_sequences(state_path, sequences)
            sent += 1
            if sent % 100 == 0:
                logging.info("sent %d events", sent)
            deadline += interval
            sleep_until(deadline, shutdown)
    finally:
        remaining = client.flush(30.0)
        if remaining:
            logging.error("shutdown left %d messages undelivered", remaining)
        else:
            logging.info("stopped cleanly after %d events", sent)


if __name__ == "__main__":
    main()
