from __future__ import annotations

import logging
import time
from typing import Any

import psycopg
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from .common import Shutdown, configure_logging, decode_event, env, env_int, json_bytes, retry
from .db import connect, initialize


class PoisonEvent(Exception):
    pass


def main() -> None:
    instance_id = env("INSTANCE_ID", "consumer-1")
    configure_logging(instance_id)
    shutdown = Shutdown.install()
    processing_delay = env_int("PROCESSING_DELAY_MS", 200) / 1000
    commit_delay = env_int("COMMIT_DELAY_MS", 100) / 1000
    bootstrap = env("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:19092,kafka-2:19092,kafka-3:19092")

    database = retry(lambda: connect(), description="database connection", shutdown=shutdown)
    retry(lambda: initialize(database), description="database initialization", shutdown=shutdown)

    dlt_producer = Producer(
        {
            "bootstrap.servers": bootstrap,
            "client.id": f"{instance_id}-dlt",
            "acks": "all",
            "enable.idempotence": True,
        }
    )
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": "events-processor",
            "client.id": instance_id,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": env("AUTO_OFFSET_RESET", "earliest"),
            "max.poll.interval.ms": 600_000,
        }
    )

    def on_assign(_consumer: Consumer, partitions) -> None:
        logging.info("assigned partitions: %s", partitions)

    def on_revoke(current_consumer: Consumer, partitions) -> None:
        logging.warning("revoking partitions: %s", partitions)
        try:
            # Commits offsets explicitly stored after completed sink writes.
            current_consumer.commit(asynchronous=False)
            logging.info("stored offsets committed before revoke")
        except KafkaException:
            logging.exception("commit during revoke failed")

    consumer.subscribe(["events"], on_assign=on_assign, on_revoke=on_revoke)
    logging.info("started; auto.offset.reset=%s", env("AUTO_OFFSET_RESET", "earliest"))

    try:
        while not shutdown.requested:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() != KafkaError._PARTITION_EOF:
                    logging.error("consumer error: %s", message.error())
                continue

            try:
                event = decode_event(message.value())
            except Exception as error:
                send_to_dlt(dlt_producer, message, None, f"invalid event: {error}")
                commit_message(consumer, message, commit_delay)
                continue

            if bool(event.get("payload", {}).get("poison")):
                reason = retry_poison(event)
                send_to_dlt(dlt_producer, message, event, reason)
                commit_message(consumer, message, commit_delay)
                continue

            while not shutdown.requested:
                try:
                    shutdown.wait(processing_delay)
                    inserted = write_to_sink(database, event, message, instance_id)
                    if inserted:
                        logging.debug("stored event %s", event["event_id"])
                    else:
                        logging.warning("duplicate event %s dropped", event["event_id"])
                    commit_message(consumer, message, commit_delay)
                    break
                except (psycopg.Error, OSError):
                    logging.exception("sink unavailable; message will not be committed")
                    try:
                        database.close()
                    except Exception:
                        pass
                    if shutdown.wait(2.0):
                        break
                    database = retry(lambda: connect(), description="database reconnection", shutdown=shutdown)
                    retry(lambda: initialize(database), description="database initialization", shutdown=shutdown)
                except KafkaException:
                    # After a broker/quorum outage the group generation can be stale.
                    # Return to poll() so librdkafka can rejoin the group. The sink
                    # primary key safely deduplicates the redelivered message.
                    logging.exception(
                        "offset commit failed; returning to poll so the consumer can rejoin"
                    )
                    break
                except Exception:
                    logging.exception("unexpected processing error; message will not be committed")
                    shutdown.wait(1.0)
    finally:
        try:
            consumer.commit(asynchronous=False)
        except KafkaException:
            logging.exception("final offset commit failed")
        consumer.close()
        dlt_producer.flush(30.0)
        database.close()
        logging.info("stopped")


def retry_poison(event: dict[str, Any]) -> str:
    last_error = ""
    for attempt in range(1, 4):
        try:
            raise PoisonEvent("payload.poison=true")
        except PoisonEvent as error:
            last_error = str(error)
            logging.warning(
                "poison event %s failed, attempt %d/3: %s",
                event["event_id"],
                attempt,
                error,
            )
            if attempt < 3:
                time.sleep(0.2 * attempt)
    return last_error


def write_to_sink(connection, event: dict[str, Any], message, instance_id: str) -> bool:
    with connection.transaction():
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO processed_events (
                    event_id, user_id, seq, event_type, processor_id,
                    topic, partition_no, offset_no, produced_at, payload
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING
                """,
                (
                    event["event_id"],
                    event["user_id"],
                    event["seq"],
                    event["type"],
                    instance_id,
                    message.topic(),
                    message.partition(),
                    message.offset(),
                    event["produced_at"],
                    psycopg.types.json.Jsonb(event["payload"]),
                ),
            )
            inserted = cursor.rowcount == 1
            if not inserted:
                cursor.execute(
                    """
                    INSERT INTO counters(name, value) VALUES ('duplicates_dropped', 1)
                    ON CONFLICT (name) DO UPDATE SET value = counters.value + 1
                    """
                )
    return inserted


def send_to_dlt(producer: Producer, message, event: dict[str, Any] | None, reason: str) -> None:
    delivery = {"done": False, "error": None}

    def report(error, _message) -> None:
        delivery["done"] = True
        delivery["error"] = error

    headers = [
        ("error-reason", reason.encode()),
        ("original-topic", message.topic().encode()),
        ("original-partition", str(message.partition()).encode()),
        ("original-offset", str(message.offset()).encode()),
    ]
    producer.produce(
        "events.dlt",
        key=message.key(),
        value=message.value() if event is None else json_bytes(event),
        headers=headers,
        on_delivery=report,
    )
    remaining = producer.flush(30.0)
    if remaining or not delivery["done"] or delivery["error"]:
        raise RuntimeError(f"DLT delivery failed: {delivery['error'] or 'timeout'}")
    logging.warning(
        "sent %s[%d]@%d to events.dlt",
        message.topic(),
        message.partition(),
        message.offset(),
    )


def commit_message(consumer: Consumer, message, commit_delay: float) -> None:
    consumer.store_offsets(message=message)
    if commit_delay:
        time.sleep(commit_delay)
    consumer.commit(message=message, asynchronous=False)


if __name__ == "__main__":
    main()
