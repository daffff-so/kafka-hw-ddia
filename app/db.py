from __future__ import annotations

import os

import psycopg


SCHEMA = """
CREATE TABLE IF NOT EXISTS processed_events (
    event_id UUID PRIMARY KEY,
    user_id TEXT NOT NULL,
    seq BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    processor_id TEXT NOT NULL,
    topic TEXT NOT NULL,
    partition_no INTEGER NOT NULL,
    offset_no BIGINT NOT NULL,
    produced_at TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS processed_events_user_seq_idx
    ON processed_events (user_id, seq);

CREATE TABLE IF NOT EXISTS counters (
    name TEXT PRIMARY KEY,
    value BIGINT NOT NULL DEFAULT 0
);
INSERT INTO counters(name, value) VALUES ('duplicates_dropped', 0)
ON CONFLICT (name) DO NOTHING;
"""


def connect(*, autocommit: bool = False) -> psycopg.Connection:
    return psycopg.connect(
        os.getenv(
            "DATABASE_URL",
            "postgresql://kafka:kafka@postgres:5432/kafka_hw",
        ),
        autocommit=autocommit,
    )


def initialize(connection: psycopg.Connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute(SCHEMA)
    connection.commit()

