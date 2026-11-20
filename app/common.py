from __future__ import annotations

import json
import logging
import os
import signal
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Event
from typing import Any


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


def env_int(name: str, default: int) -> int:
    return int(env(name, str(default)))


def env_float(name: str, default: float) -> float:
    return float(env(name, str(default)))


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def configure_logging(instance_id: str) -> None:
    logging.basicConfig(
        level=env("LOG_LEVEL", "INFO"),
        format=f"%(asctime)s %(levelname)s [{instance_id}] %(message)s",
    )


def json_bytes(value: dict[str, Any]) -> bytes:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode()


def decode_event(raw: bytes | None) -> dict[str, Any]:
    if raw is None:
        raise ValueError("message value is null")
    value = json.loads(raw)
    required = {"event_id", "user_id", "type", "seq", "produced_at", "payload"}
    missing = required.difference(value)
    if missing:
        raise ValueError(f"missing event fields: {sorted(missing)}")
    return value


@dataclass
class Shutdown:
    event: Event

    @classmethod
    def install(cls) -> "Shutdown":
        event = Event()

        def stop(_signum: int, _frame: object) -> None:
            event.set()

        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
        return cls(event)

    @property
    def requested(self) -> bool:
        return self.event.is_set()

    def wait(self, seconds: float) -> bool:
        return self.event.wait(seconds)


def retry(operation, *, description: str, shutdown: Shutdown, delay: float = 2.0):
    while not shutdown.requested:
        try:
            return operation()
        except Exception:
            logging.exception("%s failed; retrying in %.1fs", description, delay)
            shutdown.wait(delay)
    raise RuntimeError(f"shutdown while waiting for {description}")


def sleep_until(deadline: float, shutdown: Shutdown) -> None:
    remaining = deadline - time.monotonic()
    if remaining > 0:
        shutdown.wait(remaining)

