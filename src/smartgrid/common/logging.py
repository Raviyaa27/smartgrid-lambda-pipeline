"""
Structured JSON logging with correlation-id propagation.

Written by hand rather than pulled from a library for two reasons: the log
shape is a deliberate design artifact (see docs/adr/), and the viva
requires defending every line of pipeline logic.

Every record is one JSON object on one line, so the logs are greppable by a
human and parseable by a collector without a regex. Arbitrary structured
fields ride along via the standard `extra=` kwarg:

    log.info("reading accepted", extra={"meter_id": "MTR-00007", "kwh": 0.42})

A correlation id is stamped on an event at the producer and carried through
every stage, so one reading can be followed end to end:

    with correlation_scope() as cid:
        log.info("emitting", extra={"event_id": eid})
"""

from __future__ import annotations

import json
import logging
import sys
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Any

# ContextVar rather than a global: correct under threads AND asyncio, which
# matters once FastAPI serves concurrent requests in Section 9.
_correlation_id: ContextVar[str | None] = ContextVar("correlation_id", default=None)

# Attributes the stdlib puts on every LogRecord. Anything NOT in this set was
# supplied by the caller via `extra=` and is promoted into the JSON payload.
# Derived at import time so it stays correct across Python versions.
_RESERVED: frozenset[str] = frozenset(
    logging.LogRecord("", 0, "", 0, "", (), None).__dict__
) | {"asctime", "message", "taskName"}


def new_correlation_id() -> str:
    """Short, collision-safe id. 12 hex chars keeps log lines readable."""
    return uuid.uuid4().hex[:12]


def set_correlation_id(cid: str | None) -> None:
    _correlation_id.set(cid)


def get_correlation_id() -> str | None:
    return _correlation_id.get()


@contextmanager
def correlation_scope(cid: str | None = None) -> Iterator[str]:
    """Bind a correlation id for the duration of the block, then restore."""
    cid = cid or new_correlation_id()
    token = _correlation_id.set(cid)
    try:
        yield cid
    finally:
        _correlation_id.reset(token)


class JsonFormatter(logging.Formatter):
    """Renders each LogRecord as a single-line JSON object."""

    def __init__(self, service: str) -> None:
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=UTC)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "level": record.levelname,
            "service": self.service,
            "logger": record.name,
            "message": record.getMessage(),
        }

        if (cid := _correlation_id.get()) is not None:
            payload["correlation_id"] = cid

        # Promote caller-supplied `extra=` fields to top level.
        for key, value in record.__dict__.items():
            if key not in _RESERVED:
                payload[key] = value

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        # default=str so datetimes and Decimals never blow up a log call.
        return json.dumps(payload, default=str, ensure_ascii=False)


def configure_logging(service: str, level: str | int = "INFO") -> None:
    """
    Install the JSON formatter on the root logger. Idempotent, so it is safe
    to call from every entry point including Spark executors.
    """
    root = logging.getLogger()
    root.setLevel(level)

    for existing in list(root.handlers):
        root.removeHandler(existing)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(service))
    root.addHandler(handler)

    # Third-party libraries are noisy at INFO; they are not our signal.
    for noisy in ("kafka", "botocore", "urllib3", "s3transfer", "py4j"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


class PipelineStage:
    """
    Context manager that emits one structured record per pipeline stage.

    This is the backbone of the observability story: every stage reports how
    many records entered, left, and were quarantined, plus wall-clock
    duration. Those same counters become Prometheus metrics in Section 10.

        with PipelineStage(log, "validate", source="kafka") as st:
            st.records_in = len(batch)
            st.records_out = len(good)
            st.records_quarantined = len(bad)
    """

    def __init__(self, logger: logging.Logger, stage: str, **context: Any) -> None:
        self._log = logger
        self.stage = stage
        self.context = context
        self.records_in = 0
        self.records_out = 0
        self.records_quarantined = 0
        self._started = 0.0

    def __enter__(self) -> PipelineStage:
        self._started = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        duration_ms = round((time.perf_counter() - self._started) * 1000, 2)
        fields: dict[str, Any] = {
            "stage": self.stage,
            "records_in": self.records_in,
            "records_out": self.records_out,
            "records_quarantined": self.records_quarantined,
            "duration_ms": duration_ms,
            "status": "error" if exc_type else "ok",
            **self.context,
        }
        if exc_type:
            self._log.error(
                f"stage {self.stage} failed", extra=fields, exc_info=(exc_type, exc, tb)
            )
        else:
            self._log.info(f"stage {self.stage} complete", extra=fields)
        return False  # never swallow the exception
