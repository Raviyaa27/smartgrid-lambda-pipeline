"""
Inspect the live meter-reading stream and measure fault detection.

Consumes from Kafka for a fixed period, runs every message through the SAME
shared validator both Lambda layers use, and compares each verdict against
the ground truth the simulator recorded in the `injected_fault` header.

The result is a detection matrix: for every kind of injected fault, what
the pipeline concluded and whether that was the designed outcome. It turns
"our validation works" from a claim into a measured number for the report.

    python scripts/inspect_stream.py                  # 20 s from now
    python scripts/inspect_stream.py --seconds 60
    python scripts/inspect_stream.py --from-beginning # replay retained history

Exit code 0 when every message was classified as designed, 1 otherwise.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import uuid
from collections import Counter
from datetime import datetime

from confluent_kafka import Consumer, KafkaException

from smartgrid.common.clock_store import shared_clock
from smartgrid.common.config import get_settings
from smartgrid.common.domain import build_fleet_from_settings
from smartgrid.common.transformations import validate_record
from smartgrid.producers.faults import EXPECTED_QUARANTINE, FaultKind

LATE_THRESHOLD_SIM_MINUTES = 30.0
RULE = "-" * 76


def _verdict(raw: bytes, known_ids: frozenset[str], now: datetime) -> tuple[str, dict | None]:
    try:
        record = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return "malformed_json", None
    result = validate_record(record, known_household_ids=known_ids, now=now)
    return ("valid", result.record) if result.ok else (result.reason.value, None)


def _expected(fault: str) -> str:
    if fault == "none":
        return "valid"
    reason = EXPECTED_QUARANTINE[FaultKind(fault)]
    return "valid" if reason is None else reason.value


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--seconds", type=float, default=20.0)
    parser.add_argument("--from-beginning", action="store_true")
    args = parser.parse_args()

    settings = get_settings()
    clock = shared_clock(settings)
    known_ids = build_fleet_from_settings(settings).household_ids

    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            # A throwaway group: inspecting must never move the pipeline's offsets.
            "group.id": f"inspect-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest" if args.from_beginning else "latest",
            "enable.auto.commit": False,
            "logger": logging.getLogger("kafka.consumer"),
        }
    )
    consumer.subscribe([settings.kafka_topic_readings])

    matrix: Counter[tuple[str, str]] = Counter()
    zones: Counter[str] = Counter()
    meters: set[str] = set()
    seen_ids: Counter[str] = Counter()
    event_times: list[datetime] = []
    late = 0
    max_lag_min = 0.0
    total = 0

    print(f"\nListening to '{settings.kafka_topic_readings}' for {args.seconds:g}s ...")
    deadline = time.monotonic() + args.seconds
    first_at: float | None = None
    try:
        while time.monotonic() < deadline:
            msg = consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            first_at = first_at or time.monotonic()
            total += 1

            headers = {k: v.decode() for k, v in (msg.headers() or [])}
            fault = headers.get("injected_fault", "none")
            now = clock.now()
            verdict, record = _verdict(msg.value(), known_ids, now)
            matrix[(fault, verdict)] += 1

            if record is not None:
                zones[record["grid_zone"]] += 1
                meters.add(record["meter_id"])
                seen_ids[record["event_id"]] += 1
                event_times.append(record["event_time"])
                lag_min = (now - record["event_time"]).total_seconds() / 60.0
                max_lag_min = max(max_lag_min, lag_min)
                if lag_min > LATE_THRESHOLD_SIM_MINUTES:
                    late += 1
    finally:
        consumer.close()

    if total == 0:
        print("\nNo messages received. Is the meter simulator running?\n")
        return 1

    elapsed = max(1e-9, time.monotonic() - (first_at or deadline))
    repeats = sum(count - 1 for count in seen_ids.values() if count > 1)

    print(f"\n{RULE}\n  Stream summary\n{RULE}")
    print(f"  messages            : {total:,}  ({total / elapsed:,.1f}/s)")
    if event_times:
        print(
            f"  simulated time      : {min(event_times):%Y-%m-%d %H:%M} .. "
            f"{max(event_times):%Y-%m-%d %H:%M}"
        )
    print(f"  distinct meters     : {len(meters)}")
    print("  readings per zone   : " + " | ".join(f"{z} {n}" for z, n in sorted(zones.items())))
    print(f"  repeated event_ids  : {repeats}   (retransmissions, for dedup to remove)")
    print(
        f"  {f'late (> {LATE_THRESHOLD_SIM_MINUTES:g} sim min)':<20}: {late}   "
        f"(max lag {max_lag_min:,.0f} sim min)"
    )

    print(f"\n{RULE}\n  Fault detection: injected fault -> verdict of the shared validator\n{RULE}")
    print(f"  {'injected':<19}{'count':>7}   {'verdict':<20}{'designed outcome':<20}ok")
    wrong = 0
    for (fault, verdict), count in sorted(
        matrix.items(), key=lambda kv: (kv[0][0] != "none", kv[0])
    ):
        expected = _expected(fault)
        ok = verdict == expected
        wrong += 0 if ok else count
        print(f"  {fault:<19}{count:>7,}   {verdict:<20}{expected:<20}{'yes' if ok else 'NO'}")

    injected = sum(c for (f, _), c in matrix.items() if f != "none")
    print(RULE)
    print(
        f"  {injected:,} faults injected in {total:,} messages "
        f"({injected / total:.1%}); {wrong:,} classified differently from design."
    )
    print(f"  Detection accuracy: {(total - wrong) / total:.2%}\n")
    return 0 if wrong == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
