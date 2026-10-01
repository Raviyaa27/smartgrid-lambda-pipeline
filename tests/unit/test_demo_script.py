"""The demo script's own logic: reading `docker compose ps` and deciding readiness."""

import importlib.util
import json
import sys
from pathlib import Path

SPEC = importlib.util.spec_from_file_location(
    "demo", Path(__file__).resolve().parents[2] / "scripts" / "demo.py"
)
demo = importlib.util.module_from_spec(SPEC)
sys.modules["demo"] = demo  # @dataclass resolves its module through sys.modules
SPEC.loader.exec_module(demo)


def container(service: str, state: str = "running", health: str = "") -> dict:
    return {"Service": service, "State": state, "Health": health}


def test_compose_ps_is_read_in_both_formats_compose_has_used():
    rows = [container("api", health="healthy"), container("kafka-init", "exited")]
    as_lines = "\n".join(json.dumps(r) for r in rows)
    as_array = json.dumps(rows)
    assert demo.parse_ps(as_lines) == rows == demo.parse_ps(as_array)
    assert demo.parse_ps("") == []


def zone(name: str, gap: float, late: int) -> dict:
    return {"grid_zone": name, "speed_vs_batch_pct": gap, "late_readings_recovered": late}


def test_the_outage_zone_is_judged_on_recovery_and_the_rest_on_accuracy():
    # The day-1 figures from a real demo run: ZONE-C backfilled after a 90 s outage.
    zones = [
        zone("ZONE-A", -1.46, 84),
        zone("ZONE-B", -1.09, 37),
        zone("ZONE-C", -36.05, 1331),
        zone("ZONE-D", -0.70, 36),
    ]
    split = demo.outage_split(zones, "ZONE-C")
    assert split.median_other_gap == 1.09
    assert (split.worst_other, split.worst_other_gap) == ("ZONE-A", -1.46)
    assert (split.outage_gap, split.outage_late, split.other_late) == (-36.05, 1331, 84)
    assert split.most_over == -0.70  # every zone under-reported: none over


def test_one_meters_backfill_does_not_fail_the_accuracy_check():
    # A later run: one high-consumption meter in ZONE-F was offline for four
    # simulated hours, so its zone's real-time figure was 6.3 % low. The median
    # still says how good the real-time view typically is.
    zones = [
        zone("ZONE-A", -0.75, 81),
        zone("ZONE-B", -3.14, 52),
        zone("ZONE-C", -36.06, 1334),
        zone("ZONE-D", -0.69, 35),
        zone("ZONE-E", -0.88, 62),
        zone("ZONE-F", -6.30, 96),
    ]
    split = demo.outage_split(zones, "ZONE-C")
    assert split.median_other_gap == 0.88
    assert (split.worst_other, split.worst_other_gap, split.worst_other_late) == (
        "ZONE-F",
        -6.30,
        96,
    )
    assert split.most_over < 0


def test_over_reporting_anywhere_is_visible():
    zones = [zone("ZONE-A", 0.40, 0), zone("ZONE-B", -0.9, 10), zone("ZONE-C", -30.0, 900)]
    assert demo.outage_split(zones, "ZONE-C").most_over == 0.40


def test_output_is_stamped_in_real_minutes_and_seconds_since_the_start():
    assert demo.elapsed(1000.0, 1000.0) == "+00:00"
    assert demo.elapsed(1000.0, 1245.9) == "+04:05"
    assert demo.elapsed(1000.0, 1000.0 + 16 * 60 + 2) == "+16:02"
    assert demo.elapsed(1000.0, 999.0) == "+00:00"  # never negative


def test_ready_means_running_and_healthy_where_a_health_check_exists():
    ready = [
        container("api", health="healthy"),
        container("speed-layer"),  # no health check: running is enough
        container("kafka-init", "exited"),  # one-shot topic creation: exited is done
    ]
    assert demo.stack_ready(ready)
    assert not demo.stack_ready([*ready, container("grafana", health="starting")])
    assert not demo.stack_ready([*ready, container("airflow", "restarting")])
    assert not demo.stack_ready([])
