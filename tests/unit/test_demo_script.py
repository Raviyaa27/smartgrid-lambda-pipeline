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
    assert split.worst_other_gap == 1.46
    assert (split.outage_gap, split.outage_late, split.other_late) == (-36.05, 1331, 84)


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
