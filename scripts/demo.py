"""
One-command demo: build and start the whole stack, run a fresh simulation,
and walk through what the system does -- checking each claim as it happens.

    python scripts/demo.py               # everything, about 16 minutes
    python scripts/demo.py --quick       # stop once day 1 is settled, about 8 minutes
    python scripts/demo.py --no-build    # images already built

Needs only Docker and Python 3.11+ on the host: every pipeline command runs
in a container. It prints what to open, and when, so it doubles as the
script for the demo video. It ends with a PASS/FAIL table: the results in
the report, reproduced. The stack is left running for exploring afterwards.

    1. Readings flow end to end; every zone is live; every component is scraped.
    2. A zone outage (ZONE-C, scheduled) fires ZoneSilent for that zone alone,
       and it clears when the zone returns.
    3. Day 1 settles on schedule. Outside the outage, the real-time view was
       within 2 % of settled; in ZONE-C, settlement recovered the backfill the
       real-time view missed. Bills exist only once a day is settled.
    4. A backdated tariff revision restates day 1: only the revised tier changes.
    5. Day 2's drop is corrupt (deliberately, from the seed): the gate refuses
       it, DropRefused fires, the drop is republished and the day settles.
"""

from __future__ import annotations

import argparse
import ctypes
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
API = "http://localhost:8000"
PROMETHEUS = "http://localhost:9090"
OUTAGE_ZONE = "ZONE-C"
OUTAGE = f"{OUTAGE_ZONE}:150:90"  # offline 150 s after start, for 90 s
PIPELINE = ["meter-simulator", "batch-source", "speed-layer", "airflow"]
REVISION = "Regulator backdated revision: DOMESTIC_STD +10%"
URLS = [
    ("Business dashboard", "http://localhost:8501"),
    ("Grafana (operations)", "http://localhost:3000"),
    ("Airflow", "http://localhost:8080"),
    ("Serving API docs", "http://localhost:8000/docs"),
    ("Prometheus alerts", "http://localhost:9090/alerts"),
    ("MinIO console", "http://localhost:9001"),
]


# -- Output -------------------------------------------------------------------------


def say(text: str = "") -> None:
    print(text, flush=True)


def phase(title: str) -> None:
    say(f"\n{'=' * 78}\n  {title}\n{'=' * 78}")


def look(what: str) -> None:
    say(f"  -> look: {what}")


@dataclass
class Check:
    claim: str
    passed: bool
    detail: str


results: list[Check] = []


def record(claim: str, passed: bool, detail: str) -> bool:
    results.append(Check(claim, passed, detail))
    say(f"  [{'PASS' if passed else 'FAIL'}] {claim}: {detail}")
    return passed


# -- Plumbing ----------------------------------------------------------------------------


def compose(*args: str, env: dict[str, str] | None = None, quiet: bool = True) -> str:
    result = subprocess.run(
        ["docker", "compose", *args],
        cwd=ROOT,
        env={**os.environ, **(env or {})},
        capture_output=quiet,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"docker compose {' '.join(args)} failed:\n{result.stderr or ''}")
    return result.stdout or ""


def http(url: str) -> tuple[int, Any]:
    """(status, JSON body). Status 0 means unreachable."""
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as exc:
        try:
            return exc.code, json.load(exc)
        except ValueError:
            return exc.code, None
    except (urllib.error.URLError, OSError, ValueError):
        return 0, None


def api(path: str) -> Any | None:
    """The API's JSON body for a 200, else None."""
    status, body = http(API + path)
    return body if status == 200 else None


def wait_for(condition: Callable[[], Any], timeout: float, label: str, every: float = 5.0) -> Any:
    """Poll until `condition()` returns something truthy; None on timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = condition()
        if value:
            return value
        time.sleep(every)
    say(f"  ... gave up waiting for {label} after {timeout:.0f} s")
    return None


def parse_ps(output: str) -> list[dict]:
    """`docker compose ps --format json`: one object per line, or one array."""
    output = output.strip()
    if not output:
        return []
    if output.startswith("["):
        return json.loads(output)
    return [json.loads(line) for line in output.splitlines() if line.strip()]


def stack_ready(containers: list[dict]) -> bool:
    """Every long-running service up, and healthy where it has a health check."""
    if not containers:
        return False
    for container in containers:
        if container.get("Service") == "kafka-init":  # one-shot: it exits when done
            continue
        if container.get("State") != "running":
            return False
        if container.get("Health") not in ("", None, "healthy"):
            return False
    return True


def available_memory_gb() -> float | None:
    """Best effort: free physical memory on Windows or Linux; None elsewhere."""
    if sys.platform == "win32":

        class MemoryStatus(ctypes.Structure):
            _fields_ = [
                ("dwLength", ctypes.c_ulong),
                ("dwMemoryLoad", ctypes.c_ulong),
                ("ullTotalPhys", ctypes.c_ulonglong),
                ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong),
                ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong),
                ("ullAvailVirtual", ctypes.c_ulonglong),
                ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
            ]

        status = MemoryStatus()
        status.dwLength = ctypes.sizeof(MemoryStatus)
        ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status))
        return status.ullAvailPhys / 2**30
    meminfo = Path("/proc/meminfo")
    if meminfo.exists():
        for line in meminfo.read_text().splitlines():
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) / 2**20
    return None


@dataclass(frozen=True)
class OutageSplit:
    worst_other_gap: float  # largest |real-time - settled| % outside the outage zone
    outage_gap: float  # the outage zone's real-time - settled %
    outage_late: int  # late readings settlement recovered in the outage zone
    other_late: int  # the most recovered in any other zone


def outage_split(zones: list[dict], outage_zone: str) -> OutageSplit:
    """
    A silenced zone backfills its readings on return, after the speed layer's
    watermark, so its real-time figure for the day is far too low until
    settlement recovers them. Judge the other zones on accuracy, and the
    outage zone on the recovery.
    """
    others = [z for z in zones if z["grid_zone"] != outage_zone]
    outage = next(z for z in zones if z["grid_zone"] == outage_zone)
    return OutageSplit(
        worst_other_gap=max(abs(z["speed_vs_batch_pct"]) for z in others),
        outage_gap=outage["speed_vs_batch_pct"],
        outage_late=outage["late_readings_recovered"] or 0,
        other_late=max(z["late_readings_recovered"] or 0 for z in others),
    )


# -- Conditions the demo waits for -------------------------------------------------------


def fresh_health() -> dict | None:
    body = api("/health")
    return body if body and body.get("status") == "ok" else None


def all_zones_live() -> dict | None:
    body = api("/api/v1/zones/live")
    return body if body and len(body["zones"]) == 6 and not body["stale"] else None


def every_target_up() -> list | None:
    status, body = http(f"{PROMETHEUS}/api/v1/targets")
    if status != 200:
        return None
    targets = body["data"]["activeTargets"]
    return targets if targets and all(t["health"] == "up" for t in targets) else None


def firing(name: str) -> list[dict]:
    status, body = http(f"{PROMETHEUS}/api/v1/alerts")
    alerts = body["data"]["alerts"] if status == 200 else []
    return [a for a in alerts if a["labels"]["alertname"] == name and a["state"] == "firing"]


def settled(day: date, *, reconciled: bool = False) -> dict | None:
    body = api(f"/api/v1/zones/daily?date={day.isoformat()}")
    if not body or body["source"]["status"] != "SETTLED":
        return None
    if reconciled and body["zones"] and body["zones"][0].get("speed_vs_batch_pct") is None:
        return None
    return body


def current_run(day: date) -> dict | None:
    runs = api(f"/api/v1/settlements?date={day.isoformat()}") or []
    return next((run for run in runs if run["current"]), None)


def tier_totals(day: date) -> dict[str, str]:
    body = api(f"/api/v1/bills?date={day.isoformat()}&limit=1")
    return {t["tariff_tier"]: t["total_payable"] for t in body["tiers"]} if body else {}


def trigger_settlement(day: date, sim_id: int, trigger: str, reason: str = "") -> None:
    conf = {
        "business_date": day.isoformat(),
        "trigger": trigger,
        "reason": reason,
        "sim_id": sim_id,
    }
    run_id = f"{trigger}__{day.isoformat()}__{int(time.time())}"
    compose(
        *["exec", "-T", "airflow", "airflow", "dags", "trigger", "daily_settlement"],
        *["--run-id", run_id, "--conf", json.dumps(conf)],
    )


def batch_source(*args: str) -> None:
    compose(
        "exec",
        "-T",
        "batch-source",
        "python",
        "-m",
        "smartgrid.producers.daily_batch_source",
        *args,
    )


# -- Phases --------------------------------------------------------------------------------


def preflight() -> None:
    phase("0. Preflight")
    if shutil.which("docker") is None:
        sys.exit("Docker is not installed or not on PATH.")
    if subprocess.run(["docker", "info"], capture_output=True).returncode != 0:
        sys.exit("The Docker engine is not running. Start Docker Desktop and retry.")
    env = ROOT / ".env"
    if not env.exists():
        shutil.copy(ROOT / ".env.example", env)
        say("  created .env from .env.example (development defaults)")
    free = available_memory_gb()
    if free is None:
        say("  free memory: unknown on this platform")
        return
    say(f"  free memory: {free:.1f} GB")
    if free < 4:
        say("  WARNING: the full stack needs about 6 GB while a day is settled. Close")
        say("  memory-heavy applications (browsers, chat apps) first, or the run may slow.")


def bring_up(build: bool) -> None:
    phase("1. Build and start the stack" + ("" if build else " (no build)"))
    if build:
        say("  first run builds 5 images (Spark, Airflow, API, dashboard, sources): 10-20 min")
    compose("up", "-d", "--remove-orphans", *(["--build"] if build else []), quiet=False)
    ready = wait_for(
        lambda: stack_ready(parse_ps(compose("ps", "--format", "json"))),
        timeout=600,
        label="every service healthy",
    )
    if not ready:
        sys.exit("The stack did not become healthy. See `docker compose ps`.")
    say("  every service is up and healthy")


def fresh_simulation() -> tuple[float, dict]:
    phase("2. A fresh simulation, with a zone outage scheduled")
    compose("stop", *PIPELINE)
    compose(
        *["run", "--rm", "--no-deps", "meter-simulator"],
        *["python", "-m", "smartgrid.common.simulation", "reset", "--yes"],
    )
    t0 = time.time()
    compose("up", "-d", *PIPELINE, env={"SIM_SILENCE": OUTAGE})
    clock = wait_for(lambda: api("/api/v1/clock"), timeout=60, label="the simulated clock")
    if not clock:
        sys.exit("The serving API did not report a simulation.")
    start = clock["simulation_started"]
    say(f"  simulation {clock['sim_id']}: 1 day = 300 real seconds, from {start}")
    say(f"  {OUTAGE_ZONE} goes offline at +150 s for 90 s (SIM_SILENCE={OUTAGE})")
    say("\n  Open these now:")
    for name, url in URLS:
        say(f"    {name:<22} {url}")
    return t0, clock


def live_flow() -> None:
    phase("3. Live: readings flow end to end")
    look("business dashboard, Grid now -- zones fill in, labelled PROVISIONAL")
    health = wait_for(fresh_health, timeout=180, label="fresh live data")
    lag = health["checks"]["speed_layer"]["lag_real_seconds"] if health else None
    record(
        "Readings flow end to end",
        health is not None,
        f"real-time view {lag} s behind (target 60 s)" if health else "no fresh data",
    )
    live = wait_for(all_zones_live, timeout=120, label="all six zones live")
    record(
        "Every zone is live",
        live is not None,
        f"6 zones, {live['total_grid_load_kw']:.0f} kW in total" if live else "zones missing",
    )
    targets = wait_for(every_target_up, timeout=90, label="every scrape target up")
    record(
        "Prometheus scrapes every component",
        targets is not None,
        f"{len(targets)} targets up" if targets else "a target is down",
    )


def outage(t0: float) -> None:
    phase(f"4. Outage: {OUTAGE_ZONE} goes offline")
    look(f"Grafana -- 'Firing alerts' gains ZoneSilent for {OUTAGE_ZONE}")
    fired = wait_for(
        lambda: firing("ZoneSilent"), timeout=max(30, t0 + 330 - time.time()), label="ZoneSilent"
    )
    zones = sorted({alert["labels"].get("zone") for alert in fired or []})
    record(
        "ZoneSilent fires for the silenced zone alone",
        zones == [OUTAGE_ZONE],
        f"firing for {zones or 'no zone'} at +{time.time() - t0:.0f} s",
    )
    cleared = wait_for(lambda: not firing("ZoneSilent"), timeout=150, label="ZoneSilent clearing")
    record(
        "ZoneSilent clears when the zone returns",
        bool(cleared),
        f"resolved at +{time.time() - t0:.0f} s" if cleared else "still firing",
    )


def settle_day_one(t0: float, day1: date, day2: date) -> None:
    phase("5. Day 1 ends and is settled by the batch layer")
    look("Airflow -- daily_settlement: drop, archive, gate, settle, reconcile, report")
    daily = wait_for(
        lambda: settled(day1, reconciled=True),
        timeout=max(60, t0 + 600 - time.time()),
        label="day 1 settled and reconciled",
    )
    run = current_run(day1)
    if daily and run:
        detail = (
            f"run {run['run_id']} at +{time.time() - t0:.0f} s: {run['readings_settled']:,} "
            f"readings, {run['duplicates_removed']} duplicates removed, "
            f"{run['late_readings_recovered']} late readings recovered, "
            f"LKR {float(run['total_billed']):,.2f} billed"
        )
    else:
        detail = "not settled"
    record("Day 1 settles on schedule", bool(daily and run), detail)
    if daily:
        split = outage_split(daily["zones"], OUTAGE_ZONE)
        record(
            "Outside the outage, the real-time view was within 2 % of settled",
            split.worst_other_gap < 2.0,
            f"largest gap in the other zones {split.worst_other_gap:.2f} %",
        )
        record(
            f"Settlement recovered {OUTAGE_ZONE}'s backfill the real-time view missed",
            split.outage_late > 5 * max(split.other_late, 1),
            f"{split.outage_late} late readings recovered in {OUTAGE_ZONE} (other zones at most "
            f"{split.other_late}); its real-time figure was {abs(split.outage_gap):.1f} % low",
        )
    look("business dashboard, Zone history -- day 1 blue (settled), today orange")
    status_d1, _ = http(f"{API}/api/v1/bills?date={day1.isoformat()}&limit=1")
    status_d2, body = http(f"{API}/api/v1/bills?date={day2.isoformat()}&limit=1")
    record(
        "Bills exist only for settled days",
        status_d1 == 200 and status_d2 == 404,
        f"day 1 -> {status_d1}; day 2, in progress -> {status_d2}: "
        f"{(body or {}).get('detail', '')}",
    )


def restatement(day1: date, sim_id: int) -> None:
    phase("6. A backdated tariff revision restates day 1")
    before = tier_totals(day1)
    batch_source("revise", "--date", day1.isoformat(), "--rate-change", "10",
                 "--tier", "DOMESTIC_STD", "--reason", REVISION)  # fmt: skip
    trigger_settlement(day1, sim_id, "restatement", REVISION)
    say("  revision published as a new drop version; restatement triggered in Airflow")
    look("business dashboard, Household bills -- a DOMESTIC_STD bill now has two revisions")
    run = wait_for(
        lambda: (lambda r: r if r and r["trigger"] == "restatement" else None)(current_run(day1)),
        timeout=300,
        label="the restatement",
    )
    after = tier_totals(day1)
    changed = {tier for tier in after if after[tier] != before.get(tier)}
    ok = run is not None and changed == {"DOMESTIC_STD"}
    record(
        "Restatement changes only the revised tier",
        ok,
        f"run {run['run_id']}: DOMESTIC_STD {before.get('DOMESTIC_STD')} -> "
        f"{after.get('DOMESTIC_STD')} LKR; other tiers unchanged"
        if ok
        else f"changed tiers: {sorted(changed)}",
    )


def refused_drop(t0: float, day2: date, sim_id: int) -> None:
    phase("7. Day 2's drop arrives corrupt: refused, alerted, recovered")
    look("Grafana -- 'Days blocked' turns red; DropRefused in the alerts table")
    fired = wait_for(
        lambda: firing("DropRefused"), timeout=max(60, t0 + 780 - time.time()), label="DropRefused"
    )
    record(
        "A corrupt drop is refused and alerted",
        bool(fired),
        f"DropRefused firing at +{time.time() - t0:.0f} s" if fired else "no alert",
    )
    batch_source("publish", "--date", day2.isoformat())
    trigger_settlement(day2, sim_id, "manual")
    say("  drop republished as a new version; settlement re-triggered")
    recovered = wait_for(lambda: settled(day2), timeout=300, label="day 2 settled")
    cleared = wait_for(lambda: not firing("DropRefused"), timeout=60, label="DropRefused clearing")
    record(
        "Republishing recovers the day",
        bool(recovered and cleared),
        "day 2 settled, alert cleared" if recovered and cleared else "not recovered",
    )


def summary(started: float) -> int:
    phase("Summary")
    for check in results:
        say(f"  [{'PASS' if check.passed else 'FAIL'}] {check.claim}")
    passed = sum(check.passed for check in results)
    say(f"\n  {passed} of {len(results)} checks passed in {(time.time() - started) / 60:.1f} min.")
    say("  The stack is still running. Stop it with `docker compose down` (data kept)")
    say("  or `docker compose down -v` (everything removed).")
    return 0 if results and passed == len(results) else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--quick", action="store_true", help="stop once day 1 is settled")
    parser.add_argument("--no-build", action="store_true", help="do not rebuild images")
    args = parser.parse_args()
    started = time.time()
    try:
        preflight()
        bring_up(build=not args.no_build)
        t0, clock = fresh_simulation()
        day1 = date.fromisoformat(clock["simulation_started"])
        day2 = day1 + timedelta(days=1)
        live_flow()
        outage(t0)
        settle_day_one(t0, day1, day2)
        if not args.quick:
            restatement(day1, clock["sim_id"])
            refused_drop(t0, day2, clock["sim_id"])
    except KeyboardInterrupt:
        say("\n  interrupted")
    except RuntimeError as exc:
        say(f"\n  stopped: {exc}")
        record("Demo ran to completion", False, str(exc).splitlines()[0])
    return summary(started)


if __name__ == "__main__":
    sys.exit(main())
