"""
Trigger settlement of a simulated day through Airflow -- or restate one.

    python scripts/settle.py --date 2026-01-02
    python scripts/settle.py --date 2026-01-02 --restate --reason "Regulator backdated revision"

Normally the sim_clock_tick DAG settles each day on its own. Use this to
settle a day by hand, or to RESTATE one: after a retroactive tariff revision
(`daily_batch_source revise`) or a late meter backfill, re-running the day
produces a new settlement run whose bills sit beside the originals.

Arguments go to Airflow as a list, never through a shell, so a reason that
contains quotes cannot break -- or become -- a command.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import date

from smartgrid.common.clock_store import shared_clock
from smartgrid.common.config import get_settings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    parser.add_argument("--restate", action="store_true", help="mark the run as a restatement")
    parser.add_argument("--reason", default="", help="why; required with --restate")
    args = parser.parse_args()
    if args.restate and not args.reason:
        parser.error("--restate needs a --reason: a restatement must say why")

    trigger = "restatement" if args.restate else "manual"
    run_id = f"{trigger}__{args.date.isoformat()}__{int(time.time())}"
    # Pin the run to the current simulation, so a reset before it runs voids it.
    sim_id = int(shared_clock(get_settings()).real_start)
    conf = {
        "business_date": args.date.isoformat(),
        "trigger": trigger,
        "reason": args.reason,
        "sim_id": sim_id,
    }
    command = [
        "docker",
        "compose",
        "exec",
        "-T",
        "airflow",
        "airflow",
        "dags",
        "trigger",
        "daily_settlement",
        "--run-id",
        run_id,
        "--conf",
        json.dumps(conf),
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        print(result.stderr or result.stdout, file=sys.stderr)
        return result.returncode
    print(f"Triggered daily_settlement for {args.date} ({trigger}), Airflow run id {run_id}")
    print("Follow it at http://localhost:8080/dags/daily_settlement")
    return 0


if __name__ == "__main__":
    sys.exit(main())
