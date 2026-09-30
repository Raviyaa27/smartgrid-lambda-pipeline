"""
Inspect the batch layer: settlement runs, bills, and the speed-vs-batch gap.

    python scripts/inspect_settlement.py
    python scripts/inspect_settlement.py --date 2026-01-02    # one day in detail

For every settled day: which run is current, what it billed, how many late
readings it recovered, and how far the real-time view was from the settled
figures per zone. For a restated day, shows each run so the change is visible.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date

import psycopg

from smartgrid.common.config import get_settings

RULE = "-" * 100


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--date", type=date.fromisoformat, default=None)
    args = parser.parse_args()
    dsn = get_settings().postgres_dsn

    with psycopg.connect(dsn) as conn:
        runs = conn.execute(
            "SELECT r.run_id, r.business_date, r.status, r.trigger, r.drop_version, "
            "r.readings_archived, r.duplicates_removed, r.readings_rejected, r.readings_settled, "
            "r.households_billed, r.total_billed, r.reason, "
            "(c.run_id = r.run_id) AS current, "
            "(SELECT sum(missed_by_speed) FROM ops.reconciliation x WHERE x.run_id = r.run_id), "
            "(SELECT round(avg(abs(delta_pct))::numeric, 2) FROM ops.reconciliation x "
            " WHERE x.run_id = r.run_id), "
            "(SELECT object_key FROM ops.daily_reports d WHERE d.run_id = r.run_id) "
            "FROM ops.settlement_runs r LEFT JOIN batch.current_runs c USING (business_date) "
            + ("WHERE r.business_date = %s " if args.date else "")
            + "ORDER BY r.business_date, r.run_id",
            (args.date,) if args.date else (),
        ).fetchall()
        gates = conn.execute(
            "SELECT business_date, passed, findings FROM ops.quality_gate_results "
            + ("WHERE business_date = %s " if args.date else "")
            + "ORDER BY id",
            (args.date,) if args.date else (),
        ).fetchall()

        if not runs and not gates:
            print("\nNo settlement runs yet. Is Airflow running, and has a simulated day ended?\n")
            return 1

        print(f"\n{RULE}\n  Settlement runs\n{RULE}")
        print(
            f"  {'run':>4}  {'date':<11}{'status':<10}{'trigger':<12}{'drop':<5}{'archived':>9}"
            f"{'dupes':>7}{'settled':>9}{'late':>6}{'bills':>6}{'billed LKR':>14}{'gap%':>7}"
        )
        for (
            run_id,
            day,
            status,
            trigger,
            version,
            archived,
            dupes,
            _rejected,
            settled,
            bills,
            total,
            reason,
            current,
            missed,
            delta,
            _key,
        ) in runs:
            mark = "*" if current else " "
            print(
                f" {mark}{run_id:>4}  {day!s:<11}{status:<10}{trigger:<12}v{version:<4}"
                f"{archived or 0:>9,}{dupes or 0:>7,}{settled or 0:>9,}{missed or 0:>6,}"
                f"{bills or 0:>6}{(total or 0):>14,.2f}{delta if delta is not None else '-':>7}"
            )
            if reason:
                print(f"         reason: {reason}")
        print(
            "  * = the run the serving layer shows for that day.  late = readings only the batch "
            "layer saw.\n  gap% = mean absolute gap between the real-time view and settlement, "
            "across zones."
        )

        failed_gates = [(d, f) for d, passed, f in gates if not passed]
        if failed_gates:
            print(f"\n  Quality gate refusals: {len(failed_gates)}")
            for day, findings in failed_gates[-5:]:
                print(f"    {day}: " + "; ".join(f"{x['check']} x{x['count']}" for x in findings))

        if args.date:
            print(f"\n{RULE}\n  {args.date}: speed vs batch, by zone (current run)\n{RULE}")
            for zone, speed, batch, pct, missed in conn.execute(
                "SELECT x.grid_zone, x.speed_kwh, x.batch_kwh, x.delta_pct, x.missed_by_speed "
                "FROM ops.reconciliation x JOIN batch.current_runs c USING (run_id) "
                "WHERE c.business_date = %s ORDER BY 1",
                (args.date,),
            ).fetchall():
                print(
                    f"  {zone}  real-time {speed or 0:>9.1f} kWh   settled {batch:>9.1f} kWh   "
                    f"gap {pct if pct is not None else float('nan'):>6.2f}%   "
                    f"late readings {missed or 0}"
                )
            keys = [r[-1] for r in runs if r[-1]]
            if keys:
                print(f"\n  report: s3://lake/{keys[-1]}")
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
