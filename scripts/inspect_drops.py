"""
Gate every daily drop and measure the quality gate against ground truth.

For each business date, runs the SAME quality gate the batch layer will run
before settling (common.drop_quality.check_latest), then compares its
verdict with the fault the batch source recorded under `_ground_truth/`:

    clean, late or revised drop   -> the gate must PASS
    corrupt drop                  -> the gate must FAIL, naming the expected check
    missing drop                  -> the gate must FAIL with manifest_missing

    python scripts/inspect_drops.py
    python scripts/inspect_drops.py --date 2026-01-04     # full findings for one day

Exit code 0 when every verdict matches its designed outcome, 1 otherwise.
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from datetime import date

from smartgrid.common import drops
from smartgrid.common.config import get_settings
from smartgrid.common.domain import build_fleet_from_settings
from smartgrid.common.drop_quality import DropCheck, QualityReport, check_latest
from smartgrid.common.storage import s3_client

RULE = "-" * 110


def _label(truth: dict | None) -> str:
    if not truth:
        return "unknown"
    if truth.get("revision"):
        revision = truth["revision"]
        pct = (float(revision["rate_factor"]) - 1) * 100
        return f"revision {pct:+.0f}%"
    if truth.get("fault") == "corrupt":
        return f"corrupt: {truth['corruption']}"
    return truth.get("fault") or "clean"


def _as_designed(report: QualityReport, truth: dict | None) -> bool:
    fault = (truth or {}).get("fault")
    if fault == "corrupt":
        return not report.passed and truth["expected_check"] in {
            c.value for c in report.checks_failed
        }
    if fault == "missing":
        return DropCheck.MANIFEST_MISSING in report.checks_failed
    return report.passed


def _verdict(report: QualityReport, width: int | None = None) -> str:
    if report.passed:
        return "PASS"
    checks = sorted(c.value for c in report.checks_failed)
    full = "FAIL " + ", ".join(checks)
    if width is None or len(full) <= width:
        return full
    return f"FAIL {checks[0]} (+{len(checks) - 1} more)"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--date",
        type=date.fromisoformat,
        default=None,
        help="show every finding for one business date",
    )
    args = parser.parse_args()

    settings = get_settings()
    client = s3_client(settings)
    bucket = settings.minio_bucket_raw
    fleet = build_fleet_from_settings(settings)

    if args.date is not None:
        report = check_latest(client, bucket, args.date, fleet)
        truth = drops.read_ground_truth(client, bucket, args.date, report.version or 1)
        print(f"\n{args.date}  v{report.version}  -> {_verdict(report)}")
        print(
            f"injected: {_label(truth)}   as designed: "
            f"{'yes' if _as_designed(report, truth) else 'NO'}\n"
        )
        for finding in report.findings:
            print(f"  [{finding.check.value}] x{finding.count}: {finding.detail}")
        if report.stats:
            print(f"\n  stats: {dict(report.stats)}")
        print()
        return 0 if _as_designed(report, truth) else 1

    days = sorted(
        set(drops.published_dates(client, bucket)) | set(drops.ground_truth_dates(client, bucket))
    )
    if not days:
        print("\nNo drops found. Is the daily batch source running?\n")
        return 1

    print(f"\n{RULE}\n  Daily drops: quality gate verdict vs injected fault\n{RULE}")
    print(f"  {'business date':<15}{'versions':<10}{'gate':<46}{'injected':<32}ok")
    tally: Counter[str] = Counter()
    wrong = 0
    for day in days:
        report = check_latest(client, bucket, day, fleet)
        truth = drops.read_ground_truth(client, bucket, day, report.version or 1)
        ok = _as_designed(report, truth)
        wrong += 0 if ok else 1
        label = _label(truth)
        tally[label.split(":")[0].split(" ")[0]] += 1
        found = drops.versions(client, bucket, day)
        versions = ",".join(f"v{v}" for v in found) if found else "-"
        print(
            f"  {day.isoformat():<15}{versions:<10}{_verdict(report, width=44):<46}{label:<32}"
            f"{'yes' if ok else 'NO'}"
        )

    print(RULE)
    print("  " + " | ".join(f"{k} {v}" for k, v in sorted(tally.items())))
    print(f"  {len(days)} business dates; {wrong} verdict(s) differ from design.")
    print(f"  Gate accuracy: {(len(days) - wrong) / len(days):.2%}\n")
    return 0 if wrong == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
