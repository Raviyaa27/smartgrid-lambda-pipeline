"""
The quality gate must pass every clean drop and name the right fault in
every damaged one -- including each corruption the batch source can inject.
"""

import random
from datetime import date

import pytest

from smartgrid.common import drops
from smartgrid.common.domain import build_fleet
from smartgrid.common.drop_quality import DropCheck, check_drop
from smartgrid.producers.daily_batch_source import (
    EXPECTED_CHECK,
    Corruption,
    corrupt_files,
    render_drop,
)

DAY = date(2026, 1, 3)
SEED = 20260101
FLEET = build_fleet(num_households=40, num_zones=4, seed=11)


def make_drop(described=None, uploaded=None, *, day=DAY, filed_under=DAY, version=1):
    described = described if described is not None else render_drop(FLEET, day, SEED)
    manifest = drops.build_manifest(day, version, described, published_at_sim="2026-01-03T00:30:00")
    return drops.LoadedDrop(
        business_date=filed_under,
        version=version,
        manifest=manifest.to_dict(),
        files=dict(uploaded if uploaded is not None else described),
    )


def test_a_clean_drop_passes():
    report = check_drop(make_drop(), FLEET)
    assert report.passed, report.findings
    assert report.stats["households_covered"] == len(FLEET)
    assert report.stats["zones_forecast"] == len(FLEET.zones)


@pytest.mark.parametrize("corruption", list(Corruption))
def test_every_injected_corruption_is_caught_with_the_expected_check(corruption):
    for seed in range(15):  # cover each random branch
        files = render_drop(FLEET, DAY, SEED)
        described, uploaded = corrupt_files(files, corruption, random.Random(seed))
        report = check_drop(make_drop(described, uploaded), FLEET)
        assert not report.passed, f"{corruption} seed={seed} passed the gate"
        assert EXPECTED_CHECK[corruption] in report.checks_failed, (
            corruption,
            seed,
            report.findings,
        )


def test_every_corruption_has_an_expected_check():
    assert set(EXPECTED_CHECK) == set(Corruption)


def test_a_drop_without_a_manifest_is_absent():
    drop = make_drop()
    incomplete = drops.LoadedDrop(drop.business_date, drop.version, None, drop.files)
    report = check_drop(incomplete, FLEET)
    assert report.checks_failed == {DropCheck.MANIFEST_MISSING}


def test_a_file_listed_in_the_manifest_but_absent_is_caught():
    drop = make_drop()
    files = {k: v for k, v in drop.files.items() if k != drops.WEATHER_FILE}
    report = check_drop(drops.LoadedDrop(DAY, 1, drop.manifest, files), FLEET)
    assert DropCheck.FILE_MISSING in report.checks_failed


def test_integrity_failure_stops_the_gate_before_content_checks():
    files = render_drop(FLEET, DAY, SEED)
    described, uploaded = corrupt_files(files, Corruption.TRUNCATED_FILE, random.Random(0))
    report = check_drop(make_drop(described, uploaded), FLEET)
    assert report.checks_failed <= {DropCheck.CHECKSUM_MISMATCH, DropCheck.RECORD_COUNT_MISMATCH}


def test_a_drop_for_another_date_is_caught():
    files = render_drop(FLEET, date(2026, 1, 2), SEED)
    report = check_drop(make_drop(files, day=date(2026, 1, 2), filed_under=DAY), FLEET)
    assert DropCheck.WRONG_DATE in report.checks_failed


def test_a_missing_zone_forecast_is_caught():
    files = render_drop(FLEET, DAY, SEED)
    lines = files[drops.WEATHER_FILE].decode().splitlines()
    files[drops.WEATHER_FILE] = ("\n".join(lines[1:]) + "\n").encode()
    assert DropCheck.ZONE_MISSING in check_drop(make_drop(files), FLEET).checks_failed


def test_findings_are_aggregated_not_repeated():
    files = render_drop(FLEET, DAY, SEED)
    described, uploaded = corrupt_files(files, Corruption.UNKNOWN_TIER, random.Random(3))
    report = check_drop(make_drop(described, uploaded), FLEET)
    unknown_tier = [f for f in report.findings if f.check is DropCheck.UNKNOWN_TIER]
    assert len(unknown_tier) == 1
    assert unknown_tier[0].count >= 1


def test_report_serialises_for_the_run_log():
    doc = check_drop(make_drop(), FLEET).to_dict()
    assert doc["passed"] is True
    assert doc["business_date"] == DAY.isoformat()
