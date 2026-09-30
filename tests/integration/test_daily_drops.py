"""
Publish, load, gate and revise a drop against the real MinIO.

Everything is written under a unique `_it/<id>/` root prefix and deleted
afterwards, so the test never touches the pipeline's own drops.
"""

import json
import uuid
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from smartgrid.common import drops, storage
from smartgrid.common.billing import DEFAULT_SCHEDULE, TariffSchedule
from smartgrid.common.config import get_settings
from smartgrid.common.domain import build_fleet
from smartgrid.common.drop_quality import DropCheck, check_latest
from smartgrid.producers.daily_batch_source import Corruption, DropFault, publish

pytestmark = pytest.mark.integration

DAY = date(2026, 1, 3)
SEED = 20260101
NOW = datetime(2026, 1, 3, 0, 30, tzinfo=UTC)
FLEET = build_fleet(num_households=20, num_zones=3, seed=3)


@pytest.fixture(scope="module")
def s3():
    settings = get_settings()
    client = storage.s3_client(settings)
    try:
        client.head_bucket(Bucket=settings.minio_bucket_raw)
    except Exception:  # noqa: BLE001 - any failure means the stack is not up
        pytest.skip("MinIO is not reachable; run `docker compose up -d` first")
    return client, settings.minio_bucket_raw


@pytest.fixture
def root(s3):
    client, bucket = s3
    prefix = f"_it/{uuid.uuid4().hex[:10]}/"
    yield prefix
    storage.delete_keys(client, bucket, storage.list_keys(client, bucket, prefix))


def test_a_published_drop_is_complete_and_passes_the_gate(s3, root):
    client, bucket = s3
    manifest = publish(client, bucket, FLEET, DAY, seed=SEED, sim_now=NOW, root=root)
    assert manifest.version == 1
    assert drops.latest_complete_version(client, bucket, DAY, root) == 1
    report = check_latest(client, bucket, DAY, FLEET, root)
    assert report.passed, report.findings


def test_republishing_adds_a_version_and_never_overwrites(s3, root):
    client, bucket = s3
    publish(client, bucket, FLEET, DAY, seed=SEED, sim_now=NOW, root=root)
    original = storage.get_bytes(
        client, bucket, drops.drop_prefix(DAY, 1, root) + drops.MANIFEST_FILE
    )
    second = publish(client, bucket, FLEET, DAY, seed=SEED, sim_now=NOW, root=root)
    assert second.version == 2 and second.supersedes == 1
    assert drops.versions(client, bucket, DAY, root) == [1, 2]
    assert (
        storage.get_bytes(client, bucket, drops.drop_prefix(DAY, 1, root) + drops.MANIFEST_FILE)
        == original
    )


def test_a_corrupt_drop_fails_and_a_republish_recovers_it(s3, root):
    client, bucket = s3
    publish(
        client,
        bucket,
        FLEET,
        DAY,
        seed=SEED,
        sim_now=NOW,
        root=root,
        corruption=Corruption.UNKNOWN_TIER,
        fault=DropFault.CORRUPT,
    )
    failed = check_latest(client, bucket, DAY, FLEET, root)
    assert DropCheck.UNKNOWN_TIER in failed.checks_failed

    publish(client, bucket, FLEET, DAY, seed=SEED, sim_now=NOW, root=root)
    assert check_latest(client, bucket, DAY, FLEET, root).passed


def test_a_revision_changes_the_schedule_the_batch_layer_will_read(s3, root):
    client, bucket = s3
    publish(client, bucket, FLEET, DAY, seed=SEED, sim_now=NOW, root=root)
    revised = DEFAULT_SCHEDULE.with_rate_change(Decimal("1.10"), ["DOMESTIC_STD"])
    publish(
        client,
        bucket,
        FLEET,
        DAY,
        seed=SEED,
        sim_now=NOW,
        root=root,
        schedule=revised,
        revision={"reason": "test", "rate_factor": "1.10"},
    )

    latest = drops.latest_complete_version(client, bucket, DAY, root)
    loaded = drops.load_drop(client, bucket, DAY, latest, root)
    schedule = TariffSchedule.from_dict(json.loads(loaded.files[drops.SCHEDULE_FILE]))
    assert schedule.headline_rate("DOMESTIC_STD") == Decimal("13.20")
    assert check_latest(client, bucket, DAY, FLEET, root).passed

    truth = drops.read_ground_truth(client, bucket, DAY, latest, root)
    assert truth["revision"]["reason"] == "test"


def test_files_without_a_manifest_are_treated_as_absent(s3, root):
    client, bucket = s3
    prefix = drops.drop_prefix(DAY, 1, root)
    storage.put_bytes(client, bucket, prefix + drops.HOUSEHOLDS_FILE, b"{}\n", "application/json")
    assert drops.latest_complete_version(client, bucket, DAY, root) is None
    report = check_latest(client, bucket, DAY, FLEET, root)
    assert report.checks_failed == {DropCheck.MANIFEST_MISSING}
