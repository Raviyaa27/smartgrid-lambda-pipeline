"""
Layout and I/O of the daily reference drop in the raw landing zone.

One drop per business date, holding three files and a manifest:

    raw/daily/dt=2026-01-03/v=1/tariff_schedule.json      the day's tariff (TariffSchedule)
    raw/daily/dt=2026-01-03/v=1/household_tariffs.jsonl   tier + subsidy per household
    raw/daily/dt=2026-01-03/v=1/weather_forecast.jsonl    forecast per grid zone
    raw/daily/dt=2026-01-03/v=1/_MANIFEST.json            written LAST

Two rules make the landing zone trustworthy (see ADR-0008):

  Immutable, versioned. A published drop is never overwritten. A correction
  -- a regulator backdating a tariff change -- is published as v=2 beside
  v=1. Settlement reads the latest complete version; the earlier ones stay,
  so any past bill can be recomputed under the exact tariff it was first
  issued with.

  Complete only when the manifest exists. Files are uploaded first and the
  manifest last. A reader that sees no manifest treats the drop as absent,
  so a half-finished upload is never read. The manifest also carries each
  file's SHA-256 and record count, so corruption in transit is detectable.

Line-delimited JSON (one record per line) rather than one large JSON
document: it streams, it appends, and Spark reads it natively without the
`multiLine` option that a pretty-printed document silently requires.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date
from typing import Any

from smartgrid.common import storage
from smartgrid.common.schemas import SCHEMA_VERSION

DATASET = "daily"
SCHEDULE_FILE = "tariff_schedule.json"
HOUSEHOLDS_FILE = "household_tariffs.jsonl"
WEATHER_FILE = "weather_forecast.jsonl"
MANIFEST_FILE = "_MANIFEST.json"
DATA_FILES: tuple[str, ...] = (SCHEDULE_FILE, HOUSEHOLDS_FILE, WEATHER_FILE)

# Ground truth for injected faults lives under its own prefix. The pipeline
# must never read it -- it exists so detection can be measured, exactly like
# the `injected_fault` header on the stream.
GROUND_TRUTH_PREFIX = "_ground_truth/daily"

_VERSION_RE = re.compile(r"/v=(\d+)/$")
_DATE_RE = re.compile(r"/dt=(\d{4}-\d{2}-\d{2})/$")


def day_prefix(day: date, root: str = "") -> str:
    return f"{root}{DATASET}/dt={day.isoformat()}/"


def drop_prefix(day: date, version: int, root: str = "") -> str:
    return f"{day_prefix(day, root)}v={version}/"


def ground_truth_key(day: date, version: int, root: str = "") -> str:
    return f"{root}{GROUND_TRUTH_PREFIX}/dt={day.isoformat()}/v={version}.json"


# -- Serialisation ---------------------------------------------------------


def to_jsonl(records: Iterable[Mapping[str, Any]]) -> bytes:
    """One JSON object per line. Keys sorted, so identical data is identical bytes."""
    lines = [json.dumps(r, sort_keys=True, separators=(",", ":"), default=str) for r in records]
    return ("\n".join(lines) + "\n").encode("utf-8") if lines else b""


def to_json(document: Mapping[str, Any]) -> bytes:
    return (json.dumps(document, sort_keys=True, indent=2, default=str) + "\n").encode("utf-8")


def count_records(name: str, body: bytes) -> int:
    if name.endswith(".jsonl"):
        return sum(1 for line in body.splitlines() if line.strip())
    return 1


def sha256(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


# -- Manifest --------------------------------------------------------------


@dataclass(frozen=True)
class DropManifest:
    business_date: date
    version: int
    files: Mapping[str, Mapping[str, Any]]  # name -> {sha256, bytes, records}
    published_at_sim: str
    supersedes: int | None = None
    revision_reason: str | None = None
    schema_version: str = SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset": DATASET,
            "business_date": self.business_date.isoformat(),
            "version": self.version,
            "supersedes": self.supersedes,
            "revision_reason": self.revision_reason,
            "published_at_sim": self.published_at_sim,
            "schema_version": self.schema_version,
            "files": {name: dict(meta) for name, meta in sorted(self.files.items())},
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DropManifest:
        try:
            return cls(
                business_date=date.fromisoformat(data["business_date"]),
                version=int(data["version"]),
                files={str(k): dict(v) for k, v in data["files"].items()},
                published_at_sim=str(data["published_at_sim"]),
                supersedes=data.get("supersedes"),
                revision_reason=data.get("revision_reason"),
                schema_version=str(data.get("schema_version", SCHEMA_VERSION)),
            )
        except (KeyError, TypeError, ValueError, AttributeError) as exc:
            raise ValueError(f"malformed manifest: {exc!r}") from exc


def build_manifest(
    business_date: date,
    version: int,
    files: Mapping[str, bytes],
    *,
    published_at_sim: str,
    supersedes: int | None = None,
    revision_reason: str | None = None,
) -> DropManifest:
    """Describe the files AS INTENDED. Checksums are taken before upload."""
    return DropManifest(
        business_date=business_date,
        version=version,
        files={
            name: {"sha256": sha256(body), "bytes": len(body), "records": count_records(name, body)}
            for name, body in files.items()
        },
        published_at_sim=published_at_sim,
        supersedes=supersedes,
        revision_reason=revision_reason,
    )


# -- Object-store I/O ------------------------------------------------------


def _content_type(name: str) -> str:
    return "application/x-ndjson" if name.endswith(".jsonl") else "application/json"


def upload_drop(
    client: Any,
    bucket: str,
    manifest: DropManifest,
    uploads: Mapping[str, bytes],
    root: str = "",
) -> str:
    """
    Upload the data files, THEN the manifest. `uploads` is normally the same
    content the manifest describes; a fault injector may pass damaged bytes
    to simulate corruption in transit.
    """
    prefix = drop_prefix(manifest.business_date, manifest.version, root)
    for name, body in uploads.items():
        storage.put_bytes(client, bucket, prefix + name, body, _content_type(name))
    storage.put_bytes(
        client, bucket, prefix + MANIFEST_FILE, to_json(manifest.to_dict()), "application/json"
    )
    return prefix


def versions(client: Any, bucket: str, day: date, root: str = "") -> list[int]:
    """Every version folder for a day, complete or not, ascending."""
    found = []
    for child in storage.list_child_prefixes(client, bucket, day_prefix(day, root)):
        match = _VERSION_RE.search(child)
        if match:
            found.append(int(match.group(1)))
    return sorted(found)


def latest_complete_version(client: Any, bucket: str, day: date, root: str = "") -> int | None:
    """Highest version with a manifest -- the only kind a reader may use."""
    for version in reversed(versions(client, bucket, day, root)):
        if storage.exists(client, bucket, drop_prefix(day, version, root) + MANIFEST_FILE):
            return version
    return None


def published_dates(client: Any, bucket: str, root: str = "") -> list[date]:
    found = []
    for child in storage.list_child_prefixes(client, bucket, f"{root}{DATASET}/"):
        match = _DATE_RE.search(child)
        if match:
            found.append(date.fromisoformat(match.group(1)))
    return sorted(found)


def ground_truth_dates(client: Any, bucket: str, root: str = "") -> list[date]:
    found = []
    for child in storage.list_child_prefixes(client, bucket, f"{root}{GROUND_TRUTH_PREFIX}/"):
        match = _DATE_RE.search(child)
        if match:
            found.append(date.fromisoformat(match.group(1)))
    return sorted(found)


@dataclass(frozen=True)
class LoadedDrop:
    business_date: date
    version: int
    manifest: dict[str, Any] | None  # raw document; None if absent
    files: dict[str, bytes]  # only the files that exist


def load_drop(client: Any, bucket: str, day: date, version: int, root: str = "") -> LoadedDrop:
    prefix = drop_prefix(day, version, root)
    manifest_bytes = storage.get_bytes(client, bucket, prefix + MANIFEST_FILE)
    try:
        manifest = json.loads(manifest_bytes) if manifest_bytes is not None else None
    except json.JSONDecodeError:
        manifest = {"_unparseable": True}
    files = {}
    for name in DATA_FILES:
        body = storage.get_bytes(client, bucket, prefix + name)
        if body is not None:
            files[name] = body
    return LoadedDrop(business_date=day, version=version, manifest=manifest, files=files)


def write_ground_truth(
    client: Any, bucket: str, day: date, version: int, truth: Mapping[str, Any], root: str = ""
) -> None:
    storage.put_bytes(
        client, bucket, ground_truth_key(day, version, root), to_json(truth), "application/json"
    )


def read_ground_truth(
    client: Any, bucket: str, day: date, version: int, root: str = ""
) -> dict[str, Any] | None:
    body = storage.get_bytes(client, bucket, ground_truth_key(day, version, root))
    return json.loads(body) if body is not None else None
