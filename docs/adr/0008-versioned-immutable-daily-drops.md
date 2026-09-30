# ADR-0008: Daily reference drops are versioned, immutable and manifest-completed

- **Status:** Accepted
- **Date:** 2026-09-30
- **Deciders:** Project team

## Context

[ADR-0001](0001-lambda-over-kappa.md) chose Lambda largely because billing
must be **restatable**: a regulator backdates a tariff change, and every
affected bill must be recomputed. That argument has two unstated
prerequisites, and both were missing:

1. **The tariff must be data, not code.** The block rates were constants in
   `billing.py`. A backdated revision would have meant editing source code
   and redeploying -- and the recomputed bills could not then be compared
   with the originals, because the original tariff would no longer exist
   anywhere.
2. **The reference data must keep its history.** If a correction simply
   overwrites the day's file, nobody can later show what a bill was
   originally computed from. That is fatal for a disputed bill, which is
   exactly the case restatement exists to serve.

There is also an operational hazard. The batch source uploads several files
per day. A reader that starts while an upload is in progress sees a partial
drop, and a partial drop looks just like a small valid one.

## Decision

**Each business date's reference data is a versioned, immutable drop in the
raw landing zone, complete only when its manifest exists, and it carries the
day's full tariff schedule as data.**

```
raw/daily/dt=2026-01-03/v=1/tariff_schedule.json      the tariff in force: blocks, fixed
                                                       charges, export credit, subsidy
raw/daily/dt=2026-01-03/v=1/household_tariffs.jsonl   tier and subsidy per household
raw/daily/dt=2026-01-03/v=1/weather_forecast.jsonl    forecast per grid zone
raw/daily/dt=2026-01-03/v=1/_MANIFEST.json            written LAST
raw/daily/dt=2026-01-03/v=2/...                       a later correction, beside v=1
```

- **Tariff as data.** `TariffSchedule` expresses the whole pricing policy
  and serialises every amount as a string, so no rate ever passes through a
  float. `calculate_bill` takes the schedule it is given. Today's rates are
  the default schedule, so existing behaviour is unchanged.
- **Immutable and versioned.** Nothing is ever overwritten. A correction or
  a tariff revision is published as the next version, with a manifest
  recording which version it supersedes and why. Settlement reads the
  latest complete version; the earlier versions remain.
- **Complete only with a manifest.** Data files are uploaded first and the
  manifest last. A version with no manifest does not exist as far as any
  reader is concerned. The manifest records each file's SHA-256 and record
  count, so damage in transit is detectable.
- **Gated before use.** `common/drop_quality.py` checks integrity (manifest,
  checksums, counts) and then validity (schedule consistency, schema,
  known tiers, quoted rates, one record per household, a forecast per zone,
  correct dates). Settlement runs only on a drop that passes, and fails
  closed otherwise.

## Alternatives considered

### Overwrite the day's file in place

Simplest to read, since there is only ever one file. Rejected because it
destroys the original, and with it any possibility of showing what a
disputed bill was first computed from. It also makes the correction itself
invisible: afterwards, nothing records that a revision happened.

### A table format (Iceberg or Delta Lake) for the landing zone

Gives versioning, atomic commits and time travel natively, and would
replace both the version folders and the manifest convention. Rejected for
the same scope reason as in [ADR-0005](0005-parquet-master-dataset-postgres-serving.md),
where it is already recorded as the first production upgrade. The folder
and manifest convention here is deliberately the minimal form of the same
idea, so migrating means changing the storage, not the semantics.

### A "done" marker file instead of a manifest

The common `_SUCCESS` convention signals completion but proves nothing about
content. Rejected in favour of a manifest, which costs nothing extra to
write and also carries the checksums and record counts the quality gate
needs.

### Keep the tariff in code and version the code

Git history would record rate changes. Rejected because a rate change would
then need a deployment, and restating a past day would mean checking out
old code. Reference data belongs in the data plane.

## Consequences

**Positive**

- A retroactive tariff revision is a routine operation (`revise`), not a
  code change.
- Every bill can be recomputed under the exact tariff it was issued with,
  and the original and restated versions can be compared side by side.
- A half-uploaded drop can never be read.
- Transit corruption and source-data faults are caught separately, and
  with specific diagnostics.
- The recovery path for a bad drop is simply to republish it: a new
  version supersedes the bad one, and the bad one stays for the record.

**Negative**

- Storage grows with every revision. At the volumes described in ADR-0001
  a drop is small, so this is negligible, but it is unbounded.
- Readers must implement "latest complete version" correctly. Reading
  `v=1` directly, or reading a version without checking its manifest,
  would silently bypass both the corrections and the completeness rule.
- The manifest convention is ours, not a standard; other tools will not
  understand it.

**Mitigations**

- There is exactly one implementation of version resolution
  (`drops.latest_complete_version`), and the quality gate goes through it,
  so the batch layer does not reimplement it.
- A lifecycle policy expiring superseded versions after the dispute window
  would bound the storage growth; it is noted as future work.

## Revisit if

- The landing zone moves to Iceberg or Delta, whose snapshots replace the
  version folders and manifests.
- Revisions become frequent enough that per-version full copies are
  wasteful -- publish deltas instead.
- More than one publisher can write the same date. The next-version number
  is then a race, and publication needs a conditional write or a lock.
