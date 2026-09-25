# ADR-0007: Simulated clock at 288x time compression

- **Status:** Accepted
- **Date:** 2026-09-20
- **Deciders:** Project team

## Context

The system's two data sources operate on different natural timescales: meter
readings arrive every few seconds, while the tariff and weather feeds arrive
once per day. Demonstrating the daily settlement cycle — and, more
importantly, demonstrating *restatement across several days* — is impossible
in real time within a demo session.

The assessment brief permits compressing simulated time and requires the
compression to be stated explicitly.

The design problem is not the compression ratio. It is deciding **which
notion of time each component uses**, because getting that wrong produces a
pipeline whose behaviour cannot be reproduced.

## Decision

**One simulated day = 300 real seconds (288× compression), implemented in a
single class, `src/smartgrid/common/clock.py`.**

Starting simulated date is `2026-01-01`, configured as `SIM_DAY_SECONDS` and
`SIM_START_DATE` in `.env`.

### The rule that matters

> **Nothing calls `datetime.now()` to decide what day it is.** Every
> component derives simulated time from `SimulatedClock`.

Real wall-clock time appears in exactly two places, both of which are about
the machine rather than the domain:

- `ingest_time`, stamped by the producer, used to measure producer lag;
- log record timestamps.

Everything domain-facing — `event_time`, the `dt=` partition key, the
settlement date, the billing date, window boundaries — is simulated time.

This separation is what makes the pipeline reproducible. Because the
partition key is derived from the simulated clock rather than the wall clock,
re-running the batch layer for `dt=2026-01-03` reads the same data and
produces the same bills regardless of when the re-run happens. If the
partition key were wall-clock derived, "recompute that day" would be
meaningless — which would break the restatement argument that
[ADR-0001](0001-lambda-over-kappa.md) rests on.

### Why 288×

| Ratio | 1 sim day | 7 sim days | Assessment |
|---|---|---|---|
| 1440× | 60 s | 7 min | Too fast — a settlement run would not finish before the next day rolls over |
| **288×** | **5 min** | **35 min** | Chosen: a full week fits a demo session, and settlement completes comfortably within a simulated day |
| 96× | 15 min | 1 h 45 m | Safe but too slow to show multi-day restatement live |

288× is the fastest ratio at which the batch settlement for day *N* reliably
completes before day *N+1* ends. Faster ratios would produce a backlog and
would be demonstrating a broken system rather than a compressed one.

### Consequence for the batch layer

A simulated day is shorter than Airflow's practical scheduling granularity,
so settlement is **triggered externally on simulated-day rollover** with the
simulated date passed as a DAG parameter, rather than driven by Airflow's own
schedule. The DAG remains date-parameterised, so backfill still works as
described in [ADR-0006](0006-airflow-for-orchestration.md).

## Alternatives considered

### Real time, no compression

Rejected. A single settlement cycle would take 24 hours and multi-day
restatement could not be demonstrated at all. The most interesting behaviour
in the system — a past day being recomputed and its figures changing from
provisional to settled — would be invisible.

### Pre-generate a fixed historical dataset and replay it

**What it gives.** Perfect determinism and fast iteration; a well-established
testing technique.

**Why rejected as the primary mechanism.** It removes the live-streaming
property the brief requires — a continuously emitting source. It also cannot
exercise the operational behaviour that matters here: consumer lag under
load, watermark handling of genuinely late arrivals, or a meter going silent
while the system is running. Determinism is instead achieved through the
seeded fleet (`domain.build_fleet`), which gives reproducibility without
giving up live emission.

### Scale down the data instead of the time

Reduce households and emission rate so a real day is cheap. Rejected because
it compresses the wrong dimension: the daily settlement cycle would still
take a real day, which is the thing that needs to be fast.

### Let each component read the wall clock and apply the ratio itself

Rejected outright. Components would drift apart, and the `dt=` partition key
would depend on which process computed it. Centralising in one class is the
point of the decision, not an implementation detail of it.

## Consequences

**Positive**

- A seven-day scenario, including multiple settlements and a restatement,
  fits in a 35-minute session.
- The batch layer is reproducible, because partition keys derive from
  simulated time.
- The compression ratio is a single configuration value, so the whole
  scenario can be slowed down for debugging or sped up for a demo without
  touching code.
- `SimulatedClock` is deterministic under test: every method accepts an
  injected `real_now`, so the clock's behaviour is unit-tested rather than
  assumed.

**Negative**

- Two notions of time coexist, and confusing them is an easy and damaging
  mistake — a wall-clock partition key would silently break replay.
- Windowing intervals in the speed layer are in simulated time, so a
  "one-minute" window is roughly 200 ms of real time. Latency measurements
  must state which clock they are in.
- Compressed time exaggerates throughput relative to a real deployment: 288
  simulated days' worth of readings pass in one real day. Performance figures
  are not directly comparable to production.

**Mitigations**

- The rule is stated once, here, and enforced by convention plus review:
  domain time is simulated, machine time is real.
- The report states the compression ratio wherever a latency or throughput
  figure appears, and names which clock each figure is measured in.
- `SimulatedClock.describe()` emits the ratio into the logs at startup, so
  any captured log or screenshot carries its own context.

## Revisit if

- Settlement stops completing within one simulated day — reduce the ratio;
  it is a single configuration change.
- The demo needs to show a longer horizon, such as a full billing month, which
  would need a higher ratio and a re-check of the settlement-completion
  constraint above.
- The system is ever deployed against real meters, at which point compression
  is removed entirely and `SimulatedClock` collapses to a pass-through over
  the wall clock.
