# Demo video: script and recording guide

The brief asks for a 5–10 minute video "showing the pipeline running
end-to-end and the observability results". This is the plan for a
**9-minute** video. It is recorded in one take of the one-command demo and
then cut to remove the waiting. The same script works for a live demo.

The video shows both sources and both layers, as well as the merge rule that
decides which layer answers. Two alerts fire and clear on camera: a zone outage
and a refused tariff drop. It also shows a restatement and the evidence that
every claim was checked.

## How it is made

1. Record one full run of `python scripts/demo.py --no-build`. It takes about
   13 minutes of real time.
2. Cut it to about 9 minutes by removing the waits. **The cuts only remove
   real time spent waiting.** They never change what the system shows. Mark
   each cut on screen with a caption such as *"1 minute later"*, so nobody
   mistakes an edit for the system being faster than it is.
3. Record the introduction (scene 1) separately, over the report's
   architecture figure, and put it first.

From the moment the simulation starts, the demo stamps its output with the
real time elapsed, e.g. `[+04:05]`. The timings below come from a
rehearsal on 2026-10-01 and will differ by a few seconds on your run. Use
the stamps in the terminal to find each moment when you edit.

## Before recording

- [ ] **Memory.** Close every application you do not need: chat apps, other
      browsers, IDEs. While a day is being settled, the stack needs about 6 GB,
      plus the recorder and one browser. `demo.py` warns below 4 GB free. A
      starved host slows settlement from 1.5 minutes to over 7.
- [ ] **Docker Desktop:** turn off automatic update downloads (Settings →
      Software updates). An update during a run once crashed the engine.
- [ ] **Images built**, never on camera: `docker compose build` (10–20
      minutes the first time).
- [ ] **Stack up and healthy:** `docker compose up -d`, then
      `python scripts/smoke_test.py` must pass 8 of 8.
- [ ] **One browser window, tabs in this order:**
      1. business dashboard: http://localhost:8501
      2. Grafana: http://localhost:3000. It opens on *Smart grid: pipeline
         operations*, showing the last 15 minutes and refreshing every 10 s.
      3. Airflow: http://localhost:8080. Open the `daily_settlement` DAG.
      4. Prometheus alerts: http://localhost:9090/alerts
      5. serving API docs: http://localhost:8000/docs
      6. the report, `docs/report/report.pdf`: Figures 1 and 2.
- [ ] **Browser zoom 110–125 %** so that text is readable at 1080p.
- [ ] **Terminal:** font 16 pt or larger, at least 100 columns wide, opened in
      the repository folder. Keep a second terminal tab for scene 8.
- [ ] **Recorder:** OBS Studio (free), or the Windows Snipping Tool's screen
      recording (Win+Shift+R). Use 1920×1080 at 30 fps, with the microphone on
      and system audio off. Record 10 seconds as a test and play it back.
- [ ] **Editor:** Clipchamp (built into Windows 11) or any editor that can
      trim and add a text caption.

## Timeline

| Scene | Real time in the run | What it shows | In the video |
|---|---|---|---|
| 1. Introduction | recorded separately | use case, the Lambda decision, architecture | 0:00–0:50 |
| 2. One command | start → +00:10 | the demo, the simulated clock, a scheduled outage | 0:50–1:30 |
| 3. Live data | +00:10 → +01:40 | speed layer: provisional zone figures within seconds | 1:30–2:40 |
| 4. Outage and alert | +01:40 → +04:45 | `ZoneSilent` fires for ZONE-C alone, then clears | 2:40–4:00 |
| 5. Day 1 settles | +05:00 → +07:00 | batch layer: Airflow, reconciliation, bills | 4:00–5:50 |
| 6. Restatement | +07:00 → +08:20 | a backdated tariff, the original bill kept | 5:50–7:00 |
| 7. Refused drop | +10:30 → +12:05 | quality gate fails closed, `DropRefused`, recovery | 7:00–8:20 |
| 8. Wrap-up | after the summary | 12 of 12 checks, structured logs | 8:20–9:10 |

Between +08:20 and +10:30 nothing new happens: day 2 is running, and its
drop is checked only once the day ends. Cut that stretch, or use it to show
the API docs or the MinIO console (http://localhost:9001, with the
`MINIO_ROOT_*` login from `.env`): the raw drops, and the Parquet archive
partitioned by date and zone.

Narration is written at about 130 words a minute. Each scene stands on its
own, so the narration can change hands at a scene boundary. Where a number
varies from run to run, the script says *"about"*: read the actual figure
from the screen.

---

## Scene 1. Introduction (0:00–0:50)

**On screen:** the report's title page, then Figure 1 (architecture).

**Say:**

> This is our EC8203 mini-project: smart grid energy monitoring and billing,
> built on a Lambda architecture. A utility asks two questions of the same
> smart-meter data. What is the grid load and solar contribution by zone
> right now? And what will each household's bill be, once the day's tariff
> is applied?
>
> The first question needs freshness. The second needs exactness: a bill is
> regulated, and it must be reproducible when a tariff changes after the
> fact. So we run two paths. A Spark streaming layer answers the first from
> Kafka in seconds. An Airflow-orchestrated Spark batch layer recomputes each
> day from an immutable Parquet archive to answer the second. A serving API
> decides, by an explicit rule, which layer answers each question.

## Scene 2. One command (0:50–1:30)

**On screen:** the terminal. Type and run:

```bash
python scripts/demo.py --no-build
```

Let phases 0–2 scroll past. **Cut** the `docker compose` start-up lines. Stop
on phase 2's lines: the simulation id, *"1 day = 300 real seconds"*, and
*"ZONE-C goes offline at +100 s for 150 s"*.

**Say:**

> Everything runs from one command, in Docker Compose: Kafka, Spark, Airflow,
> PostgreSQL, MinIO, the API, both dashboards, Prometheus and Grafana, and
> the two simulated sources. The demo starts a fresh simulation. Time is
> compressed 288 times, so one simulated day takes five real minutes. It has
> also scheduled an outage: in under two minutes, every meter in ZONE-C goes
> silent for two and a half minutes. As it goes, the demo checks each of its
> own claims against the running system.

## Scene 3. Live data (1:30–2:40), at about +00:10 to +01:40

**On screen:**
1. **Dashboard, Grid now.** Zones fill in, under an orange **PROVISIONAL**
   badge. Point at the freshness badge and the simulated clock in the header.
2. **Serving API docs.** Run `GET /api/v1/zones/live`: each zone's load,
   solar share, and age, with `"status": "PROVISIONAL"`.
3. **Grafana, top row.** *Real-time lag* is green and well under 60 s, and
   *Readings rejected* is at its baseline of about 0.7 %.

**Say:**

> A meter simulator publishes a reading from each of 200 households every
> two seconds, into Kafka. Spark Structured Streaming
> validates every message. It sends rejects to a dead-letter topic, and
> aggregates the rest into 15-minute windows per zone. Here is the result:
> grid load and solar share by zone, a few seconds behind the meters, against
> a 60-second target. Everything here is labelled provisional. Late readings
> can still change these numbers, so nothing on this page is ever billed.
>
> The API returns the same figures, each one carrying its status and the
> layer it came from. In Grafana, about 0.7 percent of readings are rejected:
> that is the simulator injecting faults on purpose, at a known rate.

## Scene 4. Outage and alert (2:40–4:00), at about +01:40 to +04:45

**On screen:**
1. **Dashboard, Grid now**, at about +01:40, when ZONE-C goes silent. Open
   *Table view* under the charts and point at ZONE-C's *Age (real s)*. It keeps
   rising, while the other zones stay at a few seconds.
2. **Prometheus alerts**, at about +03:10. `ZoneSilent` shows as *pending* for
   ZONE-C, which means the condition is true and the rule is waiting out its
   20 s `for` clause.
3. **Grafana**, at about +03:25. *Alerts firing* goes up by one, and the *Firing
   alerts* table names **ZoneSilent, zone ZONE-C**. The zone is still offline.
4. **Cut** to about +04:15, when the zone returns. The alert clears about
   35 s later.

**Say:**

> Now ZONE-C's meters go silent. Every component is still up, so a simple
> "is it running" check sees nothing wrong. The data from one zone has simply
> stopped: its age keeps climbing while the other zones stay fresh.
> Prometheus scrapes the pipeline every ten seconds. This rule fires when one
> zone's data is more than a minute old, for twenty seconds, while the stream
> itself is fresh.
>
> It fires for ZONE-C alone. Because the rule only applies while the stream is
> fresh, a stopped speed layer raises one alert, not six. The alert rules are
> code, and each one is unit-tested with promtool, including the cases where
> it must not fire.
>
> When the zone comes back, its meters upload the readings they missed, and
> the alert clears. But those readings arrive after the speed layer's
> watermark, so the real-time view never counts them. Keep that in mind for
> the next scene.

## Scene 5. Day 1 settles (4:00–5:50), at about +05:00 to +07:00

**On screen:**
1. **Airflow.** `sim_clock_tick` has triggered `daily_settlement` for
   2026-01-01. Open the run's graph: `wait_for_drop`, `wait_for_archive`,
   `quality_gate`, `settle`, `reconcile`, `report`, turning green. **Cut**
   while `settle` runs, which takes about 1.5 minutes. Open the `settle` task
   log and scroll to *"settlement timings"*.
2. **Dashboard, Zone history.** Day 1 is now **blue, SETTLED** (from the batch
   layer), and today is orange, provisional.
3. **Dashboard, Settlement.** The speed-versus-batch gap per zone: under about
   1 % in five zones, and about 60 % in ZONE-C, where about 2,300 late readings
   were recovered.
4. **Dashboard, Household bills.** Day 1 has a bill. Today has none, listed as
   *"day in progress"*.
5. Open the **daily report** from the Settlement page.

**Say:**

> Day 1 has ended. Once the grace period passes, Airflow settles it. It waits
> for the daily tariff drop and for the archive to pass midnight. It checks
> the drop's quality, and then a Spark job recomputes the whole day from the
> Parquet archive. That job uses the same validation code and the same window
> function as the streaming layer. Here there is no watermark, so nothing late
> is lost. It then joins the readings with each household's tariff tier and
> subsidy, and bills all 200 households in exact decimal arithmetic.
>
> The serving layer switches on its own: day 1 is now settled and comes from
> the batch layer, while today is still provisional. Reconciliation measures
> how far the real-time view was off. In five zones it was under one percent,
> and never above the settled figure. In ZONE-C it was more than half too low:
> those were the readings that came back after the outage. Settlement
> recovered them. That is exactly why bills come only from the batch layer.
>
> A bill exists for day 1. For today, the API refuses, and gives the reason:
> the day is in progress. There is never a provisional bill.

## Scene 6. Restatement (5:50–7:00), at about +07:00 to +08:20

**On screen:**
1. **Terminal**, phase 6: *"revision published as a new drop version;
   restatement triggered in Airflow"*.
2. **Airflow.** A second `daily_settlement` run for 2026-01-01, triggered as
   a restatement. **Cut** while it runs.
3. **Dashboard, Household bills** for a `DOMESTIC_STD` household:
   http://localhost:8501/bills?household=HH-00071. Under *Bill history*, its
   day 1 bill now has **two settlements**: the original under drop v1, and the
   restated one under drop v2, with the regulator's reason. The one marked
   *shown now* is the restated bill.
4. **Terminal:** `[PASS] Restatement changes only the revised tier`.

**Say:**

> A regulator now revises the standard domestic tariff for day 1,
> backdated: plus ten percent. The daily source publishes it as a new version
> of that day's drop, beside the original, never over it. Airflow restates
> the day by re-running the same job for that date.
>
> Only the revised tier's bills changed, and every other tier is identical to
> the cent. The original bills are still in the database beside the new ones,
> so the auditor's question, "what did we bill, and why did it change?", has
> an answer.

## Scene 7. Refused drop (7:00–8:20), at about +10:30 to +12:05

**On screen:**
1. **Airflow.** Day 2's run: `quality_gate` is **red**. Open its log: a
   checksum and record-count mismatch, because the drop was truncated in
   transit.
2. **Grafana.** *Days blocked* turns red at **1**, and **DropRefused** appears
   in *Firing alerts* (about +11:00).
3. **Terminal**, phase 7: *"drop republished as a new version; settlement
   re-triggered"*. **Cut** while it settles.
4. **Grafana.** The alert clears, and *Days blocked* returns to 0. On the
   dashboard, day 2 is settled.

**Say:**

> Day 2's tariff file arrives corrupt. The daily source does this on purpose,
> from its seed. The quality gate checks every file's checksum and record
> count against the drop's manifest, and it refuses the drop. It fails closed:
> no bill is written from bad reference data. The refusal raises an alert, and
> the dashboard shows the day as blocked.
>
> The provider republishes the drop as a new version. Settlement runs again,
> the gate passes, day 2 is billed, and the alert clears without anyone
> touching it.

## Scene 8. Wrap-up (8:20–9:10)

**On screen:**
1. **Terminal:** the summary. **12 of 12 checks passed.**
2. **Second terminal tab.** Run this in PowerShell (it filters out Spark's
   own log lines):
   ```powershell
   docker compose logs --tail 300 --no-log-prefix speed-layer | Select-String '"stage"' | Select-Object -Last 3
   ```
   In Git Bash, use `| grep '"stage"' | tail -3` instead. Each JSON line
   gives the service, the stage, records in, out and quarantined, the
   quarantine reasons, and the duration.
3. **Grafana,** the whole dashboard, scrolled once from top to bottom.

**Say:**

> The demo ends by listing every claim it checked: twelve of twelve passed,
> on the running system. Every service logs structured JSON with the same
> fields, every stage exports Prometheus metrics, and eight tested alert
> rules watch them.
>
> The report covers the trade-offs we accepted: the cost of running two
> processing paths, a single Kafka broker, and Spark in local mode. It also
> describes what we would change at production scale. Thank you.

---

## If something goes wrong on camera

- **A check fails:** keep recording. The summary names the failing check, and
  the stack keeps running so you can investigate. Restart from scene 2 with
  `python scripts/demo.py --no-build`, which begins a fresh simulation.
- **Settlement is slow** (over 3 minutes): the host is short of memory. Stop,
  close applications, and start again.
- **To record a shorter video:** `python scripts/demo.py --no-build --quick`
  stops after day 1 (scenes 1–5, about 6 minutes after cuts). It omits the
  restatement and the refused drop.
