"""
sim_clock_tick -- every real minute, settle each simulated day that has ended.

A simulated day lasts five real minutes, which Airflow's calendar cannot
express, so settlement is triggered from here with the simulated date as a
parameter (ADR-0006, ADR-0007). One minute of real time is 4.8 simulated
hours, so a day is picked up well within its first simulated morning.

The run id per day is deterministic and `skip_when_already_exists` is set,
so a tick that retries, or overlaps a slow one, cannot settle a day twice.
Triggered days are recorded only AFTER the triggers succeed: a failed tick
leaves the day to be picked up by the next one.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, task

with DAG(
    dag_id="sim_clock_tick",
    description="Trigger daily_settlement for every simulated day that has ended",
    schedule="* * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["smartgrid", "orchestration"],
) as dag:

    @task
    def due_settlements() -> list[dict]:
        from smartgrid.batch.orchestration import due_trigger_specs

        return due_trigger_specs()

    specs = due_settlements()

    triggered = TriggerDagRunOperator.partial(
        task_id="trigger_settlement",
        trigger_dag_id="daily_settlement",
        skip_when_already_exists=True,
        wait_for_completion=False,
    ).expand_kwargs(specs)

    @task
    def record(specs: list[dict]) -> int:
        from smartgrid.batch.orchestration import record_triggers

        return record_triggers(specs)

    triggered >> record(specs)
