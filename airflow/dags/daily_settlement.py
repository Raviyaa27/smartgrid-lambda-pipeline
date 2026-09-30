"""
daily_settlement -- settle one simulated business date (ADR-0001, ADR-0006).

    wait_for_drop -> wait_for_archive -> quality_gate -> settle -> reconcile -> report

  wait_for_drop     the day's reference drop has a manifest (it may be LATE)
  wait_for_archive  the speed layer has archived past the end of the day
  quality_gate      integrity + validity checks; FAILS CLOSED, never retried
  settle            Spark job: re-validate, deduplicate, bill -- a new run
  reconcile         speed vs batch per zone: the measured approximation error
  report            the consolidated daily report, stored in MinIO

Triggered by sim_clock_tick with {"business_date": ...}. To RESTATE a day --
after a retroactive tariff revision or a late meter backfill -- trigger it
again for the same date with trigger="restatement" and a reason. The new
run's bills sit beside the old ones; the serving layer shows the latest.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import DAG, Param, task

from smartgrid.batch.orchestration import archive_has_passed, drop_is_complete

try:  # Airflow 3 moved the exception into the task SDK
    from airflow.sdk.exceptions import AirflowFailException
except ImportError:  # pragma: no cover
    from airflow.exceptions import AirflowFailException

with DAG(
    dag_id="daily_settlement",
    description="Settle one simulated day: gate, bill, reconcile, report",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,  # one Spark job at a time; later days queue
    params={
        "business_date": Param(
            "2026-01-01", type="string", format="date", description="Simulated date to settle"
        ),
        "trigger": Param("manual", enum=["scheduled", "restatement", "manual"]),
        "reason": Param("", type="string", description="Why, for a restatement"),
        # The simulation the run was triggered for. A run left over from
        # before a simulation reset must not write into the new simulation.
        "sim_id": Param(
            None, type=["null", "integer"], description="Blank means the current simulation"
        ),
    },
    default_args={"retries": 1, "retry_delay": timedelta(seconds=15)},
    tags=["smartgrid", "batch-layer"],
) as dag:
    # Sensors reschedule rather than hold a worker slot while they wait.
    # A LATE drop lands within ~6 simulated hours (~75 real seconds);
    # a MISSING one times out and fails the run -- which is what alerts.
    wait_for_drop = PythonSensor(
        task_id="wait_for_drop",
        python_callable=drop_is_complete,
        op_kwargs={"business_date": "{{ params.business_date }}"},
        poke_interval=10,
        timeout=150,
        mode="reschedule",
        retries=0,
    )
    wait_for_archive = PythonSensor(
        task_id="wait_for_archive",
        python_callable=archive_has_passed,
        op_kwargs={"business_date": "{{ params.business_date }}"},
        poke_interval=10,
        timeout=240,
        mode="reschedule",
        retries=0,
    )

    # Parameter names avoid `reason` and `run_id`: in Airflow 3.1 both are
    # task-context keys, and a task argument with either name is rejected.

    @task(retries=0)
    def quality_gate(business_date: str, airflow_run_id: str, sim_id: str) -> int:
        from smartgrid.batch.orchestration import (
            QualityGateFailed,
            StaleSimulation,
            run_quality_gate,
        )

        try:
            return run_quality_gate(business_date, airflow_run_id, sim_id=sim_id)
        except (QualityGateFailed, StaleSimulation) as exc:
            # Fail closed, and do NOT retry: a bad drop stays bad until it is
            # republished, and a retry would only repeat the same verdict.
            raise AirflowFailException(str(exc)) from exc

    @task
    def settle(
        business_date: str,
        drop_version: int,
        trigger: str,
        restatement_reason: str,
        airflow_run_id: str,
        sim_id: str,
    ) -> int:
        from smartgrid.batch.orchestration import SettlementRefused, run_settlement

        try:
            return run_settlement(
                business_date,
                drop_version,
                trigger,
                restatement_reason,
                airflow_run_id,
                sim_id=sim_id,
            )
        except SettlementRefused as exc:
            raise AirflowFailException(str(exc)) from exc

    @task
    def reconcile(settlement_run_id: int) -> list[dict]:
        from smartgrid.batch.orchestration import reconcile

        return reconcile(settlement_run_id)

    @task
    def report(settlement_run_id: int) -> str:
        from smartgrid.batch.orchestration import publish_report

        return publish_report(settlement_run_id)

    sim_id = "{{ params.sim_id or '' }}"
    version = quality_gate("{{ params.business_date }}", "{{ run_id }}", sim_id)
    settled = settle(
        "{{ params.business_date }}",
        version,
        "{{ params.trigger }}",
        "{{ params.reason }}",
        "{{ run_id }}",
        sim_id,
    )
    wait_for_drop >> wait_for_archive >> version
    reconcile(settled) >> report(settled)
