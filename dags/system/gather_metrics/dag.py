"""
Replicon dag to pull additional metrics we want from the airflow system and export them via the statsd interface
"""
from datetime import datetime, timedelta

import airflow
from sqlalchemy import func, not_
import rail
from airflow.models import DagModel, DagRun, TaskInstance
from airflow.stats import Stats
from airflow.operators.python import PythonOperator
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.session import NEW_SESSION, provide_session

with airflow.DAG(
    dag_id="system_gather_metrics",
    schedule=timedelta(minutes=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['system_maintenance'],
    is_paused_upon_creation=False,
    default_args={
        'owner': 'system',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
    },
    default_view="graph",
    max_active_runs=1,
    doc_md=__doc__
) as dag:

    @provide_session
    def gather_dag_metrics(session=NEW_SESSION):
        dags = (
            session.query(DagModel.dag_id, func.count(DagRun.id))
            .select_from(DagModel)
            .filter(DagModel.is_active, not_(DagModel.is_paused))
            # pylint: disable=comparison-with-callable
            .outerjoin(DagRun, (DagModel.dag_id == DagRun.dag_id) & (DagRun.state == DagRunState.QUEUED))
            .group_by(DagModel.dag_id)
            .all()
        )

        for dag_id, count in dags:
            print(f"{dag_id}: {count}")
            Stats.gauge(f"global.dagrun.queued.{dag_id}", count)

    @provide_session
    def gather_ti_metrics(session=NEW_SESSION):
        states = [TaskInstanceState.SCHEDULED, TaskInstanceState.QUEUED,
                  TaskInstanceState.RUNNING, TaskInstanceState.DEFERRED]
        tis = (
            session.query(func.count(), TaskInstance.state)
            .filter(TaskInstance.state.in_(states))
            .join(TaskInstance.dag_run)
            # pylint: disable=comparison-with-callable
            .filter(DagRun.state == DagRunState.RUNNING)
            .join(TaskInstance.dag_model)
            .filter(not_(DagModel.is_paused), DagModel.is_active)
            .group_by(TaskInstance.state).all()
        )
        by_state = {state: count for count, state in tis}
        # subtract this running task from the total
        by_state[TaskInstanceState.RUNNING] = max(
            0, by_state.get(TaskInstanceState.RUNNING, 0) - 1)
        for state in states:
            Stats.gauge(f"global.taskinstance.{state}", by_state.get(state, 0))

    # pylint: disable=unused-argument
    def gather_metrics(**context):
        gather_dag_metrics()
        gather_ti_metrics()

    get_metrics = PythonOperator(
        task_id="get_metrics",
        priority_weight=10,
        python_callable=gather_metrics,
    )
    delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
        priority_weight=10,
        task_id='delete_this_dagrun')
    get_metrics >> delete_this_dagrun
