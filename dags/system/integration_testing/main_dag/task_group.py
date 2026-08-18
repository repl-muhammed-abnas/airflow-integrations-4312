from datetime import timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from rail import TaskGroup
from system.integration_testing.config import dag_operator_mapper, execution_timeout_hours


def main_dag_task_group():

    with TaskGroup(group_id="trigger_dag_by_operator_type", prefix_group_id=False) as main_dag_task:

        for dag_operator in dag_operator_mapper:

            trigger_dag = TriggerDagRunOperator(
                task_id=f"trigger_dag_{dag_operator['category']}_operator_type",
                retries=0,
                wait_for_completion=True,
                poke_interval=30,
                execution_timeout=timedelta(
                    hours=execution_timeout_hours),
                trigger_dag_id=dag_operator['dag_id'],
                conf=dag_operator['conf']
            )

            trigger_dag

    return main_dag_task
