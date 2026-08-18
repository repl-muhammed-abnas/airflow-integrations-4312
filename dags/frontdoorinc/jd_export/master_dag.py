from datetime import timedelta
from pendulum import datetime
import rail
from frontdoorinc.jd_export.utils import python_callable

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'Frontdoorinc_JDEIntegration scheduler {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2023, 9, 1, tz=config.schedule_time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs_master,
    ) as dag:

        process_export_v3_dag_run = rail.TriggerDagRunOperator(
            task_id='process_export_v3_dag_run',
            trigger_dag_id=config.jd_export_child_dag_id,
            execution_timeout=timedelta(hours=config.execution_timeout_days),
            conf={
                "start_date":python_callable.get_dag_run_conf().get('start_date'),
                "end_date":python_callable.get_dag_run_conf().get('end_date'),
                "email": config.tenant_email
            }
        )

        wait_for_process_export_v3_dag_run = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_export_v3_dag_run',
            dag_runs='{{ result("process_export_v3_dag_run") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        process_export_v3_dag_run >> wait_for_process_export_v3_dag_run

        return dag

rail.for_each_instance(create_dag)
