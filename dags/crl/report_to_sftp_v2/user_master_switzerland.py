from datetime import timedelta
from pendulum import datetime
import rail
from crl.report_to_sftp_v2.utlis.python_callable import get_csv_filename


def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.user_master_switzerland,
        description=f'CharlesRiverLaboratories Switzerland User Report Export Master',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2024, 8, 1, tz=config.utc_time_zone),
        schedule_interval=config.schedule_interval_user_switzerland,
        max_active_runs=config.max_active_runs_master
    ) as dag:

        trigger_foreach_report = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_foreach_report',
            retries=0,
            items=config.USER_REPORT_SWITZERLAND,
            trigger_dag_id=config.user_child,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item:{
                'report_name': item,
                'filname': get_csv_filename(config.company_key, config.utc_time_zone, item, "User")
            }
        )

        wait_for_trigger_foreach_report = rail.WaitForDagRunsSensor(
            task_id='wait_for_trigger_foreach_report',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_foreach_report") }}'
        )

        trigger_foreach_report >> wait_for_trigger_foreach_report

    return dag


rail.for_each_instance(create_main_airflow_dag)
