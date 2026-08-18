from datetime import timedelta
from pendulum import datetime
import pendulum
from capgemini.shift_schedule_extract_to_TETA.utils.custom_methods import get_shifts_date_range_json
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'Shift Schedule Extract to TETA - Capgemini Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2024, 5, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        logging_details = rail.PythonOperator(
            task_id='logging_details',
            python_callable=lambda: pendulum.now(config.time_zone).strftime('%Y-%m-%dT%H:%M:%S.%f%z')
        )

        create_export_for_each_month = rail.TriggerDagRunForEachItemOperator(
            task_id='create_export_for_each_month',
            items=lambda: get_shifts_date_range_json(config.time_zone, config.no_of_months_shift_data_to_export,
                config.current_month_filename_prefix, config.future_months_filename_prefix),
            trigger_dag_id=config.export_child_dag_id,
            conf=lambda item: {
                **dict(item),
                "process_start_time": rail.result("logging_details")
            },
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        logging_details >> create_export_for_each_month

    return dag

rail.for_each_instance(create_dag)
