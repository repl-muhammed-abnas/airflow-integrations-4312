from datetime import timedelta
from pendulum import datetime
import rail
from dxctechnology.lcsc_les_uk_ireland_termination_balance_v1.utils import request_payload, response_filter

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f"DXC - LCSC_LES_UK_Ireland_termination_balance_Master {config.instance}",
        company_key=config.company_key,
        start_date=datetime(2025, 6, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
        max_active_runs=1
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='logging_details',
            end_task='process_termination_file',
        )

        logging_details = rail.PythonOperator(
            task_id='logging_details',
            python_callable=request_payload.get_logging_details,
            op_args=[config.time_zone, config.date_time_format, config.duration_days]
        )

        get_specific_locations = rail.RepliconServiceOperator(
            task_id="get_specific_locations",
            endpoint="/services/LocationService1.svc/GetAllLocations",
            data_handler=lambda response: response_filter.get_specific_locations(
                response, config.termination_balance_req_data
            )
        )

        get_specific_timeoff_types = rail.RepliconServiceOperator(
            task_id="get_specific_timeoff_types",
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
            data_handler=lambda response: response_filter.get_specific_timeoff_types(
                response, config.termination_balance_req_data
            )
        )

        process_termination_file = rail.TriggerDagRunForEachItemOperator(
            task_id='process_termination_file',
            retries=0,
            items=config.termination_balance_req_data,
            trigger_dag_id=config.process_termination_balance_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: request_payload.get_location_company_data_conf(config, item)
        )

        batch_task >> process_termination_file
        batch_task >> logging_details >> get_specific_locations >> get_specific_timeoff_types >> process_termination_file

    return dag

rail.for_each_instance(create_main_dag)
