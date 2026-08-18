from datetime import timedelta
from pendulum import datetime
import rail
from dxctechnology.lcsc_us_termination_balance import request_payload


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"dxctechnology_lcsc_us_termination_balance_master_dag_{config.instance}",
        description=f"DXC - LCSC_US_termination_balance_Master - V2.0 {config.instance}",
        company_key=config.company_key,
        schedule_interval=config.schedule_interval,
        start_date=datetime(2022, 4, 1, tz=config.eastern_timezone),
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
        max_active_runs=1
    ) as dag:

        get_all_locations = rail.RepliconServiceOperator(
            task_id="get_all_locations",
            endpoint="/services/LocationService1.svc/GetAllLocations",
        )

        get_all_timeOffTypes = rail.RepliconServiceOperator(
            task_id="get_all_timeOffTypes",
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
        )

        process_termination_file_canada = rail.TriggerDagRunForEachItemOperator(
            task_id='process_termination_file_canada',
            retries=0,
            items=['1'],
            trigger_dag_id=f'dxctechnology_lcsc_terminationbalance_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: request_payload.terminationbalance_canada_child_conf(
                config)
        )


    get_all_locations >> get_all_timeOffTypes >> process_termination_file_canada
    return dag


rail.for_each_instance(create_main_dag)
