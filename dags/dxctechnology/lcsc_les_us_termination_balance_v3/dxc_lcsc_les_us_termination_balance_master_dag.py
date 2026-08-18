from datetime import timedelta
from pendulum import datetime
import rail
from dxctechnology.lcsc_les_us_termination_balance_v3 import request_payload


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"dxctechnology_lcsc_les_us_termination_balance_master_dag_v3_{config.instance}",
        description=f"DXC - LCSC_US_termination_balance_Master - V3.0 {config.instance}",
        company_key=config.company_key,
         start_date=datetime(2022, 4, 1, tz=config.eastern_timezone),
        schedule_interval=config.schedule_interval,
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

        process_termination_file_usa_les = rail.TriggerDagRunForEachItemOperator(
            task_id='process_termination_file_usa_les',
            retries=0,
            items=['1'],
            trigger_dag_id=f'dxctechnology_lcsc_les_terminationbalance_child_v3_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: request_payload.terminationbalance_usa_les_child_conf(
                config)
        )
        process_termination_file_usa_csc = rail.TriggerDagRunForEachItemOperator(
            task_id='process_termination_file_usa_csc',
            retries=0,
            items=['1'],
            trigger_dag_id=f'dxctechnology_lcsc_les_terminationbalance_child_v3_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: request_payload.terminationbalance_usa_csc_child_conf(
                config)
        )

    get_all_locations >> get_all_timeOffTypes >> process_termination_file_usa_csc
    get_all_timeOffTypes >> process_termination_file_usa_les
    return dag


rail.for_each_instance(create_main_dag)
