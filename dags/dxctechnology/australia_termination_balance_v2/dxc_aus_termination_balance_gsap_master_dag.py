from datetime import timedelta
from pendulum import datetime
import rail
from dxctechnology.australia_termination_balance_v2.utils import request_payload


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"dxctechnology_aus_termination_balance_gsap_master_dag_{config.instance}_v2",
        description=f"DXC - AUS_termination_balance_GSAP_Master - {config.instance} V2",
        company_key=config.company_key,
         start_date=datetime(2022, 4, 1, tz=config.utc_timezone),
        schedule_interval=config.schedule_interval,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_dag_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        get_all_locations = rail.RepliconServiceOperator(
            task_id="get_all_locations",
            endpoint="/services/LocationService1.svc/GetAllLocations",
        )

        get_all_timeOffTypes = rail.RepliconServiceOperator(
            task_id="get_all_timeOffTypes",
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
        )

        process_termination_file_gsap = rail.TriggerDagRunForEachItemOperator(
            task_id='process_termination_file_gsap',
            retries=0,
            items=['1'],
            trigger_dag_id=f'dxctechnology_aus_terminationbalance_child_{config.instance}_v2',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: request_payload.terminationbalance_gsap_child_conf(
                config)
        )

        get_all_locations >> get_all_timeOffTypes >> process_termination_file_gsap

    return dag


rail.for_each_instance(create_main_dag)
