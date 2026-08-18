from pendulum import datetime
import pendulum
import rail
from crl.reg_to_temp_payroll_export_us.utils import request_payload


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f"CRL - termination_balance_Master - {config.instance}",
        company_key=config.company_key,
        start_date=datetime(2022, 4, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_dag_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:
        
        def can_process_run_test():
            current_date = pendulum.now(config.time_zone).strftime("%d-%m-%Y")
            curent_hours = int(pendulum.now(config.time_zone).strftime("%H"))
            return bool(list(filter(lambda calendar_mapper: calendar_mapper["payroll_processing_date"] == current_date and calendar_mapper["processing_time"] == curent_hours, config.USA_PAYROLL_CALENDAR)))

        can_process_run = rail.IfOperator(
            task_id="can_process_run",
            test=can_process_run_test,
            yes_task="get_all_locations"
        )

        get_all_locations = rail.RepliconServiceOperator(
            task_id="get_all_locations",
            endpoint="/services/LocationService1.svc/GetAllLocations",
        )

        get_all_timeOffTypes = rail.RepliconServiceOperator(
            task_id="get_all_timeOffTypes",
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
        )

        process_reg_to_temp_file_gsap = rail.TriggerDagRunOperator(
            task_id='process_reg_to_temp_file_gsap',
            trigger_dag_id=config.child_dag_id,
            conf=lambda: request_payload.reg_to_temp_gsap_child_conf(
                config)
        )

        can_process_run >> get_all_locations >> get_all_timeOffTypes >> process_reg_to_temp_file_gsap

    return dag


rail.for_each_instance(create_main_dag)
