from datetime import timedelta
from pendulum import datetime
import pendulum
import rail
from crl.termination_balance_export_v3.utils import request_payload


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"crl_termination_balance_master_dag_{config.instance}v3",
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
        
        run_dag_on_payrollcalendar = rail.IfOperator(
            task_id="run_dag_on_payrollcalendar",
            # A manual "Trigger DAG w/ config" (any conf) skips the payroll-calendar
            # gate and runs immediately, so QA can test on demand (as in the UK DAG).
            test=lambda dag_run: not bool(dag_run.conf) and config.run_dag_payroll,
            yes_task='can_process_run',
            no_task='get_all_locations'
        )
        
        def can_process_run_test():
            current_date = pendulum.now(config.time_zone).strftime("%d-%m-%Y")
            current_hour = int(pendulum.now(config.time_zone).strftime("%H"))
            matched_payroll_period = rail.find_first_by_attr_and_get_attr(
                config.CANADA_PAYROLL_CALENDER_MAPPER_TO_USE, "payroll_processing_date", current_date)
            return bool(
                matched_payroll_period and
                matched_payroll_period.get("processing_time") == current_hour
            )

        can_process_run = rail.IfOperator(
            task_id = "can_process_run",
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

        process_termination_file_gsap = rail.TriggerDagRunForEachItemOperator(
            task_id='process_termination_file_gsap',
            retries=0,
            items=['1'],
            trigger_dag_id=f'crl_terminationbalance_child_{config.instance}v3',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: request_payload.terminationbalance_gsap_child_conf(
                config, dag_run)
        )

        run_dag_on_payrollcalendar >> rail.Label("Yes") >> can_process_run >> get_all_locations >> get_all_timeOffTypes >> process_termination_file_gsap

        run_dag_on_payrollcalendar >> rail.Label("No") >> get_all_locations

    return dag


rail.for_each_instance(create_main_dag)
