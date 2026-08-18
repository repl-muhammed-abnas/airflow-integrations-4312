from datetime import timedelta
from pendulum import datetime
import rail
from dxctechnology.australia_payroll_extract_v1.utils import request_payload
from dxctechnology.australia_payroll_extract_v1.utils import python_callable_method

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"dxctechnology_australia_payrollexport_active_user_master_dag_v1_{config.instance}",
        description=f"DXC_AUS_PayrollExport_Active_User_Master V1 {config.instance}",
        company_key=config.company_key,
        start_date=datetime(2022, 4, 1, tz=config.utc_timezone),
        schedule_interval=config.schedule_interval_active_users,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs
    ) as dag:

        is_valid_schedule = rail.IfOperator(
            task_id= 'is_valid_schedule',
            test= python_callable_method.check_schedule,
            yes_task= 'process_es_active_user_payrolldata_export',
            no_task= 'delete_this_dagrun'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id= 'delete_this_dagrun'
        )

        process_es_active_user_payrolldata_export = rail.TriggerDagRunOperator(
            task_id='process_es_active_user_payrolldata_export',
            retries=0,
            trigger_dag_id=f'dxctechnology_australia_payrolldata_export_active_user_child_v1_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: request_payload.process_active_es_user_conf(config)
        )

        check_weekday_for_gsap= rail.IfOperator(
            task_id= 'check_weekday_for_gsap',
            test= python_callable_method.check_week_day,
            yes_task= 'process_gsap_active_user_payrolldata_export',
            no_task= 'finish_export'
        )

        process_gsap_active_user_payrolldata_export = rail.TriggerDagRunOperator(
            task_id='process_gsap_active_user_payrolldata_export',
            retries=0,
            trigger_dag_id=f'dxctechnology_australia_payrolldata_export_active_user_child_v1_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda:  request_payload.process_active_gsap_user_conf(config)
        )

        finish_export = rail.EmptyOperator(
            task_id= 'finish_export'
        )

        is_valid_schedule >> rail.Label(
            "Yes") >> process_es_active_user_payrolldata_export

        is_valid_schedule >> rail.Label(
            "No") >> delete_this_dagrun

        process_es_active_user_payrolldata_export >> check_weekday_for_gsap

        check_weekday_for_gsap >> rail.Label(
            "Yes") >> process_gsap_active_user_payrolldata_export >> finish_export

        check_weekday_for_gsap >> rail.Label(
            "No") >> finish_export

    return dag


rail.for_each_instance(create_main_dag)
