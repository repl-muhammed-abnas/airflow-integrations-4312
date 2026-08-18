from datetime import timedelta
from pendulum import datetime
import rail
from dxctechnology.australia_payroll_extract.utils import request_payload
from dxctechnology.australia_payroll_extract.utils import python_callable_method

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"dxctechnology_australia_payrollexport_gsap_user_schedule_master_dag_{config.instance}",
        description=f"DXC_AUS_PayrollExport_GSAP_User_Schedule_Master {config.instance}",
        company_key=config.company_key,
        start_date=datetime(2022, 4, 1, tz=config.utc_timezone),
        schedule_interval=config.schedule_interval_user_schedule,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs
    ) as dag:

        is_valid_schedule = rail.IfOperator(
            task_id= 'is_valid_schedule',
            test= python_callable_method.check_schedule_for_user_schedule,
            yes_task= 'process_user_schedule_payrolldata_export',
            no_task= 'delete_this_dagrun'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id= 'delete_this_dagrun'
        )

        process_user_schedule_payrolldata_export = rail.TriggerDagRunOperator(
            task_id='process_user_schedule_payrolldata_export',
            retries=0,
            trigger_dag_id=f'dxctechnology_australia_payrolldata_export_user_schedule_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda:  request_payload.process_gsap_user_schedule_conf(config)
        )

        finish_export = rail.EmptyOperator(
            task_id= 'finish_export'
        )

        is_valid_schedule >> rail.Label(
            "Yes") >> process_user_schedule_payrolldata_export >> finish_export

        is_valid_schedule >> rail.Label(
            "No") >> delete_this_dagrun

    return dag


rail.for_each_instance(create_main_dag)
