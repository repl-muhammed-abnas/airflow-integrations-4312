from datetime import timedelta
from pendulum import datetime
import rail
from dxctechnology.australia_payroll_extract_v1.utils import request_payload
from dxctechnology.australia_payroll_extract_v1.tasks import master_dag_task_group
from dxctechnology.australia_payroll_extract_v1.mapper.company_code_mapper_usles_uscsc import COMPANY_CODE_MAP_GSAP


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"dxctechnology_australia_payrollexport_gsap_master_dag_v1_{config.instance}",
        description=f"DXC_AUS_PayrollExport_GSAP_Master V1 {config.instance}",
        company_key=config.company_key,
        start_date=datetime(2022, 4, 1, tz=config.utc_timezone),
        schedule_interval=config.schedule_interval_gsap,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs
    ) as dag:

        master_dag_task_group_entry, master_dag_task_group_exit = master_dag_task_group.get_master_dag_task_group(
            config.gsap_region, config.export, config.company_key, COMPANY_CODE_MAP_GSAP)

        process_payrolldata_export = rail.TriggerDagRunForEachItemOperator(
            task_id='process_payrolldata_export',
            retries=0,
            items=lambda: rail.result('search_entries_companycode_mapper'),
            trigger_dag_id=f'dxctechnology_australia_payrolldata_export_gsap_child_v1_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.process_payrolldata_export_gsap_conf
        )

        process_active_user = rail.TriggerDagRunOperator(
            task_id='process_active_user',
            retries=0,
            trigger_dag_id=f'dxctechnology_australia_payrolldata_export_active_user_child_v1_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda:  request_payload.process_active_gsap_user_conf(config)
        )

        # pylint: disable=unnecessary-lambda
        process_cashout_annual_payroll_export= rail.TriggerDagRunForEachItemOperator(
            task_id='process_cashout_annual_payroll_export',
            retries=0,
            items=lambda: rail.result('search_entries_companycode_mapper'),
            trigger_dag_id=f'dxctechnology_australia_payrolldata_export_sellback_child_v1_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.process_cashout_gsap_user_conf
        )

        master_dag_task_group_entry
        master_dag_task_group_exit>> process_payrolldata_export >> process_active_user >> process_cashout_annual_payroll_export

    return dag


rail.for_each_instance(create_main_dag)
