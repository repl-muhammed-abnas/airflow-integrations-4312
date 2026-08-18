from datetime import timedelta
from pendulum import datetime
import rail
from dxctechnology.csc_les_us_payroll_extract_v4 import request_payload
from dxctechnology.csc_les_us_payroll_extract_v4 import master_dag_task_group
from dxctechnology.csc_les_us_payroll_extract_v4.mapper.company_code_mapper_usles_uscsc import COMPANY_CODE_MAP_US


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"dxctechnology_lcsc_les_us_payrollexport_19_15_master_dag_v4_{config.instance}",
        description=f"DXC_LCSC_PayrollExport_19:15:00_Master {config.instance}",
        company_key=config.company_key,
        start_date=datetime(2022, 4, 1, tz=config.eastern_timezone),
        schedule_interval=config.schedule_interval_19_15,
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
        max_active_runs=config.max_active_runs
    ) as dag:

        master_dag_task_group_entry, master_dag_task_group_exit = master_dag_task_group.get_master_dag_task_group(
            config.time_19_15, config.export, config.frequency, config.company_key, COMPANY_CODE_MAP_US)

        process_payrolldata_export = rail.TriggerDagRunForEachItemOperator(
            task_id='process_payrolldata_export',
            retries=0,
            items=lambda: rail.result('search_entries_companycode_mapper'),
            trigger_dag_id=f'dxctechnology_lcsc_location_company_codewise_usa_payrolldata_export_child_v4_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.process_payrolldata_export_us_conf
        )

        master_dag_task_group_entry
        master_dag_task_group_exit>> process_payrolldata_export

    return dag


rail.for_each_instance(create_main_dag)
