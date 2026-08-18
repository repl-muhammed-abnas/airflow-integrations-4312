from datetime import timedelta
from pendulum import datetime
import rail
from dxctechnology.csc_payroll_extract_v3 import request_payload
from dxctechnology.csc_payroll_extract_v3 import master_dag_task_group


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"dxctechnology_lcsc_payrollexport_master_dag_v3_{config.instance}",
        description=f"DXC_LCSC_PayrollExport_Master {config.instance}",
        company_key=config.company_key,
        schedule_interval=config.schedule_interval,
        start_date=datetime(2022, 4, 1, tz=config.eastern_timezone),
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
        max_active_runs=config.max_active_runs
    ) as dag:

        master_dag_task_group_entry, master_dag_task_group_exit = master_dag_task_group.get_master_dag_task_group(
            config.time, config.export, config.frequency, config.company_key)

        process_payrolldata_export = rail.TriggerDagRunOperator(
            task_id='process_payrolldata_export',
            retries=0,
            trigger_dag_id=f'dxctechnology_lcsc_location_company_codewise_payrolldata_export_child_v3_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: request_payload.process_payrolldata_export_conf(config.file_format)
        )

        master_dag_task_group_entry
        master_dag_task_group_exit>> process_payrolldata_export

    return dag


rail.for_each_instance(create_main_dag)
