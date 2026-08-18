from datetime import timedelta
import rail
from dxctechnology.australia_payroll_extract_v3.utils import request_payload
from dxctechnology.australia_payroll_extract_v3.tasks import master_dag_task_group
from dxctechnology.australia_payroll_extract_v3.mapper.company_code_mapper_usles_uscsc import COMPANY_CODE_MAP_ES_WEEKLY

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"dxctechnology_australia_payrollexport_es_2010_2001_weekly_master_v3_{config.instance}",
        description=f"DXC_AUS_PayrollExport_ES_2010_2001_Weekly_Master V3 {config.instance}",
        company_key=config.company_key,
        schedule_interval=config.schedule_interval_es_weekly,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs
    ) as dag:

        master_dag_task_group_entry, master_dag_task_group_exit = master_dag_task_group.get_master_dag_task_group(
            'ES_WEEKLY', config.export, config.company_key, COMPANY_CODE_MAP_ES_WEEKLY)

        process_payrolldata_export = rail.TriggerDagRunForEachItemOperator(
            task_id='process_payrolldata_export',
            retries=0,
            items=lambda: rail.result('search_entries_companycode_mapper'),
            trigger_dag_id=f'dxctechnology_australia_payrolldata_export_es_child_v3_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.process_payrolldata_export_es_weekly_conf
        )

        master_dag_task_group_entry
        master_dag_task_group_exit >> process_payrolldata_export

    return dag


rail.for_each_instance(create_main_dag)
