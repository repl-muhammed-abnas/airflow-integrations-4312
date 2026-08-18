from datetime import timedelta
import rail
from dxctechnology.australia_payroll_extract_v3.utils import request_payload

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"dxctechnology_australia_payrollexport_es_0007_weekly_master_v3_{config.instance}",
        description=f"DXC_AUS_PayrollExport_ES_0007_Weekly_Master V3 {config.instance}",
        company_key=config.company_key,
        schedule_interval=config.schedule_interval_es_0007_weekly,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs
    ) as dag:

        process_user_schedule_payrolldata_export = rail.TriggerDagRunOperator(
            task_id='process_user_schedule_payrolldata_export',
            retries=0,
            trigger_dag_id=f'dxctechnology_australia_payrolldata_export_user_schedule_child_v3_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: request_payload.process_es_user_schedule_conf(config)
        )

    return dag


rail.for_each_instance(create_main_dag)
