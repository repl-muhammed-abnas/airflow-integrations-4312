import json
from datetime import timedelta, datetime
import airflow
import rail
from airflow.models import Variable
from system.check_twb_draft_status import config

with airflow.DAG(
    dag_id='system_timeworkbench_draft_status_monitor_master',
    description='System Time Workbench Draft Status monitoring alerts Master v0.1',
    schedule=config.schedule_interval,
    catchup=False,
    tags=['system'],
    start_date=datetime(2022, 1, 1),
    user_defined_macros=rail.dag.get_macros(),
    user_defined_filters=rail.dag.get_filters(),
    default_args={
        'owner': 'system',
    }
) as dag:

    start = rail.EmptyOperator(
        task_id="start"
    )

    get_dag_config = rail.PythonOperator(
        task_id='get_dag_config',
        python_callable=lambda: json.loads(Variable.get(
            config.dag_config_var_name, default_var='{}'))
    )

    process_draft_alert = rail.TriggerDagRunForEachItemOperator(
        task_id="process_draft_alert",
        retries=0,
        items=lambda: rail.result('get_dag_config')[
            'TWB_draft_export_monitoring_list'],
        trigger_dag_id=config.draft_status_alert_child_dag_id,
        execution_timeout=timedelta(days=1),
        conf=lambda item: {
            "company_key": item['company_key'],
            "connection_id": item['conn_id']
        }
    )

    wait_for_process_draft_status = rail.WaitForDagRunsSensor(
        task_id='wait_for_process_draft_status',
        dag_runs='{{ result("process_draft_alert") }}',
        execution_timeout=timedelta(days=14)
    )

    finish = rail.EmptyOperator(
        task_id='finish'
    )

    start >> get_dag_config >> process_draft_alert >> wait_for_process_draft_status >> finish
