import json
from datetime import timedelta, datetime
import airflow
from airflow.models import Variable

import rail
from system.sftp_monitoring_alerts import config

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/system/sftp_monitoring_alerts/config.py

with airflow.DAG(
    dag_id='system_sftp_monitoring_alerts_master',
    description='System SFTP Monitoring alerts Master v0.1',
    schedule=timedelta(hours=3),
    catchup=False,
    start_date=datetime(2022, 1, 1),
    tags=['system'],
    user_defined_macros=rail.dag.get_macros(),
    user_defined_filters=rail.dag.get_filters(),
    default_args={
        'owner': 'system',
    },
) as dag:

    get_dag_config = rail.PythonOperator(
        task_id='get_dag_config',
        python_callable=lambda: {**config.default_dag_config, **json.loads(
            Variable.get(config.dag_config_var_name, default_var='{}'))}
    )

    process_sftp_alerts = rail.TriggerDagRunForEachItemOperator(
        task_id='process_sftp_alerts',
        retries=0,
        items=lambda: rail.result('get_dag_config')['sftp_monitoring_list'],
        trigger_dag_id=config.sftp_alert_child_dag_id,
        execution_timeout=timedelta(days=1),
        conf=lambda item: {
            "paths": item['paths'],
            "company_key": item['company_key'],
            "sftp_conn_id": item['sftp_conn_id'],
            "sftp_file_count_threshold": item['sftp_file_count_threshold'],
            "sftp_file_hours_threshold": item['sftp_file_hours_threshold'],
            "alert_email": rail.result('get_dag_config')['alert_email']
        }
    )

    wait_for_process_sftp_alerts = rail.WaitForDagRunsSensor(
        task_id='wait_for_process_sftp_alerts',
        execution_timeout=timedelta(days=14),
        dag_runs='{{ result("process_sftp_alerts") }}'
    )

    finish = rail.EmptyOperator(
        task_id='finish'
    )

    get_dag_config >> process_sftp_alerts >> wait_for_process_sftp_alerts >> finish
