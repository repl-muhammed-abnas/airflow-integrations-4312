from datetime import timedelta,datetime as dt
from airflow.models import Variable
from rail.lib.ecid import get_dagrun_ecid
from pendulum import datetime
import rail


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.webhook_master_dagid,
        description="CRL User Import Webhook Master",
        start_date=datetime(2023, 12, 1),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_run_wehook_master,
        webhook_conf=[
            rail.WebhookConf(
                bearer_token_var=config.crl_user_import_bearer_token_var)
        ]
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run_conf")

        get_standard_file_name = rail.PythonOperator(
            task_id = 'get_standard_file_name',
            python_callable= lambda dag_run: f'{get_dagrun_ecid(dag_run).split(":")[0]}_{dt.now().strftime("%Y%m%dT%H%M%S")}'
        )

        trigger_user_import_processing_dag = rail.TriggerDagRunOperator(
            task_id="trigger_user_import_processing_dag",
            trigger_dag_id=config.process_split_country_wise_data_dagid,
            conf=lambda dag_run: {
                "payload": dag_run.conf['webhook']['data']['User_Record'],
                "log_filename": "log_"+ str(rail.result('get_standard_file_name')),
                "uploaded_payload_filename": "payload_"+ rail.result("get_standard_file_name")+".json"
            },
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        upload_input_data_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_input_data_sftp",
            sftp_conn_id=config.sftp_conn_id,
            content="{{ dag_run.conf.webhook.data }}",
            remote_filepath=config.payload_filepath +
            "/payload_{{result('get_standard_file_name') }}.json"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info=lambda dag_run:{
                "count_of_user_records": len(dag_run.conf['webhook']['data']['User_Record']),
                "uploaded_payload_filename": "payload_"+ rail.result("get_standard_file_name")+".json"
            }
        )

        get_standard_file_name >> trigger_user_import_processing_dag >> upload_input_data_sftp >> log_to_sumo

    return dag

rail.for_each_instance(create_main_dag)
