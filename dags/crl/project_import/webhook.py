from datetime import timedelta, datetime as dt
from pendulum import datetime
from rail.lib.ecid import get_dagrun_ecid
import rail

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id= config.master_dag_id,
        description= "CRL Project Import Master",
        start_date= datetime(2023,9,1),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs = config.master_max_active_run,
        webhook_conf=[
            rail.WebhookConf(bearer_token_var=config.crl_project_import_bearer_token_variable)
        ],
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dag_run_conf")

        get_log_file_timestamp = rail.PythonOperator(
            task_id = 'get_log_file_timestamp',
            python_callable= lambda dag_run: get_dagrun_ecid(dag_run).split(":")[0] + '_' + dt.now().strftime('%m%d%YT%H%M%S')
        )

        process_projects = rail.TriggerDagRunOperator(
            task_id = 'process_projects',
            trigger_dag_id= config.projects_child_dag_id,
            conf= lambda dag_run: {
                    "project_data": dag_run.conf['webhook']['data'],
                    "master_ecid": get_dagrun_ecid(dag_run),
                    "log_filename": rail.result("get_log_file_timestamp")
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

        upload_input_data_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_input_data_sftp",
            content="{{ dag_run.conf.webhook.data }}",
            remote_filepath=config.input_filepath +
            "Payload_{{ result('get_log_file_timestamp') }}.json"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info= lambda dag_run: {
                'count_of_total_records': len(dag_run.conf['webhook']['data']['ProjectRecord'])
            }
        )

        get_log_file_timestamp >> process_projects >> upload_input_data_sftp >> log_to_sumo

    return dag

rail.for_each_instance(create_main_dag)
