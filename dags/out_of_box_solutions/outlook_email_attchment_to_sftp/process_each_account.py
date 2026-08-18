from datetime import datetime
import rail
from rail.lib.alerts_email import send_dagrun_alert_email
import airflow
from out_of_box_solutions.outlook_email_attchment_to_sftp.config import replicon_conn_id, max_active_runs_child
from out_of_box_solutions.outlook_email_attchment_to_sftp.utils.custom_methods import extract_attachments_from_outlook_email
with airflow.DAG(
    dag_id='upload_outlook_mail_attachment_to_SFTP_process_each_account_child',
    description='Outlook Email Attachment to SFTP process per account child v0.1',
    tags= ["out_of_box_solutions" , "outlook_mail_attachment_to_sftp"],
    schedule=None,
    start_date=datetime(2022, 1, 1),
    max_active_runs=max_active_runs_child,
    on_failure_callback=send_dagrun_alert_email,
    default_args={
        'owner': 'system',
        'replicon_conn_id': replicon_conn_id,
    },
    user_defined_macros=rail.dag.get_macros(),
    user_defined_filters=rail.dag.get_filters(),

) as dag:

    rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

    def get_dag_run_conf():
        return rail.get_current_context()['dag_run'].conf

    get_attachment_from_outlook_mail = rail.PythonOperator(
        task_id = "get_attachment_from_outlook_mail",
        python_callable = extract_attachments_from_outlook_email
    )

    has_any_files_to_upload = rail.IfOperator(
        task_id = "has_any_files_to_upload",
        test= "{{ result('get_attachment_from_outlook_mail').status_code == 200 }}",
        yes_task="for_each_attachment",
        no_task= "delete_this_dagrun"
    )

    delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
        task_id='delete_this_dagrun')

    for_each_attachment = rail.ForEachOperator(
        task_id = "for_each_attachment",
        items="{{result('get_attachment_from_outlook_mail').response.response_success | to_json }}",
        start_task= "get_time_stamp",
        end_task="for_each_end"
    )

    get_time_stamp = rail.PythonOperator(
        task_id = "get_time_stamp",
        python_callable= lambda dag_run :{
            "timestamp" : datetime.now().strftime('%Y%m%dT%H%M%S'),
            "address_file_name": rail.result('for_each_attachment')['response']['attachment_name'].replace(
                f".{rail.result('for_each_attachment')['response']['attachment_ext']}", '.txt')
        }
    )

    upload_attachment_to_sftp = rail.SFTPUploadFileOperator(
        task_id = "upload_attachment_to_sftp",
        content="{{result('for_each_attachment').response.attachment_content_artifact}}",
        sftp_conn_id="{{ dag_run.conf.sftp_conn_id }}",
        remote_filepath="{{ dag_run.conf.upload_file_path }}/{{ result('get_time_stamp').timestamp }}_{{result('for_each_attachment').response.attachment_name}}"
    )

    upload_address_file_to_sftp = rail.SFTPUploadFileOperator(
        task_id = "upload_address_file_to_sftp",
        content="{{result('for_each_attachment').sender_email}}",
        sftp_conn_id="{{ dag_run.conf.sftp_conn_id }}",
        remote_filepath="{{ dag_run.conf.address_upload_file_path }}/{{ result('get_time_stamp').timestamp }}_{{result('get_time_stamp').address_file_name}}"
    )

    for_each_end = rail.EmptyOperator(
        task_id = "for_each_end"
    )

    log_to_sumo = rail.DagRunLogToSumoOperator(
        task_id='log_to_sumo',
        trigger_rule="all_done",
        sumo_conn_id='sumologic-dagrunlogger',
        extra_info=lambda dag_run:{
                "account": dag_run.conf['account'],
                "query_used": dag_run.conf['outlook_query'],
                "attachments": rail.result('get_attachment_from_outlook_mail')
            }
        )

    get_attachment_from_outlook_mail >> has_any_files_to_upload >> rail.Label("Yes") >> for_each_attachment >> get_time_stamp >> upload_attachment_to_sftp\
        >> upload_address_file_to_sftp >> for_each_end
    has_any_files_to_upload >> rail.Label("No") >> delete_this_dagrun
    for_each_attachment >> for_each_end >> log_to_sumo