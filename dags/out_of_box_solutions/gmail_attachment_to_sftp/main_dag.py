from datetime import timedelta
import json
from pendulum import datetime
import rail
from rail.lib.alerts_email import send_dagrun_alert_email
import airflow
from airflow.models import Variable
from out_of_box_solutions.gmail_attachment_to_sftp.config import gmail_attachment_to_sftp_account_details, max_active_runs_master
from out_of_box_solutions.gmail_attachment_to_sftp.common_util.gmail_attachment_download import refresh_token

with airflow.DAG(
    dag_id='upload_gmail_attachments_to_sftp_master',
    description='Upload gmail attachments to SFTP Automation v1.0',
    start_date=datetime(2022, 4, 1, tz='UTC'),
    schedule=timedelta(minutes=15),
    tags= ['gmail_attachment_to_sftp'],
    user_defined_macros=rail.dag.get_macros(),
    user_defined_filters=rail.dag.get_filters(),
    max_active_runs=max_active_runs_master,
    on_failure_callback=send_dagrun_alert_email,
    catchup=False,
    default_args={
        'owner': 'system',
    },
    default_view="graph"
) as dag:

    def get_list_from_variable_callable():
        gmail_account_list= json.loads(Variable.get(gmail_attachment_to_sftp_account_details))
        gmail_account_list = gmail_account_list.get("gmail_to_sftp")

        if not gmail_account_list:
            raise Exception("No Data found in the Variable")

        if not isinstance(gmail_account_list, list):
            raise Exception(f"Excepted list got {type(gmail_account_list)}")

        # check added as integration filters mails by unique labels per client
        if None in [i['mail_label'] if i['mail_label'] else None for i in gmail_account_list]:
            raise Exception("`mail_label` value is missing")
        return {
            "allowed_accounts":list(filter(lambda x: x['enabled'].lower() == 'true', gmail_account_list)),
            "not_allowed_accounts": list(filter(lambda x: x['enabled'].lower() != 'true', gmail_account_list))
        }

    get_list_from_variable = rail.PythonOperator(
        task_id = "get_list_from_variable",
        python_callable= get_list_from_variable_callable
    )

    def refresh_all_tokens():
        token_variables = list(set(map(lambda item: item['credentials_variable_name'],
                                   rail.result("get_list_from_variable")['allowed_accounts'])))

        for token_variable in token_variables:
            refresh_token(token_variable)

        return token_variables

    refresh_tokens = rail.PythonOperator(
        task_id = "refresh_tokens",
        python_callable= refresh_all_tokens
    )

    trigger_child_for_each_allowed_account = rail.TriggerDagRunForEachItemOperator(
        task_id = "trigger_child_for_each_allowed_account",
        trigger_dag_id = "upload_gmail_attachment_to_SFTP_process_each_account_child",
        items= lambda: rail.result('get_list_from_variable')['allowed_accounts'],
        conf= lambda item: {
            "creds_variable_name": item['credentials_variable_name'],
            "gmail_query": f"is:unread label:{item['mail_label']}",
            "attachment_file_format": item['attachment_file_format'],
            "sftp_conn_id": item['sftp_conn_id'],
            "upload_file_path": item['upload_file_path'],
            "address_upload_file_path": item['address_upload_file_path'],
            "account": item['company_key']
        },
        retries = 0,
        execution_timeout= timedelta(days=14)
    )

    get_list_from_variable >> refresh_tokens >> trigger_child_for_each_allowed_account
