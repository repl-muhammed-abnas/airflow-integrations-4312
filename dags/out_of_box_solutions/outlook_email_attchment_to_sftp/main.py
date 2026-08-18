from datetime import timedelta, datetime
from airflow import DAG
from rail import PythonOperator, TriggerDagRunForEachItemOperator, result, dag
from rail.lib.alerts_email import send_dagrun_alert_email
from out_of_box_solutions.outlook_email_attchment_to_sftp.config import OUTLOOK_ATTACHMENT_TO_SFTP_ACCOUNT_DETAILS_VAR_NAME
from out_of_box_solutions.outlook_email_attchment_to_sftp.config import replicon_conn_id, max_active_runs_master

with DAG(
    dag_id="outlook_email_attachment_to_sftp",
    schedule="*/15 * * * *",
    start_date=datetime(2025, 1, 10),
    catchup=False,
    default_args={
        'owner': 'system',
        'replicon_conn_id': replicon_conn_id,
    },
    tags=["out_of_box_solutions" , "outlook_mail_attachment_to_sftp"],
    user_defined_macros=dag.get_macros(),
    user_defined_filters=dag.get_filters(),
    max_active_runs=max_active_runs_master,
    on_failure_callback=send_dagrun_alert_email,
) as dag:
    
    def get_company_list_details_from_airflow_variable_callable():
        from airflow.models import Variable
        account_details = Variable.get(OUTLOOK_ATTACHMENT_TO_SFTP_ACCOUNT_DETAILS_VAR_NAME, default_var=None, deserialize_json=True)
        if not account_details:
            raise Exception("No Data found in the Variable")
        
        account_details = account_details['mail_attachment_to_sftp_account_details']
        if not isinstance(account_details, list):
            raise Exception(f"Excepted list got {type(account_details)}")
        
        if None in [i['query'] if i['query'] else None for i in account_details]:
            raise Exception("`query` value is missing")
        
        return {
            "allowed_accounts":list(filter(lambda x: x['enabled'].lower() == 'true', account_details)),
            "not_allowed_accounts": list(filter(lambda x: x['enabled'].lower() != 'true', account_details))
        }

    get_company_list_details_from_airflow_variable = PythonOperator(
        task_id="get_company_list_details_from_airflow_variable",
        python_callable=get_company_list_details_from_airflow_variable_callable
    )

    def test_outlook_connection_callable():
        from out_of_box_solutions.outlook_email_attchment_to_sftp.outlook.OutlookConnection import OutlookConnection
        token_variables = list(set(map(lambda item: item['credentials_variable_name'],
                                   result("get_company_list_details_from_airflow_variable")['allowed_accounts'])))

        for token_variable in token_variables:
            OutlookConnection(token_variable)._test_connection()

        return token_variables

    test_outlook_connection = PythonOperator(
        task_id = "test_outlook_connection",
        python_callable= test_outlook_connection_callable
    )

    trigger_child_for_each_allowed_account = TriggerDagRunForEachItemOperator(
        task_id = "trigger_child_for_each_allowed_account",
        trigger_dag_id = "upload_outlook_mail_attachment_to_SFTP_process_each_account_child",
        items= lambda: result('get_company_list_details_from_airflow_variable')['allowed_accounts'],
        conf= lambda item: {
            "creds_variable_name": item['credentials_variable_name'],
            "shared_account_user_name": item['shared_account_user_name'],
            "outlook_query": item['query'],
            "allowed_formats": item['attachment_file_format'],
            "sftp_conn_id": item['sftp_conn_id'],
            "upload_file_path": item['upload_file_path'],
            "address_upload_file_path": item['address_upload_file_path'],
            "account": item['company_key'],
            "to_address": item['email_address'],
            "folder_name": item['folder'],
            "item": item
        },
        retries = 0,
        execution_timeout= timedelta(days=14)
    )

    get_company_list_details_from_airflow_variable >> test_outlook_connection >> trigger_child_for_each_allowed_account