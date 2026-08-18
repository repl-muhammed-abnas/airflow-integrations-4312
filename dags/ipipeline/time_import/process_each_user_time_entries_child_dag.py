"""
iPipeline User Time Entry Processing - Child DAG 2
Processes time entries per user, validates data, groups by timesheet period,
and triggers sub-child DAGs for Replicon entry creation
"""

from datetime import timedelta
from airflow.models import Variable
from ipipeline.time_import.utils import request_payload, custom_methods
import rail

null = None
OPEN_BRACKETS = '{{'
CLOSE_BRACKETS = '}}'


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_each_user_time_entries_child_dag_id,
        description=f"iPipeline JIRA Time Import Process Each User Time Entries Child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.process_each_user_time_entries_max_active_runs,
    ) as dag:

        # View incoming configuration (standalone task for debugging)
        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_conf'
        )

        # Check if batch task can run based on Airflow Variable
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_user_time_entries_log'
        )

        # Batch task to optimize processing in batches
        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            start_task='create_user_time_entries_log',
            end_task='catch_and_log_errors',
        )

        # Create user-specific log
        create_user_time_entries_log = rail.CreateLogOperator(
            task_id='create_user_time_entries_log',
        )

        # Get all entries for this user from master valid records collection
        query_get_user_entries = rail.QueryCollectionOperator(
            task_id='query_get_user_entries',
            query=f"""SELECT * FROM final_valid_records_to_process WHERE author_jira_account_id = :user_jira_acc_id""",
            query_params={
                'user_jira_acc_id': '{{ dag_run.conf.author_jira_account_id }}'
            },
            name='user_time_entries'
        )

        get_user_details_from_jira = rail.SimpleHttpOperator(
            task_id='get_user_details_from_jira',
            http_conn_id=config.jira_conn_id,
            method='GET',
            # base url for access using Service Account= https://api.atlassian.com/ex/jira/{cloud_id}
            endpoint='/rest/api/3/user',
            headers={
                'Authorization': f'Bearer {OPEN_BRACKETS}var.value.{config.jira_bearer_token_var}{CLOSE_BRACKETS}',
                'Accept': 'application/json'
            },
            data={
                'accountId': '{{dag_run.conf.author_jira_account_id}}',
                'fields': 'emailAddress'
            }
        )

        get_user_email_in_jira = rail.PythonOperator(
            task_id='get_user_email_in_jira',
            python_callable=lambda: custom_methods.get_user_email(
                rail.result('get_user_details_from_jira'))
        )

        if_user_details_retrieved_from_jira = rail.IfOperator(
            task_id='if_user_details_retrieved_from_jira',
            test=lambda: not ('error' in rail.result(
                'get_user_details_from_jira')),
            yes_task='get_user_details_in_replicon',
            no_task='log_user_not_found_in_jira'
        )

        log_user_not_found_in_jira = rail.WriteLogOperator(
            task_id='log_user_not_found_in_jira',
            log="{{result('create_user_time_entries_log')}}",
            items="{{result('query_get_user_entries')}}",
            message='Validation exception',
            severity='Exception',
            properties=lambda item: {
                'task_issue_id': item['task_issue_id'],
                'task_type': item['task_type'],
                'time_entry_date': item['time_entry_date'],
                'hours': item['hours'],
                'replicon_id': item['replicon_id'],
                'action': 'Validation',
                'status': 'Exception',
                "details": "User Email not retrieved from JIRA ; " + rail.result('get_user_email_in_jira'),
            }
        )

        # Get user details from Replicon
        get_user_details_in_replicon = rail.RepliconServiceOperator(
            task_id='get_user_details_in_replicon',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data=lambda: {
                "users": [
                    {
                        "loginName": rail.result('get_user_email_in_jira')
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        get_details_of_user_in_replicon = rail.PythonOperator(
            task_id='get_details_of_user_in_replicon',
            python_callable=lambda: custom_methods.user_details_from_replicon(
                rail.result('get_user_details_in_replicon'))
        )

        # Check if user exists in Replicon
        check_user_exists = rail.IfOperator(
            task_id='check_user_exists',
            test=lambda: rail.result('get_details_of_user_in_replicon')[
                'process_further'],
            yes_task='dummy_time_entry_creation',
            no_task='log_user_not_found_in_replicon'
        )

        # Log if user not found in Replicon
        log_user_not_found_in_replicon = rail.WriteLogOperator(
            task_id='log_user_not_found_in_replicon',
            log="{{result('create_user_time_entries_log')}}",
            items="{{result('query_get_user_entries')}}",
            message='Validation exception',
            severity='Exception',
            properties=lambda item: {
                'task_issue_id': item['task_issue_id'],
                'task_type': item['task_type'],
                'time_entry_date': item['time_entry_date'],
                'hours': item['hours'],
                'replicon_id': item['replicon_id'],
                'action': 'Validation',
                'status': 'Exception',
                "details": rail.result('get_details_of_user_in_replicon')['message'],
            }
        )

        dummy_time_entry_creation = rail.EmptyOperator(
            task_id='dummy_time_entry_creation'
        )

        # Trigger Replicon time entry creation sub-child DAGs
        trigger_entry_creation = rail.TriggerDagRunForEachItemOperator(
            task_id="trigger_entry_creation",
            trigger_dag_id=config.process_each_time_entry_child_dag_id,
            items=lambda: rail.result("query_get_user_entries"),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: request_payload.get_process_each_entry_conf(item)
        )

        wait_for_time_entries_creation = rail.WaitForDagRunsSensor(
            task_id='wait_for_time_entries_creation',
            dag_runs='{{ result("trigger_entry_creation") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log="{{result('create_user_time_entries_log')}}",
            items="{{result('query_get_user_entries')}}",
            message='na',
            severity='Error',
            properties=lambda item: {
                'task_issue_id': item['task_issue_id'],
                'task_type': item['task_type'],
                'time_entry_date': item['time_entry_date'],
                'hours': item['hours'],
                'replicon_id': item['replicon_id'],
                'action': 'Put Time Entry',
                'status': 'Error',
                "details": '{{get_error_message()}}',
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> create_user_time_entries_log

        create_user_time_entries_log >> query_get_user_entries >> get_user_details_from_jira >> get_user_email_in_jira >> if_user_details_retrieved_from_jira

        if_user_details_retrieved_from_jira >> rail.Label(
            "No") >> log_user_not_found_in_jira >> catch_and_log_errors
        if_user_details_retrieved_from_jira >> rail.Label(
            "Yes") >> get_user_details_in_replicon >> get_details_of_user_in_replicon >> check_user_exists

        check_user_exists >> rail.Label(
            "No") >> log_user_not_found_in_replicon >> catch_and_log_errors
        check_user_exists >> rail.Label(
            "Yes") >> dummy_time_entry_creation >> trigger_entry_creation

        trigger_entry_creation >> wait_for_time_entries_creation >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
