"""
iPipeline JIRA Metadata Retrieval - Child DAG 1
Retrieves project and task metadata from JIRA by navigating Task → Story → Epic hierarchy
Extracts Replicon ID from Epic custom field and user email from JIRA User API
"""

from datetime import timedelta
from airflow.models import Variable
from ipipeline.time_import_v1.utils import custom_methods
import rail

null = None
OPEN_BRACKETS = '{{'
CLOSE_BRACKETS = '}}'


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_jira_project_info_child_dag_id,
        description=f"iPipeline JIRA Time Import Process Jira Project Info Child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.process_jira_project_info_max_active_runs,
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
            no_task='create_task_log'
        )

        # Batch task to optimize processing in batches
        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            start_task='create_task_log',
            end_task='catch_and_log_errors',
        )

        create_task_log = rail.CreateLogOperator(
            task_id='create_task_log'
        )

        # Step 1: Get Task details from JIRA
        get_task_details = rail.SimpleHttpOperator(
            task_id='get_task_details',
            http_conn_id=config.jira_conn_id,
            method='GET',
            # base url for access using Service Account= https://api.atlassian.com/ex/jira/{cloud_id}
            endpoint='/rest/api/3/issue/{{dag_run.conf.task_issue_id}}',
            headers={
                'Authorization': f'Bearer {OPEN_BRACKETS}var.value.{config.jira_bearer_token_var}{CLOSE_BRACKETS}'
            },
            data={
                'fields': 'parent,issuetype,summary,customfield_16456,customfield_11301',
                'expand': 'parent'
            }
        )

        transform_task_details = rail.PythonOperator(
            task_id='transform_task_details',
            python_callable=lambda: custom_methods.transform_task_jira_api_result(
                rail.result("get_task_details")),
        )

        # Check if task has parent (Story)
        check_parent_exists = rail.IfOperator(
            task_id='check_parent_exists',
            test='{{ result("transform_task_details").task_parent_issue_id | is_truthy }}',
            yes_task='get_story_details',
            no_task='log_no_parent_found_for_task'
        )

        # Log if no parent found
        log_no_parent_found_for_task = rail.WriteLogOperator(
            task_id='log_no_parent_found_for_task',
            log='{{ result("create_task_log") }}',
            severity='Exception',
            message='Parent (Story) not found for Task',
            properties=lambda dag_run: {
                'task_issue_id': dag_run.conf.get('task_issue_id'),
                'task_jira_id':  rail.result("transform_task_details")['task_jira_id'],
                'task_summary': rail.result("transform_task_details")['task_summary'],
                'task_parent_issue_id': rail.result("transform_task_details")['task_parent_issue_id'],
                'task_parent_jira_id': rail.result("transform_task_details")['task_parent_jira_id'],
                'task_parent_jira_summary': rail.result("transform_task_details")['task_parent_jira_summary'],
                'task_grandparent_issue_id': '',
                'replicon_id': '',
                'task_type': rail.result("transform_task_details")['task_type'],
                'task_issuetype': rail.result("transform_task_details")['task_issuetype'],
                'action': 'Task Metadata Retrieval',
                'task_processing_status': 'Exception',
                'details': f"Parent (Story) not found for Task issue id {dag_run.conf.get('task_issue_id')}"
            }
        )

        # Step 2: Get Story (Parent) details from JIRA
        get_story_details = rail.SimpleHttpOperator(
            task_id='get_story_details',
            http_conn_id=config.jira_conn_id,
            method='GET',
            # base url for access using Service Account= https://api.atlassian.com/ex/jira/{cloud_id}
            endpoint='/rest/api/3/issue/{{result("transform_task_details").task_parent_issue_id}}',
            headers={
                'Authorization': f'Bearer {OPEN_BRACKETS}var.value.{config.jira_bearer_token_var}{CLOSE_BRACKETS}',
                'Accept': 'application/json'
            },
            data={
                'fields': 'parent',
                'expand': 'parent'
            }
        )

        get_epid_issue_id = rail.PythonOperator(
            task_id='get_epid_issue_id',
            python_callable=lambda: custom_methods.get_epic_issue_id(
                rail.result("get_story_details"))
        )

        # Check if story has parent (Epic)
        check_epic_exists = rail.IfOperator(
            task_id='check_epic_exists',
            test='{{ result("get_epid_issue_id") | is_truthy }}',
            yes_task='get_epic_details',
            no_task='log_no_epic'
        )

        # Log if no parent found
        log_no_epic = rail.WriteLogOperator(
            task_id='log_no_epic',
            log='{{ result("create_task_log") }}',
            severity='Exception',
            message='GrandParent (Epic) not found for Task',
            properties=lambda dag_run: {
                'task_issue_id': dag_run.conf.get('task_issue_id'),
                'task_jira_id':  rail.result("transform_task_details")['task_jira_id'],
                'task_summary': rail.result("transform_task_details")['task_summary'],
                'task_parent_issue_id': rail.result("transform_task_details")['task_parent_issue_id'],
                'task_parent_jira_id': rail.result("transform_task_details")['task_parent_jira_id'],
                'task_parent_jira_summary': rail.result("transform_task_details")['task_parent_jira_summary'],
                'task_grandparent_issue_id': '',
                'replicon_id': '',
                'task_type': rail.result("transform_task_details")['task_type'],
                'task_issuetype': rail.result("transform_task_details")['task_issuetype'],
                'action': 'Task Metadata Retrieval',
                'task_processing_status': 'Exception',
                'details': f"GrandParent (Epic) not found for Task issue id {dag_run.conf.get('task_issue_id')}"
            }
        )

        # Step 3: Get Epic (Grandparent) details from JIRA
        get_epic_details = rail.SimpleHttpOperator(
            task_id='get_epic_details',
            http_conn_id=config.jira_conn_id,
            method='GET',
            # base url for access using Service Account= https://api.atlassian.com/ex/jira/{cloud_id}
            endpoint='/rest/api/3/issue/{{result("get_epid_issue_id")}}',
            headers={
                'Authorization': f'Bearer {OPEN_BRACKETS}var.value.{config.jira_bearer_token_var}{CLOSE_BRACKETS}',
                'Accept': 'application/json'
            },
            data={
                'fields': 'issuetype,summary,customfield_16301'
            }
        )

        # Extract Replicon ID (Project Code in Replicon) from Epic JIRA custom fields
        extract_replicon_id = rail.PythonOperator(
            task_id='extract_replicon_id',
            python_callable=custom_methods.get_project_code_from_epic_level
        )

        log_all_task_details = rail.WriteLogOperator(
            task_id='log_all_task_details',
            log='{{ result("create_task_log") }}',
            severity='Success',
            message='na',
            properties=lambda dag_run: {
                'task_issue_id': dag_run.conf.get('task_issue_id'),
                'task_jira_id':  rail.result("transform_task_details")['task_jira_id'],
                'task_summary': rail.result("transform_task_details")['task_summary'],
                'task_parent_issue_id': rail.result("transform_task_details")['task_parent_issue_id'],
                'task_parent_jira_id': rail.result("transform_task_details")['task_parent_jira_id'],
                'task_parent_jira_summary': rail.result("transform_task_details")['task_parent_jira_summary'],
                'task_grandparent_issue_id': rail.result("get_epid_issue_id"),
                'replicon_id': rail.result("extract_replicon_id"),
                'task_type': rail.result("transform_task_details")['task_type'],
                'task_issuetype': rail.result("transform_task_details")['task_issuetype'],
                'action': 'Task Metadata Retrieval',
                'task_processing_status': 'Success',
                'details': ""
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log='{{ result("create_task_log") }}',
            severity='Error',
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                'task_issue_id': dag_run.conf.get('task_issue_id'),
                'task_jira_id':  rail.result("transform_task_details")['task_jira_id'] if rail.result(
                    "transform_task_details") else '',
                'task_summary': rail.result("transform_task_details")['task_summary'] if rail.result(
                    "transform_task_details") else '',
                'task_parent_issue_id': rail.result("transform_task_details")['task_parent_issue_id'] if rail.result(
                    "transform_task_details") else '',
                'task_parent_jira_id': rail.result("transform_task_details")['task_parent_jira_id'] if rail.result(
                    "transform_task_details") else '',
                'task_parent_jira_summary': rail.result("transform_task_details")['task_parent_jira_summary'] if rail.result(
                    "transform_task_details") else '',
                'task_grandparent_issue_id': rail.result("get_epid_issue_id") if rail.result("get_epid_issue_id") else '',
                'replicon_id': rail.result("extract_replicon_id") if rail.result("extract_replicon_id") else '',
                'task_type': rail.result("transform_task_details")['task_type'] if rail.result("transform_task_details") else '',
                'task_issuetype': rail.result("transform_task_details")['task_issuetype'] if rail.result("transform_task_details") else '',
                'action': 'Task Metadata Retrieval',
                'task_processing_status': 'Error',
                'details': "{{ get_error_message() }}"
            }
        )

        # Define task flow
        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> create_task_log

        create_task_log >> get_task_details >> transform_task_details >> check_parent_exists

        check_parent_exists >> rail.Label('Yes') >> get_story_details
        check_parent_exists >> rail.Label(
            'No') >> log_no_parent_found_for_task >> catch_and_log_errors

        get_story_details >> get_epid_issue_id >> check_epic_exists

        check_epic_exists >> rail.Label('Yes') >> get_epic_details
        check_epic_exists >> rail.Label(
            'No') >> log_no_epic >> catch_and_log_errors

        get_epic_details >> extract_replicon_id >> log_all_task_details >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
