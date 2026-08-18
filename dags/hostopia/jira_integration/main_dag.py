from datetime import timedelta
import pendulum
import rail
from hostopia.jira_integration.utils import custom_method


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'hostopia_jira_import_master_{config.instance}',
        description=f'hostopia jira import master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=pendulum.datetime(2022, 4, 1, tz=config.pacific_timezone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.master_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        jira_sync_data = rail.SimpleHttpOperator(
            task_id='jira_sync_data',
            method='GET',
            endpoint='rest/api/3/search?jql=issuetype = Task AND updated >= -1h&maxResults=100&startAt=0',
            http_conn_id='hostopia_jira_connection',
            response_filter=lambda response: response.json()['total']
        )

        has_jira_sync_data = rail.IfOperator(
            task_id='has_jira_sync_data',
            test=bool(lambda: rail.result("jira_sync_data")),
            yes_task='get_count_of_jira_data',
            no_task='get_all_subtasks_from_jira'
        )

        get_count_of_jira_data = rail.PythonOperator(
            task_id='get_count_of_jira_data',
            python_callable=lambda: custom_method.count_of_jira_data(
                rail.result("jira_sync_data"))
        )

        process_jiras_to_replicon = rail.TriggerDagRunForEachItemOperator(
            task_id='process_jiras_to_replicon',
            trigger_dag_id=f'hostopia_jira_import_child_process_jira_data_{config.instance}',
            retries=0,
            items=lambda: rail.result('get_count_of_jira_data'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                'start_from': '{{ item }}'
            }
        )

        wait_for_process_jiras_to_replicon = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_jiras_to_replicon',
            dag_runs='{{ result("process_jiras_to_replicon") }}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_all_subtasks_from_jira = rail.SimpleHttpOperator(
            task_id='get_all_subtasks_from_jira',
            method='GET',
            endpoint='rest/api/3/search?jql=type in subTaskIssueTypes()  AND updated >= -90m ORDER BY created DESC',
            http_conn_id='hostopia_jira_connection',
            response_filter=lambda response: response.json()['issues']
        )

        has_subtask_data = rail.IfOperator(
            task_id='has_subtask_data',
            test=bool(lambda: rail.result("jira_sync_data")),
            yes_task='map_to_subtasks_schema',
            no_task='finish'
        )

        map_to_subtasks_schema = rail.DataAdaptorOperator(
            task_id="map_to_subtasks_schema",
            source=lambda: rail.result("get_all_subtasks_from_jira"),
            columns=['subtask_key', 'parent_key', 'startdate', 'enddate', 'status', 'summary'],
            data=custom_method.convert_input_data_to_subtask_data,
        )

        process_subtask_data_to_replicon = rail.TriggerDagRunForEachItemOperator(
            task_id= 'process_subtask_data_to_replicon',
            trigger_dag_id=f'hostopia_jira_import_child_process_subtask_data_{config.instance}',
            retries=0,
            items="{{ result('map_to_subtasks_schema') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                'project_code': '{{ item.parent_key }}',
                'task_code': '{{ item.subtask_key }}',
                'startdate': '{{ item.startdate }}',
                'enddate': '{{ item.enddate }}',
                'status': '{{ item.status }}',
                'summary': '{{ item.summary }}'
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        jira_sync_data >> has_jira_sync_data >> rail.Label(
            "Yes") >> get_count_of_jira_data >> process_jiras_to_replicon

        has_jira_sync_data >> rail.Label(
            "No") >> get_all_subtasks_from_jira

        process_jiras_to_replicon >> wait_for_process_jiras_to_replicon >> get_all_subtasks_from_jira >> has_subtask_data

        has_subtask_data >> rail.Label(
            "Yes") >> map_to_subtasks_schema >> process_subtask_data_to_replicon >> finish

        has_subtask_data >> rail.Label(
            "No") >> finish

    return dag


rail.for_each_instance(create_main_dag)
