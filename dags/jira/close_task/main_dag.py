from datetime import datetime, timedelta
from collections import defaultdict
import pytz
import rail
from airflow.models import Variable

null = None
# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/jira/main_dag/config.py


# pylint: disable=too-many-statements
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"standard_jira_{config.region.replace('-', '_')}_close_task_{config.instance}",
        description=f'Jira {config.region} Close Task {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_lastsync_time'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_lastsync_time',
            end_task='should_log_history',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_lastsync_time = rail.GetLastSyncTimeOperator(
            task_id='get_lastsync_time',
            workflow_name=config.workflow,
            date_format='%Y-%m-%d %H:%M',
            initial_sync_time=lambda: (
                datetime(year=1970, month=1, day=1)).strftime('%Y-%m-%d %H:%M:%S'),
            provider=config.provider
        )

        def get_current_user_details(response):
            return {
                'server_timezone': response[0]['timeZone']
            } if response[0] else null

        get_current_user = rail.JiraAPIOperator(
            task_id='get_current_user',
            request_method='GET',
            endpoint="/rest/api/3/myself",
            jira_conn_id='{{ dag_run.conf.jira_conn_id }}',
            data_handler=get_current_user_details
        )

        def get_server_time_from_utc():
            """
            Convert UTC time to local time based on server timezone.

            :return: Local time string in "YYYY-MM-DD HH:MM" format
            """
            # Get last sync time and server timezone from rail results
            last_sync_time_str = rail.result('get_lastsync_time')[
                'last_synctime']
            server_timezone = rail.result('get_current_user')[
                'server_timezone']

            try:
                # Parse the UTC time string into a datetime object
                utc_time = datetime.strptime(
                    last_sync_time_str, "%Y-%m-%d %H:%M")

                # Attach UTC timezone to the datetime object
                utc_time_with_tz = pytz.utc.localize(utc_time)

                # Convert to server timezone
                local_tz = pytz.timezone(server_timezone)
                local_time = utc_time_with_tz.astimezone(local_tz)

                return local_time.strftime("%Y-%m-%d %H:%M")
            except:  # pylint: disable=bare-except
                return None

        get_server_time = rail.PythonOperator(
            task_id='get_server_time',
            python_callable=get_server_time_from_utc
        )

        def get_issues_details(response):
            issues = list(filter(lambda x: x['status'].lower() == "done", list(map(lambda item: {
                "key": item["key"],
                "issue_summary": item['fields']["summary"],
                "project": item['fields']['project']['name'],
                "project_code": item['fields']['project']['key'],
                "status": item['fields']['status']['name']
            }, response)))) if response else []
            project_dict = defaultdict(list)

            for item in issues:
                project = item.pop('project')
                project_dict[project].append(item)

            grouped_list = [{'project': project, 'issues': issues}
                            for project, issues in project_dict.items()]
            return grouped_list

        def get_request_body():
            if rail.result('get_lastsync_time')['last_synctime'] == '1970-01-01 00:00':
                return {
                    "fields": [
                        "summary",
                        "status",
                        "assignee",
                        "project"
                    ],
                    "fieldsByKeys": "false",
                    "jql": "status = Done AND updated >= -60m ORDER BY updated DESC",
                    "maxResults": 15,
                    "startAt": 0
                }
            datetime_str = rail.result("get_server_time")
            base_jql = "status = Done AND updated >= {} ORDER BY updated DESC"
            formatted_jql = base_jql.format(f"'{datetime_str}'")
            return {
                "fields": [
                    "summary",
                    "status",
                    "assignee",
                    "project",
                    "created"
                ],
                "fieldsByKeys": "false",
                "jql": formatted_jql,
                "maxResults": 15,
                "startAt": 0
            }

        jira_updated_issues = rail.JiraAPIOperator(
            task_id='jira_updated_issues',
            request_method='POST',
            endpoint="/rest/api/3/search",
            jira_conn_id='{{ dag_run.conf.jira_conn_id }}',
            request_body=get_request_body,
            data_handler=get_issues_details
        )

        trigger_close_task_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_close_task_child_dag',
            items=lambda: rail.result('jira_updated_issues'),
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f"standard_jira_{config.region.replace('-', '_')}_close_task_child_dag_{config.instance}",
            conf=lambda item, dag_run: {
                **{
                    "project": item['project'],
                    "issues": item['issues'],
                    'replicon_conn_id': dag_run.conf['replicon_conn_id']
                },
                **{
                    k: v for k, v in dag_run.conf.items() if k not in ('_ancestry', '_ecid', '_replication_position')
                }
            }
        )

        wait_for_close_task_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_close_task_child_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_close_task_child_dag") }}'
        )

        gather_close_task_error = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_close_task_error',
            dag_runs="{{ result('trigger_close_task_child_dag') }}",
            dagrun_task_id='catch_close_task_error',
            flatten=True
        )

        is_close_task_error = rail.IfOperator(
            task_id='is_close_task_error',
            # pylint: disable=line-too-long
            test="{{ (get_task_state('gather_close_task_error') == 'success' and result('gather_close_task_error') | length > 0)}}",
            yes_task='fail_close_task_error',
            no_task='should_log_history'
        )

        fail_close_task_error = rail.FailOperator(
            task_id='fail_close_task_error',
            message="{{ result('gather_close_task_error') | map_to_attr('error') | join('|') }}"
        )

        should_log_history = rail.IfOperator(
            task_id='should_log_history',
            test="{{ not(get_task_state('jira_updated_issues') == 'success' and \
                result('jira_updated_issues') | length == 0 )}}",
            trigger_rule='all_done',
            yes_task='log_dagrun_details_to_table',
            no_task='delete_this_dagrun'
        )

        log_dagrun_details_to_table = rail.PostDagRunDetailsToRepliconOperator(
            task_id='log_dagrun_details_to_table',
            required_configs={
                'airflow_connector_ui_connid': config.airflow_connector_ui_connid,
                'hmac_secret_var': config.hmac_secret
            },
            company_key='{{ dag_run.conf.company_key }}',
            connector_name='jira',
            integration_type='close_task'
        )

        update_lastsync_time = rail.SetLastSyncTimeOperator(
            task_id='update_lastsync_time',
            workflow_name=config.workflow,
            provider=config.provider,
            value_to_set='{{result("get_lastsync_time").current_time}}'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> should_log_history
        can_run_batch_task >> rail.Label(
            'No') >> get_lastsync_time >> get_current_user >> get_server_time >> jira_updated_issues \
            >> update_lastsync_time >> trigger_close_task_child_dag >> wait_for_close_task_child_dag \
            >> gather_close_task_error >> is_close_task_error
        is_close_task_error >> rail.Label(
            'Yes') >> fail_close_task_error >> should_log_history
        is_close_task_error >> rail.Label(
            'No') >> should_log_history
        should_log_history >> rail.Label(
            'Yes') >> log_dagrun_details_to_table
        should_log_history >> rail.Label(
            'No') >> delete_this_dagrun

    return dag


rail.for_each_instance(create_main_dag)
