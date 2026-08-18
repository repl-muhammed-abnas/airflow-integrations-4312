from datetime import timedelta
import pendulum
import rail
from zaloragroup.task_import_from_global_fashion.utils import custom_method


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'zaloragroup_jira_import_from_global_fashion_master_{config.instance}',
        description=f'ZaloraGroup jira import From Global Fashion Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=pendulum.datetime(2022, 4, 1, tz=config.pacific_timezone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.master_dag_max_active_runs
    ) as dag:

        jira_sync_data = rail.SimpleHttpOperator(
            task_id='jira_sync_data',
            method='GET',
            endpoint='rest/api/3/search?jql=updated >= -1h&maxResults=100&startAt=0',
            http_conn_id=config.http_conn_id,
            response_filter=lambda response: response.json()['total']
        )

        has_jira_sync_data = rail.IfOperator(
            task_id='has_jira_sync_data',
            test=lambda: bool(rail.result("jira_sync_data")),
            yes_task='get_count_of_jira_data',
            no_task='finish'
        )

        get_count_of_jira_data = rail.PythonOperator(
            task_id='get_count_of_jira_data',
            python_callable=lambda: custom_method.count_of_jira_data(
                rail.result("jira_sync_data"))
        )

        process_jiras_to_replicon = rail.TriggerDagRunForEachItemOperator(
            task_id='process_jiras_to_replicon',
            trigger_dag_id=config.child_process_jira_dag_id,
            retries=0,
            items=lambda: rail.result('get_count_of_jira_data'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                'start_from': '{{ item }}'
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        jira_sync_data >> has_jira_sync_data >> rail.Label(
            "Yes") >> get_count_of_jira_data >> process_jiras_to_replicon >> finish

        has_jira_sync_data >> rail.Label(
            "No") >> finish

    return dag


rail.for_each_instance(create_main_dag)
