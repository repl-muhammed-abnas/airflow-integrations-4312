from datetime import timedelta
from pendulum import datetime
import json
import rail
from odessa.project_team_update_v2.utils import custom_method


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'odessa_jira_import_master_v2_{config.instance}',
        description=f'odessa jira import master V2 {config.instance}',
        company_key=config.company_key,
        start_date=datetime(2022, 4, 1, tz=config.pacific_timezone),
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        max_active_runs=config.master_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        def get_jira_sync_data(response):
            return response.json()

        jira_sync_data = rail.SimpleHttpOperator(
            task_id='jira_sync_data',
            method='POST',
            endpoint='rest/api/3/search/jql',
            data=json.dumps({
                "jql": 'Customer != null AND Wing != null AND "Sync to Replicon" = "Yes" AND ("Is it available in Replicon" = null OR "Is it available in Replicon" != "Yes") AND updated >= -1h',
                "maxResults": 1000,
                "fields": ["*all"],
                "fieldsByKeys": False
            }),
            http_conn_id=config.http_conn_id,
            response_filter=get_jira_sync_data
        )

        has_jira_sync_data = rail.IfOperator(
            task_id='has_jira_sync_data',
            test=lambda: bool(rail.result("jira_sync_data")["issues"]),
            yes_task='generate_pagination_data',
            no_task='finish'
        )

        generate_pagination_data = rail.PythonOperator(
            task_id='generate_pagination_data',
            python_callable=lambda: custom_method.generate_pagination_pages(
                rail.result("jira_sync_data"))
        )

        process_jiras_to_replicon = rail.TriggerDagRunForEachItemOperator(
            task_id='process_jiras_to_replicon',
            trigger_dag_id=f'odessa_jira_import_child_process_jira_data_v2_{config.instance}',
            items=lambda: rail.result('generate_pagination_data'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                'page_number': '{{ item.page_number }}',
                'next_page_token': '{{ item.next_page_token }}',
                'is_base_page': '{{ item.is_base_page }}',
                'base_response': '{{ item.base_response if item.is_base_page else None }}',
                'next_page_token_for_chaining': '{{ item.next_page_token_for_chaining }}'
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        jira_sync_data >> has_jira_sync_data >> rail.Label(
            "Yes") >> generate_pagination_data >> process_jiras_to_replicon

        has_jira_sync_data >> rail.Label("No") >> finish

    return dag


rail.for_each_instance(create_main_dag)
