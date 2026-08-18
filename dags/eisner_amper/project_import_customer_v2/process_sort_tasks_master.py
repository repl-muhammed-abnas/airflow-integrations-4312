from datetime import timedelta
from pendulum import datetime
import rail

def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.sort_task_master_dagid,
        description='Eisner Amper Project Import Customer SORT Task - Master',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 4, 1, tz=config.timezone),
        schedule_interval=config.master_dag_interval_sort_tasks,
        max_active_runs=config.max_active_runs_sort_tasks_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        get_tenant_wide_log_details = rail.CreateLogOperator(
            task_id="get_tenant_wide_log_details",
            tenant_wide_name=config.tenant_wide_log_name,
            existing_log_mode="append",
        )

        search_entries_tenant_wide_log = rail.FilterLogEntriesOperator(
            task_id = 'search_entries_tenant_wide_log',
            log= "{{ result('get_tenant_wide_log_details') }}",
            remove_filtered_entries= True
        )

        is_entries_present=rail.IfOperator(
            task_id='is_entries_present',
            test='''{{ result('search_entries_tenant_wide_log',"length") != 0 }}''',
            yes_task="create_collection_project_data",
            no_task="finish",
        )

        create_collection_project_data = rail.CreateCollectionOperator(
            task_id='create_collection_project_data',
            source='{{ result("search_entries_tenant_wide_log") }}',
            columns={
                "properties": "properties",
                "ecid":"ecid"
            },
            name='collection_data'
        )

        query_required_details = rail.QueryCollectionOperator(
            task_id = "query_required_details",
            query="""select cd.ecid  as ecid,
                json_extract(cd.properties, '$.client_code') as client_code,
                json_extract(cd.properties, '$.project_code') as project_code,
                json_extract(cd.properties, '$.project_uri') as project_uri
                from collection_data cd """
        )

        query_distinct_project_uris = rail.QueryCollectionOperator(
            task_id = "query_distinct_project_uris",
            query="""Select DISTINCT * From query_required_details"""
        )

        process_each_project = rail.TriggerDagRunForEachItemOperator(
            task_id = "process_each_project",
            items=lambda :  rail.result("query_distinct_project_uris"),
            trigger_dag_id= config.sort_task_child_dagid,
            conf=lambda item: {
                'client_code': item['client_code'],
                'project_code': item['project_code'],
                'project_uri': item['project_uri'],
                'ecid':item['ecid']
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        finish=rail.EmptyOperator(
            task_id='finish',
        )

        get_tenant_wide_log_details >> search_entries_tenant_wide_log >> is_entries_present >> rail.Label('No') >> finish
        is_entries_present >> rail.Label('Yes') >> create_collection_project_data >> query_required_details >> query_distinct_project_uris
        query_distinct_project_uris >> process_each_project
        process_each_project >> finish

    return dag

rail.for_each_instance(create_main_airflow_dag)
