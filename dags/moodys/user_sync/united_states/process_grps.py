from datetime import timedelta
import rail

from moodys.user_sync.united_states.utils import response_filter

null = None

# pylint: disable=too-many-statements
def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_groups_dag_id,
        description='Moodys User Sync - Process Groups',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_groups,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")


        query_valid_delta_records_divisions = rail.QueryCollectionOperator(
            name='valid_delta_divisions',
            task_id='query_valid_delta_records_divisions',
            query="""SELECT DISTINCT divisionname FROM validrecords"""
        )

        get_all_divisions = rail.RepliconServiceOperator(
            task_id="get_all_divisions",
            endpoint="/services/DivisionListService1.svc/GetData",
            data={
                    "page": "1",
                    "pagesize": "100000",
                    "columnUris": [
                        "urn:replicon:division-list-column:name",
                        "urn:replicon:division-list-column:division"
                    ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=response_filter.filter_divisions_data
        )

        create_replicon_divisions_collection = rail.CreateCollectionOperator(
            task_id="create_replicon_divisions_collection",
            columns=['name', 'uri'],
            source="{{ result ('get_all_divisions') | to_json }}",
            name="replicon_divisions"
        )

        query_divisions_to_create = rail.QueryCollectionOperator(
            task_id='query_divisions_to_create',
            query="""SELECT DISTINCT * FROM valid_delta_divisions where LOWER(divisionname) NOT IN
                    (SELECT DISTINCT LOWER(name) FROM replicon_divisions)"""
        )

        has_new_divisions = rail.IfOperator(
            task_id='has_new_divisions',
            test="{{ result('query_divisions_to_create','length') > 0 }}",
            yes_task='dummy_process_new_divisions',
            no_task='finish'
        )

        dummy_process_new_divisions = rail.EmptyOperator(
            task_id='dummy_process_new_divisions'
        )

        process_new_divisions = rail.trigger_parallel_dagrun(
            task_id='process_new_divisions',
            items=lambda: rail.result('query_divisions_to_create'),
            parallel_count=config.trigger_parallel_dagrun_count_process_divisions,
            trigger_dag_id=config.process_new_divisions_dagid,
            conf={
                "filename": "{{ dag_run.conf.file_name }}",
                "divisionname": "{{ item.divisionname }}",
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        query_valid_delta_records_divisions >> get_all_divisions >> create_replicon_divisions_collection >> query_divisions_to_create
        query_divisions_to_create >> has_new_divisions >> rail.Label('No') >> finish
        has_new_divisions >> rail.Label('Yes') >> dummy_process_new_divisions >> process_new_divisions >> finish

    return dag

rail.for_each_instance(create_child_dag)
