from datetime import timedelta
import rail

from mercury_systems_inc.user_import_v1.utils import response_filter, custom_methods

null = None
GROUPS_DELIMITER = '|'


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_groups_dagid,
        description='MercurySystemsInc User Import Process Groups',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_groups,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        query_valid_records_for_location_group_data = rail.QueryCollectionOperator(
            name='input_location_group_data',
            task_id='query_valid_records_for_location_group_data',
            query="""SELECT DISTINCT Work_Location_Name, Work_Location_Code, Work_Location_State, Work_Location_Country, 
                (Work_Location_Country || '|' || Work_Location_State) AS Parent_Fullpath_Code,
                (Work_Location_Country || '|' || Work_Location_State || '|' || Work_Location_Code) AS Location_Fullpath_Code FROM valid_records"""
        )

        get_all_replicon_locations = rail.RepliconServicePageOperator(
            task_id="get_all_replicon_locations",
            endpoint="/services/LocationListService1.svc/GetData",
            data={
                "page": 1,
                "pagesize": 10000,
                "columnUris": [
                    "urn:replicon:location-list-column:location",
                    "urn:replicon:service-center-list-column:code",
                    "urn:replicon:location-list-column:full-path-code",
                    "urn:replicon:location-list-column:effectively-enabled"
                ],
                "sort": [],
                "filterExpression": null
            },
            page_handler=custom_methods.page_handler,
            all_result_data_handler=response_filter.filter_group_data
        )

        create_replicon_locations_collection = rail.CreateCollectionOperator(
            task_id="create_replicon_locations_collection",
            columns={
                'name': 'location_name',
                'uri': 'location_uri',
                'code': 'location_code',
                'fullpath': 'location_fullpath_code',
                'enabled': 'enabled'
            },
            source="{{ result ('get_all_replicon_locations') | to_json }}",
            name="replicon_locations"
        )

        query_locations_to_create = rail.QueryCollectionOperator(
            task_id='query_locations_to_create',
            query="""SELECT new_locations_to_create.* , repl_loc.location_uri AS parent_location_uri
                FROM (
                    SELECT DISTINCT * FROM input_location_group_data where LOWER(Location_Fullpath_Code) NOT IN
                        (SELECT DISTINCT LOWER(location_fullpath_code) FROM replicon_locations)
                ) AS new_locations_to_create
                LEFT JOIN
                    replicon_locations AS repl_loc
                ON new_locations_to_create.Parent_Fullpath_Code = repl_loc.location_fullpath_code""",
        )

        has_new_location = rail.IfOperator(
            task_id='has_new_location',
            test="{{ result('query_locations_to_create','length') > 0 }}",
            yes_task='process_new_location_add',
            no_task='finish'
        )

        process_new_location_add = rail.TriggerDagRunForEachItemOperator(
            task_id='process_new_location_add',
            items=lambda: rail.result('query_locations_to_create'),
            trigger_dag_id=config.process_new_location_add_dagid,
            conf={
                "location_name": "{{ item.Work_Location_Name }}",
                "location_code": "{{ item.Work_Location_Code }}",
                "parent_uri": "{{ item.parent_location_uri }}",
                "parent_fullpath_code": "{{ item.Parent_Fullpath_Code }}",
                "groups_log_table": "{{dag_run.conf.groups_log_table}}",
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

        wait_process_new_location_add = rail.WaitForDagRunsSensor(
            task_id="wait_process_new_location_add",
            dag_runs="{{result('process_new_location_add')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        finish = rail.EmptyOperator(
            task_id='finish',
            trigger_rule='all_done'
        )

        filter_groups_table_data_for_errors = rail.FilterLogEntriesOperator(
            task_id='filter_groups_table_data_for_errors',
            log='{{dag_run.conf.groups_log_table}}',
            severity='Error',
        )

        can_fail_dag = rail.IfOperator(
            task_id='can_fail_dag',
            test="{{ result('filter_groups_table_data_for_errors', 'length') > 0 or get_error_message() | is_truthy }}",
            yes_task='fail_dag'
        )

        fail_dag = rail.FailOperator(
            task_id='fail_dag',
            message="Groups processing failed due to error(s) . Please check the groups log table for details.",
        )

        query_valid_records_for_location_group_data >> get_all_replicon_locations >> create_replicon_locations_collection >> query_locations_to_create
        query_locations_to_create >> has_new_location >> rail.Label(
            'No') >> finish
        has_new_location >> rail.Label(
            'Yes') >> process_new_location_add >> wait_process_new_location_add >> finish

        finish >> filter_groups_table_data_for_errors >> can_fail_dag >> rail.Label(
            'Yes') >> fail_dag

        return dag


rail.for_each_instance(create_child_dag)
