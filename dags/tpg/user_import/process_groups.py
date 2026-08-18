from datetime import timedelta
import rail

from tpg.user_import.utils import response_filter

null = None
GROUPS_DELIMITER = "|"

# pylint: disable=too-many-statements
def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_groups,
        description='TPG User Import - Process Groups',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_groups,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        query_valid_delta_records_locations = rail.QueryCollectionOperator(
            task_id='query_valid_delta_records_locations',
            query="""SELECT DISTINCT location FROM validrecords""",
            name='valid_delta_locations'
        )

        def get_converted_locations_data(item):
            if not item:
                return []
            if not (item and item['location']):
                return []
            split_locations = item['location'].split(GROUPS_DELIMITER)
            return [
                {
                    "location_fullpath": '|'.join(split_locations[:i+1]),
                    "length": len(('|'.join(split_locations[:i+1])).split(GROUPS_DELIMITER))
                }
                for i in range(len(split_locations))
            ]

        convert_location_data = rail.DataAdaptorOperator(
            task_id="convert_location_data",
            source="{{result('query_valid_delta_records_locations')}}",
            columns=['location_fullpath', 'length'],
            data=get_converted_locations_data
        )

        converted_location_data_collection = rail.CreateCollectionOperator(
            task_id="converted_location_data_collection",
            source="{{result('convert_location_data')}}",
            name="converted_feed_locations"
        )

        get_all_location_grps = rail.RepliconServiceOperator(
            task_id="get_all_location_grps",
            endpoint="/services/LocationListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:location-list-column:name",
                    "urn:replicon:location-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=response_filter.groups_filter
        )

        create_replicon_locations_collection = rail.CreateCollectionOperator(
            task_id='create_replicon_locations_collection',
            columns=['name', 'uri', 'full_path'],
            name="replicon_locations",
            source="{{ result('get_all_location_grps') | to_json }}",
        )

        query_locations_to_create = rail.QueryCollectionOperator(
            task_id="query_locations_to_create",
            query="""SELECT DISTINCT * FROM converted_feed_locations WHERE LOWER(location_fullpath) NOT IN
                    (SELECT DISTINCT LOWER(full_path) FROM replicon_locations ) ORDER BY length""",
            name="locations_to_add"
        )

        has_new_locations = rail.IfOperator(
            task_id='has_new_locations',
            test="{{ result('query_locations_to_create','length') > 0 }}",
            yes_task='process_new_locations',
            no_task='finish'
        )

        process_new_locations = rail.TriggerDagRunForEachItemOperator(
            task_id='process_new_locations',
            items=lambda: rail.result('query_locations_to_create'),
            trigger_dag_id=config.process_new_locations,
            conf=lambda item, dag_run: {
                "filename": dag_run.conf['file_name'],
                "location_name": item['location_fullpath'].split(GROUPS_DELIMITER)[-1],
                "location_full_path": item['location_fullpath'],
                "parent_location_full_path": GROUPS_DELIMITER.join(item['location_fullpath'].split(GROUPS_DELIMITER)[0:-1])
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries = 0
        )

        wait_process_new_locations = rail.WaitForDagRunsSensor(
            task_id="wait_process_new_locations",
            dag_runs="{{result('process_new_locations')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        query_distinct_departments_full_paths_from_validrecords = rail.QueryCollectionOperator(
            task_id='query_distinct_departments_full_paths_from_validrecords',
            name='distinct_departments',
            query="""SELECT DISTINCT costcenter FROM validrecords"""
        )

        def get_converted_departments_data(item):
            if not item:
                return []
            if not (item and item['costcenter']):
                return []
            split_departments = item['costcenter'].split(GROUPS_DELIMITER)
            return [
                {
                    "department_fullpath": '|'.join(split_departments[:i+1]),
                    "length": len(('|'.join(split_departments[:i+1])).split(GROUPS_DELIMITER))
                }
                for i in range(len(split_departments))
            ]

        convert_department_data = rail.DataAdaptorOperator(
            task_id="convert_department_data",
            source="{{result('query_distinct_departments_full_paths_from_validrecords')}}",
            columns=['department_fullpath', 'length'],
            data=get_converted_departments_data
        )

        converted_department_data_collection = rail.CreateCollectionOperator(
            task_id="converted_department_data_collection",
            source="{{result('convert_department_data')}}",
            name="converted_feed_departments"
        )

        get_all_department_grps = rail.RepliconServiceOperator(
            task_id="get_all_department_grps",
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:department-group-list-column:name",
                    "urn:replicon:department-group-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=response_filter.groups_filter
        )

        create_replicon_departments_collection = rail.CreateCollectionOperator(
            task_id='create_replicon_departments_collection',
            columns=['name', 'uri', 'full_path'],
            name="replicon_departments",
            source="{{ result('get_all_department_grps') | to_json }}",
        )

        query_departments_to_create = rail.QueryCollectionOperator(
            task_id="query_departments_to_create",
            query="""SELECT DISTINCT * FROM converted_feed_departments WHERE LOWER(department_fullpath) NOT IN
                    (SELECT DISTINCT LOWER(full_path) FROM replicon_departments ) ORDER BY length""",
            name="departments_to_add"
        )

        has_new_departments = rail.IfOperator(
            task_id='has_new_departments',
            test="{{ result('query_departments_to_create','length') > 0 }}",
            yes_task='process_new_departments',
            no_task='finish'
        )

        process_new_departments = rail.TriggerDagRunForEachItemOperator(
            task_id='process_new_departments',
            items=lambda: rail.result('query_departments_to_create'),
            trigger_dag_id=config.process_new_departments,
            conf=lambda item, dag_run: {
                "filename": dag_run.conf['file_name'],
                "department_name": item['department_fullpath'].split(GROUPS_DELIMITER)[-1],
                "department_full_path": item['department_fullpath'],
                "parent_department_full_path": GROUPS_DELIMITER.join(item['department_fullpath'].split(GROUPS_DELIMITER)[0:-1])
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries = 0
        )

        wait_process_new_departments = rail.WaitForDagRunsSensor(
            task_id="wait_process_new_departments",
            dag_runs="{{result('process_new_departments')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        query_distinct_employee_type_full_paths_from_validrecords = rail.QueryCollectionOperator(
            task_id='query_distinct_employee_type_full_paths_from_validrecords',
            name='distinct_employeetypes',
            query="""SELECT DISTINCT employeetype FROM validrecords"""
        )

        def get_converted_employee_type_data(item):
            if not item:
                return []
            if not (item and item['employeetype']):
                return []
            split_employeetype = item['employeetype'].split(GROUPS_DELIMITER)
            return [
                {
                    "employeetype_fullpath": '|'.join(split_employeetype[:i+1]),
                    "length": len(('|'.join(split_employeetype[:i+1])).split(GROUPS_DELIMITER))
                }
                for i in range(len(split_employeetype))
            ]

        convert_employee_type_data = rail.DataAdaptorOperator(
            task_id="convert_employee_type_data",
            source="{{result('query_distinct_employee_type_full_paths_from_validrecords')}}",
            columns=['employeetype_fullpath', 'length'],
            data=get_converted_employee_type_data
        )

        converted_employee_type_data_collection = rail.CreateCollectionOperator(
            task_id="converted_employee_type_data_collection",
            source="{{result('convert_employee_type_data')}}",
            name="converted_feed_employeetypes"
        )

        get_all_employeetype_grps = rail.RepliconServiceOperator(
            task_id="get_all_employeetype_grps",
            endpoint="/services/EmployeeTypeGroupListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:employee-type-group-list-column:name",
                    "urn:replicon:employee-type-group-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=response_filter.groups_filter
        )

        create_replicon_employee_type_collection = rail.CreateCollectionOperator(
            task_id='create_replicon_employee_type_collection',
            columns=['name', 'uri', 'full_path'],
            name="replicon_employeetypes",
            source="{{ result('get_all_employeetype_grps') | to_json }}",
        )

        query_employee_types_to_create = rail.QueryCollectionOperator(
            task_id="query_employee_types_to_create",
            query="""SELECT DISTINCT * FROM converted_feed_employeetypes WHERE LOWER(employeetype_fullpath) NOT IN
                    (SELECT DISTINCT LOWER(full_path) FROM replicon_employeetypes ) ORDER BY length""",
            name="employeetypes_to_add"
        )

        has_new_employee_types = rail.IfOperator(
            task_id='has_new_employee_types',
            test="{{ result('query_employee_types_to_create','length') > 0 }}",
            yes_task='process_new_employee_types',
            no_task='finish'
        )

        process_new_employee_types = rail.TriggerDagRunForEachItemOperator(
            task_id='process_new_employee_types',
            items=lambda: rail.result('query_employee_types_to_create'),
            trigger_dag_id=config.process_new_employee_types,
            conf=lambda item, dag_run: {
                "filename": dag_run.conf['file_name'],
                "employeetype_name": item['employeetype_fullpath'].split(GROUPS_DELIMITER)[-1],
                "employeetype_full_path": item['employeetype_fullpath'],
                "parent_employeetype_full_path": GROUPS_DELIMITER.join(item['employeetype_fullpath'].split(GROUPS_DELIMITER)[0:-1])
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries = 0
        )

        wait_process_new_employee_types = rail.WaitForDagRunsSensor(
            task_id="wait_process_new_employee_types",
            dag_runs="{{result('process_new_employee_types')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        query_valid_delta_records_divisions = rail.QueryCollectionOperator(
            task_id='query_valid_delta_records_divisions',
            query="""SELECT DISTINCT businessunitorgroup FROM validrecords""",
            name='valid_delta_divisions'
        )

        def get_converted_divisions_data(item):
            if not item:
                return []
            if not (item and item['businessunitorgroup']):
                return []
            split_divisions = item['businessunitorgroup'].split(GROUPS_DELIMITER)
            return [
                {
                    "division_fullpath": '|'.join(split_divisions[:i+1]),
                    "length": len(('|'.join(split_divisions[:i+1])).split(GROUPS_DELIMITER))
                }
                for i in range(len(split_divisions))
            ]

        convert_division_data = rail.DataAdaptorOperator(
            task_id="convert_division_data",
            source="{{result('query_valid_delta_records_divisions')}}",
            columns=['division_fullpath', 'length'],
            data=get_converted_divisions_data
        )

        converted_division_data_collection = rail.CreateCollectionOperator(
            task_id="converted_division_data_collection",
            source="{{result('convert_division_data')}}",
            name="converted_feed_divisions"
        )

        get_all_division_grps = rail.RepliconServiceOperator(
            task_id="get_all_division_grps",
            endpoint="/services/DivisionListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:division-list-column:name",
                    "urn:replicon:division-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=response_filter.groups_filter
        )

        create_replicon_divisions_collection = rail.CreateCollectionOperator(
            task_id='create_replicon_divisions_collection',
            columns=['name', 'uri', 'full_path'],
            name="replicon_divisions",
            source="{{ result('get_all_division_grps') | to_json }}",
        )

        query_divisions_to_create = rail.QueryCollectionOperator(
            task_id="query_divisions_to_create",
            query="""SELECT DISTINCT * FROM converted_feed_divisions WHERE LOWER(division_fullpath) NOT IN
                    (SELECT DISTINCT LOWER(full_path) FROM replicon_divisions) ORDER BY length""",
            name="divisions_to_add"
        )

        has_new_divisions = rail.IfOperator(
            task_id='has_new_divisions',
            test="{{ result('query_divisions_to_create','length') > 0 }}",
            yes_task='process_new_divisions',
            no_task='finish'
        )

        process_new_divisions = rail.TriggerDagRunForEachItemOperator(
            task_id='process_new_divisions',
            items=lambda: rail.result('query_divisions_to_create'),
            trigger_dag_id=config.process_new_divisions,
            conf=lambda item, dag_run: {
                "filename": dag_run.conf['file_name'],
                "division_name": item['division_fullpath'].split(GROUPS_DELIMITER)[-1],
                "division_full_path": item['division_fullpath'],
                "parent_division_full_path": GROUPS_DELIMITER.join(item['division_fullpath'].split(GROUPS_DELIMITER)[0:-1])
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries = 0
        )

        wait_process_new_divisions = rail.WaitForDagRunsSensor(
            task_id="wait_process_new_divisions",
            dag_runs="{{result('process_new_divisions')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        query_valid_delta_records_locations >> convert_location_data >> converted_location_data_collection >> get_all_location_grps
        get_all_location_grps >> create_replicon_locations_collection >> query_locations_to_create
        query_locations_to_create >> has_new_locations >> rail.Label('No') >> finish
        has_new_locations >> rail.Label('Yes') >> process_new_locations >> wait_process_new_locations >> finish

        query_distinct_departments_full_paths_from_validrecords >> convert_department_data >> converted_department_data_collection >> get_all_department_grps
        get_all_department_grps >> create_replicon_departments_collection >> query_departments_to_create
        query_departments_to_create >> has_new_departments >> rail.Label('No') >> finish
        has_new_departments >> rail.Label('Yes') >> process_new_departments >> wait_process_new_departments >> finish

        query_distinct_employee_type_full_paths_from_validrecords >> convert_employee_type_data >> converted_employee_type_data_collection
        converted_employee_type_data_collection >> get_all_employeetype_grps
        get_all_employeetype_grps >> create_replicon_employee_type_collection >> query_employee_types_to_create
        query_employee_types_to_create >> has_new_employee_types >> rail.Label('No') >> finish
        has_new_employee_types >> rail.Label('Yes') >> process_new_employee_types >> wait_process_new_employee_types >> finish

        query_valid_delta_records_divisions >> convert_division_data >> converted_division_data_collection >> get_all_division_grps
        get_all_division_grps >> create_replicon_divisions_collection >> query_divisions_to_create
        query_divisions_to_create >> has_new_divisions >> rail.Label('No') >> finish
        has_new_divisions >> rail.Label('Yes') >> process_new_divisions >> wait_process_new_divisions >> finish

    return dag

rail.for_each_instance(create_child_dag)
