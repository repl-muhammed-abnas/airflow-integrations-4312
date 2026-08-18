from datetime import timedelta
import rail

from lanter_delivery_systems.user_import.user_import_integration.utils import response_filter

null = None
GROUPS_DELIMITER = "|"

# pylint: disable=too-many-statements
def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_groups_dagid,
        description='Lanter Delivery Systems User Import - Process Groups',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_groups,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        query_valid_delta_records_locations = rail.QueryCollectionOperator(
            name='valid_delta_locations',
            task_id='query_valid_delta_records_locations',
            query="""SELECT DISTINCT locationname FROM validrecords"""
        )

        get_all_locations = rail.RepliconServiceOperator(
            task_id='get_all_locations',
            endpoint='/services/LocationService1.svc/GetAllLocations',
        )

        create_replicon_location_collection = rail.CreateCollectionOperator(
            task_id="create_replicon_location_collection",
            columns=['displayText', 'parameterCorrelationId', 'slug', 'uri'],
            name="replicon_locations",
            source="{{ result('get_all_locations') | to_json }}"
        )

        query_locations_to_create = rail.QueryCollectionOperator(
            task_id='query_locations_to_create',
            query="""SELECT DISTINCT * FROM valid_delta_locations where LOWER(locationname) NOT IN
                    (SELECT DISTINCT LOWER(displayText) FROM replicon_locations)"""
        )

        has_new_locations = rail.IfOperator(
            task_id='has_new_locations',
            test="{{ result('query_locations_to_create','length') > 0 }}",
            yes_task='process_new_locations',
            no_task='finish'
        )

        process_new_locations = rail.TriggerDagRunForEachItemOperator(
            task_id='process_new_locations',
            items="{{ result('query_locations_to_create') }}",
            trigger_dag_id=config.process_new_locations_dagid,
            conf={
                "filename": "{{ dag_run.conf.file_name }}",
                "locationname": "{{ item.locationname }}",
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
            query="""SELECT DISTINCT department FROM validrecords"""
        )

        def get_converted_departments_data(item):
            if not item:
                return []
            split_departments = item['department'].split(GROUPS_DELIMITER)
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
            trigger_dag_id=config.process_new_departments_dagid,
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
            trigger_dag_id=config.process_new_employee_types_dagid,
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

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        query_valid_delta_records_locations >> get_all_locations >> create_replicon_location_collection >> query_locations_to_create
        query_locations_to_create >> has_new_locations >> rail.Label('No') >> finish
        has_new_locations >> rail.Label('Yes') >> process_new_locations >> wait_process_new_locations>> finish

        query_distinct_departments_full_paths_from_validrecords >> convert_department_data >> converted_department_data_collection >> get_all_department_grps
        get_all_department_grps >> create_replicon_departments_collection >> query_departments_to_create
        query_departments_to_create >> has_new_departments >> rail.Label('No') >> finish
        has_new_departments >> rail.Label('Yes') >> process_new_departments >> wait_process_new_departments >> finish

        query_distinct_employee_type_full_paths_from_validrecords >> convert_employee_type_data >> converted_employee_type_data_collection
        converted_employee_type_data_collection >> get_all_employeetype_grps
        get_all_employeetype_grps >> create_replicon_employee_type_collection >> query_employee_types_to_create
        query_employee_types_to_create >> has_new_employee_types >> rail.Label('No') >> finish
        has_new_employee_types >> rail.Label('Yes') >> process_new_employee_types >> wait_process_new_employee_types >> finish

    return dag

rail.for_each_instance(create_child_dag)
