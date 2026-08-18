from datetime import timedelta
import rail

from incyte_biosciences_international_sarl.user_import.utils import response_filter, request_payload

null = None
GROUPS_DELIMITER = "|"

# pylint: disable=too-many-statements
def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_groups_dagid,
        description='IBIS User Import - Process Groups',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_groups,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        query_valid_delta_records_countries = rail.QueryCollectionOperator(
            name='valid_delta_countries',
            task_id='query_valid_delta_records_countries',
            query="""SELECT DISTINCT country_name, country_code FROM valid_records"""
        )

        get_all_countries = rail.RepliconServiceOperator(
            task_id='get_all_countries',
            endpoint='/services/LocationService1.svc/GetAllLocations',
        )

        create_replicon_countries_collection = rail.CreateCollectionOperator(
            task_id="create_replicon_countries_collection",
            columns=['displayText', 'parameterCorrelationId', 'slug', 'uri'],
            name="replicon_countries",
            source="{{ result('get_all_countries') | to_json }}"
        )

        query_countries_to_create = rail.QueryCollectionOperator(
            task_id='query_countries_to_create',
            query="""SELECT DISTINCT * FROM valid_delta_countries where LOWER(country_name) NOT IN
                    (SELECT DISTINCT LOWER(displayText) FROM replicon_countries)"""
        )

        has_new_countries = rail.IfOperator(
            task_id='has_new_countries',
            test="{{ result('query_countries_to_create','length') > 0 }}",
            yes_task='process_new_countries',
            no_task='finish'
        )

        process_new_countries = rail.TriggerDagRunForEachItemOperator(
            task_id='process_new_countries',
            items="{{ result('query_countries_to_create') }}",
            trigger_dag_id=config.process_new_countries_dagid,
            conf={
                "filename": "{{ dag_run.conf.file_name }}",
                "country_name": "{{ item.country_name }}",
                "country_code": "{{ item.country_code }}",
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries = 0
        )

        wait_process_new_countries = rail.WaitForDagRunsSensor(
            task_id="wait_process_new_countries",
            dag_runs="{{result('process_new_countries')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        query_distinct_departments_full_paths_from_validrecords = rail.QueryCollectionOperator(
            task_id='query_distinct_departments_full_paths_from_validrecords',
            name='distinct_departments',
            query="""SELECT DISTINCT dept_full_path FROM valid_records WHERE NULLIF(dept_full_path,"") is NOT NULL"""
        )

        def get_converted_departments_data(item):
            if not item:
                return []
            split_departments = item['dept_full_path'].split(GROUPS_DELIMITER)
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
            data_handler=response_filter.filter_departments_data
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

        query_valid_delta_records_employee_types = rail.QueryCollectionOperator(
            name='valid_delta_employee_types',
            task_id='query_valid_delta_records_employee_types',
            query="""SELECT DISTINCT employee_type FROM valid_records WHERE NULLIF(employee_type,"") is NOT NULL"""
        )

        get_all_employee_types = rail.RepliconServiceOperator(
            task_id='get_all_employee_types',
           endpoint="services/EmployeeTypeGroupListService1.svc/GetData",
            data=request_payload.get_all_employee_grp_payload,
            data_handler=response_filter.filter_group_data
        )

        create_replicon_employee_types_collection = rail.CreateCollectionOperator(
            task_id="create_replicon_employee_types_collection",
            columns=['name', 'uri'],
            name="replicon_employee_types",
            source="{{ result('get_all_employee_types') | to_json }}"
        )

        query_employee_types_to_create = rail.QueryCollectionOperator(
            task_id='query_employee_types_to_create',
            query="""SELECT DISTINCT * FROM valid_delta_employee_types where LOWER(employee_type) NOT IN
                    (SELECT DISTINCT LOWER(name) FROM replicon_employee_types)"""
        )

        has_new_employee_types = rail.IfOperator(
            task_id='has_new_employee_types',
            test="{{ result('query_employee_types_to_create','length') > 0 }}",
            yes_task='process_new_employee_types',
            no_task='finish'
        )

        process_new_employee_types = rail.TriggerDagRunForEachItemOperator(
            task_id='process_new_employee_types',
            items="{{ result('query_employee_types_to_create') }}",
            trigger_dag_id=config.process_new_employee_types_dagid,
            conf={
                "filename": "{{ dag_run.conf.file_name }}",
                "employee_type": "{{ item.employee_type }}"
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries = 0
        )

        wait_process_new_employee_types = rail.WaitForDagRunsSensor(
            task_id="wait_process_new_employee_typess",
            dag_runs="{{result('process_new_employee_types')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        query_valid_delta_records_work_location = rail.QueryCollectionOperator(
            name='valid_delta_work_location',
            task_id='query_valid_delta_records_work_location',
            query="""SELECT DISTINCT work_location_name FROM valid_records WHERE NULLIF(work_location_name,"") is NOT NULL"""
        )

        get_all_work_location = rail.RepliconServiceOperator(
            task_id="get_all_work_location",
            endpoint="/services/DivisionListService1.svc/GetData",
            data={
                    "page": "1",
                    "pagesize": "100000",
                    "columnUris": [
                        "urn:replicon:division-list-column:division"
                    ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=response_filter.filter_group_data
        )

        create_replicon_work_location_collection = rail.CreateCollectionOperator(
            task_id="create_replicon_work_location_collection",
            columns=['name', 'uri'],
            source="{{ result ('get_all_work_location') | to_json }}",
            name="replicon_work_location"
        )

        query_work_location_to_create = rail.QueryCollectionOperator(
            task_id='query_work_location_to_create',
            query="""SELECT DISTINCT * FROM valid_delta_work_location where LOWER(work_location_name) NOT IN
                    (SELECT DISTINCT LOWER(name) FROM replicon_work_location)"""
        )

        has_new_work_location = rail.IfOperator(
            task_id='has_new_work_location',
            test="{{ result('query_work_location_to_create','length') > 0 }}",
            yes_task='process_new_work_location',
            no_task='finish'
        )

        process_new_work_location = rail.TriggerDagRunForEachItemOperator(
            task_id='process_new_work_location',
            items=lambda: rail.result('query_work_location_to_create'),
            trigger_dag_id=config.process_new_work_location_dagid,
            conf={
                "filename": "{{ dag_run.conf.file_name }}",
                "work_location_name": "{{ item.work_location_name }}",
            },
            retries = 0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        wait_process_new_work_location = rail.WaitForDagRunsSensor(
            task_id="wait_process_new_work_location",
            dag_runs="{{result('process_new_work_location')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        query_valid_delta_records_standard_hours = rail.QueryCollectionOperator(
            name='valid_delta_standard_hours',
            task_id='query_valid_delta_records_standard_hours',
            query="""SELECT DISTINCT standard_hours FROM valid_records WHERE NULLIF(standard_hours,"") is NOT NULL"""
        )

        get_all_standard_hours = rail.RepliconServiceOperator(
            task_id="get_all_standard_hours",
            endpoint="/services/ServiceCenterListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:service-center-list-column:service-center"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=response_filter.filter_group_data
        )

        create_replicon_standard_hours_collection = rail.CreateCollectionOperator(
            task_id="create_replicon_standard_hours_collection",
            columns=['name', 'uri'],
            source="{{ result ('get_all_standard_hours') | to_json }}",
            name="replicon_standard_hours"
        )

        query_standard_hours_to_create = rail.QueryCollectionOperator(
            task_id='query_standard_hours_to_create',
            query="""SELECT DISTINCT * FROM valid_delta_standard_hours where LOWER(standard_hours) NOT IN
                    (SELECT DISTINCT LOWER(name) FROM replicon_standard_hours)"""
        )

        has_new_standard_hours = rail.IfOperator(
            task_id='has_new_standard_hours',
            test="{{ result('query_standard_hours_to_create','length') > 0 }}",
            yes_task='process_new_standard_hours',
            no_task='finish'
        )

        process_new_standard_hours = rail.TriggerDagRunForEachItemOperator(
            task_id='process_new_standard_hours',
            items=lambda: rail.result('query_standard_hours_to_create'),
            trigger_dag_id=config.process_new_standard_hours_dagid,
            conf={
                "filename": "{{ dag_run.conf.file_name }}",
                "standard_hours": "{{ item.standard_hours }}"
            },
            retries = 0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        wait_process_new_standard_hours = rail.WaitForDagRunsSensor(
            task_id="wait_process_new_standard_hours",
            dag_runs="{{result('process_new_standard_hours')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        query_valid_delta_records_full_part_time = rail.QueryCollectionOperator(
            name='valid_delta_full_part_time',
            task_id='query_valid_delta_records_full_part_time',
            query="""SELECT DISTINCT full_part_time FROM valid_records WHERE NULLIF(full_part_time,"") is NOT NULL"""
        )

        get_all_full_part_time = rail.RepliconServiceOperator(
            task_id="get_all_full_part_time",
            endpoint='/services/CostCenterListService1.svc/GetData',
            data=request_payload.get_costcenter_payload,
            data_handler=response_filter.filter_group_data
        )

        create_replicon_full_part_time_collection = rail.CreateCollectionOperator(
            task_id="create_replicon_full_part_time_collection",
            columns=['name', 'uri'],
            source="{{ result ('get_all_full_part_time') | to_json }}",
            name="replicon_full_part_time"
        )

        query_full_part_time_to_create = rail.QueryCollectionOperator(
            task_id='query_full_part_time_to_create',
            query="""SELECT DISTINCT * FROM valid_delta_full_part_time where LOWER(full_part_time) NOT IN
                    (SELECT DISTINCT LOWER(name) FROM replicon_full_part_time)"""
        )

        has_new_full_part_time = rail.IfOperator(
            task_id='has_new_full_part_time',
            test="{{ result('query_full_part_time_to_create','length') > 0 }}",
            yes_task='process_new_full_part_time',
            no_task='finish'
        )

        process_new_full_part_time = rail.TriggerDagRunForEachItemOperator(
            task_id='process_new_full_part_time',
            items=lambda: rail.result('query_full_part_time_to_create'),
            trigger_dag_id=config.process_new_full_part_time_dagid,
            conf={
                "filename": "{{ dag_run.conf.file_name }}",
                "full_part_time": "{{ item.full_part_time }}"
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        wait_process_new_full_part_time = rail.WaitForDagRunsSensor(
            task_id="wait_process_new_full_part_time",
            dag_runs="{{result('process_new_full_part_time')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )
        query_valid_delta_records_countries >> get_all_countries >> create_replicon_countries_collection >> query_countries_to_create
        query_countries_to_create >> has_new_countries >> rail.Label('No') >> finish
        has_new_countries >> rail.Label('Yes') >> process_new_countries >> wait_process_new_countries >> finish

        query_distinct_departments_full_paths_from_validrecords >> convert_department_data >> converted_department_data_collection >> get_all_department_grps
        get_all_department_grps >> create_replicon_departments_collection >> query_departments_to_create
        query_departments_to_create >> has_new_departments >> rail.Label('No') >> finish
        has_new_departments >> rail.Label('Yes') >> process_new_departments >> wait_process_new_departments >> finish

        query_valid_delta_records_employee_types >> get_all_employee_types >> create_replicon_employee_types_collection >> query_employee_types_to_create
        query_employee_types_to_create >> has_new_employee_types >> rail.Label('No') >> finish
        has_new_employee_types >> rail.Label('Yes') >> process_new_employee_types >> wait_process_new_employee_types >> finish

        query_valid_delta_records_work_location >> get_all_work_location >> create_replicon_work_location_collection >> query_work_location_to_create
        query_work_location_to_create >> has_new_work_location >> rail.Label('No') >> finish
        has_new_work_location >> rail.Label('Yes') >> process_new_work_location >> wait_process_new_work_location >> finish

        query_valid_delta_records_standard_hours >> get_all_standard_hours >> create_replicon_standard_hours_collection >> query_standard_hours_to_create
        query_standard_hours_to_create >> has_new_standard_hours >> rail.Label('No') >> finish
        has_new_standard_hours >> rail.Label('Yes') >> process_new_standard_hours >> wait_process_new_standard_hours >> finish

        query_valid_delta_records_full_part_time >> get_all_full_part_time >> create_replicon_full_part_time_collection >> query_full_part_time_to_create
        query_full_part_time_to_create >> has_new_full_part_time >> rail.Label('No') >> finish
        has_new_full_part_time >> rail.Label('Yes') >> process_new_full_part_time >> wait_process_new_full_part_time >> finish

    return dag

rail.for_each_instance(create_child_dag)
