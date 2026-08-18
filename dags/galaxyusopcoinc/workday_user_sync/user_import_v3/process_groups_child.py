from datetime import timedelta
import rail
from galaxyusopcoinc.workday_user_sync.user_import_v3.utils import request_payload, response_filter
null = None

# pylint: disable=too-many-statements


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_groups_dag_id,
        description=f'VialtoPartners_User_Import_ process_groups add V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_run_groups_child,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_service_centers = rail.RepliconServiceOperator(
            task_id="get_service_centers",
            endpoint="/services/ServiceCenterListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:service-center-list-column:name",
                    "urn:replicon:service-center-list-column:code",
                    "urn:replicon:service-center-list-column:description",
                    "urn:replicon:service-center-list-column:service-center"
                ],
                "sort": [],
                "filterExpression": None
            },
            data_handler=response_filter.get_service_centers_date_handler
        )

        create_replicon_service_center_collection = rail.CreateCollectionOperator(
            task_id="create_replicon_service_center_collection",
            source="{{ result ('get_service_centers') | to_json }}",
            name="replicon_service_center"
        )

        query_service_center_details = rail.QueryCollectionOperator(
            task_id="query_service_center_details",
            query="""SELECT Company as service_center_name, CompanyCode as service_center_code, Country as description FROM queryuserimportdata""",
            name="service_center_details"
        )

        query_service_center_to_create = rail.QueryCollectionOperator(
            task_id="query_service_center_to_create",
            query="""SELECT DISTINCT * FROM service_center_details WHERE service_center_code NOT IN (SELECT code FROM replicon_service_center)""",
            name="service_center_to_create"
        )

        query_service_center_to_update = rail.QueryCollectionOperator(
            task_id="query_service_center_to_update",
            query="""SELECT DISTINCT * FROM replicon_service_center
            rsc INNER JOIN service_center_details scd ON rsc.code == scd.service_center_code AND
            rsc.name != (scd.service_center_name || ' (' || scd.service_center_code || ')')""",  # GROUP BY scd.service_center_code""",
            name="service_center_to_update",
        )

        create_service_centers = rail.TriggerDagRunForEachItemOperator(
            task_id="create_service_centers",
            trigger_dag_id=config.service_center_dag_id,
            items="{{ result('query_service_center_to_create') }}",
            conf={
                "file_name": "{{ dag_run.conf.file_name}}",
                "name": "{{ item.service_center_name }}",
                "code": "{{ item.service_center_code }}",
                "description": "{{ item.description }}",
                "uri": "",
                "action": "add"
            },
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_create_service_center = rail.WaitForDagRunsSensor(
            task_id="wait_for_create_service_center",
            dag_runs="{{ result('create_service_centers')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        update_service_centers = rail.TriggerDagRunForEachItemOperator(
            task_id="update_service_centers",
            trigger_dag_id=config.service_center_dag_id,
            items="{{ result('query_service_center_to_update') }}",
            conf={
                "file_name": "{{ dag_run.conf.file_name}}",
                "name": "{{ item.service_center_name }}",
                "old_name": "{{ item.name }}",
                "code": "{{ item.code }}",
                "description": "{{ item.description }}",
                "uri": "{{ item.uri }}",
                "action": "update"
            },
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_update_service_centers = rail.WaitForDagRunsSensor(
            task_id="wait_for_update_service_centers",
            dag_runs="{{ result('update_service_centers')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        get_all_locations = rail.RepliconServiceOperator(
            task_id='get_all_locations',
            endpoint='/services/LocationService1.svc/GetAllLocations',
        )

        create_location_collection = rail.CreateCollectionOperator(
            task_id="create_location_collection",
            name="replicon_getall_location",
            source="{{ result('get_all_locations') | to_json }}"
        )

        query_newparent_locations = rail.QueryCollectionOperator(
            task_id='query_newparent_locations',
            query='''SELECT DISTINCT * FROM query_parent_locations
                    WHERE NULLIF(Country, '') IS NOT NULL AND LOWER(Country) NOT IN
                    (SELECT DISTINCT LOWER(displayText) FROM replicon_getall_location)'''
        )

        query_newchild_locations = rail.QueryCollectionOperator(
            task_id='query_newchild_locations',
            query='''SELECT DISTINCT * FROM query_child_locations
                    WHERE NULLIF(Location, '') IS NOT NULL AND LOWER(Location) NOT IN
                    (SELECT DISTINCT LOWER(displayText) FROM replicon_getall_location) GROUP BY Location'''
        )

        has_newparent_locations = rail.IfOperator(
            task_id='has_newparent_locations',
            test='{{ result("query_newparent_locations", "length") > 0 }}',
            yes_task='process_newparent_locations',
            no_task='query_newchild_locations'
        )

        process_newparent_locations = rail.TriggerDagRunForEachItemOperator(
            task_id='process_newparent_locations',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            items=lambda: rail.result('query_newparent_locations'),
            trigger_dag_id=config.location_dag_id,
            conf={
                "file_name": "{{ dag_run.conf.file_name }}",
                'is_parent_location': "Yes",
                "parent_location_name": "{{ item.Country }}",
                "location_name": "{{ item.Country }}"
            }
        )

        wait_for_process_newparent_location = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_newparent_location',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_newparent_locations") }}',
        )

        has_newchild_locations = rail.IfOperator(
            task_id='has_newchild_locations',
            test='{{ result("query_newchild_locations", "length") > 0 }}',
            yes_task='process_new_locations',
            no_task='finish'
        )

        process_new_locations = rail.TriggerDagRunForEachItemOperator(
            task_id='process_new_locations',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            items=lambda: rail.result('query_newchild_locations'),
            trigger_dag_id=config.location_dag_id,
            conf={
                "file_name": "{{ dag_run.conf.file_name }}",
                'is_parent_location': "No",
                "parent_location_name": "{{ item.Country }}",
                "location_name": "{{ item.Location }}"
            }
        )

        wait_for_process_new_location = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_new_location',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_new_locations") }}',
        )

        get_all_costcenter = rail.RepliconServiceOperator(
            task_id='get_all_costcenter',
            endpoint='/services/CostCenterListService1.svc/GetData',
            data=request_payload.get_costcenter_payload,
            response_filter=response_filter.map_list_data
        )

        create_replicon_cost_costcenter_collection = rail.CreateCollectionOperator(
            task_id="create_replicon_cost_costcenter_collection",
            name="replicon_costcenter",
            source="{{ result('get_all_costcenter') | to_json }}"
        )

        query_new_costcenter = rail.QueryCollectionOperator(
            task_id='query_new_costcenter',
            query='''SELECT DISTINCT * FROM query_distinct_costcenter
                    WHERE NULLIF(CostCenterID, '') IS NOT NULL AND LOWER(CostCenterID) NOT IN
                    (SELECT DISTINCT LOWER(code) FROM replicon_costcenter) GROUP BY CostCenterID'''
        )

        has_new_costcenter = rail.IfOperator(
            task_id='has_new_costcenter',
            test='{{ result("query_new_costcenter", "length") > 0 }}',
            yes_task='process_new_costcenter',
            no_task='query_cost_center_to_update'
        )

        process_new_costcenter = rail.TriggerDagRunForEachItemOperator(
            task_id='process_new_costcenter',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            items="{{result('query_new_costcenter')}}",
            trigger_dag_id=config.costcenter_dag_id,
            conf={
                'file_name': "{{ dag_run.conf,file_name }}",
                'cost_center_name': '{{ item.CostCenterName }}',
                'cost_center_code': '{{ item.CostCenterID }}',
                'action': 'add'
            }
        )

        wait_for_process_new_costcenter = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_new_costcenter',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_new_costcenter") }}',
        )

        query_cost_center_to_update = rail.QueryCollectionOperator(
            task_id="query_cost_center_to_update",
            query="""SELECT DISTINCT * FROM replicon_costcenter rc INNER JOIN query_distinct_costcenter qdc
                    ON rc.code == QDC.CostCenterID AND rc.name != (qdc.CostCenterName || ' (' || qdc.CostCenterID || ')') GROUP BY qdc.CostCenterID""",
            name="cost_center_to_update",
        )

        process_update_cost_center = rail.TriggerDagRunForEachItemOperator(
            task_id='process_update_cost_center',
            items="{{result('query_cost_center_to_update')}}",
            trigger_dag_id=config.costcenter_dag_id,
            conf={
                "action": 'update',
                'cost_center_name': '{{ item.CostCenterName }}',
                'cost_center_code': '{{ item.CostCenterID }}',
                'replicon_cost_center_name': '{{item.name}}',
                'cost_center_uri': "{{ item.uri }}"
            },
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_process_update_cost_center = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_update_cost_center',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_update_cost_center") }}',
        )

        get_dept_group = rail.RepliconServiceOperator(
            task_id='get_dept_group',
            endpoint='/services/DepartmentGroupListService1.svc/GetData',
            data=request_payload.get_dept_group_payload,
            response_filter=response_filter.map_list_data
        )

        create_department_collection = rail.CreateCollectionOperator(
            task_id="create_department_collection",
            name="replicon_getall_department",
            source="{{ result('get_dept_group') | to_json }}"
        )

        query_newparent_deparments = rail.QueryCollectionOperator(
            task_id='query_newparent_deparments',
            query='''SELECT DISTINCT * FROM query_parent_department
                    WHERE NULLIF(JobFamilyGroup, '') IS NOT NULL AND LOWER(JobFamilyGroup) NOT IN
                    (SELECT DISTINCT LOWER(name) FROM replicon_getall_department) GROUP BY JobFamilyGroup'''
        )

        query_newchild_departments = rail.QueryCollectionOperator(
            task_id='query_newchild_departments',
            query='''SELECT DISTINCT * FROM query_child_department
                    WHERE NULLIF(JobFamily, '') IS NOT NULL AND LOWER(JobFamily) NOT IN
                    (SELECT DISTINCT LOWER(name) FROM replicon_getall_department) GROUP BY JobFamily'''
        )

        has_newparent_departments = rail.IfOperator(
            task_id='has_newparent_departments',
            test='{{ result("query_newparent_deparments", "length") > 0 }}',
            yes_task='process_newparent_departments',
            no_task='get_updateddept_group'
        )

        process_newparent_departments = rail.TriggerDagRunForEachItemOperator(
            task_id='process_newparent_departments',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            items=lambda: rail.result('query_newparent_deparments'),
            trigger_dag_id=config.department_dag_id,
            conf=lambda item: {
                "Parent": "Yes",
                'root': rail.result('get_dept_group')[0]['name'],
                'JobFamilyGroup': item['JobFamilyGroup']
            }
        )

        wait_for_process_newparent_department = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_newparent_department',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_newparent_departments") }}',
        )

        get_updateddept_group = rail.RepliconServiceOperator(
            task_id='get_updateddept_group',
            endpoint='/services/DepartmentGroupListService1.svc/GetData',
            data=request_payload.get_dept_group_payload,
            response_filter=response_filter.map_list_data
        )

        has_newchild_departments = rail.IfOperator(
            task_id='has_newchild_departments',
            test='{{ result("query_newchild_departments", "length") > 0 }}',
            yes_task='process_new_departments',
            no_task='finish'
        )

        process_new_departments = rail.TriggerDagRunForEachItemOperator(
            task_id='process_new_departments',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            items=lambda: rail.result('query_newchild_departments'),
            trigger_dag_id=config.department_dag_id,
            conf=lambda item: {
                "Parent": "No",
                'JobFamily': item['JobFamily'],
                'JobFamilyGroup': rail.find_first_by_attr_and_get_attr(
                    rail.result('get_updateddept_group'), 'name', item['JobFamilyGroup'], 'uri'),
            }
        )

        wait_for_process_new_department = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_new_department',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_new_departments") }}',
        )

        get_all_division_from_replicon = rail.RepliconServiceOperator(
            task_id="get_all_division_from_replicon",
            endpoint="/services/DivisionListService1.svc/GetData",
            data={
                    "page": "1",
                    "pagesize": "100000",
                    "columnUris": [
                        "urn:replicon:division-list-column:name",
                        "urn:replicon:division-list-column:division",
                        "urn:replicon:division-list-column:full-path"
                    ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=response_filter.get_all_division_from_replicon_filter
        )

        create_replicon_division_collection = rail.CreateCollectionOperator(
            task_id="create_replicon_division_collection",
            source="{{ result('get_all_division_from_replicon') | to_json }}",
            name="replicon_divisions"
        )

        query_divisions_field_from_feed = rail.QueryCollectionOperator(
            task_id="query_divisions_field_from_feed",
            query="SELECT JobCategory, WorkerType, EmployeeType, ManagementLevel FROM queryuserimportdata",
            name="division_feed_values"
        )

        def get_converted_divisions_data(item):
            if not item:
                return []
            return [
                {
                    "division_fullpath": item['JobCategory'],
                    "length": 1,
                    "division_name": item['JobCategory'],
                    "parent_full_path": item['JobCategory'],
                    "parent_name": item['JobCategory']
                },
                {
                    "division_fullpath": response_filter.get_full_path([item['JobCategory'], item['WorkerType']]),
                    "length": 2,
                    "division_name": item['WorkerType'],
                    "parent_full_path": item['JobCategory'],
                    "parent_name": item['JobCategory']
                },
                {
                    "division_fullpath": response_filter.get_full_path([item['JobCategory'], item['WorkerType'], item['EmployeeType']]),
                    "length": 3,
                    "division_name": item['EmployeeType'],
                    "parent_full_path": response_filter.get_full_path([item['JobCategory'], item['WorkerType']]),
                    "parent_name": item['WorkerType']
                },
                {
                    "division_fullpath": response_filter.get_full_path([item['JobCategory'],
                                                                        item['WorkerType'], item['EmployeeType'], item['ManagementLevel']]),
                    "length": 4,
                    "division_name": item['ManagementLevel'],
                    "parent_full_path": response_filter.get_full_path([item['JobCategory'], item['WorkerType'], item['EmployeeType']]),
                    "parent_name": item['EmployeeType']
                }
            ]

        convert_division_data = rail.DataAdaptorOperator(
            task_id="convert_division_data",
            source=lambda: rail.load_all_records(
                rail.result('query_divisions_field_from_feed')),
            columns=['division_fullpath', 'length',
                     'division_name', 'parent_full_path', 'parent_name'],
            data=get_converted_divisions_data
        )

        converted_division_data_collection = rail.CreateCollectionOperator(
            task_id="converted_division_data_collection",
            source="{{result('convert_division_data')}}",
            name="converted_feed_divisions"
        )

        query_divisions_not_present_in_replicon = rail.QueryCollectionOperator(
            task_id="query_divisions_not_present_in_replicon",
            query="""SELECT DISTINCT * FROM converted_feed_divisions WHERE LOWER(division_fullpath) NOT IN
                    (SELECT DISTINCT LOWER(full_path) FROM replicon_divisions ) ORDER BY length""",
            name="divisions_to_add"
        )
        has_any_division_to_add = rail.IfOperator(
            task_id="has_any_division_to_add",
            test="{{result('query_divisions_not_present_in_replicon','length')>0}}",
            yes_task="add_division_by_level",
            no_task="finish"
        )

        add_division_by_level = rail.TriggerDagRunForEachItemOperator(
            task_id="add_division_by_level",
            items="{{result('query_divisions_not_present_in_replicon')}}",
            trigger_dag_id=config.division_dag_id,
            conf=lambda item: {
                "name": item['division_name'],
                "full_path": item['division_fullpath'],
                "parent_division_full_path": item['parent_full_path'],
                "parent_name": item['parent_name'],
                "length": item['length']
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

        wait_for_add_divisions_by_level = rail.WaitForDagRunsSensor(
            task_id="wait_for_add_divisions_by_level",
            dag_runs="{{result('add_division_by_level')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        finish = rail.EmptyOperator(
            task_id="finish"
        )

        get_employee_types_from_replicon = rail.RepliconServiceOperator(
            task_id="get_employee_types_from_replicon",
            endpoint="/services/EmployeeTypeGroupListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                    "columnUris": [
                        "urn:replicon:employee-type-group-list-column:name",
                        "urn:replicon:employee-type-group-list-column:employee-type-group",
                        "urn:replicon:employee-type-group-list-column:full-path"
                    ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=response_filter.get_all_employee_type_from_replicon_filter
        )

        create_emp_type_collection = rail.CreateCollectionOperator(
            task_id="create_emp_type_collection",
            source="{{result('get_employee_types_from_replicon') | to_json }}",
            name="replicon_emp_types"
        )

        get_employee_type_data_from_feed = rail.QueryCollectionOperator(
            task_id="get_employee_type_data_from_feed",
            query="""SELECT CompensationGrade, WorkerType, ContractType, EmployeeType FROM queryuserimportdata""",
            name="feed_employee_type_data"
        )

        def convert_employee_type_data_handler(item):
            def get_data_for_employee():
                return [
                    {
                        "employee_type_full_path": item['CompensationGrade'],
                        "emp_type": item['CompensationGrade'],
                        "parent_full_path": item['CompensationGrade'],
                        "parent_name": item['CompensationGrade'],
                        "length": 1
                    },
                    {
                        "employee_type_full_path": response_filter.get_full_path([item['CompensationGrade'], item['WorkerType']]),
                        "emp_type": item['WorkerType'],
                        "parent_full_path": item['CompensationGrade'],
                        "parent_name": item['CompensationGrade'],
                        "length": 2
                    },
                    {
                        "employee_type_full_path": response_filter.get_full_path([item['CompensationGrade'], item['WorkerType'], item['ContractType']]),
                        "emp_type": item['ContractType'],
                        "parent_full_path": response_filter.get_full_path([item['CompensationGrade'], item['WorkerType']]),
                        "parent_name": item['WorkerType'],
                        "length": 3
                    },
                    {
                        "employee_type_full_path": response_filter.get_full_path([item['CompensationGrade'],
                                                                                  item['WorkerType'], item['ContractType'], item['EmployeeType']]),
                        "emp_type": item['EmployeeType'],
                        "parent_full_path": response_filter.get_full_path([item['CompensationGrade'], item['WorkerType'], item['ContractType']]),
                        "parent_name": item['ContractType'],
                        "length": 4
                    }
                ]

            def get_data_for_worker():
                return [
                    {
                        "employee_type_full_path": response_filter.get_full_path([item['WorkerType']]),
                        "emp_type": item['WorkerType'],
                        "parent_full_path": item['WorkerType'],
                        "parent_name": item['WorkerType'],
                        "length": 1
                    },
                    {
                        "employee_type_full_path": response_filter.get_full_path([item['WorkerType'], item['ContractType']]),
                        "emp_type": item['ContractType'],
                        "parent_full_path": response_filter.get_full_path([item['WorkerType']]),
                        "parent_name": item['WorkerType'],
                        "length": 2
                    },
                    {
                        "employee_type_full_path": response_filter.get_full_path([item['WorkerType'], item['ContractType'], item['EmployeeType']]),
                        "emp_type": item['EmployeeType'],
                        "parent_full_path": response_filter.get_full_path([item['WorkerType'], item['ContractType']]),
                        "parent_name": item['ContractType'],
                        "length": 3
                    }
                ]

            if not item:
                return []
            return get_data_for_employee() if item['WorkerType'] == "Employee" else get_data_for_worker()

        convert_employee_type_data = rail.DataAdaptorOperator(
            task_id="convert_employee_type_data",
            source="{{result('get_employee_type_data_from_feed')}}",
            columns=['employee_type_full_path', 'emp_type',
                     'parent_full_path', 'parent_name', 'length'],
            data=convert_employee_type_data_handler
        )

        create_converted_employee_data_collection = rail.CreateCollectionOperator(
            task_id="create_converted_employee_data_collection",
            source="{{ result('convert_employee_type_data') }}",
            name="converted_employee_data_feed"
        )

        query_employee_type_to_create = rail.QueryCollectionOperator(
            task_id="query_employee_type_to_create",
            query="""SELECT DISTINCT * FROM converted_employee_data_feed
            WHERE employee_type_full_path NOT IN (SELECT full_path FROM replicon_emp_types) ORDER BY length"""
        )

        create_employee_types = rail.TriggerDagRunForEachItemOperator(
            task_id="create_employee_types",
            trigger_dag_id=config.employee_type_dag_id,
            items="{{ result('query_employee_type_to_create')}}",
            conf=lambda item: {
                "name": item['emp_type'],
                "full_path": item['employee_type_full_path'],
                "parent_employee_full_path": item['parent_full_path'],
                "parent_name": item['parent_name'],
                "length": item['length']
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0

        )

        wait_for_create_employee_types = rail.WaitForDagRunsSensor(
            task_id="wait_for_create_employee_types",
            dag_runs="{{ result('create_employee_types')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_employee_types_from_replicon >> create_emp_type_collection >> get_employee_type_data_from_feed >> convert_employee_type_data\
            >> create_converted_employee_data_collection >> query_employee_type_to_create >> create_employee_types >> wait_for_create_employee_types
        wait_for_create_employee_types >> finish

        get_service_centers >> create_replicon_service_center_collection >> query_service_center_details >> query_service_center_to_create\
            >> create_service_centers >> wait_for_create_service_center >> query_service_center_to_update\
            >> update_service_centers >> wait_for_update_service_centers >> finish

        get_all_locations >> create_location_collection >> query_newparent_locations >> \
            has_newparent_locations >> rail.Label(
                "No") >> query_newchild_locations
        has_newparent_locations >> rail.Label(
            "Yes") >> process_newparent_locations >> wait_for_process_newparent_location >> query_newchild_locations

        query_newchild_locations >> has_newchild_locations >> rail.Label(
            "No") >> finish
        has_newchild_locations >> rail.Label(
            "Yes") >> process_new_locations >> wait_for_process_new_location >> finish

        get_all_costcenter >> create_replicon_cost_costcenter_collection >> query_new_costcenter >> has_new_costcenter >> rail.Label(
            "No") >> query_cost_center_to_update
        has_new_costcenter >> rail.Label(
            "Yes") >> process_new_costcenter >> wait_for_process_new_costcenter >> query_cost_center_to_update\
            >> process_update_cost_center >> wait_for_process_update_cost_center >> finish

        get_dept_group >> create_department_collection >> query_newparent_deparments >> \
            has_newparent_departments >> rail.Label(
                "No") >> get_updateddept_group
        has_newparent_departments >> rail.Label(
            "Yes") >> process_newparent_departments >> wait_for_process_newparent_department >> get_updateddept_group >> query_newchild_departments

        query_newchild_departments >> has_newchild_departments >> rail.Label(
            "No") >> finish
        has_newchild_departments >> rail.Label(
            "Yes") >> process_new_departments >> wait_for_process_new_department >> finish

        get_all_division_from_replicon >> create_replicon_division_collection >> query_divisions_field_from_feed >> convert_division_data\
            >> converted_division_data_collection >> query_divisions_not_present_in_replicon >> has_any_division_to_add >> rail.Label(
                "Yes") >> add_division_by_level >> wait_for_add_divisions_by_level >> finish
        has_any_division_to_add >> rail.Label("No") >> finish

    return dag


rail.for_each_instance(create_dag)
