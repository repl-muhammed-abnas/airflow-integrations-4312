from datetime import timedelta
import rail

from crl.user_import_usa_v5.utils import response_filter

null = None
GROUPS_DELIMITER = '|'

# pylint: disable=too-many-statements
def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_groups_dagid,
        description='CRL User Import USA-Process Groups',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_groups,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        query_valid_delta_records_company_code = rail.QueryCollectionOperator(
            name='valid_delta_company_code',
            task_id='query_valid_delta_records_company_code',
            query="""SELECT DISTINCT company_code FROM valid_record WHERE emp_status!='Terminated'"""
        )

        get_all_company_code = rail.RepliconServiceOperator(
            task_id="get_all_company_code",
            endpoint="/services/ServiceCenterListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:service-center-list-column:name",
                    "urn:replicon:service-center-list-column:code",
                    "urn:replicon:service-center-list-column:service-center"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=response_filter.filter_group_data
        )

        create_replicon_company_code_collection = rail.CreateCollectionOperator(
            task_id="create_replicon_company_code_collection",
            columns=['code', 'name', 'uri'],
            source="{{ result ('get_all_company_code') | to_json }}",
            name="replicon_company_code"
        )

        query_company_code_to_create = rail.QueryCollectionOperator(
            task_id='query_company_code_to_create',
            query="""SELECT DISTINCT * FROM valid_delta_company_code where LOWER(company_code) NOT IN
                    (SELECT DISTINCT LOWER(name) FROM replicon_company_code)"""
        )

        has_new_company_code = rail.IfOperator(
            task_id='has_new_company_code',
            test="{{ result('query_company_code_to_create','length') > 0 }}",
            yes_task='process_new_company_code',
            no_task='finish'
        )

        process_new_company_code = rail.TriggerDagRunForEachItemOperator(
            task_id='process_new_company_code',
            items=lambda: rail.result('query_company_code_to_create'),
            trigger_dag_id=config.process_new_company_code_dagid,
            conf={
                "company_code_name": "{{ item.company_code }}"
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries = 0
        )

        wait_process_new_company_code = rail.WaitForDagRunsSensor(
            task_id="wait_process_new_company_code",
            dag_runs="{{result('process_new_company_code')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        query_distinct_location_full_path_from_valid_records = rail.QueryCollectionOperator(
            task_id='query_distinct_location_full_path_from_valid_records',
            name='distinct_location_full_path',
            query="""SELECT DISTINCT location_full_path FROM valid_record WHERE emp_status!='Terminated'"""
        )

        def get_converted_location_data(item):
            if not item:
                return []
            split_locations= item['location_full_path'].split(GROUPS_DELIMITER)
            return [
                {
                    "location_full_path": '|'.join(split_locations[:i+1]),
                    "length": len(('|'.join(split_locations[:i+1])).split(GROUPS_DELIMITER))
                }
                for i in range(len(split_locations))
            ]

        convert_locations_data = rail.DataAdaptorOperator(
            task_id="convert_locations_data",
            source="{{result('query_distinct_location_full_path_from_valid_records')}}",
            columns=['location_full_path', 'length'],
            data=get_converted_location_data
        )

        converted_location_data_collection = rail.CreateCollectionOperator(
            task_id="converted_location_data_collection",
            source="{{result('convert_locations_data')}}",
            name="converted_feed_locations"
        )

        get_all_location_grps = rail.RepliconServiceOperator(
            task_id="get_all_location_grps",
            endpoint="/services/LocationListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:location-list-column:name",
                    "urn:replicon:location-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=response_filter.filter_full_path_data
        )

        create_replicon_location_collection = rail.CreateCollectionOperator(
            task_id='create_replicon_location_collection',
            columns=['name', 'uri', 'full_path'],
            name="replicon_locations",
            source="{{ result('get_all_location_grps') | to_json }}",
        )

        query_locations_to_create = rail.QueryCollectionOperator(
            task_id="query_locations_to_create",
            query="""SELECT DISTINCT * FROM converted_feed_locations WHERE LOWER(location_full_path) NOT IN
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
            trigger_dag_id=config.process_new_locations_dagid,
            conf=lambda item, dag_run: {
                "location_name": item['location_full_path'].split(GROUPS_DELIMITER)[-1],
                "location_full_path": item['location_full_path'],
                "parent_location_full_path": GROUPS_DELIMITER.join(item['location_full_path'].split(GROUPS_DELIMITER)[0:-1])
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries = 0
        )

        wait_process_new_locations = rail.WaitForDagRunsSensor(
            task_id="wait_process_new_locations",
            dag_runs="{{result('process_new_locations')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        query_distinct_buisness_unit_full_path_from_valid_records = rail.QueryCollectionOperator(
            task_id='query_distinct_buisness_unit_full_path_from_valid_records',
            name='distinct_buisness_unit_full_path',
            query="""SELECT DISTINCT buisness_unit_full_path, buisness_unit_label FROM valid_record WHERE emp_status!='Terminated'"""
        )

        def get_converted_buissness_unit_data(item):
            if not item:
                return []
            split_buisness_unit= item['buisness_unit_full_path'].split(GROUPS_DELIMITER)
            return [
                {
                    "buisness_unit_full_path": '|'.join(split_buisness_unit[:i+1]),
                    "length": len(('|'.join(split_buisness_unit[:i+1])).split(GROUPS_DELIMITER)),
                    "buisness_unit_label": item['buisness_unit_label'] if i!=0 else null
                }
                for i in range(len(split_buisness_unit))
            ]

        convert_buisness_unit_data = rail.DataAdaptorOperator(
            task_id="convert_buisness_unit_data",
            source="{{result('query_distinct_buisness_unit_full_path_from_valid_records')}}",
            columns=['buisness_unit_full_path', 'length', 'buisness_unit_label'],
            data=get_converted_buissness_unit_data
        )

        converted_buisness_unit_data_collection = rail.CreateCollectionOperator(
            task_id="converted_buisness_unit_data_collection",
            source="{{result('convert_buisness_unit_data')}}",
            name="converted_feed_buisness_unit"
        )

        get_all_buisness_unit_grps = rail.RepliconServiceOperator(
            task_id="get_all_buisness_unit_grps",
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
            data_handler=response_filter.filter_full_path_data
        )

        create_replicon_buisness_unit_collection = rail.CreateCollectionOperator(
            task_id='create_replicon_buisness_unit_collection',
            columns=['name', 'uri', 'full_path'],
            name="replicon_buisness_unit",
            source="{{ result('get_all_buisness_unit_grps') | to_json }}",
        )

        query_buisness_unit_to_create = rail.QueryCollectionOperator(
            task_id="query_buisness_unit_to_create",
            query="""SELECT DISTINCT * FROM converted_feed_buisness_unit WHERE LOWER(buisness_unit_full_path) NOT IN
                    (SELECT DISTINCT LOWER(full_path) FROM replicon_buisness_unit ) ORDER BY length""",
            name="buisness_unit_to_add"
        )

        has_new_buisness_unit = rail.IfOperator(
            task_id='has_new_buisness_unit',
            test="{{ result('query_buisness_unit_to_create','length') > 0 }}",
            yes_task='process_new_buisness_unit',
            no_task='finish'
        )

        process_new_buisness_unit = rail.TriggerDagRunForEachItemOperator(
            task_id='process_new_buisness_unit',
            items=lambda: rail.result('query_buisness_unit_to_create'),
            trigger_dag_id=config.process_new_buisness_unit_dagid,
            conf=lambda item, dag_run: {
                "buisness_unit_name": item['buisness_unit_full_path'].split(GROUPS_DELIMITER)[-1],
                "buisness_unit_label": item['buisness_unit_label'],
                "buisness_unit_full_path": item['buisness_unit_full_path'],
                "parent_buisness_unit_full_path": GROUPS_DELIMITER.join(item['buisness_unit_full_path'].split(GROUPS_DELIMITER)[0:-1])
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries = 0
        )

        wait_process_new_buisness_unit = rail.WaitForDagRunsSensor(
            task_id="wait_process_new_buisness_unit",
            dag_runs="{{result('process_new_buisness_unit')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        query_distinct_cost_center_full_path_from_valid_records = rail.QueryCollectionOperator(
            task_id='query_distinct_cost_center_full_path_from_valid_records',
            name='distinct_cost_center_full_path',
            query="""SELECT DISTINCT cost_center_full_path, cost_center_label FROM valid_record WHERE emp_status!='Terminated'"""
        )

        def get_converted_cost_center_data(item):
            if not item:
                return []
            split_cost_center= item['cost_center_full_path'].split(GROUPS_DELIMITER)
            return [
                {
                    "cost_center_full_path": '|'.join(split_cost_center[:i+1]),
                    "length": len(('|'.join(split_cost_center[:i+1])).split(GROUPS_DELIMITER)),
                    "cost_center_label": item['cost_center_label'] if i==0 else null
                }
                for i in range(len(split_cost_center))
            ]

        convert_cost_center_data = rail.DataAdaptorOperator(
            task_id="convert_cost_center_data",
            source="{{result('query_distinct_cost_center_full_path_from_valid_records')}}",
            columns=['cost_center_full_path', 'length', 'cost_center_label'],
            data=get_converted_cost_center_data
        )

        converted_cost_center_data_collection = rail.CreateCollectionOperator(
            task_id="converted_cost_center_data_collection",
            source="{{result('convert_cost_center_data')}}",
            name="converted_feed_cost_center"
        )

        get_all_cost_center_grps = rail.RepliconServiceOperator(
            task_id="get_all_cost_center_grps",
            endpoint="/services/CostCenterListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:cost-center-list-column:name",
                    "urn:replicon:cost-center-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": null
                },
            data_handler=response_filter.filter_full_path_data
        )

        create_replicon_cost_center_collection = rail.CreateCollectionOperator(
            task_id='create_replicon_cost_center_collection',
            columns=['name', 'uri', 'full_path'],
            name="replicon_cost_center",
            source="{{ result('get_all_cost_center_grps') | to_json }}",
        )

        query_cost_center_to_create = rail.QueryCollectionOperator(
            task_id="query_cost_center_to_create",
            query="""SELECT DISTINCT * FROM converted_feed_cost_center WHERE LOWER(cost_center_full_path) NOT IN
                    (SELECT DISTINCT LOWER(full_path) FROM replicon_cost_center) ORDER BY length""",
            name="cost_center_to_add"
        )

        has_new_cost_center = rail.IfOperator(
            task_id='has_new_cost_center',
            test="{{ result('query_cost_center_to_create','length') > 0 }}",
            yes_task='process_new_cost_center',
            no_task='finish'
        )

        process_new_cost_center = rail.TriggerDagRunForEachItemOperator(
            task_id='process_new_cost_center',
            items=lambda: rail.result('query_cost_center_to_create'),
            trigger_dag_id=config.process_new_cost_center_dagid,
            conf=lambda item, dag_run: {
                "cost_center_name": item['cost_center_full_path'].split(GROUPS_DELIMITER)[-1],
                "cost_center_label": item['cost_center_label'],
                "cost_center_full_path": item['cost_center_full_path'],
                "parent_cost_center_full_path": GROUPS_DELIMITER.join(item['cost_center_full_path'].split(GROUPS_DELIMITER)[0:-1])
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries = 0
        )

        wait_process_new_cost_center = rail.WaitForDagRunsSensor(
            task_id="wait_process_new_cost_center",
            dag_runs="{{result('process_new_cost_center')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        query_valid_delta_records_departments = rail.QueryCollectionOperator(
            name='valid_delta_departments',
            task_id='query_valid_delta_records_departments',
            query="""SELECT DISTINCT department_name, department_code FROM valid_record WHERE emp_status!='Terminated'"""
        )

        get_all_department_grps = rail.RepliconServiceOperator(
            task_id="get_all_department_grps",
            endpoint="/services/DepartmentGroupService1.svc/GetAllDepartmentGroups",
        )

        create_replicon_departments_collection = rail.CreateCollectionOperator(
            task_id='create_replicon_departments_collection',
            columns=['displayText', 'parameterCorrelationId', 'slug', 'uri'],
            name="replicon_departments",
            source="{{ result('get_all_department_grps') | to_json }}",
        )

        query_departments_to_create = rail.QueryCollectionOperator(
            task_id='query_departments_to_create',
            query="""SELECT DISTINCT * FROM valid_delta_departments where LOWER(department_name) NOT IN
                    (SELECT DISTINCT LOWER(displayText) FROM replicon_departments)""",
            name='new_departments'
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
            trigger_dag_id=config.process_new_department_dagid,
            conf={
                "department_name": "{{ item.department_name }}",
                "department_code": "{{ item.department_code }}"
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        wait_process_new_departments = rail.WaitForDagRunsSensor(
            task_id="wait_process_new_departments",
            dag_runs="{{result('process_new_departments')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        query_valid_delta_records_company_code >> get_all_company_code >> create_replicon_company_code_collection >> query_company_code_to_create
        query_company_code_to_create >> has_new_company_code >> rail.Label('No') >> finish
        has_new_company_code >> rail.Label('Yes') >> process_new_company_code >> wait_process_new_company_code >> finish

        query_distinct_location_full_path_from_valid_records >> convert_locations_data >> converted_location_data_collection
        converted_location_data_collection >> get_all_location_grps >> create_replicon_location_collection >> query_locations_to_create
        query_locations_to_create >> has_new_locations >> rail.Label('No') >> finish
        has_new_locations >> rail.Label('Yes') >> process_new_locations >> wait_process_new_locations >> finish

        query_distinct_buisness_unit_full_path_from_valid_records >> convert_buisness_unit_data >> converted_buisness_unit_data_collection
        converted_buisness_unit_data_collection >> get_all_buisness_unit_grps >> create_replicon_buisness_unit_collection >> query_buisness_unit_to_create
        query_buisness_unit_to_create >> has_new_buisness_unit >> rail.Label('No') >> finish
        has_new_buisness_unit >> rail.Label('Yes') >> process_new_buisness_unit >> wait_process_new_buisness_unit >> finish

        query_distinct_cost_center_full_path_from_valid_records >> convert_cost_center_data >> converted_cost_center_data_collection
        converted_cost_center_data_collection >> get_all_cost_center_grps >> create_replicon_cost_center_collection >> query_cost_center_to_create
        query_cost_center_to_create >> has_new_cost_center >> rail.Label('No') >> finish
        has_new_cost_center >> rail.Label('Yes') >> process_new_cost_center >> wait_process_new_cost_center >> finish

        query_valid_delta_records_departments >> get_all_department_grps >> create_replicon_departments_collection >> query_departments_to_create
        query_departments_to_create >> has_new_departments >> rail.Label('Yes') >> process_new_departments >> wait_process_new_departments
        wait_process_new_departments >> finish
        has_new_departments >> rail.Label('No') >> finish

        return dag


rail.for_each_instance(create_child_dag)
