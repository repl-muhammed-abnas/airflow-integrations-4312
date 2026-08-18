from datetime import timedelta
import rail

from cohnreznick.user_sync.utils import response_filter,request_payload

null = None

# pylint: disable=too-many-statements
def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_groups_dag_id,
        description='Cohnreznick User Sync - Process Groups',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_groups,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        query_valid_delta_records_locations = rail.QueryCollectionOperator(
            name='valid_delta_locations',
            task_id='query_valid_delta_records_locations',
            query="""SELECT DISTINCT  locationname, locationcode FROM validrecords"""
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
            yes_task='dummy_process_new_locations',
            no_task='finish'
        )

        dummy_process_new_locations = rail.EmptyOperator(
            task_id='dummy_process_new_locations'
        )

        process_new_locations = rail.trigger_parallel_dagrun(
            task_id='process_new_locations',
            items=lambda: rail.result('query_locations_to_create'),
            parallel_count=config.trigger_parallel_dagrun_count_process_locations,
            trigger_dag_id=config.process_new_locations,
            conf={
                "filename": "{{ dag_run.conf.file_name }}",
                "locationname": "{{ item.locationname }}",
                "locationcode": "{{ item.locationcode }}"
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        query_valid_delta_records_departments = rail.QueryCollectionOperator(
            name='valid_delta_departments',
            task_id='query_valid_delta_records_departments',
            query="""SELECT DISTINCT departmentname, departmentcode FROM validrecords"""
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
            query="""SELECT DISTINCT * FROM valid_delta_departments where LOWER(departmentname) NOT IN
                    (SELECT DISTINCT LOWER(displayText) FROM replicon_departments)"""
        )

        has_new_departments = rail.IfOperator(
            task_id='has_new_departments',
            test="{{ result('query_departments_to_create','length') > 0 }}",
            yes_task='dummy_process_new_departments',
            no_task='finish'
        )

        dummy_process_new_departments = rail.EmptyOperator(
            task_id='dummy_process_new_departments'
        )

        process_new_departments = rail.trigger_parallel_dagrun(
            task_id='process_new_departments',
            items=lambda: rail.result('query_departments_to_create'),
            parallel_count=config.trigger_parallel_dagrun_count_process_departments,
            trigger_dag_id=config.process_new_departments,
            conf={
                "filename": "{{ dag_run.conf.file_name }}",
                "departmentname": "{{ item.departmentname }}",
                "departmentcode": "{{ item.departmentcode }}"
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        query_valid_delta_records_servicecenters = rail.QueryCollectionOperator(
            name='valid_delta_servicecenters',
            task_id='query_valid_delta_records_servicecenters',
            query="""SELECT DISTINCT servicecentercode, servicecentername FROM validrecords"""
        )

        get_all_service_centers = rail.RepliconServiceOperator(
            task_id="get_all_service_centers",
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
                "filterExpression": null
            },
            data_handler=response_filter.filter_servicecenters_data
        )

        create_replicon_service_center_collection = rail.CreateCollectionOperator(
            task_id="create_replicon_service_center_collection",
            columns=['code', 'description', 'name', 'uri'],
            source="{{ result ('get_all_service_centers') | to_json }}",
            name="replicon_servicecenters"
        )

        query_servicecenters_to_create = rail.QueryCollectionOperator(
            task_id='query_servicecenters_to_create',
            query="""SELECT DISTINCT * FROM valid_delta_servicecenters where LOWER(servicecentername) NOT IN
                    (SELECT DISTINCT LOWER(name) FROM replicon_servicecenters)"""
        )

        has_new_servicecenters = rail.IfOperator(
            task_id='has_new_servicecenters',
            test="{{ result('query_servicecenters_to_create','length') > 0 }}",
            yes_task='dummy_process_new_servicecenters',
            no_task='finish'
        )

        dummy_process_new_servicecenters = rail.EmptyOperator(
            task_id='dummy_process_new_servicecenters'
        )

        process_new_servicecenters = rail.trigger_parallel_dagrun(
            task_id='process_new_servicecenters',
            items=lambda: rail.result('query_servicecenters_to_create'),
            parallel_count=config.trigger_parallel_dagrun_count_process_servicecenters,
            trigger_dag_id=config.process_new_servicecenters,
            conf={
                "filename": "{{ dag_run.conf.file_name }}",
                "servicecentername": "{{ item.servicecentername }}",
                "servicecentercode": "{{ item.servicecentercode }}"
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        query_valid_delta_records_costcenters = rail.QueryCollectionOperator(
            name='valid_delta_costcenters',
            task_id='query_valid_delta_records_costcenters',
            query="""SELECT DISTINCT costcentercode, costcentername FROM validrecords"""
        )

        get_all_costcenters = rail.RepliconServiceOperator(
            task_id="get_all_costcenters",
            endpoint='/services/CostCenterListService1.svc/GetData',
            data=request_payload.get_costcenter_payload,
            data_handler=response_filter.filter_group_data
        )

        create_replicon_costcenters_collection = rail.CreateCollectionOperator(
            task_id="create_replicon_costcenters_collection",
            columns=['code', 'name', 'uri'],
            source="{{ result ('get_all_costcenters') | to_json }}",
            name="replicon_costcenters"
        )

        query_costcenters_to_create = rail.QueryCollectionOperator(
            task_id='query_costcenters_to_create',
            query="""SELECT DISTINCT * FROM valid_delta_costcenters where LOWER(costcentername) NOT IN
                    (SELECT DISTINCT LOWER(name) FROM replicon_costcenters)"""
        )

        has_new_costcenters = rail.IfOperator(
            task_id='has_new_costcenters',
            test="{{ result('query_costcenters_to_create','length') > 0 }}",
            yes_task='dummy_process_new_costcenters',
            no_task='finish'
        )

        dummy_process_new_costcenters = rail.EmptyOperator(
            task_id='dummy_process_new_costcenters'
        )

        process_new_costcenters = rail.trigger_parallel_dagrun(
            task_id='process_new_costcenters',
            items=lambda: rail.result('query_costcenters_to_create'),
            parallel_count=config.trigger_parallel_dagrun_count_process_costcenters,
            trigger_dag_id=config.process_new_costcenters,
            conf={
                "filename": "{{ dag_run.conf.file_name }}",
                "costcentername": "{{ item.costcentername }}",
                "costcentercode": "{{ item.costcentercode }}"
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        query_valid_delta_records_divisions = rail.QueryCollectionOperator(
            name='valid_delta_divisions',
            task_id='query_valid_delta_records_divisions',
            query="""SELECT DISTINCT divisionname, divisioncode FROM validrecords"""
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
            trigger_dag_id=config.process_new_divisions,
            conf={
                "filename": "{{ dag_run.conf.file_name }}",
                "divisionname": "{{ item.divisionname }}",
                "divisioncode": "{{ item.divisioncode }}"
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        query_valid_delta_records_locations >> get_all_locations >> create_replicon_location_collection >> query_locations_to_create
        query_locations_to_create >> has_new_locations >> rail.Label('No') >> finish
        has_new_locations >> rail.Label('Yes') >> dummy_process_new_locations >> process_new_locations >> finish

        query_valid_delta_records_departments >> get_all_department_grps >> create_replicon_departments_collection >> query_departments_to_create
        query_departments_to_create >> has_new_departments >> rail.Label('No') >> finish
        has_new_departments >> rail.Label('Yes') >> dummy_process_new_departments >> process_new_departments >> finish

        query_valid_delta_records_servicecenters >> get_all_service_centers >> create_replicon_service_center_collection >> query_servicecenters_to_create
        query_servicecenters_to_create >> has_new_servicecenters >> rail.Label('No') >> finish
        has_new_servicecenters >> rail.Label('Yes') >> dummy_process_new_servicecenters >> process_new_servicecenters >> finish

        query_valid_delta_records_costcenters >> get_all_costcenters >> create_replicon_costcenters_collection >> query_costcenters_to_create
        query_costcenters_to_create >> has_new_costcenters >> rail.Label('No') >> finish
        has_new_costcenters >> rail.Label('Yes') >> dummy_process_new_costcenters >> process_new_costcenters >> finish

        query_valid_delta_records_divisions >> get_all_divisions >> create_replicon_divisions_collection >> query_divisions_to_create
        query_divisions_to_create >> has_new_divisions >> rail.Label('No') >> finish
        has_new_divisions >> rail.Label('Yes') >> dummy_process_new_divisions >> process_new_divisions >> finish

    return dag

rail.for_each_instance(create_child_dag)
