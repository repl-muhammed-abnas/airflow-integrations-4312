from datetime import timedelta
import rail

from ttecholdingsinc.user_sync_v1.utils import response_filter

null = None
GROUPS_DELIMITER = '|'

# pylint: disable=too-many-statements
def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_groups_dagid,
        description='TTEC HOLDINGS INC - User Sync Process Groups',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_groups,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        query_valid_delta_records_departments = rail.QueryCollectionOperator(
            name='valid_delta_departments',
            task_id='query_valid_delta_records_departments',
            query="""SELECT DISTINCT department_name FROM validrecords"""
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
                    (SELECT DISTINCT LOWER(displayText) FROM replicon_departments)"""
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
            conf={
                "filename": "{{ dag_run.conf.file_name }}",
                "department_name": "{{ item.department_name }}",
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

        wait_process_new_departments = rail.WaitForDagRunsSensor(
            task_id="wait_process_new_departments",
            dag_runs="{{result('process_new_departments')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        query_valid_service_center_data_from_feed = rail.QueryCollectionOperator(
            task_id='query_valid_service_center_data_from_feed',
            name='valid_service_center_data',
            query="""SELECT * FROM validrecords WHERE NULLIF(client_code, '') IS NOT NULL and
                    NULLIF(client_name, '') IS NOT NULL and NULLIF(program_code, '') IS NOT NULL
                    and NULLIF(program_name, '') IS NOT NULL and NULLIF(project_name, '') IS NOT NULL"""
        )

        query_distinct_service_center_full_path = rail.QueryCollectionOperator(
            task_id='query_distinct_service_center_full_path',
            name='distinct_servicecenter_full_path',
            query="""SELECT DISTINCT client_name||'|'||program_name||'|'||project_name as servicecenter_full_path, client_code, program_code
                    FROM valid_service_center_data"""
        )

        def get_converted_service_center_date_data(item):
            if not item:
                return []
            split_servicecenter= item['servicecenter_full_path'].split(GROUPS_DELIMITER)
            return [
                {
                    "servicecenter_full_path": '|'.join(split_servicecenter[:i+1]),
                    "length": len(('|'.join(split_servicecenter[:i+1])).split(GROUPS_DELIMITER)),
                    "servicenter_code": item['client_code'] if i==0 else (item['program_code'] if i==1 else null)
                }
                for i in range(len(split_servicecenter))
            ]

        convert_servicecenter_data = rail.DataAdaptorOperator(
            task_id="convert_servicecenter_data",
            source="{{result('query_distinct_service_center_full_path')}}",
            columns=['servicecenter_full_path', 'length', 'servicenter_code'],
            data=get_converted_service_center_date_data
        )

        converted_servicecenter_data_collection = rail.CreateCollectionOperator(
            task_id="converted_servicecenter_data_collection",
            source="{{result('convert_servicecenter_data')}}",
            name="converted_feed_servicecenter"
        )

        get_all_servicecenter_grps = rail.RepliconServiceOperator(
            task_id="get_all_servicecenter_grps",
            endpoint="/services/ServiceCenterListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris":  [
                    "urn:replicon:service-center-list-column:name",
                    "urn:replicon:service-center-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=response_filter.groups_filter
        )

        create_replicon_servicecenter_collection = rail.CreateCollectionOperator(
            task_id='create_replicon_servicecenter_collection',
            columns=['name', 'uri', 'full_path'],
            name="replicon_servicecenter",
            source="{{ result('get_all_servicecenter_grps') | to_json }}",
        )

        query_servicecenter_to_create = rail.QueryCollectionOperator(
            task_id="query_servicecenter_to_create",
            query="""SELECT DISTINCT * FROM converted_feed_servicecenter WHERE LOWER(servicecenter_full_path) NOT IN
                    (SELECT DISTINCT LOWER(full_path) FROM replicon_servicecenter ) ORDER BY length""",
            name="servicecenter_to_add"
        )

        has_new_servicecenter = rail.IfOperator(
            task_id='has_new_servicecenter',
            test="{{ result('query_servicecenter_to_create','length') > 0 }}",
            yes_task='process_new_servicecenter',
            no_task='finish'
        )

        process_new_servicecenter = rail.TriggerDagRunForEachItemOperator(
            task_id='process_new_servicecenter',
            items=lambda: rail.result('query_servicecenter_to_create'),
            trigger_dag_id=config.process_new_servicecenter_dagid,
            conf=lambda item: {
                "servicecenter_name": item['servicecenter_full_path'].split(GROUPS_DELIMITER)[-1],
                "servicecenter_full_path": item['servicecenter_full_path'],
                "parent_servicecenter_full_path": GROUPS_DELIMITER.join(item['servicecenter_full_path'].split(GROUPS_DELIMITER)[0:-1]),
                "servicenter_code": item['servicenter_code'],
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries = 0
        )

        wait_process_new_servicecenter = rail.WaitForDagRunsSensor(
            task_id="wait_process_new_servicecenter",
            dag_runs="{{result('process_new_servicecenter')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        query_valid_delta_records_departments >> get_all_department_grps >> create_replicon_departments_collection >> query_departments_to_create
        query_departments_to_create >> has_new_departments >> rail.Label(
            'No') >> finish
        has_new_departments >> rail.Label(
            'Yes') >> process_new_departments >> wait_process_new_departments >> finish

        query_valid_service_center_data_from_feed >> query_distinct_service_center_full_path >> convert_servicecenter_data
        convert_servicecenter_data >> converted_servicecenter_data_collection >> get_all_servicecenter_grps >> create_replicon_servicecenter_collection
        create_replicon_servicecenter_collection >> query_servicecenter_to_create >> has_new_servicecenter
        has_new_servicecenter >> rail.Label('Yes') >> process_new_servicecenter >> wait_process_new_servicecenter >> finish
        has_new_servicecenter >> rail.Label('No') >> finish

        return dag


rail.for_each_instance(create_child_dag)
