from datetime import timedelta
import rail
from tsystems.project_import_file_based.utils import custom_methods, response_filter

def create_main_dag(config):
    """T-Systems Project Import Master DAG"""
    with rail.create_airflow_dag(
        dag_id=config.process_payload_dag_id,
        description="T-Systems Project Import Master DAG",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_max_active_run,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dag_run_conf"
        )

        create_main_log = rail.CreateLogOperator(
            task_id='create_main_log'
        )
        create_collection_input_data = rail.CreateCollectionOperator(
            task_id='create_collection_input_data',
            source=lambda dag_run: dag_run.conf['project_list'],
            name='inputdata',
            columns={
                "project_id": "project_code",
                "project_name": "project_name",
                "description": "description",
                "start_date": "start_date",
                "end_date": "end_date",
                "status": "status",
                "cost_center": "cost_center",
                "accounting_area": "accounting_area",
                "client_code": "client_code",
                "project_manager_id": "project_manager_id",
                "billing_type": "billing_type",
                "cost_type": "cost_type",
                "time_expense_entry": "time_expense_entry",
                "accounting_group": "accounting_group",
                "project_type": "project_type",
                "control_expert": "control_expert",
                "process_id_group": "process_id_group",
                "delivery_cost_center": "delivery_cost_center",
                "contract_type": "contract_type"
            }
        )

        has_collection_data = rail.IfOperator(
            task_id='has_collection_data',
            test="{{ result('create_collection_input_data', 'length') > 0 }}",
            yes_task='query_any_blankmandatory_check'
        )

        query_any_blankmandatory_check = rail.QueryCollectionOperator(
            task_id='query_any_blankmandatory_check',
            query="""SELECT * FROM inputdata WHERE
                NULLIF(project_code,'') IS NULL OR
                NULLIF(project_name,'') IS NULL OR
                NULLIF(start_date,'') IS NULL OR
                NULLIF(status,'') IS NULL OR
                NULLIF(cost_center,'') IS NULL OR
                NULLIF(accounting_area,'') IS NULL"""
        )

        has_any_blank_mandatory_field = rail.IfOperator(
            task_id='has_any_blank_mandatory_field',
            test="{{ result('query_any_blankmandatory_check', 'length') > 0 }}",
            yes_task='write_blank_mandatory_field_log',
            no_task='query_valid_data_from_rawdata'
        )

        write_blank_mandatory_field_log = rail.WriteLogOperator(
            task_id="write_blank_mandatory_field_log",
            items="{{result('query_any_blankmandatory_check')}}",
            log="{{ result('create_main_log') }}",
            severity="Skipped",
            message="mandatory field is not present",
            properties=lambda item: {
                "projectid": item.get('project_code', ''),
                "projectname": item.get('project_name', ''),
                "clientcode": item.get('client_code', ''),
                "action": "Validation",
                "status": "Exception",
                "details": custom_methods.get_missing_mandatory_fields_message(item)
            }
        )

        query_valid_data_from_rawdata = rail.QueryCollectionOperator(
            task_id='query_valid_data_from_rawdata',
            name='validprojectdata',
            query="""SELECT ROW_NUMBER() OVER(ORDER BY ROWID) AS record_id,* FROM inputdata WHERE
                NULLIF(project_code,'') IS NOT NULL AND
                NULLIF(project_name,'') IS NOT NULL AND
                NULLIF(start_date,'') IS NOT NULL AND
                NULLIF(status,'') IS NOT NULL AND
                NULLIF(cost_center,'') IS NOT NULL AND
                NULLIF(accounting_area,'') IS NOT NULL"""
        )

        has_valid_projects = rail.IfOperator(
            task_id='has_valid_projects',
            test="{{ result('query_valid_data_from_rawdata', 'length') > 0 }}",
            yes_task='gather_prerequisites',
            no_task='process_log_generation'
        )

        gather_prerequisites = rail.EmptyOperator(
            task_id='gather_prerequisites'
        )

        get_cost_center_as_department_groups = rail.RepliconServiceOperator(
            task_id='get_cost_center_as_department_groups',
            endpoint='/services/DepartmentGroupListService1.svc/GetData',
            data={
                "page": 1,
                "pagesize": 10000,
                "columnUris": [
                    "urn:replicon:department-group-list-column:department-group",
                    "urn:replicon:department-group-list-column:full-path",
                    "urn:replicon:department-group-list-column:full-path-code",
                    "urn:replicon:department-group-list-column:code"
                ],
                "sort": [],
                "filterExpression": None
            },
            data_handler=response_filter.get_existing_details_of_group
        )

        get_org_structure_as_location_groups = rail.RepliconServiceOperator(
            task_id='get_org_structure_as_location_groups',
            endpoint='/services/LocationListService1.svc/GetData',
            data={
                "page": 1,
                "pagesize": 10000,
                "columnUris": [
                    "urn:replicon:location-list-column:location",
                    "urn:replicon:location-list-column:full-path",
                    "urn:replicon:location-list-column:full-path-code",
                    "urn:replicon:location-list-column:code"
                ],
                "sort": [],
                "filterExpression": None
            },
            data_handler=response_filter.get_existing_details_of_group
        )

        get_service_centers_as_department_groups = rail.RepliconServiceOperator(
            task_id='get_service_centers_as_department_groups',
            endpoint='/services/ServiceCenterListService1.svc/GetData',
            data={
                "page": 1,
                "pagesize": 10000,
                "columnUris": [
                    "urn:replicon:service-center-list-column:service-center",
                    "urn:replicon:service-center-list-column:full-path",
                    "urn:replicon:service-center-list-column:full-path-code",
                    "urn:replicon:service-center-list-column:code"
                ],
                "sort": [],
                "filterExpression": None
            },
            data_handler=response_filter.get_existing_details_of_group
        )

        get_employee_type_groups = rail.RepliconServiceOperator(
            task_id='get_employee_type_groups',
            endpoint='/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails'
        )

        get_project_manager_permission_set = rail.RepliconServiceOperator(
            task_id='get_project_manager_permission_set',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets',
            data_handler=lambda resp: rail.find_first_by_attr_and_get_attr(
                resp, 'name', 'Project Manager', 'uri'
            )
        )

        get_project_oef_fields = rail.RepliconServiceOperator(
            task_id='get_project_oef_fields',
            endpoint='/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldBindings',
            data={
                "bindingContextUri": "urn:replicon:object-type:project"
            },
            data_handler= response_filter.get_project_oef_fields
        )

        get_oef_drop_down_project_oefs = rail.RepliconServiceCallForEachItemOperator(
            task_id="get_oef_drop_down_project_oefs",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            items="{{ result('get_project_oef_fields') | to_json}}",
            data=lambda item: {
                "objectExtensionTagDefinitionUri": item['uri'],
            },
            data_handler= lambda resp, item: response_filter.get_dropdown_uris_per_oef(resp, item['oef_name']),
        )

        query_distinct_clients = rail.QueryCollectionOperator(
            task_id='query_distinct_clients',
            name='distinctclients',
            query="""SELECT DISTINCT client_code,record_id from validprojectdata WHERE
                NULLIF(client_code, '') IS NOT NULL GROUP BY client_code"""
        )

        process_clients = rail.TriggerDagRunForEachItemOperator(
            task_id='process_clients',
            items='{{ result("query_distinct_clients") }}',
            trigger_dag_id=config.process_clients_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'client_code': item['client_code'],
                'main_log': rail.result("create_main_log")
            }
        )

        wait_for_process_clients = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_clients',
            dag_runs='{{ result("process_clients") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        process_projects = rail.trigger_parallel_dagrun(
            task_id='process_projects',
            items='{{ result("query_valid_data_from_rawdata") }}',
            parallel_count= config.parallel_count,
            trigger_dag_id=config.process_each_record_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: custom_methods.normalize_project_data(item, config.team_assignment_mapper)
        )

        finish_processing_projects = rail.EmptyOperator(
            task_id = 'finish_processing_projects'
        )

        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            trigger_dag_id=config.process_log_generation_dag_id,
            conf={
                'main_log': '{{ result("create_main_log") }}',
                'integration_type': 'Project Import',
                'log_file_name': '{{ dag_run.conf.source_file }}'
            },
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info=lambda dag_run: {
                "distinct_projects": rail.result("query_valid_data_from_rawdata", "length"),
                "distinct_clients": rail.result("query_distinct_clients", "length")
            }
        )

        create_main_log >> create_collection_input_data >> has_collection_data

        has_collection_data >> rail.Label("Yes") >> query_any_blankmandatory_check >> has_any_blank_mandatory_field

        has_any_blank_mandatory_field >> rail.Label("Yes") >> write_blank_mandatory_field_log >> query_valid_data_from_rawdata
        has_any_blank_mandatory_field >> rail.Label("No") >> query_valid_data_from_rawdata >> has_valid_projects

        has_valid_projects >> rail.Label("No") >> process_log_generation
        has_valid_projects >> rail.Label("Yes") >> gather_prerequisites

        gather_prerequisites >> get_cost_center_as_department_groups >> get_org_structure_as_location_groups >> get_service_centers_as_department_groups >> \
            get_employee_type_groups >> get_project_manager_permission_set >> get_project_oef_fields >> get_oef_drop_down_project_oefs >> query_distinct_clients
        
        query_distinct_clients >> process_clients >> wait_for_process_clients >> \
            process_projects >> finish_processing_projects >> process_log_generation >> log_to_sumo

    return dag

rail.for_each_instance(create_main_dag)