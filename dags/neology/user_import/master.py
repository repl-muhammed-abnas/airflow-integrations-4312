from datetime import timedelta
import itertools
import pendulum
import rail
from neology.user_import.utils import request_payload, response_filters, custom_methods
from neology.user_import.tasks.send_logs import get_send_logs
from airflow.models import Variable
null = None

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dagid,
        description=f'Neology BambooHR to Polaris User Sync Master {config.instance}',
        company_key=config.company_key,
        start_date=pendulum.datetime(2025, 12, 1, tz=config.time_zone),
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        max_active_runs=config.master_dag_active_runs
    ) as dag:


        logging_details = rail.PythonOperator(
            task_id='logging_details',
            python_callable=custom_methods.logging_details,
            op_args=[config.time_zone]
        )

        get_lastsync_time_and_current_time = rail.PythonOperator(
            task_id='get_lastsync_time_and_current_time',
            python_callable=lambda: {
                "process_start_time": pendulum.now(config.time_zone).strftime(config.STANDARD_EMAIL_DATE_FORMAT),
                "last_synctime": Variable.get(config.last_synctime),
                "current_time": pendulum.now(config.time_zone).strftime(config.BAMBOOHR_LASTCHANGED_DATE_FORMAT)
            }
        )

        # Paginated BambooHR employee fields workflow (page_size=1000 is BambooHR max)
        bamboohr_get_employee_fields_first_page = rail.BambooHROperator(
            task_id='bamboohr_get_employee_fields_first_page',
            company_domain=config.bamboohr_domain,
            request_method='GET',
            endpoint="/datasets/employee/fields?page=1&page_size=1000",
            bamboohr_conn_id=config.bamboohr_conn_id
        )

        set_employee_fields_var = rail.SetVariableOperator(
            task_id='set_employee_fields_var',
            name="employee_fields",
            value=lambda: custom_methods.get_page_data(bamboohr_get_employee_fields_first_page.task_id, "fields")
        )

        has_more_fields_pages = rail.IfOperator(
            task_id='has_more_fields_pages',
            test=lambda: len(custom_methods.get_remaining_page_numbers("bamboohr_get_employee_fields_first_page")) > 0,
            yes_task='fetch_remaining_fields_pages',
            no_task='get_employee_fields_var'
        )

        fetch_remaining_fields_pages = rail.ForEachOperator(
            task_id='fetch_remaining_fields_pages',
            items=lambda: custom_methods.get_remaining_page_numbers("bamboohr_get_employee_fields_first_page"),
            start_task='bamboohr_get_employee_fields_page',
            end_task='for_each_fields_page_end'
        )

        bamboohr_get_employee_fields_page = rail.BambooHROperator(
            task_id='bamboohr_get_employee_fields_page',
            company_domain=config.bamboohr_domain,
            request_method='GET',
            endpoint="/datasets/employee/fields?page={{ result('fetch_remaining_fields_pages') }}&page_size=1000",
            bamboohr_conn_id=config.bamboohr_conn_id
        )

        accumulate_fields_page_data = rail.SetVariableOperator(
            task_id='accumulate_fields_page_data',
            name="employee_fields",
            value=lambda: custom_methods.get_page_data(bamboohr_get_employee_fields_page.task_id, "fields"),
            append=True
        )

        for_each_fields_page_end = rail.EmptyOperator(
            task_id='for_each_fields_page_end'
        )

        get_employee_fields_var = rail.GetVariableOperator(
            task_id='get_employee_fields_var',
            name="employee_fields"
        )

        filter_required_employee_fields = rail.PythonOperator(
            task_id='filter_required_employee_fields',
            python_callable=lambda: response_filters.get_required_employee_datasets_fields(
                custom_methods.get_flattened_data("get_employee_fields_var", "fields"), config.required_employee_fields)
        )

        # Paginated BambooHR employee data workflow (page_size=1000 is BambooHR max)
        bamboohr_updated_employees_first_page = rail.BambooHROperator(
            task_id='bamboohr_updated_employees_first_page',
            company_domain=config.bamboohr_domain,
            request_method='POST',
            endpoint="/datasets/employee?page=1&page_size=1000",
            bamboohr_conn_id=config.bamboohr_conn_id,
            data=lambda: request_payload.get_bamboohr_employees_request(rail.result('filter_required_employee_fields'))
        )

        set_employees_data_var = rail.SetVariableOperator(
            task_id='set_employees_data_var',
            name="employees_data",
            value=lambda: custom_methods.get_page_data(bamboohr_updated_employees_first_page.task_id, "data")
        )

        has_more_employee_pages = rail.IfOperator(
            task_id='has_more_employee_pages',
            test=lambda: len(custom_methods.get_remaining_page_numbers("bamboohr_updated_employees_first_page")) > 0,
            yes_task='fetch_remaining_employee_pages',
            no_task='get_employees_data_var'
        )

        fetch_remaining_employee_pages = rail.ForEachOperator(
            task_id='fetch_remaining_employee_pages',
            items=lambda: custom_methods.get_remaining_page_numbers("bamboohr_updated_employees_first_page"),
            start_task='bamboohr_updated_employees_page',
            end_task='for_each_employee_page_end'
        )

        bamboohr_updated_employees_page = rail.BambooHROperator(
            task_id='bamboohr_updated_employees_page',
            company_domain=config.bamboohr_domain,
            request_method='POST',
            endpoint="/datasets/employee?page={{ result('fetch_remaining_employee_pages') }}&page_size=1000",
            bamboohr_conn_id=config.bamboohr_conn_id,
            data=lambda: request_payload.get_bamboohr_employees_request(rail.result('filter_required_employee_fields'))
        )

        accumulate_employee_page_data = rail.SetVariableOperator(
            task_id='accumulate_employee_page_data',
            name="employees_data",
            value=lambda: custom_methods.get_page_data(bamboohr_updated_employees_page.task_id, "data"),
            append=True
        )

        for_each_employee_page_end = rail.EmptyOperator(
            task_id='for_each_employee_page_end'
        )

        get_employees_data_var = rail.GetVariableOperator(
            task_id='get_employees_data_var',
            name="employees_data"
        )

        bamboohr_updated_employees_data = rail.PythonOperator(
            task_id='bamboohr_updated_employees_data',
            python_callable=lambda: response_filters.get_updated_employees_details(
                {"data": custom_methods.get_flattened_data("get_employees_data_var")}, rail.result('filter_required_employee_fields'))
        )

        has_bamboohr_updated_employees_data = rail.IfOperator(
            task_id='has_bamboohr_updated_employees_data',
            test=lambda: len(rail.result('bamboohr_updated_employees_data')) > 0,
            yes_task='create_groups_log',
            no_task='update_lastsync_time'
        )

        create_groups_log = rail.CreateLogOperator(
            task_id='create_groups_log'
        )

        create_supervisor_pending_log = rail.CreateLogOperator(
            task_id='create_supervisor_pending_log'
        )

        create_users_payload_collection = rail.CreateCollectionOperator(
            task_id='create_users_payload_collection',
            source='{{ result("bamboohr_updated_employees_data") | to_json }}',
            name='bamboohr_users_data'
        )

        query_invalid_user_records = rail.QueryCollectionOperator(
            task_id="query_invalid_user_records",
            query=custom_methods.generate_user_records_query(config.required_employee_fields, "invalid"),
            name="bamboohr_invalid_users_data"
        )

        if_invalid_user_records = rail.IfOperator(
            task_id="if_invalid_user_records",
            test='{{result("query_invalid_user_records", "length") > 0}}',
            yes_task="write_invalid_users_log",
            no_task="query_valid_user_records"
        )

        write_invalid_users_log = rail.WriteLogOperator(
            task_id="write_invalid_users_log",
            log='{{ result("create_groups_log") }}',
            items='{{result("query_invalid_user_records")}}',
            message=lambda item: custom_methods.get_invalid_user_log_details(item),
            severity="Exception",
            properties=lambda item: {
                "employeeid": item["employeenumber"],
                "action": "Validation",
                "status": "Exception",
                "details": custom_methods.get_invalid_user_log_details(item)
            }
        )

        query_valid_user_records = rail.QueryCollectionOperator(
            task_id="query_valid_user_records",
            query=custom_methods.generate_user_records_query(config.required_employee_fields, "valid"),
            name="bamboohr_valid_users_data"
        )

        if_valid_user_records = rail.IfOperator(
            task_id="if_valid_user_records",
            test='{{result("query_valid_user_records", "length") > 0}}',
            yes_task="query_all_employee_numbers",
            no_task="format_log_records"
        )

        query_all_employee_numbers = rail.QueryCollectionOperator(
            task_id="query_all_employee_numbers",
            query="SELECT DISTINCT employeenumber FROM bamboohr_valid_users_data WHERE NULLIF(employeenumber, '') IS NOT NULL",
            name="all_employee_numbers_in_payload"
        )

        process_groups_creation = rail.EmptyOperator(
            task_id='process_groups_creation'
        )

        errored_logs_artifact = rail.CreateLogOperator(
            task_id='errored_logs_artifact'
        )

        get_all_user_oefs = rail.RepliconServiceOperator(
            task_id="get_all_user_oefs",
            endpoint="services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data=lambda: {
                "bindingContextUri": "urn:replicon:object-type:user"
            },
            data_handler=lambda oefs_list: {
                field_data["field_attr"]: rail.find_first_by_attr_and_get_attr(
                    oefs_list, 'name', field_data["replicon_name"], 'uri')
                    for field_data in config.required_employee_fields 
                    if field_data["type"] == "oef"
            }
        )

        # Create a list of OEF configurations for list OEFs
        prepare_oef_tags_list = rail.PythonOperator(
            task_id="prepare_oef_tags_list",
            python_callable=lambda: [
                {
                    "oef_name": field["replicon_name"],
                    "oef_uri": rail.result("get_all_user_oefs").get(field["field_attr"]),
                    "bamboohr_field": field["field_attr"]
                }
                for field in config.required_employee_fields
                if field.get("type") == "oef" and field.get("oef_type") == "list"
                    and rail.result("get_all_user_oefs").get(field["field_attr"])
            ]
        )

        create_project_roles = rail.TriggerDagRunOperator(
            task_id="create_project_roles",
            trigger_dag_id=config.create_project_roles_child_dag_id,
            conf=lambda: {
                "groups_log_artifact": rail.result("errored_logs_artifact")
            }
        )

        create_oef_tags = rail.TriggerDagRunForEachItemOperator(
            task_id="create_oef_tags",
            items='{{ result("prepare_oef_tags_list") | to_json }}',
            trigger_dag_id=config.create_oef_tags_child_dag_id,
            conf=lambda item: {
                "groups_log_artifact": rail.result("errored_logs_artifact"),
                "oef_name": item["oef_name"],
                "oef_uri": item["oef_uri"],
                "bamboohr_field": item["bamboohr_field"]
            }
        )

        def gather_all_the_run_ids_callable():
            run_ids = []
            if rail.result(create_project_roles.task_id):
                run_ids.append(rail.result(create_project_roles.task_id))
            # For TriggerDagRunForEachItemOperator, we need to get the list of run IDs
            oef_tag_runs = rail.result(create_oef_tags.task_id)
            if oef_tag_runs:
                run_ids.extend(oef_tag_runs)
            return run_ids
 
        gather_all_the_run_ids = rail.PythonOperator(
            task_id="gather_all_the_run_ids",
            python_callable=gather_all_the_run_ids_callable
        )

        wait_for_groups_creation = rail.WaitForDagRunsSensor(
            task_id='wait_for_groups_creation',
            dag_runs='{{ result("gather_all_the_run_ids") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        get_errored_creation_logs = rail.FilterLogEntriesOperator(
            task_id='get_errored_creation_logs',
            severity='Error',
            log='{{ result("errored_logs_artifact") }}'
        )

        has_errored_creation_logs = rail.IfOperator(
            task_id='has_errored_creation_logs',
            test='{{ result("get_errored_creation_logs", "length") > 0 }}',
            yes_task='fail_user_sync_for_groups_creation',
            no_task='start_parallel_api_calls'
        )

        fail_user_sync_for_groups_creation = rail.FailOperator(
            task_id='fail_user_sync_for_groups_creation',
            message='User sync failed due to errors in project roles/OEF tags creation. Check logs for details.'
        )

        start_parallel_api_calls = rail.EmptyOperator(
            task_id='start_parallel_api_calls'
        )

        get_permission_set_uris = rail.RepliconServiceOperator(
            task_id='get_permission_set_uris',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data_handler=lambda response: response_filters.get_required_permission_set_uris(response, config.supervisor_permission_set)
        )

        get_all_holiday_calendars = rail.RepliconServiceOperator(
            task_id="get_all_holiday_calendars",
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars"
        )

        get_all_time_zones = rail.RepliconServiceOperator(
            task_id='get_all_time_zones',
            endpoint="/services/InternationalizationService1.svc/GetAllTimeZones"
        )

        get_all_policysets = rail.RepliconServiceOperator(
            task_id="get_all_policysets",
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets"
        )

        get_all_time_off_types = rail.RepliconServiceOperator(
            task_id="get_all_time_off_types",
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes"
        )

        get_all_payrule_scripts = rail.RepliconServiceOperator(
            task_id='get_all_payrule_scripts',
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts"
        )

        get_all_project_roles = rail.RepliconServiceOperator(
            task_id="get_all_project_roles",
            endpoint="/services/ProjectRoleService1.svc/GetAllRoles"
        )

        get_all_office_schedules = rail.RepliconServiceOperator(
            task_id='get_all_office_schedules',
            endpoint='/services/OfficeScheduleService1.svc/GetAllOfficeSchedules'
        )

        get_all_locations = rail.RepliconServiceOperator(
            task_id="get_all_locations",
            endpoint="/services/LocationService1.svc/GetEnabledLocations"
        )

        get_all_departments = rail.RepliconServiceOperator(
            task_id="get_all_departments",
            endpoint="/services/DepartmentGroupService1.svc/GetEnabledDepartmentGroups"
        )

        get_all_divisions = rail.RepliconServiceOperator(
            task_id="get_all_divisions",
            endpoint="/services/DivisionService1.svc/GetEnabledDivisions"
        )

        check_employee_type_oef_exists = rail.IfOperator(
            task_id="check_employee_type_oef_exists",
            test=lambda: rail.result("get_all_user_oefs").get("employeetype_oef") is not None,
            yes_task="get_all_employee_type_oef_tags",
            no_task="check_agency_oef_exists"
        )

        get_all_employee_type_oef_tags = rail.RepliconServiceOperator(
            task_id="get_all_employee_type_oef_tags",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data=lambda: {
                "objectExtensionTagDefinitionUri": rail.result("get_all_user_oefs").get("employeetype_oef")
            },
            data_handler=response_filters.get_all_oef_tags
        )

        check_agency_oef_exists = rail.IfOperator(
            task_id="check_agency_oef_exists",
            test=lambda: rail.result("get_all_user_oefs").get("agency_oef") is not None,
            yes_task="get_all_agency_oef_tags",
            no_task="check_adp_company_code_oef_exists"
        )

        get_all_agency_oef_tags = rail.RepliconServiceOperator(
            task_id="get_all_agency_oef_tags",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data=lambda: {
                "objectExtensionTagDefinitionUri": rail.result("get_all_user_oefs").get("agency_oef")
            },
            data_handler=response_filters.get_all_oef_tags
        )

        check_adp_company_code_oef_exists = rail.IfOperator(
            task_id="check_adp_company_code_oef_exists",
            test=lambda: rail.result("get_all_user_oefs").get("adpcompanycode_oef") is not None,
            yes_task="get_all_adp_company_code_oef_tags",
            no_task="start_process_users"
        )

        get_all_adp_company_code_oef_tags = rail.RepliconServiceOperator(
            task_id="get_all_adp_company_code_oef_tags",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data=lambda: {
                "objectExtensionTagDefinitionUri": rail.result("get_all_user_oefs").get("adpcompanycode_oef")
            },
            data_handler=response_filters.get_all_oef_tags
        )

        start_process_users = rail.EmptyOperator(
            task_id='start_process_users'
        )

        trigger_process_user = rail.trigger_parallel_dagrun(
            task_id='trigger_process_user',
            items='{{ result("query_valid_user_records") }}',
            parallel_count=config.trigger_parallel_dagrun_count,
            trigger_dag_id=config.process_user_child_dagid,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **request_payload.get_process_user_conf(item, config.required_employee_fields),
                "supervisor_pending_log": rail.result('create_supervisor_pending_log'),
                "all_employee_numbers_in_payload": rail.result("query_all_employee_numbers")
            }
        )

        get_process_user_dag_ids = rail.PythonOperator(
            task_id='get_process_user_dag_ids',
            python_callable=lambda: list(itertools.chain(
                *list(map(lambda x: (rail.result(
                    f'trigger_process_user_{x+1}') if rail.result(
                    f'trigger_process_user_{x+1}') else []), range(config.trigger_parallel_dagrun_count))))),
            show_return_value_in_logs=False
        )

        gather_user_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_logs',
            dag_runs='{{ result("get_process_user_dag_ids") }}',
            dagrun_task_id='create_user_log',
            flatten=True
        )

        filter_pending_supervisor_records = rail.FilterLogEntriesOperator(
            task_id='filter_pending_supervisor_records',
            log='{{ result("create_supervisor_pending_log") }}',
            severity='Pending'
        )

        if_filtered_pending_supervisor_records = rail.IfOperator(
            task_id='if_filtered_pending_supervisor_records',
            test='{{ result("filter_pending_supervisor_records", "length") > 0 }}',
            yes_task='create_supervisor_assignment_log',
            no_task='format_log_records'
        )

        create_supervisor_assignment_log = rail.CreateLogOperator(
            task_id='create_supervisor_assignment_log'
        )

        # Process individual pending supervisor assignment records
        process_pending_supervisor_records = rail.trigger_parallel_dagrun(
            task_id="process_pending_supervisor_records",
            items='{{ result("filter_pending_supervisor_records") }}',
            parallel_count=config.supervisor_assignment_parallel_count,
            trigger_dag_id=config.supervisor_assignment_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **dict(item['properties'].items()),
                "process_start_time": rail.result('logging_details')['current_time_json'],
                "supervisor_assign_log": rail.result('create_supervisor_assignment_log')
            }
        )

        get_supervisor_assignment_dag_ids = rail.PythonOperator(
            task_id='get_supervisor_assignment_dag_ids',
            python_callable=lambda: list(itertools.chain(
                *list(map(lambda x: (rail.result(
                    f'process_pending_supervisor_records_{x+1}') if rail.result(
                    f'process_pending_supervisor_records_{x+1}') else []), range(config.supervisor_assignment_parallel_count))))),
            show_return_value_in_logs=False
        )

        gather_supervisor_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_supervisor_logs',
            dag_runs='{{ result("get_supervisor_assignment_dag_ids") }}',
            dagrun_task_id='create_supervisor_assignment_log',
            flatten=True
        )

        format_log_records = rail.CreateCollectionOperator(
            task_id='format_log_records',
            source=custom_methods.do_format_logs,
            columns=["employeeid", "action", "status", "details", "ecid"],
            name='format_log_records'
        )

        send_logs_enter, send_logs_end = get_send_logs(config)

        update_lastsync_time = rail.PythonOperator(
            task_id='update_lastsync_time',
            python_callable=lambda: Variable.set(key=config.last_synctime, value=rail.result("get_lastsync_time_and_current_time")["current_time"])
        )


        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        should_fail_dag = rail.IfOperator(
            task_id='should_fail_dag',
            test="{{ get_failed_upstream_task_ids() | length > 0 }}",
            yes_task='fail_user_sync'
        )

        fail_user_sync = rail.FailOperator(
            task_id='fail_user_sync',
            message='{{ get_error_message() }}'
        )

        # Pagination workflow for BambooHR employee fields
        logging_details >> get_lastsync_time_and_current_time >> bamboohr_get_employee_fields_first_page \
            >> set_employee_fields_var >> has_more_fields_pages
        has_more_fields_pages >> rail.Label('Yes') >> fetch_remaining_fields_pages
        fetch_remaining_fields_pages >> bamboohr_get_employee_fields_page >> accumulate_fields_page_data >> for_each_fields_page_end
        has_more_fields_pages >> rail.Label('No') >> get_employee_fields_var
        fetch_remaining_fields_pages >> for_each_fields_page_end
        for_each_fields_page_end >> get_employee_fields_var >> filter_required_employee_fields

        # Pagination workflow for BambooHR employee data
        filter_required_employee_fields >> bamboohr_updated_employees_first_page \
            >> set_employees_data_var >> has_more_employee_pages
        has_more_employee_pages >> rail.Label('Yes') >> fetch_remaining_employee_pages
        fetch_remaining_employee_pages >> bamboohr_updated_employees_page >> accumulate_employee_page_data >> for_each_employee_page_end
        has_more_employee_pages >> rail.Label('No') >> get_employees_data_var
        fetch_remaining_employee_pages >> for_each_employee_page_end
        for_each_employee_page_end >> get_employees_data_var >> bamboohr_updated_employees_data >> has_bamboohr_updated_employees_data
        has_bamboohr_updated_employees_data >> rail.Label("Yes") >> create_groups_log >> create_supervisor_pending_log >> create_users_payload_collection \
            >> query_invalid_user_records >> if_invalid_user_records
        if_invalid_user_records >> rail.Label("Yes") >> write_invalid_users_log >> query_valid_user_records \
            >> if_valid_user_records
        if_invalid_user_records >> rail.Label("No") >> query_valid_user_records
        # Updated flow - Groups creation DAGs are commented out as groups should already exist in Replicon
        if_valid_user_records >> rail.Label("Yes") >> query_all_employee_numbers >> process_groups_creation >> errored_logs_artifact >> get_all_user_oefs >> prepare_oef_tags_list \
            >> create_project_roles >> create_oef_tags >> gather_all_the_run_ids >> wait_for_groups_creation \
                >> get_errored_creation_logs >> has_errored_creation_logs
        
        # Parallel API calls for better performance
        has_errored_creation_logs >> rail.Label("No") >> start_parallel_api_calls
        start_parallel_api_calls >> [
            get_permission_set_uris,
            get_all_holiday_calendars,
            get_all_time_zones,
            get_all_policysets,
            get_all_time_off_types,
            get_all_payrule_scripts,
            get_all_project_roles,
            get_all_office_schedules,
            get_all_locations,
            get_all_departments,
            get_all_divisions
        ] >> check_employee_type_oef_exists

        has_errored_creation_logs >> rail.Label("Yes") >> fail_user_sync_for_groups_creation >> dagrun_log_to_sumo
        
        # OEF tag fetching flow
        check_employee_type_oef_exists >> rail.Label("Yes") >> get_all_employee_type_oef_tags >> check_agency_oef_exists
        check_employee_type_oef_exists >> rail.Label("No") >> check_agency_oef_exists
        
        check_agency_oef_exists >> rail.Label("Yes") >> get_all_agency_oef_tags >> check_adp_company_code_oef_exists
        check_agency_oef_exists >> rail.Label("No") >> check_adp_company_code_oef_exists
        
        check_adp_company_code_oef_exists >> rail.Label("Yes") >> get_all_adp_company_code_oef_tags >> start_process_users
        check_adp_company_code_oef_exists >> rail.Label("No") >> start_process_users
        
        start_process_users >> trigger_process_user >> get_process_user_dag_ids >> gather_user_logs >> filter_pending_supervisor_records >> if_filtered_pending_supervisor_records
        
        if_filtered_pending_supervisor_records >> rail.Label("Yes") >> create_supervisor_assignment_log >> process_pending_supervisor_records
        if_filtered_pending_supervisor_records >> rail.Label("No") >> format_log_records
        process_pending_supervisor_records >> get_supervisor_assignment_dag_ids >> gather_supervisor_logs >> format_log_records
        
        if_valid_user_records >> rail.Label("No") >> format_log_records
        format_log_records >> send_logs_enter
        send_logs_end >> update_lastsync_time
        has_bamboohr_updated_employees_data >> rail.Label("No") >> update_lastsync_time >> dagrun_log_to_sumo >> should_fail_dag
        should_fail_dag >> rail.Label("Yes") >> fail_user_sync

    return dag


rail.for_each_instance(create_main_dag)
