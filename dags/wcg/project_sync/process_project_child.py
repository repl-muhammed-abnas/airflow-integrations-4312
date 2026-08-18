"""
WCG Project Sync v2 - Process Project Child DAG
Converted from Workato Integration - January 2026

Original Workato Recipe: Live | WCG - Netsuite Project sync_V2.0
Total Steps: 178 (matching Workato step numbers)

This child DAG processes each project following the exact Workato flow:
- Steps 27-39: Main processing, project search
- Steps 40-80: New project path (client not found, client name present)
- Steps 81-117: New project path (client name blank)
- Steps 118-149: New project path (client exists in Replicon)
- Steps 150-175: Existing project update path
- Steps 176-179: Cleanup and error handling
"""

from datetime import timedelta
from airflow.models import Variable
import rail
from wcg.project_sync.utils import custom_methods, request_methods

null = None


def create_child_dag(config):
    """
    Child DAG for processing individual projects.
    Task IDs include Workato step numbers for traceability.
    """
    with rail.create_airflow_dag(
        dag_id=config.process_project_child_dag_id,
        description=f"WCG Project Sync v2 - Process Project Child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        create_log = rail.CreateLogOperator(task_id="create_log")

        # ============================================================================
        # BATCH TASK CONTROL
        # ============================================================================

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var="true"
            ).lower() == "true",
            yes_task="batch_task",
            no_task="step_16_17_if_valid_project_record",
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            start_task="step_16_17_if_valid_project_record",
            end_task="step_177_catch_errors",
        )

        # ============================================================================
        # PHASE 1: VALIDATION (Workato Steps 16-17)
        # ============================================================================

        # Step 16-17: IF parent internal_id is BLANK -> STOP
        step_16_17_if_valid_project_record = rail.IfOperator(
            task_id="step_16_17_if_valid_project_record",
            test=lambda dag_run: (
                dag_run.conf.get("internal_id") and dag_run.conf.get("name")
            ),
            yes_task="step_28_if_export_enabled",
            no_task="step_17_log_invalid_data",
        )

        step_17_log_invalid_data = rail.WriteLogOperator(
            task_id="step_17_log_invalid_data",
            log='{{result("create_log")}}',
            message="Invalid project record - missing required fields",
            severity="Exception",
            properties=lambda dag_run: {
                "projectname": dag_run.conf.get("name", "N/A"),
                "projectcode": dag_run.conf.get("internal_id", "N/A"),
                "customer": dag_run.conf.get("customer", "N/A"),
                "status": "Exception",
                "details": "Missing required fields: internal_id or name",
            },
        )

        # ============================================================================
        # PHASE 2: MAIN PROCESSING - Steps 27-39
        # ============================================================================

        # Step 28: IF export_replicon is TRUE (we assume always true from SFTP trigger)
        step_28_if_export_enabled = rail.IfOperator(
            task_id="step_28_if_export_enabled",
            test=lambda dag_run: dag_run.conf.get("export_enabled", True),
            yes_task="step_29_log_start_processing",
            no_task="step_176_log_export_disabled",
        )

        step_176_log_export_disabled = rail.WriteLogOperator(
            task_id="step_176_log_export_disabled",
            log='{{result("create_log")}}',
            message="Export disabled - skipping project",
            severity="Info",
            properties=lambda dag_run: {
                "projectname": dag_run.conf.get("name", ""),
                "projectcode": dag_run.conf.get("internal_id", ""),
                "customer": dag_run.conf.get("customer", ""),
                "status": "Skipped",
                "details": "export_enabled flag is false",
            },
        )

        # Step 29: Logger - Log message (start processing)
        step_29_log_start_processing = rail.PythonOperator(
            task_id="step_29_log_start_processing",
            python_callable=lambda dag_run: str(dag_run.conf.get('name', ''))+" - "+ str(dag_run.conf.get('internal_id', '')),
        )

        # Step 30: GetTenantEndpointDetails (SKIPPED - using connection config)
        # step_30_get_tenant_endpoint = rail.RepliconServiceOperator(
        #     task_id="step_30_get_tenant_endpoint",
        #     endpoint="/services/TenantService1.svc/GetTenantEndpointDetails",
        #     data={},
        # )

        # Step 32: Date Splitter - Parse project start date
        step_32_parse_start_date = rail.PythonOperator(
            task_id="step_32_parse_start_date",
            python_callable=lambda dag_run: custom_methods.parse_date_safe(
                dag_run.conf.get("start_date", "")
            ),
        )

        # Step 36: Search project based on code (internal id)
        step_36_search_project_by_code = rail.RepliconServiceOperator(
            task_id="step_36_search_project_by_code",
            endpoint="/services/ProjectListService1.svc/GetData",
            data=lambda dag_run: request_methods.get_search_project_by_code_request(
                dag_run.conf.get("internal_id", "")
            ),
            data_handler=custom_methods.parse_project_list_response_with_exact_match,
        )

        # Step 39: Logger - Project URI
        step_39_log_project_uri = rail.PythonOperator(
            task_id="step_39_log_project_uri",
            python_callable=lambda: rail.result("step_36_search_project_by_code"),
        )

        # Step 40: IF Project NOT found (blank) -> New project path
        step_40_if_project_not_found = rail.IfOperator(
            task_id="step_40_if_project_not_found",
            test=lambda: not (
                rail.result("step_36_search_project_by_code")
                and rail.result("step_36_search_project_by_code").get("uri")
            ),
            yes_task="step_41_search_client_by_code",
            no_task="step_150_get_project_details",
        )

        # ============================================================================
        # PHASE 3A: NEW PROJECT PATH - Steps 41-80 (Client lookup/creation)
        # ============================================================================

        # Step 41: Search client based on code
        step_41_search_client_by_code = rail.RepliconServiceOperator(
            task_id="step_41_search_client_by_code",
            endpoint="/services/ClientListService1.svc/GetData",
            data=lambda dag_run: request_methods.get_search_client_by_code_request(
                dag_run.conf.get("customer_internal_id", "")
            ),
            data_handler=custom_methods.parse_client_list_response_with_exact_match,
        )

        # Step 44: Logger - Client URI
        step_44_log_client_uri = rail.PythonOperator(
            task_id="step_44_log_client_uri",
            python_callable=lambda: rail.result("step_41_search_client_by_code"),
        )

        # Step 45: IF Client NOT found (blank) -> Create client path
        step_45_if_client_not_found = rail.IfOperator(
            task_id="step_45_if_client_not_found",
            test=lambda: not rail.result("step_41_search_client_by_code"),
            yes_task="step_47_log_client_name",
            no_task="step_118_if_client_exists",
        )

        # Step 46: NetSuite search for customer (N/A - data from feed file)
        # step_46_netsuite_search_customer = rail.EmptyOperator(
        #     task_id="step_46_netsuite_search_customer",
        #     # NetSuite search - not applicable, data comes from SFTP feed
        # )

        # Step 47: Logger - Client name
        step_47_log_client_name = rail.PythonOperator(
            task_id="step_47_log_client_name",
            python_callable=lambda dag_run: dag_run.conf.get("customer", ""),
        )

        # Step 48: IF Client name IS PRESENT
        step_48_if_client_name_present = rail.IfOperator(
            task_id="step_48_if_client_name_present",
            test=lambda dag_run: bool(dag_run.conf.get("customer")),
            yes_task="step_49_search_project_mapper",
            no_task="step_81_if_client_name_blank",
        )

        # ============================================================================
        # PATH A: Client name PRESENT - Steps 49-80
        # ============================================================================

        # Step 49: Search WCG_Project_Mapper lookup table
        step_49_search_project_mapper = rail.PythonOperator(
            task_id="step_49_search_project_mapper",
            python_callable=lambda dag_run: custom_methods.validate_subsidiary_in_mapper(
                dag_run.conf.get("subsidiary", ""),
                dag_run.conf.get("project_template_mapper", {})
            ),
        )

        # Step 50-51: IF list size = 0 -> STOP
        step_50_if_subsidiary_mapped = rail.IfOperator(
            task_id="step_50_if_subsidiary_mapped",
            test=lambda: bool(rail.result("step_49_search_project_mapper")),
            yes_task="step_53_get_template_project",
            no_task="step_51_stop_subsidiary_not_found",
        )

        step_51_stop_subsidiary_not_found = rail.WriteLogOperator(
            task_id="step_51_stop_subsidiary_not_found",
            log='{{result("create_log")}}',
            message="Subsidiary not found in mapper",
            severity="Exception",
            properties=lambda dag_run: {
                "projectname": dag_run.conf.get("name", ""),
                "projectcode": dag_run.conf.get("internal_id", ""),
                "customer": dag_run.conf.get("customer", ""),
                "status": "Exception",
                "details": "Subsidiary is not present in the Mapper",
            },
        )

        # Step 53: Get project details from template (replicon:get_project_details)
        step_53_get_template_project = rail.RepliconServiceOperator(
            task_id="step_53_get_template_project",
            endpoint="/services/ProjectListService1.svc/GetData",
            data=lambda: request_methods.get_template_project_search_request(
                rail.result("step_49_search_project_mapper")
            ),
            data_handler=custom_methods.parse_template_project_response_path_a,
        )

        # Step 54-55: IF template blank -> STOP
        step_54_if_template_not_found = rail.IfOperator(
            task_id="step_54_if_template_not_found",
            test=lambda: (
                rail.result("step_53_get_template_project")
                and rail.result("step_53_get_template_project").get("uri")
            ),
            yes_task="step_56_create_client",
            no_task="step_55_stop_template_not_found",
        )

        step_55_stop_template_not_found = rail.WriteLogOperator(
            task_id="step_55_stop_template_not_found",
            log='{{result("create_log")}}',
            message="Template project not found in Replicon",
            severity="Error",
            properties=lambda dag_run: {
                "projectname": dag_run.conf.get("name", ""),
                "projectcode": dag_run.conf.get("internal_id", ""),
                "customer": dag_run.conf.get("customer", ""),
                "status": "Error",
                "details": f"Template project '{rail.result('step_49_search_project_mapper')}' is not available in Replicon.",
            },
        )

        # Step 56: Create client in Replicon
        step_56_create_client = rail.RepliconServiceOperator(
            task_id="step_56_create_client",
            endpoint="/services/ClientService1.svc/PutClient",
            data=request_methods.create_client_request,
        )

        # Step 57: Create Project via Existing Project (CreateProjectCopyBatch2)
        step_57_create_project_copy_batch = rail.RepliconServiceOperator(
            task_id="step_57_create_project_copy_batch",
            endpoint="/services/ProjectService1.svc/CreateProjectCopyBatch2",
            data=lambda dag_run: request_methods.create_project_copy_batch_request(
                dag_run,
                rail.result("step_53_get_template_project").get("uri"),
                custom_methods.get_client_name_for_create(dag_run)
            ),
        )

        # Step 58: Batch execution - Execute and wait for completion
        step_58_execute_batch, step_58_wait_for_batch = rail.batch_execution(
            'step_58_execute_batch', 'step_57_create_project_copy_batch',
        )

        # Step 59: GetProjectCopyBatchResults
        # Response structure: {"d": {"error": null, "project": {"uri": "...", "name": "..."}}}
        step_59_get_project_copy_results = rail.RepliconServiceOperator(
            task_id="step_59_get_project_copy_results",
            endpoint="/services/ProjectService1.svc/GetProjectCopyBatchResults",
            data=lambda: {"projectCopyBatchUri": rail.result("step_57_create_project_copy_batch")},
            data_handler=lambda response: response.get("project", {}) if response else None,
        )

        # Step 60: UpdateCode - Set project code to internal_id
        step_60_update_code = rail.RepliconServiceOperator(
            task_id="step_60_update_code",
            endpoint="/services/ProjectService1.svc/UpdateCode",
            data=lambda dag_run: request_methods.update_project_code_request(
                rail.result("step_59_get_project_copy_results").get("uri"),
                dag_run.conf.get("internal_id", "")
            ),
        )

        # Step 61-62: IF subsidiary present -> Update subsidiary dropdown
        step_61_if_has_subsidiary = rail.IfOperator(
            task_id="step_61_if_has_subsidiary",
            test=lambda dag_run: bool(dag_run.conf.get("subsidiary")),
            yes_task="step_62_update_subsidiary",
            no_task="step_63_update_project",
        )

        # Step 62: Call recipe updatesubsidiary_project (do not wait for response)
        # Matches Workato: Triggers the update_subsidiary_child DAG asynchronously
        step_62_update_subsidiary = rail.TriggerDagRunOperator(
            task_id="step_62_update_subsidiary",
            trigger_dag_id=config.update_subsidiary_dag_id,
            conf=lambda dag_run: {
                "projecturi": rail.result("step_59_get_project_copy_results").get("uri"),
                "subsidiaryvalue": (dag_run.conf.get("subsidiary")),
            },
            wait_for_completion=False,  # "do not wait for response" in Workato
        )

        # Step 63: Update project (custom fields)
        step_63_update_project = rail.RepliconServiceOperator(
            task_id="step_63_update_project",
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=lambda dag_run: request_methods.update_project_custom_fields_request(
                dag_run,
                rail.result("step_59_get_project_copy_results").get("uri")
            ),
        )

        # Step 64: IF total_budget present
        step_64_if_has_budget = rail.IfOperator(
            task_id="step_64_if_has_budget",
            test=lambda dag_run: bool(dag_run.conf.get("total_budget")),
            yes_task="step_65_log_budget",
            no_task="step_67_get_all_users_report",
        )

        # Step 65: Logger - Total Budget
        step_65_log_budget = rail.PythonOperator(
            task_id="step_65_log_budget",
            python_callable=lambda dag_run: custom_methods.parse_budget_amount(
                dag_run.conf.get("total_budget")
            ),
        )

        # Step 66: UpdateTotalEstimatedContractValue
        step_66_update_budget = rail.RepliconServiceOperator(
            task_id="step_66_update_budget",
            endpoint="/services/ProjectService1.svc/UpdateTotalEstimatedContractValue",
            data=lambda dag_run: request_methods.update_total_estimated_contract_value_request(
                rail.result("step_59_get_project_copy_results").get("uri"),
                custom_methods.parse_budget_amount(dag_run.conf.get("total_budget")),
            ),
        )

        # Step 67: GetAlluserfromreplicon (ReportService - using UserlistService instead)
        step_67_get_all_users_report = rail.RepliconServiceOperator(
            task_id="step_67_get_all_users_report",
            endpoint="/services/UserlistService1.svc/GetData",
            data=lambda dag_run: request_methods.get_search_user_request(
                dag_run.conf.get("project_manager", "")
            ),
            data_handler=custom_methods.get_project_manager_data_handler,
        )

        # Step 68: Parse CSV (N/A - data handler processes inline)
        # step_68_parse_csv = rail.EmptyOperator(task_id="step_68_parse_csv")

        # Step 69-72: NetSuite search for PM (N/A - data from feed)
        # step_69_netsuite_search_pm = rail.EmptyOperator(task_id="step_69_netsuite_search_pm")
        # step_70_log_pm_username = rail.EmptyOperator(task_id="step_70_log_pm_username")
        # step_71_log_pm_internal_id = rail.EmptyOperator(task_id="step_71_log_pm_internal_id")

        step_72_log_pm_uri = rail.PythonOperator(
            task_id="step_72_log_pm_uri",
            python_callable=lambda: rail.result("step_67_get_all_users_report"),
        )

        # Step 73: IF PM found (present)
        step_73_if_pm_found = rail.IfOperator(
            task_id="step_73_if_pm_found",
            test=lambda: bool(rail.result("step_67_get_all_users_report")),
            yes_task="step_74_get_pm_permissions",
            no_task="step_log_project_created_no_pm",
        )

        step_log_project_created_no_pm = rail.WriteLogOperator(
            task_id="step_log_project_created_no_pm",
            log='{{result("create_log")}}',
            message="Project created but PM not assigned",
            severity="Exception",
            properties=lambda dag_run: {
                "projectname": dag_run.conf.get("name", ""),
                "projectcode": dag_run.conf.get("internal_id", ""),
                "customer": dag_run.conf.get("customer", ""),
                "status": "Exception",
                "details": f"Project created via template copy (PM not found in Replicon)",
            },
        )

        # Step 74: GetAssignedPermissionSetsForUser2
        step_74_get_pm_permissions = rail.RepliconServiceOperator(
            task_id="step_74_get_pm_permissions",
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data=lambda: request_methods.get_user_permission_sets_v2_request(
                rail.result("step_67_get_all_users_report")
            ),
            data_handler=lambda response: custom_methods.has_project_management_permission(response),
        )

        # Step 75: Logger - Check if PM permission is assigned
        step_75_log_pm_permission = rail.PythonOperator(
            task_id="step_75_log_pm_permission",
            python_callable=lambda: rail.result("step_74_get_pm_permissions"),
        )

        # Step 76: IF PM has permission (present)
        step_76_if_pm_has_permission = rail.IfOperator(
            task_id="step_76_if_pm_has_permission",
            test=lambda: rail.result("step_74_get_pm_permissions") is True,
            yes_task="step_77_update_project_leader",
            no_task="step_78_if_pm_no_permission",
        )

        # Step 77: UpdateProjectLeader (has permission)
        step_77_update_project_leader = rail.RepliconServiceOperator(
            task_id="step_77_update_project_leader",
            endpoint="/services/ProjectService1.svc/UpdateProjectLeader",
            data=lambda: request_methods.update_project_leader_request(
                rail.result("step_59_get_project_copy_results").get("uri"),
                rail.result("step_67_get_all_users_report")
            ),
        )

        # Step 78: IF PM no permission (blank)
        step_78_if_pm_no_permission = rail.IfOperator(
            task_id="step_78_if_pm_no_permission",
            test=lambda: rail.result("step_74_get_pm_permissions") is not True,
            yes_task="step_78a_get_all_permission_sets",
            no_task="step_log_project_created",
        )

        # Step 78a: Get all permission sets from system (Path A)
        step_78a_get_all_permission_sets = rail.RepliconServiceOperator(
            task_id="step_78a_get_all_permission_sets",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data={},
        )

        # Step 78b: Find Project Management permission set URI (Path A)
        step_78b_find_pm_permission_uri = rail.PythonOperator(
            task_id="step_78b_find_pm_permission_uri",
            python_callable=lambda: custom_methods.find_project_management_permission_set_uri(
                rail.result("step_78a_get_all_permission_sets")
            ),
        )

        # Step 79: AssignSupervisorPermissionSetToUser
        step_79_assign_pm_permission = rail.RepliconServiceOperator(
            task_id="step_79_assign_pm_permission",
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data=lambda: request_methods.assign_permission_set_to_user_v2_request(
                rail.result("step_67_get_all_users_report"),
                rail.result("step_78b_find_pm_permission_uri")
            ),
        )

        # Step 80: UpdateProjectLeader (after permission assigned)
        step_80_update_project_leader = rail.RepliconServiceOperator(
            task_id="step_80_update_project_leader",
            endpoint="/services/ProjectService1.svc/UpdateProjectLeader",
            data=lambda: request_methods.update_project_leader_request(
                rail.result("step_59_get_project_copy_results").get("uri"),
                rail.result("step_67_get_all_users_report")
            ),
        )

        # ============================================================================
        # PATH B: Client name BLANK - Steps 81-117
        # Similar to Path A but without client creation
        # ============================================================================

        # Step 81: IF Client name blank - create project without client
        step_81_if_client_name_blank = rail.PythonOperator(
            task_id="step_81_if_client_name_blank",
            python_callable=lambda dag_run: f"Client name blank for project {dag_run.conf.get('name', '')}",
        )

        # Step 82: Search WCG_Project_Mapper (same as step 49)
        step_82_search_project_mapper = rail.PythonOperator(
            task_id="step_82_search_project_mapper",
            python_callable=lambda dag_run: custom_methods.validate_subsidiary_in_mapper(
                dag_run.conf.get("subsidiary", ""),
                dag_run.conf.get("project_template_mapper", {})
            ),
        )

        # Step 83-84: IF list size = 0 -> STOP
        step_83_if_subsidiary_not_mapped = rail.IfOperator(
            task_id="step_83_if_subsidiary_not_mapped",
            test=lambda: bool(rail.result("step_82_search_project_mapper")),
            yes_task="step_86_get_template_project",
            no_task="step_84_stop_subsidiary_not_found",
        )

        step_84_stop_subsidiary_not_found = rail.WriteLogOperator(
            task_id="step_84_stop_subsidiary_not_found",
            log='{{result("create_log")}}',
            message="Subsidiary not found in mapper",
            severity="Exception",
            properties=lambda dag_run: {
                "projectname": dag_run.conf.get("name", ""),
                "projectcode": dag_run.conf.get("internal_id", ""),
                "customer": dag_run.conf.get("customer", ""),
                "status": "Exception",
                "details": "Subsidiary not found in mapper",
            },
        )

        # Step 86: Get template project
        step_86_get_template_project = rail.RepliconServiceOperator(
            task_id="step_86_get_template_project",
            endpoint="/services/ProjectListService1.svc/GetData",
            data=lambda: request_methods.get_template_project_search_request(
                rail.result("step_82_search_project_mapper")
            ),
            data_handler=custom_methods.parse_template_project_response_path_b,
        )

        # Step 87-88: IF template blank -> STOP
        step_87_if_template_not_found = rail.IfOperator(
            task_id="step_87_if_template_not_found",
            test=lambda: (
                rail.result("step_86_get_template_project")
                and rail.result("step_86_get_template_project").get("uri")
            ),
            yes_task="step_93_create_project_no_client",
            no_task="step_88_stop_template_not_found",
        )

        step_88_stop_template_not_found = rail.WriteLogOperator(
            task_id="step_88_stop_template_not_found",
            log='{{result("create_log")}}',
            message="Template project not found in Replicon",
            severity="Error",
            properties=lambda dag_run: {
                "projectname": dag_run.conf.get("name", ""),
                "projectcode": dag_run.conf.get("internal_id", ""),
                "customer": dag_run.conf.get("customer", ""),
                "status": "Error",
                "details": f"Template '{rail.result('step_82_search_project_mapper')}' not found in Replicon",
            },
        )

        # Steps 89-92: NetSuite searches and client creation (skipped for blank client)
        # In Workato these still create a client, but we skip if no customer name

        # Step 93: Create Project via Copy (without client)
        step_93_create_project_no_client = rail.RepliconServiceOperator(
            task_id="step_93_create_project_no_client",
            endpoint="/services/ProjectService1.svc/CreateProjectCopyBatch2",
            data=lambda dag_run: request_methods.create_project_copy_batch_request(
                dag_run,
                rail.result("step_86_get_template_project").get("uri"),
                None  # No client
            ),
        )

        # Step 94: Batch execution - Execute and wait for completion
        step_94_execute_batch, step_94_wait_for_batch = rail.batch_execution(
            'step_94_execute_batch', 'step_93_create_project_no_client',
        )

        # Step 96: GetProjectCopyBatchResults
        # Response structure: {"d": {"error": null, "project": {"uri": "...", "name": "..."}}}
        step_96_get_project_copy_results = rail.RepliconServiceOperator(
            task_id="step_96_get_project_copy_results",
            endpoint="/services/ProjectService1.svc/GetProjectCopyBatchResults",
            data=lambda: {"projectCopyBatchUri": rail.result("step_93_create_project_no_client")},
            data_handler=lambda response: response.get("project", {}) if response else None,
        )

        # Step 97: UpdateCode
        step_97_update_code = rail.RepliconServiceOperator(
            task_id="step_97_update_code",
            endpoint="/services/ProjectService1.svc/UpdateCode",
            data=lambda dag_run: request_methods.update_project_code_request(
                rail.result("step_96_get_project_copy_results").get("uri"),
                dag_run.conf.get("internal_id", "")
            ),
        )

        # Step 98-99: IF subsidiary present -> Update subsidiary (Path B)
        step_98_if_has_subsidiary_b = rail.IfOperator(
            task_id="step_98_if_has_subsidiary_b",
            test=lambda dag_run: bool(dag_run.conf.get("subsidiary")),
            yes_task="step_99_update_subsidiary_b",
            no_task="step_100_update_project_b",
        )

        # Step 99: Call recipe updatesubsidiary_project (do not wait for response)
        # Matches Workato: Triggers the update_subsidiary_child DAG asynchronously
        step_99_update_subsidiary_b = rail.TriggerDagRunOperator(
            task_id="step_99_update_subsidiary_b",
            trigger_dag_id=config.update_subsidiary_dag_id,
            conf=lambda dag_run: {
                "projecturi": rail.result("step_96_get_project_copy_results").get("uri"),
                "subsidiaryvalue": dag_run.conf.get("subsidiary"),
            },
            wait_for_completion=False,  # "do not wait for response" in Workato
        )

        # Step 100: Update project custom fields (Path B)
        step_100_update_project_b = rail.RepliconServiceOperator(
            task_id="step_100_update_project_b",
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=lambda dag_run: request_methods.update_project_custom_fields_request(
                dag_run,
                rail.result("step_96_get_project_copy_results").get("uri")
            ),
        )

        # Step 101: IF budget present (Path B)
        step_101_if_has_budget_b = rail.IfOperator(
            task_id="step_101_if_has_budget_b",
            test=lambda dag_run: bool(dag_run.conf.get("total_budget")),
            yes_task="step_102_log_budget_b",
            no_task="step_104_get_all_users_b",
        )

        # Step 102: Logger - Total Budget (Path B)
        step_102_log_budget_b = rail.PythonOperator(
            task_id="step_102_log_budget_b",
            python_callable=lambda dag_run: custom_methods.parse_budget_amount(
                dag_run.conf.get("total_budget")
            ),
        )

        # Step 103: UpdateTotalEstimatedContractValue (Path B)
        step_103_update_budget_b = rail.RepliconServiceOperator(
            task_id="step_103_update_budget_b",
            endpoint="/services/ProjectService1.svc/UpdateTotalEstimatedContractValue",
            data=lambda dag_run: request_methods.update_total_estimated_contract_value_request(
                rail.result("step_96_get_project_copy_results").get("uri"),
                custom_methods.parse_budget_amount(dag_run.conf.get("total_budget")),
            ),
        )

        # Step 104: GetAlluserfromreplicon (Path B)
        step_104_get_all_users_b = rail.RepliconServiceOperator(
            task_id="step_104_get_all_users_b",
            endpoint="/services/UserlistService1.svc/GetData",
            data=lambda dag_run: request_methods.get_search_user_request(
                dag_run.conf.get("project_manager", "")
            ),
            data_handler=custom_methods.get_project_manager_data_handler,
        )

        # Step 109: Logger - PM URI (Path B)
        step_109_log_pm_uri_b = rail.PythonOperator(
            task_id="step_109_log_pm_uri_b",
            python_callable=lambda: rail.result("step_104_get_all_users_b"),
        )

        # Step 110: IF PM found (Path B)
        step_110_if_pm_found_b = rail.IfOperator(
            task_id="step_110_if_pm_found_b",
            test=lambda: bool(rail.result("step_104_get_all_users_b")),
            yes_task="step_111_get_pm_permissions_b",
            no_task="step_log_project_created_no_pm_b",
        )

        step_log_project_created_no_pm_b = rail.WriteLogOperator(
            task_id="step_log_project_created_no_pm_b",
            log='{{result("create_log")}}',
            message="Project created but PM not assigned",
            severity="Exception",
            properties=lambda dag_run: {
                "projectname": dag_run.conf.get("name", ""),
                "projectcode": dag_run.conf.get("internal_id", ""),
                "customer": dag_run.conf.get("customer", ""),
                "status": "Exception",
                "details": f"Project created via template copy (PM not found in Replicon)",
            },
        )

        # Step 111: GetAssignedPermissionSetsForUser2 (Path B)
        step_111_get_pm_permissions_b = rail.RepliconServiceOperator(
            task_id="step_111_get_pm_permissions_b",
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data=lambda: request_methods.get_user_permission_sets_v2_request(
                rail.result("step_104_get_all_users_b")
            ),
            data_handler=lambda response: custom_methods.has_project_management_permission(response),
        )

        # Step 112: Logger - PM permission (Path B)
        step_112_log_pm_permission_b = rail.PythonOperator(
            task_id="step_112_log_pm_permission_b",
            python_callable=lambda: rail.result("step_111_get_pm_permissions_b"),
        )

        # Step 113: IF PM has permission (Path B)
        step_113_if_pm_has_permission_b = rail.IfOperator(
            task_id="step_113_if_pm_has_permission_b",
            test=lambda: rail.result("step_111_get_pm_permissions_b") is True,
            yes_task="step_114_update_project_leader_b",
            no_task="step_115_if_pm_no_permission_b",
        )

        # Step 114: UpdateProjectLeader (Path B - has permission)
        step_114_update_project_leader_b = rail.RepliconServiceOperator(
            task_id="step_114_update_project_leader_b",
            endpoint="/services/ProjectService1.svc/UpdateProjectLeader",
            data=lambda: request_methods.update_project_leader_request(
                rail.result("step_96_get_project_copy_results").get("uri"),
                rail.result("step_104_get_all_users_b")
            ),
        )

        # Step 115: IF PM no permission (Path B)
        step_115_if_pm_no_permission_b = rail.IfOperator(
            task_id="step_115_if_pm_no_permission_b",
            test=lambda: rail.result("step_111_get_pm_permissions_b") is not True,
            yes_task="step_115a_get_all_permission_sets_b",
            no_task="step_log_project_created_b",
        )

        # Step 115a: Get all permission sets from system (Path B)
        # Dynamically fetch permission sets instead of hardcoding URI
        step_115a_get_all_permission_sets_b = rail.RepliconServiceOperator(
            task_id="step_115a_get_all_permission_sets_b",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data={},
        )

        # Step 115b: Find Project Management permission set URI (Path B)
        step_115b_find_pm_permission_uri_b = rail.PythonOperator(
            task_id="step_115b_find_pm_permission_uri_b",
            python_callable=lambda: custom_methods.find_project_management_permission_set_uri(
                rail.result("step_115a_get_all_permission_sets_b")
            ),
        )

        # Step 116: AssignPermissionSetToUser (Path B)
        step_116_assign_pm_permission_b = rail.RepliconServiceOperator(
            task_id="step_116_assign_pm_permission_b",
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data=lambda: request_methods.assign_permission_set_to_user_v2_request(
                rail.result("step_104_get_all_users_b"),
                rail.result("step_115b_find_pm_permission_uri_b")
            ),
        )

        # Step 117: UpdateProjectLeader (Path B - after permission)
        step_117_update_project_leader_b = rail.RepliconServiceOperator(
            task_id="step_117_update_project_leader_b",
            endpoint="/services/ProjectService1.svc/UpdateProjectLeader",
            data=lambda: request_methods.update_project_leader_request(
                rail.result("step_96_get_project_copy_results").get("uri"),
                rail.result("step_104_get_all_users_b")
            ),
        )

        step_log_project_created_b = rail.WriteLogOperator(
            task_id="step_log_project_created_b",
            log='{{result("create_log")}}',
            message="Project created successfully",
            severity="Success",
            properties=lambda dag_run: {
                "projectname": dag_run.conf.get("name", ""),
                "projectcode": dag_run.conf.get("internal_id", ""),
                "customer": dag_run.conf.get("customer", ""),
                "status": "Success",
                "details": "Project created via template copy",
            },
        )

        # ============================================================================
        # PATH C: Client EXISTS in Replicon - Steps 118-149
        # ============================================================================

        # Step 118: IF Client exists (present) -> Use existing client
        step_118_if_client_exists = rail.PythonOperator(
            task_id="step_118_if_client_exists",
            python_callable=lambda: f"Using existing client: {rail.result('step_41_search_client_by_code')}",
        )

        # Step 119: Search WCG_Project_Mapper
        step_119_search_project_mapper = rail.PythonOperator(
            task_id="step_119_search_project_mapper",
            python_callable=lambda dag_run: custom_methods.validate_subsidiary_in_mapper(
                dag_run.conf.get("subsidiary", ""),
                dag_run.conf.get("project_template_mapper", {})
            ),
        )

        # Step 120-121: IF list size = 0 -> STOP
        step_120_if_subsidiary_not_mapped = rail.IfOperator(
            task_id="step_120_if_subsidiary_not_mapped",
            test=lambda: bool(rail.result("step_119_search_project_mapper")),
            yes_task="step_123_get_template_project",
            no_task="step_121_stop_subsidiary_not_found",
        )

        step_121_stop_subsidiary_not_found = rail.WriteLogOperator(
            task_id="step_121_stop_subsidiary_not_found",
            log='{{result("create_log")}}',
            message="Subsidiary not found in mapper",
            severity="Exception",
            properties=lambda dag_run: {
                "projectname": dag_run.conf.get("name", ""),
                "projectcode": dag_run.conf.get("internal_id", ""),
                "customer": dag_run.conf.get("customer", ""),
                "status": "Exception",
                "details": "Subsidiary not found in mapper",
            },
        )

        # Step 123: Get template project
        step_123_get_template_project = rail.RepliconServiceOperator(
            task_id="step_123_get_template_project",
            endpoint="/services/ProjectListService1.svc/GetData",
            data=lambda: request_methods.get_template_project_search_request(
                rail.result("step_119_search_project_mapper")
            ),
            data_handler=custom_methods.parse_template_project_response_path_c,
        )

        # Step 124-125: IF template blank -> STOP
        step_124_if_template_not_found = rail.IfOperator(
            task_id="step_124_if_template_not_found",
            test=lambda: (
                rail.result("step_123_get_template_project")
                and rail.result("step_123_get_template_project").get("uri")
            ),
            yes_task="step_126_create_project_existing_client",
            no_task="step_125_stop_template_not_found",
        )

        step_125_stop_template_not_found = rail.WriteLogOperator(
            task_id="step_125_stop_template_not_found",
            log='{{result("create_log")}}',
            message="Template project not found in Replicon",
            severity="Error",
            properties=lambda dag_run: {
                "projectname": dag_run.conf.get("name", ""),
                "projectcode": dag_run.conf.get("internal_id", ""),
                "customer": dag_run.conf.get("customer", ""),
                "status": "Error",
                "details": f"Template '{rail.result('step_119_search_project_mapper')}' not found in Replicon",
            },
        )

        # Step 126: Create Project (with existing client)
        step_126_create_project_existing_client = rail.RepliconServiceOperator(
            task_id="step_126_create_project_existing_client",
            endpoint="/services/ProjectService1.svc/CreateProjectCopyBatch2",
            data=lambda dag_run: request_methods.create_project_copy_batch_request(
                dag_run,
                rail.result("step_123_get_template_project").get("uri"),
                dag_run.conf.get("customer", "")  # Use existing client name
            ),
        )

        # Step 127: Batch execution - Execute and wait for completion
        step_127_execute_batch, step_127_wait_for_batch = rail.batch_execution(
            'step_127_execute_batch', 'step_126_create_project_existing_client',
        )

        # Step 128: GetProjectCopyBatchResults
        # Response structure: {"d": {"error": null, "project": {"uri": "...", "name": "..."}}}
        step_128_get_project_copy_results = rail.RepliconServiceOperator(
            task_id="step_128_get_project_copy_results",
            endpoint="/services/ProjectService1.svc/GetProjectCopyBatchResults",
            data=lambda: {"projectCopyBatchUri": rail.result("step_126_create_project_existing_client")},
            data_handler=lambda response: response.get("project", {}) if response else None,
        )

        # Step 129: UpdateCode
        step_129_update_code = rail.RepliconServiceOperator(
            task_id="step_129_update_code",
            endpoint="/services/ProjectService1.svc/UpdateCode",
            data=lambda dag_run: request_methods.update_project_code_request(
                rail.result("step_128_get_project_copy_results").get("uri"),
                dag_run.conf.get("internal_id", "")
            ),
        )

        # Step 130: IF subsidiary name is present (Path C)
        step_130_if_has_subsidiary_c = rail.IfOperator(
            task_id="step_130_if_has_subsidiary_c",
            test=lambda dag_run: bool(dag_run.conf.get("subsidiary")),
            yes_task="step_131_update_subsidiary_c",
            no_task="step_132_update_project_c",
        )

        # Step 131: Call recipe updatesubsidiary_project (inside IF Yes branch)
        # Matches Workato: "Call recipe updatesubsidiary_project (do not wait for response)"
        # Triggers the update_subsidiary_child DAG asynchronously
        step_131_update_subsidiary_c = rail.TriggerDagRunOperator(
            task_id="step_131_update_subsidiary_c",
            trigger_dag_id=config.update_subsidiary_dag_id,
            conf=lambda dag_run: {
                "projecturi": rail.result("step_128_get_project_copy_results").get("uri"),
                "subsidiaryvalue": dag_run.conf.get("subsidiary"),
            },
            wait_for_completion=False,  # "do not wait for response" in Workato
        )

        # Step 132: Update project in Replicon (Path C)
        # Updates: end_date, custom_project_department, custom_p_l_type
        # Note: Subsidiary is updated separately in Step 131
        step_132_update_project_c = rail.RepliconServiceOperator(
            task_id="step_132_update_project_c",
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=lambda dag_run: request_methods.update_project_custom_fields_request(
                dag_run,
                rail.result("step_128_get_project_copy_results").get("uri")
            ),
        )

        # Step 133: IF budget present (Path C)
        step_133_if_has_budget_c = rail.IfOperator(
            task_id="step_133_if_has_budget_c",
            test=lambda dag_run: bool(dag_run.conf.get("total_budget")),
            yes_task="step_134_log_budget_c",
            no_task="step_136_get_all_users_c",
        )

        # Step 134: Logger - Total Budget (Path C)
        step_134_log_budget_c = rail.PythonOperator(
            task_id="step_134_log_budget_c",
            python_callable=lambda dag_run: custom_methods.parse_budget_amount(
                dag_run.conf.get("total_budget")
            ),
        )

        # Step 135: UpdateTotalEstimatedContractValue (Path C)
        step_135_update_budget_c = rail.RepliconServiceOperator(
            task_id="step_135_update_budget_c",
            endpoint="/services/ProjectService1.svc/UpdateTotalEstimatedContractValue",
            data=lambda dag_run: request_methods.update_total_estimated_contract_value_request(
                rail.result("step_128_get_project_copy_results").get("uri"),
                custom_methods.parse_budget_amount(dag_run.conf.get("total_budget")),
            ),
        )

        # Step 136: GetAlluserfromreplicon (Path C)
        step_136_get_all_users_c = rail.RepliconServiceOperator(
            task_id="step_136_get_all_users_c",
            endpoint="/services/UserlistService1.svc/GetData",
            data=lambda dag_run: request_methods.get_search_user_request(
                dag_run.conf.get("project_manager", "")
            ),
            data_handler=custom_methods.get_project_manager_data_handler,
        )

        # Step 141: Logger - PM URI (Path C)
        step_141_log_pm_uri_c = rail.PythonOperator(
            task_id="step_141_log_pm_uri_c",
            python_callable=lambda: rail.result("step_136_get_all_users_c"),
        )

        # Step 142: IF PM found (Path C)
        step_142_if_pm_found_c = rail.IfOperator(
            task_id="step_142_if_pm_found_c",
            test=lambda: bool(rail.result("step_136_get_all_users_c")),
            yes_task="step_143_get_pm_permissions_c",
            no_task="step_log_project_created_no_pm_c",
        )

        step_log_project_created_no_pm_c = rail.WriteLogOperator(
            task_id="step_log_project_created_no_pm_c",
            log='{{result("create_log")}}',
            message="Project created but PM not assigned",
            severity="Exception",
            properties=lambda dag_run: {
                "projectname": dag_run.conf.get("name", ""),
                "projectcode": dag_run.conf.get("internal_id", ""),
                "customer": dag_run.conf.get("customer", ""),
                "status": "Exception",
                "details": f"Project created via template copy (PM not found in Replicon)",
            },
        )

        # Step 143: GetAssignedPermissionSetsForUser2 (Path C)
        step_143_get_pm_permissions_c = rail.RepliconServiceOperator(
            task_id="step_143_get_pm_permissions_c",
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data=lambda: request_methods.get_user_permission_sets_v2_request(
                rail.result("step_136_get_all_users_c")
            ),
            data_handler=lambda response: custom_methods.has_project_management_permission(response),
        )

        # Step 144: Logger - PM permission (Path C)
        step_144_log_pm_permission_c = rail.PythonOperator(
            task_id="step_144_log_pm_permission_c",
            python_callable=lambda: rail.result("step_143_get_pm_permissions_c"),
        )

        # Step 145: IF PM has permission (Path C)
        step_145_if_pm_has_permission_c = rail.IfOperator(
            task_id="step_145_if_pm_has_permission_c",
            test=lambda: rail.result("step_143_get_pm_permissions_c") is True,
            yes_task="step_146_update_project_leader_c",
            no_task="step_147_if_pm_no_permission_c",
        )

        # Step 146: UpdateProjectLeader (Path C - has permission)
        step_146_update_project_leader_c = rail.RepliconServiceOperator(
            task_id="step_146_update_project_leader_c",
            endpoint="/services/ProjectService1.svc/UpdateProjectLeader",
            data=lambda: request_methods.update_project_leader_request(
                rail.result("step_128_get_project_copy_results").get("uri"),
                rail.result("step_136_get_all_users_c")
            ),
        )

        # Step 147: IF PM no permission (Path C)
        step_147_if_pm_no_permission_c = rail.IfOperator(
            task_id="step_147_if_pm_no_permission_c",
            test=lambda: rail.result("step_143_get_pm_permissions_c") is not True,
            yes_task="step_147a_get_all_permission_sets_c",
            no_task="step_log_project_created_c",
        )

        # Step 147a: Get all permission sets from system (Path C)
        # Dynamically fetch permission sets instead of hardcoding URI
        step_147a_get_all_permission_sets_c = rail.RepliconServiceOperator(
            task_id="step_147a_get_all_permission_sets_c",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data={},
        )

        # Step 147b: Find Project Management permission set URI (Path C)
        step_147b_find_pm_permission_uri_c = rail.PythonOperator(
            task_id="step_147b_find_pm_permission_uri_c",
            python_callable=lambda: custom_methods.find_project_management_permission_set_uri(
                rail.result("step_147a_get_all_permission_sets_c")
            ),
        )

        # Step 148: AssignPermissionSetToUser (Path C)
        step_148_assign_pm_permission_c = rail.RepliconServiceOperator(
            task_id="step_148_assign_pm_permission_c",
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data=lambda: request_methods.assign_permission_set_to_user_v2_request(
                rail.result("step_136_get_all_users_c"),
                rail.result("step_147b_find_pm_permission_uri_c")
            ),
        )

        # Step 149: UpdateProjectLeader (Path C - after permission)
        step_149_update_project_leader_c = rail.RepliconServiceOperator(
            task_id="step_149_update_project_leader_c",
            endpoint="/services/ProjectService1.svc/UpdateProjectLeader",
            data=lambda: request_methods.update_project_leader_request(
                rail.result("step_128_get_project_copy_results").get("uri"),
                rail.result("step_136_get_all_users_c")
            ),
        )

        step_log_project_created_c = rail.WriteLogOperator(
            task_id="step_log_project_created_c",
            log='{{result("create_log")}}',
            message="Project created successfully",
            severity="Success",
            properties=lambda dag_run: {
                "projectname": dag_run.conf.get("name", ""),
                "projectcode": dag_run.conf.get("internal_id", ""),
                "customer": dag_run.conf.get("customer", ""),
                "status": "Success",
                "details": "Project created via template copy",
            },
        )

        # ============================================================================
        # PHASE 4: EXISTING PROJECT UPDATE - Steps 150-175
        # ============================================================================

        # Step 150: IF Project exists (found) -> Update path
        step_150_if_project_exists = rail.RepliconServiceOperator(
            task_id="step_150_get_project_details",
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data=lambda: {
                "projects": [
                    {
                        "uri": rail.result("step_36_search_project_by_code").get("uri"),
                        "name": null,
                        "code": null,
                        "parameterCorrelationId": null,
                    }
                ]
            },
            data_handler=custom_methods.parse_project_response,
        )

        # Step 151: IF NOT inactive
        step_151_if_not_inactive = rail.IfOperator(
            task_id="step_151_if_not_inactive",
            test=lambda dag_run: dag_run.conf["status"] != "Closed",
            yes_task="step_152_update_project",
            no_task="step_172_if_has_subsidiary_existing",  # Inactive -> still check subsidiary before status
        )

        # Step 152: Update project
        step_152_update_project = rail.RepliconServiceOperator(
            task_id="step_152_update_project",
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=lambda dag_run: request_methods.update_project_request(dag_run)[0],
        )

        # Step 154: Update Start Date (UpdateTimeEntryDateRange) - SKIPPED in Workato
        # step_154_update_start_date = rail.RepliconServiceOperator(
        #     task_id="step_154_update_start_date",
        #     endpoint="/services/ProjectService1.svc/UpdateTimeEntryDateRange",
        #     data=lambda dag_run: request_methods.update_time_entry_date_range_request(
        #         rail.result("step_150_get_project_details").get("uri"),
        #         custom_methods.parse_date_safe(dag_run.conf.get("start_date", "")),
        #     ),
        # )

        # Step 155: IF budget present (for existing project)
        step_155_if_has_budget_existing = rail.IfOperator(
            task_id="step_155_if_has_budget_existing",
            test=lambda dag_run: bool(dag_run.conf.get("total_budget")),
            yes_task="step_156_log_budget",
            no_task="step_158_get_all_users_existing",
        )

        # Step 156: Logger - Total Budget
        step_156_log_budget = rail.PythonOperator(
            task_id="step_156_log_budget",
            python_callable=lambda dag_run: custom_methods.parse_budget_amount(
                dag_run.conf.get("total_budget")
            ),
        )

        # Step 157: UpdateTotalEstimatedContractValue (for existing project)
        step_157_update_budget_existing = rail.RepliconServiceOperator(
            task_id="step_157_update_budget_existing",
            endpoint="/services/ProjectService1.svc/UpdateTotalEstimatedContractValue",
            data=lambda dag_run: request_methods.update_total_estimated_contract_value_request(
                rail.result("step_150_get_project_details").get("uri"),
                custom_methods.parse_budget_amount(dag_run.conf.get("total_budget")),
            ),
        )

        # Step 158: GetAlluserfromreplicon (for existing project PM update)
        step_158_get_all_users_existing = rail.RepliconServiceOperator(
            task_id="step_158_get_all_users_existing",
            endpoint="/services/UserlistService1.svc/GetData",
            data=lambda dag_run: request_methods.get_search_user_request(
                dag_run.conf.get("project_manager", "")
            ),
            data_handler=custom_methods.get_project_manager_data_handler,
        )

        # Step 164: IF PM found (existing project)
        step_164_if_pm_found_existing = rail.IfOperator(
            task_id="step_164_if_pm_found_existing",
            test=lambda: bool(rail.result("step_158_get_all_users_existing")),
            yes_task="step_165_get_pm_permissions_existing",
            no_task="step_172_if_has_subsidiary_existing_no_pm",  # PM not found -> separate path with Exception log
        )

        # Step 165: GetAssignedPermissionSetsForUser2 (existing)
        step_165_get_pm_permissions_existing = rail.RepliconServiceOperator(
            task_id="step_165_get_pm_permissions_existing",
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data=lambda: request_methods.get_user_permission_sets_v2_request(
                rail.result("step_158_get_all_users_existing")
            ),
            data_handler=lambda response: custom_methods.has_project_management_permission(response),
        )

        # Step 167: IF PM has permission (existing)
        step_167_if_pm_has_permission_existing = rail.IfOperator(
            task_id="step_167_if_pm_has_permission_existing",
            test=lambda: rail.result("step_165_get_pm_permissions_existing") is True,
            yes_task="step_168_update_project_leader_existing",
            no_task="step_169_if_pm_no_permission_existing",
        )

        # Step 168: UpdateProjectLeader (existing, has permission)
        step_168_update_project_leader_existing = rail.RepliconServiceOperator(
            task_id="step_168_update_project_leader_existing",
            endpoint="/services/ProjectService1.svc/UpdateProjectLeader",
            data=lambda: request_methods.update_project_leader_request(
                rail.result("step_150_get_project_details").get("uri"),
                rail.result("step_158_get_all_users_existing")
            ),
        )

        # Step 169: IF PM no permission (existing)
        step_169_if_pm_no_permission_existing = rail.IfOperator(
            task_id="step_169_if_pm_no_permission_existing",
            test=lambda: rail.result("step_165_get_pm_permissions_existing") is not True,
            yes_task="step_169a_get_all_permission_sets_existing",
            no_task="step_172_if_has_subsidiary_existing",  # Has permission -> continue to subsidiary check
        )

        # Step 169a: Get all permission sets from system (Existing Project path)
        # Dynamically fetch permission sets instead of hardcoding URI
        step_169a_get_all_permission_sets_existing = rail.RepliconServiceOperator(
            task_id="step_169a_get_all_permission_sets_existing",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data={},
        )

        # Step 169b: Find Project Management permission set URI (Existing Project path)
        step_169b_find_pm_permission_uri_existing = rail.PythonOperator(
            task_id="step_169b_find_pm_permission_uri_existing",
            python_callable=lambda: custom_methods.find_project_management_permission_set_uri(
                rail.result("step_169a_get_all_permission_sets_existing")
            ),
        )

        # Step 170: AssignPermissionSet (existing)
        step_170_assign_pm_permission_existing = rail.RepliconServiceOperator(
            task_id="step_170_assign_pm_permission_existing",
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data=lambda: request_methods.assign_permission_set_to_user_v2_request(
                rail.result("step_158_get_all_users_existing"),
                rail.result("step_169b_find_pm_permission_uri_existing")
            ),
        )

        # Step 171: UpdateProjectLeader (existing, after permission)
        step_171_update_project_leader_existing = rail.RepliconServiceOperator(
            task_id="step_171_update_project_leader_existing",
            endpoint="/services/ProjectService1.svc/UpdateProjectLeader",
            data=lambda: request_methods.update_project_leader_request(
                rail.result("step_150_get_project_details").get("uri"),
                rail.result("step_158_get_all_users_existing")
            ),
        )

        # Step 172-173: IF subsidiary present -> Update (existing)
        step_172_if_has_subsidiary_existing = rail.IfOperator(
            task_id="step_172_if_has_subsidiary_existing",
            test=lambda dag_run: (
                dag_run.conf.get("subsidiary") and
                dag_run.conf.get("subsidiary") != rail.result("step_150_get_project_details", {}).get("project_subsidiary")
            ),
            yes_task="step_173_update_subsidiary_existing",
            no_task="step_174_if_inactive",  # No subsidiary update needed -> check inactive status
        )

        # Step 173: Call recipe updatesubsidiary_project (do not wait for response)
        # Matches Workato: Triggers the update_subsidiary_child DAG asynchronously
        step_173_update_subsidiary_existing = rail.TriggerDagRunOperator(
            task_id="step_173_update_subsidiary_existing",
            trigger_dag_id=config.update_subsidiary_dag_id,
            conf=lambda dag_run: {
                "projecturi": rail.result("step_150_get_project_details").get("uri"),
                "subsidiaryvalue": (dag_run.conf.get("subsidiary")),
            },
            wait_for_completion=False,  # "do not wait for response" in Workato
        )

        # Step 174-175: IF inactive -> Skip status update; IF active -> Update status
        # Workato: IF Inactive is true -> skip; IF Inactive is false -> Update status
        step_174_if_inactive = rail.IfOperator(
            task_id="step_174_if_inactive",
            test=lambda dag_run: dag_run.conf.get("status") == "Closed",
            yes_task="step_175_update_project_status",  
            no_task="step_log_project_updated",
        )

        # Step 175: Update project status to Completed
        # Only executed for Active projects (Workato: No branch of Step 174)
        step_175_update_project_status = rail.RepliconServiceOperator(
            task_id="step_175_update_project_status",
            endpoint="/services/ProjectService1.svc/UpdateStatus",
            data=lambda: {
                "projectUri": rail.result("step_150_get_project_details").get("uri"),
                "projectStatusUri": "urn:replicon:project-status-type:completed",
            },
        )

        # ============================================================================
        # PHASE 4B: EXISTING PROJECT UPDATE - PM NOT FOUND PATH (Steps 172-175 parallel)
        # When PM is not found, we still need to check subsidiary and status
        # but end with a different log (Exception instead of Success)
        # ============================================================================

        # Step 172 (PM not found path): IF subsidiary present -> Update (existing)
        step_172_if_has_subsidiary_existing_no_pm = rail.IfOperator(
            task_id="step_172_if_has_subsidiary_existing_no_pm",
            test=lambda dag_run: (
                dag_run.conf.get("subsidiary") and
                dag_run.conf.get("subsidiary") != rail.result("step_150_get_project_details", {}).get("project_subsidiary")
            ),
            yes_task="step_173_update_subsidiary_existing_no_pm",
            no_task="step_174_if_inactive_no_pm",
        )

        # Step 173 (PM not found path): Update subsidiary
        step_173_update_subsidiary_existing_no_pm = rail.TriggerDagRunOperator(
            task_id="step_173_update_subsidiary_existing_no_pm",
            trigger_dag_id=config.update_subsidiary_dag_id,
            conf=lambda dag_run: {
                "projecturi": rail.result("step_150_get_project_details").get("uri"),
                "subsidiaryvalue": (dag_run.conf.get("subsidiary")),
            },
            wait_for_completion=False,
        )

        # Step 174 (PM not found path): IF inactive -> Skip status update
        step_174_if_inactive_no_pm = rail.IfOperator(
            task_id="step_174_if_inactive_no_pm",
            test=lambda dag_run: dag_run.conf.get("status") == "Closed",
            yes_task="step_175_update_project_status_no_pm",
            no_task="step_log_project_updated_no_pm", 
        )

        # Step 175 (PM not found path): Update project status
        step_175_update_project_status_no_pm = rail.RepliconServiceOperator(
            task_id="step_175_update_project_status_no_pm",
            endpoint="/services/ProjectService1.svc/UpdateStatus",
            data=lambda: {
                "projectUri": rail.result("step_150_get_project_details").get("uri"),
                "projectStatusUri": "urn:replicon:project-status-type:completed",
            },
        )

        # ============================================================================
        # PHASE 5: COMPLETION LOGGING
        # ============================================================================

        step_log_project_created = rail.WriteLogOperator(
            task_id="step_log_project_created",
            log='{{result("create_log")}}',
            message="Project created successfully",
            severity="Success",
            properties=lambda dag_run: {
                "projectname": dag_run.conf.get("name", ""),
                "projectcode": dag_run.conf.get("internal_id", ""),
                "customer": dag_run.conf.get("customer", ""),
                "status": "Success",
                "details": "Project created via template copy",
            },
        )

        step_log_project_updated = rail.WriteLogOperator(
            task_id="step_log_project_updated",
            log='{{result("create_log")}}',
            message="Project updated successfully",
            severity="Success",
            properties=lambda dag_run: {
                "projectname": dag_run.conf.get("name", ""),
                "projectcode": dag_run.conf.get("internal_id", ""),
                "customer": dag_run.conf.get("customer", ""),
                "status": "Success",
                "details": "Existing project updated",
            },
        )

        # Log operator for existing project update when PM is not found
        step_log_project_updated_no_pm = rail.WriteLogOperator(
            task_id="step_log_project_updated_no_pm",
            log='{{result("create_log")}}',
            message="Project updated but PM not assigned",
            severity="Exception",
            properties=lambda dag_run: {
                "projectname": dag_run.conf.get("name", ""),
                "projectcode": dag_run.conf.get("internal_id", ""),
                "customer": dag_run.conf.get("customer", ""),
                "status": "Exception",
                "details": f"Existing project updated (PM not found in Replicon)",
            },
        )

        # ============================================================================
        # PHASE 6: ERROR HANDLING - Steps 176-179
        # ============================================================================

        step_177_catch_errors = rail.WriteLogOperator(
            task_id="step_177_catch_errors",
            log='{{result("create_log")}}',
            trigger_rule="one_failed",
            message="Error processing project",
            severity="Error",
            properties=lambda dag_run: {
                "projectname": dag_run.conf.get("name", ""),
                "projectcode": dag_run.conf.get("internal_id", ""),
                "customer": dag_run.conf.get("customer", ""),
                "status": "Error",
                "details": rail.render_template("{{get_error_message()}}"),
            },
        )

        # ============================================================================
        # TASK DEPENDENCIES
        # ============================================================================

        # Batch task control flow
        create_log >> can_run_batch_task
        can_run_batch_task >> rail.Label("Yes") >> batch_task >> step_177_catch_errors
        can_run_batch_task >> rail.Label("No") >> step_16_17_if_valid_project_record

        # Validation flow
        step_16_17_if_valid_project_record >> rail.Label("No") >> step_17_log_invalid_data

        # Main processing flow
        (
            step_16_17_if_valid_project_record
            >> rail.Label("Yes")
            >> step_28_if_export_enabled
        )

        step_28_if_export_enabled >> rail.Label("No") >> step_176_log_export_disabled
        (
            step_28_if_export_enabled
            >> rail.Label("Yes")
            >> step_29_log_start_processing
            >> step_32_parse_start_date
            >> step_36_search_project_by_code
            >> step_39_log_project_uri
            >> step_40_if_project_not_found
        )

        # === NEW PROJECT PATH (Step 40 Yes) ===
        (
            step_40_if_project_not_found
            >> rail.Label("Yes")
            >> step_41_search_client_by_code
            >> step_44_log_client_uri
            >> step_45_if_client_not_found
        )

        # Client not found -> check client name
        (
            step_45_if_client_not_found
            >> rail.Label("Yes")
            >> step_47_log_client_name
            >> step_48_if_client_name_present
        )

        # PATH A: Client name present (Steps 48-80)
        (
            step_48_if_client_name_present
            >> rail.Label("Yes")
            >> step_49_search_project_mapper
            >> step_50_if_subsidiary_mapped
        )

        step_50_if_subsidiary_mapped >> rail.Label("No") >> step_51_stop_subsidiary_not_found

        (
            step_50_if_subsidiary_mapped
            >> rail.Label("Yes")
            >> step_53_get_template_project
            >> step_54_if_template_not_found
        )

        step_54_if_template_not_found >> rail.Label("No") >> step_55_stop_template_not_found

        (
            step_54_if_template_not_found
            >> rail.Label("Yes")
            >> step_56_create_client
            >> step_57_create_project_copy_batch
            >> step_58_execute_batch
            >> step_58_wait_for_batch
            >> step_59_get_project_copy_results
            >> step_60_update_code
            >> step_61_if_has_subsidiary
        )

        step_61_if_has_subsidiary >> rail.Label("Yes") >> step_62_update_subsidiary >> step_63_update_project
        step_61_if_has_subsidiary >> rail.Label("No") >> step_63_update_project

        step_63_update_project >> step_64_if_has_budget

        step_64_if_has_budget >> rail.Label("Yes") >> step_65_log_budget >> step_66_update_budget >> step_67_get_all_users_report
        step_64_if_has_budget >> rail.Label("No") >> step_67_get_all_users_report

        step_67_get_all_users_report >> step_72_log_pm_uri >> step_73_if_pm_found

        step_73_if_pm_found >> rail.Label("No") >> step_log_project_created_no_pm >> step_177_catch_errors

        (
            step_73_if_pm_found
            >> rail.Label("Yes")
            >> step_74_get_pm_permissions
            >> step_75_log_pm_permission
            >> step_76_if_pm_has_permission
        )

        step_76_if_pm_has_permission >> rail.Label("Yes") >> step_77_update_project_leader >> step_log_project_created

        (
            step_76_if_pm_has_permission
            >> rail.Label("No")
            >> step_78_if_pm_no_permission
            >> rail.Label("Yes")
            >> step_78a_get_all_permission_sets
            >> step_78b_find_pm_permission_uri
            >> step_79_assign_pm_permission
            >> step_80_update_project_leader
            >> step_log_project_created
        )

        # step_78_if_pm_no_permission No branch - PM already has permission, just log success
        step_78_if_pm_no_permission >> rail.Label("No") >> step_log_project_created

        # PATH B: Client name blank (Steps 81-117)
        (
            step_48_if_client_name_present
            >> rail.Label("No")
            >> step_81_if_client_name_blank
            >> step_82_search_project_mapper
            >> step_83_if_subsidiary_not_mapped
        )

        step_83_if_subsidiary_not_mapped >> rail.Label("No") >> step_84_stop_subsidiary_not_found

        (
            step_83_if_subsidiary_not_mapped
            >> rail.Label("Yes")
            >> step_86_get_template_project
            >> step_87_if_template_not_found
        )

        step_87_if_template_not_found >> rail.Label("No") >> step_88_stop_template_not_found

        (
            step_87_if_template_not_found
            >> rail.Label("Yes")
            >> step_93_create_project_no_client
            >> step_94_execute_batch
            >> step_94_wait_for_batch
            >> step_96_get_project_copy_results
            >> step_97_update_code
            >> step_98_if_has_subsidiary_b
        )

        # Path B: Subsidiary update
        step_98_if_has_subsidiary_b >> rail.Label("Yes") >> step_99_update_subsidiary_b >> step_100_update_project_b
        step_98_if_has_subsidiary_b >> rail.Label("No") >> step_100_update_project_b

        step_100_update_project_b >> step_101_if_has_budget_b

        # Path B: Budget update
        step_101_if_has_budget_b >> rail.Label("Yes") >> step_102_log_budget_b >> step_103_update_budget_b >> step_104_get_all_users_b
        step_101_if_has_budget_b >> rail.Label("No") >> step_104_get_all_users_b

        step_104_get_all_users_b >> step_109_log_pm_uri_b >> step_110_if_pm_found_b

        # Path B: PM not found
        step_110_if_pm_found_b >> rail.Label("No") >> step_log_project_created_no_pm_b >> step_177_catch_errors

        # Path B: PM found - check permissions
        (
            step_110_if_pm_found_b
            >> rail.Label("Yes")
            >> step_111_get_pm_permissions_b
            >> step_112_log_pm_permission_b
            >> step_113_if_pm_has_permission_b
        )

        step_113_if_pm_has_permission_b >> rail.Label("Yes") >> step_114_update_project_leader_b >> step_log_project_created_b

        (
            step_113_if_pm_has_permission_b
            >> rail.Label("No")
            >> step_115_if_pm_no_permission_b
            >> rail.Label("Yes")
            >> step_115a_get_all_permission_sets_b
            >> step_115b_find_pm_permission_uri_b
            >> step_116_assign_pm_permission_b
            >> step_117_update_project_leader_b
            >> step_log_project_created_b
        )

        # step_115_if_pm_no_permission_b No branch - PM already has permission, just log success
        step_115_if_pm_no_permission_b >> rail.Label("No") >> step_log_project_created_b

        # PATH C: Client exists (Steps 118-149)
        (
            step_45_if_client_not_found
            >> rail.Label("No")
            >> step_118_if_client_exists
            >> step_119_search_project_mapper
            >> step_120_if_subsidiary_not_mapped
        )

        step_120_if_subsidiary_not_mapped >> rail.Label("No") >> step_121_stop_subsidiary_not_found

        (
            step_120_if_subsidiary_not_mapped
            >> rail.Label("Yes")
            >> step_123_get_template_project
            >> step_124_if_template_not_found
        )

        step_124_if_template_not_found >> rail.Label("No") >> step_125_stop_template_not_found

        (
            step_124_if_template_not_found
            >> rail.Label("Yes")
            >> step_126_create_project_existing_client
            >> step_127_execute_batch
            >> step_127_wait_for_batch
            >> step_128_get_project_copy_results
            >> step_129_update_code
            >> step_130_if_has_subsidiary_c  # Step 130: IF subsidiary is present
        )

        # Path C: Step 130 IF branch
        # Yes → Step 131 (update subsidiary) → Step 132 (update project)
        # No → Step 132 (update project)
        step_130_if_has_subsidiary_c >> rail.Label("Yes") >> step_131_update_subsidiary_c >> step_132_update_project_c
        step_130_if_has_subsidiary_c >> rail.Label("No") >> step_132_update_project_c

        # Path C: Step 132 → Step 133 (budget check)
        step_132_update_project_c >> step_133_if_has_budget_c

        # Path C: Budget update
        step_133_if_has_budget_c >> rail.Label("Yes") >> step_134_log_budget_c >> step_135_update_budget_c >> step_136_get_all_users_c
        step_133_if_has_budget_c >> rail.Label("No") >> step_136_get_all_users_c

        step_136_get_all_users_c >> step_141_log_pm_uri_c >> step_142_if_pm_found_c

        # Path C: PM not found
        step_142_if_pm_found_c >> rail.Label("No") >> step_log_project_created_no_pm_c >> step_177_catch_errors

        # Path C: PM found - check permissions
        (
            step_142_if_pm_found_c
            >> rail.Label("Yes")
            >> step_143_get_pm_permissions_c
            >> step_144_log_pm_permission_c
            >> step_145_if_pm_has_permission_c
        )

        step_145_if_pm_has_permission_c >> rail.Label("Yes") >> step_146_update_project_leader_c >> step_log_project_created_c

        (
            step_145_if_pm_has_permission_c
            >> rail.Label("No")
            >> step_147_if_pm_no_permission_c
            >> rail.Label("Yes")
            >> step_147a_get_all_permission_sets_c
            >> step_147b_find_pm_permission_uri_c
            >> step_148_assign_pm_permission_c
            >> step_149_update_project_leader_c
            >> step_log_project_created_c
        )

        # step_147_if_pm_no_permission_c No branch - PM already has permission, just log success
        step_147_if_pm_no_permission_c >> rail.Label("No") >> step_log_project_created_c

        # === EXISTING PROJECT PATH (Step 40 No -> Step 150) ===
        (
            step_40_if_project_not_found
            >> rail.Label("No")
            >> step_150_if_project_exists
            >> step_151_if_not_inactive
        )

        # Active project update path
        (
            step_151_if_not_inactive
            >> rail.Label("Yes")
            >> step_152_update_project
            >> step_155_if_has_budget_existing
        )

        step_155_if_has_budget_existing >> rail.Label("Yes") >> step_156_log_budget >> step_157_update_budget_existing >> step_158_get_all_users_existing
        step_155_if_has_budget_existing >> rail.Label("No") >> step_158_get_all_users_existing

        step_158_get_all_users_existing >> step_164_if_pm_found_existing

        # PM not found path - goes to separate chain with Exception log
        step_164_if_pm_found_existing >> rail.Label("No") >> step_172_if_has_subsidiary_existing_no_pm

        (
            step_164_if_pm_found_existing
            >> rail.Label("Yes")
            >> step_165_get_pm_permissions_existing
            >> step_167_if_pm_has_permission_existing
        )

        step_167_if_pm_has_permission_existing >> rail.Label("Yes") >> step_168_update_project_leader_existing >> step_172_if_has_subsidiary_existing

        (
            step_167_if_pm_has_permission_existing
            >> rail.Label("No")
            >> step_169_if_pm_no_permission_existing
            >> rail.Label("Yes")
            >> step_169a_get_all_permission_sets_existing
            >> step_169b_find_pm_permission_uri_existing
            >> step_170_assign_pm_permission_existing
            >> step_171_update_project_leader_existing
            >> step_172_if_has_subsidiary_existing
        )

        # step_169_if_pm_no_permission_existing No branch - PM already has permission, continue to subsidiary check
        step_169_if_pm_no_permission_existing >> rail.Label("No") >> step_172_if_has_subsidiary_existing

        # Step 172-173: Subsidiary update flows to Step 174 (inactive check)
        step_172_if_has_subsidiary_existing >> rail.Label("Yes") >> step_173_update_subsidiary_existing >> step_174_if_inactive
        step_172_if_has_subsidiary_existing >> rail.Label("No") >> step_174_if_inactive

        # Inactive project path (from Step 151 No branch - still goes through Step 172 for subsidiary check)
        step_151_if_not_inactive >> rail.Label("No") >> step_172_if_has_subsidiary_existing

        # Step 174-175: Status update based on source status
        # Workato: IF Inactive is true -> Yes: skip; No: update status
        step_174_if_inactive >> rail.Label("Yes") >> step_log_project_updated  # Closed - skip status update
        step_174_if_inactive >> rail.Label("No") >> step_175_update_project_status >> step_log_project_updated  # Active - update status

        # PM NOT FOUND PATH: Step 172-175 parallel chain with Exception log
        # This path is taken when PM is not found in Replicon (step_164 No)
        step_172_if_has_subsidiary_existing_no_pm >> rail.Label("Yes") >> step_173_update_subsidiary_existing_no_pm >> step_174_if_inactive_no_pm
        step_172_if_has_subsidiary_existing_no_pm >> rail.Label("No") >> step_174_if_inactive_no_pm

        # Step 174-175 (PM not found): Status update with Exception log
        step_174_if_inactive_no_pm >> rail.Label("Yes") >> step_log_project_updated_no_pm  # Closed - skip status update
        step_174_if_inactive_no_pm >> rail.Label("No") >> step_175_update_project_status_no_pm >> step_log_project_updated_no_pm  # Active - update status

        # Error handling - all terminal paths
        step_17_log_invalid_data >> step_177_catch_errors
        step_176_log_export_disabled >> step_177_catch_errors
        step_51_stop_subsidiary_not_found >> step_177_catch_errors
        step_55_stop_template_not_found >> step_177_catch_errors
        step_84_stop_subsidiary_not_found >> step_177_catch_errors
        step_88_stop_template_not_found >> step_177_catch_errors
        step_121_stop_subsidiary_not_found >> step_177_catch_errors
        step_125_stop_template_not_found >> step_177_catch_errors
        step_log_project_created >> step_177_catch_errors
        step_log_project_created_b >> step_177_catch_errors
        step_log_project_created_c >> step_177_catch_errors
        step_log_project_updated >> step_177_catch_errors
        step_log_project_updated_no_pm >> step_177_catch_errors

        return dag


rail.for_each_instance(create_child_dag)
