# pylint: disable=too-many-statements
import rail
from datetime import timedelta
from dataaxle.user_import.utils import custom_methods, request_payload


def create_update_user_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.child_update_user_dag_id,
        description=f"Dataaxle User Import - Update User child DAG {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_run_update_user_child,
        default_args={
            "execution_timeout": timedelta(days=config.execution_timeout_days),
        },
    ) as dag:

        view_dagrun_config = rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        # ── Fetch current user state from Replicon ─────────────────────────────
        get_user_data = rail.RepliconServiceOperator(
            task_id="get_user_data",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda dag_run: request_payload.get_user_data_by_uri_payload(dag_run.conf.get("user_uri"))
        )

        get_user_group_membership = rail.RepliconServiceOperator(
            task_id="get_user_group_membership",
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data=lambda dag_run: request_payload.get_user_group_membership_payload(dag_run.conf.get("user_uri"))
        )

        # ── Enable login if user is currently disabled (re-hire) ───────────────
        if_user_disabled = rail.IfOperator(
            task_id="if_user_disabled",
            test="{{ result('get_user_data')[0].userDetails.isEnabled | is_falsy }}",
            yes_task="enable_login",
            no_task="extract_login_name",
        )

        enable_login = rail.RepliconServiceOperator(
            task_id="enable_login",
            endpoint="/services/securityService1.svc/EnableLogin",
            data=lambda dag_run: {"userUri": dag_run.conf.get("user_uri")},
        )

        # ── Always update employment start date (hire/re-hire date) ───────────
        update_employment_date_range = rail.RepliconServiceOperator(
            task_id="update_employment_date_range",
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: request_payload.update_employment_date_range_payload(
                user_uri=dag_run.conf.get("user_uri"),
                start_date=dag_run.conf.get("hire_or_rehire")
            )
        )

        # ── Login / SSO name ───────────────────────────────────────────────────
        extract_login_name = rail.PythonOperator(
            task_id="extract_login_name",
            python_callable=lambda dag_run: dag_run.conf.get("email_id").split("@")[0],
        )

        if_login_name_changed = rail.IfOperator(
            task_id="if_login_name_changed",
            test="{{ result('extract_login_name') != result('get_user_data')[0].securityConfiguration.loginName }}",
            yes_task="update_login_and_sso_name",
            no_task="if_user_details_changed",
        )

        update_login_and_sso_name = rail.RepliconServiceOperator(
            task_id="update_login_and_sso_name",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda dag_run: request_payload.update_login_and_sso_name_payload(
                user_uri=dag_run.conf.get("user_uri"),
                login_name=rail.result("extract_login_name")
            ),
        )

        # ── Basic user details (name / email) ──────────────────────────────────
        if_user_details_changed = rail.IfOperator(
            task_id="if_user_details_changed",
            test="{{ dag_run.conf.email_id != result('get_user_data')[0].userDetails.emailAddress or \
                    dag_run.conf.first_name != result('get_user_data')[0].userDetails.firstName or \
                    dag_run.conf.last_name != result('get_user_data')[0].userDetails.lastName }}",
            yes_task="update_user_details",
            no_task="if_user_group_changed",
        )

        update_user_details = rail.RepliconServiceOperator(
            task_id="update_user_details",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda dag_run: request_payload.update_user_details(dag_run)
        )

        # ── Department / Holiday Calendar / Timezone / Location / Division ──────
        if_user_group_changed = rail.IfOperator(
            task_id="if_user_group_changed",
            test="{{ (dag_run.conf.department | is_truthy and \
                      dag_run.conf.company_name != (result('get_user_group_membership').departments[0].department.department.displayText \
                      if result('get_user_group_membership').departments | length > 0 else '')) or \
                    (dag_run.conf.holiday_calendar | is_truthy and \
                      dag_run.conf.holiday_calendar != (result('get_user_data')[0].holidayCalendar.name \
                      if result('get_user_data')[0].holidayCalendar else '')) or \
                    (dag_run.conf.timezone | is_truthy and \
                      dag_run.conf.timezone != (result('get_user_data')[0].timeZone.ianaName \
                      if result('get_user_data')[0].timeZone else '')) or \
                    (dag_run.conf.location_to_assign | is_truthy and \
                      dag_run.conf.location_to_assign != \
                      (result('get_user_group_membership').locations[0].location.location.displayText \
                      if result('get_user_group_membership').locations | length > 0 else '')) or \
                    (dag_run.conf.division | is_truthy and \
                      dag_run.conf.division != \
                      (result('get_user_group_membership').divisions[0].division.division.displayText \
                      if result('get_user_group_membership').divisions | length > 0 else '')) }}",
            yes_task="update_user_group_details",
            no_task="if_employee_type_group_changed",
        )

        update_user_group_details = rail.RepliconServiceOperator(
            task_id="update_user_group_details",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda dag_run: request_payload.build_group_membership_modifications_payload(dag_run),
        )

        # ── Employee type group ────────────────────────────────────────────────
        if_employee_type_group_changed = rail.IfOperator(
            task_id="if_employee_type_group_changed",
            test="{{ dag_run.conf.hrly_or_salary | is_truthy and \
                    dag_run.conf.employee_type_group | is_truthy and \
                    dag_run.conf.employee_type_group != \
                    (result('get_user_group_membership').employeeTypes[0].employeeType.employeeType.displayText \
                    if result('get_user_group_membership').employeeTypes | length > 0 else '') }}",
            yes_task="update_employee_type_group",
            no_task="if_service_center_changed",
        )

        update_employee_type_group = rail.RepliconServiceOperator(
            task_id="update_employee_type_group",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda dag_run: request_payload.update_employee_type_group_payload(dag_run)
        )

        if_employee_type_group_errors = rail.IfOperator(
            task_id="if_employee_type_group_errors",
            test="{{ result('update_employee_type_group').errors | is_truthy }}",
            yes_task="stop_employee_type_group_error",
            no_task="if_service_center_changed",
        )

        stop_employee_type_group_error = rail.FailOperator(
            task_id="stop_employee_type_group_error",
            message=lambda: rail.result("update_employee_type_group").get("errors"),
        )

        # ── Service center (job title) ─────────────────────────────────────────
        if_service_center_changed = rail.IfOperator(
            task_id="if_service_center_changed",
            test="{{ dag_run.conf.job_title | is_truthy and \
                    dag_run.conf.job_title != \
                    (result('get_user_group_membership').serviceCenters[0].serviceCenter.serviceCenter.displayText \
                    if result('get_user_group_membership').serviceCenters | length > 0 else '') }}",
            yes_task="update_service_center",
            no_task="get_effective_schedule_policy",
        )

        update_service_center = rail.RepliconServiceOperator(
            task_id="update_service_center",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda dag_run: request_payload.update_service_center_payload(dag_run),
        )

        # ── Schedule policy (standard hours) ───────────────────────────────────
        get_effective_schedule_policy = rail.PythonOperator(
            task_id="get_effective_schedule_policy",
            python_callable=custom_methods.get_effective_schedule_policy_name,
        )

        if_schedule_policy_changed = rail.IfOperator(
            task_id="if_schedule_policy_changed",
            test="{{ dag_run.conf.standard_hours | is_truthy and \
                    dag_run.conf.standard_hours != result('get_effective_schedule_policy') }}",
            yes_task="update_schedule_policy",
            no_task="if_supervisor_name_cf_present",
        )

        update_schedule_policy = rail.RepliconServiceOperator(
            task_id="update_schedule_policy",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda dag_run: request_payload.update_schedule_policy_payload(dag_run),
        )

        # ── Custom field: User's Supervisor Name ───────────────────────────────
        if_supervisor_name_cf_present = rail.IfOperator(
            task_id="if_supervisor_name_cf_present",
            test=lambda dag_run: dag_run.conf.get("user_supervisor_name_uri") and\
                dag_run.conf.get("user_supervisor_name_dropdown_uri") and\
                    custom_methods.is_change_in_custom_field_value(
                        "User's Supervisor Name", dag_run.conf.get("report_to_name")),
            yes_task="update_supervisor_name_cf",
            no_task="if_executive_level_cf_present",
        )

        update_supervisor_name_cf = rail.RepliconServiceOperator(
            task_id="update_supervisor_name_cf",
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run: request_payload.apply_custom_field_drop_down_value_payload(
                user_uri=dag_run.conf.get("user_uri"),
                custom_field_uri=dag_run.conf.get("user_supervisor_name_uri"),
                custom_field_drop_down_option_uri=dag_run.conf.get("user_supervisor_name_dropdown_uri")
            )
        )

        # ── Custom field: Executive level ──────────────────────────────────────
        if_executive_level_cf_present = rail.IfOperator(
            task_id="if_executive_level_cf_present",
            test=lambda dag_run: dag_run.conf.get("executive_level_uri") and\
                dag_run.conf.get("executive_level_dropdown_uri") and\
                    custom_methods.is_change_in_custom_field_value(
                        "Executive level", dag_run.conf.get("executive_level")),
            yes_task="update_executive_level_cf",
            no_task="if_payroll_dept_name_cf_present",
        )

        update_executive_level_cf = rail.RepliconServiceOperator(
            task_id="update_executive_level_cf",
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run: request_payload.apply_custom_field_drop_down_value_payload(
                user_uri=dag_run.conf.get("user_uri"),
                custom_field_uri=dag_run.conf.get("executive_level_uri"),
                custom_field_drop_down_option_uri=dag_run.conf.get("executive_level_dropdown_uri")
            )
        )

        # ── Custom field: Payroll Department (name) ────────────────────────────
        if_payroll_dept_name_cf_present = rail.IfOperator(
            task_id="if_payroll_dept_name_cf_present",
            test=lambda dag_run: dag_run.conf.get("payroll_department_name_uri") and\
                dag_run.conf.get("payroll_department_name_dropdown_uri") and\
                    custom_methods.is_change_in_custom_field_value(
                        "Payroll Department", dag_run.conf.get("payroll_dept_name")),
            yes_task="update_payroll_dept_name_cf",
            no_task="if_payroll_dept_no_cf_present",
        )

        update_payroll_dept_name_cf = rail.RepliconServiceOperator(
            task_id="update_payroll_dept_name_cf",
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run: request_payload.apply_custom_field_drop_down_value_payload(
                user_uri=dag_run.conf.get("user_uri"),
                custom_field_uri=dag_run.conf.get("payroll_department_name_uri"),
                custom_field_drop_down_option_uri=dag_run.conf.get("payroll_department_name_dropdown_uri")
            )
        )

        # ── Custom field: Payroll Dept # ───────────────────────────────────────
        if_payroll_dept_no_cf_present = rail.IfOperator(
            task_id="if_payroll_dept_no_cf_present",
            test=lambda dag_run: dag_run.conf.get("payroll_department_number_uri") and dag_run.conf.get("payroll_department_no_drop_down_uri") and custom_methods.is_change_in_custom_field_value(
                        "Payroll Dept #", dag_run.conf.get("payroll_dept_no")),
            yes_task="update_payroll_dept_no_cf",
            no_task="if_manager_id_present",
        )

        update_payroll_dept_no_cf = rail.RepliconServiceOperator(
            task_id="update_payroll_dept_no_cf",
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run: request_payload.apply_custom_field_drop_down_value_payload(
                user_uri=dag_run.conf.get("user_uri"),
                custom_field_uri=dag_run.conf.get("payroll_department_number_uri"),
                custom_field_drop_down_option_uri=dag_run.conf.get("payroll_department_no_drop_down_uri"),
            ),
        )

        # ── Supervisor assignment ──────────────────────────────────────────────
        if_manager_id_present = rail.IfOperator(
            task_id="if_manager_id_present",
            test="{{ dag_run.conf.reports_to_manager_id | is_truthy }}",
            yes_task="search_supervisor_by_emplid",
            no_task="add_user_updated_log",
        )

        search_supervisor_by_emplid = rail.RepliconServiceOperator(
            task_id="search_supervisor_by_emplid",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda dag_run: request_payload.get_users_details_payload(dag_run.conf.get("reports_to_manager_id"))
        )

        extract_supervisor_uri = rail.PythonOperator(
            task_id="extract_supervisor_uri",
            python_callable=custom_methods.extract_supervisor_uri_from_search,
        )

        get_current_supervisor = rail.RepliconServiceOperator(
            task_id="get_current_supervisor",
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda dag_run: request_payload.get_current_supervisor_payload(dag_run),
        )

        extract_current_supervisor_uri = rail.PythonOperator(
            task_id="extract_current_supervisor_uri",
            python_callable=lambda: custom_methods.extract_current_supervisor_uri(rail.result("get_current_supervisor")),
        )

        # Current supervisor present AND new supervisor URI present AND they differ
        if_current_supervisor_present = rail.IfOperator(
            task_id="if_current_supervisor_present",
            test="{{ result('extract_current_supervisor_uri') | is_truthy }}",
            yes_task="if_new_supervisor_present",
            no_task="if_new_supervisor_only",
        )

        if_new_supervisor_present = rail.IfOperator(
            task_id="if_new_supervisor_present",
            test="{{ result('extract_supervisor_uri') | is_truthy }}",
            yes_task="if_supervisor_changed",
            no_task="if_manager_email_present",
        )

        if_supervisor_changed = rail.IfOperator(
            task_id="if_supervisor_changed",
            test="{{ result('extract_supervisor_uri') != result('extract_current_supervisor_uri') }}",
            yes_task="get_new_supervisor_permission_sets",
            no_task="add_user_updated_log",
        )

        # No current supervisor but new one is provided — assign directly. Workato step 81
        if_new_supervisor_only = rail.IfOperator(
            task_id="if_new_supervisor_only",
            test="{{ result('extract_supervisor_uri') | is_truthy }}",
            yes_task="get_new_supervisor_permission_sets",
            no_task="if_manager_emplid_present",
        )

        # Workato step 76-79: new supervisor not found in Replicon while a current supervisor exists
        # → check manager email → trigger supervisor creation → search → assign
        if_manager_email_present = rail.IfOperator(
            task_id="if_manager_email_present",
            test="{{ dag_run.conf.manager_details.email_id | is_truthy }}",
            yes_task="trigger_create_supervisor_child",
            no_task="add_user_updated_log",
        )

        # Workato step 91-94: no current supervisor AND new supervisor not found in Replicon
        # → check manager emplid → trigger supervisor creation → search → assign
        if_manager_emplid_present = rail.IfOperator(
            task_id="if_manager_emplid_present",
            test="{{ dag_run.conf.manager_details.empl_id | is_truthy }}",
            yes_task="trigger_create_supervisor_child",
            no_task="add_user_updated_log",
        )

        trigger_create_supervisor_child = rail.TriggerDagRunOperator(
            task_id="trigger_create_supervisor_child",
            trigger_dag_id=config.child_create_user_supervisor_dag_id,
            wait_for_completion=True,
            conf=lambda dag_run: {
                **dag_run.conf.get("manager_details"),
                "user_import_log": dag_run.conf.get("user_import_log"),
                "parent_job_id": dag_run.conf.get("parent_job_id"),
            },
        )

        search_supervisor_after_creation = rail.RepliconServiceOperator(
            task_id="search_supervisor_after_creation",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda dag_run: request_payload.get_users_details_payload(
                dag_run.conf.get("reports_to_manager_id")
            ),
        )

        extract_supervisor_uri_after_creation = rail.PythonOperator(
            task_id="extract_supervisor_uri_after_creation",
            python_callable=custom_methods.extract_supervisor_uri_after_creation,
        )

        update_supervisor_after_creation = rail.RepliconServiceOperator(
            task_id="update_supervisor_after_creation",
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: request_payload.update_supervisor_payload(
                user_uri=dag_run.conf.get("user_uri"),
                supervisor_uri=rail.result("extract_supervisor_uri_after_creation"),
                start_date=dag_run.conf.get("today_date"),
            ),
        )

        get_new_supervisor_permission_sets = rail.RepliconServiceOperator(
            task_id="get_new_supervisor_permission_sets",
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data=lambda: {
                "userUri": rail.result("extract_supervisor_uri")
            },
        )

        # Check if 'Supervisor' permission set is already assigned to the new supervisor
        check_supervisor_has_supervisor_permission = rail.PythonOperator(
            task_id="check_supervisor_has_supervisor_permission",
            python_callable=lambda: custom_methods.get_supervisor_permission_uri_from_assigned(
                "get_new_supervisor_permission_sets"
            ),
        )

        if_supervisor_permission_missing = rail.IfOperator(
            task_id="if_supervisor_permission_missing",
            test="{{ result('check_supervisor_has_supervisor_permission') | is_falsy }}",
            yes_task="get_all_permission_sets",
            no_task="update_supervisor",
        )

        get_all_permission_sets = rail.RepliconServiceOperator(
            task_id="get_all_permission_sets",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets"
        )

        extract_supervisor_permission_uri = rail.PythonOperator(
            task_id="extract_supervisor_permission_uri",
            python_callable=custom_methods.get_supervisor_permission_uri_from_all,
        )

        if_supervisor_permission_uri_found = rail.IfOperator(
            task_id="if_supervisor_permission_uri_found",
            test="{{ result('extract_supervisor_permission_uri') | is_truthy }}",
            yes_task="assign_supervisor_permission",
            no_task="update_supervisor",
        )

        assign_supervisor_permission = rail.RepliconServiceOperator(
            task_id="assign_supervisor_permission",
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data=lambda: request_payload.assign_user_permission_payload(
                user_uri=rail.result("extract_supervisor_uri"),
                permission_uri=rail.result("extract_supervisor_permission_uri"),
            ),
        )

        update_supervisor = rail.RepliconServiceOperator(
            task_id="update_supervisor",
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: request_payload.update_supervisor_payload(
                user_uri=dag_run.conf.get("user_uri"),
                supervisor_uri=rail.result("extract_supervisor_uri"),
                start_date=dag_run.conf.get("today_date")
            ),
        )

        add_user_updated_log = rail.WriteLogOperator(
            task_id="add_user_updated_log",
            log="{{ dag_run.conf.user_import_log }}",
            severity="Success",
            message="User updated successfully",
            properties=lambda dag_run: custom_methods.build_user_import_log(dag_run, 
                action="update",
                status="success", 
                details="User updated successfully",
                parent_job_id=dag_run.conf.get("parent_job_id"),
                child_job_id=rail.render_template("{{ dag_run_ecid() }}"),
            ),
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            log="{{ dag_run.conf.user_import_log }}",
            trigger_rule="one_failed",
            severity="Error",
            message="{{ get_error_message() }}",
            properties=lambda dag_run: custom_methods.build_user_import_log(
                dag_run,
                action="update",
                status="failed",
                details="{{ get_error_message() }}",
                parent_job_id=dag_run.conf.get("parent_job_id"),
                child_job_id=rail.render_template("{{ dag_run_ecid() }}")
            )
        )
        

        # ── Dependency chain ───────────────────────────────────────────────────
        view_dagrun_config >> get_user_data >> get_user_group_membership >> if_user_disabled
        if_user_disabled >> rail.Label("Yes") >> enable_login >> update_employment_date_range >> extract_login_name
        if_user_disabled >> rail.Label("No") >> extract_login_name

        extract_login_name >> if_login_name_changed
        if_login_name_changed >> rail.Label("Yes") >> update_login_and_sso_name >> if_user_details_changed
        if_login_name_changed >> rail.Label("No") >> if_user_details_changed

        if_user_details_changed >> rail.Label("Yes") >> update_user_details >> if_user_group_changed
        if_user_details_changed >> rail.Label("No") >> if_user_group_changed

        if_user_group_changed >> rail.Label("Yes") >> update_user_group_details >> if_employee_type_group_changed
        if_user_group_changed >> rail.Label("No") >> if_employee_type_group_changed

        if_employee_type_group_changed >> rail.Label("Yes") >> update_employee_type_group >> if_employee_type_group_errors
        if_employee_type_group_errors >> rail.Label("Yes") >> stop_employee_type_group_error
        if_employee_type_group_errors >> rail.Label("No") >> if_service_center_changed
        if_employee_type_group_changed >> rail.Label("No") >> if_service_center_changed

        if_service_center_changed >> rail.Label("Yes") >> update_service_center >> get_effective_schedule_policy
        if_service_center_changed >> rail.Label("No") >> get_effective_schedule_policy

        get_effective_schedule_policy >> if_schedule_policy_changed
        if_schedule_policy_changed >> rail.Label("Yes") >> update_schedule_policy >> if_supervisor_name_cf_present
        if_schedule_policy_changed >> rail.Label("No") >> if_supervisor_name_cf_present

        if_supervisor_name_cf_present >> rail.Label("Yes") >> update_supervisor_name_cf >> if_executive_level_cf_present
        if_supervisor_name_cf_present >> rail.Label("No") >> if_executive_level_cf_present

        if_executive_level_cf_present >> rail.Label("Yes") >> update_executive_level_cf >> if_payroll_dept_name_cf_present
        if_executive_level_cf_present >> rail.Label("No") >> if_payroll_dept_name_cf_present

        if_payroll_dept_name_cf_present >> rail.Label("Yes") >> update_payroll_dept_name_cf >> if_payroll_dept_no_cf_present
        if_payroll_dept_name_cf_present >> rail.Label("No") >> if_payroll_dept_no_cf_present

        if_payroll_dept_no_cf_present >> rail.Label("Yes") >> update_payroll_dept_no_cf >> if_manager_id_present
        if_payroll_dept_no_cf_present >> rail.Label("No") >> if_manager_id_present

        if_manager_id_present >> rail.Label("Yes") >> search_supervisor_by_emplid >> extract_supervisor_uri >> get_current_supervisor >> extract_current_supervisor_uri >> if_current_supervisor_present
        if_manager_id_present >> rail.Label("No") >> add_user_updated_log >> catch_and_log_error

        if_current_supervisor_present >> rail.Label("Yes") >> if_new_supervisor_present
        if_current_supervisor_present >> rail.Label("No") >> if_new_supervisor_only

        if_new_supervisor_present >> rail.Label("Yes") >> if_supervisor_changed
        if_new_supervisor_present >> rail.Label("No") >> if_manager_email_present
        if_manager_email_present >> rail.Label("Yes") >> trigger_create_supervisor_child
        if_manager_email_present >> rail.Label("No") >> add_user_updated_log >> catch_and_log_error

        if_supervisor_changed >> rail.Label("Yes") >> get_new_supervisor_permission_sets
        if_supervisor_changed >> rail.Label("No") >> add_user_updated_log >> catch_and_log_error

        if_new_supervisor_only >> rail.Label("Yes") >> get_new_supervisor_permission_sets
        if_new_supervisor_only >> rail.Label("No") >> if_manager_emplid_present
        if_manager_emplid_present >> rail.Label("Yes") >> trigger_create_supervisor_child
        if_manager_emplid_present >> rail.Label("No") >> add_user_updated_log >> catch_and_log_error

        trigger_create_supervisor_child >> search_supervisor_after_creation >> extract_supervisor_uri_after_creation >> update_supervisor_after_creation >> add_user_updated_log >> catch_and_log_error

        get_new_supervisor_permission_sets >> check_supervisor_has_supervisor_permission >> if_supervisor_permission_missing
        if_supervisor_permission_missing >> rail.Label("Yes") >> get_all_permission_sets >> extract_supervisor_permission_uri >> if_supervisor_permission_uri_found
        if_supervisor_permission_missing >> rail.Label("No") >> update_supervisor

        if_supervisor_permission_uri_found >> rail.Label("Yes") >> assign_supervisor_permission >> update_supervisor
        if_supervisor_permission_uri_found >> rail.Label("No") >> update_supervisor

        update_supervisor >> add_user_updated_log >> catch_and_log_error

        return dag


rail.for_each_instance(create_update_user_child_dag)
