# pylint: disable=too-many-statements
import rail
from datetime import timedelta
from dataaxle.user_import.utils import custom_methods, request_payload


def create_create_user_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.child_create_user_dag_id,
        description=f"Dataaxle User Import - Create User child DAG {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_run_create_user_child,
        default_args={
            "execution_timeout": timedelta(days=config.execution_timeout_days),
        },
    ) as dag:

        view_dagrun_config = rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        # ── Step 2-7: Check if user already exists (possibly disabled/termed) ──
        # Workato steps 2-7: BulkGetUsers3 by employeeId → if found AND disabled
        # → trigger update_user_child (re-hire path) → finish early.
        search_user_by_emplid = rail.RepliconServiceOperator(
            task_id="search_user_by_emplid",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda dag_run: request_payload.get_users_details_payload(dag_run.conf.get("empl_id")),
        )

        extract_existing_user_data = rail.PythonOperator(
            task_id="extract_existing_user_data",
            python_callable=custom_methods.extract_existing_user_data,
        )

        if_user_exists_by_emplid = rail.IfOperator(
            task_id="if_user_exists_by_emplid",
            test="{{ result('extract_existing_user_data') | is_truthy }}",
            yes_task="if_existing_user_is_disabled",
            no_task="extract_login_name",
        )

        if_existing_user_is_disabled = rail.IfOperator(
            task_id="if_existing_user_is_disabled",
            test="{{ result('extract_existing_user_data').isEnabled | is_falsy }}",
            yes_task="trigger_update_user_for_rehire",
            no_task="extract_login_name",
        )

        # Forward all conf keys + override useruri with found URI for rehire
        trigger_update_user_for_rehire = rail.TriggerDagRunOperator(
            task_id="trigger_update_user_for_rehire",
            trigger_dag_id=config.child_update_user_dag_id,
            wait_for_completion=True,
            conf=lambda dag_run: {
                "user_uri": (rail.result("extract_existing_user_data") or {}).get("uri"),
                "empl_id": str(dag_run.conf.get("empl_id")),
                "email_id": dag_run.conf.get("email_id"),
                "first_name": dag_run.conf.get("first_name"),
                "last_name": dag_run.conf.get("last_name"),
                "term_date": dag_run.conf.get("term_date"),
                "location_description": dag_run.conf.get("location_description"),
                "company_name": dag_run.conf.get("company_name"),
                "location_state": dag_run.conf.get("location_state"),
                "country": dag_run.conf.get("country"),
                "payroll_dept_no": dag_run.conf.get("payroll_dept_no"),
                "payroll_dept_name": dag_run.conf.get("payroll_dept_name"),
                "rpc": dag_run.conf.get("rpc"),
                "job_code": dag_run.conf.get("job_code"),
                "job_title": dag_run.conf.get("job_title"),
                "standard_hours": dag_run.conf.get("standard_hours"),
                "hrly_or_salary": dag_run.conf.get("hrly_or_salary"),
                "reports_to_manager_id": dag_run.conf.get("reports_to_manager_id"),
                "executive_level": dag_run.conf.get("executive_level"),
                "report_to_name": dag_run.conf.get("report_to_name"),
                "empl_status": dag_run.conf.get("empl_status"),
                "md5": dag_run.conf.get("md5"),
                "hourly_billing_currency": dag_run.conf.get("hourly_billing_currency"),
                "hourly_cost": dag_run.conf.get("hourly_cost"),
                "hourly_payroll_currency": dag_run.conf.get("hourly_payroll_currency"),
                "holiday_calendar": dag_run.conf.get("holiday_calendar"),
                "timezone": dag_run.conf.get("timezone"),
                "division": dag_run.conf.get("division"),
                "department": dag_run.conf.get("department"),
                "location_to_assign": dag_run.conf.get("location_to_assign"),
                "payroll_department_number_uri": dag_run.conf.get("payroll_department_number_uri"),
                "payroll_department_name_uri": dag_run.conf.get("payroll_department_name_uri"),
                "executive_level_uri": dag_run.conf.get("executive_level_uri"),
                "user_supervisor_name_uri": dag_run.conf.get("user_supervisor_name_uri"),
                "payroll_department_no_drop_down_uri": dag_run.conf.get("payroll_department_no_drop_down_uri"),
                "payroll_department_name_dropdown_uri": dag_run.conf.get("payroll_department_name_dropdown_uri"),
                "executive_level_dropdown_uri": dag_run.conf.get("executive_level_dropdown_uri"),
                "user_supervisor_name_dropdown_uri": dag_run.conf.get("user_supervisor_name_dropdown_uri"),
                "currency_uri": dag_run.conf.get("currency_uri"),
                "employee_type_group": dag_run.conf.get("employee_type_group"),
                "manager_details": dag_run.conf.get("manager_details"),
                "parent_job_id": rail.render_template("{{ dag_run_ecid() }}"),
                "hire_or_rehire": dag_run.conf.get("hire_or_rehire"),
                "today_date": dag_run.conf.get("today_date"),
                "user_import_log": dag_run.conf.get("user_import_log")
            },
        )

        # ── Step 8-15: Derive login name and check for duplicates ──────────────
        # Workato steps 8-15: derive login name from email, search by loginName,
        # if exact login name match exists → fail (duplicate login guard).
        extract_login_name = rail.PythonOperator(
            task_id="extract_login_name",
            python_callable=lambda dag_run: custom_methods.extract_login_name(dag_run.conf.get("email_id"))
        )

        search_user_by_login = rail.RepliconServiceOperator(
            task_id="search_user_by_login",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=request_payload.build_bulk_get_users_payload,
        )

        check_duplicate_login = rail.PythonOperator(
            task_id="check_duplicate_login",
            python_callable=custom_methods.check_login_name_duplicate,
        )

        if_duplicate_login_name = rail.IfOperator(
            task_id="if_duplicate_login_name",
            test="{{ result('check_duplicate_login') | is_truthy }}",
            yes_task="log_duplicate_login_name",
            no_task="create_user",
        )

        log_duplicate_login_name = rail.WriteLogOperator(
            task_id="log_duplicate_login_name",
            log='{{ dag_run.conf.user_import_log }}',
            severity='Exception',
            message='Duplicate login name',
            properties=lambda dag_run: custom_methods.build_user_import_log(dag_run, 
                action="add",
                status="ignored", 
                details="A user already exist with same login name " + rail.result("extract_login_name"),
                parent_job_id=dag_run.conf.get("parent_job_id"),
                child_job_id=rail.render_template("{{ dag_run_ecid() }}"),
            ),
        )

        stop_duplicate_user_creation = rail.EmptyOperator(
            task_id="stop_duplicate_user_creation"
        )

        # ── Step 17: Create user via PutUser2 ─────────────────────────────────
        # Note: permissionSets is empty [] for create_user (differs from
        # create_user_supervisor which uses [{"name": "Supervisor"}]).
        create_user = rail.RepliconServiceOperator(
            task_id="create_user",
            endpoint="/services/ImportService1.svc/PutUser2",
            data=lambda dag_run: request_payload.build_create_user_payload(dag_run)
        )

        # ── Steps 18-19: Department ────────────────────────────────────────────
        # Workato step 18-19: if company_name → PutDepartmentGroupScheduleForUser
        # Uses DepartmentGroupService1 (not ApplyUserModifications2).
        if_department_present = rail.IfOperator(
            task_id="if_department_present",
            test="{{ dag_run.conf.department | is_truthy }}",
            yes_task="apply_department",
            no_task="if_any_user_attribute_modification_present",
        )

        apply_department = rail.RepliconServiceOperator(
            task_id="apply_department",
            endpoint="/services/DepartmentGroupService1.svc/PutDepartmentGroupScheduleForUser",
            data=lambda dag_run: request_payload.create_apply_department(dag_run),
        )

        # ── Steps 20-31: Apply all user attribute modifications in one API call ─
        # Combines: holiday calendar, timezone+location, division, employee type
        # group, service center, and schedule policy. Each is included only when
        # the corresponding conf field is present.
        if_any_user_attribute_modification_present = rail.IfOperator(
            task_id="if_any_user_attribute_modification_present",
            test="{{ (dag_run.conf.holiday_calendar | is_truthy) or "
                 "((dag_run.conf.timezone | is_truthy) and (dag_run.conf.location_to_assign | is_truthy)) or "
                 "(dag_run.conf.division | is_truthy) or "
                 "(dag_run.conf.hrly_or_salary | is_truthy) or "
                 "(dag_run.conf.job_title | is_truthy) or "
                 "(dag_run.conf.standard_hours | is_truthy) }}",
            yes_task="apply_user_attribute_modifications",
            no_task="if_payroll_dept_no_cf_present",
        )

        apply_user_attribute_modifications = rail.RepliconServiceOperator(
            task_id="apply_user_attribute_modifications",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda dag_run: request_payload.build_combined_user_modifications_payload(dag_run),
        )

        # ── Steps 32-33: Custom field — Payroll Dept # ────────────────────────
        if_payroll_dept_no_cf_present = rail.IfOperator(
            task_id="if_payroll_dept_no_cf_present",
            test="{{ dag_run.conf.payroll_department_number_uri | is_truthy and \
                    dag_run.conf.payroll_department_no_drop_down_uri | is_truthy }}",
            yes_task="apply_payroll_dept_no_cf",
            no_task="if_payroll_dept_name_cf_present",
        )

        apply_payroll_dept_no_cf = rail.RepliconServiceOperator(
            task_id="apply_payroll_dept_no_cf",
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run: request_payload.apply_custom_field_drop_down_value_payload(
                user_uri=(rail.result("create_user") or {}).get("uri"),
                custom_field_uri=dag_run.conf.get("payroll_department_number_uri"),
                custom_field_drop_down_option_uri=dag_run.conf.get("payroll_department_no_drop_down_uri")
            ),
        )

        # ── Steps 34-35: Custom field — Payroll Department name ───────────────
        if_payroll_dept_name_cf_present = rail.IfOperator(
            task_id="if_payroll_dept_name_cf_present",
            test="{{ dag_run.conf.payroll_department_name_uri | is_truthy and \
                    dag_run.conf.payroll_department_name_dropdown_uri | is_truthy }}",
            yes_task="apply_payroll_dept_name_cf",
            no_task="if_executive_level_cf_present",
        )

        apply_payroll_dept_name_cf = rail.RepliconServiceOperator(
            task_id="apply_payroll_dept_name_cf",
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run: request_payload.apply_custom_field_drop_down_value_payload(
                user_uri=(rail.result("create_user") or {}).get("uri"),
                custom_field_uri=dag_run.conf.get("payroll_department_name_uri"),
                custom_field_drop_down_option_uri=dag_run.conf.get("payroll_department_name_dropdown_uri")
            ),
        )

        # ── Steps 36-37: Custom field — Executive level ───────────────────────
        if_executive_level_cf_present = rail.IfOperator(
            task_id="if_executive_level_cf_present",
            test="{{ dag_run.conf.executive_level_uri | is_truthy and \
                    dag_run.conf.executive_level_dropdown_uri | is_truthy }}",
            yes_task="apply_executive_level_cf",
            no_task="if_supervisor_name_cf_present",
        )

        apply_executive_level_cf = rail.RepliconServiceOperator(
            task_id="apply_executive_level_cf",
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run: request_payload.apply_custom_field_drop_down_value_payload(
                user_uri=(rail.result("create_user") or {}).get("uri"),
                custom_field_uri=dag_run.conf.get("executive_level_uri"),
                custom_field_drop_down_option_uri=dag_run.conf.get("executive_level_dropdown_uri")
            ),
        )

        # ── Steps 38-39: Custom field — User's Supervisor Name ────────────────
        if_supervisor_name_cf_present = rail.IfOperator(
            task_id="if_supervisor_name_cf_present",
            test="{{ dag_run.conf.user_supervisor_name_uri | is_truthy and \
                    dag_run.conf.user_supervisor_name_dropdown_uri | is_truthy }}",
            yes_task="apply_supervisor_name_cf",
            no_task="if_hourly_payroll_currency_present",
        )

        apply_supervisor_name_cf = rail.RepliconServiceOperator(
            task_id="apply_supervisor_name_cf",
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run: request_payload.apply_custom_field_drop_down_value_payload(
                user_uri=(rail.result("create_user") or {}).get("uri"),
                custom_field_uri=dag_run.conf.get("user_supervisor_name_uri"),
                custom_field_drop_down_option_uri=dag_run.conf.get("user_supervisor_name_dropdown_uri")
            ),
        )

        # ── Steps 40-41: Hourly Payroll Currency (amount "0" for new users) ──────────────
        if_hourly_payroll_currency_present = rail.IfOperator(
            task_id="if_hourly_payroll_currency_present",
            test="{{ dag_run.conf.hourly_payroll_currency | is_truthy }}",
            yes_task="apply_hourly_payroll_currency",
            no_task="if_hourly_billing_currency_present",
        )

        apply_hourly_payroll_currency = rail.RepliconServiceOperator(
            task_id="apply_hourly_payroll_currency",
            endpoint="/services/PayrollService1.svc/PutUserPayrollRateSchedule",
            data=lambda dag_run: request_payload.apply_hourly_payroll_currency_payload(dag_run)
        )

        # ── Steps 42-43: Billing Currency (amount "0" for new users) ──────────────
        if_hourly_billing_currency_present = rail.IfOperator(
            task_id="if_hourly_billing_currency_present",
            test="{{ dag_run.conf.hourly_billing_currency | is_truthy }}",
            yes_task="update_user_specific_billing_rate_amount",
            no_task="if_hourly_cost_present",
        )

        update_user_specific_billing_rate_amount = rail.RepliconServiceOperator(
            task_id="update_user_specific_billing_rate_amount",
            endpoint="/services/BillingRateService1.svc/UpdateUserSpecificBillingRateAmount",
            data=lambda dag_run: request_payload.update_user_specific_billing_rate_amount_payload(dag_run)
        )

        # ── Steps 44-45: Hourly Cost (amount "0" for new users) ─────────────────
        if_hourly_cost_present = rail.IfOperator(
            task_id="if_hourly_cost_present",
            test="{{ dag_run.conf.hourly_cost | is_truthy }}",
            yes_task="apply_cost_rate",
            no_task="if_reports_to_manager_present",
        )

        apply_cost_rate = rail.RepliconServiceOperator(
            task_id="apply_cost_rate",
            endpoint="/services/ResourceService1.svc/UpdateUserCostRateScheduleOverDateRange",
            data=lambda dag_run: request_payload.apply_cost_rate_payload(dag_run)
        )

        # ── Steps 46-62: Supervisor assignment ────────────────────────────────
        # Step 46: Check if reports_to_manager_id is present in conf.
        if_reports_to_manager_present = rail.IfOperator(
            task_id="if_reports_to_manager_present",
            test="{{ dag_run.conf.reports_to_manager_id | is_truthy }}",
            yes_task="search_supervisor_by_emplid",
            no_task="add_user_creation_log",
        )

        # Step 47-48: Search Replicon for the supervisor by employee ID.
        search_supervisor_by_emplid = rail.RepliconServiceOperator(
            task_id="search_supervisor_by_emplid",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda dag_run: request_payload.get_users_details_payload(dag_run.conf.get("reports_to_manager_id"))
        )

        extract_supervisor_uri = rail.PythonOperator(
            task_id="extract_supervisor_uri",
            python_callable=custom_methods.extract_supervisor_uri_from_search,
        )

        # Step 49-50: Route based on whether supervisor was found in Replicon.
        if_supervisor_found = rail.IfOperator(
            task_id="if_supervisor_found",
            test="{{ result('extract_supervisor_uri') | is_truthy }}",
            yes_task="get_supervisor_permission_sets",
            no_task="if_supervisor_empl_id_present_in_dag_run_conf",
        )

        # ── Supervisor found path: permission check → assign ──────────────────
        get_supervisor_permission_sets = rail.RepliconServiceOperator(
            task_id="get_supervisor_permission_sets",
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data=lambda: {
                "userUri": rail.result("extract_supervisor_uri")
            },
        )

        check_supervisor_has_supervisor_permission = rail.PythonOperator(
            task_id="check_supervisor_has_supervisor_permission",
            python_callable=lambda: custom_methods.get_supervisor_permission_uri_from_assigned(
                "get_supervisor_permission_sets"
            ),
        )

        if_supervisor_permission_missing = rail.IfOperator(
            task_id="if_supervisor_permission_missing",
            test="{{ result('check_supervisor_has_supervisor_permission') | is_falsy }}",
            yes_task="get_all_permission_sets",
            no_task="assign_supervisor",
        )

        get_all_permission_sets = rail.RepliconServiceOperator(
            task_id="get_all_permission_sets",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets"
        )

        extract_supervisor_permission_uri = rail.PythonOperator(
            task_id="extract_supervisor_permission_uri",
            python_callable=custom_methods.get_supervisor_permission_uri_from_all,
        )

        assign_supervisor_permission = rail.RepliconServiceOperator(
            task_id="assign_supervisor_permission",
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data=lambda: request_payload.assign_user_permission_payload(
                user_uri=rail.result("extract_supervisor_uri"),
                permission_uri=rail.result("extract_supervisor_permission_uri"),
            )
        )

        assign_supervisor = rail.RepliconServiceOperator(
            task_id="assign_supervisor",
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data=lambda: request_payload.assign_supervisor_payload(
                user_uri=(rail.result("create_user") or {}).get("uri"),
                supervisor_uri=rail.result("extract_supervisor_uri")
            )
        )

        # ── Supervisor not found path: create supervisor first then assign ─────
        # Steps 59-62: If supervisor not in Replicon AND manager_details empl_id
        # is present in conf → trigger create_user_supervisor_child to create the
        # supervisor, then search for them again and assign.
        if_supervisor_empl_id_present_in_dag_run_conf = rail.IfOperator(
            task_id="if_supervisor_empl_id_present_in_dag_run_conf",
            test="{{ dag_run.conf.manager_details.empl_id | is_truthy }}",
            yes_task="trigger_create_supervisor_child",
            no_task="add_user_creation_log",
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

        # After creating the supervisor, search by emplid to obtain their URI
        search_supervisor_after_creation = rail.RepliconServiceOperator(
            task_id="search_supervisor_after_creation",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda dag_run: request_payload.get_users_details_payload(
                empl_id=dag_run.conf.get("reports_to_manager_id")
            )
        )

        extract_supervisor_uri_after_creation = rail.PythonOperator(
            task_id="extract_supervisor_uri_after_creation",
            python_callable=custom_methods.extract_supervisor_uri_after_creation,
        )

        assign_supervisor_after_creation = rail.RepliconServiceOperator(
            task_id="assign_supervisor_after_creation",
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data=lambda: request_payload.assign_supervisor_payload(
                user_uri=(rail.result("create_user") or {}).get("uri"),
                supervisor_uri=rail.result("extract_supervisor_uri_after_creation")
            )
        )

        add_user_creation_log = rail.WriteLogOperator(
            task_id="add_user_creation_log",
            log="{{ dag_run.conf.user_import_log }}",
            severity="Success",
            message="User added successfully",
            properties=lambda dag_run: custom_methods.build_user_import_log(dag_run, 
                action="add",
                status="success", 
                details="User added successfully",
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
                dag_run=dag_run,
                action="add",
                status="failed", 
                details="{{ get_error_message() }}",
                parent_job_id=dag_run.conf.get("parent_job_id"),
                child_job_id=rail.render_template("{{ dag_run_ecid() }}")
            )
        )

        finish = rail.EmptyOperator(task_id="finish")

        # ── Dependency chain ───────────────────────────────────────────────────

        # Step 2-7: Emplid check
        search_user_by_emplid >> extract_existing_user_data >> if_user_exists_by_emplid
        if_user_exists_by_emplid >> rail.Label("Yes") >> if_existing_user_is_disabled
        if_user_exists_by_emplid >> rail.Label("No") >> extract_login_name

        if_existing_user_is_disabled >> rail.Label("Yes") >> trigger_update_user_for_rehire >> finish
        if_existing_user_is_disabled >> rail.Label("No") >> extract_login_name

        # Step 8-15: Login name duplicate check
        extract_login_name >> search_user_by_login >> check_duplicate_login >> if_duplicate_login_name
        if_duplicate_login_name >> rail.Label("Yes") >> log_duplicate_login_name >> stop_duplicate_user_creation
        if_duplicate_login_name >> rail.Label("No") >> create_user

        # Step 17: Create user
        # Steps 18-19: Department
        create_user >> if_department_present
        if_department_present >> rail.Label("Yes") >> apply_department >> if_any_user_attribute_modification_present
        if_department_present >> rail.Label("No") >> if_any_user_attribute_modification_present

        # Steps 20-31: Apply all user attribute modifications in one combined API call
        if_any_user_attribute_modification_present >> rail.Label("Yes") >> apply_user_attribute_modifications >> if_payroll_dept_no_cf_present
        if_any_user_attribute_modification_present >> rail.Label("No") >> if_payroll_dept_no_cf_present

        # Steps 32-33: Payroll dept # custom field
        if_payroll_dept_no_cf_present >> rail.Label("Yes") >> apply_payroll_dept_no_cf >> if_payroll_dept_name_cf_present
        if_payroll_dept_no_cf_present >> rail.Label("No") >> if_payroll_dept_name_cf_present

        # Steps 34-35: Payroll dept name custom field
        if_payroll_dept_name_cf_present >> rail.Label("Yes") >> apply_payroll_dept_name_cf >> if_executive_level_cf_present
        if_payroll_dept_name_cf_present >> rail.Label("No") >> if_executive_level_cf_present

        # Steps 36-37: Executive level custom field
        if_executive_level_cf_present >> rail.Label("Yes") >> apply_executive_level_cf >> if_supervisor_name_cf_present
        if_executive_level_cf_present >> rail.Label("No") >> if_supervisor_name_cf_present

        # Steps 38-39: Supervisor name custom field
        if_supervisor_name_cf_present >> rail.Label("Yes") >> apply_supervisor_name_cf >> if_hourly_payroll_currency_present
        if_supervisor_name_cf_present >> rail.Label("No") >> if_hourly_payroll_currency_present

        # Steps 40-41: Payroll rate
        if_hourly_payroll_currency_present >> rail.Label("Yes") >> apply_hourly_payroll_currency >> if_hourly_billing_currency_present
        if_hourly_payroll_currency_present >> rail.Label("No") >> if_hourly_billing_currency_present

        # Steps 42-43: Billing rate
        if_hourly_billing_currency_present >> rail.Label("Yes") >> update_user_specific_billing_rate_amount >> if_hourly_cost_present
        if_hourly_billing_currency_present >> rail.Label("No") >> if_hourly_cost_present

        # Steps 44-45: Cost rate
        if_hourly_cost_present >> rail.Label("Yes") >> apply_cost_rate >> if_reports_to_manager_present
        if_hourly_cost_present >> rail.Label("No") >> if_reports_to_manager_present

        # Steps 46-58: Supervisor assignment (supervisor found in Replicon)
        if_reports_to_manager_present >> rail.Label("Yes") >> search_supervisor_by_emplid >> extract_supervisor_uri >> if_supervisor_found
        if_reports_to_manager_present >> rail.Label("No") >> add_user_creation_log >> catch_and_log_error

        if_supervisor_found >> rail.Label("Yes") >> get_supervisor_permission_sets >> check_supervisor_has_supervisor_permission >> if_supervisor_permission_missing
        if_supervisor_found >> rail.Label("No") >> if_supervisor_empl_id_present_in_dag_run_conf

        if_supervisor_permission_missing >> rail.Label("Yes") >> get_all_permission_sets >> extract_supervisor_permission_uri >> assign_supervisor_permission >> assign_supervisor
        if_supervisor_permission_missing >> rail.Label("No") >> assign_supervisor

        assign_supervisor >> add_user_creation_log >> catch_and_log_error

        # Steps 59-62: Supervisor not found — create them first then assign
        if_supervisor_empl_id_present_in_dag_run_conf >> rail.Label("Yes") >> trigger_create_supervisor_child >> search_supervisor_after_creation >> extract_supervisor_uri_after_creation >> assign_supervisor_after_creation >> add_user_creation_log >> catch_and_log_error
        if_supervisor_empl_id_present_in_dag_run_conf >> rail.Label("No") >> add_user_creation_log >> catch_and_log_error

        return dag


rail.for_each_instance(create_create_user_child_dag)
