from datetime import timedelta

from airflow.models import Variable
import rail

from valleychildrens.user_import.utils import request_payload, response_filter

null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_add_user_dagid,
        description='ValleyChildrens User Import - Process Add User',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_add_user,
    ) as dag:
        rail.ViewDagRunConfOperator(task_id='view_dagrun_conf')
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='search_user_by_login',
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='search_user_by_login',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        search_user_by_login = rail.RepliconServiceOperator(
            task_id='search_user_by_login',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data=lambda dag_run: {
                'users': [{
                    'loginName': dag_run.conf['loginname'],
                    'uri': null,
                    'employeeId': null,
                    'parameterCorrelationId': null,
                }],
                'dataLoadOptionUri': 'urn:replicon:data-load-option:omit-data-if-insufficient-access-permission',
            },
            data_handler=response_filter.first_or_none,
        )

        does_user_exist = rail.IfOperator(
            task_id='does_user_exist',
            test=lambda: bool(rail.result('search_user_by_login')),
            yes_task='is_user_disabled',
            no_task='get_all_permission_sets',
        )

        is_user_disabled = rail.IfOperator(
            task_id='is_user_disabled',
            test=lambda: not bool(
                (rail.result('search_user_by_login') or {})
                    .get('userDetails', {})
                    .get('isEnabled', True)
            ),
            yes_task='log_user_disabled',
            no_task='log_login_in_use',
        )

        log_user_disabled = rail.WriteLogOperator(
            task_id='log_user_disabled',
            log='{{ dag_run.conf["log_id"] }}',
            severity='Exception',
            message=lambda dag_run: f"User with login name '{dag_run.conf['loginname']}' is available in Disabled status.",
            properties=lambda dag_run: {
                'employee_id': dag_run.conf['employeeid'],
                'first_name': dag_run.conf['firstname'],
                'last_name': dag_run.conf['lastname'],
                'action': 'Add',
                'status': 'Exception',
                'details': f"User with login name '{dag_run.conf['loginname']}' is available in Disabled status.",
            },
        )

        log_login_in_use = rail.WriteLogOperator(
            task_id='log_login_in_use',
            log='{{ dag_run.conf["log_id"] }}',
            severity='Exception',
            message=lambda dag_run: f"Login {dag_run.conf['loginname']} already in use — user not created",
            properties=lambda dag_run: {
                'employee_id': dag_run.conf['employeeid'],
                'first_name': dag_run.conf['firstname'],
                'last_name': dag_run.conf['lastname'],
                'action': 'Add',
                'status': 'Exception',
                'details': f"Login {dag_run.conf['loginname']} already in use",
            },
        )

        get_all_permission_sets = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets',
            data={},
        )

        add_user = rail.RepliconServiceOperator(
            task_id='add_user',
            endpoint='/services/ImportService1.svc/PutUser2',
            data=lambda dag_run: {
                'user': {
                    'target': {
                        'uri': null,
                        'loginName': dag_run.conf['loginname'],
                        'parameterCorrelationId': null,
                    },
                    'firstname': dag_run.conf['firstname'],
                    'lastname': dag_run.conf['lastname'],
                    'emailAddress': dag_run.conf['email'],
                    'employeeId': dag_run.conf['employeeid'],
                    'department': null,
                    'supervisorAssignmentSchedule': null,
                    'schedulePolicySchedule': request_payload.build_office_schedule_schedule(dag_run),
                    'workWeekStartDayUri': dag_run.conf.get('workweekstartdayuri'),
                    'employmentDateRange': {
                        'startDate': request_payload.to_date_struct(dag_run.conf.get('startdate')),
                        'endDate': request_payload.to_date_struct(dag_run.conf.get('enddate')),
                        'relativeDateRangeUri': null,
                        'relativeDateRangeAsOfDate': null,
                    },
                    'securityConfiguration': {
                        'enabledAuthenticationTypeUris': [
                            dag_run.conf.get('authenticationtypeuri')
                            or 'urn:replicon:user-authentication-type:sso',
                        ],
                        'isLoginEnabled': 'true',
                        'loginName': dag_run.conf['loginname'],
                        'SSOName': dag_run.conf['loginname'],
                        'password': null,
                    },
                    'holidayCalendar': {'uri': dag_run.conf.get('holidaycalendaruri'), 'name': None}
                        if dag_run.conf.get('holidaycalendaruri') else null,
                    'timeOffPolicy': null,
                    'permissionSets': [
                        {'uri': null, 'name': 'Project Resource with Reports'},
                    ],
                    'policySets': request_payload.build_policy_sets(dag_run),
                    'employeeType': null,
                    'costRateSchedule': null,
                    'payrollRateSchedule': null,
                    'timesheetPeriodTypeUri': null,
                    'defaultBillingRate': null,
                    'timesheetApprovalPath': request_payload.build_timesheet_approval_path(dag_run),
                    'expenseApprovalPath': null,
                    'timeOffApprovalPath': request_payload.build_timeoff_approval_path(dag_run),
                    'customFieldValues': request_payload.build_custom_field_values(dag_run),
                    'assignedActivities': null,
                    'timeZone': null,
                    'overtimeRuleAssignmentSchedule': null,
                    'validationRuleAssignmentSchedule': null,
                    'locationSchedule': null,
                    'divisionSchedule': null,
                    'costCenterSchedule': null,
                    'serviceCenterSchedule': request_payload.build_service_center_schedule(dag_run),
                    'departmentGroupSchedule': request_payload.build_department_group_schedule(dag_run),
                    'employeeTypeGroupSchedule': request_payload.build_employee_type_group_schedule(dag_run),
                    'timesheetPeriodSchedule': request_payload.build_timesheet_period_schedule(dag_run),
                    'policyDataAccessScopes': [],
                    'policyDataAccessScopes2': [],
                    'payRuleScriptSchedule': null,
                    'displayNameParameter': [],
                },
            },
        )

        update_activity_assignments = rail.RepliconServiceOperator(
            task_id='update_activity_assignments',
            endpoint='/services/ActivityService1.svc/UpdateActivityAssignmentsForUser',
            data=lambda dag_run: {
                'userUri': rail.result('add_user')['uri'],
                'activityUris': dag_run.conf.get('activities') or [],
            },
        )

        clear_timeoff_assignments = rail.RepliconServiceOperator(
            task_id='clear_timeoff_assignments',
            endpoint='/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser',
            data=lambda dag_run: {
                'userUri': rail.result('add_user')['uri'],
                'timeOffTypeUris': [],
            },
        )

        has_supervisor = rail.IfOperator(
            task_id='has_supervisor',
            test=lambda dag_run: bool(dag_run.conf.get('supid')) or bool(dag_run.conf.get('supname')),
            yes_task='get_supervisor_user',
            no_task='trigger_timeoff_add_new_user',
        )

        get_supervisor_user = rail.RepliconServiceOperator(
            task_id='get_supervisor_user',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data=lambda dag_run: {
                'users': [{
                    'employeeId': dag_run.conf.get('supid'),
                    'loginName': null,
                    'uri': null,
                    'parameterCorrelationId': null,
                }],
                'dataLoadOptionUri': 'urn:replicon:data-load-option:omit-data-if-insufficient-access-permission',
            },
            data_handler=response_filter.first_or_none,
        )

        supervisor_found = rail.IfOperator(
            task_id='supervisor_found',
            test=lambda: bool(rail.result('get_supervisor_user')),
            yes_task='assign_manager_permission',
            no_task='log_supervisor_pending',
        )

        assign_manager_permission = rail.RepliconServiceOperator(
            task_id='assign_manager_permission',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data=lambda dag_run: {
                'userUri': rail.result('get_supervisor_user')['userDetails']['uri'],
                'permissionSetUri': dag_run.conf['supervisorpermissionuri'],
            },
        )

        assign_supervisor = rail.RepliconServiceOperator(
            task_id='assign_supervisor',
            endpoint='/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange',
            data=lambda dag_run: {
                'userUri': rail.result('add_user')['uri'],
                'supervisorUri': rail.result('get_supervisor_user')['userDetails']['uri'],
                'dateRange': null,
            },
        )

        log_supervisor_pending = rail.WriteLogOperator(
            task_id='log_supervisor_pending',
            log='{{ dag_run.conf["supervisor_log_id"] }}',
            severity='Pending',
            message=lambda dag_run: f"Supervisor assignment pending for employee {dag_run.conf['employeeid']}",
            properties=lambda dag_run: {
                'employee_id': dag_run.conf['employeeid'],
                'sup_id': dag_run.conf.get('supid'),
                'sup_name': dag_run.conf.get('supname'),
                'useruri': rail.result('add_user')['uri'],
                'supervisorpermissionuri': dag_run.conf.get('supervisorpermissionuri'),
                'userpermissionuri': dag_run.conf.get('userpermissionuri'),
            },
        )

        trigger_timeoff_add_new_user = rail.TriggerDagRunOperator(
            task_id='trigger_timeoff_add_new_user',
            trigger_dag_id=config.process_timeoff_add_new_user_dagid,
            conf=lambda dag_run: request_payload.get_process_timeoff_add_new_user_conf(
                {**dict(dag_run.conf), 'user_uri': rail.result('add_user')['uri']},
                config, dag_run.conf.get('log_id')),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        wait_timeoff_add_new_user = rail.WaitForDagRunsSensor(
            task_id='wait_timeoff_add_new_user',
            dag_runs="{{ result('trigger_timeoff_add_new_user') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        log_success = rail.WriteLogOperator(
            task_id='log_success',
            log='{{ dag_run.conf["log_id"] }}',
            severity='Info',
            message=lambda dag_run: f"User {dag_run.conf['loginname']} created",
            properties=lambda dag_run: {
                'employee_id': dag_run.conf['employeeid'],
                'first_name': dag_run.conf['firstname'],
                'last_name': dag_run.conf['lastname'],
                'action': 'Add',
                'status': 'Success',
                'details': f"User {dag_run.conf['loginname']} created with URI {rail.result('add_user')['uri']}",
            },
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log='{{ dag_run.conf["log_id"] }}',
            severity='Error',
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                'employee_id': dag_run.conf['employeeid'],
                'first_name': dag_run.conf['firstname'],
                'last_name': dag_run.conf['lastname'],
                'action': 'Add',
                'status': 'Error',
                'details': '{{ get_error_message() }}',
            },
        )
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> search_user_by_login
        search_user_by_login >> does_user_exist
        does_user_exist >> rail.Label('Yes') >> is_user_disabled
        does_user_exist >> rail.Label('No') >> get_all_permission_sets >> add_user
        is_user_disabled >> rail.Label('Yes') >> log_user_disabled >> catch_and_log_error
        is_user_disabled >> rail.Label('No') >> log_login_in_use >> catch_and_log_error
        add_user >> update_activity_assignments >> clear_timeoff_assignments >> has_supervisor
        has_supervisor >> rail.Label('Yes') >> get_supervisor_user >> supervisor_found
        supervisor_found >> rail.Label('Yes') >> assign_manager_permission >> assign_supervisor >> trigger_timeoff_add_new_user
        supervisor_found >> rail.Label('No') >> log_supervisor_pending >> trigger_timeoff_add_new_user
        has_supervisor >> rail.Label('No') >> trigger_timeoff_add_new_user
        trigger_timeoff_add_new_user >> wait_timeoff_add_new_user >> log_success
        log_success >> catch_and_log_error
    return dag

rail.for_each_instance(create_child_dag)

