"""
Sand Tech Inc - Child DAG for Adding New Users
"""

from datetime import timedelta, datetime
import itertools
import pendulum
from airflow.models import Variable
import rail
from rail.lib.ecid import get_dagrun_ecid

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.add_user_child_dagid,
        description=f'Sand Tech Inc - Child Add User {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='declare_exceptions_list'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='declare_exceptions_list',
            end_task='log_to_sumo',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # ========== INITIALIZE ==========
        declare_exceptions_list = rail.SetVariableOperator(
            task_id='declare_exceptions_list',
            append=False,
            name='exceptions',
            value=[]
        )

        # ========== CHECK IF USER ALREADY EXISTS BY EMAIL ==========
        def page_handler(request, result):
            if len(result['rows']) > 0:
                request['page'] += 1
                return request
            return None

        def all_result_data_handler(result, email):
            flattened_rows = list(itertools.chain(*[x['rows'] for x in result]))
            existing_user = [
                {
                    'username': row['cells'][0].get('textValue'),
                    'employeeid': row['cells'][2].get('textValue'),
                    'status': row['cells'][3].get('textValue'),
                    'loginname': row['cells'][1].get('textValue'),
                    'useruri': row['cells'][1].get('uri')
                }
                for row in flattened_rows
                if row['cells'][1].get('textValue', '').lower() == email.lower()
            ]
            return existing_user[0] if existing_user else {}

        search_user_by_email = rail.RepliconServicePageOperator(
            task_id="search_user_by_email",
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda dag_run: {
                'page': 1,
                'pagesize': 100,
                'columnUris': [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': dag_run.conf['email']
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda result, dag_run: all_result_data_handler(
                result, dag_run.conf['email'])
        )

        user_already_exists = rail.IfOperator(
            task_id='user_already_exists',
            test='{{ result("search_user_by_email") | is_truthy }}',
            yes_task="log_user_exists_exception",
            no_task="parse_start_date",
        )

        log_user_exists_exception = rail.WriteLogOperator(
            task_id='log_user_exists_exception',
            message="Exception - User already exists",
            severity="Exception",
            properties={
                "Empid": "{{ dag_run.conf.employee_id }}",
                "Username": "{{ dag_run.conf.first_name }} {{ dag_run.conf.last_name }}",
                "Action": "Add",
                "Status": "Exception",
                "Details": "User not added - Login name {{ dag_run.conf.email }} already exists in Replicon",
                "Jobid": "{{ dag_run_ecid() }}"
            }
        )

        # ========== PARSE DATES ==========
        def parse_date(date_str, date_format):
            """Parse date string to Replicon date format"""
            if not date_str:
                return None
            try:
                parsed = datetime.strptime(date_str.strip(), date_format)
                return {
                    'year': parsed.year,
                    'month': parsed.month,
                    'day': parsed.day
                }
            except ValueError:
                return None

        parse_start_date = rail.PythonOperator(
            task_id='parse_start_date',
            python_callable=lambda dag_run: parse_date(
                dag_run.conf.get('start_date'), dag_run.conf.get('date_format', '%d/%m/%Y'))
        )

        parse_end_date = rail.PythonOperator(
            task_id='parse_end_date',
            python_callable=lambda dag_run: parse_date(
                dag_run.conf.get('last_day_of_work'), dag_run.conf.get('date_format', '%d/%m/%Y'))
        )

        # ========== BUILD PERMISSION SETS ==========
        declare_permission_sets = rail.SetVariableOperator(
            task_id='declare_permission_sets',
            append=False,
            name='permission_sets',
            value=[]
        )

        add_project_resource_permission = rail.SetVariableOperator(
            task_id='add_project_resource_permission',
            append=True,
            name='{{ result("declare_permission_sets").name }}',
            value={
                "uri": "{{ dag_run.conf.project_resource_permission_uri }}",
                "name": null
            }
        )

        is_manager = rail.IfOperator(
            task_id='is_manager',
            test='{{ dag_run.conf.is_a_manager | lower == "yes" }}',
            yes_task="add_supervisor_permission",
            no_task="build_policy_sets",
        )

        add_supervisor_permission = rail.SetVariableOperator(
            task_id='add_supervisor_permission',
            append=True,
            name='{{ result("declare_permission_sets").name }}',
            value={
                "uri": "{{ dag_run.conf.supervisor_permission_uri }}",
                "name": null
            }
        )

        # ========== BUILD POLICY SETS ==========
        build_policy_sets = rail.SetVariableOperator(
            task_id='build_policy_sets',
            append=False,
            name='policy_sets',
            value=[]
        )

        has_timesheet_template = rail.IfOperator(
            task_id='has_timesheet_template',
            test='{{ dag_run.conf.timesheet_template_uri | is_truthy }}',
            yes_task="add_timesheet_template",
            no_task="has_timeoff_template",
        )

        add_timesheet_template = rail.SetVariableOperator(
            task_id='add_timesheet_template',
            append=True,
            name='{{ result("build_policy_sets").name }}',
            value={
                "uri": "{{ dag_run.conf.timesheet_template_uri }}",
                "name": null
            }
        )

        has_timeoff_template = rail.IfOperator(
            task_id='has_timeoff_template',
            test='{{ dag_run.conf.timeoff_template_uri | is_truthy }}',
            yes_task="add_timeoff_template",
            no_task="build_department_schedule",
        )

        add_timeoff_template = rail.SetVariableOperator(
            task_id='add_timeoff_template',
            append=True,
            name='{{ result("build_policy_sets").name }}',
            value={
                "uri": "{{ dag_run.conf.timeoff_template_uri }}",
                "name": null
            }
        )

        # ========== BUILD SCHEDULES ==========
        build_department_schedule = rail.SetVariableOperator(
            task_id='build_department_schedule',
            append=False,
            name='department_schedule',
            value=None
        )

        has_department = rail.IfOperator(
            task_id='has_department',
            test='{{ dag_run.conf.department_uri | is_truthy }}',
            yes_task="set_department_schedule",
            no_task="build_location_schedule",
        )

        set_department_schedule = rail.SetVariableOperator(
            task_id='set_department_schedule',
            append=False,
            name='{{ result("build_department_schedule").name }}',
            value=[{
                "departmentGroup": {
                    "uri": "{{ dag_run.conf.department_uri }}",
                    "parent": null,
                    "name": null
                },
                "effectiveDate": null
            }]
        )

        build_location_schedule = rail.SetVariableOperator(
            task_id='build_location_schedule',
            append=False,
            name='location_schedule',
            value=None
        )

        has_location = rail.IfOperator(
            task_id='has_location',
            test='{{ dag_run.conf.location_uri | is_truthy }}',
            yes_task="set_location_schedule",
            no_task="build_holiday_calendar",
        )

        set_location_schedule = rail.SetVariableOperator(
            task_id='set_location_schedule',
            append=False,
            name='{{ result("build_location_schedule").name }}',
            value=[{
                "location": {
                    "uri": "{{ dag_run.conf.location_uri }}",
                    "parentUri": null,
                    "name": null
                },
                "effectiveDate": null
            }]
        )

        # ========== BUILD HOLIDAY CALENDAR ==========
        build_holiday_calendar = rail.SetVariableOperator(
            task_id='build_holiday_calendar',
            append=False,
            name='holiday_calendar',
            value=None
        )

        has_holiday_calendar = rail.IfOperator(
            task_id='has_holiday_calendar',
            test='{{ dag_run.conf.holiday_calendar_uri | is_truthy }}',
            yes_task="set_holiday_calendar",
            no_task="build_timezone",
        )

        set_holiday_calendar = rail.SetVariableOperator(
            task_id='set_holiday_calendar',
            append=False,
            name='{{ result("build_holiday_calendar").name }}',
            value={
                "uri": "{{ dag_run.conf.holiday_calendar_uri }}",
                "name": null
            }
        )

        # ========== BUILD TIMEZONE ==========
        build_timezone = rail.SetVariableOperator(
            task_id='build_timezone',
            append=False,
            name='timezone',
            value=None
        )

        has_timezone = rail.IfOperator(
            task_id='has_timezone',
            test='{{ dag_run.conf.timezone_uri | is_truthy }}',
            yes_task="set_timezone",
            no_task="build_employee_type",
        )

        set_timezone = rail.SetVariableOperator(
            task_id='set_timezone',
            append=False,
            name='{{ result("build_timezone").name }}',
            value={
                "uri": "{{ dag_run.conf.timezone_uri }}",
                "IANAName": null
            }
        )

        # ========== BUILD EMPLOYEE TYPE ==========
        build_employee_type = rail.SetVariableOperator(
            task_id='build_employee_type',
            append=False,
            name='employee_type_schedule',
            value=None
        )

        has_employee_type = rail.IfOperator(
            task_id='has_employee_type',
            test='{{ dag_run.conf.employee_type_uri | is_truthy }}',
            yes_task="set_employee_type",
            no_task="build_timesheet_period",
        )

        set_employee_type = rail.SetVariableOperator(
            task_id='set_employee_type',
            append=False,
            name='{{ result("build_employee_type").name }}',
            value=[{
                "employeeTypeGroup": {
                    "uri": "{{ dag_run.conf.employee_type_uri }}",
                    "parent": null,
                    "name": null
                },
                "effectiveDate": null
            }]
        )

        # ========== BUILD TIMESHEET PERIOD ==========
        build_timesheet_period = rail.SetVariableOperator(
            task_id='build_timesheet_period',
            append=False,
            name='timesheet_period_schedule',
            value=None
        )

        has_timesheet_period = rail.IfOperator(
            task_id='has_timesheet_period',
            test='{{ dag_run.conf.timesheet_period_uri | is_truthy }}',
            yes_task="set_timesheet_period",
            no_task="create_user",
        )

        set_timesheet_period = rail.SetVariableOperator(
            task_id='set_timesheet_period',
            append=False,
            name='{{ result("build_timesheet_period").name }}',
            value=[{
                "timesheetPeriod": {
                    "uri": "{{ dag_run.conf.timesheet_period_uri }}",
                    "name": null
                },
                "effectiveDate": null
            }]
        )

        # ========== CREATE USER ==========
        create_user = rail.RepliconServiceOperator(
            task_id='create_user',
            endpoint="/services/importservice1.svc/PutUser3",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": dag_run.conf['email'],
                        "parameterCorrelationId": null
                    },
                    "firstname": dag_run.conf['first_name'],
                    "lastname": dag_run.conf['last_name'],
                    "displayNameParameter": {
                        "displayName": dag_run.conf.get('display_name') or null
                    },
                    "emailAddress": dag_run.conf['email'],
                    "employeeId": dag_run.conf['employee_id'],
                    "department": null,
                    "supervisorAssignmentSchedule": null,
                    "schedulePolicySchedule": [{
                        "schedulePolicy": {
                            "officeScheduleUri": null,
                            "name": config.default_office_schedule,
                            "officeSchedule": {
                                "officeScheduleUri": null,
                                "name": config.default_office_schedule
                            },
                            "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                        },
                        "effectiveDate": null
                    }],
                    "workWeekStartDayUri": config.default_work_week,
                    "employmentDateRange": {
                        "startDate": rail.result('parse_start_date'),
                        "endDate": rail.result('parse_end_date'),
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "securityConfiguration": {
                        "enabledAuthenticationTypeUris": ["urn:replicon:user-authentication-type:sso"],
                        "isLoginEnabled": "true" if not rail.result('parse_end_date') else "false",
                        "loginName": dag_run.conf['email'],
                        "SSOName": dag_run.conf['email'],
                        "password": null
                    },
                    "holidayCalendar": rail.get_dag_run_var(rail.result('build_holiday_calendar')['name']),
                    "timeOffPolicy": null,
                    "permissionSets": rail.get_dag_run_var(rail.result('declare_permission_sets')['name']),
                    "policySets": rail.get_dag_run_var(rail.result('build_policy_sets')['name']),
                    "employeeType": null,
                    "costRateSchedule": null,
                    "payrollRateSchedule": null,
                    "timesheetPeriodTypeUri": null,
                    "defaultBillingRate": null,
                    "timesheetApprovalPath": {
                        "uri": dag_run.conf.get('timesheet_approval_path_uri'),
                        "name": null
                    } if dag_run.conf.get('timesheet_approval_path_uri') else null,
                    "expenseApprovalPath": null,
                    "timeOffApprovalPath": {
                        "uri": dag_run.conf.get('timeoff_approval_path_uri'),
                        "name": null
                    } if dag_run.conf.get('timeoff_approval_path_uri') else null,
                    "customFieldValues": null,
                    "assignedActivities": null,
                    "timeZone": rail.get_dag_run_var(rail.result('build_timezone')['name']),
                    "overtimeRuleAssignmentSchedule": null,
                    "validationRuleAssignmentSchedule": null,
                    "locationSchedule": rail.get_dag_run_var(rail.result('build_location_schedule')['name']),
                    "divisionSchedule": null,
                    "costCenterSchedule": null,
                    "serviceCenterSchedule": null,
                    "departmentGroupSchedule": rail.get_dag_run_var(rail.result('build_department_schedule')['name']),
                    "employeeTypeGroupSchedule": rail.get_dag_run_var(rail.result('build_employee_type')['name']),
                    "timesheetPeriodSchedule": rail.get_dag_run_var(rail.result('build_timesheet_period')['name']),
                    "policyDataAccessScopes": [],
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": null
                }
            }
        )

        # ========== ASSIGN PRIMARY ROLE ==========
        has_role_uri = rail.IfOperator(
            task_id='has_role_uri',
            test='{{ dag_run.conf.role_uri | is_truthy }}',
            yes_task="assign_primary_role",
            no_task="check_supervisor_assignment",
        )

        assign_primary_role = rail.RepliconServiceOperator(
            task_id='assign_primary_role',
            endpoint="/services/ResourceService1.svc/PutProjectRoleAssignmentScheduleForUser",
            data=lambda dag_run: {
                "userUri": rail.result('create_user')['uri'],
                "scheduleEntries": [{
                    "effectiveDate": null,
                    "projectRoles": [{
                        "isPrimary": True,
                        "projectRole": {
                            "uri": dag_run.conf['role_uri']
                        }
                    }]
                }]
            }
        )

        # ========== SUPERVISOR ASSIGNMENT ==========
        check_supervisor_assignment = rail.IfOperator(
            task_id='check_supervisor_assignment',
            test='{{ dag_run.conf.manager_email | is_truthy }}',
            yes_task="search_supervisor_by_email",
            no_task="log_no_supervisor",
        )

        log_no_supervisor = rail.SetVariableOperator(
            task_id='log_no_supervisor',
            append=True,
            name='{{ result("declare_exceptions_list").name }}',
            value={"log": "Supervisor not assigned - Manager email not provided in input file"}
        )

        search_supervisor_by_email = rail.RepliconServicePageOperator(
            task_id="search_supervisor_by_email",
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda dag_run: {
                'page': 1,
                'pagesize': 100,
                'columnUris': [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': dag_run.conf['manager_email']
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda result, dag_run: all_result_data_handler(
                result, dag_run.conf['manager_email'])
        )

        supervisor_found = rail.IfOperator(
            task_id='supervisor_found',
            test='{{ result("search_supervisor_by_email") | is_truthy }}',
            yes_task="check_supervisor_not_self",
            no_task="log_supervisor_for_later",
        )

        log_supervisor_for_later = rail.WriteLogOperator(
            task_id='log_supervisor_for_later',
            log="{{ dag_run.conf.supervisor_processing_log }}",
            message="Supervisor pending",
            severity="Info",
            properties={
                "employee_id": "{{ dag_run.conf.employee_id }}",
                "username": "{{ dag_run.conf.first_name }} {{ dag_run.conf.last_name }}",
                "manager_email": "{{ dag_run.conf.manager_email }}",
                "useruri": "{{ result('create_user').uri }}",
                "action": "Add",
                "effective_date": "{{ dag_run.conf.reports_to_effective_date or dag_run.conf.start_date }}"
            }
        )

        check_supervisor_not_self = rail.IfOperator(
            task_id='check_supervisor_not_self',
            test='{{ result("search_supervisor_by_email").loginname | lower != dag_run.conf.email | lower }}',
            yes_task="get_supervisor_details",
            no_task="log_supervisor_is_self",
        )

        log_supervisor_is_self = rail.SetVariableOperator(
            task_id='log_supervisor_is_self',
            append=True,
            name='{{ result("declare_exceptions_list").name }}',
            value={"log": "Supervisor not assigned - User and Supervisor are the same person"}
        )

        get_supervisor_details = rail.RepliconServiceOperator(
            task_id='get_supervisor_details',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [{
                    "uri": "{{ result('search_supervisor_by_email').useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                }],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: response[0] if response else None
        )

        supervisor_is_enabled = rail.IfOperator(
            task_id='supervisor_is_enabled',
            test='{{ result("get_supervisor_details").userDetails.isEnabled == true }}',
            yes_task="check_supervisor_has_permission",
            no_task="log_supervisor_disabled",
        )

        log_supervisor_disabled = rail.SetVariableOperator(
            task_id='log_supervisor_disabled',
            append=True,
            name='{{ result("declare_exceptions_list").name }}',
            value={"log": "Supervisor not assigned - Supervisor {{ dag_run.conf.manager_email }} is disabled"}
        )

        def check_supervisor_permission():
            supervisor_data = rail.result('get_supervisor_details')
            if supervisor_data and supervisor_data.get('permissionSets'):
                for perm in supervisor_data['permissionSets']:
                    if perm.get('name') == 'Supervisor':
                        return perm.get('uri')
            return None

        check_supervisor_has_permission = rail.PythonOperator(
            task_id='check_supervisor_has_permission',
            python_callable=check_supervisor_permission
        )

        needs_supervisor_permission = rail.IfOperator(
            task_id='needs_supervisor_permission',
            test='{{ result("check_supervisor_has_permission") | is_falsy }}',
            yes_task="assign_supervisor_permission_to_manager",
            no_task="assign_initial_supervisor",
        )

        assign_supervisor_permission_to_manager = rail.RepliconServiceOperator(
            task_id='assign_supervisor_permission_to_manager',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('get_supervisor_details').userDetails.uri }}",
                "permissionSetUri": "{{ dag_run.conf.supervisor_permission_uri }}"
            }
        )

        assign_initial_supervisor = rail.RepliconServiceOperator(
            task_id='assign_initial_supervisor',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "supervisorUri": "{{ result('get_supervisor_details').userDetails.uri }}",
                "dateRange": null
            }
        )

        # ========== FINALIZE AND LOG ==========
        def combine_exceptions(list_name):
            exceptions = rail.get_dag_run_var(rail.result(list_name)['name'])
            logs = [e.get('log', '') for e in exceptions if e.get('log')]
            return ' | '.join(logs) if logs else None

        combine_exception_logs = rail.PythonOperator(
            task_id='combine_exception_logs',
            python_callable=lambda: combine_exceptions('declare_exceptions_list')
        )

        log_user_created = rail.WriteLogOperator(
            task_id='log_user_created',
            message=lambda: rail.result('combine_exception_logs') if rail.result('combine_exception_logs') else "Success",
            severity=lambda: "Exception" if rail.result('combine_exception_logs') else "Success",
            properties=lambda dag_run: {
                "Empid": dag_run.conf['employee_id'],
                "Username": dag_run.conf['first_name'] + " " + dag_run.conf['last_name'],
                "Action": "Add",
                "Status": "Exception" if rail.result('combine_exception_logs') else "Success",
                "Details": "User created with exceptions - " + rail.result('combine_exception_logs') if rail.result('combine_exception_logs') else "User created successfully",
                "Jobid": get_dagrun_ecid(dag_run)
            }
        )

        log_error = rail.WriteLogOperator(
            task_id='log_error',
            message="{{ get_error_message() }}",
            severity="Error",
            trigger_rule='one_failed',
            properties={
                "Empid": "{{ dag_run.conf.employee_id }}",
                "Username": "{{ dag_run.conf.first_name }} {{ dag_run.conf.last_name }}",
                "Action": "Add",
                "Status": "Error",
                "Details": "{{ get_error_message() }}",
                "Jobid": "{{ dag_run_ecid() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        # ========== TASK DEPENDENCIES ==========
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> declare_exceptions_list >> search_user_by_email >> user_already_exists

        user_already_exists >> rail.Label('Yes') >> log_user_exists_exception >> log_to_sumo
        user_already_exists >> rail.Label('No') >> parse_start_date >> parse_end_date >> declare_permission_sets >> \
            add_project_resource_permission >> is_manager

        is_manager >> rail.Label('Yes') >> add_supervisor_permission >> build_policy_sets
        is_manager >> rail.Label('No') >> build_policy_sets >> has_timesheet_template

        has_timesheet_template >> rail.Label('Yes') >> add_timesheet_template >> has_timeoff_template
        has_timesheet_template >> rail.Label('No') >> has_timeoff_template

        has_timeoff_template >> rail.Label('Yes') >> add_timeoff_template >> build_department_schedule
        has_timeoff_template >> rail.Label('No') >> build_department_schedule >> has_department

        has_department >> rail.Label('Yes') >> set_department_schedule >> build_location_schedule
        has_department >> rail.Label('No') >> build_location_schedule >> has_location

        has_location >> rail.Label('Yes') >> set_location_schedule >> build_holiday_calendar
        has_location >> rail.Label('No') >> build_holiday_calendar >> has_holiday_calendar

        has_holiday_calendar >> rail.Label('Yes') >> set_holiday_calendar >> build_timezone
        has_holiday_calendar >> rail.Label('No') >> build_timezone >> has_timezone

        has_timezone >> rail.Label('Yes') >> set_timezone >> build_employee_type
        has_timezone >> rail.Label('No') >> build_employee_type >> has_employee_type

        has_employee_type >> rail.Label('Yes') >> set_employee_type >> build_timesheet_period
        has_employee_type >> rail.Label('No') >> build_timesheet_period >> has_timesheet_period

        has_timesheet_period >> rail.Label('Yes') >> set_timesheet_period >> create_user
        has_timesheet_period >> rail.Label('No') >> create_user >> has_role_uri

        has_role_uri >> rail.Label('Yes') >> assign_primary_role >> check_supervisor_assignment
        has_role_uri >> rail.Label('No') >> check_supervisor_assignment

        check_supervisor_assignment >> rail.Label('Yes') >> search_supervisor_by_email >> supervisor_found
        check_supervisor_assignment >> rail.Label('No') >> log_no_supervisor >> combine_exception_logs

        supervisor_found >> rail.Label('Yes') >> check_supervisor_not_self
        supervisor_found >> rail.Label('No') >> log_supervisor_for_later >> combine_exception_logs

        check_supervisor_not_self >> rail.Label('Yes') >> get_supervisor_details >> supervisor_is_enabled
        check_supervisor_not_self >> rail.Label('No') >> log_supervisor_is_self >> combine_exception_logs

        supervisor_is_enabled >> rail.Label('Yes') >> check_supervisor_has_permission >> needs_supervisor_permission
        supervisor_is_enabled >> rail.Label('No') >> log_supervisor_disabled >> combine_exception_logs

        needs_supervisor_permission >> rail.Label('Yes') >> assign_supervisor_permission_to_manager >> assign_initial_supervisor
        needs_supervisor_permission >> rail.Label('No') >> assign_initial_supervisor >> combine_exception_logs

        combine_exception_logs >> log_user_created >> log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)