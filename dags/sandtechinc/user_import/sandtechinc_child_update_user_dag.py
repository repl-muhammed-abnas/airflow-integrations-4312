"""
Sand Tech Inc - Child DAG for Updating Existing Users
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
        dag_id=config.update_user_child_dagid,
        description=f'Sand Tech Inc - Child Update User {config.instance}',
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

        # ========== GET CURRENT USER DATA ==========
        get_user_data = rail.RepliconServiceOperator(
            task_id='get_user_data',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [{
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                }],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: response[0] if response else None
        )

        get_user_group_membership = rail.RepliconServiceOperator(
            task_id='get_user_group_membership',
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "dateRange": null
            }
        )

        # ========== HELPER FUNCTION FOR RESPONSE FILTERING ==========
        def extract_roles_from_response(response):
            """Extract list from response with {"d": [...]} format"""
            data = response.json()
            if isinstance(data, dict) and 'd' in data:
                return data['d']
            if isinstance(data, list):
                return data
            return []

        # ========== PARSE DATES ==========
        def parse_date(date_str, date_format):
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

        parse_effective_date = rail.PythonOperator(
            task_id='parse_effective_date',
            python_callable=lambda dag_run: parse_date(
                dag_run.conf.get('start_date'), dag_run.conf.get('date_format', '%d/%m/%Y'))
        )

        parse_end_date = rail.PythonOperator(
            task_id='parse_end_date',
            python_callable=lambda dag_run: parse_date(
                dag_run.conf.get('last_day_of_work'), dag_run.conf.get('date_format', '%d/%m/%Y'))
        )

        # ========== CHECK LOGIN STATUS ==========
        # If Last day of work has value -> Disable user and set End Date
        # If Last day of work is empty -> Enable user (if disabled) AND clear end date (if set)
        check_termination_status = rail.IfOperator(
            task_id='check_termination_status',
            test='{{ dag_run.conf.last_day_of_work | is_truthy }}',
            yes_task="update_end_date_and_disable",
            no_task="check_if_user_disabled",
        )

        update_end_date_and_disable = rail.RepliconServiceOperator(
            task_id='update_end_date_and_disable',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": rail.result('get_user_data')['userDetails']['employmentDateRange']['startDate'],
                    "endDate": rail.result('parse_end_date'),
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        disable_user_login = rail.RepliconServiceOperator(
            task_id='disable_user_login',
            endpoint="/services/securityService1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        check_if_user_disabled = rail.IfOperator(
            task_id='check_if_user_disabled',
            test='{{ result("get_user_data").userDetails.isEnabled == false }}',
            yes_task="enable_user_login",
            no_task="check_has_end_date_to_clear",
        )

        enable_user_login = rail.RepliconServiceOperator(
            task_id='enable_user_login',
            endpoint="/services/securityService1.svc/EnableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        clear_end_date = rail.RepliconServiceOperator(
            task_id='clear_end_date',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": rail.result('get_user_data')['userDetails']['employmentDateRange']['startDate'],
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        # Check if user has an existing end date that needs to be cleared (even if still enabled)
        check_has_end_date_to_clear = rail.IfOperator(
            task_id='check_has_end_date_to_clear',
            test='{{ result("get_user_data").userDetails.employmentDateRange.endDate | is_truthy }}',
            yes_task="clear_end_date_only",
            no_task="update_first_name_check",
        )

        clear_end_date_only = rail.RepliconServiceOperator(
            task_id='clear_end_date_only',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": rail.result('get_user_data')['userDetails']['employmentDateRange']['startDate'],
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        # ========== UPDATE PROFILE FIELDS ==========
        update_first_name_check = rail.IfOperator(
            task_id='update_first_name_check',
            test='{{ dag_run.conf.first_name | is_truthy and result("get_user_data").userDetails.firstName | lower != dag_run.conf.first_name | lower }}',
            yes_task="update_first_name",
            no_task="update_last_name_check",
        )

        update_first_name = rail.RepliconServiceOperator(
            task_id='update_first_name',
            endpoint="/services/userService1.svc/UpdateFirstName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "firstname": "{{ dag_run.conf.first_name }}"
            }
        )

        update_last_name_check = rail.IfOperator(
            task_id='update_last_name_check',
            test='{{ dag_run.conf.last_name | is_truthy and result("get_user_data").userDetails.lastName | lower != dag_run.conf.last_name | lower }}',
            yes_task="update_last_name",
            no_task="update_email_check",
        )

        update_last_name = rail.RepliconServiceOperator(
            task_id='update_last_name',
            endpoint="/services/userService1.svc/UpdateLastName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "lastname": "{{ dag_run.conf.last_name }}"
            }
        )

        update_email_check = rail.IfOperator(
            task_id='update_email_check',
            test='{{ dag_run.conf.email | is_truthy and result("get_user_data").userDetails.emailAddress | lower != dag_run.conf.email | lower }}',
            yes_task="update_email",
            no_task="update_department_check",
        )

        update_email = rail.RepliconServiceOperator(
            task_id='update_email',
            endpoint="/services/userService1.svc/UpdateEmail",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "email": "{{ dag_run.conf.email }}"
            }
        )

        update_login_name = rail.RepliconServiceOperator(
            task_id='update_login_name',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "securitySettingsToApply": {
                        "loginEnabled": "1",
                        "loginName": "{{ dag_run.conf.email }}",
                        "ssoName": "{{ dag_run.conf.email }}",
                        "password": null,
                        "enabledAuthenticationTypeUris": ["urn:replicon:user-authentication-type:sso"],
                        "emailMFAResendVerificationEmail": "false",
                        "emailMFATryAddMethodFromUsersEmail": "false",
                        "clearIsLockedOut": "false"
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        # ========== UPDATE DEPARTMENT ==========
        update_department_check = rail.IfOperator(
            task_id='update_department_check',
            test='{{ dag_run.conf.department_uri | is_truthy }}',
            yes_task="check_department_changed",
            no_task="update_location_check",
        )

        def check_department_needs_update():
            current_depts = rail.result('get_user_group_membership').get('departments', [])
            new_dept_uri = rail.get_current_context()['dag_run'].conf.get('department_uri')
            if not current_depts:
                return True
            current_dept_uri = current_depts[0].get('department', {}).get('department', {}).get('uri')
            return current_dept_uri != new_dept_uri

        check_department_changed = rail.PythonOperator(
            task_id='check_department_changed',
            python_callable=check_department_needs_update
        )

        should_update_department = rail.IfOperator(
            task_id='should_update_department',
            test='{{ result("check_department_changed") == true }}',
            yes_task="update_department",
            no_task="update_location_check",
        )

        update_department = rail.RepliconServiceOperator(
            task_id='update_department',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri'],
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "departmentGroupScheduleToApply": {
                        "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementDepartmentGroupSchedule": [],
                        "updateDepartmentGroupScheduleOverDateRange": {
                            "replacementDepartmentGroupScheduleEntries": [{
                                "departmentGroup": {
                                    "uri": dag_run.conf['department_uri'],
                                    "parent": null,
                                    "name": null,
                                    "parameterCorrelationId": null
                                },
                                "effectiveDate": rail.result('parse_effective_date')
                            }],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        # ========== UPDATE LOCATION ==========
        update_location_check = rail.IfOperator(
            task_id='update_location_check',
            test='{{ dag_run.conf.location_uri | is_truthy }}',
            yes_task="check_location_changed",
            no_task="update_role_check",
        )

        def check_location_needs_update():
            current_locs = rail.result('get_user_group_membership').get('locations', [])
            new_loc_uri = rail.get_current_context()['dag_run'].conf.get('location_uri')
            if not current_locs:
                return True
            current_loc_uri = current_locs[0].get('location', {}).get('location', {}).get('uri')
            return current_loc_uri != new_loc_uri

        check_location_changed = rail.PythonOperator(
            task_id='check_location_changed',
            python_callable=check_location_needs_update
        )

        should_update_location = rail.IfOperator(
            task_id='should_update_location',
            test='{{ result("check_location_changed") == true }}',
            yes_task="update_location",
            no_task="update_role_check",
        )

        update_location = rail.RepliconServiceOperator(
            task_id='update_location',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri'],
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "locationScheduleToApply": {
                        "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementLocationSchedule": [],
                        "updateLocationScheduleOverDateRange": {
                            "replacementLocationScheduleEntries": [{
                                "location": {
                                    "uri": dag_run.conf['location_uri'],
                                    "parentUri": null,
                                    "name": null
                                },
                                "effectiveDate": rail.result('parse_effective_date')
                            }],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        # ========== UPDATE ROLE ==========
        update_role_check = rail.IfOperator(
            task_id='update_role_check',
            test='{{ dag_run.conf.role_uri | is_truthy }}',
            yes_task="get_current_role_schedule",
            no_task="update_holiday_calendar_check",
        )

        get_current_role_schedule = rail.RepliconServiceOperator(
            task_id='get_current_role_schedule',
            endpoint="/services/ResourceService1.svc/GetProjectRoleAssignmentScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri']
            },
            response_filter=extract_roles_from_response
        )

        def build_role_schedule_entries():
            """Build schedule entries by appending new role to existing history"""
            dag_run = rail.get_current_context()['dag_run']
            current_schedule = rail.result('get_current_role_schedule') or []
            new_role_uri = dag_run.conf.get('role_uri')
            role_effective_date = dag_run.conf.get('job_title_effective_date')
            
            # Parse effective date
            parsed_date = None
            if role_effective_date:
                try:
                    from datetime import datetime
                    parsed = datetime.strptime(role_effective_date.strip(), config.date_format)
                    parsed_date = {"year": parsed.year, "month": parsed.month, "day": parsed.day}
                except:
                    parsed_date = None
            
            # Convert existing schedule to new format
            schedule_entries = []
            for entry in current_schedule:
                schedule_entries.append({
                    "effectiveDate": entry.get('effectiveDate'),
                    "projectRoles": [{
                        "isPrimary": pr.get('isPrimary', False),
                        "projectRole": {"uri": pr.get('projectRole', {}).get('uri')}
                    } for pr in entry.get('projectRoles', [])]
                })
            
            # Check if role with same effective date already exists
            date_exists = False
            for entry in schedule_entries:
                if entry.get('effectiveDate') == parsed_date:
                    # Update existing entry
                    entry['projectRoles'] = [{"isPrimary": True, "projectRole": {"uri": new_role_uri}}]
                    date_exists = True
                    break
            
            # If no matching date, append new entry
            if not date_exists:
                schedule_entries.append({
                    "effectiveDate": parsed_date,
                    "projectRoles": [{"isPrimary": True, "projectRole": {"uri": new_role_uri}}]
                })
            
            return schedule_entries

        prepare_role_schedule = rail.PythonOperator(
            task_id='prepare_role_schedule',
            python_callable=build_role_schedule_entries
        )

        update_primary_role = rail.RepliconServiceOperator(
            task_id='update_primary_role',
            endpoint="/services/ResourceService1.svc/PutProjectRoleAssignmentScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": rail.result('prepare_role_schedule')
            }
        )

        # ========== UPDATE HOLIDAY CALENDAR ==========
        update_holiday_calendar_check = rail.IfOperator(
            task_id='update_holiday_calendar_check',
            test='{{ dag_run.conf.holiday_calendar_uri | is_truthy }}',
            yes_task="check_holiday_calendar_changed",
            no_task="update_supervisor_check",
        )

        def check_holiday_calendar_needs_update():
            current_cal = rail.result('get_user_data').get('holidayCalendar')
            new_cal_uri = rail.get_current_context()['dag_run'].conf.get('holiday_calendar_uri')
            if not current_cal:
                return True
            return current_cal.get('uri') != new_cal_uri

        check_holiday_calendar_changed = rail.PythonOperator(
            task_id='check_holiday_calendar_changed',
            python_callable=check_holiday_calendar_needs_update
        )

        should_update_holiday_calendar = rail.IfOperator(
            task_id='should_update_holiday_calendar',
            test='{{ result("check_holiday_calendar_changed") == true }}',
            yes_task="update_holiday_calendar",
            no_task="update_supervisor_check",
        )

        update_holiday_calendar = rail.RepliconServiceOperator(
            task_id='update_holiday_calendar',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "holidayCalendarUri": "{{ dag_run.conf.holiday_calendar_uri }}"
            }
        )

        # ========== UPDATE SUPERVISOR ==========
        update_supervisor_check = rail.IfOperator(
            task_id='update_supervisor_check',
            test='{{ dag_run.conf.manager_email | is_truthy }}',
            yes_task="search_supervisor",
            no_task="check_manager_permission",
        )

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

        search_supervisor = rail.RepliconServicePageOperator(
            task_id="search_supervisor",
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
            test='{{ result("search_supervisor") | is_truthy }}',
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
                "useruri": "{{ dag_run.conf.useruri }}",
                "action": "Update",
                "effective_date": "{{ dag_run.conf.reports_to_effective_date or dag_run.conf.start_date }}"
            }
        )

        check_supervisor_not_self = rail.IfOperator(
            task_id='check_supervisor_not_self',
            test='{{ result("search_supervisor").loginname | lower != dag_run.conf.email | lower }}',
            yes_task="get_supervisor_details",
            no_task="log_supervisor_is_self",
        )

        log_supervisor_is_self = rail.SetVariableOperator(
            task_id='log_supervisor_is_self',
            append=True,
            name='{{ result("declare_exceptions_list").name }}',
            value={"log": "Supervisor not updated - User and Supervisor are the same person"}
        )

        get_supervisor_details = rail.RepliconServiceOperator(
            task_id='get_supervisor_details',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [{
                    "uri": "{{ result('search_supervisor').useruri }}",
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
            yes_task="get_current_supervisor",
            no_task="log_supervisor_disabled",
        )

        log_supervisor_disabled = rail.SetVariableOperator(
            task_id='log_supervisor_disabled',
            append=True,
            name='{{ result("declare_exceptions_list").name }}',
            value={"log": "Supervisor not updated - Supervisor {{ dag_run.conf.manager_email }} is disabled"}
        )

        get_current_supervisor = rail.RepliconServiceOperator(
            task_id='get_current_supervisor',
            endpoint="/services/UserService1.svc/GetSupervisorAssignmentDetails",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "asOfDate": {
                    "year": pendulum.now(config.est_timezone).year,
                    "month": pendulum.now(config.est_timezone).month,
                    "day": pendulum.now(config.est_timezone).day
                }
            }
        )

        def check_supervisor_changed():
            current_sup = rail.result('get_current_supervisor')
            new_sup = rail.result('get_supervisor_details')
            if not current_sup or not current_sup.get('supervisor'):
                return True
            current_login = current_sup.get('supervisor', {}).get('user', {}).get('loginName', '').lower()
            new_login = new_sup.get('securityConfiguration', {}).get('loginName', '').lower()
            return current_login != new_login

        check_supervisor_needs_update = rail.PythonOperator(
            task_id='check_supervisor_needs_update',
            python_callable=check_supervisor_changed
        )

        should_update_supervisor = rail.IfOperator(
            task_id='should_update_supervisor',
            test='{{ result("check_supervisor_needs_update") == true }}',
            yes_task="check_supervisor_has_permission",
            no_task="check_manager_permission",
        )

        def get_supervisor_permission():
            supervisor_data = rail.result('get_supervisor_details')
            if supervisor_data and supervisor_data.get('permissionSets'):
                for perm in supervisor_data['permissionSets']:
                    if perm.get('name') == 'Supervisor':
                        return perm.get('uri')
            return None

        check_supervisor_has_permission = rail.PythonOperator(
            task_id='check_supervisor_has_permission',
            python_callable=get_supervisor_permission
        )

        needs_supervisor_permission = rail.IfOperator(
            task_id='needs_supervisor_permission',
            test='{{ result("check_supervisor_has_permission") | is_falsy }}',
            yes_task="assign_supervisor_permission_to_manager",
            no_task="update_supervisor_assignment",
        )

        assign_supervisor_permission_to_manager = rail.RepliconServiceOperator(
            task_id='assign_supervisor_permission_to_manager',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('get_supervisor_details').userDetails.uri }}",
                "permissionSetUri": "{{ dag_run.conf.supervisor_permission_uri }}"
            }
        )

        update_supervisor_assignment = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": rail.result('get_supervisor_details')['userDetails']['uri'],
                "dateRange": {
                    "startDate": rail.result('parse_effective_date'),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        # ========== CHECK MANAGER PERMISSION (Is a manager field) ==========
        # Note: Per requirements, if Is a manager changes from Yes to No, we do NOT remove permission
        check_manager_permission = rail.IfOperator(
            task_id='check_manager_permission',
            test='{{ dag_run.conf.is_a_manager | lower == "yes" }}',
            yes_task="check_has_supervisor_permission",
            no_task="finalize_update",
        )

        def user_has_supervisor_permission():
            user_data = rail.result('get_user_data')
            if user_data and user_data.get('permissionSets'):
                for perm in user_data['permissionSets']:
                    if perm.get('name') == 'Supervisor':
                        return True
            return False

        check_has_supervisor_permission = rail.PythonOperator(
            task_id='check_has_supervisor_permission',
            python_callable=user_has_supervisor_permission
        )

        needs_supervisor_permission_user = rail.IfOperator(
            task_id='needs_supervisor_permission_user',
            test='{{ result("check_has_supervisor_permission") == false }}',
            yes_task="assign_supervisor_permission_to_user",
            no_task="finalize_update",
        )

        assign_supervisor_permission_to_user = rail.RepliconServiceOperator(
            task_id='assign_supervisor_permission_to_user',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "permissionSetUri": "{{ dag_run.conf.supervisor_permission_uri }}"
            }
        )

        # ========== FINALIZE AND LOG ==========
        finalize_update = rail.EmptyOperator(
            task_id='finalize_update',
        )

        def combine_exceptions(list_name):
            exceptions = rail.get_dag_run_var(rail.result(list_name)['name'])
            logs = [e.get('log', '') for e in exceptions if e.get('log')]
            return ' | '.join(logs) if logs else None

        combine_exception_logs = rail.PythonOperator(
            task_id='combine_exception_logs',
            python_callable=lambda: combine_exceptions('declare_exceptions_list')
        )

        log_user_updated = rail.WriteLogOperator(
            task_id='log_user_updated',
            message=lambda: rail.result('combine_exception_logs') if rail.result('combine_exception_logs') else "Success",
            severity=lambda: "Exception" if rail.result('combine_exception_logs') else "Success",
            properties=lambda dag_run: {
                "Empid": dag_run.conf['employee_id'],
                "Username": dag_run.conf['first_name'] + " " + dag_run.conf['last_name'],
                "Action": "Update",
                "Status": "Exception" if rail.result('combine_exception_logs') else "Success",
                "Details": "User updated with exceptions - " + rail.result('combine_exception_logs') if rail.result('combine_exception_logs') else "User updated successfully",
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
                "Action": "Update",
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
        can_run_batch_task >> rail.Label('No') >> declare_exceptions_list >> get_user_data >> \
            get_user_group_membership >> parse_effective_date >> parse_end_date >> check_termination_status

        check_termination_status >> rail.Label('Yes') >> update_end_date_and_disable >> disable_user_login >> \
            update_first_name_check
        check_termination_status >> rail.Label('No') >> check_if_user_disabled

        check_if_user_disabled >> rail.Label('Yes') >> enable_user_login >> clear_end_date >> update_first_name_check
        check_if_user_disabled >> rail.Label('No') >> check_has_end_date_to_clear

        check_has_end_date_to_clear >> rail.Label('Yes') >> clear_end_date_only >> update_first_name_check
        check_has_end_date_to_clear >> rail.Label('No') >> update_first_name_check

        update_first_name_check >> rail.Label('Yes') >> update_first_name >> update_last_name_check
        update_first_name_check >> rail.Label('No') >> update_last_name_check

        update_last_name_check >> rail.Label('Yes') >> update_last_name >> update_email_check
        update_last_name_check >> rail.Label('No') >> update_email_check

        update_email_check >> rail.Label('Yes') >> update_email >> update_login_name >> update_department_check
        update_email_check >> rail.Label('No') >> update_department_check

        update_department_check >> rail.Label('Yes') >> check_department_changed >> should_update_department
        update_department_check >> rail.Label('No') >> update_location_check

        should_update_department >> rail.Label('Yes') >> update_department >> update_location_check
        should_update_department >> rail.Label('No') >> update_location_check

        update_location_check >> rail.Label('Yes') >> check_location_changed >> should_update_location
        update_location_check >> rail.Label('No') >> update_role_check

        should_update_location >> rail.Label('Yes') >> update_location >> update_role_check
        should_update_location >> rail.Label('No') >> update_role_check

        update_role_check >> rail.Label('Yes') >> get_current_role_schedule >> prepare_role_schedule >> update_primary_role >> update_holiday_calendar_check
        update_role_check >> rail.Label('No') >> update_holiday_calendar_check

        update_holiday_calendar_check >> rail.Label('Yes') >> check_holiday_calendar_changed >> should_update_holiday_calendar
        update_holiday_calendar_check >> rail.Label('No') >> update_supervisor_check

        should_update_holiday_calendar >> rail.Label('Yes') >> update_holiday_calendar >> update_supervisor_check
        should_update_holiday_calendar >> rail.Label('No') >> update_supervisor_check

        update_supervisor_check >> rail.Label('Yes') >> search_supervisor >> supervisor_found
        update_supervisor_check >> rail.Label('No') >> check_manager_permission

        supervisor_found >> rail.Label('Yes') >> check_supervisor_not_self
        supervisor_found >> rail.Label('No') >> log_supervisor_for_later >> check_manager_permission

        check_supervisor_not_self >> rail.Label('Yes') >> get_supervisor_details >> supervisor_is_enabled
        check_supervisor_not_self >> rail.Label('No') >> log_supervisor_is_self >> check_manager_permission

        supervisor_is_enabled >> rail.Label('Yes') >> get_current_supervisor >> check_supervisor_needs_update >> should_update_supervisor
        supervisor_is_enabled >> rail.Label('No') >> log_supervisor_disabled >> check_manager_permission

        should_update_supervisor >> rail.Label('Yes') >> check_supervisor_has_permission >> needs_supervisor_permission
        should_update_supervisor >> rail.Label('No') >> check_manager_permission

        needs_supervisor_permission >> rail.Label('Yes') >> assign_supervisor_permission_to_manager >> update_supervisor_assignment
        needs_supervisor_permission >> rail.Label('No') >> update_supervisor_assignment >> check_manager_permission

        check_manager_permission >> rail.Label('Yes') >> check_has_supervisor_permission >> needs_supervisor_permission_user
        check_manager_permission >> rail.Label('No') >> finalize_update

        needs_supervisor_permission_user >> rail.Label('Yes') >> assign_supervisor_permission_to_user >> finalize_update
        needs_supervisor_permission_user >> rail.Label('No') >> finalize_update

        finalize_update >> combine_exception_logs >> log_user_updated >> log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)