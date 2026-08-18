from datetime import timedelta
from airflow.models import Variable
import rail
from rail.lib.ecid import get_dagrun_ecid
from strayeruniversity.user_sync_v4.utils import request_payload, python_callable
from strayeruniversity.user_sync_v4.utils.python_callable import get_today, get_current_data, get_substitueUserUris, get_substitueUser_fromsearch
from strayeruniversity.user_sync_v4.utils.request_payload import get_today_dateformat_payload


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_update_user_dag_id,
        description=f'strayeruniversity_usersync_update_user_child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.update_user_child_dag_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config", extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_user_details_for_update'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_user_details_for_update',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_user_details_for_update = rail.RepliconServiceOperator(
            task_id='get_user_details_for_update',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": "{{ dag_run.conf.useruri }}"
                    }
                ]
            }
        )

        if_user_enabled_is_false = rail.IfOperator(
            task_id='if_user_enabled_is_false',
            test='''{{ result('get_user_details_for_update')[0].userDetails.isEnabled | is_falsy }}''',
            yes_task="log_user_update_ignored",
            no_task="if_request_firstname_mismatch",
        )

        log_user_update_ignored = rail.WriteLogOperator(
            task_id="log_user_update_ignored",
            log='{{ dag_run.conf.logger}}',
            message="Ignored",
            severity="Skipped",
            properties={
                "username": "{{ dag_run.conf.username }}" + "|" + "{{ dag_run.conf.emplid }}",
                "action": "Update user",
                "status": "Ignored - User is disabled in Replicon",
                'details': "{{dag_run_ecid()}}",
            }
        )

        if_request_firstname_mismatch = rail.IfOperator(
            task_id="if_request_firstname_mismatch",
            test="{{ (result('get_user_details_for_update')[0].userDetails.firstName | is_falsy or \
                result('get_user_details_for_update')[0].userDetails.firstName.lower() != dag_run.conf.firstname.lower()) and \
                dag_run.conf.firstname | is_truthy }}",
            yes_task="update_firstname",
            no_task="if_lastname_mismatch"
        )

        update_firstname = rail.RepliconServiceOperator(
            task_id="update_firstname",
            endpoint="/services/UserService1.svc/UpdateFirstName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "firstname": "{{ dag_run.conf.firstname }}"
            }
        )

        if_lastname_mismatch = rail.IfOperator(
            task_id="if_lastname_mismatch",
            test="{{ (result('get_user_details_for_update')[0].userDetails.lastName | is_falsy or \
                result('get_user_details_for_update')[0].userDetails.lastName.lower() != dag_run.conf.lastname.lower()) and \
                dag_run.conf.lastname | is_truthy }}",
            yes_task="update_lastname",
            no_task="if_email_mismatch"
        )

        update_lastname = rail.RepliconServiceOperator(
            task_id="update_lastname",
            endpoint="/services/UserService1.svc/UpdateLastName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "lastname": "{{ dag_run.conf.lastname }}"
            }
        )

        if_email_mismatch = rail.IfOperator(
            task_id="if_email_mismatch",
            test="{{ dag_run.conf.workemail | is_truthy and \
                (result('get_user_details_for_update')[0].userDetails.emailAddress | is_falsy or \
                    result('get_user_details_for_update')[0].userDetails.emailAddress != dag_run.conf.workemail) }}",
            yes_task="update_email_address",
            no_task="if_hiredate_present"
        )

        update_email_address = rail.RepliconServiceOperator(
            task_id="update_email_address",
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "email": "{{ dag_run.conf.workemail }}"
            }
        )

        if_hiredate_present = rail.IfOperator(
            task_id="if_hiredate_present",
            test="{{ dag_run.conf.hiredate | is_truthy }}",
            yes_task="get_start_day_mismatch_val",
            no_task="get_employeetype_val"
        )

        get_start_day_mismatch_val = rail.PythonOperator(
            task_id="get_start_day_mismatch_val",
            python_callable=python_callable.check_start_date_mismatch
        )

        check_if_start_day_mismatch = rail.IfOperator(
            task_id="check_if_start_day_mismatch",
            test="{{ result('get_start_day_mismatch_val') | is_truthy}}",
            yes_task="update_emp_daterange",
            no_task="get_employeetype_val"
        )

        update_emp_daterange = rail.RepliconServiceOperator(
            task_id='update_emp_daterange',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=request_payload.update_emp_daterange_hiredate
        )

        get_employeetype_val = rail.PythonOperator(
            task_id="get_employeetype_val",
            python_callable=python_callable.get_employeetype_value
        )

        get_required_department_uri_from_all_enabled_dept_list = rail.RepliconServiceOperator(
            task_id="get_required_department_uri_from_all_enabled_dept_list",
            endpoint="/services/DepartmentService1.svc/GetEnabledDepartments",
            data_handler=lambda response, dag_run: {
                    'user_department': rail.find_first_by_attr_and_get_attr(
                        response, 'displayText', dag_run.conf['department'], 'uri', ''),
                    'parent_department': rail.find_first_by_attr_and_get_attr(
                        response, 'displayText', "Strayer University", 'uri', ''),
            }
        )

        get_all_employee_type = rail.RepliconServiceOperator(
            task_id="get_all_employee_type",
            endpoint="/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails",
            data_handler=lambda response, dag_run: {
                'employeetype_name': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', dag_run.conf['employeetype'], 'displayText', ''),
                'employeetype_uri': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', dag_run.conf['employeetype'], 'uri', '')
            }
        )

        get_all_permission_set = rail.RepliconServiceOperator(
            task_id="get_all_permission_set",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data_handler=lambda response: {
                'supervisoruri': rail.find_first_by_attr_and_get_attr(
                    response, 'name', 'Supervisor', 'uri', ''),
                'reportuseruri': rail.find_first_by_attr_and_get_attr(
                    response, 'name', 'Report User', 'uri', ''),
                'hourlyuseruri': rail.find_first_by_attr_and_get_attr(
                    response, 'name', 'Hourly User', 'uri', ''),
                'salarieduseruri': rail.find_first_by_attr_and_get_attr(
                    response, 'name', 'Salaried User', 'uri', '')
            }
        )

        get_all_timesheet_approval = rail.RepliconServiceOperator(
            task_id="get_all_timesheet_approval",
            endpoint="/services/TimesheetApprovalService1.svc/GetAllApprovalPaths",
            data_handler=lambda response: {
                'supervisor_custom_uri': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Supervisor - Custom', 'uri', ''),
                'supervisor_uri': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Supervisor', 'uri', '')
            }
        )

        get_all_timeoff_approval = rail.RepliconServiceOperator(
            task_id="get_all_timeoff_approval",
            endpoint="/services/TimeOffApprovalService1.svc/GetAllApprovalPaths",
            data_handler=lambda response: {
                'defaulturi': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'System Auto Approval', 'uri', ''),
                'supervisor_approvaluri': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Supervisor', 'uri', '')
            }
        )

        get_all_policy_sets = rail.RepliconServiceOperator(
            task_id="get_all_policy_sets",
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
            data_handler=lambda response: {
                "punch_uri": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Clock In - Clock Out', 'uri', ''),
                "tm_hourly": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'TimeSheet - Hourly', 'uri', ''),
                "tm_hourly_exmpt": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'TimeSheet - Hourly Exempt', 'uri', ''),
                "widget_tm": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Widget Timesheet with Autofill-Salaried', 'uri', ''),
                "prtm_sal": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Part Time Salaried', 'uri', ''),
                'existing_timesheettemplate': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', rail.result('get_user_details_for_update')[0]['timesheetTemplate']['displayText'], 'uri', '') if rail.result(
                        'get_user_details_for_update')[0]['timesheetTemplate'] else '',
                'timeoff': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Time Off', 'uri', ''),
            }
        )

        get_enabled_activities = rail.RepliconServiceOperator(
            task_id='get_enabled_activities',
            endpoint="/services/ActivityService1.svc/GetEnabledActivities",
            data_handler=lambda response: {
                'workstudy_uri': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Work Study', 'uri', '')
            }
        )

        get_alluser_customfields = rail.RepliconServiceOperator(
            task_id='get_alluser_customfields',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            },
            data_handler=lambda response: {
                'primaryworkstate_uri': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Primary Work State', 'uri', ''),
                'emp_type_uri': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Emp Type', 'uri', ''),
                'management_level_uri': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Management Level', 'uri', ''),
                'position_uri': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Position', 'uri', ''),
                'scheduled_hours_uri': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Scheduled Hours', 'uri', ''),
                'employeestatus_uri': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'EmployeeStatus', 'uri', ''),
                'approver_uri': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Approver', 'uri', '')
            }
        )

        get_all_active_scripts = rail.RepliconServiceOperator(
            task_id='get_all_active_scripts',
            endpoint='/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetActiveScripts',
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Starting Balance Set To   ', 'uri', '')
        )

        if_department_present = rail.IfOperator(
            task_id='if_department_present',
            test='''{{ result('get_required_department_uri_from_all_enabled_dept_list').user_department | is_falsy }}''',
            yes_task="add_department",
            no_task="department_to_assign",
        )

        add_department = rail.RepliconServiceOperator(
            task_id='add_department',
            endpoint="/services/DepartmentService1.svc/PutDepartment",
            data=lambda dag_run: {
                "department": {
                    "target": {
                        "name": dag_run.conf['department'],
                        "parent": {
                            "uri": rail.result('get_required_department_uri_from_all_enabled_dept_list')['parent_department']
                        }
                    },
                    "name": dag_run.conf['department'],
                    "isEnabled": "true"
                }
            }
        )

        department_to_assign = rail.PythonOperator(
            task_id="department_to_assign",
            python_callable=lambda: rail.result('add_department')['uri'] if rail.result('add_department') else rail.result(
                'get_required_department_uri_from_all_enabled_dept_list')['user_department']
        )

        if_employeetype_present = rail.IfOperator(
            task_id='if_employeetype_present',
            test='''{{ result('get_all_employee_type').employeetype_name | is_falsy }}''',
            yes_task="add_employeetype",
            no_task="employeetype_to_assign",
        )

        add_employeetype = rail.RepliconServiceOperator(
            task_id='add_employeetype',
            endpoint="/services/EmployeeTypeService1.svc/PutEmployeeType",
            data=lambda dag_run: {
                "employeeType": {
                    "target": {
                        "name": dag_run.conf['employeetype']
                    },
                    "name": dag_run.conf['employeetype']
                }
            }
        )

        employeetype_to_assign = rail.PythonOperator(
            task_id="employeetype_to_assign",
            python_callable=lambda: rail.result('get_all_employee_type')['employeetype_uri'] if rail.result(
                'get_all_employee_type')['employeetype_uri'] else rail.result('add_employeetype')['uri']
        )

        if_department_mismatch = rail.IfOperator(
            task_id='if_department_mismatch',
            test='''{{ result('get_user_details_for_update')[0].userDetails.department | is_truthy and \
                result('get_user_details_for_update')[0].userDetails.department.displayText | lower != dag_run.conf.department | lower }}''',
            yes_task="update_department_foruser",
            no_task="get_employeetype_foruser",
        )

        update_department_foruser = rail.RepliconServiceOperator(
            task_id='update_department_foruser',
            endpoint='/services/DepartmentService1.svc/UpdateDepartmentForUser',
            data={
                'userUri': "{{ dag_run.conf.useruri }}",
                'departmentUri': "{{ result('department_to_assign') }}"
            }
        )

        get_employeetype_foruser = rail.RepliconServiceOperator(
            task_id='get_employeetype_foruser',
            endpoint='/services/EmployeeTypeService1.svc/GetEmployeeTypeForUser',
            data={
                'userUri': "{{ dag_run.conf.useruri }}"
            }
        )

        if_employeetype_mismatch = rail.IfOperator(
            task_id='if_employeetype_mismatch',
            test='''{{ result('get_employeetype_foruser') | is_truthy and \
                result('get_employeetype_foruser').displayText | lower != dag_run.conf.employeetype | lower }}''',
            yes_task="update_employeetype_foruser",
            no_task="if_employeetype_is_federalworkstudy_pt_hourly",
        )

        update_employeetype_foruser = rail.RepliconServiceOperator(
            task_id='update_employeetype_foruser',
            endpoint='/services/EmployeeTypeService1.svc/UpdateEmployeeTypeForUser',
            data={
                'userUri': "{{ dag_run.conf.useruri }}",
                'employeeTypeUri': "{{ result('employeetype_to_assign') }}"
            }
        )

        if_employeetype_is_federalworkstudy_pt_hourly = rail.IfOperator(
            task_id='if_employeetype_is_federalworkstudy_pt_hourly',
            test='''{{ dag_run.conf.employeetype == 'Federal Work Study Part-time Hourly' and \
                result('get_enabled_activities').workstudy_uri | is_truthy }}''',
            yes_task="update_defaultuser_activity",
            no_task="if_employeetype_is_salaried_or_parttimesalaried",
        )

        update_defaultuser_activity = rail.RepliconServiceOperator(
            task_id='update_defaultuser_activity',
            endpoint='/services/ActivityService1.svc/UpdateDefaultUserActivity',
            data={
                'user': {
                    'uri': "{{ dag_run.conf.useruri }}"
                },
                'activity': {
                    'uri': "{{ result('get_enabled_activities').workstudy_uri }}"
                }
            }
        )

        if_employeetype_is_salaried_or_parttimesalaried = rail.IfOperator(
            task_id='if_employeetype_is_salaried_or_parttimesalaried',
            test='''{{ dag_run.conf.employeetype == 'Salaried' or \
                dag_run.conf.employeetype == 'Part-time Salaried' }}''',
            yes_task="if_timesheetApprovalPath_isnot_supervisorcustom",
            no_task="if_timesheetApprovalPath_isnot_supervisor",
        )

        if_timesheetApprovalPath_isnot_supervisorcustom = rail.IfOperator(
            task_id='if_timesheetApprovalPath_isnot_supervisorcustom',
            test='''{{ result('get_user_details_for_update')[0].timesheetApprovalPath | is_truthy and \
                result('get_user_details_for_update')[0].timesheetApprovalPath.displayText != 'Supervisor - Custom' }}''',
            yes_task="update_approval_pathforuser_timesheetcustom",
            no_task="if_managername_present_and_not_in_username",
        )

        update_approval_pathforuser_timesheetcustom = rail.RepliconServiceOperator(
            task_id='update_approval_pathforuser_timesheetcustom',
            endpoint='/services/TimesheetApprovalService1.svc/UpdateApprovalPathForUser',
            data={
                'userUri': "{{ dag_run.conf.useruri }}",
                'approvalPathUri': "{{ result('get_all_timesheet_approval').supervisor_custom_uri }}"
            }
        )

        if_timesheetApprovalPath_isnot_supervisor = rail.IfOperator(
            task_id='if_timesheetApprovalPath_isnot_supervisor',
            test='''{{ result('get_user_details_for_update')[0].timesheetApprovalPath | is_truthy and \
                result('get_user_details_for_update')[0].timesheetApprovalPath.displayText != 'Supervisor' }}''',
            yes_task="update_approval_pathforuser_timesheet",
            no_task="if_managername_present_and_not_in_username",
        )

        update_approval_pathforuser_timesheet = rail.RepliconServiceOperator(
            task_id='update_approval_pathforuser_timesheet',
            endpoint='/services/TimesheetApprovalService1.svc/UpdateApprovalPathForUser',
            data={
                'userUri': "{{ dag_run.conf.useruri }}",
                'approvalPathUri': "{{ result('get_all_timesheet_approval').supervisor_uri }}"
            }
        )

        if_managername_present_and_not_in_username = rail.IfOperator(
            task_id='if_managername_present_and_not_in_username',
            test=lambda dag_run: bool(dag_run.conf['managername'] and
                                      dag_run.conf['managername'] not in dag_run.conf['username']),
            yes_task="if_supervisor_loginname_mismatch",
            no_task="if_location_present",
        )

        if_supervisor_loginname_mismatch = rail.IfOperator(
            task_id='if_supervisor_loginname_mismatch',
            test='''{{ result('get_user_details_for_update')[0].userDetails.supervisor | is_falsy or \
                result('get_user_details_for_update')[0].userDetails.supervisor.user.loginName != dag_run.conf.managername   }}''',
            yes_task="search_for_supervisor_with_managername",
            no_task="if_location_present",
        )

        search_for_supervisor_with_managername = rail.RepliconServiceOperator(
            task_id='search_for_supervisor_with_managername',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:user-list-column:enabled",
                    "urn:replicon:user-list-column:login-name"
                ]
            },
            data_handler=python_callable.get_userdata_list_for_managername
        )

        if_supervisor_uri_present_and_enabled = rail.IfOperator(
            task_id='if_supervisor_uri_present_and_enabled',
            test='''{{ result('search_for_supervisor_with_managername') | is_truthy and \
                result('search_for_supervisor_with_managername')[0].uri | is_truthy and \
                result('search_for_supervisor_with_managername')[0].enabled.lower() == 'true' }}''',
            yes_task="get_assigned_permissionset_foruser",
            no_task="log_supervisor_lookup",
        )

        get_assigned_permissionset_foruser = rail.RepliconServiceOperator(
            task_id='get_assigned_permissionset_foruser',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_for_supervisor_with_managername')[0].uri }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'permissionSet.name', 'Supervisor', 'permissionSet.uri', '')
        )

        if_supervisor_permissionset_present = rail.IfOperator(
            task_id='if_supervisor_permissionset_present',
            test='''{{ result('get_assigned_permissionset_foruser') | is_truthy }}''',
            yes_task="assign_supervisor",
            no_task="if_supervisorpermissionset_present_inallpermission",
        )

        if_supervisorpermissionset_present_inallpermission = rail.IfOperator(
            task_id='if_supervisorpermissionset_present_inallpermission',
            test='''{{ result('get_all_permission_set').supervisoruri | is_truthy }}''',
            yes_task="assign_supervisorpermissionset_foruser",
            no_task="log_supervisor_lookup",
        )

        assign_supervisorpermissionset_foruser = rail.RepliconServiceOperator(
            task_id='assign_supervisorpermissionset_foruser',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('search_for_supervisor_with_managername')[0].uri }}",
                "permissionSetUri": "{{ result('get_all_permission_set').supervisoruri }}"
            }
        )

        assign_supervisor = rail.RepliconServiceOperator(
            task_id='assign_supervisor',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=request_payload.update_supervisorassignment_overdaterange
        )

        log_supervisor_lookup = rail.WriteLogOperator(
            task_id="log_supervisor_lookup",
            log='{{ dag_run.conf.supervisor_logger}}',
            message="Exception",
            severity="Exception",
            properties=lambda item, dag_run: {
                "employee_maanger_id": dag_run.conf['username'] + '|' + dag_run.conf['emplid'] + '-' + dag_run.conf['managername'],
                "date": get_today(),
                "useruri": dag_run.conf['useruri'],
                "user_log": dag_run.conf['logger']
            }
        )

        if_location_present = rail.IfOperator(
            task_id='if_location_present',
            test='''{{ dag_run.conf.location | is_truthy }}''',
            yes_task="if_location_not_current_loc",
            no_task="if_division_present",
        )

        if_location_not_current_loc = rail.IfOperator(
            task_id='if_location_not_current_loc',
            test='''{{ dag_run.conf.location != dag_run.conf.current_location }}''',
            yes_task="getenabled_location",
            no_task="if_division_present",
        )

        getenabled_location = rail.RepliconServiceOperator(
            task_id='getenabled_location',
            endpoint="/services/LocationService1.svc/GetEnabledLocations",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['location'], 'uri', '')
        )

        if_req_location_present = rail.IfOperator(
            task_id='if_req_location_present',
            test='''{{ result('getenabled_location') | is_truthy }}''',
            yes_task="put_locationschedule_foruser",
            no_task="create_new_draft_location",
        )

        create_new_draft_location = rail.RepliconServiceOperator(
            task_id='create_new_draft_location',
            endpoint="/services/LocationService1.svc/CreateNewDraft",
            data={
                "parentLocationUri": None
            }
        )

        update_location_name = rail.RepliconServiceOperator(
            task_id='update_location_name',
            endpoint="/services/LocationService1.svc/UpdateName",
            data={
                "locationUri": "{{ result('create_new_draft_location') }}",
                "name": "{{ dag_run.conf.location }}"
            }
        )

        publish_location = rail.RepliconServiceOperator(
            task_id='publish_location',
            endpoint="/services/LocationService1.svc/PublishDraft",
            data={
                "draftUri": "{{ result('create_new_draft_location') }}"
            }
        )

        put_locationschedule_foruser = rail.RepliconServiceOperator(
            task_id='put_locationschedule_foruser',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data=request_payload.put_location_payload
        )

        if_division_present = rail.IfOperator(
            task_id='if_division_present',
            test='''{{ dag_run.conf.division | is_truthy }}''',
            yes_task="if_division_not_current_div",
            no_task="get_primaryworkstate_value",
        )

        if_division_not_current_div = rail.IfOperator(
            task_id='if_division_not_current_div',
            test='''{{ dag_run.conf.division != dag_run.conf.current_division }}''',
            yes_task="getenabled_division",
            no_task="get_primaryworkstate_value",
        )

        getenabled_division = rail.RepliconServiceOperator(
            task_id='getenabled_division',
            endpoint="/services/DivisionService1.svc/GetEnabledDivisions",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['division'], 'uri', '')
        )

        if_req_division_present = rail.IfOperator(
            task_id='if_req_division_present',
            test='''{{ result('getenabled_division') | is_truthy }}''',
            yes_task="put_divisionschedule_foruser",
            no_task="create_new_draft_division",
        )

        create_new_draft_division = rail.RepliconServiceOperator(
            task_id='create_new_draft_division',
            endpoint="/services/DivisionService1.svc/CreateNewDraft",
            data={
                "parentDivisionUri": None
            }
        )

        update_division_name = rail.RepliconServiceOperator(
            task_id='update_division_name',
            endpoint="/services/DivisionService1.svc/UpdateName",
            data={
                "divisionUri": "{{ result('create_new_draft_division') }}",
                "name": "{{ dag_run.conf.division }}"
            }
        )

        publish_division = rail.RepliconServiceOperator(
            task_id='publish_division',
            endpoint="/services/DivisionService1.svc/PublishDraft",
            data={
                "draftUri": "{{ result('create_new_draft_division') }}"
            }
        )

        put_divisionschedule_foruser = rail.RepliconServiceOperator(
            task_id='put_divisionschedule_foruser',
            endpoint="/services/DivisionService1.svc/PutDivisionScheduleForUser",
            data=request_payload.put_division_payload
        )

        get_primaryworkstate_value = rail.PythonOperator(
            task_id="get_primaryworkstate_value",
            python_callable=python_callable.get_primaryworkstate_val
        )

        if_homeworkstate_mismatch = rail.IfOperator(
            task_id='if_homeworkstate_mismatch',
            test='''{{ dag_run.conf.homeworkstate | is_truthy and \
                dag_run.conf.homeworkstate != result('get_primaryworkstate_value') }}''',
            yes_task="get_payrulename_frommapper",
            no_task="get_approver_custom_field_value",
        )

        get_payrulename_frommapper = rail.PythonOperator(
            task_id="get_payrulename_frommapper",
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                config.PAYRULE_MAPPER, 'primaryworkstate', dag_run.conf["homeworkstate"], 'payrulename', '')
        )

        if_payrulename_found = rail.IfOperator(
            task_id='if_payrulename_found',
            test='''{{ result('get_payrulename_frommapper') | is_truthy }}''',
            yes_task="get_required_payrulescript_name_uri",
            no_task="get_approver_custom_field_value",
        )

        get_required_payrulescript_name_uri = rail.RepliconServiceOperator(
            task_id='get_required_payrulescript_name_uri',
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts",
            data_handler=lambda response: {
                'uri': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', rail.result('get_payrulename_frommapper'), 'uri', '')
            }
        )

        get_current_payrule_uri = rail.PythonOperator(
            task_id="get_current_payrule_uri",
            python_callable=lambda: get_current_data(
                'payRuleScriptSchedule', 'payRuleScript')
        )

        if_payrule_script_mismatch = rail.IfOperator(
            task_id='if_payrule_script_mismatch',
            test='''{{ result('get_required_payrulescript_name_uri').uri | is_truthy and \
                result('get_required_payrulescript_name_uri').uri != result('get_current_payrule_uri') }}''',
            yes_task="get_payruleassignementschedule_foruser",
            no_task="get_approver_custom_field_value",
        )

        get_payruleassignementschedule_foruser = rail.RepliconServiceOperator(
            task_id='get_payruleassignementschedule_foruser',
            endpoint="/services/PayRuleScriptService2.svc/GetPayRuleScriptAssignmentScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        get_timesheet_periods_for_user = rail.RepliconServiceOperator(
            task_id='get_timesheet_periods_for_user',
            endpoint="/services/TimesheetPeriodService1.svc/GetTimesheetPeriodsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": get_today_dateformat_payload(),
                    "endDate":  get_today_dateformat_payload()
                }
            }
        )

        add_to_payruleschedule = rail.PythonOperator(
            task_id="add_to_payruleschedule",
            python_callable=python_callable.add_to_payrule_schedule
        )

        put_payrule_script_assignment_schedule = rail.RepliconServiceOperator(
            task_id='put_payrule_script_assignment_schedule',
            endpoint="/services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                "scheduleEntries": rail.result('add_to_payruleschedule')
            }
        )

        update_homeworkstate_value = rail.RepliconServiceOperator(
            task_id='update_homeworkstate_value',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_alluser_customfields').primaryworkstate_uri }}",
                "value": "{{ dag_run.conf.homeworkstate }}"
            }
        )

        get_approver_custom_field_value = rail.PythonOperator(
            task_id='get_approver_custom_field_value',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_user_details_for_update')[
                                                                         0]['userDetails']['customFieldValues'], 'customField.displayText', 'Approver', 'text', '')
        )

        if_user_is_approver = rail.IfOperator(
            task_id='if_user_is_approver',
            test="{{dag_run.conf.approver.lower() == 'yes'}}",
            yes_task='update_approver_custom_field',
            no_task='update_approver_custom_field_to_no'
        )

        update_approver_custom_field = rail.RepliconServiceOperator(
            task_id='update_approver_custom_field',
            endpoint='/services/CustomFieldService1.svc/UpdateTextValue',
            data={
                "objectUri": "{{ dag_run.conf.useruri}}",
                "customFieldUri": "{{ result('get_alluser_customfields').approver_uri }}",
                "value": "Yes"
            }
        )

        assign_permission_sets_for_approver = rail.RepliconServiceOperator(
            task_id='assign_permission_sets_for_approver',
            endpoint='/services/PermissionSetService1.svc/PutPermissionSetAssignmentsForUser',
            data={
                'userUri': "{{ dag_run.conf.useruri }}",
                'permissionSetUris': ["{{ result('get_all_permission_set').supervisoruri }}", "{{ result('get_all_permission_set').reportuseruri }}"]
            }
        )

        if_timesheet_template_assigned = rail.IfOperator(
            task_id='if_timesheet_template_assigned',
            test="{{result('get_all_policy_sets').existing_timesheettemplate | is_truthy}}",
            yes_task='remove_timesheet_template',
            no_task='if_timeoff_template_assigned'
        )

        remove_timesheet_template = rail.RepliconServiceOperator(
            task_id="remove_timesheet_template",
            endpoint="/services/PolicySetService1.svc/RemovePolicySetAssignmentFromUser",
            data={
                "userUri": "{{ dag_run.conf.useruri}}",
                "policySetUri": "{{ result('get_all_policy_sets').existing_timesheettemplate }}"
            }
        )

        if_timeoff_template_assigned = rail.IfOperator(
            task_id='if_timeoff_template_assigned',
            test="{{result('get_all_policy_sets').timeoff | is_truthy}}",
            yes_task='remove_timeoff_template',
            no_task='if_employeetype_present_forudf'
        )

        remove_timeoff_template = rail.RepliconServiceOperator(
            task_id="remove_timeoff_template",
            endpoint="/services/PolicySetService1.svc/RemovePolicySetAssignmentFromUser",
            data={
                "userUri": "{{ dag_run.conf.useruri}}",
                "policySetUri": "{{ result('get_all_policy_sets').timeoff }}"
            }
        )

        update_approver_custom_field_to_no = rail.RepliconServiceOperator(
            task_id='update_approver_custom_field_to_no',
            endpoint='/services/CustomFieldService1.svc/UpdateTextValue',
            data={
                "objectUri": "{{ dag_run.conf.useruri}}",
                "customFieldUri": "{{ result('get_alluser_customfields').approver_uri }}",
                "value": "No"
            }
        )

        if_manager_contains_yes = rail.IfOperator(
            task_id='if_manager_contains_yes',
            test='''{{ dag_run.conf.manager.lower() == 'yes' }}''',
            yes_task="if_systemautoapproval_timeoffuri_present",
            no_task="if_manager_contains_no",
        )

        if_systemautoapproval_timeoffuri_present = rail.IfOperator(
            task_id='if_systemautoapproval_timeoffuri_present',
            test='''{{ result('get_all_timeoff_approval').defaulturi | is_truthy }}''',
            yes_task="updatetimeoffapprovalpath_foruser",
            no_task="if_reportuseruri_forpermission_present",
        )

        updatetimeoffapprovalpath_foruser = rail.RepliconServiceOperator(
            task_id='updatetimeoffapprovalpath_foruser',
            endpoint='/services/TimeOffApprovalService1.svc/UpdateApprovalPathForUser',
            data={
                'userUri': "{{ dag_run.conf.useruri }}",
                'approvalPathUri': "{{ result('get_all_timeoff_approval').defaulturi }}"
            }
        )

        if_reportuseruri_forpermission_present = rail.IfOperator(
            task_id='if_reportuseruri_forpermission_present',
            test='''{{ result('get_all_permission_set').reportuseruri | is_truthy }}''',
            yes_task="assign_reportuser_permissionset_foruser",
            no_task="if_supervisoruri_forpermission_present",
        )

        assign_reportuser_permissionset_foruser = rail.RepliconServiceOperator(
            task_id='assign_reportuser_permissionset_foruser',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data={
                'userUri': "{{ dag_run.conf.useruri }}",
                'permissionSetUri': "{{ result('get_all_permission_set').reportuseruri }}"
            }
        )

        if_supervisoruri_forpermission_present = rail.IfOperator(
            task_id='if_supervisoruri_forpermission_present',
            test='''{{ result('get_all_permission_set').supervisoruri | is_truthy }}''',
            yes_task="assign_supervisor_permissionset_foruser",
            no_task="if_manager_contains_no",
        )

        assign_supervisor_permissionset_foruser = rail.RepliconServiceOperator(
            task_id='assign_supervisor_permissionset_foruser',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data={
                'userUri': "{{ dag_run.conf.useruri }}",
                'permissionSetUri': "{{ result('get_all_permission_set').supervisoruri }}"
            }
        )

        if_manager_contains_no = rail.IfOperator(
            task_id='if_manager_contains_no',
            test='''{{ dag_run.conf.manager.lower() == 'no' }}''',
            yes_task="if_employeetype_contains_hourly",
            no_task="if_employeetype_present_forudf",
        )

        if_employeetype_contains_hourly = rail.IfOperator(
            task_id='if_employeetype_contains_hourly',
            test=lambda dag_run: bool(
                'hourly' in dag_run.conf['employeetype'].lower()),
            yes_task="if_punchentry_policyset_present",
            no_task="if_employeetype_contains_salaried",
        )

        if_punchentry_policyset_present = rail.IfOperator(
            task_id='if_punchentry_policyset_present',
            test='''{{ result('get_all_policy_sets').punch_uri | is_truthy }}''',
            yes_task="assign_punch_entry_policy",
            no_task="if_supervisorapprovalpath_timeoff_present",
        )

        assign_punch_entry_policy = rail.RepliconServiceOperator(
            task_id='assign_punch_entry_policy',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "policySetUri": "{{ result('get_all_policy_sets').punch_uri }}"
            }
        )

        if_supervisorapprovalpath_timeoff_present = rail.IfOperator(
            task_id='if_supervisorapprovalpath_timeoff_present',
            test='''{{ result('get_all_timeoff_approval').supervisor_approvaluri | is_truthy }}''',
            yes_task="updatetimeoffsupervisorapprovalpath_foruser",
            no_task="get_hourlyuser_for_user",
        )

        updatetimeoffsupervisorapprovalpath_foruser = rail.RepliconServiceOperator(
            task_id='updatetimeoffsupervisorapprovalpath_foruser',
            endpoint='/services/TimeOffApprovalService1.svc/UpdateApprovalPathForUser',
            data={
                'userUri': "{{ dag_run.conf.useruri }}",
                'approvalPathUri': "{{ result('get_all_timeoff_approval').supervisor_approvaluri }}"
            }
        )

        get_hourlyuser_for_user = rail.PythonOperator(
            task_id="get_hourlyuser_for_user",
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_user_details_for_update')[0]['permissionSets'], 'displayText', 'Hourly User', 'displayText', '')
        )

        if_hourlyuser_notpresent = rail.IfOperator(
            task_id='if_hourlyuser_notpresent',
            test='''{{ result('get_hourlyuser_for_user') | is_falsy }}''',
            yes_task="if_hourlyuser_permissionuri_present",
            no_task="if_employeetype_contains_salaried",
        )

        if_hourlyuser_permissionuri_present = rail.IfOperator(
            task_id='if_hourlyuser_permissionuri_present',
            test='''{{ result('get_all_permission_set').hourlyuseruri | is_truthy }}''',
            yes_task="assign_hourlyuser_permissionset_foruser",
            no_task="if_employeetype_contains_salaried",
        )

        assign_hourlyuser_permissionset_foruser = rail.RepliconServiceOperator(
            task_id='assign_hourlyuser_permissionset_foruser',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data={
                'userUri': "{{ dag_run.conf.useruri }}",
                'permissionSetUri': "{{ result('get_all_permission_set').hourlyuseruri }}"
            }
        )

        if_employeetype_contains_salaried = rail.IfOperator(
            task_id='if_employeetype_contains_salaried',
            test=lambda dag_run: bool(
                'salaried' in dag_run.conf['employeetype'].lower()),
            yes_task="updatetimeoffapprovalpath_forsalarieduser",
            no_task="if_employeetype_present_forudf",
        )

        updatetimeoffapprovalpath_forsalarieduser = rail.RepliconServiceOperator(
            task_id='updatetimeoffapprovalpath_forsalarieduser',
            endpoint='/services/TimeOffApprovalService1.svc/UpdateApprovalPathForUser',
            data={
                'userUri': "{{ dag_run.conf.useruri }}",
                'approvalPathUri': "{{ result('get_all_timeoff_approval').defaulturi }}"
            }
        )

        get_salarieduser_for_user = rail.PythonOperator(
            task_id="get_salarieduser_for_user",
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_user_details_for_update')[0]['permissionSets'], 'displayText', 'Salaried User', 'displayText', '')
        )

        if_salarieduser_notpresent = rail.IfOperator(
            task_id='if_salarieduser_notpresent',
            test='''{{ result('get_salarieduser_for_user') | is_falsy }}''',
            yes_task="if_salarieduser_permissionuri_present",
            no_task="if_employeetype_present_forudf",
        )

        if_salarieduser_permissionuri_present = rail.IfOperator(
            task_id='if_salarieduser_permissionuri_present',
            test='''{{ result('get_all_permission_set').salarieduseruri | is_truthy }}''',
            yes_task="assign_salarieduser_permissionset_foruser",
            no_task="if_employeetype_present_forudf",
        )

        assign_salarieduser_permissionset_foruser = rail.RepliconServiceOperator(
            task_id='assign_salarieduser_permissionset_foruser',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data={
                'userUri': "{{ dag_run.conf.useruri }}",
                'permissionSetUri': "{{ result('get_all_permission_set').salarieduseruri }}"
            }
        )

        if_employeetype_present_forudf = rail.IfOperator(
            task_id='if_employeetype_present_forudf',
            test='''{{ dag_run.conf.employeetype | is_truthy }}''',
            yes_task="trigger_customfield_dropdown_update_emptype",
            no_task="if_managementlevel_present_forudf",
        )

        trigger_customfield_dropdown_update_emptype = rail.TriggerDagRunOperator(
            task_id='trigger_customfield_dropdown_update_emptype',
            trigger_dag_id=config.child_process_customfield_for_dropdown_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "useruri": "{{ dag_run.conf.useruri }}",
                "udf_value": "{{ dag_run.conf.employeetype }}",
                "udf_uri": "{{ result('get_alluser_customfields').emp_type_uri }}",
                "username": "{{ dag_run.conf.username }}",
                "emplid": "{{ dag_run.conf.emplid }}",
                "logger": '{{ dag_run.conf.logger}}'
            }
        )

        wait_customfield_dropdown_update_emptype = rail.WaitForDagRunsSensor(
            task_id="wait_customfield_dropdown_update_emptype",
            dag_runs="{{result('trigger_customfield_dropdown_update_emptype')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        if_managementlevel_present_forudf = rail.IfOperator(
            task_id='if_managementlevel_present_forudf',
            test='''{{ dag_run.conf.managementlevel | is_truthy }}''',
            yes_task="trigger_customfield_dropdown_update_mgmtlevel",
            no_task="If_position_is_present",
        )

        trigger_customfield_dropdown_update_mgmtlevel = rail.TriggerDagRunOperator(
            task_id='trigger_customfield_dropdown_update_mgmtlevel',
            trigger_dag_id=config.child_process_customfield_for_dropdown_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "useruri": "{{ dag_run.conf.useruri }}",
                "udf_value": "{{ dag_run.conf.managementlevel }}",
                "udf_uri": "{{ result('get_alluser_customfields').management_level_uri }}",
                "username": "{{ dag_run.conf.username }}",
                "emplid": "{{ dag_run.conf.emplid }}",
                "logger": '{{ dag_run.conf.logger}}'
            }
        )

        wait_customfield_dropdown_update_mgmtlevel = rail.WaitForDagRunsSensor(
            task_id="wait_customfield_dropdown_update_mgmtlevel",
            dag_runs="{{result('trigger_customfield_dropdown_update_mgmtlevel')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        If_position_is_present = rail.IfOperator(
            task_id='If_position_is_present',
            test='''{{ dag_run.conf.position | is_truthy }}''',
            yes_task="update_position_udf",
            no_task="if_schedulehour_present_and_notequal_currentschedulehour",
        )

        update_position_udf = rail.RepliconServiceOperator(
            task_id='update_position_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_alluser_customfields').position_uri }}",
                "value": "{{ dag_run.conf.position }}"
            }
        )

        if_schedulehour_present_and_notequal_currentschedulehour = rail.IfOperator(
            task_id='if_schedulehour_present_and_notequal_currentschedulehour',
            test='''{{ dag_run.conf.scheduledhours | is_truthy and \
                dag_run.conf.scheduledhours != dag_run.conf.current_scheduledhour }}''',
            yes_task="trigger_customfield_dropdown_update_schdhrs",
            no_task="if_employeetype_present_and_div_equals_capellauniversityinc",
        )

        trigger_customfield_dropdown_update_schdhrs = rail.TriggerDagRunOperator(
            task_id='trigger_customfield_dropdown_update_schdhrs',
            trigger_dag_id=config.child_process_customfield_for_dropdown_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "useruri": "{{ dag_run.conf.useruri }}",
                "udf_value": "{{ dag_run.conf.scheduledhours }}",
                "udf_uri": "{{ result('get_alluser_customfields').scheduled_hours_uri }}",
                "username": "{{ dag_run.conf.username }}",
                "emplid": "{{ dag_run.conf.emplid }}",
                "logger": '{{ dag_run.conf.logger}}'
            }
        )

        wait_customfield_dropdown_update_schdhrs = rail.WaitForDagRunsSensor(
            task_id="wait_customfield_dropdown_update_schdhrs",
            dag_runs="{{result('trigger_customfield_dropdown_update_schdhrs')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        get_enabled_servicecenters = rail.RepliconServiceOperator(
            task_id='get_enabled_servicecenters',
            endpoint='/services/ServiceCenterService1.svc/GetEnabledServiceCenters',
            data_handler=lambda response, dag_run: {
                'scheduledhour_srvcntr_uri': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', dag_run.conf['scheduledhours'], 'uri', '')
            }
        )

        if_enabledservicecenter_present = rail.IfOperator(
            task_id='if_enabledservicecenter_present',
            test='''{{ result('get_enabled_servicecenters').scheduledhour_srvcntr_uri | is_truthy }}''',
            yes_task="modifyuser_for_srvcntr",
            no_task="if_employeetype_present_and_div_equals_capellauniversityinc",
        )

        modifyuser_for_srvcntr = rail.RepliconServiceOperator(
            task_id='modifyuser_for_srvcntr',
            endpoint='/services/ImportService1.svc/ApplyUserModifications',
            data=request_payload.userpayload_for_srvccntr
        )

        if_employeetype_present_and_div_equals_capellauniversityinc = rail.IfOperator(
            task_id='if_employeetype_present_and_div_equals_capellauniversityinc',
            test='''{{ dag_run.conf.employeetype | is_truthy and \
                dag_run.conf.division == 'Capella University Inc' }}''',
            yes_task="if_user_is_not_approver",
            no_task="if_timezone_present",
        )

        if_user_is_not_approver = rail.IfOperator(
            task_id='if_user_is_not_approver',
            test="{{dag_run.conf.approver.lower() != 'yes'}}",
            yes_task='get_policyset_for_user_value',
            no_task='if_timezone_present'
        )

        get_policyset_for_user_value = rail.PythonOperator(
            task_id="get_policyset_for_user_value",
            python_callable=python_callable.get_policyset_val_foruser
        )

        if_get_policyset_for_user_value_present = rail.IfOperator(
            task_id='if_get_policyset_for_user_value_present',
            test='''{{ result('get_policyset_for_user_value') | is_truthy }}''',
            yes_task="assign_policysettouser_foremptype",
            no_task="if_timezone_present",
        )

        assign_policysettouser_foremptype = rail.RepliconServiceOperator(
            task_id='assign_policysettouser_foremptype',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "policySetUri": "{{ result('get_policyset_for_user_value') }}"
            }
        )

        if_timezone_present = rail.IfOperator(
            task_id='if_timezone_present',
            test='''{{ dag_run.conf.timezone | is_truthy }}''',
            yes_task="get_timezone_uri_frommapper",
            no_task="if_substitutename_present_and_notcontainedin_username",
        )

        get_timezone_uri_frommapper = rail.PythonOperator(
            task_id="get_timezone_uri_frommapper",
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                config.TIMEZONE_MAPPER, 'workdaytimezone', dag_run.conf["timezone"], 'uri', '')
        )

        update_timezone_foruser = rail.RepliconServiceOperator(
            task_id='update_timezone_foruser',
            endpoint="/services/InternationalizationService1.svc/UpdateTimeZoneForUser",
            data={
                'userUri': "{{ dag_run.conf.useruri }}",
                "timeZoneUri": "{{ result('get_timezone_uri_frommapper') }}"
            }
        )

        if_substitutename_present_and_notcontainedin_username = rail.IfOperator(
            task_id='if_substitutename_present_and_notcontainedin_username',
            test='''{{ dag_run.conf.substitutename | is_truthy and \
                dag_run.conf.substitutename != dag_run.conf.username }}''',
            yes_task="get_all_substitute_user_assignments_for_user",
            no_task="if_empstatus_present",
        )

        get_all_substitute_user_assignments_for_user = rail.RepliconServiceOperator(
            task_id='get_all_substitute_user_assignments_for_user',
            endpoint="/services/SubstituteUserAssignmentService1.svc/GetAllSubstituteUserAssignmentsForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        if_substituteuser_assignment_present = rail.IfOperator(
            task_id='if_substituteuser_assignment_present',
            test='''{{ result('get_all_substitute_user_assignments_for_user') | is_truthy }}''',
            yes_task="get_substituteuserassigned",
            no_task="search_substitute",
        )

        get_substituteuserassigned = rail.PythonOperator(
            task_id='get_substituteuserassigned',
            python_callable=lambda dag_run: get_substitueUserUris(
                dag_run.conf['substitutename'], 'get_all_substitute_user_assignments_for_user')
        )

        if_subsituteuserassigned_notpresent = rail.IfOperator(
            task_id='if_subsituteuserassigned_notpresent',
            test='''{{ result('get_substituteuserassigned') | is_falsy }}''',
            yes_task="search_substitute",
            no_task="if_empstatus_present",
        )

        search_substitute = rail.RepliconServiceOperator(
            task_id='search_substitute',
            endpoint='/services/UserListService1.svc/GetData',
            data=request_payload.search_substituteuser_payload
        )

        if_searched_substitute_present = rail.IfOperator(
            task_id='if_searched_substitute_present',
            test='''{{ result('search_substitute') | is_truthy }}''',
            yes_task="get_substituteuser_uri",
            no_task="log_strayer_substituteuser_lookup",
        )

        get_substituteuser_uri = rail.PythonOperator(
            task_id='get_substituteuser_uri',
            python_callable=lambda dag_run: get_substitueUser_fromsearch(
                dag_run.conf['substitutename'], 'search_substitute')
        )

        if_substituteuser_uri_present = rail.IfOperator(
            task_id='if_substituteuser_uri_present',
            test='''{{ result('get_substituteuser_uri') | is_truthy }}''',
            yes_task="imporsonate_andcreateinteractivesession",
            no_task="log_strayer_substituteuser_lookup",
        )

        def get_headers(res):
            data = res.json()['d']
            auth_token = list(
                filter(lambda x: x['name'] == 'AUTHTOKEN', data['sessionCookies']))[0]['value']
            tenant = list(
                filter(lambda x: x['name'] == 'TENANT', data['sessionCookies']))[0]['value']
            return {'cookie': f'AUTHTOKEN={auth_token};TENANT={tenant}', 'Path': '/'}

        imporsonate_andcreateinteractivesession = rail.RepliconServiceOperator(
            task_id='imporsonate_andcreateinteractivesession',
            endpoint="/services/UserImpersonationService1.svc/AdministrativeImpersonationAndCreateInteractiveSession",
            data={
                "impersonatedUserUri": "{{ dag_run.conf.useruri }}"
            },
            response_filter=get_headers
        )

        trigger_createsubtituteuser_strayeruniversity = rail.TriggerDagRunOperator(
            task_id='trigger_createsubtituteuser_strayeruniversity',
            trigger_dag_id=config.child_assign_substitute_user_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "suburi": "{{ result('get_substituteuser_uri') }}",
                "actualuri": "{{ dag_run.conf.useruri }}",
                "username": "{{ dag_run.conf.username }}",
                "emplid": "{{ dag_run.conf.emplid }}",
                "logger": '{{ dag_run.conf.logger}}'
            }
        )

        wait_createsubtituteuser_strayeruniversity = rail.WaitForDagRunsSensor(
            task_id="wait_createsubtituteuser_strayeruniversity",
            dag_runs="{{result('trigger_createsubtituteuser_strayeruniversity')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        log_strayer_substituteuser_lookup = rail.WriteLogOperator(
            task_id="log_strayer_substituteuser_lookup",
            log='{{ dag_run.conf.substitute_user_log}}',
            message='NA',
            properties={
                "actualuri": "{{ dag_run.conf.useruri }}",
                "suburi": "{{ dag_run.conf.substitutename }}"
            }
        )

        if_empstatus_present = rail.IfOperator(
            task_id='if_empstatus_present',
            test='''{{ dag_run.conf.employeestatus | is_truthy }}''',
            yes_task="get_customfield_dropdown_foremployeestatus_udf",
            no_task="log_userimport",
        )

        get_customfield_dropdown_foremployeestatus_udf = rail.RepliconServiceOperator(
            task_id='get_customfield_dropdown_foremployeestatus_udf',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data={
                'customFieldUri': "{{ result('get_alluser_customfields').employeestatus_uri }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['employeestatus'], 'uri', '')
        )

        if_empstatus_uri_present = rail.IfOperator(
            task_id='if_empstatus_uri_present',
            test='''{{ result('get_customfield_dropdown_foremployeestatus_udf') | is_truthy }}''',
            yes_task="update_dropdown_value_for_empstatusudf",
            no_task="log_userimport",
        )

        update_dropdown_value_for_empstatusudf = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_for_empstatusudf',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_alluser_customfields').employeestatus_uri }}",
                "customFieldDropDownOptionUri": "{{ result('get_customfield_dropdown_foremployeestatus_udf') }}"
            }
        )

        log_userimport = rail.WriteLogOperator(
            task_id="log_userimport",
            log='{{ dag_run.conf.logger}}',
            message='Success',
            properties={
                "username": "{{ dag_run.conf.username }}" + "|" + "{{ dag_run.conf.emplid }}",
                "action": "Update user",
                "status": "Success",
                "details": "{{ dag_run_ecid() }}"
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            log='{{ dag_run.conf.logger}}',
            severity="Error",
            trigger_rule="one_failed",
            message='{{ get_error_message() }}',
            properties={
                "username": "{{ dag_run.conf.username }}" + "|" + "{{ dag_run.conf.emplid }}",
                "action": "Update user",
                "status": "Error",
                "details": "{{ dag_run_ecid() }}" + "-" + "{{ get_error_message() }}"
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> get_user_details_for_update

        get_user_details_for_update >> if_user_enabled_is_false

        if_user_enabled_is_false >> rail.Label(
            'Yes') >> log_user_update_ignored >> catch_and_log_error
        if_user_enabled_is_false >> rail.Label(
            'No') >> if_request_firstname_mismatch

        if_request_firstname_mismatch >> rail.Label(
            'Yes') >> update_firstname >> if_lastname_mismatch
        if_request_firstname_mismatch >> rail.Label(
            'No') >> if_lastname_mismatch

        if_lastname_mismatch >> rail.Label(
            'Yes') >> update_lastname >> if_email_mismatch
        if_lastname_mismatch >> rail.Label('No') >> if_email_mismatch

        if_email_mismatch >> rail.Label(
            'Yes') >> update_email_address >> if_hiredate_present
        if_email_mismatch >> rail.Label('No') >> if_hiredate_present

        if_hiredate_present >> rail.Label(
            'Yes') >> get_start_day_mismatch_val >> check_if_start_day_mismatch
        if_hiredate_present >> rail.Label('No') >> get_employeetype_val

        check_if_start_day_mismatch >> rail.Label(
            'Yes') >> update_emp_daterange >> get_employeetype_val
        check_if_start_day_mismatch >> rail.Label('No') >> get_employeetype_val

        get_employeetype_val >> get_required_department_uri_from_all_enabled_dept_list >> get_all_employee_type >> get_all_permission_set >> \
            get_all_timesheet_approval >> get_all_timeoff_approval >> get_all_policy_sets >> get_enabled_activities >> \
            get_alluser_customfields >> get_all_active_scripts >> if_department_present

        if_department_present >> rail.Label(
            'Yes') >> add_department >> department_to_assign >> if_employeetype_present
        if_department_present >> rail.Label(
            'No') >> department_to_assign >> if_employeetype_present

        if_employeetype_present >> rail.Label(
            'Yes') >> add_employeetype >> employeetype_to_assign >> if_department_mismatch
        if_employeetype_present >> rail.Label(
            'No') >> employeetype_to_assign >> if_department_mismatch

        if_department_mismatch >> rail.Label(
            'Yes') >> update_department_foruser >> get_employeetype_foruser
        if_department_mismatch >> rail.Label('No') >> get_employeetype_foruser

        get_employeetype_foruser >> if_employeetype_mismatch

        if_employeetype_mismatch >> rail.Label(
            'Yes') >> update_employeetype_foruser >> if_employeetype_is_federalworkstudy_pt_hourly
        if_employeetype_mismatch >> rail.Label(
            'No') >> if_employeetype_is_federalworkstudy_pt_hourly

        if_employeetype_is_federalworkstudy_pt_hourly >> rail.Label(
            'Yes') >> update_defaultuser_activity >> if_employeetype_is_salaried_or_parttimesalaried
        if_employeetype_is_federalworkstudy_pt_hourly >> rail.Label(
            'No') >> if_employeetype_is_salaried_or_parttimesalaried

        if_employeetype_is_salaried_or_parttimesalaried >> rail.Label(
            'Yes') >> if_timesheetApprovalPath_isnot_supervisorcustom
        if_employeetype_is_salaried_or_parttimesalaried >> rail.Label(
            'No') >> if_timesheetApprovalPath_isnot_supervisor

        if_timesheetApprovalPath_isnot_supervisorcustom >> rail.Label(
            'Yes') >> update_approval_pathforuser_timesheetcustom >> if_managername_present_and_not_in_username
        if_timesheetApprovalPath_isnot_supervisorcustom >> rail.Label(
            'No') >> if_managername_present_and_not_in_username

        if_timesheetApprovalPath_isnot_supervisor >> rail.Label(
            'Yes') >> update_approval_pathforuser_timesheet >> if_managername_present_and_not_in_username
        if_timesheetApprovalPath_isnot_supervisor >> rail.Label(
            'No') >> if_managername_present_and_not_in_username

        if_managername_present_and_not_in_username >> rail.Label(
            'Yes') >> if_supervisor_loginname_mismatch
        if_managername_present_and_not_in_username >> rail.Label(
            'No') >> if_location_present

        if_supervisor_loginname_mismatch >> rail.Label(
            'Yes') >> search_for_supervisor_with_managername >> if_supervisor_uri_present_and_enabled
        if_supervisor_loginname_mismatch >> rail.Label(
            'No') >> if_location_present

        if_supervisor_uri_present_and_enabled >> rail.Label(
            'Yes') >> get_assigned_permissionset_foruser >> if_supervisor_permissionset_present
        if_supervisor_uri_present_and_enabled >> rail.Label(
            'No') >> log_supervisor_lookup >> if_location_present

        if_supervisor_permissionset_present >> rail.Label(
            'Yes') >> assign_supervisor >> if_location_present
        if_supervisor_permissionset_present >> rail.Label(
            'No') >> if_supervisorpermissionset_present_inallpermission

        if_supervisorpermissionset_present_inallpermission >> rail.Label(
            'Yes') >> assign_supervisorpermissionset_foruser >> assign_supervisor >> if_location_present
        if_supervisorpermissionset_present_inallpermission >> rail.Label(
            'No') >> log_supervisor_lookup >> if_location_present

        if_location_present >> rail.Label('Yes') >> if_location_not_current_loc
        if_location_present >> rail.Label('No') >> if_division_present

        if_location_not_current_loc >> rail.Label(
            'Yes') >> getenabled_location >> if_req_location_present
        if_location_not_current_loc >> rail.Label('No') >> if_division_present

        if_req_location_present >> rail.Label(
            'Yes') >> put_locationschedule_foruser
        if_req_location_present >> rail.Label(
            'No') >> create_new_draft_location >> update_location_name >> publish_location >> put_locationschedule_foruser

        put_locationschedule_foruser >> if_division_present

        if_division_present >> rail.Label('Yes') >> if_division_not_current_div
        if_division_present >> rail.Label('No') >> get_primaryworkstate_value

        if_division_not_current_div >> rail.Label(
            'Yes') >> getenabled_division >> if_req_division_present
        if_division_not_current_div >> rail.Label(
            'No') >> get_primaryworkstate_value

        if_req_division_present >> rail.Label(
            'Yes') >> put_divisionschedule_foruser >> get_primaryworkstate_value
        if_req_division_present >> rail.Label('No') >> create_new_draft_division >> update_division_name >> publish_division >> \
            put_divisionschedule_foruser >> get_primaryworkstate_value

        get_primaryworkstate_value >> if_homeworkstate_mismatch

        if_homeworkstate_mismatch >> rail.Label(
            'Yes') >> get_payrulename_frommapper >> if_payrulename_found
        if_homeworkstate_mismatch >> rail.Label(
            'No') >> get_approver_custom_field_value

        if_payrulename_found >> rail.Label(
            'Yes') >> get_required_payrulescript_name_uri >> get_current_payrule_uri >> if_payrule_script_mismatch
        if_payrulename_found >> rail.Label(
            'No') >> get_approver_custom_field_value

        if_payrule_script_mismatch >> rail.Label('Yes') >> get_payruleassignementschedule_foruser >> get_timesheet_periods_for_user >> add_to_payruleschedule >> \
            put_payrule_script_assignment_schedule >> update_homeworkstate_value >> get_approver_custom_field_value
        if_payrule_script_mismatch >> rail.Label(
            'No') >> get_approver_custom_field_value

        get_approver_custom_field_value >> if_user_is_approver

        if_user_is_approver >> rail.Label(
            'Yes') >> update_approver_custom_field >> assign_permission_sets_for_approver
        assign_permission_sets_for_approver >> if_timesheet_template_assigned
        if_user_is_approver >> rail.Label(
            'No') >> update_approver_custom_field_to_no >> if_manager_contains_yes

        if_timesheet_template_assigned >> rail.Label(
            'Yes') >> remove_timesheet_template >> if_timeoff_template_assigned
        if_timesheet_template_assigned >> rail.Label(
            'No') >> if_timeoff_template_assigned

        if_timeoff_template_assigned >> rail.Label(
            'Yes') >> remove_timeoff_template >> if_employeetype_present_forudf
        if_timeoff_template_assigned >> rail.Label(
            'No') >> if_employeetype_present_forudf

        if_manager_contains_yes >> rail.Label(
            'Yes') >> if_systemautoapproval_timeoffuri_present
        if_manager_contains_yes >> rail.Label('No') >> if_manager_contains_no

        if_systemautoapproval_timeoffuri_present >> rail.Label(
            'Yes') >> updatetimeoffapprovalpath_foruser >> if_reportuseruri_forpermission_present
        if_systemautoapproval_timeoffuri_present >> rail.Label(
            'No') >> if_reportuseruri_forpermission_present

        if_reportuseruri_forpermission_present >> rail.Label(
            'Yes') >> assign_reportuser_permissionset_foruser >> if_supervisoruri_forpermission_present
        if_reportuseruri_forpermission_present >> rail.Label(
            'No') >> if_supervisoruri_forpermission_present

        if_supervisoruri_forpermission_present >> rail.Label(
            'Yes') >> assign_supervisor_permissionset_foruser >> if_manager_contains_no
        if_supervisoruri_forpermission_present >> rail.Label(
            'No') >> if_manager_contains_no

        if_manager_contains_no >> rail.Label(
            'Yes') >> if_employeetype_contains_hourly
        if_manager_contains_no >> rail.Label(
            'No') >> if_employeetype_present_forudf

        if_employeetype_contains_hourly >> rail.Label(
            'Yes') >> if_punchentry_policyset_present
        if_employeetype_contains_hourly >> rail.Label(
            'No') >> if_employeetype_contains_salaried

        if_punchentry_policyset_present >> rail.Label(
            'Yes') >> assign_punch_entry_policy >> if_supervisorapprovalpath_timeoff_present
        if_punchentry_policyset_present >> rail.Label(
            'No') >> if_supervisorapprovalpath_timeoff_present

        if_supervisorapprovalpath_timeoff_present >> rail.Label('Yes') >> updatetimeoffsupervisorapprovalpath_foruser >> get_hourlyuser_for_user >> \
            if_hourlyuser_notpresent
        if_supervisorapprovalpath_timeoff_present >> rail.Label(
            'No') >> get_hourlyuser_for_user >> if_hourlyuser_notpresent

        if_hourlyuser_notpresent >> rail.Label(
            'Yes') >> if_hourlyuser_permissionuri_present
        if_hourlyuser_notpresent >> rail.Label(
            'No') >> if_employeetype_contains_salaried

        if_hourlyuser_permissionuri_present >> rail.Label(
            'Yes') >> assign_hourlyuser_permissionset_foruser >> if_employeetype_contains_salaried
        if_hourlyuser_permissionuri_present >> rail.Label(
            'No') >> if_employeetype_contains_salaried

        if_employeetype_contains_salaried >> rail.Label(
            'Yes') >> updatetimeoffapprovalpath_forsalarieduser >> get_salarieduser_for_user >> if_salarieduser_notpresent
        if_employeetype_contains_salaried >> rail.Label(
            'No') >> if_employeetype_present_forudf

        if_salarieduser_notpresent >> rail.Label(
            'Yes') >> if_salarieduser_permissionuri_present
        if_salarieduser_notpresent >> rail.Label(
            'No') >> if_employeetype_present_forudf

        if_salarieduser_permissionuri_present >> rail.Label(
            'Yes') >> assign_salarieduser_permissionset_foruser >> if_employeetype_present_forudf
        if_salarieduser_permissionuri_present >> rail.Label(
            'No') >> if_employeetype_present_forudf

        if_employeetype_present_forudf >> rail.Label('Yes') >> trigger_customfield_dropdown_update_emptype >> wait_customfield_dropdown_update_emptype >> \
            if_managementlevel_present_forudf
        if_employeetype_present_forudf >> rail.Label(
            'No') >> if_managementlevel_present_forudf

        if_managementlevel_present_forudf >> rail.Label('Yes') >> trigger_customfield_dropdown_update_mgmtlevel >> wait_customfield_dropdown_update_mgmtlevel >> \
            If_position_is_present
        if_managementlevel_present_forudf >> rail.Label(
            'No') >> If_position_is_present

        If_position_is_present >> rail.Label(
            'Yes') >> update_position_udf >> if_schedulehour_present_and_notequal_currentschedulehour
        If_position_is_present >> rail.Label(
            'No') >> if_schedulehour_present_and_notequal_currentschedulehour

        if_schedulehour_present_and_notequal_currentschedulehour >> rail.Label('Yes') >> trigger_customfield_dropdown_update_schdhrs >> wait_customfield_dropdown_update_schdhrs >> \
            get_enabled_servicecenters >> if_enabledservicecenter_present
        if_schedulehour_present_and_notequal_currentschedulehour >> rail.Label(
            'No') >> if_employeetype_present_and_div_equals_capellauniversityinc

        if_enabledservicecenter_present >> rail.Label(
            'Yes') >> modifyuser_for_srvcntr >> if_employeetype_present_and_div_equals_capellauniversityinc
        if_enabledservicecenter_present >> rail.Label(
            'No') >> if_employeetype_present_and_div_equals_capellauniversityinc

        if_employeetype_present_and_div_equals_capellauniversityinc >> rail.Label(
            'Yes') >> if_user_is_not_approver

        if_user_is_not_approver >> rail.Label('No') >> if_timezone_present
        if_user_is_not_approver >> rail.Label(
            'Yes') >> get_policyset_for_user_value >> if_get_policyset_for_user_value_present

        if_employeetype_present_and_div_equals_capellauniversityinc >> rail.Label(
            'No') >> if_timezone_present

        if_get_policyset_for_user_value_present >> rail.Label(
            'Yes') >> assign_policysettouser_foremptype >> if_timezone_present
        if_get_policyset_for_user_value_present >> rail.Label(
            'No') >> if_timezone_present

        if_timezone_present >> rail.Label(
            'Yes') >> get_timezone_uri_frommapper >> update_timezone_foruser >> if_substitutename_present_and_notcontainedin_username
        if_timezone_present >> rail.Label(
            'No') >> if_substitutename_present_and_notcontainedin_username

        if_substitutename_present_and_notcontainedin_username >> rail.Label(
            'Yes') >> get_all_substitute_user_assignments_for_user >> if_substituteuser_assignment_present
        if_substitutename_present_and_notcontainedin_username >> rail.Label(
            'No') >> if_empstatus_present

        if_substituteuser_assignment_present >> rail.Label(
            'Yes') >> get_substituteuserassigned >> if_subsituteuserassigned_notpresent
        if_substituteuser_assignment_present >> rail.Label(
            'No') >> search_substitute >> if_searched_substitute_present

        if_subsituteuserassigned_notpresent >> rail.Label(
            'Yes') >> search_substitute >> if_searched_substitute_present
        if_subsituteuserassigned_notpresent >> rail.Label(
            'No') >> if_empstatus_present

        if_searched_substitute_present >> rail.Label(
            'Yes') >> get_substituteuser_uri >> if_substituteuser_uri_present
        if_searched_substitute_present >> rail.Label(
            'No') >> log_strayer_substituteuser_lookup >> if_empstatus_present

        if_substituteuser_uri_present >> rail.Label('Yes') >> imporsonate_andcreateinteractivesession >> trigger_createsubtituteuser_strayeruniversity >> \
            wait_createsubtituteuser_strayeruniversity >> if_empstatus_present
        if_substituteuser_uri_present >> rail.Label(
            'No') >> log_strayer_substituteuser_lookup >> if_empstatus_present

        if_empstatus_present >> rail.Label(
            'Yes') >> get_customfield_dropdown_foremployeestatus_udf >> if_empstatus_uri_present
        if_empstatus_present >> rail.Label('No') >> log_userimport

        if_empstatus_uri_present >> rail.Label(
            'Yes') >> update_dropdown_value_for_empstatusudf >> log_userimport
        if_empstatus_uri_present >> rail.Label('No') >> log_userimport

        log_userimport >> catch_and_log_error

    return dag


rail.for_each_instance(create_dag)
