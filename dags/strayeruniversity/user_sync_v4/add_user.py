from datetime import timedelta, datetime
import json
from airflow.models import Variable
import rail
from strayeruniversity.user_sync_v4.utils import request_payload, python_callable
from strayeruniversity.user_sync_v4.utils.python_callable import get_today, get_substitueUserUris, get_substitueUser_fromsearch, get_policyschedule_entries
from strayeruniversity.user_sync_v4.mappers.strayer_payrule_mapper import payrule_mapper
from strayeruniversity.user_sync_v4.mappers.strayer_timezone_mapper import timezone_mapper


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.child_add_user_dag_id,
        description=f'strayeruniversity_usersync_add_user_child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.add_user_child_dag_active_runs,
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
            no_task='if_empstatus_is_T'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_empstatus_is_T',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_empstatus_is_T = rail.IfOperator(
            task_id='if_empstatus_is_T',
            test='''{{ dag_run.conf.employeestatus == 'T' }}''',
            yes_task="log_terminated_user",
            no_task="get_employeetype_val",
        )

        log_terminated_user = rail.WriteLogOperator(
            task_id="log_terminated_user",
            log='{{ dag_run.conf.logger}}',
            message='Ignored',
            properties={
                "username": "{{ dag_run.conf.username }}" + "|" + "{{ dag_run.conf.emplid }}",
                "action": "Adduser",
                "status": "Ignored - Terminated users",
                "details": "{{ dag_run_ecid() }}"
            }
        )

        get_employeetype_val = rail.PythonOperator(
            task_id="get_employeetype_val",
            python_callable=python_callable.get_employeetype_value
        )

        logs_list = rail.CreateLogOperator(
            task_id="logs_list"
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
            python_callable=lambda: rail.result('get_all_employee_type')['employeetype_name'] if rail.result(
                'get_all_employee_type')['employeetype_name'] else rail.result('add_employeetype')['displayText']
        )

        log_usermail_id = rail.PythonOperator(
            task_id="log_usermail_id",
            python_callable=lambda dag_run: dag_run.conf['workemail'].lower()
        )

        create_user = rail.RepliconServiceOperator(
            task_id='create_user',
            endpoint="/services/ImportService1.svc/PutUser3",
            data=request_payload.get_createuser_payload
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

        if_employeetype_is_salaried_or_parttimesalaried = rail.IfOperator(
            task_id='if_employeetype_is_salaried_or_parttimesalaried',
            test='''{{ dag_run.conf.employeetype == 'Salaried' or \
                dag_run.conf.employeetype == 'Part-time Salaried' }}''',
            yes_task="update_approval_pathforuser_timesheetcustom",
            no_task="remove_all_timeoffs",
        )

        update_approval_pathforuser_timesheetcustom = rail.RepliconServiceOperator(
            task_id='update_approval_pathforuser_timesheetcustom',
            endpoint='/services/TimesheetApprovalService1.svc/UpdateApprovalPathForUser',
            data={
                'userUri': "{{ result('create_user').uri }}",
                'approvalPathUri': "{{ result('get_all_timesheet_approval').supervisor_custom_uri }}"
            }
        )

        remove_all_timeoffs = rail.RepliconServiceOperator(
            task_id='remove_all_timeoffs',
            endpoint='/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser',
            data={
                'userUri': "{{ result('create_user').uri }}",
                'timeOffTypeUris': []
            }
        )

        if_hiredate_present = rail.IfOperator(
            task_id='if_hiredate_present',
            test='''{{ dag_run.conf.hiredate | is_truthy }}''',
            yes_task="update_emp_daterange",
            no_task="update_department_foruser",
        )

        update_emp_daterange = rail.RepliconServiceOperator(
            task_id='update_emp_daterange',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=request_payload.update_emp_daterange_hiredate
        )

        update_department_foruser = rail.RepliconServiceOperator(
            task_id='update_department_foruser',
            endpoint='/services/DepartmentService1.svc/UpdateDepartmentForUser',
            data={
                'userUri': "{{ result('create_user').uri }}",
                'departmentUri': "{{ result('department_to_assign') }}"
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
                'timeoff': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Time Off', 'uri', ''),
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

        if_user_approver = rail.IfOperator(
            task_id='if_user_approver',
            test="{{dag_run.conf.approver.lower() == 'yes'}}",
            yes_task='if_managername_present_and_not_in_username',
            no_task='if_employeetype_present_for_timesheettemplate'
        )

        if_employeetype_present_for_timesheettemplate = rail.IfOperator(
            task_id='if_employeetype_present_for_timesheettemplate',
            test='''{{ dag_run.conf.employeetype | is_truthy }}''',
            yes_task="get_policyset_for_user_value",
            no_task="if_managername_present_and_not_in_username",
        )

        get_policyset_for_user_value = rail.PythonOperator(
            task_id="get_policyset_for_user_value",
            python_callable=python_callable.get_policyset_val_foruser
        )

        if_get_policyset_for_user_value_present = rail.IfOperator(
            task_id='if_get_policyset_for_user_value_present',
            test='''{{ result('get_policyset_for_user_value') | is_truthy }}''',
            yes_task="assign_policysettouser_foremptype",
            no_task="if_managername_present_and_not_in_username",
        )

        assign_policysettouser_foremptype = rail.RepliconServiceOperator(
            task_id='assign_policysettouser_foremptype',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "policySetUri": "{{ result('get_policyset_for_user_value') }}"
            }
        )

        if_managername_present_and_not_in_username = rail.IfOperator(
            task_id='if_managername_present_and_not_in_username',
            test=lambda dag_run: bool(dag_run.conf['managername'] and
                                      dag_run.conf['managername'] not in dag_run.conf['username']),
            yes_task="search_for_supervisor_with_managername",
            no_task="get_required_holidaycalendar_uri",
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
                result('search_for_supervisor_with_managername')[0].uri | is_truthy }}''',
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
            no_task="assign_supervisorpermissionset_foruser",
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
            data={
                "userUri": "{{ result('create_user').uri }}",
                "supervisorUri": "{{ result('search_for_supervisor_with_managername')[0].uri }}"
            }
        )

        log_supervisor_lookup = rail.WriteLogOperator(
            task_id="log_supervisor_lookup",
            log='{{ dag_run.conf.supervisor_logger}}',
            message="Exception",
            severity="Exception",
            properties=lambda dag_run: {
                "employee_maanger_id": dag_run.conf['username'] + '|' + dag_run.conf['emplid'] + '-' + dag_run.conf['managername'],
                "date": get_today(),
                "useruri": rail.result('create_user')['uri'],
                "user_log": dag_run.conf['logger']
            }
        )

        get_required_holidaycalendar_uri = rail.RepliconServiceOperator(
            task_id='get_required_holidaycalendar_uri',
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Holidays for SEI 2020', 'uri', '')
        )

        is_holidaycalendar_uri_present = rail.IfOperator(
            task_id='is_holidaycalendar_uri_present',
            test="{{ result('get_required_holidaycalendar_uri') | is_truthy }}",
            yes_task="update_holiday_calendar",
            no_task="if_location_present"
        )

        update_holiday_calendar = rail.RepliconServiceOperator(
            task_id='update_holiday_calendar',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
                'userUri': "{{ result('create_user').uri }}",
                "holidayCalendarUri": "{{ result('get_required_holidaycalendar_uri') }}"
            }
        )

        if_location_present = rail.IfOperator(
            task_id='if_location_present',
            test='''{{ dag_run.conf.location | is_truthy }}''',
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
            yes_task="getenabled_division",
            no_task="if_scheduledhours_present",
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

        if_scheduledhours_present = rail.IfOperator(
            task_id='if_scheduledhours_present',
            test='''{{ dag_run.conf.scheduledhours | is_truthy }}''',
            yes_task="get_enabled_servicecenters",
            no_task="get_alluser_customfields",
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
            yes_task="put_service_center_schedule_for_user",
            no_task="log_schedulehours_not_updated",
        )

        put_service_center_schedule_for_user = rail.RepliconServiceOperator(
            task_id='put_service_center_schedule_for_user',
            endpoint="/services/ServiceCenterService1.svc/PutServiceCenterScheduleForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "scheduleEntries": [
                    {
                        "serviceCenter": {
                            "uri": "{{ result('get_enabled_servicecenters').scheduledhour_srvcntr_uri }}"
                        }
                    }
                ]
            }
        )

        log_schedulehours_not_updated = rail.WriteLogOperator(
            task_id='log_schedulehours_not_updated',
            log="{{ result('logs_list') }}",
            message="na",
            severity="Exception",
            properties={
                "value": "Scheduled hours not updated since Scheduled hours" + "{{ dag_run.conf.scheduledhours }}" + " not available in Replicon"
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

        if_empstatus_present = rail.IfOperator(
            task_id='if_empstatus_present',
            test='''{{ dag_run.conf.employeestatus | is_truthy }}''',
            yes_task="get_customfield_dropdown_foremployeestatus_udf",
            no_task="if_homeworkstate_present",
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
            no_task="if_homeworkstate_present",
        )

        update_dropdown_value_for_empstatusudf = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_for_empstatusudf',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_alluser_customfields').employeestatus_uri }}",
                "customFieldDropDownOptionUri": "{{ result('get_customfield_dropdown_foremployeestatus_udf') }}"
            }
        )

        if_homeworkstate_present = rail.IfOperator(
            task_id='if_homeworkstate_present',
            test='''{{ dag_run.conf.homeworkstate | is_truthy }}''',
            yes_task="update_hoemworkstate_value",
            no_task="update_workweekday_foruser",
        )

        update_hoemworkstate_value = rail.RepliconServiceOperator(
            task_id='update_hoemworkstate_value',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_alluser_customfields').primaryworkstate_uri }}",
                "value": "{{ dag_run.conf.homeworkstate }}"
            }
        )

        get_payrulename_frommapper = rail.PythonOperator(
            task_id="get_payrulename_frommapper",
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                payrule_mapper, 'primaryworkstate', dag_run.conf["homeworkstate"], 'payrulename', '')
        )

        if_payrulename_found = rail.IfOperator(
            task_id='if_payrulename_found',
            test='''{{ result('get_payrulename_frommapper') | is_truthy }}''',
            yes_task="get_required_payrulescript_name_uri",
            no_task="update_workweekday_foruser",
        )

        get_required_payrulescript_name_uri = rail.RepliconServiceOperator(
            task_id='get_required_payrulescript_name_uri',
            endpoint="/services/PayRuleScriptService2.svc/GetActiveScripts",
            data_handler=lambda response: {
                'uri': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', rail.result('get_payrulename_frommapper'), 'uri', '')
            }
        )

        if_payrule_script_present = rail.IfOperator(
            task_id='if_payrule_script_present',
            test='''{{ result('get_required_payrulescript_name_uri').uri | is_truthy }}''',
            yes_task="put_payrule_script_assignment_schedule",
            no_task="update_workweekday_foruser",
        )

        put_payrule_script_assignment_schedule = rail.RepliconServiceOperator(
            task_id='put_payrule_script_assignment_schedule',
            endpoint="/services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data=lambda: {
                "userUri": rail.result('create_user')['uri'],
                "scheduleEntries": [{
                    "payRuleScript": {
                        "uri": rail.result('get_required_payrulescript_name_uri')['uri']
                    }
                }]
            }
        )

        update_workweekday_foruser = rail.RepliconServiceOperator(
            task_id='update_workweekday_foruser',
            endpoint="/services/UserService1.svc/UpdateWorkWeekStartDayForUser",
            data={
                'userUri': "{{ result('create_user').uri }}",
                "dayOfWeekUri": "urn:replicon:day-of-week:monday"
            }
        )

        create_scheduletoassign_variable = rail.SetVariableOperator(
            task_id='create_scheduletoassign_variable',
            append=False,
            name='schedule_to_assign',
            value=None
        )

        get_all_schedule = rail.RepliconServiceOperator(
            task_id='get_all_schedule',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules"
        )

        if_employeetype_contains_parttime = rail.IfOperator(
            task_id='if_employeetype_contains_parttime',
            test=lambda dag_run: bool(
                'part-time' in dag_run.conf['employeetype'].lower()),
            yes_task="get_schedulename_from_mapper",
            no_task="update_scheduletoassign_to_8hrsaday",
        )

        get_schedulename_from_mapper = rail.PythonOperator(
            task_id="get_schedulename_from_mapper",
            python_callable=lambda dag_run: python_callable.get_schedulename(
                dag_run, config)
        )

        if_schedulename_present = rail.IfOperator(
            task_id='if_schedulename_present',
            test='''{{ result('get_schedulename_from_mapper') | is_truthy }}''',
            yes_task="update_schedulename",
            no_task="update_schedulename_notfound",
        )

        update_schedulename = rail.SetVariableOperator(
            task_id='update_schedulename',
            append=False,
            name='{{ result("create_scheduletoassign_variable").name }}',
            value=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_schedule'), 'displayText', rail.result('get_schedulename_from_mapper')[0]['schedulename'], 'uri', '')
        )

        update_schedulename_notfound = rail.SetVariableOperator(
            task_id='update_schedulename_notfound',
            append=False,
            name='{{ result("create_scheduletoassign_variable").name }}',
            value='Schedule is not found'
        )

        update_scheduletoassign_to_8hrsaday = rail.SetVariableOperator(
            task_id='update_scheduletoassign_to_8hrsaday',
            append=False,
            name='{{ result("create_scheduletoassign_variable").name }}',
            value=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_schedule'), 'displayText', '8 hours/day; Mon-Fri', 'uri', '')
        )

        get_scheduletoassign_val = rail.PythonOperator(
            task_id="get_scheduletoassign_val",
            python_callable=lambda: rail.get_dag_run_var(
                rail.result('create_scheduletoassign_variable')['name'])
        )

        if_scheduletoassign_is_found = rail.IfOperator(
            task_id='if_scheduletoassign_is_found',
            test='''{{ result('get_scheduletoassign_val') != 'Schedule is not found' }}''',
            yes_task="putschedulepolicyschedule_foruser",
            no_task="log_schedulenotassigned",
        )

        putschedulepolicyschedule_foruser = rail.RepliconServiceOperator(
            task_id='putschedulepolicyschedule_foruser',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data=lambda: {
                'userUri': rail.result('create_user')['uri'],
                "scheduleEntries": [
                    {
                        "schedulePolicy": {
                            "officeScheduleUri": rail.result('get_scheduletoassign_val'),
                            "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                        }
                    }
                ]
            }
        )

        log_schedulenotassigned = rail.WriteLogOperator(
            task_id='log_schedulenotassigned',
            log="{{ result('logs_list') }}",
            message="na",
            severity="Exception",
            properties={
                "value": "Schedule not assigned since scheduled hours is not defined in the mapper"
            }
        )

        get_all_payrulescript = rail.RepliconServiceOperator(
            task_id='get_all_payrulescript',
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts"
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

        if_user_is_approver = rail.IfOperator(
            task_id='if_user_is_approver',
            test="{{dag_run.conf.approver.lower() == 'yes'}}",
            yes_task='update_approver_custom_field',
            no_task='update_approver_custom_field_no'
        )

        update_approver_custom_field = rail.RepliconServiceOperator(
            task_id='update_approver_custom_field',
            endpoint='/services/CustomFieldService1.svc/UpdateTextValue',
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_alluser_customfields').approver_uri }}",
                "value": "Yes"
            }
        )

        assign_supervisor_permission_set_for_approver = rail.RepliconServiceOperator(
            task_id='assign_supervisor_permission_set_for_approver',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data={
                'userUri': "{{ result('create_user').uri }}",
                'permissionSetUri': "{{ result('get_all_permission_set').supervisoruri }}"
            }
        )

        assign_reportuser_permissionset_for_approver = rail.RepliconServiceOperator(
            task_id='assign_reportuser_permissionset_for_approver',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data={
                'userUri': "{{ result('create_user').uri }}",
                'permissionSetUri': "{{ result('get_all_permission_set').reportuseruri }}"
            }
        )

        update_approver_custom_field_no = rail.RepliconServiceOperator(
            task_id='update_approver_custom_field_no',
            endpoint='/services/CustomFieldService1.svc/UpdateTextValue',
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_alluser_customfields').approver_uri }}",
                "value": "No"
            }
        )

        if_manager_contains_yes = rail.IfOperator(
            task_id='if_manager_contains_yes',
            test='''{{ dag_run.conf.manager.lower() == 'yes' }}''',
            yes_task="updatetimeoffapprovalpath_foruser",
            no_task="if_manager_contains_no",
        )

        updatetimeoffapprovalpath_foruser = rail.RepliconServiceOperator(
            task_id='updatetimeoffapprovalpath_foruser',
            endpoint='/services/TimeOffApprovalService1.svc/UpdateApprovalPathForUser',
            data={
                'userUri': "{{ result('create_user').uri }}",
                'approvalPathUri': "{{ result('get_all_timeoff_approval').defaulturi }}"
            }
        )

        assign_reportuser_permissionset_foruser = rail.RepliconServiceOperator(
            task_id='assign_reportuser_permissionset_foruser',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data={
                'userUri': "{{ result('create_user').uri }}",
                'permissionSetUri': "{{ result('get_all_permission_set').reportuseruri }}"
            }
        )

        assign_supervisor_permissionset_foruser = rail.RepliconServiceOperator(
            task_id='assign_supervisor_permissionset_foruser',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data={
                'userUri': "{{ result('create_user').uri }}",
                'permissionSetUri': "{{ result('get_all_permission_set').supervisoruri }}"
            }
        )

        if_manager_contains_no = rail.IfOperator(
            task_id='if_manager_contains_no',
            test='''{{ dag_run.conf.manager.lower() == 'no' }}''',
            yes_task="if_employeetype_contains_hourly",
            no_task="get_enabled_activities",
        )

        if_employeetype_contains_hourly = rail.IfOperator(
            task_id='if_employeetype_contains_hourly',
            test=lambda dag_run: bool(
                'hourly' in dag_run.conf['employeetype'].lower()),
            yes_task="assign_punch_entry_policy",
            no_task="if_employeetype_contains_salaried",
        )

        assign_punch_entry_policy = rail.RepliconServiceOperator(
            task_id='assign_punch_entry_policy',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "policySetUri": "{{ result('get_all_policy_sets').punch_uri }}"
            }
        )

        updatetimeoffsupervisorapprovalpath_foruser = rail.RepliconServiceOperator(
            task_id='updatetimeoffsupervisorapprovalpath_foruser',
            endpoint='/services/TimeOffApprovalService1.svc/UpdateApprovalPathForUser',
            data={
                'userUri': "{{ result('create_user').uri }}",
                'approvalPathUri': "{{ result('get_all_timeoff_approval').supervisor_approvaluri }}"
            }
        )

        assign_hourlyuser_permissionset_foruser = rail.RepliconServiceOperator(
            task_id='assign_hourlyuser_permissionset_foruser',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data={
                'userUri': "{{ result('create_user').uri }}",
                'permissionSetUri': "{{ result('get_all_permission_set').hourlyuseruri }}"
            }
        )

        if_employeetype_contains_salaried = rail.IfOperator(
            task_id='if_employeetype_contains_salaried',
            test=lambda dag_run: bool(
                'salaried' in dag_run.conf['employeetype'].lower()),
            yes_task="updatetimeoffapprovalpath_forsalarieduser",
            no_task="get_enabled_activities",
        )

        updatetimeoffapprovalpath_forsalarieduser = rail.RepliconServiceOperator(
            task_id='updatetimeoffapprovalpath_forsalarieduser',
            endpoint='/services/TimeOffApprovalService1.svc/UpdateApprovalPathForUser',
            data={
                'userUri': "{{ result('create_user').uri }}",
                'approvalPathUri': "{{ result('get_all_timeoff_approval').defaulturi }}"
            }
        )

        assign_salarieduser_permissionset_foruser = rail.RepliconServiceOperator(
            task_id='assign_salarieduser_permissionset_foruser',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data={
                'userUri': "{{ result('create_user').uri }}",
                'permissionSetUri': "{{ result('get_all_permission_set').salarieduseruri }}"
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

        if_employeetype_is_federalworkstudy_pt_hourly = rail.IfOperator(
            task_id='if_employeetype_is_federalworkstudy_pt_hourly',
            test='''{{ dag_run.conf.employeetype == 'Federal Work Study Part-time Hourly' and \
                result('get_enabled_activities').workstudy_uri | is_truthy }}''',
            yes_task="update_defaultuser_activity",
            no_task="if_employeetype_present_forudf",
        )

        update_defaultuser_activity = rail.RepliconServiceOperator(
            task_id='update_defaultuser_activity',
            endpoint='/services/ActivityService1.svc/UpdateDefaultUserActivity',
            data={
                'user': {
                    'uri': "{{ result('create_user').uri }}"
                },
                'activity': {
                    'uri': "{{ result('get_enabled_activities').workstudy_uri }}"
                }
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
                "useruri": "{{ result('create_user').uri }}",
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
                "useruri": "{{ result('create_user').uri }}",
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
            no_task="if_schedulehour_present",
        )

        update_position_udf = rail.RepliconServiceOperator(
            task_id='update_position_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_alluser_customfields').position_uri }}",
                "value": "{{ dag_run.conf.position }}"
            }
        )

        if_schedulehour_present = rail.IfOperator(
            task_id='if_schedulehour_present',
            test='''{{ dag_run.conf.scheduledhours | is_truthy }}''',
            yes_task="trigger_customfield_dropdown_update_schdhrs",
            no_task="if_timezone_present",
        )

        trigger_customfield_dropdown_update_schdhrs = rail.TriggerDagRunOperator(
            task_id='trigger_customfield_dropdown_update_schdhrs',
            trigger_dag_id=config.child_process_customfield_for_dropdown_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "useruri": "{{ result('create_user').uri }}",
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

        if_timezone_present = rail.IfOperator(
            task_id='if_timezone_present',
            test='''{{ dag_run.conf.timezone | is_truthy }}''',
            yes_task="get_timezone_uri_frommapper",
            no_task="if_substitutename_present_and_notcontainedin_username",
        )

        get_timezone_uri_frommapper = rail.PythonOperator(
            task_id="get_timezone_uri_frommapper",
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                timezone_mapper, 'workdaytimezone', dag_run.conf["timezone"], 'uri', '')
        )

        update_timezone_foruser = rail.RepliconServiceOperator(
            task_id='update_timezone_foruser',
            endpoint="/services/InternationalizationService1.svc/UpdateTimeZoneForUser",
            data={
                'userUri': "{{ result('create_user').uri }}",
                "timeZoneUri": "{{ result('get_timezone_uri_frommapper') }}"
            }
        )

        if_substitutename_present_and_notcontainedin_username = rail.IfOperator(
            task_id='if_substitutename_present_and_notcontainedin_username',
            test='''{{ dag_run.conf.substitutename | is_truthy and \
                dag_run.conf.substitutename != dag_run.conf.username }}''',
            yes_task="get_all_substitute_user_assignments_for_user",
            no_task="if_div_emptype_schdhrs_hmwrkst_present",
        )

        get_all_substitute_user_assignments_for_user = rail.RepliconServiceOperator(
            task_id='get_all_substitute_user_assignments_for_user',
            endpoint="/services/SubstituteUserAssignmentService1.svc/GetAllSubstituteUserAssignmentsForUser",
            data={
                "userUri": "{{ result('create_user').uri }}"
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
            no_task="if_div_emptype_schdhrs_hmwrkst_present",
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
                "impersonatedUserUri": "{{ result('create_user').uri }}"
            },
            response_filter=get_headers
        )

        trigger_createsubtituteuser_strayeruniversity = rail.TriggerDagRunOperator(
            task_id='trigger_createsubtituteuser_strayeruniversity',
            trigger_dag_id=config.child_assign_substitute_user_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "suburi": "{{ result('get_substituteuser_uri') }}",
                "actualuri": "{{ result('create_user').uri }}",
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
                "actualuri": "{{ result('create_user').uri }}",
                "suburi": "{{ dag_run.conf.substitutename }}"
            }
        )

        if_div_emptype_schdhrs_hmwrkst_present = rail.IfOperator(
            task_id='if_div_emptype_schdhrs_hmwrkst_present',
            test='''{{ dag_run.conf.division | is_truthy and \
                dag_run.conf.employeetype | is_truthy and \
                    dag_run.conf.scheduledhours | is_truthy and \
                        dag_run.conf.homeworkstate | is_truthy }}''',
            yes_task="if_user_equals_approver",
            no_task="log_userimport_success",
        )

        if_user_equals_approver = rail.IfOperator(
            task_id='if_user_equals_approver',
            test="{{dag_run.conf.approver.lower() == 'yes'}}",
            yes_task='remove_timeoff_template',
            no_task='get_alltimeoff_types'
        )

        remove_timeoff_template = rail.RepliconServiceOperator(
            task_id="remove_timeoff_template",
            endpoint="/services/PolicySetService1.svc/RemovePolicySetAssignmentFromUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "policySetUri": "{{ result('get_all_policy_sets').timeoff }}"
            }
        )

        get_alltimeoff_types = rail.RepliconServiceOperator(
            task_id='get_alltimeoff_types',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes"
        )

        declare_eligibletimeofftypes = rail.SetVariableOperator(
            task_id='declare_eligibletimeofftypes',
            append=False,
            name='eligibletimeofftypes',
            value=[]
        )

        get_homestate_to_search_val = rail.PythonOperator(
            task_id='get_homestate_to_search_val',
            python_callable=lambda dag_run: 'California' if 'CA' in dag_run.conf[
                'homeworkstate'] else 'Non CA'
        )

        create_schedulehours_variable = rail.SetVariableOperator(
            task_id='create_schedulehours_variable',
            append=False,
            name='schedule_hours',
            value='Any'
        )

        create_primaryworkstate_variable = rail.SetVariableOperator(
            task_id='create_primaryworkstate_variable',
            append=False,
            name='primaryworkstate',
            value=None
        )

        create_emptype_variable = rail.SetVariableOperator(
            task_id='create_emptype_variable',
            append=False,
            name='employeetype',
            value=None
        )

        if_scheduledhours_is_present = rail.IfOperator(
            task_id='if_scheduledhours_is_present',
            test='''{{ dag_run.conf.scheduledhours | is_truthy }}''',
            yes_task="if_div_is_capellaunivinc_and_emptype_is_fedwrkstdypthourly",
            no_task="get_scheduledhours_var_val",
        )

        if_div_is_capellaunivinc_and_emptype_is_fedwrkstdypthourly = rail.IfOperator(
            task_id='if_div_is_capellaunivinc_and_emptype_is_fedwrkstdypthourly',
            test='''{{ dag_run.conf.division == 'Capella University Inc' and \
                dag_run.conf.employeetype == 'Federal Work Study Part-time Hourly' }}''',
            yes_task="update_schedulehour_toany",
            no_task="if_scheduledhours_less_than20",
        )

        update_schedulehour_toany = rail.SetVariableOperator(
            task_id='update_schedulehour_toany',
            append=False,
            name='{{ result("create_schedulehours_variable").name }}',
            value='Any'
        )

        if_scheduledhours_less_than20 = rail.IfOperator(
            task_id='if_scheduledhours_less_than20',
            test=lambda dag_run: bool(
                int(dag_run.conf['scheduledhours']) < 20),
            yes_task="update_schedulehour_tolessthan20",
            no_task="if_scheduledhours_more_thanorequal20",
        )

        update_schedulehour_tolessthan20 = rail.SetVariableOperator(
            task_id='update_schedulehour_tolessthan20',
            append=False,
            name='{{ result("create_schedulehours_variable").name }}',
            value='<20'
        )

        update_primaryworkstate_toany = rail.SetVariableOperator(
            task_id='update_primaryworkstate_toany',
            append=False,
            name='{{ result("create_primaryworkstate_variable").name }}',
            value='Any'
        )

        if_scheduledhours_more_thanorequal20 = rail.IfOperator(
            task_id='if_scheduledhours_more_thanorequal20',
            test=lambda dag_run: bool(
                int(dag_run.conf['scheduledhours']) >= 20),
            yes_task="update_schedulehour_tomorethanequal20",
            no_task="get_scheduledhours_var_val",
        )

        update_schedulehour_tomorethanequal20 = rail.SetVariableOperator(
            task_id='update_schedulehour_tomorethanequal20',
            append=False,
            name='{{ result("create_schedulehours_variable").name }}',
            value='>=20'
        )

        get_scheduledhours_var_val = rail.PythonOperator(
            task_id="get_scheduledhours_var_val",
            python_callable=lambda: rail.get_dag_run_var(
                rail.result('create_schedulehours_variable')['name'])
        )

        get_primaryworkstate_var_val = rail.PythonOperator(
            task_id="get_primaryworkstate_var_val",
            python_callable=lambda: rail.get_dag_run_var(
                rail.result('create_primaryworkstate_variable')['name'])
        )

        if_emptype_is_not_capellaunivinc_and_schdhrs_lessthan20 = rail.IfOperator(
            task_id='if_emptype_is_not_capellaunivinc_and_schdhrs_lessthan20',
            test='''{{ result('get_scheduledhours_var_val') == '<20' and \
                dag_run.conf.employeetype != 'Capella University Inc' }}''',
            yes_task="update_employeetype_toany",
            no_task="get_employeetype_var_val",
        )

        update_employeetype_toany = rail.SetVariableOperator(
            task_id='update_employeetype_toany',
            append=False,
            name='{{ result("create_emptype_variable").name }}',
            value='Any'
        )

        get_employeetype_var_val = rail.PythonOperator(
            task_id="get_employeetype_var_val",
            python_callable=lambda: rail.get_dag_run_var(
                rail.result('create_emptype_variable')['name'])
        )

        get_timeoff_to_be_assigned_from_mapper = rail.PythonOperator(
            task_id="get_timeoff_to_be_assigned_from_mapper",
            python_callable=lambda dag_run: python_callable.get_timeoff_tobe_assigned(
                dag_run, config)
        )

        get_default_timeofftoassign = rail.PythonOperator(
            task_id="get_default_timeofftoassign",
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_timeoff_to_be_assigned_from_mapper'), "default", "Yes", "timeofftype", "")
        )

        foreach_timeofftype_for20 = rail.ForEachOperator(
            task_id='foreach_timeofftype_for20',
            items=lambda: rail.result(
                'get_timeoff_to_be_assigned_from_mapper'),
            start_task='is_scheduledhours_present',
            end_task='foreach_timeofftype_for20_end'
        )

        is_scheduledhours_present = rail.IfOperator(
            task_id='is_scheduledhours_present',
            test="{{ result('foreach_timeofftype_for20').scheduledhours | is_truthy }}",
            yes_task="get_specific_timeoff_for20",
            no_task="foreach_timeofftype_for20_end",
        )

        get_specific_timeoff_for20 = rail.PythonOperator(
            task_id="get_specific_timeoff_for20",
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_alltimeoff_types'), 'displayText', rail.result(
                    'foreach_timeofftype_for20')['timeofftype'], 'uri', ''
            )
        )

        add_to_eligibletimeoftypes_for20 = rail.SetVariableOperator(
            task_id='add_to_eligibletimeoftypes_for20',
            append=True,
            name='{{ result("declare_eligibletimeofftypes").name }}',
            value=lambda: {
                "timeoffname": rail.result('foreach_timeofftype_for20')['timeofftype'],
                "timeoffuri": rail.result('get_specific_timeoff_for20'),
                "disabled": "No",
            }
        )

        foreach_timeofftype_for20_end = rail.EmptyOperator(
            task_id='foreach_timeofftype_for20_end'
        )

        create_schedulehours_variable_for30 = rail.SetVariableOperator(
            task_id='create_schedulehours_variable_for30',
            append=False,
            name='schedule_hours30',
            value=None
        )

        if_scheduledhours_more_thanorequal30 = rail.IfOperator(
            task_id='if_scheduledhours_more_thanorequal30',
            test=lambda dag_run: bool(
                int(dag_run.conf['scheduledhours']) >= 30),
            yes_task="update_schedulehour_tomorethanequal30",
            no_task="if_scheduledhours_less_than30",
        )

        update_schedulehour_tomorethanequal30 = rail.SetVariableOperator(
            task_id='update_schedulehour_tomorethanequal30',
            append=False,
            name='{{ result("create_schedulehours_variable_for30").name }}',
            value='>=30'
        )

        if_scheduledhours_less_than30 = rail.IfOperator(
            task_id='if_scheduledhours_less_than30',
            test=lambda dag_run: bool(
                int(dag_run.conf['scheduledhours']) < 30),
            yes_task="update_schedulehour_tolessthan30",
            no_task="get_scheduledhours_var_val_for30",
        )

        update_schedulehour_tolessthan30 = rail.SetVariableOperator(
            task_id='update_schedulehour_tolessthan30',
            append=False,
            name='{{ result("create_schedulehours_variable_for30").name }}',
            value='<30'
        )

        get_scheduledhours_var_val_for30 = rail.PythonOperator(
            task_id="get_scheduledhours_var_val_for30",
            python_callable=lambda: rail.get_dag_run_var(
                rail.result('create_schedulehours_variable_for30')['name'])
        )

        get_timeoff_to_be_assigned_from_mapper_for30 = rail.PythonOperator(
            task_id="get_timeoff_to_be_assigned_from_mapper_for30",
            python_callable=lambda dag_run: python_callable.get_timeoff_tobe_assigned_for30(
                dag_run, config)
        )

        foreach_timeofftype_for30 = rail.ForEachOperator(
            task_id='foreach_timeofftype_for30',
            items=lambda: rail.result(
                'get_timeoff_to_be_assigned_from_mapper_for30'),
            start_task='get_specific_timeoff_for30',
            end_task='foreach_timeofftype_for30_end'
        )

        get_specific_timeoff_for30 = rail.PythonOperator(
            task_id="get_specific_timeoff_for30",
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_alltimeoff_types'), 'displayText', rail.result(
                    'foreach_timeofftype_for30')['timeofftype'], 'uri', ''
            )
        )

        add_to_eligibletimeoftypes_for30 = rail.SetVariableOperator(
            task_id='add_to_eligibletimeoftypes_for30',
            append=True,
            name='{{ result("declare_eligibletimeofftypes").name }}',
            value=lambda: {
                "timeoffname": rail.result('foreach_timeofftype_for30')['timeofftype'],
                "timeoffuri": rail.result('get_specific_timeoff_for30'),
                "disabled": rail.result('foreach_timeofftype_for30')['disablebooking'],
            }
        )

        foreach_timeofftype_for30_end = rail.EmptyOperator(
            task_id='foreach_timeofftype_for30_end'
        )

        get_statictimeoff_with_scheduled_as_all = rail.PythonOperator(
            task_id="get_statictimeoff_with_scheduled_as_all",
            python_callable=lambda dag_run: python_callable.get_statictimeoff_with_scheduled_all(
                dag_run, config)
        )

        foreach_timeofftype_scheduled_all = rail.ForEachOperator(
            task_id='foreach_timeofftype_scheduled_all',
            items=lambda: rail.result(
                'get_statictimeoff_with_scheduled_as_all'),
            start_task='get_specific_timeoff_scheduled_all',
            end_task='foreach_timeofftype_scheduled_all_end'
        )

        get_specific_timeoff_scheduled_all = rail.PythonOperator(
            task_id="get_specific_timeoff_scheduled_all",
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_alltimeoff_types'), 'displayText', rail.result(
                    'foreach_timeofftype_scheduled_all')['timeofftype'], 'uri', ''
            )
        )

        add_to_eligibletimeoftypes_scheduled_all = rail.SetVariableOperator(
            task_id='add_to_eligibletimeoftypes_scheduled_all',
            append=True,
            name='{{ result("declare_eligibletimeofftypes").name }}',
            value=lambda: {
                "timeoffname": rail.result('foreach_timeofftype_scheduled_all')['timeofftype'],
                "timeoffuri": rail.result('get_specific_timeoff_scheduled_all'),
                "disabled": rail.result('foreach_timeofftype_scheduled_all')['disablebooking'],
            }
        )

        foreach_timeofftype_scheduled_all_end = rail.EmptyOperator(
            task_id='foreach_timeofftype_scheduled_all_end'
        )

        get_eligibletimeofftypeslist = rail.GetVariableOperator(
            task_id='get_eligibletimeofftypeslist',
            name="{{ result('declare_eligibletimeofftypes').name }}"
        )

        if_eligibletimeoff_list_present = rail.IfOperator(
            task_id='if_eligibletimeoff_list_present',
            test="{{ result('get_eligibletimeofftypeslist') | is_truthy }}",
            yes_task="get_timeoffpolicyuri",
            no_task="log_userimport_success",
        )

        get_timeoffpolicyuri = rail.PythonOperator(
            task_id="get_timeoffpolicyuri",
            python_callable=python_callable.get_timeoff_policy_uri
        )

        put_timeoffassignmentforuser = rail.RepliconServiceOperator(
            task_id='put_timeoffassignmentforuser',
            endpoint='/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser',
            data=request_payload.puttimeoff_payload
        )

        foreach_eligibletimeofftypes = rail.ForEachOperator(
            task_id='foreach_eligibletimeofftypes',
            items=lambda: rail.result('get_eligibletimeofftypeslist')['value'],
            start_task='if_timeoffuri_is_present',
            end_task='foreach_eligibletimeofftypes_end'
        )

        if_timeoffuri_is_present = rail.IfOperator(
            task_id='if_timeoffuri_is_present',
            test="{{ result('foreach_eligibletimeofftypes').timeoffuri | is_truthy }}",
            yes_task="get_default_timeoffpolicyschedule_foruser",
            no_task="foreach_eligibletimeofftypes_end",
        )

        get_default_timeoffpolicyschedule_foruser = rail.RepliconServiceOperator(
            task_id='get_default_timeoffpolicyschedule_foruser',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ result('create_user').uri }}",
                    "timeOffTypeUri": "{{ result('foreach_eligibletimeofftypes').timeoffuri }}"
                }
            },
            data_handler=get_policyschedule_entries
        )

        if_timeofftype_is_seipto_a_or_caa = rail.IfOperator(
            task_id='if_timeofftype_is_seipto_a_or_caa',
            test="{{ result('foreach_eligibletimeofftypes').timeoffname == 'SEI PTO A' or \
                result('foreach_eligibletimeofftypes').timeoffname == 'SEI PTO CA A' }}",
            yes_task="get_existingthresholdvalue_and_scheduledhourvalue",
            no_task="if_effectivedate_is_present",
        )

        get_existingthresholdvalue_and_scheduledhourvalue = rail.PythonOperator(
            task_id="get_existingthresholdvalue_and_scheduledhourvalue",
            python_callable=python_callable.get_threshold_and_scheduledhourval
        )

        if_effectivedate_is_present_for_a_caa = rail.IfOperator(
            task_id='if_effectivedate_is_present_for_a_caa',
            test="{{ result('get_default_timeoffpolicyschedule_foruser').policySetScheduleEntries[0].effectiveDate | is_truthy }}",
            yes_task="putusertimeoffaccountpolicysetschedule_for_a_caa",
            no_task="foreach_eligibletimeofftypes_end",
        )

        putusertimeoffaccountpolicysetschedule_for_a_caa = rail.RepliconServiceOperator(
            task_id='putusertimeoffaccountpolicysetschedule_for_a_caa',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda: {
                "timeOffAccount": {
                    "userUri": rail.result('create_user')['uri'],
                    "timeOffTypeUri": rail.result('foreach_eligibletimeofftypes')['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('get_existingthresholdvalue_and_scheduledhourvalue')
            }
        )

        if_effectivedate_is_present = rail.IfOperator(
            task_id='if_effectivedate_is_present',
            test="{{ result('get_default_timeoffpolicyschedule_foruser').policySetScheduleEntries[0].effectiveDate | is_truthy }}",
            yes_task="putusertimeoffaccountpolicysetschedule",
            no_task="foreach_eligibletimeofftypes_end",
        )

        putusertimeoffaccountpolicysetschedule = rail.RepliconServiceOperator(
            task_id='putusertimeoffaccountpolicysetschedule',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda: {
                "timeOffAccount": {
                    "userUri": rail.result('create_user')['uri'],
                    "timeOffTypeUri": rail.result('foreach_eligibletimeofftypes')['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('get_default_timeoffpolicyschedule_foruser')['policySetScheduleEntries']
            }
        )

        foreach_eligibletimeofftypes_end = rail.EmptyOperator(
            task_id='foreach_eligibletimeofftypes_end'
        )

        get_final_timeoff_list = rail.PythonOperator(
            task_id="get_final_timeoff_list",
            python_callable=python_callable.get_final_timeoflist
        )

        puttimeoffassignment_foruser = rail.RepliconServiceOperator(
            task_id='puttimeoffassignment_foruser',
            endpoint='/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser',
            data=request_payload.puttimeoffassignment_payload
        )

        if_default_timeoff_present = rail.IfOperator(
            task_id='if_default_timeoff_present',
            test="{{ result('get_default_timeofftoassign') | is_truthy }}",
            yes_task="get_default_timeoffuri",
            no_task="log_userimport_success",
        )

        get_default_timeoffuri = rail.PythonOperator(
            task_id="get_default_timeoffuri",
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_alltimeoff_types'), 'displayText', rail.result(
                    'get_default_timeofftoassign'), 'uri', ''
            )
        )

        imporsonate_andcreateinteractivesession_for_timeoff = rail.RepliconServiceOperator(
            task_id='imporsonate_andcreateinteractivesession_for_timeoff',
            endpoint="/services/UserImpersonationService1.svc/AdministrativeImpersonationAndCreateInteractiveSession",
            data={
                "impersonatedUserUri": "{{ result('create_user').uri }}"
            },
            response_filter=get_headers
        )

        get_authtoken_for_timeoff = rail.PythonOperator(
            task_id='get_authtoken_for_timeoff',
            python_callable=lambda: rail.result('imporsonate_andcreateinteractivesession_for_timeoff')[
                'cookie'].split(';')[0].split('=')[1]
        )

        if_authtoken_for_timeoff_present = rail.IfOperator(
            task_id='if_authtoken_for_timeoff_present',
            test="{{ result('get_authtoken_for_timeoff') | is_truthy }}",
            yes_task="update_default_timeoff",
            no_task="log_userimport_success",
        )

        update_default_timeoff = rail.RepliconServiceOperator(
            task_id='update_default_timeoff',
            endpoint="/services/LegacyUIService1.svc/UpdateMyDefaultTimeOffTypeForBookings",
            data={
                "timeOffTypeUri": "{{ result('get_default_timeoffuri') }}"
            },
            headers=lambda: rail.result(
                'imporsonate_andcreateinteractivesession_for_timeoff')
        )

        log_userimport_success = rail.WriteLogOperator(
            task_id="log_userimport_success",
            log='{{ dag_run.conf.logger}}',
            message='Success',
            properties={
                "username": "{{ dag_run.conf.username }}" + "|" + "{{ dag_run.conf.emplid }}",
                "action": "Adduser",
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
                "action": "Adduser",
                "status": "Error",
                "details": "{{ dag_run_ecid() }}" + "-" + "{{ get_error_message() }}"
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> if_empstatus_is_T

        if_empstatus_is_T >> rail.Label(
            'Yes') >> log_terminated_user >> catch_and_log_error
        if_empstatus_is_T >> rail.Label(
            'No') >> get_employeetype_val >> logs_list >> get_required_department_uri_from_all_enabled_dept_list >> if_department_present

        if_department_present >> rail.Label(
            'Yes') >> add_department >> department_to_assign
        if_department_present >> rail.Label('No') >> department_to_assign

        department_to_assign >> get_all_employee_type >> if_employeetype_present

        if_employeetype_present >> rail.Label(
            'Yes') >> add_employeetype >> employeetype_to_assign
        if_employeetype_present >> rail.Label('No') >> employeetype_to_assign

        employeetype_to_assign >> log_usermail_id >> create_user >> get_all_timesheet_approval >> if_employeetype_is_salaried_or_parttimesalaried

        if_employeetype_is_salaried_or_parttimesalaried >> rail.Label(
            'Yes') >> update_approval_pathforuser_timesheetcustom >> remove_all_timeoffs >> if_hiredate_present
        if_employeetype_is_salaried_or_parttimesalaried >> rail.Label(
            'No') >> remove_all_timeoffs >> if_hiredate_present

        if_hiredate_present >> rail.Label(
            'Yes') >> update_emp_daterange >> update_department_foruser
        if_hiredate_present >> rail.Label('No') >> update_department_foruser

        update_department_foruser >> get_all_policy_sets >> get_all_permission_set >> if_user_approver

        if_user_approver >> rail.Label(
            'Yes') >> if_managername_present_and_not_in_username
        if_user_approver >> rail.Label(
            'No') >> if_employeetype_present_for_timesheettemplate

        if_employeetype_present_for_timesheettemplate >> rail.Label(
            'Yes') >> get_policyset_for_user_value >> if_get_policyset_for_user_value_present
        if_employeetype_present_for_timesheettemplate >> rail.Label(
            'No') >> if_managername_present_and_not_in_username

        if_get_policyset_for_user_value_present >> rail.Label(
            'Yes') >> assign_policysettouser_foremptype >> if_managername_present_and_not_in_username
        if_get_policyset_for_user_value_present >> rail.Label(
            'No') >> if_managername_present_and_not_in_username

        if_managername_present_and_not_in_username >> rail.Label(
            'Yes') >> search_for_supervisor_with_managername >> if_supervisor_uri_present_and_enabled
        if_managername_present_and_not_in_username >> rail.Label(
            'No') >> get_required_holidaycalendar_uri

        if_supervisor_uri_present_and_enabled >> rail.Label(
            'Yes') >> get_assigned_permissionset_foruser >> if_supervisor_permissionset_present
        if_supervisor_uri_present_and_enabled >> rail.Label(
            'No') >> log_supervisor_lookup >> get_required_holidaycalendar_uri

        if_supervisor_permissionset_present >> rail.Label(
            'Yes') >> assign_supervisor >> get_required_holidaycalendar_uri
        if_supervisor_permissionset_present >> rail.Label(
            'No') >> assign_supervisorpermissionset_foruser >> assign_supervisor >> get_required_holidaycalendar_uri

        get_required_holidaycalendar_uri >> is_holidaycalendar_uri_present

        is_holidaycalendar_uri_present >> rail.Label(
            'Yes') >> update_holiday_calendar >> if_location_present
        is_holidaycalendar_uri_present >> rail.Label(
            'No') >> if_location_present

        if_location_present >> rail.Label(
            'Yes') >> getenabled_location >> if_req_location_present
        if_location_present >> rail.Label('No') >> if_division_present

        if_req_location_present >> rail.Label(
            'Yes') >> put_locationschedule_foruser >> if_division_present
        if_req_location_present >> rail.Label('No') >> create_new_draft_location >> update_location_name >> publish_location >> \
            put_locationschedule_foruser >> if_division_present

        if_division_present >> rail.Label(
            'Yes') >> getenabled_division >> if_req_division_present
        if_division_present >> rail.Label('No') >> if_scheduledhours_present

        if_req_division_present >> rail.Label(
            'Yes') >> put_divisionschedule_foruser >> if_scheduledhours_present
        if_req_division_present >> rail.Label('No') >> create_new_draft_division >> update_division_name >> publish_division >> \
            put_divisionschedule_foruser >> if_scheduledhours_present

        if_scheduledhours_present >> rail.Label(
            'Yes') >> get_enabled_servicecenters >> if_enabledservicecenter_present
        if_scheduledhours_present >> rail.Label(
            'No') >> get_alluser_customfields

        if_enabledservicecenter_present >> rail.Label(
            'Yes') >> put_service_center_schedule_for_user >> get_alluser_customfields
        if_enabledservicecenter_present >> rail.Label(
            'No') >> log_schedulehours_not_updated >> get_alluser_customfields

        get_alluser_customfields >> if_empstatus_present

        if_empstatus_present >> rail.Label('No') >> if_homeworkstate_present
        if_empstatus_present >> rail.Label(
            'Yes') >> get_customfield_dropdown_foremployeestatus_udf >> if_empstatus_uri_present

        if_empstatus_uri_present >> rail.Label(
            'No') >> if_homeworkstate_present
        if_empstatus_uri_present >> rail.Label(
            'Yes') >> update_dropdown_value_for_empstatusudf >> if_homeworkstate_present

        if_homeworkstate_present >> rail.Label(
            'Yes') >> update_hoemworkstate_value >> get_payrulename_frommapper >> if_payrulename_found
        if_homeworkstate_present >> rail.Label(
            'No') >> update_workweekday_foruser

        if_payrulename_found >> rail.Label(
            'Yes') >> get_required_payrulescript_name_uri >> if_payrule_script_present
        if_payrulename_found >> rail.Label('No') >> update_workweekday_foruser

        if_payrule_script_present >> rail.Label(
            'Yes') >> put_payrule_script_assignment_schedule >> update_workweekday_foruser
        if_payrule_script_present >> rail.Label(
            'No') >> update_workweekday_foruser

        update_workweekday_foruser >> create_scheduletoassign_variable >> get_all_schedule >> if_employeetype_contains_parttime

        if_employeetype_contains_parttime >> rail.Label(
            'Yes') >> get_schedulename_from_mapper >> if_schedulename_present
        if_employeetype_contains_parttime >> rail.Label(
            'No') >> update_scheduletoassign_to_8hrsaday >> get_scheduletoassign_val

        if_schedulename_present >> rail.Label(
            'Yes') >> update_schedulename >> get_scheduletoassign_val
        if_schedulename_present >> rail.Label(
            'No') >> update_schedulename_notfound >> get_scheduletoassign_val

        get_scheduletoassign_val >> if_scheduletoassign_is_found

        if_scheduletoassign_is_found >> rail.Label(
            'Yes') >> putschedulepolicyschedule_foruser >> get_all_payrulescript
        if_scheduletoassign_is_found >> rail.Label(
            'No') >> log_schedulenotassigned >> get_all_payrulescript

        get_all_payrulescript >> get_all_timeoff_approval >> if_user_is_approver

        if_user_is_approver >> rail.Label('Yes') >> update_approver_custom_field >> assign_supervisor_permission_set_for_approver \
            >> assign_reportuser_permissionset_for_approver >> get_enabled_activities
        if_user_is_approver >> rail.Label(
            'No') >> update_approver_custom_field_no >> if_manager_contains_yes

        if_manager_contains_yes >> rail.Label('Yes') >> updatetimeoffapprovalpath_foruser >> assign_reportuser_permissionset_foruser >> \
            assign_supervisor_permissionset_foruser >> if_manager_contains_no
        if_manager_contains_yes >> rail.Label('No') >> if_manager_contains_no

        if_manager_contains_no >> rail.Label(
            'Yes') >> if_employeetype_contains_hourly
        if_manager_contains_no >> rail.Label('No') >> get_enabled_activities

        if_employeetype_contains_hourly >> rail.Label('Yes') >> assign_punch_entry_policy >> updatetimeoffsupervisorapprovalpath_foruser >> \
            assign_hourlyuser_permissionset_foruser >> get_enabled_activities
        if_employeetype_contains_hourly >> rail.Label(
            'No') >> if_employeetype_contains_salaried

        if_employeetype_contains_salaried >> rail.Label('Yes') >> updatetimeoffapprovalpath_forsalarieduser \
            >> assign_salarieduser_permissionset_foruser >> get_enabled_activities
        if_employeetype_contains_salaried >> rail.Label(
            'No') >> get_enabled_activities

        get_enabled_activities >> if_employeetype_is_federalworkstudy_pt_hourly

        if_employeetype_is_federalworkstudy_pt_hourly >> rail.Label(
            'Yes') >> update_defaultuser_activity >> if_employeetype_present_forudf
        if_employeetype_is_federalworkstudy_pt_hourly >> rail.Label(
            'No') >> if_employeetype_present_forudf

        if_employeetype_present_forudf >> rail.Label('Yes') >> trigger_customfield_dropdown_update_emptype >> wait_customfield_dropdown_update_emptype >> \
            if_managementlevel_present_forudf
        if_employeetype_present_forudf >> rail.Label(
            'No') >> if_managementlevel_present_forudf

        if_managementlevel_present_forudf >> rail.Label('Yes') >> trigger_customfield_dropdown_update_mgmtlevel \
            >> wait_customfield_dropdown_update_mgmtlevel >> If_position_is_present
        if_managementlevel_present_forudf >> rail.Label(
            'No') >> If_position_is_present

        If_position_is_present >> rail.Label(
            'Yes') >> update_position_udf >> if_schedulehour_present
        If_position_is_present >> rail.Label('No') >> if_schedulehour_present

        if_schedulehour_present >> rail.Label('Yes') >> trigger_customfield_dropdown_update_schdhrs >> wait_customfield_dropdown_update_schdhrs >> \
            if_timezone_present
        if_schedulehour_present >> rail.Label('No') >> if_timezone_present

        if_timezone_present >> rail.Label('Yes') >> get_timezone_uri_frommapper >> update_timezone_foruser \
            >> if_substitutename_present_and_notcontainedin_username
        if_timezone_present >> rail.Label(
            'No') >> if_substitutename_present_and_notcontainedin_username

        if_substitutename_present_and_notcontainedin_username >> rail.Label('Yes') >> get_all_substitute_user_assignments_for_user \
            >> if_substituteuser_assignment_present
        if_substitutename_present_and_notcontainedin_username >> rail.Label(
            'No') >> if_div_emptype_schdhrs_hmwrkst_present

        if_substituteuser_assignment_present >> rail.Label(
            'Yes') >> get_substituteuserassigned >> if_subsituteuserassigned_notpresent
        if_substituteuser_assignment_present >> rail.Label(
            'No') >> search_substitute

        if_subsituteuserassigned_notpresent >> rail.Label(
            'Yes') >> search_substitute
        if_subsituteuserassigned_notpresent >> rail.Label(
            'No') >> if_div_emptype_schdhrs_hmwrkst_present

        search_substitute >> if_searched_substitute_present

        if_searched_substitute_present >> rail.Label(
            'Yes') >> get_substituteuser_uri >> if_substituteuser_uri_present
        if_searched_substitute_present >> rail.Label(
            'No') >> log_strayer_substituteuser_lookup >> if_div_emptype_schdhrs_hmwrkst_present

        if_substituteuser_uri_present >> rail.Label('Yes') >> imporsonate_andcreateinteractivesession \
            >> trigger_createsubtituteuser_strayeruniversity >> wait_createsubtituteuser_strayeruniversity >> if_div_emptype_schdhrs_hmwrkst_present
        if_substituteuser_uri_present >> rail.Label(
            'No') >> log_strayer_substituteuser_lookup >> if_div_emptype_schdhrs_hmwrkst_present

        if_div_emptype_schdhrs_hmwrkst_present >> rail.Label(
            'Yes') >> if_user_equals_approver

        if_user_equals_approver >> rail.Label(
            'Yes') >> remove_timeoff_template >> log_userimport_success
        if_user_equals_approver >> rail.Label(
            'No') >> get_alltimeoff_types

        get_alltimeoff_types >> declare_eligibletimeofftypes >> get_homestate_to_search_val >> \
            create_schedulehours_variable >> create_primaryworkstate_variable >> create_emptype_variable >> if_scheduledhours_is_present

        if_div_emptype_schdhrs_hmwrkst_present >> rail.Label(
            'No') >> log_userimport_success

        if_scheduledhours_is_present >> rail.Label(
            'Yes') >> if_div_is_capellaunivinc_and_emptype_is_fedwrkstdypthourly
        if_scheduledhours_is_present >> rail.Label(
            'No') >> get_scheduledhours_var_val

        if_div_is_capellaunivinc_and_emptype_is_fedwrkstdypthourly >> rail.Label(
            'Yes') >> update_schedulehour_toany >> get_scheduledhours_var_val
        if_div_is_capellaunivinc_and_emptype_is_fedwrkstdypthourly >> rail.Label(
            'No') >> if_scheduledhours_less_than20

        if_scheduledhours_less_than20 >> rail.Label(
            'Yes') >> update_schedulehour_tolessthan20 >> update_primaryworkstate_toany >> get_scheduledhours_var_val
        if_scheduledhours_less_than20 >> rail.Label(
            'No') >> if_scheduledhours_more_thanorequal20

        if_scheduledhours_more_thanorequal20 >> rail.Label(
            'Yes') >> update_schedulehour_tomorethanequal20 >> get_scheduledhours_var_val
        if_scheduledhours_more_thanorequal20 >> rail.Label(
            'No') >> get_scheduledhours_var_val

        get_scheduledhours_var_val >> get_primaryworkstate_var_val >> if_emptype_is_not_capellaunivinc_and_schdhrs_lessthan20

        if_emptype_is_not_capellaunivinc_and_schdhrs_lessthan20 >> rail.Label(
            'Yes') >> update_employeetype_toany >> get_employeetype_var_val
        if_emptype_is_not_capellaunivinc_and_schdhrs_lessthan20 >> rail.Label(
            'No') >> get_employeetype_var_val

        get_employeetype_var_val >> get_timeoff_to_be_assigned_from_mapper

        get_timeoff_to_be_assigned_from_mapper >> get_default_timeofftoassign >> foreach_timeofftype_for20

        foreach_timeofftype_for20 >> is_scheduledhours_present

        is_scheduledhours_present >> rail.Label(
            'Yes') >> get_specific_timeoff_for20 >> add_to_eligibletimeoftypes_for20 >> foreach_timeofftype_for20_end
        is_scheduledhours_present >> rail.Label(
            'No') >> foreach_timeofftype_for20_end

        foreach_timeofftype_for20 >> foreach_timeofftype_for20_end >> create_schedulehours_variable_for30 >> if_scheduledhours_more_thanorequal30

        if_scheduledhours_more_thanorequal30 >> rail.Label(
            'Yes') >> update_schedulehour_tomorethanequal30 >> get_scheduledhours_var_val_for30
        if_scheduledhours_more_thanorequal30 >> rail.Label(
            'No') >> if_scheduledhours_less_than30

        if_scheduledhours_less_than30 >> rail.Label(
            'Yes') >> update_schedulehour_tolessthan30 >> get_scheduledhours_var_val_for30
        if_scheduledhours_less_than30 >> rail.Label(
            'No') >> get_scheduledhours_var_val_for30

        get_scheduledhours_var_val_for30 >> get_timeoff_to_be_assigned_from_mapper_for30 >> foreach_timeofftype_for30

        foreach_timeofftype_for30 >> get_specific_timeoff_for30 >> add_to_eligibletimeoftypes_for30 >> foreach_timeofftype_for30_end

        foreach_timeofftype_for30 >> foreach_timeofftype_for30_end >> get_statictimeoff_with_scheduled_as_all >> foreach_timeofftype_scheduled_all

        foreach_timeofftype_scheduled_all >> get_specific_timeoff_scheduled_all \
            >> add_to_eligibletimeoftypes_scheduled_all >> foreach_timeofftype_scheduled_all_end

        foreach_timeofftype_scheduled_all >> foreach_timeofftype_scheduled_all_end >> get_eligibletimeofftypeslist >> if_eligibletimeoff_list_present

        if_eligibletimeoff_list_present >> rail.Label(
            'Yes') >> get_timeoffpolicyuri >> put_timeoffassignmentforuser >> foreach_eligibletimeofftypes
        if_eligibletimeoff_list_present >> rail.Label(
            'No') >> log_userimport_success

        foreach_eligibletimeofftypes >> if_timeoffuri_is_present

        if_timeoffuri_is_present >> rail.Label(
            'Yes') >> get_default_timeoffpolicyschedule_foruser >> if_timeofftype_is_seipto_a_or_caa
        if_timeoffuri_is_present >> rail.Label(
            'No') >> foreach_eligibletimeofftypes_end

        if_timeofftype_is_seipto_a_or_caa >> rail.Label(
            'Yes') >> get_existingthresholdvalue_and_scheduledhourvalue >> if_effectivedate_is_present_for_a_caa
        if_timeofftype_is_seipto_a_or_caa >> rail.Label(
            'No') >> if_effectivedate_is_present

        if_effectivedate_is_present_for_a_caa >> rail.Label(
            'Yes') >> putusertimeoffaccountpolicysetschedule_for_a_caa >> foreach_eligibletimeofftypes_end
        if_effectivedate_is_present_for_a_caa >> rail.Label(
            'No') >> foreach_eligibletimeofftypes_end

        if_effectivedate_is_present >> rail.Label(
            'Yes') >> putusertimeoffaccountpolicysetschedule >> foreach_eligibletimeofftypes_end
        if_effectivedate_is_present >> rail.Label(
            'No') >> foreach_eligibletimeofftypes_end

        foreach_eligibletimeofftypes >> foreach_eligibletimeofftypes_end >> get_final_timeoff_list >> puttimeoffassignment_foruser >> \
            if_default_timeoff_present

        if_default_timeoff_present >> rail.Label('Yes') >> get_default_timeoffuri >> imporsonate_andcreateinteractivesession_for_timeoff \
            >> get_authtoken_for_timeoff >> if_authtoken_for_timeoff_present
        if_default_timeoff_present >> rail.Label(
            'No') >> log_userimport_success

        if_authtoken_for_timeoff_present >> rail.Label(
            'Yes') >> update_default_timeoff >> log_userimport_success
        if_authtoken_for_timeoff_present >> rail.Label(
            'No') >> log_userimport_success

        log_userimport_success >> catch_and_log_error

    return dag


rail.for_each_instance(create_dag)
