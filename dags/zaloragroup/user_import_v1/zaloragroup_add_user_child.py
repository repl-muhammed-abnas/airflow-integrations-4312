from datetime import timedelta
from airflow.models import Variable
import rail
from zaloragroup.user_import_v1.utils import python_callable_method
from zaloragroup.user_import_v1.utils import request_payload

def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=f'zaloragroup_user_import_add_user_child_{config.instance}_v1',
        description=f'zaloragroup_user_import_add_user_child_{config.instance}_v1',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.add_user_child_dag_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config", extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_user'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_user',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_user = rail.RepliconServiceOperator(
            task_id = "create_user",
            endpoint = "/services/ImportService1.svc/PutUser3",
            data = request_payload.create_user_payload
        )

        unassign_time_off_types = rail.RepliconServiceOperator(
            task_id = "unassign_time_off_types",
            endpoint = "/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data = {
                    "userUri": "{{ result('create_user').uri }}",
                    "timeOffTypeUris": []
                    }
        )

        is_request_start_end_date_present = rail.IfOperator(
            task_id='is_request_start_end_date_present',
            test="{{ dag_run.conf.startdate | is_truthy or dag_run.conf.enddate | is_truthy }}",
            yes_task="if_request_startdate_present_and_enddate_absent",
            no_task="get_enabled_department",
        )

        if_request_startdate_present_and_enddate_absent = rail.IfOperator(
            task_id='if_request_startdate_present_and_enddate_absent',
            test="{{ dag_run.conf.startdate | is_truthy  and dag_run.conf.enddate | is_falsy }}",
            yes_task="update_employment_start_date_range",
            no_task="if_request_start_and_end_date_present",
        )

        update_employment_start_date_range = rail.RepliconServiceOperator(
            task_id='update_employment_start_date_range',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=request_payload.update_emp_start_date
        )

        if_request_start_and_end_date_present = rail.IfOperator(
            task_id='if_request_start_and_end_date_present',
            test="{{ dag_run.conf.startdate | is_truthy and dag_run.conf.enddate | is_truthy }}",
            yes_task="update_employment_daterange",
            no_task="get_enabled_department",
        )

        update_employment_daterange = rail.RepliconServiceOperator(
            task_id='update_employment_daterange',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=request_payload.update_emp_date
        )

        get_enabled_department = rail.RepliconServiceOperator(
            task_id='get_enabled_department',
            endpoint="/services/DepartmentService1.svc/GetEnabledDepartmentDetails",
        )

        get_requested_dept_uri = rail.PythonOperator(
            task_id='get_requested_dept_uri',
            python_callable=python_callable_method.get_dept_uri_data
        )

        if_department_present = rail.IfOperator(
            task_id='if_department_present',
            test="{{ result('get_requested_dept_uri') | is_truthy }}",
            yes_task="update_department_for_user",
            no_task="if_request_enabled_equals_to_yes",
        )

        update_department_for_user = rail.RepliconServiceOperator(
            task_id='update_department_for_user',
            endpoint="/services/DepartmentService1.svc/UpdateDepartmentForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "departmentUri": "{{ result('get_requested_dept_uri') }}"
            }
        )

        if_request_enabled_equals_to_yes = rail.IfOperator(
            task_id='if_request_enabled_equals_to_yes',
            test="{{ dag_run.conf.enabled.lower() == 'yes' }}",
            yes_task="enable_login",
            no_task="if_request_enabled_equals_to_no",
        )

        enable_login = rail.RepliconServiceOperator(
            task_id='enable_login',
            endpoint="/services/SecurityService1.svc/EnableLogin",
            data={
                "userUri": "{{ result('create_user').uri }}"
            }
        )

        if_request_enabled_equals_to_no = rail.IfOperator(
            task_id='if_request_enabled_equals_to_no',
            test="{{ dag_run.conf.enabled.lower() == 'no' }}",
            yes_task="disable_login",
            no_task="get_public_licensed_product_data",
        )

        disable_login = rail.RepliconServiceOperator(
            task_id='disable_login',
            endpoint="/services/SecurityService1.svc/DisableLogin",
            data={
                "userUri": "{{ result('create_user').uri }}"
            }
        )

        get_public_licensed_product_data = rail.RepliconServiceOperator(
            task_id='get_public_licensed_product_data',
            endpoint="/services/AccountManagementService1.svc/GetAllPublicLicensedProducts",
            data_handler= lambda response : {
                "timeoffplus": rail.find_first_by_attr_and_get_attr(response , 'displayText', 'TimeOff Plus', 'uri', ''),
                "timecostplus": rail.find_first_by_attr_and_get_attr(response , 'displayText', 'TimeCost Plus', 'uri', '')
            }
        )

        if_licensed_product_present = rail.IfOperator(
            task_id='if_licensed_product_present',
            test="{{ result('get_public_licensed_product_data').timeoffplus | is_truthy and  \
                result('get_public_licensed_product_data').timecostplus | is_truthy}}",
            yes_task="add_licensed_product_to_user",
            no_task="get_permission_sets",
        )

        add_licensed_product_to_user = rail.RepliconServiceOperator(
            task_id='add_licensed_product_to_user',
            endpoint="/services/AccountManagementService1.svc/PutProductAssignmentsForUser",
            data ={
                "userUri": "{{ result('create_user').uri }}",
                "productUris": [
                    "{{ result('get_public_licensed_product_data').timeoffplus }}",\
                    "{{ result('get_public_licensed_product_data').timecostplus }}"
                ]
                }
        )

        get_permission_sets = rail.RepliconServiceOperator(
            task_id='get_permission_sets',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data_handler= lambda response : {
                "project_resource": rail.find_first_by_attr_and_get_attr(response , 'displayText', 'Project Resource', 'uri', '')
            }
        )

        if_permission_set_present = rail.IfOperator(
            task_id='if_permission_set_present',
            test="{{ result('get_permission_sets').project_resource | is_truthy }}",
            yes_task="add_permission_set_to_user",
            no_task="get_policy_sets",
        )

        add_permission_set_to_user = rail.RepliconServiceOperator(
            task_id='add_permission_set_to_user',
            endpoint="/services/PermissionSetService1.svc/PutPermissionSetAssignmentsForUser",
            data ={
                "userUri": "{{ result('create_user').uri }}",
                "permissionSetUris": [
                    "{{ result('get_permission_sets').project_resource }}"
                ]
                }
        )

        get_policy_sets = rail.RepliconServiceOperator(
            task_id='get_policy_sets',
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
            data_handler= lambda response : {
                "timeoff_template": rail.find_first_by_attr_and_get_attr(response , 'displayText', 'Time Off', 'uri', ''),
                "zgrp_tm_template": rail.find_first_by_attr_and_get_attr(response , 'displayText', 'Zalora Group Timesheet Template', 'uri', '')
            }
        )

        if_policy_set_present = rail.IfOperator(
            task_id='if_policy_set_present',
            test="{{ result('get_policy_sets').timeoff_template | is_truthy or \
                result('get_permission_sets').zgrp_tm_template | is_truthy }}",
            yes_task="add_policy_set_to_user",
            no_task="get_timesheet_approval_path",
        )

        add_policy_set_to_user = rail.RepliconServiceOperator(
            task_id='add_policy_set_to_user',
            endpoint="/services/PolicySetService1.svc/PutPolicySetAssignmentsForUser",
            data ={
                "userUri": "{{ result('create_user').uri }}",
                "policySetUris": [
                    "{{ result('get_policy_sets').timeoff_template }}",
                    "{{ result('get_policy_sets').zgrp_tm_template }}"
                ]
                }
        )

        get_timesheet_approval_path = rail.RepliconServiceOperator(
            task_id='get_timesheet_approval_path',
            endpoint="/services/TimesheetApprovalService1.svc/GetAllApprovalPaths",
            data_handler= lambda response : {
                "supervisor_path": rail.find_first_by_attr_and_get_attr(response , 'displayText', 'Supervisor', 'uri', '')
            }
        )

        if_timesheet_approval_present = rail.IfOperator(
            task_id='if_timesheet_approval_present',
            test="{{ result('get_timesheet_approval_path').supervisor_path | is_truthy }}",
            yes_task="add_timesheet_approval_to_user",
            no_task="get_timeoff_approval_path",
        )

        add_timesheet_approval_to_user = rail.RepliconServiceOperator(
            task_id='add_timesheet_approval_to_user',
            endpoint="/services/TimesheetApprovalService1.svc/UpdateApprovalPathForUser",
            data ={
                "userUri": "{{ result('create_user').uri }}",
                "approvalPathUri": "{{ result('get_timesheet_approval_path').supervisor_path }}"
                }
        )

        get_timeoff_approval_path = rail.RepliconServiceOperator(
            task_id='get_timeoff_approval_path',
            endpoint="/services/TimeOffApprovalService1.svc/GetAllApprovalPaths",
            data_handler= lambda response : {
                "supervisor_path": rail.find_first_by_attr_and_get_attr(response , 'displayText', 'Supervisor', 'uri', '')
            }
        )

        if_timeoff_approval_present = rail.IfOperator(
            task_id='if_timeoff_approval_present',
            test="{{ result('get_timeoff_approval_path').supervisor_path | is_truthy }}",
            yes_task="add_timeoff_approval_to_user",
            no_task="get_timeoff_type",
        )

        add_timeoff_approval_to_user = rail.RepliconServiceOperator(
            task_id='add_timeoff_approval_to_user',
            endpoint="/services/TimeOffApprovalService1.svc/UpdateApprovalPathForUser",
            data ={
                "userUri": "{{ result('create_user').uri }}",
                "approvalPathUri": "{{ result('get_timeoff_approval_path').supervisor_path }}"
                }
        )

        get_timeoff_type = rail.RepliconServiceOperator(
            task_id='get_timeoff_type',
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
            data_handler= lambda response : {
                "public_holiday": rail.find_first_by_attr_and_get_attr(response , 'displayText', 'Public Holiday', 'uri', '')
            }
        )

        if_timeoff_type_present = rail.IfOperator(
            task_id='if_timeoff_type_present',
            test="{{ result('get_timeoff_type').public_holiday | is_truthy }}",
            yes_task="add_timeoff_type_to_user",
            no_task="update_timesheet_periodtype_for_user",
        )

        add_timeoff_type_to_user = rail.RepliconServiceOperator(
            task_id='add_timeoff_type_to_user',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data ={
                "userUri": "{{ result('create_user').uri }}",
                "timeOffTypeUris": ["{{ result('get_timeoff_type').public_holiday }}" ]
                }
        )

        update_timesheet_periodtype_for_user = rail.RepliconServiceOperator(
            task_id='update_timesheet_periodtype_for_user',
            endpoint="/services/TimesheetPeriodService1.svc/UpdateTimesheetPeriodTypeForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "timesheetPeriodTypeUri": "urn:replicon:timesheet-period-type:system"
            }
        )

        update_timezone_for_user = rail.RepliconServiceOperator(
            task_id='update_timezone_for_user',
            endpoint="/services/InternationalizationService1.svc/UpdateTimeZoneForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "timeZoneUri":  "urn:replicon:time-zone:asia-singapore"
            }
        )

        update_workweek_startday_for_user = rail.RepliconServiceOperator(
            task_id='update_workweek_startday_for_user',
            endpoint="/services/UserService1.svc/UpdateWorkWeekStartDayForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "dayOfWeekUri": "urn:replicon:day-of-week:monday"
            }
        )

        update_schedule_policyschedule_for_user = rail.RepliconServiceOperator(
            task_id='update_schedule_policyschedule_for_user',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data=request_payload.schedule_policyschedule
        )



        if_request_supervisor_present = rail.IfOperator(
            task_id='if_request_supervisor_present',
            test="{{ dag_run.conf.initialsupervisorloginname | is_truthy }}",
            yes_task="if_request_loginname_not_equals_request_initialsupervisorloginname",
            no_task="if_request_holidaycalendar_present",
        )

        if_request_loginname_not_equals_request_initialsupervisorloginname = rail.IfOperator(
            task_id='if_request_loginname_not_equals_request_initialsupervisorloginname',
            test="{{ dag_run.conf.loginname != dag_run.conf.initialsupervisorloginname }}",
            yes_task="check_if_supervisor_available",
            no_task="user_import_log_for_same_login_and_supervisor",
        )

        check_if_supervisor_available = rail.RepliconServiceOperator(
            task_id='check_if_supervisor_available',
            endpoint="/services/UserListService1.svc/GetData",
            data = request_payload.get_supervisordetails,
            data_handler = python_callable_method.get_supervisor_uri_by_loginname
        )

        is_supervisor_uri_present = rail.IfOperator(
            task_id='is_supervisor_uri_present',
            test="{{ result('check_if_supervisor_available') | is_truthy }}",
            yes_task='update_supervisor_for_user',
            no_task='user_supervisor_mapper'
        )

        update_supervisor_for_user = rail.RepliconServiceOperator(
            task_id='update_supervisor_for_user',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=request_payload.update_supervisor
        )

        user_supervisor_mapper = rail.WriteLogOperator(
            task_id='user_supervisor_mapper',
            log = "{{ dag_run.conf.supervisor_mapper }}",
            message="na",
            severity="Error",
            properties={
                "dag_id": "{{ dag_run_ecid() }}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "supervisorid": "{{ dag_run.conf.initialsupervisorloginname }}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "status": "Error"
            }
        )

        user_import_log_for_same_login_and_supervisor = rail.WriteLogOperator(
            task_id='user_import_log_for_same_login_and_supervisor',
            log = "{{ dag_run.conf.logger }}",
            message="na",
            severity="Error",
            properties={
                "login_name": "{{ dag_run.conf.loginname }}",
                "status": "Error",
                "failure_reason": "Supervisor not updated from user \"{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}\" as login name for user and supervisor is same in the input file"
            }
        )

        if_request_holidaycalendar_present = rail.IfOperator(
            task_id='if_request_holidaycalendar_present',
            test="{{ dag_run.conf.holidaycalendar | is_truthy }}",
            yes_task="get_holiday_calendar_for_user",
            no_task="get_custom_field_group_user_uri",
        )

        get_holiday_calendar_for_user = rail.RepliconServiceOperator(
            task_id='get_holiday_calendar_for_user',
            endpoint="/services/HolidayCalendarService1.svc/GetHolidayCalendarForUser",
            data={
                "userUri": "{{ result('create_user').uri }}"
            }
        )

        if_holiday_calender_mismatch = rail.IfOperator(
            task_id='if_holiday_calender_mismatch',
            test="{{ result('get_holiday_calendar_for_user') | is_falsy or \
                result('get_holiday_calendar_for_user')['displayText'] != dag_run.conf.holidaycalendar }}",
            yes_task="get_all_holiday_calendar",
            no_task="get_custom_field_group_user_uri",
        )

        get_all_holiday_calendar = rail.RepliconServiceOperator(
            task_id='get_all_holiday_calendar',
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars"
        )

        get_holiday_calendaruri_from_all_holiday_calender = rail.PythonOperator(
            task_id='get_holiday_calendaruri_from_all_holiday_calender',
            python_callable = python_callable_method.get_holiday_calender_uri
        )

        if_holiday_calendar_uri_present = rail.IfOperator(
            task_id='if_holiday_calendar_uri_present',
            test="{{ result('get_holiday_calendaruri_from_all_holiday_calender') | is_truthy }}",
            yes_task="update_holiday_calendar_for_user",
            no_task="get_custom_field_group_user_uri",
        )

        update_holiday_calendar_for_user = rail.RepliconServiceOperator(
            task_id='update_holiday_calendar_for_user',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "holidayCalendarUri": "{{ result('get_holiday_calendaruri_from_all_holiday_calender') }}"
            }
        )

        get_custom_field_group_user_uri = rail.RepliconServiceOperator(
            task_id='get_custom_field_group_user_uri',
            endpoint="/services/CustomFieldService1.svc/GetCustomFieldGroups",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response, 'displayText', 'User' , 'uri', '')
        )

        if_custom_field_user_uri_present = rail.IfOperator(
            task_id='if_custom_field_user_uri_present',
            test="{{ result('get_custom_field_group_user_uri') | is_truthy }}",
            yes_task="get_all_custom_fields_for_required_group",
            no_task="catch_and_log_error",
        )

        get_all_custom_fields_for_required_group = rail.RepliconServiceOperator(
            task_id='get_all_custom_fields_for_required_group',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "{{ result('get_custom_field_group_user_uri') }}"
            },
            data_handler=lambda response:
            {
                "subdepartment_udfgroup_uri" : rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Sub Department', 'uri', ''),
                "jobfamily_udfgroup_uri" : rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Job Family', 'uri', ''),
                "jobname_udfgroup_uri" : rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Job Name', 'uri', ''),
                "legalentity_udfgroup_uri" : rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Legal Entity', 'uri', ''),
                "gradename_udfgroup_uri" : rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Grade', 'uri', '')
            }
        )

        if_subdepartment_present = rail.IfOperator(
            task_id='if_subdepartment_present',
            test="{{ dag_run.conf.subdepartment | is_truthy }}",
            yes_task="is_subdepartment_udf_grp_uri_present",
            no_task="if_jobfamily_present",
        )

        is_subdepartment_udf_grp_uri_present = rail.IfOperator(
            task_id='is_subdepartment_udf_grp_uri_present',
            test="{{ result('get_all_custom_fields_for_required_group').subdepartment_udfgroup_uri | is_truthy }}",
            yes_task="update_subdepartment",
            no_task="if_jobfamily_present",
        )

        update_subdepartment = rail.RepliconServiceOperator(
            task_id='update_subdepartment',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_all_custom_fields_for_required_group').subdepartment_udfgroup_uri }}",
                "value": "{{ dag_run.conf.subdepartment }}"
            }
        )

        if_jobfamily_present = rail.IfOperator(
            task_id='if_jobfamily_present',
            test="{{ dag_run.conf.jobfamily | is_truthy }}",
            yes_task="is_jobfamily_udf_grp_uri_present",
            no_task="if_jobname_present",
        )

        is_jobfamily_udf_grp_uri_present = rail.IfOperator(
            task_id='is_jobfamily_udf_grp_uri_present',
            test="{{ result('get_all_custom_fields_for_required_group').jobfamily_udfgroup_uri | is_truthy }}",
            yes_task="update_jobfamily",
            no_task="if_jobname_present",
        )

        update_jobfamily = rail.RepliconServiceOperator(
            task_id='update_jobfamily',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_all_custom_fields_for_required_group').jobfamily_udfgroup_uri }}",
                "value": "{{ dag_run.conf.jobfamily }}"
            }
        )

        if_jobname_present = rail.IfOperator(
            task_id='if_jobname_present',
            test="{{ dag_run.conf.jobname | is_truthy }}",
            yes_task="is_jobname_udf_grp_uri_present",
            no_task="if_legalentity_present",
        )

        is_jobname_udf_grp_uri_present = rail.IfOperator(
            task_id='is_jobname_udf_grp_uri_present',
            test="{{ result('get_all_custom_fields_for_required_group').jobname_udfgroup_uri | is_truthy }}",
            yes_task="update_jobname",
            no_task="if_legalentity_present",
        )

        update_jobname = rail.RepliconServiceOperator(
            task_id='update_jobname',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_all_custom_fields_for_required_group').jobname_udfgroup_uri }}",
                "value": "{{ dag_run.conf.jobname }}"
            }
        )

        if_legalentity_present = rail.IfOperator(
            task_id='if_legalentity_present',
            test="{{ dag_run.conf.legalentity | is_truthy }}",
            yes_task="is_legalentity_udf_grp_uri_present",
            no_task="if_gradename_present",
        )

        is_legalentity_udf_grp_uri_present = rail.IfOperator(
            task_id='is_legalentity_udf_grp_uri_present',
            test="{{ result('get_all_custom_fields_for_required_group').legalentity_udfgroup_uri | is_truthy }}",
            yes_task="update_legalentity",
            no_task="if_gradename_present",
        )

        update_legalentity = rail.RepliconServiceOperator(
            task_id='update_legalentity',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_all_custom_fields_for_required_group').legalentity_udfgroup_uri }}",
                "value": "{{ dag_run.conf.legalentity }}"
            }
        )

        if_gradename_present = rail.IfOperator(
            task_id='if_gradename_present',
            test="{{ dag_run.conf.gradename | is_truthy }}",
            yes_task="is_gradename_udf_grp_uri_present",
            no_task="user_import_log_success",
        )

        is_gradename_udf_grp_uri_present = rail.IfOperator(
            task_id='is_gradename_udf_grp_uri_present',
            test="{{ result('get_all_custom_fields_for_required_group').gradename_udfgroup_uri | is_truthy }}",
            yes_task="update_gradename",
            no_task="user_import_log_success",
        )

        update_gradename = rail.RepliconServiceOperator(
            task_id='update_gradename',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_all_custom_fields_for_required_group').gradename_udfgroup_uri }}",
                "value": "{{ dag_run.conf.gradename }}"
            }
        )

        user_import_log_success = rail.WriteLogOperator(
            task_id='user_import_log_success',
            log = "{{ dag_run.conf.logger }}",
            message="na",
            severity="Success",
            properties={
                "login_name": "{{ dag_run.conf.loginname }}",
                "status": "Success",
                "failure_reason": ""
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            log = "{{ dag_run.conf.logger }}",
            severity="Error",
            trigger_rule="one_failed",
            message='{{ get_error_message() }}',
            properties={
                "login_name": "{{dag_run.conf.loginname}}",
                "status": "Error",
                "failure_reason": '{{ get_error_message() }}'
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> create_user

        create_user >> unassign_time_off_types >> is_request_start_end_date_present
        is_request_start_end_date_present >> rail.Label(
            'Yes') >> if_request_startdate_present_and_enddate_absent
        is_request_start_end_date_present >> rail.Label(
            'No') >> get_enabled_department
        if_request_startdate_present_and_enddate_absent >> rail.Label(
            'Yes') >> update_employment_start_date_range >> get_enabled_department
        if_request_startdate_present_and_enddate_absent >> rail.Label(
            'No') >> if_request_start_and_end_date_present
        if_request_start_and_end_date_present >> rail.Label(
            'Yes') >> update_employment_daterange >> get_enabled_department
        if_request_start_and_end_date_present >> rail.Label(
            'No') >> get_enabled_department
        get_enabled_department >> get_requested_dept_uri >> if_department_present
        if_department_present >> rail.Label(
            'Yes') >> update_department_for_user >> if_request_enabled_equals_to_yes
        if_department_present >> rail.Label(
            'No') >> if_request_enabled_equals_to_yes
        if_request_enabled_equals_to_yes >> rail.Label(
            'Yes') >> enable_login >> get_public_licensed_product_data
        if_request_enabled_equals_to_yes >> rail.Label(
            'No') >> if_request_enabled_equals_to_no
        if_request_enabled_equals_to_no >> rail.Label(
            'Yes') >> disable_login >> get_public_licensed_product_data
        if_request_enabled_equals_to_no >> rail.Label(
            'No') >> get_public_licensed_product_data
        get_public_licensed_product_data >> if_licensed_product_present
        if_licensed_product_present >> rail.Label(
            'Yes') >> add_licensed_product_to_user >> get_permission_sets
        if_licensed_product_present >> rail.Label(
            'No') >> get_permission_sets
        get_permission_sets >> if_permission_set_present
        if_permission_set_present >> rail.Label(
            'Yes') >> add_permission_set_to_user >> get_policy_sets
        if_permission_set_present >> rail.Label(
            'No') >> get_policy_sets
        get_policy_sets >> if_policy_set_present
        if_policy_set_present >> rail.Label(
            'Yes') >> add_policy_set_to_user >> get_timesheet_approval_path
        if_policy_set_present >> rail.Label(
            'No') >> get_timesheet_approval_path
        get_timesheet_approval_path >> if_timesheet_approval_present
        if_timesheet_approval_present >> rail.Label(
            'Yes') >> add_timesheet_approval_to_user >> get_timeoff_approval_path
        if_timesheet_approval_present >> rail.Label(
            'No') >> get_timeoff_approval_path
        get_timeoff_approval_path >> if_timeoff_approval_present
        if_timeoff_approval_present >> rail.Label(
            'Yes') >> add_timeoff_approval_to_user >> get_timeoff_type
        if_timeoff_approval_present >> rail.Label(
            'No') >> get_timeoff_type
        get_timeoff_type >> if_timeoff_type_present
        if_timeoff_type_present >> rail.Label(
            'Yes') >> add_timeoff_type_to_user >> update_timesheet_periodtype_for_user
        if_timeoff_type_present >> rail.Label(
            'No') >> update_timesheet_periodtype_for_user
        update_timesheet_periodtype_for_user >> update_timezone_for_user >> update_workweek_startday_for_user >> \
        update_schedule_policyschedule_for_user >> if_request_supervisor_present
        if_request_supervisor_present >> rail.Label(
            'Yes') >> if_request_loginname_not_equals_request_initialsupervisorloginname
        if_request_supervisor_present >> rail.Label(
            'No') >> if_request_holidaycalendar_present
        if_request_loginname_not_equals_request_initialsupervisorloginname >> rail.Label(
            'Yes') >> check_if_supervisor_available >> is_supervisor_uri_present
        if_request_loginname_not_equals_request_initialsupervisorloginname >> rail.Label(
            'No') >> user_import_log_for_same_login_and_supervisor >> if_request_holidaycalendar_present
        is_supervisor_uri_present >> rail.Label(
            'Yes') >> update_supervisor_for_user >> if_request_holidaycalendar_present
        is_supervisor_uri_present >> rail.Label(
            'No') >> user_supervisor_mapper >> if_request_holidaycalendar_present
        if_request_holidaycalendar_present >> rail.Label(
            'Yes') >> get_holiday_calendar_for_user >> if_holiday_calender_mismatch
        if_request_holidaycalendar_present >> rail.Label(
            'No') >> get_custom_field_group_user_uri
        if_holiday_calender_mismatch >> rail.Label(
            'Yes') >> get_all_holiday_calendar >> get_holiday_calendaruri_from_all_holiday_calender >> if_holiday_calendar_uri_present
        if_holiday_calender_mismatch >> rail.Label(
            'No') >> get_custom_field_group_user_uri
        if_holiday_calendar_uri_present >> rail.Label(
            'Yes') >> update_holiday_calendar_for_user >> get_custom_field_group_user_uri
        if_holiday_calendar_uri_present >> rail.Label(
            'No') >> get_custom_field_group_user_uri
        get_custom_field_group_user_uri >> if_custom_field_user_uri_present
        if_custom_field_user_uri_present >> rail.Label(
            'Yes') >> get_all_custom_fields_for_required_group >> if_subdepartment_present
        if_custom_field_user_uri_present >> rail.Label(
            'No') >> catch_and_log_error
        if_subdepartment_present >> rail.Label(
            'Yes') >> is_subdepartment_udf_grp_uri_present
        if_subdepartment_present >> rail.Label(
            'No') >> if_jobfamily_present
        is_subdepartment_udf_grp_uri_present >> rail.Label(
            'Yes') >> update_subdepartment >> if_jobfamily_present
        is_subdepartment_udf_grp_uri_present >> rail.Label(
            'No') >> if_jobfamily_present
        if_jobfamily_present >> rail.Label(
            'Yes') >> is_jobfamily_udf_grp_uri_present
        if_jobfamily_present >> rail.Label(
            'No') >> if_jobname_present
        is_jobfamily_udf_grp_uri_present >> rail.Label(
            'Yes') >> update_jobfamily >> if_jobname_present
        is_jobfamily_udf_grp_uri_present >> rail.Label(
            'No') >> if_jobname_present
        if_jobname_present >> rail.Label(
            'Yes') >> is_jobname_udf_grp_uri_present
        if_jobname_present >> rail.Label(
            'No') >> if_legalentity_present
        is_jobname_udf_grp_uri_present >> rail.Label(
            'Yes') >> update_jobname >> if_legalentity_present
        is_jobname_udf_grp_uri_present >> rail.Label(
            'No') >> if_legalentity_present
        if_legalentity_present >> rail.Label(
            'Yes') >> is_legalentity_udf_grp_uri_present
        if_legalentity_present >> rail.Label(
            'No') >> if_gradename_present
        is_legalentity_udf_grp_uri_present >> rail.Label(
            'Yes') >> update_legalentity >> if_gradename_present
        is_legalentity_udf_grp_uri_present >> rail.Label(
            'No') >> if_gradename_present
        if_gradename_present >> rail.Label(
            'Yes') >> is_gradename_udf_grp_uri_present
        if_gradename_present >> rail.Label(
            'No') >> user_import_log_success
        is_gradename_udf_grp_uri_present >> rail.Label(
            'Yes') >> update_gradename >> user_import_log_success
        is_gradename_udf_grp_uri_present >> rail.Label(
            'No') >> user_import_log_success
        user_import_log_success >> catch_and_log_error
        catch_and_log_error >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
