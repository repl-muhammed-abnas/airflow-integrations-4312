
from datetime import timedelta
from airflow.models import Variable
import rail
from zaloragroup.user_import_v1.utils import python_callable_method
from zaloragroup.user_import_v1.utils import request_payload

def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=f'zaloragroup_user_import_update_user_child_{config.instance}_v1',
        description=f'zaloragroup_user_import_update_user_child_{config.instance}_v1',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.update_user_child_dag_active_runs,
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
            no_task='get_user_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_user_details',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id='get_user_details',
            endpoint="/services/UserService1.svc/GetUserDetails",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        if_request_enabled_equals_to_yes = rail.IfOperator(
            task_id='if_request_enabled_equals_to_yes',
            test="{{ dag_run.conf.enabled.lower() == 'yes'  and result('get_user_details').isEnabled | is_falsy }}",
            yes_task="enable_login",
            no_task="if_request_enabled_equals_to_no",
        )

        enable_login = rail.RepliconServiceOperator(
            task_id='enable_login',
            endpoint="/services/SecurityService1.svc/EnableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        if_request_enabled_equals_to_no = rail.IfOperator(
            task_id='if_request_enabled_equals_to_no',
            test="{{ dag_run.conf.enabled.lower() == 'no'  and result('get_user_details')['isEnabled'] | is_truthy }}",
            yes_task="disable_login",
            no_task="if_request_employeeid_mismatch",
        )

        disable_login = rail.RepliconServiceOperator(
            task_id='disable_login',
            endpoint="/services/SecurityService1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        if_request_employeeid_mismatch = rail.IfOperator(
            task_id='if_request_employeeid_mismatch',
            test="{{ dag_run.conf.employeeid | is_truthy and dag_run.conf.employeeid != result('get_user_details')['employeeId']}}",
            yes_task="update_employee_id",
            no_task="if_request_firstname_mismatch",
        )

        update_employee_id = rail.RepliconServiceOperator(
            task_id='update_employee_id',
            endpoint="/services/UserService1.svc/UpdateEmployeeId",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "employeeId": "{{ dag_run.conf.employeeid }}"
            }
        )

        if_request_firstname_mismatch = rail.IfOperator(
            task_id='if_request_firstname_mismatch',
            test="{{ dag_run.conf.firstname | is_truthy and result('get_user_details')['firstName'] != dag_run.conf.firstname }}",
            yes_task="update_first_name",
            no_task="if_request_lastname_mismatch",
        )

        update_first_name = rail.RepliconServiceOperator(
            task_id='update_first_name',
            endpoint="/services/UserService1.svc/UpdateFirstName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "firstname": "{{ dag_run.conf.firstname }}"
            }
        )

        if_request_lastname_mismatch = rail.IfOperator(
            task_id='if_request_lastname_mismatch',
            test="{{ dag_run.conf.lastname | is_truthy and dag_run.conf.lastname != result('get_user_details')['lastName'] }}",
            yes_task="update_last_name",
            no_task="if_request_emailaddress_mismatch",
        )

        update_last_name = rail.RepliconServiceOperator(
            task_id='update_last_name',
            endpoint="/services/UserService1.svc/UpdateLastName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "lastname": "{{ dag_run.conf.lastname }}"
            }
        )

        if_request_emailaddress_mismatch = rail.IfOperator(
            task_id='if_request_emailaddress_mismatch',
            test="{{ dag_run.conf.emailaddress | is_truthy and dag_run.conf.emailaddress != result('get_user_details')['emailAddress'] }}",
            yes_task="update_email",
            no_task="if_request_employeetype_present",
        )

        update_email = rail.RepliconServiceOperator(
            task_id='update_email',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "email": "{{ dag_run.conf.emailaddress }}"
            }
        )

        if_request_employeetype_present = rail.IfOperator(
            task_id='if_request_employeetype_present',
            test="{{ dag_run.conf.employeetype | is_truthy }}",
            yes_task="get_employee_type_for_user",
            no_task="if_request_department_present",
        )

        get_employee_type_for_user = rail.RepliconServiceOperator(
            task_id='get_employee_type_for_user',
            endpoint="/services/EmployeeTypeService1.svc/GetEmployeeTypeForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        if_request_employee_type_mismatch = rail.IfOperator(
            task_id='if_request_employee_type_mismatch',
            test="{{ result('get_employee_type_for_user')['displayText'] != dag_run.conf.employeetype }}",
            yes_task="get_all_employee_type_details",
            no_task="if_request_department_present",
        )

        get_all_employee_type_details = rail.RepliconServiceOperator(
            task_id='get_all_employee_type_details',
            endpoint="/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails"
        )

        get_required_employee_type_uri = rail.PythonOperator(
            task_id='get_required_employee_type_uri',
            python_callable=python_callable_method.get_required_employee_type_uri
        )

        if_employeetype_uri_present = rail.IfOperator(
            task_id='if_employeetype_uri_present',
            test="{{ result('get_required_employee_type_uri') | is_truthy }}",
            yes_task="update_employee_type_for_user",
            no_task="if_request_department_present",
        )

        update_employee_type_for_user = rail.RepliconServiceOperator(
            task_id='update_employee_type_for_user',
            endpoint="/services/EmployeeTypeService1.svc/UpdateEmployeeTypeForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "employeeTypeUri": "{{ result('get_required_employee_type_uri') }}"
            }
        )

        if_request_department_present = rail.IfOperator(
            task_id='if_request_department_present',
            test="{{ dag_run.conf.department | is_truthy }}",
            yes_task="get_enabled_department",
            no_task="if_request_supervisor_mismatch",
        )

        get_enabled_department = rail.RepliconServiceOperator(
            task_id='get_enabled_department',
            endpoint="/services/DepartmentService1.svc/GetEnabledDepartmentDetails",
        )

        get_requested_dept_uri = rail.PythonOperator(
            task_id='get_requested_dept_uri',
            python_callable=python_callable_method.get_dept_uri_data
        )

        if_department_mismatch = rail.IfOperator(
            task_id='if_department_mismatch',
            test="{{ result('get_requested_dept_uri') | is_truthy  and result('get_requested_dept_uri') != result('get_user_details')['department']['uri'] }}",
            yes_task="update_department_for_user",
            no_task="if_request_supervisor_mismatch",
        )

        update_department_for_user = rail.RepliconServiceOperator(
            task_id='update_department_for_user',
            endpoint="/services/DepartmentService1.svc/UpdateDepartmentForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "departmentUri": "{{ result('get_requested_dept_uri') }}"
            }
        )

        if_request_supervisor_mismatch = rail.IfOperator(
            task_id='if_request_supervisor_mismatch',
            test="{{ dag_run.conf.initialsupervisorloginname | is_truthy  and \
                ( result('get_user_details')['supervisor'] | is_falsy or \
                    dag_run.conf.initialsupervisorloginname != result('get_user_details')['supervisor']['user']['loginName'] ) }}",
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
            no_task="get_emp_daterange_from_profile",
        )

        get_holiday_calendar_for_user = rail.RepliconServiceOperator(
            task_id='get_holiday_calendar_for_user',
            endpoint="/services/HolidayCalendarService1.svc/GetHolidayCalendarForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        if_holiday_calender_mismatch = rail.IfOperator(
            task_id='if_holiday_calender_mismatch',
            test="{{ result('get_holiday_calendar_for_user') | is_falsy or \
                result('get_holiday_calendar_for_user')['displayText'] != dag_run.conf.holidaycalendar }}",
            yes_task="get_all_holiday_calendar",
            no_task="get_emp_daterange_from_profile",
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
            no_task="get_emp_daterange_from_profile",
        )

        update_holiday_calendar_for_user = rail.RepliconServiceOperator(
            task_id='update_holiday_calendar_for_user',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "holidayCalendarUri": "{{ result('get_holiday_calendaruri_from_all_holiday_calender') }}"
            }
        )

        get_emp_daterange_from_profile = rail.PythonOperator(
            task_id = "get_emp_daterange_from_profile",
            python_callable = python_callable_method.get_daterange_from_profile
        )

        if_request_startdate_present_and_enddate_absent = rail.IfOperator(
            task_id='if_request_startdate_present_and_enddate_absent',
            test="{{ dag_run.conf.startdate | is_truthy  and dag_run.conf.enddate | is_falsy }}",
            yes_task="check_request_startdate_notequals_profile_startdate",
            no_task="if_request_enddate_present",
        )

        check_request_startdate_notequals_profile_startdate = rail.IfOperator(
            task_id='check_request_startdate_notequals_profile_startdate',
            test="{{ result('get_emp_daterange_from_profile').start_date != dag_run.conf.startdate }}",
            yes_task="update_employment_start_date_range",
            no_task="if_request_enddate_present",
        )

        update_employment_start_date_range = rail.RepliconServiceOperator(
            task_id='update_employment_start_date_range',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=request_payload.update_emp_start_date
        )

        if_request_enddate_present = rail.IfOperator(
            task_id='if_request_enddate_present',
            test="{{ dag_run.conf.enddate | is_truthy }}",
            yes_task="check_request_enddate_notequals_profile_enddate",
            no_task="get_custom_field_group_user_uri",
        )

        check_request_enddate_notequals_profile_enddate = rail.IfOperator(
            task_id='check_request_enddate_notequals_profile_enddate',
            test="{{ result('get_emp_daterange_from_profile').end_date != dag_run.conf.enddate }}",
            yes_task="is_request_start_end_date_present",
            no_task="get_custom_field_group_user_uri",
        )

        is_request_start_end_date_present = rail.IfOperator(
            task_id='is_request_start_end_date_present',
            test="{{ dag_run.conf.startdate | is_truthy and dag_run.conf.enddate | is_truthy }}",
            yes_task="update_employment_daterange",
            no_task="update_employment_end_date_range",
        )

        update_employment_daterange = rail.RepliconServiceOperator(
            task_id='update_employment_daterange',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=request_payload.update_emp_date
        )

        update_employment_end_date_range = rail.RepliconServiceOperator(
            task_id='update_employment_end_date_range',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=request_payload.update_emp_end_date
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
                "objectUri": "{{ dag_run.conf.useruri }}",
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
                "objectUri": "{{ dag_run.conf.useruri }}",
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
                "objectUri": "{{ dag_run.conf.useruri }}",
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
                "objectUri": "{{ dag_run.conf.useruri }}",
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
                "objectUri": "{{ dag_run.conf.useruri }}",
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
        can_run_batch_task >> rail.Label('No') >> get_user_details
        get_user_details >> if_request_enabled_equals_to_yes
        if_request_enabled_equals_to_yes >> rail.Label(
            'Yes') >> enable_login >> if_request_employeeid_mismatch
        if_request_enabled_equals_to_yes >> rail.Label(
            'No') >> if_request_enabled_equals_to_no
        if_request_enabled_equals_to_no >> rail.Label(
            'Yes') >> disable_login >> if_request_employeeid_mismatch
        if_request_enabled_equals_to_no >> rail.Label(
            'No') >> if_request_employeeid_mismatch
        if_request_employeeid_mismatch >> rail.Label(
            'Yes') >> update_employee_id >> if_request_firstname_mismatch
        if_request_employeeid_mismatch >> rail.Label(
            'No') >> if_request_firstname_mismatch
        if_request_firstname_mismatch >> rail.Label(
            'Yes') >> update_first_name >> if_request_lastname_mismatch
        if_request_firstname_mismatch >> rail.Label(
            'No') >> if_request_lastname_mismatch
        if_request_lastname_mismatch >> rail.Label(
            'Yes') >> update_last_name >> if_request_emailaddress_mismatch
        if_request_lastname_mismatch >> rail.Label(
            'No') >> if_request_emailaddress_mismatch
        if_request_emailaddress_mismatch >> rail.Label(
            'Yes') >> update_email >> if_request_employeetype_present
        if_request_emailaddress_mismatch >> rail.Label(
            'No') >> if_request_employeetype_present
        if_request_employeetype_present >> rail.Label(
            'Yes') >> get_employee_type_for_user >> if_request_employee_type_mismatch
        if_request_employee_type_mismatch >> rail.Label(
            'Yes') >> get_all_employee_type_details >> get_required_employee_type_uri >> if_employeetype_uri_present
        if_employeetype_uri_present >> rail.Label(
            'Yes') >> update_employee_type_for_user >> if_request_department_present
        if_employeetype_uri_present >> rail.Label(
            'No') >> if_request_department_present
        if_request_employee_type_mismatch >> rail.Label(
            'No') >> if_request_department_present
        if_request_employeetype_present >> rail.Label(
            'No') >> if_request_department_present
        if_request_department_present >> rail.Label(
            'Yes') >> get_enabled_department >> get_requested_dept_uri >> if_department_mismatch
        if_department_mismatch >> rail.Label(
            'Yes') >> update_department_for_user >> if_request_supervisor_mismatch
        if_department_mismatch >> rail.Label('No') >> if_request_supervisor_mismatch
        if_request_department_present >> rail.Label(
            'No') >> if_request_supervisor_mismatch 
        if_request_supervisor_mismatch >> rail.Label(
            'Yes') >> if_request_loginname_not_equals_request_initialsupervisorloginname
        if_request_loginname_not_equals_request_initialsupervisorloginname >> rail.Label(
            'Yes') >> check_if_supervisor_available >> is_supervisor_uri_present
        is_supervisor_uri_present >> rail.Label(
            'Yes') >> update_supervisor_for_user >> if_request_holidaycalendar_present
        is_supervisor_uri_present >> rail.Label(
            'No') >> user_supervisor_mapper >> if_request_holidaycalendar_present
        if_request_loginname_not_equals_request_initialsupervisorloginname >> rail.Label(
            'No') >> user_import_log_for_same_login_and_supervisor >> if_request_holidaycalendar_present
        if_request_supervisor_mismatch >> rail.Label(
            'No') >> if_request_holidaycalendar_present
        if_request_holidaycalendar_present >> rail.Label(
            'Yes') >> get_holiday_calendar_for_user >> if_holiday_calender_mismatch
        if_holiday_calender_mismatch >> rail.Label(
            'Yes') >> get_all_holiday_calendar >> get_holiday_calendaruri_from_all_holiday_calender >> if_holiday_calendar_uri_present
        if_holiday_calendar_uri_present >> rail.Label(
            'Yes') >> update_holiday_calendar_for_user >> get_emp_daterange_from_profile
        if_holiday_calendar_uri_present >> rail.Label(
            'No') >> get_emp_daterange_from_profile
        if_holiday_calender_mismatch >> rail.Label(
            'No') >> get_emp_daterange_from_profile
        if_request_holidaycalendar_present >> rail.Label(
            'No') >> get_emp_daterange_from_profile
        get_emp_daterange_from_profile >> if_request_startdate_present_and_enddate_absent
        if_request_startdate_present_and_enddate_absent >> rail.Label(
            'Yes') >> check_request_startdate_notequals_profile_startdate
        check_request_startdate_notequals_profile_startdate >> rail.Label(
            'Yes') >> update_employment_start_date_range >> if_request_enddate_present
        check_request_startdate_notequals_profile_startdate >> rail.Label(
            'No') >> if_request_enddate_present
        if_request_startdate_present_and_enddate_absent >> rail.Label(
            'No') >> if_request_enddate_present
        if_request_enddate_present >> rail.Label(
            'Yes') >> check_request_enddate_notequals_profile_enddate
        if_request_enddate_present >> rail.Label(
            'No') >> get_custom_field_group_user_uri 
        check_request_enddate_notequals_profile_enddate >> rail.Label(
            'Yes') >> is_request_start_end_date_present
        check_request_enddate_notequals_profile_enddate >> rail.Label(
            'No') >> get_custom_field_group_user_uri
        is_request_start_end_date_present >> rail.Label(
            'Yes') >> update_employment_daterange >> get_custom_field_group_user_uri
        is_request_start_end_date_present >> rail.Label(
            'No') >> update_employment_end_date_range >> get_custom_field_group_user_uri
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
