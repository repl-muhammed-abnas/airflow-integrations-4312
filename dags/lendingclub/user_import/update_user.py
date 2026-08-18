from datetime import timedelta
from airflow.models import Variable
import rail
from lendingclub.user_import.utils import python_callable, request_payload
from lendingclub.user_import.utils.python_callable import get_schedule_list


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=f'lendingclub_user_import_update_user_child_{config.instance}',
        description=f'lendingclub_user_import_update_user_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.update_user_child_dag_active_runs,
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
            no_task='exception_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='exception_log',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        exception_log = rail.CreateLogOperator(
            task_id = "exception_log"
        )

        for_log = rail.CreateLogOperator(
            task_id = "for_log"
        )

        get_user_data = rail.RepliconServiceOperator(
            task_id='get_user_data',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data = {
                "users": [
                    {
                        "uri": "{{ dag_run.conf.useruri }}"
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:fail-if-insufficient-data-access-permission"
            }
        )

        check_rehire = rail.IfOperator(
            task_id="check_rehire",
            test="{{ result('get_user_data')[0].userDetails.isEnabled | is_falsy and dag_run.conf.employeestatus.lower() == 'active' }}",
            yes_task="enable_login",
            no_task="if_request_firstname_mismatch"
        )

        enable_login = rail.RepliconServiceOperator(
            task_id='enable_login',
            endpoint="/services/SecurityService1.svc/EnableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        log_profile_enabled = rail.WriteLogOperator(
            task_id='log_profile_enabled',
            log = "{{ result('for_log') }}",
            message="na",
            severity="Success",
            properties={
                "status" : "User profile enabled"
            }
        )

        update_employment_daterange = rail.RepliconServiceOperator(
            task_id='update_employment_daterange',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=request_payload.update_user_emp_daterange
        )

        log_enddate_removed = rail.WriteLogOperator(
            task_id='log_enddate_removed',
            log = "{{ result('for_log') }}",
            message="na",
            severity="Success",
            properties={
                "status" : "End date removed"
            }
        )

        if_request_firstname_mismatch = rail.IfOperator(
            task_id="if_request_firstname_mismatch",
            test="{{ (result('get_user_data')[0].userDetails.firstName | is_falsy or \
                result('get_user_data')[0].userDetails.firstName.lower() != dag_run.conf.firstname.lower()) and \
                dag_run.conf.firstname | is_truthy }}",
            yes_task="update_firstname",
            no_task="if_lastname_mismatch"
        )

        update_firstname = rail.RepliconServiceOperator(
            task_id = "update_firstname",
            endpoint = "/services/UserService1.svc/UpdateFirstName",
            data ={
                "userUri" : "{{ dag_run.conf.useruri }}",
                "firstname" : "{{ dag_run.conf.firstname }}"
            }
        )

        log_firstname_updated = rail.WriteLogOperator(
            task_id='log_firstname_updated',
            log = "{{ result('for_log') }}",
            message="na",
            severity="Success",
            properties={
                "status" : "First name updated"
            }
        )

        if_lastname_mismatch = rail.IfOperator(
            task_id="if_lastname_mismatch",
            test="{{ (result('get_user_data')[0].userDetails.lastName | is_falsy or \
                result('get_user_data')[0].userDetails.lastName.lower() != dag_run.conf.lastname.lower()) and \
                dag_run.conf.lastname | is_truthy }}",
            yes_task="update_lastname",
            no_task="if_hiredate_present"
        )

        update_lastname = rail.RepliconServiceOperator(
            task_id = "update_lastname",
            endpoint = "/services/UserService1.svc/UpdateLastName",
            data ={
                "userUri" : "{{ dag_run.conf.useruri }}",
                "lastname" : "{{ dag_run.conf.lastname }}"
            }
        )

        log_lastname_updated = rail.WriteLogOperator(
            task_id='log_lastname_updated',
            log = "{{ result('for_log') }}",
            message="na",
            severity="Success",
            properties={
                "status" : "Last name updated"
            }
        )

        if_hiredate_present = rail.IfOperator(
            task_id="if_hiredate_present",
            test="{{ dag_run.conf.hiredate | is_truthy }}",
            yes_task="is_hiredate_invalid",
            no_task="get_user_joblevelvalue"
        )

        is_hiredate_invalid = rail.IfOperator(
            task_id='is_hiredate_invalid',
            test=lambda dag_run: bool('-' not in dag_run.conf['hiredate']),
            yes_task="log_invalid_startdate",
            no_task="get_start_day_mismatch_val",
        )

        log_invalid_startdate = rail.WriteLogOperator(
            task_id='log_invalid_startdate',
            log = "{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value" : "Start date is not in the predefined format"
            }
        )

        get_start_day_mismatch_val = rail.PythonOperator(
            task_id = "get_start_day_mismatch_val",
            python_callable=python_callable.check_start_date_mismatch
        )

        check_if_start_day_mismatch = rail.IfOperator(
            task_id="check_if_start_day_mismatch",
            test="{{ result('get_start_day_mismatch_val') | is_truthy}}",
            yes_task="update_emp_daterange",
            no_task="get_user_joblevelvalue"
        )

        update_emp_daterange = rail.RepliconServiceOperator(
            task_id='update_emp_daterange',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=request_payload.update_emp_daterange_hiredate
        )

        log_startdate_updated = rail.WriteLogOperator(
            task_id='log_startdate_updated',
            log = "{{ result('for_log') }}",
            message="na",
            severity="Success",
            properties={
                "status" : "Start date updated"
            }
        )

        get_user_joblevelvalue = rail.PythonOperator(
            task_id = "get_user_joblevelvalue",
            python_callable= python_callable.get_user_job_level_value
        )

        if_joblevel_present = rail.IfOperator(
            task_id="if_joblevel_present",
            test="{{ dag_run.conf.joblevel | is_truthy and dag_run.conf.joblevel != result('get_user_joblevelvalue')}}",
            yes_task="update_joblevel",
            no_task="if_vendor_present_and_employeetype_contractor"
        )

        update_joblevel = rail.RepliconServiceOperator(
            task_id='update_joblevel',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.joblevel_uri }}",
                "value": "{{ dag_run.conf.joblevel }}"
            }
        )

        if_vendor_present_and_employeetype_contractor = rail.IfOperator(
            task_id='if_vendor_present_and_employeetype_contractor',
            test="{{ dag_run.conf.vendor | is_truthy and dag_run.conf.employeetypename.lower() == 'contractors' }}",
            yes_task="get_user_customfielddropdown_options",
            no_task="if_employeetypename_mismatch",
        )

        get_user_customfielddropdown_options = rail.PythonOperator(
            task_id = "get_user_customfielddropdown_options",
            python_callable= python_callable.get_user_vendor_value
        )

        if_vendor_dropdown_value_mismatch = rail.IfOperator(
            task_id='if_vendor_dropdown_value_mismatch',
            test="{{ dag_run.conf.vendor != result('get_user_customfielddropdown_options') }}",
            yes_task="get_enabled_customfielddropdown_options",
            no_task="if_employeetypename_mismatch",
        )

        get_enabled_customfielddropdown_options = rail.RepliconServiceOperator(
            task_id='get_enabled_customfielddropdown_options',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data = {
                "customFieldUri": "{{ dag_run.conf.vendor_uri }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['vendor'], 'uri', '')
        )

        if_required_vendor_uri_present = rail.IfOperator(
            task_id='if_required_vendor_uri_present',
            test="{{ result('get_enabled_customfielddropdown_options') | is_truthy }}",
            yes_task="update_vendor_dropdown_value",
            no_task="if_employeetypename_mismatch",
        )

        update_vendor_dropdown_value = rail.RepliconServiceOperator(
            task_id='update_vendor_dropdown_value',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data = {
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.vendor_uri }}",
                "customFieldDropDownOptionUri": "{{ result('get_enabled_customfielddropdown_options') }}"
            }
        )

        log_vendorddoption_updated = rail.WriteLogOperator(
            task_id='log_vendorddoption_updated',
            log = "{{ result('for_log') }}",
            message="na",
            severity="Success",
            properties={
                "status" : "Vendor dropdown option updated"
            }
        )

        if_employeetypename_mismatch = rail.IfOperator(
            task_id="if_employeetypename_mismatch",
            test="{{ dag_run.conf.employeetypename | is_truthy and \
                result('get_user_data')[0].employeeType.name.lower() != dag_run.conf.employeetypename.lower() }}",
            yes_task="get_employee_type_details",
            no_task="get_all_permissionsets"
        )

        get_employee_type_details = rail.RepliconServiceOperator(
            task_id='get_employee_type_details',
            endpoint="/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf['employeetypename'], 'uri', '')
        )

        is_employeetype_present = rail.IfOperator(
            task_id='is_employeetype_present',
            test="{{ result('get_employee_type_details') | is_truthy }}",
            yes_task="update_employeetype_for_user",
            no_task="get_all_permissionsets",
        )

        update_employeetype_for_user = rail.RepliconServiceOperator(
            task_id='update_employeetype_for_user',
            endpoint="/services/EmployeeTypeService1.svc/UpdateEmployeeTypeForUser",
            data = {
                "userUri": "{{ dag_run.conf.useruri }}",
                "employeeTypeUri" : "{{ result('get_employee_type_details') }}"
            }
        )

        log_employeetype_updated = rail.WriteLogOperator(
            task_id='log_employeetype_updated',
            log = "{{ result('for_log') }}",
            message="na",
            severity="Success",
            properties={
                "status" : "Employee type updated"
            }
        )

        get_all_permissionsets = rail.RepliconServiceOperator(
            task_id='get_all_permissionsets',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data_handler=lambda response:{
                "project_resource_uri" : rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Project Resource', 'uri', ''),
                "project_resource_with_report_uri" : rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Project Resource with Reports', 'uri', ''),
                "supervisor_uri" : rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Supervisor', 'uri', ''),
                "substitute_user_uri" : rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Substitute user', 'uri', ''),
                "project_manager_view_access_uri" : rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Project Manager View Access', 'uri', '')

            }
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id='get_user_details',
            endpoint="/services/UserListService1.svc/GetData",
            data = request_payload.get_user_details
        )

        get_supervisor_data = rail.PythonOperator(
            task_id = "get_supervisor_data",
            python_callable=python_callable.get_supervisor_data
        )

        if_managerid_present = rail.IfOperator(
            task_id="if_managerid_present",
            test="{{ dag_run.conf.managerid | is_truthy }}",
            yes_task="if_managerid_equals_empid",
            no_task="if_locationcode_or_location_is_present"
        )

        if_managerid_equals_empid = rail.IfOperator(
            task_id="if_managerid_equals_empid",
            test="{{ dag_run.conf.managerid == dag_run.conf.empid }}",
            yes_task="log_same_supervisor_and_user",
            no_task="search_for_user_with_empid"
        )

        log_same_supervisor_and_user = rail.WriteLogOperator(
            task_id='log_same_supervisor_and_user',
            log = "{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value" : "Supervsior not updated for {{ dag_run.conf.loginname }} as user's and supervsior's login name are same."
            }
        )

        search_for_user_with_empid = rail.RepliconServiceOperator(
            task_id='search_for_user_with_empid',
            endpoint="/services/UserListService1.svc/GetData",
            data = {
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:user-list-column:employee-id",
                    "urn:replicon:user-list-column:login-name"
                ]
            },
            data_handler=python_callable.get_userdata_list_for_managerid
        )

        check_if_multiple_manageruseruri_present = rail.IfOperator(
            task_id='check_if_multiple_manageruseruri_present',
            test=lambda: bool(len(rail.result('search_for_user_with_empid')) > 1 ),
            yes_task="log_multiple_user_for_same_managerid",
            no_task="check_if_single_manageruseruri_present",
        )

        log_multiple_user_for_same_managerid = rail.WriteLogOperator(
            task_id='log_multiple_user_for_same_managerid',
            log = "{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value" : "Supervisor not assigned for user {{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }} as multiple users have same Employee ID:{{ dag_run.conf.managerid }} ."
            }
        )

        check_if_single_manageruseruri_present = rail.IfOperator(
            task_id='check_if_single_manageruseruri_present',
            test=lambda: bool(len(rail.result('search_for_user_with_empid')) == 1 ),
            yes_task="get_manager_details",
            no_task="log_supervisor_assignment",
        )

        get_manager_details = rail.RepliconServiceOperator(
            task_id='get_manager_details',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data = {
                "users": [
                    {
                        "uri": "{{ result('search_for_user_with_empid')[0]['uri'] }}"
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        if_manager_details_present_and_enabled = rail.IfOperator(
            task_id='if_manager_details_present_and_enabled',
            test="{{ result('get_manager_details') | is_truthy and result('get_manager_details')[0]['userDetails']['isEnabled'] | is_truthy }}",
            yes_task="get_assigned_permissionset_foruser",
            no_task="log_supervisor_assignment",
        )

        get_assigned_permissionset_foruser = rail.RepliconServiceOperator(
            task_id='get_assigned_permissionset_foruser',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data = {
                "userUri": "{{ result('search_for_user_with_empid')[0]['uri'] }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', 'urn:replicon:policy:supervision', 'permissionSet.uri', '')
        )

        if_supervisor_permission_assigned = rail.IfOperator(
            task_id='if_supervisor_permission_assigned',
            test="{{ result('get_assigned_permissionset_foruser') | is_truthy }}",
            yes_task="if_supervisorassigned_is_null",
            no_task="log_supervisor_assignment",
        )

        if_supervisorassigned_is_null = rail.IfOperator(
            task_id='if_supervisorassigned_is_null',
            test="{{ result('get_supervisor_data').datatype == 'urn:replicon:list-type:null' }}",
            yes_task="update_initial_supervisor",
            no_task="get_current_supervisor_empid",
        )

        update_initial_supervisor = rail.RepliconServiceOperator(
            task_id='update_initial_supervisor',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data = {
                "userUri": "{{ dag_run.conf.useruri }}",
                "initialSupervisorUri": "{{ result('search_for_user_with_empid')[0]['uri'] }}"
            }
        )

        log_initialsupervisor_updated = rail.WriteLogOperator(
            task_id='log_initialsupervisor_updated',
            log = "{{ result('for_log') }}",
            message="na",
            severity="Success",
            properties={
                "status" : "Employee type updated"
            }
        )

        get_current_supervisor_empid = rail.RepliconServiceOperator(
            task_id='get_current_supervisor_empid',
            endpoint="/services/UserListService1.svc/GetData",
            data = request_payload.get_current_supervisorempid
        )

        if_current_supervisor_empid_mismatch_managerid = rail.IfOperator(
            task_id='if_current_supervisor_empid_mismatch_managerid',
            test="{{ dag_run.conf.managerid | is_truthy and \
                (result('get_current_supervisor_empid')['rows'][0]['cells'][0]['dataType'] == 'urn:replicon:list-type:null' or  \
                result('get_current_supervisor_empid')['rows'][0]['cells'][0]['textValue'] != dag_run.conf.managerid) }}",
            yes_task="update_supervisor_assignment_over_daterange",
            no_task="if_locationcode_or_location_is_present",
        )

        update_supervisor_assignment_over_daterange = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_over_daterange',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data = request_payload.update_supervisorassignment_overdaterange
        )

        log_supervisor_updated_withdaterange = rail.WriteLogOperator(
            task_id='log_supervisor_updated_withdaterange',
            log = "{{ result('for_log') }}",
            message="na",
            severity="Success",
            properties={
                "status" : "Supervisor updated with effective date"
            }
        )

        log_supervisor_assignment = rail.WriteLogOperator(
            task_id="log_supervisor_assignment",
            log = '{{ dag_run.conf.supervisor_logger}}',
            message="Exception",
            severity="Exception",
            properties=lambda item, dag_run: {
                "loginid": dag_run.conf['loginname'],
                "empid": dag_run.conf['empid'],
                "managerid": dag_run.conf['managerid'],
                "useruri": "{{ dag_run.conf.useruri }}",
                'type': "update",
            }
        )

        if_locationcode_or_location_is_present = rail.IfOperator(
            task_id='if_locationcode_or_location_is_present',
            test="{{ dag_run.conf.locationcode | is_truthy or dag_run.conf.location | is_truthy }}",
            yes_task="search_location_by_codeandname",
            no_task="if_deptcode_or_deptname_is_present",
        )

        search_location_by_codeandname = rail.RepliconServiceOperator(
            task_id='search_location_by_codeandname',
            endpoint="/services/LocationListService1.svc/GetData",
            data = request_payload.search_location_filter,
            data_handler = python_callable.get_uri_value
        )

        if_location_uri_present = rail.IfOperator(
            task_id='if_location_uri_present',
            test="{{ result('search_location_by_codeandname') | is_truthy }}",
            yes_task="if_location_is_not_assigned",
            no_task="log_location_not_assigned",
        )

        log_location_not_assigned = rail.WriteLogOperator(
            task_id='log_location_not_assigned',
            log = "{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value" : "Location not updated for User '{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}' as \
                    '{{ dag_run.conf.location }}' not available or disabled in Replicon."
            }
        )

        if_location_is_not_assigned = rail.IfOperator(
            task_id='if_location_is_not_assigned',
            test="{{ result('get_user_details')['rows'][0]['cells'][1]['dataType'] == 'urn:replicon:list-type:null' }}",
            yes_task="put_locationschedule_for_user",
            no_task="if_current_and_required_location_mismatch",
        )

        put_locationschedule_for_user = rail.RepliconServiceOperator(
            task_id='put_locationschedule_for_user',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data = {
                "userUri": "{{ dag_run.conf.useruri }}",
                "scheduleEntries": [
                    {
                        "location": {
                        "uri": "{{ result('search_location_by_codeandname') }}"
                        }
                    }
                ]
            }
        )

        if_current_and_required_location_mismatch = rail.IfOperator(
            task_id='if_current_and_required_location_mismatch',
            test="{{ result('get_user_details')['rows'][0]['cells'][1]['uri'] != result('search_location_by_codeandname') }}",
            yes_task="get_location_schedule_list",
            no_task="if_deptcode_or_deptname_is_present",
        )

        get_location_schedule_list = rail.PythonOperator(
            task_id = "get_location_schedule_list",
            python_callable=lambda: get_schedule_list('locationSchedule','location', rail.result('search_location_by_codeandname'))
        )

        put_locationschedule = rail.RepliconServiceOperator(
            task_id='put_locationschedule',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data = lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": rail.result('get_location_schedule_list')
            }
        )

        log_location_updated = rail.WriteLogOperator(
            task_id='log_location_updated',
            log = "{{ result('for_log') }}",
            message="na",
            severity="Success",
            properties={
                "status" : "Location updated"
            }
        )

        if_deptcode_or_deptname_is_present = rail.IfOperator(
            task_id='if_deptcode_or_deptname_is_present',
            test="{{ dag_run.conf.departmentcode | is_truthy or dag_run.conf.department | is_truthy }}",
            yes_task="search_department_by_codeandname",
            no_task="if_permission_is_present",
        )

        search_department_by_codeandname = rail.RepliconServiceOperator(
            task_id='search_department_by_codeandname',
            endpoint="/services/DivisionListService1.svc/GetData",
            data = request_payload.search_department_filter,
            data_handler = python_callable.get_uri_value
        )

        if_dept_uri_present = rail.IfOperator(
            task_id='if_dept_uri_present',
            test="{{ result('search_department_by_codeandname') | is_truthy }}",
            yes_task="if_division_is_not_assigned",
            no_task="log_division_not_assigned",
        )

        if_division_is_not_assigned = rail.IfOperator(
            task_id='if_division_is_not_assigned',
            test="{{ result('get_user_details')['rows'][0]['cells'][2]['dataType'] == 'urn:replicon:list-type:null' }}",
            yes_task="put_divisionschedule_for_user",
            no_task="if_current_and_required_division_mismatch",
        )

        put_divisionschedule_for_user = rail.RepliconServiceOperator(
            task_id='put_divisionschedule_for_user',
            endpoint="/services/DivisionService1.svc/PutDivisionScheduleForUser",
            data = {
                "userUri": "{{ dag_run.conf.useruri }}",
                "scheduleEntries": [
                    {
                        "division": {
                        "uri": "{{ result('search_department_by_codeandname') }}"
                        }
                    }
                ]
            }
        )

        if_current_and_required_division_mismatch = rail.IfOperator(
            task_id='if_current_and_required_division_mismatch',
            test="{{ result('get_user_details')['rows'][0]['cells'][2]['uri'] != result('search_department_by_codeandname') }}",
            yes_task="get_division_schedule_list",
            no_task="if_permission_is_present",
        )

        get_division_schedule_list = rail.PythonOperator(
            task_id = "get_division_schedule_list",
            python_callable= lambda : get_schedule_list('divisionSchedule','division', rail.result('search_department_by_codeandname'))
        )

        put_devisionschedule = rail.RepliconServiceOperator(
            task_id='put_devisionschedule',
            endpoint="/services/DivisionService1.svc/PutDivisionScheduleForUser",
            data = lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": rail.result('get_division_schedule_list')
            }
        )

        log_division_updated = rail.WriteLogOperator(
            task_id='log_division_updated',
            log = "{{ result('for_log') }}",
            message="na",
            severity="Success",
            properties={
                "status" : "Division updated"
            }
        )

        log_division_not_assigned = rail.WriteLogOperator(
            task_id='log_division_not_assigned',
            log = "{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value" : "Division not updated for User '{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}' as \
                    '{{ dag_run.conf.department }}' not available or disabled in Replicon."
            }
        )

        if_permission_is_present = rail.IfOperator(
            task_id='if_permission_is_present',
            test="{{ dag_run.conf.permission | is_truthy }}",
            yes_task="get_assignedpermissionset_for_user",
            no_task="if_employeetypename_present",
        )

        get_assignedpermissionset_for_user = rail.RepliconServiceOperator(
            task_id='get_assignedpermissionset_for_user',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data = {
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        if_permission_is_resource = rail.IfOperator(
            task_id='if_permission_is_resource',
            test="{{ dag_run.conf.permission.lower() == 'resource' }}",
            yes_task="assign_permission_resource",
            no_task="if_permission_is_supervisor",
        )

        assign_permission_resource = rail.RepliconServiceOperator(
            task_id='assign_permission_resource',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data = {
                "userUri": "{{ dag_run.conf.useruri }}",
                "permissionSetUri": "{{ result('get_all_permissionsets').project_resource_uri }}"
            }
        )

        if_permission_is_supervisor = rail.IfOperator(
            task_id='if_permission_is_supervisor',
            test="{{ dag_run.conf.permission.lower() == 'supervisor' }}",
            yes_task="assign_permission_resource_with_report",
            no_task="if_permission_is_management",
        )

        assign_permission_resource_with_report = rail.RepliconServiceOperator(
            task_id='assign_permission_resource_with_report',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data = {
                "userUri": "{{ dag_run.conf.useruri }}",
                "permissionSetUri": "{{ result('get_all_permissionsets').project_resource_with_report_uri }}"
            }
        )

        if_permission_is_management = rail.IfOperator(
            task_id='if_permission_is_management',
            test="{{ dag_run.conf.permission.lower() == 'management' }}",
            yes_task="assign_permissionsubstitute_user",
            no_task="if_permission_is_manager_view_access",
        )

        assign_permissionsubstitute_user = rail.RepliconServiceOperator(
            task_id='assign_permissionsubstitute_user',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data = {
                "userUri": "{{ dag_run.conf.useruri }}",
                "permissionSetUri": "{{ result('get_all_permissionsets').substitute_user_uri }}"
            }
        )

        assign_permission_supervisor = rail.RepliconServiceOperator(
            task_id='assign_permission_supervisor',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data = {
                "userUri": "{{ dag_run.conf.useruri }}",
                "permissionSetUri": "{{ result('get_all_permissionsets').supervisor_uri }}"
            }
        )

        if_permission_is_manager_view_access = rail.IfOperator(
            task_id='if_permission_is_manager_view_access',
            test="{{ dag_run.conf.permission.lower() == 'project manager view access' }}",
            yes_task="assign_permission_managerviewaccess",
            no_task="if_employeetypename_present",
        )

        assign_permission_managerviewaccess = rail.RepliconServiceOperator(
            task_id='assign_permission_managerviewaccess',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data = {
                "userUri": "{{ dag_run.conf.useruri }}",
                "permissionSetUri": "{{ result('get_all_permissionsets').project_manager_view_access_uri }}"
            }
        )

        assign_permission_substitute_user = rail.RepliconServiceOperator(
            task_id='assign_permission_substitute_user',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data = {
                "userUri": "{{ dag_run.conf.useruri }}",
                "permissionSetUri": "{{ result('get_all_permissionsets').substitute_user_uri }}"
            }
        )

        log_permissionassigned_updated = rail.WriteLogOperator(
            task_id='log_permissionassigned_updated',
            log = "{{ result('for_log') }}",
            message="na",
            severity="Success",
            properties={
                "status" : "Permission assignment updated"
            }
        )

        if_employeetypename_present = rail.IfOperator(
            task_id='if_employeetypename_present',
            test="{{ dag_run.conf.employeetypename | is_truthy }}",
            yes_task="get_timesheettemplate_to_assign",
            no_task="get_primary_department",
        )

        get_timesheettemplate_to_assign = rail.PythonOperator(
            task_id = "get_timesheettemplate_to_assign",
            python_callable=python_callable.get_timesheet_template_to_assign
        )

        if_timesheet_template_is_present = rail.IfOperator(
            task_id='if_timesheet_template_is_present',
            test="{{ result('get_timesheettemplate_to_assign') | is_truthy }}",
            yes_task="get_all_policysets",
            no_task="log_timesheet_not_updated",
        )

        log_timesheet_not_updated = rail.WriteLogOperator(
            task_id='log_timesheet_not_updated',
            log = "{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value" : "Timesheet template not updated since employee type is recevied blank."
            }
        )

        get_all_policysets = rail.RepliconServiceOperator(
            task_id='get_all_policysets',
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
            data_handler= lambda response: {
                "timesheettemplate" : rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', rail.result('get_timesheettemplate_to_assign'), 'uri', ''),
                "timeofftemplate" : rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Time Off', 'uri', '')
            }
        )

        put_policyassignment_for_user = rail.RepliconServiceOperator(
            task_id='put_policyassignment_for_user',
            endpoint="/services/PolicySetService1.svc/PutPolicySetAssignmentsForUser",
            data = {
                "userUri": "{{ dag_run.conf.useruri }}",
                "policySetUris": [
                    "{{ result('get_all_policysets').timesheettemplate}}",
                    "{{ result('get_all_policysets').timeofftemplate}}"
                ]
            }
        )

        log_timesheettemplate_updated = rail.WriteLogOperator(
            task_id='log_timesheettemplate_updated',
            log = "{{ result('for_log') }}",
            message="na",
            severity="Success",
            properties={
                "status" : "Timesheet template updated"
            }
        )

        get_primary_department = rail.RepliconServiceOperator(
            task_id='get_primary_department',
            endpoint="/services/DepartmentService1.svc/GetEnabledDepartments",
            data_handler=lambda response, dag_run:{
                "all_scrum_team" : rail.find_first_by_attr_and_get_attr(response, 'displayText', 'All scrum team', 'uri', ''),
                "scrum": rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf['Scrum'], 'uri', '')
            }
        )

        if_scrum_is_present = rail.IfOperator(
            task_id='if_scrum_is_present',
            test="{{ dag_run.conf.Scrum | is_truthy }}",
            yes_task="is_scrumdept_present",
            no_task="write_log_user_import",
        )

        is_scrumdept_present = rail.IfOperator(
            task_id='is_scrumdept_present',
            test="{{ result('get_primary_department').scrum | is_truthy }}",
            yes_task="update_dept_for_user",
            no_task="create_newdraft_dept",
        )

        update_dept_for_user = rail.RepliconServiceOperator(
            task_id='update_dept_for_user',
            endpoint="/services/DepartmentService1.svc/UpdateDepartmentForUser",
            data = {
                "userUri": "{{ dag_run.conf.useruri }}",
                "departmentUri": "{{ result('get_primary_department').scrum }}"
            }
        )

        create_newdraft_dept = rail.RepliconServiceOperator(
            task_id='create_newdraft_dept',
            endpoint="/services/DepartmentService1.svc/CreateNewDraft",
            data = {
                "parentDepartmentUri": "{{ result('get_primary_department').all_scrum_team }}"
            }
        )

        update_name_dept = rail.RepliconServiceOperator(
            task_id='update_name_dept',
            endpoint="/services/DepartmentService1.svc/UpdateName",
            data = {
                "departmentUri": "{{ result('create_newdraft_dept') }}",
                "name": "{{ dag_run.conf.Scrum }}"
            }
        )

        publish_draft_dept = rail.RepliconServiceOperator(
            task_id='publish_draft_dept',
            endpoint="/services/DepartmentService1.svc/PublishDraft",
            data = {
                "draftUri": "{{ result('create_newdraft_dept') }}"
            }
        )

        update_department_for_user = rail.RepliconServiceOperator(
            task_id='update_department_for_user',
            endpoint="/services/DepartmentService1.svc/UpdateDepartmentForUser",
            data = {
                "userUri": "{{ dag_run.conf.useruri }}",
                "departmentUri": "{{ result('publish_draft_dept') }}"
            }
        )

        write_log_user_import = rail.WriteCSVFileOperator(
            task_id='write_log_user_import',
            source="{{ result('exception_log') }}",
            header=['value'],
            row=lambda item: [
                item['properties']['value']
            ]
        )

        write_log_success_import = rail.WriteCSVFileOperator(
            task_id='write_log_success_import',
            source="{{ result('for_log') }}",
            header=['status'],
            row=lambda item: [
                item['properties']['status']
            ]
        )

        log_user_import = rail.WriteLogOperator(
            task_id='log_user_import',
            log = "{{ dag_run.conf.logger }}",
            message="na",
            severity="Skipped",
            properties=python_callable.get_status_and_details_for_update
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            log = '{{ dag_run.conf.logger}}',
            severity="Error",
            trigger_rule="one_failed",
            message='{{ get_error_message() }}',
            properties={
                "UserID": "{{ dag_run.conf.loginname }}" + "|" + "{{ dag_run.conf.empid }}",
                "Action": "Update",
                "Status": "Error",
                "Details": "{{ get_error_message() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> exception_log

        exception_log >> for_log >> get_user_data >> check_rehire

        check_rehire >> rail.Label('Yes') >> enable_login >> log_profile_enabled >> update_employment_daterange >> \
        log_enddate_removed >> if_request_firstname_mismatch
        check_rehire >> rail.Label('No') >> if_request_firstname_mismatch

        if_request_firstname_mismatch >> rail.Label('Yes') >> update_firstname >> log_firstname_updated >> if_lastname_mismatch
        if_request_firstname_mismatch >> rail.Label('No') >> if_lastname_mismatch

        if_lastname_mismatch >> rail.Label('Yes') >> update_lastname >> log_lastname_updated >> if_hiredate_present
        if_lastname_mismatch >> rail.Label('No') >> if_hiredate_present

        if_hiredate_present >> rail.Label('Yes') >> is_hiredate_invalid
        if_hiredate_present >> rail.Label('No') >> get_user_joblevelvalue

        is_hiredate_invalid >> rail.Label('Yes') >> log_invalid_startdate >> get_user_joblevelvalue
        is_hiredate_invalid >> rail.Label('No') >> get_start_day_mismatch_val >> check_if_start_day_mismatch

        check_if_start_day_mismatch >> rail.Label('Yes') >> update_emp_daterange >> log_startdate_updated >> get_user_joblevelvalue
        check_if_start_day_mismatch >> rail.Label('No') >> get_user_joblevelvalue

        get_user_joblevelvalue >> if_joblevel_present

        if_joblevel_present >> rail.Label('Yes') >> update_joblevel >> if_vendor_present_and_employeetype_contractor
        if_joblevel_present >> rail.Label('No') >> if_vendor_present_and_employeetype_contractor

        if_vendor_present_and_employeetype_contractor >> rail.Label('Yes') >> get_user_customfielddropdown_options >> if_vendor_dropdown_value_mismatch
        if_vendor_present_and_employeetype_contractor >> rail.Label('No') >> if_employeetypename_mismatch

        if_vendor_dropdown_value_mismatch >> rail.Label('Yes') >> get_enabled_customfielddropdown_options >> if_required_vendor_uri_present
        if_vendor_dropdown_value_mismatch >> rail.Label('No') >> if_employeetypename_mismatch

        if_required_vendor_uri_present >> rail.Label('Yes') >> update_vendor_dropdown_value >> log_vendorddoption_updated >> if_employeetypename_mismatch
        if_required_vendor_uri_present >> rail.Label('No') >> if_employeetypename_mismatch

        if_employeetypename_mismatch >> rail.Label('Yes') >> get_employee_type_details >> is_employeetype_present

        is_employeetype_present >> rail.Label('Yes') >> update_employeetype_for_user >> log_employeetype_updated >> get_all_permissionsets
        is_employeetype_present >> rail.Label('No') >> get_all_permissionsets

        if_employeetypename_mismatch >> rail.Label('No') >> get_all_permissionsets

        get_all_permissionsets >> get_user_details >> get_supervisor_data >> if_managerid_present

        if_managerid_present >> rail.Label('Yes') >> if_managerid_equals_empid
        if_managerid_present >> rail.Label('No') >> if_locationcode_or_location_is_present

        if_managerid_equals_empid >> rail.Label('Yes') >> log_same_supervisor_and_user >> if_locationcode_or_location_is_present
        if_managerid_equals_empid >> rail.Label('No') >> search_for_user_with_empid >> check_if_multiple_manageruseruri_present

        check_if_multiple_manageruseruri_present >> rail.Label('Yes') >> log_multiple_user_for_same_managerid >> if_locationcode_or_location_is_present
        check_if_multiple_manageruseruri_present >> rail.Label('No') >> check_if_single_manageruseruri_present

        check_if_single_manageruseruri_present >> rail.Label('Yes') >> get_manager_details >> if_manager_details_present_and_enabled
        check_if_single_manageruseruri_present >> rail.Label('No') >> log_supervisor_assignment >> if_locationcode_or_location_is_present

        if_manager_details_present_and_enabled >> rail.Label('Yes') >> get_assigned_permissionset_foruser >> if_supervisor_permission_assigned
        if_manager_details_present_and_enabled >> rail.Label('No') >> log_supervisor_assignment >> if_locationcode_or_location_is_present

        if_supervisor_permission_assigned >> rail.Label('Yes') >> if_supervisorassigned_is_null
        if_supervisor_permission_assigned >> rail.Label('No') >> log_supervisor_assignment >> if_locationcode_or_location_is_present

        if_supervisorassigned_is_null >> rail.Label('Yes') >> update_initial_supervisor >> log_initialsupervisor_updated >> if_locationcode_or_location_is_present
        if_supervisorassigned_is_null >> rail.Label('No') >> get_current_supervisor_empid >> if_current_supervisor_empid_mismatch_managerid

        if_current_supervisor_empid_mismatch_managerid >> rail.Label('Yes') >> update_supervisor_assignment_over_daterange >> log_supervisor_updated_withdaterange >> \
            if_locationcode_or_location_is_present
        if_current_supervisor_empid_mismatch_managerid >> rail.Label('No') >> if_locationcode_or_location_is_present

        if_locationcode_or_location_is_present >> rail.Label('Yes') >> search_location_by_codeandname >> if_location_uri_present
        if_locationcode_or_location_is_present >> rail.Label('No') >> if_deptcode_or_deptname_is_present

        if_location_uri_present >> rail.Label('Yes') >> if_location_is_not_assigned
        if_location_uri_present >> rail.Label('No') >> log_location_not_assigned >> if_deptcode_or_deptname_is_present

        if_location_is_not_assigned >> rail.Label('Yes') >> put_locationschedule_for_user >> log_location_updated >> if_deptcode_or_deptname_is_present
        if_location_is_not_assigned >> rail.Label('No') >> if_current_and_required_location_mismatch

        if_current_and_required_location_mismatch >> rail.Label('Yes') >> get_location_schedule_list >> put_locationschedule >> log_location_updated >> \
            if_deptcode_or_deptname_is_present
        if_current_and_required_location_mismatch >> rail.Label('No') >> if_deptcode_or_deptname_is_present

        if_deptcode_or_deptname_is_present >> rail.Label('Yes') >> search_department_by_codeandname >> if_dept_uri_present
        if_deptcode_or_deptname_is_present >> rail.Label('No') >> if_permission_is_present

        if_dept_uri_present >> rail.Label('Yes') >> if_division_is_not_assigned
        if_dept_uri_present >> rail.Label('No') >> log_division_not_assigned >> if_permission_is_present

        if_division_is_not_assigned >> rail.Label('Yes') >> put_divisionschedule_for_user >> log_division_updated >> if_permission_is_present
        if_division_is_not_assigned >> rail.Label('No') >> if_current_and_required_division_mismatch

        if_current_and_required_division_mismatch >> rail.Label('Yes') >> get_division_schedule_list >> put_devisionschedule >> log_division_updated >> \
            if_permission_is_present
        if_current_and_required_division_mismatch >> rail.Label('No') >> if_permission_is_present

        if_permission_is_present >> rail.Label('Yes') >> get_assignedpermissionset_for_user >> if_permission_is_resource
        if_permission_is_present >> rail.Label('No') >> if_employeetypename_present

        if_permission_is_resource >> rail.Label('Yes') >> assign_permission_resource >> log_permissionassigned_updated >> if_employeetypename_present
        if_permission_is_resource >> rail.Label('No') >> if_permission_is_supervisor

        if_permission_is_supervisor >> rail.Label('Yes') >> assign_permission_resource_with_report >> assign_permission_supervisor >> \
            log_permissionassigned_updated >> if_employeetypename_present
        if_permission_is_supervisor >> rail.Label('No') >> if_permission_is_management

        if_permission_is_management >> rail.Label('Yes') >> assign_permissionsubstitute_user >> assign_permission_supervisor >> \
            log_permissionassigned_updated >> if_employeetypename_present
        if_permission_is_management >> rail.Label('No') >> if_permission_is_manager_view_access

        if_permission_is_manager_view_access >> rail.Label('Yes') >> assign_permission_managerviewaccess >> assign_permission_substitute_user >> \
            log_permissionassigned_updated >> if_employeetypename_present
        if_permission_is_manager_view_access >> rail.Label('No') >> if_employeetypename_present

        if_employeetypename_present >> rail.Label('Yes') >> get_timesheettemplate_to_assign >> if_timesheet_template_is_present
        if_employeetypename_present >> rail.Label('No') >> get_primary_department

        if_timesheet_template_is_present >> rail.Label('Yes') >> get_all_policysets >> put_policyassignment_for_user >> \
            log_timesheettemplate_updated >> get_primary_department
        if_timesheet_template_is_present >> rail.Label('No') >> log_timesheet_not_updated >> get_primary_department

        get_primary_department >> if_scrum_is_present

        if_scrum_is_present >> rail.Label('Yes') >> is_scrumdept_present
        if_scrum_is_present >> rail.Label('No') >> write_log_user_import

        is_scrumdept_present >> rail.Label('Yes') >> update_dept_for_user >> write_log_user_import
        is_scrumdept_present >> rail.Label('No') >> create_newdraft_dept >> update_name_dept >> publish_draft_dept >> update_department_for_user >> \
        write_log_user_import

        write_log_user_import >> write_log_success_import >> log_user_import >> catch_and_log_error

        catch_and_log_error >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
