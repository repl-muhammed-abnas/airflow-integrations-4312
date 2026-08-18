from datetime import timedelta
from airflow.models import Variable
import rail
from adtalem.user_import.task.process_supervisor_assignment import process_supervisor_assignment_task_group
from adtalem.user_import.utils import request_payload, python_callable_method, response_filter


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/adtalem/user_import/config.py


# pylint: disable=too-many-statements
def create_updateuser_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'adtalem_userimport_caribbean_child_update_user_{config.instance}',
        description=f'Adtalem Carribean_Child_Update_User_Prod {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='process_caribbean_updateuser'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='process_caribbean_updateuser',
            end_task='catch_and_log_errors',
        )

        process_caribbean_updateuser = rail.EmptyOperator(
            task_id='process_caribbean_updateuser'
        )

        get_salary = rail.PythonOperator(
            task_id='get_salary',
            python_callable=python_callable_method.get_salary_details,
            op_args=['{{ dag_run.conf.paygroup }}',
                     '{{ dag_run.conf.salaryhourly }}']
        )

        get_user_customfieldgroupuri = rail.RepliconServiceOperator(
            task_id='get_user_customfieldgroupuri',
            endpoint="/services/CustomFieldService1.svc/GetCustomFieldGroup",
            data={
                "objectTypeUri": "urn:replicon:object-type:user"
            }
        )

        get_required_user_customfields = rail.RepliconServiceOperator(
            task_id='get_required_user_customfields',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={
                'objectUri': "{{ result('get_user_customfieldgroupuri').uri }}"
            },
            data_handler=lambda response: {
                'division': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Division', 'uri', ''),
                'job_function': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Job Function', 'uri', ''),
                'service_date': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Service Date', 'uri', ''),
                'rehire_date': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Rehire Date', 'uri', ''),
                'active_leave_status': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Active/Leave Status', 'uri', ''),
                'flsa_status': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'FLSA Status', 'uri', ''),
                'salary_hourly': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Salary/Hourly', 'uri', ''),
                'regular_temp': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Regular/Temp', 'uri', ''),
                'home_state': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Home State', 'uri', ''),
                'full_part_time': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Full/Part Time', 'uri', ''),
                'job_title': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Job_Title', 'uri', ''),
                'work_location': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Work Location', 'uri', ''),
                'department_number': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Department Number', 'uri', ''),
                'standard_hours': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Standard Hours', 'uri', ''),
                'job_code': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Job Code', 'uri', ''),
                'batch_id': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Batch ID', 'uri', ''),
                'file_number': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'File Number', 'uri', ''),
                'colleague_d_number': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Colleague D Number', 'uri', ''),
                'co_code': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'CoCode', 'uri', ''),
                'student_worker': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Student Worker', 'uri', '')
            }
        )

        generate_userreport = rail.RepliconServiceOperator(
            task_id='generate_userreport',
            endpoint='/services/ReportService1.svc/GenerateReport',
            data=lambda dag_run: {
                'reportUri': config.user_report_uri,
                'filterValues': [
                    {
                        'reportFilterUri': config.user_report_filter_uri,
                        'value': dag_run.conf['useruri'].split(':')[-1]
                    }
                ],
                'outputFormatUri': 'urn:replicon:report-output-format-option:csv'
            }
        )

        parse_csv = rail.LoadCSVFileOperator(
            task_id='parse_csv',
            document="{{ result('generate_userreport').payload }}",
            headers=['User First Name', 'User Last Name', 'User Email', 'User Status', 'User Start Date',
                     'User End Date', 'User Supervisor Name (Current)', 'User Department Name', 'Employee ID',
                     'Login Name', 'Employee Type', 'Punch Entry Policy Name', 'Service Date', 'Student Worker',
                     'Job Code', 'Job_Title', 'Paygroup (Current)', 'Division', 'Salary/Hourly', 'Regular/Temp',
                     'Full/Part Time', 'Active/Leave Status', 'Home State', 'FLSA Status', 'File Number', 'Rehire Date',
                     'Colleague D Number', 'CoCode', 'Holiday Calendar', 'Time Zone', 'Authentication Type',
                     'Timesheet Approval Path', 'Time Off Approval Path', 'Timesheet Period Type', 'Timesheet Template',
                     'Time Off Template', 'Schedule Name (Current)', 'Batch ID', 'Work Week', 'supervisor uri', 'Pay Rule Name',
                     'Standard Hours', 'Department Number', 'Work Location']
        )

        parse_csv_user_data = rail.PythonOperator(
            task_id='parse_csv_user_data',
            python_callable=lambda: rail.load_all_records(
                rail.result('parse_csv'))[0]
        )

        if_firstname_blank = rail.IfOperator(
            task_id='if_firstname_blank',
            test="{{ result('parse_csv_user_data')['User First Name'] | sn | is_falsy }}",
            yes_task="write_firstnameblank_replicon",
            no_task="is_activeleavestatus_not_contains_t",
        )

        write_firstnameblank_replicon = rail.WriteLogOperator(
            task_id='write_firstnameblank_replicon',
            log='{{ dag_run.conf.log }}',
            message='User not updated as the user information is not available in the reference report. Please check if the schedule is assigned.',
            severity='Ignored',
            properties={
                'login_name': '{{ dag_run.conf.loginname }}',
                'status': 'Ignored',
                'failure_reason': 'User not updated as the user information is not available in the reference report. Please check if the schedule is assigned.'
            }
        )

        is_activeleavestatus_not_contains_t = rail.IfOperator(
            task_id='is_activeleavestatus_not_contains_t',
            test=lambda dag_run: 'T' not in dag_run.conf['activeleavestatus'],
            yes_task="get_mapper_entries",
            no_task="is_activeleavestatus_contains_t",
        )

        get_mapper_entries = rail.PythonOperator(
            task_id='get_mapper_entries',
            python_callable=python_callable_method.get_mapper_entries_from_adtalem_caribbean_mapperfile,
            op_args=["{{ dag_run.conf.paygroup }}",
                     "{{ dag_run.conf.jobcode }}"]
        )

        if_userstatus_is_disabled = rail.IfOperator(
            task_id='if_userstatus_is_disabled',
            test="{{ result('parse_csv_user_data')['User Status'] == 'Disabled' }}",
            yes_task="enable_userprofile",
            no_task="is_firstname_present",
        )

        enable_userprofile = rail.RepliconServiceOperator(
            task_id='enable_userprofile',
            endpoint="/services/securityservice1.svc/EnableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        is_firstname_present = rail.IfOperator(
            task_id='is_firstname_present',
            test="{{ dag_run.conf.firstname | is_truthy and dag_run.conf.firstname != \
                result('parse_csv_user_data')['User First Name'] }}",
            yes_task="update_firstname",
            no_task="is_startdate_present",
        )

        update_firstname = rail.RepliconServiceOperator(
            task_id='update_firstname',
            endpoint="/services/UserService1.svc/UpdateFirstName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "firstname": "{{ dag_run.conf.firstname }}"
            }
        )

        is_startdate_present = rail.IfOperator(
            task_id='is_startdate_present',
            test="{{ dag_run.conf.startdate | is_truthy and dag_run.conf.startdate != \
                result('parse_csv_user_data')['User Start Date'] }}",
            yes_task="update_startdate",
            no_task="if_startdate_present_terminationdate_not_present",
        )

        update_startdate = rail.RepliconServiceOperator(
            task_id='update_startdate',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": request_payload.get_datetime_obj(dag_run.conf['startdate'])
                }
            }
        )

        if_startdate_present_terminationdate_not_present = rail.IfOperator(
            task_id='if_startdate_present_terminationdate_not_present',
            test='{{ dag_run.conf.startdate | sn | is_truthy and \
                dag_run.conf.terminationdate | sn | is_falsy }}',
            yes_task="update_startdate_2",
            no_task="is_terminationdate_present",
        )

        update_startdate_2 = rail.RepliconServiceOperator(
            task_id='update_startdate_2',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": request_payload.get_datetime_obj(dag_run.conf['startdate'])
                }
            }
        )

        is_terminationdate_present = rail.IfOperator(
            task_id='is_terminationdate_present',
            test="{{ dag_run.conf.terminationdate | is_truthy and \
                result('parse_csv_user_data')['User End Date'] != dag_run.conf.terminationdate }}",
            yes_task="update_enddate",
            no_task="is_lastname_present",
        )

        update_enddate = rail.RepliconServiceOperator(
            task_id='update_enddate',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": request_payload.get_datetime_obj(dag_run.conf['startdate']),
                    "endDate": request_payload.get_datetime_obj(dag_run.conf['enddate'])
                }
            }
        )

        is_lastname_present = rail.IfOperator(
            task_id='is_lastname_present',
            test="{{ dag_run.conf.lastname | sn | is_truthy and \
                dag_run.conf.lastname != result('parse_csv_user_data')['User Last Name'] }}",
            yes_task="update_lastname",
            no_task="is_emailaddress_present",
        )

        update_lastname = rail.RepliconServiceOperator(
            task_id='update_lastname',
            endpoint="/services/UserService1.svc/UpdateLastName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "lastname": "{{ dag_run.conf.lastname }}"
            }
        )

        is_emailaddress_present = rail.IfOperator(
            task_id='is_emailaddress_present',
            test="{{ dag_run.conf.emailaddress | sn | is_truthy and \
                dag_run.conf.emailaddress != result('parse_csv_user_data')['User Email'] }}",
            yes_task="update_email",
            no_task="is_loginname_present",
        )

        update_email = rail.RepliconServiceOperator(
            task_id='update_email',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "email": "{{ dag_run.conf.emailaddress }}"
            }
        )

        is_loginname_present = rail.IfOperator(
            task_id='is_loginname_present',
            test="{{ dag_run.conf.loginname | is_truthy and \
                dag_run.conf.loginname != result('parse_csv_user_data')['Login Name'] and \
                    result('get_required_user_customfields').colleague_d_number | is_truthy }}",
            yes_task="update_colleaguednumber",
            no_task="is_division_present",
        )

        update_colleaguednumber = rail.RepliconServiceOperator(
            task_id='update_colleaguednumber',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').colleague_d_number }}",
                "value": "{{ dag_run.conf.loginname }}"
            }
        )

        is_division_present = rail.IfOperator(
            task_id='is_division_present',
            test="{{ dag_run.conf.division | sn | is_truthy and \
                dag_run.conf.division != result('parse_csv_user_data')['Division'] and \
                    result('get_required_user_customfields').division | is_truthy }}",
            yes_task="update_division_udf",
            no_task="is_jobfunction_present",
        )

        update_division_udf = rail.RepliconServiceOperator(
            task_id='update_division_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').division }}",
                "value": "{{ dag_run.conf.division }}"
            }
        )

        is_jobfunction_present = rail.IfOperator(
            task_id='is_jobfunction_present',
            test="{{ dag_run.conf.jobfunction | is_truthy and \
                    result('get_required_user_customfields').job_function | is_truthy }}",
            yes_task="update_job_function_udf",
            no_task="is_paygroup_present",
        )

        update_job_function_udf = rail.RepliconServiceOperator(
            task_id='update_job_function_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').job_function }}",
                "value": "{{ dag_run.conf.jobfunction }}"
            }
        )

        is_paygroup_present = rail.IfOperator(
            task_id='is_paygroup_present',
            test="{{ dag_run.conf.paygroup | is_truthy and \
                dag_run.conf.paygroup != result('parse_csv_user_data')['User Department Name'] }}",
            yes_task="get_paygroup_matching_department",
            no_task="get_existing_activities_to_be_removed",
        )

        get_paygroup_matching_department = rail.RepliconServiceOperator(
            task_id='get_paygroup_matching_department',
            endpoint="/services/DepartmentService1.svc/GetEnabledDepartments",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['paygroup'], 'uri')
        )

        is_department_uri_present = rail.IfOperator(
            task_id='is_department_uri_present',
            test="{{ result('get_paygroup_matching_department') | is_truthy }}",
            yes_task="update_department_user",
            no_task="get_existing_activities_to_be_removed",
        )

        update_department_user = rail.RepliconServiceOperator(
            task_id='update_department_user',
            endpoint="/services/DepartmentService1.svc/UpdateDepartmentForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "departmentUri": "{{ result('get_paygroup_matching_department') }}"
            }
        )

        get_existing_activities_to_be_removed = rail.RepliconServiceOperator(
            task_id='get_existing_activities_to_be_removed',
            endpoint="/services/ActivityService1.svc/GetActivityAssignmentsForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=response_filter.get_activities_to_remove
        )

        get_activities_from_mapper = rail.PythonOperator(
            task_id='get_activities_from_mapper',
            python_callable=python_callable_method.get_mapper_entry_value,
            op_args=['Activity']
        )

        is_activities_present = rail.IfOperator(
            task_id='is_activities_present',
            test="{{ result('get_activities_from_mapper') | is_truthy }}",
            yes_task="get_required_activity_uris",
            no_task="is_activities_to_remove",
        )

        get_required_activity_uris = rail.RepliconServiceOperator(
            task_id='get_required_activity_uris',
            endpoint="/services/ActivityService1.svc/GetAllActivities",
            data_handler=response_filter.get_required_activityuris
        )

        is_required_activities_present = rail.IfOperator(
            task_id='is_required_activities_present',
            test="{{ result('get_required_activity_uris') | is_truthy and \
                 result('get_required_activity_uris') != result('get_existing_activities_to_be_removed') }}",
            yes_task="update_activity_assignments",
            no_task="is_activities_to_remove",
        )

        update_activity_assignments = rail.RepliconServiceOperator(
            task_id='update_activity_assignments',
            endpoint="/services/ActivityService1.svc/UpdateActivityAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "activityUris": rail.result('get_required_activity_uris')
            }
        )

        is_activities_to_remove = rail.IfOperator(
            task_id='is_activities_to_remove',
            test="{{ result('get_activities_from_mapper') | is_falsy and \
                result('get_existing_activities_to_be_removed') | is_truthy }}",
            yes_task="remove_activity_assignments",
            no_task="get_required_policysets_to_assign",
        )

        remove_activity_assignments = rail.RepliconServiceOperator(
            task_id='remove_activity_assignments',
            endpoint="/services/ActivityService1.svc/RemoveActivityAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "activityUris": rail.result('get_existing_activities_to_be_removed')
            }
        )

        get_required_policysets_to_assign = rail.RepliconServiceOperator(
            task_id='get_required_policysets_to_assign',
            endpoint='/services/PolicySetService1.svc/GetAllPolicySets',
            data_handler=python_callable_method.get_required_policysets
        )

        is_policysets_to_assign = rail.IfOperator(
            task_id='is_policysets_to_assign',
            test="{{ result('get_required_policysets_to_assign') | length > 0 }}",
            yes_task="update_templates_for_user",
            no_task="get_required_timesheet_approvalpath",
        )

        update_templates_for_user = rail.RepliconServiceOperator(
            task_id='update_templates_for_user',
            endpoint="/services/PolicySetService1.svc/PutPolicySetAssignmentsForUser",
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'policySetUris': rail.result('get_required_policysets_to_assign')
            }
        )

        get_required_timesheet_approvalpath = rail.RepliconServiceOperator(
            task_id='get_required_timesheet_approvalpath',
            endpoint='/services/TimesheetApprovalService1.svc/GetAllApprovalPaths',
            data_handler=lambda response: python_callable_method.get_required_approvalpaths_updateuser(
                response, 'Timesheet Approval', rail.result(
                    'parse_csv_user_data')['Timesheet Approval Path'])
        )

        should_update_timesheet_approvalpath = rail.IfOperator(
            task_id='should_update_timesheet_approvalpath',
            test="{{ result('get_required_timesheet_approvalpath') | is_truthy }}",
            yes_task='update_timesheet_approvalpath_user',
            no_task='get_required_timeoff_approvalpath'
        )

        update_timesheet_approvalpath_user = rail.RepliconServiceOperator(
            task_id='update_timesheet_approvalpath_user',
            endpoint='/services/TimesheetApprovalService1.svc/UpdateApprovalPathForUser',
            data={
                'userUri': "{{ dag_run.conf.useruri }}",
                'approvalPathUri': "{{ result('get_required_timesheet_approvalpath') }}"
            }
        )

        get_required_timeoff_approvalpath = rail.RepliconServiceOperator(
            task_id='get_required_timeoff_approvalpath',
            endpoint='/services/TimeOffApprovalService1.svc/GetAllApprovalPaths',
            data_handler=lambda response: python_callable_method.get_required_approvalpaths_updateuser(
                response, 'Timeoff Approval', rail.result(
                    'parse_csv_user_data')['Time Off Approval Path'])
        )

        should_update_timeoff_approvalpath = rail.IfOperator(
            task_id='should_update_timeoff_approvalpath',
            test="{{ result('get_required_timeoff_approvalpath') | is_truthy }}",
            yes_task='update_timeoff_approvalpath_user',
            no_task='get_employeetype'
        )

        update_timeoff_approvalpath_user = rail.RepliconServiceOperator(
            task_id='update_timeoff_approvalpath_user',
            endpoint='/services/TimeOffApprovalService1.svc/UpdateApprovalPathForUser',
            data={
                'userUri': "{{ dag_run.conf.useruri }}",
                'approvalPathUri': "{{ result('get_required_timeoff_approvalpath') }}"
            }
        )

        get_employeetype = rail.PythonOperator(
            task_id='get_employeetype',
            python_callable=python_callable_method.get_employeetype_details,
            op_args=['{{ dag_run.conf.salaryhourly }}']
        )

        is_get_employeetype_present = rail.IfOperator(
            task_id='is_get_employeetype_present',
            test="{{ result('get_employeetype') | is_truthy and \
                result('get_employeetype') != result('parse_csv_user_data')['Employee Type'] }}",
            yes_task="get_required_employeetypeuri",
            no_task="get_required_timesheet_period",
        )

        get_required_employeetypeuri = rail.RepliconServiceOperator(
            task_id='get_required_employeetypeuri',
            endpoint='/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails',
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response, 'displayText', rail.result(
                'get_employeetype'), 'uri', '')
        )

        is_required_employeetype_present = rail.IfOperator(
            task_id='is_required_employeetype_present',
            test="{{ result('get_required_employeetypeuri') | is_truthy }}",
            yes_task="update_employeetype",
            no_task="get_required_timesheet_period",
        )

        update_employeetype = rail.RepliconServiceOperator(
            task_id='update_employeetype',
            endpoint="/services/EmployeeTypeService1.svc/UpdateEmployeeTypeForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "employeeTypeUri": "{{ result('get_required_employeetypeuri') }}"
            }
        )

        get_required_timesheet_period = rail.PythonOperator(
            task_id='get_required_timesheet_period',
            python_callable=python_callable_method.get_mapper_entry_value,
            op_args=['Timesheet Period']
        )

        is_timesheetperiod_system = rail.IfOperator(
            task_id='is_timesheetperiod_system',
            test="{{ result('get_required_timesheet_period') | is_truthy and result('get_required_timesheet_period') == 'System' and \
                result('get_required_timesheet_period') != result('parse_csv_user_data')['Timesheet Period Type'] }}",
            yes_task='put_system_timesheetperiod_uri',
            no_task='is_timesheetperiod_employeetype'
        )

        put_system_timesheetperiod_uri = rail.RepliconServiceOperator(
            task_id='put_system_timesheetperiod_uri',
            endpoint='/services/TimesheetPeriodService1.svc/UpdateTimesheetPeriodTypeForUser',
            data={
                'userUri': "{{ dag_run.conf.useruri }}",
                'approvalPathUri': "urn:replicon:timesheet-period-type:system"
            }
        )

        is_timesheetperiod_employeetype = rail.IfOperator(
            task_id='is_timesheetperiod_employeetype',
            test="{{ result('get_required_timesheet_period') | is_truthy and result('get_required_timesheet_period') == 'Employee Type' and \
                 result('get_required_timesheet_period') != result('parse_csv_user_data')['Timesheet Period Type'] }}",
            yes_task='put_employeetype_timesheetperiod_uri',
            no_task='is_timesheetperiod_department'
        )

        put_employeetype_timesheetperiod_uri = rail.RepliconServiceOperator(
            task_id='put_employeetype_timesheetperiod_uri',
            endpoint='/services/TimesheetPeriodService1.svc/UpdateTimesheetPeriodTypeForUser',
            data={
                'userUri': "{{ dag_run.conf.useruri }}",
                'timesheetPeriodTypeUri': "urn:replicon:timesheet-period-type:based-on-employee-type-assignment"
            }
        )

        is_timesheetperiod_department = rail.IfOperator(
            task_id='is_timesheetperiod_department',
            test="{{ result('get_required_timesheet_period') | \
                is_truthy and result('get_required_timesheet_period') == 'Department' and \
                    result('get_required_timesheet_period') != result('parse_csv_user_data')['Timesheet Period Type'] }}",
            yes_task='put_department_timesheet_period',
            no_task='is_activeleavestatus_present'
        )

        put_department_timesheet_period = rail.RepliconServiceOperator(
            task_id='put_department_timesheet_period',
            endpoint='/services/TimesheetPeriodService1.svc/UpdateTimesheetPeriodTypeForUser',
            data={
                'userUri': "{{ dag_run.conf.useruri }}",
                'timesheetPeriodTypeUri': "urn:replicon:timesheet-period-type:based-on-department-assignment"
            }
        )

        is_activeleavestatus_present = rail.IfOperator(
            task_id='is_activeleavestatus_present',
            test="{{ dag_run.conf.activeleavestatus != result('parse_csv_user_data')['Active/Leave Status'] and \
                result('get_required_user_customfields').active_leave_status | is_truthy}}",
            yes_task="get_activeleavestatus_dropdown",
            no_task="is_flsa_status_present",
        )

        get_activeleavestatus_dropdown = rail.RepliconServiceOperator(
            task_id='get_activeleavestatus_dropdown',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_required_user_customfields').active_leave_status }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf[
                'activeleavestatus'], 'uri', '')
        )

        if_activeleavestatus_dropdown_present = rail.IfOperator(
            task_id='if_activeleavestatus_dropdown_present',
            test="{{ result('get_activeleavestatus_dropdown') | is_truthy }}",
            yes_task="update_activeleavestatus_udf",
            no_task="is_flsa_status_present",
        )

        update_activeleavestatus_udf = rail.RepliconServiceOperator(
            task_id='update_activeleavestatus_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').active_leave_status }}",
                "customFieldDropDownOptionUri": "{{ result('get_activeleavestatus_dropdown') }}"
            }
        )

        is_flsa_status_present = rail.IfOperator(
            task_id='is_flsa_status_present',
            test="{{ dag_run.conf.flsastatus | is_truthy and \
                dag_run.conf.flsastatus != result('parse_csv_user_data')['FLSA Status'] and \
                    result('get_required_user_customfields').flsa_status | is_truthy }}",
            yes_task="get_flsastatus_dropdown",
            no_task="is_salaryhourly_present",
        )

        get_flsastatus_dropdown = rail.RepliconServiceOperator(
            task_id='get_flsastatus_dropdown',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_required_user_customfields').flsa_status }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf[
                'flsastatus'], 'uri', '')
        )

        is_flsa_dropdown_present = rail.IfOperator(
            task_id='is_flsa_dropdown_present',
            test="{{ result('get_flsastatus_dropdown') | is_truthy }}",
            yes_task="update_flsa_status_udf",
            no_task="is_salaryhourly_present",
        )

        update_flsa_status_udf = rail.RepliconServiceOperator(
            task_id='update_flsa_status_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').flsa_status }}",
                "customFieldDropDownOptionUri": "{{ result('get_flsastatus_dropdown') }}"
            }
        )

        is_salaryhourly_present = rail.IfOperator(
            task_id='is_salaryhourly_present',
            test="{{ dag_run.conf.salaryhourly | is_truthy and \
                dag_run.conf.salaryhourly != result('parse_csv_user_data')['Salary/Hourly'] and \
                    result('get_required_user_customfields').salary_hourly | is_truthy }}",
            yes_task="get_salaryhourly_dropdown",
            no_task="is_regulartemp_present",
        )

        get_salaryhourly_dropdown = rail.RepliconServiceOperator(
            task_id='get_salaryhourly_dropdown',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_required_user_customfields').salary_hourly }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf[
                'salaryhourly'], 'uri', '')
        )

        is_salaryhourly_dropdown_present = rail.IfOperator(
            task_id='is_salaryhourly_dropdown_present',
            test="{{ result('get_salaryhourly_dropdown') | is_truthy }}",
            yes_task="update_salaryhourly_udf",
            no_task="is_regulartemp_present",
        )

        update_salaryhourly_udf = rail.RepliconServiceOperator(
            task_id='update_salaryhourly_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').salary_hourly }}",
                "customFieldDropDownOptionUri": "{{ result('get_salaryhourly_dropdown') }}"
            }
        )

        is_repliconuser_salaryhourly_present = rail.IfOperator(
            task_id='is_repliconuser_salaryhourly_present',
            test="{{ result('parse_csv_user_data')['Salary/Hourly'] | is_truthy and \
                result('parse_csv_user_data')['Salary/Hourly'] | lower == 'h' }}",
            yes_task="trigger_delete_timesheet_child_dag",
            no_task="is_regulartemp_present",
        )

        trigger_delete_timesheet_child_dag = rail.TriggerDagRunOperator(
            task_id='trigger_delete_timesheet_child_dag',
            retries=0,
            trigger_dag_id=f'adtalem_userimport_child_delete_timesheet_v1.0_{config.instance}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            conf={
                'useruri': '{{ dag_run.conf.useruri }}',
                'enddate': '{{ current_time("%d/%m/%Y") }}',
                'effectivedate': '{{ dag_run.conf.effectivedate }}'
            }
        )

        is_regulartemp_present = rail.IfOperator(
            task_id='is_regulartemp_present',
            test="{{ dag_run.conf.regulartemp | is_truthy and \
                dag_run.conf.regulartemp != result('parse_csv_user_data')['Regular/Temp'] and \
                    result('get_required_user_customfields').regular_temp | is_truthy }}",
            yes_task="get_regulartemp_customfield_dropdown",
            no_task="is_fullparttime_present",
        )

        get_regulartemp_customfield_dropdown = rail.RepliconServiceOperator(
            task_id='get_regulartemp_customfield_dropdown',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_required_user_customfields').regular_temp }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf[
                'regulartemp'], 'uri', '')
        )

        is_regulartemp_dropdown_present = rail.IfOperator(
            task_id='is_regulartemp_dropdown_present',
            test="{{ result('get_regulartemp_customfield_dropdown') | is_truthy }}",
            yes_task="update_regulartemp_udf",
            no_task="is_fullparttime_present",
        )

        update_regulartemp_udf = rail.RepliconServiceOperator(
            task_id='update_regulartemp_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').regular_temp }}",
                "customFieldDropDownOptionUri": "{{ result('get_regulartemp_customfield_dropdown') }}"
            }
        )

        is_fullparttime_present = rail.IfOperator(
            task_id='is_fullparttime_present',
            test="{{ dag_run.conf.fullparttime | is_truthy and \
                dag_run.conf.fullparttime != result('parse_csv_user_data')['Full/Part Time'] and \
                    result('get_required_user_customfields').full_part_time | is_truthy }}",
            yes_task="get_fullparttime_dropdown",
            no_task="is_homestate_present",
        )

        get_fullparttime_dropdown = rail.RepliconServiceOperator(
            task_id='get_fullparttime_dropdown',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_required_user_customfields').full_part_time }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf[
                'fullparttime'], 'uri', '')
        )

        is_fullparttime_dropdown_present = rail.IfOperator(
            task_id='is_fullparttime_dropdown_present',
            test="{{ result('get_fullparttime_dropdown') | is_truthy }}",
            yes_task="update_fullparttime_udf",
            no_task="is_homestate_present",
        )

        update_fullparttime_udf = rail.RepliconServiceOperator(
            task_id='update_fullparttime_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').full_part_time }}",
                "customFieldDropDownOptionUri": "{{ result('get_fullparttime_dropdown') }}"
            }
        )

        is_homestate_present = rail.IfOperator(
            task_id='is_homestate_present',
            test="{{ dag_run.conf.homestate | is_truthy and \
                dag_run.conf.homestate != result('parse_csv_user_data')['Home State'] and \
                    result('get_required_user_customfields').home_state | is_truthy }}",
            yes_task="get_homestate_customfield_dropdown",
            no_task="is_jobcode_present",
        )

        get_homestate_customfield_dropdown = rail.RepliconServiceOperator(
            task_id='get_homestate_customfield_dropdown',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_required_user_customfields').home_state }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf[
                'homestate'], 'uri', '')
        )

        is_homestate_customfield_dropdown_present = rail.IfOperator(
            task_id='is_homestate_customfield_dropdown_present',
            test="{{ result('get_homestate_customfield_dropdown') | is_truthy }}",
            yes_task="update_homestate_udf",
            no_task="is_jobcode_present",
        )

        update_homestate_udf = rail.RepliconServiceOperator(
            task_id='update_homestate_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').home_state }}",
                "customFieldDropDownOptionUri": "{{ result('get_homestate_customfield_dropdown') }}"
            }
        )

        is_jobcode_present = rail.IfOperator(
            task_id='is_jobcode_present',
            test="{{ dag_run.conf.jobcode | is_truthy and \
                dag_run.conf.jobcode != result('parse_csv_user_data')['Job Code'] and \
                result('get_required_user_customfields').job_code | is_truthy }}",
            yes_task="get_jobcode_dropdown",
            no_task="required_batchid",
        )

        def set_jobcode_dropdown_result(response, dag_run):
            rail.set_result(response, 'dropdowns')
            return rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf[
                'jobcode'], 'uri', '')
        get_jobcode_dropdown = rail.RepliconServiceOperator(
            task_id='get_jobcode_dropdown',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_required_user_customfields').job_code }}"
            },
            data_handler=set_jobcode_dropdown_result
        )

        is_jobcode_dropdown_present = rail.IfOperator(
            task_id='is_jobcode_dropdown_present',
            test="{{ result('get_jobcode_dropdown') | is_truthy }}",
            yes_task="update_jobcode_udf",
            no_task="put_jobcode_dropdowns",
        )

        update_jobcode_udf = rail.RepliconServiceOperator(
            task_id='update_jobcode_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').job_code }}",
                "customFieldDropDownOptionUri": "{{ result('get_jobcode_dropdown') }}"
            }
        )

        put_jobcode_dropdowns = rail.RepliconServiceOperator(
            task_id='put_jobcode_dropdowns',
            endpoint="/services/CustomFieldService1.svc/PutDropDownOptions",
            data=request_payload.get_putdropdownoption_jobcode
        )

        get_jobcode_dropdown2 = rail.RepliconServiceOperator(
            task_id='get_jobcode_dropdown2',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_required_user_customfields').job_code }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf[
                'jobcode'], 'uri', '')
        )

        is_jobcode_dropdown_present2 = rail.IfOperator(
            task_id='is_jobcode_dropdown_present2',
            test="{{ result('get_jobcode_dropdown2') | is_truthy }}",
            yes_task="update_jobcode_udf2",
            no_task="required_batchid",
        )

        update_jobcode_udf2 = rail.RepliconServiceOperator(
            task_id='update_jobcode_udf2',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').job_code }}",
                "customFieldDropDownOptionUri": "{{ result('get_jobcode_dropdown2') }}"
            }
        )

        required_batchid = rail.PythonOperator(
            task_id='required_batchid',
            python_callable=request_payload.get_batchid,
            op_args=['{{ dag_run.conf.salaryhourly }}',
                     '{{ dag_run.conf.paygroup }}']
        )

        is_batchid_udf_present = rail.IfOperator(
            task_id='is_batchid_udf_present',
            test="{{ result('get_required_user_customfields').batch_id | is_truthy and \
                result('required_batchid') != result('parse_csv_user_data')['Batch ID'] }}",
            yes_task="update_batchid_udf",
            no_task="is_standardhours_present",
        )

        update_batchid_udf = rail.RepliconServiceOperator(
            task_id='update_batchid_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').batch_id}}",
                "value": "{{ result('required_batchid') }}"
            }
        )

        is_standardhours_present = rail.IfOperator(
            task_id='is_standardhours_present',
            test="{{ dag_run.conf.standardhours | is_truthy and \
                dag_run.conf.standardhours != result('parse_csv_user_data')['Standard Hours'] and \
                    result('get_required_user_customfields').standard_hours | is_truthy }}",
            yes_task="update_standardhours_udf",
            no_task="is_worklocation_present",
        )

        update_standardhours_udf = rail.RepliconServiceOperator(
            task_id='update_standardhours_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').standard_hours }}",
                "value": "{{ dag_run.conf.standardhours }}"
            }
        )

        is_worklocation_present = rail.IfOperator(
            task_id='is_worklocation_present',
            test="{{ dag_run.conf.worklocation | is_truthy \
                and dag_run.conf.worklocation != result('parse_csv_user_data')['Work Location'] and \
                result('get_required_user_customfields').work_location | is_truthy }}",
            yes_task="update_worklocation_udf",
            no_task="is_departmentnumber_present",
        )

        update_worklocation_udf = rail.RepliconServiceOperator(
            task_id='update_worklocation_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').work_location }}",
                "value": "{{ dag_run.conf.worklocation }}"
            }
        )

        is_departmentnumber_present = rail.IfOperator(
            task_id='is_departmentnumber_present',
            test="{{ dag_run.conf.departmentnumber | is_truthy \
                and dag_run.conf.departmentnumber != result('parse_csv_user_data')['Department Number'] and \
                result('get_required_user_customfields').department_number | is_truthy }}",
            yes_task="update_department_udf",
            no_task="is_filenumber_present",
        )

        update_department_udf = rail.RepliconServiceOperator(
            task_id='update_department_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').department_number }}",
                "value": "{{ dag_run.conf.departmentnumber }}"
            }
        )

        is_filenumber_present = rail.IfOperator(
            task_id='is_filenumber_present',
            test="{{ dag_run.conf.filenumber | is_truthy \
                and dag_run.conf.filenumber != result('parse_csv_user_data')['File Number'] and \
                    result('get_required_user_customfields').file_number | is_truthy }}",
            yes_task="update_filenumber",
            no_task="is_jobtitle_present",
        )

        update_filenumber = rail.RepliconServiceOperator(
            task_id='update_filenumber',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').file_number }}",
                "value": "{{ dag_run.conf.filenumber }}"
            }
        )

        is_jobtitle_present = rail.IfOperator(
            task_id='is_jobtitle_present',
            test="{{ dag_run.conf.jobtitle | is_truthy and dag_run.conf.jobtitle != result('parse_csv_user_data')['Job_Title'] and \
                result('get_required_user_customfields').job_title | is_truthy }}",
            yes_task="update_job_title_udf",
            no_task="if_rehiredate_present",
        )

        update_job_title_udf = rail.RepliconServiceOperator(
            task_id='update_job_title_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').job_title }}",
                "value": "{{ dag_run.conf.jobtitle }}"
            }
        )

        if_rehiredate_present = rail.IfOperator(
            task_id='if_rehiredate_present',
            test="{{ dag_run.conf.rehiredate | is_truthy and \
                dag_run.conf.rehiredate != result('parse_csv_user_data')['Rehire Date'] and \
                    result('get_required_user_customfields').rehire_date | is_truthy }}",
            yes_task="update_rehire_date_udf",
            no_task="is_servicedate_present",
        )

        update_rehire_date_udf = rail.RepliconServiceOperator(
            task_id='update_rehire_date_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['useruri'],
                "customFieldUri": rail.result('get_required_user_customfields')['rehire_date'],
                "value": request_payload.get_datetime_obj(dag_run.conf['rehiredate'])
            }
        )

        is_servicedate_present = rail.IfOperator(
            task_id='is_servicedate_present',
            test="{{ dag_run.conf.servicedate | is_truthy and \
                dag_run.conf.servicedate != result('parse_csv_user_data')['Service Date'] and \
                    result('get_required_user_customfields').service_date | is_truthy }}",
            yes_task="update_service_date_udf",
            no_task="is_loginname_present2",
        )

        update_service_date_udf = rail.RepliconServiceOperator(
            task_id='update_service_date_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['useruri'],
                "customFieldUri": rail.result('get_required_user_customfields')['service_date'],
                "value": request_payload.get_datetime_obj(dag_run.conf['servicedate'])
            }
        )

        is_loginname_present2 = rail.IfOperator(
            task_id='is_loginname_present2',
            test="{{ dag_run.conf.loginname | is_truthy and \
                dag_run.conf.loginname != result('parse_csv_user_data')['Colleague D Number'] and \
                    result('get_required_user_customfields').colleague_d_number | is_truthy }}",
            yes_task="update_colleaguednumber_udf",
            no_task="is_paygroup_present2",
        )

        update_colleaguednumber_udf = rail.RepliconServiceOperator(
            task_id='update_colleaguednumber_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').colleague_d_number }}",
                "value": "{{ dag_run.conf.loginname }}"
            }
        )

        is_paygroup_present2 = rail.IfOperator(
            task_id='is_paygroup_present2',
            test="{{ dag_run.conf.paygroup | is_truthy and \
                dag_run.conf.paygroup != result('parse_csv_user_data')['Paygroup (Current)'] and \
                    result('get_required_user_customfields').co_code | is_truthy }}",
            yes_task="update_cocode_udf",
            no_task="should_update_supervisor",
        )

        update_cocode_udf = rail.RepliconServiceOperator(
            task_id='update_cocode_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').co_code }}",
                "value": "{{ dag_run.conf.paygroup }}"
            }
        )

        (should_update_supervisor,
         finish_supervisor_assignment) = process_supervisor_assignment_task_group(
            is_update_user=True, caribbean_user_import=True)

        is_paygroup_present_3 = rail.IfOperator(
            task_id='is_paygroup_present_3',
            test="{{ dag_run.conf.paygroup | is_truthy and \
                dag_run.conf.paygroup != result('parse_csv_user_data')['Paygroup (Current)'] }}",
            yes_task="is_timesheettemplateuri_present",
            no_task="get_holidaycalendar_from_mapper"
        )

        is_timesheettemplateuri_present = rail.IfOperator(
            task_id='is_timesheettemplateuri_present',
            test="{{ result('get_required_policysets_to_assign', 'timesheet_template_uri') | is_truthy }}",
            yes_task="get_timesheet_for_date2",
            no_task="get_locationuri_to_assign",
        )

        get_timesheet_for_date2 = rail.RepliconServiceOperator(
            task_id='get_timesheet_for_date2',
            endpoint="/services/TimesheetService1.svc/GetTimesheetForDate2",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "date": request_payload.get_today_date(),
                "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
            }
        )

        get_timesheet_details = rail.RepliconServiceOperator(
            task_id='get_timesheet_details',
            endpoint="/services/TimesheetService1.svc/GetTimesheetDetails",
            data={
                "timesheetUri": "{{ result('get_timesheet_for_date2').timesheet.uri }}"
            }
        )

        get_locationuri_to_assign = rail.RepliconServiceOperator(
            task_id='get_locationuri_to_assign',
            endpoint="/services/LocationService1.svc/GetAllLocations",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['paygroup'], 'uri', '')
        )

        get_locationschedule_user = rail.RepliconServiceOperator(
            task_id='get_locationschedule_user',
            endpoint="/services/LocationService1.svc/GetLocationScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=response_filter.get_putlocationschedule_user
        )

        put_location_schedule_user = rail.RepliconServiceOperator(
            task_id='put_location_schedule_user',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": rail.result('get_locationschedule_user')
            }
        )

        get_holidaycalendar_from_mapper = rail.PythonOperator(
            task_id='get_holidaycalendar_from_mapper',
            python_callable=python_callable_method.get_mapper_entry_value,
            op_args=['Holiday Calender']
        )

        is_holidaycalendar_mapper_present = rail.IfOperator(
            task_id='is_holidaycalendar_mapper_present',
            test="{{ result('get_holidaycalendar_from_mapper') | is_truthy }}",
            yes_task="get_required_holiday_calendaruri",
            no_task="get_required_payrule_from_mapper",
        )

        get_required_holiday_calendaruri = rail.RepliconServiceOperator(
            task_id='get_required_holiday_calendaruri',
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'name', rail.result('get_holidaycalendar_from_mapper'), 'uri', '')
        )

        is_holidaycalendar_uri_present = rail.IfOperator(
            task_id='is_holidaycalendar_uri_present',
            test="{{ result('get_required_holiday_calendaruri') | is_truthy }}",
            yes_task="update_holiday_calendar",
            no_task="get_required_payrule_from_mapper",
        )

        update_holiday_calendar = rail.RepliconServiceOperator(
            task_id='update_holiday_calendar',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "holidayCalendarUri": "{{ result('get_required_holiday_calendaruri') }}"
            }
        )

        get_required_payrule_from_mapper = rail.PythonOperator(
            task_id='get_required_payrule_from_mapper',
            python_callable=python_callable_method.get_mapper_entry_value,
            op_args=['Payrule']
        )

        is_required_payrule_present = rail.IfOperator(
            task_id='is_required_payrule_present',
            test="{{ result('get_required_payrule_from_mapper') | is_truthy and \
                result('get_required_payrule_from_mapper') != result('parse_csv_user_data')['Pay Rule Name'] }}",
            yes_task="get_required_payrulescript_name_uri",
            no_task="get_required_timezone",
        )

        get_required_payrulescript_name_uri = rail.RepliconServiceOperator(
            task_id='get_required_payrulescript_name_uri',
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts",
            data_handler=lambda response: {
                'uri': rail.find_first_by_attr_and_get_attr(
                    response, 'name', rail.result('get_required_payrule_from_mapper'), 'uri', ''),
                'name': rail.find_first_by_attr_and_get_attr(
                    response, 'name', rail.result('get_required_payrule_from_mapper'), 'displayText', '')
            }
        )

        get_payrule_schedule_entries = rail.RepliconServiceOperator(
            task_id='get_payrule_schedule_entries',
            endpoint="/services/PayRuleScriptService2.svc/GetPayRuleScriptAssignmentScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=response_filter.get_putpayrulescheduleentries_user
        )

        is_payrule_schedule_entries_present = rail.IfOperator(
            task_id='is_payrule_schedule_entries_present',
            test="{{ result('get_payrule_schedule_entries') | is_truthy }}",
            yes_task="put_payrulescript_assignment_schedule_user",
            no_task="get_required_timezone",
        )

        put_payrulescript_assignment_schedule_user = rail.RepliconServiceOperator(
            task_id='put_payrulescript_assignment_schedule_user',
            endpoint="/services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": rail.result('get_payrule_schedule_entries')
            }
        )

        get_required_timezone = rail.PythonOperator(
            task_id='get_required_timezone',
            python_callable=python_callable_method.get_timezone_mapper_entry,
            op_args=['{{ dag_run.conf.homestate }}']
        )

        is_timezone_present = rail.IfOperator(
            task_id='is_timezone_present',
            test="{{ result('get_required_timezone') | is_truthy and \
                result('get_required_timezone') != result('parse_csv_user_data')['Time Zone'] }}",
            yes_task="update_timezone_user",
            no_task="is_managerindicator_y",
        )

        update_timezone_user = rail.RepliconServiceOperator(
            task_id='update_timezone_user',
            endpoint="/services/InternationalizationService1.svc/UpdateTimeZoneForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "timeZoneUri": "{{ result('get_required_timezone') }}"
            }
        )

        is_managerindicator_y = rail.IfOperator(
            task_id='is_managerindicator_y',
            test="{{ dag_run.conf.managerindicator | is_truthy and \
                dag_run.conf.managerindicator == 'Y' }}",
            yes_task="get_permissions_to_assign",
            no_task="is_jobcode_present2",
        )

        get_permissions_to_assign = rail.RepliconServiceOperator(
            task_id='get_permissions_to_assign',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                'userUri': "{{ dag_run.conf.useruri }}"
            },
            data_handler=response_filter.get_permissions_to_assign_updateuser
        )

        is_permission_to_assign = rail.IfOperator(
            task_id='is_permission_to_assign',
            test="{{ result('get_permissions_to_assign') | length > 0 }}",
            yes_task="put_permissions_user",
            no_task="is_jobcode_present2",
        )

        put_permissions_user = rail.RepliconServiceOperator(
            task_id='put_permissions_user',
            endpoint="/services/PermissionSetService1.svc/PutPermissionSetAssignmentsForUser",
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                "permissionSetUris": rail.result('get_permissions_to_assign')
            }
        )

        is_jobcode_present2 = rail.IfOperator(
            task_id='is_jobcode_present2',
            test='{{ dag_run.conf.jobcode | is_truthy }}',
            yes_task="is_studentworker_udf_present",
            no_task="get_required_schedule_type",
        )

        is_studentworker_udf_present = rail.IfOperator(
            task_id='is_studentworker_udf_present',
            test="{{ result('get_required_user_customfields').student_worker | is_truthy }}",
            yes_task="get_studentworker_dropdown",
            no_task="get_required_schedule_type",
        )

        get_studentworker_dropdown = rail.RepliconServiceOperator(
            task_id='get_studentworker_dropdown',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_required_user_customfields').student_worker }}"
            },
            data_handler=response_filter.get_studentworker_dropdown_uri
        )

        is_studentworker_dropdown_present = rail.IfOperator(
            task_id='is_studentworker_dropdown_present',
            test="{{ result('get_studentworker_dropdown') | is_truthy }}",
            yes_task="update_student_worker_udf",
            no_task="get_required_schedule_type",
        )

        update_student_worker_udf = rail.RepliconServiceOperator(
            task_id='update_student_worker_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').student_worker }}",
                "customFieldDropDownOptionUri": "{{ result('get_studentworker_dropdown') }}"
            }
        )

        get_required_schedule_type = rail.PythonOperator(
            task_id='get_required_schedule_type',
            python_callable=python_callable_method.get_mapper_entry_value,
            op_args=['Schedule Type']
        )

        is_not_shift_schedule = rail.IfOperator(
            task_id='is_not_shift_schedule',
            test="{{ result('get_required_schedule_type') != 'Shift Schedule' }}",
            yes_task="assign_office_schedule",
            no_task="assign_shift_schedule",
        )

        assign_office_schedule = rail.RepliconServiceOperator(
            task_id='assign_office_schedule',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "scheduleEntries": [
                    {
                        "schedulePolicy": {
                            "name": "{{ result('get_required_schedule_type') }}",
                            "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                        }
                    }
                ]
            }
        )

        assign_shift_schedule = rail.RepliconServiceOperator(
            task_id='assign_shift_schedule',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "scheduleEntries": [
                    {
                        "schedulePolicy": {
                            "scheduleTypeUri": "urn:replicon:schedule-type:shift"
                        }
                    }
                ]
            }
        )

        is_jobcode_paygroup_not_same = rail.IfOperator(
            task_id='is_jobcode_paygroup_not_same',
            test="{{ dag_run.conf.jobcode != result('parse_csv_user_data')['Job Code']  or \
                dag_run.conf.paygroup != result('parse_csv_user_data')['User Department Name'] }}",
            yes_task="trigger_timeoff_updateuser_caribbean",
            no_task="is_activeleavestatus_notmatches_t_timeoff",
        )

        trigger_timeoff_updateuser_caribbean = rail.TriggerDagRunOperator(
            task_id='trigger_timeoff_updateuser_caribbean',
            retries=0,
            trigger_dag_id=f'adtalem_userimport_caribbean_child_timeoff_update_user_{config.instance}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            conf={
                "lastname": "{{ dag_run.conf.lastname }}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "jobcode": "{{ dag_run.conf.jobcode }}",
                "jobtitle": "{{ dag_run.conf.jobtitle }}",
                "managerindicator": "{{ dag_run.conf.managerindicator }}",
                "paygroup": "{{ dag_run.conf.paygroup }}",
                "division": "{{ dag_run.conf.division }}",
                "salaryhourly": "{{ dag_run.conf.salaryhourly }}",
                "regulartemp": "{{ dag_run.conf.regulartemp }}",
                "fullparttime": "{{ dag_run.conf.fullparttime }}",
                "activeleavestatus": "{{ dag_run.conf.activeleavestatus }}",
                "supervisor": "{{ dag_run.conf.supervisor }}",
                "emailaddress": "{{ dag_run.conf.emailaddress }}",
                "homestate": "{{ dag_run.conf.homestate }}",
                "standardhours": "{{ dag_run.conf.standardhours }}",
                "flsastatus": "{{ dag_run.conf.flsastatus }}",
                "filenumber": "{{ dag_run.conf.filenumber }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "rehiredate": "{{ dag_run.conf.rehiredate }}",
                "servicedate": "{{ dag_run.conf.servicedate }}",
                "colleaguednumber": "{{ dag_run.conf.colleaguednumber }}",
                "newmapperlookup": "{{ dag_run.conf.jobcode }}",
                "worklocation": "{{ dag_run.conf.worklocation }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "terminationdate": "{{ dag_run.conf.terminationdate }}",
                "userstatus": "{{ result('parse_csv_user_data')['User Status'] }}"
            }
        )

        is_activeleavestatus_notmatches_t_timeoff = rail.IfOperator(
            task_id='is_activeleavestatus_notmatches_t_timeoff',
            test="{{ result('parse_csv_user_data')['User Status'] == 'Disabled' and \
                dag_run.conf.activeleavestatus | matches('T') | is_falsy }}",
            yes_task="trigger_timeoff_updateuser_caribbean2",
            no_task="log_user_updated",
        )

        trigger_timeoff_updateuser_caribbean2 = rail.TriggerDagRunOperator(
            task_id='trigger_timeoff_updateuser_caribbean2',
            retries=0,
            trigger_dag_id=f'adtalem_userimport_caribbean_child_timeoff_update_user_{config.instance}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            conf={
                "lastname": "{{ dag_run.conf.lastname }}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "jobcode": "{{ dag_run.conf.jobcode }}",
                "jobtitle": "{{ dag_run.conf.jobtitle }}",
                "managerindicator": "{{ dag_run.conf.managerindicator }}",
                "paygroup": "{{ dag_run.conf.paygroup }}",
                "division": "{{ dag_run.conf.division }}",
                "salaryhourly": "{{ dag_run.conf.salaryhourly }}",
                "regulartemp": "{{ dag_run.conf.regulartemp }}",
                "fullparttime": "{{ dag_run.conf.fullparttime }}",
                "activeleavestatus": "{{ dag_run.conf.activeleavestatus }}",
                "supervisor": "{{ dag_run.conf.supervisor }}",
                "emailaddress": "{{ dag_run.conf.emailaddress }}",
                "homestate": "{{ dag_run.conf.homestate }}",
                "standardhours": "{{ dag_run.conf.standardhours }}",
                "flsastatus": "{{ dag_run.conf.flsastatus }}",
                "filenumber": "{{ dag_run.conf.filenumber }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "rehiredate": "{{ dag_run.conf.rehiredate }}",
                "servicedate": "{{ dag_run.conf.servicedate }}",
                "colleaguednumber": "{{ dag_run.conf.colleaguednumber }}",
                "newmapperlookup": "{{ dag_run.conf.jobcode }}",
                "worklocation": "{{ dag_run.conf.worklocation }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "terminationdate": "{{ dag_run.conf.terminationdate }}",
                "userstatus": "{{ result('parse_csv_user_data')['User Status'] }}"
            }
        )

        log_user_updated = rail.PythonOperator(
            task_id='log_user_updated',
            python_callable=lambda: 'Updated'
        )

        is_activeleavestatus_contains_t = rail.IfOperator(
            task_id='is_activeleavestatus_contains_t',
            test=lambda dag_run: 'T' in dag_run.conf['activeleavestatus'] and rail.result(
                'parse_csv_user_data')['User Status'] != 'Disabled',
            yes_task="trigger_child_disable_user_time_off_caribbean",
            no_task="is_activeleave_status_contains_t_3",
        )

        trigger_child_disable_user_time_off_caribbean = rail.TriggerDagRunOperator(
            task_id='trigger_child_disable_user_time_off_caribbean',
            retries=0,
            trigger_dag_id=f'adtalem_userimport_caribbean_child_disable_user_time_off_{config.instance}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            conf={
                "lastname": "{{ dag_run.conf.lastname }}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "jobcode": "{{ dag_run.conf.jobcode }}",
                "jobtitle": "{{ dag_run.conf.jobtitle }}",
                "managerindicator": "{{ dag_run.conf.managerindicator }}",
                "paygroup": "{{ dag_run.conf.paygroup }}",
                "division": "{{ dag_run.conf.division }}",
                "salaryhourly": "{{ dag_run.conf.salaryhourly }}",
                "regulartemp": "{{ dag_run.conf.regulartemp }}",
                "fullparttime": "{{ dag_run.conf.fullparttime }}",
                "activeleavestatus": "{{ dag_run.conf.activeleavestatus }}",
                "supervisor": "{{ dag_run.conf.supervisor }}",
                "emailaddress": "{{ dag_run.conf.emailaddress }}",
                "homestate": "{{ dag_run.conf.homestate }}",
                "standardhours": "{{ dag_run.conf.standardhours }}",
                "flsastatus": "{{ dag_run.conf.flsastatus }}",
                "filenumber": "{{ dag_run.conf.filenumber }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "rehiredate": "{{ dag_run.conf.rehiredate }}",
                "servicedate": "{{ dag_run.conf.servicedate }}",
                "colleaguednumber": "{{ dag_run.conf.colleaguednumber }}",
                "worklocation": "{{ dag_run.conf.worklocation }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "terminationdate": "{{ dag_run.conf.terminationdate }}"
            }
        )

        is_activeleavestatus_not_same = rail.IfOperator(
            task_id='is_activeleavestatus_not_same',
            test="{{ dag_run.conf.activeleavestatus | is_truthy and \
                dag_run.conf.activeleavestatus != result('parse_csv_user_data')['Active/Leave Status'] and \
                    result('get_required_user_customfields').active_leave_status | is_truthy }}",
            yes_task="get_activeleavestatus_dropdown2",
            no_task="log_disabled",
        )

        get_activeleavestatus_dropdown2 = rail.RepliconServiceOperator(
            task_id='get_activeleavestatus_dropdown2',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_required_user_customfields').active_leave_status }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf[
                'activeleavestatus'], 'uri', '')
        )

        if_activeleavestatus_dropdown_present2 = rail.IfOperator(
            task_id='if_activeleavestatus_dropdown_present2',
            test="{{ result('get_activeleavestatus_dropdown2') | is_truthy }}",
            yes_task="update_activeleavestatus_udf2",
            no_task="disable_login",
        )

        update_activeleavestatus_udf2 = rail.RepliconServiceOperator(
            task_id='update_activeleavestatus_udf2',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').active_leave_status }}",
                "customFieldDropDownOptionUri": "{{ result('get_activeleavestatus_dropdown2') }}"
            }
        )

        disable_login = rail.RepliconServiceOperator(
            task_id='disable_login',
            endpoint="/services/securityService1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        is_termination_date_present2 = rail.IfOperator(
            task_id='is_termination_date_present2',
            test="{{ dag_run.conf.terminationdate | is_truthy }}",
            yes_task="trigger_delete_timeoff_bookings_child_v1",
            no_task="log_disabled",
        )

        trigger_delete_timeoff_bookings_child_v1 = rail.TriggerDagRunOperator(
            task_id='trigger_delete_timeoff_bookings_child_v1',
            retries=0,
            trigger_dag_id=f'adtalem_userimport_child_delete_timeoffbookings_{config.instance}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "useruri": dag_run.conf['useruri'],
                "enddate": dag_run.conf['terminationdate'],
                "terminationdate": request_payload.get_datetime_obj(dag_run.conf['terminationdate'])
            }
        )

        log_disabled = rail.PythonOperator(
            task_id='log_disabled',
            python_callable=lambda: 'Disabled'
        )

        is_activeleave_status_contains_t_3 = rail.IfOperator(
            task_id='is_activeleave_status_contains_t_3',
            test=lambda dag_run: 'T' in dag_run.conf['activeleavestatus'] and rail.result(
                'parse_csv_user_data')['User Status'] == 'Disabled',
            yes_task="log_disabled2",
            no_task="write_update_userlog",
        )

        log_disabled2 = rail.PythonOperator(
            task_id='log_disabled2',
            python_callable=lambda: 'Already Disabled'
        )

        write_update_userlog = rail.WriteLogOperator(
            task_id='write_update_userlog',
            log='{{ dag_run.conf.log }}',
            message='User Updated',
            severity='Info',
            properties=lambda dag_run: {
                'login_name': dag_run.conf['loginname'],
                'status': rail.result('log_user_updated') or rail.result('log_disabled') or rail.result('log_disabled2'),
                'failure_reason': ''
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf.log }}',
            trigger_rule='one_failed',
            severity='Error',
            message="{{ get_error_message() }}",
            properties={
                'login_name': '{{ dag_run.conf.loginname }}',
                'status': 'Error',
                'failure_reason': "User \"{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}\" not updated/Disabled/Rehire: \
                    {{ get_error_message() }}"
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors

        can_run_batch_task >> rail.Label(
            'No') >> process_caribbean_updateuser >> get_salary >> get_user_customfieldgroupuri

        get_user_customfieldgroupuri >> get_required_user_customfields >> generate_userreport >> parse_csv >> \
            parse_csv_user_data >> if_firstname_blank
        if_firstname_blank >> rail.Label(
            'Yes') >> write_firstnameblank_replicon >> catch_and_log_errors
        if_firstname_blank >> rail.Label(
            'No') >> is_activeleavestatus_not_contains_t
        is_activeleavestatus_not_contains_t >> rail.Label(
            'Yes') >> get_mapper_entries >> if_userstatus_is_disabled
        if_userstatus_is_disabled >> rail.Label(
            'Yes') >> enable_userprofile >> is_firstname_present
        if_userstatus_is_disabled >> rail.Label(
            'No') >> is_firstname_present
        is_firstname_present >> rail.Label(
            'Yes') >> update_firstname >> is_startdate_present
        is_firstname_present >> rail.Label(
            'No') >> is_startdate_present
        is_startdate_present >> rail.Label(
            'Yes') >> update_startdate >> if_startdate_present_terminationdate_not_present
        is_startdate_present >> rail.Label(
            'No') >> if_startdate_present_terminationdate_not_present
        if_startdate_present_terminationdate_not_present >> rail.Label(
            'Yes') >> update_startdate_2 >> is_terminationdate_present
        if_startdate_present_terminationdate_not_present >> rail.Label(
            'No') >> is_terminationdate_present
        is_terminationdate_present >> rail.Label(
            'Yes') >> update_enddate >> is_lastname_present
        is_terminationdate_present >> rail.Label(
            'No') >> is_lastname_present
        is_lastname_present >> rail.Label(
            'Yes') >> update_lastname >> is_emailaddress_present
        is_lastname_present >> rail.Label(
            'No') >> is_emailaddress_present
        is_emailaddress_present >> rail.Label(
            'Yes') >> update_email >> is_loginname_present
        is_emailaddress_present >> rail.Label(
            'No') >> is_loginname_present
        is_loginname_present >> rail.Label(
            'Yes') >> update_colleaguednumber >> is_division_present
        is_loginname_present >> rail.Label(
            'No') >> is_division_present
        is_division_present >> rail.Label(
            'Yes') >> update_division_udf >> is_jobfunction_present
        is_division_present >> rail.Label(
            'No') >> is_jobfunction_present
        is_jobfunction_present >> rail.Label(
            'Yes') >> update_job_function_udf >> is_paygroup_present
        is_jobfunction_present >> rail.Label(
            'No') >> is_paygroup_present
        is_paygroup_present >> rail.Label(
            'Yes') >> get_paygroup_matching_department >> is_department_uri_present
        is_department_uri_present >> rail.Label(
            'Yes') >> update_department_user >> get_existing_activities_to_be_removed
        is_department_uri_present >> rail.Label(
            'No') >> get_existing_activities_to_be_removed
        is_paygroup_present >> rail.Label(
            'No') >> get_existing_activities_to_be_removed
        get_existing_activities_to_be_removed >> get_activities_from_mapper >> \
            is_activities_present
        is_activities_present >> rail.Label(
            'Yes') >> get_required_activity_uris >> is_required_activities_present
        is_required_activities_present >> rail.Label(
            'Yes') >> update_activity_assignments >> is_activities_to_remove
        is_required_activities_present >> rail.Label(
            'No') >> is_activities_to_remove
        is_activities_present >> rail.Label(
            'No') >> is_activities_to_remove
        is_activities_to_remove >> rail.Label(
            'Yes') >> remove_activity_assignments >> get_required_policysets_to_assign
        is_activities_to_remove >> rail.Label(
            'No') >> get_required_policysets_to_assign
        get_required_policysets_to_assign >> is_policysets_to_assign
        is_policysets_to_assign >> rail.Label(
            'Yes') >> update_templates_for_user >> get_required_timesheet_approvalpath
        is_policysets_to_assign >> rail.Label(
            'No') >> get_required_timesheet_approvalpath

        get_required_timesheet_approvalpath >> should_update_timesheet_approvalpath

        should_update_timesheet_approvalpath >> rail.Label(
            'Yes') >> update_timesheet_approvalpath_user >> get_required_timeoff_approvalpath

        should_update_timesheet_approvalpath >> rail.Label(
            'No') >> get_required_timeoff_approvalpath

        get_required_timeoff_approvalpath >> should_update_timeoff_approvalpath

        should_update_timeoff_approvalpath >> rail.Label(
            'Yes') >> update_timeoff_approvalpath_user >> get_employeetype
        should_update_timeoff_approvalpath >> rail.Label(
            'No') >> get_employeetype

        get_employeetype >> is_get_employeetype_present

        is_get_employeetype_present >> rail.Label(
            'Yes') >> get_required_employeetypeuri >> is_required_employeetype_present
        is_required_employeetype_present >> rail.Label(
            'Yes') >> update_employeetype >> get_required_timesheet_period
        is_required_employeetype_present >> rail.Label(
            'No') >> get_required_timesheet_period
        is_get_employeetype_present >> rail.Label(
            'No') >> get_required_timesheet_period

        get_required_timesheet_period >> is_timesheetperiod_system

        is_timesheetperiod_system >> rail.Label(
            'Yes') >> put_system_timesheetperiod_uri >> is_timesheetperiod_employeetype
        is_timesheetperiod_system >> rail.Label(
            'No') >> is_timesheetperiod_employeetype
        is_timesheetperiod_employeetype >> rail.Label(
            'Yes') >> put_employeetype_timesheetperiod_uri >> is_timesheetperiod_department
        is_timesheetperiod_employeetype >> rail.Label(
            'No') >> is_timesheetperiod_department
        is_timesheetperiod_department >> rail.Label(
            'Yes') >> put_department_timesheet_period >> is_activeleavestatus_present
        is_timesheetperiod_department >> rail.Label(
            'No') >> is_activeleavestatus_present

        is_activeleavestatus_present >> rail.Label(
            'Yes') >> get_activeleavestatus_dropdown >> if_activeleavestatus_dropdown_present
        if_activeleavestatus_dropdown_present >> rail.Label(
            'Yes') >> update_activeleavestatus_udf >> is_flsa_status_present
        if_activeleavestatus_dropdown_present >> rail.Label(
            'No') >> is_flsa_status_present
        is_activeleavestatus_present >> rail.Label(
            'No') >> is_flsa_status_present
        is_flsa_status_present >> rail.Label(
            'Yes') >> get_flsastatus_dropdown >> is_flsa_dropdown_present
        is_flsa_dropdown_present >> rail.Label(
            'Yes') >> update_flsa_status_udf >> is_salaryhourly_present
        is_flsa_dropdown_present >> rail.Label(
            'No') >> is_salaryhourly_present
        is_flsa_status_present >> rail.Label(
            'No') >> is_salaryhourly_present
        is_salaryhourly_present >> rail.Label(
            'Yes') >> get_salaryhourly_dropdown >> is_salaryhourly_dropdown_present
        is_salaryhourly_dropdown_present >> rail.Label(
            'Yes') >> update_salaryhourly_udf >> is_repliconuser_salaryhourly_present
        is_repliconuser_salaryhourly_present >> rail.Label(
            'Yes') >> trigger_delete_timesheet_child_dag >> is_regulartemp_present
        is_repliconuser_salaryhourly_present >> rail.Label(
            'No') >> is_regulartemp_present
        is_salaryhourly_dropdown_present >> rail.Label(
            'No') >> is_regulartemp_present
        is_salaryhourly_present >> rail.Label(
            'No') >> is_regulartemp_present
        is_regulartemp_present >> rail.Label(
            'Yes') >> get_regulartemp_customfield_dropdown >> is_regulartemp_dropdown_present
        is_regulartemp_dropdown_present >> rail.Label(
            'Yes') >> update_regulartemp_udf >> is_fullparttime_present
        is_regulartemp_dropdown_present >> rail.Label(
            'No') >> is_fullparttime_present
        is_regulartemp_present >> rail.Label(
            'No') >> is_fullparttime_present
        is_fullparttime_present >> rail.Label(
            'Yes') >> get_fullparttime_dropdown >> is_fullparttime_dropdown_present
        is_fullparttime_dropdown_present >> rail.Label(
            'Yes') >> update_fullparttime_udf >> is_homestate_present
        is_fullparttime_dropdown_present >> rail.Label(
            'No') >> is_homestate_present
        is_fullparttime_present >> rail.Label(
            'No') >> is_homestate_present
        is_homestate_present >> rail.Label(
            'Yes') >> get_homestate_customfield_dropdown >> is_homestate_customfield_dropdown_present
        is_homestate_customfield_dropdown_present >> rail.Label(
            'Yes') >> update_homestate_udf >> is_jobcode_present
        is_homestate_customfield_dropdown_present >> rail.Label(
            'No') >> is_jobcode_present
        is_homestate_present >> rail.Label(
            'No') >> is_jobcode_present
        is_jobcode_present >> rail.Label(
            'Yes') >> get_jobcode_dropdown >> is_jobcode_dropdown_present
        is_jobcode_dropdown_present >> rail.Label(
            'Yes') >> update_jobcode_udf >> required_batchid
        is_jobcode_dropdown_present >> rail.Label(
            'No') >> put_jobcode_dropdowns
        put_jobcode_dropdowns >> get_jobcode_dropdown2 >> is_jobcode_dropdown_present2
        is_jobcode_dropdown_present2 >> rail.Label(
            'Yes') >> update_jobcode_udf2 >> required_batchid
        is_jobcode_dropdown_present2 >> rail.Label(
            'No') >> required_batchid
        is_jobcode_present >> rail.Label(
            'No') >> required_batchid
        required_batchid >> is_batchid_udf_present
        is_batchid_udf_present >> rail.Label(
            'Yes') >> update_batchid_udf >> is_standardhours_present
        is_batchid_udf_present >> rail.Label(
            'No') >> is_standardhours_present
        is_standardhours_present >> rail.Label(
            'Yes') >> update_standardhours_udf >> is_worklocation_present
        is_standardhours_present >> rail.Label(
            'No') >> is_worklocation_present
        is_worklocation_present >> rail.Label(
            'Yes') >> update_worklocation_udf >> is_departmentnumber_present
        is_worklocation_present >> rail.Label(
            'No') >> is_departmentnumber_present
        is_departmentnumber_present >> rail.Label(
            'Yes') >> update_department_udf >> is_filenumber_present
        is_departmentnumber_present >> rail.Label(
            'No') >> is_filenumber_present
        is_filenumber_present >> rail.Label(
            'Yes') >> update_filenumber >> is_jobtitle_present
        is_filenumber_present >> rail.Label(
            'No') >> is_jobtitle_present
        is_jobtitle_present >> rail.Label(
            'Yes') >> update_job_title_udf >> if_rehiredate_present
        is_jobtitle_present >> rail.Label(
            'No') >> if_rehiredate_present
        if_rehiredate_present >> rail.Label(
            'Yes') >> update_rehire_date_udf >> is_servicedate_present
        if_rehiredate_present >> rail.Label(
            'No') >> is_servicedate_present
        is_servicedate_present >> rail.Label(
            'Yes') >> update_service_date_udf >> is_loginname_present2
        is_servicedate_present >> rail.Label(
            'No') >> is_loginname_present2
        is_loginname_present2 >> rail.Label(
            'Yes') >> update_colleaguednumber_udf >> is_paygroup_present2
        is_loginname_present2 >> rail.Label(
            'No') >> is_paygroup_present2
        is_paygroup_present2 >> rail.Label(
            'Yes') >> update_cocode_udf >> should_update_supervisor
        is_paygroup_present2 >> rail.Label(
            'No') >> should_update_supervisor
        finish_supervisor_assignment >> is_paygroup_present_3
        is_paygroup_present_3 >> rail.Label(
            'Yes') >> is_timesheettemplateuri_present
        is_timesheettemplateuri_present >> rail.Label(
            'Yes') >> get_timesheet_for_date2 >> get_timesheet_details >> get_locationuri_to_assign
        is_timesheettemplateuri_present >> rail.Label(
            'No') >> get_locationuri_to_assign
        get_locationuri_to_assign >> get_locationschedule_user >> \
            put_location_schedule_user >> get_holidaycalendar_from_mapper
        is_paygroup_present_3 >> rail.Label(
            'No') >> get_holidaycalendar_from_mapper
        get_holidaycalendar_from_mapper >> is_holidaycalendar_mapper_present
        is_holidaycalendar_mapper_present >> rail.Label(
            'Yes') >> get_required_holiday_calendaruri >> is_holidaycalendar_uri_present
        is_holidaycalendar_uri_present >> rail.Label(
            'Yes') >> update_holiday_calendar >> get_required_payrule_from_mapper
        is_holidaycalendar_uri_present >> rail.Label(
            'No') >> get_required_payrule_from_mapper
        is_holidaycalendar_mapper_present >> rail.Label(
            'No') >> get_required_payrule_from_mapper
        get_required_payrule_from_mapper >> is_required_payrule_present

        is_required_payrule_present >> rail.Label(
            'Yes') >> get_required_payrulescript_name_uri >> get_payrule_schedule_entries >> is_payrule_schedule_entries_present
        is_payrule_schedule_entries_present >> rail.Label(
            'Yes') >> put_payrulescript_assignment_schedule_user >> get_required_timezone
        is_payrule_schedule_entries_present >> rail.Label(
            'No') >> get_required_timezone
        is_required_payrule_present >> rail.Label(
            'No') >> get_required_timezone
        get_required_timezone >> is_timezone_present
        is_timezone_present >> rail.Label(
            'Yes') >> update_timezone_user >> is_managerindicator_y
        is_timezone_present >> rail.Label(
            'No') >> is_managerindicator_y
        is_managerindicator_y >> rail.Label(
            'Yes') >> get_permissions_to_assign >> is_permission_to_assign
        is_permission_to_assign >> rail.Label(
            'Yes') >> put_permissions_user >> is_jobcode_present2
        is_permission_to_assign >> rail.Label(
            'No') >> is_jobcode_present2
        is_managerindicator_y >> rail.Label(
            'No') >> is_jobcode_present2
        is_jobcode_present2 >> rail.Label(
            'Yes') >> is_studentworker_udf_present
        is_studentworker_udf_present >> rail.Label(
            'Yes') >> get_studentworker_dropdown >> is_studentworker_dropdown_present
        is_studentworker_dropdown_present >> rail.Label(
            'Yes') >> update_student_worker_udf >> get_required_schedule_type
        is_studentworker_dropdown_present >> rail.Label(
            'No') >> get_required_schedule_type
        is_studentworker_udf_present >> rail.Label(
            'No') >> get_required_schedule_type
        is_jobcode_present2 >> rail.Label(
            'No') >> get_required_schedule_type
        get_required_schedule_type >> is_not_shift_schedule
        is_not_shift_schedule >> rail.Label(
            'Yes') >> assign_office_schedule >> is_jobcode_paygroup_not_same
        is_not_shift_schedule >> rail.Label(
            'No') >> assign_shift_schedule >> is_jobcode_paygroup_not_same
        is_jobcode_paygroup_not_same >> rail.Label(
            'Yes') >> trigger_timeoff_updateuser_caribbean >> is_activeleavestatus_notmatches_t_timeoff
        is_jobcode_paygroup_not_same >> rail.Label(
            'No') >> is_activeleavestatus_notmatches_t_timeoff
        is_activeleavestatus_notmatches_t_timeoff >> rail.Label(
            'Yes') >> trigger_timeoff_updateuser_caribbean2 >> log_user_updated
        is_activeleavestatus_notmatches_t_timeoff >> rail.Label(
            'No') >> log_user_updated
        log_user_updated >> is_activeleavestatus_contains_t
        is_activeleavestatus_not_contains_t >> rail.Label(
            'No') >> is_activeleavestatus_contains_t
        is_activeleavestatus_contains_t >> rail.Label(
            'Yes') >> trigger_child_disable_user_time_off_caribbean >> is_activeleavestatus_not_same
        is_activeleavestatus_not_same >> rail.Label(
            'Yes') >> get_activeleavestatus_dropdown2 >> if_activeleavestatus_dropdown_present2
        if_activeleavestatus_dropdown_present2 >> rail.Label(
            'Yes') >> update_activeleavestatus_udf2 >> disable_login
        if_activeleavestatus_dropdown_present2 >> rail.Label(
            'No') >> disable_login
        disable_login >> is_termination_date_present2
        is_termination_date_present2 >> rail.Label(
            'Yes') >> trigger_delete_timeoff_bookings_child_v1 >> log_disabled
        is_termination_date_present2 >> rail.Label(
            'No') >> log_disabled
        is_activeleavestatus_not_same >> rail.Label(
            'No') >> log_disabled
        log_disabled >> is_activeleave_status_contains_t_3
        is_activeleavestatus_contains_t >> rail.Label(
            'No') >> is_activeleave_status_contains_t_3
        is_activeleave_status_contains_t_3 >> rail.Label(
            'Yes') >> log_disabled2 >> write_update_userlog
        is_activeleave_status_contains_t_3 >> rail.Label(
            'No') >> write_update_userlog

        write_update_userlog >> catch_and_log_errors

        catch_and_log_errors >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_updateuser_child_dag)
