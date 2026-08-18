from datetime import timedelta
from airflow.models import Variable
import rail
from adtalem.user_import.task.process_supervisor_assignment import process_supervisor_assignment_task_group
from adtalem.user_import.utils import python_callable_method
from adtalem.user_import.utils.request_payload import get_batchid, get_createuser_payload, get_datetime_obj, get_today_date
from adtalem.user_import.utils.response_filter import get_permissions_to_assign_user, get_studentworker_dropdown_uri


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/adtalem/user_import/config.py


# pylint: disable=too-many-statements
def create_adduser_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'adtalem_userimport_child_add_user_v2_2021_{config.instance}',
        description=f'Adtalem UserImport Child_Add User CR2021_V1 {config.instance}',
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
            no_task='process_adduser_cr2021'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='process_adduser_cr2021',
            end_task='catch_and_log_errors',
        )

        process_adduser_cr2021 = rail.EmptyOperator(
            task_id='process_adduser_cr2021'
        )

        is_loginname_not_present = rail.IfOperator(
            task_id='is_loginname_not_present',
            test='{{ dag_run.conf.loginname | is_falsy }}',
            yes_task='write_loginname_exception',
            no_task='search_users',
        )

        write_loginname_exception = rail.WriteLogOperator(
            task_id='write_loginname_exception',
            log='{{ dag_run.conf.log }}',
            message='User {{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }} not created : Login name not present in the feed file.',
            severity='Exception',
            properties={
                'login_name': '{{ dag_run.conf.loginname }}',
                'status': 'Exception',
                'failure_reason': 'User {{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }} not created : Login name not present in the feed file.'
            }
        )

        def get_empid_by_loginname(response, dag_run):
            user_empids = [item['cells'][1].get('textValue') for item in response['rows']
                           if item['cells'][0]['textValue'] == dag_run.conf['loginname']] if response['rows'] else []
            return rail.smartjoin_by_delim(user_empids) if user_empids else ''
        search_users = rail.RepliconServiceOperator(
            task_id='search_users',
            endpoint='/services/UserListService1.svc/GetData',
            data={
                'page': '1',
                'pagesize': '100',
                'columnUris': [
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:login-name'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': '{{ dag_run.conf.loginname }}'
                        }
                    }
                },
            },
            data_handler=get_empid_by_loginname
        )

        is_useralready_created = rail.IfOperator(
            task_id='is_useralready_created',
            test="{{ result('search_users') | is_truthy }}",
            yes_task="write_user_created_exception",
            no_task="get_ususer_mappercombination",
        )

        write_user_created_exception = rail.WriteLogOperator(
            task_id='write_user_created_exception',
            log='{{ dag_run.conf.log }}',
            message="User not added since another profile with same login name exists with employee ID \"{{ result('search_users') }}\"",
            severity="Exception",
            properties={
                "login_name": "{{ dag_run.conf.loginname }}",
                "status": "Exception",
                "failure_reason": "User not added since another profile with same login name exists with employee ID \"{{ result('search_users') }}\""
            }
        )

        get_ususer_mappercombination = rail.PythonOperator(
            task_id='get_ususer_mappercombination',
            python_callable=python_callable_method.get_ususer_mappercombination_from_dagrunconf
        )

        get_mapper_entries = rail.PythonOperator(
            task_id='get_mapper_entries',
            python_callable=python_callable_method.get_mapper_entries_from_adtalem_mapperfile,
            op_args=[
                "{{ result('get_ususer_mappercombination') if dag_run.conf.ususer == 'yes' else dag_run.conf.paygroup }}", "new"]
        )

        get_employeetype_from_mapper = rail.PythonOperator(
            task_id='get_employeetype_from_mapper',
            python_callable=python_callable_method.get_mapper_entry_value,
            op_args=['Employee Type']
        )

        is_mapper_present = rail.IfOperator(
            task_id='is_mapper_present',
            test="{{ result('get_employeetype_from_mapper') | is_truthy }}",
            yes_task='get_required_employeetypeuri',
            no_task='write_mapper_exception',
        )

        get_required_employeetypeuri = rail.RepliconServiceOperator(
            task_id='get_required_employeetypeuri',
            endpoint='/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails',
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response, 'displayText', rail.result(
                'get_employeetype_from_mapper'), 'uri', '')
        )

        is_required_employeetype_present = rail.IfOperator(
            task_id='is_required_employeetype_present',
            test="{{ result('get_required_employeetypeuri') | is_truthy }}",
            yes_task='create_user',
            no_task='write_employeetype_exception',
        )

        create_user = rail.RepliconServiceOperator(
            task_id='create_user',
            endpoint='/services/ImportService1.svc/PutUser3',
            data=get_createuser_payload
        )

        remove_all_timeoffs = rail.RepliconServiceOperator(
            task_id='remove_all_timeoffs',
            endpoint='/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser',
            data={
                'userUri': "{{ result('create_user').uri }}",
                'timeOffTypeUris': []
            }
        )

        get_required_departmenturi = rail.RepliconServiceOperator(
            task_id='get_required_departmenturi',
            endpoint='/services/DepartmentService1.svc/GetEnabledDepartments',
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['paygroup'], 'uri', '')
        )

        if_required_departmenturi_present = rail.IfOperator(
            task_id='if_required_departmenturi_present',
            test="{{ result('get_required_departmenturi') | is_truthy }}",
            yes_task='update_departmentgroup_user',
            no_task='write_department_exception',
        )

        update_departmentgroup_user = rail.RepliconServiceOperator(
            task_id='update_departmentgroup_user',
            endpoint='/services/DepartmentService1.svc/UpdateDepartmentForUser',
            data={
                'userUri': "{{ result('create_user').uri }}",
                'departmentUri': "{{ result('get_required_departmenturi') }}"
            }
        )

        write_department_exception = rail.WriteLogOperator(
            task_id='write_department_exception',
            log='{{ dag_run.conf.log }}',
            message="User \"{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}\" created ,however, \
                department \"{{ dag_run.conf.paygroup }}\" is not available/ is disabled in Replicon, hence, \"Adtalem\" is added as the department.",
            severity='Error',
            properties={
                'login_name': '{{ dag_run.conf.loginname }}',
                'status': 'Error',
                'failure_reason': "User \"{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}\" created ,however, \
                    department \"{{ dag_run.conf.paygroup }}\" is not available/ is disabled in Replicon, \
                        hence, \"Adtalem\" is added as the department."
            }
        )

        get_authenticationtype_from_mapper = rail.PythonOperator(
            task_id='get_authenticationtype_from_mapper',
            python_callable=python_callable_method.get_mapper_entry_value,
            op_args=['Authentication Type']
        )

        should_set_ssoauthentication = rail.IfOperator(
            task_id='should_set_ssoauthentication',
            test="{{ result('get_authenticationtype_from_mapper') | \
                is_truthy and result('get_authenticationtype_from_mapper') == 'SSO' }}",
            yes_task='set_sso_authentication_user',
            no_task='get_required_policysets_to_assign'
        )

        set_sso_authentication_user = rail.RepliconServiceOperator(
            task_id='set_sso_authentication_user',
            endpoint='/services/SecurityService1.svc/SetSSOAuthenticationForUser',
            data={
                'userUri': "{{ result('create_user').uri }}",
                'loginName': '{{ dag_run.conf.loginname }}'
            }
        )

        get_required_policysets_to_assign = rail.RepliconServiceOperator(
            task_id='get_required_policysets_to_assign',
            endpoint='/services/PolicySetService1.svc/GetAllPolicySets',
            data_handler=python_callable_method.get_required_policysets
        )

        update_templates_for_user = rail.RepliconServiceOperator(
            task_id='update_templates_for_user',
            endpoint="/services/PolicySetService1.svc/PutPolicySetAssignmentsForUser",
            data=lambda: {
                'userUri': rail.result('create_user')['uri'],
                'policySetUris': rail.result('get_required_policysets_to_assign')
            }
        )

        get_required_timesheet_approvalpath = rail.RepliconServiceOperator(
            task_id='get_required_timesheet_approvalpath',
            endpoint='/services/TimesheetApprovalService1.svc/GetAllApprovalPaths',
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', python_callable_method.get_mapper_entry_value('Timesheet Approval'), 'uri', '')
        )

        should_update_timesheet_approvalpath = rail.IfOperator(
            task_id='should_update_timesheet_approvalpath',
            test="{{ result('get_required_timesheet_approvalpath') | is_truthy }}",
            yes_task='update_timesheet_approvalpath_user',
            no_task='get_us_timeoff_policy_mapper_fto'
        )

        update_timesheet_approvalpath_user = rail.RepliconServiceOperator(
            task_id='update_timesheet_approvalpath_user',
            endpoint='/services/TimesheetApprovalService1.svc/UpdateApprovalPathForUser',
            data={
                'userUri': "{{ result('create_user').uri }}",
                'approvalPathUri': "{{ result('get_required_timesheet_approvalpath') }}"
            }
        )

        get_us_timeoff_policy_mapper_fto = rail.PythonOperator(
            task_id='get_us_timeoff_policy_mapper_fto',
            python_callable=python_callable_method.get_us_timeoff_policy_mapper,
            op_args=['{{ dag_run.conf.paygrade }}', 'S']
        )

        is_mapper_fto_present = rail.IfOperator(
            task_id='is_mapper_fto_present',
            test=lambda: rail.result('get_us_timeoff_policy_mapper_fto') and rail.result(
                'get_us_timeoff_policy_mapper_fto')['paygrade'],
            yes_task='specific_paygrade_timeoff',
            no_task='get_required_timeoff_approvalpath'
        )

        specific_paygrade_timeoff = rail.PythonOperator(
            task_id='specific_paygrade_timeoff',
            python_callable=lambda dag_run: 'FTO - Supervisor' if dag_run.conf['salaryhourly'] else ''
        )

        def get_timeoffname():
            return rail.result('specific_paygrade_timeoff') if rail.result(
                'specific_paygrade_timeoff') else python_callable_method.get_mapper_entry_value('Timeoff Approval')
        get_required_timeoff_approvalpath = rail.RepliconServiceOperator(
            task_id='get_required_timeoff_approvalpath',
            endpoint='/services/TimeOffApprovalService1.svc/GetAllApprovalPaths',
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', get_timeoffname(), 'uri', '')
        )

        should_update_timeoff_approvalpath = rail.IfOperator(
            task_id='should_update_timeoff_approvalpath',
            test="{{ result('get_required_timeoff_approvalpath') | is_truthy }}",
            yes_task='update_timeoff_approvalpath_user',
            no_task='get_required_timesheet_period'
        )

        update_timeoff_approvalpath_user = rail.RepliconServiceOperator(
            task_id='update_timeoff_approvalpath_user',
            endpoint='/services/TimeOffApprovalService1.svc/UpdateApprovalPathForUser',
            data={
                'userUri': "{{ result('create_user').uri }}",
                'approvalPathUri': "{{ result('get_required_timeoff_approvalpath') }}"
            }
        )

        get_required_timesheet_period = rail.PythonOperator(
            task_id='get_required_timesheet_period',
            python_callable=python_callable_method.get_mapper_entry_value,
            op_args=['Timesheet Period']
        )

        is_timesheetperiod_system = rail.IfOperator(
            task_id='is_timesheetperiod_system',
            test="{{ result('get_required_timesheet_period') | is_truthy and result('get_required_timesheet_period') == 'System' }}",
            yes_task='get_system_timesheetperiod_uri',
            no_task='process_timesheetperioduri_employeetype'
        )

        get_system_timesheetperiod_uri = rail.RepliconServiceOperator(
            task_id='get_system_timesheetperiod_uri',
            endpoint='/services/TimesheetPeriodService1.svc/GetSystemTimesheetPeriodDetails',
            data={
                'asOfDate': get_today_date()
            },
            data_handler=lambda response: response['uri'] if response else ''
        )

        is_system_timesheetperiod_uri_present = rail.IfOperator(
            task_id='is_system_timesheetperiod_uri_present',
            test="{{ result('get_system_timesheetperiod_uri') | is_truthy }}",
            yes_task='put_system_timesheetperiod_uri',
            no_task='process_timesheetperioduri_employeetype'
        )

        put_system_timesheetperiod_uri = rail.RepliconServiceOperator(
            task_id='put_system_timesheetperiod_uri',
            endpoint='/services/TimesheetPeriodService1.svc/UpdateTimesheetPeriodTypeForUser',
            data={
                'userUri': "{{ result('create_user').uri }}",
                'approvalPathUri': "{{ result('get_system_timesheetperiod_uri') }}"
            }
        )

        process_timesheetperioduri_employeetype = rail.EmptyOperator(
            task_id='process_timesheetperioduri_employeetype'
        )

        is_timesheetperiod_employeetype = rail.IfOperator(
            task_id='is_timesheetperiod_employeetype',
            test="{{ result('get_required_timesheet_period') | is_truthy and result('get_required_timesheet_period') == 'Employee Type' }}",
            yes_task='get_employeetype_timesheetperiod_uri',
            no_task='process_timesheetperioduri_department'
        )

        get_employeetype_timesheetperiod_uri = rail.RepliconServiceOperator(
            task_id='get_employeetype_timesheetperiod_uri',
            endpoint='/services/TimesheetPeriodService1.svc/GetEmployeeTypeTimesheetPeriodDetails',
            data=lambda: {
                'employeeTypeUri': rail.result('get_required_employeetypeuri'),
                'asOfDate': get_today_date()
            },
            data_handler=lambda response: response['uri'] if response else ''
        )

        is_employeetype_timesheetperiod_uri_present = rail.IfOperator(
            task_id='is_employeetype_timesheetperiod_uri_present',
            test="{{ result('get_employeetype_timesheetperiod_uri') | is_truthy }}",
            yes_task='put_employeetype_timesheetperiod_uri',
            no_task='process_timesheetperioduri_department'
        )

        put_employeetype_timesheetperiod_uri = rail.RepliconServiceOperator(
            task_id='put_employeetype_timesheetperiod_uri',
            endpoint='/services/TimesheetPeriodService1.svc/UpdateTimesheetPeriodTypeForUser',
            data={
                'userUri': "{{ result('create_user').uri }}",
                'timesheetPeriodTypeUri': "{{ result('get_employeetype_timesheetperiod_uri') }}"
            }
        )

        process_timesheetperioduri_department = rail.EmptyOperator(
            task_id='process_timesheetperioduri_department'
        )

        is_timesheetperiod_department = rail.IfOperator(
            task_id='is_timesheetperiod_department',
            test="{{ result('get_required_timesheet_period') | \
                is_truthy and result('get_required_timesheet_period') == 'Department' and result('get_required_departmenturi') | \
                    is_truthy }}",
            yes_task='get_department_timesheetperiod',
            no_task='get_user_customfield_uri'
        )

        get_department_timesheetperiod = rail.RepliconServiceOperator(
            task_id='get_department_timesheetperiod',
            endpoint='/services/TimesheetPeriodService1.svc/GetDepartmentTimesheetPeriodDetails',
            data=lambda: {
                'departmentUri': rail.result('get_required_departmenturi'),
                'asOfDate': get_today_date()
            },
            data_handler=lambda response: response['uri'] if response else ''
        )

        is_department_timesheetperiod_uri_present = rail.IfOperator(
            task_id='is_department_timesheetperiod_uri_present',
            test="{{ result('get_department_timesheetperiod') | is_truthy }}",
            yes_task='put_department_timesheet_period',
            no_task='get_user_customfield_uri'
        )

        put_department_timesheet_period = rail.RepliconServiceOperator(
            task_id='put_department_timesheet_period',
            endpoint='/services/TimesheetPeriodService1.svc/UpdateTimesheetPeriodTypeForUser',
            data={
                'userUri': "{{ result('create_user').uri }}",
                'timesheetPeriodTypeUri': "{{ result('get_department_timesheetperiod') }}"
            }
        )

        get_user_customfield_uri = rail.RepliconServiceOperator(
            task_id='get_user_customfield_uri',
            endpoint='/services/CustomFieldService1.svc/GetCustomFieldGroup',
            data={
                'objectTypeUri': 'urn:replicon:object-type:user'
            }
        )

        get_required_user_customfields = rail.RepliconServiceOperator(
            task_id='get_required_user_customfields',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={
                'objectUri': "{{ result('get_user_customfield_uri').uri }}"
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
                'student_worker': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Student Worker', 'uri', ''),
                'salary_grade_code': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Salary Grade Code', 'uri', '')
            }
        )

        is_division_udf_present = rail.IfOperator(
            task_id='is_division_udf_present',
            test="{{ result('get_required_user_customfields').division | is_truthy }}",
            yes_task='update_division_udf',
            no_task='is_jobfunction_udf_present'
        )

        update_division_udf = rail.RepliconServiceOperator(
            task_id='update_division_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').division }}",
                "value": "{{ dag_run.conf.division }}"
            }
        )

        is_jobfunction_udf_present = rail.IfOperator(
            task_id='is_jobfunction_udf_present',
            test="{{ result('get_required_user_customfields').job_function | is_truthy }}",
            yes_task="update_jobfunction_udf",
            no_task="is_servicedate_present",
        )

        update_jobfunction_udf = rail.RepliconServiceOperator(
            task_id='update_jobfunction_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').job_function }}",
                "value": "{{ dag_run.conf.jobfunction }}"
            }
        )

        is_servicedate_present = rail.IfOperator(
            task_id='is_servicedate_present',
            test="{{ result('get_required_user_customfields').service_date | is_truthy }}",
            yes_task="update_service_date_udf",
            no_task="is_rehiredate_present",
        )

        update_service_date_udf = rail.RepliconServiceOperator(
            task_id='update_service_date_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data=lambda dag_run: {
                "objectUri": rail.result('create_user')['uri'],
                "customFieldUri": rail.result('get_required_user_customfields')['service_date'],
                "value": get_datetime_obj(dag_run.conf['servicedate'])
            }
        )

        is_rehiredate_present = rail.IfOperator(
            task_id='is_rehiredate_present',
            test="{{ dag_run.conf.rehiredate | is_truthy and \
                result('get_required_user_customfields').service_date | is_truthy }}",
            yes_task="update_rehire_date_udf",
            no_task="is_activeleavestatus_present",
        )

        update_rehire_date_udf = rail.RepliconServiceOperator(
            task_id='update_rehire_date_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data=lambda dag_run: {
                "objectUri": rail.result('create_user')['uri'],
                "customFieldUri": rail.result('get_required_user_customfields')['rehire_date'],
                "value": get_datetime_obj(dag_run.conf['rehiredate'])
            }
        )

        is_activeleavestatus_present = rail.IfOperator(
            task_id='is_activeleavestatus_present',
            test="{{ dag_run.conf.activeleavestatus | is_truthy and \
                result('get_required_user_customfields').active_leave_status | is_truthy }}",
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
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').active_leave_status }}",
                "customFieldDropDownOptionUri": "{{ result('get_activeleavestatus_dropdown') }}"
            }
        )

        is_flsa_status_present = rail.IfOperator(
            task_id='is_flsa_status_present',
            test="{{ dag_run.conf.flsastatus | is_truthy and \
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
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').flsa_status }}",
                "customFieldDropDownOptionUri": "{{ result('get_flsastatus_dropdown') }}"
            }
        )

        is_salaryhourly_present = rail.IfOperator(
            task_id='is_salaryhourly_present',
            test="{{ dag_run.conf.salaryhourly | is_truthy and \
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
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').salary_hourly }}",
                "customFieldDropDownOptionUri": "{{ result('get_salaryhourly_dropdown') }}"
            }
        )

        is_regulartemp_present = rail.IfOperator(
            task_id='is_regulartemp_present',
            test="{{ dag_run.conf.regulartemp | is_truthy and \
                result('get_required_user_customfields').regular_temp | is_truthy }}",
            yes_task="get_regulartemp_customfield_dropdown",
            no_task="is_homestate_present",
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
            no_task="is_homestate_present",
        )

        update_regulartemp_udf = rail.RepliconServiceOperator(
            task_id='update_regulartemp_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').regular_temp }}",
                "customFieldDropDownOptionUri": "{{ result('get_regulartemp_customfield_dropdown') }}"
            }
        )

        is_homestate_present = rail.IfOperator(
            task_id='is_homestate_present',
            test="{{ dag_run.conf.homestate | is_truthy and \
                result('get_required_user_customfields').home_state | is_truthy }}",
            yes_task="get_homestate_customfield_dropdown",
            no_task="is_fullparttime_present",
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
            no_task="is_fullparttime_present",
        )

        update_homestate_udf = rail.RepliconServiceOperator(
            task_id='update_homestate_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').home_state }}",
                "customFieldDropDownOptionUri": "{{ result('get_homestate_customfield_dropdown') }}"
            }
        )

        is_fullparttime_present = rail.IfOperator(
            task_id='is_fullparttime_present',
            test="{{ dag_run.conf.fullparttime | is_truthy and \
                result('get_required_user_customfields').full_part_time | is_truthy }}",
            yes_task="get_fullparttime_dropdown",
            no_task="is_jobtitle_present",
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
            no_task="is_jobtitle_present",
        )

        update_fullparttime_udf = rail.RepliconServiceOperator(
            task_id='update_fullparttime_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').full_part_time }}",
                "customFieldDropDownOptionUri": "{{ result('get_fullparttime_dropdown') }}"
            }
        )

        is_jobtitle_present = rail.IfOperator(
            task_id='is_jobtitle_present',
            test="{{ dag_run.conf.jobtitle | is_truthy and \
                result('get_required_user_customfields').job_title | is_truthy }}",
            yes_task="update_jobtitle_udf",
            no_task="is_paygrade_present",
        )

        update_jobtitle_udf = rail.RepliconServiceOperator(
            task_id='update_jobtitle_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').job_title }}",
                "value": "{{ dag_run.conf.jobtitle }}"
            }
        )

        is_paygrade_present = rail.IfOperator(
            task_id='is_paygrade_present',
            test="{{ dag_run.conf.paygrade | is_truthy and \
                result('get_required_user_customfields').salary_grade_code | is_truthy }}",
            yes_task="update_paygrade_udf",
            no_task="is_worklocation_present",
        )

        update_paygrade_udf = rail.RepliconServiceOperator(
            task_id='update_paygrade_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').salary_grade_code }}",
                "value": "{{ dag_run.conf.paygrade }}"
            }
        )

        is_worklocation_present = rail.IfOperator(
            task_id='is_worklocation_present',
            test="{{ dag_run.conf.worklocation | is_truthy and \
                result('get_required_user_customfields').work_location | is_truthy }}",
            yes_task="update_worklocation_udf",
            no_task="is_departmentnumber_present",
        )

        update_worklocation_udf = rail.RepliconServiceOperator(
            task_id='update_worklocation_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').work_location }}",
                "value": "{{ dag_run.conf.worklocation }}"
            }
        )

        is_departmentnumber_present = rail.IfOperator(
            task_id='is_departmentnumber_present',
            test="{{ dag_run.conf.departmentnumber | is_truthy and \
                result('get_required_user_customfields').department_number | is_truthy }}",
            yes_task="update_departmentnumber_udf",
            no_task="is_standardhours_present",
        )

        update_departmentnumber_udf = rail.RepliconServiceOperator(
            task_id='update_departmentnumber_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').department_number }}",
                "value": "{{ dag_run.conf.departmentnumber }}"
            }
        )

        is_standardhours_present = rail.IfOperator(
            task_id='is_standardhours_present',
            test="{{ dag_run.conf.standardhours | is_truthy and \
                result('get_required_user_customfields').standard_hours | is_truthy }}",
            yes_task="update_standardhours_udf",
            no_task="is_jobcode_present",
        )

        update_standardhours_udf = rail.RepliconServiceOperator(
            task_id='update_standardhours_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').standard_hours }}",
                "value": "{{ dag_run.conf.standardhours }}"
            }
        )

        is_jobcode_present = rail.IfOperator(
            task_id='is_jobcode_present',
            test="{{ dag_run.conf.jobcode | is_truthy and \
                result('get_required_user_customfields').job_code | is_truthy }}",
            yes_task="get_jobcode_dropdown",
            no_task="is_batchid_udf_present",
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
            no_task="is_batchid_udf_present",
        )

        update_jobcode_udf = rail.RepliconServiceOperator(
            task_id='update_jobcode_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').job_code }}",
                "customFieldDropDownOptionUri": "{{ result('get_jobcode_dropdown') }}"
            }
        )

        is_batchid_udf_present = rail.IfOperator(
            task_id='is_batchid_udf_present',
            test="{{ result('get_required_user_customfields').batch_id | is_truthy and \
                dag_run.conf.paygroup | is_truthy }}",
            yes_task="update_batchid_udf",
            no_task="is_filenumber_udf_present",
        )

        update_batchid_udf = rail.RepliconServiceOperator(
            task_id='update_batchid_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: {
                "objectUri": rail.result('create_user')['uri'],
                "customFieldUri": rail.result('get_required_user_customfields')['batch_id'],
                "value": get_batchid(dag_run.conf['salaryhourly'], dag_run.conf['paygroup'])
            }
        )

        is_filenumber_udf_present = rail.IfOperator(
            task_id='is_filenumber_udf_present',
            test="{{ result('get_required_user_customfields').file_number | is_truthy }}",
            yes_task="update_file_number_udf",
            no_task="is_colleguednumber_present",
        )

        update_file_number_udf = rail.RepliconServiceOperator(
            task_id='update_file_number_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').file_number }}",
                "value": "{{ dag_run.conf.filenumber }}"
            }
        )

        is_colleguednumber_present = rail.IfOperator(
            task_id='is_colleguednumber_present',
            test="{{ result('get_required_user_customfields').colleague_d_number | is_truthy }}",
            yes_task="update_colleaguednumber_udf",
            no_task="is_cocode_udf_present",
        )

        update_colleaguednumber_udf = rail.RepliconServiceOperator(
            task_id='update_colleaguednumber_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').colleague_d_number }}",
                "value": "{{ dag_run.conf.loginname }}"
            }
        )

        is_cocode_udf_present = rail.IfOperator(
            task_id='is_cocode_udf_present',
            test="{{ result('get_required_user_customfields').co_code | is_truthy }}",
            yes_task="update_cocode_udf",
            no_task="should_update_supervisor",
        )

        update_cocode_udf = rail.RepliconServiceOperator(
            task_id='update_cocode_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').co_code }}",
                "value": "{{ dag_run.conf.paygroup }}"
            }
        )

        (should_update_supervisor,
         finish_supervisor_assignment) = process_supervisor_assignment_task_group()

        is_paygroup_present = rail.IfOperator(
            task_id='is_paygroup_present',
            test='{{ dag_run.conf.paygroup | is_truthy }}',
            yes_task="get_locationuri_to_assign",
            no_task="get_required_schedule_type",
        )

        get_locationuri_to_assign = rail.RepliconServiceOperator(
            task_id='get_locationuri_to_assign',
            endpoint="/services/LocationService1.svc/GetAllLocations",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['paygroup'], 'uri', '')
        )

        is_locationuri_present = rail.IfOperator(
            task_id='is_locationuri_present',
            test="{{ result('get_locationuri_to_assign') | is_truthy }}",
            yes_task="put_location_schedule_for_user",
            no_task="get_required_schedule_type",
        )

        put_location_schedule_for_user = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                'userUri': "{{ result('create_user').uri }}",
                "scheduleEntries": [
                    {
                        "location": {
                            "uri": "{{ result('get_locationuri_to_assign') }}"
                        }
                    }
                ]
            }
        )

        get_required_schedule_type = rail.PythonOperator(
            task_id='get_required_schedule_type',
            python_callable=python_callable_method.get_mapper_entry_value,
            op_args=['Schedule Type']
        )

        is_schedule_type_present = rail.IfOperator(
            task_id='is_schedule_type_present',
            test="{{ result('get_required_schedule_type') | is_truthy }}",
            yes_task="put_schedule_policy_user",
            no_task="get_required_workweek",
        )

        put_schedule_policy_user = rail.RepliconServiceOperator(
            task_id='put_schedule_policy_user',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data=lambda: {
                'userUri': rail.result('create_user')['uri'],
                "scheduleEntries": [
                    {
                        "schedulePolicy": {
                            "scheduleTypeUri": "urn:replicon:schedule-type:shift"
                        } if 'Shift' in rail.result('get_required_schedule_type') else {
                            "name": rail.result('get_required_schedule_type'),
                            "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                        }
                    }
                ]
            }
        )

        get_required_workweek = rail.PythonOperator(
            task_id='get_required_workweek',
            python_callable=python_callable_method.get_mapper_entry_value,
            op_args=['Work Week']
        )

        is_required_workweek_present = rail.IfOperator(
            task_id='is_required_workweek_present',
            test="{{ result('get_required_workweek') | is_truthy }}",
            yes_task="get_the_workweek_startday",
            no_task="get_required_holidaycalendar",
        )

        get_the_workweek_startday = rail.RepliconServiceOperator(
            task_id='get_the_workweek_startday',
            endpoint="/services/InternationalizationService1.svc/GetAllDaysOfWeek",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'name', rail.result('get_required_workweek').split(' ')[0], 'uri', '')
        )

        if_workweek_startday_present = rail.IfOperator(
            task_id='if_workweek_startday_present',
            test="{{ result('get_the_workweek_startday') | is_truthy }}",
            yes_task="update_workweek",
            no_task="get_required_holidaycalendar",
        )

        update_workweek = rail.RepliconServiceOperator(
            task_id='update_workweek',
            endpoint="/services/UserService1.svc/UpdateWorkWeekStartDayForUser",
            data={
                'userUri': "{{ result('create_user').uri }}",
                "dayOfWeekUri": "{{ result('get_the_workweek_startday') }}"
            }
        )

        get_required_holidaycalendar = rail.PythonOperator(
            task_id='get_required_holidaycalendar',
            python_callable=python_callable_method.get_mapper_entry_value,
            op_args=['Holiday Calendar']
        )

        is_holiday_calendar_present = rail.IfOperator(
            task_id='is_holiday_calendar_present',
            test="{{ result('get_required_holidaycalendar') | is_truthy }}",
            yes_task="get_required_holidaycalendar_uri",
            no_task="get_required_timezone",
        )

        get_required_holidaycalendar_uri = rail.RepliconServiceOperator(
            task_id='get_required_holidaycalendar_uri',
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'name', rail.result('get_required_holidaycalendar'), 'uri', '')
        )

        is_holidaycalendar_uri_present = rail.IfOperator(
            task_id='is_holidaycalendar_uri_present',
            test="{{ result('get_required_holidaycalendar_uri') | is_truthy }}",
            yes_task="update_holiday_calendar",
            no_task="get_required_timezone"
        )

        update_holiday_calendar = rail.RepliconServiceOperator(
            task_id='update_holiday_calendar',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
                'userUri': "{{ result('create_user').uri }}",
                "holidayCalendarUri": "{{ result('get_required_holidaycalendar_uri') }}"
            }
        )

        get_required_timezone = rail.PythonOperator(
            task_id='get_required_timezone',
            python_callable=python_callable_method.get_timezone_mapper_entry,
            op_args=['{{ dag_run.conf.homestate }}']
        )

        is_timezone_present = rail.IfOperator(
            task_id='is_timezone_present',
            test="{{ result('get_required_timezone') | is_truthy }}",
            yes_task="update_timezone_user",
            no_task="is_managerindicator_y",
        )

        update_timezone_user = rail.RepliconServiceOperator(
            task_id='update_timezone_user',
            endpoint="/services/InternationalizationService1.svc/UpdateTimeZoneForUser",
            data={
                'userUri': "{{ result('create_user').uri }}",
                "timeZoneUri": "{{ result('get_required_timezone') }}"
            }
        )

        is_managerindicator_y = rail.IfOperator(
            task_id='is_managerindicator_y',
            test="{{ dag_run.conf.managerindicator | is_truthy and dag_run.conf.managerindicator == 'Y' }}",
            yes_task="get_permissions_to_assign",
            no_task="is_jobcode_present2",
        )

        get_permissions_to_assign = rail.RepliconServiceOperator(
            task_id='get_permissions_to_assign',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                'userUri': "{{ result('create_user').uri }}"
            },
            data_handler=get_permissions_to_assign_user
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
            data=lambda: {
                'userUri': rail.result('create_user')['uri'],
                "permissionSetUris": rail.result('get_permissions_to_assign')
            }
        )

        is_jobcode_present2 = rail.IfOperator(
            task_id='is_jobcode_present2',
            test='{{ dag_run.conf.jobcode | is_truthy }}',
            yes_task="is_studentworker_udf_present",
            no_task="is_activeleavestatus_not_matches_t",
        )

        is_studentworker_udf_present = rail.IfOperator(
            task_id='is_studentworker_udf_present',
            test="{{ result('get_required_user_customfields').student_worker | is_truthy }}",
            yes_task="get_studentworker_dropdown",
            no_task="is_activeleavestatus_not_matches_t",
        )

        get_studentworker_dropdown = rail.RepliconServiceOperator(
            task_id='get_studentworker_dropdown',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_required_user_customfields').student_worker }}"
            },
            data_handler=get_studentworker_dropdown_uri
        )

        is_studentworker_dropdown_present = rail.IfOperator(
            task_id='is_studentworker_dropdown_present',
            test="{{ result('get_studentworker_dropdown') | is_truthy }}",
            yes_task="update_student_worker_udf",
            no_task="is_activeleavestatus_not_matches_t",
        )

        update_student_worker_udf = rail.RepliconServiceOperator(
            task_id='update_student_worker_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').student_worker }}",
                "customFieldDropDownOptionUri": "{{ result('get_studentworker_dropdown') }}"
            }
        )

        is_activeleavestatus_not_matches_t = rail.IfOperator(
            task_id='is_activeleavestatus_not_matches_t',
            test="{{ dag_run.conf.activeleavestatus | matches('T') | is_falsy }}",
            yes_task="if_regulartemp_not_equals_t",
            no_task="get_required_payrule_from_mapper",
        )

        if_regulartemp_not_equals_t = rail.IfOperator(
            task_id='if_regulartemp_not_equals_t',
            test="{{ dag_run.conf.regulartemp != 'T' }}",
            yes_task="trigger_timeoff_adduser_cr2021",
            no_task="is_homestate_equals_to_selected",
        )

        trigger_timeoff_adduser_cr2021 = rail.TriggerDagRunOperator(
            task_id='trigger_timeoff_adduser_cr2021',
            retries=0,
            trigger_dag_id=f'adtalem_userimport_child_timeoff_add_new_user_cr2021_v1_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                'lastname': '{{ dag_run.conf.lastname }}',
                'firstname': '{{ dag_run.conf.firstname }}',
                'employeeid': '{{ dag_run.conf.employeeid }}',
                'loginname': '{{ dag_run.conf.loginname }}',
                'jobcode': '{{ dag_run.conf.jobcode }}',
                'jobtitle': '{{ dag_run.conf.jobtitle }}',
                'managerindicator': '{{ dag_run.conf.managerindicator }}',
                'paygroup': '{{ dag_run.conf.paygroup }}',
                'division': '{{ dag_run.conf.division }}',
                'salaryhourly': '{{ dag_run.conf.salaryhourly }}',
                'regulartemp': '{{ dag_run.conf.regulartemp }}',
                'fullparttime': '{{ dag_run.conf.fullparttime }}',
                'activeleavestatus': '{{ dag_run.conf.activeleavestatus }}',
                'supervisor': '{{ dag_run.conf.supervisor }}',
                'emailaddress': '{{ dag_run.conf.emailaddress }}',
                'homestate': '{{ dag_run.conf.homestate }}',
                'standardhours': '{{ dag_run.conf.standardhours }}',
                'flsastatus': '{{ dag_run.conf.flsastatus }}',
                'filenumber': '{{ dag_run.conf.filenumber }}',
                'startdate': '{{ dag_run.conf.startdate }}',
                'rehiredate': '{{ dag_run.conf.rehiredate }}',
                'servicedate': '{{ dag_run.conf.servicedate }}',
                'colleaguednumber': '{{ dag_run.conf.colleaguednumber }}',
                'useruri': "{{ result('create_user').uri }}",
                'mapperlookup': "{{ result('get_ususer_mappercombination') if dag_run.conf.ususer == 'yes' else dag_run.conf.paygroup }}",
                'ususer': '{{ dag_run.conf.ususer }}',
                'paygrade': '{{ dag_run.conf.paygrade }}'
            }
        )

        is_homestate_equals_to_selected = rail.IfOperator(
            task_id='is_homestate_equals_to_selected',
            test=lambda dag_run: dag_run.conf['homestate'] in (
                'CA', 'MA', 'NJ', 'OR'),
            yes_task="trigger_timeoff_updateuser_cr14",
            no_task="get_required_payrule_from_mapper",
        )

        trigger_timeoff_updateuser_cr14 = rail.TriggerDagRunOperator(
            task_id='trigger_timeoff_updateuser_cr14',
            retries=0,
            trigger_dag_id=f'adtalem_userimport_child_timeoff_update_user_crv14.0_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                'lastname': '{{ dag_run.conf.lastname }}',
                'firstname': '{{ dag_run.conf.firstname }}',
                'employeeid': '{{ dag_run.conf.employeeid }}',
                'loginname': '{{ dag_run.conf.loginname }}',
                'jobcode': '{{ dag_run.conf.jobcode }}',
                'jobtitle': '{{ dag_run.conf.jobtitle }}',
                'managerindicator': '{{ dag_run.conf.managerindicator }}',
                'paygroup': '{{ dag_run.conf.paygroup }}',
                'division': '{{ dag_run.conf.division }}',
                'salaryhourly': '{{ dag_run.conf.salaryhourly }}',
                'regulartemp': '{{ dag_run.conf.regulartemp }}',
                'fullparttime': '{{ dag_run.conf.fullparttime }}',
                'activeleavestatus': '{{ dag_run.conf.activeleavestatus }}',
                'supervisor': '{{ dag_run.conf.supervisor }}',
                'emailaddress': '{{ dag_run.conf.emailaddress }}',
                'homestate': '{{ dag_run.conf.homestate }}',
                'standardhours': '{{ dag_run.conf.standardhours }}',
                'flsastatus': '{{ dag_run.conf.flsastatus }}',
                'filenumber': '{{ dag_run.conf.filenumber }}',
                'startdate': '{{ dag_run.conf.startdate }}',
                'rehiredate': '{{ dag_run.conf.rehiredate }}',
                'servicedate': '{{ dag_run.conf.servicedate }}',
                'colleaguednumber': '{{ dag_run.conf.colleaguednumber }}',
                'worklocation': '{{ dag_run.conf.worklocation }}',
                'userstatus': 'Enabled',
                'useruri': "{{ result('create_user').uri }}"
            }
        )

        get_required_payrule_from_mapper = rail.PythonOperator(
            task_id='get_required_payrule_from_mapper',
            python_callable=python_callable_method.get_mapper_entry_value,
            op_args=['Payrule']
        )

        is_payrule_and_timesheettemplateuri_present = rail.IfOperator(
            task_id='is_payrule_and_timesheettemplateuri_present',
            test="{{ result('get_required_payrule_from_mapper') | is_truthy and \
                result('get_required_policysets_to_assign', 'timesheet_template_uri') | sn | is_truthy }}",
            yes_task="create_timesheet",
            no_task="if_activeleavestatus_equals_t",
        )

        create_timesheet = rail.RepliconServiceOperator(
            task_id='create_timesheet',
            endpoint="/services/TimesheetService1.svc/GetTimesheetForDate2",
            data=lambda dag_run: {
                'userUri': rail.result('create_user')['uri'],
                "date": get_datetime_obj(dag_run.conf['servicedate']),
                "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
            }
        )

        get_timesheet_details = rail.RepliconServiceOperator(
            task_id='get_timesheet_details',
            endpoint="/services/TimesheetService1.svc/GetTimesheetDetails",
            data={
                "timesheetUri": "{{ result('create_timesheet').timesheet.uri }}"
            }
        )

        get_required_payrule_script = rail.RepliconServiceOperator(
            task_id='get_required_payrule_script',
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', rail.result('get_required_payrule_from_mapper'), 'uri', '')
        )

        is_required_payrule_present = rail.IfOperator(
            task_id='is_required_payrule_present',
            test="{{ result('get_required_payrule_script') | is_truthy }}",
            yes_task="put_payrule_script_assignment_schedule",
            no_task="if_activeleavestatus_equals_t",
        )

        put_payrule_script_assignment_schedule = rail.RepliconServiceOperator(
            task_id='put_payrule_script_assignment_schedule',
            endpoint="/services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data=lambda: {
                'userUri': rail.result('create_user')['uri'],
                "scheduleEntries": [
                    {
                        "payRuleScript": {
                            "uri": rail.result('get_required_payrule_script')
                        },
                        "effectiveDate": rail.result('get_timesheet_details')['dateRange']['startDate']
                    }
                ]
            }
        )

        if_activeleavestatus_equals_t = rail.IfOperator(
            task_id='if_activeleavestatus_equals_t',
            test="{{ dag_run.conf.activeleavestatus | matches('T') }}",
            yes_task="disable_login",
            no_task="write_user_log",
        )

        disable_login = rail.RepliconServiceOperator(
            task_id='disable_login',
            endpoint="/services/securityService1.svc/DisableLogin",
            data={
                'userUri': "{{ result('create_user').uri }}"
            }
        )

        write_user_log = rail.WriteLogOperator(
            task_id='write_user_log',
            log='{{ dag_run.conf.log }}',
            message="User Added in Disabled status",
            severity="Success",
            properties=lambda dag_run: {
                'login_name': dag_run.conf['loginname'],
                'status': 'Success',
                'failure_reason': 'User Added in Disabled status' if 'T' in dag_run.conf[
                    'activeleavestatus'] else ''
            }
        )

        write_employeetype_exception = rail.WriteLogOperator(
            task_id='write_employeetype_exception',
            log='{{ dag_run.conf.log }}',
            message="User not created as employee type \"{{ result('get_employeetype_from_mapper') }}\" is not available in Replicon",
            severity='Exception',
            properties={
                'login_name': '{{ dag_run.conf.loginname }}',
                'status': 'Exception',
                'failure_reason': "User not created as employee type \"{{ result('get_employeetype_from_mapper') }}\" is not available in Replicon"
            }
        )

        write_mapper_exception = rail.WriteLogOperator(
            task_id='write_mapper_exception',
            log='{{ dag_run.conf.log }}',
            message="User not created as employee type is not defined in mapper for given salary hourly,reg temp,full \
                part time,state,job code,division and standard hours combination",
            severity='Error',
            properties={
                'login_name': '{{ dag_run.conf.loginname }}',
                'status': 'Error',
                'failure_reason': "User not created as employee type is not defined in mapper for given salary hourly,reg temp,full \
                    part time,state,job code,division and standard hours combination"
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
                'failure_reason': '\
                    {%- if result("create_user") | is_truthy -%} \
                        User "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}" created, however, all values not updated: {{ get_error_message() }}\
                    {%- else -%} \
                        User not created: {{ get_error_message() }}\
                    {%- endif -%}'
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
            'No') >> process_adduser_cr2021 >> is_loginname_not_present

        is_loginname_not_present >> rail.Label(
            'Yes') >> write_loginname_exception >> catch_and_log_errors

        is_loginname_not_present >> rail.Label(
            'No') >> search_users >> is_useralready_created
        is_useralready_created >> rail.Label(
            'Yes') >> write_user_created_exception >> catch_and_log_errors
        is_useralready_created >> rail.Label(
            'No') >> get_ususer_mappercombination

        get_ususer_mappercombination >> get_mapper_entries >> get_employeetype_from_mapper >> is_mapper_present

        is_mapper_present >> rail.Label(
            'Yes') >> get_required_employeetypeuri >> is_required_employeetype_present

        is_required_employeetype_present >> rail.Label(
            'Yes') >> create_user >> remove_all_timeoffs >> get_required_departmenturi >> if_required_departmenturi_present

        if_required_departmenturi_present >> rail.Label(
            'Yes') >> update_departmentgroup_user >> get_authenticationtype_from_mapper

        if_required_departmenturi_present >> rail.Label(
            'No') >> write_department_exception >> get_authenticationtype_from_mapper

        get_authenticationtype_from_mapper >> should_set_ssoauthentication
        should_set_ssoauthentication >> rail.Label(
            'Yes') >> set_sso_authentication_user >> get_required_policysets_to_assign
        should_set_ssoauthentication >> rail.Label(
            'No') >> get_required_policysets_to_assign

        get_required_policysets_to_assign >> update_templates_for_user >> get_required_timesheet_approvalpath >> should_update_timesheet_approvalpath
        should_update_timesheet_approvalpath >> rail.Label(
            'Yes') >> update_timesheet_approvalpath_user >> get_us_timeoff_policy_mapper_fto

        should_update_timesheet_approvalpath >> rail.Label(
            'No') >> get_us_timeoff_policy_mapper_fto

        get_us_timeoff_policy_mapper_fto >> is_mapper_fto_present

        is_mapper_fto_present >> rail.Label(
            'Yes') >> specific_paygrade_timeoff >> get_required_timeoff_approvalpath
        is_mapper_fto_present >> rail.Label(
            'No') >> get_required_timeoff_approvalpath

        get_required_timeoff_approvalpath >> should_update_timeoff_approvalpath

        should_update_timeoff_approvalpath >> rail.Label(
            'Yes') >> update_timeoff_approvalpath_user >> get_required_timesheet_period
        should_update_timeoff_approvalpath >> rail.Label(
            'No') >> get_required_timesheet_period

        get_required_timesheet_period >> is_timesheetperiod_system

        is_timesheetperiod_system >> rail.Label(
            'Yes') >> get_system_timesheetperiod_uri >> is_system_timesheetperiod_uri_present

        is_system_timesheetperiod_uri_present >> rail.Label(
            'Yes') >> put_system_timesheetperiod_uri >> process_timesheetperioduri_employeetype
        is_system_timesheetperiod_uri_present >> rail.Label(
            'No') >> process_timesheetperioduri_employeetype

        is_timesheetperiod_system >> rail.Label(
            'No') >> process_timesheetperioduri_employeetype

        process_timesheetperioduri_employeetype >> is_timesheetperiod_employeetype

        is_timesheetperiod_employeetype >> rail.Label(
            'Yes') >> get_employeetype_timesheetperiod_uri >> is_employeetype_timesheetperiod_uri_present

        is_employeetype_timesheetperiod_uri_present >> rail.Label(
            'Yes') >> put_employeetype_timesheetperiod_uri >> process_timesheetperioduri_department
        is_employeetype_timesheetperiod_uri_present >> rail.Label(
            'No') >> process_timesheetperioduri_department

        is_timesheetperiod_employeetype >> rail.Label(
            'No') >> process_timesheetperioduri_department

        process_timesheetperioduri_department >> is_timesheetperiod_department

        is_timesheetperiod_department >> rail.Label(
            'Yes') >> get_department_timesheetperiod >> is_department_timesheetperiod_uri_present

        is_department_timesheetperiod_uri_present >> rail.Label(
            'Yes') >> put_department_timesheet_period >> get_user_customfield_uri
        is_department_timesheetperiod_uri_present >> rail.Label(
            'No') >> get_user_customfield_uri

        is_timesheetperiod_department >> rail.Label(
            'No') >> get_user_customfield_uri

        get_user_customfield_uri >> get_required_user_customfields >> is_division_udf_present

        is_division_udf_present >> rail.Label(
            'Yes') >> update_division_udf >> is_jobfunction_udf_present
        is_division_udf_present >> rail.Label(
            'No') >> is_jobfunction_udf_present

        is_jobfunction_udf_present >> rail.Label(
            'Yes') >> update_jobfunction_udf >> is_servicedate_present
        is_jobfunction_udf_present >> rail.Label(
            'No') >> is_servicedate_present

        is_servicedate_present >> rail.Label(
            'Yes') >> update_service_date_udf >> is_rehiredate_present
        is_servicedate_present >> rail.Label(
            'No') >> is_rehiredate_present

        is_rehiredate_present >> rail.Label(
            'Yes') >> update_rehire_date_udf >> is_activeleavestatus_present
        is_rehiredate_present >> rail.Label(
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
            'Yes') >> update_salaryhourly_udf >> is_regulartemp_present
        is_salaryhourly_dropdown_present >> rail.Label(
            'No') >> is_regulartemp_present
        is_salaryhourly_present >> rail.Label(
            'No') >> is_regulartemp_present

        is_regulartemp_present >> rail.Label(
            'Yes') >> get_regulartemp_customfield_dropdown >> is_regulartemp_dropdown_present
        is_regulartemp_dropdown_present >> rail.Label(
            'Yes') >> update_regulartemp_udf >> is_homestate_present
        is_regulartemp_dropdown_present >> rail.Label(
            'No') >> is_homestate_present
        is_regulartemp_present >> rail.Label(
            'No') >> is_homestate_present
        is_homestate_present >> rail.Label(
            'Yes') >> get_homestate_customfield_dropdown >> is_homestate_customfield_dropdown_present
        is_homestate_customfield_dropdown_present >> rail.Label(
            'Yes') >> update_homestate_udf >> is_fullparttime_present
        is_homestate_customfield_dropdown_present >> rail.Label(
            'No') >> is_fullparttime_present
        is_homestate_present >> rail.Label(
            'No') >> is_fullparttime_present
        is_fullparttime_present >> rail.Label(
            'Yes') >> get_fullparttime_dropdown >> is_fullparttime_dropdown_present
        is_fullparttime_dropdown_present >> rail.Label(
            'Yes') >> update_fullparttime_udf >> is_jobtitle_present
        is_fullparttime_dropdown_present >> rail.Label(
            'No') >> is_jobtitle_present
        is_fullparttime_present >> rail.Label(
            'No') >> is_jobtitle_present
        is_jobtitle_present >> rail.Label(
            'Yes') >> update_jobtitle_udf >> is_paygrade_present
        is_jobtitle_present >> rail.Label(
            'No') >> is_paygrade_present

        is_paygrade_present >> rail.Label(
            'Yes') >> update_paygrade_udf >> is_worklocation_present
        is_paygrade_present >> rail.Label(
            'No') >> is_worklocation_present

        is_worklocation_present >> rail.Label(
            'Yes') >> update_worklocation_udf >> is_departmentnumber_present
        is_worklocation_present >> rail.Label(
            'No') >> is_departmentnumber_present
        is_departmentnumber_present >> rail.Label(
            'Yes') >> update_departmentnumber_udf >> is_standardhours_present
        is_departmentnumber_present >> rail.Label(
            'No') >> is_standardhours_present
        is_standardhours_present >> rail.Label(
            'Yes') >> update_standardhours_udf >> is_jobcode_present
        is_standardhours_present >> rail.Label(
            'No') >> is_jobcode_present
        is_jobcode_present >> rail.Label(
            'Yes') >> get_jobcode_dropdown >> is_jobcode_dropdown_present
        is_jobcode_dropdown_present >> rail.Label(
            'Yes') >> update_jobcode_udf >> is_batchid_udf_present
        is_jobcode_dropdown_present >> rail.Label(
            'No') >> is_batchid_udf_present
        is_jobcode_present >> rail.Label(
            'No') >> is_batchid_udf_present
        is_batchid_udf_present >> rail.Label(
            'Yes') >> update_batchid_udf >> is_filenumber_udf_present
        is_batchid_udf_present >> rail.Label(
            'No') >> is_filenumber_udf_present
        is_filenumber_udf_present >> rail.Label(
            'Yes') >> update_file_number_udf >> is_colleguednumber_present
        is_filenumber_udf_present >> rail.Label(
            'No') >> is_colleguednumber_present
        is_colleguednumber_present >> rail.Label(
            'Yes') >> update_colleaguednumber_udf >> is_cocode_udf_present
        is_colleguednumber_present >> rail.Label(
            'No') >> is_cocode_udf_present
        is_cocode_udf_present >> rail.Label(
            'Yes') >> update_cocode_udf >> should_update_supervisor
        is_cocode_udf_present >> rail.Label(
            'No') >> should_update_supervisor
        finish_supervisor_assignment >> is_paygroup_present
        is_paygroup_present >> rail.Label(
            'Yes') >> get_locationuri_to_assign >> is_locationuri_present
        is_locationuri_present >> rail.Label(
            'Yes') >> put_location_schedule_for_user >> get_required_schedule_type
        is_locationuri_present >> rail.Label(
            'No') >> get_required_schedule_type
        is_paygroup_present >> rail.Label(
            'No') >> get_required_schedule_type

        get_required_schedule_type >> is_schedule_type_present

        is_schedule_type_present >> rail.Label(
            'Yes') >> put_schedule_policy_user >> get_required_workweek
        is_schedule_type_present >> rail.Label(
            'No') >> get_required_workweek

        get_required_workweek >> is_required_workweek_present
        is_required_workweek_present >> rail.Label(
            'Yes') >> get_the_workweek_startday >> if_workweek_startday_present
        if_workweek_startday_present >> rail.Label(
            'Yes') >> update_workweek >> get_required_holidaycalendar
        if_workweek_startday_present >> rail.Label(
            'No') >> get_required_holidaycalendar
        is_required_workweek_present >> rail.Label(
            'No') >> get_required_holidaycalendar

        get_required_holidaycalendar >> is_holiday_calendar_present
        is_holiday_calendar_present >> rail.Label(
            'Yes') >> get_required_holidaycalendar_uri >> is_holidaycalendar_uri_present
        is_holidaycalendar_uri_present >> rail.Label(
            'Yes') >> update_holiday_calendar >> get_required_timezone
        is_holidaycalendar_uri_present >> rail.Label(
            'No') >> get_required_timezone
        is_holiday_calendar_present >> rail.Label(
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
            'Yes') >> update_student_worker_udf >> is_activeleavestatus_not_matches_t
        is_studentworker_dropdown_present >> rail.Label(
            'No') >> is_activeleavestatus_not_matches_t
        is_studentworker_udf_present >> rail.Label(
            'No') >> is_activeleavestatus_not_matches_t
        is_jobcode_present2 >> rail.Label(
            'No') >> is_activeleavestatus_not_matches_t
        is_activeleavestatus_not_matches_t >> rail.Label(
            'Yes') >> if_regulartemp_not_equals_t
        if_regulartemp_not_equals_t >> rail.Label(
            'Yes') >> trigger_timeoff_adduser_cr2021 >> get_required_payrule_from_mapper
        if_regulartemp_not_equals_t >> rail.Label(
            'No') >> is_homestate_equals_to_selected
        is_homestate_equals_to_selected >> rail.Label(
            'Yes') >> trigger_timeoff_updateuser_cr14 >> get_required_payrule_from_mapper

        is_homestate_equals_to_selected >> rail.Label(
            'No') >> get_required_payrule_from_mapper
        is_activeleavestatus_not_matches_t >> rail.Label(
            'No') >> get_required_payrule_from_mapper

        get_required_payrule_from_mapper >> is_payrule_and_timesheettemplateuri_present
        is_payrule_and_timesheettemplateuri_present >> rail.Label(
            'Yes') >> create_timesheet >> get_timesheet_details >> get_required_payrule_script >> is_required_payrule_present
        is_required_payrule_present >> rail.Label(
            'Yes') >> put_payrule_script_assignment_schedule >> if_activeleavestatus_equals_t
        is_required_payrule_present >> rail.Label(
            'No') >> if_activeleavestatus_equals_t
        is_payrule_and_timesheettemplateuri_present >> rail.Label(
            'No') >> if_activeleavestatus_equals_t
        if_activeleavestatus_equals_t >> rail.Label(
            'Yes') >> disable_login >> write_user_log
        if_activeleavestatus_equals_t >> rail.Label(
            'No') >> write_user_log
        write_user_log >> catch_and_log_errors
        is_required_employeetype_present >> rail.Label(
            'No') >> write_employeetype_exception >> catch_and_log_errors
        is_mapper_present >> rail.Label(
            'No') >> write_mapper_exception >> catch_and_log_errors

        catch_and_log_errors >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_adduser_child_dag)
