from datetime import timedelta
import rail
from airflow.models import Variable
from technicolorg3.user_import.task.process_mappers import process_mappers_task_group
from technicolorg3.user_import.task.process_supervisor_assignment import process_supervisor_assignment_task_group
from technicolorg3.user_import.utils import python_callable_method
from technicolorg3.user_import.utils import request_payload


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/technicolorg3/user_import/config.py


# pylint:disable=too-many-statements
def create_updateuser_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'technicolorg3_user_import_child_updateuser_{config.instance}',
        description=f'Technicolor_Child_Workflow to update user {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_updateuser_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_user_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_user_log',
            end_task='catch_and_log_errors',
        )

        create_user_log = rail.CreateLogOperator(
            task_id='create_user_log'
        )

        is_multipleuser_with_employeeid = rail.IfOperator(
            task_id='is_multipleuser_with_employeeid',
            test=lambda dag_run: len(dag_run.conf['useruri'].split('|')) > 1,
            yes_task='write_multipleuser_exception',
            no_task='bulk_getuser3'
        )

        write_multipleuser_exception = rail.WriteLogOperator(
            task_id='write_multipleuser_exception',
            log="{{ result('create_user_log') }}",
            severity='Exception',
            message='Multiple employees found with the same employee id in Replicon',
            properties={
                'globalid': '{{ dag_run.conf.globalid }}',
                'action': 'Update',
                'status': 'Exception',
                'details': 'Multiple employees found with the same employee id in Replicon',
                'username': '{{ dag_run.conf.username }}',
                'new_location': 'No',
                'location': ''
            }
        )

        bulk_getuser3 = rail.RepliconServiceOperator(
            task_id='bulk_getuser3',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data={
                'users': [
                    {
                        'uri': '{{ dag_run.conf.useruri }}'
                    }
                ],
                'dataLoadOptionUri': 'urn:replicon:data-load-option:omit-data-if-insufficient-access-permission'
            },
            data_handler=lambda response: response[0] if response else None
        )

        is_adminudf_modified = rail.IfOperator(
            task_id='is_adminudf_modified',
            test=lambda: request_payload.get_adminudf_modified_value(rail.result(
                'bulk_getuser3')['userDetails']['customFieldValues'], 'Admin Modified') == 'yes',
            yes_task='write_adminuser_udf_exception',
            no_task='process_mappers'
        )

        write_adminuser_udf_exception = rail.WriteLogOperator(
            task_id='write_adminuser_udf_exception',
            log="{{ result('create_user_log') }}",
            severity='Skipped',
            message='Admin modified udf is set to "Yes"',
            properties={
                'globalid': '{{ dag_run.conf.globalid }}',
                'action': 'Update',
                'status': 'Skipped',
                'details': 'Admin modified udf is set to "Yes"',
                'username': '{{ dag_run.conf.username }}',
                'new_location': 'No',
                'location': '{{ dag_run.conf.location }}'
            }
        )

        process_mappers = rail.EmptyOperator(
            task_id='process_mappers'
        )

        (is_businessunitname_servicelinename,
         get_default_mapper_entries_from_country) = process_mappers_task_group(config.user_master_mapper)

        is_login_disabled = rail.IfOperator(
            task_id='is_login_disabled',
            test="{{ result('bulk_getuser3').securityConfiguration.isLoginEnabled | is_falsy }}",
            yes_task='enable_login',
            no_task='process_firstname_update'
        )

        enable_login = rail.RepliconServiceOperator(
            task_id='enable_login',
            endpoint='/services/SecurityService1.svc/EnableLogin',
            data={
                'userUri': '{{ dag_run.conf.useruri }}'
            }
        )

        update_loginenabled_employment_daterange = rail.RepliconServiceOperator(
            task_id='update_loginenabled_employment_daterange',
            endpoint='/services/UserService1.svc/UpdateEmploymentDateRange',
            data={
                'userUri': '{{ dag_run.conf.useruri }}',
                'dateRange': {
                    'startDate': request_payload.get_today_date()
                }
            }
        )

        process_firstname_update = rail.EmptyOperator(
            task_id='process_firstname_update'
        )

        is_firstname_to_update = rail.IfOperator(
            task_id='is_firstname_to_update',
            test="{{ dag_run.conf.firstname | sn | is_truthy and \
                result('bulk_getuser3').userDetails.firstName | lower != dag_run.conf.firstname | lower }}",
            yes_task='update_firstname',
            no_task='process_lastname_update'
        )

        update_firstname = rail.RepliconServiceOperator(
            task_id='update_firstname',
            endpoint='/services/UserService1.svc/UpdateFirstName',
            data={
                'userUri': '{{ dag_run.conf.useruri }}',
                'firstname': '{{ dag_run.conf.firstname }}'
            }
        )

        process_lastname_update = rail.EmptyOperator(
            task_id='process_lastname_update'
        )

        is_lastname_to_update = rail.IfOperator(
            task_id='is_lastname_to_update',
            test="{{ dag_run.conf.lastname | sn | is_truthy and \
                result('bulk_getuser3').userDetails.lastName | lower != dag_run.conf.lastname | lower }}",
            yes_task='update_lastname',
            no_task='process_email_update'
        )

        update_lastname = rail.RepliconServiceOperator(
            task_id='update_lastname',
            endpoint='/services/UserService1.svc/UpdateLastName',
            data={
                'userUri': '{{ dag_run.conf.useruri }}',
                'lastname': '{{ dag_run.conf.lastname }}'
            }
        )

        process_email_update = rail.EmptyOperator(
            task_id='process_email_update'
        )

        is_emailid_to_update = rail.IfOperator(
            task_id='is_emailid_to_update',
            test="{{ dag_run.conf.workemail | sn | is_truthy and \
                dag_run.conf.workemail | lower != result('bulk_getuser3').userDetails.emailAddress | lower }}",
            yes_task='update_email',
            no_task='process_customfields_update'
        )

        update_email = rail.RepliconServiceOperator(
            task_id='update_email',
            endpoint='/services/UserService1.svc/UpdateEmail',
            data={
                'userUri': '{{ dag_run.conf.useruri }}',
                'email': '{{ dag_run.conf.workemail }}'
            }
        )

        update_loginname = rail.RepliconServiceOperator(
            task_id='update_loginname',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data={
                'user': {
                    'uri': '{{ dag_run.conf.useruri }}'
                },
                'modifications': {
                    'securitySettingsToApply': {
                        'loginName': '{{ dag_run.conf.workemail }}',
                        'ssoName': '{{ dag_run.conf.workemail }}',
                        'enabledAuthenticationTypeUris': [
                            'urn:replicon:user-authentication-type:sso'
                        ]
                    }
                },
                'userModificationOptionUri': 'urn:replicon:user-modification-option:save'
            }
        )

        process_customfields_update = rail.EmptyOperator(
            task_id='process_customfields_update'
        )

        get_customfields_to_update = rail.PythonOperator(
            task_id='get_customfields_to_update',
            python_callable=python_callable_method.get_customfields_to_updateuser
        )

        is_customfield_dropdowns_to_update = rail.IfOperator(
            task_id='is_customfield_dropdowns_to_update',
            test="{{ result('get_customfields_to_update').dropdownudf_payloads | length > 0 }}",
            yes_task='update_usercustomfields_dropdown',
            no_task='process_customfields_numericvalues'
        )

        update_usercustomfields_dropdown = rail.RepliconServiceCallForEachItemOperator(
            task_id='update_usercustomfields_dropdown',
            endpoint='/services/CustomFieldService1.svc/UpdateDropdownValue',
            items=lambda: rail.result('get_customfields_to_update')[
                'dropdownudf_payloads'],
            data=lambda item: item,
            flatten=True
        )

        process_customfields_numericvalues = rail.EmptyOperator(
            task_id='process_customfields_numericvalues')

        is_customfield_numericvalues_to_update = rail.IfOperator(
            task_id='is_customfield_numericvalues_to_update',
            test="{{ result('get_customfields_to_update').numeric_udf_payloads | length > 0 }}",
            yes_task='update_usercustomfields_numericvalues',
            no_task='process_manager_update'
        )

        update_usercustomfields_numericvalues = rail.RepliconServiceCallForEachItemOperator(
            task_id='update_usercustomfields_numericvalues',
            endpoint='/services/CustomFieldService1.svc/UpdateNumericValue',
            items=lambda: rail.result('get_customfields_to_update')[
                'numeric_udf_payloads'],
            data=lambda item: item,
            flatten=True
        )

        process_manager_update = rail.EmptyOperator(
            task_id='process_manager_update'
        )

        should_process_supervisor = rail.IfOperator(
            task_id='should_process_supervisor',
            test='{{ dag_run.conf.managerid | is_truthy }}',
            yes_task='process_supervisors',
            no_task='get_all_policysets'
        )

        process_supervisors = rail.EmptyOperator(
            task_id='process_supervisors'
        )

        (should_update_supervisor, finish_supervisor_assignment) = process_supervisor_assignment_task_group(
            is_update_user=True)

        get_all_policysets = rail.RepliconServiceOperator(
            task_id='get_all_policysets',
            endpoint='/services/PolicySetService1.svc/GetAllPolicySets'
        )

        is_creative_noncreative_present = rail.IfOperator(
            task_id='is_creative_noncreative_present',
            test=lambda dag_run: bool(dag_run.conf['creativenoncreative']),
            yes_task='get_employeetype_schedule_list',
            no_task='is_timesheetperiod_change'
        )

        get_employeetype_schedule_list = rail.PythonOperator(
            task_id='get_employeetype_schedule_list',
            python_callable=python_callable_method.get_employeetype_name_list
        )

        should_update_employeetype = rail.IfOperator(
            task_id='should_update_employeetype',
            test="{{ result('get_employeetype_schedule_list') | attr_or_default('current_employeetype_name') | sn | is_falsy or \
                result('get_employeetype_schedule_list') | attr_or_default('current_employeetype_name') | lower != \
                    dag_run.conf.creativenoncreative | lower }}",
            yes_task='get_required_employeetypegroup_uri',
            no_task='set_timeoffapprovalpath'
        )

        get_required_employeetypegroup_uri = rail.RepliconServiceOperator(
            task_id='get_required_employeetypegroup_uri',
            endpoint='/services/EmployeeTypeGroupService1.svc/GetAllEmployeeTypeGroups',
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['creativenoncreative'], 'uri')
        )

        is_employeetypegroup_present = rail.IfOperator(
            task_id='is_employeetypegroup_present',
            test="{{ result('get_required_employeetypegroup_uri') | sn | is_truthy }}",
            yes_task='put_employeetype_group_schedule',
            no_task='set_timeoffapprovalpath'
        )

        put_employeetype_group_schedule = rail.RepliconServiceOperator(
            task_id='put_employeetype_group_schedule',
            endpoint='/services/EmployeeTypeGroupService1.svc/PutEmployeeTypeGroupScheduleForUser',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'scheduleEntries': [*rail.result('get_employeetype_schedule_list')['employee_typelist'], {
                    'employeeTypeGroup': {
                        'uri': rail.result('get_required_employeetypegroup_uri')
                    },
                    'effectiveDate': request_payload.get_today_date()
                }]
            }
        )

        get_timesheet_template_name_uri = rail.PythonOperator(
            task_id='get_timesheet_template_name_uri',
            python_callable=python_callable_method.update_timesheet_vars_and_get_uri,
            op_args=['timesheetapprovalpathchange', 'timesheetperiodchange',
                     '{{ dag_run.conf.creativenoncreative }}']
        )

        is_required_timesheettemplate_uri_present = rail.IfOperator(
            task_id='is_required_timesheettemplate_uri_present',
            test="{{ result('get_timesheet_template_name_uri').uri | sn | is_truthy }}",
            yes_task='assign_timesheet_policy_set',
            no_task='set_timeoffapprovalpath'
        )

        assign_timesheet_policy_set = rail.RepliconServiceOperator(
            task_id='assign_timesheet_policy_set',
            endpoint='/services/PolicySetService1.svc/AssignPolicySetToUser',
            data={
                'userUri': '{{ dag_run.conf.useruri }}',
                'policySetUri': "{{ result('get_timesheet_template_name_uri').uri }}"
            }
        )

        set_timeoffapprovalpath = rail.PythonOperator(
            task_id='set_timeoffapprovalpath',
            python_callable=lambda: rail.set_result(
                'yes', 'timeoffapprovalpathchange')
        )

        is_timesheetperiod_change = rail.IfOperator(
            task_id='is_timesheetperiod_change',
            test="{{ result('get_timesheet_template_name_uri', 'timesheetperiodchange') == 'yes' }}",
            yes_task='get_timesheetperiod_schedule_list',
            no_task='process_servicecenter'
        )

        get_timesheetperiod_schedule_list = rail.PythonOperator(
            task_id='get_timesheetperiod_schedule_list',
            python_callable=python_callable_method.get_timesheetperiod_name_list,
            op_args=['{{ dag_run.conf.creativenoncreative }}']
        )

        should_update_timesheetperiod = rail.IfOperator(
            task_id='should_update_timesheetperiod',
            test="{{ result('get_timesheetperiod_schedule_list') | attr_or_default('required_timesheetperiod_name') | sn | is_truthy and \
                (result('get_timesheetperiod_schedule_list') | attr_or_default('current_timesheetperiod_name') | sn | is_falsy or \
                result('get_timesheetperiod_schedule_list') | attr_or_default('current_timesheetperiod_name') | lower != \
                    result('get_timesheetperiod_schedule_list') | attr_or_default('required_timesheetperiod_name') | lower) }}",
            yes_task='put_timesheet_period_group_schedule',
            no_task='process_servicecenter'
        )

        put_timesheet_period_group_schedule = rail.RepliconServiceOperator(
            task_id='put_timesheet_period_group_schedule',
            endpoint='/services/TimesheetPeriodService2.svc/PutTimesheetPeriodScheduleForUser',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'scheduleEntries': [*rail.result('get_timesheetperiod_schedule_list')['timesheet_periodlist'], {
                    'timesheetPeriod': {
                        'name': rail.result('get_timesheetperiod_schedule_list')['required_timesheetperiod_name']
                    },
                    'effectiveDate': request_payload.get_today_date()
                }]
            }
        )

        process_servicecenter = rail.EmptyOperator(
            task_id='process_servicecenter'
        )

        is_servicecenter_present = rail.IfOperator(
            task_id='is_servicecenter_present',
            test='{{ dag_run.conf.servicecenter | sn | is_truthy and dag_run.conf.servicecenter_uri | sn | is_truthy }}',
            yes_task='get_servicecenter_schedule_list',
            no_task='process_department'
        )

        get_servicecenter_schedule_list = rail.PythonOperator(
            task_id='get_servicecenter_schedule_list',
            python_callable=python_callable_method.get_servicecenter_name_list
        )

        should_update_servicecenter = rail.IfOperator(
            task_id='should_update_servicecenter',
            test="{{ result('get_servicecenter_schedule_list') | attr_or_default('current_servicecenter_name') | sn | is_falsy or \
                result('get_servicecenter_schedule_list') | attr_or_default('current_servicecenter_uri') != dag_run.conf.servicecenter_uri }}",
            yes_task='put_service_center_group_schedule',
            no_task='process_department'
        )

        put_service_center_group_schedule = rail.RepliconServiceOperator(
            task_id='put_service_center_group_schedule',
            endpoint='/services/ServiceCenterService1.svc/PutServiceCenterScheduleForUser',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'scheduleEntries': [*rail.result('get_servicecenter_schedule_list')['service_centerlist'], {
                    'serviceCenter': {
                        'uri': dag_run.conf['servicecenter_uri']
                    },
                    'effectiveDate': request_payload.get_today_date()
                }]
            }
        )

        process_department = rail.EmptyOperator(
            task_id='process_department'
        )

        is_departmentgroup_present = rail.IfOperator(
            task_id='is_departmentgroup_present',
            test="{{ dag_run.conf.departmentgroup | sn | is_truthy and dag_run.conf.departmentgroup != 'Technicolor' and \
                dag_run.conf.departmentgroup_uri | sn | is_truthy }}",
            yes_task='get_departmentgroup_schedule_list',
            no_task='process_location'
        )

        get_departmentgroup_schedule_list = rail.PythonOperator(
            task_id='get_departmentgroup_schedule_list',
            python_callable=python_callable_method.get_department_name_list
        )

        should_update_department = rail.IfOperator(
            task_id='should_update_department',
            test="{{ result('get_departmentgroup_schedule_list') | attr_or_default('current_department_name') | sn | is_falsy or \
                result('get_departmentgroup_schedule_list') | attr_or_default('current_department_uri') != dag_run.conf.departmentgroup_uri }}",
            yes_task='put_department_group_schedule',
            no_task='process_location'
        )

        put_department_group_schedule = rail.RepliconServiceOperator(
            task_id='put_department_group_schedule',
            endpoint='/services/DepartmentGroupService1.svc/PutDepartmentGroupScheduleForUser',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'scheduleEntries': [*rail.result('get_departmentgroup_schedule_list')['departmentlist'], {
                    'departmentGroup': {
                        'uri': dag_run.conf['departmentgroup_uri']
                    },
                    'effectiveDate': request_payload.get_today_date()
                }]
            }
        )

        set_timesheet_timeoff_approvalpathchange = rail.PythonOperator(
            task_id='set_timesheet_timeoff_approvalpathchange',
            python_callable=lambda: {
                'timesheetapprovalpathchange': rail.set_result('yes', 'timesheetapprovalpathchange'),
                'timeoffapprovalpathchange': rail.set_result('yes', 'timeoffapprovalpathchange')
            }
        )

        process_location = rail.EmptyOperator(
            task_id='process_location'
        )

        is_location_present = rail.IfOperator(
            task_id='is_location_present',
            test="{{ dag_run.conf.location | sn | is_truthy and \
                dag_run.conf.location_uri | sn | is_truthy }}",
            yes_task='get_location_schedule_list',
            no_task='process_punchentrychange'
        )

        get_location_schedule_list = rail.PythonOperator(
            task_id='get_location_schedule_list',
            python_callable=python_callable_method.get_location_name_list
        )

        should_update_location = rail.IfOperator(
            task_id='should_update_location',
            test="{{ result('get_location_schedule_list') | attr_or_default('current_location_uri') | sn | is_falsy or \
                result('get_location_schedule_list') | attr_or_default('current_location_uri') != dag_run.conf.location_uri }}",
            yes_task='put_location_schedule',
            no_task='process_punchentrychange'
        )

        put_location_schedule = rail.RepliconServiceOperator(
            task_id='put_location_schedule',
            endpoint='/services/LocationService1.svc/PutLocationScheduleForUser',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'scheduleEntries': [*rail.result('get_location_schedule_list')['locationlist'], {
                    'location': {
                        'uri': dag_run.conf['location_uri']
                    },
                    'effectiveDate': request_payload.get_today_date()
                }]
            }
        )

        set_approvalpath_template_change = rail.PythonOperator(
            task_id='set_approvalpath_template_change',
            python_callable=lambda: {
                'timesheetapprovalpathchange': rail.set_result('yes', 'timesheetapprovalpathchange'),
                'timeoffapprovalpathchange': rail.set_result('yes', 'timeoffapprovalpathchange'),
                'punchentrychange': rail.set_result('yes', 'punchentrychange'),
                'timesheetchange': rail.set_result('yes', 'timesheetchange')
            }
        )

        get_required_timezoneuri = rail.PythonOperator(
            task_id='get_required_timezoneuri',
            python_callable=python_callable_method.get_timezoneuri_to_update,
            op_args=['{{ dag_run.conf.worklocation }}']
        )

        should_update_timezone = rail.IfOperator(
            task_id='should_update_timezone',
            test="{{ result('get_required_timezoneuri') | is_truthy and \
                result('get_required_timezoneuri') != result('bulk_getuser3') | attr_or_default('timezone.uri') }}",
            yes_task='update_timezone',
            no_task='get_required_timeofftemplate'
        )

        update_timezone = rail.RepliconServiceOperator(
            task_id='update_timezone',
            endpoint='/services/InternationalizationService1.svc/UpdateTimeZoneForUser',
            data={
                'userUri': '{{ dag_run.conf.useruri }}',
                'timeZoneUri': "{{ result('get_required_timezoneuri') }}"
            }
        )

        get_required_timeofftemplate = rail.PythonOperator(
            task_id='get_required_timeofftemplate',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_mapper_entries_from_country'), 'type', 'Timeoff Template', 'value', ''),
        )

        should_remove_timeofftemplate = rail.IfOperator(
            task_id='should_remove_timeofftemplate',
            test="{{ result('get_required_timeofftemplate') | is_falsy and result('bulk_getuser3') | \
                attr_or_default('timeoffTemplate.uri') | sn | is_truthy }}",
            yes_task='remove_timeofftemplate',
            no_task='process_update_timeofftemplate'
        )

        remove_timeofftemplate = rail.RepliconServiceOperator(
            task_id='remove_timeofftemplate',
            endpoint='/services/PolicySetService1.svc/RemovePolicySetAssignmentFromUser',
            data={
                'userUri': '{{ dag_run.conf.useruri }}',
                'policySetUri': "{{ result('bulk_getuser3').timeoffTemplate.uri }}"
            }
        )

        process_update_timeofftemplate = rail.EmptyOperator(
            task_id='process_update_timeofftemplate'
        )

        should_update_timeofftemplate = rail.IfOperator(
            task_id='should_update_timeofftemplate',
            test="{{ result('get_required_timeofftemplate') | is_truthy and result('bulk_getuser3') | \
                attr_or_default('timeoffTemplate.uri') | is_falsy }}",
            yes_task='get_timeofftemplate_to_update',
            no_task='required_productlicenses'
        )

        get_timeofftemplate_to_update = rail.PythonOperator(
            task_id='get_timeofftemplate_to_update',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_policysets'), 'name', rail.result('get_required_timeofftemplate'), 'uri', '')
        )

        is_update_timeofftemplate = rail.IfOperator(
            task_id='is_update_timeofftemplate',
            test="{{ result('get_timeofftemplate_to_update') | is_truthy }}",
            yes_task='update_timeofftemplate',
            no_task='required_productlicenses'
        )

        update_timeofftemplate = rail.RepliconServiceOperator(
            task_id='update_timeofftemplate',
            endpoint='/services/PolicySetService1.svc/AssignPolicySetToUser',
            data={
                'userUri': '{{ dag_run.conf.useruri }}',
                'policySetUri': "{{ result('get_timeofftemplate_to_update') }}"
            }
        )

        required_productlicenses = rail.PythonOperator(
            task_id='required_productlicenses',
            python_callable=request_payload.get_product_uris
        )

        current_user_license_uris = rail.RepliconServiceOperator(
            task_id='current_user_license_uris',
            endpoint='/services/AccountManagementService1.svc/GetProductAssignmentsForUser',
            data={
                'userUri': '{{ dag_run.conf.useruri }}'
            },
            data_handler=lambda response: [x['uri'] for x in response]
        )

        should_update_licenses = rail.IfOperator(
            task_id='should_update_licenses',
            test=lambda: rail.result('required_productlicenses') != rail.result(
                'current_user_license_uris'),
            yes_task='put_product_assignments',
            no_task='process_punchentrychange'
        )

        put_product_assignments = rail.RepliconServiceOperator(
            task_id='put_product_assignments',
            endpoint='/services/AccountManagementService1.svc/PutProductAssignmentsForUser',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'productUris': rail.result('required_productlicenses')
            }
        )

        process_punchentrychange = rail.EmptyOperator(
            task_id='process_punchentrychange'
        )

        is_punchentrychange = rail.IfOperator(
            task_id='is_punchentrychange',
            # pylint: disable=line-too-long
            test="{{ result('get_customfields_to_update', 'punchentrychange') == 'yes' or result('set_approvalpath_template_change', 'punchentrychange') == 'yes' }}",
            yes_task='get_required_punchentry_policy',
            no_task='process_timeoffapprovalpathchange'
        )

        get_required_punchentry_policy = rail.PythonOperator(
            task_id='get_required_punchentry_policy',
            python_callable=python_callable_method.get_punchentry_policy,
            op_args=['{{ dag_run.conf.jobcategory }}']
        )

        get_assigned_punchentry_policy = rail.RepliconServiceOperator(
            task_id='get_assigned_punchentry_policy',
            endpoint='/services/PolicySetService1.svc/GetAssignedPolicySetsForUser',
            data={
                'userUri': '{{ dag_run.conf.useruri }}'
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'policySet.displayText', rail.result('get_required_punchentry_policy'), 'policySet', {}).get('displayText', '')
        )

        should_update_punchentry_policy = rail.IfOperator(
            task_id='should_update_punchentry_policy',
            test="{{ result('get_required_punchentry_policy') | is_truthy and \
                result('get_required_punchentry_policy') != result('get_assigned_punchentry_policy') }}",
            yes_task='get_punchentry_policy_to_update',
            no_task='process_remove_punchentry_policy'
        )

        get_punchentry_policy_to_update = rail.PythonOperator(
            task_id='get_punchentry_policy_to_update',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_policysets'), 'name', rail.result('get_required_punchentry_policy'), 'uri', '')
        )

        is_update_punchentry = rail.IfOperator(
            task_id='is_update_punchentry',
            test="{{ result('get_punchentry_policy_to_update') | is_truthy }}",
            yes_task='update_punchentry',
            no_task='process_remove_punchentry_policy'
        )

        update_punchentry = rail.RepliconServiceOperator(
            task_id='update_punchentry',
            endpoint='/services/PolicySetService1.svc/AssignPolicySetToUser',
            data={
                'userUri': '{{ dag_run.conf.useruri }}',
                'policySetUri': "{{ result('get_punchentry_policy_to_update') }}"
            }
        )

        process_remove_punchentry_policy = rail.EmptyOperator(
            task_id='process_remove_punchentry_policy'
        )

        should_remove_punchentry_policy = rail.IfOperator(
            task_id='should_remove_punchentry_policy',
            test="{{ result('get_required_punchentry_policy') | is_falsy and result('get_assigned_punchentry_policy') | is_truthy }}",
            yes_task='remove_punchentry_policy',
            no_task='process_timeoffapprovalpathchange'
        )

        remove_punchentry_policy = rail.RepliconServiceOperator(
            task_id='remove_punchentry_policy',
            endpoint='/services/PolicySetService1.svc/RemovePolicySetAssignmentFromUser',
            data={
                'userUri': '{{ dag_run.conf.useruri }}',
                'policySetUri': "{{ result('get_assigned_punchentry_policy') }}"
            }
        )

        process_timeoffapprovalpathchange = rail.EmptyOperator(
            task_id='process_timeoffapprovalpathchange'
        )

        is_timeoffapprovalpathchange = rail.IfOperator(
            task_id='is_timeoffapprovalpathchange',
            test="{{ result('get_customfields_to_update', 'timeoffapprovalpathchange') == 'yes' or \
                result('set_timeoffapprovalpath', 'timeoffapprovalpathchange') == 'yes' or \
                    result('set_timesheet_timeoff_approvalpathchange', 'timeoffapprovalpathchange') == 'yes' or \
                        result('set_approvalpath_template_change', 'timeoffapprovalpathchange') == 'yes' }}",
            yes_task='get_required_timeoffapprovalpath',
            no_task='process_timesheetapprovalpathchange'
        )

        get_required_timeoffapprovalpath = rail.PythonOperator(
            task_id='get_required_timeoffapprovalpath',
            python_callable=python_callable_method.get_timeoff_approvalpath,
            op_args=['{{ dag_run.conf.creativenoncreative }}',
                     '{{ dag_run.conf.businessunitname }}']
        )

        should_update_timeoffapprovalpath = rail.IfOperator(
            task_id='should_update_timeoffapprovalpath',
            test="{{ result('get_required_timeoffapprovalpath') | is_truthy and \
                result('get_required_timeoffapprovalpath') != result('bulk_getuser3') | attr_or_default('timeoffApprovalPath.displayText') }}",
            yes_task='get_timeoffapprovalpath_to_update',
            no_task='process_timesheetapprovalpathchange'
        )

        get_timeoffapprovalpath_to_update = rail.RepliconServiceOperator(
            task_id='get_timeoffapprovalpath_to_update',
            endpoint='/services/TimeOffApprovalService1.svc/GetAllApprovalPaths',
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', rail.result('get_required_timeoffapprovalpath'), 'uri', '')
        )

        is_update_timeoffapprovalpath = rail.IfOperator(
            task_id='is_update_timeoffapprovalpath',
            test="{{ result('get_timeoffapprovalpath_to_update') | is_truthy }}",
            yes_task='update_timeoffapprovalpath',
            no_task='process_timesheetapprovalpathchange'
        )

        update_timeoffapprovalpath = rail.RepliconServiceOperator(
            task_id='update_timeoffapprovalpath',
            endpoint='/services/TimeOffApprovalService1.svc/UpdateApprovalPathForUser',
            data={
                'userUri': '{{ dag_run.conf.useruri }}',
                'approvalPathUri': "{{ result('get_timeoffapprovalpath_to_update') }}"
            }
        )

        process_timesheetapprovalpathchange = rail.EmptyOperator(
            task_id='process_timesheetapprovalpathchange'
        )

        is_timesheetapprovalpathchange = rail.IfOperator(
            task_id='is_timesheetapprovalpathchange',
            test="{{ result('get_timesheet_template_name_uri', 'timesheetapprovalpathchange') == 'yes' or \
                    result('set_timesheet_timeoff_approvalpathchange', 'timesheetapprovalpathchange') == 'yes' or \
                        result('set_approvalpath_template_change', 'timesheetapprovalpathchange') == 'yes' }}",
            yes_task='get_required_timesheetapprovalpath',
            no_task='process_timesheetchange'
        )

        def get_timesheet_approvalpath(creativenoncreative, businessunitname, department):

            null = None
            timesheetapproval_path = null
            get_mapper_entries_from_businessunitname = rail.result(
                'get_mapper_entries_from_businessunitname')

            get_mapper_entries_from_country_location = rail.result(
                'get_mapper_entries_from_country_location')

            get_mapper_entries_from_country = rail.result(
                'get_mapper_entries_from_country')

            timesheetapproval_path_entry = request_payload.get_timesheetapproval_path_entry(
                creativenoncreative, get_mapper_entries_from_country_location, get_mapper_entries_from_country,
                businessunitname, department)

            if not timesheetapproval_path_entry:
                if get_mapper_entries_from_businessunitname:
                    timesheetapproval_path = rail.find_first_by_attr_and_get_attr(
                        get_mapper_entries_from_businessunitname, 'type', 'Timesheet Approval path', 'value')

                elif creativenoncreative == 'Creative':
                    timesheetapproval_path_entry = request_payload.get_timesheetapproval_path_entry(
                        creativenoncreative, get_mapper_entries_from_country_location, get_mapper_entries_from_country,
                        businessunitname)
                    timesheetapproval_path = timesheetapproval_path_entry[
                        0] if timesheetapproval_path_entry else null
            else:
                timesheetapproval_path = timesheetapproval_path_entry[
                    0] if timesheetapproval_path_entry else null
            return timesheetapproval_path

        get_required_timesheetapprovalpath = rail.PythonOperator(
            task_id='get_required_timesheetapprovalpath',
            python_callable=get_timesheet_approvalpath,
            op_args=['{{ dag_run.conf.creativenoncreative }}',
                     '{{ dag_run.conf.businessunitname }}',
                     '{{ dag_run.conf.department }}']
        )

        should_update_timesheetapprovalpath = rail.IfOperator(
            task_id='should_update_timesheetapprovalpath',
            test="{{ result('get_required_timesheetapprovalpath') | is_truthy and \
                result('get_required_timesheetapprovalpath') != result('bulk_getuser3') | \
                    attr_or_default('timesheetApprovalPath.displayText') }}",
            yes_task='get_timesheetapprovalpath_to_update',
            no_task='process_timesheetchange'
        )

        get_timesheetapprovalpath_to_update = rail.RepliconServiceOperator(
            task_id='get_timesheetapprovalpath_to_update',
            endpoint='/services/TimesheetApprovalService1.svc/GetAllApprovalPaths',
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', rail.result('get_required_timesheetapprovalpath'), 'uri', '')
        )

        is_update_timesheetapprovalpath = rail.IfOperator(
            task_id='is_update_timesheetapprovalpath',
            test="{{ result('get_timesheetapprovalpath_to_update') | is_truthy }}",
            yes_task='update_timesheetapprovalpath',
            no_task='process_timesheetchange'
        )

        update_timesheetapprovalpath = rail.RepliconServiceOperator(
            task_id='update_timesheetapprovalpath',
            endpoint='/services/TimesheetApprovalService1.svc/UpdateApprovalPathForUser',
            data={
                'userUri': '{{ dag_run.conf.useruri }}',
                'approvalPathUri': "{{ result('get_timesheetapprovalpath_to_update') }}"
            }
        )

        process_timesheetchange = rail.EmptyOperator(
            task_id='process_timesheetchange'
        )

        is_timesheet_change = rail.IfOperator(
            task_id='is_timesheet_change',
            test="{{ result('get_customfields_to_update', 'timesheetchange') == 'yes' or \
                result('set_approvalpath_template_change', 'timesheetchange') == 'yes' }}",
            yes_task='get_required_timesheettemplate_uri',
            no_task='process_division'
        )

        def get_required_timesheettemplate_uri_from_jobcategory(job_category):
            required_timesheettemplate_uri = ''
            should_assign = False
            get_mapper_entries_from_businessunitname = rail.result(
                'get_mapper_entries_from_businessunitname')
            get_mapper_entries_from_country_location = rail.result(
                'get_mapper_entries_from_country_location')
            get_default_mapper_entries_from_country = rail.result(
                'get_default_mapper_entries_from_country')
            timesheet_template = request_payload.get_timesheettemplate_name(job_category, get_mapper_entries_from_businessunitname,
                                                                            get_mapper_entries_from_country_location, get_default_mapper_entries_from_country)
            if timesheet_template:
                required_timesheettemplate_uri = rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_policysets'), 'name', timesheet_template, 'uri')
                should_assign = timesheet_template != rail.result(
                    'bulk_getuser3')['timesheetTemplate']['displayText']
            return {
                'name': timesheet_template,
                'uri': required_timesheettemplate_uri,
                'should_assign': should_assign
            }
        get_required_timesheettemplate_uri = rail.PythonOperator(
            task_id='get_required_timesheettemplate_uri',
            python_callable=get_required_timesheettemplate_uri_from_jobcategory,
            op_args=['{{ dag_run.conf.jobcategory }}']
        )

        should_update_timesheet = rail.IfOperator(
            task_id='should_update_timesheet',
            test="{{ result('get_required_timesheettemplate_uri').uri | is_truthy and \
                result('get_required_timesheettemplate_uri').should_assign | is_truthy }}",
            yes_task='update_timesheettemplate_for_user',
            no_task='process_division'
        )

        update_timesheettemplate_for_user = rail.RepliconServiceOperator(
            task_id='update_timesheettemplate_for_user',
            endpoint='/services/PolicySetService1.svc/AssignPolicySetToUser',
            data={
                'userUri': '{{ dag_run.conf.useruri }}',
                'policySetUri': "{{ result('get_required_timesheettemplate_uri').uri }}"
            }
        )

        process_division = rail.EmptyOperator(
            task_id='process_division'
        )

        is_division_present = rail.IfOperator(
            task_id='is_division_present',
            test="{{ dag_run.conf.legalentityname | sn | is_truthy and \
                dag_run.conf.division_uri | sn | is_truthy }}",
            yes_task='get_division_schedule_list',
            no_task='process_costcenter'
        )

        get_division_schedule_list = rail.PythonOperator(
            task_id='get_division_schedule_list',
            python_callable=python_callable_method.get_division_name_list
        )

        should_update_division = rail.IfOperator(
            task_id='should_update_division',
            test="{{ result('get_division_schedule_list') | attr_or_default('current_division_uri') | sn | is_falsy or \
                result('get_division_schedule_list') | attr_or_default('current_division_uri') != dag_run.conf.division_uri }}",
            yes_task='put_division_schedule',
            no_task='process_costcenter'
        )

        put_division_schedule = rail.RepliconServiceOperator(
            task_id='put_division_schedule',
            endpoint='/services/DivisionService1.svc/PutDivisionScheduleForUser',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'scheduleEntries': [*rail.result('get_division_schedule_list')['divisionlist'], {
                    'division': {
                        'uri': dag_run.conf['division_uri']
                    },
                    'effectiveDate': request_payload.get_today_date()
                }]
            }
        )

        process_costcenter = rail.EmptyOperator(
            task_id='process_costcenter'
        )

        is_costcenter_present = rail.IfOperator(
            task_id='is_costcenter_present',
            test="{{ dag_run.conf.costcentername | sn | is_truthy and \
                dag_run.conf.costcenter_uri | sn | is_truthy }}",
            yes_task='get_costcenter_schedule_list',
            no_task='get_payrule_to_assign'
        )

        get_costcenter_schedule_list = rail.PythonOperator(
            task_id='get_costcenter_schedule_list',
            python_callable=python_callable_method.get_costcenter_name_list
        )

        should_update_costcenter = rail.IfOperator(
            task_id='should_update_costcenter',
            test="{{ result('get_costcenter_schedule_list') | attr_or_default('current_costcenter_uri') | sn | is_falsy or \
                result('get_costcenter_schedule_list') | attr_or_default('current_costcenter_uri') != dag_run.conf.costcenter_uri }}",
            yes_task='put_costcenter_schedule',
            no_task='get_payrule_to_assign'
        )

        put_costcenter_schedule = rail.RepliconServiceOperator(
            task_id='put_costcenter_schedule',
            endpoint='/services/CostCenterService1.svc/PutCostCenterScheduleForUser',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'scheduleEntries': [*rail.result('get_costcenter_schedule_list')['costcenterlist'], {
                    'costCenter': {
                        'uri': dag_run.conf['costcenter_uri']
                    },
                    'effectiveDate': request_payload.get_today_date()
                }]
            }
        )

        get_payrule_to_assign = rail.PythonOperator(
            task_id='get_payrule_to_assign',
            python_callable=python_callable_method.get_payrule_to_assign,
            op_args=['{{ dag_run.conf.jobcategory }}']
        )

        should_assign_payrule = rail.IfOperator(
            task_id='should_assign_payrule',
            test="{{ result('get_payrule_to_assign') | is_truthy }}",
            yes_task='update_payrule',
            no_task='trigger_timeoff_assignment_updateuser'
        )

        update_payrule = rail.RepliconServiceOperator(
            task_id='update_payrule',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri']
                },
                "modifications": {
                    "payRulesScheduleModifications": {
                        "scheduleEntries": [
                            {
                                "payRuleScript": {
                                    "name": rail.result('get_payrule_to_assign')
                                },
                                "effectiveDate": request_payload.get_today_date()
                            }
                        ]
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        trigger_timeoff_assignment_updateuser = rail.TriggerDagRunOperator(
            task_id='trigger_timeoff_assignment_updateuser',
            retries=0,
            trigger_dag_id=f'technicolorg3_user_import_child_timeoff_assignment_{config.instance}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            conf=lambda dag_run: {
                'useruri': dag_run.conf['useruri'],
                'login_name': dag_run.conf['workemail'],
                'country': dag_run.conf['country'],
                'businessunitname': dag_run.conf['businessunitname'],
                'jobcategory': dag_run.conf['jobcategory'],
                'action': 'update'
            }
        )

        get_updateuser_exception_logs = rail.PythonOperator(
            task_id='get_updateuser_exception_logs',
            python_callable=python_callable_method.get_updateuser_exception_logs
        )

        write_updateuser_log = rail.WriteLogOperator(
            task_id='write_updateuser_log',
            log="{{ result('create_user_log') }}",
            severity='\
                {%- if result("get_updateuser_exception_logs").exception | is_truthy -%} \
                    Exception\
                {%- elif result("get_updateuser_exception_logs").logs | is_truthy -%} \
                    Success\
                {%- else -%}\
                    Skipped\
                {%- endif -%}',
            message='\
                {%- if result("get_updateuser_exception_logs").exception | is_truthy -%} \
                    Partialy updated - {{ result("get_updateuser_exception_logs").exception }}\
                {%- elif result("get_updateuser_exception_logs").logs | is_truthy -%} \
                    Successfully updated\
                {%- else -%}\
                    No change to the user record in Replicon\
                {%- endif -%}',
            properties={
                'globalid': '{{ dag_run.conf.globalid }}',
                'action': 'Update',
                'status': '\
                    {%- if result("get_updateuser_exception_logs").exception | is_truthy -%} \
                        Exception\
                    {%- elif result("get_updateuser_exception_logs").logs | is_truthy -%} \
                        Success\
                    {%- else -%}\
                        Skipped\
                    {%- endif -%}',
                'details': '\
                    {%- if result("get_updateuser_exception_logs").exception | is_truthy -%} \
                        Partialy updated - {{ result("get_updateuser_exception_logs").exception }}\
                    {%- elif result("get_updateuser_exception_logs").logs | is_truthy -%} \
                        Successfully updated\
                    {%- else -%}\
                        No change to the user record in Replicon\
                    {%- endif -%}',
                'username': '{{ dag_run.conf.username }}',
                'new_location': '\
                    {%- if result("get_mapper_entries_from_country_location") | length > 0 -%} \
                        No\
                    {%- else -%} \
                        Yes\
                    {%- endif -%}',
                'location': '{{ dag_run.conf.location }}'
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log="{{ result('create_user_log') }}",
            severity='Error',
            message="{{ get_error_message() }}",
            properties={
                'globalid': '{{ dag_run.conf.globalid }}',
                'action': 'Update',
                'status': 'Error',
                'details': "{{ get_error_message() }}",
                'username': '{{ dag_run.conf.username }}',
                'new_location': '\
                    {%- if result("get_mapper_entries_from_country_location") | length > 0 -%} \
                        No\
                    {%- else -%} \
                        Yes\
                    {%- endif -%}',
                'location': '{{ dag_run.conf.location }}'
            }
        )

        log_dagrun_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_dagrun_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors

        can_run_batch_task >> rail.Label(
            'No') >> create_user_log

        create_user_log >> is_multipleuser_with_employeeid

        is_multipleuser_with_employeeid >> rail.Label(
            'Yes') >> write_multipleuser_exception >> catch_and_log_errors

        is_multipleuser_with_employeeid >> rail.Label(
            'No') >> bulk_getuser3 >> is_adminudf_modified

        is_adminudf_modified >> rail.Label(
            'Yes') >> write_adminuser_udf_exception >> catch_and_log_errors

        is_adminudf_modified >> rail.Label(
            'No') >> process_mappers >> is_businessunitname_servicelinename

        get_default_mapper_entries_from_country >> is_login_disabled

        is_login_disabled >> rail.Label(
            'Yes') >> enable_login >> update_loginenabled_employment_daterange >> process_firstname_update

        is_login_disabled >> rail.Label(
            'No') >> process_firstname_update

        process_firstname_update >> is_firstname_to_update

        is_firstname_to_update >> rail.Label(
            'Yes') >> update_firstname >> process_lastname_update

        is_firstname_to_update >> rail.Label(
            'No') >> process_lastname_update

        process_lastname_update >> is_lastname_to_update

        is_lastname_to_update >> rail.Label(
            'Yes') >> update_lastname >> process_email_update

        is_lastname_to_update >> rail.Label(
            'No') >> process_email_update

        process_email_update >> is_emailid_to_update

        is_emailid_to_update >> rail.Label(
            'Yes') >> update_email >> update_loginname >> process_customfields_update

        is_emailid_to_update >> rail.Label(
            'No') >> process_customfields_update

        process_customfields_update >> get_customfields_to_update >> is_customfield_dropdowns_to_update

        is_customfield_dropdowns_to_update >> rail.Label(
            'Yes') >> update_usercustomfields_dropdown >> process_customfields_numericvalues

        is_customfield_dropdowns_to_update >> rail.Label(
            'No') >> process_customfields_numericvalues

        process_customfields_numericvalues >> is_customfield_numericvalues_to_update

        is_customfield_numericvalues_to_update >> rail.Label(
            'Yes') >> update_usercustomfields_numericvalues >> process_manager_update

        is_customfield_numericvalues_to_update >> rail.Label(
            'No') >> process_manager_update

        process_manager_update >> should_process_supervisor

        should_process_supervisor >> rail.Label(
            'Yes') >> process_supervisors >> should_update_supervisor

        finish_supervisor_assignment >> get_all_policysets

        should_process_supervisor >> rail.Label(
            'No') >> get_all_policysets

        get_all_policysets >> is_creative_noncreative_present

        is_creative_noncreative_present >> rail.Label(
            'Yes') >> get_employeetype_schedule_list >> should_update_employeetype

        should_update_employeetype >> rail.Label(
            'Yes') >> get_required_employeetypegroup_uri >> is_employeetypegroup_present

        is_employeetypegroup_present >> rail.Label(
            'Yes') >> put_employeetype_group_schedule >> get_timesheet_template_name_uri

        is_employeetypegroup_present >> rail.Label(
            'No') >> set_timeoffapprovalpath

        should_update_employeetype >> rail.Label(
            'No') >> set_timeoffapprovalpath

        is_creative_noncreative_present >> rail.Label(
            'No') >> is_timesheetperiod_change

        get_timesheet_template_name_uri >> is_required_timesheettemplate_uri_present

        is_required_timesheettemplate_uri_present >> rail.Label(
            'Yes') >> assign_timesheet_policy_set >> set_timeoffapprovalpath

        is_required_timesheettemplate_uri_present >> rail.Label(
            'No') >> set_timeoffapprovalpath

        set_timeoffapprovalpath >> is_timesheetperiod_change

        is_timesheetperiod_change >> rail.Label(
            'Yes') >> get_timesheetperiod_schedule_list >> should_update_timesheetperiod

        is_timesheetperiod_change >> rail.Label(
            'No') >> process_servicecenter

        should_update_timesheetperiod >> rail.Label(
            'Yes') >> put_timesheet_period_group_schedule >> process_servicecenter

        should_update_timesheetperiod >> rail.Label(
            'No') >> process_servicecenter

        process_servicecenter >> is_servicecenter_present

        is_servicecenter_present >> rail.Label(
            'Yes') >> get_servicecenter_schedule_list >> should_update_servicecenter

        should_update_servicecenter >> rail.Label(
            'Yes') >> put_service_center_group_schedule >> process_department

        should_update_servicecenter >> rail.Label(
            'No') >> process_department

        is_servicecenter_present >> rail.Label(
            'No') >> process_department

        process_department >> is_departmentgroup_present

        is_departmentgroup_present >> rail.Label(
            'Yes') >> get_departmentgroup_schedule_list >> should_update_department

        should_update_department >> rail.Label(
            'Yes') >> put_department_group_schedule >> set_timesheet_timeoff_approvalpathchange >> process_location

        should_update_department >> rail.Label(
            'No') >> process_location

        is_departmentgroup_present >> rail.Label(
            'No') >> process_location

        process_location >> is_location_present

        is_location_present >> rail.Label(
            'Yes') >> get_location_schedule_list >> should_update_location

        should_update_location >> rail.Label(
            'Yes') >> put_location_schedule >> set_approvalpath_template_change >> \
            get_required_timezoneuri >> should_update_timezone

        should_update_timezone >> rail.Label(
            'Yes') >> update_timezone >> get_required_timeofftemplate

        should_update_timezone >> rail.Label(
            'No') >> get_required_timeofftemplate

        get_required_timeofftemplate >> should_remove_timeofftemplate

        should_remove_timeofftemplate >> rail.Label(
            'Yes') >> remove_timeofftemplate >> process_update_timeofftemplate

        should_remove_timeofftemplate >> rail.Label(
            'No') >> process_update_timeofftemplate

        process_update_timeofftemplate >> should_update_timeofftemplate

        should_update_timeofftemplate >> rail.Label(
            'Yes') >> get_timeofftemplate_to_update >> is_update_timeofftemplate

        is_update_timeofftemplate >> rail.Label(
            'Yes') >> update_timeofftemplate >> required_productlicenses

        is_update_timeofftemplate >> rail.Label(
            'No') >> required_productlicenses

        should_update_timeofftemplate >> rail.Label(
            'No') >> required_productlicenses

        required_productlicenses >> current_user_license_uris >> should_update_licenses

        should_update_licenses >> rail.Label(
            'Yes') >> put_product_assignments >> process_punchentrychange

        should_update_licenses >> rail.Label(
            'No') >> process_punchentrychange

        should_update_location >> rail.Label(
            'No') >> process_punchentrychange

        is_location_present >> rail.Label(
            'No') >> process_punchentrychange

        process_punchentrychange >> is_punchentrychange

        is_punchentrychange >> rail.Label(
            'Yes') >> get_required_punchentry_policy >> get_assigned_punchentry_policy >> should_update_punchentry_policy

        should_update_punchentry_policy >> rail.Label(
            'Yes') >> get_punchentry_policy_to_update >> is_update_punchentry

        is_update_punchentry >> rail.Label(
            'Yes') >> update_punchentry >> process_remove_punchentry_policy

        is_update_punchentry >> rail.Label(
            'No') >> process_remove_punchentry_policy

        should_update_punchentry_policy >> rail.Label(
            'No') >> process_remove_punchentry_policy

        process_remove_punchentry_policy >> should_remove_punchentry_policy

        should_remove_punchentry_policy >> rail.Label(
            'Yes') >> remove_punchentry_policy >> process_timeoffapprovalpathchange

        should_remove_punchentry_policy >> rail.Label(
            'No') >> process_timeoffapprovalpathchange

        is_punchentrychange >> rail.Label(
            'No') >> process_timeoffapprovalpathchange

        process_timeoffapprovalpathchange >> is_timeoffapprovalpathchange

        is_timeoffapprovalpathchange >> rail.Label(
            'Yes') >> get_required_timeoffapprovalpath >> should_update_timeoffapprovalpath

        should_update_timeoffapprovalpath >> rail.Label(
            'Yes') >> get_timeoffapprovalpath_to_update >> is_update_timeoffapprovalpath

        is_update_timeoffapprovalpath >> rail.Label(
            'Yes') >> update_timeoffapprovalpath >> process_timesheetapprovalpathchange

        is_update_timeoffapprovalpath >> rail.Label(
            'No') >> process_timesheetapprovalpathchange

        should_update_timeoffapprovalpath >> rail.Label(
            'No') >> process_timesheetapprovalpathchange

        is_timeoffapprovalpathchange >> rail.Label(
            'No') >> process_timesheetapprovalpathchange

        process_timesheetapprovalpathchange >> is_timesheetapprovalpathchange

        is_timesheetapprovalpathchange >> rail.Label(
            'Yes') >> get_required_timesheetapprovalpath >> should_update_timesheetapprovalpath

        should_update_timesheetapprovalpath >> rail.Label(
            'Yes') >> get_timesheetapprovalpath_to_update >> is_update_timesheetapprovalpath

        is_update_timesheetapprovalpath >> rail.Label(
            'Yes') >> update_timesheetapprovalpath >> process_timesheetchange

        is_update_timesheetapprovalpath >> rail.Label(
            'No') >> process_timesheetchange

        should_update_timesheetapprovalpath >> rail.Label(
            'No') >> process_timesheetchange

        is_timesheetapprovalpathchange >> rail.Label(
            'No') >> process_timesheetchange

        process_timesheetchange >> is_timesheet_change

        is_timesheet_change >> rail.Label(
            'Yes') >> get_required_timesheettemplate_uri >> should_update_timesheet

        should_update_timesheet >> rail.Label(
            'Yes') >> update_timesheettemplate_for_user >> process_division

        should_update_timesheet >> rail.Label(
            'No') >> process_division

        is_timesheet_change >> rail.Label(
            'No') >> process_division

        process_division >> is_division_present

        is_division_present >> rail.Label(
            'Yes') >> get_division_schedule_list >> should_update_division

        should_update_division >> rail.Label(
            'Yes') >> put_division_schedule >> process_costcenter

        should_update_division >> rail.Label(
            'No') >> process_costcenter

        is_division_present >> rail.Label(
            'No') >> process_costcenter

        process_costcenter >> is_costcenter_present

        is_costcenter_present >> rail.Label(
            'Yes') >> get_costcenter_schedule_list >> should_update_costcenter

        should_update_costcenter >> rail.Label(
            'Yes') >> put_costcenter_schedule >> get_payrule_to_assign

        should_update_costcenter >> rail.Label(
            'No') >> get_payrule_to_assign

        is_costcenter_present >> rail.Label(
            'No') >> get_payrule_to_assign

        get_payrule_to_assign >> should_assign_payrule

        should_assign_payrule >> rail.Label(
            'Yes') >> update_payrule >> trigger_timeoff_assignment_updateuser

        should_assign_payrule >> rail.Label(
            'No') >> trigger_timeoff_assignment_updateuser

        trigger_timeoff_assignment_updateuser >> get_updateuser_exception_logs >> write_updateuser_log >> \
            catch_and_log_errors

        catch_and_log_errors >> log_dagrun_to_sumo

        return dag


rail.for_each_instance(create_updateuser_child_dag)
