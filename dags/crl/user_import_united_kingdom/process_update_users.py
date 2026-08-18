from datetime import timedelta
from airflow.models import Variable
import rail

from crl.user_import_united_kingdom.utils import request_payload, response_filter, python_callable_methods
from crl.user_import_united_kingdom.tasks.process_supervisor import process_supervisor_assignment_task_group

null= None

# pylint: disable=too-many-statements
def create_child_dag(config):
    update_dags = []

    for idx in range(0, config.BATCH_COUNT):

        with rail.create_airflow_dag(
            dag_id=f"{config.process_update_users_dagid}_batch_{idx+1}",
            description='CRL User Import United Kingdom Process Update Users',
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.max_active_runs_process_update_users,
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

            can_run_batch_task = rail.IfOperator(
                task_id='can_run_batch_task',
                test=lambda: Variable.get(
                    config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
                yes_task='batch_task',
                no_task='has_valid_data'
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id='batch_task',
                execution_timeout=timedelta(
                    days=config.execution_timeout_days),
                start_task='has_valid_data',
                end_task='catch_and_log_errors',
            )

            has_valid_data = rail.IfOperator(
                task_id='has_valid_data',
                test=lambda dag_run:request_payload.validate_update_data(dag_run, config.END_DATE_STATUS),
                yes_task="get_user_info",
                no_task="log_invalid_data"
            )

            log_invalid_data = rail.WriteLogOperator(
                task_id='log_invalid_data',
                log='{{ dag_run.conf.user_log }}',
                message=lambda dag_run:request_payload.get_invalid_update_message(dag_run, config.END_DATE_STATUS),
                severity='Exception',
                properties=lambda dag_run: {
                    "employee_id": dag_run.conf['emp_id'],
                    "first_name": dag_run.conf['first_name'],
                    "last_name": dag_run.conf['last_name'],
                    "action": "Validation",
                    "status": "Exception",
                    'details':  request_payload.get_invalid_update_message(dag_run, config.END_DATE_STATUS),
                }
            )

            get_user_info = rail.RepliconServiceOperator(
                task_id='get_user_info',
                endpoint='/services/ImportService1.svc/BulkGetUsers3',
                data={
                    "users": [
                        {
                            "uri": '{{ dag_run.conf.useruri }}',
                            "loginName": null,
                            "parameterCorrelationId": null
                        }
                    ],
                    "dataLoadOptionUri": "urn:replicon:data-load-option:fail-if-insufficient-data-access-permission"
                },
                data_handler=lambda res: res[0]
            )

            is_change_effective_date_present= rail.IfOperator(
                task_id="is_change_effective_date_present",
                test=lambda dag_run: bool(dag_run.conf['change_effective_date']),
                yes_task="is_status_active",
                no_task="log_change_effective_date_exception"
            )

            log_change_effective_date_exception = rail.WriteLogOperator(
                task_id = 'log_change_effective_date_exception',
                log = '{{ dag_run.conf.user_log }}',
                message = "Change Effective date blank in payload",
                severity='Exception',
                properties =lambda dag_run: {
                    "employee_id": dag_run.conf['emp_id'],
                    "last_name": dag_run.conf['last_name'],
                    "first_name": dag_run.conf['first_name'],
                    "action": "Rehire" if request_payload.validate_rehire(dag_run) else "Update",
                    "status": "Exception",
                    'details': "Change Effective date blank in payload",
                }
            )

            is_status_active = rail.IfOperator(
                task_id="is_status_active",
                test=lambda dag_run: dag_run.conf['emp_status'] in config.ACTIVE_STATUS,
                yes_task="is_contingent_user",
                no_task="is_status_unpaid_leave"
            )

            is_status_unpaid_leave = rail.IfOperator(
                task_id="is_status_unpaid_leave",
                test=lambda dag_run: dag_run.conf['emp_status'] == 'Unpaid Leave',
                yes_task="update_required_udfs",
                no_task="process_user_disable"
            )

            update_required_udfs = rail.RepliconServiceOperator(
                task_id='update_required_udfs',
                endpoint='/services/ImportService1.svc/ApplyUserModifications3',
                data= request_payload.update_required_udfs_payload,
            )

            process_user_disable= rail.TriggerDagRunOperator(
                task_id='process_user_disable',
                trigger_dag_id=config.process_disable_users_dagid,
                conf=lambda dag_run:{
                    "emp_id": dag_run.conf['emp_id'],
                    "emp_status": dag_run.conf['emp_status'],
                    "last_name": dag_run.conf['last_name'],
                    "first_name": dag_run.conf['first_name'],
                    'start_date': dag_run.conf['start_date'],
                    'end_date': dag_run.conf['end_date'],
                    'useruri': dag_run.conf['useruri'],
                    'starting_balance_script_uri': dag_run.conf['starting_balance_script_uri'],
                    'prevent_balance_overdraw_uri': dag_run.conf['prevent_balance_overdraw_uri'],
                    "user_log": dag_run.conf['user_log'],
                    'todays_date':dag_run.conf['todays_date'],
                    'change_effective_date':dag_run.conf['change_effective_date'],
                    'action': 'disable',
                    "emp_status_def_uri": dag_run.conf['emp_status_def_uri'],
                    "change_effective_date_def_uri": dag_run.conf['change_effective_date_def_uri'],
                    "event_def_uri": dag_run.conf['event_def_uri'],
                    "event_reason_def_uri": dag_run.conf['event_reason_def_uri'],
                    "current_udf_values":rail.result('get_user_info')['userDetails']['customFieldValues'],
                    'event': dag_run.conf['event'],
                    'event_reason_code':dag_run.conf['event_reason_code'],
                    "is_contingent": dag_run.conf['is_contingent'],
                    "country_name": dag_run.conf['location_full_path'].split('|')[0],
                },
                execution_timeout=timedelta(hours=config.execution_timeout_days),
                retries=0,
            )

            wait_for_process_user_disable = rail.WaitForDagRunsSensor(
                task_id='wait_for_process_user_disable',
                dag_runs='{{ result("process_user_disable") }}',
                execution_timeout=timedelta(days=config.execution_timeout_days),
            )

            is_contingent_user = rail.IfOperator(
                task_id = "is_contingent_user",
                test=lambda dag_run: dag_run.conf['is_contingent'] == 'Y',
                yes_task="is_end_date_present",
                no_task="is_previously_suspended_leave_or_non_live_transfer_employee"
            )

            is_end_date_present = rail.IfOperator(
                task_id="is_end_date_present",
                test=lambda dag_run: bool(dag_run.conf['end_date']),
                yes_task="process_user_disable_enddate_present",
                no_task="log_contigent_user"
            )

            log_contigent_user = rail.WriteLogOperator(
                task_id = 'log_contigent_user',
                log = '{{ dag_run.conf.user_log }}',
                message = "User not Updated because Contingent Worker",
                severity='Exception',
                properties ={
                    "employee_id": "{{ dag_run.conf.emp_id }}",
                    "last_name": "{{ dag_run.conf.last_name }}",
                    "first_name": "{{ dag_run.conf.first_name }}",
                    "action": "Update",
                    "status": "Exception",
                    'details': "User not Updated because Contingent Worker",
                }
            )

            process_user_disable_enddate_present= rail.TriggerDagRunOperator(
                task_id='process_user_disable_enddate_present',
                trigger_dag_id=config.process_disable_users_dagid,
                conf=lambda dag_run:{
                    "emp_id": dag_run.conf['emp_id'],
                    "emp_status": dag_run.conf['emp_status'],
                    "last_name": dag_run.conf['last_name'],
                    "first_name": dag_run.conf['first_name'],
                    'start_date': dag_run.conf['start_date'],
                    'end_date': dag_run.conf['end_date'],
                    'useruri': dag_run.conf['useruri'],
                    'starting_balance_script_uri': dag_run.conf['starting_balance_script_uri'],
                    'prevent_balance_overdraw_uri': dag_run.conf['prevent_balance_overdraw_uri'],
                    "user_log": dag_run.conf['user_log'],
                    'todays_date':dag_run.conf['todays_date'],
                    'change_effective_date':dag_run.conf['change_effective_date'],
                    'action': 'disable',
                    "emp_status_def_uri": dag_run.conf['emp_status_def_uri'],
                    "change_effective_date_def_uri": dag_run.conf['change_effective_date_def_uri'],
                    "event_def_uri": dag_run.conf['event_def_uri'],
                    "event_reason_def_uri": dag_run.conf['event_reason_def_uri'],
                    "current_udf_values":rail.result('get_user_info')['userDetails']['customFieldValues'],
                    'event': dag_run.conf['event'],
                    'event_reason_code':dag_run.conf['event_reason_code'],
                    "is_contingent": dag_run.conf['is_contingent'],
                    "country_name": dag_run.conf['location_full_path'].split('|')[0]

                },
                execution_timeout=timedelta(hours=config.execution_timeout_days),
                retries=0,
            )

            wait_for_process_user_disable_enddate_present = rail.WaitForDagRunsSensor(
                task_id='wait_for_process_user_disable_enddate_present',
                dag_runs='{{ result("process_user_disable_enddate_present") }}',
                execution_timeout=timedelta(days=config.execution_timeout_days),
            )

            is_previously_suspended_leave_or_non_live_transfer_employee = rail.IfOperator(
                task_id="is_previously_suspended_leave_or_non_live_transfer_employee",
                test=request_payload.validate_previous_suspended_leave_or_non_live_transfer_employee,
                yes_task="enable_login",
                no_task="is_rehire_user"
            )

            is_rehire_user = rail.IfOperator(
                task_id="is_rehire_user",
                test=request_payload.validate_rehire,
                yes_task="is_rehire_date_same_as_start_date",
                no_task="get_current_udf_values"
            )

            is_rehire_date_same_as_start_date = rail.IfOperator(
                task_id="is_rehire_date_same_as_start_date",
                test=request_payload.validate_rehire_exception,
                yes_task="log_rehire_date_exception",
                no_task="enable_login"
            )

            log_rehire_date_exception = rail.WriteLogOperator(
                task_id = 'log_rehire_date_exception',
                log = '{{ dag_run.conf.user_log }}',
                message = "User not rehired,Rehire date same as start date",
                severity='Exception',
                properties ={
                    "employee_id": "{{ dag_run.conf.emp_id }}",
                    "last_name": "{{ dag_run.conf.last_name }}",
                    "first_name": "{{ dag_run.conf.first_name }}",
                    "action": "Rehire",
                    "status": "Exception",
                    'details': "User not rehired,Rehire date same as start date",
                }
            )

            enable_login = rail.RepliconServiceOperator(
                task_id='enable_login',
                endpoint='/services/securityservice1.svc/EnableLogin',
                data={
                    "userUri": '{{ dag_run.conf.useruri }}'
                }
            )

            get_current_udf_values = rail.PythonOperator(
                task_id='get_current_udf_values',
                python_callable=lambda: rail.result('get_user_info')[
                    'userDetails']['customFieldValues']
            )

            get_current_oef_values = rail.PythonOperator(
                task_id='get_current_oef_values',
                python_callable=lambda: rail.result('get_user_info')[
                    'userDetails']['extensionFieldValues']
            )

            get_effective_user_groupmembership = rail.RepliconServiceOperator(
                task_id='get_effective_user_groupmembership',
                endpoint='/services/UserGroupService1.svc/GetEffectiveUserGroupMembership',
                data={
                    "userUri": "{{dag_run.conf.useruri}}",
                    "dateRange": null
                },
                data_handler=response_filter.get_effective_user_groupmembership_filter
            )

            get_assigned_policy_to_user = rail.RepliconServiceOperator(
                task_id='get_assigned_policy_to_user',
                endpoint='/services/PolicySetService1.svc/GetAssignedPolicySetsForUser',
                data=lambda dag_run: {
                    "userUri": dag_run.conf['useruri']
                },
                data_handler=response_filter.map_assigned_policy_to_user
            )

            get_assigned_place_to_user = rail.RepliconServiceOperator(
                task_id='get_assigned_place_to_user',
                endpoint='/services/PlaceService1.svc/GetPlaceAssignmentScheduleForUser',
                data={
                    "userTarget": {
                        "uri": "{{dag_run.conf.useruri}}",
                        "loginName": null,
                        "employeeId": null,
                        "parameterCorrelationId": null
                    }
                },
                data_handler=response_filter.map_assigned_place_to_user
            )

            get_assigned_overtime_approval_path = rail.RepliconServiceOperator(
                task_id='get_assigned_overtime_approval_path',
                endpoint='/services/WorkAuthorizationApprovalService1.svc/GetApprovalPathForUser',
                data=lambda dag_run: {
                    "userUri": dag_run.conf['useruri']
                }
            )

            apply_user_modifications = rail.RepliconServiceOperator(
                task_id='apply_user_modifications',
                endpoint='/services/ImportService1.svc/ApplyUserModifications3',
                data=lambda dag_run: request_payload.apply_user_modifications_payload(dag_run),
            )

            is_remove_hrbp_permission_set = rail.IfOperator(
                task_id='is_remove_hrbp_permission_set',
                test= request_payload.validate_is_remove_hrbp_permission_set,
                yes_task='remove_hrbp_permission_set',
                no_task='impersonate_and_create_interactive_session'
            )

            remove_hrbp_permission_set = rail.RepliconServiceCallForEachItemOperator(
                task_id='remove_hrbp_permission_set',
                endpoint='/services/PermissionSetService1.svc/RemovePermissionSetAssignmentFromUser',
                items=lambda dag_run: [dag_run.conf['ts_hrpb_permission_uri'],dag_run.conf['admin_hrpb_permission_uri']],
                data=lambda dag_run,item:{
                    "userUri": dag_run.conf['useruri'],
                    "permissionSetUri": item
                    }
            )

            def map_impersonate_and_create_interactive_session(response):
                auth_token = list(
                    filter(lambda x: x['name'] == 'AUTHTOKEN', response['sessionCookies']))[0]['value']
                tenant = list(
                    filter(lambda x: x['name'] == 'TENANT', response['sessionCookies']))[0]['value']
                return {'cookie': f'AUTHTOKEN={auth_token};TENANT={tenant}', 'Path': '/'}

            impersonate_and_create_interactive_session = rail.RepliconServiceOperator(
                task_id='impersonate_and_create_interactive_session',
                endpoint="/services/UserImpersonationService1.svc/AdministrativeImpersonationAndCreateInteractiveSession",
                data=lambda dag_run:{
                    "impersonatedUserUri": dag_run.conf['useruri']
                },
                data_handler=map_impersonate_and_create_interactive_session
            )

            update_my_default_time_off_type_for_bookings = rail.RepliconServiceOperator(
                task_id='update_my_default_time_off_type_for_bookings',
                endpoint="/services/LegacyUIService1.svc/UpdateMyDefaultTimeOffTypeForBookings",
                data={
                    "timeOffTypeUri": "{{ dag_run.conf.default_time_off_type_uri }}"
                },
                headers=lambda: rail.result(
                    'impersonate_and_create_interactive_session')
            )

            is_supervisor_in_feed_file = rail.IfOperator(
                task_id='is_supervisor_in_feed_file',
                test=lambda dag_run: bool(dag_run.conf['sup_emp_id']),
                yes_task='search_supervisor_in_replicon',
                no_task='get_time_off_types_to_assign'
            )

            process_supervisor_entry,  process_supervisor_exit= process_supervisor_assignment_task_group(
                'useruri', "{{ 'rehire_user' if get_task_state('enable_login') == 'success' else 'update_user' }}")

            get_time_off_types_to_assign = rail.PythonOperator(
                task_id = "get_time_off_types_to_assign",
                python_callable= lambda dag_run: python_callable_methods.get_time_off_to_be_assigned(dag_run, config)
            )

            def time_off_type_assignment_update_rehire_user_trigger_id(dag_run):
                return f"{config.process_timeoff_type_assignment_update_rehire_user_dagid}_batch_{dag_run.conf['modulo']+1}"

            process_time_off_type_assignment_update_rehire_user = rail.TriggerDagRunForEachItemOperator(
                task_id='process_time_off_type_assignment_update_rehire_user',
                items = [0],
                trigger_dag_id=time_off_type_assignment_update_rehire_user_trigger_id,
                conf=lambda dag_run:{
                    "employee_id": dag_run.conf['emp_id'],
                    "last_name": dag_run.conf['last_name'],
                    "first_name": dag_run.conf['first_name'],
                    'useruri': dag_run.conf['useruri'],
                    "company_code": dag_run.conf['company_code'],
                    "time_off_types_to_assign": rail.result('get_time_off_types_to_assign'),
                    "user_log": dag_run.conf['user_log'],
                    'starting_balance_script_uri': dag_run.conf['starting_balance_script_uri'],
                    'prevent_balance_overdraw_uri': dag_run.conf['prevent_balance_overdraw_uri'],
                    "rehire": 'Yes' if request_payload.validate_rehire(dag_run) else 'No',
                    "location_level_2":dag_run.conf['location_level_2'],
                    "location_level_3":dag_run.conf['location_level_3'],
                    "location_level_2_to_consider_for_timeoff": dag_run.conf['location_level_2_to_consider_for_timeoff'],
                    "location_level_3_to_consider_for_timeoff": dag_run.conf['location_level_3_to_consider_for_timeoff'],
                    "consider_home_location_for_time_off": dag_run.conf['consider_home_location_for_time_off'],
                    "home_location_level_2":dag_run.conf['home_location_level_2'],
                    "home_location_level_3": dag_run.conf['home_location_level_3'],
                    "previous_home_location_full_path": rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
                            'customField.displayText', 'Home Location', 'text') if rail.result('get_user_info')[
                            'userDetails']['customFieldValues'] else null,
                    "buisness_unit_level_2":dag_run.conf['buisness_unit_level_2'],
                    "holiday_calendar": dag_run.conf['holiday_calendar'],
                    "us_flsa_status": dag_run.conf['us_flsa_status'],
                    "full_part": dag_run.conf['full_part'],
                    "previous_full_part": rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
                            'customField.displayText', 'Full/Part', 'text') if rail.result('get_user_info')[
                            'userDetails']['customFieldValues'] else null,
                    "start_date": dag_run.conf['start_date'],
                    "adjusted_hire_date":dag_run.conf['adjusted_hire_date'],
                    "previous_adjusted_hire_date": rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
                            'customField.displayText', 'Adjusted Hire Date', 'date') if rail.result('get_user_info')[
                            'userDetails']['customFieldValues'] else null,
                    "job_code":dag_run.conf['job_code'],
                    'todays_date':dag_run.conf['todays_date'],
                    "current_assigned_job_code": rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
                        'customField.displayText', 'Job Code', 'text'),
                    "employee_type_grp": rail.result('get_effective_user_groupmembership', 'employeetype').get('displayText', ''),
                    "assigned_location_grp": rail.result('get_effective_user_groupmembership', 'location').get('displayText', ''),
                    "assigned_location_level_2": rail.result('get_effective_user_groupmembership', 'location_level_2').get('displayText', ''),
                    "emp_status": dag_run.conf['emp_status'],
                    "previous_employee_status": rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
                            'customField.displayText', 'Employee Status', 'text') if rail.result('get_user_info')[
                            'userDetails']['customFieldValues'] else null,
                    'assigned_event': rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
                            'customField.displayText', 'Event', 'text') if rail.result('get_user_info')[
                            'userDetails']['customFieldValues'] else null,
                    'assigned_event_reason_code':rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
                            'customField.displayText', 'Event Reason', 'text') if rail.result('get_user_info')[
                            'userDetails']['customFieldValues'] else null,
                    'assigned_change_effective_date': rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
                            'customField.displayText', 'Change Effective Date', 'date') if rail.result('get_user_info')[
                            'userDetails']['customFieldValues'] else null,
                    'change_effective_date':dag_run.conf['change_effective_date'],
                    'std_hrs': dag_run.conf['std_hrs'],
                    "office_schedule_uri": dag_run.conf['office_schedule_uri'],
                    "previous_std_hrs": rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
                            'customField.displayText', 'Standard Hours', 'text') if rail.result('get_user_info')[
                            'userDetails']['customFieldValues'] else null,
                    'pay_type':dag_run.conf['pay_type'],
                    "previous_pay_type": rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
                            'customField.displayText', 'Pay Type', 'text') if rail.result('get_user_info')[
                            'userDetails']['customFieldValues'] else null,
                    "previous_annual_to_placeholder": rail.find_first_by_attr_and_get_attr(rail.result('get_current_oef_values'),
                            'definition.displayText', "[UK] Annual leave - Placeholder Policy Name", 'textValue') if rail.result('get_user_info')[
                            'userDetails']['extensionFieldValues'] else null,
                    "previous_sick_to_placeholder":rail.find_first_by_attr_and_get_attr(rail.result('get_current_oef_values'),
                            'definition.displayText', "[UK] Sick leave - Placeholder Policy Name", 'textValue') if rail.result('get_user_info')[
                            'userDetails']['extensionFieldValues'] else null,
                    "annual_to_placeholder_def_uri": dag_run.conf['annual_to_placeholder_def_uri'],
                    "sick_to_placeholder_def_uri": dag_run.conf['sick_to_placeholder_def_uri'],
                    'event': dag_run.conf['event'],
                    'event_reason_code': dag_run.conf['event_reason_code']
                },
                execution_timeout=timedelta(hours=config.execution_timeout_days),
                retries=0,
            )

            wait_for_process_time_off_type_assignment_update_rehire_user = rail.WaitForDagRunsSensor(
                task_id='wait_for_process_time_off_type_assignment_update_rehire_user',
                dag_runs='{{ result("process_time_off_type_assignment_update_rehire_user") }}',
                execution_timeout=timedelta(days=config.execution_timeout_days),
            )

            gather_time_off_type_error_logs_update_rehire_user = rail.GatherResultsFromDagRunsOperator(
                task_id='gather_time_off_type_error_logs_update_rehire_user',
                dag_runs='{{ result("process_time_off_type_assignment_update_rehire_user") }}',
                dagrun_task_id='catch_and_log_errors',
                flatten=True,
            )

            gather_time_off_type_error_logs_update_disable_user = rail.GatherResultsFromDagRunsOperator(
                task_id='gather_time_off_type_error_logs_update_disable_user',
                dag_runs='{{ result("process_time_off_type_assignment_update_rehire_user") }}',
                dagrun_task_id='gather_time_off_type_error_logs_disable_user',
                flatten=True,
            )


            gather_time_off_type_exception_logs_update_rehire_user = rail.GatherResultsFromDagRunsOperator(
                task_id='gather_time_off_type_exception_logs_update_rehire_user',
                dag_runs='{{ result("process_time_off_type_assignment_update_rehire_user") }}',
                dagrun_task_id='log_time_off_type_not_available',
                flatten=True,
            )

            has_any_error_or_exceptions_present_update_rehire_user = rail.IfOperator(
                task_id="has_any_error_or_exceptions_present_update_rehire_user",
                test="{{ result('gather_time_off_type_exception_logs_update_rehire_user') | is_truthy or\
                    result('gather_time_off_type_error_logs_update_rehire_user') | is_truthy or\
                    result('gather_time_off_type_error_logs_update_disable_user') | is_truthy }}",
                yes_task= 'log_error_or_exceptions_present_update_rehire_user',
                no_task='log_user_completion'
            )

            log_error_or_exceptions_present_update_rehire_user = rail.EmptyOperator(
                task_id='log_error_or_exceptions_present_update_rehire_user'
            )

            log_user_completion = rail.WriteLogOperator(
                task_id='log_user_completion',
                log = '{{ dag_run.conf.user_log }}',
                message=lambda dag_run: request_payload.get_update_user_message(dag_run),
                severity=lambda dag_run: request_payload.get_update_user_severity(dag_run),
                properties=lambda dag_run: {
                    "employee_id": dag_run.conf['emp_id'],
                    "last_name": dag_run.conf['last_name'],
                    "first_name": dag_run.conf['first_name'],
                    "action": "Update" if not request_payload.validate_rehire(dag_run) else 'Rehire' ,
                    "status": request_payload.get_update_user_severity(dag_run),
                    'details': request_payload.get_update_user_message(dag_run)
                }
            )

            catch_and_log_errors = rail.WriteLogOperator(
                task_id='catch_and_log_errors',
                log = '{{ dag_run.conf.user_log }}',
                trigger_rule='one_failed',
                severity='Error',
                message='{{ get_error_message() }}',
                properties={
                    "employee_id": "{{dag_run.conf.emp_id}}",
                    "last_name": "{{dag_run.conf.last_name}}",
                    "first_name": "{{dag_run.conf.first_name}}",
                    "action": "{{ 'Rehire' if get_task_state('enable_login') == 'success' else 'Update' }}",
                    'status': 'Error',
                    'details': "{{ get_error_message() }}"
                }
            )

            log_to_sumo = rail.DagRunLogToSumoOperator(
                task_id='log_to_sumo',
                sumo_conn_id='sumologic-dagrunlogger',
                trigger_rule='all_done',
            )

            can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
            can_run_batch_task >> rail.Label('No') >> has_valid_data

            has_valid_data >> rail.Label('Yes') >> get_user_info >> is_change_effective_date_present >> rail.Label("Yes") >> is_status_active
            is_change_effective_date_present >> rail.Label("No") >> log_change_effective_date_exception >> catch_and_log_errors
            has_valid_data >> rail.Label('No') >> log_invalid_data >> catch_and_log_errors

            is_status_active >> rail.Label("No") >> is_status_unpaid_leave >> rail.Label("No") >> process_user_disable

            is_status_unpaid_leave >> rail.Label("Yes") >> update_required_udfs >> catch_and_log_errors
            process_user_disable >> wait_for_process_user_disable >> catch_and_log_errors
            is_status_active >> rail.Label("Yes") >> is_contingent_user >> rail.Label('No') >> is_previously_suspended_leave_or_non_live_transfer_employee

            is_previously_suspended_leave_or_non_live_transfer_employee >> rail.Label("Yes") >> enable_login
            is_previously_suspended_leave_or_non_live_transfer_employee >> rail.Label("No") >> is_rehire_user

            is_contingent_user >> rail.Label('Yes') >>  is_end_date_present >> rail.Label("No") >> log_contigent_user >> catch_and_log_errors

            is_end_date_present >> rail.Label("Yes") >> process_user_disable_enddate_present >> wait_for_process_user_disable_enddate_present
            wait_for_process_user_disable_enddate_present >> catch_and_log_errors

            is_rehire_user >> rail.Label('No') >> get_current_udf_values
            is_rehire_user >> rail.Label('Yes') >> is_rehire_date_same_as_start_date >> rail.Label('No') >> enable_login >> get_current_udf_values
            is_rehire_date_same_as_start_date >> rail.Label('Yes') >> log_rehire_date_exception >> catch_and_log_errors


            get_current_udf_values >> get_current_oef_values >> get_effective_user_groupmembership >> get_assigned_policy_to_user >> get_assigned_place_to_user
            get_assigned_place_to_user >> get_assigned_overtime_approval_path >> apply_user_modifications

            apply_user_modifications >> is_remove_hrbp_permission_set >> rail.Label('No') >> impersonate_and_create_interactive_session

            is_remove_hrbp_permission_set >> rail.Label('Yes') >> remove_hrbp_permission_set >> impersonate_and_create_interactive_session

            impersonate_and_create_interactive_session >> update_my_default_time_off_type_for_bookings >> is_supervisor_in_feed_file

            is_supervisor_in_feed_file >> rail.Label('Yes') >> process_supervisor_entry
            is_supervisor_in_feed_file >> rail.Label('No') >> get_time_off_types_to_assign

            process_supervisor_exit >> get_time_off_types_to_assign >> process_time_off_type_assignment_update_rehire_user
            process_time_off_type_assignment_update_rehire_user >> wait_for_process_time_off_type_assignment_update_rehire_user
            wait_for_process_time_off_type_assignment_update_rehire_user >> gather_time_off_type_error_logs_update_rehire_user
            gather_time_off_type_error_logs_update_rehire_user >> gather_time_off_type_error_logs_update_disable_user
            gather_time_off_type_error_logs_update_disable_user >> gather_time_off_type_exception_logs_update_rehire_user
            gather_time_off_type_exception_logs_update_rehire_user >> has_any_error_or_exceptions_present_update_rehire_user
            has_any_error_or_exceptions_present_update_rehire_user >> rail.Label('Yes') >> log_error_or_exceptions_present_update_rehire_user
            log_error_or_exceptions_present_update_rehire_user >> catch_and_log_errors
            has_any_error_or_exceptions_present_update_rehire_user >> rail.Label('No') >> log_user_completion

            log_user_completion >> catch_and_log_errors >> log_to_sumo


        update_dags.append(dag)

    return update_dags

rail.for_each_instance(create_child_dag)
