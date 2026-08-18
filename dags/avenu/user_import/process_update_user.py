from datetime import timedelta
import rail
from avenu.user_import.utils import request_payload
from avenu.user_import.utils import python_callable_method
from avenu.user_import.utils import response_filter
from avenu.user_import.task.process_supervisor import process_supervisor_assignment_task_group
from airflow.models import Variable

def create_child_dag_wbs(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'avenu_user_sync_process_update_user_{config.instance}_child',
        description='Avenu User Sync Process Update User',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_update_user,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        user_uri = '{{ dag_run.conf.useruri }}'

        null = None

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='has_valid_update_fields'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='has_valid_update_fields',
            end_task='catch_and_log_errors',
        )

        has_valid_update_fields = rail.IfOperator(
            task_id='has_valid_update_fields',
            test=request_payload.test_valid_fields,
            yes_task="update_user_exception_log",
            no_task="log_invalid_update_fields"
        )

        log_invalid_update_fields = rail.WriteLogOperator(
            task_id='log_invalid_update_fields',
            message=request_payload.get_invalid_fields_message,
            severity='Exception',
            properties=lambda dag_run: {
                "employeeid": dag_run.conf['employeeid'],
                "firstname": dag_run.conf['firstname'],
                "lastname": dag_run.conf['lastname'],
                'status': 'Exception',
            }
        )

        update_user_exception_log = rail.CreateLogOperator(
            task_id='update_user_exception_log'
        )

        update_user_error_logs = rail.CreateLogOperator(
            task_id='update_user_error_logs'
        )

        update_user_success_logs = rail.CreateLogOperator(
            task_id='update_user_success_logs'
        )

        get_user_info = rail.RepliconServiceOperator(
            task_id='get_user_info',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data={
                "users": [
                    {
                        "uri": user_uri,
                        "loginName": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:fail-if-insufficient-data-access-permission"
            },
            response_filter=lambda res: res.json()['d'][0]
        )

        is_disable_logic = rail.IfOperator(
            task_id='is_disable_logic',
            test=request_payload.user_position_status_check,
            yes_task="is_user_status_terminated",
            no_task="is_rehire_user"
        )

        is_user_status_terminated = rail.IfOperator(
            task_id='is_user_status_terminated',
            test=request_payload.test_status_delete,
            yes_task="is_user_disabled",
            no_task="is_leave_user_disabled"
        )

        is_user_disabled = rail.IfOperator(
            task_id='is_user_disabled',
            test=request_payload.is_user_disabled,
            yes_task="is_end_date_present",
            no_task="is_status_leave"
        )

        is_end_date_present = rail.IfOperator(
            task_id='is_end_date_present',
            test=request_payload.is_end_date_present,
            yes_task="catch_and_log_errors",
            no_task="is_status_leave"
        )

        is_leave_user_disabled = rail.IfOperator(
            task_id='is_leave_user_disabled',
            test=request_payload.is_user_disabled,
            yes_task="is_leave_user_end_date_present",
            no_task="is_status_leave"
        )

        is_leave_user_end_date_present = rail.IfOperator(
            task_id='is_leave_user_end_date_present',
            test=request_payload.is_end_date_present,
            yes_task="log_user_terminated",
            no_task="log_user_long_leave"
        )

        log_user_terminated = rail.WriteLogOperator(
            task_id='log_user_terminated',
            message="User is already Terminated",
            severity='Exception',
            properties=lambda dag_run: {
                "employeeid": dag_run.conf['employeeid'],
                "firstname": dag_run.conf['firstname'],
                "lastname": dag_run.conf['lastname'],
                'status': 'Exception',
            }
        )

        log_user_long_leave = rail.WriteLogOperator(
            task_id='log_user_long_leave',
            log="{{result('update_user_exception_log')}}",
            message="User is already in Long Leave",
            severity='Exception',
            properties=lambda dag_run: {
                "employeeid": dag_run.conf['employeeid'],
                "firstname": dag_run.conf['firstname'],
                "lastname": dag_run.conf['lastname'],
                'status': 'Exception',
            }
        )

        is_status_leave = rail.IfOperator(
            task_id='is_status_leave',
            test=request_payload.test_status_delete,
            yes_task="update_employee_date_range_delete",
            no_task="update_employee_date_range"
        )

        update_employee_date_range = rail.RepliconServiceOperator(
            task_id='update_employee_date_range',
            endpoint='/services/UserService1.svc/UpdateEmploymentDateRange',
            data=request_payload.update_employee_date_range,
        )

        disable_login = rail.RepliconServiceOperator(
            task_id='disable_login',
            endpoint='/services/SecurityService1.svc/DisableLogin',
            data={
                "userUri": user_uri
            }
        )

        get_user_time_off = rail.RepliconServiceOperator(
            task_id='get_user_time_off',
            endpoint='/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary',
            data={
                "userUri": user_uri
            },
            response_filter=response_filter.get_user_time_off
        )

        update_time_off_for_no_aacural = rail.TriggerDagRunForEachItemOperator(
            task_id='update_time_off_for_no_aacural',
            items=lambda dag_run:python_callable_method.get_timeoff_types_to_process_no_accrual(dag_run, get_user_time_off.task_id, config),
            trigger_dag_id=f'avenu_user_sync_update_time_off_for_no_aacural_{config.instance}_child',
            conf=request_payload.get_update_time_off_for_no_aacural,
            execution_timeout=timedelta(hours=config.execution_timeout_hours),
            retries=0,
        )

        wait_for_update_time_off_for_no_aacural = rail.WaitForDagRunsSensor(
            task_id='wait_for_update_time_off_for_no_aacural',
            dag_runs='{{ result("update_time_off_for_no_aacural") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        if_user_delete_timeoff = rail.IfOperator(
            task_id='if_user_delete_timeoff',
            test=request_payload.test_status_delete,
            yes_task="delete_time_off_scenario",
            no_task="log_user_disabled_success"
        )

        update_employee_date_range_delete = rail.RepliconServiceOperator(
            task_id='update_employee_date_range_delete',
            endpoint='/services/UserService1.svc/UpdateEmploymentDateRange',
            data=request_payload.update_employee_date_range_delete,
        )

        delete_time_off_scenario = rail.TriggerDagRunForEachItemOperator(
            task_id='delete_time_off_scenario',
            items=lambda: rail.result('get_user_time_off'),
            trigger_dag_id=f'avenu_user_sync_delete_time_off_for_user_{config.instance}_child',
            conf=request_payload.get_delete_time_off_scenario,
            execution_timeout=timedelta(hours=config.execution_timeout_hours),
            retries=0,
        )

        wait_for_delete_time_off_scenario = rail.WaitForDagRunsSensor(
            task_id='wait_for_delete_time_off_scenario',
            dag_runs='{{ result("delete_time_off_scenario") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        log_user_disabled_success = rail.WriteLogOperator(
            task_id = "log_user_disabled_success",
            severity='Success',
            message="User disabled successfully",
            properties={
                'employeeid': '{{dag_run.conf.employeeid}}',
                'firstname': '{{dag_run.conf.firstname}}',
                'lastname': '{{dag_run.conf.lastname}}',
                'status': 'Success',
            }
        )

        is_rehire_user = rail.IfOperator(
            task_id="is_rehire_user",
            test=lambda: not rail.result(
                'get_user_info')['userDetails']['isEnabled'],
            yes_task="enable_login",
            no_task="get_current_custom_field_values"
        )

        enable_login = rail.RepliconServiceOperator(
            task_id='enable_login',
            endpoint='/services/securityservice1.svc/EnableLogin',
            data={
                "userUri": user_uri
            }
        )

        log_rehire_user = rail.WriteLogOperator(
            task_id='log_rehire_user',
            message="Rehired User",
            severity='Success',
            properties=lambda dag_run: {
                "employeeid": dag_run.conf['employeeid'],
                "firstname": dag_run.conf['firstname'],
                "lastname": dag_run.conf['lastname'],
                'status': 'Success',
            }
        )

        get_current_custom_field_values = rail.PythonOperator(
            task_id='get_current_custom_field_values',
            python_callable=lambda: rail.result('get_user_info')[
                'userDetails']['customFieldValues']
        )

        get_current_oef_values = rail.PythonOperator(
            task_id='get_current_oef_values',
            python_callable=lambda: rail.result('get_user_info')[
                'userDetails']['extensionFieldValues']
        )

        get_effective_group_membership_for_user = rail.RepliconServiceOperator(
            task_id = "get_effective_group_membership_for_user",
            endpoint= "/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data= {
                "userUri": user_uri
            }
        )

        apply_user_modifications = rail.RepliconServiceOperator(
            task_id='apply_user_modifications',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data=request_payload.apply_user_modifications,
        )

        is_update_failed = rail.IfOperator(
            task_id = "is_update_failed",
            test="{{ result('apply_user_modifications').errors | is_truthy }}",
            yes_task="log_update_user_failed",
            no_task="update_sso_for_user"
        )

        log_update_user_failed = rail.WriteLogOperator(
            task_id = "log_update_user_failed",
            severity='Error',
            message="{{ result('apply_user_modifications').errors }}",
            properties={
                'employeeid': '{{dag_run.conf.employeeid}}',
                'firstname': '{{dag_run.conf.firstname}}',
                'lastname': '{{dag_run.conf.lastname}}',
                'status': 'Error',
            },
        )

        update_sso_for_user = rail.RepliconServiceOperator(
            task_id='update_sso_for_user',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data=request_payload.apply_sso_modifications,
        )

        is_hourly_rate_changed = rail.IfOperator(
            task_id='is_hourly_rate_changed',
            test=request_payload.test_hourly_rate,
            yes_task='update_hourly_rate',
            no_task='get_assigned_policy_to_user'
        )

        update_hourly_rate = rail.RepliconServiceOperator(
            task_id='update_hourly_rate',
            endpoint='/services/PayrollService1.svc/UpdateUserPayrollRateScheduleOverDateRange',
            data=request_payload.update_hourly_rate,
        )

        process_supervisor_task_entry, process_supervisor_task_exit = process_supervisor_assignment_task_group(
            'useruri', 'update_user')

        is_supervisor_in_feed_file = rail.IfOperator(
            task_id='is_supervisor_in_feed_file',
            test=lambda dag_run: dag_run.conf['reportstoid'] and dag_run.conf['reportstoname'],
            yes_task=process_supervisor_task_entry.task_id,
            no_task='log_supervisor_not_in_feedfile'
        )

        is_employee_type_changed = rail.IfOperator(
            task_id='is_employee_type_changed',
            test=lambda dag_run: (request_payload.is_employee_type_changed(dag_run) and python_callable_method.is_both_exempt(dag_run)),
            yes_task='update_punch_entry_policy',
            no_task='is_employee_location_change'
        )

        is_employee_location_change = rail.IfOperator(
            task_id = "is_employee_location_change",
            test = lambda dag_run: (python_callable_method.check_if_user_location_is_updated(dag_run, config)
                                    and python_callable_method.is_both_exempt(dag_run)),
            yes_task= "update_punch_entry_policy_location",
            no_task="is_supervisor_in_feed_file"
        )

        update_punch_entry_policy_location = rail.RepliconServiceOperator(
            task_id = "update_punch_entry_policy_location",
            endpoint='/services/PolicySetService1.svc/AssignPolicySetToUser',
            data=lambda dag_run: request_payload.update_punch_entry_policy_location(
                dag_run, config),
        )

        is_users_exempt_status_changed = rail.IfOperator(
            task_id = "is_users_exempt_status_changed",
            test=python_callable_method.test_is_users_exempt_status_changed,
            yes_task= "get_current_assigned_cost_normalization",
            no_task= "is_supervisor_in_feed_file"
        )

        get_assigned_policy_to_user = rail.RepliconServiceOperator(
            task_id='get_assigned_policy_to_user',
            endpoint='/services/PolicySetService1.svc/GetAssignedPolicySetsForUser',
            data=request_payload.get_assigned_policy_to_user,
            response_filter=response_filter.map_assigned_policy_to_user
        )

        update_punch_entry_policy = rail.RepliconServiceOperator(
            task_id='update_punch_entry_policy',
            endpoint='/services/PolicySetService1.svc/AssignPolicySetToUser',
            data=lambda dag_run: request_payload.update_punch_entry_policy(
                dag_run, config),
        )

        get_current_assigned_cost_normalization = rail.RepliconServiceOperator(
            task_id = "get_current_assigned_cost_normalization",
            endpoint= "/services/CostNormalizationRuleService1.svc/GetUserCostNormalizationRuleAssignmentSchedule",
            data={
                "userUri" : "{{dag_run.conf.useruri}}"
            }
        )

        update_cost_normalization_rule = rail.RepliconServiceOperator(
            task_id= "update_cost_normalization_rule",
            endpoint="/services/CostNormalizationRuleService1.svc/UpdateUserCostNormalizationRuleAssignmentScheduleOverDateRange",
            data= request_payload.get_cost_normalization_payload_update
        )

        is_cost_normalization_not_closed = rail.IfOperator(
            task_id = "is_cost_normalization_closed",
            test= request_payload.test_exempt_employee_type,
            no_task="get_updated_cost_normalization",
            yes_task="dummy_cost_normalization_end"
        )

        get_updated_cost_normalization = rail.RepliconServiceOperator(
            task_id = "get_updated_cost_normalization",
            endpoint= "/services/CostNormalizationRuleService1.svc/GetUserCostNormalizationRuleAssignmentSchedule",
            data={
                "userUri" : "{{dag_run.conf.useruri}}"
            },
            data_handler= response_filter.get_updated_cost_normalization_filter
        )

        delete_last_cost_normalization_for_user = rail.RepliconServiceOperator(
            task_id= "delete_last_cost_normalization_for_user",
            endpoint="/services/CostNormalizationRuleService1.svc/DeleteUserCostNormalizationRuleAssignmentScheduleEntry",
            data= {
                "costNormalizationRuleScheduleEntryUri": "{{ result('get_updated_cost_normalization','last_cost_normalization_uri') }}"
            }
        )

        dummy_cost_normalization_end = rail.EmptyOperator(
            task_id= "dummy_cost_normalization_end"
        )

        log_supervisor_not_in_feedfile = rail.WriteLogOperator(
            task_id='log_supervisor_not_in_feedfile',
            log="{{result('update_user_exception_log')}}",
            message="Supervisor details not present in feed file",
            severity='Exception',
            properties=lambda dag_run: {
                "employeeid": dag_run.conf['employeeid'],
                "firstname": dag_run.conf['firstname'],
                "lastname": dag_run.conf['lastname'],
                'status': 'Exception',
            }
        )



        process_timeoff = rail.EmptyOperator(
            task_id="process_timeoff",
        )

        can_process_timeoff_assignments = rail.IfOperator(
            task_id = "can_process_timeoff_assignments",
            test = lambda dag_run: (python_callable_method.check_if_user_location_is_changed(dag_run) or request_payload.is_employee_type_changed(dag_run)),
            yes_task = "process_time_off_assignment",
            no_task="get_all_logs"
        )

        process_time_off_assignment = rail.TriggerDagRunOperator(
            task_id='process_time_off_assignment',
            trigger_dag_id=f'avenu_user_sync_process_time_off_assignment_update_{config.instance}_child',
            conf=lambda dag_run: request_payload.get_process_time_off_assignment_conf(
                dag_run, 'update_user'),
            execution_timeout=timedelta(hours=config.execution_timeout_hours),
            retries=0,
        )

        wait_for_process_time_off_assignment = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_time_off_assignment',
            dag_runs='{{ result("process_time_off_assignment") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_all_logs = rail.EmptyOperator(
            task_id='get_all_logs'
        )

        get_all_exception_logs = rail.PythonOperator(
            task_id='get_all_exception_logs',
            python_callable=python_callable_method.get_user_logs_by_status,
            op_args=['update_user_exception_log']
        )

        get_all_error_logs = rail.PythonOperator(
            task_id='get_all_error_logs',
            python_callable=python_callable_method.get_user_logs_by_status,
            op_args=['update_user_error_logs']
        )

        get_all_success_logs = rail.PythonOperator(
            task_id='get_all_success_logs',
            python_callable=python_callable_method.get_user_logs_by_status,
            op_args=['update_user_success_logs']
        )

        log_completion = rail.WriteLogOperator(
            task_id='log_completion',
            message=request_payload.get_update_completion_message,
            severity=request_payload.get_update_severity,
            properties=lambda dag_run: {
                'employeeid': dag_run.conf['employeeid'],
                'firstname': dag_run.conf['firstname'],
                'lastname': dag_run.conf['lastname'],
                'status': 'Error' if rail.result('get_all_error_logs') else ('Exception' if rail.result('get_all_exception_logs') else 'Success'),
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'employeeid': '{{dag_run.conf.employeeid}}',
                'firstname': '{{dag_run.conf.firstname}}',
                'lastname': '{{dag_run.conf.lastname}}',
                'status': 'Error',
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> has_valid_update_fields
        has_valid_update_fields >> rail.Label(
            'No') >> log_invalid_update_fields >> catch_and_log_errors
        has_valid_update_fields >> rail.Label(
            'Yes') >> update_user_exception_log
        update_user_exception_log >> update_user_error_logs >> update_user_success_logs >> get_user_info >> is_disable_logic >> rail.Label(
            "Yes") >> is_user_status_terminated >> rail.Label("Yes") >> is_user_disabled >> rail.Label("Yes") >> is_end_date_present
        is_end_date_present >> rail.Label("Yes") >> catch_and_log_errors
        is_leave_user_disabled >> rail.Label(
            "Yes") >> is_leave_user_end_date_present
        is_leave_user_end_date_present >> rail.Label(
            "Yes") >> log_user_terminated >> catch_and_log_errors
        is_leave_user_end_date_present >> rail.Label(
            "No") >> log_user_long_leave >> catch_and_log_errors
        is_end_date_present >> rail.Label("No") >> is_status_leave
        is_user_status_terminated >> rail.Label(
            "No") >> is_leave_user_disabled >> rail.Label("No") >> is_status_leave
        is_user_disabled >> rail.Label("No") >> is_status_leave
        is_status_leave >> rail.Label(
            "Yes") >> update_employee_date_range_delete >> disable_login >> get_user_time_off >> update_time_off_for_no_aacural
        is_status_leave >> rail.Label(
            "No") >> update_employee_date_range >> disable_login
        update_time_off_for_no_aacural >> wait_for_update_time_off_for_no_aacural >> if_user_delete_timeoff
        if_user_delete_timeoff >> rail.Label(
            "Yes") >> delete_time_off_scenario >> wait_for_delete_time_off_scenario >> log_user_disabled_success >> catch_and_log_errors
        if_user_delete_timeoff >> rail.Label("No") >> log_user_disabled_success >> catch_and_log_errors
        is_disable_logic >> rail.Label(
            "No") >> is_rehire_user
        is_rehire_user >> rail.Label(
            'Yes') >> enable_login >> log_rehire_user >> get_current_custom_field_values
        is_rehire_user >> rail.Label('No') >> get_current_custom_field_values
        get_current_custom_field_values >> get_current_oef_values >> get_effective_group_membership_for_user >> apply_user_modifications
        apply_user_modifications >> is_update_failed >> rail.Label("No") >> update_sso_for_user >> is_hourly_rate_changed >> rail.Label(
            'Yes') >> update_hourly_rate >> get_assigned_policy_to_user >> is_employee_type_changed >> rail.Label("No")\
                >> is_employee_location_change >> rail.Label("No") >> is_supervisor_in_feed_file
        is_update_failed >> rail.Label("No") >> log_update_user_failed >> catch_and_log_errors
        is_employee_location_change >> rail.Label("Yes") >> update_punch_entry_policy_location >> is_supervisor_in_feed_file
        is_employee_type_changed >> rail.Label(
            "Yes") >> update_punch_entry_policy >> is_users_exempt_status_changed >> rail.Label("Yes")>>\
                get_current_assigned_cost_normalization >> update_cost_normalization_rule >> is_cost_normalization_not_closed
        is_cost_normalization_not_closed >> rail.Label("Yes") >> dummy_cost_normalization_end >> is_supervisor_in_feed_file
        is_cost_normalization_not_closed >> rail.Label("No") >> get_updated_cost_normalization\
            >> delete_last_cost_normalization_for_user >> dummy_cost_normalization_end
        is_users_exempt_status_changed >> rail.Label("No") >> is_supervisor_in_feed_file
        is_hourly_rate_changed >> rail.Label(
            'No') >> get_assigned_policy_to_user
        is_supervisor_in_feed_file >> rail.Label(
            'Yes') >> process_supervisor_task_entry
        is_supervisor_in_feed_file >> rail.Label(
            'No') >> log_supervisor_not_in_feedfile >> process_timeoff
        process_supervisor_task_exit >> process_timeoff >> can_process_timeoff_assignments
        can_process_timeoff_assignments >> rail.Label("Yes")>> process_time_off_assignment >> wait_for_process_time_off_assignment
        can_process_timeoff_assignments >> rail.Label("No") >> get_all_logs
        wait_for_process_time_off_assignment >> get_all_logs >> get_all_exception_logs >>\
            get_all_error_logs >> get_all_success_logs >> log_completion >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag_wbs)
