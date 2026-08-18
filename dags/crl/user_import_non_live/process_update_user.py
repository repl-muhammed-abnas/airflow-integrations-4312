from datetime import timedelta
from airflow.models import Variable
import rail

from crl.user_import_non_live.utils import request_payload, response_filter

null= None

# pylint: disable=too-many-statements
def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.process_update_users_dagid,
        description='CRL User Import Process Update Users',
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
            no_task='get_user_info'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_user_info',
            end_task='catch_and_log_errors',
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
            yes_task="get_current_udf_values",
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
                "action": "Update",
                "status": "Exception",
                'details': "Change Effective date blank in payload",
            }
        )

        get_current_udf_values = rail.PythonOperator(
            task_id='get_current_udf_values',
            python_callable=lambda: rail.result('get_user_info')[
                'userDetails']['customFieldValues']
        )

        is_status_active = rail.IfOperator(
            task_id="is_status_active",
            test=lambda dag_run: dag_run.conf['replicon_employee_status'] in config.ACTIVE_STATUS,
            yes_task="get_effective_user_groupmembership",
            no_task="is_enddate_greater_than_start_date"
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

        get_direct_reports_for_user = rail.RepliconServiceOperator(
            task_id='get_direct_reports_for_user',
            endpoint='/services/UserService1.svc/GetDirectReportsForUser',
            data=lambda dag_run:{
                "userUri": dag_run.conf['useruri'],
                "asOfDate": request_payload.get_replicon_date(dag_run.conf['todays_date']),
                "userStatusOptionUri": "urn:replicon:user-status-option:include-all-users"
                }
        )

        apply_user_modifications = rail.RepliconServiceOperator(
            task_id='apply_user_modifications',
            endpoint='/services/ImportService1.svc/ApplyUserModifications3',
            data= request_payload.apply_user_modifications_payload,
        )

        is_remove_hrbp_permossion_set = rail.IfOperator(
            task_id='is_remove_hrbp_permossion_set',
            test= request_payload.validate_is_remove_hrbp_permossion_set,
            yes_task='remove_hrbp_permission_set',
            no_task='get_user_time_off_policy_summary'
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

        get_user_time_off_policy_summary= rail.RepliconServiceOperator(
            task_id="get_user_time_off_policy_summary",
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler= response_filter.assigned_time_offs_types_to_user
        )

        has_any_timeoff_type_assignment = rail.IfOperator(
            task_id="has_any_timeoff_type_assignment",
            test=lambda: bool(rail.result('get_user_time_off_policy_summary')),
            yes_task="process_time_off_type_no_accrual",
            no_task="log_user_completion"
        )

        process_time_off_type_no_accrual= rail.TriggerDagRunOperator(
            task_id='process_time_off_type_no_accrual',
            trigger_dag_id=config.process_timeoff_type_no_accrual_dagid,
            conf=lambda dag_run:{
                "employee_id": dag_run.conf['emp_id'],
                "last_name": dag_run.conf['last_name'],
                "first_name": dag_run.conf['first_name'],
                'end_date': dag_run.conf['end_date'],
                'useruri': dag_run.conf['useruri'],
                'starting_balance_script_uri': dag_run.conf['starting_balance_script_uri'],
                'prevent_balance_overdraw_uri': dag_run.conf['prevent_balance_overdraw_uri'],
                "user_log": dag_run.conf['user_log'],
                'todays_date':dag_run.conf['todays_date'],
                'action': 'disable',
                "change_effective_date": dag_run.conf['change_effective_date'],
                'event': dag_run.conf['event'],
                'event_reason_code':dag_run.conf['event_reason_code'],
            },
            execution_timeout=timedelta(hours=config.execution_timeout_days),
            retries=0,
        )

        wait_for_process_time_off_type_no_accrual = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_time_off_type_no_accrual',
            dag_runs='{{ result("process_time_off_type_no_accrual") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        gather_time_off_type_error_logs_disable_user = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_time_off_type_error_logs_disable_user',
            dag_runs='{{ result("process_time_off_type_no_accrual") }}',
            dagrun_task_id='catch_and_log_errors',
            flatten=True,
        )

        has_any_error_present = rail.IfOperator(
            task_id="has_any_error_present",
            test="{{ result('gather_time_off_type_error_logs_disable_user') | is_truthy }}",
            yes_task= 'log_error_present',
            no_task='log_user_completion'
        )

        log_error_present = rail.EmptyOperator(
            task_id='log_error_present'
        )

        is_enddate_greater_than_start_date = rail.IfOperator(
            task_id ='is_enddate_greater_than_start_date',
            test = request_payload.validate_enddate,
            yes_task="update_end_date",
            no_task="log_endate_exception"
        )

        update_end_date = rail.RepliconServiceOperator(
            task_id='update_end_date',
            endpoint='/services/ImportService1.svc/ApplyUserModifications3',
            data= request_payload.update_end_date_payload,
        )

        is_enddate_in_future = rail.IfOperator(
            task_id="is_enddate_in_future",
            test=request_payload.is_enddate_in_future,
            yes_task="log_end_date_future",
            no_task="disable_login"
        )

        log_end_date_future = rail.EmptyOperator(
            task_id = 'log_end_date_future',
        )

        disable_login = rail.RepliconServiceOperator(
            task_id='disable_login',
            endpoint='/services/securityservice1.svc/DisableLogin',
            data={
                "userUri": '{{ dag_run.conf.useruri }}'
            }
        )

        log_endate_exception = rail.WriteLogOperator(
            task_id = 'log_endate_exception',
            log = '{{ dag_run.conf.user_log }}',
            message = lambda dag_run:f"User not Disabled,{'End date' if dag_run.conf['end_date'] else 'Change Effective Date'} Prior to Start date",
            severity='Exception',
            properties =lambda dag_run: {
                "employee_id": dag_run.conf['emp_id'],
                "last_name": dag_run.conf['last_name'],
                "first_name": dag_run.conf['first_name'],
                "action": "Validation",
                "status": "Exception",
                'details': f"User not Disabled,{'End date' if dag_run.conf['end_date'] else 'Change Effective Date'} Prior to Start date",
            }
        )

        log_user_disablement = rail.WriteLogOperator(
            task_id = 'log_user_disablement',
            log = '{{ dag_run.conf.user_log }}',
            message = request_payload.get_disable_message,
            severity=request_payload.get_disable_status,
            properties = lambda dag_run: {
                "employee_id": dag_run.conf['emp_id'],
                "last_name": dag_run.conf['last_name'],
                "first_name": dag_run.conf['first_name'],
                "action": "Disable",
                "status": request_payload.get_disable_status(),
                'details': request_payload.get_disable_message(),
            }
        )

        log_user_completion = rail.WriteLogOperator(
            task_id='log_user_completion',
            log = '{{ dag_run.conf.user_log }}',
            message=request_payload.get_update_user_message,
            severity=request_payload.get_update_user_severity,
            properties=lambda dag_run: {
                "employee_id": dag_run.conf['emp_id'],
                "last_name": dag_run.conf['last_name'],
                "first_name": dag_run.conf['first_name'],
                "action": "Update",
                "status": request_payload.get_update_user_severity(),
                'details': request_payload.get_update_user_message()
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
                "action": "Update",
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
        can_run_batch_task >> rail.Label('No') >>  get_user_info

        get_user_info >> is_change_effective_date_present >> rail.Label("Yes") >> get_current_udf_values >> is_status_active
        is_change_effective_date_present >> rail.Label("No") >> log_change_effective_date_exception >> catch_and_log_errors

        is_status_active >> rail.Label("Yes") >> get_effective_user_groupmembership

        get_effective_user_groupmembership >> get_direct_reports_for_user >> apply_user_modifications
        apply_user_modifications >> is_remove_hrbp_permossion_set >> rail.Label('No') >> get_user_time_off_policy_summary
        is_remove_hrbp_permossion_set >> rail.Label('Yes') >> remove_hrbp_permission_set >> get_user_time_off_policy_summary


        is_status_active >> rail.Label("No") >> is_enddate_greater_than_start_date

        is_enddate_greater_than_start_date >> rail.Label("Yes") >> update_end_date
        is_enddate_greater_than_start_date >> rail.Label("No") >> log_endate_exception >> catch_and_log_errors

        update_end_date >> is_enddate_in_future >> rail.Label("Yes") >> log_end_date_future >> log_user_disablement >> catch_and_log_errors
        is_enddate_in_future >> rail.Label("No") >> disable_login >> log_user_disablement

        get_user_time_off_policy_summary >> has_any_timeoff_type_assignment

        has_any_timeoff_type_assignment >> rail.Label("Yes") >> process_time_off_type_no_accrual >> wait_for_process_time_off_type_no_accrual
        has_any_timeoff_type_assignment >> rail.Label("No") >> log_user_completion

        log_user_disablement >> catch_and_log_errors

        wait_for_process_time_off_type_no_accrual >> gather_time_off_type_error_logs_disable_user >> has_any_error_present
        has_any_error_present >> rail.Label("Yes") >> log_error_present >> catch_and_log_errors
        has_any_error_present >> rail.Label("No") >> log_user_completion

        log_user_completion >> catch_and_log_errors >> log_to_sumo

    return dag

rail.for_each_instance(create_child_dag)
