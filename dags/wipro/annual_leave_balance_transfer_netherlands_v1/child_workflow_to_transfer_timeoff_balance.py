from datetime import timedelta
from airflow.models import Variable
import rail
from wipro.annual_leave_balance_transfer_netherlands_v1.utils import request_payload

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_workflow_to_transfer_timeoff_balance_dag_id,
        description=f'WIPRO | Annual leave Balance Transfer | Transfer Timeoff Balance Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

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
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": null,
                        "loginName": "{{dag_run.conf.login_name}}",
                        "employeeId": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:fail-if-insufficient-data-access-permission"
            },
            data_handler=lambda response: {
                "useruri": response[0]['userDetails']['uri'],
                "timeoffpolicies": response[0]['timeOffTypePolicySummary']['policiesByTimeOffType']
            }
        )

        for_each_timeofftype = rail.ForEachOperator(
            task_id='for_each_timeofftype',
            items=lambda dag_run: dag_run.conf['timeoff_type_uri_for_transferring_balance_into'],
            start_task='get_user_timeoff_policysetschedule',
            end_task='for_each_timeofftype_end'
        )

        get_user_timeoff_policysetschedule = rail.PythonOperator(
            task_id='get_user_timeoff_policysetschedule',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                "get_user_details")["timeoffpolicies"], 'timeOffType.uri', rail.result('for_each_timeofftype')['uri'], 'policySetSchedule',[])
        )

        def timeoff_type_disabled_or_not_assigned_check(dag_run):
            find_timeoff_type_in_user_details = rail.find_first_by_attr_and_get_attr(rail.result(
                "get_user_details")["timeoffpolicies"], 'timeOffType.uri', rail.result('for_each_timeofftype')['uri'], 'isTimeOffAllowedAgainstThisTimeOffType', null)
            if find_timeoff_type_in_user_details == null:
                return {
                    'check': False,
                    'details': "Time off not assigned"
                }
            elif find_timeoff_type_in_user_details != null:
                if find_timeoff_type_in_user_details == False:
                    return {
                        'check': True,
                        'details': f"Time off bookings are disabled for the required time off type {rail.result('for_each_timeofftype')['name']} for user"
                    }
            return {
                'check': False,
                'details': ""
            }

        log_timeoff_type_disabled_or_not_assigned = rail.PythonOperator(
            task_id='log_timeoff_type_disabled_or_not_assigned',
            python_callable=timeoff_type_disabled_or_not_assigned_check
        )

        if_required_timeoff_is_disabled = rail.IfOperator(
            task_id='if_required_timeoff_is_disabled',
            test=lambda: rail.result(
                "log_timeoff_type_disabled_or_not_assigned")["check"],
            yes_task='log_error_required_timeoff_type_not_assigned_to_user',
            no_task='get_all_timeoff_event_scripts'
        )

        log_error_required_timeoff_type_not_assigned_to_user = rail.WriteLogOperator(
            task_id='log_error_required_timeoff_type_not_assigned_to_user',
            log="{{dag_run.conf.user_log}}",
            message='na',
            severity='Error',
            properties=lambda dag_run: {
                'jobid': dag_run.conf['parentjobid'],
                "login_name": dag_run.conf['login_name'],
                "status": "Error",
                "details": rail.result("log_timeoff_type_disabled_or_not_assigned")["details"]
            }
        )

        get_all_timeoff_event_scripts = rail.RepliconServiceOperator(
            task_id="get_all_timeoff_event_scripts",
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetActiveScripts",
            data_handler=lambda response: {
                "yearly_monthly_accrual_with_expiry_rounding": rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', config.time_off_accrual_script_name, 'uri')
            }
        )

        get_all_timeoff_validation_scripts = rail.RepliconServiceOperator(
            task_id="get_all_timeoff_validation_scripts",
            endpoint="/services/TimeOffValidationScriptAdministrationService1.svc/GetActiveScripts",
            data_handler=lambda response: {
                "nl_past_booking_restriction": rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'NL - Past Booking Restriction', 'uri'),
                "prevent_balance_overdraw": rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Prevent balance overdraw', 'uri'),
                "prevent_use_during_probationary_period": rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Prevent use during probationary period', 'uri'),
                "require_other_time_off_balance_to_be_used_first": rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Require other time off balance to be used first', 'uri')
            }
        )

        if_required_timeoff_is_not_assigned = rail.IfOperator(
            task_id='if_required_timeoff_is_not_assigned',
            test=lambda: bool(not rail.result(
                "log_timeoff_type_disabled_or_not_assigned")["check"] and rail.result("log_timeoff_type_disabled_or_not_assigned")["details"]),
            yes_task='get_all_timeoff_type_assigned_to_user',
            no_task='if_timeoff_type_is_additional'
        )

        def get_all_time_off_type(response):
            assigned_timeoff = list(map(lambda x: x['uri'], response))
            assigned_timeoff.append(rail.result('for_each_timeofftype')['uri'])
            return assigned_timeoff

        get_all_timeoff_type_assigned_to_user = rail.RepliconServiceOperator(
            task_id='get_all_timeoff_type_assigned_to_user',
            endpoint="/services/TimeOffService1.svc/GetTimeOffTypeAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result("get_user_details")["useruri"]
            },
            data_handler=get_all_time_off_type
        )

        assign_required_timeoff_type_to_user = rail.RepliconServiceOperator(
            task_id="assign_required_timeoff_type_to_user",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda: {
                    "userUri": rail.result("get_user_details")["useruri"],
                    "timeOffTypeUris": rail.result('get_all_timeoff_type_assigned_to_user')
            }
        )

        if_timeoff_type_is_additional = rail.IfOperator(
            task_id='if_timeoff_type_is_additional',
            test=lambda: bool(rail.result('for_each_timeofftype')['name'] == config.ANNUAL_LEAVE_ADDITIONAL),
            yes_task='put_additional_time_off_type_policy_schedule_for_user',
            no_task='put_carried_over_time_off_type_policy_schedule_for_user'
        )

        put_additional_time_off_type_policy_schedule_for_user = rail.RepliconServiceOperator(
            task_id="put_additional_time_off_type_policy_schedule_for_user",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: request_payload.get_additional_time_off_type_policy_payload(dag_run,
                                                                                             config.ANNUAL_LEAVE_ADDITIONAL,
                                                                                             config.ANNUAL_LEAVE)
        )

        put_carried_over_time_off_type_policy_schedule_for_user = rail.RepliconServiceOperator(
            task_id="put_carried_over_time_off_type_policy_schedule_for_user",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: request_payload.get_carried_over_time_off_type_policy_payload(dag_run,
                                                                                               config.ANNUAL_LEAVE_CARRIED_OVER)
        )

        log_successful_transfer = rail.WriteLogOperator(
            task_id='log_successful_transfer',
            log="{{dag_run.conf.user_log}}",
            message='na',
            severity='Successful',
            properties=lambda dag_run: {
                'jobid': dag_run.conf['parentjobid'],
                "login_name": dag_run.conf['login_name'],
                "status": "Successful",
                "details": "Balance of {} transferred from time off type {} to time off type {} is successful.".format\
                        (rail.find_first_by_attr_and_get_attr(dag_run.conf["balance_to_transfer"], 'name', rail.result('for_each_timeofftype')['name'], 'balance'),
                         dag_run.conf['timeoff_type_name_from_which_balance_is_picked'],
                         rail.result('for_each_timeofftype')['name']),
            }
        )

        for_each_timeofftype_end = rail.EmptyOperator(
            task_id='for_each_timeofftype_end',
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            log="{{dag_run.conf.user_log}}",
            trigger_rule='one_failed',
            message='na',
            severity='Error',
            properties=lambda dag_run: {
                'jobid': dag_run.conf['parentjobid'],
                "login_name": dag_run.conf['login_name'],
                "status": "Error",
                "details": rail.render_template("Error in transferring annual leave balance : {{get_error_message()}}")
            }
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> get_user_details

        get_user_details >> for_each_timeofftype

        for_each_timeofftype >> for_each_timeofftype_end
        for_each_timeofftype >> get_user_timeoff_policysetschedule >> log_timeoff_type_disabled_or_not_assigned >> if_required_timeoff_is_disabled

        if_required_timeoff_is_disabled >> rail.Label(
            "Yes") >> log_error_required_timeoff_type_not_assigned_to_user >> for_each_timeofftype_end
        if_required_timeoff_is_disabled >> rail.Label(
            "No") >> get_all_timeoff_event_scripts >> get_all_timeoff_validation_scripts >> if_required_timeoff_is_not_assigned

        if_required_timeoff_is_not_assigned >> rail.Label(
            "Yes") >> get_all_timeoff_type_assigned_to_user >> assign_required_timeoff_type_to_user >> if_timeoff_type_is_additional
        if_required_timeoff_is_not_assigned >> rail.Label(
            "No") >> if_timeoff_type_is_additional

        if_timeoff_type_is_additional >> rail.Label(
            "Yes") >> put_additional_time_off_type_policy_schedule_for_user >> log_successful_transfer
        if_timeoff_type_is_additional >> rail.Label(
            "No") >> put_carried_over_time_off_type_policy_schedule_for_user >> log_successful_transfer

        log_successful_transfer >> for_each_timeofftype_end

        for_each_timeofftype_end >> catch_and_log_error

        return dag


rail.for_each_instance(create_dag)
