from datetime import timedelta
from airflow.models import Variable
import rail
from wipro.annual_leave_balance_transfer_france.utils import request_payload, python_callable
from wipro.annual_leave_balance_transfer_france.task.get_default_policy_schedules import get_default_policyschedules_task_group

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_dag,
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

        get_user_timeoff_policysetschedule = rail.PythonOperator(
            task_id='get_user_timeoff_policysetschedule',
            python_callable=lambda dag_run: {
                "_".join(key.split("_")[1:-1]): rail.find_first_by_attr_and_get_attr(
                    rail.result("get_user_details")["timeoffpolicies"],
                    'timeOffType.uri',
                    uri,
                    'policySetSchedule'
                )
                for key, uri in dag_run.conf['all_timeoff_type_uris'].items()
            }
        )

        query_nonzero_balance_records = rail.QueryCollectionOperator(
            task_id='query_nonzero_balance_records',
            query="""SELECT * FROM report_data_collection WHERE login_name='{{dag_run.conf.login_name}}' """,
            name='valid_records_to_process'
        )

        log_invalid_timeoff_type = rail.PythonOperator(
            task_id='log_invalid_timeoff_type',
            python_callable=python_callable.timeoff_type_disabled_or_not_assigned_check
        )

        if_required_timeoff_is_disabled_or_not_assigned = rail.IfOperator(
            task_id='if_required_timeoff_is_disabled_or_not_assigned',
            test=lambda: rail.result(
                "log_invalid_timeoff_type")["is_invalid"],
            yes_task='log_required_timeoff_type_not_assigned_to_user',
            no_task='get_timeoff_type_and_balance_to_transfer'
        )

        log_required_timeoff_type_not_assigned_to_user = rail.WriteLogOperator(
            task_id='log_required_timeoff_type_not_assigned_to_user',
            log="{{dag_run.conf.balance_transfer_log}}",
            message='na',
            severity='Exception',
            properties=lambda dag_run: {
                "login_name": dag_run.conf['login_name'],
                "status": "Exception",
                "details": rail.result("log_invalid_timeoff_type")["details"]
            }
        )

        get_timeoff_type_and_balance_to_transfer = rail.PythonOperator(
            task_id='get_timeoff_type_and_balance_to_transfer',
            python_callable=python_callable.get_balance_to_transfer
        )

        if_annual_leave_has_balance = rail.IfOperator(
            task_id='if_annual_leave_has_balance',
            test=lambda: True if (config.ANNUAL_LEAVE in rail.result(
                "get_timeoff_type_and_balance_to_transfer") and float(rail.result(
                "get_timeoff_type_and_balance_to_transfer")[config.ANNUAL_LEAVE]) > 0.0 and 'annual_leave_carried_over' in rail.result(
                    "get_user_timeoff_policysetschedule")) else False,
            yes_task='assign_policy_to_annual_leave_carried_over',
            no_task='if_annual_leave_accrued_has_balance'
        )

        assign_policy_to_annual_leave_carried_over = rail.RepliconServiceOperator(
            task_id='assign_policy_to_annual_leave_carried_over',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": rail.result("get_user_details")["useruri"],
                    "timeOffTypeUri": dag_run.conf['timeoff_type_uris']['into']['timeoff_annual_leave_carried_over_uri']
                },
                "policySetScheduleEntries": request_payload.get_final_policyset(
                    dag_run,
                    timeoff_uri=dag_run.conf['timeoff_type_uris']['into']['timeoff_annual_leave_carried_over_uri'],
                    default_policy_task_id='get_annual_leave_carried_over_default_policy',
                    timeoff_type_from=config.ANNUAL_LEAVE
                )
            }
        )

        log_successful_annual_leave_carried_over = rail.WriteLogOperator(
            task_id='log_successful_annual_leave_carried_over',
            log="{{dag_run.conf.balance_transfer_log}}",
            message='na',
            severity='Successful',
            properties=lambda dag_run: {
                "login_name": dag_run.conf['login_name'],
                "status": "Successful",
                "details": f"Balance transfer from time off type {config.ANNUAL_LEAVE} to time off type {config.ANNUAL_LEAVE_CARRIED_OVER} is successful"
            }
        )

        def get_annual_leave_accrued(config):
            result = rail.result("get_timeoff_type_and_balance_to_transfer")
            user_policy = rail.result("get_user_timeoff_policysetschedule")

            accrued = float(result.get(config.ANNUAL_LEAVE_ACCRUED, 0.0))
            annual = float(result.get(config.ANNUAL_LEAVE, 0.0))

            if (
                config.ANNUAL_LEAVE_ACCRUED in result and
                (
                    (annual < 0.0 and accrued == 0.0) or
                    (accrued > 0.0)
                ) and
                'annual_leave' in user_policy
            ):
                return True
            return False

        if_annual_leave_accrued_has_balance = rail.IfOperator(
            task_id='if_annual_leave_accrued_has_balance',
            test=lambda: get_annual_leave_accrued(config),
            yes_task='assign_policy_to_annual_leave',
            no_task='if_annual_leave_seniority_days_has_balance'
        )

        assign_policy_to_annual_leave = rail.RepliconServiceOperator(
            task_id='assign_policy_to_annual_leave',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": rail.result("get_user_details")["useruri"],
                    "timeOffTypeUri": dag_run.conf['timeoff_type_uris']['into']['timeoff_annual_leave_uri']
                },
                "policySetScheduleEntries": request_payload.get_final_policyset(
                    dag_run,
                    timeoff_uri=dag_run.conf['timeoff_type_uris']['into']['timeoff_annual_leave_uri'],
                    default_policy_task_id='get_annual_leave_default_policy',
                    timeoff_type_from=config.ANNUAL_LEAVE_ACCRUED,
                )
            }
        )

        log_successful_annual_leave = rail.WriteLogOperator(
            task_id='log_successful_annual_leave',
            log="{{dag_run.conf.balance_transfer_log}}",
            message='na',
            severity='Successful',
            properties=lambda dag_run: {
                "login_name": dag_run.conf['login_name'],
                "status": "Successful",
                "details": f"Balance transfer from time off type {config.ANNUAL_LEAVE_ACCRUED} to time off type {config.ANNUAL_LEAVE} is successful"
            }
        )

        if_annual_leave_seniority_days_has_balance = rail.IfOperator(
            task_id='if_annual_leave_seniority_days_has_balance',
            test=lambda: True if (config.ANNUAL_LEAVE_SENIORITY_DAYS in rail.result(
                "get_timeoff_type_and_balance_to_transfer") and float(rail.result(
                "get_timeoff_type_and_balance_to_transfer")[config.ANNUAL_LEAVE_SENIORITY_DAYS]) > 0.0 and 'annual_leave_seniority_days_carried_over' in rail.result(
                    "get_user_timeoff_policysetschedule")) else False,
            yes_task='assign_policy_to_annual_leave_seniority_days_carried_over',
            no_task='if_annual_leave_rtt_has_balance'
        )

        assign_policy_to_annual_leave_seniority_days_carried_over = rail.RepliconServiceOperator(
            task_id='assign_policy_to_annual_leave_seniority_days_carried_over',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": rail.result("get_user_details")["useruri"],
                    "timeOffTypeUri": dag_run.conf['timeoff_type_uris']['into']['timeoff_annual_leave_seniority_days_carried_over_uri']
                },
                "policySetScheduleEntries": request_payload.get_final_policyset(
                    dag_run,
                    timeoff_uri=dag_run.conf['timeoff_type_uris']['into']['timeoff_annual_leave_seniority_days_carried_over_uri'],
                    default_policy_task_id='get_annual_leave_seniority_days_carried_over_default_policy',
                    timeoff_type_from=config.ANNUAL_LEAVE_SENIORITY_DAYS
                )
            }
        )

        log_successful_annual_leave_seniority_days = rail.WriteLogOperator(
            task_id='log_successful_annual_leave_seniority_days',
            log="{{dag_run.conf.balance_transfer_log}}",
            message='na',
            severity='Successful',
            properties=lambda dag_run: {
                "login_name": dag_run.conf['login_name'],
                "status": "Successful",
                "details": f"Balance transfer from time off type {config.ANNUAL_LEAVE_SENIORITY_DAYS} to time off type {config.ANNUAL_LEAVE_SENIORITY_DAYS_CARRIED_OVER} is successful"
            }
        )

        if_annual_leave_rtt_has_balance = rail.IfOperator(
            task_id='if_annual_leave_rtt_has_balance',
            test=lambda: True if (config.ANNUAL_LEAVE_RTT in rail.result(
                "get_timeoff_type_and_balance_to_transfer") and float(rail.result(
                "get_timeoff_type_and_balance_to_transfer")[config.ANNUAL_LEAVE_RTT]) > 0.0 and 'annual_leave_rtt_carried_over' in rail.result(
                    "get_user_timeoff_policysetschedule")) else False,
            yes_task='assign_policy_to_annual_leave_rtt_carried_over',
            no_task='if_annual_leave_rtt_for_forfait_jours'
        )

        assign_policy_to_annual_leave_rtt_carried_over = rail.RepliconServiceOperator(
            task_id='assign_policy_to_annual_leave_rtt_carried_over',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": rail.result("get_user_details")["useruri"],
                    "timeOffTypeUri": dag_run.conf['timeoff_type_uris']['into']['timeoff_annual_leave_rtt_carried_over_uri']
                },
                "policySetScheduleEntries": request_payload.get_final_policyset(
                    dag_run,
                    timeoff_uri=dag_run.conf['timeoff_type_uris']['into']['timeoff_annual_leave_rtt_carried_over_uri'],
                    default_policy_task_id='get_annual_leave_rtt_carried_over_default_policy',
                    timeoff_type_from=config.ANNUAL_LEAVE_RTT
                )
            }
        )

        log_successful_annual_leave_rtt = rail.WriteLogOperator(
            task_id='log_successful_annual_leave_rtt',
            log="{{dag_run.conf.balance_transfer_log}}",
            message='na',
            severity='Successful',
            properties=lambda dag_run: {
                "login_name": dag_run.conf['login_name'],
                "status": "Successful",
                "details": f"Balance transfer from time off type {config.ANNUAL_LEAVE_RTT} to time off type {config.ANNUAL_LEAVE_RTT_CARRIED_OVER} is successful"
            }
        )

        if_annual_leave_rtt_for_forfait_jours = rail.IfOperator(
            task_id='if_annual_leave_rtt_for_forfait_jours',
            test=lambda: True if (config.ANNUAL_LEAVE_RTT_FOR_FORFAIT_JOURS in rail.result(
                "get_timeoff_type_and_balance_to_transfer") and float(rail.result(
                "get_timeoff_type_and_balance_to_transfer")[config.ANNUAL_LEAVE_RTT_FOR_FORFAIT_JOURS]) > 0.0 and 'annual_leave_rtt_for_forfait_jours_carried_over' in rail.result(
                    "get_user_timeoff_policysetschedule")) else False,
            yes_task='assign_policy_to_annual_leave_rtt_for_forfait_jours_carried_over',
            no_task='finish'
        )

        assign_policy_to_annual_leave_rtt_for_forfait_jours_carried_over = rail.RepliconServiceOperator(
            task_id='assign_policy_to_annual_leave_rtt_for_forfait_jours_carried_over',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": rail.result("get_user_details")["useruri"],
                    "timeOffTypeUri": dag_run.conf['timeoff_type_uris']['into']['timeoff_annual_leave_rtt_for_forfait_jours_carried_over_uri']
                },
                "policySetScheduleEntries": request_payload.get_final_policyset(
                    dag_run,
                    timeoff_uri=dag_run.conf['timeoff_type_uris']['into']['timeoff_annual_leave_rtt_for_forfait_jours_carried_over_uri'],
                    default_policy_task_id='get_annual_leave_rtt_for_forfait_jours_carried_over_default_policy',
                    timeoff_type_from=config.ANNUAL_LEAVE_RTT_FOR_FORFAIT_JOURS
                )
            }
        )

        log_successful_annual_leave_rtt_for_forfait_jours = rail.WriteLogOperator(
            task_id='log_successful_annual_leave_rtt_for_forfait_jours',
            log="{{dag_run.conf.balance_transfer_log}}",
            message='na',
            severity='Successful',
            properties=lambda dag_run: {
                "login_name": dag_run.conf['login_name'],
                "status": "Successful",
                "details": f"Balance transfer from time off type {config.ANNUAL_LEAVE_RTT_FOR_FORFAIT_JOURS} to time off type {config.ANNUAL_LEAVE_RTT_FOR_FORFAIT_JOURS_CARRIED_OVER} is successful"
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            log="{{dag_run.conf.balance_transfer_log}}",
            trigger_rule='one_failed',
            message='na',
            severity='Error',
            properties=lambda dag_run: {
                "login_name": dag_run.conf['login_name'],
                "status": "Error",
                "details": rail.render_template("Error in transferring annual leave balance : {{get_error_message()}}")
            }
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> get_user_details

        get_user_details >> get_user_timeoff_policysetschedule >> query_nonzero_balance_records >> log_invalid_timeoff_type >> if_required_timeoff_is_disabled_or_not_assigned

        if_required_timeoff_is_disabled_or_not_assigned >> rail.Label(
            "Yes") >> log_required_timeoff_type_not_assigned_to_user >> get_timeoff_type_and_balance_to_transfer
        if_required_timeoff_is_disabled_or_not_assigned >> rail.Label(
            "No") >> get_timeoff_type_and_balance_to_transfer

        get_timeoff_type_and_balance_to_transfer >> if_annual_leave_has_balance >> rail.Label(
            "Yes") >> assign_policy_to_annual_leave_carried_over >> log_successful_annual_leave_carried_over >> if_annual_leave_accrued_has_balance
        if_annual_leave_has_balance >> rail.Label(
            "No") >> if_annual_leave_accrued_has_balance
        
        if_annual_leave_accrued_has_balance >> rail.Label(
            "Yes") >> assign_policy_to_annual_leave >> log_successful_annual_leave >> if_annual_leave_seniority_days_has_balance
        if_annual_leave_accrued_has_balance >> rail.Label(
            "No") >> if_annual_leave_seniority_days_has_balance
        
        if_annual_leave_seniority_days_has_balance >> rail.Label(
            "Yes") >> assign_policy_to_annual_leave_seniority_days_carried_over >> log_successful_annual_leave_seniority_days >> if_annual_leave_rtt_has_balance
        if_annual_leave_seniority_days_has_balance >> rail.Label(
            "No") >> if_annual_leave_rtt_has_balance
        
        if_annual_leave_rtt_has_balance >> rail.Label(
            "Yes") >> assign_policy_to_annual_leave_rtt_carried_over >> log_successful_annual_leave_rtt >> if_annual_leave_rtt_for_forfait_jours
        if_annual_leave_rtt_has_balance >> rail.Label(
            "No") >> if_annual_leave_rtt_for_forfait_jours
        
        if_annual_leave_rtt_for_forfait_jours >> rail.Label(
            "Yes") >> assign_policy_to_annual_leave_rtt_for_forfait_jours_carried_over >> log_successful_annual_leave_rtt_for_forfait_jours >> finish
        if_annual_leave_rtt_for_forfait_jours >> rail.Label(
            "No") >> finish
        
        finish >> catch_and_log_error

        return dag

rail.for_each_instance(create_dag)
