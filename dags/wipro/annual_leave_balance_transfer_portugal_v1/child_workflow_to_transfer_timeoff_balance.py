from datetime import timedelta, datetime
import json
from airflow.models import Variable
import rail
from wipro.annual_leave_balance_transfer_portugal_v1.utils import python_callable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_workflow_to_transfer_timeoff_balance_dag_id,
        description=f'WIPRO | Annual leave Balance Transfer for Portugal | Transfer Timeoff Balance Child {config.instance}',
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
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                "get_user_details")["timeoffpolicies"], 'timeOffType.uri', dag_run.conf['timeoff_type_uri_for_transferring_balance'], 'policySetSchedule')
        )

        def timeoff_type_disabled_or_not_assigned_check(dag_run):
            find_timeoff_type_in_user_details = rail.find_first_by_attr_and_get_attr(rail.result(
                "get_user_details")["timeoffpolicies"], 'timeOffType.uri', dag_run.conf['timeoff_type_uri_for_transferring_balance'], 'isTimeOffAllowedAgainstThisTimeOffType', null)
            if find_timeoff_type_in_user_details == null:
                return {
                    'check': True,
                    'details': f"The required time off type {dag_run.conf['timeoff_type_name_for_transferring_balance']} is not assigned for user"
                }
            elif find_timeoff_type_in_user_details != null:
                if find_timeoff_type_in_user_details == False:
                    return {
                        'check': True,
                        'details': f"Time off bookings for the required time off type {dag_run.conf['timeoff_type_name_for_transferring_balance']} are disabled for user"
                    }
            return {
                'check': False,
                'details': ""
            }

        log_timeoff_type_disabled_or_not_assigned = rail.PythonOperator(
            task_id='log_timeoff_type_disabled_or_not_assigned',
            python_callable=timeoff_type_disabled_or_not_assigned_check
        )

        if_required_timeoff_is_disabled_or_not_assigned = rail.IfOperator(
            task_id='if_required_timeoff_is_disabled_or_not_assigned',
            test=lambda: rail.result(
                "log_timeoff_type_disabled_or_not_assigned")["check"],
            yes_task='log_error_required_timeoff_type_not_assigned_to_user',
            no_task='get_default_policy_from_global_level'
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

        get_default_policy_from_global_level = rail.RepliconServiceOperator(
            task_id='get_default_policy_from_global_level',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ dag_run.conf.timeoff_type_uri_for_transferring_balance }}"
            }
        )

        if_timeoff_to_transfer_balance_into_is_annual_leave_lapsed = rail.IfOperator(
            task_id='if_timeoff_to_transfer_balance_into_is_annual_leave_lapsed',
            test=lambda dag_run: dag_run.conf['timeoff_type_name_for_transferring_balance'] == config.ANNUAL_LEAVE_LAPSED,
            yes_task='get_existing_balance_for_timeoff_if_annual_leave_lapsed',
            no_task='log_get_final_modified_policy_set'
        )

        get_existing_balance_for_timeoff_if_annual_leave_lapsed = rail.RepliconServiceOperator(
            task_id='get_existing_balance_for_timeoff_if_annual_leave_lapsed',
            endpoint="/services/TimeOffService1.svc/GetUserTimeOffTypeBalanceSummary",
            data=lambda dag_run: {
                "userUri": rail.result("get_user_details")["useruri"],
                "timeOffTypeUri":  dag_run.conf['timeoff_type_uri_for_transferring_balance'],
                "asOfDate": python_callable.get_split_date(dag_run.conf['efective_date_for_new_policyset'], 'int')
            },
            data_handler=lambda res: res['timeRemaining']['decimalWorkdays'] if res else 0
        )

        def get_final_policyset(dag_run):
            balance_to_transfer = float(dag_run.conf['balance_to_transfer']) + (float(rail.result(
                'get_existing_balance_for_timeoff_if_annual_leave_lapsed')) if rail.result(
                    'get_existing_balance_for_timeoff_if_annual_leave_lapsed') else 0)
            user_timeoff_policysetschedule = json.loads(json.dumps(rail.result("get_user_timeoff_policysetschedule"), ensure_ascii=False).replace('"null"', '"effective"').replace(
                '"script"', '"scriptTarget"'))
            default_policyset_for_0_offset = rail.find_first_by_attr_and_get_attr(rail.result(
                'get_default_policy_from_global_level'), 'startOffset.offsetValue', 0, 'policySet')

            starting_balance_script_with_0_balance = json.dumps(
                {"keyUri": "urn:replicon:script-key:parameter:amount", "value": {"number": 0.0}})
            modified_script_with_required_starting_balance = json.dumps(
                {"keyUri": "urn:replicon:script-key:parameter:amount", "value": {"number": balance_to_transfer}})

            policyset_json = json.dumps(
                default_policyset_for_0_offset, ensure_ascii=False)

            if "urn:replicon:script-key:parameter:reset-on-day-of-month" in policyset_json:
                if datetime.strptime(dag_run.conf['user_start_date'], config.DATE_DEFAULT_FORMAT) >= datetime.strptime(
                        dag_run.conf['probation_cutoff_date'], config.DATE_DEFAULT_FORMAT):
                    yearly_reset_script_default = json.dumps(
                        {"keyUri": "urn:replicon:script-key:parameter:reset-on-month", "value": {"uri": "urn:replicon:month:may"}})
                    modified_yearly_reset_script_for_probation_users = json.dumps(
                        {"keyUri": "urn:replicon:script-key:parameter:reset-on-month", "value": {"uri": "urn:replicon:month:july"}})

                    policyset_json = policyset_json.replace(
                        yearly_reset_script_default, modified_yearly_reset_script_for_probation_users)

            policyset_to_add = json.loads(policyset_json.replace(
                starting_balance_script_with_0_balance, modified_script_with_required_starting_balance).replace('"null"', '"effective"').replace(
                '"script"', '"scriptTarget"'))

            user_timeoff_policysetschedule.append({
                "description": "Effective on - " + dag_run.conf['efective_date_for_new_policyset'],
                "effectiveDate": python_callable.get_split_date(dag_run.conf['efective_date_for_new_policyset'], 'int'),
                "policySet": policyset_to_add
            })

            return user_timeoff_policysetschedule

        log_get_final_modified_policy_set = rail.PythonOperator(
            task_id='log_get_final_modified_policy_set',
            python_callable=get_final_policyset
        )

        assign_modified_timeoff_policy = rail.RepliconServiceOperator(
            task_id='assign_modified_timeoff_policy',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": rail.result("get_user_details")["useruri"],
                    "timeOffTypeUri": dag_run.conf['timeoff_type_uri_for_transferring_balance']
                },
                "policySetScheduleEntries": rail.result('log_get_final_modified_policy_set')
            }
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
                "details": f"Balance transfer from time off type {dag_run.conf['timeoff_type_name_from_which_balance_is_picked']} to time off type {dag_run.conf['timeoff_type_name_for_transferring_balance']} is successful"
            }
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

        get_user_details >> get_user_timeoff_policysetschedule >> log_timeoff_type_disabled_or_not_assigned >> if_required_timeoff_is_disabled_or_not_assigned

        if_required_timeoff_is_disabled_or_not_assigned >> rail.Label(
            "Yes") >> log_error_required_timeoff_type_not_assigned_to_user >> catch_and_log_error
        if_required_timeoff_is_disabled_or_not_assigned >> rail.Label(
            "No") >> get_default_policy_from_global_level >> if_timeoff_to_transfer_balance_into_is_annual_leave_lapsed

        if_timeoff_to_transfer_balance_into_is_annual_leave_lapsed >> rail.Label(
            "Yes") >> get_existing_balance_for_timeoff_if_annual_leave_lapsed >> log_get_final_modified_policy_set
        if_timeoff_to_transfer_balance_into_is_annual_leave_lapsed >> rail.Label(
            "No") >> log_get_final_modified_policy_set

        get_existing_balance_for_timeoff_if_annual_leave_lapsed \
            >> log_get_final_modified_policy_set >> assign_modified_timeoff_policy >> log_successful_transfer >> catch_and_log_error

        return dag


rail.for_each_instance(create_dag)
