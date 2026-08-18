from datetime import timedelta
from airflow.models import Variable
import rail
from wipro.annual_leave_balance_transfer_spain.utils import request_payload, python_callable

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
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                "get_user_details")["timeoffpolicies"], 'timeOffType.uri', dag_run.conf['timeoff_type_uris']['timeoff_uri_to_transfer_balance_into'], 'policySetSchedule')
        )

        log_timeoff_type_disabled_or_not_assigned = rail.PythonOperator(
            task_id='log_timeoff_type_disabled_or_not_assigned',
            python_callable=python_callable.timeoff_type_disabled_or_not_assigned_check
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
                "timeOffTypeUri": "{{ dag_run.conf.timeoff_type_uris.timeoff_uri_to_transfer_balance_into }}"
            }
        )

        get_final_modified_policy_set = rail.PythonOperator(
            task_id='get_final_modified_policy_set',
            python_callable=request_payload.get_final_policyset
        )

        assign_modified_timeoff_policy = rail.RepliconServiceOperator(
            task_id='assign_modified_timeoff_policy',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": rail.result("get_user_details")["useruri"],
                    "timeOffTypeUri": dag_run.conf['timeoff_type_uris']['timeoff_uri_to_transfer_balance_into']
                },
                "policySetScheduleEntries": rail.result('get_final_modified_policy_set')
            }
        )

        get_default_policy_from_global_level_for_annual_leave = rail.RepliconServiceOperator(
            task_id='get_default_policy_from_global_level_for_annual_leave',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ dag_run.conf.timeoff_type_uris.timeoff_uris_to_pick_balance_from }}"
            }
        )

        get_user_timeoff_policysetschedule_for_annual_leave = rail.PythonOperator(
            task_id='get_user_timeoff_policysetschedule_for_annual_leave',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                "get_user_details")["timeoffpolicies"], 'timeOffType.uri', dag_run.conf['timeoff_type_uris']['timeoff_uris_to_pick_balance_from'], 'policySetSchedule')
        )

        get_final_modified_policy_set_for_annual_leave = rail.PythonOperator(
            task_id='get_final_modified_policy_set_for_annual_leave',
            python_callable=lambda dag_run: request_payload.get_final_policyset_for_annual_leave(dag_run, config.YEARLY_ENTITLEMENT_MAPPER)
        )

        assign_modified_timeoff_policy_for_annual_leave = rail.RepliconServiceOperator(
            task_id='assign_modified_timeoff_policy_for_annual_leave',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": rail.result("get_user_details")["useruri"],
                    "timeOffTypeUri": dag_run.conf['timeoff_type_uris']['timeoff_uris_to_pick_balance_from']
                },
                "policySetScheduleEntries": rail.result('get_final_modified_policy_set_for_annual_leave')
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
            "No") >> get_default_policy_from_global_level

        get_default_policy_from_global_level >> get_final_modified_policy_set >> assign_modified_timeoff_policy >> \
        get_default_policy_from_global_level_for_annual_leave >> get_user_timeoff_policysetschedule_for_annual_leave >> \
        get_final_modified_policy_set_for_annual_leave >> \
        assign_modified_timeoff_policy_for_annual_leave >> log_successful_transfer >> catch_and_log_error

        return dag


rail.for_each_instance(create_dag)
