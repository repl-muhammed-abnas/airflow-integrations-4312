from datetime import timedelta
from airflow.models import Variable
import rail
from wipro.annual_leave_balance_transfer_switzerland.utils import request_payload, python_callable

null = None


def create_dag(config):
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

        get_user_timeoff_policysetschedule = rail.PythonOperator(
            task_id='get_user_timeoff_policysetschedule',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                "get_user_details")["timeoffpolicies"], 'timeOffType.uri',
                dag_run.conf['timeoff_type_uri_for_transferring_balance_into'], 'policySetSchedule',[])
        )

        query_nonzero_balance_records = rail.QueryCollectionOperator(
            task_id='query_nonzero_balance_records',
            query="""SELECT * FROM report_data_collection WHERE login_name='{{dag_run.conf.login_name}}' """,
            name='nonzero_balance_records'
        )

        get_timeoff_type_and_balance_to_transfer = rail.PythonOperator(
            task_id='get_timeoff_type_and_balance_to_transfer',
            python_callable=lambda: python_callable.get_balance_to_transfer(config)
        )

        if_timeoff_has_balance = rail.IfOperator(
            task_id='if_timeoff_has_balance',
            test=lambda: rail.result(
                "get_timeoff_type_and_balance_to_transfer")["from"],
            yes_task='can_transfer_timeoff',
            no_task='catch_and_log_error'
        )

        can_transfer_timeoff = rail.PythonOperator(
            task_id='can_transfer_timeoff',
            python_callable=python_callable.can_transfer_timeoff_balance
        )

        if_required_timeoff_is_disabled = rail.IfOperator(
            task_id='if_required_timeoff_is_disabled',
            test=lambda: rail.result(
                "can_transfer_timeoff")["check"],
            yes_task='log_transfer_timeoff_not_assigned',
            no_task='if_required_timeoff_is_not_assigned'
        )

        log_transfer_timeoff_not_assigned = rail.WriteLogOperator(
            task_id='log_transfer_timeoff_not_assigned',
            log="{{dag_run.conf.user_log}}",
            message='na',
            severity='Exception',
            properties=lambda dag_run: {
                'jobid': dag_run.conf['parentjobid'],
                "login_name": dag_run.conf['login_name'],
                "status": "Exception",
                "details": rail.result("can_transfer_timeoff")["details"]
            }
        )

        # if the transfer timeoff type is not assigned then we will be assigning the timeoff type first
        if_required_timeoff_is_not_assigned = rail.IfOperator(
            task_id='if_required_timeoff_is_not_assigned',
            test=lambda: bool(not rail.result(
                "can_transfer_timeoff")["check"] and rail.result("can_transfer_timeoff")["details"]),
            yes_task='get_all_timeoff_type_assigned_to_user',
            no_task='put_additional_time_off_type_policy_schedule_for_user'
        )

        get_all_timeoff_type_assigned_to_user = rail.RepliconServiceOperator(
            task_id='get_all_timeoff_type_assigned_to_user',
            endpoint="/services/TimeOffService1.svc/GetTimeOffTypeAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result("get_user_details")["useruri"]
            },
            data_handler=lambda response, dag_run: python_callable.get_all_time_off_type(dag_run,response)
        )

        assign_required_timeoff_type_to_user = rail.RepliconServiceOperator(
            task_id="assign_required_timeoff_type_to_user",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda: {
                    "userUri": rail.result("get_user_details")["useruri"],
                    "timeOffTypeUris": rail.result('get_all_timeoff_type_assigned_to_user')
            }
        )

        put_additional_time_off_type_policy_schedule_for_user = rail.RepliconServiceOperator(
            task_id="put_additional_time_off_type_policy_schedule_for_user",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": rail.result("get_user_details")["useruri"],
                    "timeOffTypeUri": dag_run.conf['timeoff_type_uri_for_transferring_balance_into']
                },
                "policySetScheduleEntries": request_payload.get_final_policyset(dag_run, config)
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
                "details": python_callable.get_transfer_success_message(dag_run.conf, rail.result('get_timeoff_type_and_balance_to_transfer'))
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

        get_user_details >> get_user_timeoff_policysetschedule >> query_nonzero_balance_records >> \
        get_timeoff_type_and_balance_to_transfer >> if_timeoff_has_balance >> rail.Label(
            "Yes") >> can_transfer_timeoff >> if_required_timeoff_is_disabled
        if_timeoff_has_balance >> rail.Label(
            "No") >> catch_and_log_error

        if_required_timeoff_is_disabled >> rail.Label(
            "Yes") >> log_transfer_timeoff_not_assigned >> catch_and_log_error
        if_required_timeoff_is_disabled >> rail.Label(
            "No") >> if_required_timeoff_is_not_assigned

        if_required_timeoff_is_not_assigned >> rail.Label(
            "Yes") >> get_all_timeoff_type_assigned_to_user >> assign_required_timeoff_type_to_user >> put_additional_time_off_type_policy_schedule_for_user
        if_required_timeoff_is_not_assigned >> rail.Label(
            "No") >> put_additional_time_off_type_policy_schedule_for_user

        put_additional_time_off_type_policy_schedule_for_user >> log_successful_transfer

        log_successful_transfer >> catch_and_log_error

        return dag


rail.for_each_instance(create_dag)
