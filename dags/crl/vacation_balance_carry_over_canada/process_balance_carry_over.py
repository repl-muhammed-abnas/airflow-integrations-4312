from datetime import timedelta,  datetime
from airflow.models import Variable
import json
import rail

from crl.vacation_balance_carry_over_canada.utils import response_filter

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_dagid,
        description='CRL - CANADA - Vacation Balance Carry Over CHILD',
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
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id='get_user_details',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri":"{{dag_run.conf.user_uri}}",
                        "loginName":null,
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

        is_carry_over_timeoff_type_assigned_to_user = rail.IfOperator(
            task_id='is_carry_over_timeoff_type_assigned_to_user',
            test=lambda dag_run: bool(rail.find_first_by_attr_and_get_attr(rail.result(
                "get_user_details")["timeoffpolicies"], 'timeOffType.uri', dag_run.conf['timeoff_type_uri_for_transferring_balance_into']['uri'])),
            yes_task="is_carry_over_timeoff_type_disabled",
            no_task="get_all_timeoff_type_to_be_assigned_to_user"
        )

        is_carry_over_timeoff_type_disabled = rail.IfOperator(
            task_id='is_carry_over_timeoff_type_disabled',
            test=lambda  dag_run: not rail.find_first_by_attr_and_get_attr(rail.result(
                "get_user_details")["timeoffpolicies"], 'timeOffType.uri', dag_run.conf['timeoff_type_uri_for_transferring_balance_into']['uri'],
                'isTimeOffAllowedAgainstThisTimeOffType', null),
            yes_task='get_all_timeoff_type_to_be_assigned_to_user',
            no_task='get_historical_timeoff_policy_sets'
        )

        get_all_timeoff_type_to_be_assigned_to_user = rail.RepliconServiceOperator(
            task_id='get_all_timeoff_type_to_be_assigned_to_user',
            endpoint="/services/TimeOffService1.svc/GetTimeOffTypeAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result("get_user_details")["useruri"]
            },
            data_handler=lambda response, dag_run: response_filter.get_all_time_off_type(response,dag_run)
        )

        assign_carry_over_timeoff_type_to_user = rail.RepliconServiceOperator(
            task_id="assign_carry_over_timeoff_type_to_user",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda: {
                    "userUri": rail.result("get_user_details")["useruri"],
                    "timeOffTypeUris": rail.result('get_all_timeoff_type_to_be_assigned_to_user')
            }
        )

        get_historical_timeoff_policy_sets = rail.PythonOperator(
            task_id = "get_historical_timeoff_policy_sets",
            python_callable= lambda dag_run: response_filter.get_historical_timeoff_policy_set(dag_run)
        )

        get_default_time_off_policy_set = rail.RepliconServiceOperator(
                task_id="get_default_time_off_policy_set",
                endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
                data= lambda dag_run:{
                    "timeOffTypeUri": dag_run.conf['timeoff_type_uri_for_transferring_balance_into']['uri']
                },
                data_handler=lambda response, dag_run:response_filter.get_policy_to_assign_for_timeoff(response,dag_run)
            )
        

        get_all_policy_to_assign = rail.PythonOperator(
            task_id='get_all_policy_to_assign',
            python_callable= response_filter.get_all_policy_to_assign_to_carry_over
        )

        put_carry_over_time_off_type_policy_schedule_for_user = rail.RepliconServiceOperator(
            task_id="put_carry_over_time_off_type_policy_schedule_for_user",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run:  {
                "timeOffAccount": {
                    "userUri": rail.result("get_user_details")["useruri"],
                    "timeOffTypeUri": dag_run.conf['timeoff_type_uri_for_transferring_balance_into']['uri']
                },
                "policySetScheduleEntries": json.loads(rail.result('get_all_policy_to_assign'))
            }
        )

        finish = rail.EmptyOperator(
            task_id="finish"
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> finish
        can_run_batch_task >> rail.Label("No") >> get_user_details

        get_user_details >> is_carry_over_timeoff_type_assigned_to_user >> rail.Label('No') >> get_all_timeoff_type_to_be_assigned_to_user
        is_carry_over_timeoff_type_assigned_to_user >> rail.Label('Yes') >> is_carry_over_timeoff_type_disabled
        is_carry_over_timeoff_type_disabled >> rail.Label('No') >> get_historical_timeoff_policy_sets
        is_carry_over_timeoff_type_disabled >> rail.Label('Yes') >> get_all_timeoff_type_to_be_assigned_to_user
        get_all_timeoff_type_to_be_assigned_to_user >> assign_carry_over_timeoff_type_to_user >> get_historical_timeoff_policy_sets
        get_historical_timeoff_policy_sets >> get_default_time_off_policy_set >> get_all_policy_to_assign >> put_carry_over_time_off_type_policy_schedule_for_user
        put_carry_over_time_off_type_policy_schedule_for_user >> finish

        return dag


rail.for_each_instance(create_dag)
