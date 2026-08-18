from datetime import timedelta
import json
from airflow.models import Variable
import rail
from assuredpartnersinc.user_import_v4.utils import python_callable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_new_user_sick_pay_h_policy_assignment_dag_id,
        description=f'Assured Partners User Import new user sick pay h Policy Assignment Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_default_policy_from_global_level_4'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_default_policy_from_global_level_4',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_default_policy_from_global_level_4 = rail.RepliconServiceOperator(
            task_id='get_default_policy_from_global_level_4',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
            }
        )

        get_policies_to_be_assigned_and_max_min_offsets = rail.PythonOperator(
            task_id='get_policies_to_be_assigned_and_max_min_offsets',
            python_callable=lambda dag_run: python_callable.policies_to_be_assigned(
                rail.result('get_default_policy_from_global_level_4'), dag_run)
        )

        get_policysets_list = rail.PythonOperator(
            task_id='get_policysets_list',
            python_callable=lambda dag_run: python_callable.add_items_to_policysets_list(
                rail.result('get_policies_to_be_assigned_and_max_min_offsets'), dag_run)
        )

        log_gsub_for_gettiing_rid_of_starting_balance_set_to_29 = rail.PythonOperator(
            task_id='log_gsub_for_gettiing_rid_of_starting_balance_set_to_29',
            python_callable=lambda:  python_callable.get_timeoffbalanceeventscript_to_gsub(
                rail.result("get_default_policy_from_global_level_4"), 0, 'Set initial balance for the first day of a policy')
        )

        log_final_policy_to_assign_31 = rail.PythonOperator(
            task_id='log_final_policy_to_assign_31',
            python_callable=lambda:  json.loads(json.dumps(rail.result("get_policysets_list"), ensure_ascii=False).replace(rail.result('log_gsub_for_gettiing_rid_of_starting_balance_set_to_29'), "").replace('null', '"effective"').replace(
                '"script"', '"scriptTarget"'))
        )

        assign_time_offpolicy_32 = rail.RepliconServiceOperator(
            task_id='assign_time_offpolicy_32',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('log_final_policy_to_assign_31')
            }
        )

        catch_and_log_error = rail.SetVariableOperator(
            task_id="catch_and_log_error",
            trigger_rule='one_failed',
            name='response_from_dag',
            append=False,
            value="Error in Timeoff Assignment - Sick Pay-H : {{get_error_message()}}"
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.get_dag_run_var(
                "response_from_dag") if rail.result('catch_and_log_error') else ""
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error >> final_response_from_dag
        can_run_batch_task >> rail.Label(
            'No') >> get_default_policy_from_global_level_4

        get_default_policy_from_global_level_4 >> get_policies_to_be_assigned_and_max_min_offsets \
            >> get_policysets_list >> log_gsub_for_gettiing_rid_of_starting_balance_set_to_29 \
            >> log_final_policy_to_assign_31 >> assign_time_offpolicy_32
        assign_time_offpolicy_32 >> catch_and_log_error

        return dag


rail.for_each_instance(create_dag)
