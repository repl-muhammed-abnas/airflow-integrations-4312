# pylint: disable=line-too-long
from datetime import timedelta
from airflow.models import Variable
import rail
from momentive.common_recipes_userimport.utils import request_payload, python_callable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.momentive_othercountries_user_sync_put_zero_balance_payout_child_dag_id,
        description=f'Momentive_othercountries_user_sync_put_zero_balance_for_payout_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config", extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_assigned_policy_for_timeofftype'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_assigned_policy_for_timeofftype',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # Recipe step 6: the user's time-off policy summary (per-type policy schedules).
        get_assigned_policy_for_timeofftype = rail.RepliconServiceOperator(
            task_id='get_assigned_policy_for_timeofftype',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data=request_payload.get_user_timeoff_policy_summary_payload
        )

        # Recipe steps 7-18: past-dated policy-set entries for the time-off type
        # (effectiveDate < termination date; null->"effective"/script->"scriptTarget").
        get_past_policyset_entries = rail.PythonOperator(
            task_id='get_past_policyset_entries',
            python_callable=lambda dag_run: python_callable.past_policyset_entries(
                rail.result('get_assigned_policy_for_timeofftype'),
                dag_run.conf['timeoffuri'], dag_run.conf['terminationdate'])
        )

        # Recipe step 19: only write when at least one past-dated policy entry exists.
        if_past_policysetschedule_present = rail.IfOperator(
            task_id='if_past_policysetschedule_present',
            test="{{ result('get_past_policyset_entries') | is_truthy }}",
            yes_task='put_time_off_policy_with_remaining_balance',
            no_task='log_no_policy'
        )

        # Recipe step 20: PutUserTimeOffAccountPolicySetSchedule (past entries + new balance entry).
        put_time_off_policy_with_remaining_balance = rail.RepliconServiceOperator(
            task_id='put_time_off_policy_with_remaining_balance',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=request_payload.put_zero_balance_payout_payload
        )

        # Recipe step 22: nothing to do when no policy exists for the type.
        log_no_policy = rail.PythonOperator(
            task_id='log_no_policy',
            python_callable=lambda: 'No policy, hence no 0 balance required'
        )

        # Recipe step 23/24: leaf error reply (gathered by the parent on failure).
        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "Put 0 balance for payout - Dag_Run Error - {{ get_error_message() }}")
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.result('catch_error') if rail.result('catch_error') else ""
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error >> final_response_from_dag
        can_run_batch_task >> rail.Label('No') >> get_assigned_policy_for_timeofftype

        get_assigned_policy_for_timeofftype >> get_past_policyset_entries >> if_past_policysetschedule_present

        if_past_policysetschedule_present >> rail.Label('Yes') >> put_time_off_policy_with_remaining_balance >> final_response_from_dag
        if_past_policysetschedule_present >> rail.Label('No') >> log_no_policy >> final_response_from_dag

    return dag


rail.for_each_instance(create_dag)
