# pylint: disable=line-too-long
from datetime import timedelta
from airflow.models import Variable
import rail
from momentive.common_recipes_userimport.utils import request_payload, python_callable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.momentive_othercountries_user_sync_policy_rehire_update_days_child_dag_id,
        description=f'Momentive_othercountries_user_sync_policy_assignment_rehire_update_days_child_{config.instance}',
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
            no_task='get_user_policy_summary'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_user_policy_summary',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # Recipe steps 2-6: the user's per-type time-off policy summary.
        get_user_policy_summary = rail.RepliconServiceOperator(
            task_id='get_user_policy_summary',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data=request_payload.get_user_timeoff_policy_summary_payload
        )

        # Recipe steps 7-13: preserve this type's past-dated schedule entries
        # (effectiveDate strictly before the re-hire/start date).
        get_past_rehire_entries = rail.PythonOperator(
            task_id='get_past_rehire_entries',
            python_callable=lambda dag_run: python_callable.past_rehire_policyset_entries(
                rail.result('get_user_policy_summary'),
                dag_run.conf['timeoffuri'], dag_run.conf['startdate'])
        )

        # Recipe step 16: the default (seniority-tier) policy-set schedule for the type.
        get_default_policyset_schedule = rail.RepliconServiceOperator(
            task_id='get_default_policyset_schedule',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data=request_payload.get_default_policyset_schedule_for_type_payload
        )

        # Recipe steps 18-28: re-anchor each seniority tier (offset 0/1/5/10 years) onto
        # the start date, append to the preserved past entries, rename script->scriptTarget.
        build_rehire_policy_entries = rail.PythonOperator(
            task_id='build_rehire_policy_entries',
            python_callable=lambda dag_run: python_callable.build_rehire_policy_entries(
                rail.result('get_past_rehire_entries'),
                rail.result('get_default_policyset_schedule'),
                dag_run.conf['startdate'])
        )

        # Recipe step 27: only write when at least one entry was built.
        if_policy_entries_present = rail.IfOperator(
            task_id='if_policy_entries_present',
            test="{{ result('build_rehire_policy_entries') | is_truthy }}",
            yes_task='put_rehire_policy_schedule',
            no_task='log_no_policy'
        )

        # Recipe step 29: PutUserTimeOffAccountPolicySetSchedule (past + re-anchored tiers).
        put_rehire_policy_schedule = rail.RepliconServiceOperator(
            task_id='put_rehire_policy_schedule',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=request_payload.put_rehire_policy_schedule_payload
        )

        # Recipe step 27 (empty list): nothing to assign.
        log_no_policy = rail.PythonOperator(
            task_id='log_no_policy',
            python_callable=lambda: 'No policy entries built, hence nothing to assign'
        )

        # Recipe steps 31-32: leaf error reply (gathered by the parent on failure).
        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "Policy assignment rehire update days - Dag_Run Error - {{ get_error_message() }}")
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.result('catch_error') if rail.result('catch_error') else ""
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error >> final_response_from_dag
        can_run_batch_task >> rail.Label('No') >> get_user_policy_summary

        get_user_policy_summary >> get_past_rehire_entries >> get_default_policyset_schedule \
            >> build_rehire_policy_entries >> if_policy_entries_present

        if_policy_entries_present >> rail.Label('Yes') >> put_rehire_policy_schedule >> final_response_from_dag
        if_policy_entries_present >> rail.Label('No') >> log_no_policy >> final_response_from_dag

    return dag


rail.for_each_instance(create_dag)
