from datetime import timedelta


"""
Mirrors Workato recipe live_valleychildrens_child_for_timeoff_policy_update_payoutbalance_v1_0.

Single REST call: PutUserTimeOffAccountPolicySetSchedule with an end-only
policySetScheduleEntries — used to end the existing policy schedule on the
FTE-change effective date so accrued balance is paid out at the old FTE
before the new policy takes effect.

Payload shape (verified against Workato):
  {
    "timeOffAccount": {"userUri": "...", "timeOffTypeUri": "..."},
    "policySetScheduleEntries": [{effectiveDate, endDate, policySet:null, policyUri}]
  }
"""

from airflow.models import Variable
import rail

from valleychildrens.user_import.utils import request_payload

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_timeoff_policy_payoutbalance_dagid,
        description='ValleyChildrens User Import - Time Off Policy Payout Balance',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_timeoff_policy_payoutbalance,
    ) as dag:
        rail.ViewDagRunConfOperator(task_id='view_dagrun_conf')
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='end_existing_policy_schedule',
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='end_existing_policy_schedule',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        end_existing_policy_schedule = rail.RepliconServiceOperator(
            task_id='end_existing_policy_schedule',
            endpoint='/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule',
            data=lambda dag_run: {
                'timeOffAccount': {
                    'userUri': dag_run.conf['useruri'],
                    'timeOffTypeUri': dag_run.conf.get('timeofftypeuri') or dag_run.conf.get('timeoffuri'),
                },
                'policySetScheduleEntries': request_payload.build_policy_set_schedule_entries(
                    dag_run, None, end_only=True,
                ),
            },
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log='{{ dag_run.conf["log_id"] }}',
            severity='Error',
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                'user_uri': dag_run.conf.get('useruri'),
                'time_off_type_uri': dag_run.conf.get('timeofftypeuri') or dag_run.conf.get('timeoffuri'),
                'action': 'TimeoffPolicyPayoutBalance',
                'status': 'Error',
                'details': '{{ get_error_message() }}',
            },
        )
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> end_existing_policy_schedule
        end_existing_policy_schedule >> catch_and_log_error
    return dag

rail.for_each_instance(create_child_dag)

