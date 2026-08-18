from datetime import timedelta

"""
Mirrors Workato recipe live_valleychildrens_time_off_policy_add_pto_v2_0.

API payload shapes verified against the working Workato payloads:
  - GetDefaultTimeOffPolicySetScheduleForTimeOffType: {timeOffTypeUri}
  - GetUserTimeOffTypePolicySummary: {userUri}
  - GetAllScripts: {}
  - PutUserTimeOffAccountPolicySetSchedule: {timeOffAccount: {userUri, timeOffTypeUri}, policySetScheduleEntries: [...]}
"""
from airflow.models import Variable
import rail

from valleychildrens.user_import.utils import request_payload

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_time_off_policy_add_pto_dagid,
        description='ValleyChildrens User Import - Add PTO Time Off Policy',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_time_off_policy_add_pto,
    ) as dag:
        rail.ViewDagRunConfOperator(task_id='view_dagrun_conf')
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_default_policy_schedule',
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_default_policy_schedule',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_default_policy_schedule = rail.RepliconServiceOperator(
            task_id='get_default_policy_schedule',
            endpoint='/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType',
            data=lambda dag_run: {
                'timeOffTypeUri': dag_run.conf.get('timeofftypeuri') or dag_run.conf.get('timeoffuri'),
            },
        )

        get_user_timeoff_summary = rail.RepliconServiceOperator(
            task_id='get_user_timeoff_summary',
            endpoint='/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
            },
        )

        get_balance_event_scripts = rail.RepliconServiceOperator(
            task_id='get_balance_event_scripts',
            endpoint='/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts',
            data={},
        )

        put_time_off_account_policy = rail.RepliconServiceOperator(
            task_id='put_time_off_account_policy',
            endpoint='/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule',
            data=lambda dag_run: {
                'timeOffAccount': {
                    'userUri': dag_run.conf['useruri'],
                    'timeOffTypeUri': dag_run.conf.get('timeofftypeuri') or dag_run.conf.get('timeoffuri'),
                },
                'policySetScheduleEntries': request_payload.build_policy_set_schedule_entries(
                    dag_run,
                    request_payload.resolve_time_off_policy_uri(
                        dag_run,
                        rail.result('get_default_policy_schedule'),
                        rail.result('get_user_timeoff_summary'),
                    ),
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
                'action': 'AddPTOTimeOffPolicy',
                'status': 'Error',
                'details': '{{ get_error_message() }}',
            },
        )
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> get_default_policy_schedule
        get_default_policy_schedule >> get_user_timeoff_summary >> get_balance_event_scripts \
            >> put_time_off_account_policy >> catch_and_log_error
    return dag

rail.for_each_instance(create_child_dag)
