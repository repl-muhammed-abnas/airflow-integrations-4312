from datetime import timedelta
from airflow.models import Variable
import rail

from guidehouse.workday_user_import.utils import custom_method

null = None


def create_dag(config):
    """
    Child DAG: zero out one time-off policy balance per triggered run.

    Triggered by TriggerDagRunForEachItemOperator in process_update_users.
    Handles both termination (set balance to 0 at end_date) and non-eligible
    (set balance to 0 at change_effective_date) scenarios.

    Expected conf keys:
        useruri                     - Replicon user URI
        timeoffuri                  - Time-off type URI to zero
        effective_date              - Date for the zero-balance entry (MM/DD/YYYY)
        policyset                   - Existing policy schedule entries (already filtered)
        user_log                    - User log reference for error logging
        starting_balance_script_uri - URI of 'Starting Balance Set To' script (from master)
    """
    with rail.create_airflow_dag(
        dag_id=config.process_zero_timeoff_policies,
        description='Guidehouse Workday User Import - Zero Time-Off Policy Balance',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_zero_timeoff_policies,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id='view_dagrun_config')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='build_zero_balance_policyset'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            start_task='build_zero_balance_policyset',
            end_task='catch_and_log_errors',
        )

        build_zero_balance_policyset = rail.PythonOperator(
            task_id='build_zero_balance_policyset',
            python_callable=lambda dag_run: custom_method.get_zero_balance_policyset(
                dag_run, dag_run.conf['policyset']
            )
        )

        put_user_time_off_account_policy_set_schedule = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('build_zero_balance_policyset')
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf.user_log }}',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                "lastname": "{{dag_run.conf.last_name}}",
                "firstname": "{{dag_run.conf.first_name}}",
                "loginname": "{{dag_run.conf.login_name}}",
                "employeeid": "{{dag_run.conf.employee_id}}",
                'action': 'Zero Timeoff Balance',
                'status': 'Error',
                'details': '{{ get_error_message() }}',
            }
        )

        # Wiring
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> build_zero_balance_policyset
        build_zero_balance_policyset >> put_user_time_off_account_policy_set_schedule >> catch_and_log_errors

    return dag


rail.for_each_instance(create_dag)