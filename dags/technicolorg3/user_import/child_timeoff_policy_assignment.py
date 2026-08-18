from datetime import timedelta
import rail
from airflow.models import Variable

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/technicolorg3/user_import/config.py


def create_timeoff_policy_assignment_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'technicolorg3_user_import_child_timeoff_policy_assignment_{config.instance}',
        description=f'Technicolor_User Sync_Child_Timeoff Policy assignment {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_timeoff_assignment_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='default_user_timeoff_policy'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='default_user_timeoff_policy',
            end_task='finish',
        )

        default_user_timeoff_policy = rail.RepliconServiceOperator(
            task_id='default_user_timeoff_policy',
            endpoint='/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser',
            data={
                'timeOffAccount': {
                    'userUri': '{{ dag_run.conf.useruri }}',
                    'timeOffTypeUri': '{{ dag_run.conf.timeoff_type_uri }}'
                }
            }
        )

        should_add_timeoff_policy = rail.IfOperator(
            task_id='should_add_timeoff_policy',
            test="{{ result('default_user_timeoff_policy') | is_truthy }}",
            yes_task='add_timeoff_policy',
            no_task='finish'
        )

        add_timeoff_policy = rail.RepliconServiceOperator(
            task_id='add_timeoff_policy',
            endpoint='/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule',
            data=lambda dag_run: {
                'timeOffAccount': {
                    'userUri': dag_run.conf['useruri'],
                    'timeOffTypeUri': dag_run.conf['timeoff_type_uri']
                },
                'policySetScheduleEntries': rail.result('default_user_timeoff_policy')
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish

        can_run_batch_task >> rail.Label(
            'No') >> default_user_timeoff_policy

        default_user_timeoff_policy >> should_add_timeoff_policy

        should_add_timeoff_policy >> rail.Label(
            'Yes') >> add_timeoff_policy >> finish

        should_add_timeoff_policy >> rail.Label(
            'No') >> finish

        return dag


rail.for_each_instance(create_timeoff_policy_assignment_child_dag)
