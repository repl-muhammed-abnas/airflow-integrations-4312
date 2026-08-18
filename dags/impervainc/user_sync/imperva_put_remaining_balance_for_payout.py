from datetime import timedelta
from airflow.models import Variable
import rail
from impervainc.user_sync.utils import python_callable, request_payload

null=None

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.imperva_put_remaining_balance_for_payout,
        description=f'impervainc put remaining balance for payout child dag {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='false').lower() == 'true',
            yes_task='batch_task',
            no_task='get_existingpolicy_schedule_for_timeoff'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_existingpolicy_schedule_for_timeoff',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_existingpolicy_schedule_for_timeoff = rail.RepliconServiceOperator(
            task_id='get_existingpolicy_schedule_for_timeoff',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response['policiesByTimeOffType'], 'timeOffType.uri', dag_run.conf['timeoffuri'], 'policySetSchedule', '')
        )

        is_first_description_present = rail.IfOperator(
            task_id='is_first_description_present',
            test="{{ result('get_existingpolicy_schedule_for_timeoff') | first_or_default(default='') | \
                is_truthy and result('get_existingpolicy_schedule_for_timeoff') | first_or_default(default='') | \
                    attr_or_default('description') | is_truthy }}",
            yes_task="past_policyset_schedule",
            no_task="log_to_sumo"
        )

        past_policyset_schedule = rail.PythonOperator(
            task_id='past_policyset_schedule',
            python_callable=lambda dag_run: python_callable.construct_policyschedule(
                rail.result('get_existingpolicy_schedule_for_timeoff'),
                dag_run.conf['terminationdate'].split('T')[0]
            )
        )

        is_policyset_schedule_present = rail.IfOperator(
            task_id='is_policyset_schedule_present',
            test="{{ result('past_policyset_schedule') | is_truthy }}",
            yes_task="put_timeoff_account_policyset_schedule_18",
            no_task="put_timeoff_account_policyset_schedule_20"
        )

        put_timeoff_account_policyset_schedule_18 = rail.RepliconServiceOperator(
            task_id='put_timeoff_account_policyset_schedule_18',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: request_payload.get_put_timeoff_account_policyset_schedule_payload(
                dag_run, rail.result('past_policyset_schedule')
            )
        )

        put_timeoff_account_policyset_schedule_20 = rail.RepliconServiceOperator(
            task_id='put_timeoff_account_policyset_schedule_20',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: request_payload.get_put_timeoff_account_policyset_schedule_payload(
                dag_run, []
            )
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> get_existingpolicy_schedule_for_timeoff
        get_existingpolicy_schedule_for_timeoff >> is_first_description_present >> rail.Label("Yes") >> \
        past_policyset_schedule >> is_policyset_schedule_present
        is_first_description_present >> rail.Label("No") >> log_to_sumo
        is_policyset_schedule_present >> rail.Label("Yes") >> put_timeoff_account_policyset_schedule_18 >> log_to_sumo
        is_policyset_schedule_present >> rail.Label("No") >> put_timeoff_account_policyset_schedule_20 >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
