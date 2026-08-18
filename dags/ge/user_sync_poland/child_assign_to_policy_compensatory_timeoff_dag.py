from datetime import timedelta
from airflow.models import Variable
from ge.user_sync_poland.utils import custom_methods
import rail

null = None


def create_dag(config):
    # pylnot: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_assign_timeoff_policy_compensatory_timeoff_dag_id,
        description=f'GE POLAND User Import Assign Time Off Policy Compemdatory Timeoff Child',
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
            no_task='ge_poland_user_sync_master_mapper_search_entries_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='ge_poland_user_sync_master_mapper_search_entries_3',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        ge_poland_user_sync_master_mapper_search_entries_3 = rail.PythonOperator(
            task_id='ge_poland_user_sync_master_mapper_search_entries_3',
            python_callable=lambda dag_run:  next(iter(filter(
                lambda x: x["legal_entity"] == dag_run.conf['legal_entity'] and (
                    x["type"] == "PL_Compensatory time off/Odbiór nadgodzin"), config.POLAND_MASTER_MAPPER)), {}).get('value', '')
        )

        get_default_time_off_type_policy_schedule_for_user_5 = rail.RepliconServiceOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user_5',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
                }
            }
        )

        if_effectivedate_day_present_7 = rail.IfOperator(
            task_id='if_effectivedate_day_present_7',
            test=lambda: bool(rail.result('get_default_time_off_type_policy_schedule_for_user_5')[0]) and bool(rail.result(
                'get_default_time_off_type_policy_schedule_for_user_5')[0]['effectiveDate']),
            yes_task="log_final_timeoff_policy_line_to_put_8_19",
            no_task="catch_and_log_error",
        )

        log_final_timeoff_policy_line_to_put_8_19 = rail.PythonOperator(
            task_id='log_final_timeoff_policy_line_to_put_8_19',
            python_callable=lambda: custom_methods.get_final_timeoff_policy_line(rail.result(
                'get_default_time_off_type_policy_schedule_for_user_5'), rail.result(
                'ge_poland_user_sync_master_mapper_search_entries_3'))
        )

        put_user_time_off_account_policy_set_schedule_20 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_20',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('log_final_timeoff_policy_line_to_put_8_19')
            }
        )

        catch_and_log_error = rail.PythonOperator(
            task_id="catch_and_log_error",
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "Error in assigning prorated time off Compensatory time off : {{get_error_message()}}")
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.result(
                "catch_and_log_error") or "Success"
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label(
            'No') >> ge_poland_user_sync_master_mapper_search_entries_3

        ge_poland_user_sync_master_mapper_search_entries_3 >> get_default_time_off_type_policy_schedule_for_user_5 \
            >> if_effectivedate_day_present_7

        if_effectivedate_day_present_7 >> rail.Label(
            'No') >> catch_and_log_error
        if_effectivedate_day_present_7 >> rail.Label('Yes') >> log_final_timeoff_policy_line_to_put_8_19 \
            >> put_user_time_off_account_policy_set_schedule_20 >> catch_and_log_error

        catch_and_log_error >> final_response_from_dag

    return dag


rail.for_each_instance(create_dag)
