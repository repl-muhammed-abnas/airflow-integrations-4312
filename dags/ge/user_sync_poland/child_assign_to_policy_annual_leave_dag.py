from datetime import datetime, timedelta
from airflow.models import Variable
from ge.user_sync_poland.utils import custom_methods
import rail

null = None


def create_dag(config):
    # pylnot: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_assign_prorated_timeoff_policy_annual_leave_dag_id,
        description=f'GE POLAND User Import Assign Prorated TO Policy Annual Leave Child',
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
            python_callable=lambda dag_run:  list(filter(
                lambda x: x["legal_entity"] == dag_run.conf['legal_entity'] and x["type"] == "balance" and
                    x["identifier__1__(_legal_entity_code/_type/_timeoff_type)"] == str(12 - int(datetime.strptime(
                        dag_run.conf['startdate'], config.DATE_DEFAULT_FORMAT).strftime("%m")) + 1) and
                x["identifier__2__(_legal_entity_name/_start_date_month)"] == str(int(dag_run.conf['exp'])), config.POLAND_MASTER_MAPPER))
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
            yes_task="log_required_value_to_calculate_starting_balance_8",
            no_task="catch_and_log_error",
        )

        log_required_value_to_calculate_starting_balance_8 = rail.PythonOperator(
            task_id='log_required_value_to_calculate_starting_balance_8',
            python_callable=lambda: rail.result(
                'ge_poland_user_sync_master_mapper_search_entries_3')[0].get('value', '') if rail.result(
                'ge_poland_user_sync_master_mapper_search_entries_3') else ''
        )

        log_required_scripts_balances_from_user_timeoff_policy_schedule_9_30 = rail.PythonOperator(
            task_id='log_required_scripts_balances_from_user_timeoff_policy_schedule_9_30',
            python_callable=lambda dag_run: custom_methods.get_required_scripts_balances(
                rail.result('get_default_time_off_type_policy_schedule_for_user_5'), config.DATE_DEFAULT_FORMAT, dag_run)
        )

        if_type_downcase_equals_to_add_32 = rail.IfOperator(
            task_id='if_type_downcase_equals_to_add_32',
            test=lambda dag_run: dag_run.conf['type'].lower(
            ) == 'add' or dag_run.conf['overwrite_policy'].lower() == 'yes',
            yes_task="get_all_scripts_timeoffbalanceeventscripts",
            no_task="if_type_downcase_equals_to_update_74",
        )

        get_all_scripts_timeoffbalanceeventscripts = rail.RepliconServiceOperator(
            task_id='get_all_scripts_timeoffbalanceeventscripts',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda res: rail.find_first_by_attr_and_get_attr(
                res, 'displayText', 'Monthly Accrual', 'uri', '')
        )

        get_required_timeoff_policyset_add_scenario = rail.PythonOperator(
            task_id='get_required_timeoff_policyset_add_scenario',
            python_callable=lambda dag_run: custom_methods.get_timeoff_policy_to_assign(rail.result(
                'get_default_time_off_type_policy_schedule_for_user_5'), rail.result(
                    'log_required_scripts_balances_from_user_timeoff_policy_schedule_9_30'), rail.result(
                        'get_all_scripts_timeoffbalanceeventscripts'), config.DATE_DEFAULT_FORMAT, dag_run)
        )

        put_user_time_off_account_policy_set_schedule_73 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_73',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('get_required_timeoff_policyset_add_scenario')
            }
        )

        if_type_downcase_equals_to_update_74 = rail.IfOperator(
            task_id='if_type_downcase_equals_to_update_74',
            test=lambda dag_run: dag_run.conf['type'].lower(
            ) == 'update' or dag_run.conf['monthlyaccrual'].lower() == 'no',
            yes_task="get_user_time_off_policysetschedule_for_given_timeoff_type_75",
            no_task="catch_and_log_error",
        )

        get_user_time_off_policysetschedule_for_given_timeoff_type_75 = rail.RepliconServiceOperator(
            task_id='get_user_time_off_policysetschedule_for_given_timeoff_type_75',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri']
            },
            data_handler=lambda res, dag_run: rail.find_first_by_attr_and_get_attr(
                res['policiesByTimeOffType'], 'timeOffType.name', dag_run.conf['timeofftype'], 'policySetSchedule', '')
        )

        get_max_date_from_policy_lines_84 = rail.PythonOperator(
            task_id='get_max_date_from_policy_lines_84',
            python_callable=lambda: custom_methods.get_max_date_from_policy_line(rail.result(
                'get_user_time_off_policysetschedule_for_given_timeoff_type_75'), config.DATE_DEFAULT_FORMAT)
        )

        get_policy_lines_prior_to_last_line_90 = rail.PythonOperator(
            task_id='get_policy_lines_prior_to_last_line_90',
            python_callable=lambda: custom_methods.get_timeoff_policy_lines_prior_to_last_line(rail.result(
                'get_user_time_off_policysetschedule_for_given_timeoff_type_75'), rail.result(
                'get_max_date_from_policy_lines_84'), config.DATE_DEFAULT_FORMAT)
        )

        get_new_policy_lines_to_update_91_146 = rail.PythonOperator(
            task_id='get_new_policy_lines_to_update_91_146',
            python_callable=lambda dag_run: custom_methods.get_new_policy_lines_to_update(rail.result('get_policy_lines_prior_to_last_line_90'), rail.result(
                'get_default_time_off_type_policy_schedule_for_user_5'), rail.result(
                    'log_required_scripts_balances_from_user_timeoff_policy_schedule_9_30'), rail.result(
                        'get_max_date_from_policy_lines_84'),  config.POLAND_MASTER_MAPPER, config.DATE_DEFAULT_FORMAT, dag_run)
        )

        put_user_time_off_account_policy_set_schedule_148 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_148',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('get_new_policy_lines_to_update_91_146')
            }
        )

        catch_and_log_error = rail.PythonOperator(
            task_id="catch_and_log_error",
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "Error in assigning prorated time off policy annual leave : {{get_error_message()}}")
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

        ge_poland_user_sync_master_mapper_search_entries_3 >> get_default_time_off_type_policy_schedule_for_user_5 >> if_effectivedate_day_present_7

        if_effectivedate_day_present_7 >> rail.Label(
            'No') >> catch_and_log_error
        if_effectivedate_day_present_7 >> rail.Label(
            'Yes') >> log_required_value_to_calculate_starting_balance_8 >> log_required_scripts_balances_from_user_timeoff_policy_schedule_9_30 \
            >> if_type_downcase_equals_to_add_32

        if_type_downcase_equals_to_add_32 >> rail.Label(
            'No') >> if_type_downcase_equals_to_update_74
        if_type_downcase_equals_to_add_32 >> rail.Label(
            'Yes') >> get_all_scripts_timeoffbalanceeventscripts >> get_required_timeoff_policyset_add_scenario \
            >> put_user_time_off_account_policy_set_schedule_73 >> if_type_downcase_equals_to_update_74

        if_type_downcase_equals_to_update_74 >> rail.Label(
            'No') >> catch_and_log_error
        if_type_downcase_equals_to_update_74 >> rail.Label(
            'Yes') >> get_user_time_off_policysetschedule_for_given_timeoff_type_75 >> get_max_date_from_policy_lines_84 \
            >> get_policy_lines_prior_to_last_line_90 >> get_new_policy_lines_to_update_91_146 >> put_user_time_off_account_policy_set_schedule_148 \
            >> catch_and_log_error

        catch_and_log_error >> final_response_from_dag

    return dag


rail.for_each_instance(create_dag)
