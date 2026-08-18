from datetime import timedelta, datetime
import json
from airflow.models import Variable
import rail

from fujifilmdbtl.user_import.utils import python_callable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'fujifilmdbtl_child_for_timeoff_policy_update_on_each_time_off_type_for_no_accrual_{config.instance}',
        description=f'FDT_Child for timeoff policy update on each time off type for no accrual {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_all_scripts_time_off_validation_script_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_all_scripts_time_off_validation_script_3',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_all_scripts_time_off_validation_script_3 = rail.RepliconServiceOperator(
            task_id='get_all_scripts_time_off_validation_script_3',
            endpoint="/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts"
        )

        log_get_script_urifor_prevent_balance_overdraw_4 = rail.PythonOperator(
            task_id='log_get_script_urifor_prevent_balance_overdraw_4',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_scripts_time_off_validation_script_3'), 'displayText', "Prevent balance overdraw", 'uri', "")
        )

        get_all_scripts_time_off_balance_event_script_5 = rail.RepliconServiceOperator(
            task_id='get_all_scripts_time_off_balance_event_script_5',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts",
        )

        log_get_script_urifor_initial_balance_6 = rail.PythonOperator(
            task_id='log_get_script_urifor_initial_balance_6',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_scripts_time_off_balance_event_script_5'), 'displayText', "Starting Balance Set To", 'uri', "")
        )

        log_relevant_historical_policies = rail.PythonOperator(
            task_id='log_relevant_historical_policies',
            python_callable=lambda dag_run: python_callable.get_relevant_historical_policies(
                json.loads(dag_run.conf['policyset']), dag_run.conf['enddate'])
        )

        new_policyset_schedule_with_historical_policies = rail.PythonOperator(
            task_id='new_policyset_schedule_with_historical_policies',
            python_callable=lambda:  python_callable.create_new_policyset_schedule_with_historical_policies(
                rail.result('log_relevant_historical_policies'))
        )

        final_policyset_schedule_for_timeoff = rail.PythonOperator(
            task_id='final_policyset_schedule_for_timeoff',
            python_callable=lambda dag_run: python_callable.get_final_policy_with_remaining_balance_policy_line(dag_run.conf['newschedulebalance'], rail.result(
                    'new_policyset_schedule_with_historical_policies'), dag_run.conf['enddate'], "%m/%d/%Y", rail.result(
                        'log_get_script_urifor_initial_balance_6'), rail.result('log_get_script_urifor_prevent_balance_overdraw_4'))
        )

        put_user_time_off_account_policy_set_schedule_23 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_23',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('final_policyset_schedule_for_timeoff')
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> get_all_scripts_time_off_validation_script_3 \
            >> log_get_script_urifor_prevent_balance_overdraw_4 \
            >> get_all_scripts_time_off_balance_event_script_5 >> log_get_script_urifor_initial_balance_6 \
            >> log_relevant_historical_policies \
            >> new_policyset_schedule_with_historical_policies >> final_policyset_schedule_for_timeoff \
            >> put_user_time_off_account_policy_set_schedule_23 >> finish

    return dag


rail.for_each_instance(create_dag)
