import rail
from airflow.models import Variable
from mercury_systems_inc.user_import.utils import custom_methods


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_stop_accrual_for_timeoff,
        description='MercurySystemsInc User Import Process Stop Accrual For Timeoff',
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        max_active_runs=config.max_active_runs_stop_accrual_for_timeoff
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task="get_current_balance_for_timeoff"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="get_current_balance_for_timeoff",
            end_task="catch_errors"
        )

        get_current_balance_for_timeoff = rail.RepliconServiceOperator(
            task_id='get_current_balance_for_timeoff',
            endpoint="/services/TimeOffService2.svc/GetBalanceSummaryForAccount",
            data=lambda dag_run: {
                "account": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": dag_run.conf['timeoff_uri_for_stopping_accrual']
                },
                "asOfDate": rail.parse_date(dag_run.conf['effective_date'], config.DATE_FORMAT)
            },
            data_handler=lambda res: res['timeRemaining'] if res else 0
        )

        if_existing_policies_blank_and_0_balance = rail.IfOperator(
            task_id='if_current_policies_blank_and_0_balance',
            test=lambda dag_run: not (dag_run.conf['existing_policyset_schedule_for_timeoff']) and float(rail.result(
                'get_current_balance_for_timeoff')) == 0.0,
            yes_task='catch_errors',
            no_task='log_relevant_historical_policies'
        )

        log_relevant_historical_policies = rail.PythonOperator(
            task_id='log_relevant_historical_policies',
            python_callable=lambda dag_run: custom_methods.get_relevant_historical_policies(
                dag_run.conf['existing_policyset_schedule_for_timeoff'], dag_run.conf['effective_date'])
        )

        new_policyset_schedule_with_historical_policies = rail.PythonOperator(
            task_id='new_policyset_schedule_with_historical_policies',
            python_callable=lambda:  custom_methods.create_new_policyset_schedule_with_historical_policies(
                rail.result('log_relevant_historical_policies'))
        )

        final_policyset_schedule_for_timeoff = rail.PythonOperator(
            task_id='final_policyset_schedule_for_timeoff',
            python_callable=lambda dag_run: custom_methods.get_final_policy_with_remaining_balance_policy_line(rail.result(
                'get_current_balance_for_timeoff'), rail.result(
                    'new_policyset_schedule_with_historical_policies'), dag_run.conf['effective_date'], config.DATE_FORMAT,
                dag_run.conf['starting_balance_set_to_script_uri'], dag_run.conf['prevent_balance_overdraw_script_uri'])
        )

        update_policy = rail.RepliconServiceOperator(
            task_id="update_policy",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": dag_run.conf['timeoff_uri_for_stopping_accrual']
                },
                "policySetScheduleEntries": rail.result('final_policyset_schedule_for_timeoff')
            }
        )

        catch_errors = rail.PythonOperator(
            task_id="catch_errors",
            trigger_rule="one_failed",
            python_callable=lambda: rail.render_template(
                "{{ get_error_message() }}")
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_errors
        can_run_batch_task >> rail.Label(
            "No") >> get_current_balance_for_timeoff >> if_existing_policies_blank_and_0_balance

        if_existing_policies_blank_and_0_balance >> rail.Label(
            "Yes") >> catch_errors
        if_existing_policies_blank_and_0_balance >> rail.Label(
            "No") >> log_relevant_historical_policies

        log_relevant_historical_policies >> new_policyset_schedule_with_historical_policies >>\
            final_policyset_schedule_for_timeoff >> update_policy >> catch_errors

    return dag


rail.for_each_instance(create_dag)
