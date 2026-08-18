import rail
from airflow.models import Variable
from mercury_systems_inc.user_import_v1.utils import custom_methods


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_existing_eligible_timeoff_types_for_rehire_users_dagid,
        description='MercurySystemsInc User Import Process Existing Eligible Timeoff Types For Rehire Users',
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
            no_task="get_default_policysets_from_global_level"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="get_default_policysets_from_global_level",
            end_task="catch_errors",
        )

        get_default_policysets_from_global_level = rail.RepliconServiceOperator(
            task_id='get_default_policysets_from_global_level',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ dag_run.conf.timeoff_uri_for_updating }}"
            },
            data_handler=lambda res: res or []
        )

        if_no_default_policyset = rail.IfOperator(
            task_id='if_no_default_policyset',
            test=lambda: not rail.result(
                'get_default_policysets_from_global_level'),
            yes_task="catch_errors",
            no_task="get_current_balance_for_timeoff"
        )

        get_current_balance_for_timeoff = rail.RepliconServiceOperator(
            task_id='get_current_balance_for_timeoff',
            endpoint="/services/TimeOffService2.svc/GetBalanceSummaryForAccount",
            data=lambda dag_run: {
                "account": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": dag_run.conf['timeoff_uri_for_updating']
                },
                "asOfDate": rail.parse_date(dag_run.conf['effective_date'], config.DATE_FORMAT)
            },
            data_handler=lambda res: res['timeRemaining'] if res else 0
        )

        log_relevant_historical_policies = rail.PythonOperator(
            task_id='log_relevant_historical_policies',
            python_callable=lambda dag_run: custom_methods.get_relevant_historical_policies(
                dag_run.conf['existing_policyset_schedule_for_timeoff'], dag_run.conf['effective_date'])
        )

        policyset_schedule_with_historical_policies = rail.PythonOperator(
            task_id='policyset_schedule_with_historical_policies',
            python_callable=lambda:  custom_methods.create_new_policyset_schedule_with_historical_policies(
                rail.result('log_relevant_historical_policies'))
        )

        get_new_policysets_to_consider_based_on_tenure_and_rehire_type = rail.PythonOperator(
            task_id='get_new_policysets_to_consider_based_on_tenure_and_rehire_type',
            python_callable=lambda dag_run: custom_methods.policysets_to_consider(
                rail.result('get_default_policysets_from_global_level'), dag_run.conf['tenure_of_user'], dag_run.conf['rehire_type'])
        )

        final_policyset_schedule_for_timeoff = rail.PythonOperator(
            task_id='final_policyset_schedule_for_timeoff',
            python_callable=lambda dag_run: custom_methods.get_rehire_final_policy_with_remaining_balance_policy_line(rail.result(
                'get_current_balance_for_timeoff'), dag_run.conf['new_hire_date'], rail.result(
                    'policyset_schedule_with_historical_policies'), dag_run.conf['effective_date'], config.DATE_FORMAT, rail.result(
                        'get_new_policysets_to_consider_based_on_tenure_and_rehire_type'), dag_run.conf['starting_balance_set_to_script_uri'], dag_run.conf['rehire_type'])
        )

        update_policy = rail.RepliconServiceOperator(
            task_id="update_policy",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": dag_run.conf['timeoff_uri_for_updating']
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
            "No") >> get_default_policysets_from_global_level >> if_no_default_policyset

        if_no_default_policyset >> rail.Label(
            "Yes") >> catch_errors
        if_no_default_policyset >> rail.Label(
            "No") >> get_current_balance_for_timeoff

        get_current_balance_for_timeoff >> log_relevant_historical_policies >> policyset_schedule_with_historical_policies

        policyset_schedule_with_historical_policies >> get_new_policysets_to_consider_based_on_tenure_and_rehire_type >>\
            final_policyset_schedule_for_timeoff >> update_policy >> catch_errors

    return dag


rail.for_each_instance(create_dag)
