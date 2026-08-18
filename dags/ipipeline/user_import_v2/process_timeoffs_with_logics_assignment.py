from ipipeline.user_import_v2.utils import custom_methods, request_payload
from airflow.models import Variable
import json
import rail

null = None


def create_timeoff_with_logic_assignment_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.timeoff_with_logic_assignment_dag_id,
        description=f"iPipeline User Import Timeoff With Logic Assignment {config.instance}",
        company_key=config.company_key,
        max_active_runs=config.timeoff_with_logic_assignment_max_active_runs,
        replicon_conn_id=config.replicon_conn_id,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var="true").lower() == "true",
            yes_task="batch_task",
            no_task="get_matching_entry_from_accrual_mapper"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="get_matching_entry_from_accrual_mapper",
            end_task="catch_and_log_error"
        )

        get_matching_entry_from_accrual_mapper = rail.PythonOperator(
            task_id="get_matching_entry_from_accrual_mapper",
            python_callable=lambda dag_run: custom_methods.get_accrual_details_from_config(
                dag_run, dag_run.conf.get("seniority_level"), config.timeoff_accrual_mapper_data)
        )

        if_matching_entry_found = rail.IfOperator(
            task_id="if_matching_entry_found",
            test=lambda dag_run: rail.result('get_matching_entry_from_accrual_mapper') or dag_run.conf.get(
                'timeoff_reference_logic_type') == 'Type1-A1',
            yes_task="if_action_update",
            no_task="catch_and_log_error"
        )

        if_action_update = rail.IfOperator(
            task_id="if_action_update",
            test=lambda dag_run: dag_run.conf.get("action") == "Update",
            yes_task="is_only_seniority_level_changed",
            no_task="get_final_timeoff_policyset_schedule"
        )

        # Since Type1-A1 logic is dependant on Seniority level, so even if is_only_seniority_level_changed is True, we need to move further with logic
        is_only_seniority_level_changed = rail.IfOperator(
            task_id="is_only_seniority_level_changed",
            test=lambda dag_run: dag_run.conf.get(
                "is_only_seniority_level_changed", False) and not dag_run.conf.get(
                    'timeoff_reference_logic_type') == 'Type1-A1',
            yes_task="check_if_accrual_data_changed",
            no_task="get_relevant_historical_timeoff_policy_lines"
        )

        check_if_accrual_data_changed = rail.IfOperator(
            task_id="check_if_accrual_data_changed",
            test=lambda dag_run: custom_methods.get_accrual_details_from_config(
                dag_run, dag_run.conf.get("seniority_level"), config.timeoff_accrual_mapper_data) != custom_methods.get_accrual_details_from_config(
                dag_run, dag_run.conf.get("previous_seniority_level"), config.timeoff_accrual_mapper_data),
            yes_task="get_relevant_historical_timeoff_policy_lines",
            no_task="catch_and_log_error"
        )

        get_relevant_historical_timeoff_policy_lines = rail.PythonOperator(
            task_id="get_relevant_historical_timeoff_policy_lines",
            python_callable=lambda dag_run: custom_methods.get_relevant_historical_policies(
                dag_run.conf.get("existing_timeoff_policyset_schedule_for_timeoff"), dag_run.conf.get("current_date"), config.YMD_DATE_FORMAT)
        )

        if_timeoff_reference_logic_type_2_or_3 = rail.IfOperator(
            task_id="if_timeoff_reference_logic_type_2_or_3",
            test=lambda dag_run: dag_run.conf.get(
                "timeoff_reference_logic_type") in ["Type2-A", "Type2-B", "Type3"],
            yes_task="get_current_balance_for_timeoff",
            no_task="get_final_timeoff_policyset_schedule"
        )

        get_current_balance_for_timeoff = rail.RepliconServiceOperator(
            task_id='get_current_balance_for_timeoff',
            endpoint="/services/TimeOffService2.svc/GetBalanceSummaryForAccount",
            data=lambda dag_run: {
                "account": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": dag_run.conf['timeoff_uri']
                },
                "asOfDate": rail.parse_date(dag_run.conf.get("current_date"), config.YMD_DATE_FORMAT)
            },
            data_handler=lambda res: res['timeRemaining'] if res else 0
        )

        get_final_timeoff_policyset_schedule = rail.PythonOperator(
            task_id="get_final_timeoff_policyset_schedule",
            python_callable=lambda dag_run: custom_methods.get_modified_policyset_schedule(
                dag_run, config.REP_DATE_FORMAT, config.YMD_DATE_FORMAT)
        )

        put_policyset_schedule = rail.RepliconServiceOperator(
            task_id='put_policyset_schedule',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": dag_run.conf['timeoff_uri']
                },
                "policySetScheduleEntries": rail.result('get_final_timeoff_policyset_schedule')
            }
        )

        catch_and_log_error = rail.PythonOperator(
            task_id="catch_and_log_error",
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "Error in Timeoff Assignment - {{dag_run.conf.timeoff_type_name}}")
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.result('catch_and_log_error') if rail.result(
                'catch_and_log_error') else "Success"
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label(
            "No") >> get_matching_entry_from_accrual_mapper

        get_matching_entry_from_accrual_mapper >> if_matching_entry_found

        if_matching_entry_found >> rail.Label("No") >> catch_and_log_error
        if_matching_entry_found >> rail.Label("Yes") >> if_action_update

        if_action_update >> rail.Label(
            "No") >> get_final_timeoff_policyset_schedule
        if_action_update >> rail.Label(
            "Yes") >> is_only_seniority_level_changed

        is_only_seniority_level_changed >> rail.Label(
            "No") >> get_relevant_historical_timeoff_policy_lines
        is_only_seniority_level_changed >> rail.Label(
            "Yes") >> check_if_accrual_data_changed

        check_if_accrual_data_changed >> rail.Label(
            "No") >> catch_and_log_error
        check_if_accrual_data_changed >> rail.Label(
            "Yes") >> get_relevant_historical_timeoff_policy_lines

        get_relevant_historical_timeoff_policy_lines >> if_timeoff_reference_logic_type_2_or_3

        if_timeoff_reference_logic_type_2_or_3 >> rail.Label(
            "No") >> get_current_balance_for_timeoff >> get_final_timeoff_policyset_schedule
        if_timeoff_reference_logic_type_2_or_3 >> rail.Label(
            "Yes") >> get_final_timeoff_policyset_schedule

        get_final_timeoff_policyset_schedule >> put_policyset_schedule >> catch_and_log_error

        catch_and_log_error >> final_response_from_dag

    return dag


rail.for_each_instance(create_timeoff_with_logic_assignment_dag)
