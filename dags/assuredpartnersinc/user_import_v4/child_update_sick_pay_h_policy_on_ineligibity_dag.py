from datetime import timedelta, datetime
import json
from airflow.models import Variable
from assuredpartnersinc.user_import_v4.utils import python_callable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_update_sick_pay_h_policy_on_ineligibity_dag_id,
        description=f'Assured Partners User Import Update Sick Pay-H Policy on Ineligibility Child{config.instance}',
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
            no_task='get_required_time_off_uri_sick_pay_h_ineligible_2'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_required_time_off_uri_sick_pay_h_ineligible_2',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_required_time_off_uri_sick_pay_h_ineligible_2 = rail.RepliconServiceOperator(
            task_id='get_required_time_off_uri_sick_pay_h_ineligible_2',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'name', "Sick Pay-H Ineligible", 'uri')
        )

        get_defaultpolicyfromgloballevel_6 = rail.RepliconServiceOperator(
            task_id='get_defaultpolicyfromgloballevel_6',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ result('get_required_time_off_uri_sick_pay_h_ineligible_2') }}"
            }
        )

        get_user_time_off_type_policy_summary_7 = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary_7',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response['policiesByTimeOffType'], 'timeOffType.uri', dag_run.conf['timeoffuri'], 'policySetSchedule', '')
        )

        log_relevant_historical_policies = rail.PythonOperator(
            task_id='log_relevant_historical_policies',
            python_callable=lambda dag_run: python_callable.get_relevant_historical_policies(rail.result(
                'get_user_time_off_type_policy_summary_7'), python_callable.get_split_date(dag_run.conf['integration_run_date'], 'int'))
        )

        def add_historical_policies_to_policysets_list(relevant_historical_policies):
            policyset_list = []
            if "urn" in json.dumps(relevant_historical_policies):
                for item in relevant_historical_policies:
                    policyset_list.append({
                        'description': item['description'],
                        'effectiveDate': item['effectiveDate'],
                        'policySet': item['policySet']
                    })
            return policyset_list

        log_add_historical_policies_to_policyset_list_27 = rail.PythonOperator(
            task_id='log_add_historical_policies_to_policyset_list_27',
            python_callable=lambda:  add_historical_policies_to_policysets_list(
                rail.result('log_relevant_historical_policies'))
        )

        log_category_of_time_off_type_and_tenure_of_employee_28_30 = rail.PythonOperator(
            task_id='log_category_of_time_off_type_and_tenure_of_employee_28_30',
            python_callable=lambda dag_run:  {
                'category_of_time_off_type': (dag_run.conf['timeofftypename'].replace('-H', "").replace('-EX', "").replace('H', "").replace('EX', "")).strip(),
                'tenure_of_employee': abs((python_callable.get_split_date(
                    dag_run.conf['startdate'], 'no_split') - datetime.strptime(dag_run.conf['integration_run_date'], config.DATE_DEFAULT_FORMAT).date()).days) / 365
            }
        )

        log_starting_balance_script_from_timeoffbalanceeventscripts = rail.RepliconServiceOperator(
            task_id='log_starting_balance_script_from_timeoffbalanceeventscripts',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Starting Balance Set To', 'uri')
        )

        def final_policy_set_to_assign(starting_balance_script_uri, policyset_list, dag_run):
            policyset_set_for_0_balance = json.loads(json.dumps({"timeOffBalanceEventScripts": [{"scriptTarget": {"uri": starting_balance_script_uri}, "additionalParameters": [
                {"keyUri": "urn:replicon:script-key:parameter:amount", "value": {"number": "0"}}, {"keyUri": "urn:replicon:script-key:parameter:precedence", "value": {"number": "20"}}]}], "timeOffValidationScripts": []}))
            policyset_list.append({
                'description': "Effective on - " + dag_run.conf['integration_run_date'],
                'effectiveDate': python_callable.get_split_date(dag_run.conf['integration_run_date'], 'int'),
                'policySet': policyset_set_for_0_balance
            })
            return policyset_list

        get_final_policy_set_to_assign = rail.PythonOperator(
            task_id='get_final_policy_set_to_assign',
            python_callable=lambda dag_run: final_policy_set_to_assign(rail.result("log_starting_balance_script_from_timeoffbalanceeventscripts"), rail.result(
                "log_add_historical_policies_to_policyset_list_27"), dag_run)
        )

        if_to_s_contains_urn_35 = rail.IfOperator(
            task_id='if_to_s_contains_urn_35',
            test=lambda: 'urn' in json.dumps(
                rail.result('get_final_policy_set_to_assign')),
            yes_task="assign_time_offpolicy_36",
            no_task="catch_and_log_error",
        )

        assign_time_offpolicy_36 = rail.RepliconServiceOperator(
            task_id='assign_time_offpolicy_36',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('get_final_policy_set_to_assign')
            }
        )

        catch_and_log_error = rail.PythonOperator(
            task_id="catch_and_log_error",
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "Error in Update sick pay-h policy on ineligibility : {{get_error_message()}}")
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
            'No') >> get_required_time_off_uri_sick_pay_h_ineligible_2

        get_required_time_off_uri_sick_pay_h_ineligible_2 >> get_defaultpolicyfromgloballevel_6 >> get_user_time_off_type_policy_summary_7 >> log_relevant_historical_policies >> log_add_historical_policies_to_policyset_list_27 >> log_category_of_time_off_type_and_tenure_of_employee_28_30 >> log_starting_balance_script_from_timeoffbalanceeventscripts >> get_final_policy_set_to_assign >> if_to_s_contains_urn_35

        if_to_s_contains_urn_35 >> rail.Label('No') >> catch_and_log_error
        if_to_s_contains_urn_35 >> rail.Label(
            'Yes') >> assign_time_offpolicy_36 >> catch_and_log_error

        catch_and_log_error >> final_response_from_dag

    return dag


rail.for_each_instance(create_dag)
