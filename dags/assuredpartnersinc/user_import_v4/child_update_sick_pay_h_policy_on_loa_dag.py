from datetime import timedelta, datetime
import json
from airflow.models import Variable
from assuredpartnersinc.user_import_v4.utils import python_callable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_update_sick_pay_h_policy_on_loa_dag_id,
        description=f'Assured Partners User Import Update Sick Pay-H Policy on LOA Child{config.instance}',
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
            no_task='get_enabled_time_off_types_2'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_enabled_time_off_types_2',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_enabled_time_off_types_2 = rail.RepliconServiceOperator(
            task_id='get_enabled_time_off_types_2',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
        )

        get_defaultpolicyfromgloballevel_5 = rail.RepliconServiceOperator(
            task_id='get_defaultpolicyfromgloballevel_5',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
            }
        )

        get_user_time_off_type_policy_summary_6 = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary_6',
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
                'get_user_time_off_type_policy_summary_6'), python_callable.get_split_date(dag_run.conf['loastartdate'], 'int'))
        )

        log_add_historical_policies_to_policyset_list_26 = rail.PythonOperator(
            task_id='log_add_historical_policies_to_policyset_list_26',
            python_callable=lambda:  python_callable.add_historical_policies_to_policysets_list(
                rail.result('log_relevant_historical_policies'))
        )

        log_category_of_time_off_type_and_tenure_of_employee_27_29 = rail.PythonOperator(
            task_id='log_category_of_time_off_type_and_tenure_of_employee_27_29',
            python_callable=lambda dag_run:  {
                'category_of_time_off_type': (dag_run.conf['timeofftypename'].replace('-H', "").replace('-EX', "").replace('H', "").replace('EX', "")).strip(),
                'tenure_of_employee': abs((python_callable.get_split_date(
                    dag_run.conf['startdate'], 'no_split') - datetime.strptime(dag_run.conf['integration_run_date'], config.DATE_DEFAULT_FORMAT).date()).days) / 365
            }
        )

        def add_policy_line_max_balance_cap(max_balance_script_uri, policyset_list, dag_run):
            policyset_to_add_in_policyset_list = json.loads(json.dumps({"timeOffBalanceEventScripts": [{"additionalParameters": [{"keyUri": "urn:replicon:script-key:parameter:daily-maximum-balance-amount", "value": {"number": 80}}, {
                                                            "keyUri": "urn:replicon:script-key:parameter:precedence", "value": {"number": 10000.0}}], "scriptTarget": {"description": "Set maximum balance cap", "name": "Max Balance Limit", "uri": max_balance_script_uri}}], "timeOffValidationScripts": []}))
            policyset_list.append({
                'description': "Effective on - " + dag_run.conf['loastartdate'],
                'effectiveDate': python_callable.get_split_date(dag_run.conf['loastartdate'], 'int'),
                'policySet': policyset_to_add_in_policyset_list
            })
            return policyset_list

        log_final_policyset_list_33 = rail.PythonOperator(
            task_id='log_final_policyset_list_33',
            python_callable=lambda dag_run: add_policy_line_max_balance_cap(dag_run.conf['max_balance_limit_script_uri'], rail.result(
                "log_add_historical_policies_to_policyset_list_26"), dag_run),
        )

        if_final_policyset_list_contains_urn_34 = rail.IfOperator(
            task_id='if_final_policyset_list_contains_urn_34',
            test=lambda: 'urn' in json.dumps(
                rail.result('log_final_policyset_list_33')),
            yes_task="assign_time_offpolicy_35",
            no_task="catch_and_log_error",
        )

        assign_time_offpolicy_35 = rail.RepliconServiceOperator(
            task_id='assign_time_offpolicy_35',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('log_final_policyset_list_33')
            }
        )

        catch_and_log_error = rail.PythonOperator(
            task_id="catch_and_log_error",
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "Error in Update Sick Pay-H on LOA : {{get_error_message()}}")
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
            'No') >> get_enabled_time_off_types_2

        get_enabled_time_off_types_2 >> get_defaultpolicyfromgloballevel_5 >> get_user_time_off_type_policy_summary_6 >> log_relevant_historical_policies \
            >> log_add_historical_policies_to_policyset_list_26 >> log_category_of_time_off_type_and_tenure_of_employee_27_29 \
            >> log_final_policyset_list_33 >> if_final_policyset_list_contains_urn_34

        if_final_policyset_list_contains_urn_34 >> rail.Label(
            'No') >> catch_and_log_error
        if_final_policyset_list_contains_urn_34 >> rail.Label(
            'Yes') >> assign_time_offpolicy_35 >> catch_and_log_error

        catch_and_log_error >> final_response_from_dag

    return dag


rail.for_each_instance(create_dag)
