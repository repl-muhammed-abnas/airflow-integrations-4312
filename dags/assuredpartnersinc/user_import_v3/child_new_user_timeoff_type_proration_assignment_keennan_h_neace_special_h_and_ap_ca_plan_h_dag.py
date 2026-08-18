from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
import json
from airflow.models import Variable
import rail
from assuredpartnersinc.user_import_v3.utils import python_callable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_new_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_dag_id,
        description=f'Assured Partners User Import new user Timeoff Type proration assignment - Keennan-H, Neace-Special-H and AP CA Plan-H AP CO PLAN Child {config.instance}',
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
            no_task='combined_initial_tasks'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='combined_initial_tasks',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        combined_initial_tasks = rail.PythonOperator(
            task_id='combined_initial_tasks',
            python_callable=lambda dag_run: python_callable.timeoff_proration_assignment_initial_tasks(
                dag_run, config)
        )

        log_hoursday_5 = rail.PythonOperator(
            task_id='log_hoursday_5',
            python_callable=lambda dag_run:  float(dag_run.conf['weekly_scheduled_hours']) / float(
                rail.result('combined_initial_tasks')['number_of_working_days_in_week'])
        )

        get_defaultpolicyfromgloballevel_11 = rail.RepliconServiceOperator(
            task_id='get_defaultpolicyfromgloballevel_11',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
            }
        )

        if_log_hoursday_5_present_16 = rail.IfOperator(
            task_id='if_log_hoursday_5_present_16',
            test='''{{ result('log_hoursday_5') | is_truthy }}''',
            yes_task="get_policies_to_be_assigned_and_max_min_offsets",
            no_task="catch_and_log_error",
        )

        get_policies_to_be_assigned_and_max_min_offsets = rail.PythonOperator(
            task_id='get_policies_to_be_assigned_and_max_min_offsets',
            python_callable=lambda dag_run: python_callable.policies_to_be_assigned(
                rail.result('get_defaultpolicyfromgloballevel_11'), dag_run)
        )

        def get_policy_set_list_keenan_h_ca_plan_h_1(mapper_search_entries, hours_per_day, policies_to_be_assigned_and_max_min_offsets, default_timeoff_policy_set_schedule, dag_run):
            policy_set = []
            for entry in policies_to_be_assigned_and_max_min_offsets['policies_to_be_assigned_2']:
                if str(entry['offset']) == str(policies_to_be_assigned_and_max_min_offsets['max_offset_from_policies_2']):
                    entitlement_derived_in_hours = float(list(filter(
                        lambda x: x['offset'] == str(entry['offset']), mapper_search_entries))[0]['entitlement']) * hours_per_day
                    accrual_annual_amount_from_default_policy = python_callable.get_required_value_from_policy_set_schedule(
                        default_timeoff_policy_set_schedule, entry['offset'], 'Accrues time once per month.', 'urn:replicon:script-key:parameter:accrual-annual-amount')
                    default_accrual_annual_amount_script = json.dumps(
                        {"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": accrual_annual_amount_from_default_policy}})
                    new_accrual_annual_amount_script = json.dumps(
                        {"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": entitlement_derived_in_hours}})

                    new_max_balance = float(list(filter(lambda x: x['offset'] == str(entry['offset']), mapper_search_entries))[
                        0]['carryover']) * hours_per_day
                    existing_max_balance_from_default_policy = python_callable.get_required_value_from_policy_set_schedule(
                        default_timeoff_policy_set_schedule, entry['offset'], 'Set maximum balance cap', 'urn:replicon:script-key:parameter:daily-maximum-balance-amount')
                    default_gsub_value_for_max_value = json.dumps({"keyUri": "urn:replicon:script-key:parameter:daily-maximum-balance-amount", "value": {
                                                                  "number": existing_max_balance_from_default_policy}})
                    new_max_balance_gsub = json.dumps({"keyUri": "urn:replicon:script-key:parameter:daily-maximum-balance-amount", "value": {
                                                      "number": new_max_balance}})

                    complete_policyset_based_on_offset = json.loads(json.dumps(entry['policyset'], ensure_ascii=False).replace(
                        default_accrual_annual_amount_script, new_accrual_annual_amount_script).replace(default_gsub_value_for_max_value, new_max_balance_gsub).replace(
                        '"null"', '"effective"').replace('"script"', '"scriptTarget"'))
                    policy_set.append({
                        "effectiveDate": python_callable.get_split_date(dag_run.conf['startdate'], 'int'),
                        "policySet": complete_policyset_based_on_offset,
                        "description": "Effective on - " + dag_run.conf['startdate']
                    })
            return policy_set

        policy_set_list_part_1 = rail.PythonOperator(
            task_id='policy_set_list_part_1',
            python_callable=lambda dag_run: get_policy_set_list_keenan_h_ca_plan_h_1(rail.result(
                "combined_initial_tasks")['time_off_policy_mapper_search_entries'], rail.result(
                "log_hoursday_5"), rail.result('get_policies_to_be_assigned_and_max_min_offsets'), rail.result('get_defaultpolicyfromgloballevel_11'), dag_run)
        )

        def get_final_policy_set_list_keenan_h_ca_plan_h(final_policy_set_list, mapper_search_entries, hours_per_day, policies_to_be_assigned_and_max_min_offsets, default_timeoff_policy_set_schedule, dag_run):
            for entry in policies_to_be_assigned_and_max_min_offsets['policies_to_be_assigned_1']:
                entitlement_derived_in_hours = float(list(filter(
                    lambda x: x['offset'] == str(entry['offset']), mapper_search_entries))[0]['entitlement']) * hours_per_day
                accrual_annual_amount_from_default_policy = python_callable.get_required_value_from_policy_set_schedule(
                    default_timeoff_policy_set_schedule, entry['offset'], 'Accrues time once per month.', 'urn:replicon:script-key:parameter:accrual-annual-amount')
                default_accrual_annual_amount_script = json.dumps(
                    {"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": accrual_annual_amount_from_default_policy}})
                new_accrual_annual_amount_script = json.dumps(
                    {"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": entitlement_derived_in_hours}})

                gsub_to_get_rid_of_starting_balance = python_callable.get_timeoffbalanceeventscript_to_gsub(
                    default_timeoff_policy_set_schedule, entry['offset'], 'Set initial balance for the first day of a policy')

                new_max_balance = float(list(filter(lambda x: x['offset'] == str(entry['offset']), mapper_search_entries))[
                                        0]['carryover']) * hours_per_day
                existing_max_balance_from_default_policy = python_callable.get_required_value_from_policy_set_schedule(
                    default_timeoff_policy_set_schedule, entry['offset'], 'Set maximum balance cap', 'urn:replicon:script-key:parameter:daily-maximum-balance-amount')
                default_gsub_value_for_max_value = json.dumps({"keyUri": "urn:replicon:script-key:parameter:daily-maximum-balance-amount", "value": {
                    "number": existing_max_balance_from_default_policy}})
                new_max_balance_gsub = json.dumps({"keyUri": "urn:replicon:script-key:parameter:daily-maximum-balance-amount", "value": {
                    "number": new_max_balance}})

                complete_policyset_based_on_offset = json.loads(json.dumps(entry['policyset'], ensure_ascii=False).replace(default_accrual_annual_amount_script, new_accrual_annual_amount_script).replace(
                    default_gsub_value_for_max_value, new_max_balance_gsub).replace(gsub_to_get_rid_of_starting_balance, "").replace(", ]", "]").replace("[,", "[").replace(", ,", ",").replace('"null"', '"effective"').replace(
                    '"script"', '"scriptTarget"'))

                pto_policy_effective_date_with_offset_in_months = datetime.strptime(
                    dag_run.conf['PTOSeniorityDate'], config.DATE_DEFAULT_FORMAT) + relativedelta(months=int(entry['offset'])) if dag_run.conf['PTOSeniorityDate'] else datetime.strptime(
                    dag_run.conf['servicedate'], config.DATE_DEFAULT_FORMAT) + relativedelta(months=int(entry['offset']))
                pto_policy_effective_date_with_offset = datetime.strptime(
                    dag_run.conf['PTOSeniorityDate'], config.DATE_DEFAULT_FORMAT) + relativedelta(months=int(entry['offset'])*12) if dag_run.conf['PTOSeniorityDate'] else datetime.strptime(
                    dag_run.conf['servicedate'], config.DATE_DEFAULT_FORMAT) + relativedelta(months=int(entry['offset'])*12)

                if str(entry['offset']) == str(policies_to_be_assigned_and_max_min_offsets['min_offset_from_policies_1']):
                    if bool(policies_to_be_assigned_and_max_min_offsets['policies_to_be_assigned_2']):
                        if entry['offsetunituri'] == 'urn:replicon:time-off-policy-offset-unit:months':
                            final_policy_set_list.append({
                                "description": "Effective on - " + datetime.strftime(pto_policy_effective_date_with_offset_in_months, config.DATE_DEFAULT_FORMAT),
                                "effectiveDate": python_callable.get_split_date(pto_policy_effective_date_with_offset_in_months, 'int'),
                                "policySet": complete_policyset_based_on_offset
                            })
                        elif entry['offsetunituri'] != 'urn:replicon:time-off-policy-offset-unit:months':
                            final_policy_set_list.append({
                                "description": "Effective on - " + datetime.strftime(pto_policy_effective_date_with_offset, config.DATE_DEFAULT_FORMAT),
                                "effectiveDate": python_callable.get_split_date(pto_policy_effective_date_with_offset, 'int'),
                                "policySet": complete_policyset_based_on_offset
                            })

                    else:
                        final_policy_set_list.append({
                            "effectiveDate": python_callable.get_split_date(dag_run.conf['startdate'], 'int'),
                            "policySet": complete_policyset_based_on_offset,
                            "description": "Effective on - " + dag_run.conf['startdate']
                        })

                elif str(entry['offset']) != str(policies_to_be_assigned_and_max_min_offsets['min_offset_from_policies_1']):
                    if entry['offsetunituri'] == 'urn:replicon:time-off-policy-offset-unit:months':
                        final_policy_set_list.append({
                            "description": "Effective on - " + datetime.strftime(pto_policy_effective_date_with_offset_in_months, config.DATE_DEFAULT_FORMAT),
                            "effectiveDate": python_callable.get_split_date(pto_policy_effective_date_with_offset_in_months, 'int'),
                            "policySet": complete_policyset_based_on_offset
                        })
                    elif entry['offsetunituri'] != 'urn:replicon:time-off-policy-offset-unit:months':
                        final_policy_set_list.append({
                            "description": "Effective on - " + datetime.strftime(pto_policy_effective_date_with_offset, config.DATE_DEFAULT_FORMAT),
                            "effectiveDate": python_callable.get_split_date(pto_policy_effective_date_with_offset, 'int'),
                            "policySet": complete_policyset_based_on_offset
                        })

            return final_policy_set_list

        final_policy_set_list = rail.PythonOperator(
            task_id='final_policy_set_list',
            python_callable=lambda dag_run: get_final_policy_set_list_keenan_h_ca_plan_h(rail.result("policy_set_list_part_1"), rail.result(
                "combined_initial_tasks")['time_off_policy_mapper_search_entries'], rail.result(
                "log_hoursday_5"), rail.result('get_policies_to_be_assigned_and_max_min_offsets'), rail.result('get_defaultpolicyfromgloballevel_11'), dag_run)
        )

        assign_time_offpolicy_95 = rail.RepliconServiceOperator(
            task_id='assign_time_offpolicy_95',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('final_policy_set_list')
            }
        )

        catch_and_log_error = rail.SetVariableOperator(
            task_id="catch_and_log_error",
            trigger_rule='one_failed',
            name='response_from_dag',
            append=False,
            value="Error in Timeoff Assignment - Keennan-H, Neace-Special-H and AP CA Plan-H : {{get_error_message()}}"
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.get_dag_run_var(
                "response_from_dag") if rail.result('catch_and_log_error') else ""
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error >> final_response_from_dag
        can_run_batch_task >> rail.Label(
            'No') >> combined_initial_tasks

        combined_initial_tasks >> log_hoursday_5 >> get_defaultpolicyfromgloballevel_11 >> if_log_hoursday_5_present_16

        if_log_hoursday_5_present_16 >> rail.Label(
            'No') >> catch_and_log_error
        if_log_hoursday_5_present_16 >> rail.Label(
            'Yes') >> get_policies_to_be_assigned_and_max_min_offsets

        get_policies_to_be_assigned_and_max_min_offsets \
            >> policy_set_list_part_1 >> final_policy_set_list >> assign_time_offpolicy_95 >> catch_and_log_error

        return dag


rail.for_each_instance(create_dag)
