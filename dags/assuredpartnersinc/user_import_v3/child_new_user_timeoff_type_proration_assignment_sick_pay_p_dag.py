from datetime import timedelta, datetime
import json
from airflow.models import Variable
import rail
from assuredpartnersinc.user_import_v3.utils import python_callable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_new_user_timeoff_type_proration_assignment_sick_pay_p_dag_id,
        description=f'Assured Partners User Import new user Timeoff type Proration Assignment-Sick Pay-P Child {config.instance}',
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
            no_task='number_of_working_days_in_week'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='number_of_working_days_in_week',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        number_of_working_days_in_week = rail.PythonOperator(
            task_id='number_of_working_days_in_week',
            python_callable=lambda dag_run: python_callable.parse_schedule_name(
                dag_run.conf['schedulename'])['number_of_working_days_in_week']
        )

        log_hoursday_5 = rail.PythonOperator(
            task_id='log_hoursday_5',
            python_callable=lambda dag_run:  float(
                dag_run.conf['weekly_scheduled_hours']) / float(rail.result('number_of_working_days_in_week'))
        )

        def get_users_starting_month(dag_run):
            if dag_run.conf['type'] == 'Add':
                if bool(dag_run.conf['startdate']):
                    return datetime.strptime(dag_run.conf['startdate'], config.DATE_DEFAULT_FORMAT).strftime("%B")
                return datetime.strptime(dag_run.conf['servicedate'], config.DATE_DEFAULT_FORMAT).strftime("%B")
            return datetime.strptime(dag_run.conf['integration_run_date'], config.DATE_DEFAULT_FORMAT).strftime("%B")

        log_users_starting_month_6 = rail.PythonOperator(
            task_id='log_users_starting_month_6',
            python_callable=get_users_starting_month
        )

        assured_partners_time_off_policy_mapper_for_sick_pay_p_search_entries_14 = rail.PythonOperator(
            task_id='assured_partners_time_off_policy_mapper_for_sick_pay_p_search_entries_14',
            python_callable=lambda:  next(iter(filter(
                lambda x: x["type"] == "Sick Pay-P" and x["startingmonth"] == rail.result('log_users_starting_month_6'), config.TO_SICK_PAY_P_MAPPER)), {})
        )

        log_derived_carry_overvaluefor_limitation_rule_yearlyreset_15 = rail.PythonOperator(
            task_id='log_derived_carry_overvaluefor_limitation_rule_yearlyreset_15',
            python_callable=lambda:  float(rail.result('log_hoursday_5')) * float(rail.result(
                "assured_partners_time_off_policy_mapper_for_sick_pay_p_search_entries_14")['carry_over'])
        )

        log_derived_startingbalancevaluebasedonusersjoiningmonth_16 = rail.PythonOperator(
            task_id='log_derived_startingbalancevaluebasedonusersjoiningmonth_16',
            python_callable=lambda:  float(rail.result('log_hoursday_5')) * float(rail.result(
                "assured_partners_time_off_policy_mapper_for_sick_pay_p_search_entries_14")['startingbalance'])
        )

        get_defaultpolicyfromgloballevel_18 = rail.RepliconServiceOperator(
            task_id='get_defaultpolicyfromgloballevel_18',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{dag_run.conf.timeoffuri}}"
            }
        )

        def get_policy_sets_list(default_timeoff_policy_set_schedule, users_start_month, dag_run):
            policy_list = []
            for item in default_timeoff_policy_set_schedule:
                policy_list.append({
                    "description": "Effective on - " + users_start_month,
                    "effectiveDate": python_callable.get_split_date(dag_run.conf['startdate'], 'int'),
                    "policySet": item['policySet']
                })

            return policy_list

        policy_sets_list = rail.PythonOperator(
            task_id='policy_sets_list',
            python_callable=lambda dag_run: get_policy_sets_list(rail.result(
                "get_defaultpolicyfromgloballevel_18"), rail.result("log_users_starting_month_6"), dag_run)
        )

        pluck_existing_carry_over_default_value = rail.PythonOperator(
            task_id='pluck_existing_carry_over_default_value',
            python_callable=lambda: python_callable.get_required_value_from_policy_set_schedule(
                rail.result("get_defaultpolicyfromgloballevel_18"), 0, 'Reset balance once a year', 'urn:replicon:script-key:parameter:reset-balance-amount')
        )

        log_defaultscriptforcarryover_35 = rail.PythonOperator(
            task_id='log_defaultscriptforcarryover_35',
            python_callable=lambda:  json.dumps({"keyUri": "urn:replicon:script-key:parameter:reset-balance-amount", "value": {
                                                "number": rail.result('pluck_existing_carry_over_default_value')}})
        )

        log_newcarryoverbasedonmapperuserjoiningmonth_36 = rail.PythonOperator(
            task_id='log_newcarryoverbasedonmapperuserjoiningmonth_36',
            python_callable=lambda:  json.dumps({"keyUri": "urn:replicon:script-key:parameter:reset-balance-amount", "value": {
                                                "number": rail.result('log_derived_carry_overvaluefor_limitation_rule_yearlyreset_15')}})
        )

        gsub_to_get_rid_of_previous_starting_balance = rail.PythonOperator(
            task_id='gsub_to_get_rid_of_previous_starting_balance',
            python_callable=lambda:  python_callable.get_timeoffbalanceeventscript_to_gsub(
                rail.result("get_defaultpolicyfromgloballevel_18"), 0, 'Set initial balance for the first day of a policy')
        )

        log_gsubstartingbalancevaluebasedonmapperusersjoiningmonth_38 = rail.PythonOperator(
            task_id='log_gsubstartingbalancevaluebasedonmapperusersjoiningmonth_38',
            python_callable=lambda:  python_callable.starting_balance_script_with_required_starting_balance(json.loads(rail.result(
                'gsub_to_get_rid_of_previous_starting_balance')), rail.result('log_derived_startingbalancevaluebasedonusersjoiningmonth_16'))
        )

        log_final_policy_to_assign_39 = rail.PythonOperator(
            task_id='log_final_policy_to_assign_39',
            python_callable=lambda: json.loads(json.dumps(rail.result('policy_sets_list'), ensure_ascii=False).replace(rail.result('log_defaultscriptforcarryover_35'), rail.result('log_newcarryoverbasedonmapperuserjoiningmonth_36')).replace(
                rail.result('gsub_to_get_rid_of_previous_starting_balance'), rail.result('log_gsubstartingbalancevaluebasedonmapperusersjoiningmonth_38')).replace('"null"', '"effective"').replace(
                '"script"', '"scriptTarget"'))
        )

        assign_time_offpolicy_40 = rail.RepliconServiceOperator(
            task_id='assign_time_offpolicy_40',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('log_final_policy_to_assign_39')
            }
        )

        catch_and_log_error = rail.SetVariableOperator(
            task_id="catch_and_log_error",
            trigger_rule='one_failed',
            name='response_from_dag',
            append=False,
            value="Error in Timeoff Assignment - Sick Pay-P : {{get_error_message()}}"
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
            'No') >> number_of_working_days_in_week

        number_of_working_days_in_week >> log_hoursday_5 >> log_users_starting_month_6 \
            >> assured_partners_time_off_policy_mapper_for_sick_pay_p_search_entries_14 >> log_derived_carry_overvaluefor_limitation_rule_yearlyreset_15 \
            >> log_derived_startingbalancevaluebasedonusersjoiningmonth_16 >> get_defaultpolicyfromgloballevel_18 >> policy_sets_list \
            >> pluck_existing_carry_over_default_value >> log_defaultscriptforcarryover_35 >> log_newcarryoverbasedonmapperuserjoiningmonth_36 \
            >> gsub_to_get_rid_of_previous_starting_balance >> log_gsubstartingbalancevaluebasedonmapperusersjoiningmonth_38 >> log_final_policy_to_assign_39 \
            >> assign_time_offpolicy_40 >> catch_and_log_error

        return dag


rail.for_each_instance(create_dag)
