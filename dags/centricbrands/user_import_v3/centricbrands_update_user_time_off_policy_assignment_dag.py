from datetime import timedelta, datetime
import json
from airflow.models import Variable
from dateutil.relativedelta import relativedelta
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.child_update_user_time_off_policy_assignment_dag_id,
        description=f'CentricBrands_Update User - Time Off policy assignment child',
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
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_integrationdate_object'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_integrationdate_object',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def get_date_object(datestring):
            date_obj = datetime.strptime(datestring, '%m/%d/%Y')
            return {
                'day': date_obj.day,
                'month': date_obj.month,
                'year': date_obj.year,
                'datestring': date_obj.strftime("%m/%d/%Y")
            }

        get_integrationdate_object = rail.PythonOperator(
            task_id='get_integrationdate_object',
            python_callable=lambda dag_run: get_date_object(
                dag_run.conf['integrationdate'])
        )

        get_startdate_object = rail.PythonOperator(
            task_id='get_startdate_object',
            python_callable=lambda dag_run: get_date_object(
                dag_run.conf['startdate'])
        )

        get_existing_usertimeoff_policysummary = rail.RepliconServiceOperator(
            task_id='get_existing_usertimeoff_policysummary',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.uri }}"
            }
        )

        get_default_time_off_policy_set_schedule_for_time_off_type = rail.RepliconServiceOperator(
            task_id='get_default_time_off_policy_set_schedule_for_time_off_type',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
            }
        )

        get_default_policyset = rail.PythonOperator(
            task_id='get_default_policyset',
            python_callable=lambda: [schedule['policySet'] for schedule in rail.result(
                'get_default_time_off_policy_set_schedule_for_time_off_type')]
        )

        def get_existingpolicies(dag_run):
            matchingtimeofftypepolicy = list(filter(lambda policy: policy['timeOffType']['uri'] == dag_run.conf['timeoffuri'], rail.result(
                'get_existing_usertimeoff_policysummary')['policiesByTimeOffType']))
            policy_schedule = matchingtimeofftypepolicy[0][
                'policySetSchedule'] if matchingtimeofftypepolicy else ''
            if not policy_schedule:
                return False
            policy_schedule_array = (json.dumps(policy_schedule)).replace(
                'null', '\"effective\"').replace('\"script\"', '\"scriptTarget\"')
            policy_schedule_string = policy_schedule_array.replace(
                '[[', '').replace(']]', '')
            return json.loads(policy_schedule_string)

        get_existing_policies = rail.PythonOperator(
            task_id='get_existing_policies',
            python_callable=get_existingpolicies
        )

        get_integrationdate_effective_month = rail.PythonOperator(
            task_id='get_integrationdate_effective_month',
            python_callable=lambda dag_run: (datetime.strptime(
                dag_run.conf['integrationdate'], "%m/%d/%Y")).strftime("%B")
        )

        get_today_dateobject = rail.PythonOperator(
            task_id='get_today_dateobject',
            python_callable=lambda: get_date_object(
                datetime.now().strftime("%m/%d/%Y"))
        )

        search_matching_timeoff_policy = rail.PythonOperator(
            task_id='search_matching_timeoff_policy',
            python_callable=lambda dag_run:  list(filter(lambda entry: entry['timeofftype'] == dag_run.conf['timeofftypename'] and entry[
                'offset'] == '0' and entry['month'] == rail.result(
                'get_integrationdate_effective_month'), config.TO_POLICY_STARTING_BALANCE_MAPPER))
        )

        if_timeoff_other_than_pto_vacation_sick = rail.IfOperator(
            task_id='if_timeoff_other_than_pto_vacation_sick',
            test=lambda dag_run: all(timeoff not in (dag_run.conf['timeofftypename']).lower(
            ) for timeoff in ['pto', 'sick', 'vacation']),
            yes_task="if_default_policy_set_present",
            no_task="if_timeoff_is_sick",
        )

        if_default_policy_set_present = rail.IfOperator(
            task_id='if_default_policy_set_present',
            test=lambda: bool(rail.result('get_default_policyset')),
            yes_task="get_default_policy_modified",
            no_task="if_timeoff_is_sick",
        )

        def get_default_policy():
            final_policyset = rail.result('get_existing_policies') if rail.result(
                'get_existing_policies') else []
            default_policy_modified = json.dumps(rail.result('get_default_policyset')).replace(
                '"null"', '"effective"').replace('"script"', '"scriptTarget"')
            default_policy_modified = '[' + default_policy_modified + ']'
            default_policy_modified = default_policy_modified.replace(
                '[[', '').replace(']]', '')
            final_policyset.append({
                "description": "Effective On " + rail.result('get_today_dateobject')['datestring'],
                "effectiveDate": {
                    "year": rail.result('get_today_dateobject')['year'],
                    "month": rail.result('get_today_dateobject')['month'],
                    "day": rail.result('get_today_dateobject')['day']
                },
                "policySet": json.loads(default_policy_modified)
            })
            return final_policyset

        get_default_policy_modified = rail.PythonOperator(
            task_id='get_default_policy_modified',
            python_callable=get_default_policy
        )

        if_existing_policies_present = rail.IfOperator(
            task_id='if_existing_policies_present',
            test=lambda: bool(rail.result('get_existing_policies')),
            yes_task="put_user_time_off_account_policy_set_schedule_withhistoricalpolicies",
            no_task="put_user_time_off_account_policy_set_schedule_withouthistoricalpolicies",
        )

        put_user_time_off_account_policy_set_schedule_withhistoricalpolicies = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_withhistoricalpolicies',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['uri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('get_default_policy_modified')
            }
        )

        put_user_time_off_account_policy_set_schedule_withouthistoricalpolicies = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_withouthistoricalpolicies',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['uri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('get_default_policy_modified')
            }
        )

        if_timeoff_is_sick = rail.IfOperator(
            task_id='if_timeoff_is_sick',
            test=lambda dag_run: 'sick' in (
                dag_run.conf['timeofftypename']).lower(),
            yes_task="search_timeoffpolicy_for_timeofftype_and_month",
            no_task="if_timeoff_pto_or_vacation",
        )

        search_timeoffpolicy_for_timeofftype_and_month = rail.PythonOperator(
            task_id='search_timeoffpolicy_for_timeofftype_and_month',
            python_callable=lambda dag_run:  list(filter(lambda entry: entry['timeofftype'] == dag_run.conf['timeofftypename'] and entry[
                'offset'] == '0' and entry['month'] == datetime.now(
            ).strftime('%B'), config.TO_POLICY_STARTING_BALANCE_MAPPER))
        )

        def get_modified_default_policy():
            final_policyset_to_apply = []
            if bool(rail.result('get_existing_policies')):
                for policy_line in rail.result('get_existing_policies'):
                    if datetime.strptime(rail.result('get_integrationdate_object')['datestring'], "%m/%d/%Y").date() != datetime.strptime(
                            (str(policy_line['effectiveDate']['month']) + "/" + str(policy_line['effectiveDate']['day']) + "/" + str(policy_line['effectiveDate']['year'])), "%m/%d/%Y").date():
                        final_policyset_to_apply.append(policy_line)

            originalbalance = '"keyUri": "urn:replicon:script-key:parameter:amount", "value": {"number": 0.0}}'
            newbalance = '"keyUri": "urn:replicon:script-key:parameter:amount", "value": {"number": ' + str(
                rail.result('search_timeoffpolicy_for_timeofftype_and_month')[0]['balance']) + '}}'
            timeoff_policies_to_assign = (json.dumps(rail.result('get_default_policyset'))).replace(
                'null', '\"effective\"').replace('\"script\"', '\"scriptTarget\"').replace(originalbalance, newbalance)
            timeoff_policies_to_assign = '[' + timeoff_policies_to_assign + ']'
            timeoff_policies_to_assign = timeoff_policies_to_assign.replace(
                '[[', '').replace(']]', '')
            final_policyset_to_apply.append({
                "description": "Effective On " + rail.result('get_integrationdate_object')['datestring'],
                "effectiveDate": {
                    "year": rail.result('get_integrationdate_object')['year'],
                    "month": rail.result('get_integrationdate_object')['month'],
                    "day": rail.result('get_integrationdate_object')['day']
                },
                "policySet": json.loads(timeoff_policies_to_assign)
            })
            return final_policyset_to_apply

        get_defaultpolicy_modified = rail.PythonOperator(
            task_id='get_defaultpolicy_modified',
            python_callable=get_modified_default_policy
        )

        if_existingpolicies_present = rail.IfOperator(
            task_id='if_existingpolicies_present',
            test=lambda: bool(rail.result('get_existing_policies')),
            yes_task="put_user_time_off_account_policy_set_schedule_with_historicalpolicies",
            no_task="put_user_time_off_account_policy_set_schedule_without_historicalpolicies",
        )

        put_user_time_off_account_policy_set_schedule_with_historicalpolicies = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_with_historicalpolicies',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['uri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('get_defaultpolicy_modified')
            }
        )

        put_user_time_off_account_policy_set_schedule_without_historicalpolicies = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_without_historicalpolicies',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['uri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('get_defaultpolicy_modified')
            }
        )

        if_timeoff_pto_or_vacation = rail.IfOperator(
            task_id='if_timeoff_pto_or_vacation',
            test=lambda dag_run: 'pto' in (dag_run.conf['timeofftypename']).lower(
            ) or 'vacation' in (dag_run.conf['timeofftypename']).lower(),
            yes_task="decalre_ptopolicy_list",
            no_task="catch_error",
        )

        decalre_ptopolicy_list = rail.SetVariableOperator(
            task_id='decalre_ptopolicy_list',
            append=False,
            name='ptopolicylist',
            value=[]
        )

        get_tenure_of_employee = rail.PythonOperator(
            task_id='get_tenure_of_employee',
            python_callable=lambda dag_run: float(((datetime.strptime(datetime.now().strftime(
                "%m/%d/%Y"), "%m/%d/%Y") - datetime.strptime(dag_run.conf['startdate'], "%m/%d/%Y")).days)/365)
        )

        if_tenure_present = rail.IfOperator(
            task_id='if_tenure_present',
            test=lambda: rail.result('get_tenure_of_employee'),
            yes_task="if_timeoff_contains_pto",
            no_task="catch_error",
        )

        if_timeoff_contains_pto = rail.IfOperator(
            task_id='if_timeoff_contains_pto',
            test=lambda dag_run: 'pto' in (
                dag_run.conf['timeofftypename']).lower(),
            yes_task="get_offset_value_if_pto",
            no_task="if_timeoffname_contains_hourly",
        )

        get_offset_value_if_pto = rail.PythonOperator(
            task_id='get_offset_value_if_pto',
            python_callable=lambda: "0" if float(rail.result('get_tenure_of_employee')) < 5 else ('5' if float(
                rail.result('get_tenure_of_employee')) > 5 and float(rail.result('get_tenure_of_employee')) < 10 else '10')
        )

        get_index_to_consider_if_pto = rail.PythonOperator(
            task_id='get_index_to_consider_if_pto',
            python_callable=lambda: "0" if float(rail.result('get_tenure_of_employee')) < 5 else ('1' if float(
                rail.result('get_tenure_of_employee')) == 5 and float(rail.result('get_tenure_of_employee')) < 10 else '2')
        )

        if_timeoffname_contains_hourly = rail.IfOperator(
            task_id='if_timeoffname_contains_hourly',
            test=lambda dag_run: 'hourly' in (
                dag_run.conf['timeofftypename']).lower(),
            yes_task="get_offset_value_if_vacationhourly",
            no_task="get_offset_value_if_vacation_hourly",
        )

        get_offset_value_if_vacationhourly = rail.PythonOperator(
            task_id='get_offset_value_if_vacationhourly',
            python_callable=lambda: "0" if float(rail.result('get_tenure_of_employee')) < 3 else ('3' if float(
                rail.result('get_tenure_of_employee')) > 3 and float(rail.result('get_tenure_of_employee')) < 10 else '10')
        )

        get_indextoconsider_if_vacationhourly = rail.PythonOperator(
            task_id='get_indextoconsider_if_vacationhourly',
            python_callable=lambda: "0" if float(rail.result('get_tenure_of_employee')) < 3 else ('1' if float(
                rail.result('get_tenure_of_employee')) == 3 and float(rail.result('get_tenure_of_employee')) < 10 else '2')
        )

        get_offset_value_if_vacation_hourly = rail.PythonOperator(
            task_id='get_offset_value_if_vacation_hourly',
            python_callable=lambda: "0" if float(rail.result('get_tenure_of_employee')) < 5 else ('5' if float(
                rail.result('get_tenure_of_employee')) > 5 and float(rail.result('get_tenure_of_employee')) < 10 else '10')
        )

        get_indextoconsider_if_vacation_hourly = rail.PythonOperator(
            task_id='get_indextoconsider_if_vacation_hourly',
            python_callable=lambda: "0" if float(rail.result('get_tenure_of_employee')) < 5 else ('1' if float(
                rail.result('get_tenure_of_employee')) == 5 and float(rail.result('get_tenure_of_employee')) < 10 else '2')
        )

        get_final_offset_value_if_vacationhourly = rail.PythonOperator(
            task_id='get_final_offset_value_if_vacationhourly',
            python_callable=lambda: rail.result('get_offset_value_if_vacationhourly') if rail.result(
                'get_offset_value_if_vacationhourly') else rail.result('get_offset_value_if_vacation_hourly')
        )

        get_final_indextoconsider_if_vacationhourly = rail.PythonOperator(
            task_id='get_final_indextoconsider_if_vacationhourly',
            python_callable=lambda: rail.result('get_indextoconsider_if_vacationhourly') if rail.result(
                'get_indextoconsider_if_vacationhourly') else rail.result('get_indextoconsider_if_vacation_hourly')
        )

        if_existing_policiespresent = rail.IfOperator(
            task_id='if_existing_policiespresent',
            test=lambda: bool(rail.result('get_existing_policies')),
            yes_task="foreach_item_in_raw_existing_policies",
            no_task="get_final_offset",
        )

        def get_existingpolicies_array(dag_run):
            matchingtimeofftypepolicy = list(filter(lambda policy: policy['timeOffType']['uri'] == dag_run.conf['timeoffuri'], rail.result(
                'get_existing_usertimeoff_policysummary')['policiesByTimeOffType']))
            policy_schedule = matchingtimeofftypepolicy[0][
                'policySetSchedule'] if matchingtimeofftypepolicy else ''
            policy_schedule_array = (json.dumps(policy_schedule)).replace(
                'null', '\"effective\"').replace('\"script\"', '\"scriptTarget\"')
            return json.loads(policy_schedule_array)

        foreach_item_in_raw_existing_policies = rail.ForEachOperator(
            task_id='foreach_item_in_raw_existing_policies',
            items=get_existingpolicies_array,
            start_task='if_effectivedate_lessthan_today',
            end_task='foreach_item_in_raw_existing_policies_end'
        )

        def get_date_string(dateobj, dateformat=True):
            return str(dateobj['month']) + "/" + str(dateobj['day']) + "/" + str(dateobj['year']) if dateformat else (
                str(dateobj['year']) + "-" + str(dateobj['month']) + "-" + str(dateobj['day']))

        if_effectivedate_lessthan_today = rail.IfOperator(
            task_id='if_effectivedate_lessthan_today',
            test=lambda: datetime.strptime(get_date_string(rail.result('foreach_item_in_raw_existing_policies')[
                                           'effectiveDate']), "%m/%d/%Y") < datetime.strptime(rail.result('get_today_dateobject')['datestring'], "%m/%d/%Y"),
            yes_task="insert_to_ptopolicy_list",
            no_task="foreach_item_in_raw_existing_policies_end",
        )

        insert_to_ptopolicy_list = rail.SetVariableOperator(
            task_id='insert_to_ptopolicy_list',
            append=True,
            name='{{ result("decalre_ptopolicy_list").name }}',
            value=lambda: {
                "description": rail.result('foreach_item_in_raw_existing_policies')['description'],
                "effectiveDate": {
                    "day": rail.result('foreach_item_in_raw_existing_policies')['effectiveDate']['day'],
                    "month": rail.result('foreach_item_in_raw_existing_policies')['effectiveDate']['month'],
                    "year": rail.result('foreach_item_in_raw_existing_policies')['effectiveDate']['year']
                },
                "policySet": rail.result('foreach_item_in_raw_existing_policies')['policySet']
            }
        )

        foreach_item_in_raw_existing_policies_end = rail.EmptyOperator(
            task_id='foreach_item_in_raw_existing_policies_end',
        )

        get_final_offset = rail.PythonOperator(
            task_id='get_final_offset',
            python_callable=lambda: rail.result('get_offset_value_if_pto') if rail.result('get_offset_value_if_pto') else rail.result(
                'get_final_offset_value_if_vacationhourly')
        )

        search_timeoff_policy_with_matching_offset = rail.PythonOperator(
            task_id='search_timeoff_policy_with_matching_offset',
            python_callable=lambda dag_run: list(filter(lambda entry: entry['timeofftype'] == dag_run.conf['timeofftypename'] and entry[
                'offset'] == rail.result(
                'get_final_offset') and entry['month'] == datetime.now().strftime("%B"), config.TO_POLICY_STARTING_BALANCE_MAPPER))
        )

        def get_timeoff_balance_event_script():
            default_policysetschedule = rail.result(
                'get_default_time_off_policy_set_schedule_for_time_off_type')
            timeoffbalanceeventscript = list(filter(lambda schedule: schedule['startOffset']['offsetValue'] == int(
                rail.result('get_final_offset')), default_policysetschedule))
            timeoffbalance_eventscript = timeoffbalanceeventscript[0]['policySet'][
                'timeOffBalanceEventScripts'] if timeoffbalanceeventscript else []
            timeoffbalance_eventscript = [timeoffbalance_eventscript]
            timeoffbalance_eventscript = (json.dumps(timeoffbalance_eventscript)).replace(
                '[[', '').replace(']]', '').replace(' ', '').replace('=>', ':')
            return timeoffbalance_eventscript

        get_timeoff_balance_event_script_for_0_year_offset = rail.PythonOperator(
            task_id='get_timeoff_balance_event_script_for_0_year_offset',
            python_callable=get_timeoff_balance_event_script
        )

        def get_starting_balance_script(dag_run):
            return json.dumps({
                "scriptTarget": {
                    "uri": dag_run.conf['startingbalancesettouri']
                },
                "additionalParameters": [
                    {
                        "keyUri": "urn:replicon:script-key:parameter:amount",
                        "value": {
                            "number": dag_run.conf['previousbalance'] if dag_run.conf['previousbalance'] else rail.result(
                                'search_timeoff_policy_with_matching_offset')[0]['balance']
                        }
                    },
                    {
                        "keyUri": "urn:replicon:script-key:parameter:precedence",
                        "value": {
                            "number": "10"
                        }
                    }
                ]
            }) + ',' + rail.result('get_timeoff_balance_event_script_for_0_year_offset')

        get_value_for_startingbalance_setto = rail.PythonOperator(
            task_id='get_value_for_startingbalance_setto',
            python_callable=get_starting_balance_script
        )

        def get_finalbalancescript_for_startingbalancesetto():
            default_policysetschedule = rail.result(
                'get_default_time_off_policy_set_schedule_for_time_off_type')
            requiredpolicyset = list(filter(lambda policy: policy['startOffset']['offsetValue'] == int(
                rail.result('get_final_offset')), default_policysetschedule))
            requiredpolicyset = requiredpolicyset[0]['policySet'] if requiredpolicyset else ''
            requiredpolicyset = (json.dumps(requiredpolicyset)).replace(' ', '').replace(rail.result(
                'get_timeoff_balance_event_script_for_0_year_offset'), rail.result('get_value_for_startingbalance_setto')).replace('=>', ':')
            return json.loads(requiredpolicyset)

        get_finalbalance_script_for_startingbalancesetto = rail.PythonOperator(
            task_id='get_finalbalance_script_for_startingbalancesetto',
            python_callable=get_finalbalancescript_for_startingbalancesetto
        )

        get_finalindex_for_comparison = rail.PythonOperator(
            task_id='get_finalindex_for_comparison',
            python_callable=lambda: rail.result('get_index_to_consider_if_pto') if rail.result(
                'get_index_to_consider_if_pto') else rail.result('get_final_indextoconsider_if_vacationhourly')
        )

        log_required_index_for_reference = rail.PythonOperator(
            task_id='log_required_index_for_reference',
            python_callable=lambda: 3 -
            int(rail.result('get_finalindex_for_comparison'))
        )

        get_index_where_effectivedate_is_today = rail.PythonOperator(
            task_id='get_index_where_effectivedate_is_today',
            python_callable=lambda: '0' if rail.result('log_required_index_for_reference') == 3 else (
                '1' if rail.result('log_required_index_for_reference') == 2 else '2')
        )

        declare_iteration_index = rail.SetVariableOperator(
            task_id='declare_iteration_index',
            name='index',
            append=False,
            value=0
        )

        foreach_set_schedule = rail.ForEachOperator(
            task_id='foreach_set_schedule',
            items="{{ result('get_default_time_off_policy_set_schedule_for_time_off_type') | to_json }}",
            start_task='if_index_greaterthan_equal_final_index',
            end_task='foreach_set_schedule_end'
        )

        if_index_greaterthan_equal_final_index = rail.IfOperator(
            task_id='if_index_greaterthan_equal_final_index',
            test=lambda: bool(rail.get_dag_run_var('index') >= int(
                rail.result('get_finalindex_for_comparison'))),
            yes_task="if_index_equal_index_for_effectivedate_tobe_today",
            no_task="increase_index",
        )

        if_index_equal_index_for_effectivedate_tobe_today = rail.IfOperator(
            task_id='if_index_equal_index_for_effectivedate_tobe_today',
            test=lambda: rail.get_dag_run_var('index') == int(
                rail.result('get_index_where_effectivedate_is_today')),
            yes_task="insertto_pto_policy_list",
            no_task="get_offset_value",
        )

        insertto_pto_policy_list = rail.SetVariableOperator(
            task_id='insertto_pto_policy_list',
            append=True,
            name='{{ result("decalre_ptopolicy_list").name }}',
            value=lambda: {
                "description": "Effective on" + datetime.now().strftime("%m") + "/" + datetime.now().strftime("%d") + "/" + datetime.now().strftime("%Y"),
                "effectiveDate": {
                    "day": int(datetime.now().strftime("%d")),
                    "month": int(datetime.now().strftime("%m")),
                    "year": int(datetime.now().strftime("%Y"))
                },
                "policySet": rail.result('get_finalbalance_script_for_startingbalancesetto')
            }
        )

        get_offset_value = rail.PythonOperator(
            task_id='get_offset_value',
            python_callable=lambda:  int(rail.result('foreach_set_schedule')[
                                         'startOffset']['offsetValue']) * 12
        )

        get_startdate_plus_offsetvalue_months = rail.PythonOperator(
            task_id='get_startdate_plus_offsetvalue_months',
            python_callable=lambda dag_run: get_date_object((datetime.strptime(
                dag_run.conf['startdate'], "%m/%d/%Y") + relativedelta(months=rail.result('get_offset_value'))).strftime("%m/%d/%Y"))
        )

        insertto_pto_policylist = rail.SetVariableOperator(
            task_id='insertto_pto_policylist',
            append=True,
            name='{{ result("decalre_ptopolicy_list").name }}',
            value=lambda: {
                "description": "Eeffective On" + str(rail.result('get_startdate_plus_offsetvalue_months')['month']) + "/" + str(rail.result(
                    'get_startdate_plus_offsetvalue_months')['day']) + "/" + str(rail.result('get_startdate_plus_offsetvalue_months')['year']),
                "effectiveDate": {
                    "day": rail.result('get_startdate_plus_offsetvalue_months')['day'],
                    "month": rail.result('get_startdate_plus_offsetvalue_months')['month'],
                    "year": rail.result('get_startdate_plus_offsetvalue_months')['year']
                },
                "policySet": rail.result('foreach_set_schedule')['policySet']
            }
        )

        increase_index = rail.SetVariableOperator(
            task_id='increase_index',
            name='index',
            append=False,
            value=lambda: rail.get_dag_run_var('index') + 1
        )

        foreach_set_schedule_end = rail.EmptyOperator(
            task_id='foreach_set_schedule_end',
        )

        get_final_policyset = rail.PythonOperator(
            task_id='get_final_policyset',
            python_callable=lambda: json.loads((json.dumps(rail.get_dag_run_var('ptopolicylist'))).replace(
                'null', '\"effective\"').replace('\"script\"', '\"scriptTarget\"'))
        )

        put_user_time_off_account_policy_set_schedule = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['uri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('get_final_policyset')
            }
        )

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "{{get_error_message()}}")
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error
        can_run_batch_task >> rail.Label('No') >> get_integrationdate_object
        get_integrationdate_object >> get_startdate_object >> get_existing_usertimeoff_policysummary
        get_existing_usertimeoff_policysummary >> get_default_time_off_policy_set_schedule_for_time_off_type >> get_default_policyset >> get_existing_policies
        get_existing_policies >> get_integrationdate_effective_month >> get_today_dateobject >> search_matching_timeoff_policy >> if_timeoff_other_than_pto_vacation_sick
        if_timeoff_other_than_pto_vacation_sick >> rail.Label(
            'Yes') >> if_default_policy_set_present
        if_default_policy_set_present >> rail.Label(
            'Yes') >> get_default_policy_modified >> if_existing_policies_present
        if_existing_policies_present >> rail.Label(
            'Yes') >> put_user_time_off_account_policy_set_schedule_withhistoricalpolicies >> if_timeoff_is_sick
        if_existing_policies_present >> rail.Label(
            'No') >> put_user_time_off_account_policy_set_schedule_withouthistoricalpolicies >> if_timeoff_is_sick
        if_default_policy_set_present >> rail.Label('No') >> if_timeoff_is_sick
        if_timeoff_other_than_pto_vacation_sick >> rail.Label(
            'No') >> if_timeoff_is_sick
        if_timeoff_is_sick >> rail.Label(
            'Yes') >> search_timeoffpolicy_for_timeofftype_and_month >> get_defaultpolicy_modified >> if_existingpolicies_present
        if_existingpolicies_present >> rail.Label(
            'Yes') >> put_user_time_off_account_policy_set_schedule_with_historicalpolicies >> if_timeoff_pto_or_vacation
        if_existingpolicies_present >> rail.Label(
            'No') >> put_user_time_off_account_policy_set_schedule_without_historicalpolicies >> if_timeoff_pto_or_vacation
        if_timeoff_is_sick >> rail.Label('No') >> if_timeoff_pto_or_vacation
        if_timeoff_pto_or_vacation >> rail.Label(
            'Yes') >> decalre_ptopolicy_list >> get_tenure_of_employee >> if_tenure_present
        if_tenure_present >> rail.Label('Yes') >> if_timeoff_contains_pto
        if_timeoff_contains_pto >> rail.Label(
            'Yes') >> get_offset_value_if_pto >> get_index_to_consider_if_pto >> if_existing_policiespresent
        if_timeoff_contains_pto >> rail.Label(
            'No') >> if_timeoffname_contains_hourly
        if_timeoffname_contains_hourly >> rail.Label(
            'Yes') >> get_offset_value_if_vacationhourly >> get_indextoconsider_if_vacationhourly >> get_final_offset_value_if_vacationhourly
        if_timeoffname_contains_hourly >> rail.Label(
            'No') >> get_offset_value_if_vacation_hourly >> get_indextoconsider_if_vacation_hourly >> get_final_offset_value_if_vacationhourly
        get_final_offset_value_if_vacationhourly >> get_final_indextoconsider_if_vacationhourly >> if_existing_policiespresent
        if_existing_policiespresent >> rail.Label(
            'Yes') >> foreach_item_in_raw_existing_policies >> if_effectivedate_lessthan_today
        if_effectivedate_lessthan_today >> rail.Label(
            'Yes') >> insert_to_ptopolicy_list >> foreach_item_in_raw_existing_policies_end
        if_effectivedate_lessthan_today >> rail.Label(
            'No') >> foreach_item_in_raw_existing_policies_end
        foreach_item_in_raw_existing_policies >> foreach_item_in_raw_existing_policies_end >> get_final_offset
        if_existing_policiespresent >> rail.Label(
            'No') >> get_final_offset >> search_timeoff_policy_with_matching_offset
        search_timeoff_policy_with_matching_offset >> get_timeoff_balance_event_script_for_0_year_offset >> get_value_for_startingbalance_setto
        get_value_for_startingbalance_setto >> get_finalbalance_script_for_startingbalancesetto >> get_finalindex_for_comparison
        get_finalindex_for_comparison >> log_required_index_for_reference >> get_index_where_effectivedate_is_today >> declare_iteration_index
        declare_iteration_index >> foreach_set_schedule >> if_index_greaterthan_equal_final_index
        if_index_greaterthan_equal_final_index >> rail.Label(
            'Yes') >> if_index_equal_index_for_effectivedate_tobe_today
        if_index_equal_index_for_effectivedate_tobe_today >> rail.Label(
            'Yes') >> insertto_pto_policy_list >> increase_index >> foreach_set_schedule_end
        if_index_equal_index_for_effectivedate_tobe_today >> rail.Label(
            'No') >> get_offset_value >> get_startdate_plus_offsetvalue_months >> insertto_pto_policylist >> increase_index >> foreach_set_schedule_end
        if_index_greaterthan_equal_final_index >> rail.Label(
            'No') >> increase_index >> foreach_set_schedule_end
        foreach_set_schedule >> foreach_set_schedule_end >> get_final_policyset >> put_user_time_off_account_policy_set_schedule >> catch_error
        if_tenure_present >> rail.Label('No') >> catch_error
        if_timeoff_pto_or_vacation >> rail.Label(
            'No') >> catch_error

    return dag


rail.for_each_instance(create_dag)
