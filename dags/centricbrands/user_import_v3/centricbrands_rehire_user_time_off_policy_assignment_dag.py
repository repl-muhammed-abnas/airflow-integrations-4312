
from datetime import timedelta, datetime
import json
from dateutil.relativedelta import relativedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.child_rehire_user_time_off_policy_assignment_dag_id,
        description=f'CentricBrands_Rehire User - Time Off policy assignment Child',
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
            dateobj = datetime.strptime(datestring, '%m/%d/%Y')
            return {
                'day': dateobj.day,
                'month': dateobj.month,
                'year': dateobj.year,
                'datestring': datestring
            }

        get_integrationdate_object = rail.PythonOperator(
            task_id='get_integrationdate_object',
            python_callable=lambda dag_run: get_date_object(
                dag_run.conf['integrationdate'] if dag_run.conf['integrationdate'] else datetime.now().strftime('%m/%d/%Y'))
        )

        get_startdate_obejct = rail.PythonOperator(
            task_id='get_startdate_obejct',
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
            policy_schedule = (json.dumps(policy_schedule)).replace('null', '\"effective\"').replace(
                '\"script\"', '\"scriptTarget\"').replace('[[', '').replace(']]', '')
            return json.loads(policy_schedule)

        get_existing_policies = rail.PythonOperator(
            task_id='get_existing_policies',
            python_callable=get_existingpolicies
        )

        get_integrationdate_effective_month = rail.PythonOperator(
            task_id='get_integrationdate_effective_month',
            python_callable=lambda dag_run: (datetime.strptime(
                dag_run.conf['integrationdate'], "%m/%d/%Y")).strftime("%B")
        )

        search_matching_timeoff_policy = rail.PythonOperator(
            task_id='search_matching_timeoff_policy',
            python_callable=lambda dag_run:  list(filter(lambda entry: entry['timeofftype'] == dag_run.conf['timeofftypename'] and entry[
                'offset'] == '0' and entry['month'] == rail.result(
                'get_integrationdate_effective_month'), config.TO_POLICY_STARTING_BALANCE_MAPPER))
        )

        if_timeoff_other_than_pto_vacation_sick = rail.IfOperator(
            task_id='if_timeoff_other_than_pto_vacation_sick',
            test=lambda dag_run: all(timeoff not in dag_run.conf['timeofftypename'] for timeoff in [
                                     'PTO', 'Sick', 'Vacation']),
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
            default_policy = (json.dumps(rail.result('get_default_policyset'))).replace(
                'null', '\"effective\"').replace('\"script\"', '\"scriptTarget\"')
            default_policy = '[' + default_policy + ']'
            default_policy = default_policy.replace('[[', '').replace(']]', '')
            return json.loads(default_policy)

        get_default_policy_modified = rail.PythonOperator(
            task_id='get_default_policy_modified',
            python_callable=get_default_policy
        )

        if_existing_policies_present = rail.IfOperator(
            task_id='if_existing_policies_present',
            test=lambda: bool(rail.result('get_existing_policies')),
            yes_task="put_user_time_off_account_policy_set_schedule_with_historical_policies",
            no_task="put_user_time_off_account_policy_set_schedule_without_historicalpolicies",
        )

        put_user_time_off_account_policy_set_schedule_with_historical_policies = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_with_historical_policies',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['uri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('get_existing_policies') + [{
                    "effectiveDate": {
                        "year": rail.result('get_integrationdate_object')['year'],
                        "month": rail.result('get_integrationdate_object')['month'],
                        "day": rail.result('get_integrationdate_object')['day']
                    },
                    "description": "Effective On " + rail.result('get_integrationdate_object')['datestring'],
                    "policySet": rail.result('get_default_policy_modified')
                }
                ]
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
                "policySetScheduleEntries": [
                    {
                        "effectiveDate": {
                            "year": rail.result('get_integrationdate_object')['year'],
                            "month": rail.result('get_integrationdate_object')['month'],
                            "day": rail.result('get_integrationdate_object')['day']
                        },
                        "description": "Effective On " + rail.result('get_integrationdate_object')['datestring'],
                        "policySet": rail.result('get_default_policy_modified')
                    }
                ]
            }
        )

        if_timeoff_is_sick = rail.IfOperator(
            task_id='if_timeoff_is_sick',
            test=lambda dag_run: 'sick' in (
                dag_run.conf['timeofftypename']).lower(),
            yes_task="get_defaultpolicy_modified",
            no_task="if_timeoff_pto_or_vacation",
        )

        def get_modified_default_policy():
            final_policyset_to_apply = []
            if bool(rail.result('get_existing_policies')):
                for policy_line in rail.result('get_existing_policies'):
                    if datetime.strptime(rail.result('get_integrationdate_object')['datestring'], "%m/%d/%Y").date() != datetime.strptime(
                            (str(policy_line['effectiveDate']['month']) + "/" + str(policy_line['effectiveDate']['day']) + "/" + str(policy_line['effectiveDate']['year'])), "%m/%d/%Y").date():
                        final_policyset_to_apply.append(policy_line)

            originalbalance = '"keyUri": "urn:replicon:script-key:parameter:amount", "value": {"number":0.0}}'
            newbalance = '"keyUri": "urn:replicon:script-key:parameter:amount", "value": {"number": ' + str(
                rail.result('search_matching_timeoff_policy')[0]['balance']) + '}}'
            timeoff_policies_to_assign = (json.dumps(rail.result('get_default_policyset'))).replace(
                'null', '\"effective\"').replace('\"script\"', '\"scriptTarget\"').replace(originalbalance, newbalance)
            timeoff_policies_to_assign = '[' + timeoff_policies_to_assign + ']'
            timeoff_policies_to_assign = timeoff_policies_to_assign.replace(
                '[[', '').replace(']]', '')
            final_policyset_to_apply.append({
                "effectiveDate": {
                    "year": rail.result('get_integrationdate_object')['year'],
                    "month": rail.result('get_integrationdate_object')['month'],
                    "day": rail.result('get_integrationdate_object')['day']
                },
                "description": "Effective On " + rail.result('get_integrationdate_object')['datestring'],
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
                "policySetScheduleEntries": rail.result('get_defaultpolicy_modified')
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
                "policySetScheduleEntries": rail.result('get_defaultpolicy_modified')
            }
        )

        if_timeoff_pto_or_vacation = rail.IfOperator(
            task_id='if_timeoff_pto_or_vacation',
            test=lambda dag_run: 'pto' in (dag_run.conf['timeofftypename']).lower(
            ) or 'vacation' in (dag_run.conf['timeofftypename']).lower(),
            yes_task="declare_variable_prorationrequired",
            no_task="catch_error",
        )

        declare_variable_prorationrequired = rail.SetVariableOperator(
            task_id='declare_variable_prorationrequired',
            append=False,
            name='prorationrequired',
            value=None
        )

        decalre_ptopolicy_list = rail.SetVariableOperator(
            task_id='decalre_ptopolicy_list',
            append=False,
            name='ptopolicylist',
            value=[]
        )

        get_difference_of_integration_and_startdate = rail.PythonOperator(
            task_id='get_difference_of_integration_and_startdate',
            python_callable=lambda dag_run: ((datetime.strptime(
                dag_run.conf['startdate'], "%m/%d/%Y") - datetime.strptime(dag_run.conf['integrationdate'], "%m/%d/%Y")).days)/365
        )

        if_difference_greater_than_1 = rail.IfOperator(
            task_id='if_difference_greater_than_1',
            test=lambda: float(rail.result(
                'get_difference_of_integration_and_startdate')) > 1,
            yes_task="update_variable_prorationrequired",
            no_task="if_integrationdate_day_greater_than_1",
        )

        update_variable_prorationrequired = rail.SetVariableOperator(
            task_id='update_variable_prorationrequired',
            append=False,
            name='{{ result("declare_variable_prorationrequired").name }}',
            value='yes'
        )

        if_integrationdate_day_greater_than_1 = rail.IfOperator(
            task_id='if_integrationdate_day_greater_than_1',
            test=lambda: rail.result('get_integrationdate_object')['day'] > 1,
            yes_task="update_variable_proration_required",
            no_task="if_prorationrequired_equals_yes",
        )

        update_variable_proration_required = rail.SetVariableOperator(
            task_id='update_variable_proration_required',
            append=False,
            name='{{ result("declare_variable_prorationrequired").name }}',
            value='yes'
        )

        if_prorationrequired_equals_yes = rail.IfOperator(
            task_id='if_prorationrequired_equals_yes',
            test=lambda: rail.get_dag_run_var('prorationrequired') == 'yes',
            yes_task="if_timeoff_contains_pto",
            no_task="if_proratedaccrual_not_equals_yes_and_difference_equals_0",
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
            python_callable=lambda: "0" if float(rail.result('get_difference_of_integration_and_startdate')) < 5 else ('5' if float(rail.result(
                'get_difference_of_integration_and_startdate')) > 5 and float(rail.result('get_difference_of_integration_and_startdate')) < 10 else '10')
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
            python_callable=lambda: "0" if float(rail.result('get_difference_of_integration_and_startdate')) < 3 else ('3' if float(rail.result(
                'get_difference_of_integration_and_startdate')) > 3 and float(rail.result('get_difference_of_integration_and_startdate')) < 10 else '10')
        )

        get_offset_value_if_vacation_hourly = rail.PythonOperator(
            task_id='get_offset_value_if_vacation_hourly',
            python_callable=lambda: "0" if float(rail.result('get_difference_of_integration_and_startdate')) < 5 else ('5' if float(rail.result(
                'get_difference_of_integration_and_startdate')) > 5 and float(rail.result('get_difference_of_integration_and_startdate')) < 10 else '10')
        )

        get_final_offset_value_if_vacationhourly = rail.PythonOperator(
            task_id='get_final_offset_value_if_vacationhourly',
            python_callable=lambda: rail.result('get_offset_value_if_vacationhourly') if rail.result(
                'get_offset_value_if_vacationhourly') else rail.result('get_offset_value_if_vacation_hourly')
        )

        if_existing_policiespresent = rail.IfOperator(
            task_id='if_existing_policiespresent',
            test=lambda: bool(rail.result('get_existing_policies')),
            yes_task="foreach_item_in_raw_existing_policies",
            no_task="search_timeoffpolicy_matching_with_offset",
        )

        def getexisting_policies(dag_run):
            matchingtimeofftypepolicy = list(filter(lambda policy: policy['timeOffType']['uri'] == dag_run.conf['timeoffuri'], rail.result(
                'get_existing_usertimeoff_policysummary')['policiesByTimeOffType']))
            policy_schedule = matchingtimeofftypepolicy[0][
                'policySetSchedule'] if matchingtimeofftypepolicy else ''
            policy_schedule = (json.dumps(policy_schedule)).replace(
                'null', '\"effective\"').replace('\"script\"', '\"scriptTarget\"')
            return json.loads(policy_schedule)

        foreach_item_in_raw_existing_policies = rail.ForEachOperator(
            task_id='foreach_item_in_raw_existing_policies',
            items=getexisting_policies,
            start_task='if_effectivedate_lessthan_integrationdate',
            end_task='foreach_item_in_raw_existing_policies_end'
        )

        def get_date_string(dateobj, dateformat=True):
            return str(dateobj['month']) + "/" + str(dateobj['day']) + "/" + str(dateobj['year']) if dateformat else (
                str(dateobj['year']) + "-" + str(dateobj['month']) + "-" + str(dateobj['day']))

        if_effectivedate_lessthan_integrationdate = rail.IfOperator(
            task_id='if_effectivedate_lessthan_integrationdate',
            test=lambda dag_run: datetime.strptime(get_date_string(rail.result('foreach_item_in_raw_existing_policies')[
                                                   'effectiveDate']), "%m/%d/%Y") < datetime.strptime(dag_run.conf['integrationdate'], "%m/%d/%Y"),
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

        search_timeoffpolicy_matching_with_offset = rail.PythonOperator(
            task_id='search_timeoffpolicy_matching_with_offset',
            python_callable=lambda dag_run: list(filter(lambda entry: entry['timeofftype'] == dag_run.conf['timeofftypename'] and entry[
                'offset'] == (rail.result('get_offset_value_if_pto') if rail.result(
                    'get_offset_value_if_pto') else rail.result('get_final_offset_value_if_vacationhourly')) and entry['month'] == rail.result(
                'get_integrationdate_effective_month'), config.TO_POLICY_STARTING_BALANCE_MAPPER))
        )

        get_final_offset = rail.PythonOperator(
            task_id='get_final_offset',
            python_callable=lambda: rail.result('get_offset_value_if_pto') if rail.result('get_offset_value_if_pto') else rail.result(
                'get_final_offset_value_if_vacationhourly')
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
                            "number": rail.result('search_timeoffpolicy_matching_with_offset')[0]['balance']
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

        if_integrationeffectivedate_day_equals_1 = rail.IfOperator(
            task_id='if_integrationeffectivedate_day_equals_1',
            test=lambda dag_run: (datetime.strptime(
                dag_run.conf['integrationdate'], "%m/%d/%Y")).day == 1,
            yes_task="get_modifiedbalancescript_forstartingbalancesetto_withprorationat_startofpolicy",
            no_task="get_modifiedbalancescriptfor_startingbalancesetto_withoutproration",
        )

        def get_modifiedbalancescript(isproration):
            default_setschedule = rail.result(
                'get_default_time_off_policy_set_schedule_for_time_off_type')
            required_schedule = list(filter(lambda schedule: schedule['startOffset']['offsetValue'] == int(
                rail.result('get_final_offset')), default_setschedule))
            required_policyset = required_schedule[0]['policySet']
            required_policyset = json.dumps(required_policyset)
            required_policyset = required_policyset.replace(' ', '').replace(rail.result(
                'get_timeoff_balance_event_script_for_0_year_offset'), rail.result('get_value_for_startingbalance_setto')).replace('=>', ':')
            return required_policyset.replace('do-not-prorate', 'start-of-policy') if isproration else required_policyset

        get_modifiedbalancescript_forstartingbalancesetto_withprorationat_startofpolicy = rail.PythonOperator(
            task_id='get_modifiedbalancescript_forstartingbalancesetto_withprorationat_startofpolicy',
            python_callable=lambda: get_modifiedbalancescript(True)
        )

        get_modifiedbalancescriptfor_startingbalancesetto_withoutproration = rail.PythonOperator(
            task_id='get_modifiedbalancescriptfor_startingbalancesetto_withoutproration',
            python_callable=lambda: get_modifiedbalancescript(False)
        )

        get_finalbalancescriptfor_startingbalancesetto = rail.PythonOperator(
            task_id='get_finalbalancescriptfor_startingbalancesetto',
            python_callable=lambda: rail.result('get_modifiedbalancescript_forstartingbalancesetto_withprorationat_startofpolicy') if rail.result(
                'get_modifiedbalancescript_forstartingbalancesetto_withprorationat_startofpolicy') else rail.result(
                'get_modifiedbalancescriptfor_startingbalancesetto_withoutproration')
        )

        parse_json_final_script = rail.PythonOperator(
            task_id='parse_json_final_script',
            python_callable=lambda: json.loads(rail.result(
                'get_finalbalancescriptfor_startingbalancesetto'))
        )

        declare_index_for_iteration = rail.SetVariableOperator(
            task_id='declare_index_for_iteration',
            name='index',
            append=False,
            value=0
        )

        foreach_set_schedule = rail.ForEachOperator(
            task_id='foreach_set_schedule',
            items="{{ result('get_default_time_off_policy_set_schedule_for_time_off_type') | to_json }}",
            start_task='if_first_set_schedule',
            end_task='foreach_set_schedule_end'
        )

        if_first_set_schedule = rail.IfOperator(
            task_id='if_first_set_schedule',
            test=lambda: bool(rail.get_dag_run_var('index') == 0),
            yes_task="insert_to_ptopolicylist",
            no_task="get_numberof_months_toadd",
        )

        insert_to_ptopolicylist = rail.SetVariableOperator(
            task_id='insert_to_ptopolicylist',
            append=True,
            name='{{ result("decalre_ptopolicy_list").name }}',
            value=lambda: {
                "description": "Effective on " + rail.result('get_integrationdate_object')['datestring'],
                "effectiveDate": {
                    "day": rail.result('get_integrationdate_object')['day'],
                    "month": rail.result('get_integrationdate_object')['month'],
                    "year": rail.result('get_integrationdate_object')['year']
                },
                "policySet": rail.result('parse_json_final_script')
            }
        )

        get_numberof_months_toadd = rail.PythonOperator(
            task_id='get_numberof_months_toadd',
            python_callable=lambda: rail.result('foreach_set_schedule')[
                'startOffset']['offsetValue'] * 12
        )

        get_effective_date = rail.PythonOperator(
            task_id='get_effective_date',
            python_callable=lambda dag_run: get_date_object((datetime.strptime(
                dag_run.conf['startdate'], "%m/%d/%Y") + relativedelta(months=rail.result('get_numberof_months_toadd'))).strftime("%m/%d/%Y"))
        )

        if_effectivedate_greater_than_integrationdate = rail.IfOperator(
            task_id='if_effectivedate_greater_than_integrationdate',
            test=lambda dag_run: (datetime.strptime(dag_run.conf['startdate'], "%m/%d/%Y") + relativedelta(
                months=rail.result('get_numberof_months_toadd'))) > datetime.strptime(dag_run.conf['integrationdate'], "%m/%d/%Y"),
            yes_task="insert_to_pto_policy_list",
            no_task="increment_index",
        )

        insert_to_pto_policy_list = rail.SetVariableOperator(
            task_id='insert_to_pto_policy_list',
            append=True,
            name='{{ result("decalre_ptopolicy_list").name }}',
            value=lambda: {
                "description": "Effective On " + get_date_string(rail.result('get_effective_date')),
                "effectiveDate": {
                    "day": rail.result('get_effective_date')['day'],
                    "month": rail.result('get_effective_date')['month'],
                    "year": rail.result('get_effective_date')['year']
                },
                "policySet": rail.result('foreach_set_schedule')['policySet']
            }
        )

        increment_index = rail.SetVariableOperator(
            task_id='increment_index',
            name='index',
            append=False,
            value=lambda: rail.get_dag_run_var('index') + 1
        )

        foreach_set_schedule_end = rail.EmptyOperator(
            task_id='foreach_set_schedule_end',
        )

        if_startdate_unequal_integrationdate = rail.IfOperator(
            task_id='if_startdate_unequal_integrationdate',
            test=lambda dag_run: datetime.strptime(
                dag_run.conf['startdate'], "%m/%d/%Y") != datetime.strptime(dag_run.conf['integrationdate'], "%m/%d/%Y"),
            yes_task="parse_json_77",
            no_task="if_integrationdate_month_greater_than_1_and_not_lastday_of_year",
        )

        parse_json_77 = rail.PythonOperator(
            task_id='parse_json_77',
            python_callable=lambda dag_run: [{
                "timeOffBalanceEventScripts": [
                    {
                        "scriptTarget": {
                            "uri": dag_run.conf['startingbalancesettouri']
                        },
                        "additionalParameters": [
                            {
                                "keyUri": "urn:replicon:script-key:parameter:amount",
                                "value": {
                                    "number": 0
                                }
                            },
                            {
                                "keyUri": "urn:replicon:script-key:parameter:precedence",
                                "value": {
                                    "number": "10"
                                }
                            }
                        ]
                    }
                ],
                "timeOffValidationScripts": [
                    {
                        "scriptTarget": {
                            "uri": dag_run.conf['preventbalanceoverdrawuri']
                        },
                        "additionalParameters": [
                            {
                                "keyUri": "urn:replicon:script-key:parameter:maximum-overdraw",
                                "value": {
                                    "number": "0"
                                }
                            }
                        ]
                    }
                ]
            }]
        )

        insertto_pto_policy_list = rail.SetVariableOperator(
            task_id='insertto_pto_policy_list',
            append=True,
            name='{{ result("decalre_ptopolicy_list").name }}',
            value=lambda: {
                "description": "Effective on " + str(rail.result('get_startdate_obejct')['month']) + "/" + str(rail.result('get_startdate_obejct')['day']) +
                    "/" + str(rail.result('get_startdate_obejct')['year']),
                "effectiveDate": {
                    "day": rail.result('get_startdate_obejct')['day'],
                    "month": rail.result('get_startdate_obejct')['month'],
                    "year": rail.result('get_startdate_obejct')['year']
                },
                "policySet": rail.result('parse_json_77')[0]
            }
        )

        if_integrationdate_month_greater_than_1_and_not_lastday_of_year = rail.IfOperator(
            task_id='if_integrationdate_month_greater_than_1_and_not_lastday_of_year',
            test=lambda dag_run: (datetime.strptime(dag_run.conf['integrationdate'], "%m/%d/%Y")).month > 1 and (
                datetime.strptime(dag_run.conf['integrationdate'], "%m/%d/%Y")).strftime('%m/%d') != '12/31',
            yes_task="get_timeoffbalanceeventscript_for0yearoffset_modified",
            no_task="get_final_policyset",
        )

        def get_timeoff_balanceevent_script_for0yearoffset_modified():
            default_setschedule = rail.result(
                'get_default_time_off_policy_set_schedule_for_time_off_type')
            required_schedule = list(filter(lambda schedule: schedule['startOffset']['offsetValue'] == int(
                rail.result('get_final_offset')), default_setschedule))
            return required_schedule[0]['policySet']

        get_timeoffbalanceeventscript_for0yearoffset_modified = rail.PythonOperator(
            task_id='get_timeoffbalanceeventscript_for0yearoffset_modified',
            python_callable=get_timeoff_balanceevent_script_for0yearoffset_modified
        )

        insertto_ptopolicy_list = rail.SetVariableOperator(
            task_id='insertto_ptopolicy_list',
            append=True,
            name='{{ result("decalre_ptopolicy_list").name }}',
            value=lambda: {
                "description": "Effective on 12/31/" + str(rail.result('get_integrationdate_object')['year']),
                "effectiveDate": {
                    "day": 31,
                    "month": 12,
                    "year": rail.result('get_startdate_obejct')['year']
                },
                "policySet": rail.result('get_timeoffbalanceeventscript_for0yearoffset_modified')
            }
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

        if_proratedaccrual_not_equals_yes_and_difference_equals_0 = rail.IfOperator(
            task_id='if_proratedaccrual_not_equals_yes_and_difference_equals_0',
            test=lambda: rail.get_dag_run_var('prorationrequired') != 'yes' and float(
                rail.result('get_difference_of_integration_and_startdate')) == 0,
            yes_task="foreach_default_set_schedule",
            no_task="catch_error",
        )

        foreach_default_set_schedule = rail.ForEachOperator(
            task_id='foreach_default_set_schedule',
            items="{{ result('get_default_time_off_policy_set_schedule_for_time_off_type') }}",
            start_task='log_numberofmonthstoadd_87',
            end_task='foreach_default_set_schedule_end'
        )

        get_effectivedate = rail.PythonOperator(
            task_id='get_effectivedate',
            python_callable=lambda dag_run: get_date_object(datetime.strftime(
                dag_run.conf['startdate'], '%m/%d/%Y') + relativedelta(months=rail.result('foreach_set_schedule')['startOffset']['offsetValue'] * 12))
        )

        insertto_pto_policylist = rail.SetVariableOperator(
            task_id='insertto_pto_policylist',
            append=True,
            name='{{ result("decalre_ptopolicy_list").name }}',
            value=lambda: {
                "description": "Effective On " + str(rail.result('get_effectivedate')['month']) + "/" + str(rail.result('get_effectivedate')['day']) + "/" +
                    str(rail.result('get_effectivedate')['year']),
                "effectiveDate": {
                    "day": rail.result('get_effectivedate')['day'],
                    "month": rail.result('get_effectivedate')['month'],
                    "year": rail.result('get_effectivedate')['year']
                },
                "policySet": rail.result('foreach_default_set_schedule')['policySet']
            }
        )

        foreach_default_set_schedule_end = rail.EmptyOperator(
            task_id='foreach_default_set_schedule_end',
        )

        log_final_policyset = rail.PythonOperator(
            task_id='log_final_policyset',
            python_callable=lambda: (json.dumps(rail.get_dag_run_var('ptopolicylist'))).replace(
                'null', '\"effective\"').replace('\"script\"', '\"scriptTarget\"')
        )

        put_user_time_off_account_policysetschedule = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policysetschedule',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['uri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('log_final_policyset')
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
        get_integrationdate_object >> get_startdate_obejct >> get_existing_usertimeoff_policysummary
        get_existing_usertimeoff_policysummary >> get_default_time_off_policy_set_schedule_for_time_off_type >> get_default_policyset >> get_existing_policies
        get_existing_policies >> get_integrationdate_effective_month >> search_matching_timeoff_policy >> if_timeoff_other_than_pto_vacation_sick
        if_timeoff_other_than_pto_vacation_sick >> rail.Label(
            'Yes') >> if_default_policy_set_present
        if_default_policy_set_present >> rail.Label(
            'Yes') >> get_default_policy_modified >> if_existing_policies_present
        if_existing_policies_present >> rail.Label(
            'Yes') >> put_user_time_off_account_policy_set_schedule_with_historical_policies >> if_timeoff_is_sick
        if_existing_policies_present >> rail.Label(
            'No') >> put_user_time_off_account_policy_set_schedule_without_historicalpolicies >> if_timeoff_is_sick
        if_default_policy_set_present >> rail.Label('No') >> if_timeoff_is_sick
        if_timeoff_other_than_pto_vacation_sick >> rail.Label(
            'No') >> if_timeoff_is_sick
        if_timeoff_is_sick >> rail.Label(
            'Yes') >> get_defaultpolicy_modified >> if_existingpolicies_present
        if_existingpolicies_present >> rail.Label(
            'Yes') >> put_user_time_off_account_policy_set_schedule_withhistoricalpolicies >> if_timeoff_pto_or_vacation
        if_existingpolicies_present >> rail.Label(
            'No') >> put_user_time_off_account_policy_set_schedule_withouthistoricalpolicies >> if_timeoff_pto_or_vacation
        if_timeoff_is_sick >> rail.Label('No') >> if_timeoff_pto_or_vacation
        if_timeoff_pto_or_vacation >> rail.Label(
            'Yes') >> declare_variable_prorationrequired >> decalre_ptopolicy_list >> get_difference_of_integration_and_startdate
        get_difference_of_integration_and_startdate >> if_difference_greater_than_1
        if_difference_greater_than_1 >> rail.Label(
            'Yes') >> update_variable_prorationrequired >> if_integrationdate_day_greater_than_1
        if_difference_greater_than_1 >> rail.Label(
            'No') >> if_integrationdate_day_greater_than_1
        if_integrationdate_day_greater_than_1 >> rail.Label(
            'Yes') >> update_variable_proration_required >> if_prorationrequired_equals_yes
        if_integrationdate_day_greater_than_1 >> rail.Label(
            'No') >> if_prorationrequired_equals_yes
        if_prorationrequired_equals_yes >> rail.Label(
            'Yes') >> if_timeoff_contains_pto
        if_timeoff_contains_pto >> rail.Label(
            'Yes') >> get_offset_value_if_pto >> if_existing_policiespresent
        if_timeoff_contains_pto >> rail.Label(
            'No') >> if_timeoffname_contains_hourly
        if_timeoffname_contains_hourly >> rail.Label(
            'Yes') >> get_offset_value_if_vacationhourly >> get_final_offset_value_if_vacationhourly
        if_timeoffname_contains_hourly >> rail.Label(
            'No') >> get_offset_value_if_vacation_hourly >> get_final_offset_value_if_vacationhourly >> if_existing_policiespresent
        if_existing_policiespresent >> rail.Label(
            'Yes') >> foreach_item_in_raw_existing_policies >> if_effectivedate_lessthan_integrationdate
        if_effectivedate_lessthan_integrationdate >> rail.Label(
            'Yes') >> insert_to_ptopolicy_list >> foreach_item_in_raw_existing_policies_end
        if_effectivedate_lessthan_integrationdate >> rail.Label(
            'No') >> foreach_item_in_raw_existing_policies_end
        foreach_item_in_raw_existing_policies >> foreach_item_in_raw_existing_policies_end >> search_timeoffpolicy_matching_with_offset
        if_existing_policiespresent >> rail.Label(
            'No') >> search_timeoffpolicy_matching_with_offset >> get_final_offset >> get_timeoff_balance_event_script_for_0_year_offset
        get_timeoff_balance_event_script_for_0_year_offset >> get_value_for_startingbalance_setto >> if_integrationeffectivedate_day_equals_1
        if_integrationeffectivedate_day_equals_1 >> rail.Label(
            'Yes') >> get_modifiedbalancescript_forstartingbalancesetto_withprorationat_startofpolicy >> get_finalbalancescriptfor_startingbalancesetto
        if_integrationeffectivedate_day_equals_1 >> rail.Label(
            'No') >> get_modifiedbalancescriptfor_startingbalancesetto_withoutproration >> get_finalbalancescriptfor_startingbalancesetto
        get_finalbalancescriptfor_startingbalancesetto >> parse_json_final_script >> declare_index_for_iteration
        declare_index_for_iteration >> foreach_set_schedule >> if_first_set_schedule
        if_first_set_schedule >> rail.Label(
            'Yes') >> insert_to_ptopolicylist >> increment_index >> foreach_set_schedule_end
        if_first_set_schedule >> rail.Label(
            'No') >> get_numberof_months_toadd >> get_effective_date >> if_effectivedate_greater_than_integrationdate
        if_effectivedate_greater_than_integrationdate >> rail.Label(
            'Yes') >> insert_to_pto_policy_list >> increment_index >> foreach_set_schedule_end
        if_effectivedate_greater_than_integrationdate >> rail.Label(
            'No') >> increment_index >> foreach_set_schedule_end
        foreach_set_schedule >> foreach_set_schedule_end >> if_startdate_unequal_integrationdate
        if_startdate_unequal_integrationdate >> rail.Label(
            'Yes') >> parse_json_77 >> insertto_pto_policy_list >> if_integrationdate_month_greater_than_1_and_not_lastday_of_year
        if_startdate_unequal_integrationdate >> rail.Label(
            'No') >> if_integrationdate_month_greater_than_1_and_not_lastday_of_year
        if_integrationdate_month_greater_than_1_and_not_lastday_of_year >> rail.Label(
            'Yes') >> get_timeoffbalanceeventscript_for0yearoffset_modified >> insertto_ptopolicy_list >> get_final_policyset
        if_integrationdate_month_greater_than_1_and_not_lastday_of_year >> rail.Label(
            'No') >> get_final_policyset >> put_user_time_off_account_policy_set_schedule >> if_proratedaccrual_not_equals_yes_and_difference_equals_0
        if_prorationrequired_equals_yes >> rail.Label(
            'No') >> if_proratedaccrual_not_equals_yes_and_difference_equals_0
        if_proratedaccrual_not_equals_yes_and_difference_equals_0 >> rail.Label(
            'Yes') >> foreach_default_set_schedule >> get_effectivedate >> insertto_pto_policylist >> foreach_default_set_schedule_end
        foreach_default_set_schedule >> foreach_default_set_schedule_end >> log_final_policyset >> put_user_time_off_account_policysetschedule >> catch_error
        if_proratedaccrual_not_equals_yes_and_difference_equals_0 >> rail.Label(
            'No') >> catch_error
        if_timeoff_pto_or_vacation >> rail.Label(
            'No') >> catch_error

    return dag


rail.for_each_instance(create_dag)
