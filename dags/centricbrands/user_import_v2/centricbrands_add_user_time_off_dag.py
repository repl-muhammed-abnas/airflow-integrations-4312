
from datetime import timedelta, datetime
import json
from dateutil.relativedelta import relativedelta
from centricbrands.user_import_v2.mappers.centric_brands_time_off_type_assignment_mapper import (
    centric_brands_time_off_type_assignment)
from centricbrands.user_import_v2.mappers.centric_brands_time_off_policy_starting_policy_mapper_mapper import (
    centric_brands_time_off_policy_starting_policy_mapper)
from centricbrands.user_import_v2.mappers.centric_brands_time_off_policy_starting_policy_china_hongkong_mapper import (
    centric_brands_time_off_policy_starting_policy_china_hongkong)
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'centricbrands_user_import_add_user_time_off_{config.instance}_v2',
        description=f'CentricBrands Add User - Time Off {config.instance}_v2',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
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
            no_task='check_location_hk'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='check_location_hk',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        check_location_hk = rail.IfOperator(
            task_id='check_location_hk',
            test='''{{dag_run.conf.location | lower == 'hong kong'}}''',
            yes_task='search_matching_timeoff_hk',
            no_task='check_location_china'
        )

        search_matching_timeoff_hk = rail.PythonOperator(
            task_id='search_matching_timeoff_hk',
            python_callable=lambda dag_run: list(filter(lambda entry: entry['country'] == (
                dag_run.conf['location']).lower() and entry['hong_kong_levels'] == (
                dag_run.conf['hongkonglevels']).lower(), centric_brands_time_off_type_assignment))
        )

        check_location_china = rail.IfOperator(
            task_id='check_location_china',
            test='''{{dag_run.conf.location | lower == 'china'}}''',
            yes_task='search_matching_timeoff_china',
            no_task='search_matching_timeoff_type_in_mapper'
        )

        search_matching_timeoff_china = rail.PythonOperator(
            task_id='search_matching_timeoff_china',
            python_callable=lambda dag_run: list(filter(lambda entry: entry['country'] == (
                dag_run.conf['location']).lower() and entry['state'] == (
                dag_run.conf['stateprovince']).lower(), centric_brands_time_off_type_assignment))
        )

        search_matching_timeoff_type_in_mapper = rail.PythonOperator(
            task_id='search_matching_timeoff_type_in_mapper',
            python_callable=lambda dag_run: list(filter(lambda entry: entry['country'] == (
                dag_run.conf['location']).lower() and entry['state'] == (
                dag_run.conf['stateprovince']).lower() and entry['employeetype'] == (
                dag_run.conf['employeetype']).lower(), centric_brands_time_off_type_assignment))
        )

        required_mapper_record_with_timeoff_type = rail.PythonOperator(
            task_id='required_mapper_record_with_timeoff_type',
            python_callable=lambda: rail.result('search_matching_timeoff_hk') or rail.result(
                'search_matching_timeoff_china') or rail.result('search_matching_timeoff_type_in_mapper')
        )

        if_timeoff_type_present = rail.IfOperator(
            task_id='if_timeoff_type_present',
            test='''{{ result('required_mapper_record_with_timeoff_type') | is_truthy }}''',
            yes_task="get_integrationdate_object",
            no_task="catch_error",
        )

        def get_date_object(datestring):
            date_obj = datetime.strptime(datestring, '%m/%d/%Y')
            return {
                'day': date_obj.day,
                'month': date_obj.month,
                'year': date_obj.year,
                'date': date_obj.strftime("%Y-%m-%d"),
            }

        get_integrationdate_object = rail.PythonOperator(
            task_id='get_integrationdate_object',
            python_callable=lambda dag_run: get_date_object(
                dag_run.conf['integrationdate'] if dag_run.conf['integrationdate'] else dag_run.conf['startdate'])
        )

        get_startdate_object = rail.PythonOperator(
            task_id='get_startdate_object',
            python_callable=lambda dag_run: get_date_object(
                dag_run.conf['startdate'])
        )

        get_all_time_off_types = rail.RepliconServiceOperator(
            task_id='get_all_time_off_types',
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes"
        )

        get_timeoff_types_to_assign = rail.PythonOperator(
            task_id='get_timeoff_types_to_assign',
            python_callable=lambda: ((rail.result('required_mapper_record_with_timeoff_type')[
                                     0])['timeofftypes']).split('|')
        )

        get_timeoff_types_to_assign_with_uri = rail.PythonOperator(
            task_id='get_timeoff_types_to_assign_with_uri',
            python_callable=lambda: [{
                "name": timeoff,
                "uri": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_time_off_types'), 'displayText', timeoff.strip(), 'uri')
            } for timeoff in rail.result('get_timeoff_types_to_assign')]
        )

        assign_timeofftypes = rail.RepliconServiceOperator(
            task_id='assign_timeofftypes',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['uri'],
                "timeOffTypeUris": [timeoff['uri'] for timeoff in rail.result('get_timeoff_types_to_assign_with_uri')]
            }
        )

        foreach_timeofftype_assigned = rail.ForEachOperator(
            task_id='foreach_timeofftype_assigned',
            items=lambda: rail.result('get_timeoff_types_to_assign_with_uri'),
            start_task='get_default_timeoff_policysetschedule_for_timeofftype',
            end_task='foreach_timeofftype_assigned_end'
        )

        get_default_timeoff_policysetschedule_for_timeofftype = rail.RepliconServiceOperator(
            task_id='get_default_timeoff_policysetschedule_for_timeofftype',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ result('foreach_timeofftype_assigned').uri }}"
            }
        )

        def get_timeoffpolicies_to_assign():
            default_setschedule = rail.result(
                'get_default_timeoff_policysetschedule_for_timeofftype')
            return [{
                "effectiveDate": {
                    "day": (datetime.strptime(rail.result('get_integrationdate_object')['date'], "%Y-%m-%d") +
                            relativedelta(months=(schedule['startOffset']['offsetValue']) * 12)).day,
                    "month": (datetime.strptime(rail.result('get_integrationdate_object')['date'], "%Y-%m-%d") +
                              relativedelta(months=(schedule['startOffset']['offsetValue']) * 12)).month,
                    "year": (datetime.strptime(rail.result('get_integrationdate_object')['date'], "%Y-%m-%d") +
                             relativedelta(months=(schedule['startOffset']['offsetValue']) * 12)).year
                },
                "description": "Effective on " + (datetime.strptime(rail.result('get_integrationdate_object')['date'], "%Y-%m-%d") +
                                                  relativedelta(months=(schedule['startOffset']['offsetValue']) * 12)).strftime("%Y-%m-%d"),
                "policySet": schedule['policySet']
            } for schedule in default_setschedule]

        get_list_of_timeoffpolicies_to_assign = rail.PythonOperator(
            task_id='get_list_of_timeoffpolicies_to_assign',
            python_callable=get_timeoffpolicies_to_assign
        )

        get_all_scripts_balanceeventscripts = rail.RepliconServiceOperator(
            task_id='get_all_scripts_balanceeventscripts',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Starting Balance Set To', 'uri', '')
        )

        get_all_scriptsvalidationscripts = rail.RepliconServiceOperator(
            task_id='get_all_scriptsvalidationscripts',
            endpoint="/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Prevent balance overdraw', 'uri', '')
        )

        log_default_policyset = rail.PythonOperator(
            task_id='log_default_policyset',
            python_callable=lambda: [schedule['policySet'] for schedule in rail.result(
                'get_default_timeoff_policysetschedule_for_timeofftype')]
        )

        check_location_hongkong_china = rail.IfOperator(
            task_id='check_location_hongkong_china',
            test='''{{dag_run.conf.location | lower == "china" or dag_run.conf.location | lower == "hong kong"}}''',
            yes_task='search_timeoff_policy_in_mapper_for_timeofftype_china_hongkong',
            no_task='search_timeoff_policy_in_mapper_for_timeofftype'
        )

        search_timeoff_policy_in_mapper_for_timeofftype_china_hongkong = rail.PythonOperator(
            task_id='search_timeoff_policy_in_mapper_for_timeofftype_china_hongkong',
            python_callable=lambda dag_run: list(filter(lambda policy: policy['timeofftype'] == rail.result('foreach_timeofftype_assigned')['name'] and
                                                        policy['offset'] == '0' and policy['month'] == (
                                                            datetime.strptime(dag_run.conf['integrationdate'], '%m/%d/%Y')).strftime(
                "%B"), centric_brands_time_off_policy_starting_policy_china_hongkong))
        )

        if_timeoff_isnt_china_supplementary_hk_annual = rail.IfOperator(
            task_id='if_timeoff_isnt_china_supplementary_hk_annual',
            test=lambda: all(timeoff not in rail.result('foreach_timeofftype_assigned')[
                             'name'] for timeoff in ['Supplementary', 'HK_Annual']),
            yes_task="if_default_policy_set_present_china_hk",
            no_task="if_timeoff_supplementary_hk_annual",
        )

        if_default_policy_set_present_china_hk = rail.IfOperator(
            task_id='if_default_policy_set_present_china_hk',
            test=lambda: bool(rail.result('log_default_policyset')),
            yes_task="get_default_policy_modified_china_hk",
            no_task="if_timeoff_supplementary_hk_annual",
        )

        def get_default_policy():
            timeoff_policies_to_assign = (json.dumps(rail.result('get_list_of_timeoffpolicies_to_assign'))).replace(
                'null', '\"effective\"').replace('\"script\"', '\"scriptTarget\"')
            timeoff_policies_to_assign = '[' + timeoff_policies_to_assign + ']'
            timeoff_policies_to_assign = timeoff_policies_to_assign.replace(
                '[[', '').replace(']]', '')
            return json.loads(timeoff_policies_to_assign)

        get_default_policy_modified_china_hk = rail.PythonOperator(
            task_id='get_default_policy_modified_china_hk',
            python_callable=lambda: json.loads((json.dumps(rail.result('get_list_of_timeoffpolicies_to_assign'))).replace(
                'null', '\"effective\"').replace('\"script\"', '\"scriptTarget\"'))
        )

        put_user_timeoff_account_policysetschedule_china_hk = rail.RepliconServiceOperator(
            task_id='put_user_timeoff_account_policysetschedule_china_hk',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['uri'],
                    "timeOffTypeUri": rail.result('foreach_timeofftype_assigned')['uri']
                },
                "policySetScheduleEntries": rail.result('get_default_policy_modified_china_hk')
            }
        )

        if_timeoff_supplementary_hk_annual = rail.IfOperator(
            task_id='if_timeoff_supplementary_hk_annual',
            test=lambda: 'supplementary' in (rail.result('foreach_timeofftype_assigned')['name']).lower(
            ) or 'hk_annual' in (rail.result('foreach_timeofftype_assigned')['name']).lower(),
            yes_task="declare_variable_proratedaccrual_china_hk",
            no_task="foreach_timeofftype_assigned_end",
        )

        declare_variable_proratedaccrual_china_hk = rail.SetVariableOperator(
            task_id='declare_variable_proratedaccrual_china_hk',
            append=False,
            name='proratedaccrual',
            value=None
        )

        decalre_policy_list_china_hk = rail.SetVariableOperator(
            task_id='decalre_policy_list_china_hk',
            append=False,
            name='policylist',
            value=[]
        )

        get_difference_of_integration_and_startdate_china_hk = rail.PythonOperator(
            task_id='get_difference_of_integration_and_startdate_china_hk',
            python_callable=lambda dag_run: ((datetime.strptime(
                dag_run.conf['integrationdate'], "%m/%d/%Y") - datetime.strptime(dag_run.conf['startdate'], "%m/%d/%Y")).days)/365
        )

        if_timeoff_contains_supplementary_china_hk = rail.IfOperator(
            task_id='if_timeoff_contains_supplementary_china_hk',
            test=lambda: 'supplementary' in (rail.result(
                'foreach_timeofftype_assigned')['name']).lower(),
            yes_task="get_offset_value_if_supplementary_china_hk",
            no_task="if_timeoffname_contains_hk_annual_general_staff",
        )

        get_offset_value_if_supplementary_china_hk = rail.PythonOperator(
            task_id='get_offset_value_if_supplementary_china_hk',
            python_callable=lambda: "0" if float(
                rail.result('get_difference_of_integration_and_startdate_china_hk')) < 2 else ('2' if float(
                    rail.result('get_difference_of_integration_and_startdate_china_hk')) >= 2 and float(
                        rail.result('get_difference_of_integration_and_startdate_china_hk')) < 4 else '4')
        )

        if_timeoffname_contains_hk_annual_general_staff = rail.IfOperator(
            task_id='if_timeoffname_contains_hk_annual_general_staff',
            test=lambda: 'hk_annual leave_general staff' in (rail.result(
                'foreach_timeofftype_assigned')['name']).lower(),
            yes_task="get_offset_value_if_hk_annual_general_staff",
            no_task="get_offset_value_if_not_hk_annual_general_staff",
        )

        get_offset_value_if_hk_annual_general_staff = rail.PythonOperator(
            task_id='get_offset_value_if_hk_annual_general_staff',
            python_callable=lambda: "0" if float(
                rail.result('get_difference_of_integration_and_startdate_china_hk')) < 5 else ('5' if float(
                    rail.result('get_difference_of_integration_and_startdate_china_hk')) >= 5 and float(
                        rail.result('get_difference_of_integration_and_startdate_china_hk')) < 10 else '10')
        )

        get_offset_value_if_not_hk_annual_general_staff = rail.PythonOperator(
            task_id='get_offset_value_if_not_hk_annual_general_staff',
            python_callable=lambda: "0" if float(rail.result(
                'get_difference_of_integration_and_startdate_china_hk')) < 5 else "5"
        )

        get_final_offset_value_if_hk_annual = rail.PythonOperator(
            task_id='get_final_offset_value_if_hk_annual',
            python_callable=lambda: rail.result('get_offset_value_if_hk_annual_general_staff') if rail.result(
                'get_offset_value_if_hk_annual_general_staff') else rail.result('get_offset_value_if_not_hk_annual_general_staff')
        )

        get_final_offset_china_hk = rail.PythonOperator(
            task_id='get_final_offset_china_hk',
            python_callable=lambda: rail.result('get_offset_value_if_supplementary_china_hk') if rail.result(
                'get_offset_value_if_supplementary_china_hk') else rail.result('get_final_offset_value_if_hk_annual')
        )

        search_timeoff_policy_for_timeoff_with_offset_china_hk = rail.PythonOperator(
            task_id='search_timeoff_policy_for_timeoff_with_offset_china_hk',
            python_callable=lambda dag_run: list(filter(lambda policy: policy['timeofftype'] == rail.result('foreach_timeofftype_assigned')['name'] and
                                                        policy['offset'] == (rail.result('get_final_offset_china_hk')) and policy['month'] == (
                datetime.strptime(dag_run.conf['integrationdate'], '%m/%d/%Y')).strftime(
                "%B"), centric_brands_time_off_policy_starting_policy_china_hongkong))
        )

        def get_timeoff_balance_event_script_china_hk():
            default_policysetschedule = rail.result(
                'get_default_timeoff_policysetschedule_for_timeofftype')
            timeoffbalanceeventscript = list(filter(lambda schedule: schedule['startOffset']['offsetValue'] == int(
                rail.result('get_final_offset_china_hk')), default_policysetschedule))
            timeoffbalance_eventscript = timeoffbalanceeventscript[0]['policySet'][
                'timeOffBalanceEventScripts'] if timeoffbalanceeventscript else []
            timeoffbalance_eventscript = '[' + \
                json.dumps(timeoffbalance_eventscript) + ']'
            timeoffbalance_eventscript = timeoffbalance_eventscript.replace(
                '[[', '').replace(']]', '').replace(' ', '').replace('=>', ':')
            return timeoffbalance_eventscript

        get_timeoff_balance_event_script_for_0_year_offset_china_hk = rail.PythonOperator(
            task_id='get_timeoff_balance_event_script_for_0_year_offset_china_hk',
            python_callable=get_timeoff_balance_event_script_china_hk
        )

        def get_starting_balance_script_china_hk():
            return json.dumps({
                "scriptTarget": {
                    "uri": rail.result('get_all_scripts_balanceeventscripts')
                },
                "additionalParameters": [
                    {
                        "keyUri": "urn:replicon:script-key:parameter:amount",
                        "value": {
                            "number": rail.result('search_timeoff_policy_for_timeoff_with_offset_china_hk')[0]['balance']
                        }
                    },
                    {
                        "keyUri": "urn:replicon:script-key:parameter:precedence",
                        "value": {
                            "number": "10"
                        }
                    }
                ]
            }) + ',' + rail.result('get_timeoff_balance_event_script_for_0_year_offset_china_hk')

        get_value_for_startingbalance_setto_china_hk = rail.PythonOperator(
            task_id='get_value_for_startingbalance_setto_china_hk',
            python_callable=get_starting_balance_script_china_hk
        )

        def get_modifiedbalancescript_china_hk():
            default_setschedule = rail.result(
                'get_default_timeoff_policysetschedule_for_timeofftype')
            required_schedule = list(filter(lambda schedule: schedule['startOffset']['offsetValue'] == int(
                rail.result('get_final_offset_china_hk')), default_setschedule))
            required_policyset = required_schedule[0]['policySet']
            required_policyset = json.dumps(required_policyset)
            required_policyset = required_policyset.replace(" ", "").replace(rail.result(
                'get_timeoff_balance_event_script_for_0_year_offset_china_hk'), rail.result('get_value_for_startingbalance_setto_china_hk')).replace('=>', ':').replace("China_StatutoryAnnualLeave", "China_Statutory Annual Leave")
            return required_policyset

        get_modifiedbalancescriptfor_startingbalancesetto_withoutproration_china_hk = rail.PythonOperator(
            task_id='get_modifiedbalancescriptfor_startingbalancesetto_withoutproration_china_hk',
            python_callable=get_modifiedbalancescript_china_hk
        )

        parse_json_final_script_china_hk = rail.PythonOperator(
            task_id='parse_json_final_script_china_hk',
            python_callable=lambda: json.loads(rail.result(
                'get_modifiedbalancescriptfor_startingbalancesetto_withoutproration_china_hk'))
        )

        declare_index_for_interation_china_hk = rail.SetVariableOperator(
            task_id='declare_index_for_interation_china_hk',
            name='index',
            append=False,
            value=0
        )

        foreach_set_schedule_china_hk = rail.ForEachOperator(
            task_id='foreach_set_schedule_china_hk',
            items="{{ result('get_default_timeoff_policysetschedule_for_timeofftype') | to_json}}",
            start_task='if_first_set_schedule_china_hk',
            end_task='foreach_set_schedule_end_china_hk'
        )

        if_first_set_schedule_china_hk = rail.IfOperator(
            task_id='if_first_set_schedule_china_hk',
            test=lambda: rail.get_dag_run_var('index') == 0,
            yes_task="insert_to_policylist_china_hk",
            no_task="get_numberof_months_toadd_china_hk",
        )

        insert_to_policylist_china_hk = rail.SetVariableOperator(
            task_id='insert_to_policylist_china_hk',
            append=True,
            name='{{ result("decalre_policy_list_china_hk").name }}',
            value=lambda: {
                "description": "Effective on " + str(rail.result('get_integrationdate_object')['month']) + "/" +
                    str(rail.result('get_integrationdate_object')[
                        'day']) + "/" + str(rail.result('get_integrationdate_object')['year']),
                "effectiveDate": {
                    "day": rail.result('get_integrationdate_object')['day'],
                    "month": rail.result('get_integrationdate_object')['month'],
                    "year": rail.result('get_integrationdate_object')['year']
                },
                "policySet": rail.result('parse_json_final_script_china_hk')
            }
        )

        get_numberof_months_toadd_china_hk = rail.PythonOperator(
            task_id='get_numberof_months_toadd_china_hk',
            python_callable=lambda: rail.result('foreach_set_schedule_china_hk')[
                'startOffset']['offsetValue'] * 12
        )

        get_effective_date_china_hk = rail.PythonOperator(
            task_id='get_effective_date_china_hk',
            python_callable=lambda dag_run: get_date_object((datetime.strptime(
                dag_run.conf['startdate'], "%m/%d/%Y") + relativedelta(months=rail.result('get_numberof_months_toadd_china_hk'))).strftime("%m/%d/%Y"))
        )

        if_effectivedate_greater_than_integrationdate_china_hk = rail.IfOperator(
            task_id='if_effectivedate_greater_than_integrationdate_china_hk',
            test=lambda dag_run: (datetime.strptime(dag_run.conf['startdate'], "%m/%d/%Y") + relativedelta(
                months=rail.result('get_numberof_months_toadd_china_hk'))) > datetime.strptime(dag_run.conf['integrationdate'], "%m/%d/%Y"),
            yes_task="insert_into_policy_list_china_hk",
            no_task="increase_index_china_hk",
        )

        insert_into_policy_list_china_hk = rail.SetVariableOperator(
            task_id='insert_into_policy_list_china_hk',
            append=True,
            name='{{ result("decalre_policy_list_china_hk").name }}',
            value=lambda: {
                "description": "Effective On " + (rail.result('get_effective_date_china_hk'))['date'],
                "effectiveDate": {
                    "day": rail.result('get_effective_date_china_hk')['day'],
                    "month": rail.result('get_effective_date_china_hk')['month'],
                    "year": rail.result('get_effective_date_china_hk')['year']
                },
                "policySet": rail.result('foreach_set_schedule_china_hk')['policySet']
            }
        )

        increase_index_china_hk = rail.SetVariableOperator(
            task_id='increase_index_china_hk',
            name='index',
            append=False,
            value=lambda: rail.get_dag_run_var('index') + 1
        )

        foreach_set_schedule_end_china_hk = rail.EmptyOperator(
            task_id='foreach_set_schedule_end_china_hk',
        )

        parse_json_85_china_hk = rail.PythonOperator(
            task_id='parse_json_85_china_hk',
            python_callable=lambda: [{
                "timeOffBalanceEventScripts": [],
                "timeOffValidationScripts": [
                    {
                        "scriptTarget": {
                            "uri": rail.result('get_all_scriptsvalidationscripts')
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

        if_startdate_unequal_integrationdate_china_hk = rail.IfOperator(
            task_id='if_startdate_unequal_integrationdate_china_hk',
            test=lambda dag_run: datetime.strptime(
                dag_run.conf['startdate'], "%m/%d/%Y") != datetime.strptime(dag_run.conf['integrationdate'], "%m/%d/%Y"),
            yes_task="insert_to_policy_list_china_hk",
            no_task="get_final_policyset_china_hk",
        )

        insert_to_policy_list_china_hk = rail.SetVariableOperator(
            task_id='insert_to_policy_list_china_hk',
            append=True,
            name='{{ result("decalre_policy_list_china_hk").name }}',
            value=lambda: {
                "description": "Effective on " + str(rail.result('get_startdate_object')['month']) + "/" + str(rail.result('get_startdate_object')['day']) +
                    "/" + str(rail.result('get_startdate_object')['year']),
                "effectiveDate": {
                    "day": rail.result('get_startdate_object')['day'],
                    "month": rail.result('get_startdate_object')['month'],
                    "year": rail.result('get_startdate_object')['year']
                },
                "policySet": rail.result('parse_json_85_china_hk')[0]
            }
        )

        get_final_policyset_china_hk = rail.PythonOperator(
            task_id='get_final_policyset_china_hk',
            python_callable=lambda: json.loads((json.dumps(rail.get_dag_run_var('policylist'))).replace(
                'null', '\"effective\"').replace('\"script\"', '\"scriptTarget\"'))
        )

        put_user_time_off_account_policy_set_schedule_china_hk = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_china_hk',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['uri'],
                    "timeOffTypeUri": rail.result('foreach_timeofftype_assigned')['uri']
                },
                "policySetScheduleEntries": rail.result('get_final_policyset_china_hk')
            }
        )

        search_timeoff_policy_in_mapper_for_timeofftype = rail.PythonOperator(
            task_id='search_timeoff_policy_in_mapper_for_timeofftype',
            python_callable=lambda dag_run: list(filter(lambda policy: policy['timeofftype'] == rail.result('foreach_timeofftype_assigned')['name'] and
                                                        policy['offset'] == '0' and policy['month'] == (
                                                            datetime.strptime(dag_run.conf['integrationdate'], '%m/%d/%Y')).strftime(
                "%B"), centric_brands_time_off_policy_starting_policy_mapper))
        )

        if_timeoff_isnt_pto_sick_vacation = rail.IfOperator(
            task_id='if_timeoff_isnt_pto_sick_vacation',
            test=lambda: all(timeoff not in rail.result('foreach_timeofftype_assigned')[
                             'name'] for timeoff in ['PTO', 'Sick', 'Vacation']),
            yes_task="if_default_policy_set_present",
            no_task="if_timeoff_is_sick",
        )

        if_default_policy_set_present = rail.IfOperator(
            task_id='if_default_policy_set_present',
            test=lambda: bool(rail.result('log_default_policyset')),
            yes_task="get_default_policy_modified",
            no_task="if_timeoff_is_sick",
        )

        get_default_policy_modified = rail.PythonOperator(
            task_id='get_default_policy_modified',
            python_callable=get_default_policy
        )

        put_user_timeoff_account_policysetschedule = rail.RepliconServiceOperator(
            task_id='put_user_timeoff_account_policysetschedule',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['uri'],
                    "timeOffTypeUri": rail.result('foreach_timeofftype_assigned')['uri']
                },
                "policySetScheduleEntries": [rail.result('get_default_policy_modified')]
            }
        )

        if_timeoff_is_sick = rail.IfOperator(
            task_id='if_timeoff_is_sick',
            test=lambda: 'sick' in (rail.result(
                'foreach_timeofftype_assigned')['name']).lower(),
            yes_task="get_defaultpolicy_modified",
            no_task="if_timeoff_pto_or_vacation",
        )

        def get_modified_default_policy():
            originalbalance = '"keyUri": "urn:replicon:script-key:parameter:amount", "value": {"number": 0.0}}'
            newbalance = '"keyUri": "urn:replicon:script-key:parameter:amount","value":{"number":' + str(
                rail.result('search_timeoff_policy_in_mapper_for_timeofftype')[0]['balance']) + '}}'
            timeoff_policies_to_assign = (json.dumps(rail.result('get_list_of_timeoffpolicies_to_assign'))).replace(
                'null', '\"effective\"').replace('\"script\"', '\"scriptTarget\"').replace(originalbalance, newbalance)
            timeoff_policies_to_assign = '[' + timeoff_policies_to_assign + ']'
            timeoff_policies_to_assign = timeoff_policies_to_assign.replace(
                '[[', '').replace(']]', '')
            return json.loads(timeoff_policies_to_assign)

        get_defaultpolicy_modified = rail.PythonOperator(
            task_id='get_defaultpolicy_modified',
            python_callable=get_modified_default_policy
        )

        put_user_timeoff_accountpolicy_set_schedule = rail.RepliconServiceOperator(
            task_id='put_user_timeoff_accountpolicy_set_schedule',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['uri'],
                    "timeOffTypeUri": rail.result('foreach_timeofftype_assigned')['uri']
                },
                "policySetScheduleEntries": [rail.result('get_defaultpolicy_modified')]
            }
        )

        if_timeoff_pto_or_vacation = rail.IfOperator(
            task_id='if_timeoff_pto_or_vacation',
            test=lambda: 'pto' in (rail.result('foreach_timeofftype_assigned')['name']).lower(
            ) or 'vacation' in (rail.result('foreach_timeofftype_assigned')['name']).lower(),
            yes_task="declare_variable_proratedaccrual",
            no_task="foreach_timeofftype_assigned_end",
        )

        declare_variable_proratedaccrual = rail.SetVariableOperator(
            task_id='declare_variable_proratedaccrual',
            append=False,
            name='proratedaccrual',
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
                dag_run.conf['integrationdate'], "%m/%d/%Y") - datetime.strptime(dag_run.conf['startdate'], "%m/%d/%Y")).days)/365
        )

        if_difference_greater_than_0 = rail.IfOperator(
            task_id='if_difference_greater_than_0',
            test=lambda: float(rail.result(
                'get_difference_of_integration_and_startdate')) > 0,
            yes_task="set_prorated_accrual_yes",
            no_task="if_integrationdate_day_greater_than_1",
        )

        set_prorated_accrual_yes = rail.SetVariableOperator(
            task_id='set_prorated_accrual_yes',
            append=False,
            name='{{ result("declare_variable_proratedaccrual").name }}',
            value='yes'
        )

        if_integrationdate_day_greater_than_1 = rail.IfOperator(
            task_id='if_integrationdate_day_greater_than_1',
            test=lambda: rail.result('get_integrationdate_object')['day'] > 1,
            yes_task="set_proratedaccrual_yes",
            no_task="if_proratedaccrual_equals_yes",
        )

        set_proratedaccrual_yes = rail.SetVariableOperator(
            task_id='set_proratedaccrual_yes',
            append=False,
            name='{{ result("declare_variable_proratedaccrual").name }}',
            value='yes'
        )

        if_proratedaccrual_equals_yes = rail.IfOperator(
            task_id='if_proratedaccrual_equals_yes',
            test=lambda: rail.get_dag_run_var('proratedaccrual') == 'yes',
            yes_task="if_timeoff_contains_pto",
            no_task="if_proratedaccrual_not_equals_yes_and_difference_equals_0",
        )

        if_timeoff_contains_pto = rail.IfOperator(
            task_id='if_timeoff_contains_pto',
            test=lambda: 'pto' in (rail.result(
                'foreach_timeofftype_assigned')['name']).lower(),
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
            test=lambda: 'hourly' in (rail.result(
                'foreach_timeofftype_assigned')['name']).lower(),
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
                'get_difference_of_integration_and_startdate')) > 3 and float(rail.result('get_difference_of_integration_and_startdate')) < 10 else '10')
        )

        get_final_offset_value_if_vacationhourly = rail.PythonOperator(
            task_id='get_final_offset_value_if_vacationhourly',
            python_callable=lambda: rail.result('get_offset_value_if_vacationhourly') if rail.result(
                'get_offset_value_if_vacationhourly') else rail.result('get_offset_value_if_vacation_hourly')
        )

        get_final_offset = rail.PythonOperator(
            task_id='get_final_offset',
            python_callable=lambda: rail.result('get_offset_value_if_pto') if rail.result('get_offset_value_if_pto') else rail.result(
                'get_final_offset_value_if_vacationhourly')
        )

        search_timeoff_policy_for_timeoff_with_offset = rail.PythonOperator(
            task_id='search_timeoff_policy_for_timeoff_with_offset',
            python_callable=lambda dag_run: list(filter(lambda policy: policy['timeofftype'] == rail.result('foreach_timeofftype_assigned')['name'] and
                                                        policy['offset'] == (rail.result('get_final_offset')) and policy['month'] == (
                datetime.strptime(dag_run.conf['integrationdate'], '%m/%d/%Y')).strftime(
                "%B"), centric_brands_time_off_policy_starting_policy_mapper))
        )

        def get_timeoff_balance_event_script():
            default_policysetschedule = rail.result(
                'get_default_timeoff_policysetschedule_for_timeofftype')
            timeoffbalanceeventscript = list(filter(lambda schedule: schedule['startOffset']['offsetValue'] == int(
                rail.result('get_final_offset')), default_policysetschedule))
            timeoffbalance_eventscript = timeoffbalanceeventscript[0]['policySet'][
                'timeOffBalanceEventScripts'] if timeoffbalanceeventscript else []
            timeoffbalance_eventscript = '[' + \
                json.dumps(timeoffbalance_eventscript) + ']'
            timeoffbalance_eventscript = timeoffbalance_eventscript.replace(
                '[[', '').replace(']]', '').replace(' ', '').replace('=>', ':')
            return timeoffbalance_eventscript

        get_timeoff_balance_event_script_for_0_year_offset = rail.PythonOperator(
            task_id='get_timeoff_balance_event_script_for_0_year_offset',
            python_callable=get_timeoff_balance_event_script
        )

        def get_starting_balance_script():
            return json.dumps({
                "scriptTarget": {
                    "uri": rail.result('get_all_scripts_balanceeventscripts')
                },
                "additionalParameters": [
                    {
                        "keyUri": "urn:replicon:script-key:parameter:amount",
                        "value": {
                            "number": rail.result('search_timeoff_policy_for_timeoff_with_offset')[0]['balance']
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
                'get_default_timeoff_policysetschedule_for_timeofftype')
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

        declare_index_for_interation = rail.SetVariableOperator(
            task_id='declare_index_for_interation',
            name='index',
            append=False,
            value=0
        )

        foreach_set_schedule = rail.ForEachOperator(
            task_id='foreach_set_schedule',
            items="{{ result('get_default_timeoff_policysetschedule_for_timeofftype') | to_json}}",
            start_task='if_first_set_schedule',
            end_task='foreach_set_schedule_end'
        )

        if_first_set_schedule = rail.IfOperator(
            task_id='if_first_set_schedule',
            test=lambda: rail.get_dag_run_var('index') == 0,
            yes_task="insert_to_ptopolicylist",
            no_task="get_numberof_months_toadd",
        )

        insert_to_ptopolicylist = rail.SetVariableOperator(
            task_id='insert_to_ptopolicylist',
            append=True,
            name='{{ result("decalre_ptopolicy_list").name }}',
            value=lambda: {
                "description": "Effective on " + str(rail.result('get_integrationdate_object')['month']) + "/" +
                    str(rail.result('get_integrationdate_object')[
                        'day']) + "/" + str(rail.result('get_integrationdate_object')['year']),
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
            yes_task="insert_to_ptopolicy_list",
            no_task="increase_index",
        )

        insert_to_ptopolicy_list = rail.SetVariableOperator(
            task_id='insert_to_ptopolicy_list',
            append=True,
            name='{{ result("decalre_ptopolicy_list").name }}',
            value=lambda: {
                "description": "Effective On " + (rail.result('get_effective_date'))['date'],
                "effectiveDate": {
                    "day": rail.result('get_effective_date')['day'],
                    "month": rail.result('get_effective_date')['month'],
                    "year": rail.result('get_effective_date')['year']
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

        if_startdate_unequal_integrationdate = rail.IfOperator(
            task_id='if_startdate_unequal_integrationdate',
            test=lambda dag_run: datetime.strptime(
                dag_run.conf['startdate'], "%m/%d/%Y") != datetime.strptime(dag_run.conf['integrationdate'], "%m/%d/%Y"),
            yes_task="parse_json_85",
            no_task="if_integrationdate_month_greater_than_1_and_not_lastday_of_year",
        )

        parse_json_85 = rail.PythonOperator(
            task_id='parse_json_85',
            python_callable=lambda: [{
                "timeOffBalanceEventScripts": [
                    {
                        "scriptTarget": {
                            "uri": rail.result('get_all_scripts_balanceeventscripts')
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
                            "uri": rail.result('get_all_scriptsvalidationscripts')
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

        insert_to_pto_policy_list = rail.SetVariableOperator(
            task_id='insert_to_pto_policy_list',
            append=True,
            name='{{ result("decalre_ptopolicy_list").name }}',
            value=lambda: {
                "description": "Effective on " + str(rail.result('get_startdate_object')['month']) + "/" + str(rail.result('get_startdate_object')['day']) +
                    "/" + str(rail.result('get_startdate_object')['year']),
                "effectiveDate": {
                    "day": rail.result('get_startdate_object')['day'],
                    "month": rail.result('get_startdate_object')['month'],
                    "year": rail.result('get_startdate_object')['year']
                },
                "policySet": rail.result('parse_json_85')[0]
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
                'get_default_timeoff_policysetschedule_for_timeofftype')
            required_schedule = list(filter(lambda schedule: schedule['startOffset']['offsetValue'] == int(
                rail.result('get_final_offset')), default_setschedule))
            return required_schedule[0]['policySet']

        get_timeoffbalanceeventscript_for0yearoffset_modified = rail.PythonOperator(
            task_id='get_timeoffbalanceeventscript_for0yearoffset_modified',
            python_callable=get_timeoff_balanceevent_script_for0yearoffset_modified
        )

        insertto_pto_policy_list = rail.SetVariableOperator(
            task_id='insertto_pto_policy_list',
            append=True,
            name='{{ result("decalre_ptopolicy_list").name }}',
            value=lambda: {
                "description": "Effective on 12/31/" + str(rail.result('get_integrationdate_object')['year']),
                "effectiveDate": {
                    "day": 31,
                    "month": 12,
                    "year": rail.result('get_integrationdate_object')['year']
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
                    "timeOffTypeUri": rail.result('foreach_timeofftype_assigned')['uri']
                },
                "policySetScheduleEntries": rail.result('get_final_policyset')
            }
        )

        if_proratedaccrual_not_equals_yes_and_difference_equals_0 = rail.IfOperator(
            task_id='if_proratedaccrual_not_equals_yes_and_difference_equals_0',
            test=lambda: rail.get_dag_run_var('proratedaccrual') != 'yes' and float(
                rail.result('get_difference_of_integration_and_startdate')) == 0,
            yes_task="foreach_default_set_schedule",
            no_task="foreach_timeofftype_assigned_end",
        )

        foreach_default_set_schedule = rail.ForEachOperator(
            task_id='foreach_default_set_schedule',
            items="{{ result('get_default_timeoff_policysetschedule_for_timeofftype') | to_json}}",
            start_task='get_effectivedate',
            end_task='foreach_default_set_schedule_end'
        )

        get_effectivedate = rail.PythonOperator(
            task_id='get_effectivedate',
            python_callable=lambda dag_run: get_date_object((datetime.strptime(dag_run.conf['startdate'], '%m/%d/%Y') + relativedelta(
                months=rail.result('foreach_default_set_schedule')['startOffset']['offsetValue'] * 12)).strftime("%m/%d/%Y"))
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
            python_callable=lambda: json.loads((json.dumps(rail.get_dag_run_var('ptopolicylist'))).replace(
                'null', '\"effective\"').replace('\"script\"', '\"scriptTarget\"'))
        )

        put_user_time_off_account_policysetschedule = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policysetschedule',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['uri'],
                    "timeOffTypeUri": rail.result('foreach_timeofftype_assigned')['uri']
                },
                "policySetScheduleEntries": rail.result('log_final_policyset')
            }
        )

        foreach_timeofftype_assigned_end = rail.EmptyOperator(
            task_id='foreach_timeofftype_assigned_end',
        )

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "{{get_error_message()}}")
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error
        can_run_batch_task >> rail.Label(
            'No') >> check_location_hk
        check_location_hk >> rail.Label(
            'Yes') >> search_matching_timeoff_hk >> required_mapper_record_with_timeoff_type
        check_location_hk >> rail.Label('No') >> check_location_china
        check_location_china >> rail.Label(
            'Yes') >> search_matching_timeoff_china >> required_mapper_record_with_timeoff_type
        check_location_china >> rail.Label(
            'No') >> search_matching_timeoff_type_in_mapper >> required_mapper_record_with_timeoff_type
        required_mapper_record_with_timeoff_type >> if_timeoff_type_present
        if_timeoff_type_present >> rail.Label(
            'Yes') >> get_integrationdate_object >> get_startdate_object >> get_all_time_off_types
        get_all_time_off_types >> get_timeoff_types_to_assign >> get_timeoff_types_to_assign_with_uri >> assign_timeofftypes
        assign_timeofftypes >> foreach_timeofftype_assigned >> get_default_timeoff_policysetschedule_for_timeofftype >> get_list_of_timeoffpolicies_to_assign
        get_list_of_timeoffpolicies_to_assign >> get_all_scripts_balanceeventscripts >> get_all_scriptsvalidationscripts >> log_default_policyset
        log_default_policyset >> check_location_hongkong_china
        check_location_hongkong_china >> rail.Label(
            'No') >> search_timeoff_policy_in_mapper_for_timeofftype
        check_location_hongkong_china >> rail.Label(
            'Yes') >> search_timeoff_policy_in_mapper_for_timeofftype_china_hongkong >> if_timeoff_isnt_china_supplementary_hk_annual
        if_timeoff_isnt_china_supplementary_hk_annual >> rail.Label(
            'Yes') >> if_default_policy_set_present_china_hk
        if_default_policy_set_present_china_hk >> rail.Label('Yes') >> get_default_policy_modified_china_hk \
            >> put_user_timeoff_account_policysetschedule_china_hk >> if_timeoff_supplementary_hk_annual
        if_default_policy_set_present_china_hk >> rail.Label(
            'No') >> if_timeoff_supplementary_hk_annual
        if_timeoff_isnt_china_supplementary_hk_annual >> rail.Label(
            'No') >> if_timeoff_supplementary_hk_annual
        if_timeoff_supplementary_hk_annual >> rail.Label(
            'No') >> foreach_timeofftype_assigned_end
        if_timeoff_supplementary_hk_annual >> rail.Label('Yes') >> declare_variable_proratedaccrual_china_hk \
            >> decalre_policy_list_china_hk >> get_difference_of_integration_and_startdate_china_hk >> if_timeoff_contains_supplementary_china_hk
        if_timeoff_contains_supplementary_china_hk >> rail.Label(
            'Yes') >> get_offset_value_if_supplementary_china_hk >> get_final_offset_china_hk
        if_timeoff_contains_supplementary_china_hk >> rail.Label(
            'No') >> if_timeoffname_contains_hk_annual_general_staff
        if_timeoffname_contains_hk_annual_general_staff >> rail.Label(
            'No') >> get_offset_value_if_not_hk_annual_general_staff >> get_final_offset_value_if_hk_annual
        if_timeoffname_contains_hk_annual_general_staff >> rail.Label(
            'Yes') >> get_offset_value_if_hk_annual_general_staff >> get_final_offset_value_if_hk_annual >> get_final_offset_china_hk
        get_final_offset_china_hk >> search_timeoff_policy_for_timeoff_with_offset_china_hk \
            >> get_timeoff_balance_event_script_for_0_year_offset_china_hk \
            >> get_value_for_startingbalance_setto_china_hk >> get_modifiedbalancescriptfor_startingbalancesetto_withoutproration_china_hk
        get_modifiedbalancescriptfor_startingbalancesetto_withoutproration_china_hk >> parse_json_final_script_china_hk \
            >> declare_index_for_interation_china_hk >> foreach_set_schedule_china_hk >> if_first_set_schedule_china_hk
        if_first_set_schedule_china_hk >> rail.Label(
            'Yes') >> insert_to_policylist_china_hk >> increase_index_china_hk
        if_first_set_schedule_china_hk >> rail.Label(
            'No') >> get_numberof_months_toadd_china_hk >> get_effective_date_china_hk >> if_effectivedate_greater_than_integrationdate_china_hk
        if_effectivedate_greater_than_integrationdate_china_hk >> rail.Label(
            'No') >> increase_index_china_hk
        if_effectivedate_greater_than_integrationdate_china_hk >> rail.Label(
            'Yes') >> insert_into_policy_list_china_hk >> increase_index_china_hk >> foreach_set_schedule_end_china_hk
        foreach_set_schedule_china_hk >> foreach_set_schedule_end_china_hk >> parse_json_85_china_hk >> if_startdate_unequal_integrationdate_china_hk
        if_startdate_unequal_integrationdate_china_hk >> rail.Label(
            'No') >> get_final_policyset_china_hk
        if_startdate_unequal_integrationdate_china_hk >> rail.Label(
            'Yes') >> insert_to_policy_list_china_hk >> get_final_policyset_china_hk
        get_final_policyset_china_hk >> put_user_time_off_account_policy_set_schedule_china_hk >> foreach_timeofftype_assigned_end

        search_timeoff_policy_in_mapper_for_timeofftype >> if_timeoff_isnt_pto_sick_vacation
        if_timeoff_isnt_pto_sick_vacation >> rail.Label(
            'Yes') >> if_default_policy_set_present
        if_default_policy_set_present >> rail.Label(
            'Yes') >> get_default_policy_modified >> put_user_timeoff_account_policysetschedule >> if_timeoff_is_sick
        if_default_policy_set_present >> rail.Label('No') >> if_timeoff_is_sick
        if_timeoff_isnt_pto_sick_vacation >> rail.Label(
            'No') >> if_timeoff_is_sick
        if_timeoff_is_sick >> rail.Label(
            'Yes') >> get_defaultpolicy_modified >> put_user_timeoff_accountpolicy_set_schedule >> if_timeoff_pto_or_vacation
        if_timeoff_is_sick >> rail.Label('No') >> if_timeoff_pto_or_vacation
        if_timeoff_pto_or_vacation >> rail.Label(
            'Yes') >> declare_variable_proratedaccrual >> decalre_ptopolicy_list >> get_difference_of_integration_and_startdate >> if_difference_greater_than_0
        if_difference_greater_than_0 >> rail.Label(
            'Yes') >> set_prorated_accrual_yes >> if_integrationdate_day_greater_than_1
        if_difference_greater_than_0 >> rail.Label(
            'No') >> if_integrationdate_day_greater_than_1
        if_integrationdate_day_greater_than_1 >> rail.Label(
            'Yes') >> set_proratedaccrual_yes >> if_proratedaccrual_equals_yes
        if_integrationdate_day_greater_than_1 >> rail.Label(
            'No') >> if_proratedaccrual_equals_yes
        if_proratedaccrual_equals_yes >> rail.Label(
            'Yes') >> if_timeoff_contains_pto
        if_timeoff_contains_pto >> rail.Label(
            'Yes') >> get_offset_value_if_pto >> get_final_offset
        if_timeoff_contains_pto >> rail.Label(
            'No') >> if_timeoffname_contains_hourly
        if_timeoffname_contains_hourly >> rail.Label(
            'Yes') >> get_offset_value_if_vacationhourly >> get_final_offset_value_if_vacationhourly
        if_timeoffname_contains_hourly >> rail.Label(
            'No') >> get_offset_value_if_vacation_hourly >> get_final_offset_value_if_vacationhourly >> get_final_offset
        get_final_offset >> search_timeoff_policy_for_timeoff_with_offset >> get_timeoff_balance_event_script_for_0_year_offset
        get_timeoff_balance_event_script_for_0_year_offset >> get_value_for_startingbalance_setto >> if_integrationeffectivedate_day_equals_1
        if_integrationeffectivedate_day_equals_1 >> rail.Label(
            'Yes') >> get_modifiedbalancescript_forstartingbalancesetto_withprorationat_startofpolicy >> get_finalbalancescriptfor_startingbalancesetto
        if_integrationeffectivedate_day_equals_1 >> rail.Label(
            'No') >> get_modifiedbalancescriptfor_startingbalancesetto_withoutproration >> get_finalbalancescriptfor_startingbalancesetto
        get_finalbalancescriptfor_startingbalancesetto >> parse_json_final_script >> declare_index_for_interation
        declare_index_for_interation >> foreach_set_schedule >> if_first_set_schedule
        if_first_set_schedule >> rail.Label(
            'Yes') >> insert_to_ptopolicylist >> increase_index >> foreach_set_schedule_end
        if_first_set_schedule >> rail.Label(
            'No') >> get_numberof_months_toadd >> get_effective_date >> if_effectivedate_greater_than_integrationdate
        if_effectivedate_greater_than_integrationdate >> rail.Label(
            'Yes') >> insert_to_ptopolicy_list >> increase_index >> foreach_set_schedule_end
        if_effectivedate_greater_than_integrationdate >> rail.Label(
            'No') >> increase_index >> foreach_set_schedule_end
        foreach_set_schedule >> foreach_set_schedule_end >> if_startdate_unequal_integrationdate
        if_startdate_unequal_integrationdate >> rail.Label(
            'Yes') >> parse_json_85 >> insert_to_pto_policy_list >> if_integrationdate_month_greater_than_1_and_not_lastday_of_year
        if_startdate_unequal_integrationdate >> rail.Label(
            'No') >> if_integrationdate_month_greater_than_1_and_not_lastday_of_year
        if_integrationdate_month_greater_than_1_and_not_lastday_of_year >> rail.Label(
            'Yes') >> get_timeoffbalanceeventscript_for0yearoffset_modified >> insertto_pto_policy_list >> get_final_policyset
        if_integrationdate_month_greater_than_1_and_not_lastday_of_year >> rail.Label(
            'No') >> get_final_policyset >> put_user_time_off_account_policy_set_schedule >> if_proratedaccrual_not_equals_yes_and_difference_equals_0
        if_proratedaccrual_equals_yes >> rail.Label(
            'No') >> if_proratedaccrual_not_equals_yes_and_difference_equals_0
        if_proratedaccrual_not_equals_yes_and_difference_equals_0 >> rail.Label(
            'Yes') >> foreach_default_set_schedule >> get_effectivedate >> insertto_pto_policylist >> foreach_default_set_schedule_end
        foreach_default_set_schedule >> foreach_default_set_schedule_end >> log_final_policyset >> put_user_time_off_account_policysetschedule
        put_user_time_off_account_policysetschedule >> foreach_timeofftype_assigned_end
        if_proratedaccrual_not_equals_yes_and_difference_equals_0 >> rail.Label(
            'No') >> foreach_timeofftype_assigned_end
        if_timeoff_pto_or_vacation >> rail.Label(
            'No') >> foreach_timeofftype_assigned_end
        foreach_timeofftype_assigned >> foreach_timeofftype_assigned_end >> catch_error
        if_timeoff_type_present >> rail.Label(
            'No') >> catch_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
