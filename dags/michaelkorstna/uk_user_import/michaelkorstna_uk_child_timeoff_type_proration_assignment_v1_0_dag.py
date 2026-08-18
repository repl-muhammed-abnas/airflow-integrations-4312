
from datetime import timedelta, datetime
import json
from airflow.models import Variable
import rail
from dateutil.relativedelta import relativedelta
null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'michaelkorstna_uk_user_import_timeoff_type_proration_assignment_child_{config.instance}',
        description=f'MichaelKorsTnA UK_Child Timeoff type Proration Assignment v1.0 {config.instance}',
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
            no_task='get_default_time_off_type_policy_schedule_for_user_4'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_default_time_off_type_policy_schedule_for_user_4',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_default_time_off_type_policy_schedule_for_user_4 = rail.RepliconServiceOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user_4',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
                }
            }
        )

        if_effectivedate_day_present_6 = rail.IfOperator(
            task_id='if_effectivedate_day_present_6',
            test=lambda: bool(rail.result('get_default_time_off_type_policy_schedule_for_user_4') and rail.result(
                'get_default_time_off_type_policy_schedule_for_user_4')[0]['effectiveDate']['day']),
            yes_task="log_gettheaccrualbalancesetup_7",
            no_task="catch_and_log_error",
        )

        def get_accrualbalance_setup():
            defaultschedule = rail.result(
                'get_default_time_off_type_policy_schedule_for_user_4')[0]
            requiredscript = rail.find_first_by_attr_and_get_attr(
                defaultschedule['policySet']['timeOffBalanceEventScripts'], 'script.name', 'Yearly Accrual', 'additionalParameters', '')
            return (json.dumps(requiredscript)).replace("[[", "[").replace("]]", "]")

        log_gettheaccrualbalancesetup_7 = rail.PythonOperator(
            task_id='log_gettheaccrualbalancesetup_7',
            python_callable=get_accrualbalance_setup
        )

        log_gettheaccrualbalance_9 = rail.PythonOperator(
            task_id='log_gettheaccrualbalance_9',
            python_callable=lambda: float(rail.find_first_by_attr_and_get_attr(json.loads(rail.result(
                'log_gettheaccrualbalancesetup_7')), 'keyUri', 'urn:replicon:script-key:parameter:accrual-annual-amount', 'value.number'))
        )

        log_existing_accrual_10 = rail.PythonOperator(
            task_id='log_existing_accrual_10',
            python_callable=lambda:  '{"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": ' + str(
                rail.result('log_gettheaccrualbalance_9')) + '}}'
        )

        def get_starting_balance_setup():
            defaultschedule = rail.result(
                'get_default_time_off_type_policy_schedule_for_user_4')[0]
            requiredscript = rail.find_first_by_attr_and_get_attr(
                defaultschedule['policySet']['timeOffBalanceEventScripts'], 'script.name', 'Starting Balance Set To', 'additionalParameters', '')
            return (json.dumps(requiredscript)).replace("[[", "[").replace("]]", "]")

        log_getthestartingbalancesetup_11 = rail.PythonOperator(
            task_id='log_getthestartingbalancesetup_11',
            python_callable=get_starting_balance_setup
        )

        def get_starting_balance_script():
            defaultschedule = rail.result(
                'get_default_time_off_type_policy_schedule_for_user_4')[0]
            requiredscript = rail.find_first_by_attr_and_get_attr(
                defaultschedule['policySet']['timeOffBalanceEventScripts'], 'script.name', 'Starting Balance Set To')
            return (json.dumps(requiredscript)).replace('[{"additionalParameters"', '{"additionalParameters"').replace("}}]", "}}").replace(
                '}},"script"', '}}],"script"')

        log_getthestartingbalancescript_12 = rail.PythonOperator(
            task_id='log_getthestartingbalancescript_12',
            python_callable=get_starting_balance_script
        )

        log_getthestartingbalance_14 = rail.PythonOperator(
            task_id='log_getthestartingbalance_14',
            python_callable=lambda: float(rail.find_first_by_attr_and_get_attr(json.loads(rail.result(
                'log_gettheaccrualbalancesetup_7')), 'keyUri', 'urn:replicon:script-key:parameter:amount', 'value.number', 0))
        )

        log_existing_starting_balance_15 = rail.PythonOperator(
            task_id='log_existing_starting_balance_15',
            python_callable=lambda:  '{"keyUri": "urn:replicon:script-key:parameter:amount", "value": {"number": ' + str(
                rail.result('log_getthestartingbalance_14')) + '}}'
        )

        log_required_numberofdaysforprorationcalculation_16 = rail.PythonOperator(
            task_id='log_required_numberofdaysforprorationcalculation_16',
            python_callable=lambda dag_run: (((datetime.strptime(dag_run.conf['startdate'], "%d/%m/%Y") + relativedelta(
                months=12)).replace(day=1, month=1)) - datetime.strptime(dag_run.conf['startdate'], "%d/%m/%Y")).days
        )

        log_required_accrual_17 = rail.PythonOperator(
            task_id='log_required_accrual_17',
            python_callable=lambda dag_run: round(float(float(
                dag_run.conf['scheduledweeklyhours'])/40) * float(rail.result('log_gettheaccrualbalance_9')))
        )

        log_required_accrual_json_18 = rail.PythonOperator(
            task_id='log_required_accrual_json_18',
            python_callable=lambda: '{"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": ' + str(
                rail.result('log_required_accrual_17')) + '}}'
        )

        if_request_type_equals_to_add_19 = rail.IfOperator(
            task_id='if_request_type_equals_to_add_19',
            test='''{{ dag_run.conf.type == 'Add' }}''',
            yes_task="log_required_starting_balance_20",
            no_task="if_request_type_equals_to_update_24",
        )

        log_required_starting_balance_20 = rail.PythonOperator(
            task_id='log_required_starting_balance_20',
            python_callable=lambda dag_run: 0 if datetime.strptime(dag_run.conf['startdate'],"%d/%m/%Y") == ((datetime.strptime(dag_run.conf[
                'startdate'],"%d/%m/%Y")).replace(day=1,month=1)) else ((round((float(dag_run.conf['scheduledweeklyhours'])/40) * float(rail.result(
                'log_gettheaccrualbalance_9')))) if 'sick leave' in (dag_run.conf['timeofftype'].lower()) else (round((((float(dag_run.conf[
                'scheduledweeklyhours'])/40) * float(rail.result('log_gettheaccrualbalance_9'))) / (float((((datetime.strptime(
                dag_run.conf['startdate'], "%d/%m/%Y") + relativedelta(months=12)).replace(day=1, month=1)) - timedelta(days=1)).strftime(
                "%j"))) * float(rail.result('log_required_numberofdaysforprorationcalculation_16'))))))
        )

        log_required_starting_balance_json_21 = rail.PythonOperator(
            task_id='log_required_starting_balance_json_21',
            python_callable=lambda:  '{"keyUri": "urn:replicon:script-key:parameter:amount", "value": {"number": ' + str(
                rail.result('log_required_starting_balance_20')) + '}}'
        )

        def get_timeoff_policy():
            return (json.dumps(rail.result('get_default_time_off_type_policy_schedule_for_user_4'))).replace('null', '\"effective\"').replace(
                '\"script\"', '\"scriptTarget\"').replace(rail.result('log_existing_starting_balance_15'), rail.result(
                'log_required_starting_balance_json_21')).replace(rail.result('log_existing_accrual_10'), rail.result('log_required_accrual_json_18'))

        log_timeoff_policy_22 = rail.PythonOperator(
            task_id='log_timeoff_policy_22',
            python_callable=get_timeoff_policy
        )

        put_user_time_off_account_policy_set_schedule_23 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_23',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": json.loads(rail.result('log_timeoff_policy_22'))
            }
        )

        if_request_type_equals_to_update_24 = rail.IfOperator(
            task_id='if_request_type_equals_to_update_24',
            test='''{{ dag_run.conf.type == 'Update' }}''',
            yes_task="log_tenure_25",
            no_task="catch_and_log_error",
        )

        log_tenure_25 = rail.PythonOperator(
            task_id='log_tenure_25',
            python_callable=lambda dag_run: ((datetime.strptime(
                dag_run.conf['startdate'], "%d/%m/%Y") - datetime.strptime(dag_run.conf['actualstartdate'], '%d/%m/%Y')).days)/365
        )

        log_required_accrual_26 = rail.PythonOperator(
            task_id='log_required_accrual_26',
            python_callable=lambda dag_run: round(((float(dag_run.conf['scheduledweeklyhours']) / 40) * float(rail.result(
                'log_gettheaccrualbalance_9')))) if 'sick leave' in (dag_run.conf['timeofftype'].lower())
                else round((((float(dag_run.conf['scheduledweeklyhours']) / 40) * float(rail.result(
                'log_gettheaccrualbalance_9'))) / float((((datetime.strptime(
                dag_run.conf['startdate'], "%d/%m/%Y") + relativedelta(months=12)).replace(day=1, month=1)) - timedelta(days=1)).strftime(
                "%j")) * float(rail.result('log_required_numberofdaysforprorationcalculation_16'))))
        )

        log_required_accrual_27 = rail.PythonOperator(
            task_id='log_required_accrual_27',
            python_callable=lambda:  '{"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": ' + str(
                rail.result('log_required_accrual_26')) + '}}'
        )

        declare_list_28 = rail.SetVariableOperator(
            task_id='declare_list_28',
            append=False,
            name='timeoffpolicy',
            value=[]
        )

        get_user_time_off_type_policy_summary_29 = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary_29',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        create_effectivedateofpolicies_list = rail.SetVariableOperator(
            task_id='create_effectivedateofpolicies_list',
            name='effectivedateofpolicies',
            append=False,
            value=[]
        )

        create_index_variable = rail.SetVariableOperator(
            task_id = 'create_index_variable',
            name='indexvar',
            append=False,
            value=0
        )

        foreach_d_30 = rail.ForEachOperator(
            task_id='foreach_d_30',
            items=lambda: rail.result('get_user_time_off_type_policy_summary_29')['policiesByTimeOffType'],
            start_task='if_timeofftype_name_equals_to_dataworkato_servicereceive_requestrequesttimeofftype_31',
            end_task='foreach_d_30_end'
        )

        if_timeofftype_name_equals_to_dataworkato_servicereceive_requestrequesttimeofftype_31 = rail.IfOperator(
            task_id='if_timeofftype_name_equals_to_dataworkato_servicereceive_requestrequesttimeofftype_31',
            test='''{{ result('foreach_d_30').timeOffType.name == dag_run.conf.timeofftype }}''',
            yes_task="foreach_foreach_d_30_32",
            no_task="increment_index",
        )

        foreach_foreach_d_30_32 = rail.ForEachOperator(
            task_id='foreach_foreach_d_30_32',
            items=lambda: rail.result('foreach_d_30')['policySetSchedule'],
            start_task='log_effective_date_33',
            end_task='foreach_foreach_d_30_32_end'
        )

        def get_date_string(dateobj):
            return str(dateobj['day']) + "/" + str(dateobj['month']) + "/" + str(dateobj['year'])

        log_effective_date_33 = rail.PythonOperator(
            task_id='log_effective_date_33',
            python_callable=lambda: get_date_string(rail.result('foreach_foreach_d_30_32')['effectiveDate'])
        )

        if_to_date_less_than_dataworkato_servicereceive_requestrequeststartdateto_date_34 = rail.IfOperator(
            task_id='if_to_date_less_than_dataworkato_servicereceive_requestrequeststartdateto_date_34',
            test=lambda dag_run: datetime.strptime(rail.result(
                'log_effective_date_33'), "%d/%m/%Y") < datetime.strptime(dag_run.conf['startdate'], "%d/%m/%Y"),
            yes_task="accumulate_list_items_35",
            no_task="foreach_foreach_d_30_32_end",
        )

        accumulate_list_items_35 = rail.SetVariableOperator(
            task_id='accumulate_list_items_35',
            name='effectivedateofpolicies',
            append=True,
            value=lambda: {
                "effectivedate": (datetime.strptime(rail.result('log_effective_date_33'), "%d/%m/%Y")).strftime("%Y-%m-%d"),
                "policyset": rail.result('foreach_foreach_d_30_32')['policySet'],
                "count": rail.get_dag_run_var('indexvar') + 1
            }
        )

        foreach_foreach_d_30_32_end = rail.EmptyOperator(
            task_id='foreach_foreach_d_30_32_end',
        )

        declare_variable_36 = rail.SetVariableOperator(
            task_id='declare_variable_36',
            append=False,
            name='effective_date_to_consider',
            value=None
        )

        log_effective_dateto_consider_37 = rail.PythonOperator(
            task_id='log_effective_dateto_consider_37',
            python_callable=lambda: (max(rail.get_dag_run_var('effectivedateofpolicies'), key=lambda x: x['effectivedate']))[
                'effectivedate'] if rail.get_dag_run_var('effectivedateofpolicies') else ''
        )

        if_max_to_date_less_than_dataworkato_servicereceive_requestrequeststartdateto_datebeginning_of_year_38 = rail.IfOperator(
            task_id='if_max_to_date_less_than_dataworkato_servicereceive_requestrequeststartdateto_datebeginning_of_year_38',
            test=lambda dag_run: datetime.strptime(rail.result(
                'log_effective_dateto_consider_37'), "%Y-%m-%d") < datetime.strptime(dag_run.conf['startdate'], "%d/%m/%Y").replace(day=1, month=1),
            yes_task="update_variable_39",
            no_task="log_effective_dateto_consider_40",
        )

        update_variable_39 = rail.SetVariableOperator(
            task_id='update_variable_39',
            append=False,
            name='{{ result("declare_variable_36").name }}',
            value=lambda dag_run: (datetime.strptime(
                dag_run.conf['startdate'], "%d/%m/%Y").replace(day=1, month=1)).strftime("%d/%m/%Y")
        )

        log_effective_dateto_consider_40 = rail.PythonOperator(
            task_id='log_effective_dateto_consider_40',
            python_callable=lambda: rail.get_dag_run_var(
                'effective_date_to_consider')
        )

        foreach_foreach_d_30_41 = rail.ForEachOperator(
            task_id='foreach_foreach_d_30_41',
            items=lambda: rail.result('foreach_d_30')['policySetSchedule'],
            start_task='log_effective_date_42',
            end_task='foreach_foreach_d_30_41_end'
        )

        log_effective_date_42 = rail.PythonOperator(
            task_id='log_effective_date_42',
            python_callable=lambda: get_date_string(rail.result('foreach_foreach_d_30_41')['effectiveDate'])
        )

        if_to_date_less_than_dataloggerlog_effective_dateto_consider_40messageto_date_43 = rail.IfOperator(
            task_id='if_to_date_less_than_dataloggerlog_effective_dateto_consider_40messageto_date_43',
            test=lambda: datetime.strptime(rail.result('log_effective_date_42'), "%d/%m/%Y") < datetime.strptime(
                rail.result('log_effective_dateto_consider_40'), "%d/%m/%Y"),
            yes_task="insert_to_list_44",
            no_task="foreach_foreach_d_30_41_end",
        )

        insert_to_list_44 = rail.SetVariableOperator(
            task_id='insert_to_list_44',
            append=True,
            name='{{ result("declare_list_28").name }}',
            value=lambda: {
                "description": rail.result('foreach_foreach_d_30_41')['description'],
                "effectiveDate": {
                    "day": rail.result('foreach_foreach_d_30_41')['effectiveDate']['day'],
                    "month": rail.result('foreach_foreach_d_30_41')['effectiveDate']['month'],
                    "year": rail.result('foreach_foreach_d_30_41')['effectiveDate']['year']
                },
                "policySet": rail.result('foreach_foreach_d_30_41')['policySet']
            }
        )

        foreach_foreach_d_30_41_end = rail.EmptyOperator(
            task_id='foreach_foreach_d_30_41_end',
        )

        increment_index = rail.SetVariableOperator(
            task_id = 'increment_index',
            name='indexvar',
            append=False,
            value=lambda: rail.get_dag_run_var('indexvar') + 1
        )

        foreach_d_30_end = rail.EmptyOperator(
            task_id='foreach_d_30_end',
        )

        log_effective_datetobeconsidered_76 = rail.PythonOperator(
            task_id='log_effective_datetobeconsidered_76',
            python_callable=lambda dag_run: dag_run.conf['startdate']
        )

        get_used_sick_leave = rail.RepliconServiceOperator(
            task_id='get_used_sick_leave',
            endpoint="/services//TimeOffService1.svc/GetTimeOffTakenSeriesForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate":  {
                    "day": ((datetime.strptime(dag_run.conf['startdate'], "%d/%m/%Y")).replace(day=1, month=1)).day,
                    "month": ((datetime.strptime(dag_run.conf['startdate'], "%d/%m/%Y")).replace(day=1, month=1)).month,
                    "year": ((datetime.strptime(dag_run.conf['startdate'], "%d/%m/%Y")).replace(day=1, month=1)).year
                },
                    "endDate": {
                    "day": (datetime.strptime(dag_run.conf['startdate'], "%d/%m/%Y")).day,
                    "month": (datetime.strptime(dag_run.conf['startdate'], "%d/%m/%Y")).month,
                    "year": (datetime.strptime(dag_run.conf['startdate'], "%d/%m/%Y")).year
                },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                },
                "periodResolutionUri": "urn:replicon:period-resolution:yearly",
                "timeOffTypeUri": dag_run.conf['timeoffuri']
            },
            data_handler= lambda res: res['dataPoints'][0]['timeTaken']['calendarDayDuration']['hours'] if res['dataPoints'] else 0
        )

        log_required_starting_balance_json= rail.PythonOperator(
            task_id='log_required_starting_balance_json',
            python_callable=lambda:  '{"keyUri": "urn:replicon:script-key:parameter:amount", "value": {"number": ' + str(
                int(rail.result('log_required_accrual_26') - rail.result('get_used_sick_leave'))) + '}}'
        )

        get_default_time_off_policy_set_schedule_for_time_off_type = rail.RepliconServiceOperator(
            task_id='get_default_time_off_policy_set_schedule_for_time_off_type',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
            }
        )

        log_policy_set = rail.PythonOperator(
            task_id='log_policy_set',
            python_callable=lambda: (json.dumps(rail.result('get_default_time_off_policy_set_schedule_for_time_off_type')[0]['policySet'])).replace(
                rail.result('log_existing_accrual_10'), rail.result('log_required_accrual_27')).replace(rail.result(
                'log_existing_starting_balance_15'), rail.result('log_required_starting_balance_json')).replace("[,{", "[{").replace("},,{", "},{").replace(
                '"timeOffValidationScripts":}', '"timeOffValidationScripts":[]}').replace("[]}]", "[]}").replace("}},]", "}}]")
        )

        insert_to_list = rail.SetVariableOperator(
            task_id='insert_to_list',
            append=True,
            name='{{ result("declare_list_28").name }}',
            value=lambda: {
                "description": "Effective on " + rail.result('log_effective_datetobeconsidered_76'),
                "effectiveDate": {
                    "day": datetime.strptime(rail.result('log_effective_datetobeconsidered_76'), "%d/%m/%Y").day,
                    "month": datetime.strptime(rail.result('log_effective_datetobeconsidered_76'), "%d/%m/%Y").month,
                    "year": datetime.strptime(rail.result('log_effective_datetobeconsidered_76'), "%d/%m/%Y").year
                },
                "policySet": json.loads(rail.result('log_policy_set'))
            }
        )

        log_policytoassign_88 = rail.PythonOperator(
            task_id='log_policytoassign_88',
            python_callable=lambda: (json.dumps(rail.get_dag_run_var(
                'timeoffpolicy'))).replace('\"script\"', '\"scriptTarget\"').replace("[,{", "[{").replace("},,{", "},{")
        )

        put_user_time_off_account_policy_set_schedule_89 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_89',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": json.loads(rail.result('log_policytoassign_88'))
            }
        )

        catch_and_log_error = rail.PythonOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "{{get_error_message()}}")
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label(
            'No') >> get_default_time_off_type_policy_schedule_for_user_4
        get_default_time_off_type_policy_schedule_for_user_4 >> if_effectivedate_day_present_6
        if_effectivedate_day_present_6 >> rail.Label(
            'Yes') >> log_gettheaccrualbalancesetup_7 >> log_gettheaccrualbalance_9 >> log_existing_accrual_10 >> log_getthestartingbalancesetup_11
        log_getthestartingbalancesetup_11 >> log_getthestartingbalancescript_12 >> log_getthestartingbalance_14 >> log_existing_starting_balance_15
        log_existing_starting_balance_15 >> log_required_numberofdaysforprorationcalculation_16 >> log_required_accrual_17 >> log_required_accrual_json_18
        log_required_accrual_json_18 >> if_request_type_equals_to_add_19
        if_request_type_equals_to_add_19 >> rail.Label(
            'Yes') >> log_required_starting_balance_20 >> log_required_starting_balance_json_21 >> log_timeoff_policy_22
        log_timeoff_policy_22 >> put_user_time_off_account_policy_set_schedule_23 >> if_request_type_equals_to_update_24
        if_request_type_equals_to_add_19 >> rail.Label(
            'No') >> if_request_type_equals_to_update_24
        if_request_type_equals_to_update_24 >> rail.Label(
            'Yes') >> log_tenure_25 >> log_required_accrual_26 >> log_required_accrual_27 >> declare_list_28 >> get_user_time_off_type_policy_summary_29
        get_user_time_off_type_policy_summary_29 >> create_effectivedateofpolicies_list
        create_effectivedateofpolicies_list >> create_index_variable
        create_index_variable >> foreach_d_30 >> if_timeofftype_name_equals_to_dataworkato_servicereceive_requestrequesttimeofftype_31
        if_timeofftype_name_equals_to_dataworkato_servicereceive_requestrequesttimeofftype_31 >> rail.Label(
            'Yes') >> foreach_foreach_d_30_32 >> log_effective_date_33 >> if_to_date_less_than_dataworkato_servicereceive_requestrequeststartdateto_date_34
        if_to_date_less_than_dataworkato_servicereceive_requestrequeststartdateto_date_34 >> rail.Label(
            'Yes') >> accumulate_list_items_35 >> foreach_foreach_d_30_32_end
        if_to_date_less_than_dataworkato_servicereceive_requestrequeststartdateto_date_34 >> rail.Label(
            'No') >> foreach_foreach_d_30_32_end
        foreach_foreach_d_30_32 >> foreach_foreach_d_30_32_end >> declare_variable_36 >> log_effective_dateto_consider_37
        log_effective_dateto_consider_37 >> if_max_to_date_less_than_dataworkato_servicereceive_requestrequeststartdateto_datebeginning_of_year_38
        if_max_to_date_less_than_dataworkato_servicereceive_requestrequeststartdateto_datebeginning_of_year_38 >> rail.Label(
            'Yes') >> update_variable_39 >> log_effective_dateto_consider_40
        if_max_to_date_less_than_dataworkato_servicereceive_requestrequeststartdateto_datebeginning_of_year_38 >> rail.Label(
            'No') >> log_effective_dateto_consider_40 >> foreach_foreach_d_30_41 >> log_effective_date_42
        log_effective_date_42 >> if_to_date_less_than_dataloggerlog_effective_dateto_consider_40messageto_date_43
        if_to_date_less_than_dataloggerlog_effective_dateto_consider_40messageto_date_43 >> rail.Label(
            'Yes') >> insert_to_list_44 >> foreach_foreach_d_30_41_end
        if_to_date_less_than_dataloggerlog_effective_dateto_consider_40messageto_date_43 >> rail.Label(
            'No') >> foreach_foreach_d_30_41_end
        foreach_foreach_d_30_41 >> foreach_foreach_d_30_41_end >> increment_index >> foreach_d_30_end
        if_timeofftype_name_equals_to_dataworkato_servicereceive_requestrequesttimeofftype_31 >> rail.Label(
            'No') >> increment_index >> foreach_d_30_end
        foreach_d_30 >> foreach_d_30_end >> log_effective_datetobeconsidered_76 >> get_used_sick_leave >> log_required_starting_balance_json \
        >> get_default_time_off_policy_set_schedule_for_time_off_type >> log_policy_set >> insert_to_list >> log_policytoassign_88
        log_policytoassign_88 >> put_user_time_off_account_policy_set_schedule_89 >> catch_and_log_error
        if_request_type_equals_to_update_24 >> rail.Label(
            'No') >> catch_and_log_error
        if_effectivedate_day_present_6 >> rail.Label(
            'No') >> catch_and_log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
