
from datetime import timedelta, datetime
import json
from dateutil.relativedelta import relativedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.holiday_termination_proration_child_dag_id,
        description=f'MichaelKorsTnA UK_Child Holiday Timeoff type Termination Proration Assignment v1.0 {config.instance}',
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
            no_task='get_user_time_off_type_policy_summary_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_user_time_off_type_policy_summary_3',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_user_time_off_type_policy_summary_3 = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary_3',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        if_timeofftype_uri_present_4 = rail.IfOperator(
            task_id='if_timeofftype_uri_present_4',
            test=lambda: rail.result('get_user_time_off_type_policy_summary_3') and rail.result('get_user_time_off_type_policy_summary_3')[
                'policiesByTimeOffType'] and rail.result('get_user_time_off_type_policy_summary_3')['policiesByTimeOffType'][0]['timeOffType']['uri'],
            yes_task="declare_list_5",
            no_task="catch_and_log_error",
        )

        declare_list_5 = rail.SetVariableOperator(
            task_id='declare_list_5',
            append=False,
            name='timeoffpolicy',
            value=[]
        )

        foreach_d_6 = rail.ForEachOperator(
            task_id='foreach_d_6',
            items=lambda: rail.result('get_user_time_off_type_policy_summary_3')[
                'policiesByTimeOffType'],
            start_task='if_timeofftype_name_equals_to_dataworkato_servicereceive_requestrequesttimeofftype_7',
            end_task='foreach_d_6_end'
        )

        if_timeofftype_name_equals_to_dataworkato_servicereceive_requestrequesttimeofftype_7 = rail.IfOperator(
            task_id='if_timeofftype_name_equals_to_dataworkato_servicereceive_requestrequesttimeofftype_7',
            test='''{{ result('foreach_d_6').timeOffType.name == dag_run.conf.timeofftype }}''',
            yes_task="foreach_foreach_d_6_8",
            no_task="foreach_d_6_end",
        )

        foreach_foreach_d_6_8 = rail.ForEachOperator(
            task_id='foreach_foreach_d_6_8',
            items=lambda: rail.result('foreach_d_6')['policySetSchedule'],
            start_task='log_effective_date_9',
            end_task='foreach_foreach_d_6_8_end'
        )

        log_effective_date_9 = rail.PythonOperator(
            task_id='log_effective_date_9',
            #pylint: disable = line-too-long
            python_callable=lambda: rail.render_template("{{ result('foreach_foreach_d_6_8').effectiveDate.day }}/{{ result('foreach_foreach_d_6_8').effectiveDate.month }}/{{ result('foreach_foreach_d_6_8').effectiveDate.year }}")
        )

        if_to_date_less_than_dataworkato_servicereceive_requestrequestdisabledateto_date_10 = rail.IfOperator(
            task_id='if_to_date_less_than_dataworkato_servicereceive_requestrequestdisabledateto_date_10',
            test=lambda dag_run: datetime.strptime(rail.result(
                'log_effective_date_9'), "%d/%m/%Y") < datetime.strptime(dag_run.conf['disabledate'], "%d/%m/%Y"),
            yes_task="insert_to_list_11",
            no_task="foreach_foreach_d_6_8_end",
        )

        insert_to_list_11 = rail.SetVariableOperator(
            task_id='insert_to_list_11',
            append=True,
            name='{{ result("declare_list_5").name }}',
            value=lambda: {
                "description": rail.result('foreach_foreach_d_6_8')['description'],
                "effectiveDate": {
                    "day": rail.result('foreach_foreach_d_6_8')['effectiveDate']['day'],
                    "month": rail.result('foreach_foreach_d_6_8')['effectiveDate']['month'],
                    "year": rail.result('foreach_foreach_d_6_8')['effectiveDate']['year']
                },
                "policySet": rail.result('foreach_foreach_d_6_8')['policySet']
            }
        )

        log_gettheaccrualbalancesetup_12 = rail.PythonOperator(
            task_id='log_gettheaccrualbalancesetup_12',
            python_callable=lambda: json.dumps(rail.find_first_by_attr_and_get_attr(rail.result('foreach_foreach_d_6_8')['policySet'][
                'timeOffBalanceEventScripts'], 'script.name', 'Yearly Accrual', 'additionalParameters')).replace("[[", "[").replace("]]", "]")
        )

        log_effective_datetobeconsidered_13 = rail.PythonOperator(
            task_id='log_effective_datetobeconsidered_13',
            python_callable=lambda: rail.result('log_effective_date_9')
        )

        foreach_foreach_d_6_8_end = rail.EmptyOperator(
            task_id='foreach_foreach_d_6_8_end',
        )

        foreach_d_6_end = rail.EmptyOperator(
            task_id='foreach_d_6_end',
        )

        if_effectivedate_tobe_considered_present = rail.IfOperator(
            task_id = 'if_effectivedate_tobe_considered_present',
            test="{{result('log_effective_datetobeconsidered_13') | is_truthy}}",
            yes_task='log_gettheaccrualbalance_15',
            no_task='catch_and_log_error'
        )

        log_gettheaccrualbalance_15 = rail.PythonOperator(
            task_id='log_gettheaccrualbalance_15',
            python_callable=lambda:  float(rail.find_first_by_attr_and_get_attr(json.loads(rail.result(
                'log_gettheaccrualbalancesetup_12')), 'keyUri', 'urn:replicon:script-key:parameter:accrual-annual-amount', 'value.number', 0))
        )

        log_gettheaccrual_day_16 = rail.PythonOperator(
            task_id='log_gettheaccrual_day_16',
            python_callable=lambda: ((rail.find_first_by_attr_and_get_attr(json.loads(rail.result(
                'log_gettheaccrualbalancesetup_12')), 'keyUri', 'urn:replicon:script-key:parameter:accrue-on-day-of-month', 'value.uri', '')).split(":"))[-1]
        )

        log_gettheaccrual_month_17 = rail.PythonOperator(
            task_id='log_gettheaccrual_month_17',
            python_callable=lambda: ((rail.find_first_by_attr_and_get_attr(json.loads(rail.result(
                'log_gettheaccrualbalancesetup_12')), 'keyUri', 'urn:replicon:script-key:parameter:accrue-on-month', 'value.uri', '')).split(":"))[-1]
        )

        log_accrual_day_18 = rail.PythonOperator(
            task_id='log_accrual_day_18',
            python_callable=lambda dag_run: (datetime.strptime((rail.result('log_gettheaccrual_day_16'))[0] + " " + rail.result(
                'log_gettheaccrual_month_17') + "," +
                datetime.strptime(dag_run.conf['disabledate'], "%d/%m/%Y").strftime("%Y"), "%d %B,%Y")).strftime("%d/%m/%Y")
        )

        log_accrual_day_19 = rail.PythonOperator(
            task_id='log_accrual_day_19',
            python_callable=lambda: rail.result('log_effective_datetobeconsidered_13') if datetime.strptime(rail.result(
                'log_accrual_day_18'), "%d/%m/%Y") < datetime.strptime(rail.result('log_effective_datetobeconsidered_13'), "%d/%m/%Y") else rail.result(
                'log_accrual_day_18')
        )

        log_required_numberofdaysforprorationcalculation_20 = rail.PythonOperator(
            task_id='log_required_numberofdaysforprorationcalculation_20',
            python_callable=lambda dag_run: (datetime.strptime(
                dag_run.conf['disabledate'],"%d/%m/%Y") - datetime.strptime(rail.result('log_accrual_day_19'), "%d/%m/%Y")).days
        )

        def get_date_object(datestring):
            dateobj = datetime.strptime(datestring, '%d/%m/%Y')
            return {
                'day': dateobj.day,
                'month': dateobj.month,
                'year': dateobj.year
            }

        invoke_custom_ruby_code_21 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_21',
            python_callable=lambda: get_date_object(
                rail.result('log_accrual_day_19'))
        )

        invoke_custom_ruby_code_22 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_22',
            python_callable=lambda dag_run: get_date_object(
                dag_run.conf['disabledate'])
        )

        get_time_off_taken_series_for_user_23 = rail.RepliconServiceOperator(
            task_id='get_time_off_taken_series_for_user_23',
            endpoint="/services/TimeOffService1.svc/GetTimeOffTakenSeriesForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ result('invoke_custom_ruby_code_21').year }}",
                        "month": "{{ result('invoke_custom_ruby_code_21').month }}",
                        "day": "{{ result('invoke_custom_ruby_code_21').day }}"
                    },
                    "endDate": {
                        "year": "{{ result('invoke_custom_ruby_code_22').year }}",
                        "month": "{{ result('invoke_custom_ruby_code_22').month }}",
                        "day": "{{ result('invoke_custom_ruby_code_22').day }}"
                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                },
                "periodResolutionUri": "urn:replicon:period-resolution:daily",
                "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
            },
            data_handler=lambda response: [{
                'date': str(data['period']['month']) + "/" + str(data['period']['day']) + "/" + str(data['period']['year']),
                'hours': data['timeTaken']['calendarDayDuration']['Hours'],
                'minutes': int(data['timeTaken']['calendarDayDuration']['minutes'])
            } for data in response['dataPoints']]
        )

        get_all_scriptsfor_time_off_balance_event_script_administration_service_24 = rail.RepliconServiceOperator(
            task_id='get_all_scriptsfor_time_off_balance_event_script_administration_service_24',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda response: {
                'startingbalancesettouri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Starting Balance Set To', 'uri', '')
            }
        )

        def get_total_sum_of_hours():
            sumofhours = 0
            sumofminutes = 0
            for item in rail.result('get_time_off_taken_series_for_user_23'):
                sumofhours += float(item['hours'])
                sumofminutes += float(item['minutes'])
            return sumofhours + (sumofminutes/60)

        log_numberof_booking_hours_26 = rail.PythonOperator(
            task_id='log_numberof_booking_hours_26',
            python_callable=get_total_sum_of_hours
        )

        log_requiredtermination_proration_balance_27 = rail.PythonOperator(
            task_id='log_requiredtermination_proration_balance_27',
            python_callable=lambda dag_run: round(float((float(rail.result(
                'log_gettheaccrualbalance_15')) / float((((datetime.strptime(dag_run.conf['disabledate'], "%d/%m/%Y") + relativedelta(months=12)).replace(
                day=1, month=1)) - timedelta(days=1)).strftime("%j"))) * float(rail.result(
                'log_required_numberofdaysforprorationcalculation_20'))), 2) - float(rail.result('log_numberof_booking_hours_26'))
        )

        declare_list_29 = rail.SetVariableOperator(
            task_id='declare_list_29',
            append=False,
            name='Disable_Policy_List',
            value=[]
        )

        insert_to_list_30 = rail.SetVariableOperator(
            task_id='insert_to_list_30',
            append=True,
            name='{{ result("declare_list_29").name }}',
            value=lambda: {
                "policySet": {
                    "timeOffBalanceEventScripts": {
                        "additionalParameters": {
                            "keyUri": "urn:replicon:script-key:parameter:amount",
                            "value": {
                                "number": int(rail.result('log_requiredtermination_proration_balance_27'))
                            }
                        },
                        "script": {
                            "description": "Starting Balance Set To",
                            "name": "Starting Balance Set To",
                            "uri": rail.result('get_all_scriptsfor_time_off_balance_event_script_administration_service_24')['startingbalancesettouri']
                        }
                    }
                }
            }
        )

        log_policy_31 = rail.PythonOperator(
            task_id='log_policy_31',
            python_callable=lambda: json.loads((json.dumps(rail.get_dag_run_var('Disable_Policy_List'))).replace(
                '[{"effectiveDate', '{"effectiveDate').replace('{"additionalParameters"', '[{"additionalParameters"').replace(
                '"additionalParameters": {"keyUri"', '"additionalParameters":[{"keyUri"').replace('}}, "script"', '}}],"script"').replace(
                "}}}}]", "}}]}").replace('[{"policySet":', ""))
        )

        insert_to_list_33 = rail.SetVariableOperator(
            task_id='insert_to_list_33',
            append=True,
            name='{{ result("declare_list_5").name }}',
            value=lambda: {
                "description": "Effective on " + str(rail.result('invoke_custom_ruby_code_22')['day']) + "/" +
                    str(rail.result('invoke_custom_ruby_code_22')['month']) + "/" + str(rail.result('invoke_custom_ruby_code_22')['year']),
                "effectiveDate": {
                    "day": rail.result('invoke_custom_ruby_code_22')['day'],
                    "month": rail.result('invoke_custom_ruby_code_22')['month'],
                    "year": rail.result('invoke_custom_ruby_code_22')['year']
                },
                "policySet": rail.result('log_policy_31')
            }
        )

        log_policytoassign_34 = rail.PythonOperator(
            task_id='log_policytoassign_34',
            python_callable=lambda: (json.dumps(rail.get_dag_run_var(
                'timeoffpolicy'))).replace('\"script\"', '\"scriptTarget\"')
        )

        put_user_time_off_account_policy_set_schedule_35 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_35',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": json.loads(rail.result('log_policytoassign_34'))
            }
        )

        catch_and_log_error = rail.PythonOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                '{{get_error_message()}}')
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label(
            'No') >> get_user_time_off_type_policy_summary_3
        get_user_time_off_type_policy_summary_3 >> if_timeofftype_uri_present_4
        if_timeofftype_uri_present_4 >> rail.Label(
            'Yes') >> declare_list_5 >> foreach_d_6 >> if_timeofftype_name_equals_to_dataworkato_servicereceive_requestrequesttimeofftype_7
        if_timeofftype_name_equals_to_dataworkato_servicereceive_requestrequesttimeofftype_7 >> rail.Label(
            'Yes') >> foreach_foreach_d_6_8 >> log_effective_date_9 >> if_to_date_less_than_dataworkato_servicereceive_requestrequestdisabledateto_date_10
        if_to_date_less_than_dataworkato_servicereceive_requestrequestdisabledateto_date_10 >> rail.Label(
            'Yes') >> insert_to_list_11 >> log_gettheaccrualbalancesetup_12 >> log_effective_datetobeconsidered_13 >> foreach_foreach_d_6_8_end
        if_to_date_less_than_dataworkato_servicereceive_requestrequestdisabledateto_date_10 >> rail.Label(
            'No') >> foreach_foreach_d_6_8_end
        foreach_foreach_d_6_8 >> foreach_foreach_d_6_8_end >> foreach_d_6_end
        if_timeofftype_name_equals_to_dataworkato_servicereceive_requestrequesttimeofftype_7 >> rail.Label(
            'No') >> foreach_d_6_end
        foreach_d_6 >> foreach_d_6_end >> if_effectivedate_tobe_considered_present
        if_effectivedate_tobe_considered_present >> rail.Label('Yes') >> log_gettheaccrualbalance_15 >> log_gettheaccrual_day_16
        log_gettheaccrual_day_16 >> log_gettheaccrual_month_17 >> log_accrual_day_18
        log_accrual_day_18 >> log_accrual_day_19 >> log_required_numberofdaysforprorationcalculation_20 >> invoke_custom_ruby_code_21
        invoke_custom_ruby_code_21 >> invoke_custom_ruby_code_22 >> get_time_off_taken_series_for_user_23
        get_time_off_taken_series_for_user_23 >> get_all_scriptsfor_time_off_balance_event_script_administration_service_24
        get_all_scriptsfor_time_off_balance_event_script_administration_service_24 >> log_numberof_booking_hours_26
        log_numberof_booking_hours_26 >> log_requiredtermination_proration_balance_27 >> declare_list_29 >> insert_to_list_30
        insert_to_list_30 >> log_policy_31 >> insert_to_list_33 >> log_policytoassign_34
        if_effectivedate_tobe_considered_present >> rail.Label('No') >> catch_and_log_error
        log_policytoassign_34 >> put_user_time_off_account_policy_set_schedule_35 >> catch_and_log_error
        if_timeofftype_uri_present_4 >> rail.Label(
            'No') >> catch_and_log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
