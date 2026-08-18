
from datetime import datetime, timedelta
import json
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'kla_timeoff_policy_import_kla_time_off_balance_and_accrual_import_child_v2_0_{config.instance}',
        description=f'KLA_Time Off balance and accrual Import Child V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
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
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_log',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        if_split_smart_join_present_4 = rail.IfOperator(
            task_id='if_split_smart_join_present_4',
            test='''{{ (('' if dag_run.conf.employeeid | is_truthy else "Employee ID not present, ")  +  ('' if dag_run.conf.timeofftypename| is_truthy  else "Time Off type name not present, ") + ('' if dag_run.conf.timeoffbalance| is_truthy  else "Time off balance name not present, ") + ('' if dag_run.conf.effectivedate| is_truthy  else "Effective date not present, ") + ('' if dag_run.conf.accrualrate| is_truthy else "Accrual rate not present, ")) | is_truthy }}''',
            yes_task="kla_time_off_policy_logs_add_entry_5",
            no_task="search_users_7",
        )

        kla_time_off_policy_logs_add_entry_5 = rail.WriteLogOperator(
            task_id='kla_time_off_policy_logs_add_entry_5',
            log="{{ result('create_log') }}",
            message="na",
            severity="Skipped",
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Skipped",
                "reason":  '''{{ (('' if dag_run.conf.employeeid | is_truthy else "Employee ID not present, ")  +  ('' if dag_run.conf.timeofftypename| is_truthy  else "Time Off type name not present, ") + ('' if dag_run.conf.timeoffbalance| is_truthy  else "Time off balance name not present, ") + ('' if dag_run.conf.effectivedate| is_truthy  else "Effective date not present, ") + ('' if dag_run.conf.accrualrate| is_truthy else "Accrual rate not present, ")) }}''',
                "child_job_id": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employeeid }}"
            }
        )

        search_users_7 = rail.RepliconServiceOperator(
            task_id='search_users_7',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:user-list-column:employee-id",
                    "urn:replicon:user-list-column:user"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": [],
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": "{{ dag_run.conf.employeeid }}",
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            },
            data_handler=lambda data: list(filter(lambda x: x['empid'] == rail.get_dag_run_conf()['employeeid'],
                                                  map(lambda x: {
                                                      "empid": x['cells'][0].get('textValue'),
                                                      "uri": x['cells'][1].get('uri')
                                                  }, data['rows'])))
        )

        log_useruri_8 = rail.PythonOperator(
            task_id='log_useruri_8',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'search_users_7'), 'empid', rail.get_dag_run_conf()['employeeid'], 'uri'),
        )

        if_log_useruri_8_blank_9 = rail.IfOperator(
            task_id='if_log_useruri_8_blank_9',
            test='''{{ result('log_useruri_8') | is_falsy }}''',
            yes_task="kla_time_off_policy_logs_add_entry_10",
            no_task="get_time_off_type_assignments_for_user_12",
        )

        kla_time_off_policy_logs_add_entry_10 = rail.WriteLogOperator(
            task_id='kla_time_off_policy_logs_add_entry_10',
            log="{{ result('create_log') }}",
            message="na",
            severity="Skipped",
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Skipped",
                "reason": "User not found in Replicon",
                "child_job_id": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employeeid }}"
            }
        )

        stop_11 = rail.EmptyOperator(
            task_id='stop_11',

        )

        get_time_off_type_assignments_for_user_12 = rail.RepliconServiceOperator(
            task_id='get_time_off_type_assignments_for_user_12',
            endpoint="/services/TimeOffService1.svc/GetTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ result('log_useruri_8') }}"
            }
        )

        def get_timeoff_type_name():
            return "PTO" if rail.get_dag_run_conf()['timeofftypename'] and "PTO" in rail.get_dag_run_conf()['timeofftypename'].upper() else "Sick" if rail.get_dag_run_conf()['timeofftypename'] and "SICK" in rail.get_dag_run_conf()['timeofftypename'].upper() else "2022 PANDEMIC LEAVE" if rail.get_dag_run_conf()['timeofftypename'] and rail.get_dag_run_conf()['timeofftypename'].upper() in "2022 PANDEMIC LEAVE" else null

        invoke_custom_ruby_code_13 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_13',
            python_callable=lambda: {
                "timeofftypename":   get_timeoff_type_name(),
                "timeofftypeuri": rail.find_first_by_attr_and_get_attr(rail.result('get_time_off_type_assignments_for_user_12'), 'displayText',  get_timeoff_type_name(), 'uri')
            }
        )

        if_output_timeofftypename_blank_14 = rail.IfOperator(
            task_id='if_output_timeofftypename_blank_14',
            test='''{{ result('invoke_custom_ruby_code_13').timeofftypename | is_falsy }}''',
            yes_task="kla_time_off_policy_logs_add_entry_15",
            no_task="get_all_timeoffvalidation_scripts_17",
        )

        kla_time_off_policy_logs_add_entry_15 = rail.WriteLogOperator(
            task_id='kla_time_off_policy_logs_add_entry_15',
            log="{{ result('create_log') }}",
            message="na",
            severity="Skipped",
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Skipped",
                "reason": "Time Off type - {{ result('invoke_custom_ruby_code_13').timeofftypename }} not enabled in the user profile or not assigned  to User.",
                "child_job_id": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employeeid }}"
            }
        )

        stop_16 = rail.EmptyOperator(
            task_id='stop_16',

        )

        get_all_timeoffvalidation_scripts_17 = rail.RepliconServiceOperator(
            task_id='get_all_timeoffvalidation_scripts_17',
            endpoint="/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts",
            data=None
        )

        get_all_timeoffbalanceevent_scripts_18 = rail.RepliconServiceOperator(
            task_id='get_all_timeoffbalanceevent_scripts_18',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts",
            data=None
        )

        invoke_custom_ruby_code_21 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_21',
            python_callable=lambda: {
                "setstartingbalance_scripturi": rail.find_first_by_attr_and_get_attr(rail.result('get_all_timeoffbalanceevent_scripts_18'), 'displayText', "Starting Balance Set To", ('uri')),
                "preventbalanceoverdraw_scripturi": rail.find_first_by_attr_and_get_attr(rail.result('get_all_timeoffvalidation_scripts_17'), 'displayText', "Prevent balance overdraw - KLA", ('uri')),
                "preventzerohoursbooking_scripturi": rail.find_first_by_attr_and_get_attr(rail.result('get_all_timeoffvalidation_scripts_17'), 'displayText', "Prevent 0 hour bookings", ('uri')),
                "maxbalancelimit_scripturi": rail.find_first_by_attr_and_get_attr(rail.result('get_all_timeoffbalanceevent_scripts_18'), 'displayText', "Max Balance Limit", ('uri')),
                "effectiveyear": rail.get_dag_run_conf()['effectivedate'].replace('- ', "/").split("/")[2],
                "effectivemonth": rail.get_dag_run_conf()['effectivedate'].replace('- ', "/").split("/")[0],
                "effectiveday": rail.get_dag_run_conf()['effectivedate'].replace('- ', "/").split("/")[1],
                "effectivedate": rail.parse_date(rail.get_dag_run_conf()['effectivedate'], '%m/%d/%Y'),
                "effectivedate_feedfile": rail.get_dag_run_conf()['effectivedate'].replace('- ', "/").split("/")[1] + "/" + rail.get_dag_run_conf()['effectivedate'].replace('- ', "/").split("/")[0] + "/" + rail.get_dag_run_conf()['effectivedate'].replace('- ', "/").split("/")[2],
                "reportfiltervalue_asofdate": rail.get_dag_run_conf()['effectivedate'].replace('- ', "/"),
                "userfiltervalue": rail.result('log_useruri_8').split(":")[-1] if rail.result('invoke_custom_ruby_code_13')['timeofftypeuri'] else null,
                "timeofftype_filtervalue": rail.result('invoke_custom_ruby_code_13')['timeofftypeuri'].split(":")[-1] if rail.result('invoke_custom_ruby_code_13')['timeofftypeuri'] else null,
                "biweekly_scripturi": rail.find_first_by_attr_and_get_attr(rail.result('get_all_timeoffbalanceevent_scripts_18'), 'displayText', "Bi-Weekly Accrual", ('uri')),
                "useruri": rail.result('log_useruri_8'),
                "limittimeofftakenscripturi": rail.find_first_by_attr_and_get_attr(rail.result('get_all_timeoffvalidation_scripts_17'), 'displayText', "Limit amount of time off taken", ('uri'))
            }
        )

        if_output_timeofftypeuri_present_22 = rail.IfOperator(
            task_id='if_output_timeofftypeuri_present_22',
            test='''{{ result('invoke_custom_ruby_code_13').timeofftypeuri | is_truthy  and result('log_useruri_8') | is_truthy }}''',
            yes_task="get_user_time_off_type_policy_summary_23",
            no_task="if_output_timeofftypeuri_blank_111",
        )

        get_user_time_off_type_policy_summary_23 = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary_23',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ result('log_useruri_8') }}"
            }
        )

        log_existing_timeoffpolicyschedule_24 = rail.PythonOperator(
            task_id='log_existing_timeoffpolicyschedule_24',
            python_callable=lambda: json.loads(json.dumps(list(map(lambda x: x['policySetSchedule'], filter(lambda x: x["timeOffType"]['uri'] == rail.result('invoke_custom_ruby_code_13')[
                'timeofftypeuri'], rail.result('get_user_time_off_type_policy_summary_23')['policiesByTimeOffType'])))).replace("[[{", "[{").replace("}]]", "}]"))
        )

        if_timeofftypename_downcase_contains_pto_25 = rail.IfOperator(
            task_id='if_timeofftypename_downcase_contains_pto_25',
            test='''{{ dag_run.conf.timeofftypename.lower() | matches('pto') }}''',
            yes_task="declare_variable_26",
            no_task="if_timeofftypename_downcase_contains_sick_60",
        )

        declare_variable_26 = rail.SetVariableOperator(
            task_id='declare_variable_26',
            append=False,
            name='PTO  Trigger',
            value=None
        )

        if_to_s_contains_urn_27 = rail.IfOperator(
            task_id='if_to_s_contains_urn_27',
            test='''{{ (result('log_existing_timeoffpolicyschedule_24') | to_json) |  matches('urn') }}''',
            yes_task="parse_json_28",
            no_task="if_first_toconsider_present_31",
        )

        parse_json_28 = rail.PythonOperator(
            task_id='parse_json_28',
            python_callable=lambda: rail.result(
                'log_existing_timeoffpolicyschedule_24')
        )

        def get_day_diff(item):
            day_diff = (datetime(
                **rail.result('invoke_custom_ruby_code_21')['effectivedate'])-datetime(**item['effectiveDate'])).days
            return day_diff if day_diff > 0 else 0

        invoke_custom_ruby_code_29 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_29',
            python_callable=lambda: {"timeoffoutput": min(list(filter(lambda x: x["toconsider"] == "Yes", map(lambda item: {
                "description":  item['description'],
                "effectivedate": item['effectiveDate'],
                "daydiff": get_day_diff(item),
                "toconsider": "Yes" if get_day_diff(item) > 0 else "No",
                "policyset": item['policySet'],
            }, rail.result('parse_json_28')))), key=lambda x: x['daydiff'], default=null)}
        )

        invoke_custom_ruby_code_30 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_30',
            python_callable=lambda: {"timeoffoutput": list(filter(lambda x: x["toconsider"] == "Yes", map(lambda item: {
                "description":  item['description'],
                "effectivedate": item['effectiveDate'],
                "daydiff": get_day_diff(item),
                "toconsider": "Yes" if get_day_diff(item) > 0 else "No",
                "policyset": item['policySet'],
            }, rail.result('parse_json_28'))))}
        )

        if_first_toconsider_present_31 = rail.IfOperator(
            task_id='if_first_toconsider_present_31',
            test='''{{ result('invoke_custom_ruby_code_29') | is_truthy and result('invoke_custom_ruby_code_29').timeoffoutput | is_truthy }}''',
            yes_task="parse_json_32",
            no_task="invoke_custom_ruby_code_37",
        )

        parse_json_32 = rail.PythonOperator(
            task_id='parse_json_32',
            python_callable=lambda: rail.result(
                'invoke_custom_ruby_code_29')['timeoffoutput']['policyset']
        )

        if_script_name_present_33 = rail.IfOperator(
            task_id='if_script_name_present_33',
            test='''{{ result('parse_json_32').timeOffBalanceEventScripts | find_first_by_attr_and_get_attr('script.name',"Bi-Weekly Accrual",'additionalParameters') | is_truthy }}''',
            yes_task="parse_json_34",
            no_task="invoke_custom_ruby_code_37",
        )

        parse_json_34 = rail.PythonOperator(
            task_id='parse_json_34',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('parse_json_32')[
                                                                         'timeOffBalanceEventScripts'], 'script.name', "Bi-Weekly Accrual", 'additionalParameters')
        )

        if_first_keyuri_present_35 = rail.IfOperator(
            task_id='if_first_keyuri_present_35',
            test='''{{ result('parse_json_34') | find_first_by_attr_and_get_attr('keyUri', "urn:replicon:script-key:parameter:accrual-annual-amount") | is_truthy }}''',
            yes_task="parse_json_36",
            no_task="invoke_custom_ruby_code_37",
        )

        parse_json_36 = rail.PythonOperator(
            task_id='parse_json_36',
            python_callable=lambda: json.loads(json.dumps(rail.find_first_by_attr_and_get_attr(rail.result(
                'parse_json_34'), 'keyUri', "urn:replicon:script-key:parameter:accrual-annual-amount", 'value')).replace("[{", "{").replace("}]", "}"))
        )

        invoke_custom_ruby_code_37 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_37',
            python_callable=lambda: {
                "derivedaccrual": rail.result('parse_json_36')['number'] if rail.result('parse_json_36') else 0,
                "annualentitlement": round((float(rail.get_dag_run_conf()['accrualrate']) * 26), 2),
                "difference": round(abs(float(rail.get_dag_run_conf()['accrualrate']) * 26 - (rail.result('parse_json_36')['number'] if rail.result('parse_json_36') else 0)), 2)
            }
        )

        if_output_difference_equals_to_025_38 = rail.IfOperator(
            task_id='if_output_difference_equals_to_025_38',
            test='''{{ result('invoke_custom_ruby_code_37').difference == 0.25  or result('invoke_custom_ruby_code_37').difference > 0.25 }}''',
            yes_task="update_variable_39",
            no_task="get_timeoffbalance_41",
        )

        update_variable_39 = rail.SetVariableOperator(
            task_id='update_variable_39',
            append=False,
            name='{{ result("declare_variable_26").name }}',
            value='yes'
        )

        get_timeoffbalance_41 = rail.RepliconServiceOperator(
            task_id='get_timeoffbalance_41',
            endpoint="/services/reportService1.svc/GenerateReport",
            data={
                "reportUri": "{{ dag_run.conf.reporturi }}",
                "filterValues": [
                    {
                        "reportFilterUri": "{{ dag_run.conf.userfilteruri }}",
                        "value": "{{ result('invoke_custom_ruby_code_21').userfiltervalue }}"
                    },
                    {
                        "reportFilterUri": "{{ dag_run.conf.timeofftypefilteruri }}",
                        "value": "{{ result('invoke_custom_ruby_code_21').timeofftype_filtervalue }}"
                    },
                    {
                        "reportFilterUri": "{{ dag_run.conf.asofdatefilteruri }}",
                        "value": null
                    },
                    {
                        "reportFilterUri": "{{ dag_run.conf.asofdatefilteruri }}",
                        "value": null
                    },
                    {
                        "reportFilterUri": "{{ dag_run.conf.asofdatefilteruri }}",
                        "value": "{{ result('invoke_custom_ruby_code_21').reportfiltervalue_asofdate }}"
                    }
                ],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        )

        if_d_payload_not_contains_nodata_44 = rail.IfOperator(
            task_id='if_d_payload_not_contains_nodata_44',
            test='''{{ not result('get_timeoffbalance_41').payload | matches('No Data') }}''',
            yes_task="parse_csv_45",
            no_task="log_differencebetweencurrentbalanceandbalancefromfeedfile_46",
        )

        parse_csv_45 = rail.LoadCSVFileOperator(
            task_id='parse_csv_45',
            document="{{ result('get_timeoffbalance_41').payload }}",
            headers=['time_off_type', 'time_off_balance']
        )

        load_all_csv_records_45 = rail.PythonOperator(
            task_id='load_all_csv_records_45',
            python_callable=lambda: rail.load_all_records(
                rail.result('parse_csv_45'))
        )

        log_differencebetweencurrentbalanceandbalancefromfeedfile_46 = rail.PythonOperator(
            task_id='log_differencebetweencurrentbalanceandbalancefromfeedfile_46',
            python_callable=lambda: abs(round(float(rail.result('load_all_csv_records_45')[
                                        0]['time_off_balance']) - float(rail.get_dag_run_conf()['timeoffbalance']), 2))
        )

        if_log_differencebetweencurrentbalanceandbalancefromfeedfile_46_equals_to_025_47 = rail.IfOperator(
            task_id='if_log_differencebetweencurrentbalanceandbalancefromfeedfile_46_equals_to_025_47',
            test='''{{ result('log_differencebetweencurrentbalanceandbalancefromfeedfile_46') == 0.25  or result('log_differencebetweencurrentbalanceandbalancefromfeedfile_46') > 0.25 }}''',
            yes_task="update_variable_identifiertotriggerpolicyupdate_48",
            no_task="if_declare_variable_26_value_equals_to_yes_49",
        )

        update_variable_identifiertotriggerpolicyupdate_48 = rail.SetVariableOperator(
            task_id='update_variable_identifiertotriggerpolicyupdate_48',
            append=False,
            name='{{ result("declare_variable_26").name }}',
            value='yes'
        )

        if_declare_variable_26_value_equals_to_yes_49 = rail.IfOperator(
            task_id='if_declare_variable_26_value_equals_to_yes_49',
            test='''{{ dag_run_var(result('declare_variable_26').name) == 'yes' }}''',
            yes_task="if_to_s_contains_urn_50",
            no_task="kla_time_off_policy_logs_add_entry_59",
        )

        if_to_s_contains_urn_50 = rail.IfOperator(
            task_id='if_to_s_contains_urn_50',
            test='''{{ (result('log_existing_timeoffpolicyschedule_24') | to_json) |   matches('urn') }}''',
            yes_task="invoke_custom_ruby_code_51",
            no_task="if_to_s_contains_urn_53",
        )

        invoke_custom_ruby_code_51 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_51',
            python_callable=lambda: {
                "numberofpolicytoconsider": len(rail.result('invoke_custom_ruby_code_30')['timeoffoutput']),
                "indexforhistory": len(rail.result('invoke_custom_ruby_code_30')['timeoffoutput']) - 5 if len(rail.result('invoke_custom_ruby_code_30')['timeoffoutput']) > 5 else 0
            }
        )

        log_historicalpoliciestobeassignedmodified_52 = rail.PythonOperator(
            task_id='log_historicalpoliciestobeassignedmodified_52',
            python_callable=lambda:  json.loads((json.dumps(rail.result('parse_json_28')[rail.result('invoke_custom_ruby_code_51')[
                                                'indexforhistory']:5]).replace('null', '"effective"').replace('"script"', '"scriptTarget"')))
        )

        if_to_s_contains_urn_53 = rail.IfOperator(
            task_id='if_to_s_contains_urn_53',
            test='''{{ result('log_historicalpoliciestobeassignedmodified_52')| to_json |  matches('urn') }}''',
            yes_task="assign_time_offpolicyalongwithhistoricalpolicy_54",
            no_task="assign_time_offpolicyalongwithouthistoricalpolicy_56",
        )

        assign_time_offpolicyalongwithhistoricalpolicy_54 = rail.RepliconServiceOperator(
            task_id='assign_time_offpolicyalongwithhistoricalpolicy_54',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda: {
                "timeOffAccount": {
                    "userUri": rail.render_template("{{ result('log_useruri_8') }}"),
                    "timeOffTypeUri": rail.render_template("{{ result('invoke_custom_ruby_code_13').timeofftypeuri }}")
                },
                "policySetScheduleEntries":
                    rail.result(
                        'log_historicalpoliciestobeassignedmodified_52') +
                    [{
                        "effectiveDate": {
                            "year": rail.render_template("{{result('invoke_custom_ruby_code_21').effectiveyear}}"),
                            "month": rail.render_template("{{result('invoke_custom_ruby_code_21').effectivemonth}}"),
                            "day": rail.render_template("{{result('invoke_custom_ruby_code_21').effectiveday}}")
                        },
                        "description": rail.render_template("Effective On {{ result('invoke_custom_ruby_code_21').effectiveyear }}-{{ result('invoke_custom_ruby_code_21').effectivemonth }}-{{ result('invoke_custom_ruby_code_21').effectiveday }}"),
                        "policySet": {
                            "timeOffBalanceEventScripts": [
                                {
                                    "scriptTarget": {
                                        "uri": rail.render_template("{{ result('invoke_custom_ruby_code_21').biweekly_scripturi }}"),
                                        "slug": null,
                                        "name": null
                                    },
                                    "additionalParameters": [
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                                            "value": {
                                                "uri": null,
                                                "slug": null,
                                                "bool": null,
                                                "date": null,
                                                "number": rail.render_template("{{ result('invoke_custom_ruby_code_37').annualentitlement }}"),
                                                "text": null,
                                                "time": null,
                                                "calendarDayDurationValue": null,
                                                "workdayDurationValue": null,
                                                "dateRange": null,
                                                "collection": []
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:accrue-on-week",
                                            "value": {
                                                "uri": "urn:replicon:bi-weekly-accrual-option:week1",
                                                "slug": null,
                                                "bool": null,
                                                "date": null,
                                                "number": null,
                                                "text": null,
                                                "time": null,
                                                "calendarDayDurationValue": null,
                                                "workdayDurationValue": null,
                                                "dateRange": null,
                                                "collection": []
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:accrue-on-day-of-week",
                                            "value": {
                                                "uri": "urn:replicon:day-of-week:saturday",
                                                "slug": null,
                                                "bool": null,
                                                "date": null,
                                                "number": null,
                                                "text": null,
                                                "time": null,
                                                "calendarDayDurationValue": null,
                                                "workdayDurationValue": null,
                                                "dateRange": null,
                                                "collection": []
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:proration-option",
                                            "value": {
                                                "uri": "urn:replicon:time-off-policy-proration-option:do-not-prorate",
                                                "slug": null,
                                                "bool": null,
                                                "date": null,
                                                "number": null,
                                                "text": null,
                                                "time": null,
                                                "calendarDayDurationValue": null,
                                                "workdayDurationValue": null,
                                                "dateRange": null,
                                                "collection": []
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:precedence",
                                            "value": {
                                                "uri": null,
                                                "slug": null,
                                                "bool": null,
                                                "date": null,
                                                "number": "30",
                                                "text": null,
                                                "time": null,
                                                "calendarDayDurationValue": null,
                                                "workdayDurationValue": null,
                                                "dateRange": null,
                                                "collection": []
                                            }
                                        }
                                    ]
                                },
                                {
                                    "scriptTarget": {
                                        "uri": rail.render_template("{{ result('invoke_custom_ruby_code_21').setstartingbalance_scripturi }}"),
                                        "slug": null,
                                        "name": null
                                    },
                                    "additionalParameters": [
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:amount",
                                            "value": {
                                                "uri": null,
                                                "slug": null,
                                                "bool": null,
                                                "date": null,
                                                "number": rail.render_template("{{ dag_run.conf.timeoffbalance }}"),
                                                "text": null,
                                                "time": null,
                                                "calendarDayDurationValue": null,
                                                "workdayDurationValue": null,
                                                "dateRange": null,
                                                "collection": []
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:precedence",
                                            "value": {
                                                "uri": null,
                                                "slug": null,
                                                "bool": null,
                                                "date": null,
                                                "number": "10",
                                                "text": null,
                                                "time": null,
                                                "calendarDayDurationValue": null,
                                                "workdayDurationValue": null,
                                                "dateRange": null,
                                                "collection": []
                                            }
                                        }
                                    ]
                                },
                                {
                                    "scriptTarget": {
                                        "uri": rail.render_template("{{ result('invoke_custom_ruby_code_21').maxbalancelimit_scripturi }}"),
                                        "slug": null,
                                        "name": null
                                    },
                                    "additionalParameters": [
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:daily-maximum-balance-amount",
                                            "value": {
                                                "uri": null,
                                                "slug": null,
                                                "bool": null,
                                                "date": null,
                                                "number": "320",
                                                "text": null,
                                                "time": null,
                                                "calendarDayDurationValue": null,
                                                "workdayDurationValue": null,
                                                "dateRange": null,
                                                "collection": []
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:precedence",
                                            "value": {
                                                "uri": null,
                                                "slug": null,
                                                "bool": null,
                                                "date": null,
                                                "number": "10000",
                                                "text": null,
                                                "time": null,
                                                "calendarDayDurationValue": null,
                                                "workdayDurationValue": null,
                                                "dateRange": null,
                                                "collection": []
                                            }
                                        }
                                    ]
                                }
                            ],
                            "timeOffValidationScripts": [
                                {
                                    "scriptTarget": {
                                        "uri": rail.render_template("{{ result('invoke_custom_ruby_code_21').preventbalanceoverdraw_scripturi }}"),
                                        "slug": null,
                                        "name": null
                                    },
                                    "additionalParameters": [
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:maximum-overdraw",
                                            "value": {
                                                "uri": null,
                                                "slug": null,
                                                "bool": null,
                                                "date": null,
                                                "number": config.pto_prevent_balance_overdraw_amount,
                                                "text": null,
                                                "time": null,
                                                "calendarDayDurationValue": null,
                                                "workdayDurationValue": null,
                                                "dateRange": null,
                                                "collection": []
                                            }
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                ]
            }
        )

        assign_time_offpolicyalongwithouthistoricalpolicy_56 = rail.RepliconServiceOperator(
            task_id='assign_time_offpolicyalongwithouthistoricalpolicy_56',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data={
                "timeOffAccount": {
                    "userUri": "{{ result('log_useruri_8') }}",
                    "timeOffTypeUri": "{{ result('invoke_custom_ruby_code_13').timeofftypeuri }}"
                },
                "policySetScheduleEntries": [
                    {
                        "effectiveDate": {
                            "year": "{{result('invoke_custom_ruby_code_21').effectiveyear}}",
                            "month": "{{result('invoke_custom_ruby_code_21').effectivemonth}}",
                            "day": "{{result('invoke_custom_ruby_code_21').effectiveday}}"
                        },
                        "description": "Effective On {{ result('invoke_custom_ruby_code_21').effectiveyear }}-{{ result('invoke_custom_ruby_code_21').effectivemonth }}-{{ result('invoke_custom_ruby_code_21').effectiveday }}",
                        "policySet": {
                            "timeOffBalanceEventScripts": [
                                {
                                    "scriptTarget": {
                                        "uri": "{{ result('invoke_custom_ruby_code_21').biweekly_scripturi }}",
                                        "slug": null,
                                        "name": null
                                    },
                                    "additionalParameters": [
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                                            "value": {
                                                "uri": null,
                                                "slug": null,
                                                "bool": null,
                                                "date": null,
                                                "number": "{{ result('invoke_custom_ruby_code_37').annualentitlement }}",
                                                "text": null,
                                                "time": null,
                                                "calendarDayDurationValue": null,
                                                "workdayDurationValue": null,
                                                "dateRange": null,
                                                "collection": []
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:accrue-on-week",
                                            "value": {
                                                "uri": "urn:replicon:bi-weekly-accrual-option:week1",
                                                "slug": null,
                                                "bool": null,
                                                "date": null,
                                                "number": null,
                                                "text": null,
                                                "time": null,
                                                "calendarDayDurationValue": null,
                                                "workdayDurationValue": null,
                                                "dateRange": null,
                                                "collection": []
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:accrue-on-day-of-week",
                                            "value": {
                                                "uri": "urn:replicon:day-of-week:saturday",
                                                "slug": null,
                                                "bool": null,
                                                "date": null,
                                                "number": null,
                                                "text": null,
                                                "time": null,
                                                "calendarDayDurationValue": null,
                                                "workdayDurationValue": null,
                                                "dateRange": null,
                                                "collection": []
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:proration-option",
                                            "value": {
                                                "uri": "urn:replicon:time-off-policy-proration-option:do-not-prorate",
                                                "slug": null,
                                                "bool": null,
                                                "date": null,
                                                "number": null,
                                                "text": null,
                                                "time": null,
                                                "calendarDayDurationValue": null,
                                                "workdayDurationValue": null,
                                                "dateRange": null,
                                                "collection": []
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:precedence",
                                            "value": {
                                                "uri": null,
                                                "slug": null,
                                                "bool": null,
                                                "date": null,
                                                "number": "30",
                                                "text": null,
                                                "time": null,
                                                "calendarDayDurationValue": null,
                                                "workdayDurationValue": null,
                                                "dateRange": null,
                                                "collection": []
                                            }
                                        }
                                    ]
                                },
                                {
                                    "scriptTarget": {
                                        "uri": "{{ result('invoke_custom_ruby_code_21').setstartingbalance_scripturi }}",
                                        "slug": null,
                                        "name": null
                                    },
                                    "additionalParameters": [
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:amount",
                                            "value": {
                                                "uri": null,
                                                "slug": null,
                                                "bool": null,
                                                "date": null,
                                                "number": "{{ dag_run.conf.timeoffbalance }}",
                                                "text": null,
                                                "time": null,
                                                "calendarDayDurationValue": null,
                                                "workdayDurationValue": null,
                                                "dateRange": null,
                                                "collection": []
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:precedence",
                                            "value": {
                                                "uri": null,
                                                "slug": null,
                                                "bool": null,
                                                "date": null,
                                                "number": "10",
                                                "text": null,
                                                "time": null,
                                                "calendarDayDurationValue": null,
                                                "workdayDurationValue": null,
                                                "dateRange": null,
                                                "collection": []
                                            }
                                        }
                                    ]
                                },
                                {
                                    "scriptTarget": {
                                        "uri": "{{ result('invoke_custom_ruby_code_21').maxbalancelimit_scripturi }}",
                                        "slug": null,
                                        "name": null
                                    },
                                    "additionalParameters": [
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:daily-maximum-balance-amount",
                                            "value": {
                                                "uri": null,
                                                "slug": null,
                                                "bool": null,
                                                "date": null,
                                                "number": "320",
                                                "text": null,
                                                "time": null,
                                                "calendarDayDurationValue": null,
                                                "workdayDurationValue": null,
                                                "dateRange": null,
                                                "collection": []
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:precedence",
                                            "value": {
                                                "uri": null,
                                                "slug": null,
                                                "bool": null,
                                                "date": null,
                                                "number": "10000",
                                                "text": null,
                                                "time": null,
                                                "calendarDayDurationValue": null,
                                                "workdayDurationValue": null,
                                                "dateRange": null,
                                                "collection": []
                                            }
                                        }
                                    ]
                                }
                            ],
                            "timeOffValidationScripts": [
                                {
                                    "scriptTarget": {
                                        "uri": "{{ result('invoke_custom_ruby_code_21').preventbalanceoverdraw_scripturi }}",
                                        "slug": null,
                                        "name": null
                                    },
                                    "additionalParameters": [
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:maximum-overdraw",
                                            "value": {
                                                "uri": null,
                                                "slug": null,
                                                "bool": null,
                                                "date": null,
                                                "number": config.pto_prevent_balance_overdraw_amount,
                                                "text": null,
                                                "time": null,
                                                "calendarDayDurationValue": null,
                                                "workdayDurationValue": null,
                                                "dateRange": null,
                                                "collection": []
                                            }
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                ]
            }
        )

        kla_time_off_policy_logs_add_entry_57 = rail.WriteLogOperator(
            task_id='kla_time_off_policy_logs_add_entry_57',
            log="{{ result('create_log') }}",
            message="na",
            severity="Success",
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Success",
                "reason": "PTO time Off balance or accruals updated.",
                "child_job_id": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employeeid }}"
            }
        )

        kla_time_off_policy_logs_add_entry_59 = rail.WriteLogOperator(
            task_id='kla_time_off_policy_logs_add_entry_59',
            log="{{ result('create_log') }}",
            message="na",
            severity="Skipped",
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Skipped",
                "reason": "No change in PTO time Off balance or accruals received.",
                "child_job_id": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employeeid }}"
            }
        )

        if_timeofftypename_downcase_contains_sick_60 = rail.IfOperator(
            task_id='if_timeofftypename_downcase_contains_sick_60',
            test='''{{ dag_run.conf.timeofftypename.lower() | matches('sick') }}''',
            yes_task="get_timeoffbalance_62",
            no_task="if_timeofftypename_downcase_contains_2022pandemicleave_85",
        )

        get_timeoffbalance_62 = rail.RepliconServiceOperator(
            task_id='get_timeoffbalance_62',
            endpoint="/services/reportService1.svc/GenerateReport",
            data={
                "reportUri": "{{ dag_run.conf.reporturi }}",
                "filterValues": [
                    {
                        "reportFilterUri": "{{ dag_run.conf.userfilteruri }}",
                        "value": "{{ result('invoke_custom_ruby_code_21').userfiltervalue }}"
                    },
                    {
                        "reportFilterUri": "{{ dag_run.conf.timeofftypefilteruri }}",
                        "value": "{{ result('invoke_custom_ruby_code_21').timeofftype_filtervalue }}"
                    },
                    {
                        "reportFilterUri": "{{ dag_run.conf.asofdatefilteruri }}",
                        "value": null
                    },
                    {
                        "reportFilterUri": "{{ dag_run.conf.asofdatefilteruri }}",
                        "value": null
                    },
                    {
                        "reportFilterUri": "{{ dag_run.conf.asofdatefilteruri }}",
                        "value": "{{ result('invoke_custom_ruby_code_21').reportfiltervalue_asofdate }}"
                    }
                ],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        )

        if_d_payload_not_contains_nodata_65 = rail.IfOperator(
            task_id='if_d_payload_not_contains_nodata_65',
            test='''{{ not result('get_timeoffbalance_62').payload | matches('No Data') }}''',
            yes_task="parse_csv_66",
            no_task="log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_67",
        )

        parse_csv_66 = rail.LoadCSVFileOperator(
            task_id='parse_csv_66',
            document="{{ result('get_timeoffbalance_62').payload }}",
            headers=['time_off_type', 'time_off_balance']
        )

        load_all_csv_records_66 = rail.PythonOperator(
            task_id='load_all_csv_records_66',
            python_callable=lambda: rail.load_all_records(
                rail.result('parse_csv_66'))
        )

        log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_67 = rail.PythonOperator(
            task_id='log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_67',
            python_callable=lambda:  abs(round(float(rail.result('load_all_csv_records_66')[
                                         0]['time_off_balance']) - float(rail.get_dag_run_conf()['timeoffbalance']), 2))
        )

        if_log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_67_equals_to_025_68 = rail.IfOperator(
            task_id='if_log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_67_equals_to_025_68',
            test='''{{ result('log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_67') == 0.25  or result('log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_67') > 0.25 }}''',
            yes_task="if_to_s_contains_urn_69",
            no_task="if_log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_67_less_than_025_83",
        )

        if_to_s_contains_urn_69 = rail.IfOperator(
            task_id='if_to_s_contains_urn_69',
            test='''{{ result('log_existing_timeoffpolicyschedule_24')| to_json |  matches('urn') }}''',
            yes_task="parse_json_70",
            no_task="if_to_s_contains_urn_76",
        )

        parse_json_70 = rail.PythonOperator(
            task_id='parse_json_70',
            python_callable=lambda: rail.result(
                'log_existing_timeoffpolicyschedule_24')
        )

        invoke_custom_ruby_code_71 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_71',
            python_callable=lambda: {"timeoffoutput": min(list(filter(lambda x: x["toconsider"] == "Yes", list(map(lambda item: {
                "description":  item['description'],
                "effectivedate": item['effectiveDate'],
                "daydiff": get_day_diff(item),
                "toconsider": "Yes" if get_day_diff(item) > 0 else "No",
                "policyset": item['policySet'],
            }, rail.result('parse_json_70'))))), key=lambda x: x['daydiff'], default=null)}
        )

        invoke_custom_ruby_code_72 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_72',
            python_callable=lambda: {"timeoffoutput": list(filter(lambda x: x["toconsider"] == "Yes", map(lambda item: {
                "description":  item['description'],
                "effectivedate": item['effectiveDate'],
                "daydiff": get_day_diff(item),
                "toconsider": "Yes" if get_day_diff(item) > 0 else "No",
                "policyset": item['policySet'],
            }, rail.result('parse_json_70'))))}
        )

        invoke_custom_ruby_code_73 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_73',
            python_callable=lambda: {
                "numberofpolicytoconsider": len(rail.result('invoke_custom_ruby_code_72')['timeoffoutput']),
                "indexforhistory": len(rail.result('invoke_custom_ruby_code_72')['timeoffoutput']) - 5 if len(rail.result('invoke_custom_ruby_code_72')['timeoffoutput']) > 5 else 0,
                "limittimeofftakenscripturi": rail.find_first_by_attr_and_get_attr(rail.result('get_all_timeoffvalidation_scripts_17'), 'displayText', "Limit amount of time off taken", ('uri'))
            }
        )

        log_historicalpoliciestobeassigned_74 = rail.PythonOperator(
            task_id='log_historicalpoliciestobeassigned_74',
            python_callable=lambda:  json.loads((json.dumps(rail.result('parse_json_70')[rail.result('invoke_custom_ruby_code_73')[
                                                'indexforhistory']:5]).replace('null', '"effective"').replace('"script"', '"scriptTarget"')))
        )

        if_to_s_contains_urn_76 = rail.IfOperator(
            task_id='if_to_s_contains_urn_76',
            test='''{{ result('log_historicalpoliciestobeassigned_74')|to_json| matches('urn') }}''',
            yes_task="assign_time_offpolicyalongwithhistoricalpolicy_77",
            no_task="assign_time_offpolicywithouthistoricalpolicy_79",
        )

        assign_time_offpolicyalongwithhistoricalpolicy_77 = rail.RepliconServiceOperator(
            task_id='assign_time_offpolicyalongwithhistoricalpolicy_77',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda: {
                "timeOffAccount": {
                    "userUri": rail.render_template("{{ result('log_useruri_8') }}"),
                    "timeOffTypeUri": rail.render_template("{{ result('invoke_custom_ruby_code_13').timeofftypeuri }}")
                },
                "policySetScheduleEntries":
                    rail.result('log_historicalpoliciestobeassigned_74') +
                    [{
                        "effectiveDate": {
                            "year": rail.render_template("{{result('invoke_custom_ruby_code_21').effectiveyear}}"),
                            "month": rail.render_template("{{result('invoke_custom_ruby_code_21').effectivemonth}}"),
                            "day": rail.render_template("{{result('invoke_custom_ruby_code_21').effectiveday}}")
                        },
                        "description": rail.render_template("Effective On {{ result('invoke_custom_ruby_code_21').effectiveyear }}-{{ result('invoke_custom_ruby_code_21').effectivemonth }}-{{ result('invoke_custom_ruby_code_21').effectiveday }}"),
                        "policySet": {
                            "timeOffBalanceEventScripts": [
                                {
                                    "scriptTarget": {
                                        "uri": rail.render_template("{{ result('invoke_custom_ruby_code_21').setstartingbalance_scripturi }}"),
                                        "slug": null,
                                        "name": null
                                    },
                                    "additionalParameters": [
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:amount",
                                            "value": {
                                                "uri": null,
                                                "slug": null,
                                                "bool": null,
                                                "date": null,
                                                "number": rail.render_template("{{ dag_run.conf.timeoffbalance }}"),
                                                "text": null,
                                                "time": null,
                                                "calendarDayDurationValue": null,
                                                "workdayDurationValue": null,
                                                "dateRange": null,
                                                "collection": []
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:precedence",
                                            "value": {
                                                "uri": null,
                                                "slug": null,
                                                "bool": null,
                                                "date": null,
                                                "number": "10",
                                                "text": null,
                                                "time": null,
                                                "calendarDayDurationValue": null,
                                                "workdayDurationValue": null,
                                                "dateRange": null,
                                                "collection": []
                                            }
                                        }
                                    ]
                                }
                            ],
                            "timeOffValidationScripts": [
                                {
                                    "scriptTarget": {
                                        "uri": rail.render_template("{{ result('invoke_custom_ruby_code_73').limittimeofftakenscripturi }}"),
                                        "slug": null,
                                        "name": null
                                    },
                                    "additionalParameters": [
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:maximum-amount",
                                            "value": {
                                                "uri": null,
                                                "slug": null,
                                                "bool": null,
                                                "date": null,
                                                "number": "24",
                                                "text": null,
                                                "time": null,
                                                "calendarDayDurationValue": null,
                                                "workdayDurationValue": null,
                                                "dateRange": null,
                                                "collection": []
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:month",
                                            "value": {
                                                "uri": "urn:replicon:month:january",
                                                "slug": null,
                                                "bool": null,
                                                "date": null,
                                                "number": null,
                                                "text": null,
                                                "time": null,
                                                "calendarDayDurationValue": null,
                                                "workdayDurationValue": null,
                                                "dateRange": null,
                                                "collection": []
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:day-of-month",
                                            "value": {
                                                "uri": "urn:replicon:monthly-frequency-start-day-option:1st",
                                                "slug": null,
                                                "bool": null,
                                                "date": null,
                                                "number": null,
                                                "text": null,
                                                "time": null,
                                                "calendarDayDurationValue": null,
                                                "workdayDurationValue": null,
                                                "dateRange": null,
                                                "collection": []
                                            }
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                ]
            }
        )

        assign_time_offpolicywithouthistoricalpolicy_79 = rail.RepliconServiceOperator(
            task_id='assign_time_offpolicywithouthistoricalpolicy_79',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data={
                "timeOffAccount": {
                    "userUri": "{{ result('log_useruri_8') }}",
                    "timeOffTypeUri": "{{ result('invoke_custom_ruby_code_13').timeofftypeuri }}"
                },
                "policySetScheduleEntries": [
                    {
                        "effectiveDate": {
                            "year": "{{result('invoke_custom_ruby_code_21').effectiveyear}}",
                            "month": "{{result('invoke_custom_ruby_code_21').effectivemonth}}",
                            "day": "{{result('invoke_custom_ruby_code_21').effectiveday}}"
                        },
                        "description": "Effective On {{ result('invoke_custom_ruby_code_21').effectiveyear }}-{{ result('invoke_custom_ruby_code_21').effectivemonth }}-{{ result('invoke_custom_ruby_code_21').effectiveday }}",
                        "policySet": {
                            "timeOffBalanceEventScripts": [
                                {
                                    "scriptTarget": {
                                        "uri": "{{ result('invoke_custom_ruby_code_21').setstartingbalance_scripturi }}",
                                        "slug": null,
                                        "name": null
                                    },
                                    "additionalParameters": [
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:amount",
                                            "value": {
                                                "uri": null,
                                                "slug": null,
                                                "bool": null,
                                                "date": null,
                                                "number": "{{ dag_run.conf.timeoffbalance }}",
                                                "text": null,
                                                "time": null,
                                                "calendarDayDurationValue": null,
                                                "workdayDurationValue": null,
                                                "dateRange": null,
                                                "collection": []
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:precedence",
                                            "value": {
                                                "uri": null,
                                                "slug": null,
                                                "bool": null,
                                                "date": null,
                                                "number": "10",
                                                "text": null,
                                                "time": null,
                                                "calendarDayDurationValue": null,
                                                "workdayDurationValue": null,
                                                "dateRange": null,
                                                "collection": []
                                            }
                                        }
                                    ]
                                }
                            ],
                            "timeOffValidationScripts": [
                                {
                                    "scriptTarget": {
                                        "uri": "{{ result('invoke_custom_ruby_code_21').limittimeofftakenscripturi }}",
                                        "slug": null,
                                        "name": null
                                    },
                                    "additionalParameters": [
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:maximum-amount",
                                            "value": {
                                                "uri": null,
                                                "slug": null,
                                                "bool": null,
                                                "date": null,
                                                "number": "24",
                                                "text": null,
                                                "time": null,
                                                "calendarDayDurationValue": null,
                                                "workdayDurationValue": null,
                                                "dateRange": null,
                                                "collection": []
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:month",
                                            "value": {
                                                "uri": "urn:replicon:month:january",
                                                "slug": null,
                                                "bool": null,
                                                "date": null,
                                                "number": null,
                                                "text": null,
                                                "time": null,
                                                "calendarDayDurationValue": null,
                                                "workdayDurationValue": null,
                                                "dateRange": null,
                                                "collection": []
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:day-of-month",
                                            "value": {
                                                "uri": "urn:replicon:monthly-frequency-start-day-option:1st",
                                                "slug": null,
                                                "bool": null,
                                                "date": null,
                                                "number": null,
                                                "text": null,
                                                "time": null,
                                                "calendarDayDurationValue": null,
                                                "workdayDurationValue": null,
                                                "dateRange": null,
                                                "collection": []
                                            }
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                ]
            }
        )

        kla_time_off_policy_logs_add_entry_80 = rail.WriteLogOperator(
            task_id='kla_time_off_policy_logs_add_entry_80',
            log="{{ result('create_log') }}",
            message="na",
            severity="Success",
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Success",
                "reason": "NA",
                "child_job_id": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employeeid }}"
            }
        )

        if_log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_67_less_than_025_83 = rail.IfOperator(
            task_id='if_log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_67_less_than_025_83',
            test='''{{ result('log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_67') < 0.25 }}''',
            yes_task="kla_time_off_policy_logs_add_entry_84",
            no_task="if_timeofftypename_downcase_contains_2022pandemicleave_85",
        )

        kla_time_off_policy_logs_add_entry_84 = rail.WriteLogOperator(
            task_id='kla_time_off_policy_logs_add_entry_84',
            log="{{ result('create_log') }}",
            message="na",
            severity="Skipped",
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Skipped",
                "reason": "No Change in Sick Time Off balance received.",
                "child_job_id": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employeeid }}"
            }
        )

        if_timeofftypename_downcase_contains_2022pandemicleave_85 = rail.IfOperator(
            task_id='if_timeofftypename_downcase_contains_2022pandemicleave_85',
            test='''{{ dag_run.conf.timeofftypename.lower() | matches('2022 pandemic leave') }}''',
            yes_task="get_timeoffbalance_87",
            no_task="finish",
        )

        get_timeoffbalance_87 = rail.RepliconServiceOperator(
            task_id='get_timeoffbalance_87',
            endpoint="/services/reportService1.svc/GenerateReport",
            data={
                "reportUri": "{{ dag_run.conf.reporturi }}",
                "filterValues": [
                    {
                        "reportFilterUri": "{{ dag_run.conf.userfilteruri }}",
                        "value": "{{ result('invoke_custom_ruby_code_21').userfiltervalue }}"
                    },
                    {
                        "reportFilterUri": "{{ dag_run.conf.timeofftypefilteruri }}",
                        "value": "{{ result('invoke_custom_ruby_code_21').timeofftype_filtervalue }}"
                    },
                    {
                        "reportFilterUri": "{{ dag_run.conf.asofdatefilteruri }}",
                        "value": null
                    },
                    {
                        "reportFilterUri": "{{ dag_run.conf.asofdatefilteruri }}",
                        "value": null
                    },
                    {
                        "reportFilterUri": "{{ dag_run.conf.asofdatefilteruri }}",
                        "value": "{{ result('invoke_custom_ruby_code_21').reportfiltervalue_asofdate }}"
                    }
                ],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        )

        if_d_payload_not_contains_nodata_90 = rail.IfOperator(
            task_id='if_d_payload_not_contains_nodata_90',
            test='''{{ not result('get_timeoffbalance_87').payload |  matches('No Data') }}''',
            yes_task="parse_csv_91",
            no_task="log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_92",
        )

        parse_csv_91 = rail.LoadCSVFileOperator(
            task_id='parse_csv_91',
            document="{{ result('get_timeoffbalance_87').payload }}",
            headers=['time_off_type', 'time_off_balance']
        )

        load_all_csv_records_91 = rail.PythonOperator(
            task_id='load_all_csv_records_91',
            python_callable=lambda: rail.load_all_records(
                rail.result('parse_csv_91'))
        )

        log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_92 = rail.PythonOperator(
            task_id='log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_92',
            python_callable=lambda: abs(round(float(rail.result('load_all_csv_records_91')[
                                        0]['time_off_balance']) - float(rail.get_dag_run_conf()['timeoffbalance']), 2))
        )

        if_log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_92_equals_to_025_93 = rail.IfOperator(
            task_id='if_log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_92_equals_to_025_93',
            test='''{{ result('log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_92') == 0.25  or result('log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_92') > 0.25 }}''',
            yes_task="if_to_s_contains_urn_94",
            no_task="if_log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_92_less_than_025_108",
        )

        if_to_s_contains_urn_94 = rail.IfOperator(
            task_id='if_to_s_contains_urn_94',
            test='''{{ result('log_existing_timeoffpolicyschedule_24')|  is_truthy }}''',
            yes_task="parse_json_95",
            no_task="if_log_historicalpoliciestobeassigned_99_to_s_contains_urn_101",
        )

        parse_json_95 = rail.PythonOperator(
            task_id='parse_json_95',
            python_callable=lambda: rail.result(
                'log_existing_timeoffpolicyschedule_24')
        )

        invoke_custom_ruby_code_96 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_96',
            python_callable=lambda: {"timeoffoutput": min(list(filter(lambda x: x["toconsider"] == "Yes", map(lambda item: {
                "description":  item['description'],
                "effectivedate": item['effectiveDate'],
                "daydiff": get_day_diff(item),
                "toconsider": "Yes" if get_day_diff(item) > 0 else "No",
                "policyset": item['policySet'],
            }, rail.result('parse_json_95')))), key=lambda x: x['daydiff'], default=null)}

        )

        invoke_custom_ruby_code_97 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_97',
            python_callable=lambda: {"timeoffoutput": list(filter(lambda x: x["toconsider"] == "Yes", map(lambda item: {
                "description":  item['description'],
                "effectivedate": item['effectiveDate'],
                "daydiff": get_day_diff(item),
                "toconsider": "Yes" if get_day_diff(item) > 0 else "No",
                "policyset": item['policySet'],
            }, rail.result('parse_json_95'))))}
        )

        invoke_custom_ruby_code_98 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_98',
            python_callable=lambda: {
                "numberofpolicytoconsider": len(rail.result('invoke_custom_ruby_code_97')['timeoffoutput']),
                "indexforhistory": len(rail.result('invoke_custom_ruby_code_97')['timeoffoutput']) - 5 if len(rail.result('invoke_custom_ruby_code_97')['timeoffoutput']) > 5 else 0,
                "limittimeofftakenscripturi": rail.find_first_by_attr_and_get_attr(rail.result('get_all_timeoffvalidation_scripts_17'), 'displayText', "Limit amount of time off taken", ('uri'))
            }
        )

        log_historicalpoliciestobeassigned_99 = rail.PythonOperator(
            task_id='log_historicalpoliciestobeassigned_99',
            python_callable=lambda:  json.loads((json.dumps(rail.result('parse_json_95')[rail.result('invoke_custom_ruby_code_51')[
                                                'indexforhistory']:5]).replace('null', '"effective"').replace('"script"', '"scriptTarget"')))
        )

        if_log_historicalpoliciestobeassigned_99_to_s_contains_urn_101 = rail.IfOperator(
            task_id='if_log_historicalpoliciestobeassigned_99_to_s_contains_urn_101',
            test='''{{ result('log_historicalpoliciestobeassigned_99') | to_json | matches('urn')  }}''',
            yes_task="assign_time_offpolicyalongwithhistoricalpolicy_102",
            no_task="assign_time_offpolicywithouthistoricalpolicy_104",
        )

        assign_time_offpolicyalongwithhistoricalpolicy_102 = rail.RepliconServiceOperator(
            task_id='assign_time_offpolicyalongwithhistoricalpolicy_102',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda: {
                "timeOffAccount": {
                    "userUri": rail.render_template("{{ result('log_useruri_8') }}"),
                    "timeOffTypeUri": rail.render_template("{{ result('invoke_custom_ruby_code_13').timeofftypeuri }}")
                },
                "policySetScheduleEntries":
                    rail.result('log_historicalpoliciestobeassigned_99') +
                    [{
                        "effectiveDate": {
                            "year": rail.render_template("{{result('invoke_custom_ruby_code_21').effectiveyear}}"),
                            "month": rail.render_template("{{result('invoke_custom_ruby_code_21').effectivemonth}}"),
                            "day": rail.render_template("{{result('invoke_custom_ruby_code_21').effectiveday}}")
                        },
                        "description": rail.render_template("Effective On {{ result('invoke_custom_ruby_code_21').effectiveyear }}-{{ result('invoke_custom_ruby_code_21').effectivemonth }}-{{ result('invoke_custom_ruby_code_21').effectiveday }}"),
                        "policySet": {
                            "timeOffBalanceEventScripts": [
                                {
                                    "scriptTarget": {
                                        "uri": rail.render_template("{{ result('invoke_custom_ruby_code_21').setstartingbalance_scripturi }}"),
                                        "slug": null,
                                        "name": null
                                    },
                                    "additionalParameters": [
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:amount",
                                            "value": {
                                                "uri": null,
                                                "slug": null,
                                                "bool": null,
                                                "date": null,
                                                "number": rail.render_template("{{ dag_run.conf.timeoffbalance }}"),
                                                "text": null,
                                                "time": null,
                                                "calendarDayDurationValue": null,
                                                "workdayDurationValue": null,
                                                "dateRange": null,
                                                "collection": []
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:precedence",
                                            "value": {
                                                "uri": null,
                                                "slug": null,
                                                "bool": null,
                                                "date": null,
                                                "number": "10",
                                                "text": null,
                                                "time": null,
                                                "calendarDayDurationValue": null,
                                                "workdayDurationValue": null,
                                                "dateRange": null,
                                                "collection": []
                                            }
                                        }
                                    ]
                                }
                            ],
                            "timeOffValidationScripts": []
                        }
                    }
                ]
            }
        )

        assign_time_offpolicywithouthistoricalpolicy_104 = rail.RepliconServiceOperator(
            task_id='assign_time_offpolicywithouthistoricalpolicy_104',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data={
                "timeOffAccount": {
                    "userUri": "{{ result('log_useruri_8') }}",
                    "timeOffTypeUri": "{{ result('invoke_custom_ruby_code_13').timeofftypeuri }}"
                },
                "policySetScheduleEntries": [
                    {
                        "effectiveDate": {
                            "year": "{{result('invoke_custom_ruby_code_21').effectiveyear}}",
                            "month": "{{result('invoke_custom_ruby_code_21').effectivemonth}}",
                            "day": "{{result('invoke_custom_ruby_code_21').effectiveday}}"
                        },
                        "description": "Effective On {{ result('invoke_custom_ruby_code_21').effectiveyear }}-{{ result('invoke_custom_ruby_code_21').effectivemonth }}-{{ result('invoke_custom_ruby_code_21').effectiveday }}",
                        "policySet": {
                            "timeOffBalanceEventScripts": [
                                {
                                    "scriptTarget": {
                                        "uri": "{{ result('invoke_custom_ruby_code_21').setstartingbalance_scripturi }}",
                                        "slug": null,
                                        "name": null
                                    },
                                    "additionalParameters": [
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:amount",
                                            "value": {
                                                "uri": null,
                                                "slug": null,
                                                "bool": null,
                                                "date": null,
                                                "number": "{{ dag_run.conf.timeoffbalance }}",
                                                "text": null,
                                                "time": null,
                                                "calendarDayDurationValue": null,
                                                "workdayDurationValue": null,
                                                "dateRange": null,
                                                "collection": []
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:precedence",
                                            "value": {
                                                "uri": null,
                                                "slug": null,
                                                "bool": null,
                                                "date": null,
                                                "number": "10",
                                                "text": null,
                                                "time": null,
                                                "calendarDayDurationValue": null,
                                                "workdayDurationValue": null,
                                                "dateRange": null,
                                                "collection": []
                                            }
                                        }
                                    ]
                                }
                            ],
                            "timeOffValidationScripts": []
                        }
                    }
                ]
            }
        )

        kla_time_off_policy_logs_add_entry_105 = rail.WriteLogOperator(
            task_id='kla_time_off_policy_logs_add_entry_105',
            log="{{ result('create_log') }}",
            message="na",
            severity="Success",
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Success",
                "reason": "NA",
                "child_job_id": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employeeid }}"
            }
        )

        if_log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_92_less_than_025_108 = rail.IfOperator(
            task_id='if_log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_92_less_than_025_108',
            test='''{{ result('log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_92') < 0.25 }}''',
            yes_task="kla_time_off_policy_logs_add_entry_109",
            no_task="finish",
        )

        kla_time_off_policy_logs_add_entry_109 = rail.WriteLogOperator(
            task_id='kla_time_off_policy_logs_add_entry_109',
            log="{{ result('create_log') }}",
            message="na",
            severity="Skipped",
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Skipped",
                "reason": "No Change in 2022 pandemic leave balance received.",
                "child_job_id": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employeeid }}"
            }
        )

        if_output_timeofftypeuri_blank_111 = rail.IfOperator(
            task_id='if_output_timeofftypeuri_blank_111',
            test='''{{ result('invoke_custom_ruby_code_13').timeofftypeuri | is_falsy }}''',
            yes_task="kla_time_off_policy_logs_add_entry_112",
            no_task="finish",
        )

        kla_time_off_policy_logs_add_entry_112 = rail.WriteLogOperator(
            task_id='kla_time_off_policy_logs_add_entry_112',
            log="{{ result('create_log') }}",
            message="na",
            severity="Skipped",
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Skipped",
                "reason": "Timeoff type not found in Replicon",
                "child_job_id": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employeeid }}"
            }
        )

        kla_time_off_policy_logs_add_entry_115 = rail.WriteLogOperator(
            task_id='kla_time_off_policy_logs_add_entry_115',
            log="{{ result('create_log') }}",
            message="na",
            trigger_rule='one_failed',
            severity="Error",
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Error",
                "reason": "{{ get_error_message() }}",
                "child_job_id": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employeeid }}"
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> create_log >> if_split_smart_join_present_4
        if_split_smart_join_present_4
        if_split_smart_join_present_4 >> rail.Label(
            'Yes') >> kla_time_off_policy_logs_add_entry_5 >> finish
        if_split_smart_join_present_4 >> rail.Label(
            'No') >> search_users_7 >> log_useruri_8 >> if_log_useruri_8_blank_9
        if_log_useruri_8_blank_9 >> rail.Label(
            'Yes') >> kla_time_off_policy_logs_add_entry_10 >> stop_11 >> finish
        if_log_useruri_8_blank_9 >> rail.Label(
            'No') >> get_time_off_type_assignments_for_user_12 >> invoke_custom_ruby_code_13 >> if_output_timeofftypename_blank_14
        if_output_timeofftypename_blank_14 >> rail.Label(
            'Yes') >> kla_time_off_policy_logs_add_entry_15 >> stop_16 >> finish
        if_output_timeofftypename_blank_14 >> rail.Label(
            'No') >> get_all_timeoffvalidation_scripts_17 >> get_all_timeoffbalanceevent_scripts_18 >> invoke_custom_ruby_code_21 >> if_output_timeofftypeuri_present_22
        if_output_timeofftypeuri_present_22 >> rail.Label(
            'Yes') >> get_user_time_off_type_policy_summary_23 >> log_existing_timeoffpolicyschedule_24 >> if_timeofftypename_downcase_contains_pto_25
        if_output_timeofftypeuri_present_22 >> rail.Label(
            'No') >> if_output_timeofftypeuri_blank_111
        if_timeofftypename_downcase_contains_pto_25 >> rail.Label(
            'Yes') >> declare_variable_26 >> if_to_s_contains_urn_27
        if_to_s_contains_urn_27 >> rail.Label(
            'Yes') >> parse_json_28 >> invoke_custom_ruby_code_29 >> invoke_custom_ruby_code_30 >> if_first_toconsider_present_31
        if_to_s_contains_urn_27 >> rail.Label(
            'No') >> if_first_toconsider_present_31
        if_first_toconsider_present_31 >> rail.Label(
            'Yes') >> parse_json_32 >> if_script_name_present_33
        if_script_name_present_33 >> rail.Label(
            'Yes') >> parse_json_34 >> if_first_keyuri_present_35
        if_first_keyuri_present_35 >> rail.Label(
            'Yes') >> parse_json_36 >> invoke_custom_ruby_code_37
        if_first_keyuri_present_35 >> rail.Label(
            'No') >> invoke_custom_ruby_code_37
        if_script_name_present_33 >> rail.Label(
            'No') >> invoke_custom_ruby_code_37
        if_first_toconsider_present_31 >> rail.Label(
            'No') >> invoke_custom_ruby_code_37 >> if_output_difference_equals_to_025_38
        if_output_difference_equals_to_025_38 >> rail.Label(
            'Yes') >> update_variable_39 >> get_timeoffbalance_41
        if_output_difference_equals_to_025_38 >> rail.Label(
            'No') >> get_timeoffbalance_41 >> if_d_payload_not_contains_nodata_44
        if_d_payload_not_contains_nodata_44 >> rail.Label(
            'Yes') >> parse_csv_45 >> load_all_csv_records_45 >> log_differencebetweencurrentbalanceandbalancefromfeedfile_46
        if_d_payload_not_contains_nodata_44 >> rail.Label(
            'No') >> log_differencebetweencurrentbalanceandbalancefromfeedfile_46 >> if_log_differencebetweencurrentbalanceandbalancefromfeedfile_46_equals_to_025_47
        if_log_differencebetweencurrentbalanceandbalancefromfeedfile_46_equals_to_025_47 >> rail.Label(
            'Yes') >> update_variable_identifiertotriggerpolicyupdate_48 >> if_declare_variable_26_value_equals_to_yes_49
        if_log_differencebetweencurrentbalanceandbalancefromfeedfile_46_equals_to_025_47 >> rail.Label(
            'No') >> if_declare_variable_26_value_equals_to_yes_49
        if_declare_variable_26_value_equals_to_yes_49 >> rail.Label(
            'Yes') >> if_to_s_contains_urn_50
        if_to_s_contains_urn_50 >> rail.Label(
            'Yes') >> invoke_custom_ruby_code_51 >> log_historicalpoliciestobeassignedmodified_52 >> if_to_s_contains_urn_53
        if_to_s_contains_urn_50 >> rail.Label('No') >> if_to_s_contains_urn_53
        if_to_s_contains_urn_53 >> rail.Label(
            'Yes') >> assign_time_offpolicyalongwithhistoricalpolicy_54 >> kla_time_off_policy_logs_add_entry_57
        if_to_s_contains_urn_53 >> rail.Label(
            'No') >> assign_time_offpolicyalongwithouthistoricalpolicy_56 >> kla_time_off_policy_logs_add_entry_57 >> if_timeofftypename_downcase_contains_sick_60
        if_declare_variable_26_value_equals_to_yes_49 >> rail.Label(
            'No') >> kla_time_off_policy_logs_add_entry_59 >> if_timeofftypename_downcase_contains_sick_60
        if_timeofftypename_downcase_contains_pto_25 >> rail.Label(
            'No') >> if_timeofftypename_downcase_contains_sick_60
        if_timeofftypename_downcase_contains_sick_60 >> rail.Label(
            'Yes') >> get_timeoffbalance_62 >> if_d_payload_not_contains_nodata_65
        if_d_payload_not_contains_nodata_65 >> rail.Label(
            'Yes') >> parse_csv_66 >> load_all_csv_records_66 >> log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_67
        if_d_payload_not_contains_nodata_65 >> rail.Label(
            'No') >> log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_67 >> if_log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_67_equals_to_025_68
        if_log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_67_equals_to_025_68 >> rail.Label(
            'Yes') >> if_to_s_contains_urn_69
        if_to_s_contains_urn_69 >> rail.Label(
            'Yes') >> parse_json_70 >> invoke_custom_ruby_code_71 >> invoke_custom_ruby_code_72 >> invoke_custom_ruby_code_73 >> log_historicalpoliciestobeassigned_74 >> if_to_s_contains_urn_76
        if_to_s_contains_urn_69 >> rail.Label('No') >> if_to_s_contains_urn_76
        if_to_s_contains_urn_76 >> rail.Label(
            'Yes') >> assign_time_offpolicyalongwithhistoricalpolicy_77 >> kla_time_off_policy_logs_add_entry_80 >> if_log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_67_less_than_025_83
        if_to_s_contains_urn_76 >> rail.Label(
            'No') >> assign_time_offpolicywithouthistoricalpolicy_79 >> kla_time_off_policy_logs_add_entry_80 >> if_log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_67_less_than_025_83
        if_log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_67_equals_to_025_68 >> rail.Label(
            'No') >> if_log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_67_less_than_025_83
        if_log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_67_less_than_025_83 >> rail.Label(
            'Yes') >> kla_time_off_policy_logs_add_entry_84 >> if_timeofftypename_downcase_contains_2022pandemicleave_85
        if_log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_67_less_than_025_83 >> rail.Label(
            'No') >> if_timeofftypename_downcase_contains_2022pandemicleave_85
        if_timeofftypename_downcase_contains_sick_60 >> rail.Label(
            'No') >> if_timeofftypename_downcase_contains_2022pandemicleave_85
        if_timeofftypename_downcase_contains_2022pandemicleave_85 >> rail.Label(
            'Yes') >> get_timeoffbalance_87 >> if_d_payload_not_contains_nodata_90
        if_d_payload_not_contains_nodata_90 >> rail.Label(
            'Yes') >> parse_csv_91 >> load_all_csv_records_91 >> log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_92
        if_d_payload_not_contains_nodata_90 >> rail.Label(
            'No') >> log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_92 >> if_log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_92_equals_to_025_93
        if_log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_92_equals_to_025_93 >> rail.Label(
            'Yes') >> if_to_s_contains_urn_94
        if_to_s_contains_urn_94 >> rail.Label(
            'Yes') >> parse_json_95 >> invoke_custom_ruby_code_96 >> invoke_custom_ruby_code_97 >> invoke_custom_ruby_code_98 >> log_historicalpoliciestobeassigned_99 >> if_log_historicalpoliciestobeassigned_99_to_s_contains_urn_101
        if_to_s_contains_urn_94 >> rail.Label(
            'No') >> if_log_historicalpoliciestobeassigned_99_to_s_contains_urn_101
        if_log_historicalpoliciestobeassigned_99_to_s_contains_urn_101 >> rail.Label(
            'Yes') >> assign_time_offpolicyalongwithhistoricalpolicy_102 >> kla_time_off_policy_logs_add_entry_105 >> if_log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_92_less_than_025_108
        if_log_historicalpoliciestobeassigned_99_to_s_contains_urn_101 >> rail.Label(
            'No') >> assign_time_offpolicywithouthistoricalpolicy_104 >> kla_time_off_policy_logs_add_entry_105 >> if_log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_92_less_than_025_108
        if_log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_92_equals_to_025_93 >> rail.Label(
            'No') >> if_log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_92_less_than_025_108
        if_log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_92_less_than_025_108 >> rail.Label(
            'Yes') >> kla_time_off_policy_logs_add_entry_109 >> finish
        if_log_differencebetweencurrentandbalancefromfeedfileconvertedtopositivevalues_92_less_than_025_108 >> rail.Label(
            'No') >> finish
        if_timeofftypename_downcase_contains_2022pandemicleave_85 >> rail.Label(
            'No') >> finish
        if_output_timeofftypeuri_blank_111 >> rail.Label(
            'Yes') >> kla_time_off_policy_logs_add_entry_112 >> finish
        if_output_timeofftypeuri_blank_111 >> rail.Label(
            'No') >> finish >> kla_time_off_policy_logs_add_entry_115 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
