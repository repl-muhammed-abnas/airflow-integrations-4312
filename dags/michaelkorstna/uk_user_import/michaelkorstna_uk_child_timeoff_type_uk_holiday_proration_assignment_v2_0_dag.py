
from datetime import timedelta, datetime
import json
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'michaelkorstna_uk_user_import_timeoff_type_uk_holiday_proration_assignment_child_{config.instance}',
        description=f'MichaelKorsTnA UK_Child Timeoff type UK Holiday Proration Assignment v2.0 {config.instance}',
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
            end_task='log_to_sumo',
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
            test=lambda: rail.result('get_default_time_off_type_policy_schedule_for_user_4') and rail.result(
                'get_default_time_off_type_policy_schedule_for_user_4')[0]['effectiveDate'] and rail.result(
                'get_default_time_off_type_policy_schedule_for_user_4')[0]['effectiveDate']['day'],
            yes_task="log_gettheaccrualbalancesetup_7",
            no_task="catch_and_handle_error",
        )

        log_gettheaccrualbalancesetup_7 = rail.PythonOperator(
            task_id='log_gettheaccrualbalancesetup_7',
            python_callable=lambda: (json.dumps(rail.find_first_by_attr_and_get_attr(rail.result(
                'get_default_time_off_type_policy_schedule_for_user_4')[0]['policySet']['timeOffBalanceEventScripts'], 'script.name',
                'Yearly/Monthly Accrual with Expiry & Rounding', 'additionalParameters', ''))).replace("[[", "[").replace("]]", "]")
        )

        log_gettheaccrualbalance_9 = rail.PythonOperator(
            task_id='log_gettheaccrualbalance_9',
            python_callable=lambda: float(rail.find_first_by_attr_and_get_attr(json.loads(rail.result(
                'log_gettheaccrualbalancesetup_7')), 'keyUri', 'urn:replicon:script-key:parameter:yearly-entitlement', 'value.number', 0))
        )

        log_existing_accrual_10 = rail.PythonOperator(
            task_id='log_existing_accrual_10',
            python_callable=lambda:  '''{"keyUri": "urn:replicon:script-key:parameter:yearly-entitlement", "value": {"number": ''' + str(
                rail.result('log_gettheaccrualbalance_9')) + '''}}'''
        )

        log_getthestartingbalancesetup_11 = rail.PythonOperator(
            task_id='log_getthestartingbalancesetup_11',
            python_callable=lambda: (json.dumps(rail.find_first_by_attr_and_get_attr(rail.result(
                'get_default_time_off_type_policy_schedule_for_user_4')[0]['policySet']['timeOffBalanceEventScripts'], 'script.name',
                'Starting Balance Set To', 'additionalParameters', ''))).replace("[[", "[").replace("]]", "]")
        )

        log_getthestartingbalancescript_12 = rail.PythonOperator(
            task_id='log_getthestartingbalancescript_12',
            python_callable=lambda: (json.dumps(rail.find_first_by_attr_and_get_attr(rail.result(
                'get_default_time_off_type_policy_schedule_for_user_4')[0]['policySet']['timeOffBalanceEventScripts'], 'script.name',
                'Starting Balance Set To'))).replace('[{"additionalParameters"', '{"additionalParameters"').replace("}}]", "}}").replace(
                '}},"script"', '}}],"script"')
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

        log_required_accrual_16 = rail.PythonOperator(
            task_id='log_required_accrual_16',
            python_callable=lambda dag_run: round(float(float(
                dag_run.conf['scheduledweeklyhours']) / 40) * float(rail.result('log_gettheaccrualbalance_9')))
        )

        if_request_yearlyentitlement_present_17 = rail.IfOperator(
            task_id='if_request_yearlyentitlement_present_17',
            test='''{{ dag_run.conf.yearlyentitlement | is_truthy }}''',
            yes_task="updated_u_d_fforyearlyentitlement_18",
            no_task="log_required_accrual_json_19",
        )

        updated_u_d_fforyearlyentitlement_18 = rail.RepliconServiceOperator(
            task_id='updated_u_d_fforyearlyentitlement_18',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.yearlyentitlement }}",
                "value": "{{ result('log_required_accrual_16') }}"
            }
        )

        log_required_accrual_json_19 = rail.PythonOperator(
            task_id='log_required_accrual_json_19',
            python_callable=lambda:  '{"keyUri": "urn:replicon:script-key:parameter:yearly-entitlement", "value": {"number": ' + str(
                rail.result('log_required_accrual_16')) + '}}'
        )

        log_usedtoremovestartingbalancesetup_20 = rail.PythonOperator(
            task_id='log_usedtoremovestartingbalancesetup_20',
            #pylint: disable= line-too-long
            python_callable=lambda: '{"additionalParameters":[{"keyUri":"urn:replicon:script-key:parameter:amount","value":{"number":0.0}},{"keyUri":"urn:replicon:script-key:parameter:precedence","value":{"number":10.0}}],"scriptTarget":{"description":"Set initial balance for the first day of a policy","name":"Starting Balance Set To","uri":"urn:replicon-tenant:' + rail.get_tenant_slug() +
                ':script:b4650f77-bf85-488f-9b14-ad1effe82081"}},'
        )

        log_usedtoremovestartingbalancesetup_21 = rail.PythonOperator(
            task_id='log_usedtoremovestartingbalancesetup_21',
            #pylint: disable= line-too-long
            python_callable=lambda:  '{"additionalParameters":[{"keyUri":"urn:replicon:script-key:parameter:amount","value":{"number":0.0}},{"keyUri":"urn:replicon:script-key:parameter:precedence","value":{"number":10.0}}],"scriptTarget":{"description":"Set initial balance for the first day of a policy","name":"Starting Balance Set To","uri":"urn:replicon-tenant:' + rail.get_tenant_slug() +
            ':script:b4650f77-bf85-488f-9b14-ad1effe82081"}}'
        )

        if_request_type_equals_to_add_22 = rail.IfOperator(
            task_id='if_request_type_equals_to_add_22',
            test='''{{ dag_run.conf.type == 'Add' }}''',
            yes_task="log_timeoff_policy_23",
            no_task="if_request_type_equals_to_update_25",
        )

        log_timeoff_policy_23 = rail.PythonOperator(
            task_id='log_timeoff_policy_23',
            python_callable=lambda: (json.dumps(rail.result('get_default_time_off_type_policy_schedule_for_user_4'))).replace(
                "null", '\"effective\"').replace('\"script\"', '\"scriptTarget\"').replace(rail.result('log_existing_accrual_10'), rail.result(
                    'log_required_accrual_json_19')).replace(rail.result('log_usedtoremovestartingbalancesetup_20'), "").replace(rail.result(
                        'log_usedtoremovestartingbalancesetup_21'), "")
        )

        put_user_time_off_account_policy_set_schedule_24 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_24',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": json.loads(rail.result('log_timeoff_policy_23'))
            }
        )

        if_request_type_equals_to_update_25 = rail.IfOperator(
            task_id='if_request_type_equals_to_update_25',
            test='''{{ dag_run.conf.type == 'Update' }}''',
            yes_task="declare_list_26",
            no_task="catch_and_handle_error",
        )

        declare_list_26 = rail.SetVariableOperator(
            task_id='declare_list_26',
            append=False,
            name='timeoffpolicy',
            value=[]
        )

        get_user_time_off_type_policy_summary_27 = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary_27',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        foreach_d_28 = rail.ForEachOperator(
            task_id='foreach_d_28',
            items=lambda: rail.result('get_user_time_off_type_policy_summary_27')[
                'policiesByTimeOffType'],
            start_task='if_timeofftype_name_equals_to_dataworkato_servicereceive_requestrequesttimeofftype_29',
            end_task='foreach_d_28_end'
        )

        if_timeofftype_name_equals_to_dataworkato_servicereceive_requestrequesttimeofftype_29 = rail.IfOperator(
            task_id='if_timeofftype_name_equals_to_dataworkato_servicereceive_requestrequesttimeofftype_29',
            test='''{{ result('foreach_d_28').timeOffType.name == dag_run.conf.timeofftype }}''',
            yes_task="foreach_foreach_d_28_30",
            no_task="foreach_d_28_end",
        )

        declare_list_to_store_effectivedateofpolicies = rail.SetVariableOperator(
            task_id='declare_list_to_store_effectivedateofpolicies',
            name='effectivedateofpolicies',
            append=False,
            value=[]
        )

        foreach_foreach_d_28_30 = rail.ForEachOperator(
            task_id='foreach_foreach_d_28_30',
            items=lambda: rail.result('foreach_d_28')['policySetSchedule'],
            start_task='log_effective_date_31',
            end_task='foreach_foreach_d_28_30_end'
        )

        def get_date_string(dateobj):
            return str(dateobj['day']) + '/' + str(dateobj['month']) + '/' + str(dateobj['year'])

        log_effective_date_31 = rail.PythonOperator(
            task_id='log_effective_date_31',
            python_callable=lambda:  (datetime.strptime(get_date_string(rail.result(
                'foreach_foreach_d_28_30')['effectiveDate']), "%d/%m/%Y")).strftime("%Y-%m-%d")
        )

        if_to_date_less_than_dataworkato_servicereceive_requestrequeststartdateto_date_32 = rail.IfOperator(
            task_id='if_to_date_less_than_dataworkato_servicereceive_requestrequeststartdateto_date_32',
            test=lambda dag_run: datetime.strptime(rail.result(
                'log_effective_date_31'), '%Y-%m-%d') < datetime.strptime(dag_run.conf['startdate'], '%d/%m/%Y'),
            yes_task="accumulate_list_items_33",
            no_task="foreach_foreach_d_28_30_end",
        )

        accumulate_list_items_33 = rail.SetVariableOperator(
            task_id='accumulate_list_items_33',
            name='effectivedateofpolicies',
            append=True,
            value=lambda: {
                "effectivedate": rail.result('log_effective_date_31'),
                "policyset": rail.result('foreach_foreach_d_28_30')['policySet']
            }
        )

        foreach_foreach_d_28_30_end = rail.EmptyOperator(
            task_id='foreach_foreach_d_28_30_end',
        )

        log_effective_dateto_consider_35 = rail.PythonOperator(
            task_id='log_effective_dateto_consider_35',
            python_callable=lambda: (max(rail.get_dag_run_var('effectivedateofpolicies'), key=lambda x: x['effectivedate']))[
                'effectivedate'] if rail.result('accumulate_list_items_33') else ''
        )

        foreach_foreach_d_28_37 = rail.ForEachOperator(
            task_id='foreach_foreach_d_28_37',
            items=lambda: rail.result('foreach_d_28')['policySetSchedule'],
            start_task='log_effective_date_38',
            end_task='foreach_foreach_d_28_37_end'
        )

        log_effective_date_38 = rail.PythonOperator(
            task_id='log_effective_date_38',
            python_callable=lambda: (datetime.strptime(get_date_string(rail.result(
                'foreach_foreach_d_28_37')['effectiveDate']), "%d/%m/%Y")).strftime("%Y-%m-%d")
        )

        if_to_date_less_than_dataloggerlog_effective_dateto_consider_36messageto_date_39 = rail.IfOperator(
            task_id='if_to_date_less_than_dataloggerlog_effective_dateto_consider_36messageto_date_39',
            test=lambda: datetime.strptime(rail.result('log_effective_date_38'), '%Y-%m-%d') < datetime.strptime(
                rail.result('log_effective_dateto_consider_35'), '%Y-%m-%d'),
            yes_task="insert_to_list_40",
            no_task="foreach_foreach_d_28_37_end",
        )

        insert_to_list_40 = rail.SetVariableOperator(
            task_id='insert_to_list_40',
            append=True,
            name='{{ result("declare_list_26").name }}',
            value=lambda: {
                "description": rail.result('foreach_foreach_d_28_37')['description'],
                "effectiveDate": {
                    "day": rail.result('foreach_foreach_d_28_37')['effectiveDate']['day'],
                    "month": rail.result('foreach_foreach_d_28_37')['effectiveDate']['month'],
                    "year": rail.result('foreach_foreach_d_28_37')['effectiveDate']['year']
                },
                "policySet": rail.result('foreach_foreach_d_28_37')['policySet']
            }
        )

        foreach_foreach_d_28_37_end = rail.EmptyOperator(
            task_id='foreach_foreach_d_28_37_end',
        )

        foreach_d_28_end = rail.EmptyOperator(
            task_id='foreach_d_28_end',
        )

        def get_effective_policy_to_consider():
            policylist = rail.get_dag_run_var('effectivedateofpolicies')
            effectivepolicy = []
            for policy in policylist:
                if policy not in effectivepolicy:
                    effectivepolicy.append(policy)
            return (json.dumps(effectivepolicy)).replace('[{"timeOffBalanceEventScripts"', '{"timeOffBalanceEventScripts"').replace(
                "[]}]", "[]}").replace("[,{", "[{").replace('}},],"timeOffValidationScripts', '}}],"timeOffValidationScripts').replace(
                '"timeOffValidationScripts":}', '"timeOffValidationScripts":[]}').replace("[]}]", "[]}").replace("}}]}]", "}}]}").replace(
                '"additionalParameters":,', '"additionalParameters":[],')

        log_policy_set_43 = rail.PythonOperator(
            task_id='log_policy_set_43',
            python_callable=get_effective_policy_to_consider
        )

        insert_to_list_45 = rail.SetVariableOperator(
            task_id='insert_to_list_45',
            append=True,
            name='{{ result("declare_list_26").name }}',
            value=lambda: {
                "description": "Effective on " + rail.result('log_effective_dateto_consider_35'),
                "effectiveDate": {
                    "day": (datetime.strptime(rail.result('log_effective_dateto_consider_35'), '%Y-%m-%d')).day,
                    "month": (datetime.strptime(rail.result('log_effective_dateto_consider_35'), '%Y-%m-%d')).month,
                    "year": (datetime.strptime(rail.result('log_effective_dateto_consider_35'), '%Y-%m-%d')).year
                },
                "policySet": json.loads(rail.result('log_policy_set_43'))
            }
        )

        get_default_time_off_policy_set_schedule_for_time_off_type_46 = rail.RepliconServiceOperator(
            task_id='get_default_time_off_policy_set_schedule_for_time_off_type_46',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
            }
        )

        log_policy_set_47 = rail.PythonOperator(
            task_id='log_policy_set_47',
            python_callable=lambda: (json.dumps(rail.result(
                'get_default_time_off_policy_set_schedule_for_time_off_type_46')[0]['policySet'])).replace(rail.result(
                    'log_existing_accrual_10'), rail.result('log_required_accrual_json_19')).replace(rail.result(
                        'log_getthestartingbalancescript_12'), "").replace("[,{", "[{").replace("},,{", "},{").replace(
                '"timeOffValidationScripts":}', '"timeOffValidationScripts":[]}').replace("[]}]", "[]}").replace("}},]", "}}]")
        )

        insert_to_list_49 = rail.SetVariableOperator(
            task_id='insert_to_list_49',
            append=True,
            name='{{ result("declare_list_26").name }}',
            value=lambda dag_run: {
                "description": "Effective on " + dag_run.conf['startdate'],
                "effectiveDate": {
                    "day": (datetime.strptime(dag_run.conf['startdate'], "%d/%m/%Y")).day,
                    "month": (datetime.strptime(dag_run.conf['startdate'], "%d/%m/%Y")).month,
                    "year": (datetime.strptime(dag_run.conf['startdate'], "%d/%m/%Y")).year
                },
                "policySet": json.loads(rail.result('log_policy_set_47'))
            }
        )

        log_policytoassign_50 = rail.PythonOperator(
            task_id='log_policytoassign_50',
            python_callable=lambda: (json.dumps(rail.get_dag_run_var('timeoffpolicy'))).replace('\"script\"', '\"scriptTarget\"').replace(
                "[,{", "[{").replace("},,{", "},{")
        )

        put_user_time_off_account_policy_set_schedule_51 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_51',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": json.loads(rail.result('log_policytoassign_50'))
            }
        )

        executetimeoffpolicyrecalculation_52 = rail.RepliconServiceOperator(
            task_id='executetimeoffpolicyrecalculation_52',
            endpoint="/services/TimeOffService2.svc/ExecuteTimeOffPolicyTransactionCalculation",
            data={
                "account": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
                },
                "calculationEndDate": null,
                "scriptDataRecalculationOptionUri": "urn:replicon:time-off-script-data-recalculation-option:recalculate-full-history",
                "unitOfWorkId": "{{ dag_run_ecid() }}"
            }
        )

        catch_and_handle_error = rail.EmptyOperator(
            task_id='catch_and_handle_error',
            trigger_rule='one_failed',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> get_default_time_off_type_policy_schedule_for_user_4
        get_default_time_off_type_policy_schedule_for_user_4 >> if_effectivedate_day_present_6
        if_effectivedate_day_present_6 >> rail.Label(
            'Yes') >> log_gettheaccrualbalancesetup_7 >> log_gettheaccrualbalance_9 >> log_existing_accrual_10 >> log_getthestartingbalancesetup_11
        log_getthestartingbalancesetup_11 >> log_getthestartingbalancescript_12 >> log_getthestartingbalance_14 >> log_existing_starting_balance_15
        log_existing_starting_balance_15 >> log_required_accrual_16 >> if_request_yearlyentitlement_present_17
        if_request_yearlyentitlement_present_17 >> rail.Label(
            'Yes') >> updated_u_d_fforyearlyentitlement_18 >> log_required_accrual_json_19
        if_request_yearlyentitlement_present_17 >> rail.Label(
            'No') >> log_required_accrual_json_19 >> log_usedtoremovestartingbalancesetup_20 >> log_usedtoremovestartingbalancesetup_21
        log_usedtoremovestartingbalancesetup_21 >> if_request_type_equals_to_add_22
        if_request_type_equals_to_add_22 >> rail.Label(
            'Yes') >> log_timeoff_policy_23 >> put_user_time_off_account_policy_set_schedule_24 >> if_request_type_equals_to_update_25
        if_request_type_equals_to_add_22 >> rail.Label(
            'No') >> if_request_type_equals_to_update_25
        if_request_type_equals_to_update_25 >> rail.Label(
            'Yes') >> declare_list_26 >> get_user_time_off_type_policy_summary_27 >> declare_list_to_store_effectivedateofpolicies
        declare_list_to_store_effectivedateofpolicies >> foreach_d_28 >> if_timeofftype_name_equals_to_dataworkato_servicereceive_requestrequesttimeofftype_29
        if_timeofftype_name_equals_to_dataworkato_servicereceive_requestrequesttimeofftype_29 >> rail.Label(
            'Yes') >> foreach_foreach_d_28_30 >> log_effective_date_31 >> if_to_date_less_than_dataworkato_servicereceive_requestrequeststartdateto_date_32
        if_to_date_less_than_dataworkato_servicereceive_requestrequeststartdateto_date_32 >> rail.Label(
            'Yes') >> accumulate_list_items_33 >> foreach_foreach_d_28_30_end
        if_to_date_less_than_dataworkato_servicereceive_requestrequeststartdateto_date_32 >> rail.Label(
            'No') >> foreach_foreach_d_28_30_end
        foreach_foreach_d_28_30 >> foreach_foreach_d_28_30_end >> log_effective_dateto_consider_35 >> foreach_foreach_d_28_37 >> log_effective_date_38
        log_effective_date_38 >> if_to_date_less_than_dataloggerlog_effective_dateto_consider_36messageto_date_39
        if_to_date_less_than_dataloggerlog_effective_dateto_consider_36messageto_date_39 >> rail.Label(
            'Yes') >> insert_to_list_40 >> foreach_foreach_d_28_37_end
        if_to_date_less_than_dataloggerlog_effective_dateto_consider_36messageto_date_39 >> rail.Label(
            'No') >> foreach_foreach_d_28_37_end
        foreach_foreach_d_28_37 >> foreach_foreach_d_28_37_end >> foreach_d_28_end
        if_timeofftype_name_equals_to_dataworkato_servicereceive_requestrequesttimeofftype_29 >> rail.Label(
            'No') >> foreach_d_28_end
        foreach_d_28 >> foreach_d_28_end >> log_policy_set_43 >> insert_to_list_45 >> get_default_time_off_policy_set_schedule_for_time_off_type_46
        get_default_time_off_policy_set_schedule_for_time_off_type_46 >> log_policy_set_47 >> insert_to_list_49 >> log_policytoassign_50
        log_policytoassign_50 >> put_user_time_off_account_policy_set_schedule_51 >> executetimeoffpolicyrecalculation_52 >> catch_and_handle_error
        if_request_type_equals_to_update_25 >> rail.Label(
            'No') >> catch_and_handle_error
        if_effectivedate_day_present_6 >> rail.Label(
            'No') >> catch_and_handle_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
