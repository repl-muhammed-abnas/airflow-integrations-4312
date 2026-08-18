
from datetime import timedelta, datetime
import json
from dateutil.relativedelta import relativedelta
from airflow.models import Variable
import rail
from michaelkorstna.austria_user_import.utils import custom_methods
null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'michaelkorstna_austria_user_import_timeoff_type_accrual_stoppage_child_{config.instance}',
        description=f'MichaelKorsTnA Austria_child Timeoff type Accrual Stoppage v1.0 {config.instance}',
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
            no_task='log_requireddatetoconsiderforpast_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_requireddatetoconsiderforpast_3',
            end_task='catch_and_handle_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_requireddatetoconsiderforpast_3 = rail.PythonOperator(
            task_id='log_requireddatetoconsiderforpast_3',
            python_callable=lambda dag_run: {
                'datetoconsiderforpast': (datetime.strptime(dag_run.conf['bookingstartdate'], "%d/%m/%Y").replace(month=1, day=1)).strftime("%d/%m/%Y"),
                'yearstoconsider': int(((datetime.strptime(dag_run.conf['bookingenddate'], "%d/%m/%Y") +
                    relativedelta(months=12)).replace(month=1, day=1)).strftime("%Y")) - int((datetime.strptime(dag_run.conf['bookingstartdate'],
                    "%d/%m/%Y")).strftime("%Y"))
            }
        )

        get_default_time_off_type_policy_schedule_for_user_6 = rail.RepliconServiceOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user_6',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ dag_run.conf.timeofftypeuri }}"
                }
            }
        )

        if_effectivedate_day_present_8 = rail.IfOperator(
            task_id='if_effectivedate_day_present_8',
            test=lambda: rail.result('get_default_time_off_type_policy_schedule_for_user_6') and rail.result(
                'get_default_time_off_type_policy_schedule_for_user_6')[0]['effectiveDate'] and rail.result(
                'get_default_time_off_type_policy_schedule_for_user_6')[0]['effectiveDate']['day'],
            yes_task="declare_list_9",
            no_task="catch_and_handle_error",
        )

        declare_list_9 = rail.SetVariableOperator(
            task_id='declare_list_9',
            append=False,
            name='timeoffpolicy',
            value=[]
        )

        get_user_time_off_type_policy_summary_10 = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary_10',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        foreach_d_11 = rail.ForEachOperator(
            task_id='foreach_d_11',
            items=lambda: rail.result('get_user_time_off_type_policy_summary_10')[
                'policiesByTimeOffType'],
            start_task='if_timeofftype_name_equals_to_dataworkato_servicereceive_requestrequesttimeofftype_12',
            end_task='foreach_d_11_end'
        )

        if_timeofftype_name_equals_to_dataworkato_servicereceive_requestrequesttimeofftype_12 = rail.IfOperator(
            task_id='if_timeofftype_name_equals_to_dataworkato_servicereceive_requestrequesttimeofftype_12',
            test='''{{ result('foreach_d_11').timeOffType.name == dag_run.conf.timeofftype }}''',
            yes_task="foreach_foreach_d_11_13",
            no_task="foreach_d_11_end",
        )

        foreach_foreach_d_11_13 = rail.ForEachOperator(
            task_id='foreach_foreach_d_11_13',
            items=lambda: rail.result('foreach_d_11')['policySetSchedule'],
            start_task='log_effective_date_14',
            end_task='foreach_foreach_d_11_13_end'
        )

        def get_date_string(dateobj):
            return str(dateobj['day']) + "/" + str(dateobj['month']) + "/" + str(dateobj['year'])

        log_effective_date_14 = rail.PythonOperator(
            task_id='log_effective_date_14',
            python_callable=lambda: get_date_string(rail.result('foreach_foreach_d_11_13')['effectiveDate'])
        )

        if_to_date_less_than_dataloggerlog_requireddatetoconsiderforpast_3messageto_date_15 = rail.IfOperator(
            task_id='if_to_date_less_than_dataloggerlog_requireddatetoconsiderforpast_3messageto_date_15',
            test=lambda: datetime.strptime(rail.result('log_effective_date_14'), "%d/%m/%Y") < datetime.strptime(
                rail.result('log_requireddatetoconsiderforpast_3')['datetoconsiderforpast'], "%d/%m/%Y"),
            yes_task="insert_to_list_16",
            no_task="foreach_foreach_d_11_13_end",
        )

        insert_to_list_16 = rail.SetVariableOperator(
            task_id='insert_to_list_16',
            append=True,
            name='{{ result("declare_list_9").name }}',
            value=lambda: {
                "description": rail.result('foreach_foreach_d_11_13')['description'],
                "effectiveDate": {
                    "day": rail.result('foreach_foreach_d_11_13')['effectiveDate']['day'],
                    "month": rail.result('foreach_foreach_d_11_13')['effectiveDate']['month'],
                    "year": rail.result('foreach_foreach_d_11_13')['effectiveDate']['year']
                },
                "policySet": rail.result('foreach_foreach_d_11_13')['policySet']
            }
        )

        foreach_foreach_d_11_13_end = rail.EmptyOperator(
            task_id='foreach_foreach_d_11_13_end',
        )

        foreach_d_11_end = rail.EmptyOperator(
            task_id='foreach_d_11_end',
        )

        def get_accrual_balance_setup():
            required_script = rail.find_first_by_attr_and_get_attr(rail.result(
                'get_default_time_off_type_policy_schedule_for_user_6')[0]['policySet']['timeOffBalanceEventScripts'], 'script.name',
                'Yearly Accrual', 'additionalParameters')
            return json.dumps(required_script).replace("[[", "[").replace("]]", "]")

        log_gettheaccrualbalancesetup_17 = rail.PythonOperator(
            task_id='log_gettheaccrualbalancesetup_17',
            python_callable=get_accrual_balance_setup
        )

        def get_startingbalance_script():
            required_script = rail.find_first_by_attr_and_get_attr(rail.result(
                'get_default_time_off_type_policy_schedule_for_user_6')[0]['policySet']['timeOffBalanceEventScripts'], 'script.name', "Starting Balance Set To")
            return json.dumps(required_script).replace('[{"additionalParameters"', '{"additionalParameters"').replace(
                "}}]", "}}").replace('}}, "script"', '}}], "script"')

        log_getthestartingbalancescript_18 = rail.PythonOperator(
            task_id='log_getthestartingbalancescript_18',
            python_callable=get_startingbalance_script
        )

        parse_json_19 = rail.PythonOperator(
            task_id='parse_json_19',
            python_callable=lambda: json.loads(
                rail.result('log_gettheaccrualbalancesetup_17'))
        )

        log_gettheaccrualbalance_20 = rail.PythonOperator(
            task_id='log_gettheaccrualbalance_20',
            python_callable=lambda: float(rail.find_first_by_attr_and_get_attr(rail.result(
                'parse_json_19'), 'keyUri', 'urn:replicon:script-key:parameter:accrual-annual-amount', 'value.number', ''))
        )

        log_existing_accrual_21 = rail.PythonOperator(
            task_id='log_existing_accrual_21',
            python_callable=lambda: '{"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": ' +
            str(rail.result('log_gettheaccrualbalance_20')) + '}}'
        )

        create_list_22 = rail.PythonOperator(
            task_id='create_list_22',
            python_callable=lambda: {
                'iterable': [{
                    'index': item,
                    'seqno': item+1
                } for item in range(rail.result('log_requireddatetoconsiderforpast_3')['yearstoconsider'])],
                'size': rail.result('log_requireddatetoconsiderforpast_3')['yearstoconsider']
            }
        )

        foreach_create_list_22_23 = rail.ForEachOperator(
            task_id='foreach_create_list_22_23',
            items=lambda: rail.result('create_list_22')['iterable'],
            start_task='if_foreach_create_list_22_23_indexforeach_meta_equals_to_0_24',
            end_task='foreach_create_list_22_23_end'
        )

        if_foreach_create_list_22_23_indexforeach_meta_equals_to_0_24 = rail.IfOperator(
            task_id='if_foreach_create_list_22_23_indexforeach_meta_equals_to_0_24',
            test=lambda: rail.result('foreach_create_list_22_23')['index'] == 0,
            yes_task="log_effective_date_25",
            no_task="if_foreach_create_list_22_23_sizeforeach_meta_equals_to_1_35",
        )

        log_effective_date_25 = rail.PythonOperator(
            task_id='log_effective_date_25',
            python_callable=lambda dag_run: (datetime.strptime(
                dag_run.conf['bookingstartdate'], "%d/%m/%Y") + relativedelta(day=1, month=1)).strftime("%d/%m/%Y")
        )

        log_required_numberofmonthsforprorationcalculation_26 = rail.PythonOperator(
            task_id='log_required_numberofmonthsforprorationcalculation_26',
            python_callable=lambda dag_run: (datetime.strptime(dag_run.conf['bookingstartdate'], "%d/%m/%Y") - (
                datetime.strptime(dag_run.conf['bookingstartdate'], "%d/%m/%Y")).replace(day=1, month=1)).days
        )

        if_yearof_bookingstartdate_equal_bookingenddate = rail.IfOperator(
            task_id='if_yearof_bookingstartdate_equal_bookingenddate',
            test=lambda dag_run: datetime.strptime(dag_run.conf['bookingenddate'], "%d/%m/%Y").strftime(
                "%Y") == datetime.strptime(dag_run.conf['bookingstartdate'], "%d/%m/%Y").strftime("%Y"),
            yes_task="log_numberofdaysincurrentyear_28",
            no_task="log_final_required_numberofmonthsforprorationcalculation_29",
        )

        log_numberofdaysincurrentyear_28 = rail.PythonOperator(
            task_id='log_numberofdaysincurrentyear_28',
            python_callable=lambda dag_run:  ((datetime.strptime(dag_run.conf['bookingenddate'], "%d/%m/%Y") + relativedelta(
                months=12)).replace(day=1, month=1) - datetime.strptime(dag_run.conf['bookingenddate'], "%d/%m/%Y")).days
        )

        log_final_required_numberofmonthsforprorationcalculation_29 = rail.PythonOperator(
            task_id='log_final_required_numberofmonthsforprorationcalculation_29',
            python_callable=lambda:  rail.result('log_required_numberofmonthsforprorationcalculation_26') + (
                rail.result('log_numberofdaysincurrentyear_28') if rail.result('log_numberofdaysincurrentyear_28') else 0)
        )

        log_required_accrual_30 = rail.PythonOperator(
            task_id='log_required_accrual_30',
            python_callable=lambda dag_run: round(float(((int((((datetime.strptime(dag_run.conf['bookingstartdate'], "%d/%m/%Y") +
                relativedelta(months=12)).replace(day=1, month=1)) - timedelta(days=1)).strftime("%j")) - int(rail.result(
                'log_final_required_numberofmonthsforprorationcalculation_29'))) / float((((datetime.strptime(dag_run.conf['bookingstartdate'], "%d/%m/%Y") +
                relativedelta(months=12)).replace(day=1, month=1)) - timedelta(days=1)).strftime("%j"))) * float(rail.result(
                'log_gettheaccrualbalance_20'))),2)
        )

        log_required_accrual_json_31 = rail.PythonOperator(
            task_id='log_required_accrual_json_31',
            python_callable=lambda: '{"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": ' +
            str(round(rail.result('log_required_accrual_30'),2)) + '}}'
        )

        def get_required_policy_set(toreplacewith):
            policyset = rail.result('get_default_time_off_type_policy_schedule_for_user_6')[
                0]['policySet']
            return json.dumps(policyset).replace(rail.result('log_existing_accrual_21'), toreplacewith).replace(
                rail.result('log_getthestartingbalancescript_18'), "").replace("[, {", "[{").replace("},,{", "},{").replace("}},]", "}}]")

        log_policy_set_32 = rail.PythonOperator(
            task_id='log_policy_set_32',
            python_callable=lambda: get_required_policy_set(
                rail.result('log_required_accrual_json_31'))
        )

        insert_to_list_34 = rail.SetVariableOperator(
            task_id='insert_to_list_34',
            append=True,
            name='{{ result("declare_list_9").name }}',
            value=lambda: {
                "description": "Effective on " + rail.result('log_effective_date_25'),
                "effectiveDate": custom_methods.get_date_object(rail.result('log_effective_date_25')),
                "policySet": json.loads(rail.result('log_policy_set_32'))
            }
        )

        if_foreach_create_list_22_23_sizeforeach_meta_equals_to_1_35 = rail.IfOperator(
            task_id='if_foreach_create_list_22_23_sizeforeach_meta_equals_to_1_35',
            test=lambda: rail.result('create_list_22')['size'] == 1,
            yes_task="log_effective_date_36",
            no_task="if_foreach_create_list_22_23_indexforeach_meta_greater_than_0_42",
        )

        log_effective_date_36 = rail.PythonOperator(
            task_id='log_effective_date_36',
            python_callable=lambda dag_run:  (datetime.strptime(
                dag_run.conf['bookingstartdate'], "%d/%m/%Y").replace(day=1, month=1) + relativedelta(months=12)).strftime("%d/%m/%Y")
        )

        log_required_accrual_json_38 = rail.PythonOperator(
            task_id='log_required_accrual_json_38',
            python_callable=lambda: '{"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": ' + str(float(
                rail.result('log_gettheaccrualbalance_20'))) + '}}'
        )

        log_policy_set_39 = rail.PythonOperator(
            task_id='log_policy_set_39',
            python_callable=lambda: get_required_policy_set(
                rail.result('log_required_accrual_json_38'))
        )

        insert_to_list_41 = rail.SetVariableOperator(
            task_id='insert_to_list_41',
            append=True,
            name='{{ result("declare_list_9").name }}',
            value=lambda: {
                "description": "Effective on " + rail.result('log_effective_date_36'),
                "effectiveDate": custom_methods.get_date_object(rail.result('log_effective_date_36')),
                "policySet": json.loads(rail.result('log_policy_set_39'))
            }
        )

        if_foreach_create_list_22_23_indexforeach_meta_greater_than_0_42 = rail.IfOperator(
            task_id='if_foreach_create_list_22_23_indexforeach_meta_greater_than_0_42',
            test=lambda: rail.result('foreach_create_list_22_23')['index'] > 0 and ( int(rail.result(
                'foreach_create_list_22_23')['seqno']) < int(rail.result('create_list_22')['size'])),
            yes_task="log_effective_date_43",
            no_task="if_seq_no_to_i_equals_to_dataforeachforeach_create_list_22_23sizeforeach_metato_i_50",
        )

        log_effective_date_43 = rail.PythonOperator(
            task_id='log_effective_date_43',
            python_callable=lambda dag_run:  (datetime.strptime(dag_run.conf['bookingstartdate'], "%d/%m/%Y").replace(
                day=1, month=1) + relativedelta(months=(12 * int(rail.result('foreach_create_list_22_23'))))).strftime("%d/%m/%Y")
        )

        log_required_accrual_json_46 = rail.PythonOperator(
            task_id='log_required_accrual_json_46',
            python_callable=lambda:  '{"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": ' + '365}}'
        )

        log_policy_set_47 = rail.PythonOperator(
            task_id='log_policy_set_47',
            python_callable=lambda: get_required_policy_set(
                rail.result('log_required_accrual_json_46'))
        )

        insert_to_list_49 = rail.SetVariableOperator(
            task_id='insert_to_list_49',
            append=True,
            name='{{ result("declare_list_9").name }}',
            value=lambda: {
                "description": "Effective on " + rail.result('log_effective_date_43'),
                "effectiveDate": custom_methods.get_date_object(rail.result('log_effective_date_43')),
                "policySet": json.loads(rail.result('log_policy_set_47'))
            }
        )

        if_seq_no_to_i_equals_to_dataforeachforeach_create_list_22_23sizeforeach_metato_i_50 = rail.IfOperator(
            task_id='if_seq_no_to_i_equals_to_dataforeachforeach_create_list_22_23sizeforeach_metato_i_50',
            test=lambda: (int(rail.result('foreach_create_list_22_23')['seqno']) == int(rail.result(
                'create_list_22')['size'])) and int(rail.result('create_list_22')['size']) > 1,
            yes_task="log_effective_date_51",
            no_task="foreach_create_list_22_23_end",
        )

        log_effective_date_51 = rail.PythonOperator(
            task_id='log_effective_date_51',
            python_callable=lambda dag_run:  (datetime.strptime(
                dag_run.conf['bookingenddate'], "%d/%m/%Y").replace(day=1, month=1)).strftime("%d/%m/%Y")
        )

        log_required_numberofdaysforprorationcalculation_52 = rail.PythonOperator(
            task_id='log_required_numberofdaysforprorationcalculation_52',
            python_callable=lambda: lambda dag_run:  ((datetime.strptime(dag_run.conf['bookingenddate'], "%d/%m/%Y") + relativedelta(
                months=12)).replace(day=1, month=1) - datetime.strptime(dag_run.conf['bookingenddate'], "%d/%m/%Y")).days
        )

        log_required_accrual_53 = rail.PythonOperator(
            task_id='log_required_accrual_53',
            python_callable=lambda: lambda dag_run: (int((((datetime.strptime(dag_run.conf['bookingstartdate'], "%d/%m/%Y") +
                relativedelta(months=12)).replace(day=1, month=1)) - timedelta(days=1)).strftime("%j")) - int(rail.result(
                'log_required_numberofdaysforprorationcalculation_52'))) / (
                round(float((((datetime.strptime(dag_run.conf['bookingstartdate'], "%d/%m/%Y") +
                relativedelta(months=12)).replace(day=1, month=1)) - timedelta(days=1)).strftime("%j")) * float(rail.result(
                'log_gettheaccrualbalance_20'))))
        )

        log_required_accrual_json_54 = rail.PythonOperator(
            task_id='log_required_accrual_json_54',
            python_callable=lambda:  '{"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": ' +
                str(rail.result('log_required_accrual_53')) + '}}'
        )

        log_policy_set_55 = rail.PythonOperator(
            task_id='log_policy_set_55',
            python_callable=lambda: get_required_policy_set(
                rail.result('log_required_accrual_json_54'))
        )

        insert_to_list_57 = rail.SetVariableOperator(
            task_id='insert_to_list_57',
            append=True,
            name='{{ result("declare_list_9").name }}',
            value=lambda: {
                "description": "Effective on " + rail.result('log_effective_date_51'),
                "effectiveDate": custom_methods.get_date_object(rail.result('log_effective_date_51')),
                "policySet": json.loads(rail.result('log_policy_set_55'))
            }
        )

        log_effective_date_58 = rail.PythonOperator(
            task_id='log_effective_date_58',
            python_callable=lambda dag_run:  (datetime.strptime(
                dag_run.conf['bookingenddate'], "%d/%m/%Y").replace(day=1, month=1) + relativedelta(months=12)).strftime("%d/%m/%Y")
        )

        log_required_accrual_json_59 = rail.PythonOperator(
            task_id='log_required_accrual_json_59',
            python_callable=lambda:  '{"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": ' +
            str(rail.result('log_gettheaccrualbalance_20')) + '}}'
        )

        log_policy_set_60 = rail.PythonOperator(
            task_id='log_policy_set_60',
            python_callable=lambda: get_required_policy_set(
                rail.result('log_required_accrual_json_59'))
        )

        insert_to_list_62 = rail.SetVariableOperator(
            task_id='insert_to_list_62',
            append=True,
            name='{{ result("declare_list_9").name }}',
            value=lambda: {
                "description": "Effective on " + rail.result('log_effective_date_58'),
                "effectiveDate": custom_methods.get_date_object(rail.result('log_effective_date_58')),
                "policySet": json.loads(rail.result('log_policy_set_60'))
            }
        )

        foreach_create_list_22_23_end = rail.EmptyOperator(
            task_id='foreach_create_list_22_23_end',
        )

        def get_final_policy_toassign():
            timeoffpolicy = rail.get_dag_run_var('timeoffpolicy')
            return json.loads(json.dumps(timeoffpolicy).replace('\"script\"', '\"scriptTarget\"').replace("[,{", "[{").replace("},,{", "},{"))

        log_policytoassign_63 = rail.PythonOperator(
            task_id='log_policytoassign_63',
            python_callable=get_final_policy_toassign
        )

        put_user_time_off_account_policy_set_schedule_64 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_64',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeofftypeuri']
                },
                "policySetScheduleEntries": rail.result('log_policytoassign_63')
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

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_handle_error
        can_run_batch_task >> rail.Label(
            'No') >> log_requireddatetoconsiderforpast_3
        log_requireddatetoconsiderforpast_3 >> get_default_time_off_type_policy_schedule_for_user_6 >> if_effectivedate_day_present_8
        if_effectivedate_day_present_8 >> rail.Label(
            'Yes') >> declare_list_9 >> get_user_time_off_type_policy_summary_10
        get_user_time_off_type_policy_summary_10 >> foreach_d_11 >> if_timeofftype_name_equals_to_dataworkato_servicereceive_requestrequesttimeofftype_12
        if_timeofftype_name_equals_to_dataworkato_servicereceive_requestrequesttimeofftype_12 >> rail.Label(
            'Yes') >> foreach_foreach_d_11_13 >> log_effective_date_14 >> if_to_date_less_than_dataloggerlog_requireddatetoconsiderforpast_3messageto_date_15
        if_to_date_less_than_dataloggerlog_requireddatetoconsiderforpast_3messageto_date_15 >> rail.Label(
            'Yes') >> insert_to_list_16 >> foreach_foreach_d_11_13_end
        if_to_date_less_than_dataloggerlog_requireddatetoconsiderforpast_3messageto_date_15 >> rail.Label(
            'No') >> foreach_foreach_d_11_13_end
        foreach_foreach_d_11_13 >> foreach_foreach_d_11_13_end >> foreach_d_11_end
        if_timeofftype_name_equals_to_dataworkato_servicereceive_requestrequesttimeofftype_12 >> rail.Label(
            'No') >> foreach_d_11_end
        foreach_d_11 >> foreach_d_11_end >> log_gettheaccrualbalancesetup_17 >> log_getthestartingbalancescript_18 >> parse_json_19
        parse_json_19 >> log_gettheaccrualbalance_20 >> log_existing_accrual_21 >> create_list_22
        create_list_22 >> foreach_create_list_22_23 >> if_foreach_create_list_22_23_indexforeach_meta_equals_to_0_24
        if_foreach_create_list_22_23_indexforeach_meta_equals_to_0_24 >> rail.Label(
            'Yes') >> log_effective_date_25 >> log_required_numberofmonthsforprorationcalculation_26 >> if_yearof_bookingstartdate_equal_bookingenddate
        if_yearof_bookingstartdate_equal_bookingenddate >> rail.Label(
            'Yes') >> log_numberofdaysincurrentyear_28 >> log_final_required_numberofmonthsforprorationcalculation_29
        if_yearof_bookingstartdate_equal_bookingenddate >> rail.Label(
            'No') >> log_final_required_numberofmonthsforprorationcalculation_29 >> log_required_accrual_30 >> log_required_accrual_json_31
        log_required_accrual_json_31 >> log_policy_set_32 >> insert_to_list_34 >> if_foreach_create_list_22_23_sizeforeach_meta_equals_to_1_35
        if_foreach_create_list_22_23_indexforeach_meta_equals_to_0_24 >> rail.Label(
            'No') >> if_foreach_create_list_22_23_sizeforeach_meta_equals_to_1_35
        if_foreach_create_list_22_23_sizeforeach_meta_equals_to_1_35 >> rail.Label(
            'Yes') >> log_effective_date_36 >> log_required_accrual_json_38 >> log_policy_set_39 >> insert_to_list_41
        insert_to_list_41 >> if_foreach_create_list_22_23_indexforeach_meta_greater_than_0_42
        if_foreach_create_list_22_23_sizeforeach_meta_equals_to_1_35 >> rail.Label(
            'No') >> if_foreach_create_list_22_23_indexforeach_meta_greater_than_0_42
        if_foreach_create_list_22_23_indexforeach_meta_greater_than_0_42 >> rail.Label(
            'Yes') >> log_effective_date_43 >> log_required_accrual_json_46 >> log_policy_set_47 >> insert_to_list_49
        insert_to_list_49 >> if_seq_no_to_i_equals_to_dataforeachforeach_create_list_22_23sizeforeach_metato_i_50
        if_foreach_create_list_22_23_indexforeach_meta_greater_than_0_42 >> rail.Label(
            'No') >> if_seq_no_to_i_equals_to_dataforeachforeach_create_list_22_23sizeforeach_metato_i_50
        if_seq_no_to_i_equals_to_dataforeachforeach_create_list_22_23sizeforeach_metato_i_50 >> rail.Label(
            'Yes') >> log_effective_date_51 >> log_required_numberofdaysforprorationcalculation_52 >> log_required_accrual_53
        log_required_accrual_53 >> log_required_accrual_json_54 >> log_policy_set_55 >> insert_to_list_57 >> log_effective_date_58
        log_effective_date_58 >> log_required_accrual_json_59 >> log_policy_set_60 >> insert_to_list_62 >> foreach_create_list_22_23_end
        if_seq_no_to_i_equals_to_dataforeachforeach_create_list_22_23sizeforeach_metato_i_50 >> rail.Label(
            'No') >> foreach_create_list_22_23_end
        foreach_create_list_22_23 >> foreach_create_list_22_23_end >> log_policytoassign_63 >> put_user_time_off_account_policy_set_schedule_64
        put_user_time_off_account_policy_set_schedule_64 >> catch_and_handle_error
        if_effectivedate_day_present_8 >> rail.Label(
            'No') >> catch_and_handle_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
