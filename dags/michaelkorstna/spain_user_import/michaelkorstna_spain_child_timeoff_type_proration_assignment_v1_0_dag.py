
from datetime import timedelta, datetime
import json
from uuid import uuid4
from airflow.models import Variable
import rail
from michaelkorstna.spain_user_import.utils import custom_methods
null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'michaelkorstna_spain_user_import_timeoff_type_proration_assignment_child_{config.instance}',
        description=f'MichaelKorsTnA Spain_Child Timeoff type Proration Assignment v1.0 {config.instance}',
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
                defaultschedule['policySet']['timeOffBalanceEventScripts'], 'script.name',
                'Yearly/Monthly Accrual with Expiry & Rounding', 'additionalParameters', '')
            return (json.dumps(requiredscript)).replace("[[", "[").replace("]]", "]")

        log_gettheaccrualbalancesetup_7 = rail.PythonOperator(
            task_id='log_gettheaccrualbalancesetup_7',
            python_callable=get_accrualbalance_setup
        )

        log_gettheaccrualbalance_9 = rail.PythonOperator(
            task_id='log_gettheaccrualbalance_9',
            python_callable=lambda: float(rail.find_first_by_attr_and_get_attr(json.loads(rail.result(
                'log_gettheaccrualbalancesetup_7')), 'keyUri', 'urn:replicon:script-key:parameter:yearly-entitlement', 'value.number'))
        )

        log_existing_accrual_10 = rail.PythonOperator(
            task_id='log_existing_accrual_10',
            python_callable=lambda:  '{"keyUri": "urn:replicon:script-key:parameter:yearly-entitlement", "value": {"number": ' + str(
                rail.result('log_gettheaccrualbalance_9')) + '}}'
        )

        log_required_accrual_17 = rail.PythonOperator(
            task_id='log_required_accrual_17',
            python_callable=lambda dag_run: round(float(dag_run.conf['accrualdays']))
        )

        log_required_accrual_json_18 = rail.PythonOperator(
            task_id='log_required_accrual_json_18',
            python_callable=lambda: '{"keyUri": "urn:replicon:script-key:parameter:yearly-entitlement", "value": {"number": ' + str(
                rail.result('log_required_accrual_17')) + '}}'
        )

        if_request_type_equals_to_add_19 = rail.IfOperator(
            task_id='if_request_type_equals_to_add_19',
            test='''{{ dag_run.conf.type == 'Add' }}''',
            yes_task="log_timeoff_policy_22",
            no_task="if_request_type_equals_to_update_24",
        )

        def get_timeoff_policy():
            return (json.dumps(rail.result('get_default_time_off_type_policy_schedule_for_user_4'))).replace('null', '\"effective\"').replace(
                '\"script\"', '\"scriptTarget\"').replace(rail.result('log_existing_accrual_10'), rail.result('log_required_accrual_json_18'))

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
            yes_task="declare_list_28",
            no_task="catch_and_log_error",
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

        log_effective_date_33 = rail.PythonOperator(
            task_id='log_effective_date_33',
            python_callable=lambda: custom_methods.get_date_string(rail.result('foreach_foreach_d_30_32')['effectiveDate'])
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

        foreach_foreach_d_30_41 = rail.ForEachOperator(
            task_id='foreach_foreach_d_30_41',
            items=lambda: rail.result('foreach_d_30')['policySetSchedule'],
            start_task='log_effective_date_42',
            end_task='foreach_foreach_d_30_41_end'
        )

        log_effective_date_42 = rail.PythonOperator(
            task_id='log_effective_date_42',
            python_callable=lambda: custom_methods.get_date_string(rail.result('foreach_foreach_d_30_41')['effectiveDate'])
        )

        if_to_date_less_than_dataloggerlog_effective_dateto_consider_40messageto_date_43 = rail.IfOperator(
            task_id='if_to_date_less_than_dataloggerlog_effective_dateto_consider_40messageto_date_43',
            test=lambda: datetime.strptime(rail.result('log_effective_date_42'), "%d/%m/%Y") < datetime.strptime(
                rail.result('log_effective_dateto_consider_37'), "%Y-%m-%d"),
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

        def get_effectivepolicyto_consider():
            policies = list(filter(lambda x: x['effectivedate'] == rail.result(
                'log_effective_dateto_consider_37'), rail.get_dag_run_var('effectivedateofpolicies')))
            policysets = [policy['policyset'] for policy in policies]
            unique_policysets = []
            for policyset in policysets:
                if policyset not in unique_policysets:
                    unique_policysets.append(policyset)
            return unique_policysets

        log_effective_policyto_consider_45 = rail.PythonOperator(
            task_id='log_effective_policyto_consider_45',
            python_callable=get_effectivepolicyto_consider
        )

        def get_required_policy_set():
            policysets = rail.result('log_effective_policyto_consider_45')
            return json.dumps(policysets).replace('[{"timeOffBalanceEventScripts"', '{"timeOffBalanceEventScripts"').replace(
                "[]}]", "[]}").replace("[,{", "[{").replace('}},],"timeOffValidationScripts', '}}],"timeOffValidationScripts').replace(
                '"timeOffValidationScripts":}', '"timeOffValidationScripts":[]}').replace("}}]}]", "}}]}").replace(
                '[{"additionalParameters":,', '[{"additionalParameters":[],')

        log_policy_set_62 = rail.PythonOperator(
            task_id='log_policy_set_62',
            python_callable=get_required_policy_set
        )

        def get_required_value_tobe_inserted(effectivedate,policyset):
            effectivedateobject = datetime.strptime(effectivedate, "%d/%m/%Y")
            return {
                "description": "Effective on " + effectivedate,
                "effectiveDate": {
                    "day": effectivedateobject.day,
                    "month": effectivedateobject.month,
                    "year": effectivedateobject.year
                },
                "policySet": json.loads(policyset)
            }

        insert_to_list_64 = rail.SetVariableOperator(
            task_id='insert_to_list_64',
            append=True,
            name='{{ result("declare_list_28").name }}',
            value=lambda: get_required_value_tobe_inserted((datetime.strptime(rail.result(
                'log_effective_dateto_consider_37'),"%Y-%m-%d")).strftime("%d/%m/%Y"),rail.result('log_policy_set_62'))
        )

        get_default_time_off_policy_set_schedule_for_time_off_type_65 = rail.RepliconServiceOperator(
            task_id='get_default_time_off_policy_set_schedule_for_time_off_type_65',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
            }
        )

        foreach_response_67 = rail.ForEachOperator(
            task_id='foreach_response_67',
            items=lambda: rail.result(
                'get_default_time_off_policy_set_schedule_for_time_off_type_65'),
            start_task='log_effective_datetobeconsidered_76',
            end_task='foreach_response_67_end'
        )

        log_effective_datetobeconsidered_76 = rail.PythonOperator(
            task_id='log_effective_datetobeconsidered_76',
            python_callable=lambda dag_run: dag_run.conf['startdate']
        )

        log_policy_set_81 = rail.PythonOperator(
            task_id='log_policy_set_81',
            python_callable=lambda: (json.dumps(rail.result('foreach_response_67')['policySet'])).replace(rail.result(
                'log_existing_accrual_10'), rail.result('log_required_accrual_json_18')).replace("[,{", "[{").replace("},,{", "},{").replace("}},]", "}}]")
        )

        insert_to_list_84 = rail.SetVariableOperator(
            task_id='insert_to_list_84',
            append=True,
            name='{{ result("declare_list_28").name }}',
            value=lambda: get_required_value_tobe_inserted(rail.result('log_effective_datetobeconsidered_76'),rail.result('log_policy_set_81'))
        )

        foreach_response_67_end = rail.EmptyOperator(
            task_id='foreach_response_67_end',
        )

        log_policy_set_85 = rail.PythonOperator(
            task_id='log_policy_set_85',
            python_callable=lambda: (json.dumps(rail.result('get_default_time_off_policy_set_schedule_for_time_off_type_65')[0]['policySet'])).replace(
                rail.result('log_existing_accrual_10'), rail.result('log_required_accrual_json_18')).replace("[,{", "[{").replace(
                "},,{", "},{").replace("},]", "}]")
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

        execute_timeoff_policy_recalculation = rail.RepliconServiceOperator(
            task_id = 'execute_timeoff_policy_recalculation',
            endpoint='/services/TimeOffService2.svc/ExecuteTimeOffPolicyTransactionCalculation',
            data=lambda dag_run:{
                "account": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "calculationEndDate": null,
                "scriptDataRecalculationOptionUri": "urn:replicon:time-off-script-data-recalculation-option:recalculate-full-history",
                "unitOfWorkId": str(uuid4())
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
            'Yes') >> log_gettheaccrualbalancesetup_7 >> log_gettheaccrualbalance_9 >> log_existing_accrual_10 >> log_required_accrual_17
        log_required_accrual_17 >> log_required_accrual_json_18
        log_required_accrual_json_18 >> if_request_type_equals_to_add_19
        if_request_type_equals_to_add_19 >> rail.Label(
            'Yes') >> log_timeoff_policy_22
        log_timeoff_policy_22 >> put_user_time_off_account_policy_set_schedule_23 >> if_request_type_equals_to_update_24
        if_request_type_equals_to_add_19 >> rail.Label(
            'No') >> if_request_type_equals_to_update_24
        if_request_type_equals_to_update_24 >> rail.Label(
            'Yes') >> declare_list_28 >> get_user_time_off_type_policy_summary_29
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
        log_effective_dateto_consider_37 >> foreach_foreach_d_30_41 >> log_effective_date_42
        log_effective_date_42 >> if_to_date_less_than_dataloggerlog_effective_dateto_consider_40messageto_date_43
        if_to_date_less_than_dataloggerlog_effective_dateto_consider_40messageto_date_43 >> rail.Label(
            'Yes') >> insert_to_list_44 >> foreach_foreach_d_30_41_end
        if_to_date_less_than_dataloggerlog_effective_dateto_consider_40messageto_date_43 >> rail.Label(
            'No') >> foreach_foreach_d_30_41_end
        foreach_foreach_d_30_41 >> foreach_foreach_d_30_41_end >> increment_index >> foreach_d_30_end
        if_timeofftype_name_equals_to_dataworkato_servicereceive_requestrequesttimeofftype_31 >> rail.Label(
            'No') >> increment_index >> foreach_d_30_end
        foreach_d_30 >> foreach_d_30_end >> log_effective_policyto_consider_45 >> log_policy_set_62 >> insert_to_list_64
        insert_to_list_64 >> get_default_time_off_policy_set_schedule_for_time_off_type_65
        get_default_time_off_policy_set_schedule_for_time_off_type_65 >> foreach_response_67 >> log_effective_datetobeconsidered_76 >> log_policy_set_81
        log_policy_set_81 >> insert_to_list_84 >> foreach_response_67_end
        foreach_response_67 >> foreach_response_67_end >> log_policy_set_85 >> log_policytoassign_88
        log_policytoassign_88 >> put_user_time_off_account_policy_set_schedule_89 >> execute_timeoff_policy_recalculation >> catch_and_log_error
        if_request_type_equals_to_update_24 >> rail.Label(
            'No') >> catch_and_log_error
        if_effectivedate_day_present_6 >> rail.Label(
            'No') >> catch_and_log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
