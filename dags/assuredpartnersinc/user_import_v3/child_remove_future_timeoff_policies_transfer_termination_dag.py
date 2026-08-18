from datetime import timedelta
import json
from airflow.models import Variable
import rail
from assuredpartnersinc.user_import_v3.utils import python_callable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_remove_future_timeoff_policies_transfer_termination_dag_id,
        description=f'Assured Partners User Import Remove Future Timeoff Policies Transfer Termination{config.instance}',
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
            no_task='get_enabled_time_off_types_2'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_enabled_time_off_types_2',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_enabled_time_off_types_2 = rail.RepliconServiceOperator(
            task_id='get_enabled_time_off_types_2',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'name', 'PTO Payout', 'uri')
        )

        log_effectivedatederived_4 = rail.PythonOperator(
            task_id='log_effectivedatederived_4',
            python_callable=lambda dag_run: python_callable.get_split_date(dag_run.conf['enddate'], 'int') if "terminate" in dag_run.conf['type'] else (
                python_callable.get_split_date(dag_run.conf['enddate'], 'int') if "loa" in dag_run.conf['type'] else python_callable.get_split_date(dag_run.conf['ChangeEffectiveDate'], 'int'))
        )

        get_defaultpolicyfromgloballevel_6 = rail.RepliconServiceOperator(
            task_id='get_defaultpolicyfromgloballevel_6',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
            }
        )

        get_user_time_off_type_policy_summary_7 = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary_7',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response['policiesByTimeOffType'], 'timeOffType.uri', dag_run.conf['timeoffuri'], 'policySetSchedule', '')
        )

        log_relevant_historical_policies_and_offset_to_consider = rail.PythonOperator(
            task_id='log_relevant_historical_policies_and_offset_to_consider',
            python_callable=lambda: python_callable.get_relevant_historical_policies(rail.result('get_user_time_off_type_policy_summary_7'), rail.result(
                'log_effectivedatederived_4'))
        )

        def add_historical_policies_to_policysetschedule_list(historical_policies):
            modified_policysetschedule = []
            if "urn" in json.dumps(historical_policies):
                for item in historical_policies:
                    modified_policysetschedule.append({
                        'description': item['description'],
                        'effectiveDate': item['effectiveDate'],
                        'policySet': item['policySet']
                    })

            return modified_policysetschedule

        log_modified_policysetschedule_with_historical_policies_27 = rail.PythonOperator(
            task_id='log_modified_policysetschedule_with_historical_policies_27',
            python_callable=lambda:  add_historical_policies_to_policysetschedule_list(rail.result(
                'log_relevant_historical_policies_and_offset_to_consider'))
        )

        if_request_estatus_equals_to_a_28 = rail.IfOperator(
            task_id='if_request_estatus_equals_to_a_28',
            test='''{{ dag_run.conf.estatus == 'A' }}''',
            yes_task="if_request_pto_1_blank_29",
            no_task="if_request_estatus_equals_to_t_33",
        )

        if_request_pto_1_blank_29 = rail.IfOperator(
            task_id='if_request_pto_1_blank_29',
            test=lambda dag_run: not (
                dag_run.conf['pto_1']) and dag_run.conf['illness'] == 'Sick Pay-H',
            yes_task="parse_json_parsenewpolicyline_previousbalancesetto0_30",
            no_task="parse_json_parsenewpolicyline_32",
        )

        parse_json_parsenewpolicyline_previousbalancesetto0_30 = rail.PythonOperator(
            task_id='parse_json_parsenewpolicyline_previousbalancesetto0_30',
            python_callable=lambda dag_run: json.dumps({
                "timeOffBalanceEventScripts": [{
                    "scriptTarget": {
                        "uri": dag_run.conf['starting_balance_set_to_uri']
                    },
                    "additionalParameters": [{
                        "keyUri": "urn:replicon:script-key:parameter:amount",
                        "value": {
                            "number": dag_run.conf['previousbalance']
                        }
                    },
                        {
                        "keyUri": "urn:replicon:script-key:parameter:precedence",
                            "value":
                                {
                                    "number": "20"
                                }
                    }]
                }],
                "timeOffValidationScripts": []
            })
        )

        parse_json_parsenewpolicyline_32 = rail.PythonOperator(
            task_id='parse_json_parsenewpolicyline_32',
            python_callable=lambda dag_run: json.dumps({
                "timeOffBalanceEventScripts": [{
                    "scriptTarget": {
                        "uri": dag_run.conf['starting_balance_set_to_uri']
                    },
                    "additionalParameters": [{
                        "keyUri": "urn:replicon:script-key:parameter:amount",
                        "value": {
                            "number": dag_run.conf['previousbalance']
                        }
                    },
                        {
                        "keyUri": "urn:replicon:script-key:parameter:precedence",
                            "value":
                                {
                                    "number": "20"
                                }
                    }]
                }],
                "timeOffValidationScripts": []
            })
        )

        if_request_estatus_equals_to_t_33 = rail.IfOperator(
            task_id='if_request_estatus_equals_to_t_33',
            test='''{{ dag_run.conf.estatus == 'T' }}''',
            yes_task="parse_json_parsenewpolicyline_previousbalancesetto0_34",
            no_task="invoke_custom_ruby_code_35",
        )

        parse_json_parsenewpolicyline_previousbalancesetto0_34 = rail.PythonOperator(
            task_id='parse_json_parsenewpolicyline_previousbalancesetto0_34',
            python_callable=lambda dag_run: json.dumps({
                "timeOffBalanceEventScripts": [{
                    "scriptTarget": {
                        "uri": dag_run.conf['starting_balance_set_to_uri']
                    },
                    "additionalParameters": [{
                        "keyUri": "urn:replicon:script-key:parameter:amount",
                        "value": {
                            "number": "0"
                        }
                    },
                        {
                        "keyUri": "urn:replicon:script-key:parameter:precedence",
                            "value":
                                {
                                    "number": "20"
                                }
                    }]
                }],
                "timeOffValidationScripts": []
            })
        )

        invoke_custom_ruby_code_35 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_35',
            python_callable=lambda dag_run: python_callable.get_split_date(python_callable.get_split_date(dag_run.conf['enddate'], 'no_split') + timedelta(days=1), 'int') if "terminate" in dag_run.conf['type'] else (
                python_callable.get_split_date(dag_run.conf['enddate'], 'int') if "loa" in dag_run.conf['type'] else python_callable.get_split_date(dag_run.conf['ChangeEffectiveDate'], 'int'))
        )

        def add_policy_line_to_policysetschedule_list(modified_policysetschedule_list, eff_date):
            modified_policysetschedule_list.append({
                'description': 'Effective on ' + str(eff_date['month']) + "/" + str(eff_date['day']) + "/" + str(eff_date['year']),
                'effectiveDate': eff_date,
                'policySet': json.loads(rail.result('parse_json_parsenewpolicyline_previousbalancesetto0_30') or rail.result('parse_json_parsenewpolicyline_32') or rail.result('parse_json_parsenewpolicyline_previousbalancesetto0_34'))
            })

            return modified_policysetschedule_list

        insert_policy_line_to_policysetschedule_list_and_get_final_policysetschedule_list_38 = rail.PythonOperator(
            task_id='insert_policy_line_to_policysetschedule_list_and_get_final_policysetschedule_list_38',
            python_callable=lambda: add_policy_line_to_policysetschedule_list(rail.result(
                'log_modified_policysetschedule_with_historical_policies_27'), rail.result('invoke_custom_ruby_code_35'))
        )

        if_to_s_contains_urn_39 = rail.IfOperator(
            task_id='if_to_s_contains_urn_39',
            test=lambda: 'urn' in json.dumps(rail.result(
                'insert_policy_line_to_policysetschedule_list_and_get_final_policysetschedule_list_38')),
            yes_task="assign_time_offpolicy_40",
            no_task="if_request_type_equals_to_terminate_41",
        )

        assign_time_offpolicy_40 = rail.RepliconServiceOperator(
            task_id='assign_time_offpolicy_40',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('insert_policy_line_to_policysetschedule_list_and_get_final_policysetschedule_list_38')
            }
        )

        if_request_type_equals_to_terminate_41 = rail.IfOperator(
            task_id='if_request_type_equals_to_terminate_41',
            test='''{{ dag_run.conf.type == 'terminate' }}''',
            yes_task="if_request_previousptoname_present_42",
            no_task="if_request_type_equals_to_loa_44",
        )

        if_request_previousptoname_present_42 = rail.IfOperator(
            task_id='if_request_previousptoname_present_42',
            test='''{{ dag_run.conf.previousptoname | is_truthy }}''',
            yes_task="trigger_dag_run_child_transfer_pto_balance_to_pto_payout_termination_43",
            no_task="if_request_type_equals_to_loa_44",
        )

        trigger_dag_run_child_transfer_pto_balance_to_pto_payout_termination_43 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_transfer_pto_balance_to_pto_payout_termination_43',
            retries=0,
            trigger_dag_id=config.child_transfer_pto_balance_to_pto_payout_termination_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "parentjobid": "{{ dag_run.conf.parentjobid }}",
                "employeenumber": "{{ dag_run.conf.employeenumber }}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "lastname": "{{ dag_run.conf.lastname }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "timeoffuri": "{{ dag_run.conf.timeoffuri }}",
                "timeofftypename": "{{ dag_run.conf.timeofftypename }}",
                "schedulename": "{{ dag_run.conf.schedulename }}",
                "type": "terminate",
                "previousstartdate": "{{ dag_run.conf.previousstartdate }}",
                "previousbalance": "{{dag_run.conf.previousbalance}}",
                "enddate": "{{ dag_run.conf.enddate }}",
                "starting_balance_set_to_uri": "{{dag_run.conf.starting_balance_set_to_uri}}",
                "ptopayouturi": "{{result('get_enabled_time_off_types_2')}}",
                "integration_run_date": "{{ dag_run.conf.integration_run_date }}"
            }
        )

        wait_for_completion_trigger_dag_run_child_transfer_pto_balance_to_pto_payout_termination_43 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_child_transfer_pto_balance_to_pto_payout_termination_43',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_child_transfer_pto_balance_to_pto_payout_termination_43") }}'
        )

        if_request_type_equals_to_loa_44 = rail.IfOperator(
            task_id='if_request_type_equals_to_loa_44',
            test='''{{ dag_run.conf.type == 'loa' }}''',
            yes_task="if_request_previousptoname_present_45",
            no_task="catch_and_log_error",
        )

        if_request_previousptoname_present_45 = rail.IfOperator(
            task_id='if_request_previousptoname_present_45',
            test='''{{ dag_run.conf.previousptoname | is_truthy  and dag_run.conf.pto_1 | is_falsy  and dag_run.conf.illness == 'Sick Pay-H' }}''',
            yes_task="trigger_dag_run_child_transfer_pto_balance_to_pto_payout_termination_46",
            no_task="catch_and_log_error",
        )

        trigger_dag_run_child_transfer_pto_balance_to_pto_payout_termination_46 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_transfer_pto_balance_to_pto_payout_termination_46',
            retries=0,
            trigger_dag_id=config.child_transfer_pto_balance_to_pto_payout_termination_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "parentjobid": "{{ dag_run.conf.parentjobid }}",
                "employeenumber": "{{ dag_run.conf.employeenumber }}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "lastname": "{{ dag_run.conf.lastname }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "timeoffuri": "{{ dag_run.conf.timeoffuri }}",
                "timeofftypename": "{{ dag_run.conf.timeofftypename }}",
                "schedulename": "{{ dag_run.conf.schedulename }}",
                "type": "{{ dag_run.conf.type }}",
                "previousstartdate": "{{ dag_run.conf.previousstartdate }}",
                "previousbalance": "{{dag_run.conf.previousbalance}}",
                "enddate": "{{ dag_run.conf.enddate }}",
                "starting_balance_set_to_uri": "{{dag_run.conf.starting_balance_set_to_uri}}",
                "ptopayouturi": "{{result('get_enabled_time_off_types_2')}}",
                "integration_run_date": "{{ dag_run.conf.integration_run_date }}"
            }
        )

        wait_for_completion_trigger_dag_run_child_transfer_pto_balance_to_pto_payout_termination_46 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_child_transfer_pto_balance_to_pto_payout_termination_46',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_child_transfer_pto_balance_to_pto_payout_termination_46") }}'
        )

        catch_and_log_error = rail.PythonOperator(
            task_id="catch_and_log_error",
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "Error in Workflow to remove future timeoff policies transfer termination : {{get_error_message()}}")
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.result(
                "catch_and_log_error") or "Success"
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error >> final_response_from_dag
        can_run_batch_task >> rail.Label(
            'No') >> get_enabled_time_off_types_2

        get_enabled_time_off_types_2 >> log_effectivedatederived_4 >> get_defaultpolicyfromgloballevel_6 >> get_user_time_off_type_policy_summary_7 \
            >> log_relevant_historical_policies_and_offset_to_consider >> log_modified_policysetschedule_with_historical_policies_27 \
            >> if_request_estatus_equals_to_a_28

        if_request_estatus_equals_to_a_28 >> rail.Label(
            'No') >> if_request_estatus_equals_to_t_33
        if_request_estatus_equals_to_a_28 >> rail.Label(
            'Yes') >> if_request_pto_1_blank_29

        if_request_pto_1_blank_29 >> rail.Label(
            'No') >> parse_json_parsenewpolicyline_32 >> if_request_estatus_equals_to_t_33
        if_request_pto_1_blank_29 >> rail.Label(
            'Yes') >> parse_json_parsenewpolicyline_previousbalancesetto0_30 >> if_request_estatus_equals_to_t_33

        if_request_estatus_equals_to_t_33 >> rail.Label(
            'No') >> invoke_custom_ruby_code_35
        if_request_estatus_equals_to_t_33 >> rail.Label(
            'Yes') >> parse_json_parsenewpolicyline_previousbalancesetto0_34 >> invoke_custom_ruby_code_35

        invoke_custom_ruby_code_35 >> insert_policy_line_to_policysetschedule_list_and_get_final_policysetschedule_list_38 >> if_to_s_contains_urn_39

        if_to_s_contains_urn_39 >> rail.Label(
            'No') >> if_request_type_equals_to_terminate_41
        if_to_s_contains_urn_39 >> rail.Label(
            'Yes') >> assign_time_offpolicy_40 >> if_request_type_equals_to_terminate_41

        if_request_type_equals_to_terminate_41 >> rail.Label(
            'No') >> if_request_type_equals_to_loa_44
        if_request_type_equals_to_terminate_41 >> rail.Label(
            'Yes') >> if_request_previousptoname_present_42

        if_request_previousptoname_present_42 >> rail.Label(
            'No') >> if_request_type_equals_to_loa_44
        if_request_previousptoname_present_42 >> rail.Label(
            'Yes') >> trigger_dag_run_child_transfer_pto_balance_to_pto_payout_termination_43 >> wait_for_completion_trigger_dag_run_child_transfer_pto_balance_to_pto_payout_termination_43 >> if_request_type_equals_to_loa_44

        if_request_type_equals_to_loa_44 >> rail.Label(
            'No') >> catch_and_log_error
        if_request_type_equals_to_loa_44 >> rail.Label(
            'Yes') >> if_request_previousptoname_present_45

        if_request_previousptoname_present_45 >> rail.Label(
            'No') >> catch_and_log_error
        if_request_previousptoname_present_45 >> rail.Label(
            'Yes') >> trigger_dag_run_child_transfer_pto_balance_to_pto_payout_termination_46 >> wait_for_completion_trigger_dag_run_child_transfer_pto_balance_to_pto_payout_termination_46 >> catch_and_log_error

    return dag


rail.for_each_instance(create_dag)
