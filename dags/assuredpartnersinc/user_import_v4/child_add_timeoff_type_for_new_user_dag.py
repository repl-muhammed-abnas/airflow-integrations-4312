from datetime import timedelta
import json
from airflow.models import Variable
import rail
from assuredpartnersinc.user_import_v4.utils import python_callable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_workflow_to_add_timeoff_type_for_new_user_dag_id,
        description=f'Assured Partners User Import Add timeoff for new user Child {config.instance}',
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
            no_task='response_from_dag'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='response_from_dag',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        response_from_dag = rail.SetVariableOperator(
            task_id="response_from_dag",
            name='response_from_dag',
            append=False,
            value=''
        )

        log_tenure_basedon_replicon_t_s_date_3 = rail.PythonOperator(
            task_id='log_tenure_basedon_replicon_t_s_date_3',
            python_callable=lambda dag_run:  python_callable.get_user_tenure_in_years(
                dag_run.conf['tsstartdate'], dag_run.conf['ServiceDate'], dag_run) if dag_run.conf['tsstartdate'] else 0
        )

        log_tenure_basedon_pto_seniority_date = rail.PythonOperator(
            task_id='log_tenure_basedon_pto_seniority_date',
            python_callable=lambda dag_run:  python_callable.get_user_tenure_in_years(
                dag_run.conf['PTOSeniorityDate'], dag_run.conf['ServiceDate'], dag_run) if dag_run.conf['PTOSeniorityDate'] else 0
        )

        adhoc_http_action_4 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_4',
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
        )

        if_first_displaytext_present_5 = rail.IfOperator(
            task_id='if_first_displaytext_present_5',
            test=lambda: bool(rail.result("adhoc_http_action_4")),
            yes_task="get_timeoff_uri_name_list",
            no_task="catch_and_log_error",
        )

        get_timeoff_uri_name_list = rail.PythonOperator(
            task_id='get_timeoff_uri_name_list',
            python_callable=lambda dag_run: python_callable.final_timeoffs_to_be_added_list(
                dag_run, rail.result("adhoc_http_action_4"))
        )

        log_final_set_timeoff_uris_34 = rail.PythonOperator(
            task_id='log_final_set_timeoff_uris_34',
            python_callable=lambda: [item['uri']
                                     for item in rail.result("get_timeoff_uri_name_list") if item['uri']]
        )

        if_log_12_present_35 = rail.IfOperator(
            task_id='if_log_12_present_35',
            test='''{{ result('log_final_set_timeoff_uris_34') | is_truthy }}''',
            yes_task="put_time_off_type_assignments_for_user_37",
            no_task="catch_and_log_error",
        )

        put_time_off_type_assignments_for_user_37 = rail.RepliconServiceOperator(
            task_id='put_time_off_type_assignments_for_user_37',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUris": rail.result('log_final_set_timeoff_uris_34')
            }
        )

        assured_partners_pto_1_time_off_list_search_entries_38 = rail.PythonOperator(
            task_id='assured_partners_pto_1_time_off_list_search_entries_38',
            python_callable=lambda:  list(
                filter(lambda x: x["identifier"] == "timeoff", config.TO_PTO1_MAPPER))
        )

        log_pto1_timeofflist_h_39 = rail.PythonOperator(
            task_id='log_pto1_timeofflist_h_39',
            python_callable=lambda:  [item['time_off_type_name'] for item in rail.result(
                "assured_partners_pto_1_time_off_list_search_entries_38")]
        )

        create_child_triggered_list = rail.SetVariableOperator(
            task_id='create_child_triggered_list',
            name='wait_for_dag_runs',
            append=False,
            value=[]
        )

        foreach_declare_list_6_40 = rail.ForEachOperator(
            task_id='foreach_declare_list_6_40',
            items=lambda: rail.result('get_timeoff_uri_name_list'),
            start_task='if_foreach_1_uri_present_41',
            end_task='foreach_declare_list_6_40_end'
        )

        if_foreach_1_uri_present_41 = rail.IfOperator(
            task_id='if_foreach_1_uri_present_41',
            test='''{{ result('foreach_declare_list_6_40').uri | is_truthy }}''',
            yes_task="accumulate_list_items_42",
            no_task="foreach_declare_list_6_40_end",
        )

        accumulate_list_items_42 = rail.SetVariableOperator(
            task_id='accumulate_list_items_42',
            name='assigned_timeoff_types',
            append=True,
            value={
                    "timeofftype": "{{ result('foreach_declare_list_6_40').name }}"
            }
        )

        get_default_time_off_type_policy_schedule_for_user_44 = rail.RepliconServiceOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user_44',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ result('foreach_declare_list_6_40').uri }}"
                }
            }
        )

        if_log_pto1_timeofflist_h_39_not_contains_foreach_1name_46 = rail.IfOperator(
            task_id='if_log_pto1_timeofflist_h_39_not_contains_foreach_1name_46',
            test=lambda: bool(rail.result('foreach_declare_list_6_40')[
                              'name'] not in rail.result('log_pto1_timeofflist_h_39')),
            yes_task="log_timeoff_policy_47",
            no_task="log_p_t_o_type_54",
        )

        log_timeoff_policy_47 = rail.PythonOperator(
            task_id='log_timeoff_policy_47',
            python_callable=lambda: json.dumps(rail.result('get_default_time_off_type_policy_schedule_for_user_44'), ensure_ascii=False).replace('null', '"effective"').replace(
                '"script"', '"scriptTarget"') if rail.result('get_default_time_off_type_policy_schedule_for_user_44') else null
        )

        if_log_13_present_48 = rail.IfOperator(
            task_id='if_log_13_present_48',
            test='''{{ result('log_timeoff_policy_47') | is_truthy }}''',
            yes_task="if_foreach_1_name_equals_to_sickpayh_49",
            no_task="foreach_declare_list_6_40_end",
        )

        if_foreach_1_name_equals_to_sickpayh_49 = rail.IfOperator(
            task_id='if_foreach_1_name_equals_to_sickpayh_49',
            test='''{{ result('foreach_declare_list_6_40').name == 'Sick Pay-H' }}''',
            yes_task="trigger_dag_run_assured_partners_child_new_user_sick_pay_h_policy_assignment_50",
            no_task="trigger_dag_run_assured_partners_child_new_user_timeoff_type_default_assignment_52",
        )

        trigger_dag_run_assured_partners_child_new_user_sick_pay_h_policy_assignment_50 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_assured_partners_child_new_user_sick_pay_h_policy_assignment_50',
            retries=0,
            trigger_dag_id=config.child_new_user_sick_pay_h_policy_assignment_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "parentjobid": "{{ dag_run.conf.parentjobid }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "startdate": "{{ dag_run.conf.tsstartdate }}",
                "timeoffuri": "{{ result('foreach_declare_list_6_40').uri }}",
                "employeenumber": "{{ dag_run.conf.EmplID_Login }}",
                "firstname": "{{ dag_run.conf.FirstName }}",
                "lastname": "{{ dag_run.conf.LastName }}",
                "timeofftypename": "{{ result('foreach_declare_list_6_40').name }}",
                "schedulename": "{{ dag_run.conf.Schedule }}",
                "weekly_scheduled_hours": "{{ dag_run.conf.WeeklySTDHrs }}",
                "tenure": "{{ result('log_tenure_basedon_replicon_t_s_date_3') }}",
                "servicedate": "{{ dag_run.conf.ServiceDate }}",
                "integration_run_date": "{{ dag_run.conf.integration_run_date }}"
            }
        )

        insert_child_50_wait_list = rail.SetVariableOperator(
            task_id='insert_child_50_wait_list',
            name="{{result('create_child_triggered_list').name}}",
            append=True,
            value="{{result('trigger_dag_run_assured_partners_child_new_user_sick_pay_h_policy_assignment_50')}}"
        )

        trigger_dag_run_assured_partners_child_new_user_timeoff_type_default_assignment_52 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_assured_partners_child_new_user_timeoff_type_default_assignment_52',
            retries=0,
            trigger_dag_id=config.child_new_user_timeoff_type_default_assignment_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "parentjobid": "{{ dag_run.conf.parentjobid }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "startdate": "{{ dag_run.conf.tsstartdate }}",
                "timeoffuri": "{{ result('foreach_declare_list_6_40').uri }}",
                "employeenumber": "{{ dag_run.conf.EmplID_Login }}",
                "firstname": "{{ dag_run.conf.FirstName }}",
                "lastname": "{{ dag_run.conf.LastName }}",
                "timeofftypename": "{{ result('foreach_declare_list_6_40').name }}",
                "schedulename": "{{ dag_run.conf.Schedule }}",
                "weekly_scheduled_hours": "{{ dag_run.conf.WeeklySTDHrs }}",
                "tenure": "{{ result('log_tenure_basedon_replicon_t_s_date_3') }}",
                "servicedate": "{{ dag_run.conf.ServiceDate }}",
                "integration_run_date": "{{ dag_run.conf.integration_run_date }}"
            }
        )

        insert_child_52_wait_list = rail.SetVariableOperator(
            task_id='insert_child_52_wait_list',
            name="{{result('create_child_triggered_list').name}}",
            append=True,
            value="{{result('trigger_dag_run_assured_partners_child_new_user_timeoff_type_default_assignment_52')}}"
        )

        log_p_t_o_type_54 = rail.PythonOperator(
            task_id='log_p_t_o_type_54',
            python_callable=lambda: next(iter(filter(lambda x: x["time_off_type_name"] == rail.result(
                'foreach_declare_list_6_40')['name'], rail.result("assured_partners_pto_1_time_off_list_search_entries_38"))), {}).get('type', '')
        )

        if_log_p_t_o_type_54_equals_to_type1_55 = rail.IfOperator(
            task_id='if_log_p_t_o_type_54_equals_to_type1_55',
            test='''{{ result('log_p_t_o_type_54') == 'Type 1' }}''',
            yes_task="trigger_dag_run_child_new_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_56",
            no_task="if_log_pto_type_54_equals_to_type2_57",
        )

        trigger_dag_run_child_new_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_56 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_new_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_56',
            retries=0,
            trigger_dag_id=config.child_new_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "parentjobid": "{{ dag_run.conf.parentjobid }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "startdate": "{{ dag_run.conf.tsstartdate }}",
                "timeoffuri": "{{ result('foreach_declare_list_6_40').uri }}",
                "employeenumber": "{{ dag_run.conf.EmplID_Login }}",
                "firstname": "{{ dag_run.conf.FirstName }}",
                "lastname": "{{ dag_run.conf.LastName }}",
                "timeofftypename": "{{ result('foreach_declare_list_6_40').name }}",
                "schedulename": "{{ dag_run.conf.Schedule }}",
                "weekly_scheduled_hours": "{{ dag_run.conf.WeeklySTDHrs }}",
                "tenure": "{{ result('log_tenure_basedon_pto_seniority_date') }}",
                "servicedate": "{{ dag_run.conf.ServiceDate }}",
                "PTOSeniorityDate": "{{ dag_run.conf.PTOSeniorityDate }}",
                "integration_run_date": "{{ dag_run.conf.integration_run_date }}"
            }
        )

        insert_child_56_wait_list = rail.SetVariableOperator(
            task_id='insert_child_56_wait_list',
            name="{{result('create_child_triggered_list').name}}",
            append=True,
            value="{{result('trigger_dag_run_child_new_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_56')}}"
        )

        if_log_pto_type_54_equals_to_type2_57 = rail.IfOperator(
            task_id='if_log_pto_type_54_equals_to_type2_57',
            test='''{{ result('log_p_t_o_type_54') == 'Type 2' }}''',
            yes_task="if_foreach_1_name_equals_to_keenannoncah_58",
            no_task="if_log_p_t_o_type_54_equals_to_sickpto_62",
        )

        if_foreach_1_name_equals_to_keenannoncah_58 = rail.IfOperator(
            task_id='if_foreach_1_name_equals_to_keenannoncah_58',
            test='''{{ result('foreach_declare_list_6_40').name == 'Keenan Non-CA H'  or result('foreach_declare_list_6_40').name == 'Keenan Non-CA EX' }}''',
            yes_task="trigger_dag_run_child_new_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex_59",
            no_task="trigger_dag_run_child_new_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_61",
        )

        trigger_dag_run_child_new_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex_59 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_new_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex_59',
            retries=0,
            trigger_dag_id=config.child_new_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "parentjobid": "{{ dag_run.conf.parentjobid }}",
                "employeenumber": "{{ dag_run.conf.EmplID_Login }}",
                "firstname": "{{ dag_run.conf.FirstName }}",
                "lastname": "{{ dag_run.conf.LastName }}",
                "startdate": "{{ dag_run.conf.tsstartdate }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "timeoffuri": "{{ result('foreach_declare_list_6_40').uri }}",
                "timeofftypename": "{{ result('foreach_declare_list_6_40').name }}",
                "schedulename": "{{ dag_run.conf.Schedule }}",
                "weekly_scheduled_hours": "{{ dag_run.conf.WeeklySTDHrs }}",
                "tenure": "{{ result('log_tenure_basedon_pto_seniority_date') }}",
                "servicedate": "{{ dag_run.conf.ServiceDate }}",
                "PTOSeniorityDate": "{{ dag_run.conf.PTOSeniorityDate }}",
                "integration_run_date": "{{ dag_run.conf.integration_run_date }}"
            }
        )

        insert_child_59_wait_list = rail.SetVariableOperator(
            task_id='insert_child_59_wait_list',
            name="{{result('create_child_triggered_list').name}}",
            append=True,
            value="{{result('trigger_dag_run_child_new_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex_59')}}"
        )

        trigger_dag_run_child_new_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_61 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_new_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_61',
            retries=0,
            trigger_dag_id=config.child_new_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "parentjobid": "{{ dag_run.conf.parentjobid }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "startdate": "{{ dag_run.conf.tsstartdate }}",
                "timeoffuri": "{{ result('foreach_declare_list_6_40').uri }}",
                "employeenumber": "{{ dag_run.conf.EmplID_Login }}",
                "firstname": "{{ dag_run.conf.FirstName }}",
                "lastname": "{{ dag_run.conf.LastName }}",
                "timeofftypename": "{{ result('foreach_declare_list_6_40').name }}",
                "schedulename": "{{ dag_run.conf.Schedule }}",
                "weekly_scheduled_hours": "{{ dag_run.conf.WeeklySTDHrs }}",
                "tenure": "{{ result('log_tenure_basedon_pto_seniority_date') }}",
                "servicedate": "{{ dag_run.conf.ServiceDate }}",
                "PTOSeniorityDate": "{{ dag_run.conf.PTOSeniorityDate }}",
                "integration_run_date": "{{ dag_run.conf.integration_run_date }}"
            }
        )

        insert_child_61_wait_list = rail.SetVariableOperator(
            task_id='insert_child_61_wait_list',
            name="{{result('create_child_triggered_list').name}}",
            append=True,
            value="{{result('trigger_dag_run_child_new_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_61')}}"
        )

        if_log_p_t_o_type_54_equals_to_sickpto_62 = rail.IfOperator(
            task_id='if_log_p_t_o_type_54_equals_to_sickpto_62',
            test='''{{ result('log_p_t_o_type_54') == 'Sick PTO' }}''',
            yes_task="trigger_dag_run_child_new_user_timeoff_type_proration_assignment_sick_pay_p_063",
            no_task="foreach_declare_list_6_40_end",
        )

        trigger_dag_run_child_new_user_timeoff_type_proration_assignment_sick_pay_p_063 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_new_user_timeoff_type_proration_assignment_sick_pay_p_063',
            retries=0,
            trigger_dag_id=config.child_new_user_timeoff_type_proration_assignment_sick_pay_p_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "parentjobid": "{{ dag_run.conf.parentjobid }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "startdate": "{{ dag_run.conf.tsstartdate }}",
                "timeoffuri": "{{ result('foreach_declare_list_6_40').uri }}",
                "employeenumber": "{{ dag_run.conf.EmplID_Login }}",
                "firstname": "{{ dag_run.conf.FirstName }}",
                "lastname": "{{ dag_run.conf.LastName }}",
                "timeofftypename": "{{ result('foreach_declare_list_6_40').name }}",
                "schedulename": "{{ dag_run.conf.Schedule }}",
                "weekly_scheduled_hours": "{{ dag_run.conf.WeeklySTDHrs }}",
                "tenure": "{{ result('log_tenure_basedon_pto_seniority_date') }}",
                "servicedate": "{{ dag_run.conf.ServiceDate }}",
                "type": "Add",
                "integration_run_date": "{{ dag_run.conf.integration_run_date }}"
            }
        )

        insert_child_63_wait_list = rail.SetVariableOperator(
            task_id='insert_child_63_wait_list',
            name="{{result('create_child_triggered_list').name}}",
            append=True,
            value="{{result('trigger_dag_run_child_new_user_timeoff_type_proration_assignment_sick_pay_p_063')}}"
        )

        foreach_declare_list_6_40_end = rail.EmptyOperator(
            task_id='foreach_declare_list_6_40_end',
        )

        child_dag_ids = rail.PythonOperator(
            task_id='child_dag_ids',
            python_callable=lambda: [
                int(item) for item in rail.get_dag_run_var('wait_for_dag_runs')] if rail.get_dag_run_var('wait_for_dag_runs') else []
        )

        wait_for_child_dags = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_dags',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{result('child_dag_ids') | to_json}}"
        )

        gather_responses_from_child = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_responses_from_child',
            dag_runs="{{ result('child_dag_ids') | to_json}}",
            dagrun_task_id='final_response_from_dag',
            execution_timeout=timedelta(
                hours=config.responses_from_child_timeout),
            flatten=True
        )

        filter_error_responses = rail.PythonOperator(
            task_id='filter_error_responses',
            python_callable=lambda: [item for item in rail.result(
                'gather_responses_from_child') if item]
        )

        catch_and_log_error = rail.SetVariableOperator(
            task_id="catch_and_log_error",
            trigger_rule='one_failed',
            name='response_from_dag',
            append=False,
            value="Add Timeoff for new user- Dag_Run Error - {{get_error_message()}}"
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.get_dag_run_var(
                'response_from_dag') if rail.result('catch_and_log_error') else (rail.result('filter_error_responses') or null)
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error >> final_response_from_dag
        can_run_batch_task >> rail.Label(
            'No') >> response_from_dag

        response_from_dag >> log_tenure_basedon_replicon_t_s_date_3 >> log_tenure_basedon_pto_seniority_date >> adhoc_http_action_4 >> if_first_displaytext_present_5

        if_first_displaytext_present_5 >> rail.Label(
            'No') >> catch_and_log_error
        if_first_displaytext_present_5 >> rail.Label(
            'Yes') >> get_timeoff_uri_name_list >> log_final_set_timeoff_uris_34 >> if_log_12_present_35

        if_log_12_present_35 >> rail.Label(
            'No') >> catch_and_log_error
        if_log_12_present_35 >> rail.Label(
            'Yes') >> put_time_off_type_assignments_for_user_37

        put_time_off_type_assignments_for_user_37 >> assured_partners_pto_1_time_off_list_search_entries_38 \
            >> log_pto1_timeofflist_h_39 >> create_child_triggered_list >> foreach_declare_list_6_40

        foreach_declare_list_6_40 >> if_foreach_1_uri_present_41

        if_foreach_1_uri_present_41 >> rail.Label(
            'No') >> foreach_declare_list_6_40_end
        if_foreach_1_uri_present_41 >> rail.Label('Yes') >> accumulate_list_items_42 \
            >> get_default_time_off_type_policy_schedule_for_user_44 >> if_log_pto1_timeofflist_h_39_not_contains_foreach_1name_46

        if_log_pto1_timeofflist_h_39_not_contains_foreach_1name_46 >> rail.Label(
            'Yes') >> log_timeoff_policy_47 >> if_log_13_present_48

        if_log_13_present_48 >> rail.Label(
            'No') >> foreach_declare_list_6_40_end
        if_log_13_present_48 >> rail.Label(
            'Yes') >> if_foreach_1_name_equals_to_sickpayh_49

        if_foreach_1_name_equals_to_sickpayh_49 >> rail.Label(
            'No') >> trigger_dag_run_assured_partners_child_new_user_timeoff_type_default_assignment_52 >> insert_child_52_wait_list >> foreach_declare_list_6_40_end
        if_foreach_1_name_equals_to_sickpayh_49 >> rail.Label(
            'Yes') >> trigger_dag_run_assured_partners_child_new_user_sick_pay_h_policy_assignment_50 >> insert_child_50_wait_list >> foreach_declare_list_6_40_end

        if_log_pto1_timeofflist_h_39_not_contains_foreach_1name_46 >> rail.Label(
            'No') >> log_p_t_o_type_54

        log_p_t_o_type_54 >> if_log_p_t_o_type_54_equals_to_type1_55

        if_log_p_t_o_type_54_equals_to_type1_55 >> rail.Label(
            'No') >> if_log_pto_type_54_equals_to_type2_57
        if_log_p_t_o_type_54_equals_to_type1_55 >> rail.Label(
            'Yes') >> trigger_dag_run_child_new_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_56 \
            >> insert_child_56_wait_list >> if_log_pto_type_54_equals_to_type2_57

        if_log_pto_type_54_equals_to_type2_57 >> rail.Label(
            'No') >> if_log_p_t_o_type_54_equals_to_sickpto_62
        if_log_pto_type_54_equals_to_type2_57 >> rail.Label(
            'Yes') >> if_foreach_1_name_equals_to_keenannoncah_58

        if_foreach_1_name_equals_to_keenannoncah_58 >> rail.Label(
            'No') >> trigger_dag_run_child_new_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_61 \
            >> insert_child_61_wait_list >> if_log_p_t_o_type_54_equals_to_sickpto_62
        if_foreach_1_name_equals_to_keenannoncah_58 >> rail.Label(
            'Yes') >> trigger_dag_run_child_new_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex_59 \
            >> insert_child_59_wait_list >> if_log_p_t_o_type_54_equals_to_sickpto_62

        if_log_p_t_o_type_54_equals_to_sickpto_62 >> rail.Label(
            'No') >> foreach_declare_list_6_40_end
        if_log_p_t_o_type_54_equals_to_sickpto_62 >> rail.Label(
            'Yes') >> trigger_dag_run_child_new_user_timeoff_type_proration_assignment_sick_pay_p_063 >> insert_child_63_wait_list >> foreach_declare_list_6_40_end

        foreach_declare_list_6_40 >> foreach_declare_list_6_40_end >> child_dag_ids >> wait_for_child_dags >> gather_responses_from_child \
            >> filter_error_responses >> catch_and_log_error

    return dag


rail.for_each_instance(create_dag)
