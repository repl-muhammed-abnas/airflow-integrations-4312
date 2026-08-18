from datetime import timedelta
from airflow.models import Variable
import rail
from assuredpartnersinc.user_import_v3.utils import python_callable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_workflow_to_add_timeoff_type_for_rehire_dag_id,
        description=f'Assured Partners User Import Add time off type for rehire child {config.instance}',
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
            no_task='log_tenure_basedon_replicon_t_s_date_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_tenure_basedon_replicon_t_s_date_3',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
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
                                     for item in rail.result("get_timeoff_uri_name_list")]
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
            start_task='accumulate_list_items_41',
            end_task='foreach_declare_list_6_40_end'
        )

        accumulate_list_items_41 = rail.SetVariableOperator(
            task_id='accumulate_list_items_41',
            name='assigned_timeoff_types',
            append=True,
            value={
                    "timeofftype": "{{ result('foreach_declare_list_6_40').name }}"
            }
        )

        if_log_pto1_timeofflist_h_39_not_contains_foreach_40_name_42 = rail.IfOperator(
            task_id='if_log_pto1_timeofflist_h_39_not_contains_foreach_40_name_42',
            test=lambda: bool(rail.result('foreach_declare_list_6_40')[
                'name'] not in rail.result('log_pto1_timeofflist_h_39')),
            yes_task="trigger_dag_run_child_rehire_transfer_user_timeoff_policy_assignment_for_non_pto1_time_off_types_43",
            no_task="log_p_t_o_type_45",
        )

        trigger_dag_run_child_rehire_transfer_user_timeoff_policy_assignment_for_non_pto1_time_off_types_43 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_rehire_transfer_user_timeoff_policy_assignment_for_non_pto1_time_off_types_43',
            retries=0,
            trigger_dag_id=config.child_rehire_transfer_user_timeoff_policy_assignment_for_non_pto1_time_off_types_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "employeenumber": dag_run.conf['EmplID_Login'],
                "firstname": dag_run.conf['FirstName'],
                "lastname": dag_run.conf['LastName'],
                "startdate": dag_run.conf['ServiceDate'],
                "useruri": dag_run.conf['useruri'],
                "timeoffuri": rail.result('foreach_declare_list_6_40')['uri'],
                "timeofftypename": rail.result('foreach_declare_list_6_40')['name'],
                "schedulename": dag_run.conf['Schedule'],
                "weekly_scheduled_hours": dag_run.conf['WeeklySTDHrs'],
                "type": "rehire",
                "previousstartdate": dag_run.conf['previousstartdate'],
                "previousbalance": 0,
                "loaend": dag_run.conf['LOASuspendPTOEnd'] if dag_run.conf['LOASuspendPTOEnd'] else dag_run.conf['integration_run_date'],
                "ChangeEffectiveDate":  dag_run.conf['ChangeEffectiveDate'],
                "integration_run_date": dag_run.conf['integration_run_date'],
            }
        )

        insert_dag_43_to_wait_list = rail.SetVariableOperator(
            task_id='insert_dag_43_to_wait_list',
            name="{{result('create_child_triggered_list').name}}",
            append=True,
            value="{{result('trigger_dag_run_child_rehire_transfer_user_timeoff_policy_assignment_for_non_pto1_time_off_types_43')}}"
        )

        log_p_t_o_type_45 = rail.PythonOperator(
            task_id='log_p_t_o_type_45',
            python_callable=lambda: next(iter(filter(lambda x: x["time_off_type_name"] == rail.result(
                'foreach_declare_list_6_40')['name'], rail.result("assured_partners_pto_1_time_off_list_search_entries_38"))), {}).get('type', '')
        )

        if_log_p_t_o_type_45_equals_to_type1_46 = rail.IfOperator(
            task_id='if_log_p_t_o_type_45_equals_to_type1_46',
            test='''{{ result('log_p_t_o_type_45') == 'Type 1' }}''',
            yes_task="trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_47",
            no_task="if_log_p_t_o_type_45_equals_to_type1_48",
        )

        trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_47 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_47',
            retries=0,
            trigger_dag_id=config.child_rehire_transfer_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "employeenumber": dag_run.conf['EmplID_Login'],
                "firstname": dag_run.conf['FirstName'],
                "lastname": dag_run.conf['LastName'],
                "startdate": dag_run.conf['ServiceDate'],
                "useruri": dag_run.conf['useruri'],
                "timeoffuri": rail.result('foreach_declare_list_6_40')['uri'],
                "timeofftypename": rail.result('foreach_declare_list_6_40')['name'],
                "schedulename": dag_run.conf['Schedule'],
                "weekly_scheduled_hours": dag_run.conf['WeeklySTDHrs'],
                "type": "rehire",
                "previousstartdate": dag_run.conf['previousstartdate'],
                "previousbalance": 0,
                "loaend": dag_run.conf['LOASuspendPTOEnd'] if dag_run.conf['LOASuspendPTOEnd'] else dag_run.conf['integration_run_date'],
                "tenure": rail.result('log_tenure_basedon_pto_seniority_date'),
                "ChangeEffectiveDate":  dag_run.conf['ChangeEffectiveDate'],
                "PTOSeniorityDate":  dag_run.conf['PTOSeniorityDate'],
                "integration_run_date": dag_run.conf['integration_run_date'],
            }
        )

        insert_dag_47_to_wait_list = rail.SetVariableOperator(
            task_id='insert_dag_47_to_wait_list',
            name="{{result('create_child_triggered_list').name}}",
            append=True,
            value="{{result('trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_47')}}"
        )

        if_log_p_t_o_type_45_equals_to_type1_48 = rail.IfOperator(
            task_id='if_log_p_t_o_type_45_equals_to_type1_48',
            test='''{{ result('log_p_t_o_type_45') == 'Type 2' }}''',
            yes_task="if_foreach_1_name_equals_to_keenannoncah_49",
            no_task="if_log_p_t_o_type_45_equals_to_sickpto_53",
        )

        if_foreach_1_name_equals_to_keenannoncah_49 = rail.IfOperator(
            task_id='if_foreach_1_name_equals_to_keenannoncah_49',
            test='''{{ result('foreach_declare_list_6_40').name == 'Keenan Non-CA H'  or result('foreach_declare_list_6_40').name == 'Keenan Non-CA EX' }}''',
            yes_task="trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex_50",
            no_task="trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_52",
        )

        trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex_50 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex_50',
            retries=0,
            trigger_dag_id=config.child_rehire_transfer_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "employeenumber": dag_run.conf['EmplID_Login'],
                "firstname": dag_run.conf['FirstName'],
                "lastname": dag_run.conf['LastName'],
                "startdate": dag_run.conf['ServiceDate'],
                "useruri": dag_run.conf['useruri'],
                "timeoffuri": rail.result('foreach_declare_list_6_40')['uri'],
                "timeofftypename": rail.result('foreach_declare_list_6_40')['name'],
                "schedulename": dag_run.conf['Schedule'],
                "weekly_scheduled_hours": dag_run.conf['WeeklySTDHrs'],
                "type": "rehire",
                "previousstartdate": dag_run.conf['previousstartdate'],
                "previousbalance": 0,
                "loaend": dag_run.conf['LOASuspendPTOEnd'] if dag_run.conf['LOASuspendPTOEnd'] else dag_run.conf['integration_run_date'],
                "tenure": rail.result('log_tenure_basedon_pto_seniority_date'),
                "ChangeEffectiveDate":  dag_run.conf['ChangeEffectiveDate'],
                "PTOSeniorityDate":  dag_run.conf['PTOSeniorityDate'],
                "integration_run_date": dag_run.conf['integration_run_date'],
            }
        )

        insert_dag_50_to_wait_list = rail.SetVariableOperator(
            task_id='insert_dag_50_to_wait_list',
            name="{{result('create_child_triggered_list').name}}",
            append=True,
            value="{{result('trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex_50')}}"
        )

        trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_52 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_52',
            retries=0,
            trigger_dag_id=config.child_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "employeenumber": dag_run.conf['EmplID_Login'],
                "firstname": dag_run.conf['FirstName'],
                "lastname": dag_run.conf['LastName'],
                "startdate": dag_run.conf['ServiceDate'],
                "useruri": dag_run.conf['useruri'],
                "timeoffuri": rail.result('foreach_declare_list_6_40')['uri'],
                "timeofftypename": rail.result('foreach_declare_list_6_40')['name'],
                "schedulename": dag_run.conf['Schedule'],
                "weekly_scheduled_hours": dag_run.conf['WeeklySTDHrs'],
                "type": dag_run.conf['type'],
                "previousstartdate": dag_run.conf['previousstartdate'],
                "previousbalance": 0,
                "loaend": dag_run.conf['LOASuspendPTOEnd'] if dag_run.conf['LOASuspendPTOEnd'] else dag_run.conf['integration_run_date'],
                "tenure": rail.result('log_tenure_basedon_pto_seniority_date'),
                "ChangeEffectiveDate":  dag_run.conf['ChangeEffectiveDate'],
                "PTOSeniorityDate":  dag_run.conf['PTOSeniorityDate'],
                "integration_run_date": dag_run.conf['integration_run_date'],
            }
        )

        insert_dag_52_to_wait_list = rail.SetVariableOperator(
            task_id='insert_dag_52_to_wait_list',
            name="{{result('create_child_triggered_list').name}}",
            append=True,
            value="{{result('trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_52')}}"
        )

        if_log_p_t_o_type_45_equals_to_sickpto_53 = rail.IfOperator(
            task_id='if_log_p_t_o_type_45_equals_to_sickpto_53',
            test='''{{ result('log_p_t_o_type_45') == 'Sick PTO' }}''',
            yes_task="trigger_dag_run_child_update_rehire_user_timeoff_type_proration_assignment_sick_pay_p_54",
            no_task="foreach_declare_list_6_40_end",
        )

        trigger_dag_run_child_update_rehire_user_timeoff_type_proration_assignment_sick_pay_p_54 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_update_rehire_user_timeoff_type_proration_assignment_sick_pay_p_54',
            retries=0,
            trigger_dag_id=config.child_update_rehire_user_timeoff_type_proration_assignment_sick_pay_p_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "employeenumber": dag_run.conf['EmplID_Login'],
                "firstname": dag_run.conf['FirstName'],
                "lastname": dag_run.conf['LastName'],
                "startdate": dag_run.conf['ServiceDate'],
                "servicedate": dag_run.conf['ServiceDate'],
                "useruri": dag_run.conf['useruri'],
                "timeoffuri": rail.result('foreach_declare_list_6_40')['uri'],
                "timeofftypename": rail.result('foreach_declare_list_6_40')['name'],
                "schedulename": dag_run.conf['Schedule'],
                "weekly_scheduled_hours": dag_run.conf['WeeklySTDHrs'],
                "type": "rehire",
                "currentschedule": dag_run.conf['currentschedule'],
                "currentscheduleuri": dag_run.conf['currentscheduleuri'],
                "schedulechange": dag_run.conf['schedulechange'],
                "ChangeEffectiveDate":  dag_run.conf['ChangeEffectiveDate'],
                "integration_run_date": dag_run.conf['integration_run_date'],
            }
        )

        insert_dag_54_to_wait_list = rail.SetVariableOperator(
            task_id='insert_dag_54_to_wait_list',
            name="{{result('create_child_triggered_list').name}}",
            append=True,
            value="{{result('trigger_dag_run_child_update_rehire_user_timeoff_type_proration_assignment_sick_pay_p_54')}}"
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
            dag_runs='{{ result("child_dag_ids") }}',
            dagrun_task_id='final_response_from_dag',
            execution_timeout=timedelta(
                hours=config.responses_from_child_timeout),
            flatten=True
        )

        catch_and_log_error = rail.PythonOperator(
            task_id="catch_and_log_error",
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "Add Timeoff for rehire user- Dag_Run Error - {{get_error_message()}}")
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.result(
                'catch_and_log_error') or rail.result('gather_responses_from_child')
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error >> final_response_from_dag
        can_run_batch_task >> rail.Label(
            'No') >> log_tenure_basedon_replicon_t_s_date_3

        log_tenure_basedon_replicon_t_s_date_3 >> log_tenure_basedon_pto_seniority_date >> adhoc_http_action_4 >> if_first_displaytext_present_5

        if_first_displaytext_present_5 >> rail.Label(
            'No') >> catch_and_log_error >> final_response_from_dag
        if_first_displaytext_present_5 >> rail.Label(
            'Yes') >> get_timeoff_uri_name_list >> log_final_set_timeoff_uris_34 >> if_log_12_present_35

        if_log_12_present_35 >> rail.Label('No') >> catch_and_log_error
        if_log_12_present_35 >> rail.Label('Yes') >> put_time_off_type_assignments_for_user_37 \
            >> assured_partners_pto_1_time_off_list_search_entries_38 >> log_pto1_timeofflist_h_39 >> create_child_triggered_list >> foreach_declare_list_6_40

        foreach_declare_list_6_40 >> accumulate_list_items_41 >> if_log_pto1_timeofflist_h_39_not_contains_foreach_40_name_42

        if_log_pto1_timeofflist_h_39_not_contains_foreach_40_name_42 >> rail.Label(
            'No') >> log_p_t_o_type_45
        if_log_pto1_timeofflist_h_39_not_contains_foreach_40_name_42 >> rail.Label(
            'Yes') >> trigger_dag_run_child_rehire_transfer_user_timeoff_policy_assignment_for_non_pto1_time_off_types_43 >> insert_dag_43_to_wait_list \
            >> foreach_declare_list_6_40_end

        log_p_t_o_type_45 >> if_log_p_t_o_type_45_equals_to_type1_46

        if_log_p_t_o_type_45_equals_to_type1_46 >> rail.Label(
            'No') >> if_log_p_t_o_type_45_equals_to_type1_48
        if_log_p_t_o_type_45_equals_to_type1_46 >> rail.Label(
            'Yes') >> trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_47 \
            >> insert_dag_47_to_wait_list >> if_log_p_t_o_type_45_equals_to_type1_48

        if_log_p_t_o_type_45_equals_to_type1_48 >> rail.Label(
            'No') >> if_log_p_t_o_type_45_equals_to_sickpto_53
        if_log_p_t_o_type_45_equals_to_type1_48 >> rail.Label(
            'Yes') >> if_foreach_1_name_equals_to_keenannoncah_49

        if_foreach_1_name_equals_to_keenannoncah_49 >> rail.Label(
            'No') >> trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_52 \
            >> insert_dag_52_to_wait_list >> if_log_p_t_o_type_45_equals_to_sickpto_53
        if_foreach_1_name_equals_to_keenannoncah_49 >> rail.Label(
            'Yes') >> trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex_50 \
            >> insert_dag_50_to_wait_list >> if_log_p_t_o_type_45_equals_to_sickpto_53

        if_log_p_t_o_type_45_equals_to_sickpto_53 >> rail.Label(
            'No') >> foreach_declare_list_6_40_end
        if_log_p_t_o_type_45_equals_to_sickpto_53 >> rail.Label('Yes') >> trigger_dag_run_child_update_rehire_user_timeoff_type_proration_assignment_sick_pay_p_54 \
            >> insert_dag_54_to_wait_list >> foreach_declare_list_6_40_end

        foreach_declare_list_6_40 >> foreach_declare_list_6_40_end >> child_dag_ids >> wait_for_child_dags >> gather_responses_from_child >> catch_and_log_error

    return dag


rail.for_each_instance(create_dag)
