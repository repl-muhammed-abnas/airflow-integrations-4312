from datetime import timedelta
from airflow.models import Variable
import rail
from assuredpartnersinc.user_import_v4.utils import python_callable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_update_timeoff_type_for_seniority_date_dag_id,
        description=f'Assured Partners User Import Update Timeoff Type For Seniority Child {config.instance}',
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
            no_task='response_from_dag_variable'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='response_from_dag_variable',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        response_from_dag_variable = rail.SetVariableOperator(
            task_id="response_from_dag_variable",
            name='response_from_dag',
            append=False,
            value=""
        )

        log_tenure_basedon_pto_seniority_date = rail.PythonOperator(
            task_id='log_tenure_basedon_pto_seniority_date',
            python_callable=lambda dag_run:  python_callable.get_user_tenure_in_years(
                dag_run.conf['PTOSeniorityDate'], dag_run.conf['ChangeEffectiveDate'], dag_run) if dag_run.conf['PTOSeniorityDate'] else 0
        )

        if_request_type_blank_3 = rail.IfOperator(
            task_id='if_request_type_blank_3',
            test='''{{ dag_run.conf.type | is_falsy }}''',
            yes_task="catch_and_log_error",
            no_task="get_user_time_off_type_policy_summary_6",
        )

        get_user_time_off_type_policy_summary_6 = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary_6',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        assured_partners_pto_1_time_off_list_search_entries_7 = rail.PythonOperator(
            task_id='assured_partners_pto_1_time_off_list_search_entries_7',
            python_callable=lambda:  list(
                filter(lambda x: x["identifier"] == "timeoff", config.TO_PTO1_MAPPER))
        )

        log_pto_1_list_8 = rail.PythonOperator(
            task_id='log_pto_1_list_8',
            python_callable=lambda:  [item['time_off_type_name'] for item in rail.result(
                "assured_partners_pto_1_time_off_list_search_entries_7")]
        )

        dag_run_wait_list = rail.SetVariableOperator(
            task_id='dag_run_wait_list',
            name='wait_list',
            append=False,
            value=[]
        )

        foreach_foreach_response_9_10 = rail.ForEachOperator(
            task_id='foreach_foreach_response_9_10',
            items=lambda: rail.result('get_user_time_off_type_policy_summary_6')[
                'policiesByTimeOffType'],
            start_task='if_foreach_foreach_response_9_10_istimeoffallowedagainstthistimeofftype_is_true_11',
            end_task='foreach_foreach_response_9_10_end'
        )

        if_foreach_foreach_response_9_10_istimeoffallowedagainstthistimeofftype_is_true_11 = rail.IfOperator(
            task_id='if_foreach_foreach_response_9_10_istimeoffallowedagainstthistimeofftype_is_true_11',
            test='''{{ result('foreach_foreach_response_9_10').isTimeOffAllowedAgainstThisTimeOffType | is_truthy }}''',
            yes_task="log_check_pto_type_12",
            no_task="foreach_foreach_response_9_10_end",
        )

        log_check_pto_type_12 = rail.PythonOperator(
            task_id='log_check_pto_type_12',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(
                rail.result('assured_partners_pto_1_time_off_list_search_entries_7'), 'time_off_type_name', rail.result(
                    'foreach_foreach_response_9_10')['timeOffType']['name'], 'type')
        )

        if_service_date_changed = rail.IfOperator(
            task_id='if_service_date_changed',
            test="{{dag_run.conf.service_date_change | is_truthy }}",
            yes_task='if_log_check_pto_type_12_present_and_service_date_changed',
            no_task='if_log_check_pto_type_12_present_13'
        )

        if_log_check_pto_type_12_present_and_service_date_changed = rail.IfOperator(
            task_id='if_log_check_pto_type_12_present_and_service_date_changed',
            test='''{{ result('log_check_pto_type_12') | is_truthy }}''',
            yes_task="foreach_foreach_response_9_10_end",
            no_task="trigger_dag_run_child_user_timeoff_policy_update_for_start_date_update_time_off_types_19",
        )

        if_log_check_pto_type_12_present_13 = rail.IfOperator(
            task_id='if_log_check_pto_type_12_present_13',
            test='''{{ result('log_check_pto_type_12') | is_truthy }}''',
            yes_task="get_user_time_off_type_balance_summary",
            no_task="foreach_foreach_response_9_10_end",
        )

        get_user_time_off_type_balance_summary = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_balance_summary',
            endpoint="/services/TimeOffService1.svc/GetUserTimeOffTypeBalanceSummary",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUri": rail.result(
                    'foreach_foreach_response_9_10')['timeOffType']['uri'],
                "asOfDate": python_callable.get_split_date(dag_run.conf['ChangeEffectiveDate'], 'int')
            },
            data_handler=lambda res: (res['timeRemaining']['calendarDayDuration']['hours'] + ((float(
                res['timeRemaining']['calendarDayDuration']['minutes']) / 60) if int(
                    res['timeRemaining']['calendarDayDuration']['minutes']) > 0 else 0) + ((float(
                        res['timeRemaining']['calendarDayDuration']['seconds']) / 3600) if int(
                            res['timeRemaining']['calendarDayDuration']['seconds']) > 0 else 0)) if res else 0
        )

        if_log_check_pto_type_12_equals_to_type1_14 = rail.IfOperator(
            task_id='if_log_check_pto_type_12_equals_to_type1_14',
            test='''{{ result('log_check_pto_type_12') == 'Type 1' }}''',
            yes_task="trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_15",
            no_task="if_log_check_pto_type_12_equals_to_type2_16",
        )

        trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_15 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_15',
            retries=0,
            trigger_dag_id=config.child_rehire_transfer_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "parentjobid": "{{ dag_run.conf.parentjobid }}",
                "employeenumber": "{{ dag_run.conf.EmplID_Login }}",
                "firstname": "{{ dag_run.conf.FirstName }}",
                "lastname": "{{ dag_run.conf.LastName }}",
                "startdate": "{{ dag_run.conf.ServiceDate }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "timeoffuri": "{{ result('foreach_foreach_response_9_10').timeOffType.uri }}",
                "timeofftypename": "{{ result('foreach_foreach_response_9_10').timeOffType.name }}",
                "schedulename": "{{ dag_run.conf.Schedule }}",
                "weekly_scheduled_hours": "{{ dag_run.conf.WeeklySTDHrs }}",
                "previousstartdate": "{{ dag_run.conf.previousstartdate }}",
                "type": "{{ dag_run.conf.type }}",
                "previousbalance": 0,
                "loaend": "{{ dag_run.conf.LOASuspendPTOEnd }}",
                "tenure": "{{result('log_tenure_basedon_pto_seniority_date')}}",
                "ChangeEffectiveDate":  "{{dag_run.conf.ChangeEffectiveDate }}",
                "PTOSeniorityDate":  "{{dag_run.conf.PTOSeniorityDate }}",
                "integration_run_date": "{{ dag_run.conf.integration_run_date }}"
            }
        )

        add_dag_run_to_wait_list_15 = rail.SetVariableOperator(
            task_id='add_dag_run_to_wait_list_15',
            name='wait_list',
            append=True,
            value="{{result('trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_15')}}"
        )

        if_log_check_pto_type_12_equals_to_type2_16 = rail.IfOperator(
            task_id='if_log_check_pto_type_12_equals_to_type2_16',
            test='''{{ result('log_check_pto_type_12') == 'Type 2' }}''',
            yes_task="if_timeoff_name_equals_to_keenan_non_ca_h",
            no_task="foreach_foreach_response_9_10_end",
        )

        if_timeoff_name_equals_to_keenan_non_ca_h = rail.IfOperator(
            task_id='if_timeoff_name_equals_to_keenan_non_ca_h',
            test='''{{ result('foreach_foreach_response_9_10').timeOffType.name == 'Keenan Non-CA H'  or result('foreach_foreach_response_9_10').timeOffType.name == 'Keenan Non-CA EX' }}''',
            yes_task="trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex",
            no_task="trigger_dag_run_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_017",
        )

        trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex',
            retries=0,
            trigger_dag_id=config.child_rehire_transfer_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "parentjobid": "{{ dag_run.conf.parentjobid }}",
                "employeenumber": "{{ dag_run.conf.EmplID_Login }}",
                "firstname": "{{ dag_run.conf.FirstName }}",
                "lastname": "{{ dag_run.conf.LastName }}",
                "startdate": "{{ dag_run.conf.ServiceDate }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "timeoffuri": "{{ result('foreach_foreach_response_9_10').timeOffType.uri }}",
                "timeofftypename": "{{ result('foreach_foreach_response_9_10').timeOffType.name }}",
                "schedulename": "{{ dag_run.conf.Schedule }}",
                "weekly_scheduled_hours": "{{ dag_run.conf.WeeklySTDHrs }}",
                "previousstartdate": "{{ dag_run.conf.previousstartdate }}",
                "type": "{{ dag_run.conf.type }}",
                "previousbalance": 0,
                "loaend": "{{ dag_run.conf.LOASuspendPTOEnd }}",
                "tenure": "{{result('log_tenure_basedon_pto_seniority_date')}}",
                "ChangeEffectiveDate":  "{{dag_run.conf.ChangeEffectiveDate }}",
                "PTOSeniorityDate":  "{{dag_run.conf.PTOSeniorityDate }}",
                "integration_run_date": "{{ dag_run.conf.integration_run_date }}"
            }
        )

        add_dag_run_to_wait_list_keenan = rail.SetVariableOperator(
            task_id='add_dag_run_to_wait_list_keenan',
            name='wait_list',
            append=True,
            value="{{result('trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex')}}"
        )

        trigger_dag_run_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_017 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_017',
            retries=0,
            trigger_dag_id=config.child_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "parentjobid": "{{ dag_run.conf.parentjobid }}",
                "employeenumber": "{{ dag_run.conf.EmplID_Login }}",
                "firstname": "{{ dag_run.conf.FirstName }}",
                "lastname": "{{ dag_run.conf.LastName }}",
                "startdate": "{{ dag_run.conf.ServiceDate }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "timeoffuri": "{{ result('foreach_foreach_response_9_10').timeOffType.uri }}",
                "timeofftypename": "{{ result('foreach_foreach_response_9_10').timeOffType.name }}",
                "schedulename": "{{ dag_run.conf.Schedule }}",
                "weekly_scheduled_hours": "{{ dag_run.conf.WeeklySTDHrs }}",
                "previousstartdate": "{{ dag_run.conf.previousstartdate }}",
                "type": "{{ dag_run.conf.type }}",
                "previousbalance": 0,
                "loaend": "{{ dag_run.conf.LOASuspendPTOEnd }}",
                "tenure": "{{result('log_tenure_basedon_pto_seniority_date')}}",
                "ChangeEffectiveDate":  "{{dag_run.conf.ChangeEffectiveDate }}",
                "PTOSeniorityDate":  "{{dag_run.conf.PTOSeniorityDate }}",
                "integration_run_date": "{{ dag_run.conf.integration_run_date }}"
            }
        )

        add_dag_run_to_wait_list_17 = rail.SetVariableOperator(
            task_id='add_dag_run_to_wait_list_17',
            name='wait_list',
            append=True,
            value="{{result('trigger_dag_run_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_017')}}"
        )

        trigger_dag_run_child_user_timeoff_policy_update_for_start_date_update_time_off_types_19 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_user_timeoff_policy_update_for_start_date_update_time_off_types_19',
            retries=0,
            trigger_dag_id=config.child_user_timeoff_policy_update_for_start_date_update_time_off_types_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "parentjobid": "{{ dag_run.conf.parentjobid }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "startdate": "{{ dag_run.conf.ServiceDate }}",
                "timeoffuri": "{{ result('foreach_foreach_response_9_10').timeOffType.uri }}",
                "employeenumber": "{{ dag_run.conf.EmplID_Login }}",
                "firstname": "{{ dag_run.conf.FirstName }}",
                "lastname": "{{ dag_run.conf.LastName }}",
                "timeofftypename": "{{ result('foreach_foreach_response_9_10').timeOffType.name }}",
                "schedulename": "{{ dag_run.conf.Schedule }}",
                "weekly_scheduled_hours": "{{ dag_run.conf.WeeklySTDHrs }}",
                "type": "update",
                "previousstartdate": "{{ dag_run.conf.previousstartdate }}",
                "previousbalance": 0,
                "integration_run_date": "{{ dag_run.conf.integration_run_date }}"
            }
        )

        add_dag_run_to_wait_list_19 = rail.SetVariableOperator(
            task_id='add_dag_run_to_wait_list_19',
            name='wait_list',
            append=True,
            value="{{result('trigger_dag_run_child_user_timeoff_policy_update_for_start_date_update_time_off_types_19')}}"
        )

        foreach_foreach_response_9_10_end = rail.EmptyOperator(
            task_id='foreach_foreach_response_9_10_end',
        )

        child_dag_ids = rail.PythonOperator(
            task_id='child_dag_ids',
            python_callable=lambda: [
                int(item) for item in rail.get_dag_run_var('wait_list')] if rail.get_dag_run_var('wait_list') else []
        )

        wait_for_completion_of_triggered_dags = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_of_triggered_dags',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{result('child_dag_ids') | to_json}}"
        )

        gather_response_from_dag_runs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_response_from_dag_runs',
            dag_runs="{{result('child_dag_ids') | to_json}}",
            dagrun_task_id='final_response_from_dag',
            execution_timeout=timedelta(
                hours=config.gather_response_from_dag_runs_timeout_hours),
            flatten=True
        )

        catch_and_log_error = rail.PythonOperator(
            task_id="catch_and_log_error",
            trigger_rule='one_failed',
            python_callable=lambda dag_run: rail.render_template(
                "Error in Update Timeoff Type for Service Date Change : {{get_error_message()}}") if dag_run.conf['service_date_change'] else rail.render_template(
                    "Error in Update Timeoff Type for PTO Seniority Date Change : {{get_error_message()}}")
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.result(
                'catch_and_log_error') or rail.result('gather_response_from_dag_runs')
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> response_from_dag_variable

        response_from_dag_variable >> log_tenure_basedon_pto_seniority_date >> if_request_type_blank_3

        if_request_type_blank_3 >> rail.Label('Yes') >> catch_and_log_error
        if_request_type_blank_3 >> rail.Label(
            'No') >> get_user_time_off_type_policy_summary_6 >> assured_partners_pto_1_time_off_list_search_entries_7 >> log_pto_1_list_8 >> dag_run_wait_list >> foreach_foreach_response_9_10

        foreach_foreach_response_9_10 >> if_foreach_foreach_response_9_10_istimeoffallowedagainstthistimeofftype_is_true_11

        if_foreach_foreach_response_9_10_istimeoffallowedagainstthistimeofftype_is_true_11 >> rail.Label(
            'Yes') >> log_check_pto_type_12 >> if_service_date_changed

        if_service_date_changed >> rail.Label(
            'No') >> if_log_check_pto_type_12_present_13
        if_service_date_changed >> rail.Label(
            'Yes') >> if_log_check_pto_type_12_present_and_service_date_changed

        if_log_check_pto_type_12_present_and_service_date_changed >> rail.Label(
            'No') >> trigger_dag_run_child_user_timeoff_policy_update_for_start_date_update_time_off_types_19
        if_log_check_pto_type_12_present_and_service_date_changed >> rail.Label(
            'Yes') >> foreach_foreach_response_9_10_end

        trigger_dag_run_child_user_timeoff_policy_update_for_start_date_update_time_off_types_19 \
            >> add_dag_run_to_wait_list_19 >> foreach_foreach_response_9_10_end

        if_log_check_pto_type_12_present_13
        if_foreach_foreach_response_9_10_istimeoffallowedagainstthistimeofftype_is_true_11 >> rail.Label(
            'No') >> foreach_foreach_response_9_10_end

        if_log_check_pto_type_12_present_13 >> rail.Label(
            'No') >> foreach_foreach_response_9_10_end

        if_log_check_pto_type_12_present_13 >> rail.Label(
            'Yes') >> get_user_time_off_type_balance_summary >> if_log_check_pto_type_12_equals_to_type1_14

        if_log_check_pto_type_12_equals_to_type1_14 >> rail.Label(
            'No') >> if_log_check_pto_type_12_equals_to_type2_16
        if_log_check_pto_type_12_equals_to_type1_14 >> rail.Label(
            'Yes') >> trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_15 \
            >> add_dag_run_to_wait_list_15 >> if_log_check_pto_type_12_equals_to_type2_16

        if_log_check_pto_type_12_equals_to_type2_16 >> rail.Label(
            'No') >> foreach_foreach_response_9_10_end
        if_log_check_pto_type_12_equals_to_type2_16 >> rail.Label(
            'Yes') >> if_timeoff_name_equals_to_keenan_non_ca_h

        if_timeoff_name_equals_to_keenan_non_ca_h >> rail.Label(
            'No') >> trigger_dag_run_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_017
        if_timeoff_name_equals_to_keenan_non_ca_h >> rail.Label(
            'Yes') >> trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex \
            >> add_dag_run_to_wait_list_keenan >> foreach_foreach_response_9_10_end

        trigger_dag_run_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_017 \
            >> add_dag_run_to_wait_list_17 >> foreach_foreach_response_9_10_end

        foreach_foreach_response_9_10 >> foreach_foreach_response_9_10_end >> child_dag_ids >> wait_for_completion_of_triggered_dags >> gather_response_from_dag_runs\
            >> catch_and_log_error

        catch_and_log_error >> final_response_from_dag

    return dag


rail.for_each_instance(create_dag)
