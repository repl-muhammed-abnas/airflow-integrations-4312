from datetime import timedelta
from airflow.models import Variable
from assuredpartnersinc.user_import_v3.utils import python_callable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_workflow_for_change_in_schedule_and_pto_1_policy_update_dag_id,
        description=f'Assured Partners User Import Workflow for change in Schedule and PTO 1 Policy Update Child{config.instance}',
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
            no_task='if_log_check_if_previous_scheduled_hours_equals_new_scheduled_hours_12'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_log_check_if_previous_scheduled_hours_equals_new_scheduled_hours_12',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def check_new_old_scheduled_hours_per_day(dag_run):
            new_schedule_parsed = python_callable.parse_schedule_name(dag_run.conf['Schedule'])
            new_schedule_number_of_working_days = new_schedule_parsed['number_of_working_days_in_week']
            new_schedule_hours_per_day = float(
                dag_run.conf['WeeklySTDHrs']) / float(new_schedule_number_of_working_days)

            if dag_run.conf['previous_schedule']:
                previous_schedule_parsed = python_callable.parse_schedule_name(dag_run.conf['previous_schedule'])
                previous_schedule_number_of_working_days = previous_schedule_parsed['number_of_working_days_in_week']
                previous_schedule_weekly_scheduled_hours = previous_schedule_parsed['weekly_scheduled_hours']
                previous_schedule_hours_per_day = (
                    previous_schedule_weekly_scheduled_hours / previous_schedule_number_of_working_days)
            else:
                previous_schedule_hours_per_day = 0

            if previous_schedule_hours_per_day == new_schedule_hours_per_day:
                return True
            return False

        if_log_check_if_previous_scheduled_hours_equals_new_scheduled_hours_12 = rail.IfOperator(
            task_id='if_log_check_if_previous_scheduled_hours_equals_new_scheduled_hours_12',
            test=lambda dag_run: check_new_old_scheduled_hours_per_day(
                dag_run),
            yes_task="catch_and_log_error",
            no_task="get_time_off_type_assignments_for_user_14",
        )

        get_time_off_type_assignments_for_user_14 = rail.RepliconServiceOperator(
            task_id='get_time_off_type_assignments_for_user_14',
            endpoint="/services/TimeOffService1.svc/GetTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        assured_partners_pto_1_time_off_list_search_entries_15 = rail.PythonOperator(
            task_id='assured_partners_pto_1_time_off_list_search_entries_15',
            python_callable=lambda:  list(
                filter(lambda x: x["identifier"] == "timeoff", config.TO_PTO1_MAPPER))
        )

        create_child_triggered_list = rail.SetVariableOperator(
            task_id='create_child_triggered_list',
            name='wait_for_dag_runs',
            append=False,
            value=[]
        )

        foreach_response_16 = rail.ForEachOperator(
            task_id='foreach_response_16',
            items=lambda: rail.result(
                'get_time_off_type_assignments_for_user_14'),
            start_task='if_foreach_1_uri_present_17',
            end_task='foreach_response_16_end'
        )

        if_foreach_1_uri_present_17 = rail.IfOperator(
            task_id='if_foreach_1_uri_present_17',
            test='''{{ result('foreach_response_16').uri | is_truthy }}''',
            yes_task="log_pto_type",
            no_task="foreach_response_16_end",
        )

        log_pto_type = rail.PythonOperator(
            task_id='log_pto_type',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'assured_partners_pto_1_time_off_list_search_entries_15'), 'time_off_type_name', rail.result('foreach_response_16')['name'], 'type')
        )

        if_log_p_t_o_type_18_equals_to_type1_19 = rail.IfOperator(
            task_id='if_log_p_t_o_type_18_equals_to_type1_19',
            test='''{{ result('log_pto_type') == 'Type 1' }}''',
            yes_task="trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_20",
            no_task="if_log_p_t_o_type_18_equals_to_type2_21",
        )

        trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_20 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_20',
            retries=0,
            trigger_dag_id=config.child_rehire_transfer_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "useruri": dag_run.conf['useruri'],
                "startdate": dag_run.conf['ServiceDate'],
                "timeoffuri": rail.result('foreach_response_16')['uri'],
                "employeenumber": dag_run.conf['EmplID_Login'],
                "firstname": dag_run.conf['FirstName'],
                "lastname": dag_run.conf['LastName'],
                "timeofftypename": rail.result('foreach_response_16')['name'],
                "schedulename": dag_run.conf['Schedule'],
                "weekly_scheduled_hours": dag_run.conf['WeeklySTDHrs'],
                "type": "schedule change",
                "previousstartdate": dag_run.conf['previousstartdate'],
                "previousbalance": "0",
                "loaend": dag_run.conf['LOASuspendPTOEnd'] if dag_run.conf['LOASuspendPTOEnd'] else dag_run.conf['integration_run_date'],
                "tenure": dag_run.conf['tenure'],
                "ChangeEffectiveDate":  dag_run.conf['ChangeEffectiveDate'],
                "PTOSeniorityDate":  dag_run.conf['PTOSeniorityDate'],
                "integration_run_date": dag_run.conf['integration_run_date'],
            }
        )

        insert_child_20_wait_list = rail.SetVariableOperator(
            task_id='insert_child_20_wait_list',
            name="{{result('create_child_triggered_list').name}}",
            append=True,
            value="{{result('trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_20')}}"
        )

        if_log_p_t_o_type_18_equals_to_type2_21 = rail.IfOperator(
            task_id='if_log_p_t_o_type_18_equals_to_type2_21',
            test='''{{ result('log_pto_type') == 'Type 2' }}''',
            yes_task="if_timeoff_name_equals_to_keenan_non_ca_h",
            no_task="foreach_response_16_end",
        )

        if_timeoff_name_equals_to_keenan_non_ca_h = rail.IfOperator(
            task_id='if_timeoff_name_equals_to_keenan_non_ca_h',
            test='''{{ result('foreach_response_16').name == 'Keenan Non-CA H'  or result('foreach_response_16').name == 'Keenan Non-CA EX' }}''',
            yes_task="trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex",
            no_task="trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_22",
        )

        trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex',
            retries=0,
            trigger_dag_id=config.child_rehire_transfer_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "useruri": dag_run.conf['useruri'],
                "startdate": dag_run.conf['ServiceDate'],
                "timeoffuri": rail.result('foreach_response_16')['uri'],
                "employeenumber": dag_run.conf['EmplID_Login'],
                "firstname": dag_run.conf['FirstName'],
                "lastname": dag_run.conf['LastName'],
                "timeofftypename": rail.result('foreach_response_16')['name'],
                "schedulename": dag_run.conf['Schedule'],
                "weekly_scheduled_hours": dag_run.conf['WeeklySTDHrs'],
                "type": "schedule change",
                "previousstartdate": dag_run.conf['previousstartdate'],
                "previousbalance": "0",
                "loaend": dag_run.conf['LOASuspendPTOEnd'] if dag_run.conf['LOASuspendPTOEnd'] else dag_run.conf['integration_run_date'],
                "tenure": dag_run.conf['tenure'],
                "ChangeEffectiveDate":  dag_run.conf['ChangeEffectiveDate'],
                "PTOSeniorityDate":  dag_run.conf['PTOSeniorityDate'],
                "integration_run_date": dag_run.conf['integration_run_date'],
            }
        )

        insert_child_keennan_non_ca_wait_list = rail.SetVariableOperator(
            task_id='insert_child_keennan_non_ca_wait_list',
            name="{{result('create_child_triggered_list').name}}",
            append=True,
            value="{{result('trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex')}}"
        )

        trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_22 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_22',
            retries=0,
            trigger_dag_id=config.child_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "useruri": dag_run.conf['useruri'],
                "startdate": dag_run.conf['ServiceDate'],
                "timeoffuri": rail.result('foreach_response_16')['uri'],
                "employeenumber": dag_run.conf['EmplID_Login'],
                "firstname": dag_run.conf['FirstName'],
                "lastname": dag_run.conf['LastName'],
                "timeofftypename": rail.result('foreach_response_16')['name'],
                "schedulename": dag_run.conf['Schedule'],
                "weekly_scheduled_hours": dag_run.conf['WeeklySTDHrs'],
                "type": "schedule change",
                "previousstartdate": dag_run.conf['previousstartdate'],
                "previousbalance": "0",
                "loaend": dag_run.conf['LOASuspendPTOEnd'] if dag_run.conf['LOASuspendPTOEnd'] else dag_run.conf['integration_run_date'],
                "tenure": dag_run.conf['tenure'],
                "ChangeEffectiveDate":  dag_run.conf['ChangeEffectiveDate'],
                "PTOSeniorityDate":  dag_run.conf['PTOSeniorityDate'],
                "integration_run_date": dag_run.conf['integration_run_date'],
            }
        )

        insert_child_22_wait_list = rail.SetVariableOperator(
            task_id='insert_child_22_wait_list',
            name="{{result('create_child_triggered_list').name}}",
            append=True,
            value="{{result('trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_22')}}"
        )

        foreach_response_16_end = rail.EmptyOperator(
            task_id='foreach_response_16_end',
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
                "Error in Workflow for change in Schedule and PTO 1 Policy Update : {{get_error_message()}}")
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.result(
                "catch_and_log_error") or rail.result('gather_responses_from_child')
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label(
            'No') >> if_log_check_if_previous_scheduled_hours_equals_new_scheduled_hours_12

        if_log_check_if_previous_scheduled_hours_equals_new_scheduled_hours_12 >> rail.Label(
            'Yes') >> catch_and_log_error
        if_log_check_if_previous_scheduled_hours_equals_new_scheduled_hours_12 >> rail.Label('No') >> get_time_off_type_assignments_for_user_14 \
            >> assured_partners_pto_1_time_off_list_search_entries_15 >> create_child_triggered_list >> foreach_response_16

        foreach_response_16 >> if_foreach_1_uri_present_17

        if_foreach_1_uri_present_17 >> rail.Label(
            'No') >> foreach_response_16_end
        if_foreach_1_uri_present_17 >> rail.Label(
            'Yes') >> log_pto_type >> if_log_p_t_o_type_18_equals_to_type1_19

        if_log_p_t_o_type_18_equals_to_type1_19 >> rail.Label(
            'No') >> if_log_p_t_o_type_18_equals_to_type2_21
        if_log_p_t_o_type_18_equals_to_type1_19 >> rail.Label(
            'Yes') >> trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_ap_pto_h_and_ahm_plan_h_and_seattle_plan_h_20 \
            >> insert_child_20_wait_list >> if_log_p_t_o_type_18_equals_to_type2_21

        if_log_p_t_o_type_18_equals_to_type2_21 >> rail.Label(
            'No') >> foreach_response_16_end
        if_log_p_t_o_type_18_equals_to_type2_21 >> rail.Label(
            'Yes') >> if_timeoff_name_equals_to_keenan_non_ca_h

        if_timeoff_name_equals_to_keenan_non_ca_h >> rail.Label(
            'No') >> trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_22
        if_timeoff_name_equals_to_keenan_non_ca_h >> rail.Label(
            'Yes') >> trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex \
            >> insert_child_keennan_non_ca_wait_list >> foreach_response_16_end

        trigger_dag_run_child_rehire_transfer_user_timeoff_type_proration_assignment_keennan_h_neace_special_h_and_ap_ca_plan_h_22 \
            >> insert_child_22_wait_list >> foreach_response_16_end

        foreach_response_16 >> foreach_response_16_end >> child_dag_ids >> wait_for_child_dags >> gather_responses_from_child \
            >> catch_and_log_error >> final_response_from_dag

    return dag


rail.for_each_instance(create_dag)
