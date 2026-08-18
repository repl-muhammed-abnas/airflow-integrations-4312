
from datetime import timedelta
import json
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'genoadesign_user_import_gdi_child_workflow_to_add_timeoff_type_for_new_user_v1_0_{config.instance}',
        description=f'Live|GDI_Child Workflow to add timeoff type for new user V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        # schedule_interval=config.schedule_interval,
        max_active_runs=1,
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
            no_task='declare_list_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='declare_list_3',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        declare_list_3 = rail.SetVariableOperator(
            task_id='declare_list_3',
            append=False,
            name='timeofftypestoassign',
            value=[]
        )

        _adhoc_http_action_4 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_4',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
            data=None
        )

        if_first_displaytext_present_5 = rail.IfOperator(
            task_id='if_first_displaytext_present_5',
            test='''{{ result('_adhoc_http_action_4')[0].displayText | is_truthy }}''',
            yes_task="declare_variable_6",
            no_task="finish",
        )

        declare_variable_6 = rail.SetVariableOperator(
            task_id='declare_variable_6',
            append=False,
            name='Timeoff types to assign',
            value=None
        )

        if_request_employeetype_equals_to_fulltimehourly_7 = rail.IfOperator(
            task_id='if_request_employeetype_equals_to_fulltimehourly_7',
            test='''{{ dag_run.conf.employeetype | lower == 'full time hourly' }}''',
            yes_task="update_variable_8",
            no_task="if_request_employeetype_equals_to_fulltimesalaried_9",
        )

        update_variable_8 = rail.SetVariableOperator(
            task_id='update_variable_8',
            append=False,
            name='{{ result("declare_variable_6").name }}',
            value="Banked Time, Vacation, Discretionary, Yay-cation, Unpaid Time, Bereavement Leave, Maternity/Parental Leave, Compassionate Care Leave, Jury Duty, Holidays"
        )

        if_request_employeetype_equals_to_fulltimesalaried_9 = rail.IfOperator(
            task_id='if_request_employeetype_equals_to_fulltimesalaried_9',
            test='''{{ dag_run.conf.employeetype | lower == 'full time salaried' }}''',
            yes_task="update_variable_10",
            no_task="log_timeofftypestoassign_11",
        )

        update_variable_10 = rail.SetVariableOperator(
            task_id='update_variable_10',
            append=False,
            name='{{ result("declare_variable_6").name }}',
            value="Vacation, Discretionary, Yay-cation, Unpaid Time, Bereavement Leave, Maternity/Parental Leave, Compassionate Care Leave, Jury Duty, Holidays"
        )

        log_timeofftypestoassign_11 = rail.PythonOperator(
            task_id='log_timeofftypestoassign_11',
            python_callable=lambda:  rail.get_dag_run_var(
                rail.result('declare_variable_6')['name'])
        )

        def get_timeoffs_to_assign():
            new_timeoff_to_assign = []
            timeoffs_to_assign = rail.result(
                'log_timeofftypestoassign_11').split(',') if rail.result(
                'log_timeofftypestoassign_11') else []
            for timeoff in timeoffs_to_assign:
                to_uri = rail.find_first_by_attr_and_get_attr(rail.result(
                    '_adhoc_http_action_4'), 'displayText', timeoff.strip(), 'uri')
                if to_uri:
                    new_timeoff_to_assign.append({
                        "name": timeoff,
                        "uri": to_uri
                    })
            return new_timeoff_to_assign

        def get_timeoff_uri_to_assign():
            timeoff_info = get_timeoffs_to_assign()
            return [to['uri'] for to in timeoff_info]

        log_final_set_timeoff_uris_16 = rail.PythonOperator(
            task_id='log_final_set_timeoff_uris_16',
            python_callable=get_timeoff_uri_to_assign
        )

        if_log_12_present_17 = rail.IfOperator(
            task_id='if_log_12_present_17',
            test='''{{ result('log_final_set_timeoff_uris_16') | is_truthy }}''',
            yes_task="put_time_off_type_assignments_for_user_18",
            no_task="finish",
        )

        put_time_off_type_assignments_for_user_18 = rail.RepliconServiceOperator(
            task_id='put_time_off_type_assignments_for_user_18',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUris": rail.result('log_final_set_timeoff_uris_16')
            }
        )

        get_eligible_time_off_types_for_booking_time_off_19 = rail.RepliconServiceOperator(
            task_id='get_eligible_time_off_types_for_booking_time_off_19',
            endpoint="/services/TimeOffService1.svc/GetEligibleTimeOffTypesForBookingTimeOff",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        foreach_response_20 = rail.ForEachOperator(
            task_id='foreach_response_20',
            items="{{ result('get_eligible_time_off_types_for_booking_time_off_19') | to_json }}",
            start_task='accumulate_list_items_21',
            end_task='foreach_response_20_end'
        )

        accumulate_list_items_21 = rail.SetVariableOperator(
            task_id='accumulate_list_items_21',
            name='Assigned timeoff types',
            append=True,
            value={
                "timeofftype": "{{ result('foreach_response_20').displayText }}"
            }
        )

        get_default_time_off_type_policy_schedule_for_user_23 = rail.RepliconServiceOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user_23',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ result('foreach_response_20').uri }}"
                }
            }
        )

        log_timeoff_policy_25 = rail.PythonOperator(
            task_id='log_timeoff_policy_25',
            python_callable=lambda: json.loads(json.dumps(
                    rail.result('get_default_time_off_type_policy_schedule_for_user_23'), ensure_ascii=False).replace('null', '"effective"').replace(
                        '"script"', '"scriptTarget"')) if rail.result('get_default_time_off_type_policy_schedule_for_user_23') else null
        )

        if_log_13_present_26 = rail.IfOperator(
            task_id='if_log_13_present_26',
            test='''{{ result('log_timeoff_policy_25') | is_truthy }}''',
            yes_task="put_user_time_off_account_policy_set_schedule_27",
            no_task="foreach_response_20_end",
        )

        put_user_time_off_account_policy_set_schedule_27 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_27',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_response_20')['uri']
                },
                "policySetScheduleEntries": rail.result('log_timeoff_policy_25')
            }
        )

        foreach_response_20_end = rail.EmptyOperator(
            task_id='foreach_response_20_end',
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> declare_list_3
        declare_list_3 >> _adhoc_http_action_4 >> if_first_displaytext_present_5
        if_first_displaytext_present_5 >> rail.Label(
            'Yes') >> declare_variable_6 >> if_request_employeetype_equals_to_fulltimehourly_7
        if_request_employeetype_equals_to_fulltimehourly_7 >> rail.Label(
            'Yes') >> update_variable_8 >> if_request_employeetype_equals_to_fulltimesalaried_9
        if_request_employeetype_equals_to_fulltimehourly_7 >> rail.Label(
            'No') >> if_request_employeetype_equals_to_fulltimesalaried_9
        if_request_employeetype_equals_to_fulltimesalaried_9 >> rail.Label(
            'Yes') >> update_variable_10 >> log_timeofftypestoassign_11
        if_request_employeetype_equals_to_fulltimesalaried_9 >> rail.Label(
            'No') >> log_timeofftypestoassign_11 >> log_final_set_timeoff_uris_16 >> if_log_12_present_17
        if_log_12_present_17 >> rail.Label(
            'Yes') >> put_time_off_type_assignments_for_user_18 >> get_eligible_time_off_types_for_booking_time_off_19 >> \
            foreach_response_20 >> accumulate_list_items_21 >> get_default_time_off_type_policy_schedule_for_user_23 >> \
            log_timeoff_policy_25 >> if_log_13_present_26
        if_log_13_present_26 >> rail.Label(
            'Yes') >> put_user_time_off_account_policy_set_schedule_27 >> foreach_response_20_end
        if_log_13_present_26 >> rail.Label('No') >> foreach_response_20_end
        foreach_response_20 >> foreach_response_20_end >> finish
        if_log_12_present_17 >> rail.Label('No') >> finish
        if_first_displaytext_present_5 >> rail.Label(
            'No') >> finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
