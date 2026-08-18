
from datetime import timedelta
import json
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'genoadesign_user_import_gdi_child_workflow_to_add_remove_timeoff_type_for_existing_user_employee_type_change_v1_0_{config.instance}',
        description=f'Live|GDI_Child Workflow to add/remove timeoff type for existing user-employee type change V1.0 {config.instance}',
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
            no_task='get_eligible_time_off_types_for_booking_time_off_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_eligible_time_off_types_for_booking_time_off_3',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_eligible_time_off_types_for_booking_time_off_3 = rail.RepliconServiceOperator(
            task_id='get_eligible_time_off_types_for_booking_time_off_3',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        def get_assigned_time_offs(booking_task):
            assigned_timeoffs = []
            timeoffs = rail.result(booking_task)['policiesByTimeOffType']
            for to_info in timeoffs:
                if to_info['isTimeOffAllowedAgainstThisTimeOffType'] is True:
                    assigned_timeoffs.append({
                        "name": to_info['timeOffType']['name'],
                        "uri": to_info['timeOffType']['uri']
                    })
            return assigned_timeoffs

        assigned_timeoffs_6 = rail.PythonOperator(
            task_id='assigned_timeoffs_6',
            python_callable=lambda:  get_assigned_time_offs(
                'get_eligible_time_off_types_for_booking_time_off_3')
        )

        _adhoc_http_action_7 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_7',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
            data=None
        )

        if_first_displaytext_present_8 = rail.IfOperator(
            task_id='if_first_displaytext_present_8',
            test='''{{ result('_adhoc_http_action_7')[0].displayText | is_truthy }}''',
            yes_task="declare_variable_9",
            no_task="finish",
        )

        declare_variable_9 = rail.SetVariableOperator(
            task_id='declare_variable_9',
            append=False,
            name='Timeoff types to assign',
            value=None
        )

        if_request_employeetype_equals_to_fulltimehourly_10 = rail.IfOperator(
            task_id='if_request_employeetype_equals_to_fulltimehourly_10',
            test='''{{ dag_run.conf.employeetype | lower == 'full time hourly' }}''',
            yes_task="update_variable_11",
            no_task="if_request_employeetype_equals_to_fulltimesalaried_12",
        )

        update_variable_11 = rail.SetVariableOperator(
            task_id='update_variable_11',
            append=False,
            name='{{ result("declare_variable_9").name }}',
            value="Banked Time, Vacation, Discretionary, Yay-cation, Unpaid Time, Bereavement Leave, Maternity/Parental Leave, Compassionate Care Leave, Jury Duty, Holidays"
        )

        if_request_employeetype_equals_to_fulltimesalaried_12 = rail.IfOperator(
            task_id='if_request_employeetype_equals_to_fulltimesalaried_12',
            test='''{{ dag_run.conf.employeetype | lower == 'full time salaried' }}''',
            yes_task="update_variable_13",
            no_task="log_timeofftypestoassign_14",
        )

        update_variable_13 = rail.SetVariableOperator(
            task_id='update_variable_13',
            append=False,
            name='{{ result("declare_variable_9").name }}',
            value="Vacation, Discretionary, Yay-cation, Unpaid Time, Bereavement Leave, Maternity/Parental Leave, Compassionate Care Leave, Jury Duty, Holidays"
        )

        log_timeofftypestoassign_14 = rail.PythonOperator(
            task_id='log_timeofftypestoassign_14',
            python_callable=lambda:  rail.get_dag_run_var(
                rail.result('declare_variable_9')['name'])
        )

        def get_timeoffs_to_assign():
            new_timeoff_to_assign = []
            timeoffs_to_assign = rail.result(
                'log_timeofftypestoassign_14').split(',') if rail.result(
                'log_timeofftypestoassign_14') else []
            for timeoff in timeoffs_to_assign:
                to_uri = rail.find_first_by_attr_and_get_attr(rail.result(
                    '_adhoc_http_action_7'), 'displayText', timeoff.strip(), 'uri')
                if to_uri:
                    new_timeoff_to_assign.append({
                        "name": timeoff,
                        "uri": to_uri
                    })
            return new_timeoff_to_assign

        def get_timeoff_uri_to_assign():
            timeoff_info = get_timeoffs_to_assign()
            return [to['uri'] for to in timeoff_info]

        log_final_set_timeoff_uris_19 = rail.PythonOperator(
            task_id='log_final_set_timeoff_uris_19',
            # pylint: disable=unnecessary-lambda
            python_callable=lambda:  get_timeoff_uri_to_assign()
        )

        if_log_12_present_20 = rail.IfOperator(
            task_id='if_log_12_present_20',
            test='''{{ result('log_final_set_timeoff_uris_19') | is_truthy }}''',
            yes_task="put_time_off_type_assignments_for_user_21",
            no_task="finish",
        )

        put_time_off_type_assignments_for_user_21 = rail.RepliconServiceOperator(
            task_id='put_time_off_type_assignments_for_user_21',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUris": rail.result('log_final_set_timeoff_uris_19')
            }
        )

        get_eligible_time_off_types_for_booking_time_off_22 = rail.RepliconServiceOperator(
            task_id='get_eligible_time_off_types_for_booking_time_off_22',
            endpoint="/services/TimeOffService1.svc/GetEligibleTimeOffTypesForBookingTimeOff",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        foreach_response_23 = rail.ForEachOperator(
            task_id='foreach_response_23',
            items="{{ result('get_eligible_time_off_types_for_booking_time_off_22') | to_json }}",
            start_task='log_checkifthetimeoffisalreadyassigned_24',
            end_task='foreach_response_23_end'
        )

        log_checkifthetimeoffisalreadyassigned_24 = rail.PythonOperator(
            task_id='log_checkifthetimeoffisalreadyassigned_24',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'assigned_timeoffs_6'), 'name', rail.result('foreach_response_23')['uri'], 'uri')
        )

        if_log_checkifthetimeoffisalreadyassigned_24_blank_25 = rail.IfOperator(
            task_id='if_log_checkifthetimeoffisalreadyassigned_24_blank_25',
            test='''{{ result('log_checkifthetimeoffisalreadyassigned_24') | is_falsy }}''',
            yes_task="accumulate_list_items_26",
            no_task="foreach_response_23_end",
        )

        accumulate_list_items_26 = rail.SetVariableOperator(
            task_id='accumulate_list_items_26',
            name='Assigned timeoff types',
            append=True,
            value={
                "timeofftype": "{{ result('foreach_response_23').displayText }}"
            }
        )

        get_default_time_off_type_policy_schedule_for_user_28 = rail.RepliconServiceOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user_28',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ result('foreach_response_23').uri }}"
                }
            }
        )

        log_timeoff_policy_30 = rail.PythonOperator(
            task_id='log_timeoff_policy_30',
            python_callable=lambda: json.loads(json.dumps(
                    rail.result('get_default_time_off_type_policy_schedule_for_user_28'), ensure_ascii=False).replace('null', '"effective"').replace(
                        '"script"', '"scriptTarget"'))
        )

        if_log_13_present_31 = rail.IfOperator(
            task_id='if_log_13_present_31',
            test='''{{ result('log_timeoff_policy_30') | is_truthy }}''',
            yes_task="put_user_time_off_account_policy_set_schedule_32",
            no_task="foreach_response_23_end",
        )

        put_user_time_off_account_policy_set_schedule_32 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_32',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_response_23')['uri']
                },
                "policySetScheduleEntries": rail.result('log_timeoff_policy_30')
            }
        )

        foreach_response_23_end = rail.EmptyOperator(
            task_id='foreach_response_23_end',
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
            'No') >> get_eligible_time_off_types_for_booking_time_off_3
        get_eligible_time_off_types_for_booking_time_off_3 >> assigned_timeoffs_6 >> \
            _adhoc_http_action_7 >> if_first_displaytext_present_8
        if_first_displaytext_present_8 >> rail.Label(
            'Yes') >> declare_variable_9 >> if_request_employeetype_equals_to_fulltimehourly_10
        if_request_employeetype_equals_to_fulltimehourly_10 >> rail.Label(
            'Yes') >> update_variable_11 >> if_request_employeetype_equals_to_fulltimesalaried_12
        if_request_employeetype_equals_to_fulltimehourly_10 >> rail.Label(
            'No') >> if_request_employeetype_equals_to_fulltimesalaried_12
        if_request_employeetype_equals_to_fulltimesalaried_12 >> rail.Label(
            'Yes') >> update_variable_13 >> log_timeofftypestoassign_14
        if_request_employeetype_equals_to_fulltimesalaried_12 >> rail.Label(
            'No') >> log_timeofftypestoassign_14 >> log_final_set_timeoff_uris_19 >> if_log_12_present_20
        if_log_12_present_20 >> rail.Label(
            'Yes') >> put_time_off_type_assignments_for_user_21 >> \
            get_eligible_time_off_types_for_booking_time_off_22 >> foreach_response_23 >> \
            log_checkifthetimeoffisalreadyassigned_24 >> if_log_checkifthetimeoffisalreadyassigned_24_blank_25
        if_log_checkifthetimeoffisalreadyassigned_24_blank_25 >> rail.Label(
            'Yes') >> accumulate_list_items_26 >> get_default_time_off_type_policy_schedule_for_user_28 >> \
            log_timeoff_policy_30 >> if_log_13_present_31
        if_log_13_present_31 >> rail.Label(
            'Yes') >> put_user_time_off_account_policy_set_schedule_32 >> foreach_response_23_end
        if_log_13_present_31 >> rail.Label('No') >> foreach_response_23_end
        if_log_checkifthetimeoffisalreadyassigned_24_blank_25 >> rail.Label(
            'No') >> foreach_response_23_end
        foreach_response_23 >> foreach_response_23_end >> finish
        if_log_12_present_20 >> rail.Label('No') >> finish
        if_first_displaytext_present_8 >> rail.Label(
            'No') >> finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
