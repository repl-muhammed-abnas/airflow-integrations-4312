
from datetime import timedelta
import json
from airflow.models import Variable
import rail

from sunovion.user_import.mappers.sunovion_mapper_file import sunovion_mapper

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'sunovion_user_import_child_workflow_to_update_timeoff_type_for_existing_user_rehire_{config.instance}',
        description=f'Live|Sunovion_Child Workflow to update timeoff type for existing user_Rehire {config.instance}',
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
            no_task='adhoc_http_action_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='adhoc_http_action_3',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        adhoc_http_action_3 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_3',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
        )

        if_first_displaytext_present_4 = rail.IfOperator(
            task_id='if_first_displaytext_present_4',
            test=lambda: bool(rail.result('adhoc_http_action_3') and rail.result(
                'adhoc_http_action_3')[0]['displayText']),
            yes_task="sunovion_mapper_file_search_entries_5",
            no_task="catch_error",
        )

        sunovion_mapper_file_search_entries_5 = rail.PythonOperator(
            task_id='sunovion_mapper_file_search_entries_5',
            python_callable=lambda dag_run:  list(filter(
                lambda x: x["type"] == "timeoff type" and x["identifier_1"] == dag_run.conf['employeetype'], sunovion_mapper))
        )

        def get_timeofftype_uris():
            timeoffs = [entry['data_set'] for entry in rail.result(
                'sunovion_mapper_file_search_entries_5')]
            enabled_timeofftypes = rail.result('adhoc_http_action_3')
            return [rail.find_first_by_attr_and_get_attr(enabled_timeofftypes, 'displayText', timeoff, 'uri', '') for timeoff in timeoffs]

        get_first_set_of_uris = rail.PythonOperator(
            task_id='get_first_set_of_uris',
            python_callable=get_timeofftype_uris
        )

        if_request_workdayemployeetype_equals_to_inpatriate_11 = rail.IfOperator(
            task_id='if_request_workdayemployeetype_equals_to_inpatriate_11',
            test='''{{ dag_run.conf.workdayemployeetype == 'Inpatriate' }}''',
            yes_task="log_second_set_timeoff_uris_14",
            no_task="if_request_workdayexecutive_equals_to_yes_15",
        )

        log_second_set_timeoff_uris_14 = rail.PythonOperator(
            task_id='log_second_set_timeoff_uris_14',
            python_callable=lambda: [rail.find_first_by_attr_and_get_attr(rail.result(
                'adhoc_http_action_3'), 'displayText', "*Vacation: In-Pat", 'uri', '')]
        )

        if_request_workdayexecutive_equals_to_yes_15 = rail.IfOperator(
            task_id='if_request_workdayexecutive_equals_to_yes_15',
            test='''{{ dag_run.conf.workdayexecutive == 'Yes'  and result('log_second_set_timeoff_uris_14') | is_falsy }}''',
            yes_task="log_third_set_timeoff_uris_18",
            no_task="if_log_9_blank_19",
        )

        log_third_set_timeoff_uris_18 = rail.PythonOperator(
            task_id='log_third_set_timeoff_uris_18',
            python_callable=lambda: [rail.find_first_by_attr_and_get_attr(rail.result(
                'adhoc_http_action_3'), 'displayText', '*Vacation: ELT', 'uri', '')]
        )

        if_log_9_blank_19 = rail.IfOperator(
            task_id='if_log_9_blank_19',
            test='''{{ result('log_second_set_timeoff_uris_14') | is_falsy  and result('log_third_set_timeoff_uris_18') | is_falsy }}''',
            yes_task="if_request_workdayemployeetype_equals_to_parttime_20",
            no_task="if_log_fourth_set_timeoff_uris_23_blank_24",
        )

        if_request_workdayemployeetype_equals_to_parttime_20 = rail.IfOperator(
            task_id='if_request_workdayemployeetype_equals_to_parttime_20',
            test='''{{ dag_run.conf.workdayemployeetype == 'Part-Time' }}''',
            yes_task="log_fourth_set_timeoff_uris_23",
            no_task="if_log_fourth_set_timeoff_uris_23_blank_24",
        )

        log_fourth_set_timeoff_uris_23 = rail.PythonOperator(
            task_id='log_fourth_set_timeoff_uris_23',
            python_callable=lambda: [rail.find_first_by_attr_and_get_attr(rail.result(
                'adhoc_http_action_3'), 'displayText', '*Vacation: Part Time', 'uri', '')]
        )

        if_log_fourth_set_timeoff_uris_23_blank_24 = rail.IfOperator(
            task_id='if_log_fourth_set_timeoff_uris_23_blank_24',
            #pylint: disable = line-too-long
            test='''{{ result('log_fourth_set_timeoff_uris_23') | is_falsy  and result('log_third_set_timeoff_uris_18') | is_falsy  and result('log_second_set_timeoff_uris_14') | is_falsy }}''',
            yes_task="if_request_location_equals_to_ca_25",
            no_task="log_final_set_timeoff_uris_41",
        )

        if_request_location_equals_to_ca_25 = rail.IfOperator(
            task_id='if_request_location_equals_to_ca_25',
            test='''{{ dag_run.conf.location == 'CA'  and dag_run.conf.workdayemployeetype == 'Regular' }}''',
            yes_task="log_fifthtimeoff_uri_27",
            no_task="if_request_location_equals_to_ca_28",
        )

        log_fifthtimeoff_uri_27 = rail.PythonOperator(
            task_id='log_fifthtimeoff_uri_27',
            python_callable=lambda: [rail.find_first_by_attr_and_get_attr(
                rail.result('adhoc_http_action_3'), 'displayText', '*Vacation: CA', 'uri', '')]
        )

        if_request_location_equals_to_ca_28 = rail.IfOperator(
            task_id='if_request_location_equals_to_ca_28',
            test='''{{ dag_run.conf.location == 'CA'  and dag_run.conf.workdayemployeetype == 'Expatriate' }}''',
            yes_task="log_fifthtimeoff_uri_30",
            no_task="log_fifthtimeoff_uri_31",
        )

        log_fifthtimeoff_uri_30 = rail.PythonOperator(
            task_id='log_fifthtimeoff_uri_30',
            python_callable=lambda: [rail.find_first_by_attr_and_get_attr(
                rail.result('adhoc_http_action_3'), 'displayText', '*Vacation: CA', 'uri', '')]
        )

        log_fifthtimeoff_uri_31 = rail.PythonOperator(
            task_id='log_fifthtimeoff_uri_31',
            python_callable=lambda: rail.result(
                'log_fifthtimeoff_uri_27') or rail.result('log_fifthtimeoff_uri_30')
        )

        if_request_location_not_equals_to_ca_32 = rail.IfOperator(
            task_id='if_request_location_not_equals_to_ca_32',
            test='''{{ dag_run.conf.location != 'CA'  and dag_run.conf.workdayemployeetype == 'Regular' }}''',
            yes_task="log_sixth_check_timeoff_uri_34",
            no_task="if_request_location_not_equals_to_ca_35",
        )

        log_sixth_check_timeoff_uri_34 = rail.PythonOperator(
            task_id='log_sixth_check_timeoff_uri_34',
            python_callable=lambda: [rail.find_first_by_attr_and_get_attr(
                rail.result('adhoc_http_action_3'), 'displayText', "*Vacation", 'uri', '')]
        )

        if_request_location_not_equals_to_ca_35 = rail.IfOperator(
            task_id='if_request_location_not_equals_to_ca_35',
            test='''{{ dag_run.conf.location != 'CA'  and dag_run.conf.workdayemployeetype == 'Expatriate' }}''',
            yes_task="log_sixth_check_timeoff_uri_37",
            no_task="log_sixth_check_timeoff_uri_38",
        )

        log_sixth_check_timeoff_uri_37 = rail.PythonOperator(
            task_id='log_sixth_check_timeoff_uri_37',
            python_callable=lambda: [rail.find_first_by_attr_and_get_attr(
                rail.result('adhoc_http_action_3'), 'displayText', "*Vacation", 'uri', '')]
        )

        log_sixth_check_timeoff_uri_38 = rail.PythonOperator(
            task_id='log_sixth_check_timeoff_uri_38',
            python_callable=lambda:  rail.result(
                'log_sixth_check_timeoff_uri_34') or rail.result('log_sixth_check_timeoff_uri_37')
        )

        def get_final_uris_for_required_timeofftypes():
            all_uris = rail.result('get_first_set_of_uris') + (rail.result('log_second_set_timeoff_uris_14') if rail.result(
                'log_second_set_timeoff_uris_14') else []) + (rail.result('log_third_set_timeoff_uris_18') if rail.result(
                    'log_third_set_timeoff_uris_18') else []) + (rail.result('log_fourth_set_timeoff_uris_23') if rail.result(
                        'log_fourth_set_timeoff_uris_23') else []) + (rail.result('log_fifthtimeoff_uri_31') if rail.result(
                            'log_fifthtimeoff_uri_31') else []) + (rail.result('log_sixth_check_timeoff_uri_38') if rail.result(
                                'log_sixth_check_timeoff_uri_38') else [])
            return list({uri for uri in all_uris if uri != ''})

        log_final_set_timeoff_uris_41 = rail.PythonOperator(
            task_id='log_final_set_timeoff_uris_41',
            python_callable=get_final_uris_for_required_timeofftypes
        )

        if_log_12_present_42 = rail.IfOperator(
            task_id='if_log_12_present_42',
            test='''{{ result('log_final_set_timeoff_uris_41') | is_truthy }}''',
            yes_task="put_time_off_type_assignments_for_user_43",
            no_task="catch_error",
        )

        put_time_off_type_assignments_for_user_43 = rail.RepliconServiceOperator(
            task_id='put_time_off_type_assignments_for_user_43',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUris": rail.result('log_final_set_timeoff_uris_41')
            }
        )

        get_eligible_time_off_types_for_booking_time_off_44 = rail.RepliconServiceOperator(
            task_id='get_eligible_time_off_types_for_booking_time_off_44',
            endpoint="/services/TimeOffService1.svc/GetEligibleTimeOffTypesForBookingTimeOff",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        foreach_response_45 = rail.ForEachOperator(
            task_id='foreach_response_45',
            items="{{ result('get_eligible_time_off_types_for_booking_time_off_44') | to_json }}",
            start_task='get_default_time_off_type_policy_schedule_for_user_48',
            end_task='foreach_response_45_end'
        )

        get_default_time_off_type_policy_schedule_for_user_48 = rail.RepliconServiceOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user_48',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ result('foreach_response_45').uri }}"
                }
            }
        )

        if_effectivedate_day_present_50 = rail.IfOperator(
            task_id='if_effectivedate_day_present_50',
            test=lambda: bool(rail.result('get_default_time_off_type_policy_schedule_for_user_48') and rail.result(
                'get_default_time_off_type_policy_schedule_for_user_48')[0]['effectiveDate']['day']),
            yes_task="log_timeoff_policy_51",
            no_task="foreach_response_45_end",
        )

        log_timeoff_policy_51 = rail.PythonOperator(
            task_id='log_timeoff_policy_51',
            python_callable=lambda: json.loads((json.dumps(rail.result('get_default_time_off_type_policy_schedule_for_user_48'))).replace(
                'null', '\"effective\"').replace('\"script\"', '\"scriptTarget\"'))
        )

        put_user_time_off_account_policy_set_schedule_52 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_52',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run:{
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_response_45')['uri']
                },
                "policySetScheduleEntries": rail.result('log_timeoff_policy_51')
            }
        )

        foreach_response_45_end = rail.EmptyOperator(
            task_id='foreach_response_45_end',
        )

        catch_error = rail.EmptyOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error
        can_run_batch_task >> rail.Label('No') >> adhoc_http_action_3
        adhoc_http_action_3 >> if_first_displaytext_present_4
        if_first_displaytext_present_4 >> rail.Label(
            'Yes') >> sunovion_mapper_file_search_entries_5 >> get_first_set_of_uris >> if_request_workdayemployeetype_equals_to_inpatriate_11
        if_request_workdayemployeetype_equals_to_inpatriate_11 >> rail.Label(
            'Yes') >> log_second_set_timeoff_uris_14 >> if_request_workdayexecutive_equals_to_yes_15
        if_request_workdayemployeetype_equals_to_inpatriate_11 >> rail.Label(
            'No') >> if_request_workdayexecutive_equals_to_yes_15
        if_request_workdayexecutive_equals_to_yes_15 >> rail.Label(
            'Yes') >> log_third_set_timeoff_uris_18 >> if_log_9_blank_19
        if_request_workdayexecutive_equals_to_yes_15 >> rail.Label(
            'No') >> if_log_9_blank_19
        if_log_9_blank_19 >> rail.Label(
            'Yes') >> if_request_workdayemployeetype_equals_to_parttime_20
        if_request_workdayemployeetype_equals_to_parttime_20 >> rail.Label(
            'Yes') >> log_fourth_set_timeoff_uris_23 >> if_log_fourth_set_timeoff_uris_23_blank_24
        if_request_workdayemployeetype_equals_to_parttime_20 >> rail.Label(
            'No') >> if_log_fourth_set_timeoff_uris_23_blank_24
        if_log_9_blank_19 >> rail.Label(
            'No') >> if_log_fourth_set_timeoff_uris_23_blank_24
        if_log_fourth_set_timeoff_uris_23_blank_24 >> rail.Label(
            'Yes') >> if_request_location_equals_to_ca_25
        if_request_location_equals_to_ca_25 >> rail.Label(
            'Yes') >> log_fifthtimeoff_uri_27 >> if_request_location_equals_to_ca_28
        if_request_location_equals_to_ca_25 >> rail.Label(
            'No') >> if_request_location_equals_to_ca_28
        if_request_location_equals_to_ca_28 >> rail.Label(
            'Yes') >> log_fifthtimeoff_uri_30 >> log_fifthtimeoff_uri_31 >> if_request_location_not_equals_to_ca_32
        if_request_location_equals_to_ca_28 >> rail.Label(
            'No') >> log_fifthtimeoff_uri_31 >> if_request_location_not_equals_to_ca_32
        if_request_location_not_equals_to_ca_32 >> rail.Label(
            'Yes') >> log_sixth_check_timeoff_uri_34 >> if_request_location_not_equals_to_ca_35
        if_request_location_not_equals_to_ca_32 >> rail.Label(
            'No') >> if_request_location_not_equals_to_ca_35
        if_request_location_not_equals_to_ca_35 >> rail.Label(
            'Yes') >> log_sixth_check_timeoff_uri_37 >> log_sixth_check_timeoff_uri_38
        if_request_location_not_equals_to_ca_35 >> rail.Label(
            'No') >> log_sixth_check_timeoff_uri_38 >> log_final_set_timeoff_uris_41
        if_log_fourth_set_timeoff_uris_23_blank_24 >> rail.Label(
            'No') >> log_final_set_timeoff_uris_41 >> if_log_12_present_42
        if_log_12_present_42 >> rail.Label(
            'Yes') >> put_time_off_type_assignments_for_user_43 >> get_eligible_time_off_types_for_booking_time_off_44 >> foreach_response_45
        foreach_response_45 >> get_default_time_off_type_policy_schedule_for_user_48 >> if_effectivedate_day_present_50
        if_effectivedate_day_present_50 >> rail.Label(
            'Yes') >> log_timeoff_policy_51 >> put_user_time_off_account_policy_set_schedule_52 >> foreach_response_45_end
        if_effectivedate_day_present_50 >> rail.Label(
            'No') >> foreach_response_45_end
        foreach_response_45 >> foreach_response_45_end >> catch_error
        if_log_12_present_42 >> rail.Label('No') >> catch_error
        if_first_displaytext_present_4 >> rail.Label(
            'No') >> catch_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
