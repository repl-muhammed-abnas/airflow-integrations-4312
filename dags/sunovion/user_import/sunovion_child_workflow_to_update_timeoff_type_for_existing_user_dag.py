
from datetime import timedelta
import json
from airflow.models import Variable
import rail
from sunovion.user_import.mappers.sunovion_mapper_file import sunovion_mapper

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'sunovion_user_import_workflow_to_update_timeoff_type_for_existing_user_child_{config.instance}',
        description=f'Live|Sunovion_Child Workflow to update timeoff type for existing user {config.instance}',
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
            no_task='log_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_3',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_3 = rail.PythonOperator(
            task_id='log_3',
            python_callable=lambda dag_run: (
                (dag_run.conf['useruri']).split(':'))[-1]
        )

        get_timeoffbalance_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_timeoffbalance_report_details',
            report_name=config.timeoff_balance_report
        )

        run_timeoffbalance_report = rail.run_report2(
            group_id="run_timeoffbalance_report",
            report_params=lambda: {
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_timeoffbalance_report_details')['uri'],
                        "filterValues": [
                            {
                                "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result(
                                    'get_timeoffbalance_report_details')['filterConfiguration']['enabledFilters'], 'displayText', 'UserFilter', 'uri', ''),
                                "value": rail.result('log_3'),
                            }
                        ],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv",
                    }
                ]
            },
            target='artifact'
        )

        parse_csv_5_5_5 = rail.LoadCSVFileOperator(
            task_id='parse_csv_5_5_5',
            document="{{(result('run_timeoffbalance_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload}}"
        )

        load_timeoffbalance_report = rail.PythonOperator(
            task_id='load_timeoffbalance_report',
            python_callable=lambda: rail.load_all_records(
                rail.result('parse_csv_5_5_5'))
        )

        adhoc_http_action_6 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_6',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        declare_child_waitlist = rail.SetVariableOperator(
            task_id='declare_child_waitlist',
            name='childwaitlist',
            append=False,
            value=[]
        )

        foreach_timeoff_type_policy = rail.ForEachOperator(
            task_id='foreach_timeoff_type_policy',
            items=lambda: rail.result('adhoc_http_action_6')[
                'policiesByTimeOffType'],
            start_task='log_policyschedule_8',
            end_task='foreach_timeoff_type_policy_end'
        )

        log_policyschedule_8 = rail.PythonOperator(
            task_id='log_policyschedule_8',
            python_callable=lambda: rail.result('foreach_timeoff_type_policy')[
                'policySetSchedule']
        )

        log_timeoff_balanceforrequiredtimeoff_type_9 = rail.PythonOperator(
            task_id='log_timeoff_balanceforrequiredtimeoff_type_9',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'load_timeoffbalance_report'), 'Timeoff URI', rail.result('foreach_timeoff_type_policy')['timeOffType']['uri'], 'Time Off Balance', '')
        )

        if_log_15_present_10 = rail.IfOperator(
            task_id='if_log_15_present_10',
            test='''{{ result('log_policyschedule_8') | is_truthy }}''',
            yes_task="trigger_dag_run_sunovion_user_import_sunovion_child_for_timeoff_policy_update_on_each_time_off_type11",
            no_task="foreach_timeoff_type_policy_end",
        )

        trigger_dag_run_sunovion_user_import_sunovion_child_for_timeoff_policy_update_on_each_time_off_type11 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_sunovion_user_import_sunovion_child_for_timeoff_policy_update_on_each_time_off_type11',
            retries=0,
            trigger_dag_id=f'sunovion_user_import_child_for_timeoff_policy_update_on_each_time_off_type_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "callerjobid": dag_run.conf['callerjobid'],
                "useruri": dag_run.conf['useruri'],
                "timeoffuri": rail.result('foreach_timeoff_type_policy')['timeOffType']['uri'],
                "policyset": json.dumps(rail.result('log_policyschedule_8')),
                "newschedulebalance": rail.result('log_timeoff_balanceforrequiredtimeoff_type_9') if rail.result(
                    'log_timeoff_balanceforrequiredtimeoff_type_9') else 0.00
            }
        )

        insert_child_to_waitlist = rail.SetVariableOperator(
            task_id='insert_child_to_waitlist',
            name='childwaitlist',
            append=True,
            value="{{result('trigger_dag_run_sunovion_user_import_sunovion_child_for_timeoff_policy_update_on_each_time_off_type11')}}"
        )

        foreach_timeoff_type_policy_end = rail.EmptyOperator(
            task_id='foreach_timeoff_type_policy_end',
        )

        if_timeoff_policyupdate_child_triggered = rail.IfOperator(
            task_id = 'if_timeoff_policyupdate_child_triggered',
            test=lambda: bool(rail.get_dag_run_var('childwaitlist')),
            yes_task='wait_for_timeoff_policy_update_on_each_timeoff_type',
            no_task='adhoc_http_action_15'
        )

        wait_for_timeoff_policy_update_on_each_timeoff_type = rail.WaitForDagRunsSensor(
            task_id='wait_for_timeoff_policy_update_on_each_timeoff_type',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("insert_child_to_waitlist").value | to_json }}'
        )

        adhoc_http_action_15 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_15',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
        )

        create_enabled_timeoffs_list = rail.SetVariableOperator(
            task_id='create_enabled_timeoffs_list',
            name='enabledtimeoffslist',
            append=False,
            value=[]
        )

        foreach_enabled_timeoff_type = rail.ForEachOperator(
            task_id='foreach_enabled_timeoff_type',
            items=lambda: rail.result('adhoc_http_action_15'),
            start_task='adhoc_http_action_17',
            end_task='foreach_enabled_timeoff_type_end'
        )

        adhoc_http_action_17 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_17',
            endpoint="/services/TimeOffService1.svc/GetTimeOffTypeDetails",
            data={
                "timeOffTypeUri": "{{result('foreach_enabled_timeoff_type').uri}}"
            }
        )

        accumulate_list_items_18_18_18 = rail.SetVariableOperator(
            task_id='accumulate_list_items_18_18_18',
            name='enabledtimeoffslist',
            append=True,
            value={
                "displayText": "{{result('adhoc_http_action_17').displayText}}",
                "description": "{{result('adhoc_http_action_17').description}}",
                "enabled": "{{result('adhoc_http_action_17').enabled}}",
                "uri": "{{result('adhoc_http_action_17').uri}}"
            }
        )

        foreach_enabled_timeoff_type_end = rail.EmptyOperator(
            task_id='foreach_enabled_timeoff_type_end',
        )

        if_first_displaytext_present_19 = rail.IfOperator(
            task_id='if_first_displaytext_present_19',
            test=lambda: bool(rail.get_dag_run_var('enabledtimeoffslist')),
            yes_task="sunovion_mapper_file_search_entries_20",
            no_task="finish",
        )

        sunovion_mapper_file_search_entries_20 = rail.PythonOperator(
            task_id='sunovion_mapper_file_search_entries_20',
            python_callable=lambda dag_run:  list(filter(
                lambda x: x["type"] == "timeoff type" and x["identifier_1"] == dag_run.conf['employeetype'], sunovion_mapper))
        )

        def get_timeofftype_uris():
            mapper_entries = rail.result(
                'sunovion_mapper_file_search_entries_20')
            timeoffs = [entry['data_set'] for entry in mapper_entries]
            enabled_timeofftypes = rail.get_dag_run_var('enabledtimeoffslist')
            return [rail.find_first_by_attr_and_get_attr(enabled_timeofftypes, 'displayText', timeoff, 'uri', '') for timeoff in timeoffs]

        log_first_set_timeoff_uris_25 = rail.PythonOperator(
            task_id='log_first_set_timeoff_uris_25',
            python_callable=get_timeofftype_uris
        )

        if_request_workdayemployeetype_equals_to_inpatriate_27 = rail.IfOperator(
            task_id='if_request_workdayemployeetype_equals_to_inpatriate_27',
            test='''{{ dag_run.conf.workdayemployeetype == 'Inpatriate' }}''',
            yes_task="log_second_set_timeoff_uris_30",
            no_task="if_request_workdayexecutive_equals_to_yes_31",
        )

        log_second_set_timeoff_uris_30 = rail.PythonOperator(
            task_id='log_second_set_timeoff_uris_30',
            python_callable=lambda: [rail.find_first_by_attr_and_get_attr(rail.result(
                'adhoc_http_action_3'), 'displayText', "*Vacation: In-Pat", 'uri', '')]
        )

        if_request_workdayexecutive_equals_to_yes_31 = rail.IfOperator(
            task_id='if_request_workdayexecutive_equals_to_yes_31',
            test='''{{ dag_run.conf.workdayexecutive == 'Yes'  and result('log_second_set_timeoff_uris_30') | is_falsy }}''',
            yes_task="log_third_set_timeoff_uris_34",
            no_task="if_log_11_blank_35",
        )

        log_third_set_timeoff_uris_34 = rail.PythonOperator(
            task_id='log_third_set_timeoff_uris_34',
            python_callable=lambda: [rail.find_first_by_attr_and_get_attr(
                rail.result('adhoc_http_action_3'), 'displayText', '*Vacation: ELT', 'uri', '')]
        )

        if_log_11_blank_35 = rail.IfOperator(
            task_id='if_log_11_blank_35',
            test='''{{ result('log_third_set_timeoff_uris_34') | is_falsy  and result('log_second_set_timeoff_uris_30') | is_falsy }}''',
            yes_task="if_request_workdayemployeetype_equals_to_parttime_36",
            no_task="if_log_fourth_set_timeoff_uris_39_blank_40",
        )

        if_request_workdayemployeetype_equals_to_parttime_36 = rail.IfOperator(
            task_id='if_request_workdayemployeetype_equals_to_parttime_36',
            test='''{{ dag_run.conf.workdayemployeetype == 'Part-Time' }}''',
            yes_task="log_fourth_set_timeoff_uris_39",
            no_task="if_log_fourth_set_timeoff_uris_39_blank_40",
        )

        log_fourth_set_timeoff_uris_39 = rail.PythonOperator(
            task_id='log_fourth_set_timeoff_uris_39',
            python_callable=lambda: [rail.find_first_by_attr_and_get_attr(rail.result(
                'adhoc_http_action_3'), 'displayText', '*Vacation: Part Time', 'uri', '')]
        )

        if_log_fourth_set_timeoff_uris_39_blank_40 = rail.IfOperator(
            task_id='if_log_fourth_set_timeoff_uris_39_blank_40',
            #pylint: disable = line-too-long
            test='''{{ result('log_fourth_set_timeoff_uris_39') | is_falsy  and result('log_second_set_timeoff_uris_30') | is_falsy  and result('log_third_set_timeoff_uris_34') | is_falsy }}''',
            yes_task="if_request_residentstate_equals_to_ca_41",
            no_task="log_final_set_timeoff_uris_57",
        )

        if_request_residentstate_equals_to_ca_41 = rail.IfOperator(
            task_id='if_request_residentstate_equals_to_ca_41',
            test='''{{ dag_run.conf.residentstate == 'CA'  and dag_run.conf.workdayemployeetype == 'Regular' }}''',
            yes_task="log_fifth_check_43",
            no_task="if_request_residentstate_equals_to_ca_44",
        )

        log_fifth_check_43 = rail.PythonOperator(
            task_id='log_fifth_check_43',
            python_callable=lambda: [rail.find_first_by_attr_and_get_attr(
                rail.result('adhoc_http_action_3'), 'displayText', '*Vacation: CA', 'uri', '')]
        )

        if_request_residentstate_equals_to_ca_44 = rail.IfOperator(
            task_id='if_request_residentstate_equals_to_ca_44',
            test='''{{ dag_run.conf.residentstate == 'CA'  and dag_run.conf.workdayemployeetype == 'Expatriate' }}''',
            yes_task="log_fifth_check_46",
            no_task="log_fifth_check_47",
        )

        log_fifth_check_46 = rail.PythonOperator(
            task_id='log_fifth_check_46',
            python_callable=lambda: lambda: [rail.find_first_by_attr_and_get_attr(
                rail.result('adhoc_http_action_3'), 'displayText', '*Vacation: CA', 'uri', '')]
        )

        log_fifth_check_47 = rail.PythonOperator(
            task_id='log_fifth_check_47',
            python_callable=lambda: rail.result(
                'log_fifth_check_43') or rail.result('log_fifth_check_46')
        )

        if_request_residentstate_not_equals_to_ca_48 = rail.IfOperator(
            task_id='if_request_residentstate_not_equals_to_ca_48',
            test='''{{ dag_run.conf.residentstate != 'CA'  and dag_run.conf.workdayemployeetype == 'Regular' }}''',
            yes_task="log_sixth_check_50",
            no_task="if_request_residentstate_not_equals_to_ca_51",
        )

        log_sixth_check_50 = rail.PythonOperator(
            task_id='log_sixth_check_50',
            python_callable=lambda: [rail.find_first_by_attr_and_get_attr(
                rail.result('adhoc_http_action_3'), 'displayText', "*Vacation", 'uri', '')]
        )

        if_request_residentstate_not_equals_to_ca_51 = rail.IfOperator(
            task_id='if_request_residentstate_not_equals_to_ca_51',
            test='''{{ dag_run.conf.residentstate != 'CA'  and dag_run.conf.workdayemployeetype == 'Expatriate' }}''',
            yes_task="log_sixth_check_53",
            no_task="log_sixth_check_54",
        )

        log_sixth_check_53 = rail.PythonOperator(
            task_id='log_sixth_check_53',
            python_callable=lambda: [rail.find_first_by_attr_and_get_attr(
                rail.result('adhoc_http_action_3'), 'displayText', "*Vacation", 'uri', '')]
        )

        log_sixth_check_54 = rail.PythonOperator(
            task_id='log_sixth_check_54',
            python_callable=lambda:  rail.result(
                'log_sixth_check_50') or rail.result('log_sixth_check_53')
        )

        def get_final_uris_for_required_timeofftypes():
            all_uris = rail.result('log_first_set_timeoff_uris_25') + (rail.result('log_second_set_timeoff_uris_30') if rail.result(
                'log_second_set_timeoff_uris_30') else []) + (rail.result('log_third_set_timeoff_uris_34') if rail.result(
                    'log_third_set_timeoff_uris_34') else []) + (rail.result('log_fourth_set_timeoff_uris_39') if rail.result(
                        'log_fourth_set_timeoff_uris_39') else []) + (rail.result('log_fifth_check_47') if rail.result(
                            'log_fifth_check_47') else []) + (rail.result('log_sixth_check_54') if rail.result(
                                'log_sixth_check_54') else [])
            return list({uri for uri in all_uris if uri != ''})

        log_final_set_timeoff_uris_57 = rail.PythonOperator(
            task_id='log_final_set_timeoff_uris_57',
            python_callable=get_final_uris_for_required_timeofftypes
        )

        if_log_12_present_58 = rail.IfOperator(
            task_id='if_log_12_present_58',
            test='''{{ result('log_final_set_timeoff_uris_57') | is_truthy }}''',
            yes_task="put_time_off_type_assignments_for_user_59",
            no_task="finish",
        )

        put_time_off_type_assignments_for_user_59 = rail.RepliconServiceOperator(
            task_id='put_time_off_type_assignments_for_user_59',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUris": rail.result('log_final_set_timeoff_uris_57')
            }
        )

        get_eligible_time_off_types_for_booking_time_off_60 = rail.RepliconServiceOperator(
            task_id='get_eligible_time_off_types_for_booking_time_off_60',
            endpoint="/services/TimeOffService1.svc/GetEligibleTimeOffTypesForBookingTimeOff",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        declare_child_list_for_updating_timeoff_policy_with_vacation_balance = rail.SetVariableOperator(
            task_id='declare_child_list_for_updating_timeoff_policy_with_vacation_balance',
            name='waitlistchild',
            append=False,
            value=[]
        )

        foreach_eligible_timeoff_type_for_booking = rail.ForEachOperator(
            task_id='foreach_eligible_timeoff_type_for_booking',
            items="{{ result('get_eligible_time_off_types_for_booking_time_off_60') | to_json }}",
            start_task='get_default_time_off_type_policy_schedule_for_user_64',
            end_task='foreach_eligible_timeoff_type_for_booking_end'
        )

        get_default_time_off_type_policy_schedule_for_user_64 = rail.RepliconServiceOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user_64',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{result('foreach_eligible_timeoff_type_for_booking').uri}}"
                }
            }
        )

        if_effectivedate_day_present_67 = rail.IfOperator(
            task_id='if_effectivedate_day_present_67',
            test=lambda: bool(rail.result('get_default_time_off_type_policy_schedule_for_user_64') and rail.result(
                'get_default_time_off_type_policy_schedule_for_user_64')[0]['effectiveDate']['day']),
            yes_task="if_foreach__adhoc_http_action_16_adhoc_http_action_15_16_1_displaytext_not_contains_vacation_68",
            no_task="foreach_eligible_timeoff_type_for_booking_end",
        )

        if_foreach__adhoc_http_action_16_adhoc_http_action_15_16_1_displaytext_not_contains_vacation_68 = rail.IfOperator(
            task_id='if_foreach__adhoc_http_action_16_adhoc_http_action_15_16_1_displaytext_not_contains_vacation_68',
            test=lambda: 'Vacation' not in rail.result(
                'foreach_eligible_timeoff_type_for_booking')['displyText'],
            yes_task="log_timeoff_policy_69",
            no_task="if_foreach__adhoc_http_action_16_adhoc_http_action_15_16_1_displaytext_contains_vacation_71",
        )

        log_timeoff_policy_69 = rail.PythonOperator(
            task_id='log_timeoff_policy_69',
            python_callable=lambda: json.loads((json.dumps(rail.result('get_default_time_off_type_policy_schedule_for_user_64'))).replace(
                'null', '\"effective\"').replace('\"script\"', '\"scriptTarget\"'))
        )

        put_user_time_off_account_policy_set_schedule_70 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_70',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run:{
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_eligible_timeoff_type_for_booking')['uri']
                },
                "policySetScheduleEntries": rail.result('log_timeoff_policy_69')
            }
        )

        if_foreach__adhoc_http_action_16_adhoc_http_action_15_16_1_displaytext_contains_vacation_71 = rail.IfOperator(
            task_id='if_foreach__adhoc_http_action_16_adhoc_http_action_15_16_1_displaytext_contains_vacation_71',
            test=lambda: 'Vacation' in rail.result(
                'foreach_eligible_timeoff_type_for_booking')['displyText'],
            yes_task="if_first_column_2_present_72",
            no_task="foreach_eligible_timeoff_type_for_booking_end",
        )

        if_first_column_2_present_72 = rail.IfOperator(
            task_id='if_first_column_2_present_72',
            test='''{{ result('load_timeoffbalance_report') | is_truthy }}''',
            yes_task="log_nameofthe_vacationtypetimeoff_75",
            no_task="if_log_nameofthe_vacationtypetimeoff_75_blank_78",
        )

        def get_existing_vacation_timeofftype_details():
            existing_timeoff = rail.result('load_timeoffbalance_report')
            return [{
                'name': rail.result('foreach_existing_timeoff')['timeOffType'],
                'balance': rail.result('foreach_existing_timeoff')['timeOffBalance'],
                'uri': rail.result('foreach_existing_timeoff')['timeOffUri']
            } for timeoff in existing_timeoff if 'Vacation' in timeoff]

        log_nameofthe_vacationtypetimeoff_75 = rail.PythonOperator(
            task_id='log_nameofthe_vacationtypetimeoff_75',
            python_callable=get_existing_vacation_timeofftype_details
        )

        if_log_nameofthe_vacationtypetimeoff_75_blank_78 = rail.IfOperator(
            task_id='if_log_nameofthe_vacationtypetimeoff_75_blank_78',
            test='''{{ result('log_nameofthe_vacationtypetimeoff_75') | is_falsy }}''',
            yes_task="log_timeoff_policy_79",
            no_task="if_log_nameofthe_vacationtypetimeoff_75_present_81",
        )

        log_timeoff_policy_79 = rail.PythonOperator(
            task_id='log_timeoff_policy_79',
            python_callable=lambda: json.loads((json.dumps(rail.result('get_default_time_off_type_policy_schedule_for_user_64'))).replace(
                'null', '\"effective\"').replace('\"script\"', '\"scriptTarget\"'))
        )

        put_user_time_off_account_policy_set_schedule_80 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_80',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run:{
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_eligible_timeoff_type_for_booking')['uri']
                },
                "policySetScheduleEntries": rail.result('log_timeoff_policy_79')
            }
        )

        if_log_nameofthe_vacationtypetimeoff_75_present_81 = rail.IfOperator(
            task_id='if_log_nameofthe_vacationtypetimeoff_75_present_81',
            test='''{{ result('log_nameofthe_vacationtypetimeoff_75') | is_truthy }}''',
            yes_task="trigger_child_to_update_timeoff_policy_with_exisiting_vacation_balance",
            no_task="foreach_eligible_timeoff_type_for_booking_end",
        )

        trigger_child_to_update_timeoff_policy_with_exisiting_vacation_balance = rail.TriggerDagRunOperator(
            task_id='trigger_child_to_update_timeoff_policy_with_exisiting_vacation_balance',
            retries=0,
            trigger_dag_id=f'sunovion_user_import_child_workflow_to_update_timeoff_policy_with_existing_vacation_balance_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "callerjobid": dag_run.conf['callerjobid'],
                "username": dag_run.conf['username'],
                "userloginname": dag_run.conf['userloginname'],
                "useruri": dag_run.conf['useruri'],
                "timeofftypename": rail.result('log_nameofthe_vacationtypetimeoff_75')['name'],
                "timeofftypebalance": rail.result('log_nameofthe_vacationtypetimeoff_75')['balance'],
                "timeofftypeuri": rail.result('log_nameofthe_vacationtypetimeoff_75')['uri'],
                "newtimeofftypeuri": rail.result('foreach_eligible_timeoff_type_for_booking')['uri'],
                "newtimeofftypename": rail.result('foreach_eligible_timeoff_type_for_booking')['displayText'],
            }
        )

        insert_to_wait_list_for_child = rail.SetVariableOperator(
            task_id='insert_to_wait_list_for_child',
            name='waitlistchild',
            append=True,
            value="{{result('trigger_child_to_update_timeoff_policy_with_exisiting_vacation_balance')}}"
        )

        foreach_eligible_timeoff_type_for_booking_end = rail.EmptyOperator(
            task_id='foreach_eligible_timeoff_type_for_booking_end',
        )

        if_child_for_updating_existing_vacation_balance_triggered = rail.IfOperator(
            task_id = 'if_child_for_updating_existing_vacation_balance_triggered',
            test=lambda: bool(rail.get_dag_run_var('waitlistchild')),
            yes_task='wait_for_child_to_update_timeoff_policy_with_exisiting_vacation_balance',
            no_task='finish'
        )

        wait_for_child_to_update_timeoff_policy_with_exisiting_vacation_balance = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_to_update_timeoff_policy_with_exisiting_vacation_balance',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('insert_to_wait_list_for_child').value | to_json }}"
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> log_3
        log_3 >> get_timeoffbalance_report_details >> run_timeoffbalance_report >> parse_csv_5_5_5 >> load_timeoffbalance_report
        load_timeoffbalance_report >> adhoc_http_action_6 >> declare_child_waitlist >> foreach_timeoff_type_policy >> log_policyschedule_8
        log_policyschedule_8 >> log_timeoff_balanceforrequiredtimeoff_type_9 >> if_log_15_present_10
        if_log_15_present_10 >> rail.Label(
            'Yes') >> trigger_dag_run_sunovion_user_import_sunovion_child_for_timeoff_policy_update_on_each_time_off_type11 >> insert_child_to_waitlist
        insert_child_to_waitlist >> foreach_timeoff_type_policy_end
        if_log_15_present_10 >> rail.Label(
            'No') >> foreach_timeoff_type_policy_end
        foreach_timeoff_type_policy >> foreach_timeoff_type_policy_end >> if_timeoff_policyupdate_child_triggered
        if_timeoff_policyupdate_child_triggered >> rail.Label('Yes') >> wait_for_timeoff_policy_update_on_each_timeoff_type >> adhoc_http_action_15
        if_timeoff_policyupdate_child_triggered >> rail.Label('No') >> adhoc_http_action_15 >> create_enabled_timeoffs_list
        create_enabled_timeoffs_list >> foreach_enabled_timeoff_type >> adhoc_http_action_17 >> accumulate_list_items_18_18_18
        accumulate_list_items_18_18_18 >> foreach_enabled_timeoff_type_end
        foreach_enabled_timeoff_type >> foreach_enabled_timeoff_type_end >> if_first_displaytext_present_19
        if_first_displaytext_present_19 >> rail.Label(
            'Yes') >> sunovion_mapper_file_search_entries_20 >> log_first_set_timeoff_uris_25 >> if_request_workdayemployeetype_equals_to_inpatriate_27
        if_request_workdayemployeetype_equals_to_inpatriate_27 >> rail.Label(
            'Yes') >> log_second_set_timeoff_uris_30 >> if_request_workdayexecutive_equals_to_yes_31
        if_request_workdayemployeetype_equals_to_inpatriate_27 >> rail.Label(
            'No') >> if_request_workdayexecutive_equals_to_yes_31
        if_request_workdayexecutive_equals_to_yes_31 >> rail.Label(
            'Yes') >> log_third_set_timeoff_uris_34 >> if_log_11_blank_35
        if_request_workdayexecutive_equals_to_yes_31 >> rail.Label(
            'No') >> if_log_11_blank_35
        if_log_11_blank_35 >> rail.Label(
            'Yes') >> if_request_workdayemployeetype_equals_to_parttime_36
        if_request_workdayemployeetype_equals_to_parttime_36 >> rail.Label(
            'Yes') >> log_fourth_set_timeoff_uris_39 >> if_log_fourth_set_timeoff_uris_39_blank_40
        if_request_workdayemployeetype_equals_to_parttime_36 >> rail.Label(
            'No') >> if_log_fourth_set_timeoff_uris_39_blank_40
        if_log_11_blank_35 >> rail.Label(
            'No') >> if_log_fourth_set_timeoff_uris_39_blank_40
        if_log_fourth_set_timeoff_uris_39_blank_40 >> rail.Label(
            'Yes') >> if_request_residentstate_equals_to_ca_41
        if_request_residentstate_equals_to_ca_41 >> rail.Label(
            'Yes') >> log_fifth_check_43 >> if_request_residentstate_equals_to_ca_44
        if_request_residentstate_equals_to_ca_41 >> rail.Label(
            'No') >> if_request_residentstate_equals_to_ca_44
        if_request_residentstate_equals_to_ca_44 >> rail.Label(
            'Yes') >> log_fifth_check_46 >> log_fifth_check_47
        if_request_residentstate_equals_to_ca_44 >> rail.Label(
            'No') >> log_fifth_check_47 >> if_request_residentstate_not_equals_to_ca_48
        if_request_residentstate_not_equals_to_ca_48 >> rail.Label(
            'Yes') >> log_sixth_check_50 >> if_request_residentstate_not_equals_to_ca_51
        if_request_residentstate_not_equals_to_ca_48 >> rail.Label(
            'No') >> if_request_residentstate_not_equals_to_ca_51
        if_request_residentstate_not_equals_to_ca_51 >> rail.Label(
            'Yes') >> log_sixth_check_53 >> log_sixth_check_54
        if_request_residentstate_not_equals_to_ca_51 >> rail.Label(
            'No') >> log_sixth_check_54 >> log_final_set_timeoff_uris_57
        if_log_fourth_set_timeoff_uris_39_blank_40 >> rail.Label(
            'No') >> log_final_set_timeoff_uris_57 >> if_log_12_present_58
        if_log_12_present_58 >> rail.Label(
            'Yes') >> put_time_off_type_assignments_for_user_59 >> get_eligible_time_off_types_for_booking_time_off_60
        get_eligible_time_off_types_for_booking_time_off_60 >> declare_child_list_for_updating_timeoff_policy_with_vacation_balance
        declare_child_list_for_updating_timeoff_policy_with_vacation_balance >> foreach_eligible_timeoff_type_for_booking
        foreach_eligible_timeoff_type_for_booking >> get_default_time_off_type_policy_schedule_for_user_64 >> if_effectivedate_day_present_67
        if_effectivedate_day_present_67 >> rail.Label(
            'Yes') >> if_foreach__adhoc_http_action_16_adhoc_http_action_15_16_1_displaytext_not_contains_vacation_68
        if_foreach__adhoc_http_action_16_adhoc_http_action_15_16_1_displaytext_not_contains_vacation_68 >> rail.Label(
            'Yes') >> log_timeoff_policy_69 >> put_user_time_off_account_policy_set_schedule_70
        put_user_time_off_account_policy_set_schedule_70 >> if_foreach__adhoc_http_action_16_adhoc_http_action_15_16_1_displaytext_contains_vacation_71
        if_foreach__adhoc_http_action_16_adhoc_http_action_15_16_1_displaytext_not_contains_vacation_68 >> rail.Label(
            'No') >> if_foreach__adhoc_http_action_16_adhoc_http_action_15_16_1_displaytext_contains_vacation_71
        if_foreach__adhoc_http_action_16_adhoc_http_action_15_16_1_displaytext_contains_vacation_71 >> rail.Label(
            'Yes') >> if_first_column_2_present_72
        if_first_column_2_present_72 >> rail.Label(
            'Yes') >> log_nameofthe_vacationtypetimeoff_75 >> if_log_nameofthe_vacationtypetimeoff_75_blank_78
        if_first_column_2_present_72 >> rail.Label(
            'No') >> if_log_nameofthe_vacationtypetimeoff_75_blank_78
        if_log_nameofthe_vacationtypetimeoff_75_blank_78 >> rail.Label(
            'Yes') >> log_timeoff_policy_79 >> put_user_time_off_account_policy_set_schedule_80 >> if_log_nameofthe_vacationtypetimeoff_75_present_81
        if_log_nameofthe_vacationtypetimeoff_75_blank_78 >> rail.Label(
            'No') >> if_log_nameofthe_vacationtypetimeoff_75_present_81
        if_log_nameofthe_vacationtypetimeoff_75_present_81 >> rail.Label(
            'Yes') >> trigger_child_to_update_timeoff_policy_with_exisiting_vacation_balance >> insert_to_wait_list_for_child
        insert_to_wait_list_for_child >> foreach_eligible_timeoff_type_for_booking_end
        if_log_nameofthe_vacationtypetimeoff_75_present_81 >> rail.Label(
            'No') >> foreach_eligible_timeoff_type_for_booking_end
        if_foreach__adhoc_http_action_16_adhoc_http_action_15_16_1_displaytext_contains_vacation_71 >> rail.Label(
            'No') >> foreach_eligible_timeoff_type_for_booking_end
        if_effectivedate_day_present_67 >> rail.Label(
            'No') >> foreach_eligible_timeoff_type_for_booking_end
        foreach_eligible_timeoff_type_for_booking >> foreach_eligible_timeoff_type_for_booking_end
        foreach_eligible_timeoff_type_for_booking_end >> if_child_for_updating_existing_vacation_balance_triggered
        if_child_for_updating_existing_vacation_balance_triggered >> rail.Label(
            'Yes') >> wait_for_child_to_update_timeoff_policy_with_exisiting_vacation_balance >> finish
        if_child_for_updating_existing_vacation_balance_triggered >> rail.Label('No') >> finish
        if_log_12_present_58 >> rail.Label('No') >> finish
        if_first_displaytext_present_19 >> rail.Label(
            'No') >> finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
