
from datetime import timedelta
import json
from airflow.models import Variable
from ge_healthcare.user_sync_denmark.denmark_master_mapper import denmark_master_mapper
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'gehealthcare_user_sync_denmark_ge_denmark_child_workflow_to_add_timeoff_type_for_lm_wind_users_v1_0_{config.instance}',
        description=f'GE_Denmark_Child Workflow to add timeoff type for LM wind users v1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
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
            no_task='ge_denmark_user_sync_master_mapper_v2_0_search_entries_timeoffto_assign_search_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='ge_denmark_user_sync_master_mapper_v2_0_search_entries_timeoffto_assign_search_3',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def get_entity_from_mapper(dag_run):
            overtime_eligibility = dag_run.conf['OvertimeEligibility'] if dag_run.conf[
                'LegalEntity'] == 'RE1018' or dag_run.conf['LegalEntity'] == 'RE1014' else ""
            mapper_timeoffs = list(filter(lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type']
                                   == 'Timeoff types' and x['overtime_eligibility'] == overtime_eligibility, denmark_master_mapper))
            return [mapper_timeoff['value'] for mapper_timeoff in mapper_timeoffs] if mapper_timeoffs else []

        ge_denmark_user_sync_master_mapper_v2_0_search_entries_timeoffto_assign_search_3 = rail.PythonOperator(
            task_id='ge_denmark_user_sync_master_mapper_v2_0_search_entries_timeoffto_assign_search_3',
            python_callable=get_entity_from_mapper
        )

        if_first_id_blank_4 = rail.IfOperator(
            task_id='if_first_id_blank_4',
            test='''{{ result('ge_denmark_user_sync_master_mapper_v2_0_search_entries_timeoffto_assign_search_3') | is_falsy }}''',
            yes_task="log_to_sumo",
            no_task="_adhoc_http_action_6",
        )

        _adhoc_http_action_6 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_6',
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
            data=None
        )

        def get_timeoff_infos():
            timeoff_infos = []
            for mapper_to_info in rail.result('ge_denmark_user_sync_master_mapper_v2_0_search_entries_timeoffto_assign_search_3'):
                timeoff_uri = rail.find_first_by_attr_and_get_attr(rail.result(
                    '_adhoc_http_action_6'), 'displayText', mapper_to_info, 'uri')
                if timeoff_uri:
                    timeoff_infos.append(
                        {"uri": timeoff_uri, "name": mapper_to_info})
            return timeoff_infos

        invoke_custom_ruby_code_8 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_8',
            python_callable=get_timeoff_infos
        )

        if_log_time_off_typeto_assign_9_blank_10 = rail.IfOperator(
            task_id='if_log_time_off_typeto_assign_9_blank_10',
            test='''{{ result('invoke_custom_ruby_code_8') | is_falsy }}''',
            yes_task="log_to_sumo",
            no_task="get_user_time_off_type_policy_summary_12",
        )

        get_user_time_off_type_policy_summary_12 = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary_12',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        def get_parsed_timeoff_type_policy_summary():
            parsed_timeoff_type_policy_summary = []
            user_timeOff_type_policy_summary = rail.result(
                'get_user_time_off_type_policy_summary_12')
            user_timeOff_type_policy_summary_arr = user_timeOff_type_policy_summary[
                'policiesByTimeOffType'] if user_timeOff_type_policy_summary else []
            for user_time_type_policy in user_timeOff_type_policy_summary_arr:
                print("user_time_type_policy", user_time_type_policy)
                parsed_timeoff_type_policy_summary.append({
                    "name": user_time_type_policy['timeOffType']['name'],
                    "enabled": user_time_type_policy['isTimeOffAllowedAgainstThisTimeOffType'],
                    "uri": user_time_type_policy['timeOffType']['uri'],
                    "policy": user_time_type_policy['policySetSchedule'][0]['effectiveDate']['day'] if user_time_type_policy['policySetSchedule'] else None,
                })
            return parsed_timeoff_type_policy_summary

        invoke_custom_ruby_code_13 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_13',
            python_callable=get_parsed_timeoff_type_policy_summary
        )

        def get_timeoff_type_policy_summary(timeoff_uri):
            return list(filter(lambda x: x['uri'] == timeoff_uri, rail.result('invoke_custom_ruby_code_8')))

        def get_existing_timeoff_type_policy_summary(timeoff_uri):
            return list(filter(lambda x: x['uri'] == timeoff_uri, rail.result('invoke_custom_ruby_code_13')))

        def get_unique_timeoff_uri():
            timeoff_uris = []
            timeoff_infos = rail.result('invoke_custom_ruby_code_8')
            for to_info in timeoff_infos:
                if to_info['uri'] and to_info['uri'] not in timeoff_uris:
                    timeoff_uris.append(to_info['uri'])
            return timeoff_uris

        def get_timeoff_to_be_assigned():
            timeoff_to_be_assigned = []
            timeoff_uris = get_unique_timeoff_uri()
            for timeoff_uri in timeoff_uris:
                mapper_policy_summary = get_timeoff_type_policy_summary(
                    timeoff_uri)
                existing_policy_summary = get_existing_timeoff_type_policy_summary(
                    timeoff_uri)
                to_status = "Yes" if existing_policy_summary and existing_policy_summary[
                    0]['name'] else "No"
                if to_status == "No":
                    timeoff_to_be_assigned.append({
                        "name": mapper_policy_summary[0]['name'],
                        "enabled": existing_policy_summary[0]['enabled'] if existing_policy_summary else None,
                        "uri": timeoff_uri,
                        "status": to_status,
                    })
            return timeoff_to_be_assigned

        invoke_custom_ruby_code_15 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_15',
            python_callable=get_timeoff_to_be_assigned
        )

        if_output_timeofftypesoutput_greater_than_0_16 = rail.IfOperator(
            task_id='if_output_timeofftypesoutput_greater_than_0_16',
            test='''{{ result('invoke_custom_ruby_code_15') | length > 0 }}''',
            yes_task="assign_timeoffassignmentsforexistingusers_17",
            no_task="log_to_sumo",
        )

        assign_timeoffassignmentsforexistingusers_17 = rail.RepliconServiceOperator(
            task_id='assign_timeoffassignmentsforexistingusers_17',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUris": get_unique_timeoff_uri()
            }
        )

        foreach_output_18 = rail.ForEachOperator(
            task_id='foreach_output_18',
            items="{{ result('invoke_custom_ruby_code_15') | to_json }}",
            start_task='get_default_time_off_type_policy_schedule_for_user_19',
            end_task='foreach_output_18_end'
        )

        get_default_time_off_type_policy_schedule_for_user_19 = rail.RepliconServiceOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user_19',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ result('foreach_output_18').uri }}"
                }
            }
        )

        log_policyto_assign_20 = rail.PythonOperator(
            task_id='log_policyto_assign_20',
            python_callable=lambda: json.loads(json.dumps(
                    rail.result('get_default_time_off_type_policy_schedule_for_user_19'), ensure_ascii=False).replace('null', '"effective"').replace(
                        '"script"', '"scriptTarget"'))
        )

        if_log_policyto_assign_20_present_21 = rail.IfOperator(
            task_id='if_log_policyto_assign_20_present_21',
            test='''{{ result('log_policyto_assign_20') | is_truthy }}''',
            yes_task="put_user_time_off_account_policy_set_schedule_22",
            no_task="foreach_output_18_end",
        )

        put_user_time_off_account_policy_set_schedule_22 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_22',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_output_18')['uri']
                },
                "policySetScheduleEntries": rail.result('log_policyto_assign_20')
            }
        )

        foreach_output_18_end = rail.EmptyOperator(
            task_id='foreach_output_18_end',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> ge_denmark_user_sync_master_mapper_v2_0_search_entries_timeoffto_assign_search_3
        ge_denmark_user_sync_master_mapper_v2_0_search_entries_timeoffto_assign_search_3 >> if_first_id_blank_4
        if_first_id_blank_4 >> rail.Label('Yes') >> log_to_sumo
        if_first_id_blank_4 >> rail.Label(
            'No') >> _adhoc_http_action_6 >> invoke_custom_ruby_code_8 >> if_log_time_off_typeto_assign_9_blank_10
        if_log_time_off_typeto_assign_9_blank_10 >> rail.Label(
            'Yes') >> log_to_sumo
        if_log_time_off_typeto_assign_9_blank_10 >> rail.Label('No') >> get_user_time_off_type_policy_summary_12 >> invoke_custom_ruby_code_13 >> \
            invoke_custom_ruby_code_15 >> if_output_timeofftypesoutput_greater_than_0_16
        if_output_timeofftypesoutput_greater_than_0_16 >> rail.Label('Yes') >> \
            assign_timeoffassignmentsforexistingusers_17 >> foreach_output_18 >> get_default_time_off_type_policy_schedule_for_user_19 >> \
            log_policyto_assign_20 >> if_log_policyto_assign_20_present_21
        if_log_policyto_assign_20_present_21 >> rail.Label(
            'Yes') >> put_user_time_off_account_policy_set_schedule_22 >> foreach_output_18_end
        if_log_policyto_assign_20_present_21 >> rail.Label(
            'No') >> foreach_output_18_end
        foreach_output_18 >> foreach_output_18_end >> log_to_sumo
        if_output_timeofftypesoutput_greater_than_0_16 >> rail.Label(
            'No') >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
