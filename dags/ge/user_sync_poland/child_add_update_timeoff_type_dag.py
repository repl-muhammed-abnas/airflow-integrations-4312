
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
import json
from airflow.models import Variable
import rail

from ge.user_sync_poland.utils import custom_methods

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_add_update_timeoff_type_dag_id,
        description=f'GE POLAND User Import Workflow to Add/Update Time off Type Child',
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
            no_task='response_from_dag_var'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='response_from_dag_var',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        response_from_dag_var = rail.SetVariableOperator(
            task_id='response_from_dag_var',
            append=False,
            name='response_from_dag',
            value='Success'
        )

        log_total_experience_including_education_level_3_9 = rail.PythonOperator(
            task_id='log_total_experience_including_education_level_3_9',
            python_callable=lambda dag_run: custom_methods.get_total_experience_including_education_level(
                config.POLAND_MASTER_MAPPER, config.DATE_DEFAULT_FORMAT, dag_run)
        )

        if_request_startdate_present_10 = rail.IfOperator(
            task_id='if_request_startdate_present_10',
            test='''{{ dag_run.conf.startdate | is_truthy }}''',
            yes_task="log_effectivedate_10_year_11",
            no_task="get_all_timeoff_types_in_replicon_12",
        )

        log_effectivedate_10_year_11 = rail.PythonOperator(
            task_id='log_effectivedate_10_year_11',
            python_callable=lambda dag_run:  ((((datetime.strptime(dag_run.conf['startdate'], config.DATE_DEFAULT_FORMAT) - relativedelta(
                years=int(rail.result('log_total_experience_including_education_level_3_9')['total_years_of_exp_with_education_level']))) - relativedelta(
                    months=int(rail.result('log_total_experience_including_education_level_3_9')['prev_exp_months']))) - relativedelta(
                        days=int(rail.result('log_total_experience_including_education_level_3_9')['prev_exp_days']))) + relativedelta(
                            years=10)).strftime(config.DATE_DEFAULT_FORMAT)
        )

        get_all_timeoff_types_in_replicon_12 = rail.RepliconServiceOperator(
            task_id='get_all_timeoff_types_in_replicon_12',
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes"
        )

        if_request_type_equals_to_update_13 = rail.IfOperator(
            task_id='if_request_type_equals_to_update_13',
            test='''{{ dag_run.conf.type == 'update' }}''',
            yes_task="if_request_contracttype_not_equals_to_un00_14",
            no_task="if_first_displaytext_present_21",
        )

        if_request_contracttype_not_equals_to_un00_14 = rail.IfOperator(
            task_id='if_request_contracttype_not_equals_to_un00_14',
            test='''{{ dag_run.conf.contracttype != 'UN00' }}''',
            yes_task="if_request_contractstart_blank_15",
            no_task="trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_annual_leave_19",
        )

        if_request_contractstart_blank_15 = rail.IfOperator(
            task_id='if_request_contractstart_blank_15',
            test='''{{ dag_run.conf.contractstart | is_falsy  or dag_run.conf.contractend | is_falsy }}''',
            yes_task="set_response_from_dag_var_16",
            no_task="trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_annual_leave_19",
        )

        set_response_from_dag_var_16 = rail.SetVariableOperator(
            task_id='set_response_from_dag_var_16',
            append=False,
            name='response_from_dag',
            value=lambda dag_run: "Time off policy not assigned - " + str("" if dag_run.conf['contractstart'] else "Contract Start date missing, ") + str(
                "" if dag_run.conf['contractend'] else "Contract End date missing")
        )

        trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_annual_leave_19 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_annual_leave_19',
            trigger_dag_id=config.child_assign_prorated_timeoff_policy_annual_leave_dag_id,
            execution_timeout=timedelta(config.execution_timeout_days),
            conf=lambda dag_run: {
                "userloginname": dag_run.conf['userloginname'],
                "useruri": dag_run.conf['useruri'],
                "startdate": dag_run.conf['startdate'],
                "type": dag_run.conf['type'],
                "timeoffuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_timeoff_types_in_replicon_12'), 'displayText', '01. PL_Urlop wypoczynkowy/Annual Leave', 'uri'),
                "scheduledweeklyhours": dag_run.conf['scheduledweeklyhours'],
                "fullpart": dag_run.conf['fullpart'],
                "timeofftype": "01. PL_Urlop wypoczynkowy/Annual Leave",
                "monthlyaccrual": "no" if dag_run.conf['PreviousExperience'] else "yes",
                "legal_entity": dag_run.conf['legalentity'],
                "exp": 20 if (datetime.strptime(rail.result('log_effectivedate_10_year_11'), config.DATE_DEFAULT_FORMAT).date() > datetime.strptime(
                    dag_run.conf['startdate'], config.DATE_DEFAULT_FORMAT).date()) else 26,
                "effective_date_10_years": rail.result('log_effectivedate_10_year_11'),
                "overwrite_policy": dag_run.conf['overwritepolicy'],
                "ContractType": dag_run.conf['contracttype'],
                "contract_end_date": dag_run.conf['contractend'],
                "PreviousExperience": dag_run.conf['PreviousExperience'],
                "education_level": dag_run.conf['educationlevel'],
                "old_scheduled_weekly_hrs": dag_run.conf['old_scheduled_hours'],
                "education_level_old": dag_run.conf['education_level_old'],
            }
        )

        wait_for_completion_trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_annual_leave_19 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_annual_leave_19',
            execution_timeout=timedelta(config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_annual_leave_19") }}'
        )

        if_first_displaytext_present_21 = rail.IfOperator(
            task_id='if_first_displaytext_present_21',
            test='''{{ result('get_all_timeoff_types_in_replicon_12')| is_truthy }}''',
            yes_task="ge_poland_user_sync_master_mapper_search_entries_22",
            no_task="if_log_49_present_51",
        )

        ge_poland_user_sync_master_mapper_search_entries_22 = rail.PythonOperator(
            task_id='ge_poland_user_sync_master_mapper_search_entries_22',
            python_callable=lambda dag_run:  list(filter(
                    lambda x: x['legal_entity'] == dag_run.conf['legalentity'] and (
                        x['type'] == "Timeoff types"), config.POLAND_MASTER_MAPPER))
        )

        if_timeoff_type_mapper_search_result_null_23 = rail.IfOperator(
            task_id='if_timeoff_type_mapper_search_result_null_23',
            test='''{{ result('ge_poland_user_sync_master_mapper_search_entries_22') | is_falsy }}''',
            yes_task="set_response_from_dag_var_24",
            no_task="log_final_set_timeoff_uris_26_29",
        )

        set_response_from_dag_var_24 = rail.SetVariableOperator(
            task_id='set_response_from_dag_var_24',
            append=False,
            name='response_from_dag',
            value='Success :Timeoff not assigned/updated as no timeoff is defined in mapper'
        )

        def get_final_set_timeoff_uris(mapper_search_result_timeoff, all_replicon_timeoffs):
            timeoff_to_apply_list = []
            mapper_search_result_timeoff = rail.result(
                'ge_poland_user_sync_master_mapper_search_entries_22')
            for timeoff in mapper_search_result_timeoff:
                timeoff_to_apply_list.append({
                    'name': timeoff['value'].strip(),
                    'uri': rail.find_first_by_attr_and_get_attr(all_replicon_timeoffs, 'displayText', timeoff['value'].strip(), 'uri')
                })

            timeoff_uris_list = [
                x['uri'] for x in timeoff_to_apply_list if x['uri']] if timeoff_to_apply_list else []

            return {
                'timeoff_to_apply_list': timeoff_to_apply_list,
                'timeoff_uris_list': timeoff_uris_list
            }

        log_final_set_timeoff_uris_26_29 = rail.PythonOperator(
            task_id='log_final_set_timeoff_uris_26_29',
            python_callable=lambda:  get_final_set_timeoff_uris(rail.result('ge_poland_user_sync_master_mapper_search_entries_22'), rail.result(
                'get_all_timeoff_types_in_replicon_12'))
        )

        if_timeoffs_to_apply_present_30 = rail.IfOperator(
            task_id='if_timeoffs_to_apply_present_30',
            test='''{{ result('log_final_set_timeoff_uris_26_29').timeoff_uris_list | is_truthy }}''',
            yes_task="put_time_off_type_assignments_for_user_31",
            no_task="if_log_49_present_51",
        )

        put_time_off_type_assignments_for_user_31 = rail.RepliconServiceOperator(
            task_id='put_time_off_type_assignments_for_user_31',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUris": rail.result('log_final_set_timeoff_uris_26_29')['timeoff_uris_list']
            }
        )

        create_child_triggered_list = rail.SetVariableOperator(
            task_id='create_child_triggered_list',
            name='wait_for_dag_runs',
            append=False,
            value=[]
        )

        foreach_timeoff_to_apply_26_32 = rail.ForEachOperator(
            task_id='foreach_timeoff_to_apply_26_32',
            items=lambda: rail.result(
                'log_final_set_timeoff_uris_26_29')['timeoff_to_apply_list'],
            start_task='if_timeoff_to_assign_uri_present_33',
            end_task='foreach_timeoff_to_apply_26_32_end'
        )

        if_timeoff_to_assign_uri_present_33 = rail.IfOperator(
            task_id='if_timeoff_to_assign_uri_present_33',
            test='''{{ result('foreach_timeoff_to_apply_26_32').uri | is_truthy }}''',
            yes_task="get_default_time_off_type_policy_schedule_for_user_36",
            no_task="foreach_timeoff_to_apply_26_32_end",
        )

        get_default_time_off_type_policy_schedule_for_user_36 = rail.RepliconServiceOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user_36',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ result('foreach_timeoff_to_apply_26_32').uri }}"
                }
            }
        )

        log_timeoff_policy_38 = rail.PythonOperator(
            task_id='log_timeoff_policy_38',
            python_callable=lambda: json.loads(json.dumps(rail.result('get_default_time_off_type_policy_schedule_for_user_36'), ensure_ascii=False).replace(
                'null', '"effective"').replace('"script"', '"scriptTarget"')) if rail.result(
                    'get_default_time_off_type_policy_schedule_for_user_36') else ''
        )

        if_timeoff_name_not_equals_to_annual_leave_or_compensatory_leave_39 = rail.IfOperator(
            task_id='if_timeoff_name_not_equals_to_annual_leave_or_compensatory_leave_39',
            test='''{{ result('foreach_timeoff_to_apply_26_32').name != '01. PL_Urlop wypoczynkowy/Annual Leave' and result(
                'foreach_timeoff_to_apply_26_32').name != 'PL_Compensatory time off/Odbiór nadgodzin' }}''',
            yes_task="if_log_timeoff_policy_38_present_40",
            no_task="if_foreach_timeoff_to_apply_26_32_name_equals_compensatory_timeoff_43",
        )

        if_log_timeoff_policy_38_present_40 = rail.IfOperator(
            task_id='if_log_timeoff_policy_38_present_40',
            test='''{{ result('log_timeoff_policy_38') | is_truthy }}''',
            yes_task="put_user_time_off_account_policy_set_schedule_41",
            no_task="foreach_timeoff_to_apply_26_32_end",
        )

        put_user_time_off_account_policy_set_schedule_41 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_41',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_timeoff_to_apply_26_32')['uri']
                },
                "policySetScheduleEntries": rail.result('log_timeoff_policy_38')
            }
        )

        if_foreach_timeoff_to_apply_26_32_name_equals_compensatory_timeoff_43 = rail.IfOperator(
            task_id='if_foreach_timeoff_to_apply_26_32_name_equals_compensatory_timeoff_43',
            test='''{{ result('foreach_timeoff_to_apply_26_32').name == 'PL_Compensatory time off/Odbiór nadgodzin' }}''',
            yes_task="trigger_dag_run_ge_poland_assign_time_off_policy_compensatory_time_off_44",
            no_task="if_foreach_timeoff_to_apply_26_32_name_equals_annual_leave_45",
        )

        trigger_dag_run_ge_poland_assign_time_off_policy_compensatory_time_off_44 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_ge_poland_assign_time_off_policy_compensatory_time_off_44',
            trigger_dag_id=config.child_assign_timeoff_policy_compensatory_timeoff_dag_id,
            execution_timeout=timedelta(config.execution_timeout_days),
            conf={
                "userloginname": "{{ dag_run.conf.userloginname }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "timeoffuri": "{{ result('foreach_timeoff_to_apply_26_32').uri }}",
                "timeofftype": "{{ result('foreach_timeoff_to_apply_26_32').name }}",
                "legal_entity": "{{ dag_run.conf.legalentity }}"
            }
        )

        insert_dag_44_to_wait_list = rail.SetVariableOperator(
            task_id='insert_dag_44_to_wait_list',
            name="{{result('create_child_triggered_list').name}}",
            append=True,
            value="{{result('trigger_dag_run_ge_poland_assign_time_off_policy_compensatory_time_off_44')}}"
        )

        if_foreach_timeoff_to_apply_26_32_name_equals_annual_leave_45 = rail.IfOperator(
            task_id='if_foreach_timeoff_to_apply_26_32_name_equals_annual_leave_45',
            test='''{{ result('foreach_timeoff_to_apply_26_32').name == '01. PL_Urlop wypoczynkowy/Annual Leave' }}''',
            yes_task="log_timeoff_uri_46",
            no_task="foreach_timeoff_to_apply_26_32_end",
        )

        log_timeoff_uri_46 = rail.PythonOperator(
            task_id='log_timeoff_uri_46',
            python_callable=lambda:  rail.render_template(
                "{{ result('foreach_timeoff_to_apply_26_32').uri }}")
        )

        if_request_contracttype_not_equals_to_un00_47 = rail.IfOperator(
            task_id='if_request_contracttype_not_equals_to_un00_47',
            test='''{{ dag_run.conf.contracttype != 'UN00' }}''',
            yes_task="if_request_contractstart_blank_48",
            no_task="foreach_timeoff_to_apply_26_32_end",
        )

        if_request_contractstart_blank_48 = rail.IfOperator(
            task_id='if_request_contractstart_blank_48',
            test='''{{ dag_run.conf.contractstart | is_falsy  or dag_run.conf.contractend | is_falsy }}''',
            yes_task="log_referencetoreturnresponse_49",
            no_task="foreach_timeoff_to_apply_26_32_end",
        )

        log_referencetoreturnresponse_49 = rail.PythonOperator(
            task_id='log_referencetoreturnresponse_49',
            python_callable=lambda dag_run:  "Time off policy not assigned - " + str(
                "" if dag_run.conf['contractstart'] else "Contract Start date missing, ") + str(
                    "" if dag_run.conf['contractend'] else "Contract End date missing")
        )

        foreach_timeoff_to_apply_26_32_end = rail.EmptyOperator(
            task_id='foreach_timeoff_to_apply_26_32_end',
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

        if_log_49_present_51 = rail.IfOperator(
            task_id='if_log_49_present_51',
            test='''{{ result('log_referencetoreturnresponse_49') | is_truthy }}''',
            yes_task="set_response_from_dag_var_52",
            no_task="if_log_46_present_54",
        )

        set_response_from_dag_var_52 = rail.SetVariableOperator(
            task_id='set_response_from_dag_var_52',
            append=False,
            name='response_from_dag',
            value="Success : {{result('log_referencetoreturnresponse_49')}}"
        )

        if_log_46_present_54 = rail.IfOperator(
            task_id='if_log_46_present_54',
            test='''{{ result('log_timeoff_uri_46') | is_truthy }}''',
            yes_task="trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_annual_leave_55",
            no_task="if_response_from_55_present_56",
        )

        trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_annual_leave_55 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_annual_leave_55',
            trigger_dag_id=config.child_assign_prorated_timeoff_policy_annual_leave_dag_id,
            execution_timeout=timedelta(config.execution_timeout_days),
            conf=lambda dag_run: {
                "userloginname": dag_run.conf['userloginname'],
                "useruri": dag_run.conf['useruri'],
                "startdate": dag_run.conf['startdate'],
                "type": dag_run.conf['type'],
                "timeoffuri": rail.result('log_timeoff_uri_46'),
                "scheduledweeklyhours": dag_run.conf['scheduledweeklyhours'],
                "fullpart": dag_run.conf['fullpart'],
                "timeofftype": "01. PL_Urlop wypoczynkowy/Annual Leave",
                "monthlyaccrual": "no" if dag_run.conf['PreviousExperience'] else "yes",
                "legal_entity": dag_run.conf['legalentity'],
                "exp": 20 if (datetime.strptime(rail.result(
                    'log_effectivedate_10_year_11'), config.DATE_DEFAULT_FORMAT).date() > datetime.strptime(
                    dag_run.conf['startdate'], config.DATE_DEFAULT_FORMAT).date()) else 26,
                "effective_date_10_years": rail.result('log_effectivedate_10_year_11'),
                "overwrite_policy": dag_run.conf['overwritepolicy'],
                "ContractType": dag_run.conf['ContractType'],
                "contract_end_date": dag_run.conf['contractend'],
                "PreviousExperience": dag_run.conf['PreviousExperience'],
                "education_level": dag_run.conf['educationlevel'],
                "old_scheduled_weekly_hrs": dag_run.conf['old_scheduled_hours'],
                "education_level_old": dag_run.conf['education_level_old'],
            }
        )

        wait_for_completion_trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_annual_leave_55 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_annual_leave_55',
            execution_timeout=timedelta(config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_annual_leave_55") }}'
        )

        gather_responses_dag_55 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_responses_dag_55',
            dag_runs='{{ result("trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_annual_leave_55") }}',
            dagrun_task_id='final_response_from_dag',
            execution_timeout=timedelta(
                hours=config.responses_from_child_timeout),
            flatten=True
        )

        if_response_from_55_present_56 = rail.IfOperator(
            task_id='if_response_from_55_present_56',
            test='''{{ result('gather_responses_dag_55') | is_truthy }}''',
            yes_task="set_response_from_dag_var_57",
            no_task="catch_and_log_error",
        )

        set_response_from_dag_var_57 = rail.SetVariableOperator(
            task_id='set_response_from_dag_var_57',
            append=False,
            name='response_from_dag',
            value="Success : {{result('gather_responses_dag_55') | to_json}}"
        )

        catch_and_log_error = rail.PythonOperator(
            task_id="catch_and_log_error",
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "Error in add/update time off policy : {{get_error_message()}}")
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.result(
                "catch_and_log_error") or rail.get_dag_run_var('response_from_dag')
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> response_from_dag_var

        response_from_dag_var >> log_total_experience_including_education_level_3_9 >> if_request_startdate_present_10

        if_request_startdate_present_10 >> rail.Label(
            'No') >> get_all_timeoff_types_in_replicon_12
        if_request_startdate_present_10 >> rail.Label(
            'Yes') >> log_effectivedate_10_year_11 >> get_all_timeoff_types_in_replicon_12

        get_all_timeoff_types_in_replicon_12 >> if_request_type_equals_to_update_13

        if_request_type_equals_to_update_13 >> rail.Label(
            'No') >> if_first_displaytext_present_21
        if_request_type_equals_to_update_13 >> rail.Label(
            'Yes') >> if_request_contracttype_not_equals_to_un00_14

        if_request_contracttype_not_equals_to_un00_14 >> rail.Label(
            'No') >> trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_annual_leave_19
        if_request_contracttype_not_equals_to_un00_14 >> rail.Label(
            'Yes') >> if_request_contractstart_blank_15

        if_request_contractstart_blank_15 >> rail.Label(
            'No') >> trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_annual_leave_19
        if_request_contractstart_blank_15 >> rail.Label(
            'Yes') >> set_response_from_dag_var_16 >> catch_and_log_error

        trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_annual_leave_19 \
            >> wait_for_completion_trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_annual_leave_19 >> catch_and_log_error

        if_first_displaytext_present_21 >> rail.Label(
            'No') >> if_log_49_present_51
        if_first_displaytext_present_21 >> rail.Label('Yes') >> ge_poland_user_sync_master_mapper_search_entries_22 \
            >> if_timeoff_type_mapper_search_result_null_23

        if_timeoff_type_mapper_search_result_null_23 >> rail.Label(
            'No') >> log_final_set_timeoff_uris_26_29 >> if_timeoffs_to_apply_present_30
        if_timeoff_type_mapper_search_result_null_23 >> rail.Label(
            'Yes') >> set_response_from_dag_var_24 >> catch_and_log_error

        if_timeoffs_to_apply_present_30 >> rail.Label(
            'No') >> if_log_49_present_51
        if_timeoffs_to_apply_present_30 >> rail.Label('Yes') >> put_time_off_type_assignments_for_user_31 >> create_child_triggered_list \
            >> foreach_timeoff_to_apply_26_32

        foreach_timeoff_to_apply_26_32 >> if_timeoff_to_assign_uri_present_33

        if_timeoff_to_assign_uri_present_33 >> rail.Label(
            'No') >> foreach_timeoff_to_apply_26_32_end
        if_timeoff_to_assign_uri_present_33 >> rail.Label('Yes') >> get_default_time_off_type_policy_schedule_for_user_36 \
            >> log_timeoff_policy_38 >> if_timeoff_name_not_equals_to_annual_leave_or_compensatory_leave_39

        if_timeoff_name_not_equals_to_annual_leave_or_compensatory_leave_39 >> rail.Label(
            'No') >> if_foreach_timeoff_to_apply_26_32_name_equals_compensatory_timeoff_43
        if_timeoff_name_not_equals_to_annual_leave_or_compensatory_leave_39 >> rail.Label(
            'Yes') >> if_log_timeoff_policy_38_present_40

        if_log_timeoff_policy_38_present_40 >> rail.Label(
            'No') >> foreach_timeoff_to_apply_26_32_end
        if_log_timeoff_policy_38_present_40 >> rail.Label(
            'Yes') >> put_user_time_off_account_policy_set_schedule_41 \
            >> foreach_timeoff_to_apply_26_32_end

        if_foreach_timeoff_to_apply_26_32_name_equals_compensatory_timeoff_43 >> rail.Label(
            'No') >> if_foreach_timeoff_to_apply_26_32_name_equals_annual_leave_45
        if_foreach_timeoff_to_apply_26_32_name_equals_compensatory_timeoff_43 >> rail.Label(
            'Yes') >> trigger_dag_run_ge_poland_assign_time_off_policy_compensatory_time_off_44 >> insert_dag_44_to_wait_list \
            >> if_foreach_timeoff_to_apply_26_32_name_equals_annual_leave_45

        if_foreach_timeoff_to_apply_26_32_name_equals_annual_leave_45 >> rail.Label(
            'No') >> foreach_timeoff_to_apply_26_32_end
        if_foreach_timeoff_to_apply_26_32_name_equals_annual_leave_45 >> rail.Label(
            'Yes') >> log_timeoff_uri_46 >> if_request_contracttype_not_equals_to_un00_47

        if_request_contracttype_not_equals_to_un00_47 >> rail.Label(
            'No') >> foreach_timeoff_to_apply_26_32_end
        if_request_contracttype_not_equals_to_un00_47 >> rail.Label(
            'Yes') >> if_request_contractstart_blank_48

        if_request_contractstart_blank_48 >> rail.Label(
            'No') >> foreach_timeoff_to_apply_26_32_end
        if_request_contractstart_blank_48 >> rail.Label(
            'Yes') >> log_referencetoreturnresponse_49 >> foreach_timeoff_to_apply_26_32_end

        foreach_timeoff_to_apply_26_32 >> foreach_timeoff_to_apply_26_32_end >> child_dag_ids >> wait_for_child_dags \
            >> gather_responses_from_child >> if_log_49_present_51

        if_log_49_present_51 >> rail.Label('No') >> if_log_46_present_54
        if_log_49_present_51 >> rail.Label(
            'Yes') >> set_response_from_dag_var_52 >> catch_and_log_error

        if_log_46_present_54 >> rail.Label(
            'No') >> if_response_from_55_present_56
        if_log_46_present_54 >> rail.Label(
            'Yes') >> trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_annual_leave_55 \
            >> wait_for_completion_trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_annual_leave_55 \
            >> gather_responses_dag_55 >> if_response_from_55_present_56

        if_response_from_55_present_56 >> rail.Label(
            'No') >> catch_and_log_error
        if_response_from_55_present_56 >> rail.Label(
            'Yes') >> set_response_from_dag_var_57 >> catch_and_log_error

        catch_and_log_error >> final_response_from_dag

    return dag


rail.for_each_instance(create_dag)
