from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
from airflow.models import Variable
import json
from ge.user_sync_poland.utils import custom_methods, request_payload
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_update_user_dag_id,
        description=f'GE POLAND User Import Update User Child',
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
            no_task='update_and_exception_logs'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='update_and_exception_logs',
            end_task='catch_and_log_error',
        )

        update_and_exception_logs = rail.CreateLogOperator(
            task_id='update_and_exception_logs'
        )

        declare_variable_time_off_trigger_6 = rail.SetVariableOperator(
            task_id='declare_variable_time_off_trigger_6',
            append=False,
            name='time_off_trigger',
            value='no'
        )

        declare_variable_schedule_to_assign = rail.SetVariableOperator(
            task_id='declare_variable_schedule_to_assign',
            append=False,
            name='schedule_to_assign',
            value='''{{dag_run.conf.DWSMonday}}|{{dag_run.conf.DWSTuesday}}|{{dag_run.conf.DWSWednesday}}|{{dag_run.conf.DWSThursday}}|\
                {{dag_run.conf.DWSFriday}}|{{dag_run.conf.DWSSaturday}}|{{dag_run.conf.DWSSunday}}'''
        )

        bulk_get_users3_9 = rail.RepliconServiceOperator(
            task_id='bulk_get_users3_9',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": "{{ dag_run.conf.useruri }}",
                        "loginName": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        get_current_group_assignment_for_user = rail.RepliconServiceOperator(
            task_id='get_current_group_assignment_for_user',
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": rail.parse_date(dag_run.conf['HireEffectiveDate'] or dag_run.conf['integration_run_date'], config.DATE_DEFAULT_FORMAT),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            },
            data_handler=lambda res: {
                'effective_or_current_costcentre_name': (res['costCenters'][0]['costCenter']['costCenter']['displayText'] if (
                    res['costCenters'][0]['costCenter']) else '') if res['costCenters'] else '',
                'effective_or_current_division_name': (res['divisions'][0]['division']['division']['displayText'] if (
                    res['divisions'][0]['division']) else '') if res['divisions'] else '',
                'effective_or_current_servicecenter_name': (res['serviceCenters'][0]['serviceCenter']['serviceCenter']['displayText'] if (
                    res['serviceCenters'][0]['serviceCenter']) else '') if res['serviceCenters'] else '',
                'effective_or_current_servicecenter_uri': (res['serviceCenters'][0]['serviceCenter']['serviceCenter']['uri'] if (
                    res['serviceCenters'][0]['serviceCenter']) else '') if res['serviceCenters'] else '',
            }
        )

        get_required_user_customfield_field_uris = rail.PythonOperator(
            task_id='get_required_user_customfield_field_uris',
            python_callable=lambda: custom_methods.get_required_customfield_uris(rail.result('bulk_get_users3_9')[
                0]['userDetails']['customFieldValues'])
        )

        get_required_user_customfield_values = rail.PythonOperator(
            task_id='get_required_user_customfield_values',
            python_callable=lambda: custom_methods.get_required_customfield_values(rail.result('bulk_get_users3_9')[
                0]['userDetails']['customFieldValues'])
        )

        poland_master_mapper_search_matching_legal_entity_11 = rail.PythonOperator(
            task_id='poland_master_mapper_search_matching_legal_entity_11',
            python_callable=lambda dag_run: list(
                filter(lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'], config.POLAND_MASTER_MAPPER))
        )

        get_number_of_working_days_12_28 = rail.PythonOperator(
            task_id='get_number_of_working_days_12_28',
            python_callable=lambda dag_run: custom_methods.get_number_of_working_days(rail.get_dag_run_var('schedule_to_assign'), rail.result(
                'poland_master_mapper_search_matching_legal_entity_11'), dag_run)
        )

        get_all_timeoffs_29 = rail.RepliconServiceOperator(
            task_id="get_all_timeoffs_29",
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
        )

        poland_master_mapper_search_legal_entity_timeoff_to_update_30 = rail.PythonOperator(
            task_id='poland_master_mapper_search_legal_entity_timeoff_to_update_30',
            python_callable=lambda dag_run: list(
                filter(lambda x: x['legal_entity'] == "Timeoff to update", config.POLAND_MASTER_MAPPER))
        )

        if_timeoff_to_update_legal_entity_not_in_mapper_31 = rail.IfOperator(
            task_id='if_timeoff_to_update_legal_entity_not_in_mapper_31',
            test='''{{ result('poland_master_mapper_search_matching_legal_entity_11') | is_falsy }}''',
            yes_task="if_user_isenabled_is_true_32",
            no_task="if_check_for_disable_and_update_end_date_41",
        )

        if_user_isenabled_is_true_32 = rail.IfOperator(
            task_id='if_user_isenabled_is_true_32',
            test='''{{ result('bulk_get_users3_9')[0].userDetails.isEnabled | is_truthy }}''',
            yes_task="disable_login_33",
            no_task="log_profile_already_disabled_39",
        )

        disable_login_33 = rail.RepliconServiceOperator(
            task_id='disable_login_33',
            endpoint="/services/SecurityService1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_termination_35 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_termination_35',
            retries=0,
            items=lambda: rail.result('poland_master_mapper_search_legal_entity_timeoff_to_update_30'),
            trigger_dag_id=config.child_assign_timeoff_policy_annual_leave_on_termination_dag_id,
            execution_timeout=timedelta(config.execution_timeout_days),
            conf=lambda dag_run, item: {
                "userloginname": dag_run.conf['OHRID'],
                "useruri": dag_run.conf['useruri'],
                "timeoffuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_timeoffs_29'), 'name', item['value'], 'uri', ''),
                "disabledate": dag_run.conf['integration_run_date'],
                "timeofftype": "01. PL_Urlop wypoczynkowy/Annual Leave",
                "PreviousExperience": dag_run.conf['PreviousExperience'],
                "education_level": dag_run.conf['EducationLevel'],
                "ContractType": dag_run.conf['ContractType'],
                "legal_entity_hire_date": dag_run.conf['LegalEntityHireDate'],
                "radiation_flag": dag_run.conf['RadiationFlag'],
                "legal_entity": dag_run.conf['LegalEntity'],
            }
        )

        wait_for_completion_trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_termination_35_36 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_termination_35_36',
            execution_timeout=timedelta(config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_termination_35") }}'
        )

        gather_results_from_35_dag_run = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_results_from_35_dag_run',
            dag_runs="{{result('trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_termination_35')}}",
            dagrun_task_id="final_response_from_dag",
            target='result'
        )

        if_error_in_gather_reponse_from_35_dag_run = rail.IfOperator(
            task_id='if_error_in_gather_reponse_from_35_dag_run',
            test=lambda: bool(rail.result("gather_results_from_35_dag_run")) and "Error" in json.dumps(rail.result(
                "gather_results_from_35_dag_run")[0]),
            yes_task="fail_with_error_in_assign_timeoff_policy_on_termination",
            no_task="log_profile_disabled_36",
        )

        fail_with_error_in_assign_timeoff_policy_on_termination = rail.FailOperator(
            task_id='fail_with_error_in_assign_timeoff_policy_on_termination',
            message="Error in updating timeoff policy Annual leave on termination"
        )

        log_profile_disabled_36 = rail.WriteLogOperator(
            task_id='log_profile_disabled_36',
            log="{{ dag_run.conf.user_import_log }}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "OHRID": dag_run.conf['OHRID'],
                "action": "Disable",
                "status": "Success",
                "details": "User not in allowed list of Legal Entities, profile disabled",
                "username": dag_run.conf['EmployeeFirstName'] + " " + dag_run.conf['EmployeeLastName']
            }
        )

        log_profile_already_disabled_39 = rail.WriteLogOperator(
            task_id='log_profile_already_disabled_39',
            log="{{ dag_run.conf.user_import_log }}",
            message="na",
            severity="Skipped",
            properties=lambda dag_run: {
                "OHRID": dag_run.conf['OHRID'],
                "action": "Disable",
                "status": "Skipped",
                "details": "User not in allowed list of Legal Entities, profile is already disabled",
                "username": dag_run.conf['EmployeeFirstName'] + " " + dag_run.conf['EmployeeLastName']
            }
        )

        if_check_for_disable_and_update_end_date_41 = rail.IfOperator(
            task_id='if_check_for_disable_and_update_end_date_41',
            test=lambda dag_run: bool(dag_run.conf['TerminationEffectiveDate']) and not (dag_run.conf['RevTermEffectiveDate']) and str(rail.result(
                'bulk_get_users3_9')[0]['userDetails']['isEnabled']) == "True" and datetime.strptime(
                    dag_run.conf['TerminationEffectiveDate'], config.DATE_DEFAULT_FORMAT).date() > (custom_methods.dict_date_to_datetime(rail.result(
                        'bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['startDate']) - timedelta(days=1)),
            yes_task="update_end_date",
            no_task="if_check_for_skipping_disabled_48",
        )

        update_end_date = rail.RepliconServiceOperator(
            task_id='update_end_date',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": rail.result('bulk_get_users3_9')[0]["userDetails"]["employmentDateRange"]["startDate"],
                    "endDate": rail.parse_date(dag_run.conf['TerminationEffectiveDate'], config.DATE_DEFAULT_FORMAT),
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_termination_45 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_termination_45',
            retries=0,
            items=lambda: rail.result('poland_master_mapper_search_legal_entity_timeoff_to_update_30'),
            trigger_dag_id=config.child_assign_timeoff_policy_annual_leave_on_termination_dag_id,
            execution_timeout=timedelta(config.execution_timeout_days),
            conf=lambda dag_run, item: {
                "userloginname": dag_run.conf['OHRID'],
                "useruri": dag_run.conf['useruri'],
                "timeoffuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_timeoffs_29'), 'name', item['value'], 'uri', ''),
                "disabledate": dag_run.conf['TerminationEffectiveDate'],
                "timeofftype": "01. PL_Urlop wypoczynkowy/Annual Leave",
                "PreviousExperience": dag_run.conf['PreviousExperience'],
                "education_level": dag_run.conf['EducationLevel'],
                "ContractType": dag_run.conf['ContractType'],
                "legal_entity_hire_date": dag_run.conf['LegalEntityHireDate'],
                "radiation_flag": dag_run.conf['RadiationFlag'],
                "legal_entity": dag_run.conf['LegalEntity'],
            }
        )

        wait_for_completion_trigger_dag_run_45_46 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_45_46',
            execution_timeout=timedelta(config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_termination_45") }}'
        )

        gather_results_from_45_dag_run = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_results_from_45_dag_run',
            dag_runs="{{result('trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_termination_45')}}",
            dagrun_task_id="final_response_from_dag",
            target='result'
        )

        if_error_in_gather_reponse_from_45_dag_run = rail.IfOperator(
            task_id='if_error_in_gather_reponse_from_45_dag_run',
            test=lambda: bool(rail.result("gather_results_from_45_dag_run")) and "Error" in json.dumps(rail.result(
                "gather_results_from_45_dag_run")[0]),
            yes_task="fail_with_error_in_assign_time_off_policy_on_termination",
            no_task="log_profile_disabled_46",
        )

        fail_with_error_in_assign_time_off_policy_on_termination = rail.FailOperator(
            task_id='fail_with_error_in_assign_time_off_policy_on_termination',
            message="Error in updating timeoff policy Annual leave on termination"
        )

        log_profile_disabled_46 = rail.WriteLogOperator(
            task_id='log_profile_disabled_46',
            log="{{ dag_run.conf.user_import_log }}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "OHRID": dag_run.conf['OHRID'],
                "action": "Disable",
                "status": "Success",
                "details": "User not in allowed list of Legal Entities, profile disabled",
                "username": dag_run.conf['EmployeeFirstName'] + " " + dag_run.conf['EmployeeLastName']
            }
        )

        if_check_for_skipping_disabled_48 = rail.IfOperator(
            task_id='if_check_for_skipping_disabled_48',
            test=lambda dag_run: bool(dag_run.conf['TerminationEffectiveDate']) and not (dag_run.conf['RevTermEffectiveDate']) and str(rail.result(
                'bulk_get_users3_9')[0]['userDetails']['isEnabled']) == "True" and datetime.strptime(
                    dag_run.conf['TerminationEffectiveDate'], config.DATE_DEFAULT_FORMAT).date() < (custom_methods.dict_date_to_datetime(rail.result(
                        'bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['startDate']) - timedelta(days=1)),
            yes_task="log_profile_disable_skipped_49",
            no_task="if_user_rehire_51",
        )

        log_profile_disable_skipped_49 = rail.WriteLogOperator(
            task_id='log_profile_disable_skipped_49',
            log="{{ dag_run.conf.user_import_log }}",
            message="na",
            severity="Skipped",
            properties=lambda dag_run: {
                "OHRID": dag_run.conf['OHRID'],
                "action": "Disable",
                "status": "Skipped",
                "details": "End Date not Updated as termination date is prior to start date",
                "username": dag_run.conf['EmployeeFirstName'] + " " + dag_run.conf['EmployeeLastName']
            }
        )

        if_user_rehire_51 = rail.IfOperator(
            task_id='if_user_rehire_51',
            test=lambda dag_run: str(rail.result(
                'bulk_get_users3_9')[0]['userDetails']['isEnabled']) != "True" and not (dag_run.conf['RevTermEffectiveDate']),
            yes_task="if_hireeffectivedate_not_present_52",
            no_task="if_enddate_present_reverse_termination_62",
        )

        if_hireeffectivedate_not_present_52 = rail.IfOperator(
            task_id='if_hireeffectivedate_not_present_52',
            test=lambda dag_run: not (dag_run.conf['HireEffectiveDate']),
            yes_task="log_hire_effectivedate_not_present_53",
            no_task="if_enddate_not_present_55",
        )

        log_hire_effectivedate_not_present_53 = rail.WriteLogOperator(
            task_id='log_hire_effectivedate_not_present_53',
            log="{{ dag_run.conf.user_import_log }}",
            message="na",
            severity="Skipped",
            properties=lambda dag_run: {
                "OHRID": dag_run.conf['OHRID'],
                "action": "Rehire",
                "status": "Skipped",
                "details": "Hire effective date not available",
                "username": dag_run.conf['EmployeeFirstName'] + " " + dag_run.conf['EmployeeLastName']
            }
        )

        if_enddate_not_present_55 = rail.IfOperator(
            task_id='if_enddate_not_present_55',
            test=lambda: not (rail.result(
                'bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['endDate']),
            yes_task="log_no_enddate_in_existing_profile_56",
            no_task="update_loginname_59",
        )

        log_no_enddate_in_existing_profile_56 = rail.WriteLogOperator(
            task_id='log_no_enddate_in_existing_profile_56',
            log="{{ dag_run.conf.user_import_log }}",
            message="na",
            severity="Skipped",
            properties=lambda dag_run: {
                "OHRID": dag_run.conf['OHRID'],
                "action": "Rehire",
                "status": "Skipped",
                "details": "The existing profile doesn't have an end date in Replicon",
                "username": dag_run.conf['EmployeeFirstName'] + " " + dag_run.conf['EmployeeLastName']
            }
        )

        update_loginname_59 = rail.RepliconServiceOperator(
            task_id='update_loginname_59',
            endpoint="/services/securityservice1.svc/SetRepliconAuthenticationForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "loginName": rail.result('bulk_get_users3_9')[0]['userDetails']['securityConfiguration']['loginName'] + "" + str(rail.result(
                    'bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['endDate']['month']) + "" + str(rail.result(
                        'bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['endDate']['day']) + str(rail.result(
                            'bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['endDate']['year'])[2:4]
            }
        )

        trigger_dag_run_ge_poland_user_add_rehire_60 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_ge_poland_user_add_rehire_60',
            retries=0,
            trigger_dag_id=config.child_add_user_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: request_payload.get_add_update_dag_conf(
                dag_run, 'rehire_add')
        )

        wait_for_trigger_dag_run_ge_poland_user_add_rehire_60 = rail.WaitForDagRunsSensor(
            task_id='wait_for_trigger_dag_run_ge_poland_user_add_rehire_60',
            dag_runs='{{ result("trigger_dag_run_ge_poland_user_add_rehire_60") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        if_enddate_present_reverse_termination_62 = rail.IfOperator(
            task_id='if_enddate_present_reverse_termination_62',
            test=lambda: bool(rail.result(
                'bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['endDate']),
            yes_task="if_check_dates_for_reverse_termination_64",
            no_task="check_firstname_lastname_email_change_70_77",
        )

        if_check_dates_for_reverse_termination_64 = rail.IfOperator(
            task_id='if_check_dates_for_reverse_termination_64',
            test=lambda dag_run: bool(dag_run.conf['RevTermEffectiveDate']) and datetime.strptime(
                dag_run.conf['RevTermEffectiveDate'], config.DATE_DEFAULT_FORMAT).date() < (custom_methods.dict_date_to_datetime(rail.result(
                    'bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['endDate'])) and datetime.strptime(
                        dag_run.conf['RevTermEffectiveDate'], config.DATE_DEFAULT_FORMAT).date() > (custom_methods.dict_date_to_datetime(rail.result(
                            'bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['startDate'])),
            yes_task="if_user_isenabled_is_not_true_65",
            no_task="check_firstname_lastname_email_change_70_77",
        )

        if_user_isenabled_is_not_true_65 = rail.IfOperator(
            task_id='if_user_isenabled_is_not_true_65',
            test='''{{ result('bulk_get_users3_9')[0].userDetails.isEnabled | is_falsy }}''',
            yes_task="enable_login_66",
            no_task="remove_end_date_68",
        )

        enable_login_66 = rail.RepliconServiceOperator(
            task_id='enable_login_66',
            endpoint="/services/SecurityService1.svc/EnableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        insert_to_update_logs_67 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_67',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "User profile re-enabled, reverse termination date older than end date and newer than start date."
            }
        )

        remove_end_date_68 = rail.RepliconServiceOperator(
            task_id='remove_end_date_68',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": rail.result('bulk_get_users3_9')[0]["userDetails"]["employmentDateRange"]["startDate"],
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        insert_to_update_logs_69 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_69',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "End date removed, reverse termination date older than end date and newer than start date."
            }
        )

        check_firstname_lastname_email_change_70_77 = rail.PythonOperator(
            task_id="check_firstname_lastname_email_change_70_77",
            python_callable=lambda dag_run: custom_methods.check_name_email_changes(dag_run, rail.result('bulk_get_users3_9')[0]['userDetails']['firstName'] or '', rail.result(
                'bulk_get_users3_9')[0]['userDetails']['lastName'] or '', rail.result('bulk_get_users3_9')[0]['userDetails']['emailAddress'] or '')
        )

        if_check_firstname_lastname_email_change = rail.IfOperator(
            task_id='if_check_firstname_lastname_email_change',
            test=lambda: bool(rail.result(
                'check_firstname_lastname_email_change_70_77')['log_message']),
            yes_task="apply_user_modifications_name_email_change",
            no_task="if_current_overtime_eligibility_not_equals_new_130",
        )

        apply_user_modifications_name_email_change = rail.RepliconServiceOperator(
            task_id='apply_user_modifications_name_email_change',
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda: rail.result('check_firstname_lastname_email_change_70_77')[
                'applyusermodifications_payload']
        )

        insert_to_update_logs_78 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_78',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties=lambda: {
                "details": rail.result('check_firstname_lastname_email_change_70_77')['log_message']
            }
        )

        if_current_overtime_eligibility_not_equals_new_130 = rail.IfOperator(
            task_id='if_current_overtime_eligibility_not_equals_new_130',
            test=lambda dag_run: bool(dag_run.conf['OvertimeEligibility']) and rail.result(
                'get_required_user_customfield_values')['overtime_eligibility'] != dag_run.conf['OvertimeEligibility'],
            yes_task='get_overtime_eligibility_customfield_matching_dropdown_value_uri_132_133',
            no_task='customfield_values_change_payload_and_logs_79_160'
        )

        get_overtime_eligibility_customfield_matching_dropdown_value_uri_132_133 = rail.RepliconServiceOperator(
            task_id="get_overtime_eligibility_customfield_matching_dropdown_value_uri_132_133",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result('get_required_user_customfield_field_uris')['overtime_eligibility_field_uri']
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['OvertimeEligibility'], 'uri', '')
        )

        update_dropdown_value_overtime_eligibility_udf_134 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_overtime_eligibility_udf_134',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['useruri'],
                "customFieldUri": rail.result('get_required_user_customfield_field_uris')['overtime_eligibility_field_uri'],
                "customFieldDropDownOptionUri": rail.result('get_overtime_eligibility_customfield_matching_dropdown_value_uri_132_133')
            }
        )

        insert_to_update_logs_135 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_135',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties=lambda: {
                "details": 'Overtime Eligibility Udf updated'
            }
        )

        customfield_values_change_payload_and_logs_79_160 = rail.PythonOperator(
            task_id='customfield_values_change_payload_and_logs_79_160',
            python_callable=lambda dag_run: custom_methods.get_customfield_values_change_payload_logs(rail.result(
                'get_required_user_customfield_field_uris'), rail.result('get_required_user_customfield_values'), config.DATE_DEFAULT_FORMAT, dag_run)
        )

        apply_user_modifications_udf_updates = rail.RepliconServiceOperator(
            task_id='apply_user_modifications_udf_updates',
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda: rail.result('customfield_values_change_payload_and_logs_79_160')[
                'user_modifications_payload']
        )

        update_variable_time_off_trigger_104_110_123_152_159 = rail.SetVariableOperator(
            task_id='update_variable_time_off_trigger_104_110_123_152_159',
            append=False,
            name='time_off_trigger',
            value='{{result("customfield_values_change_payload_and_logs_79_160").time_off_trigger}}'
        )

        if_current_suspend_assignment_catagory_not_equals_new_162 = rail.IfOperator(
            task_id='if_current_suspend_assignment_catagory_not_equals_new_162',
            test=lambda dag_run: bool(dag_run.conf['SuspendAssignmentCategory']) and rail.result(
                'get_required_user_customfield_values')['suspend_assignment_catagory'] != dag_run.conf['SuspendAssignmentCategory'],
            yes_task='get_suspend_assignment_catagory_customfield_matching_dropdown_value_uri_165',
            no_task='if_supervisor_sso_id_present_168'
        )

        get_suspend_assignment_catagory_customfield_matching_dropdown_value_uri_165 = rail.RepliconServiceOperator(
            task_id="get_suspend_assignment_catagory_customfield_matching_dropdown_value_uri_165",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result('get_required_user_customfield_field_uris')['suspend_assignment_catagory_field_uri']
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['SuspendAssignmentCategory'], 'uri', '')
        )

        update_dropdown_value_suspend_assignment_catagory_udf_166 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_suspend_assignment_catagory_udf_166',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['useruri'],
                "customFieldUri": rail.result('get_required_user_customfield_field_uris')['suspend_assignment_catagory_field_uri'],
                "customFieldDropDownOptionUri": rail.result('get_suspend_assignment_catagory_customfield_matching_dropdown_value_uri_165')
            }
        )

        insert_to_update_logs_167 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_167',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties=lambda: {
                "details": 'Suspend Assignment Category updated'
            }
        )

        if_supervisor_sso_id_present_168 = rail.IfOperator(
            task_id='if_supervisor_sso_id_present_168',
            test=lambda dag_run: bool(dag_run.conf['SupervisorSSOID']),
            yes_task='if_ohrid_not_equals_supervisor_sso_id_present_169',
            no_task='if_legalentity_present_209'
        )

        if_ohrid_not_equals_supervisor_sso_id_present_169 = rail.IfOperator(
            task_id='if_ohrid_not_equals_supervisor_sso_id_present_169',
            test=lambda dag_run: dag_run.conf['OHRID'] != dag_run.conf['SupervisorSSOID'],
            yes_task='get_current_supervisor_of_user_171_183',
            no_task='insert_exception_to_logs_207'
        )

        get_current_supervisor_of_user_171_183 = rail.RepliconServiceOperator(
            task_id="get_current_supervisor_of_user_171_183",
            endpoint='/services/UserService1.svc/GetSupervisorAssignmentDetails',
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "asOfDate": dag_run.conf['integration_run_date']
            },
            data_handler=lambda res: {
                'current_supervisor_loginname': rail.result('get_effective_supervisor_of_user')['supervisor']['user']['loginName'],
                'current_supervisor_uri': rail.result('get_effective_supervisor_of_user')['supervisor']['user']['uri'],
            } if res else null
        )

        if_current_supervisor_loginname_not_equals_new_184 = rail.IfOperator(
            task_id='if_current_supervisor_loginname_not_equals_new_184',
            test=lambda dag_run: not (rail.result('get_current_supervisor_of_user_171_183')) or rail.result(
                'get_current_supervisor_of_user_171_183')['current_supervisor_loginname'] != dag_run.conf['SupervisorSSOID'],
            yes_task='search_supervisor_in_replicon_185',
            no_task='if_legalentity_present_209'
        )

        search_supervisor_in_replicon_185 = rail.RepliconServiceOperator(
            task_id='search_supervisor_in_replicon_185',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data=lambda dag_run: {
                "users": [{
                    "uri": null,
                    "loginName": dag_run.conf['SupervisorSSOID'],
                    "employeeId": null,
                    "parameterCorrelationId": null
                }],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda res: {
                'uri': res[0]['userDetails']['uri'],
                'employee_id': res[0]['userDetails']['employeeId'],
                'status': res[0]['userDetails']['isEnabled'],
                'end_date': custom_methods.dict_date_to_datetime(res[0]['userDetails']['employmentDateRange']['endDate']).strftime(config.DATE_DEFAULT_FORMAT)
                if res[0]['userDetails']['employmentDateRange']['endDate'] else null
            } if res else []
        )

        is_supervisor_profile_not_available_187 = rail.IfOperator(
            task_id='is_supervisor_profile_not_available_187',
            test=lambda: rail.result(
                'search_supervisor_in_replicon_185') == [],
            yes_task='log_supervisor_not_present_188',
            no_task='is_supervisor_profile_disabled_190'
        )

        log_supervisor_not_present_188 = rail.WriteLogOperator(
            task_id='log_supervisor_not_present_188',
            log='{{ dag_run.conf.supervisor_log }}',
            message="Supervisor not found in Replicon",
            severity='queued',
            properties=lambda dag_run: {
                "username": dag_run.conf['OHRID'],
                'useruri': dag_run.conf['useruri'],
                'supervisorloginname': dag_run.conf['SupervisorSSOID'],
                'action': 'update',
                'status': 'queued',
                'supervisoreffectivedate': dag_run.conf['AssignmentEffectiveDate'] if dag_run.conf['AssignmentEffectiveDate'] else (
                    dag_run.conf['integration_run_date']),
                'supervisorusername': dag_run.conf['SupervisorName'],
                'user_log': dag_run.conf['user_import_log']
            },
        )

        is_supervisor_profile_disabled_190 = rail.IfOperator(
            task_id='is_supervisor_profile_disabled_190',
            test=lambda: not (rail.result(
                'search_supervisor_in_replicon')['status']),
            yes_task='log_assignment_queued_supervisor_disabled_in_replicon_191',
            no_task='is_supervisor_profile_present_and_enabled_in_replicon_192'
        )

        log_assignment_queued_supervisor_disabled_in_replicon_191 = rail.WriteLogOperator(
            task_id='log_assignment_queued_supervisor_disabled_in_replicon_191',
            log='{{ dag_run.conf.supervisor_log }}',
            message="Supervisor profile is disabled in Replicon",
            severity='queued',
            properties=lambda dag_run: {
                "username": dag_run.conf['OHRID'],
                'useruri': dag_run.conf['useruri'],
                'supervisorloginname': dag_run.conf['SupervisorSSOID'],
                'action': 'update',
                'status': 'queued',
                'supervisoreffectivedate': dag_run.conf['AssignmentEffectiveDate'] if dag_run.conf['AssignmentEffectiveDate'] else (
                    dag_run.conf['integration_run_date']),
                'supervisorusername': dag_run.conf['SupervisorName'],
                'user_log': dag_run.conf['user_import_log']
            },
        )

        is_supervisor_profile_present_and_enabled_in_replicon_192 = rail.IfOperator(
            task_id='is_supervisor_profile_present_and_enabled_in_replicon_192',
            test=lambda: rail.result('search_supervisor_in_replicon_185') and rail.result(
                'search_supervisor_in_replicon')['status'],
            yes_task='get_required_supervisor_permission_to_be_assigned_193',
            no_task='if_legalentity_present_209'
        )

        get_required_supervisor_permission_to_be_assigned_193 = rail.PythonOperator(
            task_id='get_required_supervisor_permission_to_be_assigned_193',
            python_callable=lambda dag_run: list(filter(
                lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "Permission" and
                    x['identifier__1__(_legal_entity_code/_type/_timeoff_type)'] == 'Supervisor', config.POLAND_MASTER_MAPPER))
        )

        get_uris_for_matching_permissions_from_mapper = rail.RepliconServiceOperator(
            task_id='get_uris_for_matching_permissions_from_mapper',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data_handler=lambda response: [rail.find_first_by_attr_and_get_attr(response, 'displayText', entry['value'], 'uri') for entry in rail.result(
                    'get_required_supervisor_permission_to_be_assigned_193') if rail.find_first_by_attr_and_get_attr(
                        response, 'displayText', entry['value'], 'uri')] if rail.result('get_required_supervisor_permission_to_be_assigned_193') else []
        )

        get_missing_supervisor_permissions_194_201 = rail.RepliconServiceOperator(
            task_id='get_missing_supervisor_permissions_194_201',
            endpoint='/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2',
            data={
                'userUri': "{{ result('search_supervisor_in_replicon_185').uri }}"
            },
            log_response=True,
            data_handler=lambda response: custom_methods.get_missing_user_permissions(rail.result(
                'get_uris_for_matching_permissions_from_mapper'), response)
        )

        should_add_missing_permissions = rail.IfOperator(
            task_id='should_add_missing_permissions',
            test="{{ result('get_missing_supervisor_permissions_194_201') | length > 0 }}",
            yes_task='add_missing_supervisor_permissions_202',
            no_task='update_supervisor_schedule_for_user_204'
        )

        add_missing_supervisor_permissions_202 = rail.RepliconServiceCallForEachItemOperator(
            task_id='add_missing_supervisor_permissions_202',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            items=lambda: rail.result(
                'get_missing_supervisor_permissions_194_201'),
            data={
                'userUri': "{{ result('search_supervisor_in_replicon_185').uri }}",
                'permissionSetUri': '{{ item }}'
            }
        )

        update_supervisor_schedule_for_user_204 = rail.RepliconServiceOperator(
            task_id="update_supervisor_schedule_for_user_204",
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": rail.result('search_supervisor_in_replicon_185')['uri'],
                "dateRange": {
                    "startDate": rail.parse_date(
                        dag_run.conf['AssignmentEffectiveDate'] or dag_run.conf['integration_run_date'], config.DATE_DEFAULT_FORMAT)
                }
            }
        )

        insert_to_update_logs_205 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_205',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties=lambda: {
                "details": 'Supervisor updated'
            }
        )

        insert_exception_to_logs_207 = rail.WriteLogOperator(
            task_id='insert_exception_to_logs_207',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='Exception',
            properties={
                "details": "Supervisor not assigned/updated since the user and supervisor SSO ID are same"
            }
        )

        if_legalentity_present_209 = rail.IfOperator(
            task_id='if_legalentity_present_209',
            test=lambda dag_run: bool(dag_run.conf['LegalEntity']),
            yes_task='mapper_search_legal_entity_as_per_feed_file_228',
            no_task='if_industryfocus_group_present_250'
        )

        mapper_search_legal_entity_as_per_feed_file_228 = rail.PythonOperator(
            task_id='mapper_search_legal_entity_as_per_feed_file_228',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'poland_master_mapper_search_matching_legal_entity_11'), 'type', 'Legal Entity', 'value', '')
        )

        if_current_legal_entity_not_present_or_not_equal_to_new_229 = rail.IfOperator(
            task_id='if_current_legal_entity_not_present_or_not_equal_to_new_229',
            test=lambda dag_run: not (rail.result('get_current_group_assignment_for_user')['effective_or_current_costcentre_name']) or rail.result(
                'get_current_group_assignment_for_user')['effective_or_current_costcentre_name'] != rail.result(
                    'mapper_search_legal_entity_as_per_feed_file_228'),
            yes_task='log_get_new_legal_entity_costcenter_228_uri_231',
            no_task='if_industryfocus_group_present_250'
        )

        log_get_new_legal_entity_costcenter_228_uri_231 = rail.RepliconServiceOperator(
            task_id='log_get_new_legal_entity_costcenter_228_uri_231',
            endpoint="/services/CostCenterService1.svc/GetAllCostCenters",
            data_handler=lambda res: rail.find_first_by_attr_and_get_attr(
                res, 'displayText', rail.result('mapper_search_legal_entity_as_per_feed_file_228'), 'uri')
        )

        if_new_legal_entity_uri_present_232 = rail.IfOperator(
            task_id='if_new_legal_entity_uri_present_232',
            test=lambda: bool(rail.result(
                'log_get_new_legal_entity_costcenter_228_uri_231')),
            yes_task='put_cost_center_schedule_for_user_235',
            no_task='insert_exception_to_logs_249'
        )

        put_cost_center_schedule_for_user_235 = rail.RepliconServiceOperator(
            task_id='put_cost_center_schedule_for_user_235',
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri']
                },
                "modifications": {
                    "costCenterScheduleToApply": {
                        "userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementCostCenterSchedule": [],
                        "updateCostCenterScheduleOverDateRange": {
                            "replacementCostCenterScheduleEntries": [
                                {
                                    "costCenter": {
                                        "uri": rail.result('log_get_new_legal_entity_costcenter_228_uri_231'),
                                        "parentUri": null,
                                        "name": null
                                    },
                                    "effectiveDate": rail.parse_date(
                                        dag_run.conf['HireEffectiveDate'] or dag_run.conf['integration_run_date'], config.DATE_DEFAULT_FORMAT)
                                }
                            ],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        insert_to_update_logs_236 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_236',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties=lambda: {
                "details": 'Legal entity updated'
            }
        )

        log_timesheet_template_name_from_mapper_237 = rail.PythonOperator(
            task_id='log_timesheet_template_name_from_mapper_237',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'poland_master_mapper_search_matching_legal_entity_11'), 'type', 'Timesheet Template', 'value', '')
        )

        if_log_timesheet_template_237_not_present_238 = rail.IfOperator(
            task_id='if_log_timesheet_template_237_not_present_238',
            test=lambda: not (rail.result(
                'log_timesheet_template_name_from_mapper_237')),
            yes_task='insert_exception_to_logs_239',
            no_task='get_required_timesheet_template_uri_241_242'
        )

        insert_exception_to_logs_239 = rail.WriteLogOperator(
            task_id='insert_exception_to_logs_239',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='Exception',
            properties={
                "details": '''Cost Center not updated since the "{{result('mapper_search_legal_entity_as_per_feed_file_228')}}" is not available in Replicon'''
            }
        )

        get_required_timesheet_template_uri_241_242 = rail.RepliconServiceOperator(
            task_id='get_required_timesheet_template_uri_241_242',
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response, 'name', rail.result(
                'log_timesheet_template_name_from_mapper_237'), 'uri')
        )

        if_get_required_timesheet_template_uri_present_243 = rail.IfOperator(
            task_id='if_get_required_timesheet_template_uri_present_243',
            test=lambda: bool(rail.result(
                'get_required_timesheet_template_uri_241_242')),
            yes_task='update_timesheet_template_for_user_244',
            no_task='insert_exception_to_logs_247'
        )

        update_timesheet_template_for_user_244 = rail.RepliconServiceOperator(
            task_id='update_timesheet_template_for_user_244',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "policySetUri": "{{ result('get_required_timesheet_template_uri_241_242') }}"
            }
        )

        insert_to_update_logs_245 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_245',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Timesheet Tempalte updated"
            }
        )

        insert_exception_to_logs_247 = rail.WriteLogOperator(
            task_id='insert_exception_to_logs_247',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='Exception',
            properties={
                "details": '''Timesheet Tempalte "{{result('log_timesheet_template_name_from_mapper_237')}}" not available in Replicon'''
            }
        )

        insert_exception_to_logs_249 = rail.WriteLogOperator(
            task_id='insert_exception_to_logs_249',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='Exception',
            properties={
                "details": '''Legal entity not updated since "{{result('mapper_search_legal_entity_as_per_feed_file_228')}}" is not available in Replicon'''
            }
        )

        if_industryfocus_group_present_250 = rail.IfOperator(
            task_id='if_industryfocus_group_present_250',
            test=lambda dag_run: bool(dag_run.conf['IndustryFocusGroup']),
            yes_task='if_current_industryfocus_group_not_present_or_not_equal_to_new_269',
            no_task='if_hrmssoid_servicecenter_present_279'
        )

        if_current_industryfocus_group_not_present_or_not_equal_to_new_269 = rail.IfOperator(
            task_id='if_current_industryfocus_group_not_present_or_not_equal_to_new_269',
            test=lambda dag_run: not (rail.result('get_current_group_assignment_for_user')['effective_or_current_division_name']) or rail.result(
                'get_current_group_assignment_for_user')['effective_or_current_division_name'] != dag_run.conf['IndustryFocusGroup'],
            yes_task='log_get_new_industryfocus_group_division_uri_271',
            no_task='if_hrmssoid_servicecenter_present_279'
        )

        log_get_new_industryfocus_group_division_uri_271 = rail.RepliconServiceOperator(
            task_id='log_get_new_industryfocus_group_division_uri_271',
            endpoint="/services/DivisionService1.svc/GetAllDivisions",
            data_handler=lambda res, dag_run: rail.find_first_by_attr_and_get_attr(
                res, 'displayText', dag_run.conf['IndustryFocusGroup'], 'uri')
        )

        if_new_industryfocus_group_uri_present_272 = rail.IfOperator(
            task_id='if_new_industryfocus_group_uri_present_272',
            test=lambda: bool(rail.result(
                'log_get_new_industryfocus_group_division_uri_271')),
            yes_task='put_division_schedule_for_user_275',
            no_task='insert_exception_to_logs_278'
        )

        put_division_schedule_for_user_275 = rail.RepliconServiceOperator(
            task_id='put_division_schedule_for_user_275',
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri']
                },
                "modifications": {
                    "divisionScheduleToApply": {
                        "userDivisionScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementDivisionSchedule": [],
                        "updateDivisionScheduleOverDateRange": {
                            "replacementDivisionScheduleEntries": [
                                {
                                    "division": {
                                        "uri": rail.result('log_get_new_industryfocus_group_division_uri_271'),
                                        "parentUri": null,
                                        "name": null
                                    },
                                    "effectiveDate": rail.parse_date(
                                        dag_run.conf['HireEffectiveDate'] or dag_run.conf['integration_run_date'], config.DATE_DEFAULT_FORMAT)
                                }
                            ],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        insert_to_update_logs_276 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_276',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Industry focus group updated"
            }
        )

        insert_exception_to_logs_278 = rail.WriteLogOperator(
            task_id='insert_exception_to_logs_278',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='Exception',
            properties={
                "details": '''Industry focus group not updated since the "{{dag_run.conf.IndustryFocusGroup}}" is not available in Replicon'''
            }
        )

        if_hrmssoid_servicecenter_present_279 = rail.IfOperator(
            task_id='if_hrmssoid_servicecenter_present_279',
            test=lambda dag_run: bool(dag_run.conf['HRMSSOID']),
            yes_task='if_current_servicecenter_not_present_or_not_equal_to_new_298',
            no_task='check_if_schedule_change_305_309'
        )

        if_current_servicecenter_not_present_or_not_equal_to_new_298 = rail.IfOperator(
            task_id='if_current_servicecenter_not_present_or_not_equal_to_new_298',
            test=lambda dag_run: not (rail.result('get_current_group_assignment_for_user')['effective_or_current_servicecenter_uri']) or rail.result(
                'get_current_group_assignment_for_user')['effective_or_current_servicecenter_uri'] != dag_run.conf['legacypayroll_service_center_uri'],
            yes_task='if_legacypayroll_service_center_uri_present_299',
            no_task='check_if_schedule_change_305_309'
        )

        if_legacypayroll_service_center_uri_present_299 = rail.IfOperator(
            task_id='if_legacypayroll_service_center_uri_present_299',
            test=lambda dag_run: bool(
                dag_run.conf['legacypayroll_service_center_uri']),
            yes_task='put_service_center_schedule_for_user_302',
            no_task='check_if_schedule_change_305_309'
        )

        put_service_center_schedule_for_user_302 = rail.RepliconServiceOperator(
            task_id='put_service_center_schedule_for_user_302',
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri']
                },
                "modifications": {
                    "divisionScheduleToApply": {
                        "userDivisionScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementDivisionSchedule": [],
                        "updateDivisionScheduleOverDateRange": {
                            "replacementDivisionScheduleEntries": [
                                {
                                    "division": {
                                        "uri": rail.result('log_get_new_industryfocus_group_division_uri_271'),
                                        "parentUri": null,
                                        "name": null
                                    },
                                    "effectiveDate": rail.parse_date(
                                        dag_run.conf['HireEffectiveDate'] or dag_run.conf['integration_run_date'], config.DATE_DEFAULT_FORMAT)
                                }
                            ],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        insert_to_update_logs_303 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_303',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Legacy Payroll ID Group updated"
            }
        )

        check_if_schedule_change_305_309 = rail.IfOperator(
            task_id='check_if_schedule_change_305_309',
            test=custom_methods.check_schedule_change,
            yes_task='log_get_current_officeschedule_name_uri_328',
            no_task='if_time_off_trigger_equals_yes_355'
        )

        log_get_current_officeschedule_name_uri_328 = rail.PythonOperator(
            task_id='log_get_current_officeschedule_name_uri_328',
            python_callable=lambda dag_run: custom_methods.get_current_officescedule_name(rail.result('bulk_get_users3_9')[
                0]['schedulePolicies'], config.DATE_DEFAULT_FORMAT, dag_run)
        )

        if_log_get_current_officeschedule_name_uri_328_present_330 = rail.IfOperator(
            task_id='if_log_get_current_officeschedule_name_uri_328_present_330',
            test=lambda dag_run: bool(rail.result(
                'log_get_current_officeschedule_name_uri_328')['current_schedule_name']),
            yes_task="get_current_office_schedule_number_of_days_331_335",
            no_task="if_log_currentofficeschedulename_blank_or_unequal_new_336",
        )

        get_current_office_schedule_number_of_days_331_335 = rail.RepliconServiceOperator(
            task_id='get_current_office_schedule_number_of_days_331_335',
            endpoint="/services/OfficeScheduleService1.svc/GetOfficeScheduleDetails",
            data={
                "officeScheduleUri": "{{result('log_get_current_officeschedule_name_uri_328').current_schedule_uri}}"
            },
            data_handler=lambda res: sum(1 for day in res['recurringPattern']['patternEntries'] if int(
                day['workDuration']['hours']) > 0) if res['recurringPattern'] and res['recurringPattern']['patternEntries'] else 0
        )

        if_log_currentofficeschedulename_blank_or_unequal_new_336 = rail.IfOperator(
            task_id='if_log_currentofficeschedulename_blank_or_unequal_new_336',
            test=lambda dag_run: not (rail.result('log_get_current_officeschedule_name_uri_328')['current_schedule_name']) or rail.result(
                'log_get_current_officeschedule_name_uri_328')['current_schedule_name'] != rail.get_dag_run_var('schedule_to_assign'),
            yes_task="get_new_office_schedule_uri_337",
            no_task="if_time_off_trigger_equals_yes_355",
        )

        get_new_office_schedule_uri_337 = rail.RepliconServiceOperator(
            task_id='get_new_office_schedule_uri_337',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
            data_handler=lambda res: rail.find_first_by_attr_and_get_attr(
                res, 'displayText', rail.get_dag_run_var('schedule_to_assign'), 'uri')
        )

        if_required_office_schedule_uri_present_339 = rail.IfOperator(
            task_id='if_required_office_schedule_uri_present_339',
            test=lambda dag_run: bool(rail.result(
                'get_new_office_schedule_uri_337')),
            yes_task="put_schedule_policy_schedule_for_user_342",
            no_task="fail_office_schedule_not_in_replicon_354",
        )

        put_schedule_policy_schedule_for_user_342 = rail.RepliconServiceOperator(
            task_id='put_schedule_policy_schedule_for_user_342',
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri']
                },
                "modifications": {
                    "schedulePolicyToApply": {
                        "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementSchedule": [],
                        "updateScheduleOverDateRange": {
                            "replacementScheduleEntries": [
                                {
                                    "schedulePolicy": {
                                        "officeScheduleUri": rail.result('get_new_office_schedule_uri_337'),
                                        "name": null,
                                        "officeSchedule": {
                                            "officeScheduleUri": rail.result('get_new_office_schedule_uri_337'),
                                            "name": null
                                        },
                                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                                    },
                                    "effectiveDate": rail.parse_date(
                                        dag_run.conf['DWSStartDate'] or dag_run.conf['integration_run_date'], config.DATE_DEFAULT_FORMAT)
                                }
                            ],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        insert_to_update_logs_343 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_343',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Office schedule updated"
            }
        )

        get_new_office_schedule_number_of_days = rail.RepliconServiceOperator(
            task_id='get_new_office_schedule_number_of_days',
            endpoint="/services/OfficeScheduleService1.svc/GetOfficeScheduleDetails",
            data={
                "officeScheduleUri": "{{result('get_new_office_schedule_uri_337')}}"
            },
            data_handler=lambda res: sum(1 for day in res['recurringPattern']['patternEntries'] if int(
                day['workDuration']['hours']) > 0) if res['recurringPattern'] and res['recurringPattern']['patternEntries'] else 0
        )

        if_current_number_of_working_days_unequal_new_344 = rail.IfOperator(
            task_id='if_current_number_of_working_days_unequal_new_344',
            test=lambda: rail.result('get_current_office_schedule_number_of_days_331_335') != rail.result(
                'get_new_office_schedule_number_of_days'),
            yes_task="update_variable_time_off_trigger_345",
            no_task="if_current_previous_experience_unequal_new_previous_experience_346",
        )

        update_variable_time_off_trigger_345 = rail.SetVariableOperator(
            task_id='update_variable_time_off_trigger_345',
            append=False,
            name='time_off_trigger',
            value='yes'
        )

        if_current_previous_experience_unequal_new_previous_experience_346 = rail.IfOperator(
            task_id='if_current_previous_experience_unequal_new_previous_experience_346',
            test=lambda dag_run: rail.result('get_required_user_customfield_values')[
                'previous_experience'] != dag_run.conf['PreviousExperience'],
            yes_task="update_variable_time_off_trigger_347",
            no_task="if_contracttype_not_equals_cn00_348",
        )

        update_variable_time_off_trigger_347 = rail.SetVariableOperator(
            task_id='update_variable_time_off_trigger_347',
            append=False,
            name='time_off_trigger',
            value='yes'
        )

        if_contracttype_not_equals_cn00_348 = rail.IfOperator(
            task_id='if_contracttype_not_equals_cn00_348',
            test=lambda dag_run: dag_run.conf['ContractType'] != "CN00",
            yes_task="if_position_capacity_or_radiation_flag_not_present_349",
            no_task="if_time_off_trigger_equals_yes_355",
        )

        if_position_capacity_or_radiation_flag_not_present_349 = rail.IfOperator(
            task_id='if_position_capacity_or_radiation_flag_not_present_349',
            test=lambda dag_run: not (dag_run.conf['PositionCapacity']) or not (
                dag_run.conf['RadiationFlag']),
            yes_task="update_variable_time_off_trigger_350",
            no_task="if_previous_experience_not_present_351",
        )

        update_variable_time_off_trigger_350 = rail.SetVariableOperator(
            task_id='update_variable_time_off_trigger_350',
            append=False,
            name='time_off_trigger',
            value='no'
        )

        if_previous_experience_not_present_351 = rail.IfOperator(
            task_id='if_previous_experience_not_present_351',
            test=lambda dag_run: not (dag_run.conf['PreviousExperience']),
            yes_task="update_variable_time_off_trigger_352",
            no_task="if_time_off_trigger_equals_yes_355",
        )

        update_variable_time_off_trigger_352 = rail.SetVariableOperator(
            task_id='update_variable_time_off_trigger_352',
            append=False,
            name='time_off_trigger',
            value='no'
        )

        fail_office_schedule_not_in_replicon_354 = rail.FailOperator(
            task_id='fail_office_schedule_not_in_replicon_354',
            message='''Office schedule not updated since the schedule - "{{dag_run_var('schedule_to_assign')}}" is not available in Replicon '''
        )

        if_time_off_trigger_equals_yes_355 = rail.IfOperator(
            task_id='if_time_off_trigger_equals_yes_355',
            test=lambda: rail.get_dag_run_var('time_off_trigger') == "yes",
            yes_task="log_previous_experience_356_358",
            no_task="gather_exceptions_from_logs",
        )

        log_previous_experience_356_358 = rail.PythonOperator(
            task_id='log_previous_experience_356_358',
            python_callable=custom_methods.get_previous_experience
        )

        if_education_level_present_359 = rail.IfOperator(
            task_id='if_education_level_present_359',
            test=lambda dag_run: bool(dag_run.conf['EducationLevel']),
            yes_task="get_education_level_from_mapper_360",
            no_task="get_new_total_years_of_experience_361",
        )

        get_education_level_from_mapper_360 = rail.PythonOperator(
            task_id='get_education_level_from_mapper_360',
            python_callable=lambda dag_run: next(iter(filter(
                lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "educationlevel" and
                    x['identifier__1__(_legal_entity_code/_type/_timeoff_type)'] == dag_run.conf['EducationLevel'], config.POLAND_MASTER_MAPPER)), {}).get(
                        'value', '')
        )

        get_new_total_years_of_experience_361 = rail.PythonOperator(
            task_id='get_new_total_years_of_experience_361',
            python_callable=lambda dag_run: (int(rail.result('get_education_level_from_mapper_360')) + rail.result(
                'log_previous_experience_356_358')['years']) if rail.result('get_education_level_from_mapper_360') else (
                    0 + rail.result('log_previous_experience_356_358')['years'])
        )

        log_effective_date_to_consider_362 = rail.PythonOperator(
            task_id='log_effective_date_to_consider_362',
            python_callable=lambda dag_run: (dag_run.conf['LegalEntityHireDate'] if (
                dag_run.conf['ContractType'] == "UN00") else dag_run.conf['RadiationFlag']) if (
                    dag_run.conf['ContractType']) else dag_run.conf['LegalEntityHireDate']
        )

        log_prev_experience_reduced_from_362_plus_10_years_363 = rail.PythonOperator(
            task_id='log_prev_experience_reduced_from_362_plus_10_years_363',
            python_callable=lambda: ((((datetime.strptime(rail.result('log_effective_date_to_consider_362'), config.DATE_DEFAULT_FORMAT) - relativedelta(
                years=rail.result('log_previous_experience_356_358')['years'])) - relativedelta(
                months=rail.result('log_previous_experience_356_358')['months'])) - relativedelta(
                days=rail.result('log_previous_experience_356_358')['days'])) + relativedelta(years=10)).strftime(config.DATE_DEFAULT_FORMAT)
        )

        trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_annual_leave_365 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_annual_leave_365',
            retries=0,
            items=lambda: rail.result('poland_master_mapper_search_legal_entity_timeoff_to_update_30'),
            trigger_dag_id=config.child_assign_prorated_timeoff_policy_annual_leave_dag_id,
            execution_timeout=timedelta(config.execution_timeout_days),
            conf=lambda dag_run, item: {
                "userloginname": dag_run.conf['OHRID'],
                "useruri": dag_run.conf['useruri'],
                "startdate": rail.result('log_effective_date_to_consider_362'),
                "type": rail.get_dag_run_var('time_off_trigger'),
                "timeoffuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_timeoffs_29'), 'name', item['value'], 'uri', ''),
                "actualstartdate": str(rail.result('bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['startDate']['day']) + "/" + str(rail.result(
                    'bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['startDate']['month']) + "/" + str(rail.result(
                        'bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['startDate']['year']),
                "scheduledweeklyhours": 40 if (rail.result('get_number_of_working_days_12_28') > 39) else rail.result('get_number_of_working_days_12_28'),
                "fullpart": "full" if (rail.result('get_number_of_working_days_12_28') < 39) else "part",
                "timeofftype": item['value'],
                "monthlyaccrual": "no" if dag_run.conf['PreviousExperience'] else "yes",
                "legal_entity": dag_run.conf['LegalEntity'],
                "exp": 26 if (datetime.strptime(rail.result('log_effective_date_to_consider_362'), config.DATE_DEFAULT_FORMAT) > datetime.strptime(
                    rail.result('log_prev_experience_reduced_from_362_plus_10_years_363'), config.DATE_DEFAULT_FORMAT)) else 20,
                "effective_date_10_years": rail.result('log_prev_experience_reduced_from_362_plus_10_years_363'),
                "overwrite_policy": rail.result('customfield_values_change_payload_and_logs_79_160')['overwrite_policy'],
                "ContractType": dag_run.conf['ContractType'],
                "contract_end_date": "" if dag_run.conf['ContractType'] == "UN00" else dag_run.conf['PositionCapacity'],
                "PreviousExperience": dag_run.conf['PreviousExperience'],
                "education_level": dag_run.conf['EducationLevel'],
                "old_scheduled_weekly_hrs": rail.result('get_current_office_schedule_number_of_days_331_335'),
                "education_level_old":  rail.result('get_required_user_customfield_values')['education_level'],
            }
        )

        wait_for_completion_trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_annual_leave_365 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_annual_leave_365',
            execution_timeout=timedelta(config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_annual_leave_365") }}'
        )

        gather_response_from_dag_run_365 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_response_from_dag_run_365',
            dag_runs="{{result('trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_annual_leave_365')}}",
            dagrun_task_id="final_response_from_dag",
            target='result'
        )

        if_reponse_from_dag_run_365_366 = rail.IfOperator(
            task_id='if_reponse_from_dag_run_365_366',
            test=lambda: bool(rail.result("gather_response_from_dag_run_365")),
            yes_task="insert_exception_to_logs_367",
            no_task="insert_to_update_logs_369",
        )

        insert_exception_to_logs_367 = rail.WriteLogOperator(
            task_id='insert_exception_to_logs_367',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='Exception',
            properties={
                "details": "{{result('gather_response_from_dag_run_365')[0]}}"
            }
        )

        insert_to_update_logs_369 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_369',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "PL_Urlop wypoczynkowy/Annual Leave policy updated"
            }
        )

        gather_exceptions_from_logs = rail.FilterLogEntriesOperator(
            task_id='gather_exceptions_from_logs',
            log="{{result('update_and_exception_logs')}}",
            severity='Exception'
        )

        gather_update_entries_from_logs = rail.FilterLogEntriesOperator(
            task_id='gather_update_entries_from_logs',
            log="{{result('update_and_exception_logs')}}",
            severity='record_updated'
        )

        log_status_details_for_exceptions_updates_skipped_record = rail.PythonOperator(
            task_id='log_status_details_for_exceptions_updates_skipped_record',
            python_callable=lambda:  custom_methods.sort_updates_exceptions_logs(
                rail.result("gather_exceptions_from_logs"), rail.result("gather_update_entries_from_logs"))
        )

        ge_poland_user_sync_logs_entry_370 = rail.WriteLogOperator(
            task_id='ge_poland_user_sync_logs_entry_370',
            log="{{ dag_run.conf.user_import_log }}",
            message="na",
            severity=lambda: rail.result(
                'log_status_details_for_exceptions_updates_skipped_record')['status'],
            properties=lambda dag_run: {
                "OHRID": dag_run.conf['OHRID'],
                "action": "Update",
                "status": rail.result('log_status_details_for_exceptions_updates_skipped_record')['status'],
                "details": rail.result('log_status_details_for_exceptions_updates_skipped_record')['details'],
                "username": dag_run.conf['EmployeeFirstName'] + " " + dag_run.conf['EmployeeLastName']
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log="{{ dag_run.conf.user_import_log }}",
            message="na",
            severity="Error",
            properties=lambda dag_run: {
                "OHRID": dag_run.conf['OHRID'],
                "action": "Update",
                "status": "Error",
                "details": rail.render_template("{{ get_error_message() }}"),
                "username": dag_run.conf['EmployeeFirstName'] + " " + dag_run.conf['EmployeeLastName']
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label(
            'No') >> update_and_exception_logs

        update_and_exception_logs >> declare_variable_time_off_trigger_6 >> declare_variable_schedule_to_assign >> bulk_get_users3_9 >>\
            get_current_group_assignment_for_user >> get_required_user_customfield_field_uris >> get_required_user_customfield_values >>\
            poland_master_mapper_search_matching_legal_entity_11 >> get_number_of_working_days_12_28 >> get_all_timeoffs_29 >>\
            poland_master_mapper_search_legal_entity_timeoff_to_update_30 >> if_timeoff_to_update_legal_entity_not_in_mapper_31

        if_timeoff_to_update_legal_entity_not_in_mapper_31 >> rail.Label(
            'No') >> if_check_for_disable_and_update_end_date_41
        if_timeoff_to_update_legal_entity_not_in_mapper_31 >> rail.Label(
            'Yes') >> if_user_isenabled_is_true_32

        if_user_isenabled_is_true_32 >> rail.Label(
            'No') >> log_profile_already_disabled_39 >> catch_and_log_error
        if_user_isenabled_is_true_32 >> rail.Label('Yes') >> disable_login_33 >> trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_termination_35 >>\
            wait_for_completion_trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_termination_35_36 >> gather_results_from_35_dag_run

        gather_results_from_35_dag_run >> if_error_in_gather_reponse_from_35_dag_run

        if_error_in_gather_reponse_from_35_dag_run >> rail.Label(
            'No') >> log_profile_disabled_36 >> catch_and_log_error
        if_error_in_gather_reponse_from_35_dag_run >> rail.Label(
            'Yes') >> fail_with_error_in_assign_timeoff_policy_on_termination >> log_profile_disabled_36

        if_check_for_disable_and_update_end_date_41 >> rail.Label(
            'No') >> if_check_for_skipping_disabled_48

        if_check_for_disable_and_update_end_date_41 >> rail.Label(
            'Yes') >> update_end_date >> trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_termination_45 >>\
            wait_for_completion_trigger_dag_run_45_46 >> gather_results_from_45_dag_run

        gather_results_from_45_dag_run >> if_error_in_gather_reponse_from_45_dag_run

        if_error_in_gather_reponse_from_45_dag_run >> rail.Label(
            'No') >> log_profile_disabled_46 >> catch_and_log_error
        if_error_in_gather_reponse_from_45_dag_run >> rail.Label(
            'Yes') >> fail_with_error_in_assign_time_off_policy_on_termination >> log_profile_disabled_46

        if_check_for_skipping_disabled_48 >> rail.Label(
            'No') >> if_user_rehire_51
        if_check_for_skipping_disabled_48 >> rail.Label(
            'Yes') >> log_profile_disable_skipped_49 >> catch_and_log_error

        if_user_rehire_51 >> rail.Label(
            'No') >> if_enddate_present_reverse_termination_62
        if_user_rehire_51 >> rail.Label(
            'Yes') >> if_hireeffectivedate_not_present_52

        if_hireeffectivedate_not_present_52 >> rail.Label(
            'No') >> if_enddate_not_present_55
        if_hireeffectivedate_not_present_52 >> rail.Label(
            'Yes') >> log_hire_effectivedate_not_present_53 >> catch_and_log_error

        if_enddate_not_present_55 >> rail.Label('No') >> update_loginname_59
        if_enddate_not_present_55 >> rail.Label(
            'Yes') >> log_no_enddate_in_existing_profile_56 >> catch_and_log_error

        update_loginname_59 >> trigger_dag_run_ge_poland_user_add_rehire_60 >> wait_for_trigger_dag_run_ge_poland_user_add_rehire_60 >> catch_and_log_error

        if_enddate_present_reverse_termination_62 >> rail.Label(
            'No') >> check_firstname_lastname_email_change_70_77
        if_enddate_present_reverse_termination_62 >> rail.Label(
            'Yes') >> if_check_dates_for_reverse_termination_64

        if_check_dates_for_reverse_termination_64 >> rail.Label(
            'No') >> check_firstname_lastname_email_change_70_77
        if_check_dates_for_reverse_termination_64 >> rail.Label(
            'Yes') >> if_user_isenabled_is_not_true_65

        if_user_isenabled_is_not_true_65 >> rail.Label(
            'No') >> remove_end_date_68
        if_user_isenabled_is_not_true_65 >> rail.Label(
            'Yes') >> enable_login_66 >> insert_to_update_logs_67 >> remove_end_date_68

        remove_end_date_68 >> insert_to_update_logs_69 >> check_firstname_lastname_email_change_70_77

        check_firstname_lastname_email_change_70_77 >> if_check_firstname_lastname_email_change

        if_check_firstname_lastname_email_change >> rail.Label(
            'No') >> if_current_overtime_eligibility_not_equals_new_130
        if_check_firstname_lastname_email_change >> rail.Label(
            'Yes') >> apply_user_modifications_name_email_change >> insert_to_update_logs_78 >> if_current_overtime_eligibility_not_equals_new_130

        if_current_overtime_eligibility_not_equals_new_130 >> rail.Label(
            'No') >> customfield_values_change_payload_and_logs_79_160
        if_current_overtime_eligibility_not_equals_new_130 >> rail.Label(
            'Yes') >> get_overtime_eligibility_customfield_matching_dropdown_value_uri_132_133 >> update_dropdown_value_overtime_eligibility_udf_134 >>\
            insert_to_update_logs_135 >> customfield_values_change_payload_and_logs_79_160

        customfield_values_change_payload_and_logs_79_160 >> apply_user_modifications_udf_updates >> update_variable_time_off_trigger_104_110_123_152_159 >>\
            if_current_suspend_assignment_catagory_not_equals_new_162

        if_current_suspend_assignment_catagory_not_equals_new_162 >> rail.Label(
            'No') >> if_supervisor_sso_id_present_168
        if_current_suspend_assignment_catagory_not_equals_new_162 >> rail.Label(
            'Yes') >> get_suspend_assignment_catagory_customfield_matching_dropdown_value_uri_165 >> update_dropdown_value_suspend_assignment_catagory_udf_166 >>\
            insert_to_update_logs_167 >> if_supervisor_sso_id_present_168

        if_supervisor_sso_id_present_168 >> rail.Label(
            'No') >> if_legalentity_present_209
        if_supervisor_sso_id_present_168 >> rail.Label(
            'Yes') >> if_ohrid_not_equals_supervisor_sso_id_present_169

        if_ohrid_not_equals_supervisor_sso_id_present_169 >> rail.Label(
            'No') >> insert_exception_to_logs_207 >> if_legalentity_present_209
        if_ohrid_not_equals_supervisor_sso_id_present_169 >> rail.Label(
            'Yes') >> get_current_supervisor_of_user_171_183 >> if_current_supervisor_loginname_not_equals_new_184

        if_current_supervisor_loginname_not_equals_new_184 >> rail.Label(
            'No') >> if_legalentity_present_209
        if_current_supervisor_loginname_not_equals_new_184 >> rail.Label(
            'Yes') >> search_supervisor_in_replicon_185 >> is_supervisor_profile_not_available_187

        is_supervisor_profile_not_available_187 >> rail.Label(
            'No') >> is_supervisor_profile_disabled_190
        is_supervisor_profile_not_available_187 >> rail.Label(
            'Yes') >> log_supervisor_not_present_188 >> is_supervisor_profile_disabled_190

        is_supervisor_profile_disabled_190 >> rail.Label(
            'No') >> is_supervisor_profile_present_and_enabled_in_replicon_192
        is_supervisor_profile_disabled_190 >> rail.Label(
            'Yes') >> log_assignment_queued_supervisor_disabled_in_replicon_191 >> is_supervisor_profile_present_and_enabled_in_replicon_192

        is_supervisor_profile_present_and_enabled_in_replicon_192 >> rail.Label(
            'No') >> if_legalentity_present_209
        is_supervisor_profile_present_and_enabled_in_replicon_192 >> rail.Label(
            'Yes') >> get_required_supervisor_permission_to_be_assigned_193 >> get_uris_for_matching_permissions_from_mapper >>\
            get_missing_supervisor_permissions_194_201 >> should_add_missing_permissions

        should_add_missing_permissions >> rail.Label(
            'No') >> update_supervisor_schedule_for_user_204
        should_add_missing_permissions >> rail.Label(
            'Yes') >> add_missing_supervisor_permissions_202 >> update_supervisor_schedule_for_user_204

        update_supervisor_schedule_for_user_204 >> insert_to_update_logs_205 >> if_legalentity_present_209

        if_legalentity_present_209 >> rail.Label(
            'No') >> if_industryfocus_group_present_250
        if_legalentity_present_209 >> rail.Label(
            'Yes') >> mapper_search_legal_entity_as_per_feed_file_228 >> if_current_legal_entity_not_present_or_not_equal_to_new_229

        if_current_legal_entity_not_present_or_not_equal_to_new_229 >> rail.Label(
            'No') >> if_industryfocus_group_present_250
        if_current_legal_entity_not_present_or_not_equal_to_new_229 >> rail.Label(
            'Yes') >> log_get_new_legal_entity_costcenter_228_uri_231 >> if_new_legal_entity_uri_present_232

        if_new_legal_entity_uri_present_232 >> rail.Label(
            'No') >> insert_exception_to_logs_249 >> if_industryfocus_group_present_250
        if_new_legal_entity_uri_present_232 >> rail.Label(
            'Yes') >> put_cost_center_schedule_for_user_235 >> insert_to_update_logs_236 >> log_timesheet_template_name_from_mapper_237 >>\
            if_log_timesheet_template_237_not_present_238

        if_log_timesheet_template_237_not_present_238 >> rail.Label(
            'No') >> insert_exception_to_logs_239 >> if_industryfocus_group_present_250
        if_log_timesheet_template_237_not_present_238 >> rail.Label(
            'Yes') >> get_required_timesheet_template_uri_241_242 >> if_get_required_timesheet_template_uri_present_243

        if_get_required_timesheet_template_uri_present_243 >> rail.Label(
            'No') >> insert_exception_to_logs_247 >> if_industryfocus_group_present_250
        if_get_required_timesheet_template_uri_present_243 >> rail.Label(
            'Yes') >> update_timesheet_template_for_user_244 >> insert_to_update_logs_245 >> if_industryfocus_group_present_250

        if_industryfocus_group_present_250 >> rail.Label(
            'No') >> if_hrmssoid_servicecenter_present_279
        if_industryfocus_group_present_250 >> rail.Label(
            'Yes') >> if_current_industryfocus_group_not_present_or_not_equal_to_new_269

        if_current_industryfocus_group_not_present_or_not_equal_to_new_269 >> rail.Label(
            'No') >> if_hrmssoid_servicecenter_present_279
        if_current_industryfocus_group_not_present_or_not_equal_to_new_269 >> rail.Label(
            'Yes') >> log_get_new_industryfocus_group_division_uri_271 >> if_new_industryfocus_group_uri_present_272

        if_new_industryfocus_group_uri_present_272 >> rail.Label(
            'No') >> insert_exception_to_logs_278 >> if_hrmssoid_servicecenter_present_279
        if_new_industryfocus_group_uri_present_272 >> rail.Label(
            'Yes') >> put_division_schedule_for_user_275 >> insert_to_update_logs_276 >> if_hrmssoid_servicecenter_present_279

        if_hrmssoid_servicecenter_present_279 >> rail.Label(
            'No') >> check_if_schedule_change_305_309
        if_hrmssoid_servicecenter_present_279 >> rail.Label(
            'Yes') >> if_current_servicecenter_not_present_or_not_equal_to_new_298

        if_current_servicecenter_not_present_or_not_equal_to_new_298 >> rail.Label(
            'No') >> check_if_schedule_change_305_309
        if_current_servicecenter_not_present_or_not_equal_to_new_298 >> rail.Label(
            'Yes') >> if_legacypayroll_service_center_uri_present_299

        if_legacypayroll_service_center_uri_present_299 >> rail.Label(
            'No') >> check_if_schedule_change_305_309
        if_legacypayroll_service_center_uri_present_299 >> rail.Label(
            'Yes') >> put_service_center_schedule_for_user_302 >> insert_to_update_logs_303 >> check_if_schedule_change_305_309

        check_if_schedule_change_305_309 >> rail.Label(
            'No') >> if_time_off_trigger_equals_yes_355
        check_if_schedule_change_305_309 >> rail.Label(
            'Yes') >> log_get_current_officeschedule_name_uri_328 >> if_log_get_current_officeschedule_name_uri_328_present_330

        if_log_get_current_officeschedule_name_uri_328_present_330 >> rail.Label(
            'No') >> if_log_currentofficeschedulename_blank_or_unequal_new_336
        if_log_get_current_officeschedule_name_uri_328_present_330 >> rail.Label(
            'Yes') >> get_current_office_schedule_number_of_days_331_335 >> if_log_currentofficeschedulename_blank_or_unequal_new_336

        if_log_currentofficeschedulename_blank_or_unequal_new_336 >> rail.Label(
            'No') >> if_time_off_trigger_equals_yes_355
        if_log_currentofficeschedulename_blank_or_unequal_new_336 >> rail.Label(
            'Yes') >> get_new_office_schedule_uri_337 >> if_required_office_schedule_uri_present_339

        if_required_office_schedule_uri_present_339 >> rail.Label(
            'No') >> fail_office_schedule_not_in_replicon_354 >> if_time_off_trigger_equals_yes_355
        if_required_office_schedule_uri_present_339 >> rail.Label(
            'Yes') >> put_schedule_policy_schedule_for_user_342 >> insert_to_update_logs_343 >>\
            get_new_office_schedule_number_of_days >> if_current_number_of_working_days_unequal_new_344

        if_current_number_of_working_days_unequal_new_344 >> rail.Label(
            'No') >> if_current_previous_experience_unequal_new_previous_experience_346
        if_current_number_of_working_days_unequal_new_344 >> rail.Label(
            'Yes') >> update_variable_time_off_trigger_345 >> if_current_previous_experience_unequal_new_previous_experience_346

        if_current_previous_experience_unequal_new_previous_experience_346 >> rail.Label(
            'No') >> if_contracttype_not_equals_cn00_348
        if_current_previous_experience_unequal_new_previous_experience_346 >> rail.Label(
            'Yes') >> update_variable_time_off_trigger_347 >> if_contracttype_not_equals_cn00_348

        if_contracttype_not_equals_cn00_348 >> rail.Label(
            'No') >> if_time_off_trigger_equals_yes_355
        if_contracttype_not_equals_cn00_348 >> rail.Label(
            'Yes') >> if_position_capacity_or_radiation_flag_not_present_349

        if_position_capacity_or_radiation_flag_not_present_349 >> rail.Label(
            'No') >> if_previous_experience_not_present_351
        if_position_capacity_or_radiation_flag_not_present_349 >> rail.Label(
            'Yes') >> update_variable_time_off_trigger_350 >> if_previous_experience_not_present_351

        if_previous_experience_not_present_351 >> rail.Label(
            'No') >> if_time_off_trigger_equals_yes_355
        if_previous_experience_not_present_351 >> rail.Label(
            'Yes') >> update_variable_time_off_trigger_352 >> if_time_off_trigger_equals_yes_355

        if_time_off_trigger_equals_yes_355 >> rail.Label(
            'No') >> gather_exceptions_from_logs
        if_time_off_trigger_equals_yes_355 >> rail.Label(
            'Yes') >> log_previous_experience_356_358 >> if_education_level_present_359

        if_education_level_present_359 >> rail.Label(
            'No') >> get_new_total_years_of_experience_361
        if_education_level_present_359 >> rail.Label(
            'Yes') >> get_education_level_from_mapper_360 >> get_new_total_years_of_experience_361

        get_new_total_years_of_experience_361 >> log_effective_date_to_consider_362 >> log_prev_experience_reduced_from_362_plus_10_years_363 >>\
            trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_annual_leave_365 >>\
            wait_for_completion_trigger_dag_run_ge_poland_assign_prorated_timeoff_policy_annual_leave_365 >> gather_response_from_dag_run_365

        gather_response_from_dag_run_365 >> if_reponse_from_dag_run_365_366

        if_reponse_from_dag_run_365_366 >> rail.Label(
            'No') >> insert_to_update_logs_369 >> gather_exceptions_from_logs
        if_reponse_from_dag_run_365_366 >> rail.Label(
            'Yes') >> insert_exception_to_logs_367 >> gather_exceptions_from_logs

        gather_exceptions_from_logs >> gather_update_entries_from_logs >> log_status_details_for_exceptions_updates_skipped_record >>\
            ge_poland_user_sync_logs_entry_370 >> catch_and_log_error

        return dag


rail.for_each_instance(create_dag)
