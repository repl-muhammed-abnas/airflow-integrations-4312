from datetime import timedelta, datetime
import json
from airflow.models import Variable
import rail
from rail.lib.ecid import get_dagrun_ecid
from assuredpartnersinc.user_import_v3.utils import python_callable, request_payload

null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_update_user_dag_id,
        description=f'Assured Partners User Import Update User Child {config.instance}',
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
            start_task='update_and_exception_logs',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        update_and_exception_logs = rail.CreateLogOperator(
            task_id='update_and_exception_logs'
        )

        declare_variable_5 = rail.SetVariableOperator(
            task_id='declare_variable_5',
            append=False,
            name='updatetype',
            value='update'
        )

        assured_partners_user_sync_master_mapper_search_entries_7 = rail.PythonOperator(
            task_id='assured_partners_user_sync_master_mapper_search_entries_7',
            python_callable=lambda:  list(
                filter(lambda x: x["country"] == "global", config.MASTER_MAPPER))
        )

        bulk_get_users3_8 = rail.RepliconServiceOperator(
            task_id='bulk_get_users3_8',
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

        get_user_current_group_assignment_details = rail.RepliconServiceOperator(
            task_id='get_user_current_group_assignment_details',
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "dateRange": null
            },
            data_handler=lambda res: {
                'current_costcentre_name': (res['costCenters'][0]['costCenter']['costCenter']['displayText'] if res['costCenters'][0]['costCenter'] else '') if res['costCenters'] else '',
                'current_department_name': (res['departments'][0]['department']['department']['displayText'] if res['departments'][0]['department'] else '') if res['departments'] else '',
                'current_division_name': (res['divisions'][0]['division']['division']['displayText'] if res['divisions'][0]['division'] else '') if res['divisions'] else '',
                'current_employeetype_name': (res['employeeTypes'][0]['employeeType']['employeeType']['displayText'] if res['employeeTypes'][0]['employeeType'] else '') if res['employeeTypes'] else '',
                'current_location_name': (res['locations'][0]['location']['location']['displayText'] if res['locations'][0]['location'] else '') if res['locations'] else '',
                'current_servicecenter_name': (res['serviceCenters'][0]['serviceCenter']['serviceCenter']['displayText'] if res['serviceCenters'][0]['serviceCenter'] else '') if res['serviceCenters'] else '',
            }
        )

        get_timesheet_period_schedule_for_user_9 = rail.RepliconServiceOperator(
            task_id='get_timesheet_period_schedule_for_user_9',
            endpoint="/services/TimesheetPeriodService2.svc/GetTimesheetPeriodScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        def get_current_timesheet_period_name(dag_run):
            user_start_date_from_replicon = rail.result("bulk_get_users3_8")[
                0]['userDetails']['employmentDateRange']['startDate']
            all_assigned_timesheet_preiod_details = list(map(lambda x: {
                "effectivedate": python_callable.dict_date_to_datetime(x['effectiveDate']) if x['effectiveDate'] else python_callable.dict_date_to_datetime(user_start_date_from_replicon),
                "displaytext": x['timesheetPeriod']['displayText'] if x['timesheetPeriod'] else "",
                "daydiff":  (datetime.strptime(
                    dag_run.conf['integration_run_date'], config.DATE_DEFAULT_FORMAT).date() - python_callable.dict_date_to_datetime(x['effectiveDate'])) if x['effectiveDate'] else (
                        datetime.strptime(dag_run.conf['integration_run_date'], config.DATE_DEFAULT_FORMAT).date() - python_callable.dict_date_to_datetime(user_start_date_from_replicon))
            }, rail.result('get_timesheet_period_schedule_for_user_9')))

            current_timesheet_details_with_min_daydiff = min(
                all_assigned_timesheet_preiod_details, key=lambda y: y['daydiff']) if all_assigned_timesheet_preiod_details else ''

            return current_timesheet_details_with_min_daydiff['displaytext'] if current_timesheet_details_with_min_daydiff else ''

        log_current_timesheet_period_11 = rail.PythonOperator(
            task_id='log_current_timesheet_period_11',
            python_callable=get_current_timesheet_period_name
        )

        log_custom_field_values_for_reference_12 = rail.PythonOperator(
            task_id='log_custom_field_values_for_reference_12',
            python_callable=lambda: python_callable.get_required_customfield_values(rail.result('bulk_get_users3_8')[
                0]['userDetails']['customFieldValues'])
        )

        if_change_effective_date_present_in_input_and_is_delta = rail.IfOperator(
            task_id='if_change_effective_date_present_in_input_and_is_delta',
            test=lambda dag_run: dag_run.conf['ChangeEffectiveDate'] and (not (rail.result(
                'log_custom_field_values_for_reference_12')['change_effective_date']) or python_callable.dict_date_to_datetime(rail.result(
                    'log_custom_field_values_for_reference_12')['change_effective_date']) != python_callable.get_split_date(
                    dag_run.conf['ChangeEffectiveDate'], 'no_split')),
            yes_task="update_change_effective_date_udf",
            no_task="get_assigned_policy_sets_for_user_14",
        )

        update_change_effective_date_udf = rail.RepliconServiceOperator(
            task_id='update_change_effective_date_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['useruri'],
                "customFieldUri": dag_run.conf['change_effective_date_udf_uri'],
                "value": python_callable.get_split_date(dag_run.conf['ChangeEffectiveDate'], 'int')
            }
        )

        insert_to_update_logs_cef = rail.WriteLogOperator(
            task_id='insert_to_update_logs_cef',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Change Effective Date updated"
            }
        )

        get_assigned_policy_sets_for_user_14 = rail.RepliconServiceOperator(
            task_id='get_assigned_policy_sets_for_user_14',
            endpoint="/services/PolicySetService1.svc/GetAssignedPolicySetsForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=lambda response, dag_run: {
                'assigned_punch_entry_policy': rail.find_first_by_attr_and_get_attr(response, 'policySet.displayText', dag_run.conf['punch_entry_policy'], 'policySet.displayText'),
                'check_time_punch_entry_policy_assigned': rail.find_first_by_attr_and_get_attr(response, 'policyUri', "urn:replicon:policy:time-punch", 'policySet.uri')
            }
        )

        if_loginname_not_equals_to_emplid_15 = rail.IfOperator(
            task_id='if_loginname_not_equals_to_emplid_15',
            test=lambda dag_run: rail.result('bulk_get_users3_8')[
                0]['securityConfiguration']['loginName'].lower() != dag_run.conf['EmplID_Login'].lower(),
            yes_task="updateloginname_16",
            no_task="if_request_eestatus_present_18",
        )

        updateloginname_16 = rail.RepliconServiceOperator(
            task_id='updateloginname_16',
            endpoint="/services/securityservice1.svc/SetSSOAuthenticationForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "loginName": "{{ dag_run.conf.EmplID_Login }}"
            }
        )

        insert_to_update_logs_17 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_17',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Login name updated"
            }
        )

        if_request_eestatus_present_18 = rail.IfOperator(
            task_id='if_request_eestatus_present_18',
            test=lambda dag_run: dag_run.conf['EEStatus'] and rail.result(
                'log_custom_field_values_for_reference_12')['ee_status'].lower() != dag_run.conf['EEStatus'].lower(),
            yes_task="update_text_value_e_estatus_u_d_f_19",
            no_task="if_request_eestatus_equals_to_a_22",
        )

        update_text_value_e_estatus_u_d_f_19 = rail.RepliconServiceOperator(
            task_id='update_text_value_e_estatus_u_d_f_19',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.EEstatusuri }}",
                "value": "{{ dag_run.conf.EEStatus }}"
            }
        )

        insert_to_update_logs_20 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_20',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "EEStatus (UDF) updated"
            }
        )

        if_request_eestatus_equals_to_a_22 = rail.IfOperator(
            task_id='if_request_eestatus_equals_to_a_22',
            test='''{{ dag_run.conf.EEStatus == 'A'  and result('log_custom_field_values_for_reference_12').ee_status == 'L' }}''',
            yes_task="trigger_dag_run_assured_partners_loa_logic_023",
            no_task="if_request_eestatus_equals_to_l_28",
        )

        trigger_dag_run_assured_partners_loa_logic_023 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_assured_partners_loa_logic_023',
            retries=0,
            trigger_dag_id=config.child_loa_logic_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "new_ee_status": dag_run.conf['EEStatus'],
                "loaend": dag_run.conf['LOASuspendPTOEnd'] if dag_run.conf['LOASuspendPTOEnd'] else dag_run.conf['ChangeEffectiveDate'],
                "loastart": dag_run.conf['LOASuspendPTOStart']if dag_run.conf['LOASuspendPTOStart'] else dag_run.conf['ChangeEffectiveDate'],
                "useruri": dag_run.conf['useruri'],
                "previous_ee_status": rail.result('log_custom_field_values_for_reference_12')['ee_status'],
                "type": "na",
                "timesheettemplate": dag_run.conf['TimesheetTemplate'] or null,
                "timeofftemplate": dag_run.conf['TimeOffTemplate'],
                "currenttimesheetperiod": rail.result('log_current_timesheet_period_11') or '',
                "integration_run_date": dag_run.conf['integration_run_date']
            }
        )

        wait_for_completion_trigger_dag_assured_partners_loa_logic_023 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_assured_partners_loa_logic_023',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_assured_partners_loa_logic_023") }}'
        )

        gather_results_from_23_dag_run = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_results_from_23_dag_run',
            dag_runs="{{result('trigger_dag_run_assured_partners_loa_logic_023')}}",
            dagrun_task_id="final_response_from_dag",
            target='result'
        )

        if_error_in_gather_reponse_from_23_dag_run = rail.IfOperator(
            task_id='if_error_in_gather_reponse_from_23_dag_run',
            test=lambda: bool(rail.result("gather_results_from_23_dag_run")) and "Error" in json.dumps(rail.result(
                "gather_results_from_23_dag_run")[0]),
            yes_task="fail_with_error_in_loa_logic",
            no_task="if_request_loasuspendptoend_blank_24",
        )

        fail_with_error_in_loa_logic = rail.FailOperator(
            task_id='fail_with_error_in_loa_logic',
            message="Error while applying LOA Logic"
        )

        if_request_loasuspendptoend_blank_24 = rail.IfOperator(
            task_id='if_request_loasuspendptoend_blank_24',
            test='''{{ dag_run.conf.LOASuspendPTOEnd | is_falsy }}''',
            yes_task="insert_excception_to_logs_25",
            no_task="if_reply_output_present_26",
        )

        insert_excception_to_logs_25 = rail.WriteLogOperator(
            task_id='insert_excception_to_logs_25',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='Exception',
            properties={
                "details": "LOASuspendPTOEnd date is received blank"
            }
        )

        if_reply_output_present_26 = rail.IfOperator(
            task_id='if_reply_output_present_26',
            test=lambda: bool(rail.result("gather_results_from_23_dag_run")
                              and "Error" not in rail.result("gather_results_from_23_dag_run")[0]),
            yes_task="insert_to_update_logs_27",
            no_task="if_request_eestatus_equals_to_l_28",
        )

        insert_to_update_logs_27 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_27',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "LOA logic processed"
            }
        )

        if_request_eestatus_equals_to_l_28 = rail.IfOperator(
            task_id='if_request_eestatus_equals_to_l_28',
            test='''{{ dag_run.conf.EEStatus == 'L'  and result('log_custom_field_values_for_reference_12').ee_status == 'A' }}''',
            yes_task="trigger_dag_run_assured_partners_loa_logic_029",
            no_task="trigger_dag_run_activity_assignment_36",
        )

        trigger_dag_run_assured_partners_loa_logic_029 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_assured_partners_loa_logic_029',
            retries=0,
            trigger_dag_id=config.child_loa_logic_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "new_ee_status": dag_run.conf['EEStatus'],
                "loaend": dag_run.conf['LOASuspendPTOEnd'] if dag_run.conf['LOASuspendPTOEnd'] else dag_run.conf['ChangeEffectiveDate'],
                "loastart": dag_run.conf['LOASuspendPTOStart']if dag_run.conf['LOASuspendPTOStart'] else dag_run.conf['ChangeEffectiveDate'],
                "useruri": dag_run.conf['useruri'],
                "previous_ee_status": rail.result('log_custom_field_values_for_reference_12')['ee_status'],
                "type": "na",
                "timesheettemplate": dag_run.conf['TimesheetTemplate'] or null,
                "timeofftemplate": dag_run.conf['TimeOffTemplate'],
                "currenttimesheetperiod": rail.result('log_current_timesheet_period_11') or '',
                "integration_run_date": dag_run.conf['integration_run_date']
            }
        )

        wait_for_completion_trigger_dag_assured_partners_loa_logic_029 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_assured_partners_loa_logic_029',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_assured_partners_loa_logic_029") }}'
        )

        gather_results_from_29_dag_run = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_results_from_29_dag_run',
            dag_runs="{{result('trigger_dag_run_assured_partners_loa_logic_029')}}",
            dagrun_task_id="final_response_from_dag",
            target='result'
        )

        if_error_in_gather_reponse_from_29_dag_run = rail.IfOperator(
            task_id='if_error_in_gather_reponse_from_29_dag_run',
            test=lambda: bool(rail.result("gather_results_from_29_dag_run")) and "Error" in json.dumps(rail.result(
                "gather_results_from_29_dag_run")[0]),
            yes_task="fail_with_error_in_loa_logic_workflow",
            no_task="if_request_loasuspendptoend_blank_30",
        )

        fail_with_error_in_loa_logic_workflow = rail.FailOperator(
            task_id='fail_with_error_in_loa_logic_workflow',
            message="Error while applying LOA Logic"
        )

        if_request_loasuspendptoend_blank_30 = rail.IfOperator(
            task_id='if_request_loasuspendptoend_blank_30',
            test='''{{ dag_run.conf.LOASuspendPTOEnd | is_falsy }}''',
            yes_task="insert_excception_to_logs_31",
            no_task="if_reply_output_present_32",
        )

        insert_excception_to_logs_31 = rail.WriteLogOperator(
            task_id='insert_excception_to_logs_31',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='Exception',
            properties={
                "details": "LOASuspendPTOEnd date is received blank"
            }
        )

        if_reply_output_present_32 = rail.IfOperator(
            task_id='if_reply_output_present_32',
            test=lambda: bool(rail.result("gather_results_from_29_dag_run")
                              and "Error" not in rail.result("gather_results_from_29_dag_run")[0]),
            yes_task="insert_to_update_logs_33",
            no_task="trigger_dag_run_activity_assignment_36",
        )

        insert_to_update_logs_33 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_33',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "LOA logic processed"
            }
        )

        trigger_dag_run_activity_assignment_36 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_activity_assignment_36',
            retries=0,
            trigger_dag_id=config.child_activity_assignment_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "useruri": dag_run.conf['useruri'],
                "activity": dag_run.conf['activity'] or null,
                "integration_run_date": dag_run.conf['integration_run_date']
            }
        )

        wait_for_completion_trigger_dag_activity_assignment_36 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_activity_assignment_36',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_activity_assignment_36") }}'
        )

        gather_results_from_36_dag_run = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_results_from_36_dag_run',
            dag_runs="{{result('trigger_dag_run_activity_assignment_36')}}",
            dagrun_task_id="final_response_from_dag",
            target='result'
        )

        if_error_in_gather_reponse_from_dag_run_36 = rail.IfOperator(
            task_id='if_error_in_gather_reponse_from_dag_run_36',
            test=lambda: bool(rail.result("gather_results_from_36_dag_run")) and "Error" in json.dumps(rail.result(
                "gather_results_from_36_dag_run")[0]),
            yes_task="fail_with_error_activity_assignment",
            no_task="if_reply_output_present_37",
        )

        fail_with_error_activity_assignment = rail.FailOperator(
            task_id='fail_with_error_activity_assignment',
            message="Error in Activity Assignment for Update User"
        )

        if_reply_output_present_37 = rail.IfOperator(
            task_id='if_reply_output_present_37',
            test=lambda: bool(rail.result("gather_results_from_36_dag_run")),
            yes_task="insert_to_update_logs_38",
            no_task="get_all_permission_sets_39",
        )

        insert_to_update_logs_38 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_38',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "{{result('gather_results_from_36_dag_run')[0]}}"
            }
        )

        get_all_permission_sets_39 = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets_39',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data_handler=lambda response, dag_run: {
                "supervisor": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Supervisor', 'uri')
            }
        )

        log_start_date_timeoff_schedule_hire_date_52_53_55 = rail.PythonOperator(
            task_id='log_start_date_timeoff_schedule_hire_date_52_53_55',
            python_callable=lambda dag_run:  {
                'start_date': python_callable.get_split_date(python_callable.dict_date_to_datetime(rail.result('bulk_get_users3_8')[0]['userDetails']['employmentDateRange']['startDate'])) if rail.result('bulk_get_users3_8')[0]['userDetails']['employmentDateRange']['startDate'] else null,
                'timeoff_schedule': rail.result('bulk_get_users3_8')[0]['timeOffTypePolicySummary']['policiesByTimeOffType'],
                'hire_date': python_callable.get_split_date(dag_run.conf['ServiceDate'])
            }
        )

        if_user_rehire_56 = rail.IfOperator(
            task_id='if_user_rehire_56',
            test='''{{ result('bulk_get_users3_8')[0].userDetails.isEnabled | is_falsy  and dag_run.conf.EEStatus == 'A' }}''',
            yes_task="enable_login_57",
            no_task="if_request_terminationdate_present_63",
        )

        enable_login_57 = rail.RepliconServiceOperator(
            task_id='enable_login_57',
            endpoint="/services/securityservice1.svc/EnableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        log_rehire_58 = rail.PythonOperator(
            task_id='log_rehire_58',
            python_callable=lambda: "Rehire"
        )

        removeenddateon_profile_60 = rail.RepliconServiceOperator(
            task_id='removeenddateon_profile_60',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "userDetailsToApply": {
                        "employmentEndDate": {
                            "date": null
                        },
                        "displayNameParameter": null
                    },
                    "objectExtensionFieldsToApply": []
                }
            }
        )

        removeenddatevalueudfon_profile_61 = rail.RepliconServiceOperator(
            task_id='removeenddatevalueudfon_profile_61',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.enddateudfuri }}",
                "value": null
            }
        )

        update_variable_62 = rail.SetVariableOperator(
            task_id='update_variable_62',
            append=False,
            name='{{ result("declare_variable_5").name }}',
            value="rehire"
        )

        if_request_terminationdate_present_63 = rail.IfOperator(
            task_id='if_request_terminationdate_present_63',
            test='''{{ dag_run.conf.TerminationDate | is_truthy }}''',
            yes_task="if_log_currentenddate_65_blank_66",
            no_task="if_userdetails_isenabled_is_not_true_disable_71",
        )

        def is_termination_date_equal_to_end_date(dag_run):
            current_end_date = python_callable.dict_date_to_datetime(
                rail.result('bulk_get_users3_8')[0]['userDetails']['employmentDateRange']['endDate']) if rail.result(
                    'bulk_get_users3_8')[0]['userDetails']['employmentDateRange']['endDate'] else null

            if current_end_date == null or python_callable.get_split_date(dag_run.conf['TerminationDate'], 'no_split') != current_end_date:
                return True

            return False

        if_log_currentenddate_65_blank_66 = rail.IfOperator(
            task_id='if_log_currentenddate_65_blank_66',
            test=is_termination_date_equal_to_end_date,
            yes_task="update_end_dateon_profile_68",
            no_task="if_userdetails_isenabled_is_not_true_disable_71",
        )

        update_end_dateon_profile_68 = rail.RepliconServiceOperator(
            task_id='update_end_dateon_profile_68',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": rail.result("log_start_date_timeoff_schedule_hire_date_52_53_55")['hire_date'],
                    "endDate": python_callable.get_split_date(dag_run.conf['TerminationDate']),
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        add_end_date_value_udf_on_profile_69 = rail.RepliconServiceOperator(
            task_id='add_end_date_value_udf_on_profile_69',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['useruri'],
                "customFieldUri": dag_run.conf['enddateudfuri'],
                "value": python_callable.get_split_date(dag_run.conf['TerminationDate'])
            }
        )

        insert_to_update_logs_70 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_70',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "End Date updated"
            }
        )

        if_userdetails_isenabled_is_not_true_disable_71 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_not_true_disable_71',
            test=lambda dag_run: not (rail.result('bulk_get_users3_8')[
                                      0]['userDetails']['isEnabled']) and dag_run.conf['EEStatus'] == 'T',
            yes_task="insert_to_update_logs_72",
            no_task="if_request_firstname_present_and_not_equal_to_current_73",
        )

        insert_to_update_logs_72 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_72',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "User already disabled"
            }
        )

        if_request_firstname_present_and_not_equal_to_current_73 = rail.IfOperator(
            task_id='if_request_firstname_present_and_not_equal_to_current_73',
            test=lambda dag_run:  dag_run.conf['FirstName'] and rail.result('bulk_get_users3_8')[
                0]['userDetails']['firstName'].lower() != dag_run.conf['FirstName'].lower(),
            yes_task="update_first_name_74",
            no_task="if_request_lastname_present_and_not_equal_to_current_76",
        )

        update_first_name_74 = rail.RepliconServiceOperator(
            task_id='update_first_name_74',
            endpoint="/services/userService1.svc/UpdateFirstName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "firstname": "{{ dag_run.conf.FirstName }}"
            }
        )

        insert_to_update_logs_75 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_75',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "First name updated"
            }
        )

        if_request_lastname_present_and_not_equal_to_current_76 = rail.IfOperator(
            task_id='if_request_lastname_present_and_not_equal_to_current_76',
            test=lambda dag_run:  dag_run.conf['LastName'] and rail.result('bulk_get_users3_8')[
                0]['userDetails']['lastName'].lower() != dag_run.conf['LastName'].lower(),
            yes_task="update_last_name_77",
            no_task="if_request_e_mail_present_79",
        )

        update_last_name_77 = rail.RepliconServiceOperator(
            task_id='update_last_name_77',
            endpoint="/services/userService1.svc/UpdateLastName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "lastname": "{{ dag_run.conf.LastName }}"
            }
        )

        insert_to_update_logs_78 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_78',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Last name updated"
            }
        )

        if_request_e_mail_present_79 = rail.IfOperator(
            task_id='if_request_e_mail_present_79',
            test=lambda dag_run:  dag_run.conf['E_Mail'] and dag_run.conf['E_Mail'].lower() != (rail.result('bulk_get_users3_8')[
                0]['userDetails']['emailAddress'].lower() if rail.result('bulk_get_users3_8')[0]['userDetails']['emailAddress'] else null),
            yes_task="update_email_80",
            no_task="if_request_jobcode_present_83",
        )

        update_email_80 = rail.RepliconServiceOperator(
            task_id='update_email_80',
            endpoint="/services/userService1.svc/UpdateEmail",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "email": "{{ dag_run.conf.E_Mail }}"
            }
        )

        insert_to_update_logs_81 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_81',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Email updated"
            }
        )

        if_request_jobcode_present_83 = rail.IfOperator(
            task_id='if_request_jobcode_present_83',
            test=lambda dag_run:  dag_run.conf['JobCode'] and rail.result('log_custom_field_values_for_reference_12')[
                'job_code'].lower() != dag_run.conf['JobCode'].lower(),
            yes_task="update_text_value_jobcode_u_d_f_84",
            no_task="if_request_jobcode_blank_86",
        )

        update_text_value_jobcode_u_d_f_84 = rail.RepliconServiceOperator(
            task_id='update_text_value_jobcode_u_d_f_84',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.job_code_udf_uri }}",
                "value": "{{ dag_run.conf.JobCode }}"
            }
        )

        insert_to_update_logs_85 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_85',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Job Code(UDF) updated"
            }
        )

        if_request_jobcode_blank_86 = rail.IfOperator(
            task_id='if_request_jobcode_blank_86',
            test='''{{ dag_run.conf.JobCode | is_falsy }}''',
            yes_task="update_text_value_jobcode_u_d_f_87",
            no_task="if_request_cpnycode_present_90",
        )

        update_text_value_jobcode_u_d_f_87 = rail.RepliconServiceOperator(
            task_id='update_text_value_jobcode_u_d_f_87',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.job_code_udf_uri }}",
                "value": " "
            }
        )

        insert_to_update_logs_88 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_88',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Job Code(UDF) updated"
            }
        )

        if_request_cpnycode_present_90 = rail.IfOperator(
            task_id='if_request_cpnycode_present_90',
            test=lambda dag_run:  dag_run.conf['CpnyCode'] and rail.result('log_custom_field_values_for_reference_12')[
                'cpny_code'].lower() != dag_run.conf['CpnyCode'].lower(),
            yes_task="update_text_value_cpnycode_91",
            no_task="if_request_cpnycode_blank_93",
        )

        update_text_value_cpnycode_91 = rail.RepliconServiceOperator(
            task_id='update_text_value_cpnycode_91',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.cpnycode_udf_uri }}",
                "value": "{{ dag_run.conf.CpnyCode }}"
            }
        )

        insert_to_update_logs_92 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_92',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Job Profile updated"
            }
        )

        if_request_cpnycode_blank_93 = rail.IfOperator(
            task_id='if_request_cpnycode_blank_93',
            test='''{{ dag_run.conf.CpnyCode | is_falsy }}''',
            yes_task="update_text_value_cpnycode_94",
            no_task="if_request_replicontsdate_present_96",
        )

        update_text_value_cpnycode_94 = rail.RepliconServiceOperator(
            task_id='update_text_value_cpnycode_94',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.cpnycode_udf_uri }}",
                "value": " "
            }
        )

        insert_to_update_logs_95 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_95',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Job Profile updated"
            }
        )

        if_request_replicontsdate_present_96 = rail.IfOperator(
            task_id='if_request_replicontsdate_present_96',
            test='''{{ dag_run.conf.RepliconTSDate | is_truthy }}''',
            yes_task="if_log_valuefor_replicon_t_s_date_97_blank_98",
            no_task="if_request_servicedate_present_and_terminationdate_not_present",
        )

        if_log_valuefor_replicon_t_s_date_97_blank_98 = rail.IfOperator(
            task_id='if_log_valuefor_replicon_t_s_date_97_blank_98',
            test=lambda dag_run: not (rail.result('log_custom_field_values_for_reference_12')['replicon_ts_date']) or python_callable.get_split_date(
                rail.result('log_custom_field_values_for_reference_12')['replicon_ts_date'], 'no_split') != python_callable.get_split_date(
                    dag_run.conf['RepliconTSDate'], 'no_split'),
            yes_task="trigger_dag_run_child_workflow_to_add_timeoff_type_for_new_user_99",
            no_task="if_request_servicedate_present_and_terminationdate_not_present",
        )

        trigger_dag_run_child_workflow_to_add_timeoff_type_for_new_user_99 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_workflow_to_add_timeoff_type_for_new_user_99',
            retries=0,
            trigger_dag_id=config.child_workflow_to_add_timeoff_type_for_new_user_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "useruri":  dag_run.conf['useruri'],
                "EEStatus":  dag_run.conf['EEStatus'],
                "EmplID_Login":  dag_run.conf['EmplID_Login'],
                "FirstName":  dag_run.conf['FirstName'],
                "LastName":  dag_run.conf['LastName'],
                "EEType":  dag_run.conf['EEType'],
                "JobCode":  dag_run.conf['JobCode'],
                "JobTitle":  dag_run.conf['JobTitle'],
                "FLSAStatus":  dag_run.conf['FLSAStatus'],
                "ServiceDate":  dag_run.conf['ServiceDate'],
                "TerminationDate":  dag_run.conf['TerminationDate'],
                "Agency_Org2":  dag_run.conf['Agency_Org2'],
                "AgencyDescription":  dag_run.conf['AgencyDescription'],
                "SupervisorID":  dag_run.conf['SupervisorID'],
                "SupervisorName":  dag_run.conf['SupervisorName'],
                "E_Mail":  dag_run.conf['E_Mail'],
                "HourlyRate":  dag_run.conf['HourlyRate'],
                "WeeklySTDHrs":  dag_run.conf['WeeklySTDHrs'],
                "Schedule":  dag_run.conf['Schedule'],
                "PTOSeniorityDate":  dag_run.conf['PTOSeniorityDate'],
                "ProfitCenter":  dag_run.conf['ProfitCenter'],
                "ProfitCenterDescription":  dag_run.conf['ProfitCenterDescription'],
                "CpnyCode":  dag_run.conf['CpnyCode'],
                "PayGroupCode":  dag_run.conf['PayGroupCode'],
                "PayGroup":  dag_run.conf['PayGroup'],
                "PTO_1":  dag_run.conf['PTO_1'],
                "PTO_Bereavement":  dag_run.conf['PTO_Bereavement'],
                "PTO_JuryDuty":  dag_run.conf['PTO_JuryDuty'],
                "HolidayType":  dag_run.conf['HolidayType'],
                "Illness":  dag_run.conf['Illness'],
                "ChangeEffectiveDate":  dag_run.conf['ChangeEffectiveDate'],
                "VTO":  dag_run.conf['VTO'],
                "EmergencySick":  dag_run.conf['EmergencySick'],
                "PayRules":  dag_run.conf['PayRules'],
                "TimesheetTemplate":  dag_run.conf['TimesheetTemplate'],
                "TimeOffTemplate":  dag_run.conf['TimeOffTemplate'],
                "HolidayCalendars":  dag_run.conf['HolidayCalendars'],
                "TimeZone":  dag_run.conf['TimeZone'],
                "WorkWeek":  dag_run.conf['WorkWeek'],
                "LocationCode_Work":  dag_run.conf['LocationCode_Work'],
                "Dept_Org4":  dag_run.conf['Dept_Org4'],
                "Dept_Org4Desc":  dag_run.conf['Dept_Org4Desc'],
                "CoreSupervisorID":  dag_run.conf['CoreSupervisorID'],
                "CoreSupervisorName":  dag_run.conf['CoreSupervisorName'],
                "LOASuspendPTOStart":  dag_run.conf['LOASuspendPTOStart'],
                "LOASuspendPTOEnd":  dag_run.conf['LOASuspendPTOEnd'],
                "agency_org2_department_uri":  dag_run.conf['agency_org2_department_uri'],
                "deptorg4desc_employeetype_uri":  dag_run.conf['deptorg4desc_employeetype_uri'],
                "profitcenter_division_uri":  dag_run.conf['profitcenter_division_uri'],
                "pay_group_code_location_uri":  dag_run.conf['pay_group_code_location_uri'],
                "payroll_grouping_cost_center_uri":  dag_run.conf['payroll_grouping_cost_center_uri'],
                "location_code_work_division_uri":  dag_run.conf['location_code_work_division_uri'],
                "eetype_udf_uri":  dag_run.conf['eetype_udf_uri'],
                "job_code_udf_uri":  dag_run.conf['job_code_udf_uri'],
                "flsastatus_udf_uri":  dag_run.conf['flsastatus_udf_uri'],
                "companyjobdata_udf_uri":  dag_run.conf['companyjobdata_udf_uri'],
                "agencyorg2_udf_uri":  dag_run.conf['agencyorg2_udf_uri'],
                "hourlyrate_udf_uri":  dag_run.conf['hourlyrate_udf_uri'],
                "cpnycode_udf_uri":  dag_run.conf['cpnycode_udf_uri'],
                "pay_group_code_udf_uri":  dag_run.conf['pay_group_code_udf_uri'],
                "location_code_work_udf_uri":  dag_run.conf['location_code_work_udf_uri'],
                "dept_org4_desc_udf_uri":  dag_run.conf['dept_org4_desc_udf_uri'],
                "core_supervisorID_udf_uri":  dag_run.conf['core_supervisorID_udf_uri'],
                "core_supervisor_name_udf_uri":  dag_run.conf['core_supervisor_name_udf_uri'],
                "officeschedule_uri":  dag_run.conf['officeschedule_uri'],
                "type": "add",
                "makeuptimepto":  dag_run.conf['makeuptimepto'],
                "additionaltimeofftypes":  dag_run.conf['AdditionalTimeOffTypes'],
                "tsstartdate": dag_run.conf['RepliconTSDate'] if dag_run.conf['RepliconTSDate'] else dag_run.conf['ServiceDate'],
                "illnesspto":  dag_run.conf['illnesspto'],
                "integration_run_date": dag_run.conf['integration_run_date']
            }
        )

        wait_for_completion_trigger_dag_run_child_workflow_to_add_timeoff_type_for_new_user_v3_099 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_child_workflow_to_add_timeoff_type_for_new_user_v3_099',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_child_workflow_to_add_timeoff_type_for_new_user_99") }}'
        )

        gather_response_from_dag_run_99 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_response_from_dag_run_99',
            dag_runs="{{result('trigger_dag_run_child_workflow_to_add_timeoff_type_for_new_user_99')}}",
            dagrun_task_id="final_response_from_dag",
            target='result'
        )

        if_error_in_gather_reponse_from_dag_run_99 = rail.IfOperator(
            task_id='if_error_in_gather_reponse_from_dag_run_99',
            test=lambda: bool(rail.result("gather_response_from_dag_run_99")) and "Error" in json.dumps(rail.result(
                "gather_response_from_dag_run_99")[0]),
            yes_task="fail_with_error_in_adding_timeoff_type_for_new_user",
            no_task="update_text_value_replicon_t_s_date_101",
        )

        fail_with_error_in_adding_timeoff_type_for_new_user = rail.FailOperator(
            task_id='fail_with_error_in_adding_timeoff_type_for_new_user',
            message="Error in workflow add timeoff type for new user"
        )

        update_text_value_replicon_t_s_date_101 = rail.RepliconServiceOperator(
            task_id='update_text_value_replicon_t_s_date_101',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.replicontsdateudfuri }}",
                "value": "{{ dag_run.conf.RepliconTSDate }}"
            }
        )

        insert_to_update_logs_102 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_102',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Time Off Policies Updated, Replicon TS Date updated"
            }
        )

        if_request_timesheettemplate_present_103 = rail.IfOperator(
            task_id='if_request_timesheettemplate_present_103',
            test='''{{ dag_run.conf.TimesheetTemplate | is_truthy }}''',
            yes_task="put_timesheet_period_schedule_for_user_104",
            no_task="log_replicon_t_s_dateupdated_106",
        )

        put_timesheet_period_schedule_for_user_104 = rail.RepliconServiceOperator(
            task_id='put_timesheet_period_schedule_for_user_104',
            endpoint="/services/TimesheetPeriodService2.svc/PutTimesheetPeriodScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": [{
                    "timesheetPeriod": {
                        "uri": null,
                        "name": "Weekly starting on Sunday"
                    },
                    "effectiveDate": python_callable.get_split_date(dag_run.conf['RepliconTSDate'])
                }]
            }
        )

        insert_to_update_logs_105 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_105',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Timesheet Period updated"
            }
        )

        log_replicon_t_s_dateupdated_106 = rail.PythonOperator(
            task_id='log_replicon_t_s_dateupdated_106',
            python_callable=lambda:  "Replicon TS Date updated"
        )

        if_request_servicedate_present_and_terminationdate_not_present = rail.IfOperator(
            task_id='if_request_servicedate_present_and_terminationdate_not_present',
            test='''{{ dag_run.conf.ServiceDate | is_truthy and dag_run.conf.TerminationDate | is_falsy }}''',
            yes_task="if_log_currentstartdate_blank_or_not_delta",
            no_task="if_ptosenioritydate_present_and_terminationdate_not_present_107",
        )

        if_log_currentstartdate_blank_or_not_delta = rail.IfOperator(
            task_id='if_log_currentstartdate_blank_or_not_delta',
            test=lambda dag_run: not (rail.result('log_start_date_timeoff_schedule_hire_date_52_53_55')['start_date']) or python_callable.get_split_date(
                dag_run.conf['ServiceDate'], 'no_split') != python_callable.dict_date_to_datetime(rail.result('bulk_get_users3_8')[0]['userDetails']['employmentDateRange']['startDate']),
            yes_task="updatestart_date_on_profile",
            no_task="if_ptosenioritydate_present_and_terminationdate_not_present_107",
        )

        updatestart_date_on_profile = rail.RepliconServiceOperator(
            task_id='updatestart_date_on_profile',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": rail.result('log_start_date_timeoff_schedule_hire_date_52_53_55')['hire_date'],
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        insert_to_update_logs_start_date_updated = rail.WriteLogOperator(
            task_id='insert_to_update_logs_start_date_updated',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Start Date updated"
            }
        )

        if_replicon_tsdate_not_updated_and_user_not_rehired = rail.IfOperator(
            task_id='if_replicon_tsdate_not_updated_and_user_not_rehired',
            test=lambda: not (rail.result('log_replicon_t_s_dateupdated_106')) and not (
                rail.result('log_rehire_58')),
            yes_task="trigger_dag_run_child_to_update_timeoff_type_for_seniority_date_or_service_date_change",
            no_task="if_ptosenioritydate_present_and_terminationdate_not_present_107",
        )

        trigger_dag_run_child_to_update_timeoff_type_for_seniority_date_or_service_date_change = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_to_update_timeoff_type_for_seniority_date_or_service_date_change',
            retries=0,
            trigger_dag_id=config.child_update_timeoff_type_for_seniority_date_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "parentjobid": "{{dag_run.conf.parentjobid}}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "EEStatus": "{{ dag_run.conf.EEStatus }}",
                "EmplID_Login": "{{ dag_run.conf.EmplID_Login }}",
                "FirstName": "{{ dag_run.conf.FirstName }}",
                "LastName": "{{ dag_run.conf.LastName }}",
                "EEType": "{{ dag_run.conf.EEType }}",
                "JobCode": "{{ dag_run.conf.JobCode }}",
                "JobTitle": "{{ dag_run.conf.JobTitle }}",
                "FLSAStatus": "{{ dag_run.conf.FLSAStatus }}",
                "ServiceDate": "{{ dag_run.conf.ServiceDate }}",
                "TerminationDate": "{{ dag_run.conf.TerminationDate }}",
                "Agency_Org2": "{{ dag_run.conf.Agency_Org2 }}",
                "AgencyDescription": "{{ dag_run.conf.AgencyDescription }}",
                "SupervisorID": "{{ dag_run.conf.SupervisorID }}",
                "SupervisorName": "{{ dag_run.conf.SupervisorName }}",
                "E_Mail": "{{ dag_run.conf.E_Mail }}",
                "HourlyRate": "{{ dag_run.conf.HourlyRate }}",
                "WeeklySTDHrs": "{{ dag_run.conf.WeeklySTDHrs }}",
                "Schedule": "{{ dag_run.conf.Schedule }}",
                "PTOSeniorityDate": "{{ dag_run.conf.PTOSeniorityDate }}",
                "ProfitCenter": "{{ dag_run.conf.ProfitCenter }}",
                "ProfitCenterDescription": "{{ dag_run.conf.ProfitCenterDescription }}",
                "CpnyCode": "{{ dag_run.conf.CpnyCode }}",
                "PayGroupCode": "{{ dag_run.conf.PayGroupCode }}",
                "PayGroup": "{{ dag_run.conf.PayGroup }}",
                "PTO_1": "{{ dag_run.conf.PTO_1 }}",
                "PTO_Bereavement": "{{ dag_run.conf.PTO_Bereavement }}",
                "PTO_JuryDuty": "{{ dag_run.conf.PTO_JuryDuty }}",
                "HolidayType": "{{ dag_run.conf.HolidayType }}",
                "Illness": "{{ dag_run.conf.Illness }}",
                "ChangeEffectiveDate": "{{ dag_run.conf.ChangeEffectiveDate }}",
                "VTO": "{{ dag_run.conf.VTO }}",
                "EmergencySick": "{{ dag_run.conf.EmergencySick }}",
                "PayRules": "{{ dag_run.conf.PayRules }}",
                "TimesheetTemplate": "{{ dag_run.conf.TimesheetTemplate }}",
                "TimeOffTemplate": "{{ dag_run.conf.TimeOffTemplate }}",
                "HolidayCalendars": "{{ dag_run.conf.HolidayCalendars }}",
                "TimeZone": "{{ dag_run.conf.TimeZone }}",
                "WorkWeek": "{{ dag_run.conf.WorkWeek }}",
                "LocationCode_Work": "{{ dag_run.conf.LocationCode_Work }}",
                "Dept_Org4": "{{ dag_run.conf.Dept_Org4 }}",
                "Dept_Org4Desc": "{{ dag_run.conf.Dept_Org4Desc }}",
                "CoreSupervisorID": "{{ dag_run.conf.CoreSupervisorID }}",
                "CoreSupervisorName": "{{ dag_run.conf.CoreSupervisorName }}",
                "LOASuspendPTOStart": "{{ dag_run.conf.LOASuspendPTOStart }}",
                "LOASuspendPTOEnd": "{{ dag_run.conf.LOASuspendPTOEnd }}",
                "makeuptimepto": "{{ dag_run.conf.makeuptimepto }}",
                "additionaltimeofftypes": "{{ dag_run.conf.AdditionalTimeOffTypes }}",
                "previousstartdate": null,
                "schedulechange": null,
                "previous_schedule": null,
                "loa_stop_accruals": "",
                "type": "seniority date update",
                "loa_return": "",
                "service_date_change": "Yes",
                "integration_run_date": "{{ dag_run.conf.integration_run_date }}"
            }
        )

        wait_for_completion_trigger_dag_run_child_to_update_timeoff_type_for_seniority_date_or_service_date_change = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_child_to_update_timeoff_type_for_seniority_date_or_service_date_change',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_child_to_update_timeoff_type_for_seniority_date_or_service_date_change") }}'
        )

        gather_response_from_dag_run_seniority_date_or_service_date_change = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_response_from_dag_run_seniority_date_or_service_date_change',
            dag_runs="{{result('trigger_dag_run_child_to_update_timeoff_type_for_seniority_date_or_service_date_change')}}",
            dagrun_task_id="final_response_from_dag",
            target='result'
        )

        if_error_in_gather_reponse_from_dag_run_seniority_date_or_service_date_change = rail.IfOperator(
            task_id='if_error_in_gather_reponse_from_dag_run_seniority_date_or_service_date_change',
            test=lambda: bool(rail.result("gather_response_from_dag_run_seniority_date_or_service_date_change")) and "Error" in json.dumps(rail.result(
                "gather_response_from_dag_run_seniority_date_or_service_date_change")[0]),
            yes_task="fail_with_error_in_workflow_to_update_timeoff_type_for_seniority_date_or_service_date_change",
            no_task="if_ptosenioritydate_present_and_terminationdate_not_present_107",
        )

        fail_with_error_in_workflow_to_update_timeoff_type_for_seniority_date_or_service_date_change = rail.FailOperator(
            task_id='fail_with_error_in_workflow_to_update_timeoff_type_for_seniority_date_or_service_date_change',
            message="Error in workflow to update timeoff type for service date change"
        )

        if_ptosenioritydate_present_and_terminationdate_not_present_107 = rail.IfOperator(
            task_id='if_ptosenioritydate_present_and_terminationdate_not_present_107',
            test='''{{ dag_run.conf.PTOSeniorityDate | is_truthy and dag_run.conf.TerminationDate | is_falsy }}''',
            yes_task="if_pto_seniority_date_added_or_changed",
            no_task="if_userdetails_isenabled_is_true_disable_115",
        )

        if_pto_seniority_date_added_or_changed = rail.IfOperator(
            task_id='if_pto_seniority_date_added_or_changed',
            test=lambda dag_run: not (rail.result(
                'log_custom_field_values_for_reference_12')['pto_seniority_date']) or python_callable.dict_date_to_datetime(rail.result(
                    'log_custom_field_values_for_reference_12')['pto_seniority_date']) != python_callable.get_split_date(
                    dag_run.conf['PTOSeniorityDate'], 'no_split'),
            yes_task="update_pto_seniority_date_udf",
            no_task="if_userdetails_isenabled_is_true_disable_115",
        )

        update_pto_seniority_date_udf = rail.RepliconServiceOperator(
            task_id='update_pto_seniority_date_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['useruri'],
                "customFieldUri": dag_run.conf['pto_seniority_date_udf_uri'],
                "value": python_callable.get_split_date(dag_run.conf['PTOSeniorityDate'], 'int')
            }
        )

        insert_to_update_logs_ptosdate = rail.WriteLogOperator(
            task_id='insert_to_update_logs_ptosdate',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "PTO Seniority Date updated"
            }
        )

        if_log_replicon_t_s_dateupdated_106_blank_113 = rail.IfOperator(
            task_id='if_log_replicon_t_s_dateupdated_106_blank_113',
            test=lambda: not (rail.result('log_replicon_t_s_dateupdated_106')) and not (
                rail.result('log_rehire_58')),
            yes_task="trigger_dag_run_child_workflow_to_update_timeoff_type_for_seniority_date_114",
            no_task="if_userdetails_isenabled_is_true_disable_115",
        )

        trigger_dag_run_child_workflow_to_update_timeoff_type_for_seniority_date_114 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_workflow_to_update_timeoff_type_for_seniority_date_114',
            retries=0,
            trigger_dag_id=config.child_update_timeoff_type_for_seniority_date_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "parentjobid": "{{dag_run.conf.parentjobid}}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "EEStatus": "{{ dag_run.conf.EEStatus }}",
                "EmplID_Login": "{{ dag_run.conf.EmplID_Login }}",
                "FirstName": "{{ dag_run.conf.FirstName }}",
                "LastName": "{{ dag_run.conf.LastName }}",
                "EEType": "{{ dag_run.conf.EEType }}",
                "JobCode": "{{ dag_run.conf.JobCode }}",
                "JobTitle": "{{ dag_run.conf.JobTitle }}",
                "FLSAStatus": "{{ dag_run.conf.FLSAStatus }}",
                "ServiceDate": "{{ dag_run.conf.ServiceDate }}",
                "TerminationDate": "{{ dag_run.conf.TerminationDate }}",
                "Agency_Org2": "{{ dag_run.conf.Agency_Org2 }}",
                "AgencyDescription": "{{ dag_run.conf.AgencyDescription }}",
                "SupervisorID": "{{ dag_run.conf.SupervisorID }}",
                "SupervisorName": "{{ dag_run.conf.SupervisorName }}",
                "E_Mail": "{{ dag_run.conf.E_Mail }}",
                "HourlyRate": "{{ dag_run.conf.HourlyRate }}",
                "WeeklySTDHrs": "{{ dag_run.conf.WeeklySTDHrs }}",
                "Schedule": "{{ dag_run.conf.Schedule }}",
                "PTOSeniorityDate": "{{ dag_run.conf.PTOSeniorityDate }}",
                "ProfitCenter": "{{ dag_run.conf.ProfitCenter }}",
                "ProfitCenterDescription": "{{ dag_run.conf.ProfitCenterDescription }}",
                "CpnyCode": "{{ dag_run.conf.CpnyCode }}",
                "PayGroupCode": "{{ dag_run.conf.PayGroupCode }}",
                "PayGroup": "{{ dag_run.conf.PayGroup }}",
                "PTO_1": "{{ dag_run.conf.PTO_1 }}",
                "PTO_Bereavement": "{{ dag_run.conf.PTO_Bereavement }}",
                "PTO_JuryDuty": "{{ dag_run.conf.PTO_JuryDuty }}",
                "HolidayType": "{{ dag_run.conf.HolidayType }}",
                "Illness": "{{ dag_run.conf.Illness }}",
                "ChangeEffectiveDate": "{{ dag_run.conf.ChangeEffectiveDate }}",
                "VTO": "{{ dag_run.conf.VTO }}",
                "EmergencySick": "{{ dag_run.conf.EmergencySick }}",
                "PayRules": "{{ dag_run.conf.PayRules }}",
                "TimesheetTemplate": "{{ dag_run.conf.TimesheetTemplate }}",
                "TimeOffTemplate": "{{ dag_run.conf.TimeOffTemplate }}",
                "HolidayCalendars": "{{ dag_run.conf.HolidayCalendars }}",
                "TimeZone": "{{ dag_run.conf.TimeZone }}",
                "WorkWeek": "{{ dag_run.conf.WorkWeek }}",
                "LocationCode_Work": "{{ dag_run.conf.LocationCode_Work }}",
                "Dept_Org4": "{{ dag_run.conf.Dept_Org4 }}",
                "Dept_Org4Desc": "{{ dag_run.conf.Dept_Org4Desc }}",
                "CoreSupervisorID": "{{ dag_run.conf.CoreSupervisorID }}",
                "CoreSupervisorName": "{{ dag_run.conf.CoreSupervisorName }}",
                "LOASuspendPTOStart": "{{ dag_run.conf.LOASuspendPTOStart }}",
                "LOASuspendPTOEnd": "{{ dag_run.conf.LOASuspendPTOEnd }}",
                "makeuptimepto": "{{ dag_run.conf.makeuptimepto }}",
                "additionaltimeofftypes": "{{ dag_run.conf.AdditionalTimeOffTypes }}",
                "previousstartdate": null,
                "schedulechange": null,
                "previous_schedule": null,
                "loa_stop_accruals": "",
                "type": "seniority date update",
                "loa_return": "",
                "service_date_change": "",
                "integration_run_date": "{{ dag_run.conf.integration_run_date }}"
            }
        )

        wait_for_completion_trigger_dag_run_child_workflow_to_update_timeoff_type_for_seniority_date_114 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_child_workflow_to_update_timeoff_type_for_seniority_date_114',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_child_workflow_to_update_timeoff_type_for_seniority_date_114") }}'
        )

        gather_response_from_dag_run_114 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_response_from_dag_run_114',
            dag_runs="{{result('trigger_dag_run_child_workflow_to_update_timeoff_type_for_seniority_date_114')}}",
            dagrun_task_id="final_response_from_dag",
            target='result'
        )

        if_error_in_gather_reponse_from_dag_run_114 = rail.IfOperator(
            task_id='if_error_in_gather_reponse_from_dag_run_114',
            test=lambda: bool(rail.result("gather_response_from_dag_run_114")) and "Error" in json.dumps(rail.result(
                "gather_response_from_dag_run_114")[0]),
            yes_task="fail_with_error_in_workflow_to_update_timeoff_type_for_seniority_date",
            no_task="if_userdetails_isenabled_is_true_disable_115",
        )

        fail_with_error_in_workflow_to_update_timeoff_type_for_seniority_date = rail.FailOperator(
            task_id='fail_with_error_in_workflow_to_update_timeoff_type_for_seniority_date',
            message="Error in workflow to update timeoff type for PTO seniority date change"
        )

        if_userdetails_isenabled_is_true_disable_115 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_true_disable_115',
            test=lambda dag_run: bool(rail.result('bulk_get_users3_8')[
                0]['userDetails']['isEnabled'] and dag_run.conf['TerminationDate'] and dag_run.conf['EEStatus'] == 'T'),
            yes_task="disable_login_116",
            no_task="if_request_dailyhours_present_119",
        )

        disable_login_116 = rail.RepliconServiceOperator(
            task_id='disable_login_116',
            endpoint="/services/securityservice1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        trigger_dag_run_assured_partners_child_workflow_timeoff_type_for_disable_user_117 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_assured_partners_child_workflow_timeoff_type_for_disable_user_117',
            retries=0,
            trigger_dag_id=config.child_workflow_timeoff_type_for_disable_user_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "useruri":  dag_run.conf['useruri'],
                "EEStatus":  dag_run.conf['EEStatus'],
                "EmplID_Login":  dag_run.conf['EmplID_Login'],
                "FirstName":  dag_run.conf['FirstName'],
                "LastName":  dag_run.conf['LastName'],
                "EEType":  dag_run.conf['EEType'],
                "JobCode":  dag_run.conf['JobCode'],
                "JobTitle":  dag_run.conf['JobTitle'],
                "FLSAStatus":  dag_run.conf['FLSAStatus'],
                "ServiceDate":  dag_run.conf['ServiceDate'],
                "TerminationDate":  dag_run.conf['TerminationDate'],
                "Agency_Org2":  dag_run.conf['Agency_Org2'],
                "AgencyDescription":  dag_run.conf['AgencyDescription'],
                "SupervisorID":  dag_run.conf['SupervisorID'],
                "SupervisorName":  dag_run.conf['SupervisorName'],
                "E_Mail":  dag_run.conf['E_Mail'],
                "HourlyRate":  dag_run.conf['HourlyRate'],
                "WeeklySTDHrs":  dag_run.conf['WeeklySTDHrs'],
                "Schedule":  dag_run.conf['Schedule'],
                "PTOSeniorityDate":  dag_run.conf['PTOSeniorityDate'],
                "ProfitCenter":  dag_run.conf['ProfitCenter'],
                "ProfitCenterDescription":  dag_run.conf['ProfitCenterDescription'],
                "CpnyCode":  dag_run.conf['CpnyCode'],
                "PayGroupCode":  dag_run.conf['PayGroupCode'],
                "PayGroup":  dag_run.conf['PayGroup'],
                "PTO_1":  dag_run.conf['PTO_1'],
                "PTO_Bereavement":  dag_run.conf['PTO_Bereavement'],
                "PTO_JuryDuty":  dag_run.conf['PTO_JuryDuty'],
                "HolidayType":  dag_run.conf['HolidayType'],
                "Illness":  dag_run.conf['Illness'],
                "ChangeEffectiveDate":  dag_run.conf['ChangeEffectiveDate'],
                "VTO":  dag_run.conf['VTO'],
                "EmergencySick":  dag_run.conf['EmergencySick'],
                "PayRules":  dag_run.conf['PayRules'],
                "TimesheetTemplate":  dag_run.conf['TimesheetTemplate'],
                "TimeOffTemplate":  dag_run.conf['TimeOffTemplate'],
                "HolidayCalendars":  dag_run.conf['HolidayCalendars'],
                "TimeZone":  dag_run.conf['TimeZone'],
                "WorkWeek":  dag_run.conf['WorkWeek'],
                "PayrollRegional": null,
                "PayrollGrouping": dag_run.conf['PayrollGrouping'],
                "TimeAdministrator": null,
                "TimeAdministratorGrouping": null,
                "Agency_Access": null,
                "AgencyGrouping": null,
                "LocationCode_Work":  dag_run.conf['LocationCode_Work'],
                "Dept_Org4":  dag_run.conf['Dept_Org4'],
                "Dept_Org4Desc":  dag_run.conf['Dept_Org4Desc'],
                "CoreSupervisorID":  dag_run.conf['CoreSupervisorID'],
                "CoreSupervisorName":  dag_run.conf['CoreSupervisorName'],
                "LOASuspendPTOStart":  dag_run.conf['LOASuspendPTOStart'],
                "LOASuspendPTOEnd":  dag_run.conf['LOASuspendPTOEnd'],
                "type": "terminate",
                "previousstartdate": str(rail.result('bulk_get_users3_8')[0]['userDetails']['employmentDateRange']['startDate']['month']) + "/" + str(
                    rail.result('bulk_get_users3_8')[0]['userDetails']['employmentDateRange']['startDate']['day']) + "/" + str(
                        rail.result('bulk_get_users3_8')[0]['userDetails']['employmentDateRange']['startDate']['year']),
                "integration_run_date": dag_run.conf['integration_run_date'],
            }
        )

        wait_for_completion_trigger_dag_run_assured_partners_child_workflow_timeoff_type_for_disable_user_117 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_assured_partners_child_workflow_timeoff_type_for_disable_user_117',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_assured_partners_child_workflow_timeoff_type_for_disable_user_117") }}'
        )

        gather_response_from_dag_run_117 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_response_from_dag_run_117',
            dag_runs="{{result('trigger_dag_run_assured_partners_child_workflow_timeoff_type_for_disable_user_117')}}",
            dagrun_task_id="final_response_from_dag",
            target='result'
        )

        if_error_in_gather_reponse_from_dag_run_117 = rail.IfOperator(
            task_id='if_error_in_gather_reponse_from_dag_run_117',
            test=lambda: bool(rail.result("gather_response_from_dag_run_117")) and "Error" in json.dumps(rail.result(
                "gather_response_from_dag_run_117")[0]),
            yes_task="fail_with_error_in_timeoff_type_for_disable_user",
            no_task="insert_to_update_logs_118",
        )

        fail_with_error_in_timeoff_type_for_disable_user = rail.FailOperator(
            task_id='fail_with_error_in_timeoff_type_for_disable_user',
            message="Error in workflow for timeoff type for disable user"
        )

        insert_to_update_logs_118 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_118',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "User disbaled"
            }
        )

        if_request_dailyhours_present_119 = rail.IfOperator(
            task_id='if_request_dailyhours_present_119',
            test='''{{ dag_run.conf.DailyHours | is_truthy }}''',
            yes_task="if_log_valuefor_daily_hours_120_blank_121",
            no_task="if_request_agency_org2_present_125",
        )

        if_log_valuefor_daily_hours_120_blank_121 = rail.IfOperator(
            task_id='if_log_valuefor_daily_hours_120_blank_121',
            test=lambda dag_run: not (rail.result('log_custom_field_values_for_reference_12')['daily_hours']) or rail.result(
                'log_custom_field_values_for_reference_12')['daily_hours'] != dag_run.conf['DailyHours'],
            yes_task="update_text_value_daily_hours_122",
            no_task="if_request_agency_org2_present_125",
        )

        update_text_value_daily_hours_122 = rail.RepliconServiceOperator(
            task_id='update_text_value_daily_hours_122',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.dailyhoursudfuri }}",
                "value": "{{ dag_run.conf.DailyHours }}"
            }
        )

        insert_to_update_logs_123 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_123',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Daily hours updated"
            }
        )

        if_request_agency_org2_present_125 = rail.IfOperator(
            task_id='if_request_agency_org2_present_125',
            test=lambda dag_run: dag_run.conf['Agency_Org2'] and dag_run.conf['Agency_Org2'].lower() != rail.result(
                'log_custom_field_values_for_reference_12')['agency_org_2'].lower(),
            yes_task="update_text_value_agency_org2_126",
            no_task="if_request_agency_org2_blank_128",
        )

        update_text_value_agency_org2_126 = rail.RepliconServiceOperator(
            task_id='update_text_value_agency_org2_126',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.agencyorg2_udf_uri }}",
                "value": "{{ dag_run.conf.Agency_Org2 }}"
            }
        )

        insert_to_update_logs_127 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_127',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Agency (Org 2) updated"
            }
        )

        if_request_agency_org2_blank_128 = rail.IfOperator(
            task_id='if_request_agency_org2_blank_128',
            test='''{{ dag_run.conf.Agency_Org2 | is_falsy }}''',
            yes_task="update_text_value_agency_org2_129",
            no_task="if_assignment_number_present_and_not_equals_existing",
        )

        update_text_value_agency_org2_129 = rail.RepliconServiceOperator(
            task_id='update_text_value_agency_org2_129',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.agencyorg2_udf_uri }}",
                "value": " "
            }
        )

        insert_to_update_logs_130 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_130',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Agency (Org 2) updated"
            }
        )

        if_assignment_number_present_and_not_equals_existing = rail.IfOperator(
            task_id='if_assignment_number_present_and_not_equals_existing',
            test=lambda dag_run:  dag_run.conf['AssignmentNumber'] and dag_run.conf['assignmentnumber_udf_uri'] and (
                dag_run.conf['AssignmentNumber'] != rail.result('log_custom_field_values_for_reference_12')['assignment_number']),
            yes_task="update_assignment_number_udf",
            no_task="if_request_hourlyrate_present_132",
        )

        update_assignment_number_udf = rail.RepliconServiceOperator(
            task_id='update_assignment_number_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.assignmentnumber_udf_uri }}",
                "value": "{{ dag_run.conf.AssignmentNumber }}"
            }
        )

        insert_to_update_logs_assignment_number = rail.WriteLogOperator(
            task_id='insert_to_update_logs_assignment_number',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Assignment Number updated"
            }
        )

        if_request_hourlyrate_present_132 = rail.IfOperator(
            task_id='if_request_hourlyrate_present_132',
            test=lambda dag_run: dag_run.conf['HourlyRate'] and dag_run.conf['HourlyRate'].lower(
            ) != rail.result('log_custom_field_values_for_reference_12')['hourly_rate'].lower(),
            yes_task="update_text_value_hourlyrate_133",
            no_task="if_request_hourlyrate_blank_135",
        )

        update_text_value_hourlyrate_133 = rail.RepliconServiceOperator(
            task_id='update_text_value_hourlyrate_133',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.hourlyrate_udf_uri }}",
                "value": "{{ dag_run.conf.HourlyRate }}"
            }
        )

        insert_to_update_logs_134 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_134',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Hourly rate updated"
            }
        )

        if_request_hourlyrate_blank_135 = rail.IfOperator(
            task_id='if_request_hourlyrate_blank_135',
            test='''{{ dag_run.conf.HourlyRate | is_falsy }}''',
            yes_task="update_text_value_hourlyrate_136",
            no_task="if_request_loasuspendptoend_present_139",
        )

        update_text_value_hourlyrate_136 = rail.RepliconServiceOperator(
            task_id='update_text_value_hourlyrate_136',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.hourlyrate_udf_uri }}",
                "value": " "
            }
        )

        insert_to_update_logs_137 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_137',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Hourly rate updated"
            }
        )

        if_request_loasuspendptoend_present_139 = rail.IfOperator(
            task_id='if_request_loasuspendptoend_present_139',
            test=lambda dag_run: dag_run.conf['LOASuspendPTOEnd'] and (rail.result('log_custom_field_values_for_reference_12')['loa_suspend_pto_end'] if rail.result(
                'log_custom_field_values_for_reference_12')['loa_suspend_pto_end'] else null) != python_callable.get_split_date(dag_run.conf['LOASuspendPTOEnd'], 'no_split'),
            yes_task="update_date_value_l_o_a_end_date_141",
            no_task="if_request_loasuspendptostart_present_144",
        )

        update_date_value_l_o_a_end_date_141 = rail.RepliconServiceOperator(
            task_id='update_date_value_l_o_a_end_date_141',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['useruri'],
                "customFieldUri": dag_run.conf['loaenddateuri'],
                "value": python_callable.get_split_date(dag_run.conf['LOASuspendPTOEnd'])
            }
        )

        insert_to_update_logs_142 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_142',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "LOA End Date Updated"
            }
        )

        if_request_loasuspendptostart_present_144 = rail.IfOperator(
            task_id='if_request_loasuspendptostart_present_144',
            test=lambda dag_run: dag_run.conf['LOASuspendPTOStart'] and (rail.result('log_custom_field_values_for_reference_12')['loa_suspend_pto_start'] if rail.result(
                'log_custom_field_values_for_reference_12')['loa_suspend_pto_start'] else null) != python_callable.get_split_date(dag_run.conf['LOASuspendPTOStart'], 'no_split'),
            yes_task="update_date_value_l_o_a_start_date_146",
            no_task="if_request_paygroupcode_present_149",
        )

        update_date_value_l_o_a_start_date_146 = rail.RepliconServiceOperator(
            task_id='update_date_value_l_o_a_start_date_146',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['useruri'],
                "customFieldUri": dag_run.conf['loastartdateuri'],
                "value": python_callable.get_split_date(dag_run.conf['LOASuspendPTOStart'])
            }
        )

        insert_to_update_logs_147 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_147',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "LOA Start Date Updated"
            }
        )

        if_request_paygroupcode_present_149 = rail.IfOperator(
            task_id='if_request_paygroupcode_present_149',
            test=lambda dag_run: dag_run.conf['PayGroupCode'] and dag_run.conf['PayGroupCode'].lower(
            ) != rail.result('log_custom_field_values_for_reference_12')['pay_group_code'].lower(),
            yes_task="update_text_value_paygroupcode_150",
            no_task="if_request_paygroupcode_blank_152",
        )

        update_text_value_paygroupcode_150 = rail.RepliconServiceOperator(
            task_id='update_text_value_paygroupcode_150',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['useruri'],
                "customFieldUri": dag_run.conf['pay_group_code_udf_uri'],
                "value": dag_run.conf['PayGroupCode']
            }
        )

        insert_to_update_logs_151 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_151',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Pay group code updated"
            }
        )

        if_request_paygroupcode_blank_152 = rail.IfOperator(
            task_id='if_request_paygroupcode_blank_152',
            test='''{{ dag_run.conf.PayGroupCode | is_falsy }}''',
            yes_task="update_text_value_paygroupcode_153",
            no_task="if_request_locationcode_work_present_156",
        )

        update_text_value_paygroupcode_153 = rail.RepliconServiceOperator(
            task_id='update_text_value_paygroupcode_153',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.pay_group_code_udf_uri }}",
                "value": " "
            }
        )

        insert_to_update_logs_154 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_154',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Pay group code updated"
            }
        )

        if_request_locationcode_work_present_156 = rail.IfOperator(
            task_id='if_request_locationcode_work_present_156',
            test=lambda dag_run: dag_run.conf['LocationCode_Work'] and dag_run.conf['LocationCode_Work'].lower(
            ) != rail.result('log_custom_field_values_for_reference_12')['location_code_work'].lower(),
            yes_task="update_text_value_locationcodework_157",
            no_task="if_request_locationcode_work_blank_159",
        )

        update_text_value_locationcodework_157 = rail.RepliconServiceOperator(
            task_id='update_text_value_locationcodework_157',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.location_code_work_udf_uri }}",
                "value": "{{ dag_run.conf.LocationCode_Work }}"
            }
        )

        insert_to_update_logs_158 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_158',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Location code (work) updated"
            }
        )

        if_request_locationcode_work_blank_159 = rail.IfOperator(
            task_id='if_request_locationcode_work_blank_159',
            test='''{{ dag_run.conf.LocationCode_Work | is_falsy }}''',
            yes_task="update_text_value_locationcodework_160",
            no_task="if_request_dept_org4desc_present_163",
        )

        update_text_value_locationcodework_160 = rail.RepliconServiceOperator(
            task_id='update_text_value_locationcodework_160',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.location_code_work_udf_uri }}",
                "value": " "
            }
        )

        insert_to_update_logs_161 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_161',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Location code (work) updated"
            }
        )

        if_request_dept_org4desc_present_163 = rail.IfOperator(
            task_id='if_request_dept_org4desc_present_163',
            test=lambda dag_run: dag_run.conf['Dept_Org4Desc'] and dag_run.conf['Dept_Org4Desc'].lower(
            ) != rail.result('log_custom_field_values_for_reference_12')['dept_org_4_desc'].lower(),
            yes_task="update_text_value_dept_org4_desc_164",
            no_task="if_request_dept_org4desc_blank_166",
        )

        update_text_value_dept_org4_desc_164 = rail.RepliconServiceOperator(
            task_id='update_text_value_dept_org4_desc_164',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.dept_org4_desc_udf_uri }}",
                "value": "{{ dag_run.conf.Dept_Org4Desc }}"
            }
        )

        insert_to_update_logs_165 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_165',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Dept (Org 4 Desc) updated"
            }
        )

        if_request_dept_org4desc_blank_166 = rail.IfOperator(
            task_id='if_request_dept_org4desc_blank_166',
            test='''{{ dag_run.conf.Dept_Org4Desc | is_falsy }}''',
            yes_task="update_text_value_dept_org4_desc_167",
            no_task="if_request_coresupervisorid_present_170",
        )

        update_text_value_dept_org4_desc_167 = rail.RepliconServiceOperator(
            task_id='update_text_value_dept_org4_desc_167',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.dept_org4_desc_udf_uri }}",
                "value": " "
            }
        )

        insert_to_update_logs_168 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_168',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Dept (Org 4 Desc) updated"
            }
        )

        if_request_coresupervisorid_present_170 = rail.IfOperator(
            task_id='if_request_coresupervisorid_present_170',
            test=lambda dag_run: dag_run.conf['CoreSupervisorID'] and dag_run.conf['CoreSupervisorID'].lower(
            ) != rail.result('log_custom_field_values_for_reference_12')['core_supervisor_id'].lower(),
            yes_task="update_text_value_core_supervisor_i_d_171",
            no_task="if_request_coresupervisorid_blank_173",
        )

        update_text_value_core_supervisor_i_d_171 = rail.RepliconServiceOperator(
            task_id='update_text_value_core_supervisor_i_d_171',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.core_supervisorID_udf_uri }}",
                "value": "{{ dag_run.conf.CoreSupervisorID }}"
            }
        )

        insert_to_update_logs_172 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_172',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Core Supervisor ID updated"
            }
        )

        if_request_coresupervisorid_blank_173 = rail.IfOperator(
            task_id='if_request_coresupervisorid_blank_173',
            test='''{{ dag_run.conf.CoreSupervisorID | is_falsy }}''',
            yes_task="update_text_value_core_supervisor_i_d_174",
            no_task="if_request_coresupervisorname_present_177",
        )

        update_text_value_core_supervisor_i_d_174 = rail.RepliconServiceOperator(
            task_id='update_text_value_core_supervisor_i_d_174',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.core_supervisorID_udf_uri }}",
                "value": " "
            }
        )

        insert_to_update_logs_175 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_175',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Core Supervisor ID updated"
            }
        )

        if_request_coresupervisorname_present_177 = rail.IfOperator(
            task_id='if_request_coresupervisorname_present_177',
            test=lambda dag_run: dag_run.conf['CoreSupervisorName'] and dag_run.conf['CoreSupervisorName'].lower(
            ) != rail.result('log_custom_field_values_for_reference_12')['core_supervisor_name'].lower(),
            yes_task="update_text_value_core_supervisor_name_178",
            no_task="if_request_coresupervisorname_blank_180",
        )

        update_text_value_core_supervisor_name_178 = rail.RepliconServiceOperator(
            task_id='update_text_value_core_supervisor_name_178',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.core_supervisor_name_udf_uri }}",
                "value": "{{ dag_run.conf.CoreSupervisorName }}"
            }
        )

        insert_to_update_logs_179 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_179',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Core Supervisor Name updated"
            }
        )

        if_request_coresupervisorname_blank_180 = rail.IfOperator(
            task_id='if_request_coresupervisorname_blank_180',
            test='''{{ dag_run.conf.CoreSupervisorName | is_falsy }}''',
            yes_task="update_text_value_core_supervisor_name_181",
            no_task="if_request_eetype_present_184",
        )

        update_text_value_core_supervisor_name_181 = rail.RepliconServiceOperator(
            task_id='update_text_value_core_supervisor_name_181',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.core_supervisor_name_udf_uri }}",
                "value": " "
            }
        )

        insert_to_update_logs_182 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_182',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Core Supervisor Name updated"
            }
        )

        if_request_eetype_present_184 = rail.IfOperator(
            task_id='if_request_eetype_present_184',
            test=lambda dag_run: dag_run.conf['EEType'] and (not (rail.result(
                'log_custom_field_values_for_reference_12')['employee_type']) or dag_run.conf['EEType'].lower(
            ) != rail.result('log_custom_field_values_for_reference_12')['employee_type'].lower()),
            yes_task="get_all_custom_field_drop_down_options_185",
            no_task="if_request_flsastatus_present_193",
        )

        get_all_custom_field_drop_down_options_185 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_options_185',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ dag_run.conf.eetype_udf_uri }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['EEType'], 'uri')
        )

        if_log_e_etypedropdownoptionuri_186_present_187 = rail.IfOperator(
            task_id='if_log_e_etypedropdownoptionuri_186_present_187',
            test='''{{ result('get_all_custom_field_drop_down_options_185') | is_truthy }}''',
            yes_task="update_dropdown_value_e_e_type_188",
            no_task="insert_to_update_logs_191",
        )

        update_dropdown_value_e_e_type_188 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_e_e_type_188',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.eetype_udf_uri }}",
                "customFieldDropDownOptionUri": "{{ result('get_all_custom_field_drop_down_options_185') }}"
            }
        )

        insert_to_update_logs_189 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_189',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Employee type updated"
            }
        )

        insert_to_update_logs_191 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_191',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Employee type not updated since {{ dag_run.conf.EEType }} is not available"
            }
        )

        if_request_flsastatus_present_193 = rail.IfOperator(
            task_id='if_request_flsastatus_present_193',
            test=lambda dag_run: dag_run.conf['FLSAStatus'] and (not (rail.result(
                'log_custom_field_values_for_reference_12')['flsa_status']) or dag_run.conf['FLSAStatus'].lower(
            ) != rail.result('log_custom_field_values_for_reference_12')['flsa_status'].lower()),
            yes_task="get_flsa_custom_field_drop_down_options_194",
            no_task="if_request_payrules_present_201",
        )

        get_flsa_custom_field_drop_down_options_194 = rail.RepliconServiceOperator(
            task_id='get_flsa_custom_field_drop_down_options_194',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ dag_run.conf.flsastatus_udf_uri }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['FLSAStatus'], 'uri')
        )

        if_log_f_l_s_astatusdropdownoptionuri_195_present_196 = rail.IfOperator(
            task_id='if_log_f_l_s_astatusdropdownoptionuri_195_present_196',
            test='''{{ result('get_flsa_custom_field_drop_down_options_194') | is_truthy }}''',
            yes_task="update_dropdown_value_f_l_s_astatus_197",
            no_task="insert_to_update_logs_200",
        )

        update_dropdown_value_f_l_s_astatus_197 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_f_l_s_astatus_197',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.flsastatus_udf_uri }}",
                "customFieldDropDownOptionUri": "{{ result('get_flsa_custom_field_drop_down_options_194') }}"
            }
        )

        insert_to_update_logs_198 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_198',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "FLSA status updated"
            }
        )

        insert_to_update_logs_200 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_200',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "FLSA status not updated since {{ dag_run.conf.FLSAStatus }} is not available"
            }
        )

        if_request_payrules_present_201 = rail.IfOperator(
            task_id='if_request_payrules_present_201',
            test='''{{ dag_run.conf.PayRules | is_truthy }}''',
            yes_task="log_payrule_list_payrule_schedule_current_payrule_name",
            no_task="if_hourly_rate_in_feed",
        )

        log_payrule_list_payrule_schedule_current_payrule_name = rail.PythonOperator(
            task_id='log_payrule_list_payrule_schedule_current_payrule_name',
            python_callable=lambda dag_run: python_callable.get_payrule_list_and_payrule_schedule(
                rail.result('bulk_get_users3_8')[0]['payRuleScriptSchedule'], rail.result('bulk_get_users3_8')[0]['userDetails']['employmentDateRange']['startDate'], dag_run)
        )

        if_log_currentpayrulename_219_blank_220 = rail.IfOperator(
            task_id='if_log_currentpayrulename_219_blank_220',
            test=lambda dag_run: not (rail.result('log_payrule_list_payrule_schedule_current_payrule_name')['current_payrule_name']) or rail.result(
                'log_payrule_list_payrule_schedule_current_payrule_name')['current_payrule_name'].lower() != dag_run.conf['PayRules'].lower(),
            yes_task="get_all_scriptsforpayrule_221",
            no_task="if_hourly_rate_in_feed",
        )

        get_all_scriptsforpayrule_221 = rail.RepliconServiceOperator(
            task_id='get_all_scriptsforpayrule_221',
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['PayRules'], 'uri')
        )

        if_log_payrule_script_uri_222_blank_223 = rail.IfOperator(
            task_id='if_log_payrule_script_uri_222_blank_223',
            test='''{{ result('get_all_scriptsforpayrule_221') | is_falsy }}''',
            yes_task="insert_excception_to_logs_224",
            no_task="log_final_payrule_list_to_assign",
        )

        insert_excception_to_logs_224 = rail.WriteLogOperator(
            task_id='insert_excception_to_logs_224',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='Exception',
            properties={
                "details": "Payrule {{ dag_run.conf.PayRules }} not available in Replicon"
            }
        )

        def get_modified_payrule_list(payrule_list, required_pay_rule_script_uri, dag_run):
            payrule_list.append({
                "effectiveDate": python_callable.get_split_date(dag_run.conf['ChangeEffectiveDate'], 'int'),
                "payRuleScript": {
                    "uri": required_pay_rule_script_uri,
                    "parentUri": null,
                    "name": null
                }
            })
            final_gsub_payrule_list = json.loads(json.dumps(payrule_list, ensure_ascii=False).replace(
                'effectiveDate": {}', 'effectiveDate": null').replace('uri: ""', 'uri: null'))
            return final_gsub_payrule_list

        log_final_payrule_list_to_assign = rail.PythonOperator(
            task_id='log_final_payrule_list_to_assign',
            python_callable=lambda dag_run: get_modified_payrule_list(rail.result('log_payrule_list_payrule_schedule_current_payrule_name')[
                'payrule_list'], rail.result('get_all_scriptsforpayrule_221'), dag_run)
        )

        put_pay_rule_script_assignment_schedule_for_user_228 = rail.RepliconServiceOperator(
            task_id='put_pay_rule_script_assignment_schedule_for_user_228',
            endpoint="/services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": rail.result('log_final_payrule_list_to_assign')
            }
        )

        insert_to_update_logs_229 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_229',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Payrule updated"
            }
        )

        if_hourly_rate_in_feed = rail.IfOperator(
            task_id='if_hourly_rate_in_feed',
            test='''{{ dag_run.conf.hourly_rate_amount | is_truthy }}''',
            yes_task="get_current_hourly_rate_for_user",
            no_task="if_payroll_grouping_present_or_not_costcenter_231",
        )

        get_current_hourly_rate_for_user = rail.PythonOperator(
            task_id='get_current_hourly_rate_for_user',
            python_callable=lambda dag_run: python_callable.get_current_value_from_schedule_list_for_user(rail.result('bulk_get_users3_8')[
                0]['payrollRateSchedule'], 'hourlyRate', 'amount', dag_run, config)
        )

        if_hourlyrate_not_present_or_mismatch_in_existing_and_new = rail.IfOperator(
            task_id='if_hourlyrate_not_present_or_mismatch_in_existing_and_new',
            test=lambda dag_run: not (rail.result('get_current_hourly_rate_for_user')) or (
                float(rail.result('get_current_hourly_rate_for_user')) != float(dag_run.conf['hourly_rate_amount'])),
            yes_task='update_hourlyrate_for_user',
            no_task='if_payroll_grouping_present_or_not_costcenter_231'
        )

        update_hourlyrate_for_user = rail.RepliconServiceOperator(
            task_id='update_hourlyrate_for_user',
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri']
                },
                "modifications": {
                    "payrollRatesModifications": {
                        "scheduleEntriesToAdd": [
                            {
                                "hourlyRate": {
                                    "amount": float(dag_run.conf['hourly_rate_amount']),
                                    "currency": {
                                        "uri": null,
                                        "name": dag_run.conf['hourly_rate_amount_currency_name'],
                                        "symbol": null
                                    }
                                },
                                "effectiveDate": python_callable.get_split_date(dag_run.conf['ChangeEffectiveDate'], 'int')
                            }
                        ],
                        "scheduleEntriesToPut": []
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        insert_to_update_logs_hourlyrate_updated = rail.WriteLogOperator(
            task_id='insert_to_update_logs_hourlyrate_updated',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Pay Rates updated"
            }
        )

        if_payroll_grouping_present_or_not_costcenter_231 = rail.IfOperator(
            task_id='if_payroll_grouping_present_or_not_costcenter_231',
            test='''{{ dag_run.conf.PayrollGrouping | is_truthy or dag_run.conf.PayrollGrouping | is_falsy }}''',
            yes_task="if_request_payrollgrouping_present_250",
            no_task="log_get_service_centre_schedule_list_service_centre_list_current_servicecentre_name",
        )

        if_request_payrollgrouping_present_250 = rail.IfOperator(
            task_id='if_request_payrollgrouping_present_250',
            test='''{{ dag_run.conf.PayrollGrouping | is_truthy }}''',
            yes_task="if_log_currentcostcentername_249_blank_251",
            no_task="if_log_currentcostcentername_249_present_258",
        )

        if_log_currentcostcentername_249_blank_251 = rail.IfOperator(
            task_id='if_log_currentcostcentername_249_blank_251',
            test=lambda dag_run: not (rail.result('get_user_current_group_assignment_details')['current_costcentre_name']) or rail.result(
                'get_user_current_group_assignment_details')['current_costcentre_name'].lower() != dag_run.conf['PayrollGrouping'].lower(),
            yes_task="put_cost_center_schedule_for_user_255",
            no_task="if_log_currentcostcentername_249_present_258",
        )

        put_cost_center_schedule_for_user_255 = rail.RepliconServiceOperator(
            task_id='put_cost_center_schedule_for_user_255',
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
                                        "uri": dag_run.conf['payroll_grouping_cost_center_uri'],
                                        "parentUri": null,
                                        "name": null
                                    },
                                    "effectiveDate": python_callable.get_split_date(dag_run.conf['ChangeEffectiveDate'], 'int')
                                }
                            ],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        insert_to_update_logs_257 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_257',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Payroll grouping (Cost center) updated"
            }
        )

        if_log_currentcostcentername_249_present_258 = rail.IfOperator(
            task_id='if_log_currentcostcentername_249_present_258',
            test=lambda dag_run: rail.result('get_user_current_group_assignment_details')[
                'current_costcentre_name'] and not (dag_run.conf['PayrollGrouping']),
            yes_task="put_cost_center_schedule_for_user_261",
            no_task="log_get_service_centre_schedule_list_service_centre_list_current_servicecentre_name",
        )

        put_cost_center_schedule_for_user_261 = rail.RepliconServiceOperator(
            task_id='put_cost_center_schedule_for_user_261',
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
                                    "costCenter": null,
                                    "effectiveDate": python_callable.get_split_date(dag_run.conf['ChangeEffectiveDate'], 'int')
                                }
                            ],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        insert_to_update_logs_263 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_263',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Payroll grouping (Cost center) updated"
            }
        )

        log_get_service_centre_schedule_list_service_centre_list_current_servicecentre_name = rail.PythonOperator(
            task_id='log_get_service_centre_schedule_list_service_centre_list_current_servicecentre_name',
            python_callable=lambda: {
                'current_servicecentre_name': rail.result('get_user_current_group_assignment_details')['current_servicecenter_name']
            }
        )

        if_request_profitcenter_present_284 = rail.IfOperator(
            task_id='if_request_profitcenter_present_284',
            test='''{{ dag_run.conf.ProfitCenter | is_truthy }}''',
            yes_task="if_log_currentservicecentername_283_blank_285",
            no_task="if_log_currentservicecentername_283_present_291",
        )

        if_log_currentservicecentername_283_blank_285 = rail.IfOperator(
            task_id='if_log_currentservicecentername_283_blank_285',
            test=lambda dag_run: not (rail.result('log_get_service_centre_schedule_list_service_centre_list_current_servicecentre_name')['current_servicecentre_name']) or rail.result(
                'log_get_service_centre_schedule_list_service_centre_list_current_servicecentre_name')['current_servicecentre_name'].lower() != dag_run.conf['ProfitCenter'].lower(),
            yes_task="put_service_center_schedule_for_user_288",
            no_task="if_log_currentservicecentername_283_present_291",
        )

        put_service_center_schedule_for_user_288 = rail.RepliconServiceOperator(
            task_id='put_service_center_schedule_for_user_288',
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri']
                },
                "modifications": {
                    "serviceCenterScheduleToApply": {
                        "userServiceCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementServiceCenterSchedule": [],
                        "updateServiceCenterScheduleOverDateRange": {
                            "replacementServiceCenterScheduleEntries": [
                                {
                                    "serviceCenter": {
                                        "uri": dag_run.conf['profitcenter_division_uri'],
                                        "parentUri": null,
                                        "name": null
                                    },
                                    "effectiveDate": python_callable.get_split_date(dag_run.conf['ChangeEffectiveDate'], 'int')
                                }
                            ],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        insert_to_update_logs_290 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_290',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Profit Center (Service center) updated"
            }
        )

        if_log_currentservicecentername_283_present_291 = rail.IfOperator(
            task_id='if_log_currentservicecentername_283_present_291',
            test=lambda dag_run: rail.result('log_get_service_centre_schedule_list_service_centre_list_current_servicecentre_name')[
                'current_servicecentre_name'] and not (dag_run.conf['ProfitCenter']),
            yes_task="put_service_center_schedule_for_user_294",
            no_task="log_required_department_name_agenceies_297",
        )

        put_service_center_schedule_for_user_294 = rail.RepliconServiceOperator(
            task_id='put_service_center_schedule_for_user_294',
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri']
                },
                "modifications": {
                    "serviceCenterScheduleToApply": {
                        "userServiceCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementServiceCenterSchedule": [],
                        "updateServiceCenterScheduleOverDateRange": {
                            "replacementServiceCenterScheduleEntries": [
                                {
                                    "serviceCenter": null,
                                    "effectiveDate": python_callable.get_split_date(dag_run.conf['ChangeEffectiveDate'], 'int')
                                }
                            ],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        insert_to_update_logs_296 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_296',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Profit Centers (Service center) updated"
            }
        )

        log_required_department_name_agenceies_297 = rail.PythonOperator(
            task_id='log_required_department_name_agenceies_297',
            python_callable=lambda dag_run:  rail.smartjoin_by_delim(
                ("AssuredPartnersInc" + "/" + str(dag_run.conf['Agency_Org2'])).split("/"), "/")
        )

        log_get_department_schedule_list_department_list_current_department_name_AgencyOrg2 = rail.PythonOperator(
            task_id='log_get_department_schedule_list_department_list_current_department_name_AgencyOrg2',
            python_callable=lambda: {
                'current_required_group_name': rail.result('get_user_current_group_assignment_details')['current_department_name']
            }
        )

        if_request_agency_org2_present_318 = rail.IfOperator(
            task_id='if_request_agency_org2_present_318',
            test='''{{ dag_run.conf.Agency_Org2 | is_truthy }}''',
            yes_task="if_log_currentdepartmentname_blank_or_delta_319",
            no_task="if_request_supervisorid_present_330",
        )

        if_log_currentdepartmentname_blank_or_delta_319 = rail.IfOperator(
            task_id='if_log_currentdepartmentname_blank_or_delta_319',
            test=lambda dag_run: not (rail.result('log_get_department_schedule_list_department_list_current_department_name_AgencyOrg2')['current_required_group_name']) or rail.result(
                'log_get_department_schedule_list_department_list_current_department_name_AgencyOrg2')['current_required_group_name'].lower() != dag_run.conf['Agency_Org2'].lower(),
            yes_task="put_department_group_schedule_for_user_322",
            no_task="if_log_currentdepartmentname_317_present_and_new_department_blank_325",
        )

        put_department_group_schedule_for_user_322 = rail.RepliconServiceOperator(
            task_id='put_department_group_schedule_for_user_322',
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri']
                },
                "modifications": {
                    "departmentGroupScheduleToApply": {
                        "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementDepartmentGroupSchedule": [],
                        "updateDepartmentGroupScheduleOverDateRange": {
                            "replacementDepartmentGroupScheduleEntries": [
                                {
                                    "departmentGroup": {
                                        "uri": dag_run.conf['agency_org2_department_uri'],
                                        "parent": null,
                                        "name": null,
                                        "parameterCorrelationId": null
                                    },
                                    "effectiveDate": python_callable.get_split_date(dag_run.conf['ChangeEffectiveDate'], 'int')
                                }
                            ],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        insert_to_update_logs_323 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_323',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Agency (Department group) updated"
            }
        )

        if_log_currentdepartmentname_317_present_and_new_department_blank_325 = rail.IfOperator(
            task_id='if_log_currentdepartmentname_317_present_and_new_department_blank_325',
            test=lambda dag_run: rail.result('log_get_department_schedule_list_department_list_current_department_name_AgencyOrg2')[
                'current_required_group_name'] and not (dag_run.conf['Agency_Org2']),
            yes_task="put_department_group_schedule_for_user_328",
            no_task="if_request_supervisorid_present_330",
        )

        put_department_group_schedule_for_user_328 = rail.RepliconServiceOperator(
            task_id='put_department_group_schedule_for_user_328',
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri']
                },
                "modifications": {
                    "departmentGroupScheduleToApply": {
                        "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementDepartmentGroupSchedule": [],
                        "updateDepartmentGroupScheduleOverDateRange": {
                            "replacementDepartmentGroupScheduleEntries": [
                                {
                                    "departmentGroup": null,
                                    "effectiveDate": python_callable.get_split_date(dag_run.conf['ChangeEffectiveDate'], 'int')
                                }
                            ],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        insert_to_update_logs_329 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_329',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Agencies (Department group) updated"
            }
        )

        if_request_supervisorid_present_330 = rail.IfOperator(
            task_id='if_request_supervisorid_present_330',
            test='''{{ dag_run.conf.SupervisorID | is_truthy }}''',
            yes_task="if_request_supervisorid_equals_to_emplid_login_331",
            no_task="log_get_employee_type_schedule_list_employee_type_list_current_employee_type_name_DeptOrg4desc",
        )

        if_request_supervisorid_equals_to_emplid_login_331 = rail.IfOperator(
            task_id='if_request_supervisorid_equals_to_emplid_login_331',
            test=lambda dag_run: dag_run.conf['SupervisorID'] == dag_run.conf['EmplID_Login'],
            yes_task="insert_excception_to_logs_332",
            no_task="log_get_current_supervisor_name",
        )

        insert_excception_to_logs_332 = rail.WriteLogOperator(
            task_id='insert_excception_to_logs_332',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='Exception',
            properties={
                "details": "Supervisor not updated - Supervisor login name is same as User login name"
            }
        )

        def get_current_supervisor_loginname(user_supervisor_schedule, user_start_date, dag_run):
            supervisor_schedule_list = []
            if 'urn' in json.dumps(user_supervisor_schedule):
                for item in user_supervisor_schedule:
                    if not (item['effectiveDate']):
                        supervisor_schedule_list.append({
                            "loginname": item['supervisor']['user']['loginName'],
                            "uri": item['supervisor']['user']['uri'],
                            "effectivedate": python_callable.dict_date_to_datetime(user_start_date),
                            "name": item['supervisor']['displayText']
                        })
                    elif item['effectiveDate']:
                        if python_callable.dict_date_to_datetime(item['effectiveDate']) < datetime.strptime(
                                dag_run.conf['ChangeEffectiveDate'], config.DATE_DEFAULT_FORMAT).date():
                            supervisor_schedule_list.append({
                                "loginname": item['supervisor']['user']['loginName'],
                                "uri": item['supervisor']['user']['uri'],
                                "effectivedate": python_callable.dict_date_to_datetime(item['effectiveDate']),
                                "name": item['supervisor']['displayText']
                            })
            current_supervisor_name = (max(supervisor_schedule_list, key=lambda y: y['effectivedate']))[
                'loginname'] if supervisor_schedule_list else null

            return current_supervisor_name

        log_get_current_supervisor_name = rail.PythonOperator(
            task_id='log_get_current_supervisor_name',
            python_callable=lambda dag_run: get_current_supervisor_loginname(rail.result('bulk_get_users3_8')[
                0]['supervisorAssignmentSchedule'], rail.result('bulk_get_users3_8')[0]['userDetails']['employmentDateRange']['startDate'], dag_run)
        )

        if_log_currentsupervisorloginname_347_blank_348 = rail.IfOperator(
            task_id='if_log_currentsupervisorloginname_347_blank_348',
            test=lambda dag_run: not (rail.result('log_get_current_supervisor_name')) or rail.result(
                'log_get_current_supervisor_name') != dag_run.conf['SupervisorID'],
            yes_task="search_supervisor_in_replicon_349",
            no_task="log_get_employee_type_schedule_list_employee_type_list_current_employee_type_name_DeptOrg4desc",
        )

        search_supervisor_in_replicon_349 = rail.RepliconServiceOperator(
            task_id='search_supervisor_in_replicon_349',
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.search_supervisor_payload,
            data_handler=lambda response, dag_run: python_callable.get_supervisor_uri_status(
                response, dag_run.conf['SupervisorID'])
        )

        if_log_supervisoruri_350_blank_false_if_log_supervisoruri_350_blank_false_supervisorprofileisnotavailable_351 = rail.IfOperator(
            task_id='if_log_supervisoruri_350_blank_false_if_log_supervisoruri_350_blank_false_supervisorprofileisnotavailable_351',
            test='''{{ result('search_supervisor_in_replicon_349') | is_falsy }}''',
            yes_task="assured_partners_supervisor_assignment_table_add_entry_352",
            no_task="if_log_supervisor_profile_disabled_354",
        )

        assured_partners_supervisor_assignment_table_add_entry_352 = rail.WriteLogOperator(
            task_id='assured_partners_supervisor_assignment_table_add_entry_352',
            log="{{ dag_run.conf.supervisor_assignment_log }}",
            message="na",
            severity="queued",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['parentjobid'],
                "username": dag_run.conf['EmplID_Login'],
                "useruri": dag_run.conf['useruri'],
                "supervisorloginname": dag_run.conf['SupervisorID'],
                "action": "update",
                "childjobid": get_dagrun_ecid(dag_run),
                "status": "queued",
                "supervisoreffectivedate": dag_run.conf['ChangeEffectiveDate'],
                "supervisorusername": dag_run.conf['SupervisorName']
            }
        )

        if_log_supervisor_profile_disabled_354 = rail.IfOperator(
            task_id='if_log_supervisor_profile_disabled_354',
            test=lambda: bool(rail.result('search_supervisor_in_replicon_349')[
                "status"].lower() == 'false'),
            yes_task="assured_partners_supervisor_assignment_table_add_entry_355",
            no_task="if_log_supervisoruri_350_present_356",
        )

        assured_partners_supervisor_assignment_table_add_entry_355 = rail.WriteLogOperator(
            task_id='assured_partners_supervisor_assignment_table_add_entry_355',
            log="{{ dag_run.conf.supervisor_assignment_log }}",
            message="na",
            severity="queued",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['parentjobid'],
                "username": dag_run.conf['EmplID_Login'],
                "useruri":  dag_run.conf['useruri'],
                "supervisorloginname": dag_run.conf['SupervisorID'],
                "action": "update",
                "status": "queued",
                "supervisoreffectivedate": dag_run.conf['ChangeEffectiveDate'],
                "supervisorusername": dag_run.conf['SupervisorName'],
                "childjobid": get_dagrun_ecid(dag_run)
            }
        )

        if_log_supervisoruri_350_present_356 = rail.IfOperator(
            task_id='if_log_supervisoruri_350_present_356',
            test=lambda: rail.result('search_supervisor_in_replicon_349')['uri'] and rail.result(
                'search_supervisor_in_replicon_349')["status"].lower() == 'true',
            yes_task="get_assigned_supervisor_permission_set_for_user_357",
            no_task="log_get_employee_type_schedule_list_employee_type_list_current_employee_type_name_DeptOrg4desc",
        )

        get_assigned_supervisor_permission_set_for_user_357 = rail.RepliconServiceOperator(
            task_id='get_assigned_supervisor_permission_set_for_user_357',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_supervisor_in_replicon_349').uri }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', "urn:replicon:policy:supervision", 'permissionSet.name') if response else null
        )

        if_log_checkif_manager_permissionsetisassigned_358_blank_359 = rail.IfOperator(
            task_id='if_log_checkif_manager_permissionsetisassigned_358_blank_359',
            test='''{{ result('get_assigned_supervisor_permission_set_for_user_357') | is_falsy }}''',
            yes_task="assign_permission_set_to_user_manager_361",
            no_task="update_supervisor_assignment_schedule_over_date_range_362",
        )

        assign_permission_set_to_user_manager_361 = rail.RepliconServiceOperator(
            task_id='assign_permission_set_to_user_manager_361',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('search_supervisor_in_replicon_349').uri }}",
                "permissionSetUri": "{{ result('get_all_permission_sets_39').supervisor }}"
            }
        )

        update_supervisor_assignment_schedule_over_date_range_362 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_362',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": rail.result('search_supervisor_in_replicon_349')['uri'],
                "dateRange": {
                    "startDate": python_callable.get_split_date(dag_run.conf['ChangeEffectiveDate'], 'int'),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        insert_to_update_logs_363 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_363',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Supervisor updated"
            }
        )

        log_get_employee_type_schedule_list_employee_type_list_current_employee_type_name_DeptOrg4desc = rail.PythonOperator(
            task_id='log_get_employee_type_schedule_list_employee_type_list_current_employee_type_name_DeptOrg4desc',
            python_callable=lambda: {
                'current_required_group_name': rail.result('get_user_current_group_assignment_details')['current_employeetype_name']
            }
        )

        if_request_dept_org4desc_present_383 = rail.IfOperator(
            task_id='if_request_dept_org4desc_present_383',
            test='''{{ dag_run.conf.Dept_Org4Desc | is_truthy }}''',
            yes_task="if_log_currentemployeetypename_382_blank_384",
            no_task="if_log_currentemployeetypename_382_present_391",
        )

        if_log_currentemployeetypename_382_blank_384 = rail.IfOperator(
            task_id='if_log_currentemployeetypename_382_blank_384',
            test=lambda dag_run: not (rail.result('log_get_employee_type_schedule_list_employee_type_list_current_employee_type_name_DeptOrg4desc')['current_required_group_name']) or rail.result(
                'log_get_employee_type_schedule_list_employee_type_list_current_employee_type_name_DeptOrg4desc')['current_required_group_name'].lower() != dag_run.conf['Dept_Org4Desc'].lower(),
            yes_task="get_required_employee_type_group_uri_385",
            no_task="if_log_currentemployeetypename_382_present_391",
        )

        get_required_employee_type_group_uri_385 = rail.RepliconServiceOperator(
            task_id='get_required_employee_type_group_uri_385',
            endpoint="/services/EmployeeTypeGroupService1.svc/GetAllEmployeeTypeGroups",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['Dept_Org4Desc'], 'uri')
        )

        put_employee_type_group_schedule_for_user_389 = rail.RepliconServiceOperator(
            task_id='put_employee_type_group_schedule_for_user_389',
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri']
                },
                "modifications": {
                    "employeeTypeGroupScheduleToApply": {
                        "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementEmployeeTypeGroupSchedule": [],
                        "updateEmployeeTypeGroupScheduleOverDateRange": {
                            "replacementEmployeeTypeGroupScheduleEntries": [
                                {
                                    "employeeTypeGroup": {
                                        "uri": rail.result('get_required_employee_type_group_uri_385'),
                                        "parent": null,
                                        "name": null,
                                        "parameterCorrelationId": null
                                    },
                                    "effectiveDate": python_callable.get_split_date(dag_run.conf['ChangeEffectiveDate'], 'int')
                                }
                            ],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        insert_to_update_logs_390 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_390',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Employee type updated"
            }
        )

        if_log_currentemployeetypename_382_present_391 = rail.IfOperator(
            task_id='if_log_currentemployeetypename_382_present_391',
            test=lambda dag_run: rail.result('log_get_employee_type_schedule_list_employee_type_list_current_employee_type_name_DeptOrg4desc')[
                'current_required_group_name'] and not (dag_run.conf['Dept_Org4Desc']),
            yes_task="put_employee_type_group_schedule_for_user_394",
            no_task="log_get_location_schedule_list_location_list_current_location_name_PayGroup",
        )

        put_employee_type_group_schedule_for_user_394 = rail.RepliconServiceOperator(
            task_id='put_employee_type_group_schedule_for_user_394',
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri']
                },
                "modifications": {
                    "employeeTypeGroupScheduleToApply": {
                        "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementEmployeeTypeGroupSchedule": [],
                        "updateEmployeeTypeGroupScheduleOverDateRange": {
                            "replacementEmployeeTypeGroupScheduleEntries": [
                                {
                                    "employeeTypeGroup": null,
                                    "effectiveDate": python_callable.get_split_date(dag_run.conf['ChangeEffectiveDate'], 'int')
                                }
                            ],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        insert_to_update_logs_395 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_395',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Employee type updated"
            }
        )

        log_get_location_schedule_list_location_list_current_location_name_PayGroup = rail.PythonOperator(
            task_id='log_get_location_schedule_list_location_list_current_location_name_PayGroup',
            python_callable=lambda: {
                'current_required_group_name': rail.result('get_user_current_group_assignment_details')['current_location_name']
            }
        )

        if_request_paygroupcode_present_415 = rail.IfOperator(
            task_id='if_request_paygroupcode_present_415',
            test='''{{ dag_run.conf.PayGroupCode | is_truthy }}''',
            yes_task="if_log_currentlocationname_414_blank_416",
            no_task="if_log_currentlocationname_414_present_421",
        )

        if_log_currentlocationname_414_blank_416 = rail.IfOperator(
            task_id='if_log_currentlocationname_414_blank_416',
            test=lambda dag_run: not (rail.result('log_get_location_schedule_list_location_list_current_location_name_PayGroup')['current_required_group_name']) or rail.result(
                'log_get_location_schedule_list_location_list_current_location_name_PayGroup')['current_required_group_name'].lower() != dag_run.conf['PayGroupCode'].lower(),
            yes_task="put_location_schedule_for_user_419",
            no_task="if_log_currentlocationname_414_present_421",
        )

        put_location_schedule_for_user_419 = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user_419',
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri']
                },
                "modifications": {
                    "locationScheduleToApply": {
                        "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementLocationSchedule": [],
                        "updateLocationScheduleOverDateRange": {
                            "replacementLocationScheduleEntries": [
                                {
                                    "location": {
                                        "uri": dag_run.conf['pay_group_code_location_uri'],
                                        "parentUri": null,
                                        "name": null
                                    },
                                    "effectiveDate": python_callable.get_split_date(dag_run.conf['ChangeEffectiveDate'], 'int')
                                }
                            ],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        insert_to_update_logs_420 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_420',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Pay group (Location group) updated"
            }
        )

        if_log_currentlocationname_414_present_421 = rail.IfOperator(
            task_id='if_log_currentlocationname_414_present_421',
            test=lambda dag_run: rail.result('log_get_location_schedule_list_location_list_current_location_name_PayGroup')[
                'current_required_group_name'] and not (dag_run.conf['PayGroup']),
            yes_task="put_location_schedule_for_user_424",
            no_task="log_get_division_schedule_list_division_list_current_division_name_LocationCodeWork",
        )

        put_location_schedule_for_user_424 = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user_424',
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri']
                },
                "modifications": {
                    "locationScheduleToApply": {
                        "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementLocationSchedule": [],
                        "updateLocationScheduleOverDateRange": {
                            "replacementLocationScheduleEntries": [
                                {
                                    "location": null,
                                    "effectiveDate": python_callable.get_split_date(dag_run.conf['ChangeEffectiveDate'], 'int')
                                }
                            ],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        insert_to_update_logs_425 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_425',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Pay group (Location group) updated"
            }
        )

        log_get_division_schedule_list_division_list_current_division_name_LocationCodeWork = rail.PythonOperator(
            task_id='log_get_division_schedule_list_division_list_current_division_name_LocationCodeWork',
            python_callable=lambda: {
                'current_required_group_name': rail.result('get_user_current_group_assignment_details')['current_division_name']
            }
        )

        if_request_locationcode_work_present_445 = rail.IfOperator(
            task_id='if_request_locationcode_work_present_445',
            test='''{{ dag_run.conf.LocationCode_Work | is_truthy }}''',
            yes_task="if_log_current_divisionname_444_blank_446",
            no_task="if_log_current_divisionname_444_present_451",
        )

        if_log_current_divisionname_444_blank_446 = rail.IfOperator(
            task_id='if_log_current_divisionname_444_blank_446',
            test=lambda dag_run: not (rail.result('log_get_division_schedule_list_division_list_current_division_name_LocationCodeWork')['current_required_group_name']) or rail.result(
                'log_get_division_schedule_list_division_list_current_division_name_LocationCodeWork')['current_required_group_name'].lower() != dag_run.conf['LocationCode_Work'].lower(),
            yes_task="put_division_schedule_for_user_449",
            no_task="if_log_current_divisionname_444_present_451",
        )

        put_division_schedule_for_user_449 = rail.RepliconServiceOperator(
            task_id='put_division_schedule_for_user_449',
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
                                        "uri": dag_run.conf['location_code_work_division_uri'],
                                        "parentUri": null,
                                        "name": null
                                    },
                                    "effectiveDate": python_callable.get_split_date(dag_run.conf['ChangeEffectiveDate'], 'int')
                                }
                            ],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        insert_to_update_logs_450 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_450',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Location code work (Division group) updated"
            }
        )

        if_log_current_divisionname_444_present_451 = rail.IfOperator(
            task_id='if_log_current_divisionname_444_present_451',
            test=lambda dag_run: rail.result('log_get_division_schedule_list_division_list_current_division_name_LocationCodeWork')[
                'current_required_group_name'] and not (dag_run.conf['LocationCode_Work']),
            yes_task="put_division_schedule_for_user_454",
            no_task="get_required_policy_set_uris_456",
        )

        put_division_schedule_for_user_454 = rail.RepliconServiceOperator(
            task_id='put_division_schedule_for_user_454',
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
                                    "division": null,
                                    "effectiveDate": python_callable.get_split_date(dag_run.conf['ChangeEffectiveDate'], 'int')
                                }
                            ],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        insert_to_update_logs_455 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_455',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Location code work (Division group) updated"
            }
        )

        get_required_policy_set_uris_456 = rail.RepliconServiceOperator(
            task_id='get_required_policy_set_uris_456',
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
            data_handler=lambda response, dag_run: {
                'punch_entry_policy_uri': rail.find_first_by_attr_and_get_attr(response, 'name', dag_run.conf['punch_entry_policy'], 'uri'),
                'timeoff_template_uri': rail.find_first_by_attr_and_get_attr(response, 'name', dag_run.conf['TimeOffTemplate'], 'uri'),
                'timesheet_template_uri': rail.find_first_by_attr_and_get_attr(response, 'name', dag_run.conf['TimesheetTemplate'], 'uri'),
            }
        )

        if_request_punch_entry_policy_present_457 = rail.IfOperator(
            task_id='if_request_punch_entry_policy_present_457',
            test='''{{ dag_run.conf.punch_entry_policy | is_truthy }}''',
            yes_task="if_punch_entry_policy_not_equals_user_punch_entry_policy_458",
            no_task="if_punch_entry_policy_is_assigned_464",
        )

        if_punch_entry_policy_not_equals_user_punch_entry_policy_458 = rail.IfOperator(
            task_id='if_punch_entry_policy_not_equals_user_punch_entry_policy_458',
            test=lambda dag_run: dag_run.conf['punch_entry_policy'] != rail.result(
                'get_assigned_policy_sets_for_user_14')['assigned_punch_entry_policy'],
            yes_task="assign_policy_set_to_user_460",
            no_task="if_timeofftemplate_displaytext_present_467",
        )

        assign_policy_set_to_user_460 = rail.RepliconServiceOperator(
            task_id='assign_policy_set_to_user_460',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "policySetUri": "{{ result('get_required_policy_set_uris_456').punch_entry_policy_uri }}"
            }
        )

        insert_to_update_logs_461 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_461',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Punch entry policy updated"
            }
        )

        if_punch_entry_policy_is_assigned_464 = rail.IfOperator(
            task_id='if_punch_entry_policy_is_assigned_464',
            test='''{{ result('get_assigned_policy_sets_for_user_14').check_time_punch_entry_policy_assigned | is_truthy }}''',
            yes_task="remove_policy_set_assignment_from_user_465",
            no_task="if_timeofftemplate_displaytext_present_467",
        )

        remove_policy_set_assignment_from_user_465 = rail.RepliconServiceOperator(
            task_id='remove_policy_set_assignment_from_user_465',
            endpoint="/services/PolicySetService1.svc/RemovePolicySetAssignmentFromUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "policySetUri": "{{ result('get_assigned_policy_sets_for_user_14').check_time_punch_entry_policy_assigned }}"
            }
        )

        insert_to_update_logs_466 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_466',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Punch entry policy removed"
            }
        )

        if_timeofftemplate_displaytext_present_467 = rail.IfOperator(
            task_id='if_timeofftemplate_displaytext_present_467',
            test=lambda dag_run: rail.result('bulk_get_users3_8')[
                0]['timeOffTemplate'] and rail.result('bulk_get_users3_8')[
                0]['timeOffTemplate']['displayText'] and not (dag_run.conf['TimeOffTemplate']),
            yes_task="remove_policy_set_assignment_from_user_timeofftemplate_468",
            no_task="if_request_timeofftemplate_present_470",
        )

        remove_policy_set_assignment_from_user_timeofftemplate_468 = rail.RepliconServiceOperator(
            task_id='remove_policy_set_assignment_from_user_timeofftemplate_468',
            endpoint="/services/PolicySetService1.svc/RemovePolicySetAssignmentFromUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "policySetUri": rail.result('bulk_get_users3_8')[0]['timeOffTemplate']['uri']
            }
        )

        insert_to_update_logs_469 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_469',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Timeoff template removed"
            }
        )

        if_request_timeofftemplate_present_470 = rail.IfOperator(
            task_id='if_request_timeofftemplate_present_470',
            test='''{{ dag_run.conf.TimeOffTemplate | is_truthy }}''',
            yes_task="if_timeofftemplate_not_equals_user_existing_timeoff_template_472",
            no_task="if_request_timezone_present_480",
        )

        if_timeofftemplate_not_equals_user_existing_timeoff_template_472 = rail.IfOperator(
            task_id='if_timeofftemplate_not_equals_user_existing_timeoff_template_472',
            test=lambda dag_run: not (rail.result(
                'bulk_get_users3_8')[0]['timeOffTemplate']) or dag_run.conf['TimeOffTemplate'] != rail.result(
                'bulk_get_users3_8')[0]['timeOffTemplate']['name'],
            yes_task="if_log_requiredtimeofftemplateuri_present_474",
            no_task="if_request_timezone_present_480",
        )

        if_log_requiredtimeofftemplateuri_present_474 = rail.IfOperator(
            task_id='if_log_requiredtimeofftemplateuri_present_474',
            test='''{{ result('get_required_policy_set_uris_456').timeoff_template_uri | is_truthy }}''',
            yes_task="assign_policy_set_to_user_timeofftemplate_475",
            no_task="insert_excception_to_logs_478",
        )

        assign_policy_set_to_user_timeofftemplate_475 = rail.RepliconServiceOperator(
            task_id='assign_policy_set_to_user_timeofftemplate_475',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "policySetUri": "{{ result('get_required_policy_set_uris_456').timeoff_template_uri }}"
            }
        )

        insert_to_update_logs_476 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_476',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Timeoff template updated"
            }
        )

        insert_excception_to_logs_478 = rail.WriteLogOperator(
            task_id='insert_excception_to_logs_478',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='Exception',
            properties={
                "details": "Timeoff template {{ dag_run.conf.TimeOffTemplate }} not available in Replicon"
            }
        )

        if_request_timezone_present_480 = rail.IfOperator(
            task_id='if_request_timezone_present_480',
            test='''{{ dag_run.conf.TimeZone | is_truthy }}''',
            yes_task="if_request_timezoneuri_present_481",
            no_task="log_workweektoassign_488",
        )

        if_request_timezoneuri_present_481 = rail.IfOperator(
            task_id='if_request_timezoneuri_present_481',
            test='''{{ dag_run.conf.timezoneuri | is_truthy }}''',
            yes_task="if_request_timezoneuri_not_equals_to_user_existing_timezoneuri_482",
            no_task="insert_excception_to_logs_487",
        )

        if_request_timezoneuri_not_equals_to_user_existing_timezoneuri_482 = rail.IfOperator(
            task_id='if_request_timezoneuri_not_equals_to_user_existing_timezoneuri_482',
            test=lambda dag_run: not (rail.result(
                'bulk_get_users3_8')[0]['timeZone']) or dag_run.conf['timezoneuri'] != rail.result(
                'bulk_get_users3_8')[0]['timeZone']['uri'],
            yes_task="update_time_zone_for_user_483",
            no_task="log_workweektoassign_488",
        )

        update_time_zone_for_user_483 = rail.RepliconServiceOperator(
            task_id='update_time_zone_for_user_483',
            endpoint="/services/InternationalizationService1.svc/UpdateTimeZoneForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "timeZoneUri": "{{ dag_run.conf.timezoneuri }}"
            }
        )

        insert_to_update_logs_484 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_484',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Time zone updated"
            }
        )

        insert_excception_to_logs_487 = rail.WriteLogOperator(
            task_id='insert_excception_to_logs_487',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='Exception',
            properties={
                "details": '''Time Zone "{{ dag_run.conf.TimeZone }}" not available in Replicon'''
            }
        )

        log_workweektoassign_488 = rail.PythonOperator(
            task_id='log_workweektoassign_488',
            python_callable=lambda dag_run:  next(iter(
                filter(lambda x: x["country"] == "global" and x["type"] == "workweek" and x["identifier_1"] == dag_run.conf['WorkWeek'], rail.result('assured_partners_user_sync_master_mapper_search_entries_7'))), {}).get('value', '')
        )

        if_workweekstartday_uri_not_equals_to_user_current_workweekstartday_uri_489 = rail.IfOperator(
            task_id='if_workweekstartday_uri_not_equals_to_user_current_workweekstartday_uri_489',
            test=lambda: rail.result('bulk_get_users3_8')[
                0]['userDetails']['workWeekStartDay']['uri'] != rail.result('log_workweektoassign_488'),
            yes_task="update_work_week_start_day_for_user_490",
            no_task="if_timesheettemplate_name_not_equals_to_user_existing_timesheettemplate_492",
        )

        update_work_week_start_day_for_user_490 = rail.RepliconServiceOperator(
            task_id='update_work_week_start_day_for_user_490',
            endpoint="/services/UserService1.svc/UpdateWorkWeekStartDayForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "dayOfWeekUri": "{{ result('log_workweektoassign_488') }}"
            }
        )

        insert_excception_to_logs_491 = rail.WriteLogOperator(
            task_id='insert_excception_to_logs_491',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Work week updated"
            }
        )

        if_timesheettemplate_name_not_equals_to_user_existing_timesheettemplate_492 = rail.IfOperator(
            task_id='if_timesheettemplate_name_not_equals_to_user_existing_timesheettemplate_492',
            test=lambda dag_run: dag_run.conf['TimesheetTemplate'] and (
                not (rail.result('bulk_get_users3_8')[0]['timesheetTemplate']) or rail.result('bulk_get_users3_8')[
                    0]['timesheetTemplate']['name'] != dag_run.conf['TimesheetTemplate']),
            yes_task="if_log_requiredtimesheettemplateuri_493_present_494",
            no_task="if_timesheettemplate_presence_present_499",
        )

        if_log_requiredtimesheettemplateuri_493_present_494 = rail.IfOperator(
            task_id='if_log_requiredtimesheettemplateuri_493_present_494',
            test='''{{ result('get_required_policy_set_uris_456').timesheet_template_uri | is_truthy }}''',
            yes_task="assign_policy_set_to_user_timesheet_template_495",
            no_task="insert_excception_to_logs_498",
        )

        assign_policy_set_to_user_timesheet_template_495 = rail.RepliconServiceOperator(
            task_id='assign_policy_set_to_user_timesheet_template_495',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "policySetUri": "{{ result('get_required_policy_set_uris_456').timesheet_template_uri  }}"
            }
        )

        insert_to_update_logs_496 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_496',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Timesheet template updated"
            }
        )

        insert_excception_to_logs_498 = rail.WriteLogOperator(
            task_id='insert_excception_to_logs_498',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='Exception',
            properties={
                "details": "Timesheet template {{ dag_run.conf.TimesheetTemplate }} not available in Replicon"
            }
        )

        if_timesheettemplate_presence_present_499 = rail.IfOperator(
            task_id='if_timesheettemplate_presence_present_499',
            test=lambda dag_run: dag_run.conf['TimesheetTemplate'] and not (rail.result(
                'log_current_timesheet_period_11')) and dag_run.conf['EEStatus'] == rail.result('log_custom_field_values_for_reference_12')['ee_status'] and dag_run.conf['EEStatus'] == 'A',
            yes_task="update_timesheet_period_schedule_for_user_500",
            no_task="if_timesheettemplate_presence_blank_502",
        )

        update_timesheet_period_schedule_for_user_500 = rail.RepliconServiceOperator(
            task_id='update_timesheet_period_schedule_for_user_500',
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri']
                },
                "modifications": {
                    "timesheetPeriodScheduleToApply": {
                        "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementTimesheetPeriodSchedule": [],
                        "updateTimesheetPeriodScheduleOverDateRange": {
                            "replacementTimesheetPeriodScheduleEntries": [
                                {
                                    "timesheetPeriod": {
                                        "uri": null,
                                        "name": "Weekly starting on Sunday"
                                    },
                                    "effectiveDate": python_callable.get_split_date(dag_run.conf['integration_run_date'], 'int')
                                }
                            ],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        insert_to_update_logs_501 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_501',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Timesheet Period updated"
            }
        )

        if_timesheettemplate_presence_blank_502 = rail.IfOperator(
            task_id='if_timesheettemplate_presence_blank_502',
            test=lambda dag_run: not (dag_run.conf['TimesheetTemplate']) and rail.result(
                'log_current_timesheet_period_11') and dag_run.conf['EEStatus'] == rail.result('log_custom_field_values_for_reference_12')['ee_status'],
            yes_task="assign_no_timesheet_period_503",
            no_task="if_holidaycalendar_displaytext_not_equals_to_user_existing_holidaycalendar_505",
        )

        assign_no_timesheet_period_503 = rail.RepliconServiceOperator(
            task_id='assign_no_timesheet_period_503',
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri']
                },
                "modifications": {
                    "timesheetPeriodScheduleToApply": {
                        "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementTimesheetPeriodSchedule": [],
                        "updateTimesheetPeriodScheduleOverDateRange": {
                            "replacementTimesheetPeriodScheduleEntries": [
                                {
                                    "timesheetPeriod": null,
                                    "effectiveDate": python_callable.get_split_date(dag_run.conf['integration_run_date'], 'int')
                                }
                            ],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        insert_to_update_logs_504 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_504',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Timesheet Period updated"
            }
        )

        if_holidaycalendar_displaytext_not_equals_to_user_existing_holidaycalendar_505 = rail.IfOperator(
            task_id='if_holidaycalendar_displaytext_not_equals_to_user_existing_holidaycalendar_505',
            test=lambda dag_run: not (rail.result('bulk_get_users3_8')[
                0]['holidayCalendar']) or rail.result('bulk_get_users3_8')[
                0]['holidayCalendar']['displayText'] != dag_run.conf['HolidayCalendars'],
            yes_task="if_request_holidaycalendars_blank_506",
            no_task="get_all_office_schedules",
        )

        if_request_holidaycalendars_blank_506 = rail.IfOperator(
            task_id='if_request_holidaycalendars_blank_506',
            test='''{{ dag_run.conf.HolidayCalendars | is_falsy }}''',
            yes_task="update_holiday_calendar_for_user_507",
            no_task="get_required_holiday_calendar_510",
        )

        update_holiday_calendar_for_user_507 = rail.RepliconServiceOperator(
            task_id='update_holiday_calendar_for_user_507',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "holidayCalendarUri": null
            }
        )

        insert_to_update_logs_508 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_508',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Holiday calendar removed"
            }
        )

        get_required_holiday_calendar_510 = rail.RepliconServiceOperator(
            task_id='get_required_holiday_calendar_510',
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['HolidayCalendars'], 'uri')
        )

        if_log_required_holidaycalendar_511_present_512 = rail.IfOperator(
            task_id='if_log_required_holidaycalendar_511_present_512',
            test='''{{ result('get_required_holiday_calendar_510') | is_truthy }}''',
            yes_task="update_holiday_calendar_for_user_513",
            no_task="insert_excception_to_logs_516",
        )

        update_holiday_calendar_for_user_513 = rail.RepliconServiceOperator(
            task_id='update_holiday_calendar_for_user_513',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "holidayCalendarUri": "{{ result('get_required_holiday_calendar_510') }}"
            }
        )

        insert_to_update_logs_514 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_514',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Holiday calendar updated"
            }
        )

        insert_excception_to_logs_516 = rail.WriteLogOperator(
            task_id='insert_excception_to_logs_516',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='Exception',
            properties={
                "details": "Holiday calendar {{ dag_run.conf.HolidayCalendars }} not available in Replicon"
            }
        )

        get_all_office_schedules = rail.RepliconServiceOperator(
            task_id='get_all_office_schedules',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules"
        )

        if_request_schedule_present_517 = rail.IfOperator(
            task_id='if_request_schedule_present_517',
            test='''{{ dag_run.conf.Schedule | is_truthy }}''',
            yes_task="log_get_officeschedule_schedule_list_officeschedule_list_current_officeschedule_name",
            no_task="if_log_replicon_t_s_dateupdated_106_blank_553",
        )

        log_get_officeschedule_schedule_list_officeschedule_list_current_officeschedule_name = rail.PythonOperator(
            task_id='log_get_officeschedule_schedule_list_officeschedule_list_current_officeschedule_name',
            python_callable=lambda dag_run: python_callable.get_current_value_from_schedule_list_for_user(rail.result('bulk_get_users3_8')[
                0]['schedulePolicies'], 'officeSchedule', 'displayText', dag_run, config)
        )

        if_log_currentofficeschedulename_546_blank_547 = rail.IfOperator(
            task_id='if_log_currentofficeschedulename_546_blank_547',
            test=lambda dag_run: not (rail.result('log_get_officeschedule_schedule_list_officeschedule_list_current_officeschedule_name')) or rail.result(
                'log_get_officeschedule_schedule_list_officeschedule_list_current_officeschedule_name').lower() != dag_run.conf['Schedule'].lower(),
            yes_task="put_schedule_policy_schedule_for_user_550",
            no_task="if_log_replicon_t_s_dateupdated_106_blank_553",
        )

        put_schedule_policy_schedule_for_user_550 = rail.RepliconServiceOperator(
            task_id='put_schedule_policy_schedule_for_user_550',
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
                                        "officeScheduleUri": null,
                                        "name": dag_run.conf['Schedule'],
                                        "officeSchedule": {
                                            "officeScheduleUri": null,
                                            "name": dag_run.conf['Schedule']
                                        },
                                        "scheduleTypeUri": "urn:replicon:schedule-type:shift" if "shift" in rail.result(
                                            'log_get_officeschedule_schedule_list_officeschedule_list_current_officeschedule_name').lower() else "urn:replicon:schedule-type:office-schedule"
                                    },
                                    "effectiveDate": python_callable.get_split_date(dag_run.conf['ChangeEffectiveDate'], 'int')
                                }
                            ],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        insert_to_update_logs_551 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_551',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Office schedule updated"
            }
        )

        log_changeinschedule_552 = rail.PythonOperator(
            task_id='log_changeinschedule_552',
            python_callable=lambda: "change in schedule"
        )

        if_log_replicon_t_s_dateupdated_106_blank_553 = rail.IfOperator(
            task_id='if_log_replicon_t_s_dateupdated_106_blank_553',
            test='''{{ result('log_replicon_t_s_dateupdated_106') | is_falsy }}''',
            yes_task="if_declare_variable_5_value_equals_to_rehire_554",
            no_task="gather_exceptions_from_logs",
        )

        if_declare_variable_5_value_equals_to_rehire_554 = rail.IfOperator(
            task_id='if_declare_variable_5_value_equals_to_rehire_554',
            test=lambda: rail.get_dag_run_var('updatetype') == 'rehire',
            yes_task="trigger_dag_run_child_workflow_to_add_timeoff_type_for_rehire_555",
            no_task="trigger_dag_run_child_workflow_to_add_timeoff_type_for_transfer_559",
        )

        trigger_dag_run_child_workflow_to_add_timeoff_type_for_rehire_555 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_workflow_to_add_timeoff_type_for_rehire_555',
            retries=0,
            trigger_dag_id=config.child_workflow_to_add_timeoff_type_for_rehire_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "useruri":  dag_run.conf['useruri'],
                "EEStatus":  dag_run.conf['EEStatus'],
                "EmplID_Login":  dag_run.conf['EmplID_Login'],
                "FirstName":  dag_run.conf['FirstName'],
                "LastName":  dag_run.conf['LastName'],
                "EEType":  dag_run.conf['EEType'],
                "JobCode":  dag_run.conf['JobCode'],
                "JobTitle":  dag_run.conf['JobTitle'],
                "FLSAStatus":  dag_run.conf['FLSAStatus'],
                "ServiceDate":  dag_run.conf['ServiceDate'],
                "TerminationDate":  dag_run.conf['TerminationDate'],
                "Agency_Org2":  dag_run.conf['Agency_Org2'],
                "AgencyDescription":  dag_run.conf['AgencyDescription'],
                "SupervisorID":  dag_run.conf['SupervisorID'],
                "SupervisorName":  dag_run.conf['SupervisorName'],
                "E_Mail":  dag_run.conf['E_Mail'],
                "HourlyRate":  dag_run.conf['HourlyRate'],
                "WeeklySTDHrs":  dag_run.conf['WeeklySTDHrs'],
                "Schedule":  dag_run.conf['Schedule'],
                "PTOSeniorityDate":  dag_run.conf['PTOSeniorityDate'],
                "ProfitCenter":  dag_run.conf['ProfitCenter'],
                "ProfitCenterDescription":  dag_run.conf['ProfitCenterDescription'],
                "CpnyCode":  dag_run.conf['CpnyCode'],
                "PayGroupCode":  dag_run.conf['PayGroupCode'],
                "PayGroup":  dag_run.conf['PayGroup'],
                "PTO_1":  dag_run.conf['PTO_1'],
                "PTO_Bereavement":  dag_run.conf['PTO_Bereavement'],
                "PTO_JuryDuty":  dag_run.conf['PTO_JuryDuty'],
                "HolidayType":  dag_run.conf['HolidayType'],
                "Illness":  dag_run.conf['Illness'],
                "ChangeEffectiveDate":  dag_run.conf['ChangeEffectiveDate'],
                "VTO":  dag_run.conf['VTO'],
                "EmergencySick":  dag_run.conf['EmergencySick'],
                "PayRules":  dag_run.conf['PayRules'],
                "TimesheetTemplate":  dag_run.conf['TimesheetTemplate'],
                "TimeOffTemplate":  dag_run.conf['TimeOffTemplate'],
                "HolidayCalendars":  dag_run.conf['HolidayCalendars'],
                "TimeZone":  dag_run.conf['TimeZone'],
                "WorkWeek":  dag_run.conf['WorkWeek'],
                "PayrollRegional": null,
                "PayrollGrouping": dag_run.conf['PayrollGrouping'],
                "TimeAdministrator": null,
                "TimeAdministratorGrouping": null,
                "Agency_Access": null,
                "AgencyGrouping": null,
                "LocationCode_Work":  dag_run.conf['LocationCode_Work'],
                "Dept_Org4":  dag_run.conf['Dept_Org4'],
                "Dept_Org4Desc":  dag_run.conf['Dept_Org4Desc'],
                "CoreSupervisorID":  dag_run.conf['CoreSupervisorID'],
                "CoreSupervisorName":  dag_run.conf['CoreSupervisorName'],
                "LOASuspendPTOStart":  dag_run.conf['LOASuspendPTOStart'],
                "LOASuspendPTOEnd":  dag_run.conf['LOASuspendPTOEnd'],
                "type": rail.get_dag_run_var('updatetype'),
                "previousstartdate": str(rail.result('bulk_get_users3_8')[0]['userDetails']['employmentDateRange']['startDate']['month']) + "/" + str(
                    rail.result('bulk_get_users3_8')[0]['userDetails']['employmentDateRange']['startDate']['day']) + "/" + str(
                    rail.result('bulk_get_users3_8')[0]['userDetails']['employmentDateRange']['startDate']['year']),
                "officeschedule_uri": null,
                "makeuptimepto": dag_run.conf['makeuptimepto'],
                "additionaltimeofftypes": dag_run.conf['AdditionalTimeOffTypes'],
                "tsstartdate": dag_run.conf['RepliconTSDate'] or dag_run.conf['ServiceDate'],
                "illnesspto": dag_run.conf['illnesspto'],
                "currentschedule": rail.result(
                    'log_get_officeschedule_schedule_list_officeschedule_list_current_officeschedule_name') if rail.result(
                    'log_get_officeschedule_schedule_list_officeschedule_list_current_officeschedule_name') else null,
                "flsa_changed": rail.result('get_flsa_custom_field_drop_down_options_194') or null,
                "currentscheduleuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_office_schedules'), 'displayText', rail.result(
                    'log_get_officeschedule_schedule_list_officeschedule_list_current_officeschedule_name'), 'uri'),
                "schedulechange": "yes" if rail.result('log_changeinschedule_552') else "no",
                "integration_run_date": dag_run.conf['integration_run_date'],
            }
        )

        wait_for_completion_trigger_dag_run_child_workflow_to_add_timeoff_type_for_rehire_555 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_child_workflow_to_add_timeoff_type_for_rehire_555',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_child_workflow_to_add_timeoff_type_for_rehire_555") }}'
        )

        gather_response_from_dag_run_555 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_response_from_dag_run_555',
            dag_runs="{{result('trigger_dag_run_child_workflow_to_add_timeoff_type_for_rehire_555')}}",
            dagrun_task_id="final_response_from_dag",
            target='result'
        )

        if_error_in_gather_reponse_from_dag_run_555 = rail.IfOperator(
            task_id='if_error_in_gather_reponse_from_dag_run_555',
            test=lambda: bool(rail.result("gather_response_from_dag_run_555")) and "Error" in json.dumps(rail.result(
                "gather_response_from_dag_run_555")[0]),
            yes_task="fail_with_error_in_add_timeoff_type_for_rehire",
            no_task="updaterehiredate_556",
        )

        fail_with_error_in_add_timeoff_type_for_rehire = rail.FailOperator(
            task_id='fail_with_error_in_add_timeoff_type_for_rehire',
            message="Error in workflow for adding timeoff type for rehire user"
        )

        updaterehiredate_556 = rail.RepliconServiceOperator(
            task_id='updaterehiredate_556',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": rail.result("log_start_date_timeoff_schedule_hire_date_52_53_55")['hire_date'],
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        insert_to_update_logs_557 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_557',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Time off types updated"
            }
        )

        trigger_dag_run_child_workflow_to_add_timeoff_type_for_transfer_559 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_child_workflow_to_add_timeoff_type_for_transfer_559',
            retries=0,
            trigger_dag_id=config.child_workflow_to_add_timeoff_type_for_transfer_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "useruri":  dag_run.conf['useruri'],
                "EEStatus":  dag_run.conf['EEStatus'],
                "EmplID_Login":  dag_run.conf['EmplID_Login'],
                "FirstName":  dag_run.conf['FirstName'],
                "LastName":  dag_run.conf['LastName'],
                "EEType":  dag_run.conf['EEType'],
                "JobCode":  dag_run.conf['JobCode'],
                "JobTitle":  dag_run.conf['JobTitle'],
                "FLSAStatus":  dag_run.conf['FLSAStatus'],
                "ServiceDate":  dag_run.conf['ServiceDate'],
                "TerminationDate":  dag_run.conf['TerminationDate'],
                "Agency_Org2":  dag_run.conf['Agency_Org2'],
                "AgencyDescription":  dag_run.conf['AgencyDescription'],
                "SupervisorID":  dag_run.conf['SupervisorID'],
                "SupervisorName":  dag_run.conf['SupervisorName'],
                "E_Mail":  dag_run.conf['E_Mail'],
                "HourlyRate":  dag_run.conf['HourlyRate'],
                "WeeklySTDHrs":  dag_run.conf['WeeklySTDHrs'],
                "Schedule":  dag_run.conf['Schedule'],
                "PTOSeniorityDate":  dag_run.conf['PTOSeniorityDate'],
                "ProfitCenter":  dag_run.conf['ProfitCenter'],
                "ProfitCenterDescription":  dag_run.conf['ProfitCenterDescription'],
                "CpnyCode":  dag_run.conf['CpnyCode'],
                "PayGroupCode":  dag_run.conf['PayGroupCode'],
                "PayGroup":  dag_run.conf['PayGroup'],
                "PTO_1":  dag_run.conf['PTO_1'],
                "PTO_Bereavement":  dag_run.conf['PTO_Bereavement'],
                "PTO_JuryDuty":  dag_run.conf['PTO_JuryDuty'],
                "HolidayType":  dag_run.conf['HolidayType'],
                "Illness":  dag_run.conf['Illness'],
                "ChangeEffectiveDate":  dag_run.conf['ChangeEffectiveDate'],
                "VTO":  dag_run.conf['VTO'],
                "EmergencySick":  dag_run.conf['EmergencySick'],
                "PayRules":  dag_run.conf['PayRules'],
                "TimesheetTemplate":  dag_run.conf['TimesheetTemplate'],
                "TimeOffTemplate":  dag_run.conf['TimeOffTemplate'],
                "HolidayCalendars":  dag_run.conf['HolidayCalendars'],
                "TimeZone":  dag_run.conf['TimeZone'],
                "WorkWeek":  dag_run.conf['WorkWeek'],
                "LocationCode_Work":  dag_run.conf['LocationCode_Work'],
                "Dept_Org4":  dag_run.conf['Dept_Org4'],
                "Dept_Org4Desc":  dag_run.conf['Dept_Org4Desc'],
                "CoreSupervisorID":  dag_run.conf['CoreSupervisorID'],
                "CoreSupervisorName":  dag_run.conf['CoreSupervisorName'],
                "LOASuspendPTOStart":  dag_run.conf['LOASuspendPTOStart'],
                "LOASuspendPTOEnd":  dag_run.conf['LOASuspendPTOEnd'],
                "type": rail.get_dag_run_var('updatetype'),
                "previousstartdate": str(rail.result(
                    'bulk_get_users3_8')[0]['userDetails']['employmentDateRange']['startDate']['month']) + "/" + str(rail.result(
                        'bulk_get_users3_8')[0]['userDetails']['employmentDateRange']['startDate']['day']) + "/" + str(rail.result(
                            'bulk_get_users3_8')[0]['userDetails']['employmentDateRange']['startDate']['year']),
                "makeuptimepto": dag_run.conf['makeuptimepto'],
                "previous_schedule": rail.result(
                    'log_get_officeschedule_schedule_list_officeschedule_list_current_officeschedule_name') if rail.result(
                    'log_get_officeschedule_schedule_list_officeschedule_list_current_officeschedule_name') else null,
                "loa_stop_accruals": "yes" if rail.result('gather_results_from_29_dag_run') else "no",
                "loa_return": "no" if rail.result('gather_results_from_29_dag_run') else ("yes" if rail.result('gather_results_from_23_dag_run') else "no"),
                "additionaltimeofftypes": dag_run.conf['AdditionalTimeOffTypes'],
                "tsstartdate": dag_run.conf['RepliconTSDate'] or dag_run.conf['ServiceDate'],
                "illnesspto": dag_run.conf['illnesspto'],
                "currentschedule": rail.result(
                    'log_get_officeschedule_schedule_list_officeschedule_list_current_officeschedule_name') if rail.result(
                    'log_get_officeschedule_schedule_list_officeschedule_list_current_officeschedule_name') else null,
                "flsa_changed": rail.result('get_flsa_custom_field_drop_down_options_194') or null,
                "currentscheduleuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_office_schedules'), 'displayText', rail.result(
                    'log_get_officeschedule_schedule_list_officeschedule_list_current_officeschedule_name'), 'uri'),
                "schedulechange": "yes" if rail.result('log_changeinschedule_552') else "no",
                "integration_run_date": dag_run.conf['integration_run_date']
            }
        )

        wait_for_completion_trigger_dag_run_child_workflow_to_add_timeoff_type_for_transfer_559 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_child_workflow_to_add_timeoff_type_for_transfer_559',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_child_workflow_to_add_timeoff_type_for_transfer_559") }}'
        )

        gather_response_from_dag_run_559 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_response_from_dag_run_559',
            dag_runs="{{result('trigger_dag_run_child_workflow_to_add_timeoff_type_for_transfer_559')}}",
            dagrun_task_id="final_response_from_dag",
            target='result'
        )

        if_error_in_gather_reponse_from_dag_run_559 = rail.IfOperator(
            task_id='if_error_in_gather_reponse_from_dag_run_559',
            test=lambda: bool(rail.result("gather_response_from_dag_run_559")) and "Error" in json.dumps(rail.result(
                "gather_response_from_dag_run_559")[0]),
            yes_task="fail_with_error_in_add_timeoff_type_for_transfer",
            no_task="if_exception_in_gather_reponse_from_dag_run_559",
        )

        fail_with_error_in_add_timeoff_type_for_transfer = rail.FailOperator(
            task_id='fail_with_error_in_add_timeoff_type_for_transfer',
            message="Error in workflow for adding timeoff type for transfer user"
        )

        if_exception_in_gather_reponse_from_dag_run_559 = rail.IfOperator(
            task_id='if_exception_in_gather_reponse_from_dag_run_559',
            test=lambda: bool(rail.result("gather_response_from_dag_run_559")) and "Exception" in json.dumps(rail.result(
                "gather_response_from_dag_run_559")[0]),
            yes_task="insert_to_exception_logs",
            no_task="insert_to_update_logs_560",
        )

        def get_exception_from_gather_results(gather_response_result):
            exceptions = []
            for item in gather_response_result:
                if 'Exception :' in json.dumps(item):
                    exceptions.append(str(item).replace('Exception :', ''))
            return ';'.join(exceptions) if exceptions else ''

        insert_to_exception_logs = rail.WriteLogOperator(
            task_id='insert_to_exception_logs',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='Exception',
            properties=lambda: {
                "details": get_exception_from_gather_results(rail.result('gather_response_from_dag_run_559')[0])
            }
        )

        insert_to_update_logs_560 = rail.WriteLogOperator(
            task_id='insert_to_update_logs_560',
            log="{{result('update_and_exception_logs')}}",
            message='na',
            severity='record_updated',
            properties={
                "details": "Time off types updated"
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
            python_callable=lambda:  python_callable.sort_updates_exceptions_logs(
                rail.result("gather_exceptions_from_logs"), rail.result("gather_update_entries_from_logs"))
        )

        assured_partners_user_sync_logs_add_entry_561 = rail.WriteLogOperator(
            task_id='assured_partners_user_sync_logs_add_entry_561',
            log="{{ dag_run.conf.user_import_log }}",
            message="na",
            severity=lambda: rail.result(
                'log_status_details_for_exceptions_updates_skipped_record')['status'],
            properties=lambda dag_run: {
                "action": "Update",
                "status": rail.result('log_status_details_for_exceptions_updates_skipped_record')['status'],
                "job_id": dag_run.conf['parentjobid'],
                "details": rail.result('log_status_details_for_exceptions_updates_skipped_record')['details'],
                "username": dag_run.conf['FirstName'] + " " + dag_run.conf['LastName'],
                "loginname": dag_run.conf['EmplID_Login'],
                "childjobid": get_dagrun_ecid(dag_run)
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log="{{ dag_run.conf.user_import_log }}",
            message="na",
            severity="Error",
            properties=lambda dag_run: {
                "action": "Update",
                "status": "Error",
                "job_id": dag_run.conf['parentjobid'],
                "details": rail.render_template("{{get_error_message()}}"),
                "username": dag_run.conf['FirstName'] + " " + dag_run.conf['LastName'],
                "loginname": dag_run.conf['EmplID_Login'],
                "childjobid": get_dagrun_ecid(dag_run)
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> update_and_exception_logs

        update_and_exception_logs >> declare_variable_5 >> assured_partners_user_sync_master_mapper_search_entries_7 >> bulk_get_users3_8 \
            >> get_user_current_group_assignment_details >> get_timesheet_period_schedule_for_user_9 >> log_current_timesheet_period_11 \
            >> log_custom_field_values_for_reference_12 >> if_change_effective_date_present_in_input_and_is_delta

        if_change_effective_date_present_in_input_and_is_delta >> rail.Label(
            'No') >> get_assigned_policy_sets_for_user_14
        if_change_effective_date_present_in_input_and_is_delta >> rail.Label('Yes') >> update_change_effective_date_udf \
            >> insert_to_update_logs_cef >> get_assigned_policy_sets_for_user_14

        get_assigned_policy_sets_for_user_14 >> if_loginname_not_equals_to_emplid_15

        if_loginname_not_equals_to_emplid_15 >> rail.Label(
            'Yes') >> updateloginname_16 >> insert_to_update_logs_17 >> if_request_eestatus_present_18
        if_loginname_not_equals_to_emplid_15 >> rail.Label(
            'No') >> if_request_eestatus_present_18

        if_request_eestatus_present_18 >> rail.Label(
            'Yes') >> update_text_value_e_estatus_u_d_f_19 >> insert_to_update_logs_20 >> if_request_eestatus_equals_to_a_22
        if_request_eestatus_present_18 >> rail.Label(
            'No') >> if_request_eestatus_equals_to_a_22

        if_request_eestatus_equals_to_a_22 >> rail.Label(
            'No') >> if_request_eestatus_equals_to_l_28
        if_request_eestatus_equals_to_a_22 >> rail.Label('Yes') >> trigger_dag_run_assured_partners_loa_logic_023 \
            >> wait_for_completion_trigger_dag_assured_partners_loa_logic_023 >> gather_results_from_23_dag_run >> if_error_in_gather_reponse_from_23_dag_run

        if_error_in_gather_reponse_from_23_dag_run >> rail.Label(
            'Yes') >> fail_with_error_in_loa_logic >> if_request_loasuspendptoend_blank_24
        if_error_in_gather_reponse_from_23_dag_run >> rail.Label(
            'No') >> if_request_loasuspendptoend_blank_24

        if_request_loasuspendptoend_blank_24 >> rail.Label(
            'Yes') >> insert_excception_to_logs_25 >> if_reply_output_present_26
        if_request_loasuspendptoend_blank_24 >> rail.Label(
            'No') >> if_reply_output_present_26

        if_reply_output_present_26 >> rail.Label(
            'Yes') >> insert_to_update_logs_27 >> if_request_eestatus_equals_to_l_28
        if_reply_output_present_26 >> rail.Label(
            'No') >> if_request_eestatus_equals_to_l_28

        if_request_eestatus_equals_to_l_28 >> rail.Label(
            'No') >> trigger_dag_run_activity_assignment_36
        if_request_eestatus_equals_to_l_28 >> rail.Label('Yes') >> trigger_dag_run_assured_partners_loa_logic_029 \
            >> wait_for_completion_trigger_dag_assured_partners_loa_logic_029 >> gather_results_from_29_dag_run >> if_error_in_gather_reponse_from_29_dag_run

        if_error_in_gather_reponse_from_29_dag_run >> rail.Label(
            'No') >> if_request_loasuspendptoend_blank_30
        if_error_in_gather_reponse_from_29_dag_run >> rail.Label(
            'Yes') >> fail_with_error_in_loa_logic_workflow >> if_request_loasuspendptoend_blank_30

        if_request_loasuspendptoend_blank_30 >> rail.Label(
            'No') >> if_reply_output_present_32
        if_request_loasuspendptoend_blank_30 >> rail.Label(
            'Yes') >> insert_excception_to_logs_31 >> if_reply_output_present_32

        if_reply_output_present_32 >> rail.Label(
            'No') >> trigger_dag_run_activity_assignment_36
        if_reply_output_present_32 >> rail.Label(
            'Yes') >> insert_to_update_logs_33 >> trigger_dag_run_activity_assignment_36

        trigger_dag_run_activity_assignment_36 >> wait_for_completion_trigger_dag_activity_assignment_36 >> gather_results_from_36_dag_run >> if_error_in_gather_reponse_from_dag_run_36

        if_error_in_gather_reponse_from_dag_run_36 >> rail.Label(
            'Yes') >> fail_with_error_activity_assignment >> if_reply_output_present_37
        if_error_in_gather_reponse_from_dag_run_36 >> rail.Label(
            'No') >> if_reply_output_present_37

        if_reply_output_present_37 >> rail.Label(
            'Yes') >> insert_to_update_logs_38 >> get_all_permission_sets_39
        if_reply_output_present_37 >> rail.Label(
            'No') >> get_all_permission_sets_39

        get_all_permission_sets_39 >> log_start_date_timeoff_schedule_hire_date_52_53_55

        log_start_date_timeoff_schedule_hire_date_52_53_55 >> if_user_rehire_56

        if_user_rehire_56 >> rail.Label(
            'No') >> if_request_terminationdate_present_63
        if_user_rehire_56 >> rail.Label('Yes') >> enable_login_57 >> log_rehire_58 >> removeenddateon_profile_60 >> removeenddatevalueudfon_profile_61 \
            >> update_variable_62 >> if_request_terminationdate_present_63

        if_request_terminationdate_present_63 >> rail.Label(
            'No') >> if_userdetails_isenabled_is_not_true_disable_71
        if_request_terminationdate_present_63 >> rail.Label(
            'Yes') >> if_log_currentenddate_65_blank_66

        if_log_currentenddate_65_blank_66 >> rail.Label(
            'No') >> if_userdetails_isenabled_is_not_true_disable_71
        if_log_currentenddate_65_blank_66 >> rail.Label('Yes') >> update_end_dateon_profile_68 >> add_end_date_value_udf_on_profile_69 \
            >> insert_to_update_logs_70 >> if_userdetails_isenabled_is_not_true_disable_71

        if_userdetails_isenabled_is_not_true_disable_71 >> rail.Label(
            'Yes') >> insert_to_update_logs_72 >> if_request_firstname_present_and_not_equal_to_current_73
        if_userdetails_isenabled_is_not_true_disable_71 >> rail.Label(
            'No') >> if_request_firstname_present_and_not_equal_to_current_73

        if_request_firstname_present_and_not_equal_to_current_73 >> rail.Label(
            'Yes') >> update_first_name_74 >> insert_to_update_logs_75 >> if_request_lastname_present_and_not_equal_to_current_76
        if_request_firstname_present_and_not_equal_to_current_73 >> rail.Label(
            'No') >> if_request_lastname_present_and_not_equal_to_current_76

        if_request_lastname_present_and_not_equal_to_current_76 >> rail.Label(
            'No') >> if_request_e_mail_present_79
        if_request_lastname_present_and_not_equal_to_current_76 >> rail.Label(
            'Yes') >> update_last_name_77 >> insert_to_update_logs_78 >> if_request_e_mail_present_79

        if_request_e_mail_present_79 >> rail.Label(
            'No') >> if_request_jobcode_present_83
        if_request_e_mail_present_79 >> rail.Label(
            'Yes') >> update_email_80 >> insert_to_update_logs_81 >> if_request_jobcode_present_83

        if_request_jobcode_present_83 >> rail.Label(
            'No') >> if_request_jobcode_blank_86
        if_request_jobcode_present_83 >> rail.Label(
            'Yes') >> update_text_value_jobcode_u_d_f_84 >> insert_to_update_logs_85 >> if_request_jobcode_blank_86

        if_request_jobcode_blank_86 >> rail.Label(
            'No') >> if_request_cpnycode_present_90
        if_request_jobcode_blank_86 >> rail.Label(
            'Yes') >> update_text_value_jobcode_u_d_f_87 >> insert_to_update_logs_88 >> if_request_cpnycode_present_90

        if_request_cpnycode_present_90 >> rail.Label(
            'No') >> if_request_cpnycode_blank_93
        if_request_cpnycode_present_90 >> rail.Label(
            'Yes') >> update_text_value_cpnycode_91 >> insert_to_update_logs_92 >> if_request_cpnycode_blank_93

        if_request_cpnycode_blank_93 >> rail.Label(
            'No') >> if_request_replicontsdate_present_96
        if_request_cpnycode_blank_93 >> rail.Label(
            'Yes') >> update_text_value_cpnycode_94 >> insert_to_update_logs_95 >> if_request_replicontsdate_present_96

        if_request_replicontsdate_present_96 >> rail.Label(
            'No') >> if_request_servicedate_present_and_terminationdate_not_present
        if_request_replicontsdate_present_96 >> rail.Label(
            'Yes') >> if_log_valuefor_replicon_t_s_date_97_blank_98

        if_log_valuefor_replicon_t_s_date_97_blank_98 >> rail.Label(
            'No') >> if_request_servicedate_present_and_terminationdate_not_present
        if_log_valuefor_replicon_t_s_date_97_blank_98 >> rail.Label('Yes') >> trigger_dag_run_child_workflow_to_add_timeoff_type_for_new_user_99 \
            >> wait_for_completion_trigger_dag_run_child_workflow_to_add_timeoff_type_for_new_user_v3_099 >> gather_response_from_dag_run_99 \
            >> if_error_in_gather_reponse_from_dag_run_99

        if_error_in_gather_reponse_from_dag_run_99 >> rail.Label(
            'No') >> update_text_value_replicon_t_s_date_101
        if_error_in_gather_reponse_from_dag_run_99 >> rail.Label(
            'Yes') >> fail_with_error_in_adding_timeoff_type_for_new_user >> update_text_value_replicon_t_s_date_101

        update_text_value_replicon_t_s_date_101 >> insert_to_update_logs_102 >> if_request_timesheettemplate_present_103

        if_request_timesheettemplate_present_103 >> rail.Label(
            'No') >> log_replicon_t_s_dateupdated_106
        if_request_timesheettemplate_present_103 >> rail.Label(
            'Yes') >> put_timesheet_period_schedule_for_user_104 >> insert_to_update_logs_105 >> log_replicon_t_s_dateupdated_106

        log_replicon_t_s_dateupdated_106 >> if_request_servicedate_present_and_terminationdate_not_present

        if_request_servicedate_present_and_terminationdate_not_present >> rail.Label(
            'No') >> if_ptosenioritydate_present_and_terminationdate_not_present_107
        if_request_servicedate_present_and_terminationdate_not_present >> rail.Label(
            'Yes') >> if_log_currentstartdate_blank_or_not_delta

        if_log_currentstartdate_blank_or_not_delta >> rail.Label(
            'No') >> if_ptosenioritydate_present_and_terminationdate_not_present_107
        if_log_currentstartdate_blank_or_not_delta >> rail.Label('Yes') >> updatestart_date_on_profile \
            >> insert_to_update_logs_start_date_updated >> if_replicon_tsdate_not_updated_and_user_not_rehired

        if_replicon_tsdate_not_updated_and_user_not_rehired >> rail.Label(
            'No') >> if_ptosenioritydate_present_and_terminationdate_not_present_107
        if_replicon_tsdate_not_updated_and_user_not_rehired >> rail.Label(
            'Yes') >> trigger_dag_run_child_to_update_timeoff_type_for_seniority_date_or_service_date_change

        trigger_dag_run_child_to_update_timeoff_type_for_seniority_date_or_service_date_change \
            >> wait_for_completion_trigger_dag_run_child_to_update_timeoff_type_for_seniority_date_or_service_date_change \
            >> gather_response_from_dag_run_seniority_date_or_service_date_change >> if_error_in_gather_reponse_from_dag_run_seniority_date_or_service_date_change

        if_error_in_gather_reponse_from_dag_run_seniority_date_or_service_date_change >> rail.Label(
            'No') >> if_ptosenioritydate_present_and_terminationdate_not_present_107
        if_error_in_gather_reponse_from_dag_run_seniority_date_or_service_date_change >> rail.Label(
            'Yes') >> fail_with_error_in_workflow_to_update_timeoff_type_for_seniority_date_or_service_date_change >> if_ptosenioritydate_present_and_terminationdate_not_present_107

        if_ptosenioritydate_present_and_terminationdate_not_present_107 >> rail.Label(
            'No') >> if_userdetails_isenabled_is_true_disable_115
        if_ptosenioritydate_present_and_terminationdate_not_present_107 >> rail.Label(
            'Yes') >> if_pto_seniority_date_added_or_changed

        if_pto_seniority_date_added_or_changed >> rail.Label(
            'No') >> if_userdetails_isenabled_is_true_disable_115
        if_pto_seniority_date_added_or_changed >> rail.Label('Yes') >> update_pto_seniority_date_udf \
            >> insert_to_update_logs_ptosdate >> if_log_replicon_t_s_dateupdated_106_blank_113

        if_log_replicon_t_s_dateupdated_106_blank_113 >> rail.Label(
            'No') >> if_userdetails_isenabled_is_true_disable_115
        if_log_replicon_t_s_dateupdated_106_blank_113 >> rail.Label('Yes') >> trigger_dag_run_child_workflow_to_update_timeoff_type_for_seniority_date_114 \
            >> wait_for_completion_trigger_dag_run_child_workflow_to_update_timeoff_type_for_seniority_date_114 >> gather_response_from_dag_run_114 \
            >> if_error_in_gather_reponse_from_dag_run_114

        if_error_in_gather_reponse_from_dag_run_114 >> rail.Label(
            'No') >> if_userdetails_isenabled_is_true_disable_115
        if_error_in_gather_reponse_from_dag_run_114 >> rail.Label(
            'Yes') >> fail_with_error_in_workflow_to_update_timeoff_type_for_seniority_date >> if_userdetails_isenabled_is_true_disable_115

        if_userdetails_isenabled_is_true_disable_115 >> rail.Label(
            'No') >> if_request_dailyhours_present_119
        if_userdetails_isenabled_is_true_disable_115 >> rail.Label(
            'Yes') >> disable_login_116 >> trigger_dag_run_assured_partners_child_workflow_timeoff_type_for_disable_user_117 \
            >> wait_for_completion_trigger_dag_run_assured_partners_child_workflow_timeoff_type_for_disable_user_117 >> gather_response_from_dag_run_117 \
            >> if_error_in_gather_reponse_from_dag_run_117

        if_error_in_gather_reponse_from_dag_run_117 >> rail.Label(
            'No') >> insert_to_update_logs_118 >> if_request_dailyhours_present_119
        if_error_in_gather_reponse_from_dag_run_117 >> rail.Label(
            'Yes') >> fail_with_error_in_timeoff_type_for_disable_user >> if_request_dailyhours_present_119

        if_request_dailyhours_present_119 >> rail.Label(
            'No') >> if_request_agency_org2_present_125
        if_request_dailyhours_present_119 >> rail.Label(
            'Yes') >> if_log_valuefor_daily_hours_120_blank_121

        if_log_valuefor_daily_hours_120_blank_121 >> rail.Label(
            'No') >> if_request_agency_org2_present_125
        if_log_valuefor_daily_hours_120_blank_121 >> rail.Label(
            'Yes') >> update_text_value_daily_hours_122 >> insert_to_update_logs_123 >> if_request_agency_org2_present_125

        if_request_agency_org2_present_125 >> rail.Label(
            'No') >> if_request_agency_org2_blank_128
        if_request_agency_org2_present_125 >> rail.Label(
            'Yes') >> update_text_value_agency_org2_126 >> insert_to_update_logs_127 >> if_request_agency_org2_blank_128

        if_request_agency_org2_blank_128 >> rail.Label(
            'No') >> if_assignment_number_present_and_not_equals_existing
        if_request_agency_org2_blank_128 >> rail.Label(
            'Yes') >> update_text_value_agency_org2_129 >> insert_to_update_logs_130 >> if_assignment_number_present_and_not_equals_existing

        if_assignment_number_present_and_not_equals_existing >> rail.Label(
            'No') >> if_request_hourlyrate_present_132
        if_assignment_number_present_and_not_equals_existing >> rail.Label(
            'Yes') >> update_assignment_number_udf >> insert_to_update_logs_assignment_number >> if_request_hourlyrate_present_132

        if_request_hourlyrate_present_132 >> rail.Label(
            'No') >> if_request_hourlyrate_blank_135
        if_request_hourlyrate_present_132 >> rail.Label(
            'Yes') >> update_text_value_hourlyrate_133 >> insert_to_update_logs_134 >> if_request_hourlyrate_blank_135

        if_request_hourlyrate_blank_135 >> rail.Label(
            'No') >> if_request_loasuspendptoend_present_139
        if_request_hourlyrate_blank_135 >> rail.Label(
            'Yes') >> update_text_value_hourlyrate_136 >> insert_to_update_logs_137 >> if_request_loasuspendptoend_present_139

        if_request_loasuspendptoend_present_139 >> rail.Label(
            'No') >> if_request_loasuspendptostart_present_144
        if_request_loasuspendptoend_present_139 >> rail.Label(
            'Yes') >> update_date_value_l_o_a_end_date_141 >> insert_to_update_logs_142 >> if_request_loasuspendptostart_present_144

        if_request_loasuspendptostart_present_144 >> rail.Label(
            'No') >> if_request_paygroupcode_present_149
        if_request_loasuspendptostart_present_144 >> rail.Label(
            'Yes') >> update_date_value_l_o_a_start_date_146 >> insert_to_update_logs_147 >> if_request_paygroupcode_present_149

        if_request_paygroupcode_present_149 >> rail.Label(
            'No') >> if_request_paygroupcode_blank_152
        if_request_paygroupcode_present_149 >> rail.Label(
            'Yes') >> update_text_value_paygroupcode_150 >> insert_to_update_logs_151 >> if_request_paygroupcode_blank_152

        if_request_paygroupcode_blank_152 >> rail.Label(
            'No') >> if_request_locationcode_work_present_156
        if_request_paygroupcode_blank_152 >> rail.Label(
            'Yes') >> update_text_value_paygroupcode_153 >> insert_to_update_logs_154 >> if_request_locationcode_work_present_156

        if_request_locationcode_work_present_156 >> rail.Label(
            'No') >> if_request_locationcode_work_blank_159
        if_request_locationcode_work_present_156 >> rail.Label(
            'Yes') >> update_text_value_locationcodework_157 >> insert_to_update_logs_158 >> if_request_locationcode_work_blank_159

        if_request_locationcode_work_blank_159 >> rail.Label(
            'No') >> if_request_dept_org4desc_present_163
        if_request_locationcode_work_blank_159 >> rail.Label(
            'Yes') >> update_text_value_locationcodework_160 >> insert_to_update_logs_161 >> if_request_dept_org4desc_present_163

        if_request_dept_org4desc_present_163 >> rail.Label(
            'No') >> if_request_dept_org4desc_blank_166
        if_request_dept_org4desc_present_163 >> rail.Label(
            'Yes') >> update_text_value_dept_org4_desc_164 >> insert_to_update_logs_165 >> if_request_dept_org4desc_blank_166

        if_request_dept_org4desc_blank_166 >> rail.Label(
            'No') >> if_request_coresupervisorid_present_170
        if_request_dept_org4desc_blank_166 >> rail.Label(
            'Yes') >> update_text_value_dept_org4_desc_167 >> insert_to_update_logs_168 >> if_request_coresupervisorid_present_170

        if_request_coresupervisorid_present_170 >> rail.Label(
            'No') >> if_request_coresupervisorid_blank_173
        if_request_coresupervisorid_present_170 >> rail.Label(
            'Yes') >> update_text_value_core_supervisor_i_d_171 >> insert_to_update_logs_172 >> if_request_coresupervisorid_blank_173

        if_request_coresupervisorid_blank_173 >> rail.Label(
            'No') >> if_request_coresupervisorname_present_177
        if_request_coresupervisorid_blank_173 >> rail.Label(
            'Yes') >> update_text_value_core_supervisor_i_d_174 >> insert_to_update_logs_175 >> if_request_coresupervisorname_present_177

        if_request_coresupervisorname_present_177 >> rail.Label(
            'No') >> if_request_coresupervisorname_blank_180
        if_request_coresupervisorname_present_177 >> rail.Label(
            'Yes') >> update_text_value_core_supervisor_name_178 >> insert_to_update_logs_179 >> if_request_coresupervisorname_blank_180

        if_request_coresupervisorname_blank_180 >> rail.Label(
            'No') >> if_request_eetype_present_184
        if_request_coresupervisorname_blank_180 >> rail.Label(
            'Yes') >> update_text_value_core_supervisor_name_181 >> insert_to_update_logs_182 >> if_request_eetype_present_184

        if_request_eetype_present_184 >> rail.Label(
            'No') >> if_request_flsastatus_present_193
        if_request_eetype_present_184 >> rail.Label(
            'Yes') >> get_all_custom_field_drop_down_options_185 >> if_log_e_etypedropdownoptionuri_186_present_187

        if_log_e_etypedropdownoptionuri_186_present_187 >> rail.Label(
            'No') >> insert_to_update_logs_191 >> if_request_flsastatus_present_193
        if_log_e_etypedropdownoptionuri_186_present_187 >> rail.Label(
            'Yes') >> update_dropdown_value_e_e_type_188 >> insert_to_update_logs_189 >> if_request_flsastatus_present_193

        if_request_flsastatus_present_193 >> rail.Label(
            'No') >> if_request_payrules_present_201
        if_request_flsastatus_present_193 >> rail.Label(
            'Yes') >> get_flsa_custom_field_drop_down_options_194 >> if_log_f_l_s_astatusdropdownoptionuri_195_present_196

        if_log_f_l_s_astatusdropdownoptionuri_195_present_196 >> rail.Label(
            'No') >> insert_to_update_logs_200 >> if_request_payrules_present_201
        if_log_f_l_s_astatusdropdownoptionuri_195_present_196 >> rail.Label(
            'Yes') >> update_dropdown_value_f_l_s_astatus_197 >> insert_to_update_logs_198 >> if_request_payrules_present_201

        if_request_payrules_present_201 >> rail.Label(
            'No') >> if_hourly_rate_in_feed
        if_request_payrules_present_201 >> rail.Label(
            'Yes') >> log_payrule_list_payrule_schedule_current_payrule_name >> if_log_currentpayrulename_219_blank_220

        if_log_currentpayrulename_219_blank_220 >> rail.Label(
            'No') >> if_hourly_rate_in_feed
        if_log_currentpayrulename_219_blank_220 >> rail.Label(
            'Yes') >> get_all_scriptsforpayrule_221 >> if_log_payrule_script_uri_222_blank_223

        if_log_payrule_script_uri_222_blank_223 >> rail.Label(
            'No') >> log_final_payrule_list_to_assign >> put_pay_rule_script_assignment_schedule_for_user_228 >> insert_to_update_logs_229 \
            >> if_hourly_rate_in_feed
        if_log_payrule_script_uri_222_blank_223 >> rail.Label(
            'Yes') >> insert_excception_to_logs_224 >> if_hourly_rate_in_feed

        if_hourly_rate_in_feed >> rail.Label(
            'No') >> if_payroll_grouping_present_or_not_costcenter_231
        if_hourly_rate_in_feed >> rail.Label(
            'Yes') >> get_current_hourly_rate_for_user >> if_hourlyrate_not_present_or_mismatch_in_existing_and_new

        if_hourlyrate_not_present_or_mismatch_in_existing_and_new >> rail.Label(
            'No') >> if_payroll_grouping_present_or_not_costcenter_231
        if_hourlyrate_not_present_or_mismatch_in_existing_and_new >> rail.Label(
            'Yes') >> update_hourlyrate_for_user >> insert_to_update_logs_hourlyrate_updated >> if_payroll_grouping_present_or_not_costcenter_231

        if_payroll_grouping_present_or_not_costcenter_231 >> rail.Label(
            'No') >> log_get_service_centre_schedule_list_service_centre_list_current_servicecentre_name
        if_payroll_grouping_present_or_not_costcenter_231 >> rail.Label(
            'Yes') >> if_request_payrollgrouping_present_250

        if_request_payrollgrouping_present_250 >> rail.Label(
            'No') >> if_log_currentcostcentername_249_present_258
        if_request_payrollgrouping_present_250 >> rail.Label(
            'Yes') >> if_log_currentcostcentername_249_blank_251

        if_log_currentcostcentername_249_blank_251 >> rail.Label(
            'No') >> if_log_currentcostcentername_249_present_258
        if_log_currentcostcentername_249_blank_251 >> rail.Label(
            'Yes') >> put_cost_center_schedule_for_user_255 >> insert_to_update_logs_257 >> if_log_currentcostcentername_249_present_258

        if_log_currentcostcentername_249_present_258 >> rail.Label(
            'No') >> log_get_service_centre_schedule_list_service_centre_list_current_servicecentre_name
        if_log_currentcostcentername_249_present_258 >> rail.Label(
            'Yes') >> put_cost_center_schedule_for_user_261 >> insert_to_update_logs_263 >> log_get_service_centre_schedule_list_service_centre_list_current_servicecentre_name

        log_get_service_centre_schedule_list_service_centre_list_current_servicecentre_name >> if_request_profitcenter_present_284

        if_request_profitcenter_present_284 >> rail.Label(
            'No') >> if_log_currentservicecentername_283_present_291
        if_request_profitcenter_present_284 >> rail.Label(
            'Yes') >> if_log_currentservicecentername_283_blank_285

        if_log_currentservicecentername_283_blank_285 >> rail.Label(
            'No') >> if_log_currentservicecentername_283_present_291
        if_log_currentservicecentername_283_blank_285 >> rail.Label(
            'Yes') >> put_service_center_schedule_for_user_288 >> insert_to_update_logs_290 >> if_log_currentservicecentername_283_present_291

        if_log_currentservicecentername_283_present_291 >> rail.Label(
            'No') >> log_required_department_name_agenceies_297
        if_log_currentservicecentername_283_present_291 >> rail.Label(
            'Yes') >> put_service_center_schedule_for_user_294 >> insert_to_update_logs_296 >> log_required_department_name_agenceies_297

        log_required_department_name_agenceies_297 >> log_get_department_schedule_list_department_list_current_department_name_AgencyOrg2 >> if_request_agency_org2_present_318

        if_request_agency_org2_present_318 >> rail.Label(
            'No') >> if_request_supervisorid_present_330
        if_request_agency_org2_present_318 >> rail.Label(
            'Yes') >> if_log_currentdepartmentname_blank_or_delta_319

        if_log_currentdepartmentname_blank_or_delta_319 >> rail.Label(
            'No') >> if_log_currentdepartmentname_317_present_and_new_department_blank_325
        if_log_currentdepartmentname_blank_or_delta_319 >> rail.Label(
            'Yes') >> put_department_group_schedule_for_user_322 >> insert_to_update_logs_323 >> if_log_currentdepartmentname_317_present_and_new_department_blank_325

        if_log_currentdepartmentname_317_present_and_new_department_blank_325 >> rail.Label(
            'No') >> if_request_supervisorid_present_330
        if_log_currentdepartmentname_317_present_and_new_department_blank_325 >> rail.Label(
            'Yes') >> put_department_group_schedule_for_user_328 >> insert_to_update_logs_329 >> if_request_supervisorid_present_330

        if_request_supervisorid_present_330 >> rail.Label(
            'No') >> log_get_employee_type_schedule_list_employee_type_list_current_employee_type_name_DeptOrg4desc
        if_request_supervisorid_present_330 >> rail.Label(
            'Yes') >> if_request_supervisorid_equals_to_emplid_login_331

        if_request_supervisorid_equals_to_emplid_login_331 >> rail.Label('Yes') >> insert_excception_to_logs_332 \
            >> log_get_employee_type_schedule_list_employee_type_list_current_employee_type_name_DeptOrg4desc
        if_request_supervisorid_equals_to_emplid_login_331 >> rail.Label(
            'No') >> log_get_current_supervisor_name >> if_log_currentsupervisorloginname_347_blank_348

        if_log_currentsupervisorloginname_347_blank_348 >> rail.Label(
            'No') >> log_get_employee_type_schedule_list_employee_type_list_current_employee_type_name_DeptOrg4desc
        if_log_currentsupervisorloginname_347_blank_348 >> rail.Label('Yes') >> search_supervisor_in_replicon_349 \
            >> if_log_supervisoruri_350_blank_false_if_log_supervisoruri_350_blank_false_supervisorprofileisnotavailable_351

        if_log_supervisoruri_350_blank_false_if_log_supervisoruri_350_blank_false_supervisorprofileisnotavailable_351 >> rail.Label(
            'No') >> if_log_supervisor_profile_disabled_354
        if_log_supervisoruri_350_blank_false_if_log_supervisoruri_350_blank_false_supervisorprofileisnotavailable_351 >> rail.Label(
            'Yes') >> assured_partners_supervisor_assignment_table_add_entry_352 >> log_get_employee_type_schedule_list_employee_type_list_current_employee_type_name_DeptOrg4desc

        if_log_supervisor_profile_disabled_354 >> rail.Label(
            'No') >> if_log_supervisoruri_350_present_356
        if_log_supervisor_profile_disabled_354 >> rail.Label(
            'Yes') >> assured_partners_supervisor_assignment_table_add_entry_355 >> log_get_employee_type_schedule_list_employee_type_list_current_employee_type_name_DeptOrg4desc

        if_log_supervisoruri_350_present_356 >> rail.Label(
            'No') >> log_get_employee_type_schedule_list_employee_type_list_current_employee_type_name_DeptOrg4desc
        if_log_supervisoruri_350_present_356 >> rail.Label(
            'Yes') >> get_assigned_supervisor_permission_set_for_user_357 >> if_log_checkif_manager_permissionsetisassigned_358_blank_359

        if_log_checkif_manager_permissionsetisassigned_358_blank_359 >> rail.Label(
            'No') >> update_supervisor_assignment_schedule_over_date_range_362
        if_log_checkif_manager_permissionsetisassigned_358_blank_359 >> rail.Label('Yes') >> assign_permission_set_to_user_manager_361 \
            >> update_supervisor_assignment_schedule_over_date_range_362

        update_supervisor_assignment_schedule_over_date_range_362 >> insert_to_update_logs_363 \
            >> log_get_employee_type_schedule_list_employee_type_list_current_employee_type_name_DeptOrg4desc

        log_get_employee_type_schedule_list_employee_type_list_current_employee_type_name_DeptOrg4desc >> if_request_dept_org4desc_present_383

        if_request_dept_org4desc_present_383 >> rail.Label(
            'No') >> if_log_currentemployeetypename_382_present_391
        if_request_dept_org4desc_present_383 >> rail.Label(
            'Yes') >> if_log_currentemployeetypename_382_blank_384

        if_log_currentemployeetypename_382_blank_384 >> rail.Label(
            'No') >> if_log_currentemployeetypename_382_present_391
        if_log_currentemployeetypename_382_blank_384 >> rail.Label('Yes') >> get_required_employee_type_group_uri_385 \
            >> put_employee_type_group_schedule_for_user_389 >> insert_to_update_logs_390 >> if_log_currentemployeetypename_382_present_391

        if_log_currentemployeetypename_382_present_391 >> rail.Label(
            'No') >> log_get_location_schedule_list_location_list_current_location_name_PayGroup
        if_log_currentemployeetypename_382_present_391 >> rail.Label('Yes') >> put_employee_type_group_schedule_for_user_394 \
            >> insert_to_update_logs_395 >> log_get_location_schedule_list_location_list_current_location_name_PayGroup

        log_get_location_schedule_list_location_list_current_location_name_PayGroup >> if_request_paygroupcode_present_415

        if_request_paygroupcode_present_415 >> rail.Label(
            'No') >> if_log_currentlocationname_414_present_421
        if_request_paygroupcode_present_415 >> rail.Label(
            'Yes') >> if_log_currentlocationname_414_blank_416

        if_log_currentlocationname_414_blank_416 >> rail.Label(
            'No') >> if_log_currentlocationname_414_present_421
        if_log_currentlocationname_414_blank_416 >> rail.Label('Yes') >> put_location_schedule_for_user_419 \
            >> insert_to_update_logs_420 >> if_log_currentlocationname_414_present_421

        if_log_currentlocationname_414_present_421 >> rail.Label(
            'No') >> log_get_division_schedule_list_division_list_current_division_name_LocationCodeWork
        if_log_currentlocationname_414_present_421 >> rail.Label('Yes') >> put_location_schedule_for_user_424 \
            >> insert_to_update_logs_425 >> log_get_division_schedule_list_division_list_current_division_name_LocationCodeWork

        log_get_division_schedule_list_division_list_current_division_name_LocationCodeWork >> if_request_locationcode_work_present_445

        if_request_locationcode_work_present_445 >> rail.Label(
            'No') >> if_log_current_divisionname_444_present_451
        if_request_locationcode_work_present_445 >> rail.Label(
            'Yes') >> if_log_current_divisionname_444_blank_446

        if_log_current_divisionname_444_blank_446 >> rail.Label(
            'No') >> if_log_current_divisionname_444_present_451
        if_log_current_divisionname_444_blank_446 >> rail.Label('Yes') >> put_division_schedule_for_user_449 \
            >> insert_to_update_logs_450 >> if_log_current_divisionname_444_present_451

        if_log_current_divisionname_444_present_451 >> rail.Label(
            'No') >> get_required_policy_set_uris_456
        if_log_current_divisionname_444_present_451 >> rail.Label(
            'Yes') >> put_division_schedule_for_user_454 >> insert_to_update_logs_455 >> get_required_policy_set_uris_456

        get_required_policy_set_uris_456 >> if_request_punch_entry_policy_present_457

        if_request_punch_entry_policy_present_457 >> rail.Label(
            'No') >> if_punch_entry_policy_is_assigned_464
        if_request_punch_entry_policy_present_457 >> rail.Label(
            'Yes') >> if_punch_entry_policy_not_equals_user_punch_entry_policy_458

        if_punch_entry_policy_not_equals_user_punch_entry_policy_458 >> rail.Label(
            'No') >> if_timeofftemplate_displaytext_present_467
        if_punch_entry_policy_not_equals_user_punch_entry_policy_458 >> rail.Label(
            'Yes') >> assign_policy_set_to_user_460 >> insert_to_update_logs_461 >> if_timeofftemplate_displaytext_present_467

        if_punch_entry_policy_is_assigned_464 >> rail.Label(
            'No') >> if_timeofftemplate_displaytext_present_467
        if_punch_entry_policy_is_assigned_464 >> rail.Label(
            'Yes') >> remove_policy_set_assignment_from_user_465 >> insert_to_update_logs_466 >> if_timeofftemplate_displaytext_present_467

        if_timeofftemplate_displaytext_present_467 >> rail.Label(
            'No') >> if_request_timeofftemplate_present_470
        if_timeofftemplate_displaytext_present_467 >> rail.Label(
            'Yes') >> remove_policy_set_assignment_from_user_timeofftemplate_468 >> insert_to_update_logs_469 >> if_request_timeofftemplate_present_470

        if_request_timeofftemplate_present_470 >> rail.Label(
            'No') >> if_request_timezone_present_480
        if_request_timeofftemplate_present_470 >> rail.Label(
            'Yes') >> if_timeofftemplate_not_equals_user_existing_timeoff_template_472

        if_timeofftemplate_not_equals_user_existing_timeoff_template_472 >> rail.Label(
            'No') >> if_request_timezone_present_480
        if_timeofftemplate_not_equals_user_existing_timeoff_template_472 >> rail.Label(
            'Yes') >> if_log_requiredtimeofftemplateuri_present_474

        if_log_requiredtimeofftemplateuri_present_474 >> rail.Label(
            'No') >> insert_excception_to_logs_478 >> if_request_timezone_present_480
        if_log_requiredtimeofftemplateuri_present_474 >> rail.Label(
            'Yes') >> assign_policy_set_to_user_timeofftemplate_475 >> insert_to_update_logs_476 >> if_request_timezone_present_480

        if_request_timezone_present_480 >> rail.Label(
            'No') >> log_workweektoassign_488
        if_request_timezone_present_480 >> rail.Label(
            'Yes') >> if_request_timezoneuri_present_481

        if_request_timezoneuri_present_481 >> rail.Label(
            'No') >> insert_excception_to_logs_487 >> log_workweektoassign_488
        if_request_timezoneuri_present_481 >> rail.Label(
            'Yes') >> if_request_timezoneuri_not_equals_to_user_existing_timezoneuri_482

        if_request_timezoneuri_not_equals_to_user_existing_timezoneuri_482 >> rail.Label(
            'No') >> log_workweektoassign_488
        if_request_timezoneuri_not_equals_to_user_existing_timezoneuri_482 >> rail.Label(
            'Yes') >> update_time_zone_for_user_483 >> insert_to_update_logs_484 >> log_workweektoassign_488

        log_workweektoassign_488 >> if_workweekstartday_uri_not_equals_to_user_current_workweekstartday_uri_489

        if_workweekstartday_uri_not_equals_to_user_current_workweekstartday_uri_489 >> rail.Label(
            'No') >> if_timesheettemplate_name_not_equals_to_user_existing_timesheettemplate_492
        if_workweekstartday_uri_not_equals_to_user_current_workweekstartday_uri_489 >> rail.Label('Yes') >> update_work_week_start_day_for_user_490 \
            >> insert_excception_to_logs_491 >> if_timesheettemplate_name_not_equals_to_user_existing_timesheettemplate_492

        if_timesheettemplate_name_not_equals_to_user_existing_timesheettemplate_492 >> rail.Label(
            'No') >> if_timesheettemplate_presence_present_499
        if_timesheettemplate_name_not_equals_to_user_existing_timesheettemplate_492 >> rail.Label(
            'Yes') >> if_log_requiredtimesheettemplateuri_493_present_494

        if_log_requiredtimesheettemplateuri_493_present_494 >> rail.Label(
            'No') >> insert_excception_to_logs_498 >> if_timesheettemplate_presence_present_499
        if_log_requiredtimesheettemplateuri_493_present_494 >> rail.Label(
            'Yes') >> assign_policy_set_to_user_timesheet_template_495 >> insert_to_update_logs_496 >> if_timesheettemplate_presence_present_499

        if_timesheettemplate_presence_present_499 >> rail.Label(
            'No') >> if_timesheettemplate_presence_blank_502
        if_timesheettemplate_presence_present_499 >> rail.Label(
            'Yes') >> update_timesheet_period_schedule_for_user_500 >> insert_to_update_logs_501 >> if_timesheettemplate_presence_blank_502

        if_timesheettemplate_presence_blank_502 >> rail.Label(
            'No') >> if_holidaycalendar_displaytext_not_equals_to_user_existing_holidaycalendar_505
        if_timesheettemplate_presence_blank_502 >> rail.Label(
            'Yes') >> assign_no_timesheet_period_503 >> insert_to_update_logs_504 >> if_holidaycalendar_displaytext_not_equals_to_user_existing_holidaycalendar_505

        if_holidaycalendar_displaytext_not_equals_to_user_existing_holidaycalendar_505 >> rail.Label(
            'No') >> get_all_office_schedules
        if_holidaycalendar_displaytext_not_equals_to_user_existing_holidaycalendar_505 >> rail.Label(
            'Yes') >> if_request_holidaycalendars_blank_506

        if_request_holidaycalendars_blank_506 >> rail.Label(
            'No') >> get_required_holiday_calendar_510 >> if_log_required_holidaycalendar_511_present_512
        if_request_holidaycalendars_blank_506 >> rail.Label(
            'Yes') >> update_holiday_calendar_for_user_507 >> insert_to_update_logs_508 >> get_all_office_schedules

        if_log_required_holidaycalendar_511_present_512 >> rail.Label(
            'No') >> insert_excception_to_logs_516 >> get_all_office_schedules
        if_log_required_holidaycalendar_511_present_512 >> rail.Label(
            'Yes') >> update_holiday_calendar_for_user_513 >> insert_to_update_logs_514 >> get_all_office_schedules

        get_all_office_schedules >> if_request_schedule_present_517

        if_request_schedule_present_517 >> rail.Label(
            'No') >> if_log_replicon_t_s_dateupdated_106_blank_553
        if_request_schedule_present_517 >> rail.Label(
            'Yes') >> log_get_officeschedule_schedule_list_officeschedule_list_current_officeschedule_name >> if_log_currentofficeschedulename_546_blank_547

        if_log_currentofficeschedulename_546_blank_547 >> rail.Label(
            'No') >> if_log_replicon_t_s_dateupdated_106_blank_553
        if_log_currentofficeschedulename_546_blank_547 >> rail.Label('Yes') >> put_schedule_policy_schedule_for_user_550 \
            >> insert_to_update_logs_551 >> log_changeinschedule_552 >> if_log_replicon_t_s_dateupdated_106_blank_553

        if_log_replicon_t_s_dateupdated_106_blank_553 >> rail.Label(
            'No') >> gather_exceptions_from_logs
        if_log_replicon_t_s_dateupdated_106_blank_553 >> rail.Label(
            'Yes') >> if_declare_variable_5_value_equals_to_rehire_554

        if_declare_variable_5_value_equals_to_rehire_554 >> rail.Label('No') >> trigger_dag_run_child_workflow_to_add_timeoff_type_for_transfer_559 \
            >> wait_for_completion_trigger_dag_run_child_workflow_to_add_timeoff_type_for_transfer_559 >> gather_response_from_dag_run_559 >> if_error_in_gather_reponse_from_dag_run_559

        if_error_in_gather_reponse_from_dag_run_559 >> rail.Label(
            'No') >> if_exception_in_gather_reponse_from_dag_run_559
        if_error_in_gather_reponse_from_dag_run_559 >> rail.Label(
            'Yes') >> fail_with_error_in_add_timeoff_type_for_transfer >> insert_to_update_logs_560

        if_exception_in_gather_reponse_from_dag_run_559 >> rail.Label(
            'No') >> insert_to_update_logs_560
        if_exception_in_gather_reponse_from_dag_run_559 >> rail.Label(
            'Yes') >> insert_to_exception_logs >> gather_exceptions_from_logs

        insert_to_update_logs_560 >> gather_exceptions_from_logs

        if_declare_variable_5_value_equals_to_rehire_554 >> rail.Label('Yes') >> trigger_dag_run_child_workflow_to_add_timeoff_type_for_rehire_555 \
            >> wait_for_completion_trigger_dag_run_child_workflow_to_add_timeoff_type_for_rehire_555 >> gather_response_from_dag_run_555 >> if_error_in_gather_reponse_from_dag_run_555

        if_error_in_gather_reponse_from_dag_run_555 >> rail.Label(
            'No') >> updaterehiredate_556
        if_error_in_gather_reponse_from_dag_run_555 >> rail.Label(
            'Yes') >> fail_with_error_in_add_timeoff_type_for_rehire >> updaterehiredate_556

        updaterehiredate_556 >> insert_to_update_logs_557 >> gather_exceptions_from_logs

        gather_exceptions_from_logs >> gather_update_entries_from_logs >> log_status_details_for_exceptions_updates_skipped_record >> assured_partners_user_sync_logs_add_entry_561 >> catch_and_log_error

    return dag


rail.for_each_instance(create_dag)
