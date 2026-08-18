from datetime import datetime, timedelta
from airflow.models import Variable
from ge.user_sync_poland.utils import request_payload, custom_methods
import rail

null = None


def create_dag(config):
    # pylnot: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_add_user_dag_id,
        description=f'GE POLAND User Import Add User Child',
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
            no_task='add_exception_logs'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='add_exception_logs',
            end_task='catch_and_log_error',
        )

        add_exception_logs = rail.CreateLogOperator(
            task_id='add_exception_logs'
        )

        def get_validation_info(dag_run):
            validation_message = []
            if not (dag_run.conf['EmployeeFirstName']):
                validation_message.append('Employee First Name not present')
            if not (dag_run.conf['EmployeeLastName']):
                validation_message.append('Employee Last Name not present')
            if not (dag_run.conf['OHRID']):
                validation_message.append('OHRID not present')
            if dag_run.conf['LegalEntityHireDate'] and "/" not in dag_run.conf['LegalEntityHireDate']:
                validation_message.append(
                    'Legal Entry Hire date not in allowed format')
            if dag_run.conf['LegalEntityHireDate'] is None and dag_run.conf['HireEffectiveDate'] and "/" not in dag_run.conf['HireEffectiveDate']:
                validation_message.append(
                    'Hire Effective date not in allowed format')
            if dag_run.conf['LegalEntityHireDate'] is None and dag_run.conf['HireEffectiveDate'] is None:
                validation_message.append(
                    'Legal Entity Hire Date or Hire Effective date is not present')
            if dag_run.conf['LegalEntity'] is None:
                validation_message.append('Legal Entity not present')
            return rail.smartjoin_by_delim(validation_message, ';')

        check_mandatory_fields = rail.PythonOperator(
            task_id='check_mandatory_fields',
            python_callable=get_validation_info
        )

        if_missing_fields_present_5 = rail.IfOperator(
            task_id='if_missing_fields_present_5',
            test='''{{ result('check_mandatory_fields') | is_truthy }}''',
            yes_task="logs_add_entry_missing_fields_6",
            no_task="poland_master_mapper_search_matching_legal_entity_8",
        )

        logs_add_entry_missing_fields_6 = rail.WriteLogOperator(
            task_id='logs_add_entry_missing_fields_6',
            message="na",
            severity="Skipped",
            properties={
                "action": "Add",
                "status": "Skipped",
                "details": '{{result("check_mandatory_fields")}}',
                "OHRID": "{{ dag_run.conf.OHRID }}",
                "username": "{{ dag_run.conf.EmployeeFirstName }} {{ dag_run.conf.EmployeeLastName }}"
            }
        )

        poland_master_mapper_search_matching_legal_entity_8 = rail.PythonOperator(
            task_id='poland_master_mapper_search_matching_legal_entity_8',
            python_callable=lambda dag_run: list(
                filter(lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'], config.POLAND_MASTER_MAPPER))
        )

        if_mapper_search_8_blank_9 = rail.IfOperator(
            task_id='if_mapper_search_blank_9',
            test='''{{ result('poland_master_mapper_search_matching_legal_entity_8') | is_falsy }}''',
            yes_task="logs_add_entry_legal_entity_not_in_mapper_10",
            no_task="log_employee_type_name_from_mapper_12",
        )

        logs_add_entry_legal_entity_not_in_mapper_10 = rail.WriteLogOperator(
            task_id='logs_add_entry_legal_entity_not_in_mapper_10',
            message="na",
            severity="Skipped",
            properties={
                "action": "Add",
                "status": "Skipped",
                "details": 'Legal Entity is not available in Mapper',
                "OHRID": "{{ dag_run.conf.OHRID }}",
                "username": "{{ dag_run.conf.EmployeeFirstName }} {{ dag_run.conf.EmployeeLastName }}"
            }
        )

        log_employee_type_name_from_mapper_12 = rail.PythonOperator(
            task_id='log_employee_type_name_from_mapper_12',
            python_callable=lambda dag_run: next(iter(filter(
                lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "Employee Type", config.POLAND_MASTER_MAPPER)), {}).get(
                    'value', '')
        )

        if_mapper_search_12_blank_13 = rail.IfOperator(
            task_id='if_mapper_search_12_blank_13',
            test='''{{ result('log_employee_type_name_from_mapper_12') | is_falsy }}''',
            yes_task="logs_add_entry_employee_type_not_in_mapper_14",
            no_task="if_request_departmenturi_blank_16",
        )

        logs_add_entry_employee_type_not_in_mapper_14 = rail.WriteLogOperator(
            task_id='logs_add_entry_employee_type_not_in_mapper_14',
            message="na",
            severity="Skipped",
            properties={
                "action": "Add",
                "status": "Skipped",
                "details": 'Employee type is not available in Mapper for Legal Entity {{ dag_run.conf.LegalEntity }}',
                "OHRID": "{{ dag_run.conf.OHRID }}",
                "username": "{{ dag_run.conf.EmployeeFirstName }} {{ dag_run.conf.EmployeeLastName }}"
            }
        )

        if_request_departmenturi_blank_16 = rail.IfOperator(
            task_id='if_request_departmenturi_blank_16',
            test='''{{ dag_run.conf.Departmenturi | is_falsy }}''',
            yes_task="logs_add_entry_departmenturi_blank_17",
            no_task="log_required_mapper_entries_19_29",
        )

        logs_add_entry_departmenturi_blank_17 = rail.WriteLogOperator(
            task_id='logs_add_entry_departmenturi_blank_17',
            message="na",
            severity="Skipped",
            properties={
                "action": "Add",
                "status": "Skipped",
                "details": '''Department is not available in Mapper for Legal Entity {{ dag_run.conf.LegalEntity }}''',
                "OHRID": "{{ dag_run.conf.OHRID }}",
                "username": "{{ dag_run.conf.EmployeeFirstName }} {{ dag_run.conf.EmployeeLastName }}",
            }
        )

        log_required_mapper_entries_19_29 = rail.PythonOperator(
            task_id='log_required_mapper_entries_19_29',
            python_callable=lambda dag_run: custom_methods.get_mapper_entry(
                dag_run, config.POLAND_MASTER_MAPPER)
        )

        create_user_34 = rail.RepliconServiceOperator(
            task_id='create_user_34',
            endpoint="/services/importservice1.svc/PutUser3",
            data=lambda dag_run: custom_methods.get_create_user_payload(
                dag_run, config.DATE_DEFAULT_FORMAT)
        )

        remove_timeoff_assignments_35 = rail.RepliconServiceOperator(
            task_id='remove_timeoff_assignments_35',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ result('create_user_34').uri }}",
                "timeOffTypeUris": []
            }
        )

        put_product_assignments_for_user_36 = rail.RepliconServiceOperator(
            task_id='put_product_assignments_for_user_36',
            endpoint="/services/AccountManagementService1.svc/PutProductAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result('create_user_34')['uri'],
                "productUris": rail.result('log_required_mapper_entries_19_29')['licence']
            }
        )

        update_language_39 = rail.RepliconServiceOperator(
            task_id='update_language_39',
            endpoint="/services/InternationalizationService1.svc/UpdateLanguageForUser",
            data=lambda: {
                "userUri": rail.result('create_user_34')['uri'],
                "languageUri": rail.result('log_required_mapper_entries_19_29')['language']
            }
        )

        if_required_timesheet_template_from_mapper_blank_41 = rail.IfOperator(
            task_id='if_required_timesheet_template_from_mapper_blank_41',
            test='''{{ result('log_required_mapper_entries_19_29').timesheettemplate | is_falsy }}''',
            yes_task="insert_exception_to_logs_42",
            no_task="get_all_policy_sets_44_45",
        )

        insert_exception_to_logs_42 = rail.WriteLogOperator(
            task_id='insert_exception_to_logs_42',
            log="{{result('add_exception_logs')}}",
            message='na',
            severity='Exception',
            properties={
                "details": "Timesheet template not available in mapper for legal entity {{ dag_run.conf.LegalEntity }}"
            }
        )

        get_all_policy_sets_44_45 = rail.RepliconServiceOperator(
            task_id='get_all_policy_sets_44_45',
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
            data_handler=lambda res: rail.find_first_by_attr_and_get_attr(res, 'name', rail.result(
                'log_required_mapper_entries_19_29')['timesheettemplate'], 'uri', '')
        )

        if_required_timesheet_template_uri_present_46 = rail.IfOperator(
            task_id='if_required_timesheet_template_uri_present_46',
            test='''{{ result('get_all_policy_sets_44_45') | is_truthy }}''',
            yes_task="assign_policy_set_to_user_timesheet_template_47",
            no_task="insert_exception_to_logs_49",
        )

        assign_policy_set_to_user_timesheet_template_47 = rail.RepliconServiceOperator(
            task_id='assign_policy_set_to_user_timesheet_template_47',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data=lambda: {
                "userUri": rail.result('create_user_34')['uri'],
                "policySetUri": rail.result('get_all_policy_sets_44_45')
            }
        )

        insert_exception_to_logs_49 = rail.WriteLogOperator(
            task_id='insert_exception_to_logs_49',
            log="{{result('add_exception_logs')}}",
            message='na',
            severity='Exception',
            properties={
                "details": "Timesheet template {{ result('log_required_mapper_entries_19_29').timesheettemplate }} not available in Replicon"
            }
        )

        if_supervisor_sso_id_present_50 = rail.IfOperator(
            task_id='if_supervisor_sso_id_present_50',
            test=lambda dag_run: bool(dag_run.conf['SupervisorSSOID']),
            yes_task='if_ohrid_equals_supervisor_sso_id_51',
            no_task='get_required_custom_field_uris_74'
        )

        if_ohrid_equals_supervisor_sso_id_51 = rail.IfOperator(
            task_id='if_ohrid_equals_supervisor_sso_id_51',
            test=lambda dag_run: dag_run.conf['SupervisorSSOID'] == dag_run.conf['OHRID'],
            yes_task='insert_exception_to_logs_52',
            no_task='search_supervisor_in_replicon_54'
        )

        insert_exception_to_logs_52 = rail.WriteLogOperator(
            task_id='insert_exception_to_logs_52',
            log="{{result('add_exception_logs')}}",
            message='na',
            severity='Exception',
            properties={
                "details": "Supervisor not assigned/updated since the user and supervisor SSO ID are same"
            }
        )

        search_supervisor_in_replicon_54 = rail.RepliconServiceOperator(
            task_id='search_supervisor_in_replicon_54',
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
                'status': res[0]['userDetails']['isEnabled']
            } if res else []
        )

        is_supervisor_profile_not_available_56 = rail.IfOperator(
            task_id='is_supervisor_profile_not_available_56',
            test=lambda: rail.result(
                'search_supervisor_in_replicon_54') == [],
            yes_task='log_supervisor_not_present_57',
            no_task='is_supervisor_profile_disabled_59_60'
        )

        log_supervisor_not_present_57 = rail.WriteLogOperator(
            task_id='log_supervisor_not_present_57',
            log='{{ dag_run.conf.supervisor_log }}',
            message="Supervisor not found in Replicon",
            severity='queued',
            properties=lambda dag_run: {
                "username": dag_run.conf['OHRID'],
                'useruri': dag_run.conf['useruri'],
                'supervisorloginname': dag_run.conf['SupervisorSSOID'],
                'action': 'add',
                'status': 'queued',
                'supervisoreffectivedate': dag_run.conf['AssignmentEffectiveDate'] if dag_run.conf['AssignmentEffectiveDate'] else (
                    dag_run.conf['integration_run_date']),
                'supervisorusername': dag_run.conf['SupervisorName'],
                'user_log': dag_run.conf['user_import_log']
            },
        )

        is_supervisor_profile_disabled_59_60 = rail.IfOperator(
            task_id='is_supervisor_profile_disabled_59_60',
            test=lambda: not (rail.result(
                'search_supervisor_in_replicon_54')['status']),
            yes_task='log_assignment_queued_supervisor_disabled_in_replicon_61',
            no_task='get_uris_for_missing_supervisor_permissions_based_on_mapper_64'
        )

        log_assignment_queued_supervisor_disabled_in_replicon_61 = rail.WriteLogOperator(
            task_id='log_assignment_queued_supervisor_disabled_in_replicon_61',
            log='{{ dag_run.conf.supervisor_log }}',
            message="Supervisor profile is disabled in Replicon",
            severity='queued',
            properties=lambda dag_run: {
                "username": dag_run.conf['OHRID'],
                'useruri': dag_run.conf['useruri'],
                'supervisorloginname': dag_run.conf['SupervisorSSOID'],
                'action': 'add',
                'status': 'queued',
                'supervisoreffectivedate': dag_run.conf['AssignmentEffectiveDate'] if dag_run.conf['AssignmentEffectiveDate'] else (
                    dag_run.conf['integration_run_date']),
                'supervisorusername': dag_run.conf['SupervisorName'],
                'user_log': dag_run.conf['user_import_log']
            },
        )

        get_uris_for_missing_supervisor_permissions_based_on_mapper_64 = rail.RepliconServiceOperator(
            task_id='get_uris_for_missing_supervisor_permissions_based_on_mapper_64',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data_handler=lambda response: [rail.find_first_by_attr_and_get_attr(response, 'displayText', entry['value'], 'uri') for entry in rail.result(
                'log_required_mapper_entries_19_29')['required_supervisor_permission'] if rail.find_first_by_attr_and_get_attr(
                response, 'displayText', entry['value'], 'uri')] if rail.result(
                'log_required_mapper_entries_19_29')['required_supervisor_permission'] else []
        )

        get_missing_supervisor_permissions_65_66 = rail.RepliconServiceOperator(
            task_id='get_missing_supervisor_permissions_65_66',
            endpoint='/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2',
            data={
                'userUri': "{{ result('search_supervisor_in_replicon_54').uri }}"
            },
            log_response=True,
            data_handler=lambda response: custom_methods.get_missing_user_permissions(rail.result(
                'get_uris_for_missing_supervisor_permissions_based_on_mapper_64'), response)
        )

        should_add_missing_permissions = rail.IfOperator(
            task_id='should_add_missing_permissions',
            test="{{ result('get_missing_supervisor_permissions_65_66') | length > 0 }}",
            yes_task='add_missing_supervisor_permissions_72',
            no_task='assign_initial_supervisor_for_user_73'
        )

        add_missing_supervisor_permissions_72 = rail.RepliconServiceCallForEachItemOperator(
            task_id='add_missing_supervisor_permissions_72',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            items=lambda: rail.result(
                'get_missing_supervisor_permissions_65_66'),
            data={
                'userUri': "{{ result('search_supervisor_in_replicon_54').uri }}",
                'permissionSetUri': '{{ item }}'
            }
        )

        assign_initial_supervisor_for_user_73 = rail.RepliconServiceOperator(
            task_id="assign_initial_supervisor_for_user_73",
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda: {
                "userUri": rail.result('create_user_34')['uri'],
                "supervisorUri": rail.result('search_supervisor_in_replicon_54')['uri'],
                "dateRange": null
            }
        )

        get_required_custom_field_uris_74 = rail.RepliconServiceOperator(
            task_id='get_required_custom_field_uris_74',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            },
            data_handler=custom_methods.get_required_custom_field_uris
        )

        get_all_custom_fields_to_update_payload_and_exceptions_75_152 = rail.PythonOperator(
            task_id='get_all_custom_fields_to_update_payload_and_exceptions_75_152',
            python_callable=lambda dag_run: custom_methods.get_user_custom_fields_values_payload_and_exceptions(
                rail.result('get_required_custom_field_uris_74'), config.DATE_DEFAULT_FORMAT, dag_run)
        )

        add_user_apply_user_modifications_for_adding_udf_values = rail.RepliconServiceOperator(
            task_id='add_user_apply_user_modifications_for_adding_udf_values',
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda: {
                "user": {
                    "uri": rail.result('create_user_34')['uri']
                },
                "modifications": {
                    "customFieldValuesToApply": rail.result('get_all_custom_fields_to_update_payload_and_exceptions_75_152')['custom_field_value_add_payload']
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_supervisor_assignment_catagory_in_feed_153 = rail.IfOperator(
            task_id='if_supervisor_assignment_catagory_in_feed_153',
            test=lambda dag_run: bool(
                dag_run.conf['SuspendAssignmentCategory']),
            yes_task='if_supervisor_assignment_catagory_customfield_uri_not_present_155',
            no_task='if_overtime_eligibility_in_feed_164'
        )

        if_supervisor_assignment_catagory_customfield_uri_not_present_155 = rail.IfOperator(
            task_id='if_supervisor_assignment_catagory_customfield_uri_not_present_155',
            test=lambda: not (
                rail.result('get_required_custom_field_uris_74')['suspend_assignment_catagory_field_uri']),
            yes_task='insert_exception_to_logs_156',
            no_task='get_suspend_assignment_catagory_dropdown_values_158_159'
        )

        insert_exception_to_logs_156 = rail.WriteLogOperator(
            task_id='insert_exception_to_logs_156',
            log="{{result('add_exception_logs')}}",
            message='na',
            severity='Exception',
            properties={
                "details": '''Suspend Assignment Category udf is not available'''
            }
        )

        get_suspend_assignment_catagory_dropdown_values_158_159 = rail.RepliconServiceOperator(
            task_id="get_suspend_assignment_catagory_dropdown_values_158_159",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result('get_required_custom_field_uris_74')['suspend_assignment_catagory_field_uri']
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['SuspendAssignmentCategory'], 'uri', '')
        )

        if_158_159_not_present_160 = rail.IfOperator(
            task_id='if_158_159_not_present_160',
            test=lambda: not (rail.result(
                'get_suspend_assignment_catagory_dropdown_values_158_159')),
            yes_task='insert_exception_to_logs_161',
            no_task='update_dropdown_value_suspend_assignment_catagory_udf_163'
        )

        insert_exception_to_logs_161 = rail.WriteLogOperator(
            task_id='insert_exception_to_logs_161',
            log="{{result('add_exception_logs')}}",
            message='na',
            severity='Exception',
            properties={
                "details": '''Suspend Assignment Category value {{ dag_run.conf.AssignmentCategory}} is not available in Replicon'''
            }
        )

        update_dropdown_value_suspend_assignment_catagory_udf_163 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_suspend_assignment_catagory_udf_163',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run: {
                "objectUri": rail.result('create_user_34')['uri'],
                "customFieldUri": rail.result('get_required_custom_field_uris_74')['suspend_assignment_catagory_field_uri'],
                "customFieldDropDownOptionUri": rail.result('get_suspend_assignment_catagory_dropdown_values_158_159')
            }
        )

        if_overtime_eligibility_in_feed_164 = rail.IfOperator(
            task_id='if_overtime_eligibility_in_feed_164',
            test=lambda dag_run: bool(
                dag_run.conf['OvertimeEligibility']),
            yes_task='if_overtime_eligibility_customfield_uri_not_present_166',
            no_task='if_industry_focus_group_in_feed_175'
        )

        if_overtime_eligibility_customfield_uri_not_present_166 = rail.IfOperator(
            task_id='if_overtime_eligibility_customfield_uri_not_present_166',
            test=lambda: not (rail.result('get_required_custom_field_uris_74')[
                'overtime_eligibility_field_uri']),
            yes_task='insert_exception_to_logs_167',
            no_task='get_overtime_eligibility_customfield_matching_dropdown_value_uri_169_170'
        )

        insert_exception_to_logs_167 = rail.WriteLogOperator(
            task_id='insert_exception_to_logs_167',
            log="{{result('add_exception_logs')}}",
            message='na',
            severity='Exception',
            properties={
                "details": '''Overtime Eligibility UDF is not available'''
            }
        )

        get_overtime_eligibility_customfield_matching_dropdown_value_uri_169_170 = rail.RepliconServiceOperator(
            task_id="get_overtime_eligibility_customfield_matching_dropdown_value_uri_169_170",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result('get_required_custom_field_uris_74')['overtime_eligibility_field_uri']
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['OvertimeEligibility'], 'uri', '')
        )

        if_169_170_not_present_171 = rail.IfOperator(
            task_id='if_169_170_not_present_171',
            test=lambda: not (rail.result(
                'get_overtime_eligibility_customfield_matching_dropdown_value_uri_169_170')),
            yes_task='insert_exception_to_logs_172',
            no_task='update_dropdown_value_overtime_eligibility_udf_174'
        )

        insert_exception_to_logs_172 = rail.WriteLogOperator(
            task_id='insert_exception_to_logs_172',
            log="{{result('add_exception_logs')}}",
            message='na',
            severity='Exception',
            properties={
                "details": '''Suspend Assignment Category value {{ dag_run.conf.AssignmentCategory}} is not available in Replicon'''
            }
        )

        update_dropdown_value_overtime_eligibility_udf_174 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_overtime_eligibility_udf_174',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run: {
                "objectUri": rail.result('create_user_34')['uri'],
                "customFieldUri": rail.result('get_required_custom_field_uris_74')['overtime_eligibility_field_uri'],
                "customFieldDropDownOptionUri": rail.result('get_overtime_eligibility_customfield_matching_dropdown_value_uri_169_170')
            }
        )

        if_industry_focus_group_in_feed_175 = rail.IfOperator(
            task_id='if_industry_focus_group_in_feed_175',
            test=lambda dag_run: bool(dag_run.conf['IndustryFocusGroup']),
            yes_task='log_get_new_industryfocus_group_division_uri_176_177',
            no_task='declare_variable_schedule_to_assign_182'
        )

        log_get_new_industryfocus_group_division_uri_176_177 = rail.RepliconServiceOperator(
            task_id='log_get_new_industryfocus_group_division_uri_176_177',
            endpoint="/services/DivisionService1.svc/GetAllDivisions",
            data_handler=lambda res, dag_run: rail.find_first_by_attr_and_get_attr(
                res, 'displayText', dag_run.conf['IndustryFocusGroup'], 'uri')
        )

        if_new_industryfocus_group_uri_present_178 = rail.IfOperator(
            task_id='if_new_industryfocus_group_uri_present_272',
            test=lambda: bool(rail.result(
                'log_get_new_industryfocus_group_division_uri_176_177')),
            yes_task='put_division_schedule_for_user_179',
            no_task='insert_exception_to_logs_181'
        )

        put_division_schedule_for_user_179 = rail.RepliconServiceOperator(
            task_id='put_division_schedule_for_user_179',
            endpoint="/services/DivisionService1.svc/PutDivisionScheduleForUser",
            data=lambda: {
                "userUri": rail.result('create_user_34')['uri'],
                "scheduleEntries": [
                    {
                        "division": {
                            "uri": rail.result('log_get_new_industryfocus_group_division_uri_176_177'),
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        insert_exception_to_logs_181 = rail.WriteLogOperator(
            task_id='insert_exception_to_logs_181',
            log="{{result('add_exception_logs')}}",
            message='na',
            severity='Exception',
            properties={
                "details": '''Industry focus group "{{dag_run.conf.IndustryFocusGroup}}" is not available in Replicon'''
            }
        )

        declare_variable_schedule_to_assign_182 = rail.SetVariableOperator(
            task_id='declare_variable_schedule_to_assign_182',
            append=False,
            name='schedule_to_assign',
            value='{{dag_run.conf.DWSMonday}}|{{dag_run.conf.DWSTuesday}}|{{dag_run.conf.DWSWednesday}}|{{dag_run.conf.DWSThursday}}|{{dag_run.conf.DWSFriday}}|{{dag_run.conf.DWSSaturday}}|{{dag_run.conf.DWSSunday}}'
        )

        get_weekly_work_hours_and_schedule_to_assign_182_203 = rail.PythonOperator(
            task_id='get_weekly_work_hours_and_schedule_to_assign_182_203',
            python_callable=lambda dag_run: custom_methods.weekly_work_hours_and_schedule_to_assign(rail.get_dag_run_var('schedule_to_assign'), rail.result(
                'poland_master_mapper_search_matching_legal_entity_8'), dag_run)
        )

        get_new_office_schedule_uri_204_205 = rail.RepliconServiceOperator(
            task_id='get_new_office_schedule_uri_204_205',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
            data_handler=lambda res: rail.find_first_by_attr_and_get_attr(
                res, 'displayText', rail.result('get_weekly_work_hours_and_schedule_to_assign_182_203')['schedule_to_assign'], 'uri', '')
        )

        if_required_office_schedule_uri_present_206 = rail.IfOperator(
            task_id='if_required_office_schedule_uri_present_206',
            test=lambda dag_run: bool(rail.result(
                'get_new_office_schedule_uri_204_205')),
            yes_task="put_schedule_policy_schedule_for_user_207",
            no_task="log_exception_209",
        )

        put_schedule_policy_schedule_for_user_207 = rail.RepliconServiceOperator(
            task_id='put_schedule_policy_schedule_for_user_207',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data=lambda dag_run: {
                "userUri": rail.result('create_user_34')['uri'],
                "scheduleEntries": [{
                    "schedulePolicy": {
                        "officeSchedule": {
                            "officeScheduleUri": rail.result('get_new_office_schedule_uri_204_205')
                        },
                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                    }
                }]
            }
        )

        log_exception_209 = rail.WriteLogOperator(
            task_id='log_exception_209',
            log="{{result('add_exception_logs')}}",
            message='na',
            severity='Exception',
            properties={
                "details": "Schedule '{{result('get_weekly_work_hours_and_schedule_to_assign_182_203').schedule_to_assign}}' is not available in Replicon"
            }
        )

        trigger_dag_run_ge_poland_child_add_update_timeoff_type_210 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_ge_poland_child_add_update_timeoff_type_210',
            retries=0,
            trigger_dag_id=config.child_add_update_timeoff_type_dag_id,
            execution_timeout=timedelta(config.execution_timeout_days),
            conf=lambda dag_run: {
                "userloginname": dag_run.conf['OHRID'],
                "useruri": rail.result('create_user_34')['uri'],
                "legacypayrollid": dag_run.conf['LegacyPayrollID'],
                "jobtype": dag_run.conf['JobType'],
                "legalentity": dag_run.conf['LegalEntity'],
                "startdate": (dag_run.conf['LegalEntityHireDate'] if (
                    dag_run.conf['ContractType'] == "UN00") else dag_run.conf['RadiationFlag']) if (
                    dag_run.conf['ContractType']) else dag_run.conf['LegalEntityHireDate'],
                "type": "Add",
                "fullpart": "Full Time" if (rail.result('get_weekly_work_hours_and_schedule_to_assign_182_203')['weekly_work_hours'] > 39) else "Part Time",
                "payrule": "na",
                "scheduledweeklyhours": 40 if (int(rail.result('get_weekly_work_hours_and_schedule_to_assign_182_203')['weekly_work_hours']) > 39) else int(
                    rail.result('get_weekly_work_hours_and_schedule_to_assign_182_203')['weekly_work_hours']),
                "educationlevel": dag_run.conf['EducationLevel'],
                "contractstart": dag_run.conf['RadiationFlag'],
                "contractend": dag_run.conf['PositionCapacity'],
                "contracttype": dag_run.conf['ContractType'],
                "PreviousExperience":  dag_run.conf['previousemployment'],
                "overwritepolicy": "na",
                "old_scheduled_hours": "na",
                "education_level_old": "na"
            }
        )

        wait_for_completion_trigger_dag_run_ge_poland_child_add_update_timeoff_type_210 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_poland_child_add_update_timeoff_type_210',
            execution_timeout=timedelta(config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_ge_poland_child_add_update_timeoff_type_210") }}'
        )

        gather_response_from_dag_run_210 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_response_from_dag_run_210',
            dag_runs="{{result('trigger_dag_run_ge_poland_child_add_update_timeoff_type_210')}}",
            dagrun_task_id="final_response_from_dag",
            target='result'
        )

        if_reponse_from_dag_run_210_211 = rail.IfOperator(
            task_id='if_reponse_from_dag_run_210_211',
            test=lambda: bool(rail.result("gather_response_from_dag_run_210")),
            yes_task="insert_exception_to_logs_212",
            no_task="gather_exceptions_from_logs",
        )

        insert_exception_to_logs_212 = rail.WriteLogOperator(
            task_id='insert_exception_to_logs_212',
            log="{{result('add_exception_logs')}}",
            message='na',
            severity='Exception',
            properties={
                "details": "{{result('gather_response_from_dag_run_210')[0]}}"
            }
        )

        gather_exceptions_from_logs = rail.FilterLogEntriesOperator(
            task_id='gather_exceptions_from_logs',
            log="{{result('add_exception_logs')}}",
            severity='Exception'
        )

        log_status_type_and_details_for_logs = rail.PythonOperator(
            task_id='log_status_type_and_details_for_logs',
            python_callable=lambda dag_run:  custom_methods.get_status_type_and_details_for_logs(
                rail.result("gather_exceptions_from_logs"), dag_run)
        )

        ge_poland_user_sync_logs_entry_218 = rail.WriteLogOperator(
            task_id='ge_poland_user_sync_logs_entry_218',
            log="{{ dag_run.conf.user_import_log }}",
            message="na",
            severity=lambda: rail.result(
                'log_status_type_and_details_for_logs')['status'],
            properties=lambda dag_run: {
                "OHRID": dag_run.conf['OHRID'],
                "action": dag_run.conf['type'],
                "status": rail.result('log_status_type_and_details_for_logs')['status'],
                "details": rail.result('log_status_type_and_details_for_logs')['details'],
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
                "action": dag_run.conf['type'],
                "status": "Error",
                "details": rail.render_template("{{ get_error_message() }}"),
                "username": dag_run.conf['EmployeeFirstName'] + " " + dag_run.conf['EmployeeLastName']
            }
        )

        can_run_batch_task >> rail.Label('No') >> add_exception_logs
        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error

        add_exception_logs >> check_mandatory_fields >> if_missing_fields_present_5

        if_missing_fields_present_5 >> rail.Label(
            'No') >> poland_master_mapper_search_matching_legal_entity_8 >> if_mapper_search_8_blank_9
        if_missing_fields_present_5 >> rail.Label(
            'Yes') >> logs_add_entry_missing_fields_6 >> catch_and_log_error

        if_mapper_search_8_blank_9 >> rail.Label(
            'No') >> log_employee_type_name_from_mapper_12 >> if_mapper_search_12_blank_13
        if_mapper_search_8_blank_9 >> rail.Label(
            'Yes') >> logs_add_entry_legal_entity_not_in_mapper_10 >> catch_and_log_error

        if_mapper_search_12_blank_13 >> rail.Label(
            'No') >> if_request_departmenturi_blank_16
        if_mapper_search_12_blank_13 >> rail.Label(
            'Yes') >> logs_add_entry_employee_type_not_in_mapper_14 >> catch_and_log_error

        if_request_departmenturi_blank_16 >> rail.Label(
            'No') >> log_required_mapper_entries_19_29 >> create_user_34
        if_request_departmenturi_blank_16 >> rail.Label(
            'Yes') >> logs_add_entry_departmenturi_blank_17 >> catch_and_log_error

        create_user_34 >> remove_timeoff_assignments_35 >> put_product_assignments_for_user_36 >> update_language_39 >>\
            if_required_timesheet_template_from_mapper_blank_41

        if_required_timesheet_template_from_mapper_blank_41 >> rail.Label(
            'No') >> get_all_policy_sets_44_45
        if_required_timesheet_template_from_mapper_blank_41 >> rail.Label(
            'Yes') >> insert_exception_to_logs_42 >> get_all_policy_sets_44_45

        get_all_policy_sets_44_45 >> if_required_timesheet_template_uri_present_46

        if_required_timesheet_template_uri_present_46 >> rail.Label(
            'No') >> insert_exception_to_logs_49 >> if_supervisor_sso_id_present_50
        if_required_timesheet_template_uri_present_46 >> rail.Label(
            'Yes') >> assign_policy_set_to_user_timesheet_template_47 >> if_supervisor_sso_id_present_50

        if_supervisor_sso_id_present_50 >> rail.Label(
            'No') >> get_required_custom_field_uris_74
        if_supervisor_sso_id_present_50 >> rail.Label(
            'Yes') >> if_ohrid_equals_supervisor_sso_id_51

        if_ohrid_equals_supervisor_sso_id_51 >> rail.Label(
            'No') >> search_supervisor_in_replicon_54 >> is_supervisor_profile_not_available_56
        if_ohrid_equals_supervisor_sso_id_51 >> rail.Label(
            'Yes') >> insert_exception_to_logs_52 >> get_required_custom_field_uris_74

        is_supervisor_profile_not_available_56 >> rail.Label(
            'No') >> is_supervisor_profile_disabled_59_60
        is_supervisor_profile_not_available_56 >> rail.Label(
            'Yes') >> log_supervisor_not_present_57 >> get_required_custom_field_uris_74

        is_supervisor_profile_disabled_59_60 >> rail.Label(
            'No') >> get_uris_for_missing_supervisor_permissions_based_on_mapper_64 >> get_missing_supervisor_permissions_65_66 >>\
            should_add_missing_permissions
        is_supervisor_profile_disabled_59_60 >> rail.Label(
            'Yes') >> log_assignment_queued_supervisor_disabled_in_replicon_61 >> get_required_custom_field_uris_74

        should_add_missing_permissions >> rail.Label(
            'No') >> assign_initial_supervisor_for_user_73
        should_add_missing_permissions >> rail.Label(
            'Yes') >> add_missing_supervisor_permissions_72 >> assign_initial_supervisor_for_user_73

        assign_initial_supervisor_for_user_73 >> get_required_custom_field_uris_74 >> get_all_custom_fields_to_update_payload_and_exceptions_75_152 >>\
            add_user_apply_user_modifications_for_adding_udf_values >> if_supervisor_assignment_catagory_in_feed_153

        if_supervisor_assignment_catagory_in_feed_153 >> rail.Label(
            'No') >> if_overtime_eligibility_in_feed_164
        if_supervisor_assignment_catagory_in_feed_153 >> rail.Label(
            'Yes') >> if_supervisor_assignment_catagory_customfield_uri_not_present_155

        if_supervisor_assignment_catagory_customfield_uri_not_present_155 >> rail.Label(
            'No') >> get_suspend_assignment_catagory_dropdown_values_158_159 >> if_158_159_not_present_160
        if_supervisor_assignment_catagory_customfield_uri_not_present_155 >> rail.Label(
            'Yes') >> insert_exception_to_logs_156 >> if_overtime_eligibility_in_feed_164

        if_158_159_not_present_160 >> rail.Label(
            'No') >> update_dropdown_value_suspend_assignment_catagory_udf_163 >> if_overtime_eligibility_in_feed_164
        if_158_159_not_present_160 >> rail.Label(
            'Yes') >> insert_exception_to_logs_161 >> if_overtime_eligibility_in_feed_164

        if_overtime_eligibility_in_feed_164 >> rail.Label(
            'No') >> if_industry_focus_group_in_feed_175
        if_overtime_eligibility_in_feed_164 >> rail.Label(
            'Yes') >> if_overtime_eligibility_customfield_uri_not_present_166

        if_overtime_eligibility_customfield_uri_not_present_166 >> rail.Label(
            'No') >> get_overtime_eligibility_customfield_matching_dropdown_value_uri_169_170 >> if_169_170_not_present_171
        if_overtime_eligibility_customfield_uri_not_present_166 >> rail.Label(
            'Yes') >> insert_exception_to_logs_167 >> if_industry_focus_group_in_feed_175

        if_169_170_not_present_171 >> rail.Label(
            'No') >> update_dropdown_value_overtime_eligibility_udf_174 >> if_industry_focus_group_in_feed_175
        if_169_170_not_present_171 >> rail.Label(
            'Yes') >> insert_exception_to_logs_172 >> if_industry_focus_group_in_feed_175

        if_industry_focus_group_in_feed_175 >> rail.Label(
            'No') >> declare_variable_schedule_to_assign_182
        if_industry_focus_group_in_feed_175 >> rail.Label(
            'Yes') >> log_get_new_industryfocus_group_division_uri_176_177 >> if_new_industryfocus_group_uri_present_178

        if_new_industryfocus_group_uri_present_178 >> rail.Label(
            'No') >> insert_exception_to_logs_181 >> declare_variable_schedule_to_assign_182
        if_new_industryfocus_group_uri_present_178 >> rail.Label(
            'Yes') >> put_division_schedule_for_user_179 >> declare_variable_schedule_to_assign_182

        declare_variable_schedule_to_assign_182 >> get_weekly_work_hours_and_schedule_to_assign_182_203 >> get_new_office_schedule_uri_204_205 >>\
            if_required_office_schedule_uri_present_206

        if_required_office_schedule_uri_present_206 >> rail.Label(
            'No') >> log_exception_209 >> trigger_dag_run_ge_poland_child_add_update_timeoff_type_210
        if_required_office_schedule_uri_present_206 >> rail.Label(
            'Yes') >> put_schedule_policy_schedule_for_user_207 >> trigger_dag_run_ge_poland_child_add_update_timeoff_type_210

        trigger_dag_run_ge_poland_child_add_update_timeoff_type_210 >> wait_for_completion_trigger_dag_run_ge_poland_child_add_update_timeoff_type_210 >>\
            gather_response_from_dag_run_210 >> if_reponse_from_dag_run_210_211

        if_reponse_from_dag_run_210_211 >> rail.Label(
            'No') >> gather_exceptions_from_logs
        if_reponse_from_dag_run_210_211 >> rail.Label(
            'Yes') >> insert_exception_to_logs_212 >> gather_exceptions_from_logs

        gather_exceptions_from_logs >> log_status_type_and_details_for_logs >> ge_poland_user_sync_logs_entry_218 >> catch_and_log_error

    return dag


rail.for_each_instance(create_dag)
