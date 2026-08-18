from datetime import timedelta
import rail
from rail.lib.ecid import get_dagrun_ecid
from impervainc.user_sync.utils import python_callable, request_payload, response_filter

null=None

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.imperva_usersync_add,
        description=f'impervainc User Sync Add {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        warnings_3 = rail.SetVariableOperator(
            task_id='warnings_3',
            name='warnings_3',
            value='NA'
        )

        reason_4 = rail.PythonOperator(
            task_id='reason_4',
            python_callable=python_callable.reason_4
        )

        if_status_contains_disable = rail.IfOperator(
            task_id='if_status_contains_disable',
            test=lambda dag_run: bool(dag_run.conf['status'] and dag_run.conf['status'].find("Disabled") >= 0),
            yes_task="imperva_user_import_logs_add_entry_6",
            no_task="if_reason_4_present"
        )

        imperva_user_import_logs_add_entry_6 = rail.WriteLogOperator(
            task_id='imperva_user_import_logs_add_entry_6',
            message="na",
            log= "{{dag_run.conf.user_sync_log}}",
            severity="Skipped",
            properties=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "childjobid": get_dagrun_ecid(dag_run),
                "loginname": dag_run.conf['Username'],
                "employeeid": dag_run.conf['Employee_ID'],
                "status": "Warning",
                "reason": "User not created, user status in Workday is Disabled.",
                "action": "Add",
                "country": dag_run.conf['Work_Address_Country']
            }
        )

        if_reason_4_present = rail.IfOperator(
            task_id='if_reason_4_present',
            test=lambda: bool(rail.result('reason_4')),
            yes_task="imperva_user_import_logs_add_entry_9",
            no_task="get_all_employeetype_details"
        )

        imperva_user_import_logs_add_entry_9 = rail.WriteLogOperator(
            task_id='imperva_user_import_logs_add_entry_9',
            message="na",
            log= "{{dag_run.conf.user_sync_log}}",
            severity="Skipped",
            properties=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "childjobid": get_dagrun_ecid(dag_run),
                "loginname": dag_run.conf['Username'],
                "employeeid": dag_run.conf['Employee_ID'],
                "status": "Warning",
                "reason": "User not created, " + str(rail.result('reason_4')),
                "action": "Add",
                "country": dag_run.conf['Work_Address_Country']
            }
        )

        get_all_employeetype_details = rail.RepliconServiceOperator(
            task_id='get_all_employeetype_details',
            endpoint='/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails'
        )

        get_all_permissionsets_12 = rail.RepliconServiceOperator(
            task_id='get_all_permissionsets_12',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets'
        )

        get_all_timezones_13 = rail.RepliconServiceOperator(
            task_id="get_all_timezones_13",
            endpoint="/services/InternationalizationService1.svc/GetAllTimeZones",
        )

        search_timezone_type_value = rail.PythonOperator(
            task_id='search_timezone_type_value',
            python_callable=python_callable.get_timezone_type_to_assign
        )

        search_employee_type_value = rail.PythonOperator(
            task_id='search_employee_type_value',
            python_callable=lambda dag_run: python_callable.search_entries_in_imperva_mapper_table(dag_run, 'Employee Type')
        )

        search_schedule_value = rail.PythonOperator(
            task_id='search_schedule_value',
            python_callable=lambda dag_run: python_callable.search_entries_in_imperva_mapper_table(dag_run, 'Schedule')
        )

        get_required_employee_type_uri = rail.PythonOperator(
            task_id='get_required_employee_type_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_employeetype_details"), "displayText",
                rail.result('search_employee_type_value'), "uri", ""
            )
        )

        if_required_employee_type_uri_present=rail.IfOperator(
            task_id='if_required_employee_type_uri_present',
            test=lambda: bool(rail.result('get_required_employee_type_uri')),
            yes_task="get_payrule_derived_name",
            no_task="add_user_sync_lookup_table_19",
        )

        add_user_sync_lookup_table_19 = rail.WriteLogOperator(
            task_id='add_user_sync_lookup_table_19',
            message="na",
            log= "{{dag_run.conf.user_sync_log}}",
            severity="Warning",
            properties=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "childjobid": get_dagrun_ecid(dag_run),
                "loginname": dag_run.conf['Username'],
                "employeeid": dag_run.conf['Employee_ID'],
                "status": "Warning",
                "reason": "User not created, employee type nor present in Replicon.",
                "action": "Add",
                "country": dag_run.conf['Work_Address_Country']
            }
        )

        get_payrule_derived_name = rail.PythonOperator(
            task_id='get_payrule_derived_name',
            python_callable=lambda dag_run: python_callable.payrule_name_derived(dag_run, config.imperva_payrule_placeholder)
        )

        search_timesheet_period_type_value = rail.PythonOperator(
            task_id='search_timesheet_period_type_value',
            python_callable=lambda dag_run: python_callable.search_entries_in_imperva_mapper_table(dag_run, 'Timesheet period')
        )

        timesheetperiod_28 = rail.SetVariableOperator(
            task_id='timesheetperiod_28',
            name='timesheetperiod_28',
            value=''
        )

        if_timesheet_period_type_value_present=rail.IfOperator(
            task_id='if_timesheet_period_type_value_present',
            test=lambda: bool(rail.result('search_timesheet_period_type_value')),
            yes_task="update_timesheet_period_uri_variable",
            no_task="create_permissionset_list",
        )

        update_timesheet_period_uri_variable = rail.SetVariableOperator(
            task_id='update_timesheet_period_uri_variable',
            name='{{ result("timesheetperiod_28").name }}',
            value="{{result('search_timesheet_period_type_value').split('|')[-1]}}"
        )

        create_permissionset_list = rail.PythonOperator(
            task_id='create_permissionset_list',
            python_callable=python_callable.create_permissionset_list
        )

        get_timezone_uri_to_assign = rail.PythonOperator(
            task_id='get_timezone_uri_to_assign',
            python_callable=python_callable.get_timezone_uri_to_assign
        )

        final_email_address = rail.PythonOperator(
            task_id='final_email_address',
            python_callable=lambda dag_run: dag_run.conf['primaryWorkEmail'] if dag_run.conf['primaryWorkEmail'] else None
        )

        final_employee_id = rail.PythonOperator(
            task_id='final_employee_id',
            python_callable=lambda dag_run: dag_run.conf['Employee_ID'] if dag_run.conf['Employee_ID'] else None
        )

        search_timesheet_approval_path_value_50 = rail.PythonOperator(
            task_id='search_timesheet_approval_path_value_50',
            python_callable=lambda dag_run: python_callable.search_entries_in_imperva_mapper_table(dag_run, 'Timesheet Approval Path')
        )

        search_timeoff_approval_path_value_51 = rail.PythonOperator(
            task_id='search_timeoff_approval_path_value_51',
            python_callable=lambda dag_run: python_callable.search_entries_in_imperva_mapper_table(dag_run, 'Time off Approval Path')
        )

        get_timesheetperiod_28 = rail.GetVariableOperator(
            task_id='get_timesheetperiod_28',
            name="{{ result('timesheetperiod_28').name }}"
        )

        create_user_in_replicon = rail.RepliconServiceOperator(
            task_id="create_user_in_replicon",
            endpoint="/services/ImportService1.svc/PutUser3",
            data=request_payload.get_user_create_payload
        )

        remove_all_timeoffs = rail.RepliconServiceOperator(
            task_id='remove_all_timeoffs',
            endpoint='/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser',
            data={
                'userUri': "{{ result('create_user_in_replicon').uri }}",
                'timeOffTypeUris': []
            }
        )

        search_timesheet_template_value_55 = rail.PythonOperator(
            task_id='search_timesheet_template_value_55',
            python_callable=lambda dag_run: python_callable.search_entries_in_imperva_mapper_table(dag_run, 'Timesheet Template')
        )

        search_timeoff_template_value_56 = rail.PythonOperator(
            task_id='search_timeoff_template_value_56',
            python_callable=lambda dag_run: python_callable.search_entries_in_imperva_mapper_table(dag_run, 'Time off Template')
        )

        search_punch_entry_policy_value_57 = rail.PythonOperator(
            task_id='search_punch_entry_policy_value_57',
            python_callable=lambda dag_run: python_callable.search_entries_in_imperva_mapper_table(dag_run, 'Punch Entry Policy')
        )

        get_all_policysets_58 = rail.RepliconServiceOperator(
            task_id='get_all_policysets_58',
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
            data_handler= lambda response: {
                "timesheettemplate" : rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', rail.result('search_timesheet_template_value_55'), 'uri', ''),
                "timeofftemplate" : rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', rail.result('search_timeoff_template_value_56'), 'uri', ''),
                "punchentrypolicy" : rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', rail.result('search_punch_entry_policy_value_57'), 'uri', '')
            }
        )

        if_timesheettemplateuri_present_62 = rail.IfOperator(
            task_id='if_timesheettemplateuri_present_62',
            test= "{{result('get_all_policysets_58').timesheettemplate | is_truthy}}",
            yes_task="assign_timesheet_template_63",
            no_task="if_timeofftemplateuri_present_65"
        )

        assign_timesheet_template_63 = rail.RepliconServiceOperator(
            task_id='assign_timesheet_template_63',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                "userUri": "{{ result('create_user_in_replicon').uri }}",
                "policySetUri": "{{ result('get_all_policysets_58').timesheettemplate }}"
            }
        )

        if_timeofftemplateuri_present_65 = rail.IfOperator(
            task_id='if_timeofftemplateuri_present_65',
            test= "{{result('get_all_policysets_58').timeofftemplate | is_truthy}}",
            yes_task="assign_timeoff_66",
            no_task="if_punchentrypolicyuri_present_68"
        )

        assign_timeoff_66 = rail.RepliconServiceOperator(
            task_id='assign_timeoff_66',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                "userUri": "{{ result('create_user_in_replicon').uri }}",
                "policySetUri": "{{ result('get_all_policysets_58').timeofftemplate }}"
            }
        )

        if_punchentrypolicyuri_present_68 = rail.IfOperator(
            task_id='if_punchentrypolicyuri_present_68',
            test= "{{result('get_all_policysets_58').punchentrypolicy | is_truthy}}",
            yes_task="assign_punchentrypolicy_69",
            no_task="if_hourly_pay_present_71"
        )

        assign_punchentrypolicy_69 = rail.RepliconServiceOperator(
            task_id='assign_punchentrypolicy_69',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                "userUri": "{{ result('create_user_in_replicon').uri }}",
                "policySetUri": "{{ result('get_all_policysets_58').punchentrypolicy }}"
            }
        )

        if_hourly_pay_present_71 = rail.IfOperator(
            task_id='if_hourly_pay_present_71',
            test= "{{dag_run.conf.Hourly_Pay | is_truthy}}",
            yes_task="get_all_currencies_72",
            no_task="get_all_custom_fields_78"
        )

        get_all_currencies_72 = rail.RepliconServiceOperator(
            task_id="get_all_currencies_72",
            endpoint="/services/CurrencyService2.svc/GetAllCurrencies",
            data_handler= lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'symbol', dag_run.conf['Currency'], 'uri', '')
        )

        if_currency_uri_present_74 = rail.IfOperator(
            task_id='if_currency_uri_present_74',
            test= "{{result('get_all_currencies_72') | is_truthy}}",
            yes_task="update_user_payroll_rate_schedule_75",
            no_task="update_warning_variable_77"
        )

        update_user_payroll_rate_schedule_75 = rail.RepliconServiceOperator(
            task_id='update_user_payroll_rate_schedule_75',
            endpoint="/services/PayrollService1.svc/UpdateUserPayrollRateScheduleOverDateRange",
            data={
                "userUri": "{{ result('create_user_in_replicon').uri }}",
                "hourlyRate": {
                    "amount": "{{ dag_run.conf.Hourly_Pay }}",
                    "currencyUri": "{{ result('get_all_currencies_72') }}"
                },
                "dateRange": null
            }
        )

        update_warning_variable_77 = rail.SetVariableOperator(
            task_id='update_warning_variable_77',
            name='{{ result("warnings_3").name }}',
            value="Pay rate not updated since the currency is not present in Replicon"
        )

        get_all_custom_fields_78 = rail.RepliconServiceOperator(
            task_id='get_all_custom_fields_78',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={
                'objectUri': "urn:replicon:object-type:user"
            }
        )

        if_original_hire_date_present_79 = rail.IfOperator(
            task_id='if_original_hire_date_present_79',
            test= "{{dag_run.conf.Original_Hire_Date | is_truthy}}",
            yes_task="get_original_hire_date_80",
            no_task="if_cost_center_id_present_84"
        )

        get_original_hire_date_80 = rail.PythonOperator(
            task_id='get_original_hire_date_80',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_custom_fields_78"), "displayText",
                "Original Hire Date", "uri", "")
        )

        if_original_hire_date_80_present_81 = rail.IfOperator(
            task_id='if_original_hire_date_80_present_81',
            test= "{{result('get_original_hire_date_80') | is_truthy}}",
            yes_task="update_original_hire_date_83",
            no_task="if_cost_center_id_present_84"
        )

        update_original_hire_date_83 = rail.RepliconServiceOperator(
            task_id='update_original_hire_date_83',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data=lambda dag_run: {
                "objectUri": rail.result('create_user_in_replicon')['uri'],
                "customFieldUri": rail.result('get_original_hire_date_80'),
                "value": python_callable.get_originalhiredate(dag_run)
            }
        )

        if_cost_center_id_present_84 = rail.IfOperator(
            task_id='if_cost_center_id_present_84',
            test= "{{dag_run.conf.Cost_Center_ID | is_truthy}}",
            yes_task="get_cost_center_id_85",
            no_task="if_job_code_present_88"
        )

        get_cost_center_id_85 = rail.PythonOperator(
            task_id='get_cost_center_id_85',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_custom_fields_78"), "displayText",
                "Cost Center - ID", "uri", "")
        )

        if_cost_center_id_present_86 = rail.IfOperator(
            task_id='if_cost_center_id_present_86',
            test= "{{result('get_cost_center_id_85') | is_truthy}}",
            yes_task="update_cost_center_id_87",
            no_task="if_job_code_present_88"
        )

        update_cost_center_id_87 = rail.RepliconServiceOperator(
            task_id='update_cost_center_id_87',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data= {
                "objectUri": "{{result('create_user_in_replicon').uri}}",
                "customFieldUri": "{{result('get_cost_center_id_85')}}",
                "value": "{{dag_run.conf.Cost_Center_ID}}"
            }
        )

        if_job_code_present_88 = rail.IfOperator(
            task_id='if_job_code_present_88',
            test= "{{dag_run.conf.Job_Code | is_truthy}}",
            yes_task="get_job_code_uri_89",
            no_task="get_imperva_worker_type_92"
        )

        get_job_code_uri_89 = rail.PythonOperator(
            task_id='get_job_code_uri_89',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_custom_fields_78"), "displayText",
                "Job Code", "uri", "")
        )

        if_job_code_uri_89_present_90 = rail.IfOperator(
            task_id='if_job_code_uri_89_present_90',
            test= "{{result('get_job_code_uri_89') | is_truthy}}",
            yes_task="update_job_code_91",
            no_task="get_imperva_worker_type_92"
        )

        update_job_code_91 = rail.RepliconServiceOperator(
            task_id='update_job_code_91',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data= {
                "objectUri": "{{result('create_user_in_replicon').uri}}",
                "customFieldUri": "{{result('get_job_code_uri_89')}}",
                "value": "{{dag_run.conf.Job_Code}}"
            }
        )

        get_imperva_worker_type_92 = rail.PythonOperator(
            task_id='get_imperva_worker_type_92',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_custom_fields_78"), "displayText",
                "Imperva Worker Type", "uri", "") if dag_run.conf['Imperva_Worker_Type'] else None
        )

        if_imperva_worker_type_92_present_93 = rail.IfOperator(
            task_id='if_imperva_worker_type_92_present_93',
            test= "{{result('get_imperva_worker_type_92') | is_truthy}}",
            yes_task="get_workertype_dropdown_94",
            no_task="get_imperva_employee_type_98"
        )

        get_workertype_dropdown_94 = rail.RepliconServiceOperator(
            task_id='get_workertype_dropdown_94',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_imperva_worker_type_92') }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf[
                'Imperva_Worker_Type'], 'uri', '')
        )

        if_workertypeuri_94_present_96 = rail.IfOperator(
            task_id='if_workertypeuri_94_present_96',
            test= "{{result('get_workertype_dropdown_94') | is_truthy}}",
            yes_task="update_workertype_uri_97",
            no_task="get_imperva_employee_type_98"
        )

        update_workertype_uri_97 = rail.RepliconServiceOperator(
            task_id='update_workertype_uri_97',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data= {
                "objectUri": "{{result('create_user_in_replicon').uri}}",
                "customFieldUri": "{{result('get_imperva_worker_type_92')}}",
                "customFieldDropDownOptionUri": "{{result('get_workertype_dropdown_94')}}"
            }
        )

        get_imperva_employee_type_98 = rail.PythonOperator(
            task_id='get_imperva_employee_type_98',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_custom_fields_78"), "displayText",
                "Imperva Employee Type", "uri", "") if dag_run.conf['Imperva_Employee_Type'] else None
        )

        if_imperva_employee_type_98_present_99 = rail.IfOperator(
            task_id='if_imperva_employee_type_98_present_99',
            test= "{{result('get_imperva_employee_type_98') | is_truthy}}",
            yes_task="get_employeetype_dropdown_100",
            no_task="get_time_type_104"
        )

        get_employeetype_dropdown_100 = rail.RepliconServiceOperator(
            task_id='get_employeetype_dropdown_100',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_imperva_employee_type_98') }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf[
                'Imperva_Employee_Type'], 'uri', '')
        )

        if_workertype_dropdown_100_present_102 = rail.IfOperator(
            task_id='if_workertype_dropdown_100_present_102',
            test= "{{result('get_employeetype_dropdown_100') | is_truthy}}",
            yes_task="update_employeetype_uri_103",
            no_task="get_time_type_104"
        )

        update_employeetype_uri_103 = rail.RepliconServiceOperator(
            task_id='update_employeetype_uri_103',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data= {
                "objectUri": "{{result('create_user_in_replicon').uri}}",
                "customFieldUri": "{{result('get_imperva_employee_type_98')}}",
                "customFieldDropDownOptionUri": "{{result('get_employeetype_dropdown_100')}}"
            }
        )

        get_time_type_104 = rail.PythonOperator(
            task_id='get_time_type_104',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_custom_fields_78"), "displayText",
                "Time Type", "uri", "") if dag_run.conf['Time_Type'] else None
        )

        if_time_type_104_present_105 = rail.IfOperator(
            task_id='if_time_type_104_present_105',
            test= "{{result('get_time_type_104') | is_truthy}}",
            yes_task="get_timetype_dropdown_106",
            no_task="get_payrate_type_110"
        )

        get_timetype_dropdown_106 = rail.RepliconServiceOperator(
            task_id='get_timetype_dropdown_106',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_time_type_104') }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf[
                'Time_Type'], 'uri', '')
        )

        if_time_type_uri_present_108 = rail.IfOperator(
            task_id='if_time_type_uri_present_108',
            test= "{{result('get_timetype_dropdown_106') | is_truthy}}",
            yes_task="update_timetype_uri_109",
            no_task="get_payrate_type_110"
        )

        update_timetype_uri_109 = rail.RepliconServiceOperator(
            task_id='update_timetype_uri_109',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data= {
                "objectUri": "{{result('create_user_in_replicon').uri}}",
                "customFieldUri": "{{result('get_time_type_104')}}",
                "customFieldDropDownOptionUri": "{{result('get_timetype_dropdown_106')}}"
            }
        )

        get_payrate_type_110 = rail.PythonOperator(
            task_id='get_payrate_type_110',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_custom_fields_78"), "displayText",
                "PayRate Type", "uri", "") if dag_run.conf['Pay_Rate_Type'] else None
        )

        if_payrate_type_110_present_111 = rail.IfOperator(
            task_id='if_payrate_type_110_present_111',
            test= "{{result('get_payrate_type_110') | is_truthy}}",
            yes_task="get_payrateuri_dropdown_112",
            no_task="get_imperva_organization_uri_116"
        )

        get_payrateuri_dropdown_112 = rail.RepliconServiceOperator(
            task_id='get_payrateuri_dropdown_112',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_payrate_type_110') }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf[
                'Pay_Rate_Type'], 'uri', '')
        )

        if_payrateuri_dropdown_112_present_114 = rail.IfOperator(
            task_id='if_payrateuri_dropdown_112_present_114',
            test= "{{result('get_payrateuri_dropdown_112') | is_truthy}}",
            yes_task="update_payratetype_uri_115",
            no_task="get_imperva_organization_uri_116"
        )

        update_payratetype_uri_115 = rail.RepliconServiceOperator(
            task_id='update_payratetype_uri_115',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data= {
                "objectUri": "{{result('create_user_in_replicon').uri}}",
                "customFieldUri": "{{result('get_payrate_type_110')}}",
                "customFieldDropDownOptionUri": "{{result('get_payrateuri_dropdown_112')}}"
            }
        )

        get_imperva_organization_uri_116 = rail.PythonOperator(
            task_id='get_imperva_organization_uri_116',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_custom_fields_78"), "displayText",
                "Imperva Organization", "uri", "") if dag_run.conf['Imperva_Organization'] else None
        )

        if_imperva_organizationuri_116_present_117 = rail.IfOperator(
            task_id='if_imperva_organizationuri_116_present_117',
            test= "{{result('get_imperva_organization_uri_116') | is_truthy}}",
            yes_task="get_organization_uri_dropdown_118",
            no_task="get_work_country_uri_122"
        )

        get_organization_uri_dropdown_118 = rail.RepliconServiceOperator(
            task_id='get_organization_uri_dropdown_118',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_imperva_organization_uri_116') }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf[
                'Imperva_Organization'], 'uri', '')
        )

        if_organizationuri_118_present_120 = rail.IfOperator(
            task_id='if_organizationuri_118_present_120',
            test= "{{result('get_organization_uri_dropdown_118') | is_truthy}}",
            yes_task="update_organization_uri_121",
            no_task="get_work_country_uri_122"
        )

        update_organization_uri_121 = rail.RepliconServiceOperator(
            task_id='update_organization_uri_121',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data= {
                "objectUri": "{{result('create_user_in_replicon').uri}}",
                "customFieldUri": "{{result('get_imperva_organization_uri_116')}}",
                "customFieldDropDownOptionUri": "{{result('get_organization_uri_dropdown_118')}}"
            }
        )

        get_work_country_uri_122 = rail.PythonOperator(
            task_id='get_work_country_uri_122',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_custom_fields_78"), "displayText",
                "Work Country", "uri", "") if dag_run.conf['Work_Address_Country'] else None
        )

        if_work_country_uri_122_present_123 = rail.IfOperator(
            task_id='if_work_country_uri_122_present_123',
            test= "{{result('get_work_country_uri_122') | is_truthy}}",
            yes_task="get_workcountry_uri_dropdown_124",
            no_task="get_country_isocode_128"
        )

        get_workcountry_uri_dropdown_124 = rail.RepliconServiceOperator(
            task_id='get_workcountry_uri_dropdown_124',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_work_country_uri_122') }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf[
                'Work_Address_Country'], 'uri', '')
        )

        if_workcountry_uri_124_present_126 = rail.IfOperator(
            task_id='if_workcountry_uri_124_present_126',
            test= "{{result('get_workcountry_uri_dropdown_124') | is_truthy}}",
            yes_task="update_workcountry_uri_127",
            no_task="get_country_isocode_128"
        )

        update_workcountry_uri_127 = rail.RepliconServiceOperator(
            task_id='update_workcountry_uri_127',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data= {
                "objectUri": "{{result('create_user_in_replicon').uri}}",
                "customFieldUri": "{{result('get_work_country_uri_122')}}",
                "customFieldDropDownOptionUri": "{{result('get_workcountry_uri_dropdown_124')}}"
            }
        )

        get_country_isocode_128 = rail.PythonOperator(
            task_id='get_country_isocode_128',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_custom_fields_78"), "displayText",
                "Country ISO Code", "uri", "") if dag_run.conf['Country_ISO_Code'] else None
        )

        if_country_isocode_128_present_129 = rail.IfOperator(
            task_id='if_country_isocode_128_present_129',
            test= "{{result('get_country_isocode_128') | is_truthy}}",
            yes_task="get_country_isocode_uri_dropdown_130",
            no_task="get_workstate_uri_134"
        )

        get_country_isocode_uri_dropdown_130 = rail.RepliconServiceOperator(
            task_id='get_country_isocode_uri_dropdown_130',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_country_isocode_128') }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf[
                'Country_ISO_Code'], 'uri', '')
        )

        if_country_isocode_uri_130_present_132 = rail.IfOperator(
            task_id='if_country_isocode_uri_130_present_132',
            test= "{{result('get_country_isocode_uri_dropdown_130') | is_truthy}}",
            yes_task="update_country_isocode_uri_133",
            no_task="get_workstate_uri_134"
        )

        update_country_isocode_uri_133 = rail.RepliconServiceOperator(
            task_id='update_country_isocode_uri_133',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data= {
                "objectUri": "{{result('create_user_in_replicon').uri}}",
                "customFieldUri": "{{result('get_country_isocode_128')}}",
                "customFieldDropDownOptionUri": "{{result('get_country_isocode_uri_dropdown_130')}}"
            }
        )

        get_workstate_uri_134 = rail.PythonOperator(
            task_id='get_workstate_uri_134',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_custom_fields_78"), "displayText",
                "Work State", "uri", "") if dag_run.conf['Work_Address_State_Province'] else None
        )

        if_workstate_uri_134_present_135 = rail.IfOperator(
            task_id='if_workstate_uri_134_present_135',
            test= "{{result('get_workstate_uri_134') | is_truthy}}",
            yes_task="get_workstate_uri_dropdown_136",
            no_task="get_state_isocode_140"
        )

        get_workstate_uri_dropdown_136 = rail.RepliconServiceOperator(
            task_id='get_workstate_uri_dropdown_136',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_workstate_uri_134') }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf[
                'Work_Address_State_Province'], 'uri', '')
        )

        if_workstate_uri_136_present_138 = rail.IfOperator(
            task_id='if_workstate_uri_136_present_138',
            test= "{{result('get_workstate_uri_dropdown_136') | is_truthy}}",
            yes_task="update_workstate_uri_139",
            no_task="get_state_isocode_140"
        )

        update_workstate_uri_139 = rail.RepliconServiceOperator(
            task_id='update_workstate_uri_139',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data= {
                "objectUri": "{{result('create_user_in_replicon').uri}}",
                "customFieldUri": "{{result('get_workstate_uri_134')}}",
                "customFieldDropDownOptionUri": "{{result('get_workstate_uri_dropdown_136')}}"
            }
        )

        get_state_isocode_140 = rail.PythonOperator(
            task_id='get_state_isocode_140',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_custom_fields_78"), "displayText",
                "State ISO Code", "uri", "") if dag_run.conf['State_ISO_Code'] else None
        )

        if_state_isocode_140_present_141 = rail.IfOperator(
            task_id='if_state_isocode_140_present_141',
            test= "{{result('get_state_isocode_140') | is_truthy}}",
            yes_task="get_state_isocode_uri_dropdown_142",
            no_task="get_exempt_status_146"
        )

        get_state_isocode_uri_dropdown_142 = rail.RepliconServiceOperator(
            task_id='get_state_isocode_uri_dropdown_142',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_state_isocode_140') }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf[
                'State_ISO_Code'], 'uri', '')
        )

        if_state_isocode_uri_142_present_144 = rail.IfOperator(
            task_id='if_state_isocode_uri_142_present_144',
            test= "{{result('get_state_isocode_uri_dropdown_142') | is_truthy}}",
            yes_task="update_state_isocode_uri_145",
            no_task="get_exempt_status_146"
        )

        update_state_isocode_uri_145 = rail.RepliconServiceOperator(
            task_id='update_state_isocode_uri_145',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data= {
                "objectUri": "{{result('create_user_in_replicon').uri}}",
                "customFieldUri": "{{result('get_state_isocode_140')}}",
                "customFieldDropDownOptionUri": "{{result('get_state_isocode_uri_dropdown_142')}}"
            }
        )

        get_exempt_status_146 = rail.PythonOperator(
            task_id='get_exempt_status_146',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_custom_fields_78"), "displayText",
                "Exempt Status", "uri", "") if dag_run.conf['Exempt_Status'] else None
        )

        if_exempt_status_146_present_147 = rail.IfOperator(
            task_id='if_exempt_status_146_present_147',
            test= "{{result('get_exempt_status_146') | is_truthy}}",
            yes_task="get_exempt_status_uri_dropdown_148",
            no_task="get_ismanager_uri_154"
        )

        get_exempt_status_uri_dropdown_148 = rail.RepliconServiceOperator(
            task_id='get_exempt_status_uri_dropdown_148',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_exempt_status_146') }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf[
                'Exempt_Status'], 'uri', '')
        )

        if_exempt_status_uri_148_present_150 = rail.IfOperator(
            task_id='if_exempt_status_uri_148_present_150',
            test= "{{result('get_exempt_status_uri_dropdown_148') | is_truthy}}",
            yes_task="update_exempt_status_uri_151",
            no_task="update_warning_variable_153"
        )

        update_exempt_status_uri_151 = rail.RepliconServiceOperator(
            task_id='update_exempt_status_uri_151',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data= {
                "objectUri": "{{result('create_user_in_replicon').uri}}",
                "customFieldUri": "{{result('get_exempt_status_146')}}",
                "customFieldDropDownOptionUri": "{{result('get_exempt_status_uri_dropdown_148')}}"
            }
        )

        update_warning_variable_153 = rail.SetVariableOperator(
            task_id='update_warning_variable_153',
            name='{{ result("warnings_3").name }}',
            value="Exempt Status not updated since the dropdown option received doesn't exist in Replicon"
        )

        get_ismanager_uri_154 = rail.PythonOperator(
            task_id='get_ismanager_uri_154',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_custom_fields_78"), "displayText",
                "Is Manager", "uri", "") if dag_run.conf['isManager'] else None
        )

        if_ismnager_uri_154_present_155 = rail.IfOperator(
            task_id='if_ismnager_uri_154_present_155',
            test= "{{result('get_ismanager_uri_154') | is_truthy}}",
            yes_task="get_ismanager_uri_dropdown_156",
            no_task="if_manager_doesnt_equal_to_username_163"
        )

        get_ismanager_uri_dropdown_156 = rail.RepliconServiceOperator(
            task_id='get_ismanager_uri_dropdown_156',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_ismanager_uri_154') }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText', "Yes" if dag_run.conf[
                'isManager'].find("1") >= 0 else "-", 'uri', '')
        )

        if_ismanager_uri_156_present_158 = rail.IfOperator(
            task_id='if_ismanager_uri_156_present_158',
            test= "{{result('get_ismanager_uri_dropdown_156') | is_truthy}}",
            yes_task="update_ismanager_uri_160",
            no_task="update_warning_variable_162"
        )

        update_ismanager_uri_160 = rail.RepliconServiceOperator(
            task_id='update_ismanager_uri_160',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data= {
                "objectUri": "{{result('create_user_in_replicon').uri}}",
                "customFieldUri": "{{result('get_ismanager_uri_154')}}",
                "customFieldDropDownOptionUri": "{{result('get_ismanager_uri_dropdown_156')}}"
            }
        )

        update_warning_variable_162 = rail.SetVariableOperator(
            task_id='update_warning_variable_162',
            name='{{ result("warnings_3").name }}',
            value="Is Manager not updated since the dropdown option received doesn't exist in Replicon"
        )

        if_manager_doesnt_equal_to_username_163 = rail.IfOperator(
            task_id='if_manager_doesnt_equal_to_username_163',
            test= "{{dag_run.conf.Manager | is_truthy and \
                dag_run.conf.Username != dag_run.conf.Manager}}",
            yes_task="search_user_replicon_164",
            no_task="if_cost_center_name_present_178"
        )

        search_user_replicon_164 = rail.RepliconServiceOperator(
            task_id="search_user_replicon_164",
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_search_user_payload,
            data_handler=response_filter.get_filtered_user_data
        )

        if_supervisoruri_present_and_status_true_169 = rail.IfOperator(
            task_id='if_supervisoruri_present_and_status_true_169',
            test="{{result('search_user_replicon_164') | is_truthy and result('search_user_replicon_164')[0].uri | is_truthy and result('search_user_replicon_164')[0].status | is_truthy}}",
            yes_task="get_assigned_permissionsets_for_supervisor_170",
            no_task="add_supervisor_assignment_lookup_table_177"
        )

        get_assigned_permissionsets_for_supervisor_170 = rail.RepliconServiceOperator(
            task_id='get_assigned_permissionsets_for_supervisor_170',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_user_replicon_164')[0].uri }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', 'urn:replicon:policy:supervision', 'user.uri', '') if rail.result('search_user_replicon_164')[0]['status'] else None
        )

        if_permissionset_uri_present_172 = rail.IfOperator(
            task_id='if_permissionset_uri_present_172',
            test="{{result('get_assigned_permissionsets_for_supervisor_170') | is_truthy}}",
            yes_task="add_supervisor_assignment_lookup_table_173",
            no_task="update_initial_supervisor_175"
        )

        add_supervisor_assignment_lookup_table_173 = rail.WriteLogOperator(
            task_id='add_supervisor_assignment_lookup_table_173',
            message="NA",
            log="{{ dag_run.conf.supervisor_sync_log }}",
            severity='Success',
            properties=lambda dag_run: {
                'parentjobid': dag_run.conf['parentjobid'],
                'enduseruri': rail.result('create_user_in_replicon')['uri'],
                'supervisorid': dag_run.conf['Manager'],
                'status': "add",
                'loginname': dag_run.conf['Username']
            }
        )

        update_initial_supervisor_175 = rail.RepliconServiceOperator(
            task_id='update_initial_supervisor_175',
            endpoint='/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange',
            data= {
                'userUri': "{{result('create_user_in_replicon').uri}}",
                'supervisorUri': "{{ result('search_user_replicon_164')[0].uri }}"
            }
        )

        add_supervisor_assignment_lookup_table_177 = rail.WriteLogOperator(
            task_id='add_supervisor_assignment_lookup_table_177',
            message="NA",
            log="{{ dag_run.conf.supervisor_sync_log }}",
            severity='Success',
            properties=lambda dag_run: {
                'parentjobid': dag_run.conf['parentjobid'],
                'enduseruri': rail.result('create_user_in_replicon')['uri'],
                'supervisorid': dag_run.conf['Manager'],
                'status': "add",
                'loginname': dag_run.conf['Username']
            }
        )

        if_cost_center_name_present_178 = rail.IfOperator(
            task_id='if_cost_center_name_present_178',
            test= "{{dag_run.conf.Cost_Center_Name | is_truthy}}",
            yes_task="get_all_costcenters_179",
            no_task="search_workweek_value_184"
        )

        get_all_costcenters_179 = rail.RepliconServiceOperator(
            task_id="get_all_costcenters_179",
            endpoint="/services/CostCenterService1.svc/GetAllCostCenters",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText',
               dag_run.conf['Cost_Center_Name'], 'uri', '')
        )

        if_costcenter_uri_present_181 = rail.IfOperator(
            task_id='if_costcenter_uri_present_181',
            test="{{result('get_all_costcenters_179') | is_truthy}}",
            yes_task="put_cost_center_schedule_for_user",
            no_task="search_workweek_value_184"
        )

        put_cost_center_schedule_for_user = rail.RepliconServiceOperator(
            task_id='put_cost_center_schedule_for_user',
            endpoint="/services/CostCenterService1.svc/PutCostCenterScheduleForUser",
            data={
                "userUri": "{{result('create_user_in_replicon').uri}}",
                "scheduleEntries":  [
                    {
                        "costCenter": {
                            "uri": "{{result('get_all_costcenters_179')}}",
                            "parentUri": null,
                            "name": null
                        },
                    "effectiveDate": null
                    }
                ]
            }
        )

        search_workweek_value_184 = rail.PythonOperator(
            task_id='search_workweek_value_184',
            python_callable=lambda dag_run: python_callable.search_entries_in_imperva_mapper_table(dag_run, 'Work Week')
        )

        get_workweek_uri_185 = rail.PythonOperator(
            task_id='get_workweek_uri_185',
            python_callable=lambda: rail.result(
                "search_workweek_value_184").split('|')[-1].strip() if rail.result(
                "search_workweek_value_184") else None
        )

        if_workweek_uri_present_186 = rail.IfOperator(
            task_id='if_workweek_uri_present_186',
            test="{{result('get_workweek_uri_185') | is_truthy}}",
            yes_task="update_workweek_startday_187",
            no_task="get_all_holiday_calendar_188"
        )

        update_workweek_startday_187 = rail.RepliconServiceOperator(
            task_id='update_workweek_startday_187',
            endpoint="/services/UserService1.svc/UpdateWorkWeekStartDayForUser",
            data={
                'userUri': "{{result('create_user_in_replicon').uri}}",
                "dayOfWeekUri": "{{ result('get_workweek_uri_185') }}"
            }
        )

        get_all_holiday_calendar_188 = rail.RepliconServiceOperator(
            task_id="get_all_holiday_calendar_188",
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars"
        )

        if_country_isocode_present_189 = rail.IfOperator(
            task_id='if_country_isocode_present_189',
            test="{{dag_run.conf.Country_ISO_Code | is_truthy}}",
            yes_task="create_holiday_calendar_list_190",
            no_task="get_all_activities_196"
        )

        create_holiday_calendar_list_190 = rail.PythonOperator(
            task_id='create_holiday_calendar_list_190',
            python_callable=lambda: python_callable.create_holiday_calendar_list(
                rail.result('get_all_holiday_calendar_188')
            )
        )

        holiday_calendar_uri_193 = rail.PythonOperator(
            task_id='holiday_calendar_uri_193',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result("create_holiday_calendar_list_190"), "compare",
                dag_run.conf['Country_ISO_Code'], "uri")
        )

        if_holiday_calendar_uri_present_194 = rail.IfOperator(
            task_id='if_holiday_calendar_uri_present_194',
            test="{{result('holiday_calendar_uri_193') | is_truthy}}",
            yes_task="assign_holiday_calendar_195",
            no_task="get_all_activities_196"
        )

        assign_holiday_calendar_195 = rail.RepliconServiceOperator(
            task_id='assign_holiday_calendar_195',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
                'userUri': "{{result('create_user_in_replicon').uri}}",
                "holidayCalendarUri": "{{ result('holiday_calendar_uri_193') }}"
            }
        )

        get_all_activities_196 = rail.RepliconServiceOperator(
            task_id='get_all_activities_196',
            endpoint="/services/ActivityService1.svc/GetAllActivities",
            data_handler=response_filter.get_activityuris
        )

        if_country_isocode_present_197 = rail.IfOperator(
            task_id='if_country_isocode_present_197',
            test=lambda dag_run: bool(dag_run.conf['Country_ISO_Code'] and len(rail.result('get_all_activities_196')) > 0),
            yes_task="assign_activity_200",
            no_task="get_all_timeoff_types_202"
        )

        assign_activity_200 = rail.RepliconServiceOperator(
            task_id='assign_activity_200',
            endpoint="/services/ActivityService1.svc/PutActivityAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result('create_user_in_replicon')['uri'],
                "activityUris": rail.result('get_all_activities_196')
            }
        )

        get_all_timeoff_types_202 = rail.RepliconServiceOperator(
            task_id='get_all_timeoff_types_202',
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes"
        )

        create_timeoffuriall_list_203 = rail.PythonOperator(
            task_id='create_timeoffuriall_list_203',
            python_callable=lambda: python_callable.create_timeoffuriall_list_203(
                rail.result('get_all_timeoff_types_202')
            )
        )

        get_bulktimeoff_details_207 = rail.RepliconServiceOperator(
            task_id='get_bulktimeoff_details_207',
            endpoint='/services/TimeOffService1.svc/BulkGetTimeOffTypeDetails',
            data=lambda: {
                "timeOffTypeUris": rail.result('create_timeoffuriall_list_203')
            }
        )

        create_timeoff_list_208_223 = rail.PythonOperator(
            task_id='create_timeoff_list_208_223',
            python_callable=lambda dag_run: python_callable.create_timeoff_list_208_223(
                rail.result('get_all_timeoff_types_202'),
                rail.result('get_bulktimeoff_details_207'),
                dag_run
            )
        )

        if_timeoff_list_contains_urn_224 = rail.IfOperator(
            task_id='if_timeoff_list_contains_urn_224',
            test=lambda: bool(rail.result('create_timeoff_list_208_223')['uris']),
            yes_task="put_timeoff_types_225",
            no_task="get_warnings_value"
        )

        put_timeoff_types_225 = rail.RepliconServiceOperator(
            task_id='put_timeoff_types_225',
            endpoint='/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser',
            data=lambda: {
                'userUri': rail.result('create_user_in_replicon')['uri'],
                'timeOffTypeUris': rail.result('create_timeoff_list_208_223')['uris']
            }
        )

        # pylint: disable=unnecessary-lambda
        trigger_timeoff_add_user_child = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_timeoff_add_user_child',
            retries=0,
            items='{{ result("create_timeoff_list_208_223").timeoff_list | to_json }}',
            trigger_dag_id=config.imperva_user_sync_timeoff_add_user,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, dag_run:request_payload.get_timeoff_add_payload(item, dag_run)
        )

        wait_for_trigger_timeoff_add_user_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_trigger_timeoff_add_user_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_timeoff_add_user_child") }}'
        )

        get_warnings_value = rail.GetVariableOperator(
            task_id='get_warnings_value',
            name="{{ result('warnings_3').name }}"
        )

        imperva_user_import_logs_add_entry_228 = rail.WriteLogOperator(
            task_id='imperva_user_import_logs_add_entry_228',
            message="na",
            log= "{{dag_run.conf.user_sync_log}}",
            severity="Success",
            properties=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "childjobid": get_dagrun_ecid(dag_run),
                "loginname": dag_run.conf['Username'],
                "employeeid": dag_run.conf['Employee_ID'],
                "status": "Success" if rail.result('get_warnings_value')['value'].find("NA") >= 0 else "Warning",
                "reason": rail.result('get_warnings_value')['value'],
                "action": "Add",
                "country": dag_run.conf['Work_Address_Country']
            }
        )

        finish=rail.EmptyOperator(
            task_id='finish'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log="{{dag_run.conf.user_sync_log}}",
            message="Error | {{ get_error_message() }}",
            severity="Error",
            properties= {
                "parentjobid": "{{dag_run.conf.parentjobid}}",
                "childjobid": "{{ dag_run_ecid() }}",
                "loginname": "{{dag_run.conf.Username}}",
                "employeeid": "{{dag_run.conf.Employee_ID}}",
                "status": "Error",
                "reason": "{{result('get_warnings_value').value}},{{get_error_message()}}",
                "action": "Add",
                "country": "{{dag_run.conf.Work_Address_Country}}"
            }
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        warnings_3 >> reason_4 >> if_status_contains_disable >> rail.Label("Yes") >> imperva_user_import_logs_add_entry_6 >> finish
        if_status_contains_disable >> rail.Label("No") >> if_reason_4_present >> rail.Label("Yes") >> imperva_user_import_logs_add_entry_9 >> finish
        if_reason_4_present >> rail.Label("No") >> get_all_employeetype_details >> get_all_permissionsets_12 >> get_all_timezones_13 >> \
        search_timezone_type_value >> search_employee_type_value >> search_schedule_value >> get_required_employee_type_uri >> \
        if_required_employee_type_uri_present >> rail.Label("Yes") >> get_payrule_derived_name >> search_timesheet_period_type_value >> \
        timesheetperiod_28 >> if_timesheet_period_type_value_present >> rail.Label("Yes") >> update_timesheet_period_uri_variable >> create_permissionset_list
        if_timesheet_period_type_value_present >> rail.Label("No") >> create_permissionset_list >> get_timezone_uri_to_assign >> final_email_address >> \
        final_employee_id >> search_timesheet_approval_path_value_50 >> search_timeoff_approval_path_value_51 >> get_timesheetperiod_28 >> \
        create_user_in_replicon >> remove_all_timeoffs >> search_timesheet_template_value_55 >> search_timeoff_template_value_56 >> \
        search_punch_entry_policy_value_57 >> get_all_policysets_58 >> if_timesheettemplateuri_present_62 >> rail.Label(
            "Yes") >> assign_timesheet_template_63 >> if_timeofftemplateuri_present_65
        if_timesheettemplateuri_present_62 >> rail.Label(
            "No") >> if_timeofftemplateuri_present_65 >> rail.Label("Yes") >> assign_timeoff_66 >> if_punchentrypolicyuri_present_68
        if_timeofftemplateuri_present_65 >> rail.Label("No") >> if_punchentrypolicyuri_present_68 >> rail.Label(
            "Yes") >> assign_punchentrypolicy_69 >> if_hourly_pay_present_71
        if_punchentrypolicyuri_present_68 >> rail.Label(
            "No") >> if_hourly_pay_present_71 >> rail.Label("Yes") >> get_all_currencies_72 >> if_currency_uri_present_74 >> rail.Label(
            "Yes") >> update_user_payroll_rate_schedule_75 >> get_all_custom_fields_78
        if_currency_uri_present_74 >> rail.Label(
            "No") >> update_warning_variable_77 >> get_all_custom_fields_78
        if_hourly_pay_present_71 >> rail.Label("No") >> get_all_custom_fields_78 >> if_original_hire_date_present_79 >> rail.Label(
            "Yes") >> get_original_hire_date_80 >> if_original_hire_date_80_present_81 >> rail.Label(
            "Yes") >> update_original_hire_date_83 >> if_cost_center_id_present_84
        if_original_hire_date_80_present_81 >> rail.Label("No") >> if_cost_center_id_present_84
        if_original_hire_date_present_79 >> rail.Label(
            "No") >> if_cost_center_id_present_84 >> rail.Label("Yes") >> get_cost_center_id_85 >> if_cost_center_id_present_86 >> rail.Label(
            "Yes") >> update_cost_center_id_87 >> if_job_code_present_88
        if_cost_center_id_present_86 >> rail.Label(
            "No") >> if_job_code_present_88
        if_cost_center_id_present_84 >> rail.Label("No") >> if_job_code_present_88 >> rail.Label("Yes") >> get_job_code_uri_89 >> \
        if_job_code_uri_89_present_90 >> rail.Label("Yes") >> update_job_code_91 >> get_imperva_worker_type_92
        if_job_code_uri_89_present_90 >> rail.Label("No") >> get_imperva_worker_type_92
        if_job_code_present_88 >> rail.Label("No") >> get_imperva_worker_type_92 >> if_imperva_worker_type_92_present_93 >> rail.Label(
            "Yes") >> get_workertype_dropdown_94 >> if_workertypeuri_94_present_96 >> rail.Label(
            "Yes") >> update_workertype_uri_97 >> get_imperva_employee_type_98
        if_workertypeuri_94_present_96 >> rail.Label("No") >> get_imperva_employee_type_98
        if_imperva_worker_type_92_present_93 >> rail.Label(
            "No") >> get_imperva_employee_type_98 >> if_imperva_employee_type_98_present_99 >> rail.Label(
            "Yes") >> get_employeetype_dropdown_100 >> if_workertype_dropdown_100_present_102 >> rail.Label(
            "Yes") >> update_employeetype_uri_103 >> get_time_type_104
        if_workertype_dropdown_100_present_102 >> rail.Label(
            "No") >> get_time_type_104
        if_imperva_employee_type_98_present_99 >> rail.Label(
            "No") >> get_time_type_104 >> if_time_type_104_present_105 >> rail.Label("Yes") >> get_timetype_dropdown_106 >> \
        if_time_type_uri_present_108 >> rail.Label("Yes") >> update_timetype_uri_109 >> get_payrate_type_110
        if_time_type_uri_present_108 >> rail.Label("No") >> get_payrate_type_110
        if_time_type_104_present_105 >> rail.Label("No") >> get_payrate_type_110 >> if_payrate_type_110_present_111 >> rail.Label(
            "Yes") >> get_payrateuri_dropdown_112 >> if_payrateuri_dropdown_112_present_114 >> rail.Label(
            "Yes") >> update_payratetype_uri_115 >> get_imperva_organization_uri_116
        if_payrateuri_dropdown_112_present_114 >> rail.Label(
            "No") >> get_imperva_organization_uri_116
        if_payrate_type_110_present_111 >> rail.Label(
            "No") >> get_imperva_organization_uri_116 >> if_imperva_organizationuri_116_present_117 >> rail.Label(
            "Yes") >> get_organization_uri_dropdown_118 >> if_organizationuri_118_present_120 >> rail.Label(
            "Yes") >> update_organization_uri_121 >> get_work_country_uri_122
        if_organizationuri_118_present_120 >> rail.Label(
            "No") >> get_work_country_uri_122
        if_imperva_organizationuri_116_present_117 >> rail.Label("No") >> get_work_country_uri_122 >> if_work_country_uri_122_present_123 >> rail.Label(
            "Yes") >>get_workcountry_uri_dropdown_124 >> if_workcountry_uri_124_present_126 >> rail.Label(
            "Yes") >> update_workcountry_uri_127 >> get_country_isocode_128
        if_workcountry_uri_124_present_126 >> rail.Label(
            "No") >> get_country_isocode_128
        if_work_country_uri_122_present_123 >> rail.Label(
            "No") >> get_country_isocode_128 >> if_country_isocode_128_present_129 >> rail.Label(
            "Yes") >> get_country_isocode_uri_dropdown_130 >> if_country_isocode_uri_130_present_132 >> rail.Label(
            "Yes") >> update_country_isocode_uri_133 >> get_workstate_uri_134
        if_country_isocode_uri_130_present_132 >> rail.Label(
            "No") >> get_workstate_uri_134
        if_country_isocode_128_present_129 >> rail.Label(
            "No") >> get_workstate_uri_134 >> if_workstate_uri_134_present_135 >> rail.Label(
            "Yes") >> get_workstate_uri_dropdown_136 >> if_workstate_uri_136_present_138 >> rail.Label(
            "Yes") >> update_workstate_uri_139 >> get_state_isocode_140
        if_workstate_uri_136_present_138 >> rail.Label(
            "No") >> get_state_isocode_140
        if_workstate_uri_134_present_135 >> rail.Label(
            "No") >> get_state_isocode_140 >> if_state_isocode_140_present_141 >> rail.Label(
            "Yes") >> get_state_isocode_uri_dropdown_142 >> if_state_isocode_uri_142_present_144 >> rail.Label(
            "Yes") >> update_state_isocode_uri_145 >> get_exempt_status_146
        if_state_isocode_uri_142_present_144 >> rail.Label(
            "No") >> get_exempt_status_146
        if_state_isocode_140_present_141 >> rail.Label(
            "No") >> get_exempt_status_146 >> if_exempt_status_146_present_147 >> rail.Label(
            "Yes") >> get_exempt_status_uri_dropdown_148 >> if_exempt_status_uri_148_present_150 >> rail.Label(
            "Yes") >> update_exempt_status_uri_151 >> get_ismanager_uri_154
        if_exempt_status_uri_148_present_150 >> rail.Label(
            "No") >> update_warning_variable_153 >> get_ismanager_uri_154
        if_exempt_status_146_present_147 >> rail.Label(
            "No") >> get_ismanager_uri_154 >> if_ismnager_uri_154_present_155 >> rail.Label(
            "Yes") >> get_ismanager_uri_dropdown_156 >> if_ismanager_uri_156_present_158 >> rail.Label(
            "Yes") >> update_ismanager_uri_160 >> if_manager_doesnt_equal_to_username_163
        if_ismanager_uri_156_present_158 >> rail.Label(
            "No") >> update_warning_variable_162 >> if_manager_doesnt_equal_to_username_163
        if_ismnager_uri_154_present_155 >> rail.Label(
            "No") >> if_manager_doesnt_equal_to_username_163 >> rail.Label(
            "Yes") >> search_user_replicon_164 >> if_supervisoruri_present_and_status_true_169 >> rail.Label(
            "Yes") >> get_assigned_permissionsets_for_supervisor_170 >> if_permissionset_uri_present_172 >> rail.Label(
            "Yes") >> add_supervisor_assignment_lookup_table_173 >> if_cost_center_name_present_178
        if_permissionset_uri_present_172 >> rail.Label(
            "No") >> update_initial_supervisor_175 >> if_cost_center_name_present_178
        if_supervisoruri_present_and_status_true_169 >> rail.Label(
            "NO") >> add_supervisor_assignment_lookup_table_177 >> if_cost_center_name_present_178
        if_manager_doesnt_equal_to_username_163 >> rail.Label(
            "No") >> if_cost_center_name_present_178 >> rail.Label("Yes") >> get_all_costcenters_179 >> if_costcenter_uri_present_181 >> rail.Label(
            "Yes") >> put_cost_center_schedule_for_user >> search_workweek_value_184
        if_costcenter_uri_present_181 >> rail.Label(
            "No") >> search_workweek_value_184
        if_cost_center_name_present_178 >> rail.Label("No") >> search_workweek_value_184 >> get_workweek_uri_185 >> \
        if_workweek_uri_present_186 >> rail.Label("Yes") >> update_workweek_startday_187 >> get_all_holiday_calendar_188
        if_workweek_uri_present_186 >> rail.Label("No") >> get_all_holiday_calendar_188 >> if_country_isocode_present_189 >> rail.Label(
            "Yes") >> create_holiday_calendar_list_190 >> holiday_calendar_uri_193 >> if_holiday_calendar_uri_present_194 >> rail.Label(
            "Yes") >> assign_holiday_calendar_195 >> get_all_activities_196
        if_holiday_calendar_uri_present_194 >> rail.Label(
            "No") >> get_all_activities_196
        if_country_isocode_present_189 >> rail.Label(
            "No") >> get_all_activities_196 >> if_country_isocode_present_197 >> rail.Label(
            "Yes") >> assign_activity_200 >> get_all_timeoff_types_202
        if_country_isocode_present_197 >> rail.Label(
            "No") >> get_all_timeoff_types_202 >> create_timeoffuriall_list_203 >> get_bulktimeoff_details_207 >> create_timeoff_list_208_223 >> \
        if_timeoff_list_contains_urn_224 >> rail.Label("Yes") >> put_timeoff_types_225 >> trigger_timeoff_add_user_child >> \
        wait_for_trigger_timeoff_add_user_child >> get_warnings_value >> imperva_user_import_logs_add_entry_228
        if_timeoff_list_contains_urn_224 >> rail.Label("No") >> get_warnings_value >> imperva_user_import_logs_add_entry_228 >> finish
        if_required_employee_type_uri_present >> rail.Label("No") >> add_user_sync_lookup_table_19 >> finish
        finish >> catch_and_log_errors >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
