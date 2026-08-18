from datetime import timedelta
import rail
import json
from airflow.models import Variable
from rail.lib.ecid import get_dagrun_ecid
from assuredpartnersinc.user_import_v3.utils import python_callable, request_payload

null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_add_user_dag_id,
        description=f'Assured Partners User Import Add User Child {config.instance}',
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
            no_task='exception_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='exception_log',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        exception_log = rail.CreateLogOperator(
            task_id='exception_log',
        )

        assured_partners_user_sync_master_mapper_search_entries_8 = rail.PythonOperator(
            task_id='assured_partners_user_sync_master_mapper_search_entries_8',
            python_callable=lambda:  list(
                filter(lambda x: x["country"] == "global", config.MASTER_MAPPER))
        )

        email_variable = rail.SetVariableOperator(
            task_id='email_variable',
            name='email',
            value=None
        )

        if_request_e_mail_present_10 = rail.IfOperator(
            task_id='if_request_e_mail_present_10',
            test=lambda dag_run: bool(
                dag_run.conf['E_Mail'] and "@" in dag_run.conf['E_Mail']),
            yes_task="update_email_variable",
            no_task="exception_log_entry_13",
        )

        update_email_variable = rail.SetVariableOperator(
            task_id='update_email_variable',
            name='email',
            value='{{dag_run.conf.E_Mail}}'
        )

        exception_log_entry_13 = rail.WriteLogOperator(
            task_id='exception_log_entry_13',
            log='{{ result("exception_log") }}',
            message='Exception',
            severity='Exception',
            properties={
                "value": "Email not updated since email field received is blank/incorrect value"
            }
        )

        get_all_policy_sets_14 = rail.RepliconServiceOperator(
            task_id='get_all_policy_sets_14',
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
            data_handler=python_callable.get_required_policy_uris
        )

        get_all_holiday_calendars_15 = rail.RepliconServiceOperator(
            task_id='get_all_holiday_calendars_15',
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['HolidayCalendars'], 'uri') if response else null
        )

        get_all_scripts_16 = rail.RepliconServiceOperator(
            task_id='get_all_scripts_16',
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['PayRules'], 'uri') if response else null
        )

        get_all_office_schedules_17 = rail.RepliconServiceOperator(
            task_id='get_all_office_schedules_17',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['Schedule'], 'uri') if response else null
        )

        get_all_permission_sets_18 = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets_18',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data_handler=lambda response: {
                "supervisor": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Supervisor', 'uri')
            }
        )

        declare_list_19 = rail.SetVariableOperator(
            task_id='declare_list_19',
            append=False,
            name='policy_list',
            value=[]
        )

        if_request_timesheettemplate_present_20 = rail.IfOperator(
            task_id='if_request_timesheettemplate_present_20',
            test="{{ dag_run.conf.TimesheetTemplate | is_truthy }}",
            yes_task="if_log_required_timesheet_template_uri_present_22",
            no_task="exception_log_entry_27",
        )

        if_log_required_timesheet_template_uri_present_22 = rail.IfOperator(
            task_id='if_log_required_timesheet_template_uri_present_22',
            test="{{result('get_all_policy_sets_14').timesheet_template_uri | is_truthy }}",
            yes_task="insert_to_list_23",
            no_task="exception_log_entry_25",
        )

        insert_to_list_23 = rail.SetVariableOperator(
            task_id='insert_to_list_23',
            append=True,
            name='{{ result("declare_list_19").name }}',
            value={
                "uri": "{{ result('get_all_policy_sets_14').timesheet_template_uri }}",
                "name": null
            }
        )

        exception_log_entry_25 = rail.WriteLogOperator(
            task_id='exception_log_entry_25',
            log='{{ result("exception_log") }}',
            message='Exception',
            severity='Exception',
            properties={
                "value": "Timesheet template not updated since {{ dag_run.conf.TimesheetTemplate }} not available in Replicon"
            }
        )

        exception_log_entry_27 = rail.WriteLogOperator(
            task_id='exception_log_entry_27',
            log='{{ result("exception_log") }}',
            message='Exception',
            severity='Exception',
            properties={
                "value": "Timesheet template not updated since blank value received from the feedfile"
            }
        )

        if_request_timeofftemplate_present_28 = rail.IfOperator(
            task_id='if_request_timeofftemplate_present_28',
            test="{{ dag_run.conf.TimeOffTemplate | is_truthy }}",
            yes_task="if_log_required_timeoff_template_uri_30_present_31",
            no_task="exception_log_entry_36",
        )

        if_log_required_timeoff_template_uri_30_present_31 = rail.IfOperator(
            task_id='if_log_required_timeoff_template_uri_30_present_31',
            test="{{ result('get_all_policy_sets_14').timeoff_template_uri | is_truthy }}",
            yes_task="insert_to_list_32",
            no_task="exception_log_entry_34",
        )

        insert_to_list_32 = rail.SetVariableOperator(
            task_id='insert_to_list_32',
            append=True,
            name='{{ result("declare_list_19").name }}',
            value={
                "uri": "{{ result('get_all_policy_sets_14').timeoff_template_uri }}",
                "name": null
            }
        )

        exception_log_entry_34 = rail.WriteLogOperator(
            task_id='exception_log_entry_34',
            log='{{ result("exception_log") }}',
            message='Exception',
            severity='Exception',
            properties={
                "value": "Time off  template not updated since {{ dag_run.conf.TimeOffTemplate }} not available in Replicon"
            }
        )

        exception_log_entry_36 = rail.WriteLogOperator(
            task_id='exception_log_entry_36',
            log='{{ result("exception_log") }}',
            message='Exception',
            severity='Exception',
            properties={
                "value": "Time off template not updated since blank value received from the feedfile"
            }
        )

        if_request_punch_entry_policy_present_37 = rail.IfOperator(
            task_id='if_request_punch_entry_policy_present_37',
            test="{{ dag_run.conf.punch_entry_policy | is_truthy }}",
            yes_task="if_log_required_punchentrypolicy_38_present_39",
            no_task="exception_log_entry_44",
        )

        if_log_required_punchentrypolicy_38_present_39 = rail.IfOperator(
            task_id='if_log_required_punchentrypolicy_38_present_39',
            test="{{ result('get_all_policy_sets_14').punch_entry_policy_uri | is_truthy }}",
            yes_task="insert_to_list_40",
            no_task="exception_log_entry_42",
        )

        insert_to_list_40 = rail.SetVariableOperator(
            task_id='insert_to_list_40',
            append=True,
            name='{{ result("declare_list_19").name }}',
            value={
                "uri": "{{ result('get_all_policy_sets_14').punch_entry_policy_uri }}",
                "name": null
            }
        )

        exception_log_entry_42 = rail.WriteLogOperator(
            task_id='exception_log_entry_42',
            log='{{ result("exception_log") }}',
            message='Exception',
            severity='Exception',
            properties={
                "value": "Punch entry policy not updated since {{ dag_run.conf.punch_entry_policy }} not available in Replicon"
            }
        )

        exception_log_entry_44 = rail.WriteLogOperator(
            task_id='exception_log_entry_44',
            log='{{ result("exception_log") }}',
            message='Exception',
            severity='Exception',
            properties={
                "value": "Punch entry policy not updated since blank value received from the feedfile"
            }
        )

        log_policy_settoassign_45 = rail.PythonOperator(
            task_id='log_policy_settoassign_45',
            python_callable=lambda: rail.get_dag_run_var(
                rail.result('declare_list_19')['name'])
        )

        declare_variable_46 = rail.SetVariableOperator(
            task_id='declare_variable_46',
            append=False,
            name='Holidaycalendar',
            value=None
        )

        if_request_holidaycalendars_present_47 = rail.IfOperator(
            task_id='if_request_holidaycalendars_present_47',
            test="{{ dag_run.conf.HolidayCalendars | is_truthy }}",
            yes_task="if_log_required_holiday_calendar_present_49",
            no_task="exception_log_entry_54",
        )

        if_log_required_holiday_calendar_present_49 = rail.IfOperator(
            task_id='if_log_required_holiday_calendar_present_49',
            test="{{ result('get_all_holiday_calendars_15') | is_truthy }}",
            yes_task="update_variable_50",
            no_task="exception_log_entry_52",
        )

        update_variable_50 = rail.SetVariableOperator(
            task_id='update_variable_50',
            append=False,
            name='{{ result("declare_variable_46").name }}',
            value={
                "uri": "{{ result('get_all_holiday_calendars_15') }}",
                "name": null
            }
        )

        exception_log_entry_52 = rail.WriteLogOperator(
            task_id='exception_log_entry_52',
            log='{{ result("exception_log") }}',
            message='Exception',
            severity='Exception',
            properties={
                "value": "Holdiay calendar not assigned since {{ dag_run.conf.HolidayCalendars }} not avaialble in Replicon"
            }
        )

        exception_log_entry_54 = rail.WriteLogOperator(
            task_id='exception_log_entry_54',
            log='{{ result("exception_log") }}',
            message='Exception',
            severity='Exception',
            properties={
                "value": "Holdiay calendar not assigned since blank value received"
            }
        )

        declare_variable_55 = rail.SetVariableOperator(
            task_id='declare_variable_55',
            append=False,
            name='timezone',
            value=null
        )

        if_request_timezone_present_56 = rail.IfOperator(
            task_id='if_request_timezone_present_56',
            test="{{ dag_run.conf.TimeZone | is_truthy }}",
            yes_task="if_request_timezoneuri_present_57",
            no_task="exception_log_entry_62",
        )

        if_request_timezoneuri_present_57 = rail.IfOperator(
            task_id='if_request_timezoneuri_present_57',
            test="{{ dag_run.conf.timezoneuri | is_truthy }}",
            yes_task="update_variable_58",
            no_task="exception_log_entry_60",
        )

        update_variable_58 = rail.SetVariableOperator(
            task_id='update_variable_58',
            append=False,
            name='{{ result("declare_variable_55").name }}',
            value={
                "uri": "{{ dag_run.conf.timezoneuri }}",
                "IANAName": null
            }
        )

        exception_log_entry_60 = rail.WriteLogOperator(
            task_id='exception_log_entry_60',
            log='{{ result("exception_log") }}',
            message='Exception',
            severity='Exception',
            properties={
                "value": "Timezone not assigned since {{ dag_run.conf.TimeZone }} is not avaialble in Replicon"
            }
        )

        exception_log_entry_62 = rail.WriteLogOperator(
            task_id='exception_log_entry_62',
            log='{{ result("exception_log") }}',
            message='Exception',
            severity='Exception',
            properties={
                "value": "Time zone not assigned since blank value received"
            }
        )

        declare_variable_63 = rail.SetVariableOperator(
            task_id='declare_variable_63',
            append=False,
            name='workweek',
            value=None
        )

        if_request_workweek_present_64 = rail.IfOperator(
            task_id='if_request_workweek_present_64',
            test="{{ dag_run.conf.WorkWeek | is_truthy }}",
            yes_task="update_variable_65",
            no_task="exception_log_entry_67",
        )

        update_variable_65 = rail.SetVariableOperator(
            task_id='update_variable_65',
            append=False,
            name='{{ result("declare_variable_63").name }}',
            value=lambda dag_run: list(filter(lambda x: x["country"] == "global" and x["type"] ==
                                       "workweek" and x["identifier_1"] == dag_run.conf['WorkWeek'], config.MASTER_MAPPER))[0]['value']
        )

        exception_log_entry_67 = rail.WriteLogOperator(
            task_id='exception_log_entry_67',
            log='{{ result("exception_log") }}',
            message='Exception',
            severity='Exception',
            properties={
                "value": "Work week not updated since blank value received"
            }
        )

        declare_variable_68 = rail.SetVariableOperator(
            task_id='declare_variable_68',
            append=False,
            name='payrule',
            value=None
        )

        if_request_payrules_present_69 = rail.IfOperator(
            task_id='if_request_payrules_present_69',
            test="{{ dag_run.conf.PayRules | is_truthy }}",
            yes_task="if_log_required_payrule_70_present_71",
            no_task="exception_log_entry_76",
        )

        if_log_required_payrule_70_present_71 = rail.IfOperator(
            task_id='if_log_required_payrule_70_present_71',
            test="{{ result('get_all_scripts_16') | is_truthy }}",
            yes_task="update_variable_72",
            no_task="exception_log_entry_74",
        )

        update_variable_72 = rail.SetVariableOperator(
            task_id='update_variable_72',
            append=False,
            name='{{ result("declare_variable_68").name }}',
            value=[{
                "payRuleScript": {
                    "uri": "{{ result('get_all_scripts_16') }}",
                    "name": null
                },
                "effectiveDate": null
            }]
        )

        exception_log_entry_74 = rail.WriteLogOperator(
            task_id='exception_log_entry_74',
            log='{{ result("exception_log") }}',
            message='Exception',
            severity='Exception',
            properties={
                "value": "Payrule not assigned since {{ dag_run.conf.PayRules }} is not available in Replicon"
            }
        )

        exception_log_entry_76 = rail.WriteLogOperator(
            task_id='exception_log_entry_76',
            log='{{ result("exception_log") }}',
            message='Exception',
            severity='Exception',
            properties={
                "value": "Payrule not assigned since blank value received"
            }
        )

        declare_variable_77 = rail.SetVariableOperator(
            task_id='declare_variable_77',
            append=False,
            name='schedule',
            value=None
        )

        if_request_schedule_present_78 = rail.IfOperator(
            task_id='if_request_schedule_present_78',
            test="{{ dag_run.conf.Schedule | is_truthy }}",
            yes_task="if_request_schedule_equals_to_shiftschedule_79",
            no_task="declare_variable_84",
        )

        if_request_schedule_equals_to_shiftschedule_79 = rail.IfOperator(
            task_id='if_request_schedule_equals_to_shiftschedule_79',
            test="{{ dag_run.conf.Schedule == 'Shift Schedule' }}",
            yes_task="update_variable_80",
            no_task="update_variable_83",
        )

        update_variable_80 = rail.SetVariableOperator(
            task_id='update_variable_80',
            append=False,
            name='{{ result("declare_variable_77").name }}',
            value=[{
                "schedulePolicy": {
                    "officeScheduleUri": null,
                    "name": null,
                    "officeSchedule": null,
                    "scheduleTypeUri": "urn:replicon:schedule-type:shift"
                },
                "effectiveDate": null
            }]
        )

        update_variable_83 = rail.SetVariableOperator(
            task_id='update_variable_83',
            append=False,
            name='{{ result("declare_variable_77").name }}',
            value=[{
                "schedulePolicy": {
                    "officeScheduleUri": "{{ result('get_all_office_schedules_17') }}",
                    "name": null,
                    "officeSchedule": {
                        "officeScheduleUri": "{{ result('get_all_office_schedules_17') }}",
                        "name": null
                    },
                    "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                },
                "effectiveDate": null
            }]
        )

        declare_variable_84 = rail.SetVariableOperator(
            task_id='declare_variable_84',
            append=False,
            name='agency_org2_department',
            value=None
        )

        if_request_agency_org2_department_uri_present_85 = rail.IfOperator(
            task_id='if_request_agency_org2_department_uri_present_85',
            test="{{ dag_run.conf.agency_org2_department_uri | is_truthy }}",
            yes_task="update_variable_86",
            no_task="exception_log_entry_88",
        )

        update_variable_86 = rail.SetVariableOperator(
            task_id='update_variable_86',
            append=False,
            name='{{ result("declare_variable_84").name }}',
            value=[{
                "departmentGroup": {
                    "uri": "{{ dag_run.conf.agency_org2_department_uri }}",
                    "parent": null,
                    "name": null,
                    "parameterCorrelationId": null
                },
                "effectiveDate": null
            }]
        )

        exception_log_entry_88 = rail.WriteLogOperator(
            task_id='exception_log_entry_88',
            log='{{ result("exception_log") }}',
            message='Exception',
            severity='Exception',
            properties={
                "value": "Agency (org 2) not assiged since blank value received"
            }
        )

        declare_variable_89 = rail.SetVariableOperator(
            task_id='declare_variable_89',
            append=False,
            name='profitcenter_servicecenter',
            value=None
        )

        if_request_profitcenter_division_uri_present_90 = rail.IfOperator(
            task_id='if_request_profitcenter_division_uri_present_90',
            test="{{ dag_run.conf.profitcenter_division_uri | is_truthy }}",
            yes_task="update_variable_91",
            no_task="exception_log_entry_93",
        )

        update_variable_91 = rail.SetVariableOperator(
            task_id='update_variable_91',
            append=False,
            name='{{ result("declare_variable_89").name }}',
            value=[{
                "serviceCenter": {
                    "uri": "{{ dag_run.conf.profitcenter_division_uri }}",
                    "parentUri": null,
                    "name": null
                },
                "effectiveDate": null
            }]
        )

        exception_log_entry_93 = rail.WriteLogOperator(
            task_id='exception_log_entry_93',
            log='{{ result("exception_log") }}',
            message='Exception',
            severity='Exception',
            properties={
                "value": "Profit center not assiged since blank value received"
            }
        )

        declare_variable_94 = rail.SetVariableOperator(
            task_id='declare_variable_94',
            append=False,
            name='paygroupcode_location',
            value=None
        )

        if_request_pay_group_code_location_uri_present_95 = rail.IfOperator(
            task_id='if_request_pay_group_code_location_uri_present_95',
            test="{{ dag_run.conf.pay_group_code_location_uri | is_truthy }}",
            yes_task="update_variable_96",
            no_task="exception_log_entry_98",
        )

        update_variable_96 = rail.SetVariableOperator(
            task_id='update_variable_96',
            append=False,
            name='{{ result("declare_variable_94").name }}',
            value=[{
                "location": {
                    "uri": "{{ dag_run.conf.pay_group_code_location_uri }}",
                    "parentUri": null,
                    "name": null
                },
                "effectiveDate": null
            }]
        )

        exception_log_entry_98 = rail.WriteLogOperator(
            task_id='exception_log_entry_98',
            log='{{ result("exception_log") }}',
            message='Exception',
            severity='Exception',
            properties={
                "value": "Pay group code not assiged since blank value received"
            }
        )

        declare_variable_99 = rail.SetVariableOperator(
            task_id='declare_variable_99',
            append=False,
            name='payrollgrouping_costcenter',
            value=None
        )

        if_request_payroll_grouping_cost_center_uri_present_100 = rail.IfOperator(
            task_id='if_request_payroll_grouping_cost_center_uri_present_100',
            test="{{ dag_run.conf.payroll_grouping_cost_center_uri | is_truthy }}",
            yes_task="update_variable_101",
            no_task="exception_log_entry_103",
        )

        update_variable_101 = rail.SetVariableOperator(
            task_id='update_variable_101',
            append=False,
            name='{{ result("declare_variable_99").name }}',
            value=[{
                "costCenter": {
                    "uri": "{{ dag_run.conf.payroll_grouping_cost_center_uri }}",
                    "parentUri": null,
                    "name": null
                },
                "effectiveDate": null
            }]
        )

        exception_log_entry_103 = rail.WriteLogOperator(
            task_id='exception_log_entry_103',
            log='{{ result("exception_log") }}',
            message='Exception',
            severity='Exception',
            properties={
                "value": "Payroll grouping not assiged since blank value received"
            }
        )

        declare_variable_104 = rail.SetVariableOperator(
            task_id='declare_variable_104',
            append=False,
            name='locationcode_division',
            value=None
        )

        if_request_location_code_work_division_uri_present_105 = rail.IfOperator(
            task_id='if_request_location_code_work_division_uri_present_105',
            test="{{ dag_run.conf.location_code_work_division_uri | is_truthy }}",
            yes_task="update_variable_106",
            no_task="exception_log_entry_108",
        )

        update_variable_106 = rail.SetVariableOperator(
            task_id='update_variable_106',
            append=False,
            name='{{ result("declare_variable_104").name }}',
            value=[{
                "division": {
                    "uri": "{{ dag_run.conf.location_code_work_division_uri }}",
                    "parentUri": null,
                    "name": null
                },
                "effectiveDate": null
            }]
        )

        exception_log_entry_108 = rail.WriteLogOperator(
            task_id='exception_log_entry_108',
            log='{{ result("exception_log") }}',
            message='Exception',
            severity='Exception',
            properties={
                "value": "Location code not assiged since blank value received"
            }
        )

        declare_variable_109 = rail.SetVariableOperator(
            task_id='declare_variable_109',
            append=False,
            name='dept_org_4_employeetype',
            value=None
        )

        if_request_deptorg4desc_employeetype_uri_present_110 = rail.IfOperator(
            task_id='if_request_deptorg4desc_employeetype_uri_present_110',
            test="{{ dag_run.conf.deptorg4desc_employeetype_uri | is_truthy }}",
            yes_task="update_variable_111",
            no_task="exception_log_entry_113",
        )

        update_variable_111 = rail.SetVariableOperator(
            task_id='update_variable_111',
            append=False,
            name='{{ result("declare_variable_109").name }}',
            value=[{
                "employeeTypeGroup": {
                    "uri": "{{ dag_run.conf.deptorg4desc_employeetype_uri }}",
                    "parent": null,
                    "name": null,
                    "parameterCorrelationId": null
                },
                "effectiveDate": null
            }]
        )

        exception_log_entry_113 = rail.WriteLogOperator(
            task_id='exception_log_entry_113',
            log='{{ result("exception_log") }}',
            message='Exception',
            severity='Exception',
            properties={
                "value": "Dept (Org 4) desc group not assiged since blank value received"
            }
        )

        custom_fields_to_be_added_list = rail.PythonOperator(
            task_id='custom_fields_to_be_added_list',
            python_callable=python_callable.get_custom_fields_to_be_added_list
        )

        invoke_custom_ruby_code_149 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_149',
            python_callable=lambda dag_run: python_callable.get_split_date(
                dag_run.conf['ServiceDate'])
        )

        declare_variable_150 = rail.SetVariableOperator(
            task_id='declare_variable_150',
            append=False,
            name='employeestatus',
            value=None
        )

        if_request_eestatus_equals_to_a_151 = rail.IfOperator(
            task_id='if_request_eestatus_equals_to_a_151',
            test="{{ dag_run.conf.EEStatus == 'A' }}",
            yes_task="update_variable_152",
            no_task="update_variable_154",
        )

        update_variable_152 = rail.SetVariableOperator(
            task_id='update_variable_152',
            append=False,
            name='{{ result("declare_variable_150").name }}',
            value=True
        )

        update_variable_154 = rail.SetVariableOperator(
            task_id='update_variable_154',
            append=False,
            name='{{ result("declare_variable_150").name }}',
            value=False
        )

        declare_variable_155 = rail.SetVariableOperator(
            task_id='declare_variable_155',
            append=False,
            name='timesheetperiod',
            value=None
        )

        if_request_timesheettemplate_present_156 = rail.IfOperator(
            task_id='if_request_timesheettemplate_present_156',
            test="{{ dag_run.conf.TimesheetTemplate | is_truthy }}",
            yes_task="if_request_replicontsdate_blank_157",
            no_task="create_user_162",
        )

        if_request_replicontsdate_blank_157 = rail.IfOperator(
            task_id='if_request_replicontsdate_blank_157',
            test="{{ dag_run.conf.RepliconTSDate | is_falsy }}",
            yes_task="update_variable_158",
            no_task="if_request_replicontsdate_present_159",
        )

        update_variable_158 = rail.SetVariableOperator(
            task_id='update_variable_158',
            append=False,
            name='{{ result("declare_variable_155").name }}',
            value=[{
                "timesheetPeriod": {
                    "uri": null,
                    "name": "Weekly starting on Sunday"
                },
                "effectiveDate": null
            }]
        )

        if_request_replicontsdate_present_159 = rail.IfOperator(
            task_id='if_request_replicontsdate_present_159',
            test="{{ dag_run.conf.RepliconTSDate | is_truthy }}",
            yes_task="update_variable_161",
            no_task="create_user_162",
        )

        update_variable_161 = rail.SetVariableOperator(
            task_id='update_variable_161',
            append=False,
            name='{{ result("declare_variable_155").name }}',
            value=lambda dag_run: [{
                "timesheetPeriod": {
                    "uri": null,
                    "name": "Weekly starting on Sunday"
                },
                "effectiveDate": python_callable.get_split_date(dag_run.conf['RepliconTSDate'])
            }]
        )

        create_user_162 = rail.RepliconServiceOperator(
            task_id='create_user_162',
            endpoint="/services/importservice1.svc/PutUser3",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": dag_run.conf['EmplID_Login'],
                        "parameterCorrelationId": null
                    },
                    "firstname": dag_run.conf['FirstName'],
                    "lastname": dag_run.conf['LastName'],
                    "emailAddress": rail.get_dag_run_var('email'),
                    "employeeId": dag_run.conf['EmplID_Login'],
                    "department": null,
                    "supervisorAssignmentSchedule": null,
                    "schedulePolicySchedule": rail.get_dag_run_var('schedule'),
                    "workWeekStartDayUri": rail.get_dag_run_var('workweek'),
                    "employmentDateRange": {
                        "startDate": rail.result("invoke_custom_ruby_code_149"),
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "securityConfiguration": {
                        "enabledAuthenticationTypeUris": [
                            "urn:replicon:user-authentication-type:sso"
                        ],
                        "isLoginEnabled": rail.get_dag_run_var('employeestatus'),
                        "loginName": dag_run.conf['EmplID_Login'],
                        "SSOName": dag_run.conf['EmplID_Login'],
                        "password": null
                    },
                    "holidayCalendar": rail.get_dag_run_var('Holidaycalendar'),
                    "timeOffPolicy": null,
                    "permissionSets": [{
                        "uri": null,
                        "name": "Project Resource with Reports"
                    }],
                    "policySets": rail.result('log_policy_settoassign_45'),
                    "employeeType": null,
                    "timesheetPeriodTypeUri": null,
                    "costRateSchedule": null,
                    "payrollRateSchedule": {
                        "initialHourlyRate": {
                            "amount": float(dag_run.conf.get('hourly_rate_amount')),
                            "currency": {
                                "uri": null,
                                "name": dag_run.conf.get('hourly_rate_amount_currency_name'),
                                "symbol": null
                            }
                        },
                        "scheduleEntries": []
                    } if dag_run.conf.get('hourly_rate_amount') else null,
                    "defaultBillingRate": null,
                    "timesheetApprovalPath": {
                        "uri": null,
                        "name": "Supervisor"
                    },
                    "expenseApprovalPath": null,
                    "timeOffApprovalPath": {
                        "uri": null,
                        "name": "Supervisor"
                    },
                    "customFieldValues": rail.result('custom_fields_to_be_added_list'),
                    "assignedActivities": [],
                    "timeZone": rail.get_dag_run_var('timezone'),
                    "overtimeRuleAssignmentSchedule": null,
                    "validationRuleAssignmentSchedule": null,
                    "locationSchedule": rail.get_dag_run_var('paygroupcode_location'),
                    "divisionSchedule": rail.get_dag_run_var('locationcode_division'),
                    "costCenterSchedule": rail.get_dag_run_var('payrollgrouping_costcenter'),
                    "serviceCenterSchedule": rail.get_dag_run_var('profitcenter_servicecenter'),
                    "departmentGroupSchedule": rail.get_dag_run_var('agency_org2_department'),
                    "employeeTypeGroupSchedule": rail.get_dag_run_var('dept_org_4_employeetype'),
                    "timesheetPeriodSchedule": rail.get_dag_run_var('timesheetperiod'),
                    "policyDataAccessScopes": [],
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": rail.get_dag_run_var('payrule')
                }
            }
        )

        if_request_eestatus_equals_to_t_163 = rail.IfOperator(
            task_id='if_request_eestatus_equals_to_t_163',
            test="{{ dag_run.conf.EEStatus == 'T' }}",
            yes_task="exception_log_entry_164",
            no_task="remove_timeoff_assignments_165",
        )

        exception_log_entry_164 = rail.WriteLogOperator(
            task_id='exception_log_entry_164',
            log='{{ result("exception_log") }}',
            message='Exception',
            severity='Exception',
            properties={
                "value": "User created in disabled status"
            }
        )

        remove_timeoff_assignments_165 = rail.RepliconServiceOperator(
            task_id='remove_timeoff_assignments_165',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ result('create_user_162').uri }}",
                "timeOffTypeUris": []
            }
        )

        if_request_supervisorid_blank_166 = rail.IfOperator(
            task_id='if_request_supervisorid_blank_166',
            test="{{ dag_run.conf.SupervisorID | is_falsy }}",
            yes_task="exception_log_entry_167",
            no_task="if_request_supervisorid_equals_to_emplid_169",
        )

        exception_log_entry_167 = rail.WriteLogOperator(
            task_id='exception_log_entry_167',
            log='{{ result("exception_log") }}',
            message='Exception',
            severity='Exception',
            properties={
                "value": "Supervisor not assigned since the Supervisor ID is not provided"
            }
        )

        if_request_supervisorid_equals_to_emplid_169 = rail.IfOperator(
            task_id='if_request_supervisorid_equals_to_emplid_169',
            test="{{ dag_run.conf.SupervisorID == dag_run.conf.EmplID_Login }}",
            yes_task="exception_log_entry_170",
            no_task="search_users_search_supervisor_172",
        )

        exception_log_entry_170 = rail.WriteLogOperator(
            task_id='exception_log_entry_170',
            log='{{ result("exception_log") }}',
            message='Exception',
            severity='Exception',
            properties={
                "value": "Supervisor not assigned since the Supervisor ID  and user login ID are the same."
            }
        )

        search_users_search_supervisor_172 = rail.RepliconServiceOperator(
            task_id='search_users_search_supervisor_172',
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.search_supervisor_payload,
            data_handler=lambda response, dag_run: python_callable.get_supervisor_uri_status(
                response, dag_run.conf['SupervisorID'])
        )

        if_log_supervisor_uri_173_blank_174 = rail.IfOperator(
            task_id='if_log_supervisor_uri_173_blank_174',
            test="{{ result('search_users_search_supervisor_172') | is_falsy }}",
            yes_task="assured_partners_supervisor_assignment_table_add_entry_175",
            no_task="if_downcase_status_not_equals_to_true_178",
        )

        assured_partners_supervisor_assignment_table_add_entry_175 = rail.WriteLogOperator(
            task_id='assured_partners_supervisor_assignment_table_add_entry_175',
            log="{{ dag_run.conf.supervisor_assignment_log }}",
            message="na",
            severity="queued",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['parentjobid'],
                "username": dag_run.conf['EmplID_Login'],
                "useruri": rail.result('create_user_162')['uri'],
                "supervisorloginname": dag_run.conf['SupervisorID'],
                "action": "Add",
                "childjobid": get_dagrun_ecid(dag_run),
                "status": "queued",
                "supervisoreffectivedate": dag_run.conf['integration_run_date'],
                "supervisorusername": dag_run.conf['SupervisorName']
            }
        )

        if_downcase_status_not_equals_to_true_178 = rail.IfOperator(
            task_id='if_downcase_status_not_equals_to_true_178',
            test=lambda: bool(rail.result('search_users_search_supervisor_172')[
                              "status"].lower() != 'true'),
            yes_task="assured_partners_supervisor_assignment_table_add_entry_supervisor_disabled_179",
            no_task="get_assigned_permission_for_supervisor_181",
        )

        assured_partners_supervisor_assignment_table_add_entry_supervisor_disabled_179 = rail.WriteLogOperator(
            task_id='assured_partners_supervisor_assignment_table_add_entry_supervisor_disabled_179',
            log="{{ dag_run.conf.supervisor_assignment_log }}",
            message="na",
            severity="queued",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['parentjobid'],
                "username": dag_run.conf['EmplID_Login'],
                "useruri": rail.result('create_user_162')['uri'],
                "supervisorloginname": dag_run.conf['SupervisorID'],
                "action": "Add",
                "status": "queued",
                "supervisoreffectivedate": dag_run.conf['integration_run_date'],
                "supervisorusername": dag_run.conf['SupervisorName'],
                "childjobid": get_dagrun_ecid(dag_run)
            }
        )

        get_assigned_permission_for_supervisor_181 = rail.RepliconServiceOperator(
            task_id='get_assigned_permission_for_supervisor_181',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_users_search_supervisor_172').uri }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', "urn:replicon:policy:supervision", 'permissionSet.name') if response else null
        )

        if_log_supervisor_permission_assigned_to_user_blank_183 = rail.IfOperator(
            task_id='if_log_supervisor_permission_assigned_to_user_blank_183',
            test="{{ result('get_assigned_permission_for_supervisor_181') | is_falsy }}",
            yes_task="assign_permission_set_to_user_185",
            no_task="assign_initial_supervisor_186",
        )

        assign_permission_set_to_user_185 = rail.RepliconServiceOperator(
            task_id='assign_permission_set_to_user_185',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('search_users_search_supervisor_172').uri }}",
                "permissionSetUri": "{{ result('get_all_permission_sets_18').supervisor }}"
            }
        )

        assign_initial_supervisor_186 = rail.RepliconServiceOperator(
            task_id='assign_initial_supervisor_186',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('create_user_162').uri }}",
                "supervisorUri": "{{ result('search_users_search_supervisor_172').uri }}",
                "dateRange": null
            }
        )

        if_request_eetype_blank_187 = rail.IfOperator(
            task_id='if_request_eetype_blank_187',
            test="{{ dag_run.conf.EEType | is_falsy }}",
            yes_task="exception_log_entry_188",
            no_task="get_enabled_custom_field_drop_down_options_190",
        )

        exception_log_entry_188 = rail.WriteLogOperator(
            task_id='exception_log_entry_188',
            log='{{ result("exception_log") }}',
            message='Exception',
            severity='Exception',
            properties={
                "value": "EE type UDF value is blank"
            }
        )

        get_enabled_custom_field_drop_down_options_190 = rail.RepliconServiceOperator(
            task_id='get_enabled_custom_field_drop_down_options_190',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ dag_run.conf.eetype_udf_uri }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf["EEType"], 'uri') if response else null
        )

        if_log_dropdownoptionvalue_present_192 = rail.IfOperator(
            task_id='if_log_dropdownoptionvalue_present_192',
            test="{{ result('get_enabled_custom_field_drop_down_options_190') | is_truthy }}",
            yes_task="updated_udf_for_eetype_193",
            no_task="exception_log_entry_195",
        )

        updated_udf_for_eetype_193 = rail.RepliconServiceOperator(
            task_id='updated_udf_for_eetype_193',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('create_user_162').uri }}",
                "customFieldUri": "{{ dag_run.conf.eetype_udf_uri }}",
                "customFieldDropDownOptionUri": "{{ result('get_enabled_custom_field_drop_down_options_190') }}"
            }
        )

        exception_log_entry_195 = rail.WriteLogOperator(
            task_id='exception_log_entry_195',
            log='{{ result("exception_log") }}',
            message='Exception',
            severity='Exception',
            properties={
                "value": "EE type UDF dropdown option {{ dag_run.conf.EEType }} not available in Replicon"
            }
        )

        if_request_flsastatus_blank_196 = rail.IfOperator(
            task_id='if_request_flsastatus_blank_196',
            test="{{ dag_run.conf.FLSAStatus | is_falsy }}",
            yes_task="exception_log_entry_197",
            no_task="get_enabled_custom_field_drop_down_options_199",
        )

        exception_log_entry_197 = rail.WriteLogOperator(
            task_id='exception_log_entry_197',
            log='{{ result("exception_log") }}',
            message='Exception',
            severity='Exception',
            properties={
                "value": "FLSA status UDF value is blank"
            }
        )

        get_enabled_custom_field_drop_down_options_199 = rail.RepliconServiceOperator(
            task_id='get_enabled_custom_field_drop_down_options_199',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ dag_run.conf.flsastatus_udf_uri }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf["FLSAStatus"], 'uri') if response else null
        )

        if_log_dropdownoptionvalue_200_present_201 = rail.IfOperator(
            task_id='if_log_dropdownoptionvalue_200_present_201',
            test="{{ result('get_enabled_custom_field_drop_down_options_199') | is_truthy }}",
            yes_task="updated_udf_for_flsa_status_202",
            no_task="exception_log_entry_204",
        )

        updated_udf_for_flsa_status_202 = rail.RepliconServiceOperator(
            task_id='updated_udf_for_flsa_status_202',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('create_user_162').uri }}",
                "customFieldUri": "{{ dag_run.conf.flsastatus_udf_uri }}",
                "customFieldDropDownOptionUri": "{{ result('get_enabled_custom_field_drop_down_options_199') }}"
            }
        )

        exception_log_entry_204 = rail.WriteLogOperator(
            task_id='exception_log_entry_204',
            log='{{ result("exception_log") }}',
            message='Exception',
            severity='Exception',
            properties={
                "value": "FLSA status UDF dropdown option {{ dag_run.conf.FLSAStatus }} not available in Replicon"
            }
        )

        trigger_dag_child_workflow_to_add_timeoff_type_for_new_user_211 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_child_workflow_to_add_timeoff_type_for_new_user_211',
            retries=0,
            trigger_dag_id=config.child_workflow_to_add_timeoff_type_for_new_user_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "useruri":  rail.result('create_user_162')['uri'],
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
                'integration_run_date': dag_run.conf['integration_run_date'],
            }
        )

        wait_for_completion_dag_child_workflow_to_add_timeoff_type_for_new_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_dag_child_workflow_to_add_timeoff_type_for_new_user',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{result('trigger_dag_child_workflow_to_add_timeoff_type_for_new_user_211')}}"
        )

        gather_results_from_211_dag_run = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_results_from_211_dag_run',
            dag_runs="{{result('trigger_dag_child_workflow_to_add_timeoff_type_for_new_user_211')}}",
            dagrun_task_id="final_response_from_dag",
            target='result'
        )

        if_error_in_gather_reponse_from_211_dag_run = rail.IfOperator(
            task_id='if_error_in_gather_reponse_from_211_dag_run',
            test=lambda: bool(rail.result("gather_results_from_211_dag_run")) and "Error" in json.dumps(rail.result(
                "gather_results_from_211_dag_run")[0]),
            yes_task="fail_with_error_in_timeoff_assignment",
            no_task="if_request_activity_present_212",
        )

        fail_with_error_in_timeoff_assignment = rail.FailOperator(
            task_id='fail_with_error_in_timeoff_assignment',
            message="{{result('gather_results_from_211_dag_run')[0]}}"
        )

        if_request_activity_present_212 = rail.IfOperator(
            task_id='if_request_activity_present_212',
            test="{{ dag_run.conf.activity | is_truthy }}",
            yes_task="trigger_dag_run_activity_assignment_213",
            no_task="if_gather_results_from_211_dag_run_present_214",
        )

        trigger_dag_run_activity_assignment_213 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_activity_assignment_213',
            retries=0,
            trigger_dag_id=config.child_activity_assignment_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "useruri": "{{ result('create_user_162').uri }}",
                "activity": "{{ dag_run.conf.activity }}",
                "integration_run_date": "{{ dag_run.conf.integration_run_date }}"
            }
        )

        wait_for_completion_dag_run_activity_assignment = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_dag_run_activity_assignment',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{result('trigger_dag_run_activity_assignment_213')}}"
        )

        gather_results_from_dag_run_213 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_results_from_dag_run_213',
            dag_runs="{{result('trigger_dag_run_activity_assignment_213')}}",
            dagrun_task_id="final_response_from_dag",
            target='result'
        )

        if_error_in_gather_reponse_from_dag_run_213 = rail.IfOperator(
            task_id='if_error_in_gather_reponse_from_dag_run_213',
            test=lambda: bool(rail.result("gather_results_from_dag_run_213")) and "Error" in json.dumps(rail.result(
                "gather_results_from_dag_run_213")[0]),
            yes_task="fail_with_error_in_activity_assignment",
            no_task="if_gather_results_from_211_dag_run_present_214",
        )

        fail_with_error_in_activity_assignment = rail.FailOperator(
            task_id='fail_with_error_in_activity_assignment',
            message="{{result('gather_results_from_dag_run_213')[0]}}"
        )

        if_gather_results_from_211_dag_run_present_214 = rail.IfOperator(
            task_id='if_gather_results_from_211_dag_run_present_214',
            test=lambda: bool(rail.result("gather_results_from_211_dag_run")),
            yes_task="exception_log_entry_215",
            no_task="assured_partners_user_sync_logs_add_entry_216",
        )

        exception_log_entry_215 = rail.WriteLogOperator(
            task_id='exception_log_entry_215',
            log='{{ result("exception_log") }}',
            message='Exception',
            severity='Exception',
            properties={
                "value": "{{result('gather_results_from_211_dag_run')[0]}}"
            }
        )

        gather_exceptions = rail.FilterLogEntriesOperator(
            task_id='gather_exceptions',
            log="{{result('exception_log')}}",
            severity='Exception'
        )

        get_final_exception_entries = rail.PythonOperator(
            task_id='get_final_exception_entries',
            python_callable=lambda: python_callable.get_all_exceptions_from_exception_log(
                rail.result("gather_exceptions"))
        )

        assured_partners_user_sync_logs_add_entry_216 = rail.WriteLogOperator(
            task_id='assured_partners_user_sync_logs_add_entry_216',
            log="{{ dag_run.conf.user_import_log }}",
            message="na",
            severity=lambda: "Exception" if rail.result(
                'get_final_exception_entries') else "Success",
            properties=lambda dag_run: {
                "action": "Add",
                "status": "Exception" if rail.result('get_final_exception_entries') else "Success",
                "job_id": dag_run.conf['parentjobid'],
                "details": ("User created with exception," + rail.result('get_final_exception_entries')) if rail.result(
                    'get_final_exception_entries') else "User created successfully",
                "username": dag_run.conf['FirstName'] + " " + dag_run.conf['LastName'],
                "loginname": dag_run.conf['EmplID_Login'],
                "childjobid": get_dagrun_ecid(dag_run)
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            log="{{ dag_run.conf.user_import_log }}",
            trigger_rule='one_failed',
            message="\
                {%- if get_task_state('create_user_162') == 'success' -%} \
                    User Added Partially; {{ get_error_message() }}\
                {%- else -%}\
                    User not created; {{ get_error_message() }}\
                {%- endif -%}",
            severity="Error",
            properties={
                "action": "Add",
                "status": "Error",
                "job_id": "{{ dag_run.conf.parentjobid }}",
                "details": "\
                    {%- if get_task_state('create_user_162') == 'success' -%} \
                        User Added Partially; {{ get_error_message() }}\
                    {%- else -%}\
                        User not created; {{ get_error_message() }}\
                    {%- endif -%}",
                "username": "{{ dag_run.conf.FirstName }} {{ dag_run.conf.LastName }}",
                "loginname": "{{ dag_run.conf.EmplID_Login }}",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        can_run_batch_task >> rail.Label('No') >> exception_log
        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error

        exception_log >> assured_partners_user_sync_master_mapper_search_entries_8

        assured_partners_user_sync_master_mapper_search_entries_8 >> email_variable >> if_request_e_mail_present_10

        if_request_e_mail_present_10 >> rail.Label(
            'No') >> exception_log_entry_13 >> get_all_policy_sets_14
        if_request_e_mail_present_10 >> rail.Label(
            'Yes') >> update_email_variable >> get_all_policy_sets_14

        get_all_policy_sets_14 >> get_all_holiday_calendars_15 >> get_all_scripts_16 >> get_all_office_schedules_17 >> get_all_permission_sets_18

        get_all_permission_sets_18 >> declare_list_19 >> if_request_timesheettemplate_present_20

        if_request_timesheettemplate_present_20 >> rail.Label(
            'No') >> exception_log_entry_27 >> if_request_timeofftemplate_present_28
        if_request_timesheettemplate_present_20 >> rail.Label(
            'Yes') >> if_log_required_timesheet_template_uri_present_22

        if_log_required_timesheet_template_uri_present_22 >> rail.Label(
            'No') >> exception_log_entry_25 >> if_request_timeofftemplate_present_28
        if_log_required_timesheet_template_uri_present_22 >> rail.Label(
            'Yes') >> insert_to_list_23 >> if_request_timeofftemplate_present_28

        if_request_timeofftemplate_present_28 >> rail.Label(
            'No') >> exception_log_entry_36 >> if_request_punch_entry_policy_present_37
        if_request_timeofftemplate_present_28 >> rail.Label(
            'Yes') >> if_log_required_timeoff_template_uri_30_present_31

        if_log_required_timeoff_template_uri_30_present_31 >> rail.Label(
            'No') >> exception_log_entry_34 >> if_request_punch_entry_policy_present_37
        if_log_required_timeoff_template_uri_30_present_31 >> rail.Label(
            'Yes') >> insert_to_list_32 >> if_request_punch_entry_policy_present_37

        if_request_punch_entry_policy_present_37 >> rail.Label(
            'No') >> exception_log_entry_44 >> log_policy_settoassign_45
        if_request_punch_entry_policy_present_37 >> rail.Label(
            'Yes') >> if_log_required_punchentrypolicy_38_present_39

        if_log_required_punchentrypolicy_38_present_39 >> rail.Label(
            'No') >> exception_log_entry_42 >> log_policy_settoassign_45
        if_log_required_punchentrypolicy_38_present_39 >> rail.Label(
            'Yes') >> insert_to_list_40 >> log_policy_settoassign_45

        log_policy_settoassign_45 >> declare_variable_46 >> if_request_holidaycalendars_present_47

        if_request_holidaycalendars_present_47 >> rail.Label(
            'No') >> exception_log_entry_54 >> declare_variable_55
        if_request_holidaycalendars_present_47 >> rail.Label(
            'Yes') >> if_log_required_holiday_calendar_present_49

        if_log_required_holiday_calendar_present_49 >> rail.Label(
            'No') >> exception_log_entry_52 >> declare_variable_55
        if_log_required_holiday_calendar_present_49 >> rail.Label(
            'Yes') >> update_variable_50 >> declare_variable_55

        declare_variable_55 >> if_request_timezone_present_56

        if_request_timezone_present_56 >> rail.Label(
            'No') >> exception_log_entry_62 >> declare_variable_63
        if_request_timezone_present_56 >> rail.Label(
            'Yes') >> if_request_timezoneuri_present_57

        if_request_timezoneuri_present_57 >> rail.Label(
            'Yes') >> update_variable_58 >> declare_variable_63
        if_request_timezoneuri_present_57 >> rail.Label(
            'No') >> exception_log_entry_60 >> declare_variable_63

        declare_variable_63 >> if_request_workweek_present_64

        if_request_workweek_present_64 >> rail.Label(
            'No') >> exception_log_entry_67 >> declare_variable_68
        if_request_workweek_present_64 >> rail.Label(
            'Yes') >> update_variable_65 >> declare_variable_68

        declare_variable_68 >> if_request_payrules_present_69

        if_request_payrules_present_69 >> rail.Label(
            'No') >> exception_log_entry_76 >> declare_variable_77
        if_request_payrules_present_69 >> rail.Label(
            'Yes') >> if_log_required_payrule_70_present_71

        if_log_required_payrule_70_present_71 >> rail.Label(
            'No') >> exception_log_entry_74 >> declare_variable_77
        if_log_required_payrule_70_present_71 >> rail.Label(
            'Yes') >> update_variable_72 >> declare_variable_77

        declare_variable_77 >> if_request_schedule_present_78

        if_request_schedule_present_78 >> rail.Label(
            'No') >> declare_variable_84
        if_request_schedule_present_78 >> rail.Label(
            'Yes') >> if_request_schedule_equals_to_shiftschedule_79

        if_request_schedule_equals_to_shiftschedule_79 >> rail.Label(
            'No') >> update_variable_83 >> declare_variable_84
        if_request_schedule_equals_to_shiftschedule_79 >> rail.Label(
            'Yes') >> update_variable_80 >> declare_variable_84

        declare_variable_84 >> if_request_agency_org2_department_uri_present_85

        if_request_agency_org2_department_uri_present_85 >> rail.Label(
            'No') >> exception_log_entry_88 >> declare_variable_89
        if_request_agency_org2_department_uri_present_85 >> rail.Label(
            'Yes') >> update_variable_86 >> declare_variable_89

        declare_variable_89 >> if_request_profitcenter_division_uri_present_90

        if_request_profitcenter_division_uri_present_90 >> rail.Label(
            'No') >> exception_log_entry_93 >> declare_variable_94
        if_request_profitcenter_division_uri_present_90 >> rail.Label(
            'Yes') >> update_variable_91 >> declare_variable_94

        declare_variable_94 >> if_request_pay_group_code_location_uri_present_95

        if_request_pay_group_code_location_uri_present_95 >> rail.Label(
            'No') >> exception_log_entry_98 >> declare_variable_99
        if_request_pay_group_code_location_uri_present_95 >> rail.Label(
            'Yes') >> update_variable_96 >> declare_variable_99

        declare_variable_99 >> if_request_payroll_grouping_cost_center_uri_present_100

        if_request_payroll_grouping_cost_center_uri_present_100 >> rail.Label(
            'No') >> exception_log_entry_103 >> declare_variable_104
        if_request_payroll_grouping_cost_center_uri_present_100 >> rail.Label(
            'Yes') >> update_variable_101 >> declare_variable_104

        declare_variable_104 >> if_request_location_code_work_division_uri_present_105

        if_request_location_code_work_division_uri_present_105 >> rail.Label(
            'No') >> exception_log_entry_108 >> declare_variable_109
        if_request_location_code_work_division_uri_present_105 >> rail.Label(
            'Yes') >> update_variable_106 >> declare_variable_109

        declare_variable_109 >> if_request_deptorg4desc_employeetype_uri_present_110

        if_request_deptorg4desc_employeetype_uri_present_110 >> rail.Label(
            'No') >> exception_log_entry_113 >> custom_fields_to_be_added_list
        if_request_deptorg4desc_employeetype_uri_present_110 >> rail.Label(
            'Yes') >> update_variable_111 >> custom_fields_to_be_added_list

        custom_fields_to_be_added_list >> invoke_custom_ruby_code_149 >> declare_variable_150 >> if_request_eestatus_equals_to_a_151

        if_request_eestatus_equals_to_a_151 >> rail.Label(
            'No') >> update_variable_154 >> declare_variable_155
        if_request_eestatus_equals_to_a_151 >> rail.Label(
            'Yes') >> update_variable_152 >> declare_variable_155

        declare_variable_155 >> if_request_timesheettemplate_present_156

        if_request_timesheettemplate_present_156 >> rail.Label(
            'No') >> create_user_162
        if_request_timesheettemplate_present_156 >> rail.Label(
            'Yes') >> if_request_replicontsdate_blank_157

        if_request_replicontsdate_blank_157 >> rail.Label(
            'No') >> if_request_replicontsdate_present_159
        if_request_replicontsdate_blank_157 >> rail.Label(
            'Yes') >> update_variable_158 >> if_request_replicontsdate_present_159

        if_request_replicontsdate_present_159 >> rail.Label(
            'Yes') >> update_variable_161 >> create_user_162
        if_request_replicontsdate_present_159 >> rail.Label(
            'No') >> create_user_162

        create_user_162 >> if_request_eestatus_equals_to_t_163

        if_request_eestatus_equals_to_t_163 >> rail.Label(
            'No') >> remove_timeoff_assignments_165
        if_request_eestatus_equals_to_t_163 >> rail.Label(
            'Yes') >> exception_log_entry_164 >> remove_timeoff_assignments_165

        remove_timeoff_assignments_165 >> if_request_supervisorid_blank_166

        if_request_supervisorid_blank_166 >> rail.Label(
            'No') >> if_request_supervisorid_equals_to_emplid_169
        if_request_supervisorid_blank_166 >> rail.Label(
            'Yes') >> exception_log_entry_167 >> if_request_eetype_blank_187

        if_request_supervisorid_equals_to_emplid_169 >> rail.Label(
            'Yes') >> exception_log_entry_170 >> if_request_eetype_blank_187
        if_request_supervisorid_equals_to_emplid_169 >> rail.Label(
            'No') >> search_users_search_supervisor_172 >> if_log_supervisor_uri_173_blank_174

        if_log_supervisor_uri_173_blank_174 >> rail.Label(
            'Yes') >> assured_partners_supervisor_assignment_table_add_entry_175 >> if_request_eetype_blank_187
        if_log_supervisor_uri_173_blank_174 >> rail.Label(
            'No') >> if_downcase_status_not_equals_to_true_178

        if_downcase_status_not_equals_to_true_178 >> rail.Label(
            'Yes') >> assured_partners_supervisor_assignment_table_add_entry_supervisor_disabled_179 >> if_request_eetype_blank_187
        if_downcase_status_not_equals_to_true_178 >> rail.Label(
            'No') >> get_assigned_permission_for_supervisor_181 >> if_log_supervisor_permission_assigned_to_user_blank_183

        if_log_supervisor_permission_assigned_to_user_blank_183 >> rail.Label(
            'Yes') >> assign_permission_set_to_user_185 >> assign_initial_supervisor_186
        if_log_supervisor_permission_assigned_to_user_blank_183 >> rail.Label(
            'No') >> assign_initial_supervisor_186

        assign_initial_supervisor_186 >> if_request_eetype_blank_187

        if_request_eetype_blank_187 >> rail.Label(
            'Yes') >> exception_log_entry_188 >> if_request_flsastatus_blank_196
        if_request_eetype_blank_187 >> rail.Label(
            'No') >> get_enabled_custom_field_drop_down_options_190 >> if_log_dropdownoptionvalue_present_192

        if_log_dropdownoptionvalue_present_192 >> rail.Label(
            'Yes') >> updated_udf_for_eetype_193 >> if_request_flsastatus_blank_196
        if_log_dropdownoptionvalue_present_192 >> rail.Label(
            'No') >> exception_log_entry_195 >> if_request_flsastatus_blank_196

        if_request_flsastatus_blank_196 >> rail.Label(
            'Yes') >> exception_log_entry_197 >> trigger_dag_child_workflow_to_add_timeoff_type_for_new_user_211
        if_request_flsastatus_blank_196 >> rail.Label(
            'No') >> get_enabled_custom_field_drop_down_options_199 >> if_log_dropdownoptionvalue_200_present_201

        if_log_dropdownoptionvalue_200_present_201 >> rail.Label(
            'Yes') >> updated_udf_for_flsa_status_202 >> trigger_dag_child_workflow_to_add_timeoff_type_for_new_user_211
        if_log_dropdownoptionvalue_200_present_201 >> rail.Label(
            'No') >> exception_log_entry_204 >> trigger_dag_child_workflow_to_add_timeoff_type_for_new_user_211

        trigger_dag_child_workflow_to_add_timeoff_type_for_new_user_211 >> wait_for_completion_dag_child_workflow_to_add_timeoff_type_for_new_user >> gather_results_from_211_dag_run >> if_error_in_gather_reponse_from_211_dag_run

        if_error_in_gather_reponse_from_211_dag_run >> rail.Label(
            'No') >> if_request_activity_present_212
        if_error_in_gather_reponse_from_211_dag_run >> rail.Label(
            'Yes') >> fail_with_error_in_timeoff_assignment >> if_request_activity_present_212

        if_request_activity_present_212 >> rail.Label(
            'Yes') >> trigger_dag_run_activity_assignment_213 >> wait_for_completion_dag_run_activity_assignment >> gather_results_from_dag_run_213 >> if_error_in_gather_reponse_from_dag_run_213

        if_error_in_gather_reponse_from_dag_run_213 >> rail.Label(
            "No") >> if_gather_results_from_211_dag_run_present_214
        if_error_in_gather_reponse_from_dag_run_213 >> rail.Label(
            "Yes") >> fail_with_error_in_activity_assignment >> if_gather_results_from_211_dag_run_present_214

        if_request_activity_present_212 >> rail.Label(
            'No') >> if_gather_results_from_211_dag_run_present_214

        if_gather_results_from_211_dag_run_present_214 >> rail.Label(
            'Yes') >> exception_log_entry_215 >> gather_exceptions >> get_final_exception_entries >> assured_partners_user_sync_logs_add_entry_216
        if_gather_results_from_211_dag_run_present_214 >> rail.Label(
            'No') >> assured_partners_user_sync_logs_add_entry_216

        assured_partners_user_sync_logs_add_entry_216 >> catch_and_log_error

    return dag


rail.for_each_instance(create_dag)
