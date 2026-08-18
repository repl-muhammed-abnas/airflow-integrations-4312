# pylint: disable=too-many-statements
from datetime import timedelta
from airflow.models import Variable
import rail
from rail.lib.ecid import get_dagrun_ecid
from momentive.user_import_south_korea.utils import request_payload, python_callable
from momentive.user_import_south_korea.utils.python_callable import get_details_for_employeetype_and_departmentygrpuri_not_exist

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'momentive_userimport_user_sync_add_child_{config.instance}',
        description=f'momentive_userimport_user_sync_add_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.user_sync_add_child_dag_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config", extra_config=config)

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
            task_id = "exception_log"
        )

        get_input_validation_log = rail.PythonOperator(
            task_id = "get_input_validation_log",
            python_callable=python_callable.get_input_validationlog
        )

        if_input_validation_log_present = rail.IfOperator(
            task_id='if_input_validation_log_present',
            test="{{ result('get_input_validation_log').exc_present | is_truthy }}",
            yes_task="log_user_import_not_created",
            no_task="if_workertype_not_contingentworker_and_gender_not_present",
        )

        log_user_import_not_created = rail.WriteLogOperator(
            task_id="log_user_import_not_created",
            log = '{{ dag_run.conf.logger}}',
            message="Exception",
            severity="Exception",
            properties=lambda dag_run: {
                "userid": dag_run.conf['userid'],
                "username": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "action": "Add",
                "status": "Warning",
                "details": "User not created," + rail.result('get_input_validation_log')['exc_value'] + "| NA",
                "country":'South Korea' if "Korea, Republic of" in dag_run.conf['country'] else \
                    'UAE' if "United Arab Emirates" in dag_run.conf['country'] else \
                        "Belgium" if "Belgium" in dag_run.conf['country'] else ""
            }
        )

        if_workertype_not_contingentworker_and_gender_not_present = rail.IfOperator(
            task_id='if_workertype_not_contingentworker_and_gender_not_present',
            test="{{ dag_run.conf.workertype != 'Contingent Worker' and dag_run.conf.gender | is_falsy and \
                dag_run.conf.country != 'United Kingdom' }}",
            yes_task="log_user_import_not_created_gender_not_present",
            no_task="get_all_employee_type",
        )

        log_user_import_not_created_gender_not_present = rail.WriteLogOperator(
            task_id="log_user_import_not_created_gender_not_present",
            log = '{{ dag_run.conf.logger}}',
            message="Exception",
            severity="Exception",
            properties=lambda dag_run: {
                "userid": dag_run.conf['userid'],
                "username": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "action": "Add",
                "status": "Warning",
                'details': "User not created, Gender must be present for users with 'Employee' worker type|NA",
                'country':'South Korea' if "Korea, Republic of" in dag_run.conf['country'] else \
                    'UAE' if "United Arab Emirates" in dag_run.conf['country'] else \
                        "Belgium" if "Belgium" in dag_run.conf['country'] else ""
            }
        )

        get_all_employee_type = rail.RepliconServiceOperator(
            task_id="get_all_employee_type",
            endpoint="/services/EmployeeTypeGroupService1.svc/GetAllEmployeeTypeGroups"
        )

        get_all_permissionsets = rail.RepliconServiceOperator(
            task_id='get_all_permissionsets',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets',
            data_handler=lambda response: {
                'basic_user_with_report_uri': rail.find_first_by_attr_and_get_attr(
                    response,'name',"Basic User with Reports",'uri'),
                'supervisor': rail.find_first_by_attr_and_get_attr(
                    response,'name',"Supervisor - Edit",'uri')
            }
        )

        get_all_timezones = rail.RepliconServiceOperator(
            task_id="get_all_timezones",
            endpoint="/services/InternationalizationService1.svc/GetAllTimeZones",
        )

        create_location_lookup=rail.SetVariableOperator(
            task_id='create_location_lookup',
            append=False,
            name='location_lookup',
            value='Any'
        )

        create_country_lookup=rail.SetVariableOperator(
            task_id='create_country_lookup',
            append=False,
            name='country_lookup',
            value=python_callable.get_iniial_country_lookup_value
        )

        create_shift_lookup=rail.SetVariableOperator(
            task_id='create_shift_lookup',
            append=False,
            name='shift_lookup',
            value='Any'
        )

        if_workshift_equals_prod_rota_or_nonrota = rail.IfOperator(
            task_id='if_workshift_equals_prod_rota_or_nonrota',
            test="{{ dag_run.conf.work_shift == 'PRODUCTION Rota' or dag_run.conf.work_shift == 'PRODUCTION Non Rota' }}",
            yes_task="update_shift_lookup_to_workshift",
            no_task="update_shift_lookup_to_any",
        )

        update_shift_lookup_to_workshift = rail.SetVariableOperator(
            task_id='update_shift_lookup_to_workshift',
            append=False,
            name='{{ result("create_shift_lookup").name }}',
            value=lambda dag_run: dag_run.conf['work_shift']
        )

        update_shift_lookup_to_any = rail.SetVariableOperator(
            task_id='update_shift_lookup_to_any',
            append=False,
            name='{{ result("create_shift_lookup").name }}',
            value='Any'
        )

        create_workersubshift_lookup=rail.SetVariableOperator(
            task_id='create_workersubshift_lookup',
            append=False,
            name='workersubshift_lookup',
            value=lambda dag_run: dag_run.conf['legalentity']
        )

        create_timesheetapprovalpath=rail.SetVariableOperator(
            task_id='create_timesheetapprovalpath',
            append=False,
            name='timesheetapprovalpath',
            value=''
        )

        create_timeoffapprovalpath=rail.SetVariableOperator(
            task_id='create_timeoffapprovalpath',
            append=False,
            name='timeoffapprovalpath',
            value=''
        )

        create_legalentity_division=rail.SetVariableOperator(
            task_id='create_legalentity_division',
            append=False,
            name='legalentity_division',
            value=[]
        )

        create_Paygroup_servicecenter=rail.SetVariableOperator(
            task_id='create_Paygroup_servicecenter',
            append=False,
            name='Paygroup_servicecenter',
            value=[]
        )

        create_costcenter=rail.SetVariableOperator(
            task_id='create_costcenter',
            append=False,
            name='cost_center',
            value=[]
        )

        create_schedule=rail.SetVariableOperator(
            task_id='create_schedule',
            append=False,
            name='schedule',
            value=[]
        )

        create_holidaycalendar=rail.SetVariableOperator(
            task_id='create_holidaycalendar',
            append=False,
            name='holidaycalendar',
            value=''
        )

        create_loginstatus=rail.SetVariableOperator(
            task_id='create_loginstatus',
            append=False,
            name='loginstatus',
            value=''
        )

        update_timeoffapprovalpath = rail.SetVariableOperator(
            task_id='update_timeoffapprovalpath',
            append=False,
            name='{{ result("create_timeoffapprovalpath").name }}',
            value={
                "uri": None,
                "name": "Supervisor"
            }
        )

        get_location_lookup_variable = rail.GetVariableOperator(
            task_id='get_location_lookup_variable',
            name="{{ result('create_location_lookup').name }}"
        )

        get_workersubshift_lookup_variable = rail.GetVariableOperator(
            task_id='get_workersubshift_lookup_variable',
            name="{{ result('create_workersubshift_lookup').name }}"
        )

        get_country_lookup_variable = rail.GetVariableOperator(
            task_id='get_country_lookup_variable',
            name="{{ result('create_country_lookup').name }}"
        )

        search_entry_in_mapper_for_employeetype_37 = rail.PythonOperator(
            task_id = "search_entry_in_mapper_for_employeetype_37",
            python_callable=python_callable.search_in_mapper_for_employeetype
        )

        if_search_entry_in_mapper_for_employeetype_37_value_present = rail.IfOperator(
            task_id='if_search_entry_in_mapper_for_employeetype_37_value_present',
            test="{{ result('search_entry_in_mapper_for_employeetype_37').value | is_truthy }}",
            yes_task="get_required_employeetype_uri",
            no_task="if_entry_in_mapper_37_value_not_present_or_deptgrpuri_not_present",
        )

        get_required_employeetype_uri = rail.PythonOperator(
            task_id = "get_required_employeetype_uri",
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_employee_type'),'displayText', rail.result(
                    'search_entry_in_mapper_for_employeetype_37')['value'], 'uri', '')
        )

        if_entry_in_mapper_37_value_not_present_or_deptgrpuri_not_present = rail.IfOperator(
            task_id='if_entry_in_mapper_37_value_not_present_or_deptgrpuri_not_present',
            test="{{ result('search_entry_in_mapper_for_employeetype_37').value | is_falsy or \
                dag_run.conf.departmentgroupuri | is_falsy }}",
            yes_task="details_employeetype_and_departmentygrpuri_not_exist",
            no_task="if_entry_in_mapper_37_value_present_and_deptgrpuri_present",
        )

        details_employeetype_and_departmentygrpuri_not_exist = rail.PythonOperator(
            task_id = 'details_employeetype_and_departmentygrpuri_not_exist',
            python_callable=get_details_for_employeetype_and_departmentygrpuri_not_exist
        )

        log_user_import_employeetype_dept_not_exist = rail.WriteLogOperator(
            task_id="log_user_import_employeetype_dept_not_exist",
            log = '{{ dag_run.conf.logger}}',
            message="Exception",
            severity="Exception",
            properties=lambda dag_run: {
                "userid": dag_run.conf['userid'],
                "username": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "action": "Add",
                "status": "Exception",
                "details": rail.result('details_employeetype_and_departmentygrpuri_not_exist'),
                "country": rail.result('get_country_lookup_variable')['value']
            }
        )

        if_entry_in_mapper_37_value_present_and_deptgrpuri_present = rail.IfOperator(
            task_id='if_entry_in_mapper_37_value_present_and_deptgrpuri_present',
            test="{{ result('search_entry_in_mapper_for_employeetype_37').value | is_truthy and \
                dag_run.conf.departmentgroupuri | is_truthy }}",
            yes_task="search_momentive_mapper_values",
            no_task="write_log_user_import",
        )

        search_momentive_mapper_values = rail.PythonOperator(
            task_id = "search_momentive_mapper_values",
            python_callable=python_callable.search_momentivemapper_workertype_country
        )

        get_shift_lookup_variable = rail.GetVariableOperator(
            task_id='get_shift_lookup_variable',
            name="{{ result('create_shift_lookup').name }}"
        )

        usermappings_mapper = rail.PythonOperator(
            task_id = "usermappings_mapper",
            python_callable=python_callable.user_mappings_mapper,
            op_args=['{{ dag_run.conf.workertype }}','{{ dag_run.conf.exemptionstatus }}','{{ dag_run.conf.gender }}','add']
        )

        get_required_payrule_script = rail.RepliconServiceOperator(
            task_id='get_required_payrule_script',
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts"
        )

        if_timesheet_approval_path_present = rail.IfOperator(
            task_id='if_timesheet_approval_path_present',
            test="{{ result('usermappings_mapper').timesheetapprovalpath | is_truthy }}",
            yes_task="update_timesheetapprovalpath_variable",
            no_task="create_payrule_variable",
        )

        update_timesheetapprovalpath_variable = rail.SetVariableOperator(
            task_id='update_timesheetapprovalpath_variable',
            append=False,
            name='{{ result("create_timesheetapprovalpath").name }}',
            value={
                "uri": None,
                "name": "{{ result('usermappings_mapper').timesheetapprovalpath }}"
            }
        )

        create_payrule_variable=rail.SetVariableOperator(
            task_id='create_payrule_variable',
            append=False,
            name='payrule',
            value=[]
        )

        if_payrule_present_in_usermapping = rail.IfOperator(
            task_id='if_payrule_present_in_usermapping',
            test="{{ result('usermappings_mapper').payrule | is_truthy }}",
            yes_task="get_required_payrule_uri",
            no_task="if_schedule_present_in_usermapping",
        )

        get_required_payrule_uri = rail.PythonOperator(
            task_id = "get_required_payrule_uri",
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_required_payrule_script'),'displayText', rail.result('usermappings_mapper')['payrule'], 'uri', '')
        )

        if_payruleuri_present = rail.IfOperator(
            task_id='if_payruleuri_present',
            test="{{ result('get_required_payrule_uri') | is_truthy }}",
            yes_task="update_payrule_variable_54",
            no_task="log_exception_payrule_not_found",
        )

        update_payrule_variable_54 = rail.SetVariableOperator(
            task_id='update_payrule_variable_54',
            append=False,
            name='{{ result("create_payrule_variable").name }}',
            value=[
                {
                    "payRuleScript": {
                        "uri": "{{ result('get_required_payrule_uri') }}",
                        "name": None
                    },
                    "effectiveDate": None
                }
            ]
        )

        log_exception_payrule_not_found = rail.WriteLogOperator(
            task_id='log_exception_payrule_not_found',
            log = "{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value" : "Payrule - {{ result('usermappings_mapper').payrule }} not found"
            }
        )

        if_schedule_present_in_usermapping = rail.IfOperator(
            task_id='if_schedule_present_in_usermapping',
            test="{{ result('usermappings_mapper').schedule | is_truthy }}",
            yes_task="if_schedule_from_usermapping_equals_shift",
            no_task="if_legalentity_present_and_starts_with_urn",
        )

        if_schedule_from_usermapping_equals_shift = rail.IfOperator(
            task_id='if_schedule_from_usermapping_equals_shift',
            test="{{ result('usermappings_mapper').schedule == 'Shift' }}",
            yes_task="update_schedule_variable_59",
            no_task="get_ofc_sched_default_uri",
        )

        update_schedule_variable_59 = rail.SetVariableOperator(
            task_id='update_schedule_variable_59',
            append=False,
            name='{{ result("create_schedule").name }}',
            value=[
                {
                    "schedulePolicy": {
                        "officeScheduleUri": None,
                        "name": None,
                        "officeSchedule":{
                            "officeScheduleUri": None,
                            "name": None
                        },
                        "scheduleTypeUri":"urn:replicon:schedule-type:shift"
                    },
                    "effectiveDate": None
                }
            ]
        )

        get_ofc_sched_default_uri = rail.RepliconServiceOperator(
            task_id="get_ofc_sched_default_uri",
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', rail.result('usermappings_mapper')['schedule'] , 'uri', '')
        )

        if_schedule_uri_present = rail.IfOperator(
            task_id='if_schedule_uri_present',
            test="{{ result('get_ofc_sched_default_uri') | is_truthy }}",
            yes_task="update_schedule_variable_64",
            no_task="log_exception_schedule_not_found",
        )

        update_schedule_variable_64 = rail.SetVariableOperator(
            task_id='update_schedule_variable_64',
            append=False,
            name='{{ result("create_schedule").name }}',
            value=[
                {
                    "schedulePolicy": {
                        "officeScheduleUri": "{{ result('get_ofc_sched_default_uri') }}",
                        "name": None,
                        "officeSchedule":{
                            "officeScheduleUri": "{{ result('get_ofc_sched_default_uri') }}",
                            "name": None
                        },
                        "scheduleTypeUri":"urn:replicon:schedule-type:office-schedule"
                    },
                    "effectiveDate": None
                }
            ]
        )

        log_exception_schedule_not_found = rail.WriteLogOperator(
            task_id='log_exception_schedule_not_found',
            log = "{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value" : "Schedule - {{ result('usermappings_mapper').schedule }} not found in the instance/ disabled hence schedule not assigned"
            }
        )

        if_legalentity_present_and_starts_with_urn = rail.IfOperator(
            task_id='if_legalentity_present_and_starts_with_urn',
            test="{{ dag_run.conf.legalentity | is_truthy and dag_run.conf.legalentityuri | starts_with('urn') }}",
            yes_task="update_legalentity_division_variable",
            no_task="log_exception_legalentity_not_found",
        )

        update_legalentity_division_variable = rail.SetVariableOperator(
            task_id='update_legalentity_division_variable',
            append=False,
            name='{{ result("create_legalentity_division").name }}',
            value=[
                {
                    "division": {
                        "uri": "{{ dag_run.conf.legalentityuri }}",
                        "parentUri":None,
                        "name": None
                    },
                    "effectiveDate": None
                }
            ]
        )


        log_exception_legalentity_not_found = rail.WriteLogOperator(
            task_id='log_exception_legalentity_not_found',
            log = "{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value" : "Legal entity - {{ dag_run.conf.legalentityuri }} not found or is disabled in Replicon"
            }
        )

        if_paygroupuri_present_and_starts_with_urn = rail.IfOperator(
            task_id='if_paygroupuri_present_and_starts_with_urn',
            test="{{ dag_run.conf.paygroupuri | is_truthy and dag_run.conf.paygroupuri | starts_with('urn') }}",
            yes_task="update_paygroupuri_variable",
            no_task="log_exception_paygroupuri_not_found",
        )

        update_paygroupuri_variable = rail.SetVariableOperator(
            task_id='update_paygroupuri_variable',
            append=False,
            name='{{ result("create_Paygroup_servicecenter").name }}',
            value=[
                {
                    "serviceCenter": {
                        "uri": "{{ dag_run.conf.paygroupuri }}",
                        "parentUri":None,
                        "name": None
                    },
                    "effectiveDate": None
                }
            ]
        )


        log_exception_paygroupuri_not_found = rail.WriteLogOperator(
            task_id='log_exception_paygroupuri_not_found',
            log = "{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value" : "Paygroup - {{ dag_run.conf.paygroupuri }} not found or is disabled in Replicon"
            }
        )

        if_costcenteruri_present_and_starts_with_urn = rail.IfOperator(
            task_id='if_costcenteruri_present_and_starts_with_urn',
            test="{{ dag_run.conf.costcenteruri | is_truthy and dag_run.conf.costcenteruri | starts_with('urn') }}",
            yes_task="update_costcenteruri_variable",
            no_task="if_holidaycalendar_present_in_usermapping",
        )

        update_costcenteruri_variable = rail.SetVariableOperator(
            task_id='update_costcenteruri_variable',
            append=False,
            name='{{ result("create_costcenter").name }}',
            value=[
                {
                    "costCenter": {
                        "uri": "{{ dag_run.conf.costcenteruri }}",
                        "parentUri":None,
                        "name": None
                    },
                    "effectiveDate": None
                }
            ]
        )

        if_holidaycalendar_present_in_usermapping = rail.IfOperator(
            task_id='if_holidaycalendar_present_in_usermapping',
            test="{{ result('usermappings_mapper').holidaycalendar | is_truthy }}",
            yes_task="get_usermapping_holiday_calendar_uri",
            no_task="create_timesheetperiod_variable",
        )

        get_usermapping_holiday_calendar_uri = rail.RepliconServiceOperator(
            task_id='get_usermapping_holiday_calendar_uri',
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'name', rail.result('usermappings_mapper')['holidaycalendar'], 'uri', '')
        )

        if_holidaycalendaruri_present = rail.IfOperator(
            task_id='if_holidaycalendaruri_present',
            test="{{ result('get_usermapping_holiday_calendar_uri') | is_truthy }}",
            yes_task="update_holidaycalendar_variable",
            no_task="log_exception_holidaycalendar_not_found",
        )

        update_holidaycalendar_variable = rail.SetVariableOperator(
            task_id='update_holidaycalendar_variable',
            append=False,
            name='{{ result("create_holidaycalendar").name }}',
            value={
                "uri": "{{ result('get_usermapping_holiday_calendar_uri') }}",
                "name": None
            }
        )

        log_exception_holidaycalendar_not_found = rail.WriteLogOperator(
            task_id='log_exception_holidaycalendar_not_found',
            log = "{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value" : "Holiday calendar  - {{ result('usermappings_mapper').holidaycalendar }} not \
                    found in the instance hence holiday calendar not assigned."
            }
        )

        create_timesheetperiod_variable = rail.SetVariableOperator(
            task_id='create_timesheetperiod_variable',
            append=False,
            name='timesheetperiod',
            value=[]
        )

        if_legalentity_not_momentive_perf_mat_kor_and_frc = rail.IfOperator(
            task_id='if_legalentity_not_momentive_perf_mat_kor_and_frc',
            test="{{ dag_run.conf.legalentity != 'MOMENTIVE PERFORMANCE MATERIALS KOREA CO., LTD.' and \
                dag_run.conf.legalentity != 'MOMENTIVE PERFORMANCE MATERIALS FRANCE SARL' }}",
            yes_task="update_timesheetperiod_variable_92",
            no_task="if_shiftlookup_equals_any_and_legalentity_is__mom_perf_mat_ltd",
        )

        update_timesheetperiod_variable_92 = rail.SetVariableOperator(
            task_id='update_timesheetperiod_variable_92',
            append=False,
            name='{{ result("create_timesheetperiod_variable").name }}',
            value=request_payload.get_timesheetperiod_val_92
        )

        if_shiftlookup_equals_any_and_legalentity_is__mom_perf_mat_ltd = rail.IfOperator(
            task_id='if_shiftlookup_equals_any_and_legalentity_is__mom_perf_mat_ltd',
            test="{{ result('get_shift_lookup_variable').value == 'Any' and \
                dag_run.conf.legalentity == 'MOMENTIVE PERFORMANCE MATERIALS LTD' }}",
            yes_task="update_timesheetperiod_variable_94",
            no_task="create_variable_policysets",
        )

        update_timesheetperiod_variable_94 = rail.SetVariableOperator(
            task_id='update_timesheetperiod_variable_94',
            append=False,
            name='{{ result("create_timesheetperiod_variable").name }}',
            value=[]
        )

        create_variable_policysets = rail.SetVariableOperator(
            task_id='create_variable_policysets',
            append=False,
            name='policysets',
            value=[]
        )

        if_legalentity_is_mpm_fze = rail.IfOperator(
            task_id='if_legalentity_is_mpm_fze',
            test="{{ dag_run.conf.legalentity == 'MPM FZE' and \
                result('usermappings_mapper').punchentrypolicy  | is_truthy and \
                result('usermappings_mapper').timesheet | is_truthy}}",
            yes_task="update_policyset_variable_98",
            no_task="if_legalientity_is_mom_perf_mat",
        )

        if_legalientity_is_mom_perf_mat = rail.IfOperator(
            task_id='if_legalientity_is_mom_perf_mat',
            test="{{ dag_run.conf.legalentity == 'MOMENTIVE PERFORMANCE MATERIALS LTD' }}",
            yes_task="if_usermapping_punchentrypolicy_and_timesheet_present",
            no_task="if_legalientity_is_mom_perf_mat_ben",
        )

        if_usermapping_punchentrypolicy_and_timesheet_present = rail.IfOperator(
            task_id='if_usermapping_punchentrypolicy_and_timesheet_present',
            test="{{ result('usermappings_mapper').punchentrypolicy  | is_truthy and \
                result('usermappings_mapper').timesheet | is_truthy }}",
            yes_task="update_policyset_variable_98",
            no_task="update_policyset_variable_103",
        )

        update_policyset_variable_98 = rail.SetVariableOperator(
            task_id='update_policyset_variable_98',
            append=False,
            name='{{ result("create_variable_policysets").name }}',
            value=[
                {
                    "uri":None,
                    "name": "Time Off"
                },
                {
                    "uri":None,
                    "name": "{{ result('usermappings_mapper').punchentrypolicy }}"
                },
                {
                    "uri":None,
                    "name": "{{ result('usermappings_mapper').timesheet }}"
                }
            ]
        )

        update_policyset_variable_103 = rail.SetVariableOperator(
            task_id='update_policyset_variable_103',
            append=False,
            name='{{ result("create_variable_policysets").name }}',
            value=[
                {
                    "uri":None,
                    "name": "Time Off"
                }
            ]
        )

        if_legalientity_is_mom_perf_mat_ben = rail.IfOperator(
            task_id='if_legalientity_is_mom_perf_mat_ben',
            test="{{ dag_run.conf.legalentity == 'MOMENTIVE PERFORMANCE MATERIALS BENELUX BV' and \
                result('usermappings_mapper').timesheet | is_truthy }}",
            yes_task="update_policyset_variable_106",
            no_task="if_legalentity_is_momentive_perf_mat_kor_or_frc",
        )

        update_policyset_variable_106 = rail.SetVariableOperator(
            task_id='update_policyset_variable_106',
            append=False,
            name='{{ result("create_variable_policysets").name }}',
            value=[
                {
                    "uri":None,
                    "name": "Time Off"
                },
                {
                    "uri":None,
                    "name": "{{ result('usermappings_mapper').timesheet }}"
                }
            ]
        )

        if_legalentity_is_momentive_perf_mat_kor_or_frc = rail.IfOperator(
            task_id='if_legalentity_is_momentive_perf_mat_kor_or_frc',
            test="{{ dag_run.conf.legalentity == 'MOMENTIVE PERFORMANCE MATERIALS KOREA CO., LTD.' or \
                dag_run.conf.legalentity == 'MOMENTIVE PERFORMANCE MATERIALS FRANCE SARL' }}",
            yes_task="if_legalentity_is_momentive_perf_mat_kor",
            no_task="get_schedule_variable",
        )

        if_legalentity_is_momentive_perf_mat_kor = rail.IfOperator(
            task_id='if_legalentity_is_momentive_perf_mat_kor',
            test="{{ dag_run.conf.legalentity == 'MOMENTIVE PERFORMANCE MATERIALS KOREA CO., LTD.' }}",
            yes_task="update_timesheetperiod_variable_109",
            no_task="update_timesheetperiod_variable_111",
        )

        update_timesheetperiod_variable_109 = rail.SetVariableOperator(
            task_id='update_timesheetperiod_variable_109',
            append=False,
            name='{{ result("create_timesheetperiod_variable").name }}',
            value=request_payload.get_timesheetperiod_val_109
        )

        update_timesheetperiod_variable_111 = rail.SetVariableOperator(
            task_id='update_timesheetperiod_variable_111',
            append=False,
            name='{{ result("create_timesheetperiod_variable").name }}',
            value=[]
        )

        if_workertype_is_employee = rail.IfOperator(
            task_id='if_workertype_is_employee',
            test="{{ dag_run.conf.workertype == 'Employee' }}",
            yes_task="update_policyset_variable_113",
            no_task="get_schedule_variable",
        )

        update_policyset_variable_113 = rail.SetVariableOperator(
            task_id='update_policyset_variable_113',
            append=False,
            name='{{ result("create_variable_policysets").name }}',
            value=[
                {
                    "uri":None,
                    "name": "Time Off"
                },
                {
                    "uri":None,
                    "name": "{{ result('usermappings_mapper').timesheet }}"
                }
            ]
        )

        get_schedule_variable = rail.GetVariableOperator(
            task_id='get_schedule_variable',
            name="{{ result('create_schedule').name }}"
        )

        get_holidaycalendar_variable = rail.GetVariableOperator(
            task_id='get_holidaycalendar_variable',
            name="{{ result('create_holidaycalendar').name }}"
        )

        get_policyset_variable = rail.GetVariableOperator(
            task_id='get_policyset_variable',
            name="{{ result('create_variable_policysets').name }}"
        )

        get_timesheetapprovalpath_variable = rail.GetVariableOperator(
            task_id='get_timesheetapprovalpath_variable',
            name="{{ result('create_timesheetapprovalpath').name }}"
        )

        get_timeoffapprovalpath_variable = rail.GetVariableOperator(
            task_id='get_timeoffapprovalpath_variable',
            name="{{ result('create_timeoffapprovalpath').name }}"
        )

        get_legalentity_division_variable = rail.GetVariableOperator(
            task_id='get_legalentity_division_variable',
            name="{{ result('create_legalentity_division').name }}"
        )

        get_costcenter_variable = rail.GetVariableOperator(
            task_id='get_costcenter_variable',
            name="{{ result('create_costcenter').name }}"
        )

        get_paygrp_srvcenter_variable = rail.GetVariableOperator(
            task_id='get_paygrp_srvcenter_variable',
            name="{{ result('create_Paygroup_servicecenter').name }}"
        )

        get_payrule_variable = rail.GetVariableOperator(
            task_id='get_payrule_variable',
            name="{{ result('create_payrule_variable').name }}"
        )

        get_timesheetperiod_variable = rail.GetVariableOperator(
            task_id='get_timesheetperiod_variable',
            name="{{ result('create_timesheetperiod_variable').name }}"
        )

        create_user = rail.RepliconServiceOperator(
            task_id = "create_user",
            endpoint = "/services/ImportService1.svc/PutUser3",
            data = request_payload.create_user_payload
        )

        assign_policyDataAccessScopes_to_projectmanager = rail.RepliconServiceOperator(
            task_id='assign_policyDataAccessScopes_to_projectmanager',
            endpoint='/services/PermissionSetService1.svc/PutPolicyDataAccessScopesForUser',
            data=request_payload.assign_policydataaccessscope_department
        )

        remove_all_timeoffs = rail.RepliconServiceOperator(
            task_id='remove_all_timeoffs',
            endpoint='/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser',
            data={
                'userUri': "{{ result('create_user').uri }}",
                'timeOffTypeUris': []
            }
        )

        get_required_user_customfields = rail.RepliconServiceOperator(
            task_id='get_required_user_customfields',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={
                'objectUri': "urn:replicon:object-type:user"
            },
            data_handler=lambda response: {
                'date_of_birth_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Date of Birth', 'uri', ''),
                'title_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Title', 'uri', ''),
                'workersubtypeuri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Worker Sub Type', 'uri', ''),
                'year_of_service_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Years of Service', 'uri', ''),
                'hrm_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'HRM', 'uri', ''),
                'continous_years_of_service_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Continuous Years of Service - YOS', 'uri', ''),
                'timeoffservicedate_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Time off Service Date - YOSS', 'uri', ''),
                'gender_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Gender', 'uri', ''),
                'function_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Function', 'uri', ''),
                'workshift_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Work Shift', 'uri', '')
            }
        )

        if_CF_Date_of_Birth_MM_DD_YYYY_and_dob_uri_present = rail.IfOperator(
            task_id='if_CF_Date_of_Birth_MM_DD_YYYY_and_dob_uri_present',
            test="{{ dag_run.conf.date_of_birth | is_truthy and \
                result('get_required_user_customfields').date_of_birth_uri | is_truthy }}",
            yes_task="update_dob_udf",
            no_task="if_businesstitle_and_titleuri_present",
        )

        update_dob_udf = rail.RepliconServiceOperator(
            task_id='update_dob_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data=lambda dag_run: {
                "objectUri": rail.result('create_user')['uri'],
                "customFieldUri": rail.result('get_required_user_customfields')['date_of_birth_uri'],
                "value": request_payload.get_datetime_obj(dag_run.conf['date_of_birth'])
            }
        )

        if_businesstitle_and_titleuri_present = rail.IfOperator(
            task_id='if_businesstitle_and_titleuri_present',
            test="{{ dag_run.conf.businesstitle | is_truthy and \
                result('get_required_user_customfields').title_uri | is_truthy }}",
            yes_task="update_title_udf",
            no_task="if_workersubtype_and_workersubtypeuri_present",
        )

        update_title_udf = rail.RepliconServiceOperator(
            task_id='update_title_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').title_uri }}",
                "value": "{{ dag_run.conf.businesstitle }}"
            }
        )

        if_workersubtype_and_workersubtypeuri_present = rail.IfOperator(
            task_id='if_workersubtype_and_workersubtypeuri_present',
            test="{{ dag_run.conf.worker_subType | is_truthy and \
                result('get_required_user_customfields').workersubtypeuri | is_truthy }}",
            yes_task="get_workersubtype_dropdowns",
            no_task="if_yearsofservice_and_yearsofserviceuri_present",
        )

        get_workersubtype_dropdowns = rail.RepliconServiceOperator(
            task_id='get_workersubtype_dropdowns',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data={
                'customFieldUri': "{{ result('get_required_user_customfields').workersubtypeuri }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['worker_subType'], 'uri', '')
        )

        if_get_workersubtype_dropdowns_uri_present = rail.IfOperator(
            task_id='if_get_workersubtype_dropdowns_uri_present',
            test="{{ result('get_workersubtype_dropdowns') | is_truthy }}",
            yes_task="update_worker_subtype_udf",
            no_task="if_yearsofservice_and_yearsofserviceuri_present",
        )

        update_worker_subtype_udf = rail.RepliconServiceOperator(
            task_id='update_worker_subtype_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').workersubtypeuri }}",
                "customFieldDropDownOptionUri": "{{ result('get_workersubtype_dropdowns') }}"
            }
        )

        if_yearsofservice_and_yearsofserviceuri_present = rail.IfOperator(
            task_id='if_yearsofservice_and_yearsofserviceuri_present',
            test="{{ dag_run.conf.year_of_service | is_truthy and \
                result('get_required_user_customfields').year_of_service_uri | is_truthy }}",
            yes_task="update_years_of_service_udf",
            no_task="if_fieldhr_and_hrmuri_present",
        )

        update_years_of_service_udf = rail.RepliconServiceOperator(
            task_id='update_years_of_service_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').year_of_service_uri }}",
                "value": "{{ dag_run.conf.year_of_service }}"
            }
        )

        if_fieldhr_and_hrmuri_present = rail.IfOperator(
            task_id='if_fieldhr_and_hrmuri_present',
            test="{{ dag_run.conf.fieldhr | is_truthy and \
                result('get_required_user_customfields').hrm_uri | is_truthy }}",
            yes_task="update_fieldhr_udf",
            no_task="if_contsrvcdate_and_contyearsofserviceuri_present",
        )

        update_fieldhr_udf = rail.RepliconServiceOperator(
            task_id='update_fieldhr_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').hrm_uri }}",
                "value": "{{ dag_run.conf.fieldhr }}"
            }
        )

        if_contsrvcdate_and_contyearsofserviceuri_present = rail.IfOperator(
            task_id='if_contsrvcdate_and_contyearsofserviceuri_present',
            test="{{ dag_run.conf.continous_service_date | is_truthy and \
                result('get_required_user_customfields').continous_years_of_service_uri | is_truthy }}",
            yes_task="update_contsrvcdate_udf",
            no_task="if_timeoffservdate_and_timeoffservdateuri_present",
        )

        update_contsrvcdate_udf = rail.RepliconServiceOperator(
            task_id='update_contsrvcdate_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').continous_years_of_service_uri }}",
                "value": "{{ dag_run.conf.continous_service_date }}"
            }
        )

        if_timeoffservdate_and_timeoffservdateuri_present = rail.IfOperator(
            task_id='if_timeoffservdate_and_timeoffservdateuri_present',
            test="{{ dag_run.conf.timeoff_service_date | is_truthy and \
                result('get_required_user_customfields').timeoffservicedate_uri | is_truthy }}",
            yes_task="update_timeoffservicedate_udf",
            no_task="if_gender_and_genderuri_present",
        )

        update_timeoffservicedate_udf = rail.RepliconServiceOperator(
            task_id='update_timeoffservicedate_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').timeoffservicedate_uri }}",
                "value": "{{ dag_run.conf.timeoff_service_date }}"
            }
        )

        if_gender_and_genderuri_present = rail.IfOperator(
            task_id='if_gender_and_genderuri_present',
            test="{{ dag_run.conf.gender | is_truthy and \
                result('get_required_user_customfields').gender_uri | is_truthy }}",
            yes_task="update_gender_udf",
            no_task="if_function_and_functionuri_present",
        )

        update_gender_udf = rail.RepliconServiceOperator(
            task_id='update_gender_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').gender_uri }}",
                "value": "{{ dag_run.conf.gender }}"
            }
        )

        if_function_and_functionuri_present = rail.IfOperator(
            task_id='if_function_and_functionuri_present',
            test="{{ dag_run.conf.function | is_truthy and \
                result('get_required_user_customfields').function_uri | is_truthy }}",
            yes_task="update_function_udf",
            no_task="if_workshift_and_workshifturi_present",
        )

        update_function_udf = rail.RepliconServiceOperator(
            task_id='update_function_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').function_uri }}",
                "value": "{{ dag_run.conf.function }}"
            }
        )

        if_workshift_and_workshifturi_present = rail.IfOperator(
            task_id='if_workshift_and_workshifturi_present',
            test="{{ dag_run.conf.work_shift | is_truthy and \
                result('get_required_user_customfields').workshift_uri | is_truthy }}",
            yes_task="get_workshift_dropdowns",
            no_task="if_manager_id_present",
        )

        get_workshift_dropdowns = rail.RepliconServiceOperator(
            task_id='get_workshift_dropdowns',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data={
                'customFieldUri': "{{ result('get_required_user_customfields').workshift_uri }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['work_shift'], 'uri', '')
        )

        if_get_workshift_dropdowns_uri_present = rail.IfOperator(
            task_id='if_get_workshift_dropdowns_uri_present',
            test="{{ result('get_workshift_dropdowns') | is_truthy }}",
            yes_task="update_workshift_udf",
            no_task="if_manager_id_present",
        )

        update_workshift_udf = rail.RepliconServiceOperator(
            task_id='update_workshift_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').workshift_uri }}",
                "customFieldDropDownOptionUri": "{{ result('get_workshift_dropdowns') }}"
            }
        )

        if_manager_id_present = rail.IfOperator(
            task_id='if_manager_id_present',
            test="{{ dag_run.conf.managerid | is_truthy }}",
            yes_task="search_for_user_with_empid",
            no_task="if_usermapping_timesheet_present",
        )

        search_for_user_with_empid = rail.RepliconServiceOperator(
            task_id='search_for_user_with_empid',
            endpoint="/services/UserListService1.svc/GetData",
            data = request_payload.search_supervisor_payload,
            data_handler=python_callable.get_userdata_list_for_managerid
        )

        check_if_multiple_manageruseruri_present = rail.IfOperator(
            task_id='check_if_multiple_manageruseruri_present',
            test=lambda: bool(len(rail.result('search_for_user_with_empid')) > 1 ),
            yes_task="log_multiple_user_for_same_managerid",
            no_task="check_if_single_manageruseruri_present",
        )

        log_multiple_user_for_same_managerid = rail.WriteLogOperator(
            task_id='log_multiple_user_for_same_managerid',
            log = "{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value" : "Supervisor not assigned for user {{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }} as \
                    multiple users have same Employee ID:{{ dag_run.conf.managerid }} ."
            }
        )

        check_if_single_manageruseruri_present = rail.IfOperator(
            task_id='check_if_single_manageruseruri_present',
            test=lambda: bool(len(rail.result('search_for_user_with_empid')) == 1 ),
            yes_task="get_manager_details",
            no_task="log_supervisor_assignment",
        )

        get_manager_details = rail.RepliconServiceOperator(
            task_id='get_manager_details',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data = request_payload.get_manager_details_payload
        )

        if_manager_details_present_and_enabled = rail.IfOperator(
            task_id='if_manager_details_present_and_enabled',
            test="{{ result('get_manager_details') | is_truthy and result('get_manager_details')[0]['userDetails']['isEnabled'] | is_truthy }}",
            yes_task="get_assigned_permissionset_foruser",
            no_task="log_supervisor_assignment",
        )

        get_assigned_permissionset_foruser = rail.RepliconServiceOperator(
            task_id='get_assigned_permissionset_foruser',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data = {
                "userUri": "{{ result('search_for_user_with_empid')[0].uri }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', 'urn:replicon:policy:supervision', 'user.uri', '')
        )

        if_supervisor_permission_not_assigned = rail.IfOperator(
            task_id='if_supervisor_permission_not_assigned',
            test="{{ result('get_assigned_permissionset_foruser') | is_falsy }}",
            yes_task="add_missing_supervisor_permission",
            no_task="update_initial_supervisor",
        )

        add_missing_supervisor_permission = rail.RepliconServiceOperator(
            task_id='add_missing_supervisor_permission',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data=request_payload.add_missing_supervisor_permission_payload
        )

        update_initial_supervisor = rail.RepliconServiceOperator(
            task_id = "update_initial_supervisor",
            endpoint = "/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data = {
                "userUri": "{{ result('create_user').uri }}",
                "supervisorUri": "{{ result('search_for_user_with_empid')[0].uri }}",
                "dateRange": None
            }
        )

        log_supervisor_assignment = rail.WriteLogOperator(
            task_id="log_supervisor_assignment",
            log = '{{ dag_run.conf.supervisor_logger}}',
            message="Exception",
            severity="Exception",
            properties=request_payload.supervisor_assignment_log_payload
        )

        if_usermapping_timesheet_present = rail.IfOperator(
            task_id='if_usermapping_timesheet_present',
            test="{{ result('usermappings_mapper').timesheet | is_truthy }}",
            yes_task="get_timesheetdate_for2",
            no_task="if_usermapping_activities_present",
        )

        get_timesheetdate_for2 = rail.RepliconServiceOperator(
            task_id='get_timesheetdate_for2',
            endpoint="/services/TimesheetService1.svc/GetTimesheetForDate2",
            data=lambda dag_run: {
                'userUri': rail.result('create_user')['uri'],
                "date": request_payload.get_datetime_obj(dag_run.conf['hiredate']),
                "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
            }
        )

        if_usermapping_activities_present = rail.IfOperator(
            task_id='if_usermapping_activities_present',
            test="{{ result('usermappings_mapper').activities | is_truthy }}",
            yes_task="get_req_enabledactivities",
            no_task="if_usermapping_language_present",
        )

        get_req_enabledactivities=rail.RepliconServiceOperator(
            task_id='get_req_enabledactivities',
            endpoint="/services/ActivityService1.svc/GetEnabledActivities",
            data_handler=lambda response : rail.find_first_by_attr_and_get_attr(
                    response, 'name', rail.result('usermappings_mapper')['activities'], 'uri', '')
        )

        if_activity_uri_present = rail.IfOperator(
            task_id='if_activity_uri_present',
            test="{{ result('get_req_enabledactivities') | is_truthy }}",
            yes_task="put_activity_assignment_for_user",
            no_task="log_exception_activity_not_found",
        )

        put_activity_assignment_for_user = rail.RepliconServiceOperator(
            task_id='put_activity_assignment_for_user',
            endpoint="/services/ActivityService1.svc/PutActivityAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result('create_user')['uri'],
                "activityUris": [rail.result('get_req_enabledactivities')]
            }
        )

        log_exception_activity_not_found = rail.WriteLogOperator(
            task_id='log_exception_activity_not_found',
            log = "{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value" : "Activity not assigned since '{{ result('usermappings_mapper').activities }}'  is not avialble in Replicon"
            }
        )

        if_usermapping_language_present = rail.IfOperator(
            task_id='if_usermapping_language_present',
            test="{{ result('usermappings_mapper').language | is_truthy }}",
            yes_task="update_langauge_for_user",
            no_task="if_timeoffs_present_and_active_equal_1",
        )

        update_langauge_for_user = rail.RepliconServiceOperator(
            task_id='update_langauge_for_user',
            endpoint="/services/InternationalizationService1.svc/UpdateLanguageForUser",
            data=lambda: {
                "userUri": rail.result('create_user')['uri'],
                "languageUri": rail.result('usermappings_mapper')['language']
            }
        )

        if_timeoffs_present_and_active_equal_1 = rail.IfOperator(
            task_id='if_timeoffs_present_and_active_equal_1',
            test="{{ result('usermappings_mapper').timeoffs | is_truthy and \
                dag_run.conf.active == '1' }}",
            yes_task="trigger_timeoff_add_new_user",
            no_task="write_log_user_import",
        )

        trigger_timeoff_add_new_user = rail.TriggerDagRunOperator(
            task_id='trigger_timeoff_add_new_user',
            trigger_dag_id=f'momentive_userimport_timeoff_add_newuser_child_{config.instance}',
            conf=request_payload.trigger_timeoff_addnew_user,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_timeoff_add_new_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_timeoff_add_new_user',
            dag_runs='{{ result("trigger_timeoff_add_new_user") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        write_log_user_import = rail.WriteCSVFileOperator(
            task_id='write_log_user_import',
            source="{{ result('exception_log') }}",
            header=['value'],
            row=lambda item: [
                item['properties']['value']
            ]
        )

        log_user_import = rail.WriteLogOperator(
            task_id='log_user_import',
            log = "{{ dag_run.conf.logger }}",
            message="na",
            severity="Skipped",
            properties=python_callable.get_status_and_details_for_add
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            log = '{{ dag_run.conf.logger}}',
            trigger_rule='one_failed',
            message="Error",
            severity="Error",
            properties={
                "userid": "{{ dag_run.conf.userid }}",
                "username": "{{ dag_run.conf.firstname }}" + " " + "{{ dag_run.conf.lastname }}",
                "action": "Add",
                "status": "Error",
                'details': "{{ get_error_message() }}",
                'country': "{{ result('get_country_lookup_variable').value }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> exception_log

        exception_log >> get_input_validation_log >> if_input_validation_log_present

        if_input_validation_log_present >> rail.Label('Yes') >> log_user_import_not_created >> catch_and_log_error
        if_input_validation_log_present >> rail.Label('No') >> if_workertype_not_contingentworker_and_gender_not_present

        if_workertype_not_contingentworker_and_gender_not_present >> rail.Label('Yes') >> log_user_import_not_created_gender_not_present >> \
            catch_and_log_error
        if_workertype_not_contingentworker_and_gender_not_present >> rail.Label('No') >> get_all_employee_type

        get_all_employee_type >> get_all_permissionsets >> get_all_timezones >> create_location_lookup >> create_country_lookup >> \
            create_shift_lookup >> if_workshift_equals_prod_rota_or_nonrota

        if_workshift_equals_prod_rota_or_nonrota >> rail.Label('Yes') >> update_shift_lookup_to_workshift >> create_workersubshift_lookup
        if_workshift_equals_prod_rota_or_nonrota >> rail.Label('No') >> update_shift_lookup_to_any >> create_workersubshift_lookup

        create_workersubshift_lookup >> create_timesheetapprovalpath >> create_timeoffapprovalpath >> create_legalentity_division >> \
            create_Paygroup_servicecenter >> create_costcenter >> create_schedule >> create_holidaycalendar >> create_loginstatus >> \
                update_timeoffapprovalpath >> get_location_lookup_variable >> get_workersubshift_lookup_variable >> get_country_lookup_variable >> \
                    search_entry_in_mapper_for_employeetype_37 >> if_search_entry_in_mapper_for_employeetype_37_value_present

        if_search_entry_in_mapper_for_employeetype_37_value_present >> rail.Label('Yes') >> get_required_employeetype_uri >> \
            if_entry_in_mapper_37_value_not_present_or_deptgrpuri_not_present
        if_search_entry_in_mapper_for_employeetype_37_value_present >> rail.Label('No') >> if_entry_in_mapper_37_value_not_present_or_deptgrpuri_not_present

        if_entry_in_mapper_37_value_not_present_or_deptgrpuri_not_present >> rail.Label('Yes') >> details_employeetype_and_departmentygrpuri_not_exist >> \
            log_user_import_employeetype_dept_not_exist >> catch_and_log_error
        if_entry_in_mapper_37_value_not_present_or_deptgrpuri_not_present >> rail.Label('No') >> if_entry_in_mapper_37_value_present_and_deptgrpuri_present

        if_entry_in_mapper_37_value_present_and_deptgrpuri_present >> rail.Label('Yes') >> search_momentive_mapper_values
        if_entry_in_mapper_37_value_present_and_deptgrpuri_present >> rail.Label('No') >> write_log_user_import

        search_momentive_mapper_values >> get_shift_lookup_variable >> usermappings_mapper >> get_required_payrule_script >> if_timesheet_approval_path_present

        if_timesheet_approval_path_present >> rail.Label('Yes') >> update_timesheetapprovalpath_variable >> create_payrule_variable
        if_timesheet_approval_path_present >> rail.Label('No') >> create_payrule_variable

        create_payrule_variable >> if_payrule_present_in_usermapping

        if_payrule_present_in_usermapping >> rail.Label('Yes') >> get_required_payrule_uri >> if_payruleuri_present
        if_payrule_present_in_usermapping >> rail.Label('No') >> if_schedule_present_in_usermapping

        if_payruleuri_present >> rail.Label('Yes') >> update_payrule_variable_54 >> if_schedule_present_in_usermapping
        if_payruleuri_present >> rail.Label('No') >> log_exception_payrule_not_found >> if_schedule_present_in_usermapping

        if_schedule_present_in_usermapping >> rail.Label('Yes') >> if_schedule_from_usermapping_equals_shift
        if_schedule_present_in_usermapping >> rail.Label('No') >> if_legalentity_present_and_starts_with_urn

        if_schedule_from_usermapping_equals_shift >> rail.Label('Yes') >> update_schedule_variable_59 >> get_ofc_sched_default_uri
        if_schedule_from_usermapping_equals_shift >> rail.Label('No') >> get_ofc_sched_default_uri

        get_ofc_sched_default_uri >> if_schedule_uri_present

        if_schedule_uri_present >> rail.Label('Yes') >> update_schedule_variable_64 >> if_legalentity_present_and_starts_with_urn
        if_schedule_uri_present >> rail.Label('No') >> log_exception_schedule_not_found >> if_legalentity_present_and_starts_with_urn

        if_legalentity_present_and_starts_with_urn >> rail.Label('Yes') >> update_legalentity_division_variable >> if_paygroupuri_present_and_starts_with_urn
        if_legalentity_present_and_starts_with_urn >> rail.Label('No') >> log_exception_legalentity_not_found >> if_paygroupuri_present_and_starts_with_urn

        if_paygroupuri_present_and_starts_with_urn >> rail.Label('Yes') >> update_paygroupuri_variable >> if_costcenteruri_present_and_starts_with_urn
        if_paygroupuri_present_and_starts_with_urn >> rail.Label('No') >> log_exception_paygroupuri_not_found >> if_costcenteruri_present_and_starts_with_urn

        if_costcenteruri_present_and_starts_with_urn >> rail.Label('Yes') >> update_costcenteruri_variable >> if_holidaycalendar_present_in_usermapping
        if_costcenteruri_present_and_starts_with_urn >> rail.Label('No') >> if_holidaycalendar_present_in_usermapping

        if_holidaycalendar_present_in_usermapping >> rail.Label('Yes') >> get_usermapping_holiday_calendar_uri >> if_holidaycalendaruri_present
        if_holidaycalendar_present_in_usermapping >> rail.Label('No') >> create_timesheetperiod_variable

        if_holidaycalendaruri_present >> rail.Label('Yes') >> update_holidaycalendar_variable >> create_timesheetperiod_variable
        if_holidaycalendaruri_present >> rail.Label('No') >> log_exception_holidaycalendar_not_found >> create_timesheetperiod_variable

        create_timesheetperiod_variable >> if_legalentity_not_momentive_perf_mat_kor_and_frc

        if_legalentity_not_momentive_perf_mat_kor_and_frc >> rail.Label('Yes') >> update_timesheetperiod_variable_92 >> \
            if_shiftlookup_equals_any_and_legalentity_is__mom_perf_mat_ltd
        if_legalentity_not_momentive_perf_mat_kor_and_frc >> rail.Label('No') >> if_shiftlookup_equals_any_and_legalentity_is__mom_perf_mat_ltd

        if_shiftlookup_equals_any_and_legalentity_is__mom_perf_mat_ltd >> rail.Label('Yes') >> update_timesheetperiod_variable_94 >> create_variable_policysets
        if_shiftlookup_equals_any_and_legalentity_is__mom_perf_mat_ltd >> rail.Label('No') >> create_variable_policysets

        create_variable_policysets >> if_legalentity_is_mpm_fze

        if_legalentity_is_mpm_fze >> rail.Label('Yes') >> update_policyset_variable_98 >> if_legalientity_is_mom_perf_mat_ben
        if_legalentity_is_mpm_fze >> rail.Label('No') >> if_legalientity_is_mom_perf_mat

        if_legalientity_is_mom_perf_mat >> rail.Label('Yes') >> if_usermapping_punchentrypolicy_and_timesheet_present
        if_legalientity_is_mom_perf_mat >> rail.Label('No') >> if_legalientity_is_mom_perf_mat_ben

        if_usermapping_punchentrypolicy_and_timesheet_present >> rail.Label('Yes') >> update_policyset_variable_98 >> if_legalientity_is_mom_perf_mat_ben
        if_usermapping_punchentrypolicy_and_timesheet_present >> rail.Label('No') >> update_policyset_variable_103 >> if_legalientity_is_mom_perf_mat_ben

        if_legalientity_is_mom_perf_mat_ben >> rail.Label('Yes') >> update_policyset_variable_106 >> if_legalentity_is_momentive_perf_mat_kor_or_frc
        if_legalientity_is_mom_perf_mat_ben >> rail.Label('No') >> if_legalentity_is_momentive_perf_mat_kor_or_frc

        if_legalentity_is_momentive_perf_mat_kor_or_frc >> rail.Label('Yes') >> if_legalentity_is_momentive_perf_mat_kor
        if_legalentity_is_momentive_perf_mat_kor_or_frc >> rail.Label('No') >> get_schedule_variable

        if_legalentity_is_momentive_perf_mat_kor >> rail.Label('Yes') >> update_timesheetperiod_variable_109 >> if_workertype_is_employee
        if_legalentity_is_momentive_perf_mat_kor >> rail.Label('No') >> update_timesheetperiod_variable_111 >> if_workertype_is_employee

        if_workertype_is_employee >> rail.Label('Yes') >> update_policyset_variable_113 >> get_schedule_variable
        if_workertype_is_employee >> rail.Label('No') >> get_schedule_variable

        get_schedule_variable >> get_holidaycalendar_variable >> get_policyset_variable >> get_timesheetapprovalpath_variable >> \
            get_timeoffapprovalpath_variable >> get_legalentity_division_variable >> get_costcenter_variable >> get_paygrp_srvcenter_variable >> \
                get_payrule_variable >> get_timesheetperiod_variable >> create_user >> assign_policyDataAccessScopes_to_projectmanager >> \
                    remove_all_timeoffs >> get_required_user_customfields >> if_CF_Date_of_Birth_MM_DD_YYYY_and_dob_uri_present

        if_CF_Date_of_Birth_MM_DD_YYYY_and_dob_uri_present >> rail.Label('Yes') >> update_dob_udf >> if_businesstitle_and_titleuri_present
        if_CF_Date_of_Birth_MM_DD_YYYY_and_dob_uri_present >> rail.Label('No') >> if_businesstitle_and_titleuri_present

        if_businesstitle_and_titleuri_present >> rail.Label('Yes') >> update_title_udf >> if_workersubtype_and_workersubtypeuri_present
        if_businesstitle_and_titleuri_present >> rail.Label('No') >> if_workersubtype_and_workersubtypeuri_present

        if_workersubtype_and_workersubtypeuri_present >> rail.Label('Yes') >> get_workersubtype_dropdowns >> if_get_workersubtype_dropdowns_uri_present
        if_workersubtype_and_workersubtypeuri_present >> rail.Label('No') >> if_yearsofservice_and_yearsofserviceuri_present

        if_get_workersubtype_dropdowns_uri_present >> rail.Label('Yes') >> update_worker_subtype_udf >> if_yearsofservice_and_yearsofserviceuri_present
        if_get_workersubtype_dropdowns_uri_present >> rail.Label('No') >> if_yearsofservice_and_yearsofserviceuri_present

        if_yearsofservice_and_yearsofserviceuri_present >> rail.Label('Yes') >> update_years_of_service_udf >> if_fieldhr_and_hrmuri_present
        if_yearsofservice_and_yearsofserviceuri_present >> rail.Label('No') >> if_fieldhr_and_hrmuri_present

        if_fieldhr_and_hrmuri_present >> rail.Label('Yes') >> update_fieldhr_udf >> if_contsrvcdate_and_contyearsofserviceuri_present
        if_fieldhr_and_hrmuri_present >> rail.Label('No') >> if_contsrvcdate_and_contyearsofserviceuri_present

        if_contsrvcdate_and_contyearsofserviceuri_present >> rail.Label('Yes') >> update_contsrvcdate_udf >> if_timeoffservdate_and_timeoffservdateuri_present
        if_contsrvcdate_and_contyearsofserviceuri_present >> rail.Label('No') >> if_timeoffservdate_and_timeoffservdateuri_present

        if_timeoffservdate_and_timeoffservdateuri_present >> rail.Label('Yes') >> update_timeoffservicedate_udf >> if_gender_and_genderuri_present
        if_timeoffservdate_and_timeoffservdateuri_present >> rail.Label('No') >> if_gender_and_genderuri_present

        if_gender_and_genderuri_present >> rail.Label('Yes') >> update_gender_udf >> if_function_and_functionuri_present
        if_gender_and_genderuri_present >> rail.Label('No') >> if_function_and_functionuri_present

        if_function_and_functionuri_present >> rail.Label('Yes') >> update_function_udf >> if_workshift_and_workshifturi_present
        if_function_and_functionuri_present >> rail.Label('No') >> if_workshift_and_workshifturi_present

        if_workshift_and_workshifturi_present >> rail.Label('Yes') >> get_workshift_dropdowns >> if_get_workshift_dropdowns_uri_present
        if_workshift_and_workshifturi_present >> rail.Label('No') >> if_manager_id_present

        
        if_get_workshift_dropdowns_uri_present >> rail.Label('Yes') >> update_workshift_udf >> if_manager_id_present
        if_get_workshift_dropdowns_uri_present >> rail.Label('No') >> if_manager_id_present

        if_manager_id_present >> rail.Label('Yes') >> search_for_user_with_empid >> check_if_multiple_manageruseruri_present
        if_manager_id_present >> rail.Label('No') >> if_usermapping_timesheet_present

        check_if_multiple_manageruseruri_present >> rail.Label('Yes') >> log_multiple_user_for_same_managerid >> if_usermapping_timesheet_present
        check_if_multiple_manageruseruri_present >> rail.Label('No') >> check_if_single_manageruseruri_present

        check_if_single_manageruseruri_present >> rail.Label('Yes') >> get_manager_details >> if_manager_details_present_and_enabled
        check_if_single_manageruseruri_present >> rail.Label('No') >> log_supervisor_assignment >> if_usermapping_timesheet_present

        if_manager_details_present_and_enabled >> rail.Label('Yes') >> get_assigned_permissionset_foruser >> if_supervisor_permission_not_assigned
        if_manager_details_present_and_enabled >> rail.Label('No') >> log_supervisor_assignment >> if_usermapping_timesheet_present

        if_supervisor_permission_not_assigned >> rail.Label('Yes') >> add_missing_supervisor_permission >> update_initial_supervisor
        if_supervisor_permission_not_assigned >> rail.Label('No') >> update_initial_supervisor >> if_usermapping_timesheet_present

        if_usermapping_timesheet_present >> rail.Label('Yes') >> get_timesheetdate_for2 >> if_usermapping_activities_present
        if_usermapping_timesheet_present >> rail.Label('No') >> if_usermapping_activities_present

        if_usermapping_activities_present >> rail.Label('Yes') >> get_req_enabledactivities >> if_activity_uri_present
        if_usermapping_activities_present >> rail.Label('No') >> if_usermapping_language_present

        if_activity_uri_present >> rail.Label('Yes') >> put_activity_assignment_for_user >> if_usermapping_language_present
        if_activity_uri_present >> rail.Label('No') >> log_exception_activity_not_found >> if_usermapping_language_present

        if_usermapping_language_present >> rail.Label('Yes') >> update_langauge_for_user >> if_timeoffs_present_and_active_equal_1
        if_usermapping_language_present >> rail.Label('No') >> if_timeoffs_present_and_active_equal_1

        if_timeoffs_present_and_active_equal_1 >> rail.Label('Yes') >> trigger_timeoff_add_new_user >> wait_for_timeoff_add_new_user >> write_log_user_import
        if_timeoffs_present_and_active_equal_1 >> rail.Label('No') >> write_log_user_import

        write_log_user_import >> log_user_import >> catch_and_log_error

        catch_and_log_error >> log_to_sumo


    return dag

rail.for_each_instance(create_dag)
