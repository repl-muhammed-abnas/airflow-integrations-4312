
from datetime import timedelta, datetime
from omd.singapore_user_import.mappers import omd_user_import_singapore_mapper
from airflow.models import Variable
import rail

null=None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'omd_singapore_user_import_add_user_child{config.instance}',
        description=f'OMD User Import Singapre Add User {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_child, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_exception_logger'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_exception_logger',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_exception_logger=rail.SetVariableOperator(
            task_id='create_exception_logger',
            append=False,
            name='exceptionlogger',
            value=[]
        )

        get_error_message_for_log = rail.PythonOperator(
            task_id = 'get_error_message_for_log',
            python_callable=lambda dag_run: ("" if dag_run.conf['startdate'] else "Employee start date not present;") +
                                ("" if dag_run.conf['enabled'] else "Employee login status is not present;") +
                                ("" if dag_run.conf['firstname'] else "Employee Firstname is not present;") +
                                ("" if dag_run.conf['lastname'] else "Employee Lastname is not present;") +
                                ("" if dag_run.conf['department'] else "Employee department  is not present;") +
                                ("" if dag_run.conf['employeeId'] else "Employee ID  not present;")
        )

        if_error_message_present=rail.IfOperator(
            task_id='if_error_message_present',
            test='''{{ result('get_error_message_for_log') | is_truthy }}''',
            yes_task="log_user_not_created",
            no_task="if_enabled_status_is_not_yes",
        )

        log_user_not_created=rail.WriteLogOperator(
            task_id='log_user_not_created',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="Exception",
            properties={
                "employeeid": "{{ dag_run.conf.loginname }}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "status": "Exception",
                "action": "Add",
                "details": "User not created due to following reason/s:" + "{{result('get_error_message_for_log')}}",
                "jobid": "{{dag_run.conf.callerjobid}}",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_enabled_status_is_not_yes=rail.IfOperator(
            task_id='if_enabled_status_is_not_yes',
            test='''{{ dag_run.conf.enabled.lower()!='yes' }}''',
            yes_task="log_user_notcreated_due_to_enabled_not_yes",
            no_task="declare_customfields_list",
        )

        log_user_notcreated_due_to_enabled_not_yes=rail.WriteLogOperator(
            task_id='log_user_notcreated_due_to_enabled_not_yes',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="Exception",
            properties={
                "employeeid": "{{ dag_run.conf.loginname }}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "status": "Exception",
                "action": "Add",
                "details": 'User not created since user status is recived as "Yes"',
                "jobid": "{{dag_run.conf.callerjobid}}",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        declare_customfields_list=rail.SetVariableOperator(
            task_id='declare_customfields_list',
            append=False,
            name='customfields',
            value=[]
        )

        create_division_variable=rail.SetVariableOperator(
            task_id='create_division_variable',
            append=False,
            name='division',
            value=None
        )

        def get_value_from_mapper(field):
            value = ''
            for item in omd_user_import_singapore_mapper.mapper:
                if item['Field'] == field and item['condition1'] == 'any' and item['condition2'] == 'any':
                    value = item['value']
                    break
            return value

        log_locationtoassign=rail.PythonOperator(
            task_id='log_locationtoassign',
            python_callable=lambda: get_value_from_mapper('location')
        )

        log_permissiontoassign=rail.PythonOperator(
            task_id='log_permissiontoassign',
            python_callable=lambda: get_value_from_mapper('permissionset')
        )

        log_authenticationtype=rail.PythonOperator(
            task_id='log_authenticationtype',
            python_callable=lambda: get_value_from_mapper('authentication')
        )

        if_log_locationtoassign_present=rail.IfOperator(
            task_id='if_log_locationtoassign_present',
            test='''{{ result('log_locationtoassign') | is_truthy }}''',
            yes_task="update_location_variable",
            no_task="if_valid_email_present",
        )

        update_location_variable=rail.SetVariableOperator(
            task_id='update_location_variable',
            append=False,
            name='location',
            value=[
                {
                    "location": {
                    "uri": null,
                    "parentUri": null,
                    "name": "{{ result('log_locationtoassign') }}"
                    },
                    "effectiveDate": null
                }
            ]
        )

        if_valid_email_present=rail.IfOperator(
            task_id='if_valid_email_present',
            test='''{{ dag_run.conf.email | is_truthy  and dag_run.conf.email | matches('@') }}''',
            yes_task="update_email_variable",
            no_task="log_email_not_updated",
        )

        update_email_variable=rail.SetVariableOperator(
            task_id='update_email_variable',
            append=False,
            name='email',
            value="{{ dag_run.conf.email }}"
        )

        log_email_not_updated=rail.SetVariableOperator(
            task_id='log_email_not_updated',
            append=True,
            name='{{ result("create_exception_logger").name }}',
            value={
                "log": "Email not updated since email field received incorrect format"
            }
        )

        if_department_uri_present=rail.IfOperator(
            task_id='if_department_uri_present',
            test='''{{ dag_run.conf.departmenturi | is_truthy  and dag_run.conf.departmenturi != 'NA' }}''',
            yes_task="update_department_variable",
            no_task="log_department_not_available",
        )

        update_department_variable=rail.SetVariableOperator(
            task_id='update_department_variable',
            append=False,
            name='department',
            value=[
                    {
                        "departmentGroup": {
                        "uri": "{{ dag_run.conf.departmenturi }}",
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null
                        },
                        "effectiveDate": null
                    }
            ]
        )

        log_department_not_available=rail.SetVariableOperator(
            task_id='log_department_not_available',
            append=True,
            name='{{ result("create_exception_logger").name }}',
            value={
                "log": "Department {{ dag_run.conf.department }} not available/is diabled in Replicon."
            }
        )

        if_employeetype_uri_present=rail.IfOperator(
            task_id='if_employeetype_uri_present',
            test='''{{ dag_run.conf.employeetypeuri | is_truthy  and dag_run.conf.employeetypeuri != 'NA' }}''',
            yes_task="update_employeetype_variable",
            no_task="log_employeetype_not_available",
        )

        update_employeetype_variable=rail.SetVariableOperator(
            task_id='update_employeetype_variable',
            append=False,
            name='employeetype',
            value=[
                    {
                        "employeeTypeGroup": {
                        "uri": "{{ dag_run.conf.employeetypeuri }}",
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null
                        },
                        "effectiveDate": null
                    }
            ]
        )

        log_employeetype_not_available=rail.SetVariableOperator(
            task_id='log_employeetype_not_available',
            append=True,
            name='{{ result("create_exception_logger").name }}',
            value={
                "log": "Employee Type {{ dag_run.conf.employeetype }} not available/is disabled in Replicon"
            }
        )

        get_exception_logs=rail.PythonOperator(
            task_id='get_exception_logs',
            python_callable = lambda: ','.join([ log['log'] for log in rail.get_dag_run_var('exceptionlogger')]) if
                                rail.get_dag_run_var('exceptionlogger') else False
        )

        if_exception_logs_present=rail.IfOperator(
            task_id='if_exception_logs_present',
            test='''{{result('get_exception_logs') | is_truthy }}''',
            yes_task="loguser_not_created",
            no_task="get_all_policy_sets",
        )

        loguser_not_created=rail.WriteLogOperator(
            task_id='loguser_not_created',
            log="{{ dag_run.conf.logslookuptable}}",
            message="na",
            severity="Exception",
            properties={
                "employeeid": "{{ dag_run.conf.loginname }}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "status": "Exception",
                "action": "Add",
                "details": "User not created due to following reason/s:" + "{{result('get_exception_logs')}}",
                "jobid": "{{dag_run.conf.callerjobid}}",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        get_all_policy_sets=rail.RepliconServiceOperator(
            task_id='get_all_policy_sets',
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
        )

        get_all_holiday_calendars=rail.RepliconServiceOperator(
            task_id='get_all_holiday_calendars',
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
        )

        get_all_approval_paths=rail.RepliconServiceOperator(
            task_id='get_all_approval_paths',
            endpoint="/services/TimesheetApprovalService1.svc/GetAllApprovalPaths",
        )

        get_all_time_zones=rail.RepliconServiceOperator(
            task_id='get_all_time_zones',
            endpoint="/services/InternationalizationService1.svc/GetAllTimeZones",
        )

        get_all_office_schedules=rail.RepliconServiceOperator(
            task_id='get_all_office_schedules',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
        )

        create_policy_list=rail.SetVariableOperator(
            task_id='create_policy_list',
            append=False,
            name='policylist',
            value=[]
        )

        log_timesheettemplate=rail.PythonOperator(
            task_id='log_timesheettemplate',
            python_callable=lambda: get_value_from_mapper('Timesheet Template')
        )

        if_timesheettemplate_present=rail.IfOperator(
            task_id='if_timesheettemplate_present',
            test='''{{ result('log_timesheettemplate') | is_truthy }}''',
            yes_task="get_required_timesheettemplate",
            no_task="log_timeoff_template",
        )

        get_required_timesheettemplate=rail.PythonOperator(
            task_id='get_required_timesheettemplate',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr(
                                rail.result('get_all_policy_sets'),'displayText',rail.result('log_timesheettemplate'),'uri','')
        )

        if_required_timesheettemplate_present=rail.IfOperator(
            task_id='if_required_timesheettemplate_present',
            test='''{{ result('get_required_timesheettemplate') | is_truthy }}''',
            yes_task="add_timesheettemplate_to_policylist",
            no_task="log_timesheettemplate_not_updated",
        )

        add_timesheettemplate_to_policylist=rail.SetVariableOperator(
            task_id='add_timesheettemplate_to_policylist',
            append=True,
            name='{{ result("create_policy_list").name }}',
            value={
                "uri": "{{ result('get_required_timesheettemplate') }}",
                "name": null
            }
        )

        log_timesheettemplate_not_updated=rail.SetVariableOperator(
            task_id='log_timesheettemplate_not_updated',
            append=True,
            name='{{ result("create_exception_logger").name }}',
            value={
                "log": "Timesheet template not updated since {{ result('log_timesheettemplate') }} not available in Replicon"
            }
        )

        log_timeoff_template=rail.PythonOperator(
            task_id='log_timeoff_template',
            python_callable=lambda: get_value_from_mapper('Time off Template')
        )

        if_timeoff_template_present=rail.IfOperator(
            task_id='if_timeoff_template_present',
            test='''{{ result('log_timeoff_template') | is_truthy }}''',
            yes_task="add_timeofftemplate_to_policylist",
            no_task="log_policiestoassign",
        )

        add_timeofftemplate_to_policylist=rail.SetVariableOperator(
            task_id='add_timeofftemplate_to_policylist',
            append=True,
            name='{{ result("create_policy_list").name }}',
            value={
                "uri": null,
                "name": "{{ result('log_timeoff_template') }}"
            }
        )

        log_policiestoassign=rail.PythonOperator(
            task_id='log_policiestoassign',
            python_callable=lambda: rail.get_dag_run_var('policylist') if rail.get_dag_run_var('policylist')[0]['uri'] else null
        )

        log_holiday_calendar=rail.PythonOperator(
            task_id='log_holiday_calendar',
            python_callable=lambda: get_value_from_mapper('Holiday Calendar')
        )

        if_holiday_calendar_present=rail.IfOperator(
            task_id='if_holiday_calendar_present',
            test='''{{ result('log_holiday_calendar') | is_truthy }}''',
            yes_task="get_required_holiday_calendar_name",
            no_task="create_timezone_variable",
        )

        get_required_holiday_calendar_name=rail.PythonOperator(
            task_id='get_required_holiday_calendar_name',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                                rail.result('get_all_holiday_calendars'),'displayText',rail.result('log_holiday_calendar'),'uri','')
        )

        if_required_calendarname_present=rail.IfOperator(
            task_id='if_required_calendarname_present',
            test='''{{ result('get_required_holiday_calendar_name') | is_truthy }}''',
            yes_task="update_holidaycalendar_variable",
            no_task="log_holidaycalendar_not_available",
        )

        update_holidaycalendar_variable=rail.SetVariableOperator(
            task_id='update_holidaycalendar_variable',
            append=False,
            name='Holidaycalendar',
            value={
                "uri": "{{ result('get_required_holiday_calendar_name') }}",
                "name": null
            }
        )

        log_holidaycalendar_not_available=rail.SetVariableOperator(
            task_id='log_holidaycalendar_not_available',
            append=True,
            name='{{ result("create_exception_logger").name }}',
            value={
                "log": "Holdiay calendar not assigned since {{ result('log_holiday_calendar') }} not available in Replicon"
            }
        )

        create_timezone_variable=rail.SetVariableOperator(
            task_id='create_timezone_variable',
            append=False,
            name='timezone',
            value=None
        )

        get_timezone=rail.PythonOperator(
            task_id='get_timezone',
            python_callable=lambda: get_value_from_mapper('Time Zone')
        )

        if_timezone_present=rail.IfOperator(
            task_id='if_timezone_present',
            test='''{{ result('get_timezone') | is_truthy }}''',
            yes_task="update_timezone_variable",
            no_task="update_variable_timezone",
        )

        update_timezone_variable=rail.SetVariableOperator(
            task_id='update_timezone_variable',
            append=False,
            name='{{ result("create_timezone_variable").name }}',
            value={
                "uri": "{{ result('get_timezone') }}",
                "IANAName": null
            }
        )

        update_variable_timezone=rail.SetVariableOperator(
            task_id='update_variable_timezone',
            append=False,
            name='{{ result("create_timezone_variable").name }}',
            value=null
        )

        get_work_week=rail.PythonOperator(
            task_id='get_work_week',
            python_callable=lambda: get_value_from_mapper('workweek')
        )

        if_workweek_present=rail.IfOperator(
            task_id='if_workweek_present',
            test='''{{ result('get_work_week') | is_truthy }}''',
            yes_task="update_workweek_variable",
            no_task="get_timesheetapproval_path",
        )

        update_workweek_variable=rail.SetVariableOperator(
            task_id='update_workweek_variable',
            append=False,
            name='workweek',
            value="{{ result('get_work_week') }}"
        )

        get_timesheetapproval_path=rail.PythonOperator(
            task_id='get_timesheetapproval_path',
            python_callable=lambda: get_value_from_mapper('Timesheet Approval Path')
        )

        if_timesheetapproval_path_present=rail.IfOperator(
            task_id='if_timesheetapproval_path_present',
            test='''{{ result('get_timesheetapproval_path') | is_truthy }}''',
            yes_task="get_required_timesheetapproval_path",
            no_task="get_timeoff_approval_path",
        )

        get_required_timesheetapproval_path=rail.PythonOperator(
            task_id='get_required_timesheetapproval_path',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                                rail.result('get_all_approval_paths'),'displayText',rail.result('get_timesheetapproval_path'),'uri','')
        )

        if_required_timesheetapproval_path_present=rail.IfOperator(
            task_id='if_required_timesheetapproval_path_present',
            test='''{{ result('get_required_timesheetapproval_path') | is_truthy }}''',
            yes_task="update_timesheetapprovalpath_variable",
            no_task="log_timesheetapproval_path_not_available",
        )

        update_timesheetapprovalpath_variable=rail.SetVariableOperator(
            task_id='update_timesheetapprovalpath_variable',
            append=False,
            name='timesheetapprovalpath',
            value={
                "uri": "{{ result('get_required_timesheetapproval_path') }}",
                "name": null
            }
        )

        log_timesheetapproval_path_not_available=rail.SetVariableOperator(
            task_id='log_timesheetapproval_path_not_available',
            append=True,
            name='{{ result("create_exception_logger").name }}',
            value={
                "log": "Timesheet Approval Path {{ result('get_timesheetapproval_path') }} not available in Replicon"
            }
        )

        get_timeoff_approval_path=rail.PythonOperator(
            task_id='get_timeoff_approval_path',
            python_callable=lambda: get_value_from_mapper('Timeoff Approval path')
        )

        if_timeoff_approval_path_present=rail.IfOperator(
            task_id='if_timeoff_approval_path_present',
            test='''{{ result('get_timeoff_approval_path') | is_truthy }}''',
            yes_task="update_timoffapprovalpath_variable",
            no_task="get_timesheet_period",
        )

        update_timoffapprovalpath_variable=rail.SetVariableOperator(
            task_id='update_timoffapprovalpath_variable',
            append=False,
            name='timeoffapprovalpath',
            value={
                "uri": null,
                "name": "{{ result('get_timeoff_approval_path') }}"
            }
        )

        get_timesheet_period=rail.PythonOperator(
            task_id='get_timesheet_period',
            python_callable=lambda: get_value_from_mapper('Timesheet period')
        )

        if_timesheet_period_present=rail.IfOperator(
            task_id='if_timesheet_period_present',
            test='''{{ result('get_timesheet_period') | is_truthy }}''',
            yes_task="get_page_of_timesheet_periods_by_search_parameter",
            no_task="get_schedule",
        )

        get_page_of_timesheet_periods_by_search_parameter=rail.RepliconServiceOperator(
            task_id='get_page_of_timesheet_periods_by_search_parameter',
            endpoint="/services/TimesheetPeriodService2.svc/GetPageOfTimesheetPeriodsBySearchParameter",
            data={
                "page": "1",
                "pageSize": "1000",
                "timesheetPeriodSearch": {
                    "statusOptionUri": null,
                    "textSearch": {
                    "queryText": "{{ result('get_timesheet_period') }}",
                    "searchInDisplayText": "true",
                    "searchInName": "true",
                    "searchInDescription": "false"
                    }
                }
            }
        )

        get_required_timesheetperiod_to_assign=rail.PythonOperator(
            task_id='get_required_timesheetperiod_to_assign',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                                rail.result('get_page_of_timesheet_periods_by_search_parameter'),'displayText',rail.result('get_timesheet_period'),'uri','')
        )

        if_required_timesheetperiod_present=rail.IfOperator(
            task_id='if_required_timesheetperiod_present',
            test='''{{ result('get_required_timesheetperiod_to_assign') | is_truthy }}''',
            yes_task="update_timesheetperiod_variable",
            no_task="log_timesheetperiod_not_available",
        )

        update_timesheetperiod_variable=rail.SetVariableOperator(
            task_id='update_timesheetperiod_variable',
            append=False,
            name='timesheetperiod',
            value=[
                {
                    "timesheetPeriod": {
                    "uri": "{{ result('get_required_timesheetperiod_to_assign') }}",
                    "name": null
                    },
                    "effectiveDate": null
                }
            ]
        )

        log_timesheetperiod_not_available=rail.SetVariableOperator(
            task_id='log_timesheetperiod_not_available',
            append=True,
            name='{{ result("create_exception_logger").name }}',
            value={
                "log": "Timesheet period {{ result('get_timesheet_period') }} not available in Replicon"
            }
        )

        get_schedule=rail.PythonOperator(
            task_id='get_schedule',
            python_callable=lambda: get_value_from_mapper('schedule')
        )

        if_schedule_present=rail.IfOperator(
            task_id='if_schedule_present',
            test='''{{ result('get_schedule') | is_truthy }}''',
            yes_task="get_required_office_schedule",
            no_task="get_custom_field_values",
        )

        get_required_office_schedule=rail.PythonOperator(
            task_id='get_required_office_schedule',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                                rail.result('get_all_office_schedules'),'displayText',rail.result('get_schedule'),'uri','')
        )

        if_required_office_schedule_present=rail.IfOperator(
            task_id='if_required_office_schedule_present',
            test='''{{ result('get_required_office_schedule') | is_truthy }}''',
            yes_task="update_schedule_variable",
            no_task="log_schedule_not_available",
        )

        update_schedule_variable=rail.SetVariableOperator(
            task_id='update_schedule_variable',
            append=False,
            name='schedule',
            value=[
                    {
                        "schedulePolicy": {
                        "officeScheduleUri": "{{ result('get_required_office_schedule') }}",
                        "name": null,
                        "officeSchedule": {
                            "officeScheduleUri": "{{ result('get_required_office_schedule') }}",
                            "name": null
                        },
                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                        },
                        "effectiveDate": null
                    }
            ]
        )

        log_schedule_not_available=rail.SetVariableOperator(
            task_id='log_schedule_not_available',
            append=True,
            name='{{ result("create_exception_logger").name }}',
            value={
                "log": "Schedule {{ result('get_schedule') }} not available in Replicon"
            }
        )

        get_custom_field_values=rail.PythonOperator(
            task_id='get_custom_field_values',
            python_callable=lambda: rail.get_dag_run_var('customfields') if len(rail.get_dag_run_var('customfields')) > 0 else []
        )

        def get_date_dictionary(datestring):
            date = datetime.strptime(datestring,'%d/%m/%Y')
            return {
                'day':date.day,
                'month':date.month,
                'year':date.year
            }

        get_startdate_for_user=rail.PythonOperator(
            task_id='get_startdate_for_user',
            python_callable= lambda dag_run: get_date_dictionary(dag_run.conf['startdate'])
        )

        if_enabled_status_is_yes=rail.IfOperator(
            task_id='if_enabled_status_is_yes',
            test='''{{ dag_run.conf.enabled.lower()=='yes' }}''',
            yes_task="update_employeestatus_variable",
            no_task="create_user",
        )

        update_employeestatus_variable=rail.SetVariableOperator(
            task_id='update_employeestatus_variable',
            append=False,
            name='employeestatus',
            value=True
        )

        create_user=rail.RepliconServiceOperator(
            task_id='create_user',
            endpoint="/services/importservice1.svc/PutUser3",
            data=lambda dag_run: {
                "user": {
                    "target": {
                    "uri": null,
                    "loginName": dag_run.conf['loginname'],
                    "parameterCorrelationId": null
                    },
                    "firstname": dag_run.conf['firstname'],
                    "lastname": dag_run.conf['lastname'],
                    "emailAddress": rail.get_dag_run_var('email'),
                    "employeeId": dag_run.conf['employeeId'],
                    "department": null,
                    "supervisorAssignmentSchedule": null,
                    "schedulePolicySchedule": rail.get_dag_run_var('schedule'),
                    "workWeekStartDayUri": rail.get_dag_run_var('workweek'),
                    "employmentDateRange": {
                    "startDate": {
                        "year": rail.result('get_startdate_for_user')['year'],
                        "month": rail.result('get_startdate_for_user')['month'],
                        "day": rail.result('get_startdate_for_user')['day']
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                    },
                    "securityConfiguration": {
                    "enabledAuthenticationTypeUris": [
                        "urn:replicon:user-authentication-type:sso"
                    ],
                    "isLoginEnabled": rail.get_dag_run_var('employeestatus'),
                    "loginName": dag_run.conf['loginname'],
                    "SSOName": dag_run.conf['loginname'],
                    "password": null
                    },
                    "holidayCalendar": rail.get_dag_run_var('Holidaycalendar'),
                    "timeOffPolicy": null,
                    "permissionSets": [
                    {
                        "uri": null,
                        "name": rail.result('log_permissiontoassign')
                    }
                    ],
                    "policySets": rail.result('log_policiestoassign'),
                    "employeeType": null,
                    "timesheetPeriodTypeUri": null,
                    "costRateSchedule": null,
                    "payrollRateSchedule": null,
                    "defaultBillingRate": null,
                    "timesheetApprovalPath": rail.get_dag_run_var('timesheetapprovalpath'),
                    "expenseApprovalPath": null,
                    "timeOffApprovalPath": rail.get_dag_run_var('timeoffapprovalpath'),
                    "customFieldValues": rail.result('get_custom_field_values'),
                    "assignedActivities": [],
                    "timeZone": rail.get_dag_run_var('timezone'),
                    "overtimeRuleAssignmentSchedule": null,
                    "validationRuleAssignmentSchedule": null,
                    "locationSchedule": rail.get_dag_run_var('location'),
                    "divisionSchedule": rail.get_dag_run_var('division'),
                    "costCenterSchedule": null,
                    "serviceCenterSchedule": null,
                    "departmentGroupSchedule": rail.get_dag_run_var('department'),
                    "employeeTypeGroupSchedule": rail.get_dag_run_var('employeetype'),
                    "timesheetPeriodSchedule": rail.get_dag_run_var('timesheetperiod'),
                    "policyDataAccessScopes": [],
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": []
                }
            }
        )

        remove_timeoff_assignments=rail.RepliconServiceOperator(
            task_id='remove_timeoff_assignments',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "timeOffTypeUris": []
            }
        )

        if_supervisor_not_present=rail.IfOperator(
            task_id='if_supervisor_not_present',
            test='''{{ dag_run.conf.supervisor | is_falsy}}''',
            yes_task="log_supervisor_not_assigned",
            no_task="get_userid",
        )

        log_supervisor_not_assigned=rail.SetVariableOperator(
            task_id='log_supervisor_not_assigned',
            append=True,
            name='{{ result("create_exception_logger").name }}',
            value={
                "log": "Supervisor not assigned since the Supervisor ID is not provided"
            }
        )

        get_userid=rail.PythonOperator(
            task_id='get_userid',
            python_callable= lambda dag_run: dag_run.conf['employeeId'] if ('employee_id' in dag_run.conf['identifier']) else dag_run.conf['loginname']
        )

        if_supervisorid_equal_userid=rail.IfOperator(
            task_id='if_supervisorid_equal_userid',
            test=lambda dag_run: dag_run.conf['supervisor'] == rail.result('get_userid'),
            yes_task="log_supervisornot_assigned",
            no_task="search_supervisor_user_by_id",
        )

        log_supervisornot_assigned=rail.SetVariableOperator(
            task_id='log_supervisornot_assigned',
            append=True,
            name='{{ result("create_exception_logger").name }}',
            value={
                "log": "Supervisor not assigned since the Supervisor ID and user  ID are the same."
            }
        )

        search_supervisor_user_by_id=rail.RepliconServiceOperator(
            task_id='search_supervisor_user_by_id',
            endpoint="/services/UserListService1.svc/GetData",
            data={
              "page": "1",
              "pagesize": "100",
              "columnUris": [
                  "urn:replicon:user-list-column:login-name",
                  "urn:replicon:user-list-column:employee-id",
                  "urn:replicon:user-list-column:enabled"
              ],
              "sort": [],
              "filterExpression": {
                  "leftExpression": {
                      "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                  },
                  "operatorUri": "urn:replicon:filter-operator:text-search",
                  "rightExpression": {
                      "value": {
                          "text": "{{dag_run.conf.supervisor}}"
                      }
                  }
              }
            },
            data_handler=lambda response,dag_run: list(filter(lambda x: x['cells'][1]['textValue'] == dag_run.conf['supervisor'],response['rows']))
        )

        if_multiple_profiles_found=rail.IfOperator(
            task_id='if_multiple_profiles_found',
            test=lambda: bool(rail.result('search_supervisor_user_by_id') and len(rail.result('search_supervisor_user_by_id')) > 1),
            yes_task="log_multiple_profiles_found",
            no_task="get_supervisor_uri",
        )

        log_multiple_profiles_found=rail.SetVariableOperator(
            task_id='log_multiple_profiles_found',
            append=True,
            name='{{ result("create_exception_logger").name }}',
            value={
                "log": "Supervisor not assigned since the multiple profiles found with same Supervisor ID {{ dag_run.conf.supervisor }}"
            }
        )

        get_supervisor_uri=rail.PythonOperator(
            task_id='get_supervisor_uri',
            python_callable= lambda: rail.result('search_supervisor_user_by_id')[0]['cells'][0]['uri'] if
                                rail.result('search_supervisor_user_by_id') and
                                rail.result('search_supervisor_user_by_id')[0]['cells'][0]['textValue'] else null
        )

        if_supervisor_uri_present=rail.IfOperator(
            task_id='if_supervisor_uri_present',
            test='''{{ result('get_supervisor_uri') | is_truthy }}''',
            yes_task="get_supervisor_status",
            no_task="if_supervisoruri_present",
        )

        get_supervisor_status=rail.PythonOperator(
            task_id='get_supervisor_status',
            python_callable= lambda: rail.result('search_supervisor_user_by_id')[0]['cells'][2]['textValue']
        )

        if_supervisoruri_present=rail.IfOperator(
            task_id='if_supervisoruri_present',
            test='''{{ result('get_supervisor_uri') | is_truthy }}''',
            yes_task="if_supervisor_status_equals_true",
            no_task="add_entry_supervisor_assignment_queued",
        )

        if_supervisor_status_equals_true=rail.IfOperator(
            task_id='if_supervisor_status_equals_true',
            test='''{{ result('get_supervisor_status') == 'True' }}''',
            yes_task="get_permissionsets_for_supervisoruser",
            no_task="add_entry_supervisor_assignment_queued",
        )

        get_permissionsets_for_supervisoruser=rail.RepliconServiceOperator(
            task_id='get_permissionsets_for_supervisoruser',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('get_supervisor_uri') }}"
            }
        )

        get_supervision_permissionset_for_user=rail.PythonOperator(
            task_id='get_supervision_permissionset_for_user',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr(
                                rail.result('get_permissionsets_for_supervisoruser'),'policyUri','urn:replicon:policy:supervision','permissionSet.name','')
                                if rail.result('get_permissionsets_for_supervisoruser') else null
        )

        if_supervisor_permission_not_present=rail.IfOperator(
            task_id='if_supervisor_permission_not_present',
            test='''{{ result('get_supervision_permissionset_for_user') | is_falsy }}''',
            yes_task="assign_supervisor_permission",
            no_task="assign_initial_supervisor",
        )

        assign_supervisor_permission=rail.RepliconServiceOperator(
            task_id='assign_supervisor_permission',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('get_supervisor_uri') }}",
                "permissionSetUri": "{{ dag_run.conf.supervisorpermissionuri }}"
            }
        )

        assign_initial_supervisor=rail.RepliconServiceOperator(
            task_id='assign_initial_supervisor',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "supervisorUri": "{{ result('get_supervisor_uri') }}",
                "dateRange": null
            }
        )

        add_entry_supervisor_assignment_queued=rail.WriteLogOperator(
            task_id='add_entry_supervisor_assignment_queued',
            log="{{ dag_run.conf.supervisorlookuptable }}",
            message="na",
            severity="queued",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "username": "{{ dag_run.conf.employeeId }}",
                "useruri": "{{ result('create_user').uri }}",
                "supervisorloginname": "{{ dag_run.conf.supervisor }}",
                "action": "add",
                "status": "queued",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_customfield_present_and_type_equals_dropdown=rail.IfOperator(
            task_id='if_customfield_present_and_type_equals_dropdown',
            test="{{ dag_run.conf.customfield1 | is_truthy and dag_run.conf.customfield1_uri | is_truthy  and dag_run.conf.customfiled1type == 'dropdown' }}",
            yes_task="get_enabled_custom_field_drop_down_options",
            no_task="get_enabled_time_off_types",
        )

        get_enabled_custom_field_drop_down_options=rail.RepliconServiceOperator(
            task_id='get_enabled_custom_field_drop_down_options',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ dag_run.conf.customfield1_uri }}"
            }
        )

        log_dropdownoptionvalue=rail.PythonOperator(
            task_id='log_dropdownoptionvalue',
            python_callable= lambda dag_run: rail.find_first_by_attr_and_get_attr(
                                rail.result('get_enabled_custom_field_drop_down_options'),'displayText', dag_run.conf['customfield1'],'uri','')
        )

        if_dropdownoption_value_present=rail.IfOperator(
            task_id='if_dropdownoption_value_present',
            test='''{{ result('log_dropdownoptionvalue') | is_truthy }}''',
            yes_task="update_udf_for_customfield1",
            no_task="log_dropdownoption_not_available",
        )

        update_udf_for_customfield1=rail.RepliconServiceOperator(
            task_id='update_udf_for_customfield1',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ dag_run.conf.customfield1_uri }}",
                "customFieldDropDownOptionUri": "{{ result('log_dropdownoptionvalue') }}"
            }
        )

        log_dropdownoption_not_available=rail.SetVariableOperator(
            task_id='log_dropdownoption_not_available',
            append=True,
            name='{{ result("create_exception_logger").name }}',
            value={
                "log": "Dropdown option {{ dag_run.conf.customfield1 }} not available in Replicon"
            }
        )

        get_enabled_time_off_types=rail.RepliconServiceOperator(
            task_id='get_enabled_time_off_types',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
        )

        def get_time_off_uris():
            timeoffs = rail.result('get_enabled_time_off_types')
            uris = []
            uris = [ timeoff['uri'] for timeoff in timeoffs if not timeoff['displayText'].startswith('MY') ]
            return uris

        get_required_timeoff_uris = rail.PythonOperator(
            task_id = 'get_required_timeoff_uris',
            python_callable=get_time_off_uris
        )

        if_required_timeoff_uris_present=rail.IfOperator(
            task_id='if_required_timeoff_uris_present',
            test='''{{ result('get_required_timeoff_uris') | is_truthy }}''',
            yes_task="put_time_off_type_assignments_for_user",
            no_task="generate_and_add_log_for_user",
        )

        put_time_off_type_assignments_for_user=rail.RepliconServiceOperator(
            task_id='put_time_off_type_assignments_for_user',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda:{
                "userUri": rail.result('create_user')['uri'],
                "timeOffTypeUris": rail.result('get_required_timeoff_uris')
            }
        )

        generate_and_add_log_for_user=rail.WriteLogOperator(
            task_id='generate_and_add_log_for_user',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity=lambda: "Exception" if rail.get_dag_run_var('exceptionlogger') else "Success",
            properties=lambda dag_run:{
                "employeeid": dag_run.conf['loginname'],
                "username": dag_run.conf['firstname'] + dag_run.conf['lastname'],
                "status": "Exception" if rail.get_dag_run_var('exceptionlogger') else "Success",
                "action": "Add",
                "details": ('User created with exception,' + ','.join([item['log'] for item in rail.get_dag_run_var('exceptionlogger')]))
                    if rail.get_dag_run_var('exceptionlogger') else 'User created successfully',
                "jobid": dag_run.conf['callerjobid'],
                "childjobid": rail.render_template("{{dag_run_ecid()}}")
            }
        )

        catch_and_log_error=rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="Error",
            properties={
                "employeeid": "{{ dag_run.conf.loginname }}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "status": "Error",
                "action": "Add",
                "details": "{{get_error_message()}}",
                "jobid": "{{dag_run.conf.callerjobid}}",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> create_exception_logger
        create_exception_logger >> get_error_message_for_log >> if_error_message_present
        if_error_message_present >> rail.Label('Yes')  >> log_user_not_created >> catch_and_log_error
        if_error_message_present >> rail.Label('No') >> if_enabled_status_is_not_yes
        if_enabled_status_is_not_yes >> rail.Label('Yes')  >> log_user_notcreated_due_to_enabled_not_yes >> catch_and_log_error
        if_enabled_status_is_not_yes >> rail.Label(
            'No') >> declare_customfields_list >> create_division_variable >> log_locationtoassign >> log_permissiontoassign
        log_permissiontoassign >> log_authenticationtype >> if_log_locationtoassign_present
        if_log_locationtoassign_present >> rail.Label('Yes')  >> update_location_variable >> if_valid_email_present
        if_log_locationtoassign_present >> rail.Label('No') >> if_valid_email_present
        if_valid_email_present >> rail.Label('Yes')  >> update_email_variable >> if_department_uri_present
        if_valid_email_present >> rail.Label('No') >> log_email_not_updated >> if_department_uri_present
        if_department_uri_present >> rail.Label('Yes')  >> update_department_variable >> if_employeetype_uri_present
        if_department_uri_present >> rail.Label('No') >> log_department_not_available >> if_employeetype_uri_present
        if_employeetype_uri_present >> rail.Label('Yes')  >> update_employeetype_variable >> get_exception_logs
        if_employeetype_uri_present >> rail.Label('No') >> log_employeetype_not_available >> get_exception_logs >> if_exception_logs_present
        if_exception_logs_present >> rail.Label('Yes')  >> loguser_not_created >> catch_and_log_error
        if_exception_logs_present >> rail.Label('No') >> get_all_policy_sets >> get_all_holiday_calendars >> get_all_approval_paths >> get_all_time_zones
        get_all_time_zones >> get_all_office_schedules >> create_policy_list >> log_timesheettemplate >> if_timesheettemplate_present
        if_timesheettemplate_present >> rail.Label('Yes') >> get_required_timesheettemplate >> if_required_timesheettemplate_present
        if_timesheettemplate_present >> rail.Label('No') >> log_timeoff_template
        if_required_timesheettemplate_present >> rail.Label('Yes')  >> add_timesheettemplate_to_policylist >> log_timeoff_template
        if_required_timesheettemplate_present >> rail.Label('No') >> log_timesheettemplate_not_updated >> log_timeoff_template >> if_timeoff_template_present
        if_timeoff_template_present >> rail.Label('Yes')  >> add_timeofftemplate_to_policylist >> log_policiestoassign
        if_timeoff_template_present >> rail.Label(
            'No') >> log_policiestoassign >> log_holiday_calendar >> if_holiday_calendar_present
        if_holiday_calendar_present >> rail.Label('Yes') >> get_required_holiday_calendar_name >> if_required_calendarname_present
        if_holiday_calendar_present >> rail.Label('No') >> create_timezone_variable
        if_required_calendarname_present >> rail.Label('Yes')  >> update_holidaycalendar_variable >> create_timezone_variable
        if_required_calendarname_present >> rail.Label(
            'No') >> log_holidaycalendar_not_available >> create_timezone_variable >> get_timezone >> if_timezone_present
        if_timezone_present >> rail.Label('Yes')  >> update_timezone_variable >> get_work_week
        if_timezone_present >> rail.Label('No') >> update_variable_timezone >> get_work_week >> if_workweek_present
        if_workweek_present >> rail.Label('Yes')  >> update_workweek_variable >> get_timesheetapproval_path
        if_workweek_present >> rail.Label('No') >> get_timesheetapproval_path >> if_timesheetapproval_path_present
        if_timesheetapproval_path_present >> rail.Label('Yes')  >> get_required_timesheetapproval_path >> if_required_timesheetapproval_path_present
        if_timesheetapproval_path_present >> rail.Label('No')  >> get_timeoff_approval_path
        if_required_timesheetapproval_path_present >> rail.Label('Yes')  >> update_timesheetapprovalpath_variable >> get_timeoff_approval_path
        if_required_timesheetapproval_path_present >> rail.Label(
            'No') >> log_timesheetapproval_path_not_available >> get_timeoff_approval_path
        get_timeoff_approval_path >> if_timeoff_approval_path_present >> rail.Label('Yes')  >> update_timoffapprovalpath_variable >> get_timesheet_period
        if_timeoff_approval_path_present >> rail.Label('No') >> get_timesheet_period >> if_timesheet_period_present
        if_timesheet_period_present >> rail.Label(
            'Yes')  >> get_page_of_timesheet_periods_by_search_parameter >> get_required_timesheetperiod_to_assign >> if_required_timesheetperiod_present
        if_timesheet_period_present >> rail.Label('No')  >> get_schedule
        if_required_timesheetperiod_present >> rail.Label('Yes')  >> update_timesheetperiod_variable >> get_schedule
        if_required_timesheetperiod_present >> rail.Label(
            'No') >> log_timesheetperiod_not_available >> get_schedule >> if_schedule_present
        if_schedule_present >> rail.Label('Yes')  >> get_required_office_schedule >> if_required_office_schedule_present
        if_required_office_schedule_present >> rail.Label('Yes')  >> update_schedule_variable >> get_custom_field_values
        if_required_office_schedule_present >> rail.Label('No') >> log_schedule_not_available >> get_custom_field_values
        if_schedule_present >> rail.Label(
            'No') >> get_custom_field_values >> get_startdate_for_user >> if_enabled_status_is_yes
        if_enabled_status_is_yes >> rail.Label('Yes')  >> update_employeestatus_variable >> create_user
        if_enabled_status_is_yes >> rail.Label('No') >> create_user >> remove_timeoff_assignments >> if_supervisor_not_present
        if_supervisor_not_present >> rail.Label('Yes')  >> log_supervisor_not_assigned >> if_customfield_present_and_type_equals_dropdown
        if_supervisor_not_present >> rail.Label('No') >> get_userid >> if_supervisorid_equal_userid
        if_supervisorid_equal_userid >> rail.Label('Yes')  >> log_supervisornot_assigned >> if_customfield_present_and_type_equals_dropdown
        if_supervisorid_equal_userid >> rail.Label('No') >> search_supervisor_user_by_id >> if_multiple_profiles_found
        if_multiple_profiles_found >> rail.Label('Yes')  >> log_multiple_profiles_found >> if_customfield_present_and_type_equals_dropdown
        if_multiple_profiles_found >> rail.Label('No') >> get_supervisor_uri >> if_supervisor_uri_present
        if_supervisor_uri_present >> rail.Label('Yes')  >> get_supervisor_status >> if_supervisoruri_present
        if_supervisor_uri_present >> rail.Label('No') >> if_supervisoruri_present
        if_supervisoruri_present >> rail.Label('Yes') >> if_supervisor_status_equals_true
        if_supervisoruri_present >> rail.Label('No') >> add_entry_supervisor_assignment_queued >> if_customfield_present_and_type_equals_dropdown
        if_supervisor_status_equals_true >> rail.Label(
            'Yes')  >> get_permissionsets_for_supervisoruser >> get_supervision_permissionset_for_user >> if_supervisor_permission_not_present
        if_supervisor_permission_not_present >> rail.Label('Yes')  >> assign_supervisor_permission >> assign_initial_supervisor
        if_supervisor_permission_not_present >> rail.Label('No') >> assign_initial_supervisor >> if_customfield_present_and_type_equals_dropdown
        if_supervisor_status_equals_true >> rail.Label('No') >> add_entry_supervisor_assignment_queued >> if_customfield_present_and_type_equals_dropdown
        if_customfield_present_and_type_equals_dropdown >> rail.Label(
            'Yes') >> get_enabled_custom_field_drop_down_options >> log_dropdownoptionvalue >> if_dropdownoption_value_present
        if_customfield_present_and_type_equals_dropdown >> rail.Label('Yes') >> get_enabled_time_off_types
        if_dropdownoption_value_present >> rail.Label('Yes')  >> update_udf_for_customfield1 >> get_enabled_time_off_types
        if_dropdownoption_value_present >> rail.Label('No') >> log_dropdownoption_not_available >> get_enabled_time_off_types >> get_required_timeoff_uris
        get_required_timeoff_uris >> if_required_timeoff_uris_present
        if_required_timeoff_uris_present >> rail.Label('Yes')  >> put_time_off_type_assignments_for_user >> generate_and_add_log_for_user
        if_required_timeoff_uris_present >> rail.Label('No') >> generate_and_add_log_for_user >> catch_and_log_error >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
