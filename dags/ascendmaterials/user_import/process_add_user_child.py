from datetime import timedelta
from airflow.models import Variable
import rail
from ascendmaterials.user_import.mappers.ascend_master_mapper_file_mapper import ascend_master_mapper_file
from ascendmaterials.user_import.utils import python_callable, request_payload, response_filter

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.add_user_dag_id,
        description=f'Ascend_Child_Add User {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_seconday_child,
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
            no_task='if_enabled_ne_yes'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_enabled_ne_yes',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_enabled_ne_yes = rail.IfOperator(
            task_id='if_enabled_ne_yes',
            test='''{{ dag_run.conf["enabled"].lower() != 'yes' }}''',
            yes_task="log_entry_1",
            no_task="if_employeefirstname_blank_yes",
        )

        log_entry_1 = rail.WriteLogOperator(
            task_id='log_entry_1',
            log='{{ dag_run.conf["ascend_user_import_logs_lookuptable"] }}',
            message="na",
            severity="Skipped",
            properties=lambda dag_run: {
                "userloginname": dag_run.conf["loginname"],
                "username": str(dag_run.conf["employeefirstname"]) + " " + str(dag_run.conf["employeelastname"]),
                "action": "Add",
                "status": "Skipped",
                "details": "" if str(dag_run.conf["enabled"]).lower().strip() == "yes" else "Enabled (User Status) is not set to yes"
            }
        )

        if_employeefirstname_blank_yes = rail.IfOperator(
            task_id='if_employeefirstname_blank_yes',
            test='''{{ dag_run.conf["employeefirstname"] | is_falsy or dag_run.conf["employeelastname"] | is_falsy }}''',
            yes_task="log_entry_2",
            no_task="if_startdate_blank",
        )

        log_entry_2 = rail.WriteLogOperator(
            task_id='log_entry_2',
            log='{{ dag_run.conf["ascend_user_import_logs_lookuptable"] }}',
            message="na",
            severity="Skipped",
            properties=lambda dag_run: {
                "userloginname": dag_run.conf["loginname"],
                "username": str(dag_run.conf["employeefirstname"]) + " " + str(dag_run.conf["employeelastname"]),
                "action": "Add",
                "status": "Skipped",
                "details": rail.smartjoin_by_delim((("First name is blank" if not dag_run.conf["employeefirstname"] else "") + ";" + ("Last name is blank" if not dag_run.conf["employeelastname"] else "")).split(";"), ";")
            }
        )

        if_startdate_blank = rail.IfOperator(
            task_id='if_startdate_blank',
            test='''{{ dag_run.conf["startdate"] | is_falsy  or dag_run.conf["startdate"] | matches('/') | is_falsy }}''',
            yes_task="log_entry_3",
            no_task="get_todaysdate",
        )

        log_entry_3 = rail.WriteLogOperator(
            task_id='log_entry_3',
            log='{{ dag_run.conf["ascend_user_import_logs_lookuptable"] }}',
            message="na",
            severity="Skipped",
            properties=lambda dag_run: {
                "userloginname": dag_run.conf["loginname"],
                "username": str(dag_run.conf["employeefirstname"]) + " " + str(dag_run.conf["employeelastname"]),
                "action": "Add",
                "status": "Skipped",
                "details": python_callable.get_detail_messgae_10(dag_run)
            }
        )

        get_todaysdate = rail.PythonOperator(
            task_id='get_todaysdate',
            python_callable=python_callable.split_todaysdate
        )

        get_start_date = rail.PythonOperator(
            task_id='get_start_date',
            python_callable=lambda dag_run: python_callable.get_datetime_obj(
                dag_run.conf["startdate"])
        )

        if_employeetype_blank = rail.IfOperator(
            task_id='if_employeetype_blank',
            test='''{{ dag_run.conf["employeetype"] | is_falsy }}''',
            yes_task="log_entry_4",
            no_task="get_all_employee_type_details",
        )

        log_entry_4 = rail.WriteLogOperator(
            task_id='log_entry_4',
            log='{{ dag_run.conf["ascend_user_import_logs_lookuptable"] }}',
            message="na",
            severity="Skipped",
            properties=lambda dag_run: {
                "userloginname": dag_run.conf.get('loginname', ''),
                "username": dag_run.conf.get('employeefirstname', '') + " " + dag_run.conf.get('employeelastname', ''),
                "action": "Add",
                "status": "Skipped",
                "details": "Employee type is not present in feed file"
            }
        )

        get_all_employee_type_details = rail.RepliconServiceOperator(
            task_id='get_all_employee_type_details',
            endpoint="/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf["employeetype"], 'uri', '')
        )

        if_employee_type_uri_18_blank = rail.IfOperator(
            task_id='if_employee_type_uri_18_blank',
            test='''{{ result('get_all_employee_type_details') | is_falsy }}''',
            yes_task="log_entry_5",
            no_task="mapper_search_entries",
        )

        log_entry_5 = rail.WriteLogOperator(
            task_id='log_entry_5',
            log='{{ dag_run.conf["ascend_user_import_logs_lookuptable"] }}',
            message="na",
            severity="Skipped",
            properties=lambda dag_run: {
                "userloginname": dag_run.conf.get('loginname', ''),
                "username": dag_run.conf.get('employeefirstname', '') + " " + dag_run.conf.get('employeelastname', ''),
                "action": "Add",
                "status": "Skipped",
                "details": dag_run.conf.get('employeetype', '') + " is not present in Replicon"
            }
        )

        mapper_search_entries = rail.PythonOperator(
            task_id='mapper_search_entries',
            python_callable=lambda dag_run:  list(filter(
                lambda x: x["location"] == dag_run.conf["location"], ascend_master_mapper_file))
        )

        get_company_department = rail.RepliconServiceOperator(
            task_id='get_company_department',
            endpoint="/services/DepartmentService1.svc/GetCompanyDepartment",
            data=None
        )

        create_user = rail.RepliconServiceOperator(
            task_id='create_user',
            endpoint="/services/importservice1.svc/PutUser2",
            data=request_payload.create_user_24_paload_data
        )

        remove_timeoffassignmentsforusers = rail.RepliconServiceOperator(
            task_id='remove_timeoffassignmentsforusers',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "timeOffTypeUris": []
            }
        )

        get_all_user_custom_fields = rail.RepliconServiceOperator(
            task_id='get_all_user_custom_fields',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            },
            data_handler=lambda response: {
                'ft_pt': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'FT/PT', 'uri', ''),
                'home_country': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Home - Country', 'uri', ''),
                'home_state': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Home - State/Province', 'uri', ''),
                'home_city': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Home - City', 'uri', ''),
                'scheduled_hours': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Scheduled Hours', 'uri', ''),
                'continuous_service_date': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Continuous Service Date', 'uri', ''),
                'recent_hire_date': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Most Recent Hire Date', 'uri', ''),
            }
        )

        if_timetype_present = rail.IfOperator(
            task_id='if_timetype_present',
            test='''{{ dag_run.conf["timetype"] | is_truthy }}''',
            yes_task="if_get_udf_uri_f_t_p_t_present",
            no_task="if_homecountry_present",
        )

        if_get_udf_uri_f_t_p_t_present = rail.IfOperator(
            task_id='if_get_udf_uri_f_t_p_t_present',
            test='''{{ result('get_all_user_custom_fields').ft_pt | is_truthy }}''',
            yes_task="get_enabled_custome_field",
            no_task="if_homecountry_present",
        )

        get_enabled_custome_field = rail.RepliconServiceOperator(
            task_id='get_enabled_custome_field',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_all_user_custom_fields').ft_pt }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf["timetype"], 'uri', ''),

        )

        if_required_udfdropdownurifor_f_t_p_t_present = rail.IfOperator(
            task_id='if_required_udfdropdownurifor_f_t_p_t_present',
            test='''{{ result('get_enabled_custome_field') | is_truthy }}''',
            yes_task="update_dropdown_valuefor_f_t_p_t",
            no_task="if_homecountry_present",
        )

        update_dropdown_valuefor_f_t_p_t = rail.RepliconServiceOperator(
            task_id='update_dropdown_valuefor_f_t_p_t',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_all_user_custom_fields').ft_pt }}",
                "customFieldDropDownOptionUri": "{{ result('get_enabled_custome_field') }}"
            }
        )

        if_homecountry_present = rail.IfOperator(
            task_id='if_homecountry_present',
            test='''{{ dag_run.conf["homecountry"] | is_truthy }}''',
            yes_task="if_get_udf_uri_home_country_present",
            no_task="if_homestateprovince_present",
        )

        if_get_udf_uri_home_country_present = rail.IfOperator(
            task_id='if_get_udf_uri_home_country_present',
            test='''{{ result('get_all_user_custom_fields').home_country | is_truthy }}''',
            yes_task="update_text_valuefor_home_country",
            no_task="if_homestateprovince_present",
        )

        update_text_valuefor_home_country = rail.RepliconServiceOperator(
            task_id='update_text_valuefor_home_country',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_all_user_custom_fields').home_country }}",
                "value": '{{ dag_run.conf["homecountry"] }}'
            }
        )

        if_homestateprovince_present = rail.IfOperator(
            task_id='if_homestateprovince_present',
            test='''{{ dag_run.conf["homestateprovince"] | is_truthy }}''',
            yes_task="if_get_udf_uri_home_state_province_present",
            no_task="if_homecity_present",
        )

        if_get_udf_uri_home_state_province_present = rail.IfOperator(
            task_id='if_get_udf_uri_home_state_province_present',
            test='''{{ result('get_all_user_custom_fields').home_state | is_truthy }}''',
            yes_task="update_text_valuefor_home_state_province",
            no_task="if_homecity_present",
        )

        update_text_valuefor_home_state_province = rail.RepliconServiceOperator(
            task_id='update_text_valuefor_home_state_province',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_all_user_custom_fields').home_state }}",
                "value": '{{ dag_run.conf["homestateprovince"] }}'
            }
        )

        if_homecity_present = rail.IfOperator(
            task_id='if_homecity_present',
            test='''{{ dag_run.conf["homecity"] | is_truthy }}''',
            yes_task="if_get_udf_uri_home_city_present",
            no_task="if_udf_present",
        )

        if_get_udf_uri_home_city_present = rail.IfOperator(
            task_id='if_get_udf_uri_home_city_present',
            test='''{{ result('get_all_user_custom_fields').home_city | is_truthy }}''',
            yes_task="update_text_valuefor_home_home_city",
            no_task="if_udf_present",
        )

        update_text_valuefor_home_home_city = rail.RepliconServiceOperator(
            task_id='update_text_valuefor_home_home_city',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_all_user_custom_fields').home_city }}",
                "value": '{{ dag_run.conf["homecity"] }}'
            }
        )

        if_udf_present = rail.IfOperator(
            task_id='if_udf_present',
            test='''{{ dag_run.conf["udf"] | is_truthy }}''',
            yes_task="if_get_udf_uri_scheduled_hours_present",
            no_task="if_continuousservicedate_present",
        )

        if_get_udf_uri_scheduled_hours_present = rail.IfOperator(
            task_id='if_get_udf_uri_scheduled_hours_present',
            test='''{{ result('get_all_user_custom_fields').scheduled_hours | is_truthy }}''',
            yes_task="update_numeric_valuefor_scheduled_hours",
            no_task="if_continuousservicedate_present",
        )

        update_numeric_valuefor_scheduled_hours = rail.RepliconServiceOperator(
            task_id='update_numeric_valuefor_scheduled_hours',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_all_user_custom_fields').scheduled_hours }}",
                "value": '{{ dag_run.conf["udf"] }}'
            }
        )

        if_continuousservicedate_present = rail.IfOperator(
            task_id='if_continuousservicedate_present',
            test='''{{ dag_run.conf["continuousservicedate"] | is_truthy }}''',
            yes_task="if_continuousservicedate_contains",
            no_task="if_get_udf_uri_most_recent_hire_date_present",
        )

        if_continuousservicedate_contains = rail.IfOperator(
            task_id='if_continuousservicedate_contains',
            test='''{{ dag_run.conf["continuousservicedate"] | matches('/') }}''',
            yes_task="if_get_udf_uri_continuous_service_date_present",
            no_task="log_messageforcontinuousservicedateerror",
        )

        if_get_udf_uri_continuous_service_date_present = rail.IfOperator(
            task_id='if_get_udf_uri_continuous_service_date_present',
            test='''{{ result('get_all_user_custom_fields').continuous_service_date | is_truthy }}''',
            yes_task="get_continuous_service_date",
            no_task="log_messageforcontinuousservicedateerror",
        )

        get_continuous_service_date = rail.PythonOperator(
            task_id='get_continuous_service_date',
            python_callable=lambda dag_run: python_callable.get_datetime_obj(
                dag_run.conf["continuousservicedate"])
        )

        update_date_valuefor_continuous_service_date = rail.RepliconServiceOperator(
            task_id='update_date_valuefor_continuous_service_date',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_all_user_custom_fields').continuous_service_date }}",
                "value": {
                    "year": "{{ result('get_continuous_service_date').year }}",
                    "month": "{{ result('get_continuous_service_date').month }}",
                    "day": "{{ result('get_continuous_service_date').day }}"
                }
            }
        )

        log_messageforcontinuousservicedateerror = rail.PythonOperator(
            task_id='log_messageforcontinuousservicedateerror',
            python_callable=lambda dag_run: f'{dag_run.conf["continuousservicedate"]} is not in the predefined format'
        )

        if_get_udf_uri_most_recent_hire_date_present = rail.IfOperator(
            task_id='if_get_udf_uri_most_recent_hire_date_present',
            test='''{{ result('get_all_user_custom_fields').recent_hire_date | is_truthy }}''',
            yes_task="update_date_valuefor_most_recent_hire_date",
            no_task="if_departmenturi_present",
        )

        update_date_valuefor_most_recent_hire_date = rail.RepliconServiceOperator(
            task_id='update_date_valuefor_most_recent_hire_date',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_all_user_custom_fields').recent_hire_date }}",
                "value": {
                    "year": "{{ result('get_start_date').year }}",
                    "month": "{{ result('get_start_date').month }}",
                    "day": "{{ result('get_start_date').day }}"
                }
            }
        )

        if_departmenturi_present = rail.IfOperator(
            task_id='if_departmenturi_present',
            test='''{{ dag_run.conf["departmenturi"] | is_truthy }}''',
            yes_task="update_department_for_user",
            no_task="log_error_logfordepartmentnotpresent",
        )

        update_department_for_user = rail.RepliconServiceOperator(
            task_id='update_department_for_user',
            endpoint="/services/DepartmentService1.svc/UpdateDepartmentForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "departmentUri": '{{ dag_run.conf["departmenturi"] }}'
            }
        )

        log_error_logfordepartmentnotpresent = rail.PythonOperator(
            task_id='log_error_logfordepartmentnotpresent',
            python_callable=lambda dag_run:  f'''Default department is added as department for the user as "{dag_run.conf["department"]}" is not available in Replicon'''
        )

        def get_entrydata(type_name):
            record = rail.result('mapper_search_entries') or []
            return next(
                (x['value'] for x in record if x["type"] == type_name and x["employee_type"] == "All"),
                None
            )

        log_pluckifscheduleispresent = rail.PythonOperator(
            task_id='log_pluckifscheduleispresent',
            python_callable=get_entrydata,
            op_args=['Schedule Name']

        )

        if_scheduleispresent_present = rail.IfOperator(
            task_id='if_scheduleispresent_present',
            test='''{{ result('log_pluckifscheduleispresent') | is_truthy }}''',
            yes_task="get_all_office_schedules",
            no_task="log_pluckifworkweekispresent",
        )

        get_all_office_schedules = rail.RepliconServiceOperator(
            task_id='get_all_office_schedules',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', rail.result('log_pluckifscheduleispresent'), 'uri')
        )

        if_get_schedule_uri_present = rail.IfOperator(
            task_id='if_get_schedule_uri_present',
            test='''{{ result('get_all_office_schedules') | is_truthy  and result('log_pluckifscheduleispresent') != 'Shift Schedule' }}''',
            yes_task="put_schedule_policy_1",
            no_task="if_scheduleispresent_65_eq_shiftschedule",
        )

        put_schedule_policy_1 = rail.RepliconServiceOperator(
            task_id='put_schedule_policy_1',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "scheduleEntries": [
                    {
                        "schedulePolicy": {
                            "officeScheduleUri": "{{ result('get_all_office_schedules') }}",
                            "name": null,
                            "officeSchedule": null,
                            "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        if_scheduleispresent_65_eq_shiftschedule = rail.IfOperator(
            task_id='if_scheduleispresent_65_eq_shiftschedule',
            test='''{{ result('log_pluckifscheduleispresent') == 'Shift Schedule' }}''',
            yes_task="put_schedule_policy_2",
            no_task="log_pluckifworkweekispresent",
        )

        put_schedule_policy_2 = rail.RepliconServiceOperator(
            task_id='put_schedule_policy_2',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "scheduleEntries": [
                    {
                        "schedulePolicy": {
                            "officeScheduleUri": null,
                            "name": null,
                            "officeSchedule": null,
                            "scheduleTypeUri": "urn:replicon:schedule-type:shift"
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        log_pluckifworkweekispresent = rail.PythonOperator(
            task_id='log_pluckifworkweekispresent',
            python_callable=get_entrydata,
            op_args=['Work Week']
        )

        if_pluckifworkweekispresent_present = rail.IfOperator(
            task_id='if_pluckifworkweekispresent_present',
            test='''{{ result('log_pluckifworkweekispresent') | is_truthy }}''',
            yes_task="put_schedule_policy_3",
            no_task="log_pluckiftimesheetapprovalpathispresent",
        )

        put_schedule_policy_3 = rail.RepliconServiceOperator(
            task_id='put_schedule_policy_3',
            endpoint="/services/UserService1.svc/UpdateWorkWeekStartDayForUser",
            data=lambda: {
                "userUri": rail.result('create_user')['uri'],
                "dayOfWeekUri": str(rail.result('log_pluckifworkweekispresent')).rsplit('|', maxsplit=1)[-1]
            }
        )

        log_pluckiftimesheetapprovalpathispresent = rail.PythonOperator(
            task_id='log_pluckiftimesheetapprovalpathispresent',
            python_callable=get_entrydata,
            op_args=['Timesheet Approval Path']
        )

        if_timesheetapprovalpathispresent_present = rail.IfOperator(
            task_id='if_timesheetapprovalpathispresent_present',
            test='''{{ result('log_pluckiftimesheetapprovalpathispresent') | is_truthy }}''',
            yes_task="get_all_timesheet_approval_paths",
            no_task="log_pluckiftimesoffapprovalpathispresent",
        )

        get_all_timesheet_approval_paths = rail.RepliconServiceOperator(
            task_id='get_all_timesheet_approval_paths',
            endpoint="/services/TimesheetApprovalService1.svc/GetAllApprovalPaths",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', rail.result('log_pluckiftimesheetapprovalpathispresent'), 'uri')
        )

        if_gettimesheetapprovalpath_uri_present = rail.IfOperator(
            task_id='if_gettimesheetapprovalpath_uri_present',
            test='''{{ result('get_all_timesheet_approval_paths') | is_truthy }}''',
            yes_task="update_approval_path_for_userfortimesheet",
            no_task="log_pluckiftimesoffapprovalpathispresent",
        )

        update_approval_path_for_userfortimesheet = rail.RepliconServiceOperator(
            task_id='update_approval_path_for_userfortimesheet',
            endpoint="/services/TimesheetApprovalService1.svc/UpdateApprovalPathForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "approvalPathUri": "{{ result('get_all_timesheet_approval_paths') }}"
            }
        )

        log_pluckiftimesoffapprovalpathispresent = rail.PythonOperator(
            task_id='log_pluckiftimesoffapprovalpathispresent',
            python_callable=get_entrydata,
            op_args=['TimeOff Approval Path']
        )

        if_timeoffapprovalpathispresent_present = rail.IfOperator(
            task_id='if_timeoffapprovalpathispresent_present',
            test='''{{ result('log_pluckiftimesoffapprovalpathispresent') | is_truthy }}''',
            yes_task="get_all_timeoff_approval_paths",
            no_task="log_pluckiflicencesispresent",
        )

        get_all_timeoff_approval_paths = rail.RepliconServiceOperator(
            task_id='get_all_timeoff_approval_paths',
            endpoint="/services/TimeOffApprovalService1.svc/GetAllApprovalPaths",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', rail.result('log_pluckiftimesoffapprovalpathispresent'), 'uri')
        )

        if_gettimeoffapprovalpath_uri_present = rail.IfOperator(
            task_id='if_gettimeoffapprovalpath_uri_present',
            test='''{{ result('get_all_timeoff_approval_paths') | is_truthy }}''',
            yes_task="update_approval_path_for_userfortimeoff",
            no_task="log_pluckiflicencesispresent",
        )

        update_approval_path_for_userfortimeoff = rail.RepliconServiceOperator(
            task_id='update_approval_path_for_userfortimeoff',
            endpoint="/services/TimeOffApprovalService1.svc/UpdateApprovalPathForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "approvalPathUri": "{{ result('get_all_timeoff_approval_paths') }}"
            }
        )

        log_pluckiflicencesispresent = rail.PythonOperator(
            task_id='log_pluckiflicencesispresent',
            python_callable=get_entrydata,
            op_args=['License']
        )

        if_pluckiflicencesispresent_present = rail.IfOperator(
            task_id='if_pluckiflicencesispresent_present',
            test='''{{ result('log_pluckiflicencesispresent') | is_truthy }}''',
            yes_task="get_all_product_assignment",
            no_task="if_pluckiflicencesispresent_89_blank",
        )

        get_all_product_assignment = rail.RepliconServiceOperator(
            task_id='get_all_product_assignment',
            endpoint="/services/AccountManagementService1.svc/GetAllProductsAvailableForUserAssignment",
            data=None
        )

        # Replaced: log_numberoflicensestobeassigned + foreach_create_list + log_individuallicensetobeassigned
        #           + accumulate_list_items_1 + foreach_create_list_93_94_end
        # Builds license URI list directly from get_all_product_assignment API result
        log_licenseuristobeassigned = rail.PythonOperator(
            task_id='log_licenseuristobeassigned',
            python_callable=lambda: [
                uri for uri in [
                    rail.find_first_by_attr_and_get_attr(
                        rail.result('get_all_product_assignment'), 'displayText', name, 'uri'
                    )
                    for name in rail.result('log_pluckiflicencesispresent').split("|")
                ] if uri
            ] or None
        )

        if_licenseuristobeassigned_present = rail.IfOperator(
            task_id='if_licenseuristobeassigned_present',
            test='''{{ result('log_licenseuristobeassigned') | is_truthy }}''',
            yes_task="put_product_assignments_for_user_1",
            no_task="if_licenseuristobeassigned_97_blank",
        )

        put_product_assignments_for_user_1 = rail.RepliconServiceOperator(
            task_id='put_product_assignments_for_user_1',
            endpoint="/services/AccountManagementService1.svc/PutProductAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result('create_user')['uri'],
                "productUris": rail.result('log_licenseuristobeassigned') or []
            }
        )

        if_licenseuristobeassigned_97_blank = rail.IfOperator(
            task_id='if_licenseuristobeassigned_97_blank',
            test='''{{ result('log_licenseuristobeassigned') | is_falsy }}''',
            yes_task="put_product_assignments_for_user_2",
            no_task="if_pluckiflicencesispresent_89_blank",
        )

        put_product_assignments_for_user_2 = rail.RepliconServiceOperator(
            task_id='put_product_assignments_for_user_2',
            endpoint="/services/AccountManagementService1.svc/PutProductAssignmentsForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "productUris": []
            }
        )

        if_pluckiflicencesispresent_89_blank = rail.IfOperator(
            task_id='if_pluckiflicencesispresent_89_blank',
            test='''{{ result('log_pluckiflicencesispresent') | is_falsy }}''',
            yes_task="put_product_assignments_for_user_3",
            no_task="log_pluckiftimesheettemplateispresent",
        )

        put_product_assignments_for_user_3 = rail.RepliconServiceOperator(
            task_id='put_product_assignments_for_user_3',
            endpoint="/services/AccountManagementService1.svc/PutProductAssignmentsForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "productUris": []
            }
        )

        log_pluckiftimesheettemplateispresent = rail.PythonOperator(
            task_id='log_pluckiftimesheettemplateispresent',
            python_callable=lambda dag_run: next(
                (x['value'] for x in (rail.result('mapper_search_entries') or [])
                 if x["type"] == "Timesheet Template" and x["employee_type"] == dag_run.conf["employeetype"]),
                None
            )
        )

        log_pluckif_punch_entry_policyispresent = rail.PythonOperator(
            task_id='log_pluckif_punch_entry_policyispresent',
            python_callable=lambda dag_run: next(
                (x['value'] for x in (rail.result('mapper_search_entries') or [])
                 if x["type"] == "Punch Entry Policy" and x["employee_type"] == dag_run.conf["employeetype"]),
                None
            )
        )

        log_pluckif_time_off_templateispresent = rail.PythonOperator(
            task_id='log_pluckif_time_off_templateispresent',
            python_callable=lambda: next(
                (x['value'] for x in (rail.result('mapper_search_entries') or [])
                 if x["type"] == "TimeOff Template" and x["employee_type"] == "All"),
                None
            )
        )

        if_timesheettemplateispresent_present = rail.IfOperator(
            task_id='if_timesheettemplateispresent_present',
            test='''{{ result('log_pluckiftimesheettemplateispresent') | is_truthy  or result('log_pluckif_punch_entry_policyispresent') | is_truthy  or result('log_pluckif_time_off_templateispresent') | is_truthy }}''',
            yes_task="get_all_policysets",
            no_task="log_pluckif_pay_ruleispresent",
        )

        get_all_policysets = rail.RepliconServiceOperator(
            task_id='get_all_policysets',
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
            data_handler=lambda response: {
                'timesheet_template': rail.find_first_by_attr_and_get_attr(response, 'displayText', rail.result('log_pluckiftimesheettemplateispresent'), 'uri', ''),
                'punch_entry_policy_template': rail.find_first_by_attr_and_get_attr(response, 'displayText', rail.result('log_pluckif_punch_entry_policyispresent'), 'uri', ''),
                'timeoff_template': rail.find_first_by_attr_and_get_attr(response, 'displayText', rail.result('log_pluckif_time_off_templateispresent'), 'uri', '')
            }
        )

        def get_policysets_to_assign():
            policyset_uris = []
            if rail.result('get_all_policysets')['timesheet_template']:
                policyset_uris.append(rail.result('get_all_policysets')['timesheet_template'])
            if rail.result('get_all_policysets')['punch_entry_policy_template']:
                policyset_uris.append(rail.result('get_all_policysets')['punch_entry_policy_template'])
            if rail.result('get_all_policysets')['timeoff_template']:
                policyset_uris.append(rail.result('get_all_policysets')['timeoff_template'])
            return policyset_uris or None

        log_policysetstoassign = rail.PythonOperator(
            task_id='log_policysetstoassign',
            python_callable=get_policysets_to_assign
        )

        if_policysetstoassign_present = rail.IfOperator(
            task_id='if_policysetstoassign_present',
            test='''{{ result('log_policysetstoassign') | is_truthy }}''',
            yes_task="put_policy_set_assignments_for_user",
            no_task="log_pluckif_pay_ruleispresent",
        )

        put_policy_set_assignments_for_user = rail.RepliconServiceOperator(
            task_id='put_policy_set_assignments_for_user',
            endpoint="/services/PolicySetService1.svc/PutPolicySetAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result('create_user')['uri'],
                "policySetUris": rail.result('log_policysetstoassign') or []
            }
        )

        log_pluckif_pay_ruleispresent = rail.PythonOperator(
            task_id='log_pluckif_pay_ruleispresent',
            python_callable=get_entrydata,
            op_args=['Payrule Name']
        )

        get_all_payrule_scripts = rail.RepliconServiceOperator(
            task_id='get_all_payrule_scripts',
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', rail.result('log_pluckif_pay_ruleispresent'), 'uri')
        )

        if_get_pay_rule_script_uri_117_present_enabled = rail.IfOperator(
            task_id='if_get_pay_rule_script_uri_117_present_enabled',
            test='''{{ result('get_all_payrule_scripts') | is_truthy }}''',
            yes_task="put_payroll_assignment",
            no_task="if_get_pay_rule_script_uri_117_blank_enabled",
        )

        put_payroll_assignment = rail.RepliconServiceOperator(
            task_id='put_payroll_assignment',
            endpoint="/services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "scheduleEntries": [
                    {
                        "payRuleScript": {
                            "uri": "{{ result('get_all_payrule_scripts') }}",
                            "name": null
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        if_get_pay_rule_script_uri_117_blank_enabled = rail.IfOperator(
            task_id='if_get_pay_rule_script_uri_117_blank_enabled',
            test='''{{ result('get_all_payrule_scripts') | is_falsy }}''',
            yes_task="log_error_logforpayrulenotpresent",
            no_task="log_pluckif_activity_assignmentispresent",
        )

        log_error_logforpayrulenotpresent = rail.PythonOperator(
            task_id='log_error_logforpayrulenotpresent',
            python_callable=lambda: f'Payrule not added for User as "{rail.result("log_pluckif_pay_ruleispresent")}" is not available as payrule in Replicon.'
        )

        log_pluckif_activity_assignmentispresent = rail.PythonOperator(
            task_id='log_pluckif_activity_assignmentispresent',
            python_callable=get_entrydata,
            op_args=['Activity']
        )

        if_activity_assignmentispresent_present = rail.IfOperator(
            task_id='if_activity_assignmentispresent_present',
            test='''{{ result('log_pluckif_activity_assignmentispresent') | is_truthy }}''',
            yes_task="get_enabled_activities",
            no_task="log_activity_uristobeassigned",
        )

        get_enabled_activities = rail.RepliconServiceOperator(
            task_id='get_enabled_activities',
            endpoint="/services/ActivityService1.svc/GetEnabledActivities",
            data=None
        )

        log_activitiestoassign = rail.PythonOperator(
            task_id='log_activitiestoassign',
            python_callable=lambda:  rail.result(
                'log_pluckif_activity_assignmentispresent').split("|") if rail.result('log_pluckif_activity_assignmentispresent') else []
        )

        # Replaced: foreach_create_list_numberofactiviitestoassign + log_activities
        #           + accumulate_list_items_2 + foreach_activities_end
        # Builds activity URI list directly from get_enabled_activities API result
        log_activity_uristobeassigned = rail.PythonOperator(
            task_id='log_activity_uristobeassigned',
            python_callable=lambda: [
                uri for uri in [
                    rail.find_first_by_attr_and_get_attr(
                        rail.result('get_enabled_activities'), 'displayText', name, 'uri'
                    )
                    for name in (rail.result('log_activitiestoassign') or [])
                ] if uri
            ] or None
        )

        if_activity_uristobeassigned_present = rail.IfOperator(
            task_id='if_activity_uristobeassigned_present',
            test='''{{ result('log_activity_uristobeassigned') | is_truthy }}''',
            yes_task="put_activity_assignments_for_user",
            no_task="if_manger_present",
        )

        put_activity_assignments_for_user = rail.RepliconServiceOperator(
            task_id='put_activity_assignments_for_user',
            endpoint="/services/ActivityService1.svc/PutActivityAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result('create_user')['uri'],
                "activityUris": rail.result('log_activity_uristobeassigned') or []
            }
        )

        if_manger_present = rail.IfOperator(
            task_id='if_manger_present',
            test='''{{ dag_run.conf["manager"] | is_truthy }}''',
            yes_task="search_users",
            no_task="if_hourlypayrollrate_present",
        )

        search_users = rail.RepliconServiceOperator(
            task_id='search_users',
            endpoint='/services/UserListService1.svc/GetData',
            data=lambda dag_run: {
                'page': '1',
                'pagesize': '100',
                'columnUris': [
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:enabled"
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:login-name'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': dag_run.conf["manager"]
                        }
                    }
                }
            },
            data_handler=response_filter.get_manager
        )

        get_all_permissionsets = rail.RepliconServiceOperator(
            task_id='get_all_permissionsets',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Supervisor', 'uri', '')
        )

        if_loginname_ne_manger = rail.IfOperator(
            task_id='if_loginname_ne_manger',
            test='''{{ dag_run.conf.loginname != dag_run.conf.manager }}''',
            yes_task="if_getsupervisor_uri_present",
            no_task="log_errorwhenuserandsupervisorsloginnamearesame",
        )

        if_getsupervisor_uri_present = rail.IfOperator(
            task_id='if_getsupervisor_uri_present',
            test='''{{ result('search_users') | is_truthy }}''',
            yes_task="log_get_supervisor_status",
            no_task="if_getsupervisor_uri_135_blank",
        )

        log_get_supervisor_status = rail.PythonOperator(
            task_id='log_get_supervisor_status',
            python_callable=lambda: bool(rail.result(
                'search_users')['status'] == 'True')
        )

        if_get_supervisor_status_139_eq_true = rail.IfOperator(
            task_id='if_get_supervisor_status_139_eq_true',
            test='''{{ result('log_get_supervisor_status') }}''',
            yes_task="get_assigned_permissionsets",
            no_task="ascend_supervisor_assignment_table_add_entry_1",
        )

        get_assigned_permissionsets = rail.RepliconServiceOperator(
            task_id='get_assigned_permissionsets',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_users').uri }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'permissionSet.displayText', 'Supervisor', 'permissionSet.uri', ''),
        )

        if_supervisorhassupervisorpermission_142_blank = rail.IfOperator(
            task_id='if_supervisorhassupervisorpermission_142_blank',
            test='''{{ result('get_assigned_permissionsets') | is_falsy }}''',
            yes_task="assign_supervsior_permission_set_to_user",
            no_task="update_initial_supervisor",
        )

        assign_supervsior_permission_set_to_user = rail.RepliconServiceOperator(
            task_id='assign_supervsior_permission_set_to_user',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('search_users').uri }}",
                "permissionSetUri": "{{ result('get_all_permissionsets') }}"
            }
        )

        update_initial_supervisor = rail.RepliconServiceOperator(
            task_id='update_initial_supervisor',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "initialSupervisorUri": "{{ result('search_users').uri }}",
                "scheduleEntries": []
            }
        )

        ascend_supervisor_assignment_table_add_entry_1 = rail.WriteLogOperator(
            task_id='ascend_supervisor_assignment_table_add_entry_1',
            log='{{ dag_run.conf["ascend_supervisor_assignments_logs_lookuptable"] }}',
            message="Add",
            severity="Add",
            properties={
                "userloginname": '{{ dag_run.conf["loginname"] }}',
                "useruri": "{{ result('create_user').uri }}",
                "supervisorloginname": '{{ dag_run.conf["manager"] }}',
                "action": "Add",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_getsupervisor_uri_135_blank = rail.IfOperator(
            task_id='if_getsupervisor_uri_135_blank',
            test='''{{ result('search_users') | is_falsy }}''',
            yes_task="ascend_supervisor_assignment_table_add_entry_2",
            no_task="if_hourlypayrollrate_present",
        )

        ascend_supervisor_assignment_table_add_entry_2 = rail.WriteLogOperator(
            task_id='ascend_supervisor_assignment_table_add_entry_2',
            log='{{ dag_run.conf["ascend_supervisor_assignments_logs_lookuptable"] }}',
            message="na",
            severity="fixme",
            properties={
                "userloginname": '{{ dag_run.conf["loginname"] }}',
                "useruri": "{{ result('create_user').uri }}",
                "supervisorloginname": '{{ dag_run.conf["manager"] }}',
                "action": "Add",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        log_errorwhenuserandsupervisorsloginnamearesame = rail.PythonOperator(
            task_id='log_errorwhenuserandsupervisorsloginnamearesame',
            python_callable=lambda dag_run: f'''User "{dag_run.conf["employeefirstname"]} {dag_run.conf["employeelastname"]}" is created, however supervisor is not updated as the "Login name" for user and supervisor is same'''
        )

        if_hourlypayrollrate_present = rail.IfOperator(
            task_id='if_hourlypayrollrate_present',
            test='''{{ dag_run.conf["hourlypayrollrate"] | is_truthy }}''',
            yes_task="if_hourlypayrollcurrency_present",
            no_task="log_pluckiftimezoneispresent",
        )

        if_hourlypayrollcurrency_present = rail.IfOperator(
            task_id='if_hourlypayrollcurrency_present',
            test='''{{ dag_run.conf["hourlypayrollcurrency"] | is_truthy }}''',
            yes_task="get_all_currencies",
            no_task="get_base_currency",
        )

        get_all_currencies = rail.RepliconServiceOperator(
            task_id='get_all_currencies',
            endpoint="/services/CurrencyService2.svc/GetAllCurrencies",
            data=None
        )

        log_get_currency_uri_1 = rail.PythonOperator(
            task_id='log_get_currency_uri_1',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_currencies'), 'name', dag_run.conf["hourlypayrollcurrency"], 'uri')
        )

        if_get_currency_uri_present_1 = rail.IfOperator(
            task_id='if_get_currency_uri_present_1',
            test='''{{ result('log_get_currency_uri_1') | is_truthy }}''',
            yes_task="put_user_payroll_rate_schedule_initial_schedule_1",
            no_task="log_pluckiftimezoneispresent",
        )

        put_user_payroll_rate_schedule_initial_schedule_1 = rail.RepliconServiceOperator(
            task_id='put_user_payroll_rate_schedule_initial_schedule_1',
            endpoint="/services/PayrollService1.svc/PutUserPayrollRateSchedule",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "schedule": {
                    "initialHourlyRate": {
                        "amount": '{{ dag_run.conf["hourlypayrollrate"] }}',
                        "currency": {
                            "uri": "{{ result('log_get_currency_uri_1') }}",
                            "name": null,
                            "symbol": null
                        }
                    },
                    "scheduleEntries": []
                }
            }
        )

        get_base_currency = rail.RepliconServiceOperator(
            task_id='get_base_currency',
            endpoint="/services/CurrencyService2.svc/GetBaseCurrency",
            data=None
        )

        log_get_currency_uri_2 = rail.PythonOperator(
            task_id='log_get_currency_uri_2',
            python_callable=lambda: rail.result('get_base_currency')['uri']
        )

        if_get_currency_uri_present_2 = rail.IfOperator(
            task_id='if_get_currency_uri_present_2',
            test='''{{ result('log_get_currency_uri_2') | is_truthy }}''',
            yes_task="put_user_payroll_rate_schedule_initial_schedule_2",
            no_task="log_pluckiftimezoneispresent",
        )

        put_user_payroll_rate_schedule_initial_schedule_2 = rail.RepliconServiceOperator(
            task_id='put_user_payroll_rate_schedule_initial_schedule_2',
            endpoint="/services/PayrollService1.svc/PutUserPayrollRateSchedule",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "schedule": {
                    "initialHourlyRate": {
                        "amount": '{{ dag_run.conf["hourlypayrollrate"] }}',
                        "currency": {
                            "uri": "{{ result('log_get_currency_uri_2') }}",
                            "name": null,
                            "symbol": null
                        }
                    },
                    "scheduleEntries": []
                }
            }
        )

        log_pluckiftimezoneispresent = rail.PythonOperator(
            task_id='log_pluckiftimezoneispresent',
            python_callable=get_entrydata,
            op_args=['Time Zone']
        )

        if_pluckiftimezoneispresent_present = rail.IfOperator(
            task_id='if_pluckiftimezoneispresent_present',
            test='''{{ result('log_pluckiftimezoneispresent') | is_truthy }}''',
            yes_task="log_timezone",
            no_task="log_pluckif_holiday_calendarispresent",
        )

        log_timezone = rail.PythonOperator(
            task_id='log_timezone',
            python_callable=lambda:  str(rail.result(
                'log_pluckiftimezoneispresent')).rsplit('|', maxsplit=1)[-1]
        )

        update_time_zone_for_user = rail.RepliconServiceOperator(
            task_id='update_time_zone_for_user',
            endpoint="/services/InternationalizationService1.svc/UpdateTimeZoneForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "timeZoneUri": "{{ result('log_timezone') }}"
            }
        )

        log_pluckif_holiday_calendarispresent = rail.PythonOperator(
            task_id='log_pluckif_holiday_calendarispresent',
            python_callable=get_entrydata,
            op_args=['Holiday Calendar']
        )

        if_pluckif_holiday_calendarispresent_present = rail.IfOperator(
            task_id='if_pluckif_holiday_calendarispresent_present',
            test='''{{ result('log_pluckif_holiday_calendarispresent') | is_truthy }}''',
            yes_task="get_all_holiday_calendars",
            no_task="if_location_present",
        )

        get_all_holiday_calendars = rail.RepliconServiceOperator(
            task_id='get_all_holiday_calendars',
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
            data=None
        )

        log_get_holiday_calendar_uri = rail.PythonOperator(
            task_id='log_get_holiday_calendar_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_holiday_calendars'), 'name', rail.result('log_pluckif_holiday_calendarispresent'), 'uri')
        )

        if_get_holiday_calendar_uri_present = rail.IfOperator(
            task_id='if_get_holiday_calendar_uri_present',
            test='''{{ result('log_get_holiday_calendar_uri') | is_truthy }}''',
            yes_task="update_holiday_calendar_for_user",
            no_task="if_location_present",
        )

        update_holiday_calendar_for_user = rail.RepliconServiceOperator(
            task_id='update_holiday_calendar_for_user',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "holidayCalendarUri": "{{ result('log_get_holiday_calendar_uri') }}"
            }
        )

        if_location_present = rail.IfOperator(
            task_id='if_location_present',
            test='''{{ dag_run.conf["location"] | is_truthy }}''',
            yes_task="get_all_locations",
            no_task="if_costcenter_present",
        )

        get_all_locations = rail.RepliconServiceOperator(
            task_id='get_all_locations',
            endpoint="/services/LocationService1.svc/GetAllLocations",
            data=None
        )

        log_get_required_location_uri = rail.PythonOperator(
            task_id='log_get_required_location_uri',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_locations'), 'displayText', dag_run.conf["location"], 'uri')
        )

        if_location_changed = rail.IfOperator(
            task_id='if_location_changed',
            test='''{{ result('log_get_required_location_uri') | is_truthy }}''',
            yes_task="put_location_schedule_for_user",
            no_task="log_errormessageincasewhenlocationisnotavailable_1",
        )

        put_location_schedule_for_user = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "scheduleEntries": [
                    {
                        "location": {
                            "uri": "{{ result('log_get_required_location_uri') }}",
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        log_errormessageincasewhenlocationisnotavailable_1 = rail.PythonOperator(
            task_id='log_errormessageincasewhenlocationisnotavailable_1',
            python_callable=lambda dag_run: "Location not added for User as " +
            str(dag_run.conf["location"]) + " is not available in Replicon"
        )

        if_costcenter_present = rail.IfOperator(
            task_id='if_costcenter_present',
            test='''{{ dag_run.conf["costcenter"] | is_truthy }}''',
            yes_task="log_requiredcostcentername",
            no_task="if_enabled_eq_yes",
        )

        log_requiredcostcentername = rail.PythonOperator(
            task_id='log_requiredcostcentername',
            python_callable=lambda dag_run: dag_run.conf["costcenter"].split(
                "|")[-1].strip()
        )

        get_dataforcostcenter = rail.RepliconServiceOperator(
            task_id='get_dataforcostcenter',
            endpoint="/services/CostCenterlistService1.svc/GetData",
            data=request_payload.get_data_for_costcenter_payload_data
        )

        # Replaced: foreach_d + accumulate_list_items_3 + foreach_d_184_end
        # Finds the matching cost center URI directly from API result rows
        log_get_required_cost_center_uri = rail.PythonOperator(
            task_id='log_get_required_cost_center_uri',
            python_callable=lambda dag_run: next(
                (
                    row['cells'][0]['cellCollection'][-1]['uri']
                    for row in rail.result('get_dataforcostcenter')['rows']
                    if '/'.join(cell['textValue'].strip() for cell in row['cells'][0]['cellCollection']).lower() ==
                       dag_run.conf["costcenter"].replace(' | ', '/').strip().lower()
                ),
                None
            )
        )

        if_get_required_cost_center_uri_present = rail.IfOperator(
            task_id='if_get_required_cost_center_uri_present',
            test='''{{ result('log_get_required_cost_center_uri') | is_truthy }}''',
            yes_task="put_cost_center_schedule_for_user",
            no_task="log_errormessageincasewhenlocationisnotavailable_2",
        )

        put_cost_center_schedule_for_user = rail.RepliconServiceOperator(
            task_id='put_cost_center_schedule_for_user',
            endpoint="/services/CostCenterService1.svc/PutCostCenterScheduleForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "scheduleEntries": [
                    {
                        "costCenter": {
                            "uri": "{{ result('log_get_required_cost_center_uri') }}",
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        log_errormessageincasewhenlocationisnotavailable_2 = rail.PythonOperator(
            task_id='log_errormessageincasewhenlocationisnotavailable_2',
            python_callable=lambda dag_run: "Cost Center not added for User as " +
            str(dag_run.conf["costcenter"]) + " is not available in Replicon"
        )

        if_enabled_eq_yes = rail.IfOperator(
            task_id='if_enabled_eq_yes',
            test='''{{ dag_run.conf["enabled"].lower() == 'yes' }}''',
            yes_task="trigger_timeoff_add192",
            no_task="log_entry_6",
        )

        trigger_timeoff_add192 = rail.TriggerDagRunOperator(
            task_id='trigger_timeoff_add192',
            retries=0,
            trigger_dag_id=config.timeoff_add_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "userloginname": '{{ dag_run.conf["loginname"] }}',
                "useruri": "{{ result('create_user').uri }}",
                "employeetype": '{{ dag_run.conf["employeetype"] }}',
                "location": '{{ dag_run.conf["location"] }}',
                "scheduledhours": '{{ dag_run.conf["udf"] }}',
                "ascend_user_import_logs_lookuptable": '{{ dag_run.conf["ascend_user_import_logs_lookuptable"] }}'
            }
        )

        wait_live_ascend_timeoff_add192 = rail.WaitForDagRunsSensor(
            task_id='wait_live_ascend_timeoff_add192',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_timeoff_add192") }}'
        )

        log_entry_6 = rail.WriteLogOperator(
            task_id='log_entry_6',
            log='{{ dag_run.conf["ascend_user_import_logs_lookuptable"] }}',
            message="na",
            severity="Added",
            properties=lambda dag_run: {
                "userloginname": dag_run.conf.get('loginname', ''),
                "username": dag_run.conf.get('employeefirstname', '') + " " + dag_run.conf.get('employeelastname', ''),
                "action": "Add",
                "status": "Success",
                "details": "Added Successfully"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf["ascend_user_import_logs_lookuptable"] }}',
            trigger_rule='one_failed',
            severity="Error",
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                "username": dag_run.conf.get('employeefirstname', '') + " " + dag_run.conf.get('employeelastname', ''),
                "userloginname": dag_run.conf.get('loginname', ''),
                "action": "Add",
                "status": "Error",
                "details": rail.render_template("{{ get_error_message() }}")
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            'No') >> if_enabled_ne_yes
        if_enabled_ne_yes >> rail.Label(
            'Yes') >> log_entry_1 >> catch_and_log_errors
        if_enabled_ne_yes >> rail.Label(
            'No') >> if_employeefirstname_blank_yes
        if_employeefirstname_blank_yes >> rail.Label(
            'Yes') >> log_entry_2 >> catch_and_log_errors
        if_employeefirstname_blank_yes >> rail.Label(
            'No') >> if_startdate_blank
        if_startdate_blank >> rail.Label(
            'Yes') >> log_entry_3 >> catch_and_log_errors
        if_startdate_blank >> rail.Label(
            'No') >> get_todaysdate >> get_start_date >> if_employeetype_blank
        if_employeetype_blank >> rail.Label(
            'Yes') >> log_entry_4 >> catch_and_log_errors
        if_employeetype_blank >> rail.Label(
            'No') >> get_all_employee_type_details >> if_employee_type_uri_18_blank
        if_employee_type_uri_18_blank >> rail.Label(
            'Yes') >> log_entry_5 >> catch_and_log_errors
        if_employee_type_uri_18_blank >> rail.Label(
            'No') >> mapper_search_entries >> get_company_department >> create_user >> remove_timeoffassignmentsforusers >> get_all_user_custom_fields >> if_timetype_present
        if_timetype_present >> rail.Label(
            'Yes') >> if_get_udf_uri_f_t_p_t_present
        if_get_udf_uri_f_t_p_t_present >> rail.Label(
            'Yes') >> get_enabled_custome_field >> if_required_udfdropdownurifor_f_t_p_t_present
        if_required_udfdropdownurifor_f_t_p_t_present >> rail.Label(
            'Yes') >> update_dropdown_valuefor_f_t_p_t >> if_homecountry_present
        if_required_udfdropdownurifor_f_t_p_t_present >> rail.Label(
            'No') >> if_homecountry_present
        if_get_udf_uri_f_t_p_t_present >> rail.Label(
            'No') >> if_homecountry_present
        if_timetype_present >> rail.Label(
            'No') >> if_homecountry_present
        if_homecountry_present >> rail.Label(
            'Yes') >> if_get_udf_uri_home_country_present
        if_get_udf_uri_home_country_present >> rail.Label(
            'Yes') >> update_text_valuefor_home_country >> if_homestateprovince_present
        if_get_udf_uri_home_country_present >> rail.Label(
            'No') >> if_homestateprovince_present
        if_homecountry_present >> rail.Label(
            'No') >> if_homestateprovince_present
        if_homestateprovince_present >> rail.Label(
            'Yes') >> if_get_udf_uri_home_state_province_present
        if_get_udf_uri_home_state_province_present >> rail.Label(
            'Yes') >> update_text_valuefor_home_state_province >> if_homecity_present
        if_get_udf_uri_home_state_province_present >> rail.Label(
            'No') >> if_homecity_present
        if_homestateprovince_present >> rail.Label(
            'No') >> if_homecity_present
        if_homecity_present >> rail.Label(
            'Yes') >> if_get_udf_uri_home_city_present
        if_get_udf_uri_home_city_present >> rail.Label(
            'Yes') >> update_text_valuefor_home_home_city >> if_udf_present
        if_get_udf_uri_home_city_present >> rail.Label(
            'No') >> if_udf_present
        if_homecity_present >> rail.Label(
            'No') >> if_udf_present
        if_udf_present >> rail.Label(
            'Yes') >> if_get_udf_uri_scheduled_hours_present
        if_get_udf_uri_scheduled_hours_present >> rail.Label(
            'Yes') >> update_numeric_valuefor_scheduled_hours >> if_continuousservicedate_present
        if_get_udf_uri_scheduled_hours_present >> rail.Label(
            'No') >> if_continuousservicedate_present
        if_udf_present >> rail.Label(
            'No') >> if_continuousservicedate_present
        if_continuousservicedate_present >> rail.Label(
            'Yes') >> if_continuousservicedate_contains
        if_continuousservicedate_contains >> rail.Label(
            'Yes') >> if_get_udf_uri_continuous_service_date_present
        if_get_udf_uri_continuous_service_date_present >> rail.Label(
            'Yes') >> get_continuous_service_date >> update_date_valuefor_continuous_service_date >> if_get_udf_uri_most_recent_hire_date_present
        if_get_udf_uri_continuous_service_date_present >> rail.Label(
            'No') >> log_messageforcontinuousservicedateerror
        if_continuousservicedate_contains >> rail.Label(
            'No') >> log_messageforcontinuousservicedateerror
        log_messageforcontinuousservicedateerror >> if_get_udf_uri_most_recent_hire_date_present
        if_continuousservicedate_present >> rail.Label(
            'No') >> if_get_udf_uri_most_recent_hire_date_present
        if_get_udf_uri_most_recent_hire_date_present >> rail.Label(
            'Yes') >> update_date_valuefor_most_recent_hire_date >> if_departmenturi_present
        if_get_udf_uri_most_recent_hire_date_present >> rail.Label(
            'No') >> if_departmenturi_present
        if_departmenturi_present >> rail.Label(
            'Yes') >> update_department_for_user >> log_pluckifscheduleispresent
        if_departmenturi_present >> rail.Label(
            'No') >> log_error_logfordepartmentnotpresent >> log_pluckifscheduleispresent >> if_scheduleispresent_present
        if_scheduleispresent_present >> rail.Label(
            'Yes') >> get_all_office_schedules >> if_get_schedule_uri_present
        if_get_schedule_uri_present >> rail.Label(
            'Yes') >> put_schedule_policy_1 >> if_scheduleispresent_65_eq_shiftschedule
        if_get_schedule_uri_present >> rail.Label(
            'No') >> if_scheduleispresent_65_eq_shiftschedule
        if_scheduleispresent_65_eq_shiftschedule >> rail.Label(
            'Yes') >> put_schedule_policy_2 >> log_pluckifworkweekispresent
        if_scheduleispresent_65_eq_shiftschedule >> rail.Label(
            'No') >> log_pluckifworkweekispresent
        if_scheduleispresent_present >> rail.Label(
            'No') >> log_pluckifworkweekispresent
        log_pluckifworkweekispresent >> if_pluckifworkweekispresent_present
        if_pluckifworkweekispresent_present >> rail.Label(
            'Yes') >> put_schedule_policy_3 >> log_pluckiftimesheetapprovalpathispresent
        if_pluckifworkweekispresent_present >> rail.Label(
            'No') >> log_pluckiftimesheetapprovalpathispresent >> if_timesheetapprovalpathispresent_present
        if_timesheetapprovalpathispresent_present >> rail.Label(
            'Yes') >> get_all_timesheet_approval_paths >> if_gettimesheetapprovalpath_uri_present
        if_gettimesheetapprovalpath_uri_present >> rail.Label(
            'Yes') >> update_approval_path_for_userfortimesheet >> log_pluckiftimesoffapprovalpathispresent
        if_gettimesheetapprovalpath_uri_present >> rail.Label(
            'No') >> log_pluckiftimesoffapprovalpathispresent
        if_timesheetapprovalpathispresent_present >> rail.Label(
            'No') >> log_pluckiftimesoffapprovalpathispresent >> if_timeoffapprovalpathispresent_present
        log_pluckiftimesoffapprovalpathispresent >> if_timeoffapprovalpathispresent_present
        if_timeoffapprovalpathispresent_present >> rail.Label(
            'Yes') >> get_all_timeoff_approval_paths >> if_gettimeoffapprovalpath_uri_present
        if_gettimeoffapprovalpath_uri_present >> rail.Label(
            'Yes') >> update_approval_path_for_userfortimeoff >> log_pluckiflicencesispresent
        if_gettimeoffapprovalpath_uri_present >> rail.Label(
            'No') >> log_pluckiflicencesispresent
        if_timeoffapprovalpathispresent_present >> rail.Label(
            'No') >> log_pluckiflicencesispresent
        log_pluckiflicencesispresent >> if_pluckiflicencesispresent_present
        if_pluckiflicencesispresent_present >> rail.Label(
            'Yes') >> get_all_product_assignment >> log_licenseuristobeassigned >> if_licenseuristobeassigned_present
        if_licenseuristobeassigned_present >> rail.Label(
            'Yes') >> put_product_assignments_for_user_1 >> if_licenseuristobeassigned_97_blank
        if_licenseuristobeassigned_present >> rail.Label(
            'No') >> if_licenseuristobeassigned_97_blank
        if_licenseuristobeassigned_97_blank >> rail.Label(
            'Yes') >> put_product_assignments_for_user_2 >> if_pluckiflicencesispresent_89_blank
        if_licenseuristobeassigned_97_blank >> rail.Label(
            'No') >> if_pluckiflicencesispresent_89_blank
        if_pluckiflicencesispresent_present >> rail.Label(
            'No') >> if_pluckiflicencesispresent_89_blank
        if_pluckiflicencesispresent_89_blank >> rail.Label(
            'Yes') >> put_product_assignments_for_user_3 >> log_pluckiftimesheettemplateispresent
        if_pluckiflicencesispresent_89_blank >> rail.Label(
            'No') >> log_pluckiftimesheettemplateispresent >> log_pluckif_punch_entry_policyispresent >> log_pluckif_time_off_templateispresent >> if_timesheettemplateispresent_present
        if_timesheettemplateispresent_present >> rail.Label(
            'Yes') >> get_all_policysets >> log_policysetstoassign >> if_policysetstoassign_present
        if_policysetstoassign_present >> rail.Label(
            'Yes') >> put_policy_set_assignments_for_user >> log_pluckif_pay_ruleispresent
        if_policysetstoassign_present >> rail.Label(
            'No') >> log_pluckif_pay_ruleispresent
        if_timesheettemplateispresent_present >> rail.Label(
            'No') >> log_pluckif_pay_ruleispresent
        log_pluckif_pay_ruleispresent >> get_all_payrule_scripts >> if_get_pay_rule_script_uri_117_present_enabled
        if_get_pay_rule_script_uri_117_present_enabled >> rail.Label(
            'Yes') >> put_payroll_assignment >> if_get_pay_rule_script_uri_117_blank_enabled
        if_get_pay_rule_script_uri_117_present_enabled >> rail.Label(
            'No') >> if_get_pay_rule_script_uri_117_blank_enabled
        if_get_pay_rule_script_uri_117_blank_enabled >> rail.Label(
            'Yes') >> log_error_logforpayrulenotpresent >> log_pluckif_activity_assignmentispresent
        if_get_pay_rule_script_uri_117_blank_enabled >> rail.Label(
            'No') >> log_pluckif_activity_assignmentispresent >> if_activity_assignmentispresent_present
        if_activity_assignmentispresent_present >> rail.Label(
            'Yes') >> get_enabled_activities >> log_activitiestoassign >> log_activity_uristobeassigned
        if_activity_assignmentispresent_present >> rail.Label(
            'No') >> log_activity_uristobeassigned
        log_activity_uristobeassigned >> if_activity_uristobeassigned_present
        if_activity_uristobeassigned_present >> rail.Label(
            'Yes') >> put_activity_assignments_for_user >> if_manger_present
        if_activity_uristobeassigned_present >> rail.Label(
            'No') >> if_manger_present
        if_manger_present >> rail.Label(
            'Yes') >> search_users >> get_all_permissionsets >> if_loginname_ne_manger
        if_loginname_ne_manger >> rail.Label(
            'Yes') >> if_getsupervisor_uri_present
        if_getsupervisor_uri_present >> rail.Label(
            'Yes') >> log_get_supervisor_status >> if_get_supervisor_status_139_eq_true
        if_get_supervisor_status_139_eq_true >> rail.Label(
            'Yes') >> get_assigned_permissionsets >> if_supervisorhassupervisorpermission_142_blank
        if_supervisorhassupervisorpermission_142_blank >> rail.Label(
            'Yes') >> assign_supervsior_permission_set_to_user >> update_initial_supervisor
        if_supervisorhassupervisorpermission_142_blank >> rail.Label(
            'No') >> update_initial_supervisor >> if_getsupervisor_uri_135_blank
        if_get_supervisor_status_139_eq_true >> rail.Label(
            'No') >> ascend_supervisor_assignment_table_add_entry_1 >> if_getsupervisor_uri_135_blank
        if_getsupervisor_uri_present >> rail.Label(
            'No') >> if_getsupervisor_uri_135_blank
        if_getsupervisor_uri_135_blank >> rail.Label(
            'Yes') >> ascend_supervisor_assignment_table_add_entry_2
        if_getsupervisor_uri_135_blank >> rail.Label(
            'No') >> if_hourlypayrollrate_present
        if_getsupervisor_uri_135_blank >> rail.Label(
            'No') >> if_hourlypayrollrate_present
        if_loginname_ne_manger >> rail.Label(
            'Yes') >> log_errorwhenuserandsupervisorsloginnamearesame
        log_errorwhenuserandsupervisorsloginnamearesame >> if_hourlypayrollrate_present
        if_manger_present >> rail.Label(
            'No') >> if_hourlypayrollrate_present
        if_hourlypayrollrate_present >> rail.Label(
            'Yes') >> if_hourlypayrollcurrency_present
        if_hourlypayrollcurrency_present >> rail.Label(
            'Yes') >> get_all_currencies >> log_get_currency_uri_1 >> if_get_currency_uri_present_1
        if_get_currency_uri_present_1 >> rail.Label(
            'Yes') >> put_user_payroll_rate_schedule_initial_schedule_1 >> log_pluckiftimezoneispresent
        if_get_currency_uri_present_1 >> rail.Label(
            'No') >> log_pluckiftimezoneispresent
        if_hourlypayrollcurrency_present >> rail.Label(
            'No') >> get_base_currency
        get_base_currency >> log_get_currency_uri_2 >> if_get_currency_uri_present_2
        if_get_currency_uri_present_2 >> rail.Label(
            'Yes') >> put_user_payroll_rate_schedule_initial_schedule_2 >> log_pluckiftimezoneispresent
        if_get_currency_uri_present_2 >> rail.Label(
            'No') >> log_pluckiftimezoneispresent
        if_hourlypayrollrate_present >> rail.Label(
            'No') >> log_pluckiftimezoneispresent
        log_pluckiftimezoneispresent >> if_pluckiftimezoneispresent_present
        if_pluckiftimezoneispresent_present >> rail.Label(
            'Yes') >> log_timezone >> update_time_zone_for_user >> log_pluckif_holiday_calendarispresent
        if_pluckiftimezoneispresent_present >> rail.Label(
            'No') >> log_pluckif_holiday_calendarispresent >> if_pluckif_holiday_calendarispresent_present
        if_pluckif_holiday_calendarispresent_present >> rail.Label(
            'Yes') >> get_all_holiday_calendars >> log_get_holiday_calendar_uri >> if_get_holiday_calendar_uri_present
        if_get_holiday_calendar_uri_present >> rail.Label(
            'Yes') >> update_holiday_calendar_for_user >> if_location_present
        if_get_holiday_calendar_uri_present >> rail.Label(
            'No') >> if_location_present
        if_pluckif_holiday_calendarispresent_present >> rail.Label(
            'No') >> if_location_present
        if_location_present >> rail.Label(
            'Yes') >> get_all_locations >> log_get_required_location_uri >> if_location_changed
        if_location_changed >> rail.Label(
            'Yes') >> put_location_schedule_for_user >> if_costcenter_present
        if_location_changed >> rail.Label(
            'No') >> log_errormessageincasewhenlocationisnotavailable_1 >> if_costcenter_present
        if_location_present >> rail.Label(
            'No') >> if_costcenter_present
        if_costcenter_present >> rail.Label(
            'Yes') >> log_requiredcostcentername >> get_dataforcostcenter >> log_get_required_cost_center_uri >> if_get_required_cost_center_uri_present
        if_get_required_cost_center_uri_present >> rail.Label(
            'Yes') >> put_cost_center_schedule_for_user >> if_enabled_eq_yes
        if_get_required_cost_center_uri_present >> rail.Label(
            'No') >> log_errormessageincasewhenlocationisnotavailable_2 >> if_enabled_eq_yes
        if_costcenter_present >> rail.Label(
            'No') >> if_enabled_eq_yes
        if_enabled_eq_yes >> rail.Label(
            'Yes') >> trigger_timeoff_add192 >> wait_live_ascend_timeoff_add192 >> log_entry_6
        if_enabled_eq_yes >> rail.Label(
            'No') >> log_entry_6 >> catch_and_log_errors
        catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
