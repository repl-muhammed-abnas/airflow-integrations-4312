# pylint: disable=too-many-statements
from datetime import timedelta
import json
import rail
from gee.user_import.utils.python_callable import get_current_date_time
from gee.user_import.utils import request_payload, response_filter, python_callable


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.update_user_child,
        description=f'gee_user_import_update_user_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config", extra_config=config)

        # can_run_batch_task = rail.IfOperator(
        #     task_id='can_run_batch_task',
        #     test=lambda: Variable.get(
        #          config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
        #     yes_task='batch_task',
        #     no_task='exception_log'
        #     )

        # batch_task = rail.BatchTaskRunOperator(
        #     task_id='batch_task',
        #     start_task='exception_log',
        #     end_task='catch_and_log_errors',
        #     execution_timeout=timedelta(
        #         days=config.execution_timeout_days),
        #     )

        get_user_data = rail.RepliconServiceOperator(
            task_id = "get_user_data",
            endpoint = "/services/ImportService1.svc/BulkGetUsers3",
            data = request_payload.get_bulk_user_data
        )

        get_effectiveusergroupmembership = rail.RepliconServiceOperator(
            task_id = "get_effectiveusergroupmembership",
            endpoint = "/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data = {
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        is_enabled_not_true = rail.IfOperator(
            task_id="is_enabled_not_true",
            test="{{ result('get_user_data')[0].userDetails.isEnabled | lower() != 'true' }}",
            yes_task="add_entry_to_log_6",
            no_task="update_user_data_9"
        )

        add_entry_to_log_6 = rail.WriteLogOperator(
            task_id='add_entry_to_log_6',
            log = "{{ dag_run.conf.gee_user_import_lookup_table }}",
            message="na",
            severity="Success",
            properties={
                "loginname" : "{{ dag_run.conf.LoginName }}",
                "empid" : "{{ dag_run.conf.EmployeeId }}",
                "action" : "update",
                "status" : "ignored",
                "details" : "User is disabled in Replicon",
                "jobid" : "{{ dag_run.conf.calling_dag_id }}",
                "childjobid" : "{{ dag_run_ecid() }}"
            }
        )

        update_user_data_9 = rail.RepliconServiceOperator(
            task_id = "update_user_data_9",
            endpoint = "/services/ImportService1.svc/ApplyUserModifications2",
            data = request_payload.update_userdata_9
        )

        update_startdate = rail.RepliconServiceOperator(
            task_id="update_startdate",
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": python_callable.split_startdate(dag_run)
                }
            }
        )

        if_loginname_mismatch = rail.IfOperator(
            task_id="if_loginname_mismatch",
            test="{{ result('get_user_data')[0].securityConfiguration.loginName.lower() != dag_run.conf.LoginName.lower() }}",
            yes_task="update_loginname",
            no_task="if_email_firstname_or_lastname_mismatch"
        )

        update_loginname = rail.RepliconServiceOperator(
            task_id = "update_loginname",
            endpoint = "/services/ImportService1.svc/ApplyUserModifications2",
            data = request_payload.update_user_loginname
        )

        if_email_firstname_or_lastname_mismatch = rail.IfOperator(
            task_id="if_email_firstname_or_lastname_mismatch",
            test="{{ result('get_user_data')[0].userDetails.emailAddress != dag_run.conf.EmailAddress or \
                result('get_user_data')[0].userDetails.firstName != dag_run.conf.FirstName or \
                result('get_user_data')[0].userDetails.lastName != dag_run.conf.LastName }}",
            yes_task="update_email_firstname_or_lastname",
            no_task="if_department_mismatch"
        )

        update_email_firstname_or_lastname = rail.RepliconServiceOperator(
            task_id = "update_email_firstname_or_lastname",
            endpoint = "/services/ImportService1.svc/ApplyUserModifications2",
            data= request_payload.update_email_firstname_or_lastname
        )

        if_department_mismatch = rail.IfOperator(
            task_id="if_department_mismatch",
            test="{{ dag_run.conf.Department | is_truthy and result('get_effectiveusergroupmembership').departments | is_truthy and \
                ( result('get_effectiveusergroupmembership').departments[0].department.department.displayText != dag_run.conf.Department) }}",
            yes_task="update_deparment",
            no_task="if_holiday_calendar_mismatch"
        )

        update_deparment = rail.RepliconServiceOperator(
            task_id = "update_deparment",
            endpoint = "/services/ImportService1.svc/ApplyUserModifications2",
            data = request_payload.department_update_payload
        )

        if_holiday_calendar_mismatch = rail.IfOperator(
            task_id="if_holiday_calendar_mismatch",
            test="{{ dag_run.conf.HolidayCalendar | is_truthy and \
                dag_run.conf.HolidayCalendar != result('get_user_data')[0].holidayCalendar.name }}",
            yes_task="update_holiday_calendar",
            no_task="if_timezone_mismatch"
        )

        update_holiday_calendar = rail.RepliconServiceOperator(
            task_id = "update_holiday_calendar",
            endpoint = "/services/ImportService1.svc/ApplyUserModifications2",
            data = request_payload.holiday_calendar_update_payload
        )

        if_timezone_mismatch = rail.IfOperator(
            task_id="if_timezone_mismatch",
            test="{{ dag_run.conf.Timezone | is_truthy and \
                  dag_run.conf.Timezone != result('get_user_data')[0].timeZone.displayText }}",
            yes_task="update_timezone",
            no_task="if_location_mismatch"
        )

        update_timezone = rail.RepliconServiceOperator(
            task_id = "update_timezone",
            endpoint = "/services/ImportService1.svc/ApplyUserModifications2",
            data = request_payload.timezone_update_payload
        )

        if_location_mismatch = rail.IfOperator(
            task_id="if_location_mismatch",
            test="{{ dag_run.conf.Location | is_truthy and result('get_effectiveusergroupmembership').locations | is_truthy and \
                    dag_run.conf.Location != result('get_effectiveusergroupmembership').locations[0].location.location.displayText and \
                    dag_run.conf.locationuri | is_truthy }}",
            yes_task="update_location",
            no_task="if_division_mismatch"
        )

        update_location = rail.RepliconServiceOperator(
            task_id = "update_location",
            endpoint = "/services/ImportService1.svc/ApplyUserModifications2",
            data = request_payload.location_update_payload
        )

        get_enabled_time_off_types_24 = rail.RepliconServiceOperator(
            task_id='get_enabled_time_off_types_24',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
            data_handler= response_filter.timeofftypes_to_assign_24
        )

        if_timeoff_string_present = rail.IfOperator(
            task_id="if_timeoff_string_present",
            test="{{ result('get_enabled_time_off_types_24').timeoff_string | is_truthy }}",
            yes_task="update_timeoff_types_for_user",
            no_task="if_division_mismatch"
        )

        update_timeoff_types_for_user = rail.RepliconServiceOperator(
            task_id='update_timeoff_types_for_user',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUris": rail.result('get_enabled_time_off_types_24')['timeoff_string']
            }
        )

        foreach_timeofflist_28 = rail.ForEachOperator(
            task_id='foreach_timeofflist_28',
            items="{{ result('get_enabled_time_off_types_24').timeofflist | to_json }}",
            start_task='get_default_timeoff_policy_schedule_for_user',
            end_task='foreach_timeofflist_28_end'
        )

        get_default_timeoff_policy_schedule_for_user = rail.RepliconServiceOperator(
            task_id='get_default_timeoff_policy_schedule_for_user',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ result('foreach_timeofflist_28').uri }}"
                }
            },
            data_handler=lambda response: json.loads(json.dumps([x for x in response if x['policySet']], ensure_ascii=False).replace(
                'null', '"effective"').replace('"script"', '"scriptTarget"')) if response and response[0] and response[0]['policySet'] else ''
        )

        if_default_policy_schedule_present = rail.IfOperator(
            task_id="if_default_policy_schedule_present",
            test="{{ result('get_default_timeoff_policy_schedule_for_user') | is_truthy }}",
            yes_task="update_user_timeOff_account_policyset_schedule",
            no_task="foreach_timeofflist_28_end"
        )

        update_user_timeOff_account_policyset_schedule = rail.RepliconServiceOperator(
            task_id='update_user_timeOff_account_policyset_schedule',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_timeofflist_28')['uri']
                },
                "policySetScheduleEntries": rail.result('get_default_timeoff_policy_schedule_for_user')
            }
        )

        foreach_timeofflist_28_end = rail.EmptyOperator(
            task_id='foreach_timeofflist_28_end'
        )

        if_division_mismatch = rail.IfOperator(
            task_id="if_division_mismatch",
            test="{{ dag_run.conf.division | is_truthy and result('get_effectiveusergroupmembership').divisions | is_truthy and \
                    dag_run.conf.division != result('get_effectiveusergroupmembership').divisions[0].division.division.displayText }}",
            yes_task="update_division_schedule",
            no_task="if_employee_type_mismatch"
        )

        update_division_schedule = rail.RepliconServiceOperator(
            task_id = "update_division_schedule",
            endpoint = "/services/ImportService1.svc/ApplyUserModifications2",
            data = request_payload.division_update_payload
        )

        if_employee_type_mismatch = rail.IfOperator(
            task_id="if_employee_type_mismatch",
            test="{{ dag_run.conf.EmployeeType | is_truthy and result('get_effectiveusergroupmembership').employeeTypes | is_truthy and \
                    dag_run.conf.EmployeeType != result('get_effectiveusergroupmembership').employeeTypes[0].employeeType.employeeType.displayText }}",
            yes_task="update_employee_type",
            no_task="if_workweek_present"
        )

        update_employee_type = rail.RepliconServiceOperator(
            task_id = "update_employee_type",
            endpoint = "/services/ImportService1.svc/ApplyUserModifications2",
            data = request_payload.employee_type_update_payload
        )

        if_workweek_present = rail.IfOperator(
            task_id="if_workweek_present",
            test="{{ dag_run.conf.Workweek | is_truthy }}",
            yes_task="get_workweek_value",
            no_task="if_officesheduleuri_present_and_mismatch"
        )

        get_workweek_value = rail.PythonOperator(
            task_id = "get_workweek_value",
            python_callable=lambda dag_run: dag_run.conf['Workweek'].split('-')[0].strip().lower()
        )

        if_workweek_mismatch = rail.IfOperator(
            task_id="if_workweek_mismatch",
            test="{{ result('get_user_data')[0].userDetails.workWeekStartDay.displayText.find(result('get_workweek_value')) < 0 }}",
            yes_task="update_workweek_start_day_for_user",
            no_task="if_officesheduleuri_present_and_mismatch"
        )

        update_workweek_start_day_for_user = rail.RepliconServiceOperator(
            task_id='update_workweek_start_day_for_user',
            endpoint="/services/UserService1.svc/UpdateWorkWeekStartDayForUser",
            data={
                'userUri': "{{ dag_run.conf.useruri }}",
                "dayOfWeekUri": "urn:replicon:day-of-week:{{ result('get_workweek_value') }}"
            }
        )

        if_officesheduleuri_present_and_mismatch = rail.IfOperator(
            task_id="if_officesheduleuri_present_and_mismatch",
            test="{{ dag_run.conf.officescheduleuri | is_truthy  and \
                    result('get_user_data')[0].schedulePolicies[0].officeSchedule.displayText != dag_run.conf.InitialScheduleName }}",
            yes_task="update_office_schedule",
            no_task="if_anualsalary_and_annualuri"
        )

        update_office_schedule = rail.RepliconServiceOperator(
            task_id = "update_office_schedule",
            endpoint = "/services/ImportService1.svc/ApplyUserModifications2",
            data = request_payload.office_schedule_update_payload
        )

        if_anualsalary_and_annualuri = rail.IfOperator(
            task_id="if_anualsalary_and_annualuri",
            test="{{ dag_run.conf.AnnualSalary | is_truthy and dag_run.conf.annualuri | is_truthy }}",
            yes_task="foreach_user_data",
            no_task="if_elt_and_elturi_present"
        )

        foreach_user_data = rail.ForEachOperator(
            task_id='foreach_user_data',
            items="{{ result('get_user_data') | to_json }}",
            start_task='foreach_user_details_custom_field',
            end_task='foreach_user_data_end'
        )

        foreach_user_details_custom_field = rail.ForEachOperator(
            task_id='foreach_user_details_custom_field',
            items="{{ result('foreach_user_data').userDetails.customFieldValues | to_json }}",
            start_task='check_uri_and_customuri',
            end_task='foreach_user_details_custom_field_end'
        )

        check_uri_and_customuri = rail.IfOperator(
            task_id="check_uri_and_customuri",
            test="{{ result('foreach_user_details_custom_field').customField.uri == dag_run.conf.annualuri and \
                 result('foreach_user_details_custom_field').text != dag_run.conf.AnnualSalary }}",
            yes_task="update_annualsalary_udf",
            no_task="foreach_user_details_custom_field_end"
        )

        update_annualsalary_udf = rail.RepliconServiceOperator(
            task_id='update_annualsalary_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.annualuri }}",
                "value": "{{ dag_run.conf.AnnualSalary }}"
            }
        )

        foreach_user_details_custom_field_end = rail.EmptyOperator(
            task_id='foreach_user_details_custom_field_end'
        )

        foreach_user_data_end = rail.EmptyOperator(
            task_id='foreach_user_data_end'
        )

        if_elt_and_elturi_present = rail.IfOperator(
            task_id="if_elt_and_elturi_present",
            test="{{ dag_run.conf.ELT | is_truthy and dag_run.conf.elturi | is_truthy }}",
            yes_task="foreach_user_data_49",
            no_task="if_firstlineuri_and_supervisorid_present"
        )

        foreach_user_data_49 = rail.ForEachOperator(
            task_id='foreach_user_data_49',
            items="{{ result('get_user_data') | to_json }}",
            start_task='foreach_user_details_custom_field_50',
            end_task='foreach_user_data_49_end'
        )

        foreach_user_details_custom_field_50 = rail.ForEachOperator(
            task_id='foreach_user_details_custom_field_50',
            items="{{ result('foreach_user_data_49').userDetails.customFieldValues | to_json }}",
            start_task='check_elt_and_elturi',
            end_task='foreach_user_details_custom_field_50_end'
        )

        check_elt_and_elturi = rail.IfOperator(
            task_id="check_elt_and_elturi",
            test="{{ result('foreach_user_details_custom_field_50').customField.uri == dag_run.conf.elturi and \
                 result('foreach_user_details_custom_field_50').text != dag_run.conf.ELT }}",
            yes_task="update_elt_udf",
            no_task="foreach_user_details_custom_field_50_end"
        )

        update_elt_udf = rail.RepliconServiceOperator(
            task_id='update_elt_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.elturi }}",
                "value": "{{ dag_run.conf.ELT }}"
            }
        )

        foreach_user_details_custom_field_50_end = rail.EmptyOperator(
            task_id='foreach_user_details_custom_field_50_end'
        )

        foreach_user_data_49_end = rail.EmptyOperator(
            task_id='foreach_user_data_49_end'
        )

        if_firstlineuri_and_supervisorid_present = rail.IfOperator(
            task_id="if_firstlineuri_and_supervisorid_present",
            test="{{ dag_run.conf.firstlineuri | is_truthy and dag_run.conf.SupervisorID | is_truthy }}",
            yes_task="get_user_details_from_supervisorid_54",
            no_task="if_secondlineuri_and_secondlinemanager_present"
        )

        get_user_details_from_supervisorid_54 = rail.RepliconServiceOperator(
            task_id='get_user_details_from_supervisorid_54',
            endpoint='/services/UserListService1.svc/GetData',
            data=request_payload.get_user_details_from_supervisorid_54,
            data_handler=response_filter.get_supervisor_details
        )

        if_supervisorid_present_55 = rail.IfOperator(
            task_id="if_supervisorid_present_55",
            test="{{ result('get_user_details_from_supervisorid_54').supervisor | is_truthy }}",
            yes_task="foreach_user_data_58",
            no_task="if_secondlineuri_and_secondlinemanager_present"
        )

        foreach_user_data_58 = rail.ForEachOperator(
            task_id='foreach_user_data_58',
            items="{{ result('get_user_data') | to_json }}",
            start_task='foreach_user_details_custom_field_59',
            end_task='foreach_user_data_58_end'
        )

        foreach_user_details_custom_field_59 = rail.ForEachOperator(
            task_id='foreach_user_details_custom_field_59',
            items="{{ result('foreach_user_data_58').userDetails.customFieldValues | to_json }}",
            start_task='check_firstlineuri_and_formattedname',
            end_task='foreach_user_details_custom_field_59_end'
        )

        check_firstlineuri_and_formattedname = rail.IfOperator(
            task_id="check_firstlineuri_and_formattedname",
            test="{{ result('foreach_user_details_custom_field_59').customField.uri == dag_run.conf.firstlineuri and \
                 result('foreach_user_details_custom_field_59').text != result('get_user_details_from_supervisorid_54').formattedname }}",
            yes_task="update_formatted_name_udf",
            no_task="foreach_user_details_custom_field_59_end"
        )

        update_formatted_name_udf = rail.RepliconServiceOperator(
            task_id='update_formatted_name_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.firstlineuri }}",
                "value": "{{ result('get_user_details_from_supervisorid_54').formattedname }}"
            }
        )

        foreach_user_details_custom_field_59_end = rail.EmptyOperator(
            task_id='foreach_user_details_custom_field_59_end'
        )

        foreach_user_data_58_end = rail.EmptyOperator(
            task_id='foreach_user_data_58_end'
        )

        if_secondlineuri_and_secondlinemanager_present = rail.IfOperator(
            task_id="if_secondlineuri_and_secondlinemanager_present",
            test="{{ dag_run.conf.secondlineuri | is_truthy and dag_run.conf.secondlinemanager | is_truthy }}",
            yes_task="foreach_user_data_63",
            no_task="if_businesscarduri_and_businesscardtitle_present"
        )

        foreach_user_data_63 = rail.ForEachOperator(
            task_id='foreach_user_data_63',
            items="{{ result('get_user_data') | to_json }}",
            start_task='foreach_user_details_custom_field_64',
            end_task='foreach_user_data_63_end'
        )

        foreach_user_details_custom_field_64 = rail.ForEachOperator(
            task_id='foreach_user_details_custom_field_64',
            items="{{ result('foreach_user_data_63').userDetails.customFieldValues | to_json }}",
            start_task='check_secondlineuri_and_secondlinemanager',
            end_task='foreach_user_details_custom_field_64_end'
        )

        check_secondlineuri_and_secondlinemanager = rail.IfOperator(
            task_id="check_secondlineuri_and_secondlinemanager",
            test="{{ result('foreach_user_details_custom_field_64').customField.uri == dag_run.conf.secondlineuri and \
                 result('foreach_user_details_custom_field_64').text != dag_run.conf.secondlinemanager }}",
            yes_task="update_secondline_manager_udf",
            no_task="foreach_user_details_custom_field_64_end"
        )

        update_secondline_manager_udf = rail.RepliconServiceOperator(
            task_id='update_secondline_manager_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.secondlineuri }}",
                "value": "{{ dag_run.conf.secondlinemanager }}"
            }
        )

        foreach_user_details_custom_field_64_end = rail.EmptyOperator(
            task_id='foreach_user_details_custom_field_64_end'
        )

        foreach_user_data_63_end = rail.EmptyOperator(
            task_id='foreach_user_data_63_end'
        )

        if_businesscarduri_and_businesscardtitle_present = rail.IfOperator(
            task_id="if_businesscarduri_and_businesscardtitle_present",
            test="{{ dag_run.conf.businesscardtitleuri | is_truthy and dag_run.conf.businesscardtitle | is_truthy }}",
            yes_task="foreach_user_data_68",
            no_task="if_workweekuri_workhour_present"
        )

        foreach_user_data_68 = rail.ForEachOperator(
            task_id='foreach_user_data_68',
            items="{{ result('get_user_data') | to_json }}",
            start_task='foreach_user_details_custom_field_69',
            end_task='foreach_user_data_68_end'
        )

        foreach_user_details_custom_field_69 = rail.ForEachOperator(
            task_id='foreach_user_details_custom_field_69',
            items="{{ result('foreach_user_data_68').userDetails.customFieldValues | to_json }}",
            start_task='check_businesscardtitleuri_and_businesscardtitle',
            end_task='foreach_user_details_custom_field_69_end'
        )

        check_businesscardtitleuri_and_businesscardtitle = rail.IfOperator(
            task_id="check_businesscardtitleuri_and_businesscardtitle",
            test="{{ result('foreach_user_details_custom_field_69').customField.uri == dag_run.conf.businesscardtitleuri and \
                 result('foreach_user_details_custom_field_69').text != dag_run.conf.businesscardtitle }}",
            yes_task="update_businesscardtitle_udf",
            no_task="foreach_user_details_custom_field_69_end"
        )

        update_businesscardtitle_udf = rail.RepliconServiceOperator(
            task_id='update_businesscardtitle_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.businesscardtitleuri }}",
                "value": "{{ dag_run.conf.businesscardtitle }}"
            }
        )

        foreach_user_details_custom_field_69_end = rail.EmptyOperator(
            task_id='foreach_user_details_custom_field_69_end'
        )

        foreach_user_data_68_end = rail.EmptyOperator(
            task_id='foreach_user_data_68_end'
        )

        if_workweekuri_workhour_present = rail.IfOperator(
            task_id="if_workweekuri_workhour_present",
            test="{{ dag_run.conf.businesscardtitleuri | is_truthy and dag_run.conf.businesscardtitle | is_truthy }}",
            yes_task="foreach_user_data_73",
            no_task="if_permissionsets_present"
        )

        foreach_user_data_73 = rail.ForEachOperator(
            task_id='foreach_user_data_73',
            items="{{ result('get_user_data') | to_json }}",
            start_task='foreach_user_details_custom_field_74',
            end_task='foreach_user_data_73_end'
        )

        foreach_user_details_custom_field_74 = rail.ForEachOperator(
            task_id='foreach_user_details_custom_field_74',
            items="{{ result('foreach_user_data_73').userDetails.customFieldValues | to_json }}",
            start_task='check_workweekuri_and_workhours',
            end_task='foreach_user_details_custom_field_74_end'
        )

        check_workweekuri_and_workhours = rail.IfOperator(
            task_id="check_workweekuri_and_workhours",
            test="{{ result('foreach_user_details_custom_field_74').customField.uri == dag_run.conf.workweekuri and \
                 result('foreach_user_details_custom_field_74').text != dag_run.conf.workweekhours }}",
            yes_task="update_workweek_dropdown_valueuri_udf",
            no_task="foreach_user_details_custom_field_74_end"
        )

        update_workweek_dropdown_valueuri_udf = rail.RepliconServiceOperator(
            task_id='update_workweek_dropdown_valueuri_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.workweekuri }}",
                "customFieldDropDownOptionUri": "{{ dag_run.conf.workweek_dropdown_valueuri }}"
            }
        )

        foreach_user_details_custom_field_74_end = rail.EmptyOperator(
            task_id='foreach_user_details_custom_field_74_end'
        )

        foreach_user_data_73_end = rail.EmptyOperator(
            task_id='foreach_user_data_73_end'
        )

        if_permissionsets_present = rail.IfOperator(
            task_id="if_permissionsets_present",
            test="{{ dag_run.conf.PermissionSets | is_truthy }}",
            yes_task="get_permissionsets_78",
            no_task="if_supervisorid_present_92"
        )

        get_permissionsets_78 = rail.PythonOperator(
            task_id = "get_permissionsets_78",
            python_callable=request_payload.get_permissionsets_78
        )

        get_permissionsets_80 = rail.PythonOperator(
            task_id = "get_permissionsets_80",
            python_callable=request_payload.get_permissionsets_80
        )

        get_all_permissionsets_81 = rail.RepliconServiceOperator(
            task_id='get_all_permissionsets_81',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets'
        )

        if_value_78_present = rail.IfOperator(
            task_id="if_value_78_present",
            test=lambda: bool(rail.result('get_permissionsets_78')),
            yes_task="foreach_permissionsets_82",
            no_task="get_permission_to_assign_uri_90"
        )

        foreach_permissionsets_82 = rail.ForEachOperator(
            task_id='foreach_permissionsets_82',
            items="{{ result('get_permissionsets_80') | to_json }}",
            start_task='get_desired_permissionset_is_assigned',
            end_task='foreach_permissionsets_82_end'
        )

        get_desired_permissionset_is_assigned = rail.PythonOperator(
            task_id = "get_desired_permissionset_is_assigned",
            python_callable=lambda : rail.find_first_by_attr_and_get_attr(
                rail.result('get_permissionsets_78'), 'value',
                rail.result('foreach_permissionsets_82')['value'], 'value', '')
        )

        if_desired_permissionset_is_assigned_not_present = rail.IfOperator(
            task_id = "if_desired_permissionset_is_assigned_not_present",
            test=lambda: not bool(rail.result('get_desired_permissionset_is_assigned')),
            yes_task="get_permissionset_uri_85",
            no_task="foreach_permissionsets_82_end"
        )

        get_permissionset_uri_85 = rail.PythonOperator(
            task_id = "get_permissionset_uri_85",
            python_callable=lambda : rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_permissionsets_81'), 'name',
                rail.result('foreach_permissionsets_82')['value'], 'uri', '')
        )

        permissionset_uri_present_86 = rail.IfOperator(
            task_id = "permissionset_uri_present_86",
            test=lambda: bool(rail.result('get_permissionset_uri_85')),
            yes_task="assign_permissionset_to_user",
            no_task="foreach_permissionsets_82_end"
        )

        assign_permissionset_to_user = rail.RepliconServiceOperator(
            task_id='assign_permissionset_to_user',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data={
                'userUri': "{{ dag_run.conf.useruri }}",
                'permissionSetUri': '{{ result("get_permissionset_uri_85") }}'
            }
        )

        foreach_permissionsets_82_end = rail.EmptyOperator(
            task_id='foreach_permissionsets_82_end'
        )

        def get_permissionseturis():
            uris = []
            for item in rail.result('get_permissionsets_80'):
                uri = rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_permissionsets_81'), 'name', item['value'], 'uri'
                )
                if uri:
                    uris.append(uri)
            return uris

        get_permission_to_assign_uri_90 = rail.PythonOperator(
            task_id = "get_permission_to_assign_uri_90",
            python_callable=get_permissionseturis
        )

        put_permissions_user_91 = rail.RepliconServiceOperator(
            task_id='put_permissions_user_91',
            endpoint="/services/PermissionSetService1.svc/PutPermissionSetAssignmentsForUser",
            data=lambda dag_run: {
                'userUri': dag_run.conf["useruri"],
                "permissionSetUris": rail.result('get_permission_to_assign_uri_90')
            }
        )

        if_supervisorid_present_92 = rail.IfOperator(
            task_id="if_supervisorid_present_92",
            test="{{ dag_run.conf.SupervisorID | is_truthy }}",
            yes_task="get_user_details_from_supervisorid_93",
            no_task="add_to_lookup_table"
        )

        get_user_details_from_supervisorid_93 = rail.RepliconServiceOperator(
            task_id='get_user_details_from_supervisorid_93',
            endpoint='/services/UserListService1.svc/GetData',
            data=request_payload.get_user_details_from_supervisorid_93,
            data_handler=response_filter.check_if_user_exists
        )

        get_user_details_from_supervisorid_95 = rail.RepliconServiceOperator(
            task_id='get_user_details_from_supervisorid_95',
            endpoint='/services/UserListService1.svc/GetData',
            data=request_payload.get_user_details_from_supervisorid_95,
            data_handler=lambda response: response['rows'][0]['cells'][0]['uri'] if response['rows'] else None
        )

        if_current_supervisor_assigned = rail.IfOperator(
            task_id="if_current_supervisor_assigned",
            test="{{ result('get_user_details_from_supervisorid_95') | is_truthy }}",
            yes_task="if_user_details_from_supervisorid_93_present",
            no_task="if_user_details_from_supervisorid_93_present_115"
        )

        if_user_details_from_supervisorid_93_present = rail.IfOperator(
            task_id="if_user_details_from_supervisorid_93_present",
            test="{{ result('get_user_details_from_supervisorid_93') | is_truthy }}",
            yes_task="if_supervisor_mismatch_99",
            no_task="if_loginname_present_110"
        )

        if_supervisor_mismatch_99 = rail.IfOperator(
            task_id="if_supervisor_mismatch_99",
            test="{{ result('get_user_details_from_supervisorid_93') != result('get_user_details_from_supervisorid_95') }}",
            yes_task="get_permissions_to_assign_100",
            no_task="add_to_lookup_table"
        )

        get_permissions_to_assign_100 = rail.RepliconServiceOperator(
            task_id='get_permissions_to_assign_100',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                'userUri': "{{ result('get_user_details_from_supervisorid_93') }}"
            },
            data_handler=response_filter.get_permissions_to_assign_user
        )

        if_supervisor_present_102 = rail.IfOperator(
            task_id="if_supervisor_present_102",
            test="{{ result('get_permissions_to_assign_100') | is_truthy }}",
            yes_task="assign_supervisor_103",
            no_task="get_all_permissionset_105"
        )

        assign_supervisor_103 = rail.RepliconServiceOperator(
            task_id='assign_supervisor_103',
            endpoint='/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'supervisorUri': rail.result('get_user_details_from_supervisorid_93'),
                'dateRange': {
                    'startDate': get_current_date_time()
                }
            }
        )

        get_all_permissionset_105 = rail.RepliconServiceOperator(
            task_id='get_all_permissionset_105',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets',
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'name', 'Supervisor', 'uri')
        )

        assign_permissionset_to_user_107 = rail.RepliconServiceOperator(
            task_id='assign_permissionset_to_user_107',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data={
                'userUri': "{{ result('get_user_details_from_supervisorid_93') }}",
                'permissionSetUri': '{{ result("get_all_permissionset_105") }}'
            }
        )

        assign_supervisor_108 = rail.RepliconServiceOperator(
            task_id='assign_supervisor_108',
            endpoint='/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'supervisorUri': rail.result('get_user_details_from_supervisorid_93'),
                'dateRange': {
                    'startDate': get_current_date_time()
                }
            }
        )

        if_loginname_present_110 = rail.IfOperator(
            task_id="if_loginname_present_110",
            test="{{ dag_run.conf.LoginName | is_truthy }}",
            yes_task="trigger_create_user_supervisor_111",
            no_task="add_to_lookup_table"
        )

        trigger_create_user_supervisor_111 = rail.TriggerDagRunOperator(
            task_id='trigger_create_user_supervisor_111',
            trigger_dag_id=config.create_supervisor_child,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.trigger_user_supervisor_dag
        )

        wait_for_trigger_create_user_supervisor_111 = rail.WaitForDagRunsSensor(
            task_id='wait_for_trigger_create_user_supervisor_111',
            dag_runs='{{ result("trigger_create_user_supervisor_111") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        #STEP 112-113 IS NOT CLEAR

        if_user_details_from_supervisorid_93_present_115 = rail.IfOperator(
            task_id="if_user_details_from_supervisorid_93_present_115",
            test="{{ result('get_user_details_from_supervisorid_93') | is_truthy }}",
            yes_task="get_permissions_to_assign_116",
            no_task="if_employeeid_and_loginname_present_126"
        )

        get_permissions_to_assign_116 = rail.RepliconServiceOperator(
            task_id='get_permissions_to_assign_116',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                'userUri': "{{ result('get_user_details_from_supervisorid_93') }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'permissionSet.name', 'Supervisor', 'permissionSet')
        )

        if_supervisor_permission_assigned = rail.IfOperator(
            task_id="if_supervisor_permission_assigned",
            test="{{ result('get_permissions_to_assign_116') | is_truthy }}",
            yes_task="assign_supervisor_119",
            no_task="get_all_permissionset_121"
        )

        assign_supervisor_119 = rail.RepliconServiceOperator(
            task_id='assign_supervisor_119',
            endpoint='/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'supervisorUri': rail.result('get_user_details_from_supervisorid_93'),
                'dateRange': {
                    'startDate': get_current_date_time()
                }
            }
        )

        get_all_permissionset_121 = rail.RepliconServiceOperator(
            task_id='get_all_permissionset_121',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets',
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'name', 'Supervisor', 'uri')
        )

        assign_permissionset_to_user_123 = rail.RepliconServiceOperator(
            task_id='assign_permissionset_to_user_123',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data={
                'userUri': "{{ result('get_user_details_from_supervisorid_93') }}",
                'permissionSetUri': '{{ result("get_all_permissionset_121") }}'
            }
        )

        assign_supervisor_124 = rail.RepliconServiceOperator(
            task_id='assign_supervisor_124',
            endpoint='/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'supervisorUri': rail.result('get_user_details_from_supervisorid_93'),
                'dateRange': {
                    'startDate': get_current_date_time()
                }
            }
        )

        if_employeeid_and_loginname_present_126 = rail.IfOperator(
            task_id="if_employeeid_and_loginname_present_126",
            test="{{ dag_run.conf.EmployeeId | is_truthy and dag_run.conf.LoginName | is_truthy }}",
            yes_task="trigger_create_user_supervisor_128",
            no_task="add_to_lookup_table"
        )

        trigger_create_user_supervisor_128 = rail.TriggerDagRunOperator(
            task_id='trigger_create_user_supervisor_128',
            trigger_dag_id=config.create_supervisor_child,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.trigger_user_supervisor_dag
        )

        wait_for_trigger_create_user_supervisor_128 = rail.WaitForDagRunsSensor(
            task_id='wait_for_trigger_create_user_supervisor_128',
            dag_runs='{{ result("trigger_create_user_supervisor_128") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        add_to_lookup_table = rail.WriteLogOperator(
            task_id='add_to_lookup_table',
            log = "{{ dag_run.conf.gee_user_import_lookup_table }}",
            message="na",
            severity="Success",
            properties={
                "loginname" : "{{ dag_run.conf.LoginName }}",
                "empid" : "{{ dag_run.conf.EmployeeId }}",
                "action" : "Update",
                "status" : "Success",
                "details" : "",
                "jobid" : "{{ dag_run.conf.calling_dag_id }}",
                "childjobid" : "{{ dag_run_ecid() }}"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log = "{{ dag_run.conf.gee_user_import_lookup_table }}",
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                "loginname" : "{{ dag_run.conf.FirstName }} {{ dag_run.conf.LastName }}",
                "empid" : "{{ dag_run.conf.EmployeeId }}",
                "action" : "Update",
                "status" : "Failed",
                "details" : "{{ get_error_message() }}",
                "jobid" : "{{ dag_run.conf.calling_dag_id }}",
                "childjobid" : "{{ dag_run_ecid() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )
        #STEP 112-113 IS NOT CLEAR

        get_user_data >> get_effectiveusergroupmembership >> is_enabled_not_true >> rail.Label(
            'Yes') >> add_entry_to_log_6 >> catch_and_log_errors
        is_enabled_not_true >> rail.Label(
            'No') >> update_user_data_9 >> update_startdate >> if_loginname_mismatch >> rail.Label(
            'Yes') >> update_loginname >> if_email_firstname_or_lastname_mismatch
        if_loginname_mismatch >> rail.Label(
            'No') >> if_email_firstname_or_lastname_mismatch >> rail.Label(
            'Yes') >> update_email_firstname_or_lastname >> if_department_mismatch
        if_email_firstname_or_lastname_mismatch >> rail.Label(
            'No') >> if_department_mismatch >> rail.Label(
            'Yes') >> update_deparment >> if_holiday_calendar_mismatch
        if_department_mismatch >> rail.Label(
            'No') >> if_holiday_calendar_mismatch >> rail.Label(
            'Yes') >> update_holiday_calendar >> if_timezone_mismatch
        if_holiday_calendar_mismatch >> rail.Label(
            'No') >> if_timezone_mismatch >> rail.Label(
            'Yes') >> update_timezone >> if_location_mismatch
        if_timezone_mismatch >> rail.Label(
            'No') >> if_location_mismatch >> rail.Label(
            'Yes') >> update_location >> get_enabled_time_off_types_24 >> if_timeoff_string_present >> rail.Label(
            'Yes') >> update_timeoff_types_for_user >> foreach_timeofflist_28 >> get_default_timeoff_policy_schedule_for_user >> \
        if_default_policy_schedule_present >> rail.Label(
            'Yes') >> update_user_timeOff_account_policyset_schedule >> foreach_timeofflist_28_end
        if_default_policy_schedule_present >> rail.Label(
            'No') >> foreach_timeofflist_28_end
        foreach_timeofflist_28 >> foreach_timeofflist_28_end >> if_division_mismatch
        if_timeoff_string_present >> rail.Label(
            'No') >> if_division_mismatch 
        if_location_mismatch >> rail.Label(
            'No') >> if_division_mismatch >> rail.Label(
            'Yes') >> update_division_schedule >> if_employee_type_mismatch
        if_division_mismatch >> rail.Label(
            'No') >> if_employee_type_mismatch >> rail.Label(
            'Yes') >> update_employee_type >> if_workweek_present
        if_employee_type_mismatch >> rail.Label(
            'No') >> if_workweek_present >> rail.Label(
            'Yes') >> get_workweek_value >> if_workweek_mismatch >> rail.Label(
            'Yes') >> update_workweek_start_day_for_user >> if_officesheduleuri_present_and_mismatch
        if_workweek_mismatch >> rail.Label(
            'No') >> if_officesheduleuri_present_and_mismatch
        if_workweek_present >> rail.Label(
            'No') >> if_officesheduleuri_present_and_mismatch >> rail.Label(
            'Yes') >> update_office_schedule >> if_anualsalary_and_annualuri
        if_officesheduleuri_present_and_mismatch >> rail.Label(
            'No') >> if_anualsalary_and_annualuri >> rail.Label(
            'Yes') >> foreach_user_data >> foreach_user_details_custom_field >> check_uri_and_customuri >> rail.Label(
            'Yes') >> update_annualsalary_udf >> foreach_user_details_custom_field_end
        check_uri_and_customuri >> rail.Label(
            'No') >> foreach_user_details_custom_field_end
        foreach_user_details_custom_field >> foreach_user_details_custom_field_end >> foreach_user_data_end
        foreach_user_data >> foreach_user_data_end >> if_elt_and_elturi_present
        if_anualsalary_and_annualuri >> rail.Label(
            'No') >> if_elt_and_elturi_present >> rail.Label(
            'Yes') >> foreach_user_data_49 >> foreach_user_details_custom_field_50 >> check_elt_and_elturi >> rail.Label(
            'Yes') >> update_elt_udf >> foreach_user_details_custom_field_50_end
        check_elt_and_elturi >> rail.Label(
            'No') >> foreach_user_details_custom_field_50_end
        foreach_user_details_custom_field_50 >> foreach_user_details_custom_field_50_end >> foreach_user_data_49_end
        foreach_user_data_49 >> foreach_user_data_49_end >> if_firstlineuri_and_supervisorid_present
        if_elt_and_elturi_present >> rail.Label(
            'No') >> if_firstlineuri_and_supervisorid_present >> rail.Label(
            'Yes') >> get_user_details_from_supervisorid_54 >> if_supervisorid_present_55 >> rail.Label(
            'Yes') >> foreach_user_data_58 >> foreach_user_details_custom_field_59 >> check_firstlineuri_and_formattedname >> rail.Label(
            'Yes') >> update_formatted_name_udf >> foreach_user_details_custom_field_59_end
        check_firstlineuri_and_formattedname >> rail.Label(
            'No') >> foreach_user_details_custom_field_59_end
        foreach_user_details_custom_field_59 >> foreach_user_details_custom_field_59_end >> foreach_user_data_58_end
        foreach_user_data_58 >> foreach_user_data_58_end >> if_secondlineuri_and_secondlinemanager_present
        if_supervisorid_present_55 >> rail.Label(
            'No') >> if_secondlineuri_and_secondlinemanager_present
        if_firstlineuri_and_supervisorid_present >> rail.Label(
            'No') >> if_secondlineuri_and_secondlinemanager_present >> rail.Label(
            'Yes') >> foreach_user_data_63 >> foreach_user_details_custom_field_64 >> check_secondlineuri_and_secondlinemanager >> rail.Label(
            'Yes') >> update_secondline_manager_udf >> foreach_user_details_custom_field_64_end
        check_secondlineuri_and_secondlinemanager >> rail.Label(
            'No') >> foreach_user_details_custom_field_64_end
        foreach_user_details_custom_field_64 >> foreach_user_details_custom_field_64_end >> foreach_user_data_63_end
        foreach_user_data_63 >> foreach_user_data_63_end >> if_businesscarduri_and_businesscardtitle_present
        if_secondlineuri_and_secondlinemanager_present >> rail.Label(
            'No') >> if_businesscarduri_and_businesscardtitle_present >> rail.Label(
            'Yes') >> foreach_user_data_68 >> foreach_user_details_custom_field_69 >> check_businesscardtitleuri_and_businesscardtitle >> rail.Label(
            'Yes') >> update_businesscardtitle_udf >> foreach_user_details_custom_field_69_end
        check_businesscardtitleuri_and_businesscardtitle >> rail.Label(
            'No') >> foreach_user_details_custom_field_69_end
        foreach_user_details_custom_field_69 >> foreach_user_details_custom_field_69_end >> foreach_user_data_68_end
        foreach_user_data_68 >> foreach_user_data_68_end >> if_workweekuri_workhour_present
        if_businesscarduri_and_businesscardtitle_present >> rail.Label(
            'No') >> if_workweekuri_workhour_present >> rail.Label(
            'Yes') >> foreach_user_data_73 >> foreach_user_details_custom_field_74 >> check_workweekuri_and_workhours >> rail.Label(
            'Yes') >> update_workweek_dropdown_valueuri_udf >> foreach_user_details_custom_field_74_end
        check_workweekuri_and_workhours >> rail.Label(
            'No') >> foreach_user_details_custom_field_74_end
        foreach_user_details_custom_field_74 >> foreach_user_details_custom_field_74_end >> foreach_user_data_73_end
        foreach_user_data_73 >> foreach_user_data_73_end >> if_permissionsets_present
        if_workweekuri_workhour_present >> rail.Label(
            'No') >> if_permissionsets_present >> rail.Label(
            'Yes') >> get_permissionsets_78 >> get_permissionsets_80 >> get_all_permissionsets_81 >> if_value_78_present >> rail.Label(
            'Yes') >>  foreach_permissionsets_82 >> get_desired_permissionset_is_assigned >> \
        if_desired_permissionset_is_assigned_not_present >> rail.Label(
            'Yes') >> get_permissionset_uri_85 >> permissionset_uri_present_86 >> rail.Label(
            'Yes') >> assign_permissionset_to_user >> foreach_permissionsets_82_end
        permissionset_uri_present_86 >> rail.Label(
            'No') >> foreach_permissionsets_82_end
        if_desired_permissionset_is_assigned_not_present >> rail.Label(
            'No') >> foreach_permissionsets_82_end
        foreach_permissionsets_82 >> foreach_permissionsets_82_end >> if_supervisorid_present_92
        if_value_78_present >> rail.Label(
            'No') >> get_permission_to_assign_uri_90 >> put_permissions_user_91 >> if_supervisorid_present_92
        if_permissionsets_present >> rail.Label(
            'No') >> if_supervisorid_present_92 >> rail.Label(
            'Yes') >> get_user_details_from_supervisorid_93 >> get_user_details_from_supervisorid_95 >> if_current_supervisor_assigned >> rail.Label(
            'Yes') >> if_user_details_from_supervisorid_93_present >> rail.Label(
            'Yes') >> if_supervisor_mismatch_99 >> rail.Label(
            'Yes') >> get_permissions_to_assign_100 >> if_supervisor_present_102 >> rail.Label(
            'Yes') >> assign_supervisor_103 >> add_to_lookup_table
        if_supervisor_present_102 >> rail.Label(
            'No') >> get_all_permissionset_105 >> assign_permissionset_to_user_107 >> assign_supervisor_108 >> add_to_lookup_table
        if_supervisor_mismatch_99 >> rail.Label(
            'No') >> add_to_lookup_table
        if_user_details_from_supervisorid_93_present >> rail.Label(
            'No') >> if_loginname_present_110 >> rail.Label(
            'Yes') >> trigger_create_user_supervisor_111 >> wait_for_trigger_create_user_supervisor_111 >> add_to_lookup_table
        if_loginname_present_110 >> rail.Label(
            'No') >> add_to_lookup_table
        if_current_supervisor_assigned >> rail.Label(
            'No') >> if_user_details_from_supervisorid_93_present_115 >> rail.Label(
            'Yes') >> get_permissions_to_assign_116 >> if_supervisor_permission_assigned >> rail.Label(
            'Yes') >> assign_supervisor_119 >> add_to_lookup_table
        if_supervisor_permission_assigned >> rail.Label(
            'No') >> get_all_permissionset_121 >> assign_permissionset_to_user_123 >> assign_supervisor_124 >> add_to_lookup_table
        if_user_details_from_supervisorid_93_present_115 >> rail.Label(
            'No') >> if_employeeid_and_loginname_present_126 >> rail.Label(
            'Yes') >> trigger_create_user_supervisor_128 >> wait_for_trigger_create_user_supervisor_128 >> add_to_lookup_table
        if_employeeid_and_loginname_present_126 >> rail.Label(
            'No') >> add_to_lookup_table
        if_supervisorid_present_92 >> rail.Label(
            'No') >> add_to_lookup_table
        add_to_lookup_table >> catch_and_log_errors >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
