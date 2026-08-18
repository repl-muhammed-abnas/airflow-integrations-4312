from datetime import timedelta, datetime
from sunovion.user_import.mappers.sunovion_mapper_file import sunovion_mapper
from sunovion.user_import.utils import request_payload
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'sunovion_user_import_update_user_child_{config.instance}',
        description=f'Sunovion_Child_Update User {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
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
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='log_user_i_d_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_user_i_d_3',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_user_i_d_3 = rail.PythonOperator(
            task_id='log_user_i_d_3',
            python_callable=lambda dag_run: (
                (dag_run.conf['useruri']).split(":"))[-1]
        )

        get_user_details_4 = rail.RepliconServiceOperator(
            task_id='get_user_details_4',
            endpoint='/services/UserService1.svc/GetUserDetails',
            data={
                "userUri": '{{ dag_run.conf.useruri }}'
            }
        )

        generate_reportforuserdetails_5 = rail.RepliconReportDetailsOperator(
            task_id='generate_reportforuserdetails_5',
            report_name=config.user_details_report
        )

        run_user_details_report = rail.run_report2(
            group_id="run_user_details_report",
            report_params=lambda: {
                "reportParameters": [
                    {
                        "reportUri": rail.result('generate_reportforuserdetails_5')['uri'],
                        "filterValues": [
                            {
                                "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result(
                                    'generate_reportforuserdetails_5')['filterConfiguration']['enabledFilters'], 'displayText', 'UserFilter', 'uri', ''),
                                "value": rail.result('log_user_i_d_3'),
                            }
                        ],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv",
                    }
                ]
            },
            target='artifact'
        )

        parse_csv_6 = rail.LoadCSVFileOperator(
            task_id='parse_csv_6',
            document="{{(result('run_user_details_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload}}",
            headers=['loginname','userfirstname','userlastname','employeetype','userdepartmentname','userstatus','employeeid','userstartdate',
                     'vacationaccrualdate','workdayemployeetype','workdayexecutive','scheduledhoursperweek','userenddate','useremail',
                     'usersupervisornamecurrent','timesheettemplate','timesheetperiodtype','timesheetapprovalpath','holidaycalendar',
                     'schedulenamecurrent','timeoffapprovalpath','paygroupscurrent','residentstatecurrent','costcentercurrent','adminmodified',
                     'useruri','payrulenamecurrent','employeetypegroupcurrent'
                     ]
        )

        load_user_details_from_report = rail.PythonOperator(
            task_id='load_user_details_from_report',
            python_callable=lambda: (rail.load_all_records(
                rail.result('parse_csv_6')))[0]
        )


        log_todaysdate_7 = rail.PythonOperator(
            task_id='log_todaysdate_7',
            python_callable=request_payload.get_todays_date
        )

        if_request_firstname_present_11 = rail.IfOperator(
            task_id='if_request_firstname_present_11',
            test=lambda dag_run: dag_run.conf['firstname'] and (
                dag_run.conf['firstname'] != rail.result('load_user_details_from_report')['userfirstname']),
            yes_task="update_first_name_12",
            no_task="if_request_lastname_present_13",
        )

        update_first_name_12 = rail.RepliconServiceOperator(
            task_id='update_first_name_12',
            endpoint="/services/UserService1.svc/UpdateFirstName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "firstname": "{{ dag_run.conf.firstname }}"
            }
        )

        if_request_lastname_present_13 = rail.IfOperator(
            task_id='if_request_lastname_present_13',
            test=lambda dag_run: dag_run.conf['lastname'] and (
                dag_run.conf['lastname'] != rail.result('load_user_details_from_report')['userlastname']),
            yes_task="update_last_name_14",
            no_task="if_request_employeeid_present_15",
        )

        update_last_name_14 = rail.RepliconServiceOperator(
            task_id='update_last_name_14',
            endpoint="/services/UserService1.svc/UpdateLastName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "lastname": "{{ dag_run.conf.lastname }}"
            }
        )

        if_request_employeeid_present_15 = rail.IfOperator(
            task_id='if_request_employeeid_present_15',
            test=lambda dag_run: dag_run.conf['employeeid'] and (
                dag_run.conf['employeeid'] != rail.result('load_user_details_from_report')['employeeid']),
            yes_task="update_employee_id_16",
            no_task="if_request_emailaddress_present_17",
        )

        update_employee_id_16 = rail.RepliconServiceOperator(
            task_id='update_employee_id_16',
            endpoint="/services/UserService1.svc/UpdateEmployeeId",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "employeeId": "{{ dag_run.conf.employeeid }}"
            }
        )

        if_request_emailaddress_present_17 = rail.IfOperator(
            task_id='if_request_emailaddress_present_17',
            test=lambda dag_run: dag_run.conf['emailaddress'] and (
                dag_run.conf['emailaddress'] != rail.result('load_user_details_from_report')['useremail']),
            yes_task="update_email_18",
            no_task="adhoc_http_action_19",
        )

        update_email_18 = rail.RepliconServiceOperator(
            task_id='update_email_18',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "email": "{{ dag_run.conf.emailaddress }}"
            }
        )

        adhoc_http_action_19 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_19',
            endpoint="/services/CustomFieldService1.svc/GetCustomFieldGroups",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'User', 'uri', '')
        )

        if_log_getrequired_usergroupuri_20_present_21 = rail.IfOperator(
            task_id='if_log_getrequired_usergroupuri_20_present_21',
            test='''{{ result('adhoc_http_action_19') | is_truthy }}''',
            yes_task="adhoc_http_action_22",
            no_task="if_request_startdate_present_48",
        )

        adhoc_http_action_22 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_22',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "{{ result('adhoc_http_action_19') }}"
            },
            data_handler=lambda response: {
                'scheduledhrsperweekuri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Scheduled Hours Per Week', 'uri', ''),
                'workdayemployeetypeuri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Workday Employee Type', 'uri', ''),
                'workdayexecutiveuri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Workday Executive', 'uri', ''),
                'vacationaccrualdateuri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Vacation Accrual Date', 'uri', ''),
            }
        )

        if_request_scheduledhoursperweek_present_23 = rail.IfOperator(
            task_id='if_request_scheduledhoursperweek_present_23',
            test=lambda dag_run: dag_run.conf['scheduledhoursperweek'] and (
                dag_run.conf['scheduledhoursperweek'] != rail.result('load_user_details_from_report')['scheduledhoursperweek']),
            yes_task="if_log_getrequired_scheduled_hours_per_weekudfuri_24_present_25",
            no_task="if_request_workdayemployeetype_present_27",
        )

        if_log_getrequired_scheduled_hours_per_weekudfuri_24_present_25 = rail.IfOperator(
            task_id='if_log_getrequired_scheduled_hours_per_weekudfuri_24_present_25',
            test='''{{ result('adhoc_http_action_22').scheduledhrsperweekuri | is_truthy }}''',
            yes_task="update_text_valuefor_scheduled_hours_per_weekudf_26",
            no_task="if_request_workdayemployeetype_present_27",
        )

        update_text_valuefor_scheduled_hours_per_weekudf_26 = rail.RepliconServiceOperator(
            task_id='update_text_valuefor_scheduled_hours_per_weekudf_26',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('adhoc_http_action_22').scheduledhrsperweekuri }}",
                "value": "{{ dag_run.conf.scheduledhoursperweek }}"
            }
        )

        if_request_workdayemployeetype_present_27 = rail.IfOperator(
            task_id='if_request_workdayemployeetype_present_27',
            test=lambda dag_run: dag_run.conf['workdayemployeetype'] and (
                dag_run.conf['workdayemployeetype'] != rail.result('load_user_details_from_report')['workdayemployeetype']),
            yes_task="if_log_getrequired_workday_employee_typeudfuri_28_present_29",
            no_task="if_request_workdayexecutive_present_34",
        )

        if_log_getrequired_workday_employee_typeudfuri_28_present_29 = rail.IfOperator(
            task_id='if_log_getrequired_workday_employee_typeudfuri_28_present_29',
            test='''{{ result('adhoc_http_action_22').workdayemployeetypeuri | is_truthy }}''',
            yes_task="adhoc_http_action_30",
            no_task="if_request_workdayexecutive_present_34",
        )

        adhoc_http_action_30 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_30',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('adhoc_http_action_22').workdayemployeetypeuri }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['workdayemployeetype'], 'uri', '')
        )

        if_log_getrequired_workday_employee_typedropdownuri_31_present_32 = rail.IfOperator(
            task_id='if_log_getrequired_workday_employee_typedropdownuri_31_present_32',
            test='''{{ result('adhoc_http_action_30') | is_truthy }}''',
            yes_task="update_dropdown_valuefor_workday_employee_typeudf_33",
            no_task="if_request_workdayexecutive_present_34",
        )

        update_dropdown_valuefor_workday_employee_typeudf_33 = rail.RepliconServiceOperator(
            task_id='update_dropdown_valuefor_workday_employee_typeudf_33',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('adhoc_http_action_22').workdayemployeetypeuri }}",
                "customFieldDropDownOptionUri": "{{ result('adhoc_http_action_30') }}"
            }
        )

        if_request_workdayexecutive_present_34 = rail.IfOperator(
            task_id='if_request_workdayexecutive_present_34',
            test=lambda dag_run: dag_run.conf['workdayexecutive'] and (
                dag_run.conf['workdayexecutive'] != rail.result('load_user_details_from_report')['workdayexecutive']),
            yes_task="if_log_getrequired_workday_executiveudfuri_35_present_36",
            no_task="if_request_vacationaccrualdate_present_41",
        )

        if_log_getrequired_workday_executiveudfuri_35_present_36 = rail.IfOperator(
            task_id='if_log_getrequired_workday_executiveudfuri_35_present_36',
            test='''{{ result('adhoc_http_action_22').workdayexecutiveuri | is_truthy }}''',
            yes_task="adhoc_http_action_37",
            no_task="if_request_vacationaccrualdate_present_41",
        )

        adhoc_http_action_37 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_37',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('adhoc_http_action_22').workdayexecutiveuri }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['workdayexecutive'], 'uri', '')
        )

        if_log_getrequired_workday_executivedropdownuri_38_present_39 = rail.IfOperator(
            task_id='if_log_getrequired_workday_executivedropdownuri_38_present_39',
            test='''{{ result('adhoc_http_action_37') | is_truthy }}''',
            yes_task="update_dropdown_valuefor_workday_employee_typeudf_40",
            no_task="if_request_vacationaccrualdate_present_41",
        )

        update_dropdown_valuefor_workday_employee_typeudf_40 = rail.RepliconServiceOperator(
            task_id='update_dropdown_valuefor_workday_employee_typeudf_40',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('adhoc_http_action_22').workdayexecutiveuri }}",
                "customFieldDropDownOptionUri": "{{ result('adhoc_http_action_37') }}"
            }
        )

        if_request_vacationaccrualdate_present_41 = rail.IfOperator(
            task_id='if_request_vacationaccrualdate_present_41',
            test=lambda dag_run: dag_run.conf['vacationaccrualdate'] and (
                dag_run.conf['vacationaccrualdate'] != rail.result('load_user_details_from_report')['vacationaccrualdate']),
            yes_task="if_log_getrequired_vacation_accrual_dateudfuri_42_present_43",
            no_task="if_request_startdate_present_48",
        )

        if_log_getrequired_vacation_accrual_dateudfuri_42_present_43 = rail.IfOperator(
            task_id='if_log_getrequired_vacation_accrual_dateudfuri_42_present_43',
            test='''{{ result('adhoc_http_action_22').vacationaccrualdateuri | is_truthy }}''',
            yes_task="log_required_vacation_accrual_date_day_44",
            no_task="if_request_startdate_present_48",
        )

        log_required_vacation_accrual_date_day_44 = rail.PythonOperator(
            task_id='log_required_vacation_accrual_date_day_44',
            python_callable=lambda dag_run: request_payload.get_date_object(
                dag_run.conf['vacationaccrualdate'])
        )

        update_date_valuefor_vacation_accrual_dateudf_47 = rail.RepliconServiceOperator(
            task_id='update_date_valuefor_vacation_accrual_dateudf_47',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('adhoc_http_action_22').vacationaccrualdateuri }}",
                "value": {
                    "year": "{{ result('log_required_vacation_accrual_date_day_44').year }}",
                    "month": "{{ result('log_required_vacation_accrual_date_day_44').month }}",
                    "day": "{{ result('log_required_vacation_accrual_date_day_44').day }}"
                }
            }
        )

        if_request_startdate_present_48 = rail.IfOperator(
            task_id='if_request_startdate_present_48',
            test=lambda dag_run: dag_run.conf['startdate'] and (
                dag_run.conf['startdate'] != rail.result('load_user_details_from_report')['userstartdate']),
            yes_task="log_required_start_date_day_49",
            no_task="if_request_enddate_present_62",
        )

        log_required_start_date_day_49 = rail.PythonOperator(
            task_id='log_required_start_date_day_49',
            python_callable=lambda dag_run: request_payload.get_date_object(
                dag_run.conf['startdate'])
        )

        update_employment_date_range_52 = rail.RepliconServiceOperator(
            task_id='update_employment_date_range_52',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ result('log_required_start_date_day_49').year }}",
                        "month": "{{ result('log_required_start_date_day_49').month }}",
                        "day": "{{ result('log_required_start_date_day_49').day }}"
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_request_vacationaccrualdate_present_53 = rail.IfOperator(
            task_id='if_request_vacationaccrualdate_present_53',
            test=lambda dag_run: dag_run.conf['vacationaccrualdate'] and (dag_run.conf['vacationaccrualdate'] == rail.result(
                'load_user_details_from_report')['userstartdate']) and rail.result('load_user_details_from_report')['userenddate'],
            yes_task="log_startdateday_54",
            no_task="if_request_enddate_present_62",
        )

        log_startdateday_54 = rail.PythonOperator(
            task_id='log_startdateday_54',
            python_callable=lambda dag_run:  request_payload.get_date_object(
                dag_run.conf['startdate'])
        )

        log_differencebetweennewstartdateandcurrentenddate_57 = rail.PythonOperator(
            task_id='log_differencebetweennewstartdateandcurrentenddate_57',
            python_callable=lambda dag_run: (datetime.strptime(dag_run.conf['startdate'], '%m/%d/%Y') - datetime.strptime(
                rail.result('load_user_details_from_report')['userenddate'], '%m/%d/%Y')).days
        )

        if_log_differencebetweennewstartdateandcurrentenddate_57_greater_than_365_58 = rail.IfOperator(
            task_id='if_log_differencebetweennewstartdateandcurrentenddate_57_greater_than_365_58',
            test='''{{ result('log_differencebetweennewstartdateandcurrentenddate_57') > 365 }}''',
            yes_task="trigger_workflow_to_update_timeoff_type_for_existing_user_rehire",
            no_task="if_request_enddate_present_62",
        )

        trigger_workflow_to_update_timeoff_type_for_existing_user_rehire = rail.TriggerDagRunOperator(
            task_id='trigger_workflow_to_update_timeoff_type_for_existing_user_rehire',
            retries=0,
            trigger_dag_id=f'sunovion_user_import_child_workflow_to_update_timeoff_type_for_existing_user_rehire_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "callerjobid": "{{ dag_run.conf.callerjobid }}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "userloginname": "{{ dag_run.conf.loginname }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "workdayemployeetype": "{{ dag_run.conf.workdayemployeetype }}",
                "workdayexecutive": "{{ dag_run.conf.workdayexecutive }}",
                "employeetype": "{{ dag_run.conf.employeetype }}",
                "location": "{{ dag_run.conf.residentstate }}"
            }
        )

        wait_for_workflow_to_update_timeoff_type_for_existing_user_rehire = rail.WaitForDagRunsSensor(
            task_id='wait_for_workflow_to_update_timeoff_type_for_existing_user_rehire',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_workflow_to_update_timeoff_type_for_existing_user_rehire") }}'
        )

        if_request_enddate_present_62 = rail.IfOperator(
            task_id='if_request_enddate_present_62',
            test=lambda dag_run: dag_run.conf['enddate'] and not (
                rail.result('load_user_details_from_report')['userenddate']),
            yes_task="log_start_dateday_63",
            no_task="if_request_enabled_present_75",
        )

        log_start_dateday_63 = rail.PythonOperator(
            task_id='log_start_dateday_63',
            python_callable=lambda dag_run: request_payload.get_date_object(
                dag_run.conf['startdate'])
        )

        log_end_date_day_68 = rail.PythonOperator(
            task_id='log_end_date_day_68',
            python_callable=lambda dag_run: request_payload.get_date_object(
                dag_run.conf['enddate'])
        )

        if_log_end_date_totimeformat_72_greater_than_dataloggerlog_start_datetotimeformat_67message_73 = rail.IfOperator(
            task_id='if_log_end_date_totimeformat_72_greater_than_dataloggerlog_start_datetotimeformat_67message_73',
            test=lambda dag_run: datetime.strptime(
                dag_run.conf['enddate'], "%m/%d/%Y") > datetime.strptime(dag_run.conf['startdate'], "%m/%d/%Y"),
            yes_task="update_employment_date_range_74",
            no_task="if_request_enabled_present_75",
        )

        update_employment_date_range_74 = rail.RepliconServiceOperator(
            task_id='update_employment_date_range_74',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ result('log_start_dateday_63').year }}",
                        "month": "{{ result('log_start_dateday_63').month }}",
                        "day": "{{ result('log_start_dateday_63').day }}"
                    },
                    "endDate": {
                        "year": "{{ result('log_end_date_day_68').year }}",
                        "month": "{{ result('log_end_date_day_68').month }}",
                        "day": "{{ result('log_end_date_day_68').day }}"
                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_request_enabled_present_75 = rail.IfOperator(
            task_id='if_request_enabled_present_75',
            test=lambda dag_run: dag_run.conf['enabled'] and (
                dag_run.conf['enabled'] != rail.result('load_user_details_from_report')['userstatus']),
            yes_task="if_request_enabled_equals_to_yes_76",
            no_task="if_request_paygroup_present_85",
        )

        if_request_enabled_equals_to_yes_76 = rail.IfOperator(
            task_id='if_request_enabled_equals_to_yes_76',
            test='''{{ dag_run.conf.enabled == 'Yes' }}''',
            yes_task="adhoc_http_action_77",
            no_task="if_request_enabled_equals_to_no_83",
        )

        adhoc_http_action_77 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_77',
            endpoint="/services/SecurityService1.svc/EnableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        if_first_column_12_present_78 = rail.IfOperator(
            task_id='if_first_column_12_present_78',
            test='''{{ result('load_user_details_from_report').userenddate | is_truthy }}''',
            yes_task="log_start_dateday_79",
            no_task="if_request_enabled_equals_to_no_83",
        )

        log_start_dateday_79 = rail.PythonOperator(
            task_id='log_start_dateday_79',
            python_callable=lambda dag_run: request_payload.get_date_object(
                dag_run.conf['startdate'])
        )

        update_employment_date_range_82 = rail.RepliconServiceOperator(
            task_id='update_employment_date_range_82',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ result('log_start_dateday_79').year }}",
                        "month": "{{ result('log_start_dateday_79').month }}",
                        "day": "{{ result('log_start_dateday_79').day }}"
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_request_enabled_equals_to_no_83 = rail.IfOperator(
            task_id='if_request_enabled_equals_to_no_83',
            test='''{{ dag_run.conf.enabled == 'No' }}''',
            yes_task="adhoc_http_action_84",
            no_task="if_request_paygroup_present_85",
        )

        adhoc_http_action_84 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_84',
            endpoint="/services/SecurityService1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        if_request_paygroup_present_85 = rail.IfOperator(
            task_id='if_request_paygroup_present_85',
            test='''{{ dag_run.conf.paygroup | is_truthy  and dag_run.conf.paygroup != result('load_user_details_from_report').paygroupscurrent }}''',
            yes_task="if_request_paygroup_equals_emd_or_el8",
            no_task="if_request_employeetype_present_139",
        )

        if_request_paygroup_equals_emd_or_el8 = rail.IfOperator(
            task_id='if_request_paygroup_equals_emd_or_el8',
            test='''{{ dag_run.conf.paygroup == 'EMD' or dag_run.conf.paygroup == 'EL8' }}''',
            yes_task="set_s_s_o_authentication_for_user_87",
            no_task="if_request_paygroup_not_equals_to_el8_or_emd",
        )

        set_s_s_o_authentication_for_user_87 = rail.RepliconServiceOperator(
            task_id='set_s_s_o_authentication_for_user_87',
            endpoint="/services/SecurityService1.svc/SetSSOAuthenticationForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "loginName": "{{ dag_run.conf.loginname }}"
            }
        )

        if_request_paygroup_not_equals_to_el8_or_emd = rail.IfOperator(
            task_id='if_request_paygroup_not_equals_to_el8_or_emd',
            test='''{{ dag_run.conf.paygroup != 'EL8' and dag_run.conf.paygroup != 'EMD' }}''',
            yes_task="set_replicon_authentication_for_user_89",
            no_task="sunovion_mapper_file_search_entries_checkfordepartmentonthemapper_90",
        )

        set_replicon_authentication_for_user_89 = rail.RepliconServiceOperator(
            task_id='set_replicon_authentication_for_user_89',
            endpoint="/services/SecurityService1.svc/SetRepliconAuthenticationForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "loginName": "{{ dag_run.conf.loginname }}",
                "password": "Password123",
                "forcePasswordChangeOnLoginOption": "urn:replicon:force-password-change-on-login:enable"
            }
        )

        sunovion_mapper_file_search_entries_checkfordepartmentonthemapper_90 = rail.PythonOperator(
            task_id='sunovion_mapper_file_search_entries_checkfordepartmentonthemapper_90',
            python_callable=lambda dag_run:  list(filter(
                lambda x: x["type"] == "department" and x["identifier_1"] == dag_run.conf['paygroup'], sunovion_mapper))
        )

        log_pluckifthedepartmentispresent_91 = rail.PythonOperator(
            task_id='log_pluckifthedepartmentispresent_91',
            python_callable=lambda: rail.result('sunovion_mapper_file_search_entries_checkfordepartmentonthemapper_90')[0]['data_set'] if rail.result(
                'sunovion_mapper_file_search_entries_checkfordepartmentonthemapper_90') else ''
        )

        if_first_column_4_not_equals_to_dataloggerlog_pluckifthedepartmentispresent_91message_92 = rail.IfOperator(
            task_id='if_first_column_4_not_equals_to_dataloggerlog_pluckifthedepartmentispresent_91message_92',
            test='''{{ result('load_user_details_from_report').userdepartmentname != result('log_pluckifthedepartmentispresent_91') }}''',
            yes_task="adhoc_http_action_93",
            no_task="adhoc_http_action_112",
        )

        adhoc_http_action_93 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_93',
            endpoint="/services/DepartmentService1.svc/GetEnabledDepartments",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', rail.result('log_pluckifthedepartmentispresent_91'), 'uri', '')
        )

        if_log_departmenturi_94_present_95 = rail.IfOperator(
            task_id='if_log_departmenturi_94_present_95',
            test='''{{ result('adhoc_http_action_93') | is_truthy }}''',
            yes_task="update_department_for_user_96",
            no_task="sunovion_user_logs_file_add_entry_98",
        )

        update_department_for_user_96 = rail.RepliconServiceOperator(
            task_id='update_department_for_user_96',
            endpoint="/services/DepartmentService1.svc/UpdateDepartmentForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "departmentUri": "{{ result('adhoc_http_action_93') }}"
            }
        )

        sunovion_user_logs_file_add_entry_98 = rail.WriteLogOperator(
            task_id='sunovion_user_logs_file_add_entry_98',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="Error",
            properties={
                'parentjobid': "{{dag_run.conf.callerjobid}}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Error",
                #pylint: disable = line-too-long
                "failurereason": '''Department not added for User "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}". "{{ result('adhoc_http_action_93') }}" not available in Replicon.''',
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_first_column_24_not_equals_to_yes_99 = rail.IfOperator(
            task_id='if_first_column_24_not_equals_to_yes_99',
            test='''{{ result('load_user_details_from_report').adminmodified != 'Yes' }}''',
            yes_task="sunovion_mapper_file_search_entries_checkforlicensesonthemapper_100",
            no_task="adhoc_http_action_112",
        )

        sunovion_mapper_file_search_entries_checkforlicensesonthemapper_100 = rail.PythonOperator(
            task_id='sunovion_mapper_file_search_entries_checkforlicensesonthemapper_100',
            python_callable=lambda:  list(filter(lambda x: x["type"] == "licenses" and x["identifier_1"] == rail.result(
                'log_pluckifthedepartmentispresent_91'), sunovion_mapper))
        )

        log_pluckifthelicensesispresent_101 = rail.PythonOperator(
            task_id='log_pluckifthelicensesispresent_101',
            python_callable=lambda: rail.result('sunovion_mapper_file_search_entries_checkforlicensesonthemapper_100')[0]['data_set'] if rail.result(
                'sunovion_mapper_file_search_entries_checkforlicensesonthemapper_100') else ''
        )

        if_log_pluckifthelicensesispresent_101_present_102 = rail.IfOperator(
            task_id='if_log_pluckifthelicensesispresent_101_present_102',
            test='''{{ result('log_pluckifthelicensesispresent_101') | is_truthy }}''',
            yes_task="adhoc_http_action_103",
            no_task="adhoc_http_action_112",
        )

        adhoc_http_action_103 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_103',
            endpoint="/services/AccountManagementService1.svc/GetAllProductsAvailableForUserAssignment",
        )

        def get_required_products_uri():
            products_required = (rail.result(
                'log_pluckifthelicensesispresent_101')).split("|")
            return list(filter(None, map(lambda product: rail.find_first_by_attr_and_get_attr(rail.result(
                'adhoc_http_action_103'), 'displayText', product, 'uri') ,products_required) ))

        log_getnumberofproductstobeassigned_104 = rail.PythonOperator(
            task_id='log_getnumberofproductstobeassigned_104',
            python_callable=get_required_products_uri
        )

        put_product_assignments_for_user_111 = rail.RepliconServiceOperator(
            task_id='put_product_assignments_for_user_111',
            endpoint="/services/AccountManagementService1.svc/PutProductAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "productUris": rail.result('log_getnumberofproductstobeassigned_104')
            }
        )

        adhoc_http_action_112 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_112',
            endpoint="/services/ServiceCenterService1.svc/GetAllServiceCenters",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['paygroup'], 'uri', '')
        )

        if_log_required_paygroupuri_113_present_114 = rail.IfOperator(
            task_id='if_log_required_paygroupuri_113_present_114',
            test='''{{ result('adhoc_http_action_112') | is_truthy }}''',
            yes_task="if_first_column_21_blank_f4x_115",
            no_task="sunovion_user_logs_file_add_entry_138",
        )

        if_first_column_21_blank_f4x_115 = rail.IfOperator(
            task_id='if_first_column_21_blank_f4x_115',
            test='''{{ result('load_user_details_from_report').paygroupscurrent | is_falsy }}''',
            yes_task="log_paygroupeffectivedateday_116",
            no_task="if_first_column_21_present_f4x_120",
        )

        log_paygroupeffectivedateday_116 = rail.PythonOperator(
            task_id='log_paygroupeffectivedateday_116',
            python_callable=request_payload.get_todays_date
        )

        put_service_center_schedule_for_user_pay_group_119 = rail.RepliconServiceOperator(
            task_id='put_service_center_schedule_for_user_pay_group_119',
            endpoint="/services/ServiceCenterService1.svc/PutServiceCenterScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "scheduleEntries": [
                    {
                        "serviceCenter": {
                            "uri": "{{ result('adhoc_http_action_112') }}",
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": {
                            "year": "{{ result('log_paygroupeffectivedateday_116').year }}",
                            "month": "{{ result('log_paygroupeffectivedateday_116').month }}",
                            "day": "{{ result('log_paygroupeffectivedateday_116').day }}"
                        }
                    }
                ]
            }
        )

        if_first_column_21_present_f4x_120 = rail.IfOperator(
            task_id='if_first_column_21_present_f4x_120',
            test='''{{ result('load_user_details_from_report').paygroupscurrent | is_truthy }}''',
            yes_task="adhoc_http_action_121",
            no_task="if_request_employeetype_present_139",
        )

        def get_paygroups_list(response):
            paygroup_details = []
            initial_paygroup = []
            additional_paygroup = []
            for schedule in response:
                paygroup_details.append({
                    'displayText': schedule['serviceCenter']['displayText'],
                    'effectiveDate': request_payload.get_date_string_from_object(schedule['effectiveDate']) if schedule['effectiveDate'] else ''
                })
                if not (schedule['effectiveDate'] and schedule['effectiveDate']['day']):
                    initial_paygroup.append({
                        'serviceCenter': {
                            'uri': schedule['serviceCenter']['uri']
                        },
                        'effectiveDate': null
                    })
                else:
                    additional_paygroup.append({
                        'serviceCenter': {
                            'uri': schedule['serviceCenter']['uri']
                        },
                        'effectiveDate': {
                            'day': int(schedule['effectiveDate']['day']),
                            'month': int(schedule['effectiveDate']['month']),
                            'year': schedule['effectiveDate']['year']
                        }
                    })
                new_paygroup = [{
                    'serviceCenter': {
                        'uri': rail.result('adhoc_http_action_112')
                    },
                    'effectiveDate': {
                        'day': rail.result('log_todaysdate_7')['day'],
                        'month': rail.result('log_todaysdate_7')['month'],
                        'year': rail.result('log_todaysdate_7')['year']
                    }
                }]
                final_paygroup = additional_paygroup + new_paygroup
                final_paygroup = initial_paygroup + final_paygroup
                return final_paygroup

        adhoc_http_action_121 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_121',
            endpoint="/services/ServiceCenterService1.svc/GetServiceCenterScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=get_paygroups_list
        )

        put_service_center_schedule_for_user_pay_group_136 = rail.RepliconServiceOperator(
            task_id='put_service_center_schedule_for_user_pay_group_136',
            endpoint="/services/ServiceCenterService1.svc/PutServiceCenterScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": rail.result('adhoc_http_action_121')
            }
        )

        sunovion_user_logs_file_add_entry_138 = rail.WriteLogOperator(
            task_id='sunovion_user_logs_file_add_entry_138',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="Error",
            properties={
                'parentjobid': "{{dag_run.conf.callerjobid}}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Error",
                #pylint: disable = line-too-long
                "failurereason": '''User "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}" is updated, however paygroup is not updated as paygroup "{{ dag_run.conf.paygroup }}" is not available in Replicon''',
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_request_employeetype_present_139 = rail.IfOperator(
            task_id='if_request_employeetype_present_139',
            test="{{dag_run.conf.employeetype | is_truthy and dag_run.conf.employeetype != result('load_user_details_from_report').employeetypegroupcurrent}}",
            yes_task="adhoc_http_action_140",
            no_task="if_request_employeetype_present_160",
        )

        adhoc_http_action_140 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_140',
            endpoint="/services/DivisionService1.svc/GetAllDivisions",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['employeetype'], 'uri', '')
        )

        if_log_required_employeetypegroupuri_141_present_142 = rail.IfOperator(
            task_id='if_log_required_employeetypegroupuri_141_present_142',
            test='''{{ result('adhoc_http_action_140') | is_truthy }}''',
            yes_task="if_first_column_27_blank_f4x_143",
            no_task="if_request_employeetype_present_160",
        )

        if_first_column_27_blank_f4x_143 = rail.IfOperator(
            task_id='if_first_column_27_blank_f4x_143',
            test='''{{ result('load_user_details_from_report').employeetypegroupcurrent | is_falsy }}''',
            yes_task="put_division_schedule_for_user_employee_type_group_144",
            no_task="if_first_column_27_present_f4x_145",
        )

        put_division_schedule_for_user_employee_type_group_144 = rail.RepliconServiceOperator(
            task_id='put_division_schedule_for_user_employee_type_group_144',
            endpoint="/services/DivisionService1.svc/PutDivisionScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "scheduleEntries": [
                    {
                        "division": {
                            "uri": "{{ result('adhoc_http_action_140') }}",
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": {
                            "year": "{{ result('log_todaysdate_7').year }}",
                            "month": "{{ result('log_todaysdate_7').month }}",
                            "day": "{{ result('log_todaysdate_7').day }}"
                        }
                    }
                ]
            }
        )

        if_first_column_27_present_f4x_145 = rail.IfOperator(
            task_id='if_first_column_27_present_f4x_145',
            test='''{{ result('load_user_details_from_report').employeetypegroupcurrent | is_truthy }}''',
            yes_task="adhoc_http_action_146",
            no_task="if_request_employeetype_present_160",
        )

        def get_division_schedule(response):
            initial_employeetype = []
            additional_employeetype = []
            for schedule in response:
                if not (schedule['effectiveDate'] and schedule['effectiveDate']['day']):
                    initial_employeetype.append({
                        'division': {
                            'uri': schedule['division']['uri']
                        },
                        'effectiveDate': null
                    })
                else:
                    additional_employeetype.append({
                        'division': {
                            'uri': schedule['division']['uri']
                        },
                        'effectiveDate': {
                            'day': schedule['effectiveDate']['day'],
                            'month': schedule['effectiveDate']['month'],
                            'year': schedule['effectiveDate']['year']
                        }
                    })
            new_employeetype = [{
                'division': {
                    'uri': rail.result('adhoc_http_action_140')
                },
                'effectiveDate': {
                    'day': rail.result('log_todaysdate_7')['day'],
                    'month': rail.result('log_todaysdate_7')['month'],
                    'year': rail.result('log_todaysdate_7')['year']
                }
            }]
            final_schedule = additional_employeetype + new_employeetype
            final_schedule = initial_employeetype + final_schedule
            return final_schedule

        adhoc_http_action_146 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_146',
            endpoint="/services/DivisionService1.svc/GetDivisionScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=get_division_schedule
        )

        put_division_schedule_for_user_employee_type_group_159 = rail.RepliconServiceOperator(
            task_id='put_division_schedule_for_user_employee_type_group_159',
            endpoint="/services/DivisionService1.svc/PutDivisionScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": rail.result('adhoc_http_action_146')
            }
        )

        if_request_employeetype_present_160 = rail.IfOperator(
            task_id='if_request_employeetype_present_160',
            test='''{{ dag_run.conf.employeetype | is_truthy  and dag_run.conf.employeetype != result('load_user_details_from_report').employeetype }}''',
            yes_task="adhoc_http_action_161",
            no_task="if_request_supervisorid_present_214",
        )

        adhoc_http_action_161 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_161',
            endpoint="/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['employeetype'], 'uri', '')
        )

        if_log_requiredemployeetypeuri_162_present_163 = rail.IfOperator(
            task_id='if_log_requiredemployeetypeuri_162_present_163',
            test='''{{ result('adhoc_http_action_161') | is_truthy }}''',
            yes_task="update_employee_type_for_user_164",
            no_task="sunovion_user_logs_file_add_entry_166",
        )

        update_employee_type_for_user_164 = rail.RepliconServiceOperator(
            task_id='update_employee_type_for_user_164',
            endpoint="/services/EmployeeTypeService1.svc/UpdateEmployeeTypeForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "employeeTypeUri": "{{ result('adhoc_http_action_161') }}"
            }
        )

        sunovion_user_logs_file_add_entry_166 = rail.WriteLogOperator(
            task_id='sunovion_user_logs_file_add_entry_166',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="Error",
            properties={
                'parentjobid': "{{dag_run.conf.callerjobid}}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Error",
                #pylint: disable = line-too-long
                "failurereason": '''Employee type for user "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}" is  not updated as employee type "{{ dag_run.conf.employeetype }}" is not available in Replicon''',
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        sunovion_mapper_file_search_entries_checkfortimeoffapprovalpathonthemapper_167 = rail.PythonOperator(
            task_id='sunovion_mapper_file_search_entries_checkfortimeoffapprovalpathonthemapper_167',
            python_callable=lambda dag_run:  list(filter(
                lambda x: x["type"] == "timeoff approval path" and x["identifier_1"] == dag_run.conf['employeetype'], sunovion_mapper))
        )

        log_pluckifthetimeoffapprovalpathispresent_168 = rail.PythonOperator(
            task_id='log_pluckifthetimeoffapprovalpathispresent_168',
            python_callable=lambda: rail.result(
                'sunovion_mapper_file_search_entries_checkfortimeoffapprovalpathonthemapper_167')[0]['data_set'] if rail.result(
                'sunovion_mapper_file_search_entries_checkfortimeoffapprovalpathonthemapper_167') else ''
        )

        if_log_pluckifthetimeoffapprovalpathispresent_168_present_169 = rail.IfOperator(
            task_id='if_log_pluckifthetimeoffapprovalpathispresent_168_present_169',
            test='''{{ result('log_pluckifthetimeoffapprovalpathispresent_168') | is_truthy }}''',
            yes_task="if_log_pluckifthetimeoffapprovalpathispresent_168_not_equals_to_datacsv_parserparse_csv_6linesfirstcolumn_20_170",
            no_task="sunovion_user_logs_file_add_entry_176",
        )

        if_log_pluckifthetimeoffapprovalpathispresent_168_not_equals_to_datacsv_parserparse_csv_6linesfirstcolumn_20_170 = rail.IfOperator(
            task_id='if_log_pluckifthetimeoffapprovalpathispresent_168_not_equals_to_datacsv_parserparse_csv_6linesfirstcolumn_20_170',
            test='''{{ result('log_pluckifthetimeoffapprovalpathispresent_168') != result('load_user_details_from_report').timeoffapprovalpath }}''',
            yes_task="adhoc_http_action_171",
            no_task="adhoc_http_action_177",
        )

        adhoc_http_action_171 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_171',
            endpoint="/services/TimeOffApprovalService1.svc/GetAllApprovalPaths",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', rail.result('log_pluckifthetimeoffapprovalpathispresent_168'), 'uri', '')
        )

        if_log_timeoffapprovalpathuri_172_present_173 = rail.IfOperator(
            task_id='if_log_timeoffapprovalpathuri_172_present_173',
            test='''{{ result('adhoc_http_action_171') | is_truthy }}''',
            yes_task="update_timeoff_approval_path_for_user_174",
            no_task="adhoc_http_action_177",
        )

        update_timeoff_approval_path_for_user_174 = rail.RepliconServiceOperator(
            task_id='update_timeoff_approval_path_for_user_174',
            endpoint="/services/TimeOffApprovalService1.svc/UpdateApprovalPathForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "approvalPathUri": "{{ result('adhoc_http_action_171') }}"
            }
        )

        sunovion_user_logs_file_add_entry_176 = rail.WriteLogOperator(
            task_id='sunovion_user_logs_file_add_entry_176',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="Error",
            properties={
                'parentjobid': "{{dag_run.conf.callerjobid}}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Error",
                #pylint: disable = line-too-long
                "failurereason": '''Timeoff approval path not updated for User "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}". "Timeoff approval path" not available for employee type "{{ dag_run.conf.employeetype }}"  in mapper file''',
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        adhoc_http_action_177 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_177',
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts",
            data_handler=lambda response: {
                'sunovionpayrulenonexempt': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Sunovion Payrule Non-Exempt', 'uri', ''),
                'sunovionpayrulecanonexempt': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Sunovion Payrule CA Non-Exempt', 'uri', ''),
                'sunovionpayruleexempt': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Sunovion Payrule Exempt', 'uri', '')
            }
        )

        if_request_employeetype_equals_to_nonexempt_178 = rail.IfOperator(
            task_id='if_request_employeetype_equals_to_nonexempt_178',
            #pylint: disable = line-too-long
            test='''{{ dag_run.conf.employeetype == 'Non-Exempt' and result('load_user_details_from_report').payrulenamecurrent != 'Sunovion Payrule Non-Exempt' and result('load_user_details_from_report').payrulenamecurrent | is_truthy }}''',
            yes_task="if_log_required_payruleuri_179_present_180",
            no_task="if_request_employeetype_equals_to_nonexempt_and_payrulenamecurrent_notpresent",
        )

        if_log_required_payruleuri_179_present_180 = rail.IfOperator(
            task_id='if_log_required_payruleuri_179_present_180',
            test='''{{ result('adhoc_http_action_177').sunovionpayrulenonexempt | is_truthy }}''',
            yes_task="adhoc_http_action_181",
            no_task="if_request_employeetype_equals_to_nonexempt_and_payrulenamecurrent_notpresent",
        )

        def get_payrule_schedule(response, newpayruleuri):
            initial_payrule = []
            additional_payrule = []
            for schedule in response:
                if not (schedule['effectiveDate'] and schedule['effectiveDate']['day']):
                    initial_payrule.append({
                        'payRuleScript': {
                            'uri': schedule['payRuleScript']['uri']
                        },
                        'effectiveDate': null
                    })
                else:
                    additional_payrule.append({
                        'payRuleScript': {
                            'uri': schedule['payRuleScript']['uri']
                        },
                        'effectiveDate': {
                            'day': schedule['effectiveDate']['day'],
                            'month': schedule['effectiveDate']['month'],
                            'year': schedule['effectiveDate']['year']
                        }
                    })
            new_payrule = [{
                'payRuleScript': {
                    'uri': newpayruleuri
                },
                'effectiveDate': {
                    'day': rail.result('log_todaysdate_7')['day'],
                    'month': rail.result('log_todaysdate_7')['month'],
                    'year': rail.result('log_todaysdate_7')['year']
                }
            }]
            final_payrule = additional_payrule + new_payrule
            final_payrule = initial_payrule + final_payrule
            return final_payrule

        adhoc_http_action_181 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_181',
            endpoint="/services/PayRuleScriptService2.svc/GetPayRuleScriptAssignmentScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=lambda response: get_payrule_schedule(
                response, rail.result('adhoc_http_action_177')['sunovionpayrulenonexempt'])
        )

        put_pay_rule_script_assignment_schedule_for_user_191 = rail.RepliconServiceOperator(
            task_id='put_pay_rule_script_assignment_schedule_for_user_191',
            endpoint="/services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": rail.result('adhoc_http_action_181')
            }
        )

        if_request_employeetype_equals_to_nonexempt_and_payrulenamecurrent_notpresent = rail.IfOperator(
            task_id='if_request_employeetype_equals_to_nonexempt_and_payrulenamecurrent_notpresent',
            test='''{{ dag_run.conf.employeetype == 'Non-Exempt'  and result('load_user_details_from_report').payrulenamecurrent | is_falsy }}''',
            yes_task="if_log_required_payruleuri_179_present_194",
            no_task="if_request_employeetype_equals_to_ca_nonexempt_196",
        )

        if_log_required_payruleuri_179_present_194 = rail.IfOperator(
            task_id='if_log_required_payruleuri_179_present_194',
            test='''{{ result('adhoc_http_action_177').sunovionpayrulenonexempt | is_truthy }}''',
            yes_task="put_pay_rule_script_assignment_schedule_for_user_195",
            no_task="if_request_employeetype_equals_to_ca_nonexempt_196",
        )

        put_pay_rule_script_assignment_schedule_for_user_195 = rail.RepliconServiceOperator(
            task_id='put_pay_rule_script_assignment_schedule_for_user_195',
            endpoint="/services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "scheduleEntries": [
                    {
                        "payRuleScript": {
                            "uri": "{{ result('adhoc_http_action_177').sunovionpayrulenonexempt }}",
                            "name": null
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        if_request_employeetype_equals_to_ca_nonexempt_196 = rail.IfOperator(
            task_id='if_request_employeetype_equals_to_ca_nonexempt_196',
            #pylint: disable = line-too-long
            test='''{{ dag_run.conf.employeetype == 'CA Non-Exempt' and result('load_user_details_from_report').payrulenamecurrent != 'Sunovion Payrule Non-Exempt'  and result('load_user_details_from_report').payrulenamecurrent | is_truthy }}''',
            yes_task="if_log_required_payruleuri_197_present_198",
            no_task="if_request_employeetype_equals_to_ca_nonexempt_and_payrulenamecurrent_notpresent",
        )

        if_log_required_payruleuri_197_present_198 = rail.IfOperator(
            task_id='if_log_required_payruleuri_197_present_198',
            test='''{{ result('adhoc_http_action_177').sunovionpayrulenonexempt | is_truthy }}''',
            yes_task="adhoc_http_action_199",
            no_task="if_request_employeetype_equals_to_ca_nonexempt_and_payrulenamecurrent_notpresent",
        )

        adhoc_http_action_199 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_199',
            endpoint="/services/PayRuleScriptService2.svc/GetPayRuleScriptAssignmentScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=lambda response: get_payrule_schedule(
                response, rail.result('adhoc_http_action_177')['sunovionpayrulenonexempt'])
        )

        put_pay_rule_script_assignment_schedule_for_user_209 = rail.RepliconServiceOperator(
            task_id='put_pay_rule_script_assignment_schedule_for_user_209',
            endpoint="/services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": rail.result('adhoc_http_action_199')
            }
        )

        if_request_employeetype_equals_to_ca_nonexempt_and_payrulenamecurrent_notpresent = rail.IfOperator(
            task_id='if_request_employeetype_equals_to_ca_nonexempt_and_payrulenamecurrent_notpresent',
            test='''{{ dag_run.conf.employeetype == 'CA Non-Exempt'  and result('load_user_details_from_report').payrulenamecurrent | is_falsy }}''',
            yes_task="if_log_required_payruleuri_211_present_212",
            no_task="if_employeetype_equals_exempt_and_payrulenamecurrent_unequal_exempt",
        )

        if_log_required_payruleuri_211_present_212 = rail.IfOperator(
            task_id='if_log_required_payruleuri_211_present_212',
            test='''{{ result('adhoc_http_action_177').sunovionpayrulecanonexempt | is_truthy }}''',
            yes_task="put_pay_rule_script_assignment_schedule_for_user_213",
            no_task="if_employeetype_equals_exempt_and_payrulenamecurrent_unequal_exempt",
        )

        put_pay_rule_script_assignment_schedule_for_user_213 = rail.RepliconServiceOperator(
            task_id='put_pay_rule_script_assignment_schedule_for_user_213',
            endpoint="/services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "scheduleEntries": [
                    {
                        "payRuleScript": {
                            "uri": "{{ result('adhoc_http_action_177').sunovionpayrulecanonexempt }}",
                            "name": null
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        if_employeetype_equals_exempt_and_payrulenamecurrent_unequal_exempt = rail.IfOperator(
            task_id='if_employeetype_equals_exempt_and_payrulenamecurrent_unequal_exempt',
            test=lambda dag_run: dag_run.conf['employeetype'] == 'Exempt' and rail.result('load_user_details_from_report')[
                'payrulenamecurrent'] != 'Sunovion Payrule Exempt' and rail.result('load_user_details_from_report')['payrulenamecurrent'],
            yes_task='if_sunovion_payrule_exempt_uri_present',
            no_task='if_employeetype_equals_exempt_and_payrulenamecurrent_not_present'
        )

        if_sunovion_payrule_exempt_uri_present = rail.IfOperator(
            task_id='if_sunovion_payrule_exempt_uri_present',
            test=lambda: bool(rail.result('adhoc_http_action_177')[
                              'sunovionpayruleexempt']),
            yes_task='get_payrule_script_assignment_schedule_for_user',
            no_task='if_employeetype_equals_exempt_and_payrulenamecurrent_not_present'
        )

        get_payrule_script_assignment_schedule_for_user = rail.RepliconServiceOperator(
            task_id='get_payrule_script_assignment_schedule_for_user',
            endpoint="/services/PayRuleScriptService2.svc/GetPayRuleScriptAssignmentScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=lambda response: get_payrule_schedule(
                response, rail.result('adhoc_http_action_177')['sunovionpayruleexempt'])
        )

        put_payrule_script_assignment_schedule_for_user = rail.RepliconServiceOperator(
            task_id='put_payrule_script_assignment_schedule_for_user',
            endpoint="/services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": rail.result('get_payrule_script_assignment_schedule_for_user')
            }
        )

        if_employeetype_equals_exempt_and_payrulenamecurrent_not_present = rail.IfOperator(
            task_id='if_employeetype_equals_exempt_and_payrulenamecurrent_not_present',
            test=lambda dag_run: dag_run.conf['employeetype'] == 'Exempt' and not (
                rail.result('load_user_details_from_report')['payrulenamecurrent']),
            yes_task='if_sunovion_payrueleexempt_uri_present',
            no_task='if_request_supervisorid_present_214'
        )

        if_sunovion_payrueleexempt_uri_present = rail.IfOperator(
            task_id='if_sunovion_payrueleexempt_uri_present',
            test=lambda: bool(rail.result('adhoc_http_action_177')[
                              'sunovionpayruleexempt']),
            yes_task='putpayrule_script_schedule_assignment_for_user',
            no_task='if_request_supervisorid_present_214'
        )

        putpayrule_script_schedule_assignment_for_user = rail.RepliconServiceOperator(
            task_id='putpayrule_script_schedule_assignment_for_user',
            endpoint="/services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "scheduleEntries": [
                    {
                        "payRuleScript": {
                            "uri": "{{ result('adhoc_http_action_177').sunovionpayruleexempt }}",
                            "name": null
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        if_request_supervisorid_present_214 = rail.IfOperator(
            task_id='if_request_supervisorid_present_214',
            test=lambda dag_run: dag_run.conf['supervisorid'] and dag_run.conf['supervisorid'] != (rail.result('get_user_details_4')[
                'supervisor']['user']['loginName'] if rail.result('get_user_details_4') and rail.result(
                'get_user_details_4')['supervisor'] and rail.result('get_user_details_4')['supervisor']['user'] else ''),
            yes_task="if_request_supervisorid_not_equals_to_dataworkato_service3cd9c331requestloginname_215",
            no_task="if_request_permissionsets_present_224",
        )

        if_request_supervisorid_not_equals_to_dataworkato_service3cd9c331requestloginname_215 = rail.IfOperator(
            task_id='if_request_supervisorid_not_equals_to_dataworkato_service3cd9c331requestloginname_215',
            test='''{{ dag_run.conf.supervisorid != dag_run.conf.loginname }}''',
            yes_task="search_users_216",
            no_task="if_request_supervisorid_equals_to_dataworkato_service3cd9c331requestloginname_222",
        )

        def get_user_uri(response, dag_run):
            users_found = response['rows']
            matching_user = list(filter(
                lambda user: user['cells'][0]['textValue'] == dag_run.conf['supervisorid'], users_found))
            return {
                'uri': matching_user[0]['cells'][0]['uri'] if matching_user else ''
            }

        search_users_216 = rail.RepliconServiceOperator(
            task_id='search_users_216',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:enabled",
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "value": {
                            "text": "{{dag_run.conf.supervisorid}}"
                        }
                    }
                }
            },
            data_handler=get_user_uri
        )

        if_log_getsupervisor_uri_217_present_218 = rail.IfOperator(
            task_id='if_log_getsupervisor_uri_217_present_218',
            test='''{{ result('search_users_216').uri | is_truthy }}''',
            yes_task="update_supervisorwithtodayaseffectivedate_219",
            no_task="if_log_getsupervisor_uri_217_blank_220",
        )

        update_supervisorwithtodayaseffectivedate_219 = rail.RepliconServiceOperator(
            task_id='update_supervisorwithtodayaseffectivedate_219',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "supervisorUri": "{{ result('search_users_216').uri }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ result('log_todaysdate_7').year }}",
                        "month": "{{ result('log_todaysdate_7').month }}",
                        "day": "{{ result('log_todaysdate_7').day }}"
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_log_getsupervisor_uri_217_blank_220 = rail.IfOperator(
            task_id='if_log_getsupervisor_uri_217_blank_220',
            test='''{{ result('search_users_216').uri | is_falsy }}''',
            yes_task="sunovion_user_supervisor_mapping_table_add_entry_221",
            no_task="if_request_supervisorid_equals_to_dataworkato_service3cd9c331requestloginname_222",
        )

        sunovion_user_supervisor_mapping_table_add_entry_221 = rail.WriteLogOperator(
            task_id='sunovion_user_supervisor_mapping_table_add_entry_221',
            log="{{ dag_run.conf.supervisorlookuptable }}",
            message="na",
            severity="Error",
            properties={
                'jobid': "{{dag_run.conf.callerjobid}}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "supervisorid": "{{dag_run.conf.supervisorid}}",
                "status": "Error",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}"
            }
        )

        if_request_supervisorid_equals_to_dataworkato_service3cd9c331requestloginname_222 = rail.IfOperator(
            task_id='if_request_supervisorid_equals_to_dataworkato_service3cd9c331requestloginname_222',
            test='''{{ dag_run.conf.supervisorid == dag_run.conf.loginname }}''',
            yes_task="sunovion_user_logs_file_add_entry_223",
            no_task="if_request_permissionsets_present_224",
        )

        sunovion_user_logs_file_add_entry_223 = rail.WriteLogOperator(
            task_id='sunovion_user_logs_file_add_entry_223',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="Error",
            properties={
                'parentjobid': "{{dag_run.conf.callerjobid}}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Error",
                #pylint: disable = line-too-long
                "failurereason": '''Supervisor is not updated for user "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}" as the "Login name" for user and supervisor same on the input file''',
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_request_permissionsets_present_224 = rail.IfOperator(
            task_id='if_request_permissionsets_present_224',
            test='''{{ dag_run.conf.permissionsets | is_truthy }}''',
            yes_task="adhoc_http_action_225",
            no_task="if_request_residentstate_present_243",
        )

        adhoc_http_action_225 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_225',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=lambda response: [permissionset['permissionSet']['uri'] for permissionset in response if (permissionset['policyUri'] != 'urn:replicon:policy:user' and
                permissionset['policyUri'] != 'urn:replicon:policy:supervision')]
        )

        adhoc_http_action_232 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_232',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        def get_required_permissions_uri(dag_run):
            permissions_required = (dag_run.conf['permissionsets']).split(',')
            permissions_uri = [rail.find_first_by_attr_and_get_attr(rail.result('adhoc_http_action_232'), 'displayText', ((
                permission).strip()), 'uri', '') for permission in permissions_required]
            return [uri for uri in permissions_uri if uri != '']

        log_getnumberofpermissionstobeassigned_233 = rail.PythonOperator(
            task_id='log_getnumberofpermissionstobeassigned_233',
            python_callable=get_required_permissions_uri
        )

        if_log_permissionstobeassigned_240_present_pr_241 = rail.IfOperator(
            task_id='if_log_permissionstobeassigned_240_present_pr_241',
            test='''{{ result('log_getnumberofpermissionstobeassigned_233') | is_truthy }}''',
            yes_task="put_permission_set_assignments_for_user_242",
            no_task="if_request_residentstate_present_243",
        )

        put_permission_set_assignments_for_user_242 = rail.RepliconServiceOperator(
            task_id='put_permission_set_assignments_for_user_242',
            endpoint="/services/PermissionSetService1.svc/PutPermissionSetAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "permissionSetUris": rail.result('adhoc_http_action_225') + rail.result('log_getnumberofpermissionstobeassigned_233')
            }
        )

        if_request_residentstate_present_243 = rail.IfOperator(
            task_id='if_request_residentstate_present_243',
            test='''{{ dag_run.conf.residentstate | is_truthy  and dag_run.conf.paygroup | is_truthy }}''',
            yes_task="if_request_residentstate_not_equals_to_datacsv_parserparse_csv_6linesfirstcolumn_22_244",
            no_task="if_request_employeetype_present_257",
        )

        if_request_residentstate_not_equals_to_datacsv_parserparse_csv_6linesfirstcolumn_22_244 = rail.IfOperator(
            task_id='if_request_residentstate_not_equals_to_datacsv_parserparse_csv_6linesfirstcolumn_22_244',
            #pylint: disable = line-too-long
            test='''{{ dag_run.conf.residentstate != result('load_user_details_from_report').residentstatecurrent  or dag_run.conf.paygroup != result('load_user_details_from_report').paygroupscurrent }}''',
            yes_task="log_requiredresidentstare_249",
            no_task="if_request_employeetype_present_257",
        )

        log_requiredresidentstare_249 = rail.PythonOperator(
            task_id='log_requiredresidentstare_249',
            python_callable=lambda dag_run: 'Puerto Rico' if dag_run.conf[
                'residentstate'] == 'PR' else 'US'
        )

        sunovion_mapper_file_search_entries_checkforholidaycalendaronthemapper_250 = rail.PythonOperator(
            task_id='sunovion_mapper_file_search_entries_checkforholidaycalendaronthemapper_250',
            python_callable=lambda:  list(filter(
                lambda x: x["type"] == "holiday calendar" and x["identifier_1"] == "ALL" and x["identifier_2"] == "ALL", sunovion_mapper))
        )

        if_log_pluckiftheholidaycalendarispresentonthemapper_251_present_252 = rail.IfOperator(
            task_id='if_log_pluckiftheholidaycalendarispresentonthemapper_251_present_252',
            test='''{{ result('sunovion_mapper_file_search_entries_checkforholidaycalendaronthemapper_250') | is_truthy }}''',
            yes_task="adhoc_http_action_253",
            no_task="if_request_employeetype_present_257",
        )

        adhoc_http_action_253 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_253',
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response, 'displayText', (rail.result(
                'sunovion_mapper_file_search_entries_checkforholidaycalendaronthemapper_250')[0]['data_set'] if rail.result(
                'sunovion_mapper_file_search_entries_checkforholidaycalendaronthemapper_250') else ''), 'uri', '')
        )

        if_log_holidaycalendar_uri_254_present_pr_255 = rail.IfOperator(
            task_id='if_log_holidaycalendar_uri_254_present_pr_255',
            test='''{{ result('adhoc_http_action_253') | is_truthy }}''',
            yes_task="update_holiday_calendar_256",
            no_task="if_request_employeetype_present_257",
        )

        update_holiday_calendar_256 = rail.RepliconServiceOperator(
            task_id='update_holiday_calendar_256',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "holidayCalendarUri": "{{ result('adhoc_http_action_253') }}"
            }
        )

        if_request_employeetype_present_257 = rail.IfOperator(
            task_id='if_request_employeetype_present_257',
            test='''{{ dag_run.conf.employeetype | is_truthy  and dag_run.conf.paygroup | is_truthy }}''',
            yes_task="if_request_employeetype_not_equals_to_datacsv_parserparse_csv_6linesfirstcolumn_3_258",
            no_task="if_request_residentstate_present_279",
        )

        if_request_employeetype_not_equals_to_datacsv_parserparse_csv_6linesfirstcolumn_3_258 = rail.IfOperator(
            task_id='if_request_employeetype_not_equals_to_datacsv_parserparse_csv_6linesfirstcolumn_3_258',
            #pylint: disable = line-too-long
            test='''{{ dag_run.conf.employeetype != result('load_user_details_from_report').employeetype  or dag_run.conf.paygroup != result('load_user_details_from_report').paygroupscurrent }}''',
            yes_task="sunovion_mapper_file_search_entries_checkfortimesheettemplateonthemapper_259",
            no_task="if_request_residentstate_present_279",
        )

        sunovion_mapper_file_search_entries_checkfortimesheettemplateonthemapper_259 = rail.PythonOperator(
            task_id='sunovion_mapper_file_search_entries_checkfortimesheettemplateonthemapper_259',
            python_callable=lambda dag_run:  list(filter(lambda x: x["type"] == "timesheet template" and x["identifier_1"]
                                                  == dag_run.conf['paygroup'] and x["identifier_2"] == dag_run.conf['employeetype'], sunovion_mapper))
        )

        log_pluckifthetimesheettemplateispresent_260 = rail.PythonOperator(
            task_id='log_pluckifthetimesheettemplateispresent_260',
            python_callable=lambda: rail.result('sunovion_mapper_file_search_entries_checkfortimesheettemplateonthemapper_259')[
                0]['data_set'] if rail.result('sunovion_mapper_file_search_entries_checkfortimesheettemplateonthemapper_259') else ''
        )

        if_log_pluckifthetimesheettemplateispresent_260_present_261 = rail.IfOperator(
            task_id='if_log_pluckifthetimesheettemplateispresent_260_present_261',
            #pylint: disable = line-too-long
            test='''{{ result('log_pluckifthetimesheettemplateispresent_260') | is_truthy  and result('log_pluckifthetimesheettemplateispresent_260') != result('load_user_details_from_report').timesheettemplate }}''',
            yes_task="adhoc_http_action_262",
            no_task="if_log_pluckifthetimesheettemplateispresent_260_blank_268",
        )

        adhoc_http_action_262 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_262',
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
            data_handler=lambda response: {
                'timesheettemplateuri': rail.find_first_by_attr_and_get_attr(response, 'displayText', rail.result(
                    'log_pluckifthetimesheettemplateispresent_260'), 'uri', ''),
                'timeofftemplateuri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Time Off', 'uri', '')
            }
        )

        if_log_timesheet_template_uri_263_present_264 = rail.IfOperator(
            task_id='if_log_timesheet_template_uri_263_present_264',
            test="{{ result('adhoc_http_action_262').timesheettemplateuri | is_truthy  and result('load_user_details_from_report').adminmodified != 'Yes'}}",
            yes_task="log_policysetstobeadded_266",
            no_task="if_log_pluckifthetimesheettemplateispresent_260_blank_268",
        )

        log_policysetstobeadded_266 = rail.PythonOperator(
            task_id='log_policysetstobeadded_266',
            python_callable=lambda: [rail.result('adhoc_http_action_262')[
                'timesheettemplateuri']] + [rail.result('adhoc_http_action_262')['timeofftemplateuri']]
        )

        updatetemplatesforuser_267 = rail.RepliconServiceOperator(
            task_id='updatetemplatesforuser_267',
            endpoint="/services/PolicySetService1.svc/PutPolicySetAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "policySetUris": rail.result('log_policysetstobeadded_266')
            }
        )

        if_log_pluckifthetimesheettemplateispresent_260_blank_268 = rail.IfOperator(
            task_id='if_log_pluckifthetimesheettemplateispresent_260_blank_268',
            test='''{{ result('log_pluckifthetimesheettemplateispresent_260') | is_falsy }}''',
            yes_task="sunovion_user_logs_file_add_entry_269",
            no_task="sunovion_mapper_file_search_entries_checkfortimesheetapprovalpathonthemapper_270",
        )

        sunovion_user_logs_file_add_entry_269 = rail.WriteLogOperator(
            task_id='sunovion_user_logs_file_add_entry_269',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="Error",
            properties={
                'parentjobid': "{{dag_run.conf.callerjobid}}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Error",
                #pylint: disable = line-too-long
                "failurereason": '''Timesheet template not updated for User "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}". "Timesheet template" not available for paygroup "{{ dag_run.conf.paygroup }}" and Employee Type "{{ dag_run.conf.employeetype }}" in mapper file''',
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        sunovion_mapper_file_search_entries_checkfortimesheetapprovalpathonthemapper_270 = rail.PythonOperator(
            task_id='sunovion_mapper_file_search_entries_checkfortimesheetapprovalpathonthemapper_270',
            python_callable=lambda dag_run:  list(filter(
                lambda x: x["type"] == "timesheet approval path" and x["identifier_1"] == dag_run.conf['employeetype'], sunovion_mapper))
        )

        log_pluckifthetimesheetapprovalpathispresent_271 = rail.PythonOperator(
            task_id='log_pluckifthetimesheetapprovalpathispresent_271',
            python_callable=lambda: rail.result('sunovion_mapper_file_search_entries_checkfortimesheetapprovalpathonthemapper_270')[
                0]['data_set'] if rail.result('sunovion_mapper_file_search_entries_checkfortimesheetapprovalpathonthemapper_270') else ''
        )

        if_log_pluckifthetimesheetapprovalpathispresent_271_present_272 = rail.IfOperator(
            task_id='if_log_pluckifthetimesheetapprovalpathispresent_271_present_272',
            test='''{{ result('log_pluckifthetimesheetapprovalpathispresent_271') | is_truthy }}''',
            yes_task="adhoc_http_action_273",
            no_task="sunovion_user_logs_file_add_entry_278",
        )

        adhoc_http_action_273 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_273',
            endpoint="/services/TimesheetApprovalService1.svc/GetAllApprovalPaths",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', rail.result('log_pluckifthetimesheetapprovalpathispresent_271'), 'uri', '')
        )

        if_log_timesheetapprovalpathuri_274_present_275 = rail.IfOperator(
            task_id='if_log_timesheetapprovalpathuri_274_present_275',
            #pylint: disable = line-too-long
            test='''{{ result('adhoc_http_action_273') | is_truthy  and result('adhoc_http_action_273') != result('load_user_details_from_report').timesheetapprovalpath }}''',
            yes_task="update_timesheet_approval_path_for_user_276",
            no_task="if_request_residentstate_present_279",
        )

        update_timesheet_approval_path_for_user_276 = rail.RepliconServiceOperator(
            task_id='update_timesheet_approval_path_for_user_276',
            endpoint="/services/TimesheetApprovalService1.svc/UpdateApprovalPathForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "approvalPathUri": "{{ result('adhoc_http_action_273') }}"
            }
        )

        sunovion_user_logs_file_add_entry_278 = rail.WriteLogOperator(
            task_id='sunovion_user_logs_file_add_entry_278',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="Error",
            properties={
                'parentjobid': "{{dag_run.conf.callerjobid}}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Error",
                #pylint: disable = line-too-long
                "failurereason": '''Timesheet approval path not updated for User "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}". "Timesheet Approval Path" not available for Employee Type "{{ dag_run.conf.employeetype }}" in mapper file''',
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_request_residentstate_present_279 = rail.IfOperator(
            task_id='if_request_residentstate_present_279',
            test="{{ dag_run.conf.residentstate | is_truthy and dag_run.conf.residentstate != result('load_user_details_from_report').residentstatecurrent}}",
            yes_task="adhoc_http_action_280",
            no_task="if_request_initialschedulename_present_299",
        )

        adhoc_http_action_280 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_280',
            endpoint="/services/LocationService1.svc/GetAllLocations",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['residentstate'], 'uri', '')
        )

        def get_location_schedule(response):
            initial_state = []
            additional_state = []
            for schedule in response:
                if not (schedule['effectiveDate'] and schedule['effectiveDate']['day']):
                    initial_state.append({
                        'location': {
                            'uri': schedule['location']['uri']
                        },
                        'effectiveDate': null
                    })
                else:
                    additional_state.append({
                        'location': {
                            'uri': schedule['location']['uri']
                        },
                        'effectiveDate': {
                            'day': schedule['effectiveDate']['day'],
                            'month': schedule['effectiveDate']['month'],
                            'year': schedule['effectiveDate']['year']
                        }
                    })
            return {
                'initial_state': initial_state,
                'additional_state': additional_state
            }

        adhoc_http_action_282 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_282',
            endpoint="/services/LocationService1.svc/GetLocationScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=get_location_schedule
        )

        if_log_required_residentstateuri_281_blank_288 = rail.IfOperator(
            task_id='if_log_required_residentstateuri_281_blank_288',
            test='''{{ result('adhoc_http_action_280') | is_falsy }}''',
            yes_task="adhoc_http_action_289",
            no_task="log_required_resident_state_uri_293",
        )

        adhoc_http_action_289 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_289',
            endpoint="/services/LocationService1.svc/CreateNewDraft",
        )

        update_namefor_resident_state_290 = rail.RepliconServiceOperator(
            task_id='update_namefor_resident_state_290',
            endpoint="/services/LocationService1.svc/UpdateName",
            data={
                "locationUri": "{{ result('adhoc_http_action_289') }}",
                "name": "{{ dag_run.conf.residentstate }}"
            }
        )

        update_codefor_resident_state_291 = rail.RepliconServiceOperator(
            task_id='update_codefor_resident_state_291',
            endpoint="/services/LocationService1.svc/UpdateCode",
            data={
                "locationUri": "{{ result('adhoc_http_action_289') }}",
                "code": "{{ dag_run.conf.residentstate }}"
            }
        )

        adhoc_http_action_292 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_292',
            endpoint="/services/LocationService1.svc/PublishDraft",
            data={
                "draftUri": "{{ result('adhoc_http_action_289') }}"
            }
        )

        log_required_resident_state_uri_293 = rail.PythonOperator(
            task_id='log_required_resident_state_uri_293',
            python_callable=lambda: rail.result('adhoc_http_action_280') if rail.result(
                'adhoc_http_action_280') else rail.result('adhoc_http_action_292')
        )

        get_final_location_schedule = rail.PythonOperator(
            task_id='get_final_location_schedule',
            python_callable=lambda: rail.result('adhoc_http_action_282')['initial_state'] + rail.result('adhoc_http_action_282')['additional_state'] + [{
                'location': {
                    'uri': rail.result('log_required_resident_state_uri_293')
                },
                'effectiveDate': {
                    'day': rail.result('log_todaysdate_7')['day'],
                    'month': rail.result('log_todaysdate_7')['month'],
                    'year': rail.result('log_todaysdate_7')['year']
                }
            }]
        )

        if_request_initialschedulename_present_299 = rail.IfOperator(
            task_id='if_request_initialschedulename_present_299',
            #pylint: disable = line-too-long
            test='''{{ dag_run.conf.initialschedulename | is_truthy  and dag_run.conf.initialschedulename != result('load_user_details_from_report').schedulenamecurrent }}''',
            yes_task="adhoc_http_action_300",
            no_task="if_request_costcenter_present_317",
        )

        adhoc_http_action_300 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_300',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['initialschedulename'], 'uri', '')
        )

        if_log_required_office_scheduleuri_301_present_302 = rail.IfOperator(
            task_id='if_log_required_office_scheduleuri_301_present_302',
            test='''{{ result('adhoc_http_action_300') | is_truthy }}''',
            yes_task="adhoc_http_action_303",
            no_task="if_request_costcenter_present_317",
        )

        def get_schedulepolicy_schedule(response):
            schedules = response if response else []
            if not schedules:
                return False
            intial_schedule = []
            additional_schedule = []
            for schedule in schedules:
                if not (schedule['effectiveDate'] and schedule['effectiveDate']['day']):
                    intial_schedule.append({
                        'schedulePolicy': {
                            'officeScheduleUri': schedule['officeSchedule']['uri']
                        },
                        'effectiveDate': null
                    })
                else:
                    additional_schedule.append({
                        'schedulePolicy': {
                            'officeScheduleUri': schedule['officeSchedule']['uri']
                        },
                        'effectiveDate': {
                            'day': schedule['effectiveDate']['day'],
                            'month': schedule['effectiveDate']['month'],
                            'year': schedule['effectiveDate']['year']
                        }
                    })
            new_schedule = [{
                'schedulePolicy': {
                    'officeScheduleUri': rail.result('adhoc_http_action_300')
                },
                'effectiveDate': {
                    'day': rail.result('log_todaysdate_7')['day'],
                    'month': rail.result('log_todaysdate_7')['month'],
                    'year': rail.result('log_todaysdate_7')['year']
                }
            }]
            return intial_schedule + additional_schedule + new_schedule

        adhoc_http_action_303 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_303',
            endpoint="/services/SchedulingService2.svc/GetSchedulePolicyScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=get_schedulepolicy_schedule
        )

        if_officeschedule_displaytext_present_304 = rail.IfOperator(
            task_id='if_officeschedule_displaytext_present_304',
            test=lambda: bool(rail.result('adhoc_http_action_303')),
            yes_task="put_schedule_policy_schedule_for_user_314",
            no_task="if_officeschedule_displaytext_blank_315",
        )

        put_schedule_policy_schedule_for_user_314 = rail.RepliconServiceOperator(
            task_id='put_schedule_policy_schedule_for_user_314',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": rail.result('adhoc_http_action_303')
            }
        )

        if_officeschedule_displaytext_blank_315 = rail.IfOperator(
            task_id='if_officeschedule_displaytext_blank_315',
            test=lambda: not bool(rail.result('adhoc_http_action_303')),
            yes_task="put_schedule_policy_schedule_for_user_316",
            no_task="if_request_costcenter_present_317",
        )

        put_schedule_policy_schedule_for_user_316 = rail.RepliconServiceOperator(
            task_id='put_schedule_policy_schedule_for_user_316',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "scheduleEntries": [
                    {
                        "schedulePolicy": {
                            "officeScheduleUri": "{{ result('adhoc_http_action_300') }}",
                            "name": null,
                            "officeSchedule": null,
                            "scheduleTypeUri": null
                        },
                        "effectiveDate": {
                            "year": "{{ result('log_todaysdate_7').year }}",
                            "month": "{{ result('log_todaysdate_7').month }}",
                            "day": "{{ result('log_todaysdate_7').day }}"
                        }
                    }
                ]
            }
        )

        if_request_costcenter_present_317 = rail.IfOperator(
            task_id='if_request_costcenter_present_317',
            test='''{{ dag_run.conf.costcenter | is_truthy  and dag_run.conf.costcenter != result('load_user_details_from_report').costcentercurrent }}''',
            yes_task="adhoc_http_action_318",
            no_task="log_input_data_346",
        )

        adhoc_http_action_318 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_318',
            endpoint="/services/CostCenterService1.svc/GetAllCostCenters",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['costcenter'], 'uri', '')
        )

        def get_costcenter_schedule(response):
            initial_costcenter = []
            additional_costcenter = []
            for schedule in response:
                if not (schedule['effectiveDate'] and schedule['effectiveDate']['day']):
                    initial_costcenter.append({
                        'costCenter': {
                            'uri': schedule['costCenter']['uri']
                        },
                        'effectiveDate': null
                    })
                else:
                    additional_costcenter.append({
                        'costCenter': {
                            'uri': schedule['costCenter']['uri']
                        },
                        'effectiveDate': {
                            'day': schedule['effectiveDate']['day'],
                            'month': schedule['effectiveDate']['month'],
                            'year': schedule['effectiveDate']['year']
                        }
                    })
            return {
                'initial_costcenter': initial_costcenter,
                'additional_costcenter': additional_costcenter
            }

        adhoc_http_action_320 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_320',
            endpoint="/services/CostCenterService1.svc/GetCostCenterScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=get_costcenter_schedule
        )

        if_log_required_costcenteruri_319_present_326 = rail.IfOperator(
            task_id='if_log_required_costcenteruri_319_present_326',
            test='''{{ result('adhoc_http_action_318') | is_truthy }}''',
            yes_task="get_final_costcenter_schedule",
            no_task="if_log_required_costcenteruri_319_blank_332",
        )

        get_final_costcenter_schedule = rail.PythonOperator(
            task_id='get_final_costcenter_schedule',
            python_callable=lambda: rail.result('adhoc_http_action_320')['initial_costcenter'] + rail.result(
                'adhoc_http_action_320')['additional_costcenter'] + [{
                'costCenter': {
                    'uri': rail.result('adhoc_http_action_318')
                },
                'effectiveDate': {
                    'day': rail.result('log_todaysdate_7')['day'],
                    'month': rail.result('log_todaysdate_7')['month'],
                    'year': rail.result('log_todaysdate_7')['year']
                }
            }]
        )

        if_log_required_costcenteruri_319_blank_332 = rail.IfOperator(
            task_id='if_log_required_costcenteruri_319_blank_332',
            test='''{{ result('adhoc_http_action_318') | is_falsy }}''',
            yes_task="if_log_cost_center_length_333_less_than_51_334",
            no_task="log_input_data_346",
        )

        if_log_cost_center_length_333_less_than_51_334 = rail.IfOperator(
            task_id='if_log_cost_center_length_333_less_than_51_334',
            test=lambda dag_run: len(dag_run.conf['costcenter']) < 51,
            yes_task="adhoc_http_action_335",
            no_task="sunovion_user_logs_file_add_entry_345",
        )

        adhoc_http_action_335 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_335',
            endpoint="/services/CostCenterService1.svc/CreateNewDraft",
        )

        update_namefor_cost_center_336 = rail.RepliconServiceOperator(
            task_id='update_namefor_cost_center_336',
            endpoint="/services/CostCenterService1.svc/UpdateName",
            data={
                "costCenterUri": "{{result('adhoc_http_action_335')}}",
                "name": "{{ dag_run.conf.costcenter }}"
            }
        )

        update_codefor_cost_center_337 = rail.RepliconServiceOperator(
            task_id='update_codefor_cost_center_337',
            endpoint="/services/CostCenterService1.svc/UpdateCode",
            data={
                "costCenterUri": "{{result('adhoc_http_action_335')}}",
                "code": "{{ dag_run.conf.costcenter }}"
            }
        )

        adhoc_http_action_338 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_338',
            endpoint="/services/CostCenterService1.svc/PublishDraft",
            data={
                "draftUri": "{{result('adhoc_http_action_335')}}"
            }
        )

        log_merge_newcost_centerand_additionalcost_centerlistarraywithinitialcost_center_342 = rail.PythonOperator(
            task_id='log_merge_newcost_centerand_additionalcost_centerlistarraywithinitialcost_center_342',
            python_callable=lambda: rail.result('adhoc_http_action_320')['initial_costcenter'] + rail.result(
                'adhoc_http_action_320')['additional_costcenter'] + [{
                'costCenter': {
                    'uri': rail.result('adhoc_http_action_338')['uri']
                },
                'effectiveDate': {
                    'day': rail.result('log_todaysdate_7')['day'],
                    'month': rail.result('log_todaysdate_7')['month'],
                    'year': rail.result('log_todaysdate_7')['year']
                }
            }]
        )

        put_cost_center_schedule_for_user_343 = rail.RepliconServiceOperator(
            task_id='put_cost_center_schedule_for_user_343',
            endpoint="/services/CostCenterService1.svc/PutCostCenterScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": rail.result('log_merge_newcost_centerand_additionalcost_centerlistarraywithinitialcost_center_342')
            }
        )

        sunovion_user_logs_file_add_entry_345 = rail.WriteLogOperator(
            task_id='sunovion_user_logs_file_add_entry_345',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="Error",
            properties={
                'parentjobid': "{{dag_run.conf.callerjobid}}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Error",
                #pylint: disable = line-too-long
                "failurereason": '''Cost center is not updated for user "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}" as "{{ dag_run.conf.costcenter }}" have more than 50 characters''',
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        log_input_data_346 = rail.PythonOperator(
            task_id='log_input_data_346',
            python_callable=lambda dag_run:  dag_run.conf['employeetype'] + "/" + dag_run.conf['workdayemployeetype'] +
            "/" + dag_run.conf['workdayexecutive'] +
            "/" + dag_run.conf['residentstate']
        )

        log_existing_data_347 = rail.PythonOperator(
            task_id='log_existing_data_347',
            python_callable=lambda:  rail.result('load_user_details_from_report')['employeetype'] + "/" + rail.result('load_user_details_from_report')[
                'workdayemployeetype'] + "/" + rail.result('load_user_details_from_report')['workdayexecutive'] + "/" + rail.result(
                'load_user_details_from_report')['residentstatecurrent']
        )

        log_old_resident_state_349 = rail.PythonOperator(
            task_id='log_old_resident_state_349',
            python_callable=lambda:  "Non-CA" if rail.result('load_user_details_from_report')[
                'residentstatecurrent'] != 'CA' else 'CA'
        )

        if_log_input_data_346_not_equals_to_dataloggerlog_existing_data_347message_352 = rail.IfOperator(
            task_id='if_log_input_data_346_not_equals_to_dataloggerlog_existing_data_347message_352',
            test='''{{ result('log_input_data_346') != result('log_existing_data_347') }}''',
            yes_task="trigger_child_to_update_timeofftype_for_existing_user",
            no_task="sunovion_user_logs_file_add_entry_357",
        )

        trigger_child_to_update_timeofftype_for_existing_user = rail.TriggerDagRunOperator(
            task_id='trigger_child_to_update_timeofftype_for_existing_user',
            retries=0,
            trigger_dag_id=f'sunovion_user_import_workflow_to_update_timeoff_type_for_existing_user_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "callerjobid": "{{ dag_run.conf.callerjobid }}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "userloginname": "{{ dag_run.conf.loginname }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "workdayemployeetype": "{{ dag_run.conf.workdayemployeetype }}",
                "workdayexecutive": "{{ dag_run.conf.workdayexecutive }}",
                "residentstate": "{{ dag_run.conf.residentstate }}",
                "employeetype": "{{ dag_run.conf.employeetype }}",
                "oldresidentstate": "{{result('log_old_resident_state_349')}}"
            }
        )

        waitfor_child_to_update_timeofftype_for_existing_user = rail.WaitForDagRunsSensor(
            task_id='waitfor_child_to_update_timeofftype_for_existing_user',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_to_update_timeofftype_for_existing_user") }}'
        )

        sunovion_user_logs_file_add_entry_357 = rail.WriteLogOperator(
            task_id='sunovion_user_logs_file_add_entry_357',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="Success",
            properties={
                'parentjobid': "{{dag_run.conf.callerjobid}}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Success",
                "failurereason": '',
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="Error",
            properties={
                'parentjobid': "{{dag_run.conf.callerjobid}}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Error",
                "failurereason": '''All fields is not updated for user "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}" :{{get_error_message()}}''',
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> log_user_i_d_3
        log_user_i_d_3 >> get_user_details_4 >> generate_reportforuserdetails_5 >> run_user_details_report >> parse_csv_6 >> load_user_details_from_report
        load_user_details_from_report >> log_todaysdate_7 >> if_request_firstname_present_11
        if_request_firstname_present_11 >> rail.Label(
            'Yes') >> update_first_name_12 >> if_request_lastname_present_13
        if_request_firstname_present_11 >> rail.Label(
            'No') >> if_request_lastname_present_13
        if_request_lastname_present_13 >> rail.Label(
            'Yes') >> update_last_name_14 >> if_request_employeeid_present_15
        if_request_lastname_present_13 >> rail.Label(
            'No') >> if_request_employeeid_present_15
        if_request_employeeid_present_15 >> rail.Label(
            'Yes') >> update_employee_id_16 >> if_request_emailaddress_present_17
        if_request_employeeid_present_15 >> rail.Label(
            'No') >> if_request_emailaddress_present_17
        if_request_emailaddress_present_17 >> rail.Label(
            'Yes') >> update_email_18 >> adhoc_http_action_19
        if_request_emailaddress_present_17 >> rail.Label(
            'No') >> adhoc_http_action_19 >> if_log_getrequired_usergroupuri_20_present_21
        if_log_getrequired_usergroupuri_20_present_21 >> rail.Label(
            'Yes') >> adhoc_http_action_22 >> if_request_scheduledhoursperweek_present_23
        if_request_scheduledhoursperweek_present_23 >> rail.Label(
            'Yes') >> if_log_getrequired_scheduled_hours_per_weekudfuri_24_present_25
        if_log_getrequired_scheduled_hours_per_weekudfuri_24_present_25 >> rail.Label(
            'Yes') >> update_text_valuefor_scheduled_hours_per_weekudf_26 >> if_request_workdayemployeetype_present_27
        if_log_getrequired_scheduled_hours_per_weekudfuri_24_present_25 >> rail.Label(
            'No') >> if_request_workdayemployeetype_present_27
        if_request_scheduledhoursperweek_present_23 >> rail.Label(
            'No') >> if_request_workdayemployeetype_present_27
        if_request_workdayemployeetype_present_27 >> rail.Label(
            'Yes') >> if_log_getrequired_workday_employee_typeudfuri_28_present_29
        if_log_getrequired_workday_employee_typeudfuri_28_present_29 >> rail.Label(
            'Yes') >> adhoc_http_action_30 >> if_log_getrequired_workday_employee_typedropdownuri_31_present_32
        if_log_getrequired_workday_employee_typedropdownuri_31_present_32 >> rail.Label(
            'Yes') >> update_dropdown_valuefor_workday_employee_typeudf_33 >> if_request_workdayexecutive_present_34
        if_log_getrequired_workday_employee_typedropdownuri_31_present_32 >> rail.Label(
            'No') >> if_request_workdayexecutive_present_34
        if_log_getrequired_workday_employee_typeudfuri_28_present_29 >> rail.Label(
            'No') >> if_request_workdayexecutive_present_34
        if_request_workdayemployeetype_present_27 >> rail.Label(
            'No') >> if_request_workdayexecutive_present_34
        if_request_workdayexecutive_present_34 >> rail.Label(
            'Yes') >> if_log_getrequired_workday_executiveudfuri_35_present_36
        if_log_getrequired_workday_executiveudfuri_35_present_36 >> rail.Label(
            'Yes') >> adhoc_http_action_37 >> if_log_getrequired_workday_executivedropdownuri_38_present_39
        if_log_getrequired_workday_executivedropdownuri_38_present_39 >> rail.Label(
            'Yes') >> update_dropdown_valuefor_workday_employee_typeudf_40 >> if_request_vacationaccrualdate_present_41
        if_log_getrequired_workday_executivedropdownuri_38_present_39 >> rail.Label(
            'No') >> if_request_vacationaccrualdate_present_41
        if_log_getrequired_workday_executiveudfuri_35_present_36 >> rail.Label(
            'No') >> if_request_vacationaccrualdate_present_41
        if_request_workdayexecutive_present_34 >> rail.Label(
            'No') >> if_request_vacationaccrualdate_present_41
        if_request_vacationaccrualdate_present_41 >> rail.Label(
            'Yes') >> if_log_getrequired_vacation_accrual_dateudfuri_42_present_43
        if_log_getrequired_vacation_accrual_dateudfuri_42_present_43 >> rail.Label(
            'Yes') >> log_required_vacation_accrual_date_day_44 >> update_date_valuefor_vacation_accrual_dateudf_47 >> if_request_startdate_present_48
        if_log_getrequired_vacation_accrual_dateudfuri_42_present_43 >> rail.Label(
            'No') >> if_request_startdate_present_48
        if_request_vacationaccrualdate_present_41 >> rail.Label(
            'No') >> if_request_startdate_present_48
        if_log_getrequired_usergroupuri_20_present_21 >> rail.Label(
            'No') >> if_request_startdate_present_48
        if_request_startdate_present_48 >> rail.Label(
            'Yes') >> log_required_start_date_day_49 >> update_employment_date_range_52 >> if_request_vacationaccrualdate_present_53
        if_request_vacationaccrualdate_present_53 >> rail.Label(
            'Yes') >> log_startdateday_54 >> log_differencebetweennewstartdateandcurrentenddate_57
        log_differencebetweennewstartdateandcurrentenddate_57 >> if_log_differencebetweennewstartdateandcurrentenddate_57_greater_than_365_58
        if_log_differencebetweennewstartdateandcurrentenddate_57_greater_than_365_58 >> rail.Label(
            'Yes') >> trigger_workflow_to_update_timeoff_type_for_existing_user_rehire >> wait_for_workflow_to_update_timeoff_type_for_existing_user_rehire
        wait_for_workflow_to_update_timeoff_type_for_existing_user_rehire >> if_request_enddate_present_62
        if_log_differencebetweennewstartdateandcurrentenddate_57_greater_than_365_58 >> rail.Label(
            'No') >> if_request_enddate_present_62
        if_request_vacationaccrualdate_present_53 >> rail.Label(
            'No') >> if_request_enddate_present_62
        if_request_startdate_present_48 >> rail.Label(
            'No') >> if_request_enddate_present_62
        if_request_enddate_present_62 >> rail.Label(
            'Yes') >> log_start_dateday_63 >> log_end_date_day_68
        log_end_date_day_68 >> if_log_end_date_totimeformat_72_greater_than_dataloggerlog_start_datetotimeformat_67message_73
        if_log_end_date_totimeformat_72_greater_than_dataloggerlog_start_datetotimeformat_67message_73 >> rail.Label(
            'Yes') >> update_employment_date_range_74 >> if_request_enabled_present_75
        if_log_end_date_totimeformat_72_greater_than_dataloggerlog_start_datetotimeformat_67message_73 >> rail.Label(
            'No') >> if_request_enabled_present_75
        if_request_enddate_present_62 >> rail.Label(
            'No') >> if_request_enabled_present_75
        if_request_enabled_present_75 >> rail.Label(
            'Yes') >> if_request_enabled_equals_to_yes_76
        if_request_enabled_equals_to_yes_76 >> rail.Label(
            'Yes') >> adhoc_http_action_77 >> if_first_column_12_present_78
        if_first_column_12_present_78 >> rail.Label(
            'Yes') >> log_start_dateday_79 >> update_employment_date_range_82 >> if_request_enabled_equals_to_no_83
        if_first_column_12_present_78 >> rail.Label(
            'No') >> if_request_enabled_equals_to_no_83
        if_request_enabled_equals_to_yes_76 >> rail.Label(
            'No') >> if_request_enabled_equals_to_no_83
        if_request_enabled_equals_to_no_83 >> rail.Label(
            'Yes') >> adhoc_http_action_84 >> if_request_paygroup_present_85
        if_request_enabled_equals_to_no_83 >> rail.Label(
            'No') >> if_request_paygroup_present_85
        if_request_enabled_present_75 >> rail.Label(
            'No') >> if_request_paygroup_present_85
        if_request_paygroup_present_85 >> rail.Label(
            'Yes') >> if_request_paygroup_equals_emd_or_el8
        if_request_paygroup_equals_emd_or_el8 >> rail.Label(
            'Yes') >> set_s_s_o_authentication_for_user_87 >> if_request_paygroup_not_equals_to_el8_or_emd
        if_request_paygroup_equals_emd_or_el8 >> rail.Label(
            'No') >> if_request_paygroup_not_equals_to_el8_or_emd
        if_request_paygroup_not_equals_to_el8_or_emd >> rail.Label(
            'Yes') >> set_replicon_authentication_for_user_89 >> sunovion_mapper_file_search_entries_checkfordepartmentonthemapper_90
        if_request_paygroup_not_equals_to_el8_or_emd >> rail.Label(
            'No') >> sunovion_mapper_file_search_entries_checkfordepartmentonthemapper_90 >> log_pluckifthedepartmentispresent_91
        log_pluckifthedepartmentispresent_91 >> if_first_column_4_not_equals_to_dataloggerlog_pluckifthedepartmentispresent_91message_92
        if_first_column_4_not_equals_to_dataloggerlog_pluckifthedepartmentispresent_91message_92 >> rail.Label(
            'Yes') >> adhoc_http_action_93 >> if_log_departmenturi_94_present_95
        if_log_departmenturi_94_present_95 >> rail.Label(
            'Yes') >> update_department_for_user_96 >> if_first_column_24_not_equals_to_yes_99
        if_log_departmenturi_94_present_95 >> rail.Label(
            'No') >> sunovion_user_logs_file_add_entry_98 >> if_first_column_24_not_equals_to_yes_99
        if_first_column_24_not_equals_to_yes_99 >> rail.Label(
            'Yes') >> sunovion_mapper_file_search_entries_checkforlicensesonthemapper_100 >> log_pluckifthelicensesispresent_101
        log_pluckifthelicensesispresent_101 >> if_log_pluckifthelicensesispresent_101_present_102
        if_log_pluckifthelicensesispresent_101_present_102 >> rail.Label(
            'Yes') >> adhoc_http_action_103 >> log_getnumberofproductstobeassigned_104 >> put_product_assignments_for_user_111 >> adhoc_http_action_112
        if_log_pluckifthelicensesispresent_101_present_102 >> rail.Label(
            'No') >> adhoc_http_action_112
        if_first_column_24_not_equals_to_yes_99 >> rail.Label(
            'No') >> adhoc_http_action_112
        if_first_column_4_not_equals_to_dataloggerlog_pluckifthedepartmentispresent_91message_92 >> rail.Label(
            'No') >> adhoc_http_action_112 >> if_log_required_paygroupuri_113_present_114
        if_log_required_paygroupuri_113_present_114 >> rail.Label(
            'Yes') >> if_first_column_21_blank_f4x_115
        if_first_column_21_blank_f4x_115 >> rail.Label(
            'Yes') >> log_paygroupeffectivedateday_116 >> put_service_center_schedule_for_user_pay_group_119 >> if_first_column_21_present_f4x_120
        if_first_column_21_blank_f4x_115 >> rail.Label(
            'No') >> if_first_column_21_present_f4x_120
        if_first_column_21_present_f4x_120 >> rail.Label(
            'Yes') >> adhoc_http_action_121 >> put_service_center_schedule_for_user_pay_group_136 >> if_request_employeetype_present_139
        if_first_column_21_present_f4x_120 >> rail.Label(
            'No') >> if_request_employeetype_present_139
        if_log_required_paygroupuri_113_present_114 >> rail.Label(
            'No') >> sunovion_user_logs_file_add_entry_138 >> if_request_employeetype_present_139
        if_request_paygroup_present_85 >> rail.Label(
            'No') >> if_request_employeetype_present_139
        if_request_employeetype_present_139 >> rail.Label(
            'Yes') >> adhoc_http_action_140 >> if_log_required_employeetypegroupuri_141_present_142
        if_log_required_employeetypegroupuri_141_present_142 >> rail.Label(
            'Yes') >> if_first_column_27_blank_f4x_143
        if_first_column_27_blank_f4x_143 >> rail.Label(
            'Yes') >> put_division_schedule_for_user_employee_type_group_144 >> if_first_column_27_present_f4x_145
        if_first_column_27_blank_f4x_143 >> rail.Label(
            'No') >> if_first_column_27_present_f4x_145
        if_first_column_27_present_f4x_145 >> rail.Label(
            'Yes') >> adhoc_http_action_146 >> put_division_schedule_for_user_employee_type_group_159 >> if_request_employeetype_present_160
        if_first_column_27_present_f4x_145 >> rail.Label(
            'No') >> if_request_employeetype_present_160
        if_log_required_employeetypegroupuri_141_present_142 >> rail.Label(
            'No') >> if_request_employeetype_present_160
        if_request_employeetype_present_139 >> rail.Label(
            'No') >> if_request_employeetype_present_160
        if_request_employeetype_present_160 >> rail.Label(
            'Yes') >> adhoc_http_action_161 >> if_log_requiredemployeetypeuri_162_present_163
        if_log_requiredemployeetypeuri_162_present_163 >> rail.Label(
            'Yes') >> update_employee_type_for_user_164 >> sunovion_mapper_file_search_entries_checkfortimeoffapprovalpathonthemapper_167
        if_log_requiredemployeetypeuri_162_present_163 >> rail.Label(
            'No') >> sunovion_user_logs_file_add_entry_166 >> sunovion_mapper_file_search_entries_checkfortimeoffapprovalpathonthemapper_167
        sunovion_mapper_file_search_entries_checkfortimeoffapprovalpathonthemapper_167 >> log_pluckifthetimeoffapprovalpathispresent_168
        log_pluckifthetimeoffapprovalpathispresent_168 >> if_log_pluckifthetimeoffapprovalpathispresent_168_present_169
        if_log_pluckifthetimeoffapprovalpathispresent_168_present_169 >> rail.Label(
            'Yes') >> if_log_pluckifthetimeoffapprovalpathispresent_168_not_equals_to_datacsv_parserparse_csv_6linesfirstcolumn_20_170
        if_log_pluckifthetimeoffapprovalpathispresent_168_not_equals_to_datacsv_parserparse_csv_6linesfirstcolumn_20_170 >> rail.Label(
            'Yes') >> adhoc_http_action_171 >> if_log_timeoffapprovalpathuri_172_present_173
        if_log_timeoffapprovalpathuri_172_present_173 >> rail.Label(
            'Yes') >> update_timeoff_approval_path_for_user_174 >> adhoc_http_action_177
        if_log_timeoffapprovalpathuri_172_present_173 >> rail.Label(
            'No') >> adhoc_http_action_177
        if_log_pluckifthetimeoffapprovalpathispresent_168_not_equals_to_datacsv_parserparse_csv_6linesfirstcolumn_20_170 >> rail.Label(
            'No') >> adhoc_http_action_177
        if_log_pluckifthetimeoffapprovalpathispresent_168_present_169 >> rail.Label(
            'No') >> sunovion_user_logs_file_add_entry_176 >> adhoc_http_action_177 >> if_request_employeetype_equals_to_nonexempt_178
        if_request_employeetype_equals_to_nonexempt_178 >> rail.Label(
            'Yes') >> if_log_required_payruleuri_179_present_180
        if_log_required_payruleuri_179_present_180 >> rail.Label(
            'Yes') >> adhoc_http_action_181 >> put_pay_rule_script_assignment_schedule_for_user_191
        put_pay_rule_script_assignment_schedule_for_user_191 >> if_request_employeetype_equals_to_nonexempt_and_payrulenamecurrent_notpresent
        if_log_required_payruleuri_179_present_180 >> rail.Label(
            'No') >> if_request_employeetype_equals_to_nonexempt_and_payrulenamecurrent_notpresent
        if_request_employeetype_equals_to_nonexempt_178 >> rail.Label(
            'No') >> if_request_employeetype_equals_to_nonexempt_and_payrulenamecurrent_notpresent
        if_request_employeetype_equals_to_nonexempt_and_payrulenamecurrent_notpresent >> rail.Label(
            'Yes') >> if_log_required_payruleuri_179_present_194
        if_log_required_payruleuri_179_present_194 >> rail.Label(
            'Yes') >> put_pay_rule_script_assignment_schedule_for_user_195 >> if_request_employeetype_equals_to_ca_nonexempt_196
        if_log_required_payruleuri_179_present_194 >> rail.Label(
            'No') >> if_request_employeetype_equals_to_ca_nonexempt_196
        if_request_employeetype_equals_to_nonexempt_and_payrulenamecurrent_notpresent >> rail.Label(
            'No') >> if_request_employeetype_equals_to_ca_nonexempt_196
        if_request_employeetype_equals_to_ca_nonexempt_196 >> rail.Label(
            'Yes') >> if_log_required_payruleuri_197_present_198
        if_log_required_payruleuri_197_present_198 >> rail.Label(
            'Yes') >> adhoc_http_action_199 >> put_pay_rule_script_assignment_schedule_for_user_209
        put_pay_rule_script_assignment_schedule_for_user_209 >> if_request_employeetype_equals_to_ca_nonexempt_and_payrulenamecurrent_notpresent
        if_log_required_payruleuri_197_present_198 >> rail.Label(
            'No') >> if_request_employeetype_equals_to_ca_nonexempt_and_payrulenamecurrent_notpresent
        if_request_employeetype_equals_to_ca_nonexempt_196 >> rail.Label(
            'No') >> if_request_employeetype_equals_to_ca_nonexempt_and_payrulenamecurrent_notpresent
        if_request_employeetype_equals_to_ca_nonexempt_and_payrulenamecurrent_notpresent >> rail.Label(
            'Yes') >> if_log_required_payruleuri_211_present_212
        if_log_required_payruleuri_211_present_212 >> rail.Label(
            'Yes') >> put_pay_rule_script_assignment_schedule_for_user_213 >> if_employeetype_equals_exempt_and_payrulenamecurrent_unequal_exempt
        if_log_required_payruleuri_211_present_212 >> rail.Label(
            'No') >> if_employeetype_equals_exempt_and_payrulenamecurrent_unequal_exempt
        if_request_employeetype_equals_to_ca_nonexempt_and_payrulenamecurrent_notpresent >> rail.Label(
            'No') >> if_employeetype_equals_exempt_and_payrulenamecurrent_unequal_exempt
        if_employeetype_equals_exempt_and_payrulenamecurrent_unequal_exempt >> rail.Label(
            'Yes') >> if_sunovion_payrule_exempt_uri_present
        if_sunovion_payrule_exempt_uri_present >> rail.Label(
            'Yes') >> get_payrule_script_assignment_schedule_for_user >> put_payrule_script_assignment_schedule_for_user
        put_payrule_script_assignment_schedule_for_user >> if_employeetype_equals_exempt_and_payrulenamecurrent_not_present
        if_sunovion_payrule_exempt_uri_present >> rail.Label(
            'No') >> if_employeetype_equals_exempt_and_payrulenamecurrent_not_present
        if_employeetype_equals_exempt_and_payrulenamecurrent_unequal_exempt >> rail.Label(
            'No') >> if_employeetype_equals_exempt_and_payrulenamecurrent_not_present
        if_employeetype_equals_exempt_and_payrulenamecurrent_not_present >> rail.Label(
            'Yes') >> if_sunovion_payrueleexempt_uri_present
        if_sunovion_payrueleexempt_uri_present >> rail.Label(
            'Yes') >> putpayrule_script_schedule_assignment_for_user >> if_request_supervisorid_present_214
        if_sunovion_payrueleexempt_uri_present >> rail.Label(
            'No') >> if_request_supervisorid_present_214
        if_employeetype_equals_exempt_and_payrulenamecurrent_not_present >> rail.Label(
            'No') >> if_request_supervisorid_present_214
        if_request_employeetype_present_160 >> rail.Label(
            'No') >> if_request_supervisorid_present_214

        if_request_supervisorid_present_214 >> rail.Label(
            'Yes') >> if_request_supervisorid_not_equals_to_dataworkato_service3cd9c331requestloginname_215
        if_request_supervisorid_not_equals_to_dataworkato_service3cd9c331requestloginname_215 >> rail.Label(
            'Yes') >> search_users_216 >> if_log_getsupervisor_uri_217_present_218
        if_log_getsupervisor_uri_217_present_218 >> rail.Label(
            'Yes') >> update_supervisorwithtodayaseffectivedate_219 >> if_log_getsupervisor_uri_217_blank_220
        if_log_getsupervisor_uri_217_present_218 >> rail.Label(
            'No') >> if_log_getsupervisor_uri_217_blank_220
        if_log_getsupervisor_uri_217_blank_220 >> rail.Label(
            'Yes') >> sunovion_user_supervisor_mapping_table_add_entry_221 >> if_request_supervisorid_equals_to_dataworkato_service3cd9c331requestloginname_222
        if_log_getsupervisor_uri_217_blank_220 >> rail.Label(
            'No') >> if_request_supervisorid_equals_to_dataworkato_service3cd9c331requestloginname_222
        if_request_supervisorid_not_equals_to_dataworkato_service3cd9c331requestloginname_215 >> rail.Label(
            'No') >> if_request_supervisorid_equals_to_dataworkato_service3cd9c331requestloginname_222
        if_request_supervisorid_equals_to_dataworkato_service3cd9c331requestloginname_222 >> rail.Label(
            'Yes') >> sunovion_user_logs_file_add_entry_223 >> if_request_permissionsets_present_224
        if_request_supervisorid_equals_to_dataworkato_service3cd9c331requestloginname_222 >> rail.Label(
            'No') >> if_request_permissionsets_present_224
        if_request_supervisorid_present_214 >> rail.Label(
            'No') >> if_request_permissionsets_present_224
        if_request_permissionsets_present_224 >> rail.Label(
            'Yes') >> adhoc_http_action_225 >> adhoc_http_action_232 >> log_getnumberofpermissionstobeassigned_233
        log_getnumberofpermissionstobeassigned_233 >> if_log_permissionstobeassigned_240_present_pr_241
        if_log_permissionstobeassigned_240_present_pr_241 >> rail.Label(
            'Yes') >> put_permission_set_assignments_for_user_242 >> if_request_residentstate_present_243
        if_log_permissionstobeassigned_240_present_pr_241 >> rail.Label(
            'No') >> if_request_residentstate_present_243
        if_request_permissionsets_present_224 >> rail.Label(
            'No') >> if_request_residentstate_present_243
        if_request_residentstate_present_243 >> rail.Label(
            'Yes') >> if_request_residentstate_not_equals_to_datacsv_parserparse_csv_6linesfirstcolumn_22_244
        if_request_residentstate_not_equals_to_datacsv_parserparse_csv_6linesfirstcolumn_22_244 >> rail.Label(
            'Yes') >> log_requiredresidentstare_249
        log_requiredresidentstare_249 >> sunovion_mapper_file_search_entries_checkforholidaycalendaronthemapper_250
        sunovion_mapper_file_search_entries_checkforholidaycalendaronthemapper_250 >> if_log_pluckiftheholidaycalendarispresentonthemapper_251_present_252
        if_log_pluckiftheholidaycalendarispresentonthemapper_251_present_252 >> rail.Label(
            'Yes') >> adhoc_http_action_253 >> if_log_holidaycalendar_uri_254_present_pr_255
        if_log_holidaycalendar_uri_254_present_pr_255 >> rail.Label(
            'Yes') >> update_holiday_calendar_256 >> if_request_employeetype_present_257
        if_log_holidaycalendar_uri_254_present_pr_255 >> rail.Label(
            'No') >> if_request_employeetype_present_257
        if_log_pluckiftheholidaycalendarispresentonthemapper_251_present_252 >> rail.Label(
            'No') >> if_request_employeetype_present_257
        if_request_residentstate_not_equals_to_datacsv_parserparse_csv_6linesfirstcolumn_22_244 >> rail.Label(
            'No') >> if_request_employeetype_present_257
        if_request_residentstate_present_243 >> rail.Label(
            'No') >> if_request_employeetype_present_257
        if_request_employeetype_present_257 >> rail.Label(
            'Yes') >> if_request_employeetype_not_equals_to_datacsv_parserparse_csv_6linesfirstcolumn_3_258
        if_request_employeetype_not_equals_to_datacsv_parserparse_csv_6linesfirstcolumn_3_258 >> rail.Label(
            'Yes') >> sunovion_mapper_file_search_entries_checkfortimesheettemplateonthemapper_259 >> log_pluckifthetimesheettemplateispresent_260
        log_pluckifthetimesheettemplateispresent_260 >> if_log_pluckifthetimesheettemplateispresent_260_present_261
        if_log_pluckifthetimesheettemplateispresent_260_present_261 >> rail.Label(
            'Yes') >> adhoc_http_action_262 >> if_log_timesheet_template_uri_263_present_264
        if_log_timesheet_template_uri_263_present_264 >> rail.Label(
            'Yes') >> log_policysetstobeadded_266 >> updatetemplatesforuser_267 >> if_log_pluckifthetimesheettemplateispresent_260_blank_268
        if_log_timesheet_template_uri_263_present_264 >> rail.Label(
            'No') >> if_log_pluckifthetimesheettemplateispresent_260_blank_268
        if_log_pluckifthetimesheettemplateispresent_260_present_261 >> rail.Label(
            'No') >> if_log_pluckifthetimesheettemplateispresent_260_blank_268
        if_log_pluckifthetimesheettemplateispresent_260_blank_268 >> rail.Label(
            'Yes') >> sunovion_user_logs_file_add_entry_269 >> sunovion_mapper_file_search_entries_checkfortimesheetapprovalpathonthemapper_270
        if_log_pluckifthetimesheettemplateispresent_260_blank_268 >> rail.Label(
            'No') >> sunovion_mapper_file_search_entries_checkfortimesheetapprovalpathonthemapper_270 >> log_pluckifthetimesheetapprovalpathispresent_271
        log_pluckifthetimesheetapprovalpathispresent_271 >> if_log_pluckifthetimesheetapprovalpathispresent_271_present_272
        if_log_pluckifthetimesheetapprovalpathispresent_271_present_272 >> rail.Label(
            'Yes') >> adhoc_http_action_273 >> if_log_timesheetapprovalpathuri_274_present_275
        if_log_timesheetapprovalpathuri_274_present_275 >> rail.Label(
            'Yes') >> update_timesheet_approval_path_for_user_276 >> if_request_residentstate_present_279
        if_log_timesheetapprovalpathuri_274_present_275 >> rail.Label(
            'No') >> if_request_residentstate_present_279
        if_log_pluckifthetimesheetapprovalpathispresent_271_present_272 >> rail.Label(
            'No') >> sunovion_user_logs_file_add_entry_278 >> if_request_residentstate_present_279
        if_request_employeetype_not_equals_to_datacsv_parserparse_csv_6linesfirstcolumn_3_258 >> rail.Label(
            'No') >> if_request_residentstate_present_279
        if_request_employeetype_present_257 >> rail.Label(
            'No') >> if_request_residentstate_present_279
        if_request_residentstate_present_279 >> rail.Label(
            'Yes') >> adhoc_http_action_280 >> adhoc_http_action_282 >> if_log_required_residentstateuri_281_blank_288
        if_log_required_residentstateuri_281_blank_288 >> rail.Label(
            'Yes') >> adhoc_http_action_289 >> update_namefor_resident_state_290 >> update_codefor_resident_state_291 >> adhoc_http_action_292
        adhoc_http_action_292 >> log_required_resident_state_uri_293
        if_log_required_residentstateuri_281_blank_288 >> rail.Label(
            'No') >> log_required_resident_state_uri_293 >> get_final_location_schedule >> if_request_initialschedulename_present_299
        if_request_residentstate_present_279 >> rail.Label(
            'No') >> if_request_initialschedulename_present_299
        if_request_initialschedulename_present_299 >> rail.Label(
            'Yes') >> adhoc_http_action_300 >> if_log_required_office_scheduleuri_301_present_302
        if_log_required_office_scheduleuri_301_present_302 >> rail.Label(
            'Yes') >> adhoc_http_action_303 >> if_officeschedule_displaytext_present_304
        if_officeschedule_displaytext_present_304 >> rail.Label(
            'Yes') >> put_schedule_policy_schedule_for_user_314 >> if_officeschedule_displaytext_blank_315
        if_officeschedule_displaytext_present_304 >> rail.Label(
            'No') >> if_officeschedule_displaytext_blank_315
        if_officeschedule_displaytext_blank_315 >> rail.Label(
            'Yes') >> put_schedule_policy_schedule_for_user_316 >> if_request_costcenter_present_317
        if_officeschedule_displaytext_blank_315 >> rail.Label(
            'No') >> if_request_costcenter_present_317
        if_log_required_office_scheduleuri_301_present_302 >> rail.Label(
            'No') >> if_request_costcenter_present_317
        if_request_initialschedulename_present_299 >> rail.Label(
            'No') >> if_request_costcenter_present_317
        if_request_costcenter_present_317 >> rail.Label(
            'Yes') >> adhoc_http_action_318 >> adhoc_http_action_320 >> if_log_required_costcenteruri_319_present_326
        if_log_required_costcenteruri_319_present_326 >> rail.Label(
            'Yes') >> get_final_costcenter_schedule >> if_log_required_costcenteruri_319_blank_332
        if_log_required_costcenteruri_319_present_326 >> rail.Label(
            'No') >> if_log_required_costcenteruri_319_blank_332
        if_log_required_costcenteruri_319_blank_332 >> rail.Label(
            'Yes') >> if_log_cost_center_length_333_less_than_51_334
        if_log_cost_center_length_333_less_than_51_334 >> rail.Label(
            'Yes') >> adhoc_http_action_335 >> update_namefor_cost_center_336 >> update_codefor_cost_center_337 >> adhoc_http_action_338
        adhoc_http_action_338 >> log_merge_newcost_centerand_additionalcost_centerlistarraywithinitialcost_center_342 >> put_cost_center_schedule_for_user_343
        put_cost_center_schedule_for_user_343 >> log_input_data_346
        if_log_cost_center_length_333_less_than_51_334 >> rail.Label(
            'No') >> sunovion_user_logs_file_add_entry_345 >> log_input_data_346
        if_log_required_costcenteruri_319_blank_332 >> rail.Label(
            'No') >> log_input_data_346
        if_request_costcenter_present_317 >> rail.Label(
            'No') >> log_input_data_346 >> log_existing_data_347 >> log_old_resident_state_349
        log_old_resident_state_349 >> if_log_input_data_346_not_equals_to_dataloggerlog_existing_data_347message_352
        if_log_input_data_346_not_equals_to_dataloggerlog_existing_data_347message_352 >> rail.Label(
            'Yes') >> trigger_child_to_update_timeofftype_for_existing_user >> waitfor_child_to_update_timeofftype_for_existing_user
        waitfor_child_to_update_timeofftype_for_existing_user >> sunovion_user_logs_file_add_entry_357
        if_log_input_data_346_not_equals_to_dataloggerlog_existing_data_347message_352 >> rail.Label(
            'No') >> sunovion_user_logs_file_add_entry_357 >> catch_and_log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
