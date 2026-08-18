import uuid
from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'omdsingapore_timeoff_import_child_{config.instance}',
        description=f'Omdsingapore | Timeoff import - Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
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
            no_task='create_timeoff_import_child_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_timeoff_import_child_log',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_timeoff_import_child_log = rail.CreateLogOperator(
            task_id='create_timeoff_import_child_log'
        )

        log_weekday_3 = rail.PythonOperator(
            task_id='log_weekday_3',
            python_callable=lambda dag_run: datetime.strptime(
                dag_run.conf['startdate'], "%d/%m/%Y").weekday()
        )

        if_log_weekday_3_equals_to_sunday_4 = rail.IfOperator(
            task_id='if_log_weekday_3_equals_to_sunday_4',
            test='''{{ result('log_weekday_3') == 5  or result('log_weekday_3') == 6 }}''',
            yes_task="omdsingapore_timeoffimport_logs_add_entry_5",
            no_task="if_request_isholiday_present_7",
        )

        omdsingapore_timeoffimport_logs_add_entry_5 = rail.WriteLogOperator(
            task_id='omdsingapore_timeoffimport_logs_add_entry_5',
            log='{{ result("create_timeoff_import_child_log") }}',
            message="It's a weekend - {{ result('log_weekday_3') }}",
            severity="Info",
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "leavecode": "{{ dag_run.conf.leavecode }}",
                "startdaytype": "{{ dag_run.conf.startdaytype }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "recordid": "{{ dag_run.conf.recordid }}",
                "status": "{{ dag_run.conf.status }} ",
                "jobstatus": "Skipped",
                "details": "It's a weekend - {{ result('log_weekday_3') }}"
            }
        )

        if_request_isholiday_present_7 = rail.IfOperator(
            task_id='if_request_isholiday_present_7',
            test='''{{ dag_run.conf.isholiday | is_truthy }}''',
            yes_task="log_holidayname_8",
            no_task="if_request_useruri_blank_11",
        )

        log_holidayname_8 = rail.PythonOperator(
            task_id='log_holidayname_8',
            python_callable=lambda dag_run: dag_run.conf['isholiday'][0].get(
                'holidayname') if dag_run.conf['isholiday'] else " "
        )

        omdsingapore_timeoffimport_logs_add_entry_9 = rail.WriteLogOperator(
            task_id='omdsingapore_timeoffimport_logs_add_entry_9',
            log='{{ result("create_timeoff_import_child_log") }}',
            message="It's a holiday - {{ result('log_holidayname_8') }}",
            severity="Info",
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "leavecode": "{{ dag_run.conf.leavecode }}",
                "startdaytype": "{{ dag_run.conf.startdaytype }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "recordid": "{{ dag_run.conf.recordid }}",
                "status": "{{ dag_run.conf.status }}",
                "jobstatus": "Skipped",
                "details": "It's a holiday - {{ result('log_holidayname_8') }}"
            }
        )

        if_request_useruri_blank_11 = rail.IfOperator(
            task_id='if_request_useruri_blank_11',
            test='''{{ dag_run.conf.useruri | is_falsy }}''',
            yes_task="omdsingapore_timeoffimport_logs_add_entry_12",
            no_task="if_request_timeoffuri_blank_14",
        )

        omdsingapore_timeoffimport_logs_add_entry_12 = rail.WriteLogOperator(
            task_id='omdsingapore_timeoffimport_logs_add_entry_12',
            log='{{ result("create_timeoff_import_child_log") }}',
            message="User is not present or disabled",
            severity="Info",
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "leavecode": "{{ dag_run.conf.leavecode }}",
                "startdaytype": "{{ dag_run.conf.startdaytype }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "recordid": "{{ dag_run.conf.recordid }}",
                "status": "{{ dag_run.conf.status }}",
                "jobstatus": "Skipped",
                "details": "User is not present or disabled"
            }
        )

        if_request_timeoffuri_blank_14 = rail.IfOperator(
            task_id='if_request_timeoffuri_blank_14',
            test='''{{ dag_run.conf.timeoffuri | is_falsy }}''',
            yes_task="omdsingapore_timeoffimport_logs_add_entry_15",
            no_task="get_all_custom_fields_18",
        )

        omdsingapore_timeoffimport_logs_add_entry_15 = rail.WriteLogOperator(
            task_id='omdsingapore_timeoffimport_logs_add_entry_15',
            log='{{ result("create_timeoff_import_child_log") }}',
            message="Timeoff is not present",
            severity="Info",
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "leavecode": "{{ dag_run.conf.leavecode }}",
                "startdaytype": "{{ dag_run.conf.startdaytype }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "recordid": "{{ dag_run.conf.recordid }}",
                "status": "{{ dag_run.conf.status }}",
                "jobstatus": "Skipped",
                "details": "Timeoff is not present"
            }
        )

        get_all_custom_fields_18 = rail.RepliconServiceOperator(
            task_id='get_all_custom_fields_18',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:time-off"
            }
        )

        if_startdate_to_date_less_than_today1days_19 = rail.IfOperator(
            task_id='if_startdate_to_date_less_than_today1days_19',
            test=lambda dag_run: datetime.strptime(
                dag_run.conf['startdate'], "%d/%m/%Y") < datetime.today() + timedelta(days=1),
            yes_task="get_timesheet_for_date2_20",
            no_task="before_startype",
        )

        get_timesheet_for_date2_20 = rail.RepliconServiceOperator(
            task_id='get_timesheet_for_date2_20',
            endpoint="/services/TimesheetService1.svc/GetTimesheetForDate2",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "date": {
                    "year": dag_run.conf['startdate'].split('/')[2],
                    "month": dag_run.conf['startdate'].split('/')[1],
                    "day": dag_run.conf['startdate'].split('/')[0]
                },
                "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
            }
        )

        if_timesheet_uri_blank_21 = rail.IfOperator(
            task_id='if_timesheet_uri_blank_21',
            test=lambda: rail.result('get_timesheet_for_date2_20') is null,
            yes_task="omdsingapore_timeoffimport_logs_add_entry_22",
            no_task="get_timesheet_details_24",
        )

        omdsingapore_timeoffimport_logs_add_entry_22 = rail.WriteLogOperator(
            task_id='omdsingapore_timeoffimport_logs_add_entry_22',
            log='{{ result("create_timeoff_import_child_log") }}',
            message="Timesheet is not available for user",
            severity="Info",
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "leavecode": "{{ dag_run.conf.leavecode }}",
                "startdaytype": "{{ dag_run.conf.startdaytype }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "recordid": "{{ dag_run.conf.recordid }}",
                "status": "{{ dag_run.conf.status }}",
                "jobstatus": "Skipped",
                "details": "Timesheet is not available for user"
            }
        )

        get_timesheet_details_24 = rail.RepliconServiceOperator(
            task_id='get_timesheet_details_24',
            endpoint="/services/TimesheetService1.svc/GetTimesheetDetails",
            data={
                "timesheetUri": "{{ result('get_timesheet_for_date2_20').timesheet.uri }}"
            }
        )

        if_d_statusuri_not_ends_with_open_25 = rail.IfOperator(
            task_id='if_d_statusuri_not_ends_with_open_25',
            test=lambda: rail.result('get_timesheet_details_24')[
                'statusUri'].rsplit(':', 1)[-1] != 'open',
            yes_task="omdsingapore_timeoffimport_logs_add_entry_26",
            no_task="before_startype",
        )

        omdsingapore_timeoffimport_logs_add_entry_26 = rail.WriteLogOperator(
            task_id='omdsingapore_timeoffimport_logs_add_entry_26',
            log='{{ result("create_timeoff_import_child_log") }}',
            message="na",
            severity="Info",
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "leavecode": "{{ dag_run.conf.leavecode }}",
                "startdaytype": "{{ dag_run.conf.startdaytype }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "recordid": "{{ dag_run.conf.recordid }}",
                "status": "{{ dag_run.conf.status }}",
                "jobstatus": "Skipped",
                "details": "Timesheet is awaiting approval or fully approved"
            }
        )

        before_startype = rail.EmptyOperator(
            task_id='before_startype'
        )

        if_request_startdaytype_equals_to_halfday_33 = rail.IfOperator(
            task_id='if_request_startdaytype_equals_to_halfday_33',
            test='''{{ dag_run.conf.startdaytype == 'Half Day'  or dag_run.conf.startdaytype == 'Full Day' }}''',
            yes_task="get_time_off_type_assignments_for_user_34",
            no_task="omdsingapore_timeoffimport_logs_add_entry_106",
        )

        get_time_off_type_assignments_for_user_34 = rail.RepliconServiceOperator(
            task_id='get_time_off_type_assignments_for_user_34',
            endpoint="/services/TimeOffService1.svc/GetTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        if_request_startdaytype_equals_to_fullday_35 = rail.IfOperator(
            task_id='if_request_startdaytype_equals_to_fullday_35',
            test='''{{ dag_run.conf.startdaytype == 'Full Day' }}''',
            yes_task="before_status_a",
            no_task="before_startype_half_day",
        )

        before_status_a = rail.EmptyOperator(
            task_id='before_status_a'
        )

        if_request_status_equals_to_a_36 = rail.IfOperator(
            task_id='if_request_status_equals_to_a_36',
            test='''{{ dag_run.conf.status == 'A' }}''',
            yes_task="log_checkif_timeofftypeisassignedtouser_37",
            no_task="before_status_c",
        )

        log_checkif_timeofftypeisassignedtouser_37 = rail.PythonOperator(
            task_id='log_checkif_timeofftypeisassignedtouser_37',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_time_off_type_assignments_for_user_34'), 'uri', dag_run.conf['timeoffuri'], 'uri')
        )

        if_log_checkif_timeofftypeisassignedtouser_37_present_38 = rail.IfOperator(
            task_id='if_log_checkif_timeofftypeisassignedtouser_37_present_38',
            test='''{{ result('log_checkif_timeofftypeisassignedtouser_37') | is_truthy }}''',
            yes_task="get_time_off_details_for_user_and_date_range2_39",
            no_task="omdsingapore_timeoffimport_logs_add_entry_56",
        )

        get_time_off_details_for_user_and_date_range2_39 = rail.RepliconServiceOperator(
            task_id='get_time_off_details_for_user_and_date_range2_39',
            endpoint="/services/TimeOffService1.svc/GetTimeOffDetailsForUserAndDateRange2",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": {
                        "year": dag_run.conf['startdate'].split('/')[2],
                        "month": dag_run.conf['startdate'].split('/')[1],
                        "day": dag_run.conf['startdate'].split('/')[0]
                    },
                    "endDate": {
                        "year": dag_run.conf['startdate'].split('/')[2],
                        "month": dag_run.conf['startdate'].split('/')[1],
                        "day": dag_run.conf['startdate'].split('/')[0]
                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_timeofftype_name_equals_to_dataworkato_service8989a8b5requesttimeoffname_40 = rail.IfOperator(
            task_id='if_timeofftype_name_equals_to_dataworkato_service8989a8b5requesttimeoffname_40',
            test=lambda dag_run: rail.result('get_time_off_details_for_user_and_date_range2_39') and
            rail.result('get_time_off_details_for_user_and_date_range2_39')[0]['timeOffType']['name'] == dag_run.conf['timeoffname'] and
            rail.result('get_time_off_details_for_user_and_date_range2_39')[0]['customFields'][0]['text'] == dag_run.conf['recordid'] and
            rail.result('get_time_off_details_for_user_and_date_range2_39')[
                0]['startDateDetails']['relativeDurationUri'].rsplit(':', 1)[-1] == 'full-day',
            yes_task="omdsingapore_timeoffimport_logs_add_entry_41",
            no_task="if_first_uri_present_44",
        )

        omdsingapore_timeoffimport_logs_add_entry_41 = rail.WriteLogOperator(
            task_id='omdsingapore_timeoffimport_logs_add_entry_41',
            log='{{ result("create_timeoff_import_child_log") }}',
            message="na",
            severity="Info",
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "leavecode": "{{ dag_run.conf.leavecode }}",
                "startdaytype": "{{ dag_run.conf.startdaytype }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "recordid": "{{ dag_run.conf.recordid }}",
                "status": "{{ dag_run.conf.status }}",
                "jobstatus": "Skipped",
                "details": "{{ dag_run.conf.timeoffname }} is already present for this day"
            }
        )

        if_first_uri_present_44 = rail.IfOperator(
            task_id='if_first_uri_present_44',
            test=lambda: rail.result('get_time_off_details_for_user_and_date_range2_39') and rail.result(
                'get_time_off_details_for_user_and_date_range2_39')[0]['uri'] is not null,
            yes_task="delete_time_off_45",
            no_task="create_new_time_off_draft_46",
        )

        delete_time_off_45 = rail.RepliconServiceOperator(
            task_id='delete_time_off_45',
            endpoint="/services/TimeOffService1.svc/DeleteTimeOff",
            data={
                "timeOffUri": "{{ result('get_time_off_details_for_user_and_date_range2_39')[0].uri }}"
            }
        )

        create_new_time_off_draft_46 = rail.RepliconServiceOperator(
            task_id='create_new_time_off_draft_46',
            endpoint="/services/TimeOffService1.svc/CreateNewTimeOffDraft",
            data={
                "ownerUri": "{{ dag_run.conf.useruri }}"
            }
        )

        log_record_i_d_customfielduri_47 = rail.PythonOperator(
            task_id='log_record_i_d_customfielduri_47',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_custom_fields_18'), 'displayText', 'Record ID', 'uri', " ")
        )

        put_time_off2_48 = rail.RepliconServiceOperator(
            task_id='put_time_off2_48',
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: {
                "timeOff": {
                    "target": {
                        "uri": rail.result('create_new_time_off_draft_46')
                    },
                    "owner": {
                        "uri": dag_run.conf['useruri'],
                        "loginName": null,
                        "parameterCorrelationId": null
                    },
                    "timeOffType": {
                        "uri": dag_run.conf['timeoffuri'],
                        "name": null
                    },
                    "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
                    "multiDayUsingStartEndDate": {
                        "timeOffStart": {
                            "date": {
                                "year": dag_run.conf['startdate'].split('/')[2],
                                "month": dag_run.conf['startdate'].split('/')[1],
                                "day": dag_run.conf['startdate'].split('/')[0]
                            },
                            "timeOfDay": null,
                            "relativeDuration": "urn:replicon:time-off-relative-duration:full-day",
                            "specificDuration": null
                        },
                        "timeOffEnd": null
                    },
                    "userExplicitEntries": [],
                    "comments": "Added by Replicon Integration",
                    "customFieldValues": [
                        {
                            "customField": {
                                "uri": rail.result('log_record_i_d_customfielduri_47'),
                                "name": null,
                                "groupUri": "urn:replicon:object-type:time-off"
                            },
                            "text": dag_run.conf['recordid'],
                            "date": null,
                            "dropDownOption": null,
                            "number": null
                        }
                    ]
                }
            }
        )

        publish_time_off_draft_49 = rail.RepliconServiceOperator(
            task_id='publish_time_off_draft_49',
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data={
                "timeOff": "{{ result('create_new_time_off_draft_46') }}"
            }
        )

        force_approve_50 = rail.RepliconServiceOperator(
            task_id='force_approve_50',
            endpoint="/services/TimeOffApprovalService1.svc/ForceApprove",
            data=lambda: {
                "timeOffUri": rail.result('publish_time_off_draft_49')['uri'],
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Approved by Replicon Integration"
            }
        )

        omdsingapore_timeoffimport_logs_add_entry_54 = rail.WriteLogOperator(
            task_id='omdsingapore_timeoffimport_logs_add_entry_54',
            log='{{ result("create_timeoff_import_child_log") }}',
            message="na",
            severity="Success",
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "leavecode": "{{ dag_run.conf.leavecode }}",
                "startdaytype": "{{ dag_run.conf.startdaytype }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "recordid": "{{ dag_run.conf.recordid }}",
                "status": "{{ dag_run.conf.status }}",
                "jobstatus": "Success",
                "details": "{{ result('publish_time_off_draft_49').uri }}"
            }
        )

        omdsingapore_timeoffimport_logs_add_entry_56 = rail.WriteLogOperator(
            task_id='omdsingapore_timeoffimport_logs_add_entry_56',
            log='{{ result("create_timeoff_import_child_log") }}',
            message="na",
            severity="Error",
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "leavecode": "{{ dag_run.conf.leavecode }}",
                "startdaytype": "{{ dag_run.conf.startdaytype }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "recordid": "{{ dag_run.conf.recordid }}",
                "status": "{{ dag_run.conf.status }}",
                "jobstatus": "Failed",
                "details": "{{ dag_run.conf.timeoffname }} is not allowed for booking"
            }
        )

        before_status_c = rail.EmptyOperator(
            task_id='before_status_c'
        )

        if_request_status_equals_to_c_57 = rail.IfOperator(
            task_id='if_request_status_equals_to_c_57',
            test='''{{ dag_run.conf.status == 'C'  or dag_run.conf.status == 'R' }}''',
            yes_task="log_checkif_timeofftypeisassignedtouser_58",
            no_task="before_status_not_a_c_r",
        )

        log_checkif_timeofftypeisassignedtouser_58 = rail.PythonOperator(
            task_id='log_checkif_timeofftypeisassignedtouser_58',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_time_off_type_assignments_for_user_34'), 'uri', dag_run.conf['timeoffuri'], 'uri')
        )

        if_log_checkif_timeofftypeisassignedtouser_58_present_59 = rail.IfOperator(
            task_id='if_log_checkif_timeofftypeisassignedtouser_58_present_59',
            test='''{{ result('log_checkif_timeofftypeisassignedtouser_58') | is_truthy }}''',
            yes_task="get_time_off_details_for_user_and_date_range2_60",
            no_task="omdsingapore_timeoffimport_logs_add_entry_67",
        )

        get_time_off_details_for_user_and_date_range2_60 = rail.RepliconServiceOperator(
            task_id='get_time_off_details_for_user_and_date_range2_60',
            endpoint="/services/TimeOffService1.svc/GetTimeOffDetailsForUserAndDateRange2",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": {
                        "year": dag_run.conf['startdate'].split('/')[2],
                        "month": dag_run.conf['startdate'].split('/')[1],
                        "day": dag_run.conf['startdate'].split('/')[0]
                    },
                    "endDate": {
                        "year": dag_run.conf['startdate'].split('/')[2],
                        "month": dag_run.conf['startdate'].split('/')[1],
                        "day": dag_run.conf['startdate'].split('/')[0]
                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_timeofftype_name_equals_to_dataworkato_service8989a8b5requesttimeoffname_61 = rail.IfOperator(
            task_id='if_timeofftype_name_equals_to_dataworkato_service8989a8b5requesttimeoffname_61',
            test=lambda dag_run: rail.result('get_time_off_details_for_user_and_date_range2_60') and
            rail.result('get_time_off_details_for_user_and_date_range2_60')[0]['timeOffType']['name'] == dag_run.conf['timeoffname'] and
            rail.result('get_time_off_details_for_user_and_date_range2_60')[
                0]['customFields'][0]['text'] == dag_run.conf['recordid'],
            yes_task="delete_time_off_62",
            no_task="omdsingapore_timeoffimport_logs_add_entry_65",
        )

        delete_time_off_62 = rail.RepliconServiceOperator(
            task_id='delete_time_off_62',
            endpoint="/services/TimeOffService1.svc/DeleteTimeOff",
            data={
                "timeOffUri": "{{ result('get_time_off_details_for_user_and_date_range2_60')[0].uri }}"
            }
        )

        omdsingapore_timeoffimport_logs_add_entry_63 = rail.WriteLogOperator(
            task_id='omdsingapore_timeoffimport_logs_add_entry_63',
            log='{{ result("create_timeoff_import_child_log") }}',
            message="na",
            severity="Success",
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "leavecode": "{{ dag_run.conf.leavecode }}",
                "startdaytype": "{{ dag_run.conf.startdaytype }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "recordid": "{{ dag_run.conf.recordid }}",
                "status": "{{ dag_run.conf.status }}",
                "jobstatus": "Success",
                "details": "{{ result('get_time_off_details_for_user_and_date_range2_60')[0].uri }}"
            }
        )

        omdsingapore_timeoffimport_logs_add_entry_65 = rail.WriteLogOperator(
            task_id='omdsingapore_timeoffimport_logs_add_entry_65',
            log='{{ result("create_timeoff_import_child_log") }}',
            message="na",
            severity="Skipped",
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "leavecode": "{{ dag_run.conf.leavecode }}",
                "startdaytype": "{{ dag_run.conf.startdaytype }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "recordid": "{{ dag_run.conf.recordid }}",
                "status": "{{ dag_run.conf.status }}",
                "jobstatus": "Skipped",
                "details": "Timeoff booking is not present"
            }
        )

        omdsingapore_timeoffimport_logs_add_entry_67 = rail.WriteLogOperator(
            task_id='omdsingapore_timeoffimport_logs_add_entry_67',
            log='{{ result("create_timeoff_import_child_log") }}',
            message="na",
            severity="Error",
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "leavecode": "{{ dag_run.conf.leavecode }}",
                "startdaytype": "{{ dag_run.conf.startdaytype }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "recordid": "{{ dag_run.conf.recordid }}",
                "status": "{{ dag_run.conf.status }}",
                "jobstatus": "Failed",
                "details": "{{ dag_run.conf.timeoffname }} is not allowed for booking"
            }
        )

        before_status_not_a_c_r = rail.EmptyOperator(
            task_id='before_status_not_a_c_r'
        )

        if_request_status_not_equals_to_a_68 = rail.IfOperator(
            task_id='if_request_status_not_equals_to_a_68',
            test='''{{ dag_run.conf.status != 'A'  and dag_run.conf.status != 'C'  and dag_run.conf.status != 'R' }}''',
            yes_task="omdsingapore_timeoffimport_logs_add_entry_69",
            no_task="before_startype_half_day",
        )

        omdsingapore_timeoffimport_logs_add_entry_69 = rail.WriteLogOperator(
            task_id='omdsingapore_timeoffimport_logs_add_entry_69',
            log='{{ result("create_timeoff_import_child_log") }}',
            message="na",
            severity="Info",
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "leavecode": "{{ dag_run.conf.leavecode }}",
                "startdaytype": "{{ dag_run.conf.startdaytype }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "recordid": "{{ dag_run.conf.recordid }}",
                "status": "{{ dag_run.conf.status }}",
                "jobstatus": "Skipped",
                "details": "Invalid status code"
            }
        )

        before_startype_half_day = rail.EmptyOperator(
            task_id='before_startype_half_day'
        )

        if_request_startdaytype_equals_to_halfday_70 = rail.IfOperator(
            task_id='if_request_startdaytype_equals_to_halfday_70',
            test='''{{ dag_run.conf.startdaytype == 'Half Day' }}''',
            yes_task="if_request_status_equals_to_a_71",
            no_task="finish",
        )

        if_request_status_equals_to_a_71 = rail.IfOperator(
            task_id='if_request_status_equals_to_a_71',
            test='''{{ dag_run.conf.status == 'A' }}''',
            yes_task="log_checkif_timeofftypeisassignedtouser_72",
            no_task="if_request_status_equals_to_c_92",
        )

        log_checkif_timeofftypeisassignedtouser_72 = rail.PythonOperator(
            task_id='log_checkif_timeofftypeisassignedtouser_72',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_time_off_type_assignments_for_user_34'), 'uri', dag_run.conf['timeoffuri'], 'uri')
        )

        if_log_checkif_timeofftypeisassignedtouser_72_present_73 = rail.IfOperator(
            task_id='if_log_checkif_timeofftypeisassignedtouser_72_present_73',
            test='''{{ result('log_checkif_timeofftypeisassignedtouser_72') | is_truthy }}''',
            yes_task="get_time_off_details_for_user_and_date_range2_74",
            no_task="omdsingapore_timeoffimport_logs_add_entry_91",
        )

        get_time_off_details_for_user_and_date_range2_74 = rail.RepliconServiceOperator(
            task_id='get_time_off_details_for_user_and_date_range2_74',
            endpoint="/services/TimeOffService1.svc/GetTimeOffDetailsForUserAndDateRange2",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": {
                        "year": dag_run.conf['startdate'].split('/')[2],
                        "month": dag_run.conf['startdate'].split('/')[1],
                        "day": dag_run.conf['startdate'].split('/')[0]
                    },
                    "endDate": {
                        "year": dag_run.conf['startdate'].split('/')[2],
                        "month": dag_run.conf['startdate'].split('/')[1],
                        "day": dag_run.conf['startdate'].split('/')[0]
                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_timeofftype_name_equals_to_dataworkato_service8989a8b5requesttimeoffname_75 = rail.IfOperator(
            task_id='if_timeofftype_name_equals_to_dataworkato_service8989a8b5requesttimeoffname_75',
            test=lambda dag_run: rail.result('get_time_off_details_for_user_and_date_range2_74') and
            rail.result('get_time_off_details_for_user_and_date_range2_74')[0]['timeOffType']['name'] == dag_run.conf['timeoffname'] and
            rail.result('get_time_off_details_for_user_and_date_range2_74')[0]['customFields'][0]['text'] == dag_run.conf['recordid'] and
            rail.result('get_time_off_details_for_user_and_date_range2_74')[
                0]['startDateDetails']['relativeDurationUri'].rsplit(':', 1)[-1] == 'half-day',
            yes_task="omdsingapore_timeoffimport_logs_add_entry_76",
            no_task="if_first_uri_present_79",
        )

        omdsingapore_timeoffimport_logs_add_entry_76 = rail.WriteLogOperator(
            task_id='omdsingapore_timeoffimport_logs_add_entry_76',
            log='{{ result("create_timeoff_import_child_log") }}',
            message="na",
            severity="Info",
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "leavecode": "{{ dag_run.conf.leavecode }}",
                "startdaytype": "{{ dag_run.conf.startdaytype }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "recordid": "{{ dag_run.conf.recordid }}",
                "status": "{{ dag_run.conf.status }}",
                "details": "{{ dag_run.conf.timeoffname }} is already present for this day",
                "jobstatus": "Skipped"
            }
        )

        if_first_uri_present_79 = rail.IfOperator(
            task_id='if_first_uri_present_79',
            test=lambda: rail.result('get_time_off_details_for_user_and_date_range2_74') and rail.result(
                'get_time_off_details_for_user_and_date_range2_74')[0]['uri'] is not null,
            yes_task="delete_time_off_80",
            no_task="create_new_time_off_draft_81",
        )

        delete_time_off_80 = rail.RepliconServiceOperator(
            task_id='delete_time_off_80',
            endpoint="/services/TimeOffService1.svc/DeleteTimeOff",
            data={
                "timeOffUri": "{{ result('get_time_off_details_for_user_and_date_range2_74')[0].uri }}"
            }
        )

        create_new_time_off_draft_81 = rail.RepliconServiceOperator(
            task_id='create_new_time_off_draft_81',
            endpoint="/services/TimeOffService1.svc/CreateNewTimeOffDraft",
            data={
                "ownerUri": "{{ dag_run.conf.useruri }}"
            }
        )

        log_record_i_d_customfielduri_82 = rail.PythonOperator(
            task_id='log_record_i_d_customfielduri_82',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_custom_fields_18'), 'displayText', 'Record ID', 'uri', " ")
        )

        put_time_off2_83 = rail.RepliconServiceOperator(
            task_id='put_time_off2_83',
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: {
                "timeOff": {
                    "target": {
                        "uri": rail.result('create_new_time_off_draft_81')
                    },
                    "owner": {
                        "uri": dag_run.conf['useruri'],
                        "loginName": null,
                        "parameterCorrelationId": null
                    },
                    "timeOffType": {
                        "uri": dag_run.conf['timeoffuri'],
                        "name": null
                    },
                    "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
                    "multiDayUsingStartEndDate": {
                        "timeOffStart": {
                            "date": {
                                "year": dag_run.conf['startdate'].split('/')[2],
                                "month": dag_run.conf['startdate'].split('/')[1],
                                "day": dag_run.conf['startdate'].split('/')[0]
                            },
                            "timeOfDay": null,
                            "relativeDuration": "urn:replicon:time-off-relative-duration:half-day",
                            "specificDuration": null
                        },
                        "timeOffEnd": null
                    },
                    "userExplicitEntries": [],
                    "comments": "Added by Replicon Integration",
                    "customFieldValues": [
                        {
                            "customField": {
                                "uri": rail.result('log_record_i_d_customfielduri_82'),
                                "name": null,
                                "groupUri": "urn:replicon:object-type:time-off"
                            },
                            "text": dag_run.conf['recordid'],
                            "date": null,
                            "dropDownOption": null,
                            "number": null
                        }
                    ]
                }
            }
        )

        publish_time_off_draft_84 = rail.RepliconServiceOperator(
            task_id='publish_time_off_draft_84',
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data={
                "timeOff": "{{ result('create_new_time_off_draft_81') }}"
            }
        )

        force_approve_85 = rail.RepliconServiceOperator(
            task_id='force_approve_85',
            endpoint="/services/TimeOffApprovalService1.svc/ForceApprove",
            data=lambda: {
                "timeOffUri": rail.result('publish_time_off_draft_84')['uri'],
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Approved by Replicon Integration"
            }
        )

        omdsingapore_timeoffimport_logs_add_entry_89 = rail.WriteLogOperator(
            task_id='omdsingapore_timeoffimport_logs_add_entry_89',
            log='{{ result("create_timeoff_import_child_log") }}',
            message="na",
            severity="Success",
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "leavecode": "{{ dag_run.conf.leavecode }}",
                "startdaytype": "{{ dag_run.conf.startdaytype }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "recordid": "{{ dag_run.conf.recordid }}",
                "status": "{{ dag_run.conf.status }}",
                "jobstatus": "Success",
                "details": "{{ result('publish_time_off_draft_84').uri }}"
            }
        )

        omdsingapore_timeoffimport_logs_add_entry_91 = rail.WriteLogOperator(
            task_id='omdsingapore_timeoffimport_logs_add_entry_91',
            log='{{ result("create_timeoff_import_child_log") }}',
            message="na",
            severity="Error",
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "leavecode": "{{ dag_run.conf.leavecode }}",
                "startdaytype": "{{ dag_run.conf.startdaytype }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "recordid": "{{ dag_run.conf.recordid }}",
                "status": "{{ dag_run.conf.status }}",
                "jobstatus": "Failed",
                "details": "{{ dag_run.conf.timeoffname }} is not allowed for booking"
            }
        )

        if_request_status_equals_to_c_92 = rail.IfOperator(
            task_id='if_request_status_equals_to_c_92',
            test='''{{ dag_run.conf.status == 'C'  or dag_run.conf.status == 'R' }}''',
            yes_task="log_checkif_timeofftypeisassignedtouser_93",
            no_task="if_request_status_not_equals_to_a_103",
        )

        log_checkif_timeofftypeisassignedtouser_93 = rail.PythonOperator(
            task_id='log_checkif_timeofftypeisassignedtouser_93',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_time_off_type_assignments_for_user_34'), 'uri', dag_run.conf['timeoffuri'], 'uri')
        )

        if_log_checkif_timeofftypeisassignedtouser_93_present_94 = rail.IfOperator(
            task_id='if_log_checkif_timeofftypeisassignedtouser_93_present_94',
            test='''{{ result('log_checkif_timeofftypeisassignedtouser_93') | is_truthy }}''',
            yes_task="get_time_off_details_for_user_and_date_range2_95",
            no_task="omdsingapore_timeoffimport_logs_add_entry_102",
        )

        get_time_off_details_for_user_and_date_range2_95 = rail.RepliconServiceOperator(
            task_id='get_time_off_details_for_user_and_date_range2_95',
            endpoint="/services/TimeOffService1.svc/GetTimeOffDetailsForUserAndDateRange2",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": {
                        "year": dag_run.conf['startdate'].split('/')[2],
                        "month": dag_run.conf['startdate'].split('/')[1],
                        "day": dag_run.conf['startdate'].split('/')[0]
                    },
                    "endDate": {
                        "year": dag_run.conf['startdate'].split('/')[2],
                        "month": dag_run.conf['startdate'].split('/')[1],
                        "day": dag_run.conf['startdate'].split('/')[0]
                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_timeofftype_name_equals_to_dataworkato_service8989a8b5requesttimeoffname_96 = rail.IfOperator(
            task_id='if_timeofftype_name_equals_to_dataworkato_service8989a8b5requesttimeoffname_96',
            test=lambda dag_run: rail.result('get_time_off_details_for_user_and_date_range2_95') and
            rail.result('get_time_off_details_for_user_and_date_range2_95')[0]['timeOffType']['name'] == dag_run.conf['timeoffname'] and
            rail.result('get_time_off_details_for_user_and_date_range2_95')[
                0]['customFields'][0]['text'] == dag_run.conf['recordid'],
            yes_task="delete_time_off_97",
            no_task="omdsingapore_timeoffimport_logs_add_entry_100",
        )

        delete_time_off_97 = rail.RepliconServiceOperator(
            task_id='delete_time_off_97',
            endpoint="/services/TimeOffService1.svc/DeleteTimeOff",
            data={
                "timeOffUri": "{{ result('get_time_off_details_for_user_and_date_range2_95')[0].uri }}"
            }
        )

        omdsingapore_timeoffimport_logs_add_entry_98 = rail.WriteLogOperator(
            task_id='omdsingapore_timeoffimport_logs_add_entry_98',
            log='{{ result("create_timeoff_import_child_log") }}',
            message="na",
            severity="Success",
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "leavecode": "{{ dag_run.conf.leavecode }}",
                "startdaytype": "{{ dag_run.conf.startdaytype }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "recordid": "{{ dag_run.conf.recordid }}",
                "status": "{{ dag_run.conf.status }}",
                "jobstatus": "Success",
                "details": "{{ result('get_time_off_details_for_user_and_date_range2_95')[0].uri }}"
            }
        )

        omdsingapore_timeoffimport_logs_add_entry_100 = rail.WriteLogOperator(
            task_id='omdsingapore_timeoffimport_logs_add_entry_100',
            log='{{ result("create_timeoff_import_child_log") }}',
            message="na",
            severity="Error",
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "leavecode": "{{ dag_run.conf.leavecode }}",
                "startdaytype": "{{ dag_run.conf.startdaytype }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "recordid": "{{ dag_run.conf.recordid }}",
                "status": "{{ dag_run.conf.status }}",
                "jobstatus": "Failed",
                "details": "Timeoff booking is not present"
            }
        )

        omdsingapore_timeoffimport_logs_add_entry_102 = rail.WriteLogOperator(
            task_id='omdsingapore_timeoffimport_logs_add_entry_102',
            log='{{ result("create_timeoff_import_child_log") }}',
            message="na",
            severity="Error",
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "leavecode": "{{ dag_run.conf.leavecode }}",
                "startdaytype": "{{ dag_run.conf.startdaytype }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "recordid": "{{ dag_run.conf.recordid }}",
                "status": "{{ dag_run.conf.status }}",
                "jobstatus": "Failed",
                "details": "{{ dag_run.conf.timeoffname }} is not allowed for booking"
            }
        )

        if_request_status_not_equals_to_a_103 = rail.IfOperator(
            task_id='if_request_status_not_equals_to_a_103',
            test='''{{ dag_run.conf.status != 'A'  and dag_run.conf.status != 'C' and dag_run.conf.status != 'R' }}''',
            yes_task="omdsingapore_timeoffimport_logs_add_entry_104",
            no_task="finish",
        )

        omdsingapore_timeoffimport_logs_add_entry_104 = rail.WriteLogOperator(
            task_id='omdsingapore_timeoffimport_logs_add_entry_104',
            log='{{ result("create_timeoff_import_child_log") }}',
            message="Invalid status code",
            severity="Info",
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "leavecode": "{{ dag_run.conf.leavecode }}",
                "startdaytype": "{{ dag_run.conf.startdaytype }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "recordid": "{{ dag_run.conf.recordid }}",
                "status": "{{ dag_run.conf.status }}",
                "jobstatus": "Skipped",
                "details": "Invalid status code"
            }
        )

        omdsingapore_timeoffimport_logs_add_entry_106 = rail.WriteLogOperator(
            task_id='omdsingapore_timeoffimport_logs_add_entry_106',
            log='{{ result("create_timeoff_import_child_log") }}',
            message="Invalid start date type",
            severity="Info",
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "leavecode": "{{ dag_run.conf.leavecode }}",
                "startdaytype": "{{ dag_run.conf.startdaytype }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "recordid": "{{ dag_run.conf.recordid }}",
                "status": "{{ dag_run.conf.status }}",
                "jobstatus": "Skipped",
                "details": "Invalid start date type"
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ result("create_timeoff_import_child_log") }}',
            trigger_rule='one_failed',
            message="na",
            severity="Error",
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "leavecode": "{{ dag_run.conf.leavecode }}",
                "startdaytype": "{{ dag_run.conf.startdaytype }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "recordid": "{{ dag_run.conf.recordid }}",
                "status": "{{ dag_run.conf.status }}",
                "jobstatus": "Failed",
                "details": config.error_template
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            'No') >> create_timeoff_import_child_log >> log_weekday_3
        log_weekday_3 >> if_log_weekday_3_equals_to_sunday_4
        if_log_weekday_3_equals_to_sunday_4 >> rail.Label(
            'Yes') >> omdsingapore_timeoffimport_logs_add_entry_5 >> finish
        if_log_weekday_3_equals_to_sunday_4 >> rail.Label(
            'No') >> if_request_isholiday_present_7
        if_request_isholiday_present_7 >> rail.Label(
            'Yes') >> log_holidayname_8 >> omdsingapore_timeoffimport_logs_add_entry_9 >> finish
        if_request_isholiday_present_7 >> rail.Label(
            'No') >> if_request_useruri_blank_11
        if_request_useruri_blank_11 >> rail.Label(
            'Yes') >> omdsingapore_timeoffimport_logs_add_entry_12 >> finish
        if_request_useruri_blank_11 >> rail.Label(
            'No') >> if_request_timeoffuri_blank_14
        if_request_timeoffuri_blank_14 >> rail.Label(
            'Yes') >> omdsingapore_timeoffimport_logs_add_entry_15 >> finish
        if_request_timeoffuri_blank_14 >> rail.Label(
            'No') >> get_all_custom_fields_18 >> if_startdate_to_date_less_than_today1days_19
        if_startdate_to_date_less_than_today1days_19 >> rail.Label(
            'Yes') >> get_timesheet_for_date2_20 >> if_timesheet_uri_blank_21
        if_startdate_to_date_less_than_today1days_19 >> rail.Label(
            'No') >> before_startype >> if_request_startdaytype_equals_to_halfday_33
        if_timesheet_uri_blank_21 >> rail.Label(
            'Yes') >> omdsingapore_timeoffimport_logs_add_entry_22 >> finish
        if_timesheet_uri_blank_21 >> rail.Label(
            'No') >> get_timesheet_details_24 >> if_d_statusuri_not_ends_with_open_25
        if_d_statusuri_not_ends_with_open_25 >> rail.Label(
            'Yes') >> omdsingapore_timeoffimport_logs_add_entry_26 >> finish
        if_d_statusuri_not_ends_with_open_25 >> rail.Label(
            'No') >> before_startype >> if_request_startdaytype_equals_to_halfday_33
        if_request_startdaytype_equals_to_halfday_33 >> rail.Label(
            'Yes') >> get_time_off_type_assignments_for_user_34 >> if_request_startdaytype_equals_to_fullday_35
        if_request_startdaytype_equals_to_halfday_33 >> rail.Label(
            'No') >> omdsingapore_timeoffimport_logs_add_entry_106 >> finish
        if_request_startdaytype_equals_to_fullday_35 >> rail.Label(
            'Yes') >> before_status_a >> if_request_status_equals_to_a_36
        if_request_status_equals_to_a_36 >> rail.Label(
            'Yes') >> log_checkif_timeofftypeisassignedtouser_37 >> if_log_checkif_timeofftypeisassignedtouser_37_present_38
        if_log_checkif_timeofftypeisassignedtouser_37_present_38 >> rail.Label(
            'Yes') >> get_time_off_details_for_user_and_date_range2_39 >> if_timeofftype_name_equals_to_dataworkato_service8989a8b5requesttimeoffname_40
        if_timeofftype_name_equals_to_dataworkato_service8989a8b5requesttimeoffname_40 >> rail.Label(
            'Yes') >> omdsingapore_timeoffimport_logs_add_entry_41 >> before_status_c
        if_timeofftype_name_equals_to_dataworkato_service8989a8b5requesttimeoffname_40 >> rail.Label(
            'No') >> if_first_uri_present_44
        if_first_uri_present_44 >> rail.Label(
            'Yes') >> delete_time_off_45 >> create_new_time_off_draft_46
        if_first_uri_present_44 >> rail.Label(
            'No') >> create_new_time_off_draft_46 >> log_record_i_d_customfielduri_47 >> put_time_off2_48 \
            >> publish_time_off_draft_49 >> force_approve_50 >> omdsingapore_timeoffimport_logs_add_entry_54 >> before_status_c
        if_log_checkif_timeofftypeisassignedtouser_37_present_38 >> rail.Label(
            'No') >> omdsingapore_timeoffimport_logs_add_entry_56 >> before_status_c
        if_request_status_equals_to_a_36 >> rail.Label(
            'No') >> before_status_c >> if_request_status_equals_to_c_57
        if_request_status_equals_to_c_57 >> rail.Label(
            'Yes') >> log_checkif_timeofftypeisassignedtouser_58 >> if_log_checkif_timeofftypeisassignedtouser_58_present_59
        if_log_checkif_timeofftypeisassignedtouser_58_present_59 >> rail.Label(
            'Yes') >> get_time_off_details_for_user_and_date_range2_60 >> if_timeofftype_name_equals_to_dataworkato_service8989a8b5requesttimeoffname_61
        if_timeofftype_name_equals_to_dataworkato_service8989a8b5requesttimeoffname_61 >> rail.Label(
            'Yes') >> delete_time_off_62 >> omdsingapore_timeoffimport_logs_add_entry_63 >> before_status_not_a_c_r
        if_timeofftype_name_equals_to_dataworkato_service8989a8b5requesttimeoffname_61 >> rail.Label(
            'No') >> omdsingapore_timeoffimport_logs_add_entry_65 >> before_status_not_a_c_r
        if_log_checkif_timeofftypeisassignedtouser_58_present_59 >> rail.Label(
            'No') >> omdsingapore_timeoffimport_logs_add_entry_67 >> before_status_not_a_c_r
        if_request_status_equals_to_c_57 >> rail.Label(
            'No') >> before_status_not_a_c_r >> if_request_status_not_equals_to_a_68
        if_request_status_not_equals_to_a_68 >> rail.Label(
            'Yes') >> omdsingapore_timeoffimport_logs_add_entry_69 >> before_startype_half_day >> if_request_startdaytype_equals_to_halfday_70
        if_request_status_not_equals_to_a_68 >> rail.Label(
            'No') >> before_startype_half_day
        if_request_startdaytype_equals_to_fullday_35 >> rail.Label(
            'No') >> before_startype_half_day
        if_request_startdaytype_equals_to_halfday_70 >> rail.Label(
            'Yes') >> if_request_status_equals_to_a_71
        if_request_status_equals_to_a_71 >> rail.Label(
            'Yes') >> log_checkif_timeofftypeisassignedtouser_72 >> if_log_checkif_timeofftypeisassignedtouser_72_present_73
        if_log_checkif_timeofftypeisassignedtouser_72_present_73 >> rail.Label(
            'Yes') >> get_time_off_details_for_user_and_date_range2_74 >> if_timeofftype_name_equals_to_dataworkato_service8989a8b5requesttimeoffname_75
        if_timeofftype_name_equals_to_dataworkato_service8989a8b5requesttimeoffname_75 >> rail.Label(
            'Yes') >> omdsingapore_timeoffimport_logs_add_entry_76 >> if_request_status_equals_to_c_92
        if_timeofftype_name_equals_to_dataworkato_service8989a8b5requesttimeoffname_75 >> rail.Label(
            'No') >> if_first_uri_present_79
        if_first_uri_present_79 >> rail.Label(
            'Yes') >> delete_time_off_80 >> create_new_time_off_draft_81
        if_first_uri_present_79 >> rail.Label(
            'No') >> create_new_time_off_draft_81 >> log_record_i_d_customfielduri_82 >> put_time_off2_83 \
            >> publish_time_off_draft_84 >> force_approve_85 >> omdsingapore_timeoffimport_logs_add_entry_89 >> if_request_status_equals_to_c_92
        if_log_checkif_timeofftypeisassignedtouser_72_present_73 >> rail.Label(
            'No') >> omdsingapore_timeoffimport_logs_add_entry_91 >> if_request_status_equals_to_c_92
        if_request_status_equals_to_a_71 >> rail.Label(
            'No') >> if_request_status_equals_to_c_92
        if_request_status_equals_to_c_92 >> rail.Label(
            'Yes') >> log_checkif_timeofftypeisassignedtouser_93 >> if_log_checkif_timeofftypeisassignedtouser_93_present_94
        if_log_checkif_timeofftypeisassignedtouser_93_present_94 >> rail.Label(
            'Yes') >> get_time_off_details_for_user_and_date_range2_95 >> if_timeofftype_name_equals_to_dataworkato_service8989a8b5requesttimeoffname_96
        if_timeofftype_name_equals_to_dataworkato_service8989a8b5requesttimeoffname_96 >> rail.Label(
            'Yes') >> delete_time_off_97 >> omdsingapore_timeoffimport_logs_add_entry_98 >> if_request_status_not_equals_to_a_103
        if_timeofftype_name_equals_to_dataworkato_service8989a8b5requesttimeoffname_96 >> rail.Label(
            'No') >> omdsingapore_timeoffimport_logs_add_entry_100 >> if_request_status_not_equals_to_a_103
        if_log_checkif_timeofftypeisassignedtouser_93_present_94 >> rail.Label(
            'No') >> omdsingapore_timeoffimport_logs_add_entry_102 >> if_request_status_not_equals_to_a_103
        if_request_status_equals_to_c_92 >> rail.Label(
            'No') >> if_request_status_not_equals_to_a_103
        if_request_status_not_equals_to_a_103 >> rail.Label(
            'Yes') >> omdsingapore_timeoffimport_logs_add_entry_104 >> finish
        if_request_status_not_equals_to_a_103 >> rail.Label(
            'No') >> finish
        if_request_startdaytype_equals_to_halfday_70 >> rail.Label(
            'No') >> finish
        finish >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
