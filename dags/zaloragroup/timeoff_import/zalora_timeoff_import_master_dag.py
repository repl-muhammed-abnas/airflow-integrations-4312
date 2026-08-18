from datetime import timedelta, datetime,timezone
import uuid
import rail
from zaloragroup.timeoff_import.utils import python_callable_method, response_filter, request_payload
null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'zaloragroup_timeoff_import_master_{config.instance}',
        description=f'Zalora Timeoff Import Master_ {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath_master,
            sftp_conn_id=config.sftp_conn_id,
            soft_fail_timeout=timedelta(minutes=10)
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='log_current_date',
            no_task='delete_this_dagrun',
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        log_current_date = rail.PythonOperator(
            task_id='log_current_date',
            python_callable= python_callable_method.get_current_date
        )

        if_file_name_ends_with_csv=rail.IfOperator(
            task_id='if_file_name_ends_with_csv',
            test="{{result('new_file_sensor') | file_ext | lower == 'csv'}}",
            yes_task="download_file"
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id = 'download_file',
            sftp_conn_id= config.sftp_conn_id,
            remote_filepath= "{{ result('new_file_sensor') }}"
        )

        declare_list = rail.SetVariableOperator(
            task_id='declare_list',
            append=False,
            name='logFile',
            value=[]
        )

        parse_csv = rail.LoadCSVFileOperator(
            task_id='parse_csv',
            encoding= 'utf-8-sig',
            document="{{result('download_file')}}",
            delimiter = '|',
        )

        get_all_time_off_types=rail.RepliconServiceOperator(
            task_id='get_all_time_off_types',
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
        )

        has_data=rail.IfOperator(
            task_id='has_data',
            test="{{result('parse_csv') | length > 0}}",
            yes_task="foreach_item_in_csv_do"
        )

        foreach_item_in_csv_do=rail.ForEachOperator(
            task_id='foreach_item_in_csv_do',
            items = "{{ result('parse_csv') }}",
            start_task='if_row_has_all_columns_present',
            end_task='foreach_item_in_csv_ends_here'
        )

        if_row_has_all_columns_present=rail.IfOperator(
            task_id='if_row_has_all_columns_present',
            test="{{ result('foreach_item_in_csv_do').USER_NAME.strip() | is_truthy  and result('foreach_item_in_csv_do').TIME_OFF_TYPE.strip() | is_truthy \
                and result('foreach_item_in_csv_do').ENTRY_DATE.strip() | is_truthy  and result('foreach_item_in_csv_do').START_DAY_HOURS.strip() | is_truthy \
                and result('foreach_item_in_csv_do').APPROVAL_STATUS.strip() | is_truthy  }}",
            yes_task="log_get_the_required_timeoffuri",
            no_task="is_mandatory_columns_blank",
        )

        log_get_the_required_timeoffuri=rail.PythonOperator(
            task_id='log_get_the_required_timeoffuri',
            python_callable= python_callable_method.get_required_timeoffuri,
            op_args=["{{result('foreach_item_in_csv_do').TIME_OFF_TYPE.strip()}}"]
        )

        if_the_timeoffuri_is_present=rail.IfOperator(
            task_id='if_the_timeoffuri_is_present',
            test="{{ result('log_get_the_required_timeoffuri') | is_truthy }}",
            yes_task="get_user_data",
            no_task="log_timeoff_type_not_available",
        )

        get_user_data=rail.RepliconServiceOperator(
            task_id='get_user_data',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": null,
                    "filterDefinitionUri": "urn:replicon:user-list-filter:login-name"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": {
                        "uri": null,
                        "uris": [],
                        "bool": null,
                        "date": null,
                        "money": null,
                        "number": null,
                        "text": "{{ result('foreach_item_in_csv_do').USER_NAME.strip() }}",
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "dateTimeUtc": null
                    },
                    "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            },
            response_filter=response_filter.get_user_details,
        )

        if_useruri_is_present=rail.IfOperator(
            task_id='if_useruri_is_present',
            test=lambda: bool(rail.result('get_user_data') and
                              rail.result('get_user_data')[0]['uri']),
            yes_task='if_userstatus_is_present_and_true',
            no_task='log_user_not_available_in_replicon',
        )

        if_userstatus_is_present_and_true=rail.IfOperator(
            task_id='if_userstatus_is_present_and_true',
            test=lambda: bool(rail.result('get_user_data') and
                              rail.result('get_user_data')[0]['status'] and
                              rail.result('get_user_data')[0]['status'] == 'True'),
            yes_task="get_timesheet_uri",
            no_task="log_user_is_disabled_in_replicon",
        )

        get_timesheet_uri=rail.RepliconServiceOperator(
            task_id='get_timesheet_uri',
            endpoint="/services/TimesheetService1.svc/GetTimesheetForDate2",
            data= lambda: {
                "userUri": rail.result('get_user_data')[0]['uri'],
                "date": request_payload.get_entry_date(),
                "timesheetGetOptionUri": null
            },
            response_filter= response_filter.get_timesheet_uri,
        )

        is_timesheet_uri_present=rail.IfOperator(
            task_id='is_timesheet_uri_present',
            test=lambda: bool(rail.result('get_timesheet_uri')),
            yes_task="get_timesheet_details",
            no_task="is_timesheet_status_not_equals_approved",
        )


        get_timesheet_details=rail.RepliconServiceOperator(
            task_id='get_timesheet_details',
            endpoint="/services/TimesheetService1.svc/GetTimesheetDetails",
            data={
                "timesheetUri": "{{ result('get_timesheet_uri') }}"
            },
            response_filter= response_filter.get_timesheet_status
        )

        is_timesheet_status_not_equals_approved=rail.IfOperator(
            task_id='is_timesheet_status_not_equals_approved',
            test="{{ result('get_timesheet_details') != 'approved' }}",
            yes_task="get_timoff_booking_for_date",
            no_task="log_timesheet_in_approved_status",
        )


        get_timoff_booking_for_date=rail.RepliconServiceOperator(
            task_id='get_timoff_booking_for_date',
            endpoint="/services/TimeOffService1.svc/GetTimeOffDetailsForUserAndDateRange2",
            data= lambda: {
                "userUri": rail.result('get_user_data')[0]['uri'] ,
                "dateRange": {
                    "startDate": request_payload.get_entry_date(),
                    "endDate": request_payload.get_entry_date(),
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        is_timeoff_booking_uri_not_present=rail.IfOperator(
            task_id='is_timeoff_booking_uri_not_present',
            test="{{ result('get_timoff_booking_for_date') | length == 0 }}",
            yes_task="create_new_time_off_draft",
            no_task="dummy_timeoff_uri_present",
        )


        create_new_time_off_draft=rail.RepliconServiceOperator(
            task_id='create_new_time_off_draft',
            endpoint="/services/TimeOffService1.svc/CreateNewTimeOffDraft",
            data= lambda: {
                "ownerUri": rail.result('get_user_data')[0]['uri']
            }
        )


        update_time_off_type=rail.RepliconServiceOperator(
            task_id='update_time_off_type',
            endpoint="/services/TimeOffService1.svc/UpdateTimeOffType",
            data= lambda: {
                "timeOffUri": rail.result('create_new_time_off_draft'),
                "timeOffTypeUri": rail.result('log_get_the_required_timeoffuri')
            }
        )


        update_time_off_comments=rail.RepliconServiceOperator(
            task_id='update_time_off_comments',
            endpoint="/services/TimeOffService1.svc/UpdateTimeOffComments",
            data= lambda: {
                "timeOffUri": rail.result('create_new_time_off_draft'),
                "comments": "Created by Integration"
            }
        )

        if_startdayhours_equals_4=rail.IfOperator(
            task_id='if_startdayhours_equals_4',
            test="{{ result('foreach_item_in_csv_do').START_DAY_HOURS.strip() == '4'}}",
            yes_task="configure_half_day_time_off",
            no_task="if_startdayhours_equals_8",
        )

        configure_half_day_time_off=rail.RepliconServiceOperator(
            task_id='configure_half_day_time_off',
            endpoint="/services/TimeOffService1.svc/ConfigureSingleDayTimeOff",
            data= lambda: {
                "timeOffUri": rail.result('create_new_time_off_draft'),
                "date": {
                    "date": request_payload.get_entry_date(),
                    "timeOfDay": null,
                    "relativeDuration": "urn:replicon:time-off-relative-duration:half-day",
                    "specificDuration": null
                }
            }
        )

        if_startdayhours_equals_8=rail.IfOperator(
            task_id='if_startdayhours_equals_8',
            test="{{ result('foreach_item_in_csv_do').START_DAY_HOURS.strip() == '8'}}",
            yes_task="configure_single_day_time_off",
            no_task="log_incorrect_hours",
        )

        configure_single_day_time_off=rail.RepliconServiceOperator(
            task_id='configure_single_day_time_off',
            endpoint="/services/TimeOffService1.svc/ConfigureSingleDayTimeOff",
            data=lambda: {
                "timeOffUri":rail.result('create_new_time_off_draft'),
                "date": {
                    "date": request_payload.get_entry_date(),
                    "timeOfDay": null,
                    "relativeDuration": "urn:replicon:time-off-relative-duration:full-day",
                    "specificDuration": null
                }
            }
        )

        log_incorrect_hours=rail.SetVariableOperator(
            task_id='log_incorrect_hours',
            append=True,
            name='{{ result("declare_list").name }}',
            value={
                "loginname": "{{ result('foreach_item_in_csv_do').USER_NAME.strip() }}",
                "timeofftype": "{{ result('foreach_item_in_csv_do').TIME_OFF_TYPE.strip()  }}" ,
                "entrydate": "{{result('foreach_item_in_csv_do').ENTRY_DATE.strip() }}",
                "status": "Ignored",
                "reason": "TimeOff "+ "{{ result('foreach_item_in_csv_do').TIME_OFF_TYPE.strip()  }}" + 
                    " not added for user " + 
                    "{{ result('foreach_item_in_csv_do').USER_NAME.strip() }}" + 
                    " for date " + "{{result('foreach_item_in_csv_do').ENTRY_DATE.strip()}}" + " as hours should either be 4 or 8."
            }
        )

        publish_time_off_draft=rail.RepliconServiceOperator(
            task_id='publish_time_off_draft',
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data={
                "timeOff": "{{ result('create_new_time_off_draft') }}"
            },
            response_filter = response_filter.get_published_timeoff_draft_uri
        )

        approve_timeoff_booking=rail.RepliconServiceOperator(
            task_id='approve_timeoff_booking',
            endpoint="/services/TimeOffApprovalService1.svc/ForceApprove",
            data=lambda: {
                "timeOffUri": rail.result('publish_time_off_draft'),
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Approved by Integration"
            }
        )

        log_timeoff_success=rail.SetVariableOperator(
            task_id='log_timeoff_success',
            append=True,
            name='{{ result("declare_list").name }}',
            value={
                "loginname": "{{ result('foreach_item_in_csv_do').USER_NAME.strip() }}",
                "timeofftype": "{{ result('foreach_item_in_csv_do').TIME_OFF_TYPE.strip()  }}" ,
                "entrydate": "{{result('foreach_item_in_csv_do').ENTRY_DATE.strip() }}",
                "status": "Success",
                "reason": "TimeOff "+ "{{ result('foreach_item_in_csv_do').TIME_OFF_TYPE.strip()  }}" + 
                    " {{'added' if result('get_timoff_booking_for_date') | length == 0  else 'updated'}} for user " + 
                    "{{ result('foreach_item_in_csv_do').USER_NAME.strip() }}" + 
                    " for date " + "{{result('foreach_item_in_csv_do').ENTRY_DATE.strip()}}"
            }
        )

        dummy_timeoff_uri_present=rail.EmptyOperator(
            task_id='dummy_timeoff_uri_present',
        )

        is_approvalstatus_equals_withdrawn=rail.IfOperator(
            task_id='is_approvalstatus_equals_withdrawn',
            test=lambda: bool(rail.result('foreach_item_in_csv_do')['APPROVAL_STATUS'].lower() == 'withdrawn'
                and rail.result('get_timoff_booking_for_date')[0]['timeOffType']['displayText'].lower()
                == rail.result('foreach_item_in_csv_do')['TIME_OFF_TYPE'].lower()),
            yes_task="delete_withdrawn_timeoff_booking",
            no_task="is_approvalstatus_not_equals_withdrawn",
        )


        delete_withdrawn_timeoff_booking=rail.RepliconServiceOperator(
            task_id='delete_withdrawn_timeoff_booking',
            endpoint="/services/TimeOffService1.svc/DeleteTimeOff",
            data=lambda :{
                "timeOffUri": rail.result('get_timoff_booking_for_date')[0]['uri']
            }
        )

        log_timeoff_deleted=rail.SetVariableOperator(
            task_id='log_timeoff_deleted',
            append=True,
            name='{{ result("declare_list").name }}',
            value={
                "loginname": "{{ result('foreach_item_in_csv_do').USER_NAME.strip() }}",
                "timeofftype": "{{ result('foreach_item_in_csv_do').TIME_OFF_TYPE.strip()  }}" ,
                "entrydate": "{{result('foreach_item_in_csv_do').ENTRY_DATE.strip() }}",
                "status": "Success",
                "reason": "TimeOff "+ "{{ result('foreach_item_in_csv_do').TIME_OFF_TYPE.strip()  }}" + 
                    " deleted for user " + "{{ result('foreach_item_in_csv_do').USER_NAME.strip() }}" + 
                    " for date " + 
                    "{{result('foreach_item_in_csv_do').ENTRY_DATE.strip()}}"
            }
        )

        is_approvalstatus_not_equals_withdrawn=rail.IfOperator(
            task_id='is_approvalstatus_not_equals_withdrawn',
            test=lambda: bool(rail.result('foreach_item_in_csv_do')['APPROVAL_STATUS'].lower() != 'withdrawn'),
            yes_task="is_calendardayduration_hours_not_equals_to_startdayhours",
            no_task="dummy_timeoff_process_complete",
        )

        is_calendardayduration_hours_not_equals_to_startdayhours=rail.IfOperator(
            task_id='is_calendardayduration_hours_not_equals_to_startdayhours',
            test=lambda: bool(str(rail.result('get_timoff_booking_for_date')[0]['totalDuration']['calendarDayDuration']['hours'])
                != rail.result('foreach_item_in_csv_do')['START_DAY_HOURS'].strip()
                and rail.result('get_timoff_booking_for_date')[0]['timeOffType']['displayText'].lower()
                == rail.result('foreach_item_in_csv_do')['TIME_OFF_TYPE'].lower() ),
            yes_task="delete_timeoff_booking",
            no_task="log_on_given_date_booking_exists",
        )


        delete_timeoff_booking=rail.RepliconServiceOperator(
            task_id='delete_timeoff_booking',
            endpoint="/services/TimeOffService1.svc/DeleteTimeOff",
            data=lambda :{
                "timeOffUri": rail.result('get_timoff_booking_for_date')[0]['uri']
            }
        )

        dummy_timeoff_process_complete = rail.EmptyOperator(
            task_id ="dummy_timeoff_process_complete"
        )

        log_on_given_date_booking_exists=rail.SetVariableOperator(
            task_id='log_on_given_date_booking_exists',
            append=True,
            name='{{ result("declare_list").name }}',
            value={
                "loginname": "{{ result('foreach_item_in_csv_do').USER_NAME.strip() }}",
                "timeofftype": "{{ result('foreach_item_in_csv_do').TIME_OFF_TYPE.strip()  }}" ,
                "entrydate": "{{result('foreach_item_in_csv_do').ENTRY_DATE.strip() }}",
                "status": "Ignored",
                "reason": "TimeOff " + "{{ result('foreach_item_in_csv_do').TIME_OFF_TYPE.strip()  }}"
                    + " not added for user " + "{{ result('foreach_item_in_csv_do').USER_NAME.strip() }}"
                    + " for date " + "{{result('foreach_item_in_csv_do').ENTRY_DATE.strip()}}"
                    + " as on the given date already a booking is there for timeoff type "
                    + "{{ result('foreach_item_in_csv_do').TIME_OFF_TYPE.strip()  }}"
            }
        )

        log_timesheet_in_approved_status=rail.SetVariableOperator(
            task_id='log_timesheet_in_approved_status',
            append=True,
            name='{{ result("declare_list").name }}',
            value={
                "loginname": "{{ result('foreach_item_in_csv_do').USER_NAME.strip() }}",
                "timeofftype": "{{ result('foreach_item_in_csv_do').TIME_OFF_TYPE.strip()  }}" ,
                "entrydate": "{{result('foreach_item_in_csv_do').ENTRY_DATE.strip() }}",
                "status": "Error",
                "reason": "TimeOff " + "{{ result('foreach_item_in_csv_do').TIME_OFF_TYPE.strip()  }}"
                    + " not imported for user " + "{{ result('foreach_item_in_csv_do').USER_NAME.strip() }}"
                    + " for date " + "{{result('foreach_item_in_csv_do').ENTRY_DATE.strip()}}" + " as the timesheet for the user with login name "
                    + "{{ result('foreach_item_in_csv_do').USER_NAME.strip() }}" + " is in approved status."
            }
        )

        log_user_is_disabled_in_replicon=rail.SetVariableOperator(
            task_id='log_user_is_disabled_in_replicon',
            append=True,
            name='{{ result("declare_list").name }}',
            value={
                "loginname": "{{ result('foreach_item_in_csv_do').USER_NAME.strip() }}",
                "timeofftype": "{{ result('foreach_item_in_csv_do').TIME_OFF_TYPE.strip()  }}" ,
                "entrydate": "{{result('foreach_item_in_csv_do').ENTRY_DATE.strip() }}",
                "status": "Error",
                "reason": "TimeOff " + "{{ result('foreach_item_in_csv_do').TIME_OFF_TYPE.strip()  }}"
                    + " not added for user " + "{{ result('foreach_item_in_csv_do').USER_NAME.strip() }}"
                    + " for date " + "{{result('foreach_item_in_csv_do').ENTRY_DATE.strip()}}"
                    + " as user with login name " + "{{ result('foreach_item_in_csv_do').USER_NAME.strip() }}"
                    + " is disabled in Replicon."
            }
        )

        log_user_not_available_in_replicon=rail.SetVariableOperator(
            task_id='log_user_not_available_in_replicon',
            append=True,
            name='{{ result("declare_list").name }}',
            value={
                "loginname": "{{ result('foreach_item_in_csv_do').USER_NAME.strip() }}",
                "timeofftype": "{{ result('foreach_item_in_csv_do').TIME_OFF_TYPE.strip()  }}" ,
                "entrydate": "{{result('foreach_item_in_csv_do').ENTRY_DATE.strip() }}",
                "status": "Error",
                "reason": "TimeOff " + "{{ result('foreach_item_in_csv_do').TIME_OFF_TYPE.strip()  }}"
                    + " not added for user " + "{{ result('foreach_item_in_csv_do').USER_NAME.strip() }}"
                    + " for date " + "{{result('foreach_item_in_csv_do').ENTRY_DATE.strip()}}" + " as user with login name "
                    + "{{ result('foreach_item_in_csv_do').USER_NAME.strip() }}" + " is not available in Replicon."
            }
        )

        log_timeoff_type_not_available=rail.SetVariableOperator(
            task_id='log_timeoff_type_not_available',
            append=True,
            name='{{ result("declare_list").name }}',
            value={
                "loginname": "{{ result('foreach_item_in_csv_do').USER_NAME.strip() }}",
                "timeofftype": "{{ result('foreach_item_in_csv_do').TIME_OFF_TYPE.strip()  }}" ,
                "entrydate": "{{result('foreach_item_in_csv_do').ENTRY_DATE.strip() }}",
                "status": "Error",
                "reason": "TimeOff " + "{{ result('foreach_item_in_csv_do').TIME_OFF_TYPE.strip()  }}"
                    + " not added for user " + "{{ result('foreach_item_in_csv_do').USER_NAME.strip() }}"
                    + " for date " + "{{result('foreach_item_in_csv_do').ENTRY_DATE.strip()}}"
                    + " as timeoff Type " + "{{ result('foreach_item_in_csv_do').TIME_OFF_TYPE.strip() }}"
                    + " is not available in Replicon."
            }
        )

        is_mandatory_columns_blank=rail.IfOperator(
            task_id='is_mandatory_columns_blank',
            test="{{result('foreach_item_in_csv_do').USER_NAME.strip() | is_falsy  or result('foreach_item_in_csv_do').TIME_OFF_TYPE.strip() | is_falsy \
                or result('foreach_item_in_csv_do').ENTRY_DATE.strip() | is_falsy  or result('foreach_item_in_csv_do').START_DAY_HOURS.strip() | is_falsy \
                or result('foreach_item_in_csv_do').APPROVAL_STATUS.strip() | is_falsy  }}",
            yes_task="log_some_column_is_blank",
            no_task="dummy_timeoff_process_complete",
        )

        log_some_column_is_blank=rail.SetVariableOperator(
            task_id='log_some_column_is_blank',
            append=True,
            name='{{ result("declare_list").name }}',
            value={
                "loginname": "{{ result('foreach_item_in_csv_do').USER_NAME.strip() }}",
                "timeofftype": "{{ result('foreach_item_in_csv_do').TIME_OFF_TYPE.strip()  }}" ,
                "entrydate": "{{result('foreach_item_in_csv_do').ENTRY_DATE.strip() }}",
                "status": "Ignored",
                "reason": "{{ '' if result('foreach_item_in_csv_do').USER_NAME.strip() else 'LoginName is not available in the input file,'}}"
                    + "{{'' if result('foreach_item_in_csv_do').TIME_OFF_TYPE.strip() else 'Timeoff type is not available in the input file,'}}"
                    + "{{ '' if result('foreach_item_in_csv_do').ENTRY_DATE.strip() else 'Entry date is not available in the input file,'}}"
                    + "{{ '' if result('foreach_item_in_csv_do').START_DAY_HOURS.strip() else 'Start day hours is not available in the input file,'}}"
                    + "{{ '' if result('foreach_item_in_csv_do').APPROVAL_STATUS.strip() else 'Approval Status is not available in the input file,'}}"
            }
        )

        foreach_item_in_csv_ends_here=rail.EmptyOperator(
            task_id='foreach_item_in_csv_ends_here'
        )

        render_logs_csv=rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.get_dag_run_var(rail.result('declare_list')['name']),
            header=['Login Name',
                    'Timeoff Type',
                    'Entry Date',
                    'Status',
                    'Reason'],
            row=[
                "{{ item.loginname }}",
                "{{ item.timeofftype }}",
                "{{ item.entrydate }}",
                "{{ item.status }}",
                "{{ item.reason }}"
            ],
        )

        log_date = rail.PythonOperator(
            task_id = 'log_date',
            python_callable= lambda: f"{datetime.now(timezone.utc).strftime('%m/%d/%Y')}"
        )

        log_message_filenamefullpath = rail.PythonOperator(
            task_id='log_message_filenamefullpath',
            python_callable=lambda:  f"TimeoffImportLogs_{rail.result('log_current_date')}.csv"
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('render_logs_csv')}}",
            output_file_name='{{ result("log_message_filenamefullpath") }}',
            expires_in_seconds=7*24*60*60,
        )

        send_mail = rail.EmailOperator(
            task_id='send_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject=config.company_key + " | " + "Timeoff import completed on " + datetime.now(timezone.utc).strftime("%m/%d/%Y"),
            html_content="templates/emails/import_completed.html",
            params=None,
        )

        archive_file=rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename="{{ result('new_file_sensor') }}",
            sftp_conn_id=config.sftp_conn_id,
            new_filename=config.archive_filepath + "Processed_Input_"+"{{ result('new_file_sensor') | file_name }}"
        )


        upload_logs=rail.SFTPUploadFileOperator(
            task_id='upload_logs',
            sftp_conn_id= config.sftp_conn_id,
            content="{{ result('render_logs_csv') }}",
            remote_filepath= config.log_filepath + datetime.now(timezone.utc).strftime("%m_%d_%Y_T%H_%M_%S") + '.csv'
        )

        new_file_sensor >> was_new_file_found >> rail.Label("Yes") >> log_current_date >> if_file_name_ends_with_csv
        if_file_name_ends_with_csv >> rail.Label('Yes') >> download_file
        download_file >> archive_file >> declare_list >> parse_csv >> get_all_time_off_types
        get_all_time_off_types >> has_data
        has_data >> rail.Label('Yes') >> foreach_item_in_csv_do >> if_row_has_all_columns_present
        if_row_has_all_columns_present >> rail.Label('Yes')  >> log_get_the_required_timeoffuri >> if_the_timeoffuri_is_present
        if_the_timeoffuri_is_present >> rail.Label('Yes')  >> get_user_data >> if_useruri_is_present
        if_useruri_is_present >> rail.Label('Yes')  >> if_userstatus_is_present_and_true
        if_userstatus_is_present_and_true >> rail.Label('Yes') >> get_timesheet_uri >> is_timesheet_uri_present
        is_timesheet_uri_present >> rail.Label('Yes')  >> get_timesheet_details >> is_timesheet_status_not_equals_approved
        is_timesheet_uri_present >> rail.Label('No') >> is_timesheet_status_not_equals_approved
        is_timesheet_status_not_equals_approved >> rail.Label('Yes')  >> get_timoff_booking_for_date
        get_timoff_booking_for_date >> is_timeoff_booking_uri_not_present
        is_timeoff_booking_uri_not_present >> rail.Label('Yes')  >> create_new_time_off_draft >> update_time_off_type
        update_time_off_type >> update_time_off_comments >> if_startdayhours_equals_4
        if_startdayhours_equals_4 >> rail.Label('Yes')  >> configure_half_day_time_off >> publish_time_off_draft
        if_startdayhours_equals_4 >> rail.Label('No') >> if_startdayhours_equals_8
        if_startdayhours_equals_8 >> rail.Label('Yes')  >> configure_single_day_time_off >> publish_time_off_draft
        if_startdayhours_equals_8 >> rail.Label('No') >> log_incorrect_hours >> dummy_timeoff_process_complete
        publish_time_off_draft >> approve_timeoff_booking >> log_timeoff_success >> dummy_timeoff_process_complete
        is_timeoff_booking_uri_not_present >> rail.Label('No') >> dummy_timeoff_uri_present >> is_approvalstatus_equals_withdrawn
        is_approvalstatus_equals_withdrawn >> rail.Label(
            'Yes')  >> delete_withdrawn_timeoff_booking >> log_timeoff_deleted >> dummy_timeoff_process_complete
        is_approvalstatus_equals_withdrawn >> rail.Label('No') >> is_approvalstatus_not_equals_withdrawn
        is_approvalstatus_not_equals_withdrawn >> rail.Label('Yes')  >> is_calendardayduration_hours_not_equals_to_startdayhours
        is_calendardayduration_hours_not_equals_to_startdayhours >> rail.Label('Yes')  >> delete_timeoff_booking
        delete_timeoff_booking >> create_new_time_off_draft
        is_calendardayduration_hours_not_equals_to_startdayhours >> rail.Label(
            'No') >> log_on_given_date_booking_exists >> dummy_timeoff_process_complete
        is_approvalstatus_not_equals_withdrawn >> rail.Label('No') >> dummy_timeoff_process_complete
        is_timesheet_status_not_equals_approved >> rail.Label('No') >> log_timesheet_in_approved_status >> dummy_timeoff_process_complete
        if_userstatus_is_present_and_true >> rail.Label('No') >> log_user_is_disabled_in_replicon >> dummy_timeoff_process_complete
        if_useruri_is_present >> rail.Label('No') >> log_user_not_available_in_replicon >> dummy_timeoff_process_complete
        if_the_timeoffuri_is_present >> rail.Label('No') >> log_timeoff_type_not_available >> dummy_timeoff_process_complete
        if_row_has_all_columns_present >> rail.Label('No') >> is_mandatory_columns_blank
        is_mandatory_columns_blank >> rail.Label('Yes') >> log_some_column_is_blank
        log_some_column_is_blank >> dummy_timeoff_process_complete >> foreach_item_in_csv_ends_here >> render_logs_csv
        is_mandatory_columns_blank >> rail.Label(
            'No') >> dummy_timeoff_process_complete >> foreach_item_in_csv_ends_here >> render_logs_csv
        render_logs_csv >>log_date >> log_message_filenamefullpath >> generate_download_link
        generate_download_link >> send_mail >> upload_logs
        foreach_item_in_csv_do >> foreach_item_in_csv_ends_here
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun

    return dag

rail.for_each_instance(create_dag)
