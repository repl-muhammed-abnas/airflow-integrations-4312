
import uuid
import rail
from horizonmedia.time_off_sync_v1.utils import request_payload
from horizonmedia.time_off_sync_v1.utils import formatted_data

null = None


def create_child_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    with rail.create_airflow_dag(
        dag_id=f'horizonmedia_timeoff_importchild{dag_id_postfix}_v1',
        description=f'HorizonMedia | Timeoff import - Child{dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_child_active_runs,
    ) as dag:
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")
        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )
        check_for_wday = rail.IfOperator(
            task_id='check_for_wday',
            test=lambda dag_run: formatted_data.check_for_weekday(
                dag_run).get('status'),
            yes_task="check_for_required_parameter",
            no_task="check_for_wday_logs_entry",
        )
        check_for_wday_logs_entry = rail.WriteLogOperator(
            task_id='check_for_wday_logs_entry',
            log="{{ result('create_log') }}",
            severity="Skipped",
            message="fixme get message from prop ",
            properties=lambda dag_run: {
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.Employeeid }}",
                "timeofftype": "{{ dag_run.conf.Timeofftype }}",
                "startdate": "{{ dag_run.conf.Startdate }}",
                "enddate": "{{ dag_run.conf.Enddate }}",
                "timeoffhrs": "{{ dag_run.conf.Timeoffhrs }}",
                "action": "{{ dag_run.conf.Action }}",
                "uniqueid": "{{ dag_run.conf.Uniqueid }}",
                "md5": "{{ dag_run.conf.md5 }}",
                "status": "Skipped",
                "details": formatted_data.check_for_weekday(dag_run).get('message')
            }
        )
        check_for_required_parameter = rail.PythonOperator(
            task_id='check_for_required_parameter',
            python_callable=formatted_data.required_parameter,
        )

        all_required_parameter = rail.IfOperator(
            task_id='all_required_parameter',
            test='''{{ result('check_for_required_parameter').get('status')}}''',
            yes_task="check_start_date_in_range",
            no_task="all_required_parameter_logs_entry",
        )
        all_required_parameter_logs_entry = rail.WriteLogOperator(
            task_id='all_required_parameter_logs_entry',
            log="{{ result('create_log') }}",
            severity="Skipped",
            message="Required Parameter is missing",
            properties=lambda dag_run: {
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.Employeeid }}",
                "timeofftype": "{{ dag_run.conf.Timeofftype }}",
                "startdate": "{{ dag_run.conf.Startdate }}",
                "enddate": "{{ dag_run.conf.Enddate }}",
                "timeoffhrs": "{{ dag_run.conf.Timeoffhrs }}",
                "action": "{{ dag_run.conf.Action }}",
                "uniqueid": "{{ dag_run.conf.Uniqueid }}",
                "md5": "{{ dag_run.conf.md5 }}",
                "status": "Skipped",
                "details": "{{ result('check_for_required_parameter').get('message') }}"
            }
        )

        check_start_date_in_range = rail.IfOperator(
            task_id='check_start_date_in_range',
            test=formatted_data.check_startdate_eligible,
            yes_task="get_timesheet_details_by_date",
            no_task="empty1",
        )
        empty1 = rail.EmptyOperator(
            task_id='empty1',
        )
        get_timesheet_details_by_date = rail.RepliconServiceOperator(
            task_id='get_timesheet_details_by_date',
            endpoint="/services/TimesheetService1.svc/GetTimesheetDetailsForDate",
            data=request_payload.get_timesheet_details_by_date_payload,
        )

        get_timesheet_uri_available = rail.IfOperator(
            task_id='get_timesheet_uri_available',
            test='''{{result('get_timesheet_details_by_date') | is_truthy and result('get_timesheet_details_by_date').timesheet.uri | is_truthy }}''',
            yes_task="empty2",
            no_task="timesheet_uri_notpresent_logs_entry",
        )
        empty2 = rail.EmptyOperator(
            task_id='empty2',
        )

        timesheet_uri_notpresent_logs_entry = rail.WriteLogOperator(
            task_id='timesheet_uri_notpresent_logs_entry',
            log="{{ result('create_log') }}",
            severity="skipped",
            message="timesheet uri not present",
            properties=lambda dag_run: {
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.Employeeid }}",
                "timeofftype": "{{ dag_run.conf.Timeofftype }}",
                "startdate": "{{ dag_run.conf.Startdate }}",
                "enddate": "{{ dag_run.conf.Enddate }}",
                "timeoffhrs": "{{ dag_run.conf.Timeoffhrs }}",
                "action": "{{ dag_run.conf.Action }}",
                "uniqueid": "{{ dag_run.conf.Uniqueid }}",
                "md5": "{{ dag_run.conf.md5 }}",
                "status": "Skipped",
                "details": "Timesheet is not available for user."
            }
        )

        get_timesheet_statusuri_ends_with_open = rail.IfOperator(
            task_id='get_timesheet_statusuri_ends_with_open',
            test='''{{result('get_timesheet_details_by_date').timesheet.statusUri | ends_with('open') }}''',
            yes_task="empty3",
            no_task="empty4",
        )
        empty3 = rail.EmptyOperator(
            task_id='empty3',
        )
        empty4 = rail.EmptyOperator(
            task_id='empty4',
        )
        get_timesheet_statusuri_contains_rejected = rail.IfOperator(
            task_id='get_timesheet_statusuri_contains_rejected',
            test='''{{result('get_timesheet_details_by_date').timesheet.statusUri | ends_with('rejected') }}''',
            yes_task="send_rejected_email",
            no_task="empty5",
        )
        empty5 = rail.EmptyOperator(
            task_id='empty5',
        )
        empty6 = rail.EmptyOperator(
            task_id='empty6',
        )
        send_rejected_email = rail.EmailOperator(
            task_id='send_rejected_email',
            to="{{ dag_run.conf['User email ID'] }}",
            bcc=config.bcc_tenant_email,
            subject="{{ dag_run.conf['Company key'] }} | Timesheet reopened for period {{ dag_run.conf.Startdate }}",
            html_content="templates/emails/email_for_rejected_timesheet_format.html"
        )

        get_timesheet_statusuri_contains_approved_waiting = rail.IfOperator(
            task_id='get_timesheet_statusuri_contains_approved_waiting',
            test="{{ result('get_timesheet_details_by_date').timesheet.statusUri | ends_with('approved') or result('get_timesheet_details_by_date').timesheet.statusUri | ends_with('waiting') }}",
            yes_task="reopen_timesheet",
            no_task="empty6",
        )

        reopen_timesheet = rail.RepliconServiceOperator(
            task_id='reopen_timesheet',
            endpoint="/services/TimesheetApprovalService1.svc/Reopen",
            data={
                "timesheetUri": "{{ result('get_timesheet_details_by_date').timesheet.uri }}",
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Timesheet is reopened by Integration"
            }
        )

        send_reopen_email = rail.EmailOperator(
            task_id='send_reopen_email',
            to="{{ dag_run.conf['User email ID'] }}",
            bcc=config.bcc_tenant_email,
            subject="{{ dag_run.conf['Company key'] }} | Timesheet reopened for period {{ dag_run.conf.Startdate }}",
            html_content="templates/emails/email_for_reopened_timesheet_format.html"
        )

        get_time_off_type_assignments_for_user = rail.RepliconServiceOperator(
            task_id='get_time_off_type_assignments_for_user',
            endpoint="/services/TimeOffService1.svc/GetTimeOffTypeAssignmentsForUser",
            data={"userUri": "{{ dag_run.conf.Useruri }}", }
        )

        request_action_equals_to_add_update = rail.IfOperator(
            task_id='request_action_equals_to_add_update',
            test='''{{dag_run.conf.Action == 'ADD' or dag_run.conf.Action == 'UPDATE'}}''',
            yes_task="request_timeoffuri_present",
            no_task="request_action_equals_to_delete",
        )

        request_timeoffuri_present = rail.IfOperator(
            task_id='request_timeoffuri_present',
            test=formatted_data.check_for_timesheet_uri,
            yes_task="get_time_off_details_for_user_and_date_range",
            no_task="timeoff_uri_notpresent_logs_entry",
        )
        timeoff_uri_notpresent_logs_entry = rail.WriteLogOperator(
            task_id='timeoff_uri_notpresent_logs_entry',
            log="{{ result('create_log') }}",
            severity="skipped",
            message="time off uri not present",
            properties=lambda dag_run: {
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.Employeeid }}",
                "timeofftype": "{{ dag_run.conf.Timeofftype }}",
                "startdate": "{{ dag_run.conf.Startdate }}",
                "enddate": "{{ dag_run.conf.Enddate }}",
                "timeoffhrs": "{{ dag_run.conf.Timeoffhrs }}",
                "action": "{{ dag_run.conf.Action }}",
                "uniqueid": "{{ dag_run.conf.Uniqueid }}",
                "md5": "{{ dag_run.conf.md5 }}",
                "status": "Skipped",
                "details": "{{ dag_run.conf.Timeofftype }}" + " is not allowed for booking."
            }
        )
        get_time_off_details_for_user_and_date_range = rail.RepliconServiceOperator(
            task_id='get_time_off_details_for_user_and_date_range',
            endpoint="/services/TimeOffService1.svc/GetTimeOffDetailsForUserAndDateRange2",
            data={
                "userUri": "{{ dag_run.conf.Useruri }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{dag_run.conf.Bookingdate.year}}",
                        "month": "{{dag_run.conf.Bookingdate.month}}",
                        "day": "{{dag_run.conf.Bookingdate.day}}"
                    },
                    "endDate": {
                        "year": "{{dag_run.conf.Bookingdate.year}}",
                        "month": "{{dag_run.conf.Bookingdate.month}}",
                        "day": "{{dag_run.conf.Bookingdate.day}}"
                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        timeofftype_name_equals_to_requested_timeoffname = rail.IfOperator(
            task_id='timeofftype_name_equals_to_requested_timeoffname',
            test='''{{result('get_time_off_details_for_user_and_date_range') | length > 0 and result('get_time_off_details_for_user_and_date_range')[0].timeOffType.name == dag_run.conf.Timeoffname and result('get_time_off_details_for_user_and_date_range')[0].customFields | length > 0 and result('get_time_off_details_for_user_and_date_range')[0].customFields[0].text == dag_run.conf.Uniqueid and result('get_time_off_details_for_user_and_date_range')[0].totalDuration.hours == dag_run.conf.Timeoffhrs}}''',
            yes_task="timeoff_type_notpresent_logs_entry",
            no_task="timeoff_uri_present_for_add_update",
        )
        timeoff_type_notpresent_logs_entry = rail.WriteLogOperator(
            task_id='timeoff_type_notpresent_logs_entry',
            log="{{ result('create_log') }}",
            severity="skipped",
            message="time off type not present",
            properties=lambda dag_run: {
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.Employeeid }}",
                "timeofftype": "{{ dag_run.conf.Timeofftype }}",
                "startdate": "{{ dag_run.conf.Startdate }}",
                "enddate": "{{ dag_run.conf.Enddate }}",
                "timeoffhrs": "{{ dag_run.conf.Timeoffhrs }}",
                "action": "{{ dag_run.conf.Action }}",
                "uniqueid": "{{ dag_run.conf.Uniqueid }}",
                "md5": "{{ dag_run.conf.md5 }}",
                "status": "Skipped",
                "details": "{{ dag_run.conf.Timeofftype }}" + " is already present for this day."
            }
        )
        timeoff_uri_present_for_add_update = rail.IfOperator(
            task_id='timeoff_uri_present_for_add_update',
            test='''{{result('get_time_off_details_for_user_and_date_range') | length > 0 and result('get_time_off_details_for_user_and_date_range')[0].uri | is_truthy}}''',
            yes_task="get_timeoff_hours_list",
            no_task="is_request_status_update",
        )

        get_timeoff_hours_list = rail.PythonOperator(
            task_id='get_timeoff_hours_list',
            python_callable=formatted_data.get_totalhours_list
        )

        check_status_update_timeoff = rail.IfOperator(
            task_id='check_status_update_timeoff',
            test='''{{dag_run.conf.Action == 'UPDATE'}}''',
            yes_task="get_time_off_details_on_unique_id",
            no_task="timeoff_hours_sum_equals_or_greater_then_8",
        )

        get_time_off_details_on_unique_id = rail.RepliconServiceOperator(
            task_id="get_time_off_details_on_unique_id",
            endpoint="/services/TimeOffListService1.svc/GetData",
            data=request_payload.get_time_off_details_on_booking_id,
            data_handler=formatted_data.get_filtered_time_off_details_on_booking_id
        )

        calculate_delta_hours_for_update = rail.PythonOperator(
            task_id='calculate_delta_hours_for_update',
            python_callable=formatted_data.calculate_delta_hours_for_update
        )

        if_no_error_present_in_calculation = rail.IfOperator(
            task_id='if_no_error_present_in_calculation',
            test='''{{result('calculate_delta_hours_for_update').status == 'True'}}''',
            yes_task="timeoff_hours_sum_equals_or_greater_then_8",
            no_task="log_update_timeoff_calculation_error",
        )

        log_update_timeoff_calculation_error = rail.WriteLogOperator(
            task_id='log_update_timeoff_calculation_error',
            log="{{ result('create_log') }}",
            severity="exception",
            message="{{ result('calculate_delta_hours_for_update').message }}",
            properties=lambda dag_run: {
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.Employeeid }}",
                "timeofftype": "{{ dag_run.conf.Timeofftype }}",
                "startdate": "{{ dag_run.conf.Startdate }}",
                "enddate": "{{ dag_run.conf.Enddate }}",
                "timeoffhrs": "{{ dag_run.conf.Timeoffhrs }}",
                "action": "{{ dag_run.conf.Action }}",
                "uniqueid": "{{ dag_run.conf.Uniqueid }}",
                "md5": "{{ dag_run.conf.md5 }}",
                "status": "success",
                "details": rail.result('calculate_delta_hours_for_update').get('message')
            }
        )

        timeoff_hours_sum_equals_or_greater_then_8 = rail.IfOperator(
            task_id='timeoff_hours_sum_equals_or_greater_then_8',
            test=formatted_data.get_sum_of_total_timeoff_hours,
            yes_task="delete_existing_time_booking",
            no_task="CreateDraft_timeoffbooking_for_user",
        )

        delete_existing_time_booking = rail.RepliconServiceOperator(
            task_id='delete_existing_time_booking',
            endpoint="/services/TimeOffService1.svc/DeleteTimeOff",
            data={
                "timeOffUri": "{{ result('get_time_off_details_for_user_and_date_range')[0].uri }}"
            }
        )

        is_request_status_update = rail.IfOperator(
            task_id='is_request_status_update',
            test='''{{dag_run.conf.Action == 'UPDATE'}}''',
            yes_task="timeoff_booking_notpresent_logs_entry",
            no_task="CreateDraft_timeoffbooking_for_user",
        )

        CreateDraft_timeoffbooking_for_user = rail.RepliconServiceOperator(
            task_id='CreateDraft_timeoffbooking_for_user',
            endpoint="/services/TimeOffService1.svc/CreateNewTimeOffDraft",
            data={
                "ownerUri": "{{ dag_run.conf.Useruri }}"
            }
        )

        Put_TimeOff_for_user = rail.RepliconServiceOperator(
            task_id='Put_TimeOff_for_user',
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=request_payload.put_timeoff_for_user_payload
        )

        Publish_TimeOffDraft_for_user = rail.RepliconServiceOperator(
            task_id='Publish_TimeOffDraft_for_user',
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data={
                "timeOff": "{{ result('CreateDraft_timeoffbooking_for_user') }}"
            }
        )
        Put_TimeOff_ExtensionFieldValues_for_user = rail.RepliconServiceOperator(
            task_id='Put_TimeOff_ExtensionFieldValues_for_user',
            endpoint="/services/TimeOffService1.svc/PutTimeOffExtensionFieldValues",
            data={
                    "timeOffUri": "{{ result('Publish_TimeOffDraft_for_user').uri }}",
                    "extensionFieldValues": [
                        {
                            "definition": {
                                "uri": "{{ dag_run.conf['Unique ID OEF URI'] }}",
                                "name": null
                            },
                            "tag": null,
                            "numericValue": null,
                            "textValue": "{{ dag_run.conf.Uniqueid }}",
                            "fileValue": null,
                            "jsonValue": null
                        }
                    ]
            }
        )
        Approve_timeoffbooking_for_user = rail.RepliconServiceOperator(
            task_id='Approve_timeoffbooking_for_user',
            endpoint="/services/TimeOffApprovalService1.svc/ForceApprove",
            data={
                "timeOffUri": "{{ result('Publish_TimeOffDraft_for_user').uri }}",
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Approved by Replicon Integration"
            }
        )
        timeoff_booking_successful_logs_entry = rail.WriteLogOperator(
            task_id='timeoff_booking_successful_logs_entry',
            log="{{ result('create_log') }}",
            severity="success",
            message="time off booking completed",
            properties=lambda dag_run: {
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.Employeeid }}",
                "timeofftype": "{{ dag_run.conf.Timeofftype }}",
                "startdate": "{{ dag_run.conf.Startdate }}",
                "enddate": "{{ dag_run.conf.Enddate }}",
                "timeoffhrs": "{{ dag_run.conf.Timeoffhrs }}",
                "action": "{{ dag_run.conf.Action }}",
                "uniqueid": "{{ dag_run.conf.Uniqueid }}",
                "md5": "{{ dag_run.conf.md5 }}",
                "status": "success",
                "details": rail.result('calculate_delta_hours_for_update').get('message', 'The time off is updated successfully.') if dag_run.conf['Action'] == 'UPDATE' else "The time off is added successfully."
            }
        )
        request_action_equals_to_delete = rail.IfOperator(
            task_id='request_action_equals_to_delete',
            test='''{{dag_run.conf.Action == 'DELETE'}}''',
            yes_task="timeoff_uri_present_for_delete",
            no_task="unknown_request_action_logs_entry",
        )
        unknown_request_action_logs_entry = rail.WriteLogOperator(
            task_id='unknown_request_action_logs_entry',
            log="{{ result('create_log') }}",
            severity="skipped",
            message="unknown request action",
            properties=lambda dag_run: {
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.Employeeid }}",
                "timeofftype": "{{ dag_run.conf.Timeofftype }}",
                "startdate": "{{ dag_run.conf.Startdate }}",
                "enddate": "{{ dag_run.conf.Enddate }}",
                "timeoffhrs": "{{ dag_run.conf.Timeoffhrs }}",
                "action": "{{ dag_run.conf.Action }}",
                "uniqueid": "{{ dag_run.conf.Uniqueid }}",
                "md5": "{{ dag_run.conf.md5 }}",
                "status": "Skipped",
                "details": "this request action is unknown which is not allowed."
            }
        )
        timeoff_uri_present_for_delete = rail.IfOperator(
            task_id='timeoff_uri_present_for_delete',
            test=formatted_data.check_for_timesheet_uri,
            yes_task="get_time_off_details2_for_user_and_date_range",
            no_task="timeoff_uri_notpresent_todelete_logs_entry",
        )
        timeoff_uri_notpresent_todelete_logs_entry = rail.WriteLogOperator(
            task_id='timeoff_uri_notpresent_todelete_logs_entry',
            log="{{ result('create_log') }}",
            severity="skipped",
            message="unknown request action",
            properties=lambda dag_run: {
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.Employeeid }}",
                "timeofftype": "{{ dag_run.conf.Timeofftype }}",
                "startdate": "{{ dag_run.conf.Startdate }}",
                "enddate": "{{ dag_run.conf.Enddate }}",
                "timeoffhrs": "{{ dag_run.conf.Timeoffhrs }}",
                "action": "{{ dag_run.conf.Action }}",
                "uniqueid": "{{ dag_run.conf.Uniqueid }}",
                "md5": "{{ dag_run.conf.md5 }}",
                "status": "Skipped",
                "details": "{{ dag_run.conf.Timeofftype }}" + "is not allowed for booking."
            }
        )
        get_time_off_details2_for_user_and_date_range = rail.RepliconServiceOperator(
            task_id='get_time_off_details2_for_user_and_date_range',
            endpoint="/services/TimeOffService1.svc/GetTimeOffDetailsForUserAndDateRange2",
            data={
                "userUri": "{{ dag_run.conf.Useruri }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{dag_run.conf.Bookingdate.year}}",
                        "month": "{{dag_run.conf.Bookingdate.month}}",
                        "day": "{{dag_run.conf.Bookingdate.day}}"
                    },
                    "endDate": {
                        "year": "{{dag_run.conf.Bookingdate.year}}",
                        "month": "{{dag_run.conf.Bookingdate.month}}",
                        "day": "{{dag_run.conf.Bookingdate.day}}"
                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        timeofftype_name_equals_to_requested_timeofftype_name = rail.IfOperator(
            task_id='timeofftype_name_equals_to_requested_timeofftype_name',
            test='''{{result('get_time_off_details2_for_user_and_date_range') | length > 0 and result('get_time_off_details2_for_user_and_date_range')[0].timeOffType.name == dag_run.conf.Timeofftype and result('get_time_off_details2_for_user_and_date_range')[0].extensionFieldValues | length > 0 and result('get_time_off_details2_for_user_and_date_range')[0].extensionFieldValues[0].textValue == dag_run.conf.Uniqueid}}''',
            yes_task="delete_timeofftype",
            no_task="timeoff_booking_notpresent_logs_entry",
        )
        timeoff_booking_notpresent_logs_entry = rail.WriteLogOperator(
            task_id='timeoff_booking_notpresent_logs_entry',
            log="{{ result('create_log') }}",
            severity="success",
            message="time off booking not present",
            properties=lambda dag_run: {
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.Employeeid }}",
                "timeofftype": "{{ dag_run.conf.Timeofftype }}",
                "startdate": "{{ dag_run.conf.Startdate }}",
                "enddate": "{{ dag_run.conf.Enddate }}",
                "timeoffhrs": "{{ dag_run.conf.Timeoffhrs }}",
                "action": "{{ dag_run.conf.Action }}",
                "uniqueid": "{{ dag_run.conf.Uniqueid }}",
                "md5": "{{ dag_run.conf.md5 }}",
                "status": "Skipped",
                "details": "Time Off booking is not present."
            }
        )
        delete_timeofftype = rail.RepliconServiceOperator(
            task_id='delete_timeofftype',
            endpoint="/services/TimeOffService1.svc/DeleteTimeOff",
            data={
                "timeOffUri": "{{ result('get_time_off_details2_for_user_and_date_range')[0].uri }}"
            }
        )
        delete_timeoff_booking_successful_logs_entry = rail.WriteLogOperator(
            task_id='delete_timeoff_booking_successful_logs_entry',
            log="{{ result('create_log') }}",
            severity="success",
            message="time off deletion completed",
            properties=lambda dag_run: {
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.Employeeid }}",
                "timeofftype": "{{ dag_run.conf.Timeofftype }}",
                "startdate": "{{ dag_run.conf.Startdate }}",
                "enddate": "{{ dag_run.conf.Enddate }}",
                "timeoffhrs": "{{ dag_run.conf.Timeoffhrs }}",
                "action": "{{ dag_run.conf.Action }}",
                "uniqueid": "{{ dag_run.conf.Uniqueid }}",
                "md5": "{{ dag_run.conf.md5 }}",
                "status": "success",
                "details": "The time off is deleted successfully."
            }
        )
        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log="{{ result('create_log') }}",
            severity="Failed",
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.Employeeid }}",
                "timeofftype": "{{ dag_run.conf.Timeofftype }}",
                "startdate": "{{ dag_run.conf.Startdate }}",
                "enddate": "{{ dag_run.conf.Enddate }}",
                "timeoffhrs": "{{ dag_run.conf.Timeoffhrs }}",
                "action": "{{ dag_run.conf.Action }}",
                "uniqueid": "{{ dag_run.conf.Uniqueid }}",
                "md5": "{{ dag_run.conf.md5 }}",
                "status": "Failed",
                "details": "The time off booking is failed due to an error."
            }
        )
        finish = rail.EmptyOperator(
            task_id='finish',
        )

        create_log >> check_for_wday >> rail.Label(
            'Yes') >> check_for_required_parameter
        create_log >> check_for_wday >> rail.Label(
            'No') >> check_for_wday_logs_entry >> catch_and_log_errors >> finish
        check_for_required_parameter >> all_required_parameter
        all_required_parameter >> rail.Label(
            'Yes') >> check_start_date_in_range
        all_required_parameter >> rail.Label(
            'No') >> all_required_parameter_logs_entry >> catch_and_log_errors >> finish
        check_start_date_in_range >> rail.Label(
            'No') >> empty1 >> get_time_off_type_assignments_for_user
        check_start_date_in_range >> rail.Label(
            'Yes') >> get_timesheet_details_by_date >> get_timesheet_uri_available
        get_timesheet_uri_available >> rail.Label(
            'Yes') >> empty2 >> get_timesheet_statusuri_ends_with_open
        get_timesheet_uri_available >> rail.Label(
            'No') >> timesheet_uri_notpresent_logs_entry >> catch_and_log_errors >> finish
        get_timesheet_statusuri_ends_with_open >> rail.Label(
            'Yes') >> empty3 >> get_time_off_type_assignments_for_user
        get_timesheet_statusuri_ends_with_open >> rail.Label(
            'No') >> empty4 >> get_timesheet_statusuri_contains_rejected
        get_timesheet_statusuri_contains_rejected >> rail.Label(
            'Yes') >> send_rejected_email >> get_time_off_type_assignments_for_user
        get_timesheet_statusuri_contains_rejected >> rail.Label(
            'No') >> empty5 >> get_timesheet_statusuri_contains_approved_waiting
        get_timesheet_statusuri_contains_approved_waiting >> rail.Label(
            'No') >> empty6 >> get_time_off_type_assignments_for_user
        get_timesheet_statusuri_contains_approved_waiting >> rail.Label(
            'yes') >> reopen_timesheet >> send_reopen_email >> get_time_off_type_assignments_for_user >> request_action_equals_to_add_update
        request_action_equals_to_add_update >> rail.Label(
            'yes') >> request_timeoffuri_present
        request_action_equals_to_add_update >> rail.Label(
            'No') >> request_action_equals_to_delete
        request_timeoffuri_present >> rail.Label(
            'Yes') >> get_time_off_details_for_user_and_date_range >> timeofftype_name_equals_to_requested_timeoffname
        request_timeoffuri_present >> rail.Label(
            'No') >> timeoff_uri_notpresent_logs_entry >> catch_and_log_errors >> finish

        timeofftype_name_equals_to_requested_timeoffname >> rail.Label(
            'Yes') >> timeoff_type_notpresent_logs_entry >> catch_and_log_errors >> finish
        timeofftype_name_equals_to_requested_timeoffname >> rail.Label(
            'No') >> timeoff_uri_present_for_add_update
        timeoff_uri_present_for_add_update >> rail.Label(
            'Yes') >> get_timeoff_hours_list

        get_timeoff_hours_list >> check_status_update_timeoff

        check_status_update_timeoff >> rail.Label('Yes') >> get_time_off_details_on_unique_id >> calculate_delta_hours_for_update >> \
            if_no_error_present_in_calculation
            
        if_no_error_present_in_calculation >> rail.Label('Yes') >> timeoff_hours_sum_equals_or_greater_then_8
        if_no_error_present_in_calculation >> rail.Label('No') >> log_update_timeoff_calculation_error >> catch_and_log_errors >> finish

        check_status_update_timeoff >> rail.Label('No') >> timeoff_hours_sum_equals_or_greater_then_8


        timeoff_uri_present_for_add_update >> rail.Label(
            'No') >> is_request_status_update

        is_request_status_update >> rail.Label('Yes') >> timeoff_booking_notpresent_logs_entry >> catch_and_log_errors >> finish
        is_request_status_update >> rail.Label('No') >> CreateDraft_timeoffbooking_for_user

        timeoff_hours_sum_equals_or_greater_then_8 >> rail.Label(
            'Yes') >> delete_existing_time_booking >> CreateDraft_timeoffbooking_for_user
        timeoff_hours_sum_equals_or_greater_then_8 >> rail.Label(
            'No') >> CreateDraft_timeoffbooking_for_user

        CreateDraft_timeoffbooking_for_user >> Put_TimeOff_for_user >> Publish_TimeOffDraft_for_user >> Put_TimeOff_ExtensionFieldValues_for_user >> Approve_timeoffbooking_for_user

        Approve_timeoffbooking_for_user >> timeoff_booking_successful_logs_entry >> catch_and_log_errors >> finish
        request_action_equals_to_delete >> rail.Label(
            'Yes') >> timeoff_uri_present_for_delete
        request_action_equals_to_delete >> rail.Label(
            'No') >> unknown_request_action_logs_entry >> catch_and_log_errors >> finish
        timeoff_uri_present_for_delete >> rail.Label(
            'No') >> timeoff_uri_notpresent_todelete_logs_entry >> catch_and_log_errors >> finish
        timeoff_uri_present_for_delete >> rail.Label(
            'Yes') >> get_time_off_details2_for_user_and_date_range >> timeofftype_name_equals_to_requested_timeofftype_name
        timeofftype_name_equals_to_requested_timeofftype_name >> rail.Label(
            'Yes') >> delete_timeofftype >> delete_timeoff_booking_successful_logs_entry >> catch_and_log_errors >> finish
        timeofftype_name_equals_to_requested_timeofftype_name >> rail.Label(
            'No') >> timeoff_booking_notpresent_logs_entry >> catch_and_log_errors >> finish

    return dag


rail.for_each_instance(create_child_dag)
