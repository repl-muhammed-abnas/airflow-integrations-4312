import rail
from datetime import timedelta
from airflow.models import Variable
from transparentbpo.timeoff_import.utils import custom_methods, request_payload


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_timeoff_bookings_child_dag_id,
        description="TransparentBPO Timeoff import Process timeoff bookings",
        company_key=config.company_key,
        max_active_runs=config.max_active_child_runs,
        replicon_conn_id=config.replicon_conn_id,
    ) as dag:
        
        rail.ViewDagRunConfOperator(
            task_id='view_dag_run_config'
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='booking_hours_equal_zero'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='booking_hours_equal_zero',
            end_task='catch_and_log_errors',
        )

        booking_hours_equal_zero = rail.IfOperator(
            task_id="booking_hours_equal_zero",
            test=lambda dag_run: float(dag_run.conf['booking_hour']) == 0,
            yes_task='log_booking_hours_equal_0',
            no_task='timeoff_is_public_and_bank_holiday'
        )

        log_booking_hours_equal_0 = rail.WriteLogOperator(
            task_id='log_booking_hours_equal_0',
            log="{{ dag_run.conf.log }}",
            severity="Exception",
            message="Zero hour booking",
            properties={
                'timeoff_id': "{{ dag_run.conf.timeoff_id }}",
                'bamboohr_id': "{{ dag_run.conf.bamboohr_id }}",
                'employee_id': "{{ dag_run.conf.employee_id }}",
                'username': "{{ dag_run.conf.name }}",
                'timeoff_type': "{{ dag_run.conf.type_name }}",
                'booking_date': "{{ dag_run.conf.booking_date }}",
                'status': "Exception",
                'details': "Zero hour booking"
            }
        )

        no_data = rail.EmptyOperator(
            task_id='no_data'
        )

        timeoff_is_public_and_bank_holiday = rail.IfOperator(
            task_id="timeoff_is_public_and_bank_holiday",
            test=lambda dag_run: (
                dag_run.conf['type_name']) == 'Public & Bank Holidays',
            yes_task='timeoff_has_scheduled_hrs',
            no_task='timeoff_is_vacation'
        )

        timeoff_has_scheduled_hrs = rail.IfOperator(
            task_id="timeoff_has_scheduled_hrs",
            test=lambda dag_run: (dag_run.conf['scheduled_hrs']),
            yes_task='get_timesheet_for_date2',
            no_task='log_scheduled_hrs_not_present'
        )

        log_scheduled_hrs_not_present = rail.WriteLogOperator(
            task_id='log_scheduled_hrs_not_present',
            log="{{ dag_run.conf.log }}",
            severity="Exception",
            message='No schedule assigned to user for "{{ dag_run.conf.booking_date }}" in Replicon',
            properties={
                'timeoff_id': "{{ dag_run.conf.timeoff_id }}",
                'bamboohr_id': "{{ dag_run.conf.bamboohr_id }}",
                'employee_id': "{{ dag_run.conf.employee_id }}",
                'username': "{{ dag_run.conf.name }}",
                'timeoff_type': "{{ dag_run.conf.type_name }}",
                'booking_date': "{{ dag_run.conf.booking_date }}",
                'status': "Exception",
                'details': 'No schedule assigned to user for "{{ dag_run.conf.booking_date }}" in Replicon'
            }
        )

        get_timesheet_for_date2 = rail.RepliconServiceOperator(
            task_id="get_timesheet_for_date2",
            endpoint="/services/TimesheetService1.svc/GetTimesheetForDate2",
            data=lambda dag_run: request_payload.get_timesheet_for_date2_payload(dag_run)
        )

        timesheet_uri_is_present = rail.IfOperator(
            task_id="timesheet_uri_is_present",
            test=lambda: custom_methods.get_timesheet_uri(rail.result("get_timesheet_for_date2")),
            yes_task='get_timesheet_details',
            no_task='get_timeoff_details_for_user_and_date_range'
        )

        get_timesheet_details = rail.RepliconServiceOperator(
            task_id="get_timesheet_details",
            endpoint="/services/TimesheetService1.svc/GetTimesheetDetails",
            data=lambda: request_payload.get_timesheet_details_payload("get_timesheet_for_date2")
        )

        timesheet_is_approved_or_waiting = rail.IfOperator(
            task_id="timesheet_is_approved_or_waiting",
            test=lambda: rail.result("get_timesheet_details")['statusUri'].endswith(
                "approved") or rail.result("get_timesheet_details")['statusUri'].endswith("waiting"),
            yes_task='reopen_timesheet',
            no_task='get_timeoff_details_for_user_and_date_range'
        )

        reopen_timesheet = rail.RepliconServiceOperator(
            task_id="reopen_timesheet",
            endpoint="/services/TimesheetApprovalService1.svc/Reopen",
            data=lambda: request_payload.reopen_timesheet_payload("get_timesheet_for_date2")
        )

        get_timeoff_details_for_user_and_date_range = rail.RepliconServiceOperator(
            task_id="get_timeoff_details_for_user_and_date_range",
            endpoint="/services/TimeOffService1.svc/GetTimeOffDetailsForUserAndDateRange2",
            data=lambda dag_run: request_payload.get_timeoff_details_for_user_and_date_range_payload(dag_run)
        )

        if_uri_present = rail.IfOperator(
            task_id='if_uri_present',
            test=lambda: bool(rail.result('get_timeoff_details_for_user_and_date_range') and rail.result(
                'get_timeoff_details_for_user_and_date_range')[0]['uri']),
            yes_task="delete_time_off_entry",
            no_task="create_new_time_off_draft",
        )

        delete_time_off_entry = rail.RepliconServiceOperator(
            task_id='delete_time_off_entry',
            endpoint="/services/TimeOffService1.svc/DeleteTimeOff",
            data={
                "timeOffUri": "{{ result('get_timeoff_details_for_user_and_date_range')[0].uri }}"
            }
        )

        create_new_time_off_draft = rail.RepliconServiceOperator(
            task_id='create_new_time_off_draft',
            endpoint="/services/TimeOffService1.svc/CreateNewTimeOffDraft",
            data={
                "ownerUri": "{{ dag_run.conf.user_uri }}"
            }
        )

        put_time_off2 = rail.RepliconServiceOperator(
            task_id='put_time_off2',
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: request_payload.put_time_off2_payload(
                dag_run, "create_new_time_off_draft", include_specific_duration=False
            )
        )

        publish_time_off_draft = rail.RepliconServiceOperator(
            task_id='publish_time_off_draft',
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data={
                "timeOff": "{{ result('create_new_time_off_draft')}}"
            }
        )

        force_approve = rail.RepliconServiceOperator(
            task_id='force_approve',
            endpoint="/services/TimeOffApprovalService1.svc/ForceApprove",
            data=lambda: request_payload.force_approve_timeoff_payload("publish_time_off_draft")
        )

        check_timesheet_uri_is_present = rail.IfOperator(
            task_id="check_timesheet_uri_is_present",
            test=lambda: custom_methods.get_timesheet_uri(rail.result('get_timesheet_for_date2')),
            yes_task='timesheet_is_in_waiting_status',
            no_task='log_success_for_public_and_bank_holiday'
        )

        timesheet_is_in_waiting_status = rail.IfOperator(
            task_id="timesheet_is_in_waiting_status",
            test=lambda: rail.result('get_timesheet_details')[
                'statusUri'].endswith("waiting"),
            yes_task='submit_timesheet',
            no_task='timesheet_is_in_approved_status'
        )

        submit_timesheet = rail.RepliconServiceOperator(
            task_id='submit_timesheet',
            endpoint="/services/TimesheetApprovalService1.svc/Submit2",
            data=lambda: request_payload.submit_timesheet_payload("get_timesheet_details")
        )

        timesheet_is_in_approved_status = rail.IfOperator(
            task_id="timesheet_is_in_approved_status",
            test=lambda: rail.result('get_timesheet_details')[
                'statusUri'].endswith("approved"),
            yes_task='force_approve_timesheet',
            no_task='log_success_for_public_and_bank_holiday'
        )

        force_approve_timesheet = rail.RepliconServiceOperator(
            task_id='force_approve_timesheet',
            endpoint="/services/TimesheetApprovalService1.svc/ForceApprove",
            data=lambda: request_payload.force_approve_timesheet_payload("get_timesheet_details")
        )

        log_success_for_public_and_bank_holiday = rail.WriteLogOperator(
            task_id='log_success_for_public_and_bank_holiday',
            log="{{ dag_run.conf.log }}",
            severity="Success",
            message="TimeOff Booked Successfully",
            properties={
                'timeoff_id': "{{ dag_run.conf.timeoff_id }}",
                'bamboohr_id': "{{ dag_run.conf.bamboohr_id }}",
                'employee_id': "{{ dag_run.conf.employee_id }}",
                'username': "{{ dag_run.conf.name }}",
                'timeoff_type': "{{ dag_run.conf.type_name }}",
                'booking_date': "{{ dag_run.conf.booking_date }}",
                'status': "Success",
                'details': "TimeOff Booked Successfully"
            }
        )

        timeoff_is_vacation = rail.IfOperator(
            task_id="timeoff_is_vacation",
            test=lambda dag_run: (dag_run.conf['type_name']) == 'Vacation',
            yes_task='get_timesheet_for_date2_vacation',
            no_task='no_data'
        )

        get_timesheet_for_date2_vacation = rail.RepliconServiceOperator(
            task_id="get_timesheet_for_date2_vacation",
            endpoint="/services/TimesheetService1.svc/GetTimesheetForDate2",
            data=lambda dag_run: request_payload.get_timesheet_for_date2_payload(dag_run)
        )

        timesheet_uri_is_present_vacation = rail.IfOperator(
            task_id="timesheet_uri_is_present_vacation",
            test=lambda: custom_methods.get_timesheet_uri(rail.result("get_timesheet_for_date2_vacation")),
            yes_task='get_timesheet_details_vacation',
            no_task='get_timeoff_details_for_user_and_date_range_vacation'
        )

        get_timesheet_details_vacation = rail.RepliconServiceOperator(
            task_id="get_timesheet_details_vacation",
            endpoint="/services/TimesheetService1.svc/GetTimesheetDetails",
            data=lambda: request_payload.get_timesheet_details_payload("get_timesheet_for_date2_vacation")
        )

        timesheet_is_approved_or_waiting_vacation = rail.IfOperator(
            task_id="timesheet_is_approved_or_waiting_vacation",
            test=lambda: rail.result("get_timesheet_details_vacation")['statusUri'].endswith(
                "approved") or rail.result("get_timesheet_details_vacation")['statusUri'].endswith("waiting"),
            yes_task='reopen_timesheet_vacation',
            no_task='get_timeoff_details_for_user_and_date_range_vacation'
        )

        reopen_timesheet_vacation = rail.RepliconServiceOperator(
            task_id="reopen_timesheet_vacation",
            endpoint="/services/TimesheetApprovalService1.svc/Reopen",
            data=lambda: request_payload.reopen_timesheet_payload("get_timesheet_for_date2_vacation")
        )

        get_timeoff_details_for_user_and_date_range_vacation = rail.RepliconServiceOperator(
            task_id="get_timeoff_details_for_user_and_date_range_vacation",
            endpoint="/services/TimeOffService1.svc/GetTimeOffDetailsForUserAndDateRange2",
            data=lambda dag_run: request_payload.get_timeoff_details_for_user_and_date_range_payload(dag_run)
        )

        if_uri_present_vacation = rail.IfOperator(
            task_id='if_uri_present_vacation',
            test=lambda: bool(rail.result('get_timeoff_details_for_user_and_date_range_vacation') and rail.result(
                'get_timeoff_details_for_user_and_date_range_vacation')[0]['uri']),
            yes_task="delete_time_off_entry_vacation",
            no_task="create_new_time_off_draft_vacation",
        )

        delete_time_off_entry_vacation = rail.RepliconServiceOperator(
            task_id='delete_time_off_entry_vacation',
            endpoint="/services/TimeOffService1.svc/DeleteTimeOff",
            data={
                "timeOffUri": "{{ result('get_timeoff_details_for_user_and_date_range_vacation')[0].uri }}"
            }
        )

        create_new_time_off_draft_vacation = rail.RepliconServiceOperator(
            task_id='create_new_time_off_draft_vacation',
            endpoint="/services/TimeOffService1.svc/CreateNewTimeOffDraft",
            data={
                "ownerUri": "{{ dag_run.conf.user_uri }}"
            }
        )

        put_time_off2_vacation = rail.RepliconServiceOperator(
            task_id='put_time_off2_vacation',
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: request_payload.put_time_off2_payload(
                dag_run, "create_new_time_off_draft_vacation", include_specific_duration=True
            )
        )

        publish_time_off_draft_vacation = rail.RepliconServiceOperator(
            task_id='publish_time_off_draft_vacation',
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data={
                "timeOff": "{{ result('create_new_time_off_draft_vacation')}}"
            }
        )

        force_approve_vacation = rail.RepliconServiceOperator(
            task_id='force_approve_vacation',
            endpoint="/services/TimeOffApprovalService1.svc/ForceApprove",
            data=lambda: request_payload.force_approve_timeoff_payload("publish_time_off_draft_vacation")
        )

        check_timesheet_uri_is_present_vacation = rail.IfOperator(
            task_id="check_timesheet_uri_is_present_vacation",
            test=lambda: custom_methods.get_timesheet_uri(rail.result('get_timesheet_for_date2_vacation')),
            yes_task='timesheet_is_in_waiting_status_vacation',
            no_task='log_success_for_vacation'
        )

        timesheet_is_in_waiting_status_vacation = rail.IfOperator(
            task_id="timesheet_is_in_waiting_status_vacation",
            test=lambda: rail.result('get_timesheet_details_vacation')[
                'statusUri'].endswith("waiting"),
            yes_task='submit_timesheet_vacation',
            no_task='timesheet_is_in_approved_status_vacation'
        )

        submit_timesheet_vacation = rail.RepliconServiceOperator(
            task_id='submit_timesheet_vacation',
            endpoint="/services/TimesheetApprovalService1.svc/Submit2",
            data=lambda: request_payload.submit_timesheet_payload("get_timesheet_details_vacation")
        )

        timesheet_is_in_approved_status_vacation = rail.IfOperator(
            task_id="timesheet_is_in_approved_status_vacation",
            test=lambda: rail.result('get_timesheet_details_vacation')[
                'statusUri'].endswith("approved"),
            yes_task='force_approve_timesheet_vacation',
            no_task='log_success_for_vacation' 
        )

        force_approve_timesheet_vacation = rail.RepliconServiceOperator(
            task_id='force_approve_timesheet_vacation',
            endpoint="/services/TimesheetApprovalService1.svc/ForceApprove",
            data=lambda: request_payload.force_approve_timesheet_payload("get_timesheet_details_vacation")
        )

        log_success_for_vacation = rail.WriteLogOperator(
            task_id='log_success_for_vacation',
            log="{{ dag_run.conf.log }}",
            severity="Success",
            message="TimeOff Booked Successfully",
            properties={
                'timeoff_id': "{{ dag_run.conf.timeoff_id }}",
                'bamboohr_id': "{{ dag_run.conf.bamboohr_id }}",
                'employee_id': "{{ dag_run.conf.employee_id }}",
                'username': "{{ dag_run.conf.name }}",
                'timeoff_type': "{{ dag_run.conf.type_name }}",
                'booking_date': "{{ dag_run.conf.booking_date }}",
                'status': "Success",
                'details': "TimeOff Booked Successfully"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log="{{ dag_run.conf.log }}",
            severity="Error",
            message="{{ get_error_message() }}",
            properties={
                'timeoff_id': "{{ dag_run.conf.timeoff_id }}",
                'bamboohr_id': "{{ dag_run.conf.bamboohr_id }}",
                'employee_id': "{{ dag_run.conf.employee_id }}",
                'username': "{{ dag_run.conf.name }}",
                'timeoff_type': "{{ dag_run.conf.type_name }}",
                'booking_date': "{{ dag_run.conf.booking_date }}",
                'status': "Error",
                'details': "{{ get_error_message() }}"
            }
        )

    can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
    can_run_batch_task >> rail.Label("No") >> booking_hours_equal_zero

    booking_hours_equal_zero >> rail.Label("Yes") >> log_booking_hours_equal_0 >> no_data
    booking_hours_equal_zero >> rail.Label("No") >> timeoff_is_public_and_bank_holiday

    timeoff_is_public_and_bank_holiday >> rail.Label("Yes") >> timeoff_has_scheduled_hrs
    timeoff_is_public_and_bank_holiday >> rail.Label("No") >> timeoff_is_vacation

    timeoff_has_scheduled_hrs >> rail.Label("Yes") >> get_timesheet_for_date2 >> timesheet_uri_is_present
    timeoff_has_scheduled_hrs >> rail.Label("No") >> log_scheduled_hrs_not_present >> no_data

    timesheet_uri_is_present >> rail.Label("Yes") >> get_timesheet_details >> timesheet_is_approved_or_waiting
    timesheet_uri_is_present >> rail.Label("No") >> get_timeoff_details_for_user_and_date_range

    timesheet_is_approved_or_waiting >>  rail.Label("Yes") >> reopen_timesheet >> get_timeoff_details_for_user_and_date_range
    timesheet_is_approved_or_waiting >>  rail.Label("No") >> get_timeoff_details_for_user_and_date_range

    get_timeoff_details_for_user_and_date_range >> if_uri_present

    if_uri_present >>  rail.Label("Yes") >> delete_time_off_entry >> create_new_time_off_draft
    if_uri_present >>  rail.Label("No") >> create_new_time_off_draft

    create_new_time_off_draft >> put_time_off2 >> publish_time_off_draft >> force_approve >> check_timesheet_uri_is_present

    check_timesheet_uri_is_present >>  rail.Label("Yes") >> timesheet_is_in_waiting_status
    check_timesheet_uri_is_present >>  rail.Label("No") >> log_success_for_public_and_bank_holiday

    timesheet_is_in_waiting_status >>  rail.Label("Yes") >>  submit_timesheet >> log_success_for_public_and_bank_holiday
    timesheet_is_in_waiting_status >>  rail.Label("No") >> timesheet_is_in_approved_status 

    timesheet_is_in_approved_status >>  rail.Label("Yes") >> force_approve_timesheet >> log_success_for_public_and_bank_holiday
    timesheet_is_in_approved_status >>  rail.Label("No") >> log_success_for_public_and_bank_holiday

    log_success_for_public_and_bank_holiday >> catch_and_log_errors

    timeoff_is_vacation >>  rail.Label("Yes") >> get_timesheet_for_date2_vacation >> timesheet_uri_is_present_vacation
    timeoff_is_vacation >>  rail.Label("No") >> no_data

    timesheet_uri_is_present_vacation >> rail.Label("Yes") >> get_timesheet_details_vacation >> timesheet_is_approved_or_waiting_vacation
    timesheet_uri_is_present_vacation >> rail.Label("No") >> get_timeoff_details_for_user_and_date_range_vacation

    timesheet_is_approved_or_waiting_vacation >> rail.Label("Yes") >> reopen_timesheet_vacation >> get_timeoff_details_for_user_and_date_range_vacation
    timesheet_is_approved_or_waiting_vacation >> rail.Label("No") >> get_timeoff_details_for_user_and_date_range_vacation

    get_timeoff_details_for_user_and_date_range_vacation >> if_uri_present_vacation

    if_uri_present_vacation >> rail.Label("Yes") >> delete_time_off_entry_vacation >> create_new_time_off_draft_vacation
    if_uri_present_vacation >> rail.Label("No") >> create_new_time_off_draft_vacation

    create_new_time_off_draft_vacation >> put_time_off2_vacation >> publish_time_off_draft_vacation >> force_approve_vacation >> check_timesheet_uri_is_present_vacation

    check_timesheet_uri_is_present_vacation >> rail.Label("Yes") >> timesheet_is_in_waiting_status_vacation
    check_timesheet_uri_is_present_vacation >> rail.Label("No") >> log_success_for_vacation

    timesheet_is_in_waiting_status_vacation >> rail.Label("Yes") >> submit_timesheet_vacation >> log_success_for_vacation
    timesheet_is_in_waiting_status_vacation >> rail.Label("No") >> timesheet_is_in_approved_status_vacation

    timesheet_is_in_approved_status_vacation >> rail.Label("Yes") >> force_approve_timesheet_vacation >> log_success_for_vacation
    timesheet_is_in_approved_status_vacation >> rail.Label("No") >> log_success_for_vacation 

    log_success_for_vacation >> catch_and_log_errors

    no_data >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
