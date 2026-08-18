import rail
from galaxyusopcoinc.timeoff_hours_booking_import_v1.utils import request_payload, custom_methods


def update_timeoff_booking():
    with rail.TaskGroup(group_id='update_timeoff_booking', prefix_group_id=False) as process_update_timeoff:

        check_if_date_is_less_than_start_date = rail.IfOperator(
            task_id='check_if_date_is_less_than_start_date_update',
            test=lambda dag_run: custom_methods.is_entry_date_less_than_startdate(dag_run.conf[
                'user_start_date'], dag_run.conf['timeoff_date']) if dag_run.conf['user_start_date'] else False,
            yes_task='log_start_date_exception_update',
            no_task='check_if_date_is_greater_than_end_date_update'
        )

        log_start_date_exception = rail.WriteLogOperator(
            task_id='log_start_date_exception_update',
            message='The received timeoff date is less than user start date in replicon',
            severity='Exception',
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "timeoff_type": dag_run.conf["plan_ref_id"],
                "entry_date": dag_run.conf["timeoff_date"],
                "hours": dag_run.conf["hours"],
                "wd_event_id": dag_run.conf["wd_event_id"],
                'action': 'Update',
                'status': 'Exception',
                "details": "The received timeoff date is before the user start date in replicon",
            }
        )

        check_if_date_is_greater_than_end_date = rail.IfOperator(
            task_id='check_if_date_is_greater_than_end_date_update',
            test=lambda dag_run: custom_methods.is_entry_date_less_than_startdate(dag_run.conf[
                'timeoff_date'], dag_run.conf['user_end_date']) if dag_run.conf['user_end_date'] else False,
            yes_task='is_received_hours_negative_update',
            no_task='check_total_hours_positive_update'
        )

        is_received_hours_negative = rail.IfOperator(
            task_id='is_received_hours_negative_update',
            test=lambda dag_run: (float(dag_run.conf['hours']) + float(
                rail.result("get_time_off_booking_details")['timeoff_hours'])) <= 0,
            yes_task='delete_timeoff_update',
            no_task='log_end_date_exception_update'
        )

        log_end_date_exception = rail.WriteLogOperator(
            task_id='log_end_date_exception_update',
            message='The received timeoff date is greater than user end date in replicon',
            severity='Exception',
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "timeoff_type": dag_run.conf["plan_ref_id"],
                "entry_date": dag_run.conf["timeoff_date"],
                "hours": dag_run.conf["hours"],
                "wd_event_id": dag_run.conf["wd_event_id"],
                'action': 'Update',
                'status': 'Exception',
                "details": "The received timeoff date is after the user end date in replicon",
            }
        )

        delete_timeoff = rail.RepliconServiceOperator(
            task_id="delete_timeoff_update",
            endpoint="/services/TimeOffService1.svc/DeleteTimeOff",
            data={
                "timeOffUri": '{{ result("get_time_off_booking_details").timeoff_uri }}'
            }
        )

        log_delete_timeoff = rail.WriteLogOperator(
            task_id='log_delete_timeoff_update',
            log="{{ dag_run.conf.log }}",
            message='Time off Booking deleted Successfully',
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "timeoff_type": dag_run.conf["plan_ref_id"],
                "entry_date": dag_run.conf["timeoff_date"],
                "hours": dag_run.conf["hours"],
                "wd_event_id": dag_run.conf["wd_event_id"],
                'action': 'Delete',
                'status': 'Success',
                'details': "Time off Booking is deleted Successfully",
            }
        )

        check_total_hours_positive = rail.IfOperator(
            task_id='check_total_hours_positive_update',
            test=lambda dag_run: (float(dag_run.conf['hours']) + float(
                rail.result("get_time_off_booking_details")['timeoff_hours'])) > 0,
            yes_task='reopen_and_put_timeoff_update',
            no_task='delete_timeoff_update'
        )

        reopen_and_put_timeoff = rail.RepliconServiceOperator(
            task_id="reopen_and_put_timeoff_update",
            endpoint="services/TimeOffApprovalService1.svc/ReopenPutAndSubmitTimeOff3",
            data=request_payload.get_reopen_and_put_timeoff_payload
        )

        get_time_off_approval_status = rail.RepliconServiceOperator(
            task_id="get_time_off_approval_status_update",
            endpoint="/services/TimeOffApprovalService1.svc/GetApprovalHistoryDetails",
            data={
                "timeOffUri": "{{ result('reopen_and_put_timeoff_update').uri  }}"
            },
            data_handler=lambda response: response['approvalStatus']['displayText']
        )

        is_timeoff_approved = rail.IfOperator(
            task_id="is_timeoff_approved_update",
            test=lambda: rail.result(
                'get_time_off_approval_status_update') == 'Approved',
            yes_task='log_booking_successful_update',
            no_task='force_approve_time_off_entry_update'
        )

        force_approve_time_off_entry = rail.RepliconServiceOperator(
            task_id="force_approve_time_off_entry_update",
            endpoint="/services/TimeOffApprovalService1.svc/ForceApprove",
            data=request_payload.get_submit_time_off_entry_payload
        )

        log_booking_successful = rail.WriteLogOperator(
            task_id='log_booking_successful_update',
            log='{{ dag_run.conf.log }}',
            message='Timeoff Synced Successfully',
            severity='Success',
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "timeoff_type": dag_run.conf["plan_ref_id"],
                "entry_date": dag_run.conf["timeoff_date"],
                "hours": dag_run.conf["hours"],
                'wd_event_id': dag_run.conf['wd_event_id'],
                'action': 'Update',
                "status": "Success",
                "details": "Time Off Booking is Updated successfully",
            }
        )

        finish = rail.EmptyOperator(
            task_id='update_timeoff_finish'
        )

        check_if_date_is_less_than_start_date >> rail.Label(
            "Yes") >> log_start_date_exception

        check_if_date_is_less_than_start_date >> rail.Label(
            "No") >> check_if_date_is_greater_than_end_date

        check_if_date_is_greater_than_end_date >> rail.Label(
            "Yes") >> is_received_hours_negative

        check_if_date_is_greater_than_end_date >> rail.Label(
            "No") >> check_total_hours_positive

        is_received_hours_negative >> rail.Label(
            "Yes") >> delete_timeoff >> log_delete_timeoff >> finish

        is_received_hours_negative >> rail.Label(
            "No") >> log_end_date_exception >> finish

        check_total_hours_positive >> rail.Label(
            "Yes") >> reopen_and_put_timeoff >> get_time_off_approval_status

        check_total_hours_positive >> rail.Label(
            "No") >> delete_timeoff

        get_time_off_approval_status >> is_timeoff_approved

        is_timeoff_approved >> rail.Label(
            "No") >> force_approve_time_off_entry >> log_booking_successful

        is_timeoff_approved >> rail.Label(
            "Yes") >> log_booking_successful >> finish

        return process_update_timeoff
