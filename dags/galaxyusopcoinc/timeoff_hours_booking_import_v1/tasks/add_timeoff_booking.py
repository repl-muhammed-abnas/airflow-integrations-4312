import rail
import rail.lib
from galaxyusopcoinc.timeoff_hours_booking_import_v1.utils import request_payload, custom_methods


def add_timeoff_booking():
    with rail.TaskGroup(group_id='add_timeoff_booking', prefix_group_id=False) as process_add_timeoff:

        check_if_date_is_less_than_start_date = rail.IfOperator(
            task_id='check_if_date_is_less_than_start_date_add',
            test=lambda dag_run: custom_methods.is_entry_date_less_than_startdate(dag_run.conf[
                'user_start_date'], dag_run.conf['timeoff_date']) if dag_run.conf['user_start_date'] else False,
            yes_task='log_start_date_exception_add',
            no_task='check_if_date_is_greater_than_end_date_add'
        )

        log_start_date_exception = rail.WriteLogOperator(
            task_id='log_start_date_exception_add',
            log='{{ dag_run.conf.log }}',
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
            task_id='check_if_date_is_greater_than_end_date_add',
            test=lambda dag_run: custom_methods.is_entry_date_less_than_startdate(dag_run.conf[
                'timeoff_date'], dag_run.conf['user_end_date']) if dag_run.conf['user_end_date'] else False,
            yes_task='log_end_date_exception_add',
            no_task='is_received_hours_positive_add'
        )

        log_end_date_exception = rail.WriteLogOperator(
            task_id='log_end_date_exception_add',
            log='{{ dag_run.conf.log }}',
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

        is_received_hours_positive = rail.IfOperator(
            task_id='is_received_hours_positive_add',
            test=lambda dag_run: float(dag_run.conf["hours"]) > 0,
            yes_task='put_and_submit_timeoff_booking_for_user_add',
            no_task='log_negative_hours_exception_add'
        )

        log_negative_hours_exception = rail.WriteLogOperator(
            task_id='log_negative_hours_exception_add',
            log='{{ dag_run.conf.log }}',
            message='The negative hours received for new booking',
            severity='Exception',
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "timeoff_type": dag_run.conf["plan_ref_id"],
                "entry_date": dag_run.conf["timeoff_date"],
                "hours": dag_run.conf["hours"],
                "wd_event_id": dag_run.conf["wd_event_id"],
                'action': 'Add',
                'status': 'Exception',
                "details": "The zero/negative hours received for 'New' booking",
            }
        )

        put_and_submit_timeoff_booking_for_user = rail.RepliconServiceOperator(
            task_id='put_and_submit_timeoff_booking_for_user_add',
            endpoint="/services/TimeOffApprovalService1.svc/PutAndSubmitTimeOff",
            data=request_payload.get_create_and_publish_timeoff_payload
        )

        get_time_off_approval_status = rail.RepliconServiceOperator(
            task_id="get_time_off_approval_status_add",
            endpoint="/services/TimeOffApprovalService1.svc/GetApprovalHistoryDetails",
            data={
                "timeOffUri": "{{ result('put_and_submit_timeoff_booking_for_user_add').uri }}"
            },
            data_handler=lambda response: response['approvalStatus']['displayText']
        )

        is_timeoff_approved = rail.IfOperator(
            task_id="is_timeoff_approved_add",
            test=lambda: rail.result(
                'get_time_off_approval_status_add') == 'Approved',
            yes_task='log_booking_add_successful_add',
            no_task='approve_timeoff_booking_for_user_add'
        )

        approve_timeoff_booking_for_user = rail.RepliconServiceOperator(
            task_id='approve_timeoff_booking_for_user_add',
            endpoint="/services/TimeOffApprovalService1.svc/ForceApprove",
            data=request_payload.get_approve_holiday_booking_payload
        )

        log_booking_add_successful = rail.WriteLogOperator(
            task_id='log_booking_add_successful_add',
            log='{{ dag_run.conf.log }}',
            message='Timeoff Synced Successfully',
            severity='Success',
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "timeoff_type": dag_run.conf["plan_ref_id"],
                "entry_date": dag_run.conf["timeoff_date"],
                "hours": dag_run.conf["hours"],
                'wd_event_id': dag_run.conf['wd_event_id'],
                'action': 'Add',
                "status": "Success",
                "details": "Time Off Booking is added successfully",
            }
        )

        finish = rail.EmptyOperator(
            task_id='add_timeoff_finish'
        )

        check_if_date_is_less_than_start_date >> rail.Label(
            "Yes") >> log_start_date_exception >> finish

        check_if_date_is_less_than_start_date >> rail.Label(
            "No") >> check_if_date_is_greater_than_end_date

        check_if_date_is_greater_than_end_date >> rail.Label(
            "Yes") >> log_end_date_exception >> finish

        check_if_date_is_greater_than_end_date >> rail.Label(
            "Yes") >> is_received_hours_positive

        is_received_hours_positive >> rail.Label(
            "Yes") >> put_and_submit_timeoff_booking_for_user

        is_received_hours_positive >> rail.Label(
            "No") >> log_negative_hours_exception >> finish

        put_and_submit_timeoff_booking_for_user >> get_time_off_approval_status >> is_timeoff_approved

        is_timeoff_approved >> rail.Label(
            "Yes") >> log_booking_add_successful

        is_timeoff_approved >> rail.Label(
            "No") >> approve_timeoff_booking_for_user >> log_booking_add_successful >> finish

    return process_add_timeoff
