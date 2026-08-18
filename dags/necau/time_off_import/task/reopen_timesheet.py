import rail
from necau.time_off_import.utils import python_callable_method
from necau.time_off_import.utils import request_payload


def get_timesheet_open_process(caller, config, delete=False, delete_approve_option=False):
    with rail.TaskGroup(group_id=f'reopen_timesheet_process_{caller}', prefix_group_id=False) as reopen_timesheet:

        open_timesheet = rail.RepliconServiceOperator(
            task_id=f'open_timesheet_{caller}',
            endpoint="/services/TimesheetApprovalService1.svc/Reopen",
            data=request_payload.get_re_open_request
        )

        has_error_in_open_timesheet = rail.IfOperator(
            task_id=f'has_error_in_open_timesheet_{caller}',
            trigger_rule='all_done',
            test=lambda: python_callable_method.is_error_in_opening(
                f'open_timesheet_{caller}'),
            yes_task=f'get_timesheet_info_{caller}',
            no_task=f'dummy_operator_{caller}'
        )

        dummy_operator = rail.EmptyOperator(
            task_id=f'dummy_operator_{caller}'
        )

        has_failed = rail.IfOperator(
            task_id=f'has_failed_{caller}',
            test=lambda: python_callable_method.is_task_triggered(
                f'open_timesheet_{caller}'),
            yes_task=f'initiate_mail_process_{caller}',
        )

        get_timesheet_info = rail.RepliconServiceOperator(
            task_id=f'get_timesheet_info_{caller}',
            trigger_rule='one_failed',
            endpoint="/services/TimesheetService1.svc/GetTimesheetDetails",
            data=request_payload.get_timesheet_uri
        )

        is_timesheet_approved_waiting = rail.IfOperator(
            task_id=f'is_timesheet_approved_waiting_{caller}',
            test=lambda: python_callable_method.get_timesheet_status(
                f'get_timesheet_info_{caller}'),
            yes_task=f'open_timesheet_again_{caller}',
            no_task=f'initiate_mail_process_{caller}'
        )

        open_timesheet_again = rail.RepliconServiceOperator(
            task_id=f'open_timesheet_again_{caller}',
            endpoint="/services/TimesheetApprovalService1.svc/Reopen",
            data=request_payload.get_re_open_request
        )

        initiate_mail_process = rail.EmptyOperator(
            task_id=f'initiate_mail_process_{caller}',
            trigger_rule='one_success',
        )

        if delete_approve_option:
            has_delete_timeoff = rail.IfOperator(
                task_id=f'has_delete_timeoff_{caller}',
                test=delete,
                yes_task=f'timeoff_booking_deletion_{caller}',
                no_task=f'approve_timeoff_booking_{caller}'
            )

            approve_timeoff_booking = rail.RepliconServiceOperator(
                task_id=f'approve_timeoff_booking_{caller}',
                endpoint="/services/TimeOffApprovalService1.svc/ForceApprove",
                data=request_payload.get_timeoff_approve_request
            )

            delete_timeoff_booking = rail.RepliconServiceOperator(
                task_id=f'timeoff_booking_deletion_{caller}',
                endpoint="/services/TimeOffService1.svc/DeleteTimeOff",
                data=request_payload.get_timeoff_delete_request
            )

        is_user_supervior_mail_present = rail.IfOperator(
            task_id=f'is_user_supervior_mail_present_{caller}',
            test=python_callable_method.get_email_status,
            yes_task=f'get_user_super_email_ids_{caller}',
            no_task=f"finish_{caller}"
        )

        get_user_super_email_ids = rail.PythonOperator(
            task_id=f'get_user_super_email_ids_{caller}',
            python_callable=python_callable_method.get_user_super_email_ids
        )

        send_mail_on_reopn_timesheet = rail.EmailOperator(
            task_id=f'send_mail_on_reopn_timesheet_{caller}',
            to="{{ result('get_user_super_email_ids_" + caller + "') }}",
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Timesheet for the entry date {{ dag_run.conf.start_date }} Reopened!- {{ current_time() }}',
            html_content="templates/email/reopen_timesheet.html",
        )

        finish = rail.EmptyOperator(
            task_id=f"finish_{caller}"
        )

        open_timesheet >> has_error_in_open_timesheet
        has_error_in_open_timesheet >> rail.Label(
            "Yes") >> get_timesheet_info >> is_timesheet_approved_waiting
        is_timesheet_approved_waiting >> rail.Label(
            'Yes') >> open_timesheet_again >> initiate_mail_process
        is_timesheet_approved_waiting >> rail.Label(
            "No") >> initiate_mail_process
        has_error_in_open_timesheet >> rail.Label(
            "No") >> dummy_operator >> has_failed
        has_failed >> rail.Label("Yes") >> initiate_mail_process
        if delete_approve_option:
            initiate_mail_process >> has_delete_timeoff
            has_delete_timeoff >> rail.Label(
                "Yes") >> delete_timeoff_booking >> is_user_supervior_mail_present
            has_delete_timeoff >> rail.Label(
                "No") >> approve_timeoff_booking >> is_user_supervior_mail_present
        else:
            initiate_mail_process >> is_user_supervior_mail_present

        is_user_supervior_mail_present >> rail.Label(
            'Yes') >> get_user_super_email_ids >> send_mail_on_reopn_timesheet >> finish
        is_user_supervior_mail_present >> rail.Label(
            'No') >> finish

        return reopen_timesheet
