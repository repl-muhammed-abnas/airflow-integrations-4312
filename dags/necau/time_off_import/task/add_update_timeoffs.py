import rail
from necau.time_off_import.utils import request_payload


def get_add_update_timeoff(caller):
    with rail.TaskGroup(group_id=f'get_add_update_timeoff_{caller}', prefix_group_id=False) as add_update_timeoff:

        create_new_time_off_draft = rail.RepliconServiceOperator(
            task_id=f'create_new_time_off_draft_{caller}',
            endpoint="/services/TimeOffService1.svc/CreateNewTimeOffDraft",
            data=lambda dag_run: {'ownerUri': dag_run.conf['user_uri']}
        )

        update_time_off_type = rail.RepliconServiceOperator(
            task_id=f'update_time_off_type_{caller}',
            endpoint="/services/TimeOffService1.svc/UpdateTimeOffType",
            data=lambda dag_run: request_payload.get_update_timeoff_request(
                dag_run, caller)
        )

        update_time_off_comments = rail.RepliconServiceOperator(
            task_id=f'update_time_off_comments_{caller}',
            endpoint="/services/TimeOffService1.svc/UpdateTimeOffComments",
            data=lambda: request_payload.get_timeoff_comments(caller)
        )

        if caller in ["single_day", "multi_day_with_partial1"]:
            configure_singleday_time_off = rail.RepliconServiceOperator(
                task_id=f'configure_singleday_time_off{caller}',
                endpoint='/services/TimeOffService1.svc/ConfigureSingleDayTimeOff',
                data=lambda dag_run: request_payload.get_configure_request_based_timeoff_days(
                    dag_run, caller)
            )
        else:
            configure_multi_time_off = rail.RepliconServiceOperator(
                task_id=f'configure_multi_time_off{caller}',
                endpoint='/services/TimeOffService1.svc/ConfigureMultiDayTimeOff',
                data=lambda dag_run: request_payload.get_configure_request_based_timeoff_days(
                    dag_run, caller)
            )

        publish_timeoff_draft = rail.RepliconServiceOperator(
            task_id=f'publish_timeoff_draft_{caller}',
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data=lambda: request_payload.get_timeoff_draft_uri(caller)
        )

        update_request_workflow_key = rail.RepliconServiceOperator(
            task_id=f'update_request_workflow_key_{caller}',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: request_payload.get_update_custom_key_request(
                dag_run.conf['request_key'], caller, 'Request Key')
        )

        update_sequence_number = rail.RepliconServiceOperator(
            task_id=f'update_sequence_number_{caller}',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: request_payload.get_update_custom_key_request(
                dag_run.conf['seq_no'], caller, 'sequencekey')
        )

        is_timeoff_approved = rail.IfOperator(
            task_id=f'is_timeoff_approved_{caller}',
            test=lambda dag_run: dag_run.conf['action_status'].lower(
            ) == 'approved',
            yes_task=f'approve_timeoff_booking_{caller}',
            no_task=f'dummy_operator_1_{caller}'
        )

        approve_timeoff_booking = rail.RepliconServiceOperator(
            task_id=f'approve_timeoff_booking_{caller}',
            endpoint="/services/TimeOffApprovalService1.svc/ForceApprove",
            data=lambda: request_payload.get_timeoff_action_request(caller)
        )

        is_timeoff_requested = rail.IfOperator(
            task_id=f'is_timeoff_requested_{caller}',
            test=lambda dag_run: dag_run.conf['action_status'].lower(
            ) == 'requested',
            yes_task=f'submit_timeoff_booking_{caller}',
            no_task=f'done_booking_{caller}'
        )

        submit_timeoff_booking = rail.RepliconServiceOperator(
            task_id=f'submit_timeoff_booking_{caller}',
            endpoint="/services/TimeOffApprovalService1.svc/Submit",
            data=lambda: request_payload.get_timeoff_action_request(
                caller, 'submit')
        )

        done_booking = rail.EmptyOperator(
            task_id=f'done_booking_{caller}'
        )

        dummy_operator_1 = rail.EmptyOperator(
            task_id=f'dummy_operator_1_{caller}'
        )

        create_new_time_off_draft >> update_time_off_type >> update_time_off_comments
        if caller in ["single_day", "multi_day_with_partial1"]:
            update_time_off_comments >> configure_singleday_time_off >> publish_timeoff_draft
        else:
            update_time_off_comments >> configure_multi_time_off >> publish_timeoff_draft
        publish_timeoff_draft >> update_request_workflow_key >> update_sequence_number >> is_timeoff_approved
        is_timeoff_approved >> rail.Label(
            "Yes") >> approve_timeoff_booking >> dummy_operator_1 >> is_timeoff_requested
        is_timeoff_requested >> rail.Label(
            "Yes") >> submit_timeoff_booking >> done_booking
        is_timeoff_approved >> rail.Label("No") >> dummy_operator_1
        is_timeoff_requested >> rail.Label("No") >> done_booking

        return add_update_timeoff
