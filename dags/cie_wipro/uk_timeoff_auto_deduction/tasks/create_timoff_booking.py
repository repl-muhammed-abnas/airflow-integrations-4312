# pylint: disable=unnecessary-lambda
import rail
from cie_wipro.uk_timeoff_auto_deduction.utils import request_payload


def get_create_timeoff():

    with rail.TaskGroup(group_id='create_timeoff_booking', prefix_group_id=False):

        createdraft_timeoff_booking_for_user = rail.RepliconServiceOperator(
            task_id='createdraft_timeoff_booking_for_user',
            endpoint="/services/TimeOffService1.svc/CreateNewTimeOffDraft",
            data={
                "ownerUri": "{{ dag_run.conf.user_uri }}"
            }
        )

        put_timeoff_booking_for_user = rail.RepliconServiceOperator(
            task_id='put_timeoff_booking_for_user',
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: request_payload.get_timeoff_booking_payload(
                dag_run)
        )

        publish_timeoff_draft_for_user = rail.RepliconServiceOperator(
            task_id='publish_timeoff_draft_for_user',
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data={
                "timeOff": "{{ result('createdraft_timeoff_booking_for_user') }}"
            }
        )

        approve_timeoff_booking_for_user = rail.RepliconServiceOperator(
            task_id='approve_timeoff_booking_for_user',
            endpoint="/services/TimeOffApprovalService1.svc/ForceApprove",
            data=lambda: request_payload.get_approve_timeoff_booking_payload()
        )

        createdraft_timeoff_booking_for_user >> put_timeoff_booking_for_user >> publish_timeoff_draft_for_user >> approve_timeoff_booking_for_user

        return createdraft_timeoff_booking_for_user, approve_timeoff_booking_for_user
