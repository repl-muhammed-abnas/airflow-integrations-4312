import rail
from adessa.timeoff_sync.utils import request_payload

def get_create_timeoff(booking_type):

    with rail.TaskGroup(group_id=f'create_timeoff_booking_type_{booking_type}', prefix_group_id=False):

        createdraft_timeoffbooking_for_user = rail.RepliconServiceOperator(
            task_id=f'createdraft_timeoffbooking_for_user_type_{booking_type}',
            endpoint="/services/TimeOffService1.svc/CreateNewTimeOffDraft",
            data={
                "ownerUri": "{{ dag_run.conf.user_uri }}"
            }
        )

        put_timeoff_booking_for_user = rail.RepliconServiceOperator(
            task_id=f'put_timeoff_booking_for_user_type_{booking_type}',
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: request_payload.get_create_timeoff_payload(dag_run, booking_type)
        )

        publish_timeoff_draft_for_user = rail.RepliconServiceOperator(
            task_id=f'publish_timeoff_draft_for_user_type_{booking_type}',
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data={
                "timeOff": "{{ result('createdraft_timeoffbooking_for_user_type_" + booking_type + "') }}"
            }
        )

        approve_timeoff_booking_for_user = rail.RepliconServiceOperator(
            task_id=f'approve_timeoff_booking_for_user_type_{booking_type}',
            endpoint="/services/TimeOffApprovalService1.svc/ForceApprove",
            data=lambda: request_payload.get_approve_timeoff_booking_payload(booking_type)
        )

        createdraft_timeoffbooking_for_user >> put_timeoff_booking_for_user >> publish_timeoff_draft_for_user >> approve_timeoff_booking_for_user

        return createdraft_timeoffbooking_for_user, approve_timeoff_booking_for_user
