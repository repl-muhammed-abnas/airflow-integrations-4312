import rail
from necau.time_off_import.utils import request_payload


def delete_timeoff_booking_process(caller):
    with rail.TaskGroup(group_id=f'delet_timeoff_booking_process_{caller}', prefix_group_id=False) as delete_bookings:

        delete_timeoff_booking = rail.RepliconServiceOperator(
            task_id=f'delete_time_off_booking_{caller}',
            endpoint="/services/TimeOffService1.svc/DeleteTimeOff",
            data=request_payload.get_timeoff_delete_request
        )

        delete_timeoff_booking

        return delete_bookings
