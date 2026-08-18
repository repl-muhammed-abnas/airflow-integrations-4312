import rail
from necau.auto_shift_assignment.utils import request_payload


def get_shift_summary(caller):
    with rail.TaskGroup(group_id=f'shift_summary_{caller}', prefix_group_id=False):

        get_shift_schedule_summary = rail.RepliconServiceOperator(
            task_id=f'get_shift_schedule_summary_{caller}',
            endpoint="/services/ShiftAssignmentService1.svc/GetShiftScheduleSummary",
            data=lambda dag_run: request_payload.get_shift_summary_payload(
                dag_run, 'get_shift_week_info', 'get_shift_day_diff', caller)
        )

        return get_shift_schedule_summary
