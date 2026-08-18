import rail
from pwcglobal.time_export_v3.request_payload import get_current_timesheet_period_payload


def current_timesheet_period_task(caller):
    with rail.TaskGroup(group_id=f'current_timesheet_period_{caller}', prefix_group_id=False):

        current_timesheet_period_replicon = rail.RepliconServiceOperator(
            task_id=f'current_timesheet_period_replicon_{caller}',
            endpoint='/services/TimesheetListService1.svc/GetData',
            data=get_current_timesheet_period_payload
        )

        return current_timesheet_period_replicon
