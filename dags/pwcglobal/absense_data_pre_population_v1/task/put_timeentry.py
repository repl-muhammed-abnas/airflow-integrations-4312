import rail
from pwcglobal.absense_data_pre_population_v1.utils import request_payload
from pwcglobal.absense_data_pre_population_v1.utils import response_filter
from pwcglobal.absense_data_pre_population_v1.utils import python_callable_method


def get_put_time_entry(caller):
    with rail.TaskGroup(group_id=f'put_time_entry_group_{caller}', prefix_group_id=False) as put_time_entry_group:

        get_timesheet_info = rail.RepliconServiceOperator(
            task_id=f'get_timesheet_info_{caller}',
            endpoint='/services/TimesheetListService1.svc/GetData',
            data=request_payload.get_timesheet_info_payload,
            response_filter=response_filter.get_timesheet_approval_status
        )

        is_timesheet_status_open = rail.IfOperator(
            task_id=f'is_timesheet_status_open_{caller}',
            test=lambda: bool(rail.result(get_timesheet_info.task_id) and rail.result(get_timesheet_info.task_id)[0]['approval_status']
                              and (rail.result('get_timesheet_info')[0]['approval_uri'] != 'urn:replicon:approval-status:open' 
                                     or rail.result('get_timesheet_info')[0]['timesheet_status_2_uri'] != "urn:replicon:timesheet-status-2:submission-failed")),
            yes_task=f'put_time_entry_{caller}',
            no_task=f'log_timesheet_status_not_open_{caller}'
        )

        log_timesheet_status_not_open = rail.WriteLogOperator(
            task_id=f'log_timesheet_status_not_open_{caller}',
            message='Time not populated as timesheet is not in open status',
            severity='Exception',
            properties=lambda: python_callable_method.get_log_properties(
                action='Validation', status='Exception')
        )

        put_time_entry = rail.RepliconServiceOperator(
            task_id=f'put_time_entry_{caller}',
            endpoint='/services/TimeEntryService3.svc/PutTimeEntry',
            data=lambda dag_run: request_payload.get_put_entry_payload(
                dag_run, caller),
        )

        log_time_entry_populated = rail.WriteLogOperator(
            task_id=f'log_time_entry_populated_{caller}',
            log='{{ result("create_child_log") }}',
            message='Time entry populated successfully',
            severity='Success',
            properties=lambda: python_callable_method.get_log_properties(
                action='Add', status='Success')
        )

        get_timesheet_info >> is_timesheet_status_open
        
        is_timesheet_status_open >> rail.Label(
            'Yes') >> put_time_entry >> log_time_entry_populated
        
        is_timesheet_status_open >> rail.Label(
            'No') >> log_timesheet_status_not_open

        return put_time_entry_group
