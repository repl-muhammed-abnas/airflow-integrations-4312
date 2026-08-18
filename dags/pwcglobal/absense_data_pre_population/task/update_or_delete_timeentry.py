import rail
from pwcglobal.absense_data_pre_population.utils import request_payload
from pwcglobal.absense_data_pre_population.utils import response_filter
from pwcglobal.absense_data_pre_population.utils import python_callable_method


def get_update_or_delete_time_entry(caller):
    with rail.TaskGroup(group_id=f'update_or_delete_time_entry_group_{caller}', prefix_group_id=False) as update_or_delete_time_entry_group:

        get_timesheet_info = rail.RepliconServiceOperator(
            task_id=f'get_timesheet_info_{caller}',
            endpoint='/services/TimesheetListService1.svc/GetData',
            data=request_payload.get_timesheet_info_payload,
            response_filter=response_filter.get_timesheet_approval_status
        )

        search_time_entry_by_id_for_update= rail.RepliconServiceOperator(
            task_id='search_time_entry_by_id_for_update',
            endpoint='/services/TimeEntryRevisionGroupListService1.svc/GetData',
            data=request_payload.get_timeentry_id_payload,
            response_filter=response_filter.get_timeentries_list
        )

        check_approval_status_for_update = rail.IfOperator(
            task_id='check_approval_status_for_update',
            test=lambda: bool(rail.result('search_time_entry_by_id_for_update')[0]['approvalstatus'] in ["Waiting for Approval","Approved"]),
            yes_task='log_time_entry_is_approved_for_update',
            no_task=f'is_timesheet_status_open_{caller}'
        )
        
        log_time_entry_is_approved_for_update = rail.WriteLogOperator(
            task_id='log_time_entry_is_approved_for_update',
            log='{{ result("create_child_log") }}',
            message='Time entry {{ dag_run.conf.TimeEntryID }}  not updated since Time Entry Revision Group is not open or approved',
            severity='Exception',
            properties=lambda: python_callable_method.get_log_properties(
                action='Update', status='Exception')
        )

        is_timesheet_status_open = rail.IfOperator(
            task_id=f'is_timesheet_status_open_{caller}',
            test=lambda: bool(rail.result(get_timesheet_info.task_id) and rail.result(get_timesheet_info.task_id)[0]['approval_status']
                              and rail.result(get_timesheet_info.task_id)[0]['approval_uri'] != 'urn:replicon:approval-status:open'),
            yes_task=f'log_timesheet_status_not_open_{caller}',
            no_task=f'is_hours_quantity_zero_{caller}'
        )

        log_timesheet_status_not_open = rail.WriteLogOperator(
            task_id=f'log_timesheet_status_not_open_{caller}',
            message='Time not populated as timesheet is not in open status',
            severity='Exception',
            properties=lambda: python_callable_method.get_log_properties(
                action='Validation', status='Exception')
        )

        is_hours_quantity_zero = rail.IfOperator(
            task_id=f'is_hours_quantity_zero_{caller}',
            test=lambda dag_run: bool(
                dag_run.conf["hoursquantityinseconds"] == 0),
            yes_task=f'delete_time_entry_{caller}',
            no_task=f'update_existing_time_entry_{caller}'
        )

        delete_time_entry = rail.RepliconServiceOperator(
            task_id=f'delete_time_entry_{caller}',
            endpoint='/services/TimeEntryRevisionGroupService1.svc/DeleteTimeEntryRevisionGroup',
            data={
                "timeEntryRevisionGroupUri": '{{ result("search_time_entry_by_id")[0]["timeentryrevisiongroup"] }}'
            }
        )

        log_time_entry_deleted = rail.WriteLogOperator(
            task_id=f'log_time_entry_deleted_{caller}',
            log='{{ result("create_child_log") }}',
            message='Time entry deleted successfully since 0 hours received in feed',
            severity='Success',
            properties=lambda: python_callable_method.get_log_properties(
                action='Update', status='Success')
        )

        update_existing_time_entry = rail.RepliconServiceOperator(
            task_id=f'update_existing_time_entry_{caller}',
            endpoint='/services/TimeEntryRevisionGroupService1.svc/PutTimeEntryRevisionGroup',
            data=lambda dag_run: request_payload.get_update_entry_payload(
                dag_run, caller)
        )

        log_time_entry_updated = rail.WriteLogOperator(
            task_id=f'log_time_entry_updated_{caller}',
            log='{{ result("create_child_log") }}',
            message='Time entry updated successfully',
            severity='Success',
            properties=lambda: python_callable_method.get_log_properties(
                action='Update', status='Success')
        )

        get_timesheet_info >> search_time_entry_by_id_for_update >> check_approval_status_for_update
        
        check_approval_status_for_update >> rail.Label(
            'Yes') >> log_time_entry_is_approved_for_update
        check_approval_status_for_update >> rail.Label(
            'No') >> is_timesheet_status_open
        
        is_timesheet_status_open >> rail.Label(
            'Yes') >> is_hours_quantity_zero
        
        is_timesheet_status_open >> rail.Label(
            'No') >> log_timesheet_status_not_open

        is_hours_quantity_zero >> rail.Label(
            'Yes') >> delete_time_entry >> log_time_entry_deleted
        is_hours_quantity_zero >> rail.Label(
            'No') >> update_existing_time_entry >> log_time_entry_updated

        return update_or_delete_time_entry_group
