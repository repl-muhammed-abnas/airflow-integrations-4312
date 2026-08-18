import rail
from pwcglobal.user_import_v4.utils import request_payload, custom_method


def get_update_timesheet_period():
    user_uri = request_payload.get_user_uri_template_exp()
    null = None
    with rail.TaskGroup(group_id='update_timesheet_period', prefix_group_id=False) as update_timesheet_period:

        get_current_timesheetperiod_schedule = rail.RepliconServiceOperator(
            task_id='get_current_timesheetperiod_schedule',
            endpoint='/services/TimesheetPeriodService2.svc/GetTimesheetPeriodScheduleForUser',
            data={
                "userUri": user_uri
            },
        )

        is_timesheetperiod_changed = rail.IfOperator(
            task_id='is_timesheetperiod_changed',
            test=lambda: rail.result(get_current_timesheetperiod_schedule.task_id) and (
                not request_payload.get_current_schedule(
                    rail.result(get_current_timesheetperiod_schedule.task_id)) or request_payload.get_current_schedule(rail.result(
                        get_current_timesheetperiod_schedule.task_id))['timesheetPeriod']['displayText'] != request_payload.get_conf()['timesheetperiodtype']),
            yes_task='get_timesheetperiodtype_uri',
            no_task='finish_update_timesheet_period',
        )

        get_timesheetperiodtype_uri = rail.RepliconServiceOperator(
            task_id='get_timesheetperiodtype_uri',
            endpoint='/services/TimesheetPeriodService2.svc/GetPageOfTimesheetPeriodsBySearchParameter',
            data={
                "page": "1",
                "pageSize": "1000",
                "timesheetPeriodSearch": {
                    "statusOptionUri": null,
                    "textSearch": {
                        "queryText": "{{ dag_run.conf.timesheetperiodtype }}",
                        "searchInDisplayText": "true",
                        "searchInName": "true",
                        "searchInDescription": "false"
                    }
                }
            },
            response_filter=custom_method.map_timesheetperiod_search_result
        )

        has_timesheetperiodtype_uri = rail.IfOperator(
            task_id='has_timesheetperiodtype_uri',
            test=lambda: rail.result('get_timesheetperiodtype_uri') and rail.result(
                'get_timesheetperiodtype_uri').get('uri'),
            yes_task='put_timesheetperiodschedule_for_user',
            no_task='finish_update_timesheet_period',
        )

        put_timesheetperiodschedule_for_user = rail.RepliconServiceOperator(
            task_id='put_timesheetperiodschedule_for_user',
            endpoint='/services/TimesheetPeriodService2.svc/PutTimesheetPeriodScheduleForUser',
            data=request_payload.get_timesheet_schedule_param
        )

        finish_update_timesheet_period = rail.EmptyOperator(
            task_id='finish_update_timesheet_period'
        )

        get_current_timesheetperiod_schedule >> is_timesheetperiod_changed
        is_timesheetperiod_changed >> rail.Label(
            'yes') >> get_timesheetperiodtype_uri >> has_timesheetperiodtype_uri
        is_timesheetperiod_changed >> rail.Label(
            'no') >> finish_update_timesheet_period
        has_timesheetperiodtype_uri >> rail.Label(
            'Yes') >> put_timesheetperiodschedule_for_user >> finish_update_timesheet_period
        has_timesheetperiodtype_uri >> rail.Label(
            'No') >> finish_update_timesheet_period

    return update_timesheet_period, finish_update_timesheet_period
