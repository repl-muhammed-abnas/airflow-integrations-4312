
from datetime import timedelta, date
import uuid
from airflow.models import Variable
import rail
from daimlertrucks.liquidplanner_time_entry_sync.utils import request_payload, response_filter

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'daimlertrucks_timeimport_puttimeentrieschild_{config.instance}',
        description=f'Live|DTNA_Time import_Put time entries (Child) {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_timesheets_to_submit_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_timesheets_to_submit_log',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_timesheets_to_submit_log = rail.CreateLogOperator(
            task_id='create_timesheets_to_submit_log'
        )

        create_time_entry_import_log = rail.CreateLogOperator(
            task_id='create_time_entry_import_log'
        )

        getalltaskcodes_getthetaskenabled_56 = rail.RepliconServiceOperator(
            task_id='getalltaskcodes_getthetaskenabled_56',
            endpoint="/services/TaskListService1.svc/GetData",
            data=request_payload.get_all_tasks_enabled_payload,
            response_filter=response_filter.get_task_details
        )

        log_taskuri = rail.PythonOperator(
            task_id='log_taskuri',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'getalltaskcodes_getthetaskenabled_56'), 'taskcode', dag_run.conf['taskcode'].lower(), 'uri')
        )

        if_request_taskuri_present_10 = rail.IfOperator(
            task_id='if_request_taskuri_present_10',
            test=lambda: rail.result('log_taskuri') is not null,
            yes_task="log_rownumbertopass_5",
            no_task="dtna_time_entry_import_logs_add_entry_46",
        )

        log_rownumbertopass_5 = rail.PythonOperator(
            task_id='log_rownumbertopass_5',
            python_callable=lambda: rail.result(
                'log_taskuri').rsplit(':', 1)[-1]
        )

        log_minutesworkedtouse_9 = rail.PythonOperator(
            task_id='log_minutesworkedtouse_9',
            python_callable=lambda dag_run: int(
                float(dag_run.conf['hoursworked']) * 3600)
        )

        gettaskdetails_12 = rail.RepliconServiceOperator(
            task_id='gettaskdetails_12',
            endpoint="/services/TaskService1.svc/GetTaskDetails",
            data={
                "taskUri": "{{ result('log_taskuri') }}"
            }
        )

        if_enddate_day_present_15 = rail.IfOperator(
            task_id='if_enddate_day_present_15',
            test=lambda: bool(rail.result('gettaskdetails_12') and rail.result('gettaskdetails_12')['timeEntryDateRange']
                              and rail.result('gettaskdetails_12')['timeEntryDateRange']['endDate']
                              and rail.result('gettaskdetails_12')['timeEntryDateRange']['endDate']['day']),
            yes_task="enddate_day_present",
            no_task="enddate_day_not_present",
        )

        enddate_day_present = rail.EmptyOperator(
            task_id='enddate_day_present'
        )

        enddate_day_not_present = rail.EmptyOperator(
            task_id='enddate_day_not_present'
        )

        def check_enddate_less_than_entrydate(dag_run):
            if rail.result('gettaskdetails_12') and rail.result('gettaskdetails_12')['timeEntryDateRange'] \
                    and rail.result('gettaskdetails_12')['timeEntryDateRange']['endDate'] \
                    and rail.result('gettaskdetails_12')['timeEntryDateRange']['endDate']['day']:
                end_date = date(year=int(rail.result('gettaskdetails_12')['timeEntryDateRange']['endDate']['year']),
                                month=int(rail.result('gettaskdetails_12')[
                                          'timeEntryDateRange']['endDate']['month']),
                                day=int(rail.result('gettaskdetails_12')['timeEntryDateRange']['endDate']['day']))
            else:
                end_date = date(1972, 1, 1)

            entry_date = date(year=int(dag_run.conf['entrydateyear']),
                              month=int(dag_run.conf['entrydatemonth']),
                              day=int(dag_run.conf['entrydateday']))

            return end_date < entry_date

        if_log_enddateintimeformat_17_less_than_datalogger501d791dmessage_18 = rail.IfOperator(
            task_id='if_log_enddateintimeformat_17_less_than_datalogger501d791dmessage_18',
            test=check_enddate_less_than_entrydate,
            yes_task="dtna_time_entry_import_logs_add_entry_19",
            no_task="if_isclosed_to_s_equals_to_true_21",
        )

        dtna_time_entry_import_logs_add_entry_19 = rail.WriteLogOperator(
            task_id='dtna_time_entry_import_logs_add_entry_19',
            log="{{ result('create_time_entry_import_log') }}",
            message="Time entry not imported since task has end date set before the entry date received.",
            severity="Error",
            properties={
                "user_name": "{{ dag_run.conf.userid }}",
                "status": "Error",
                "reason": "Time entry not imported since task has end date set before the entry date received.",
                "entrydate": "{{ dag_run.conf.entrydate_received }}",
                "taskcode": "{{ dag_run.conf.taskcode }}",
                "hoursworked": "{{ dag_run.conf.hoursworked }}"
            }
        )

        if_isclosed_to_s_equals_to_true_21 = rail.IfOperator(
            task_id='if_isclosed_to_s_equals_to_true_21',
            test=lambda: rail.result('gettaskdetails_12')['isClosed'] is True or rail.result(
                'gettaskdetails_12')['isTimeEntryAllowed'] is False,
            yes_task="dtna_time_entry_import_logs_add_entry_22",
            no_task="get_timesheet_for_date2_26",
        )

        dtna_time_entry_import_logs_add_entry_22 = rail.WriteLogOperator(
            task_id='dtna_time_entry_import_logs_add_entry_22',
            log="{{ result('create_time_entry_import_log') }}",
            message="Time entry not imported since task code is set as closed/time entry not allowed on task in Replicon.",
            severity="Error",
            properties={
                "user_name": "{{ dag_run.conf.userid }}",
                "status": "Error",
                "reason": "Time entry not imported since task code is set as closed/time entry not allowed on task in Replicon.",
                "entrydate": "{{ dag_run.conf.entrydate_received }}",
                "taskcode": "{{ dag_run.conf.taskcode }}",
                "hoursworked": "{{ dag_run.conf.hoursworked }}"
            }
        )

        get_timesheet_for_date2_26 = rail.RepliconServiceOperator(
            task_id='get_timesheet_for_date2_26',
            endpoint="/services/TimesheetService1.svc/GetTimesheetForDate2",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "date": {
                    "year": "{{dag_run.conf.entrydateyear}}",
                    "month": "{{dag_run.conf.entrydatemonth}}",
                    "day": "{{dag_run.conf.entrydateday}}"
                },
                "timesheetGetOptionUri": null
            }
        )

        if_timesheet_uri_blank_27 = rail.IfOperator(
            task_id='if_timesheet_uri_blank_27',
            test=lambda: rail.result('get_timesheet_for_date2_26') is null,
            yes_task="get_timesheet_for_date2_28",
            no_task="log_timesheeturi_33",
        )

        get_timesheet_for_date2_28 = rail.RepliconServiceOperator(
            task_id='get_timesheet_for_date2_28',
            endpoint="/services/TimesheetService1.svc/GetTimesheetForDate2",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "date": {
                    "year": "{{dag_run.conf.entrydateyear}}",
                    "month": "{{dag_run.conf.entrydatemonth}}",
                    "day": "{{dag_run.conf.entrydateday}}"
                },
                "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
            }
        )

        get_timesheet_details_29 = rail.RepliconServiceOperator(
            task_id='get_timesheet_details_29',
            endpoint="/services/TimesheetService1.svc/GetTimesheetDetails",
            data={
                "timesheetUri": "{{ result('get_timesheet_for_date2_28').timesheet.uri }}"
            }
        )

        get_time_entries_for_user_and_date_range_30 = rail.RepliconServiceOperator(
            task_id='get_time_entries_for_user_and_date_range_30',
            endpoint="/services/TimeEntryService3.svc/GetTimeEntriesForUserAndDateRange",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "dateRange": {
                    "startDate": {
                        "year": "{{result('get_timesheet_details_29').dateRange.startDate.year}}",
                        "month": "{{result('get_timesheet_details_29').dateRange.startDate.month}}",
                        "day": "{{result('get_timesheet_details_29').dateRange.startDate.day}}"
                    },
                    "endDate": {
                        "year": "{{result('get_timesheet_details_29').dateRange.endDate.year}}",
                        "month": "{{result('get_timesheet_details_29').dateRange.endDate.month}}",
                        "day": "{{result('get_timesheet_details_29').dateRange.endDate.day}}"
                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                },
                "asOf": null
            }
        )

        delete_time_entry_32 = rail.RepliconServiceCallForEachItemOperator(
            task_id='delete_time_entry_32',
            items="{{ result('get_time_entries_for_user_and_date_range_30') | to_json }}",
            endpoint="/services/TimeEntryService3.svc/DeleteTimeEntry",
            data={
                "timeEntryUri": "{{ item.uri }}"
            }
        )

        def get_timesheeturi_value():
            if rail.result('get_timesheet_for_date2_28') and rail.result('get_timesheet_for_date2_28')['timesheet']['uri']:
                return rail.result('get_timesheet_for_date2_28')['timesheet']['uri']
            return rail.result('get_timesheet_for_date2_26')['timesheet']['uri'] if rail.result('get_timesheet_for_date2_26') else ''

        log_timesheeturi_33 = rail.PythonOperator(
            task_id='log_timesheeturi_33',
            python_callable=get_timesheeturi_value
        )

        get_timesheetapprovalsatus_34 = rail.RepliconServiceOperator(
            task_id='get_timesheetapprovalsatus_34',
            endpoint="/services/TimesheetApprovalService1.svc/GetTimesheetApprovalDetails2",
            data={
                "timesheetUri": "{{ result('log_timesheeturi_33') }}"
            }
        )

        if_approvalstatus_displaytext_equals_to_notsubmitted_35 = rail.IfOperator(
            task_id='if_approvalstatus_displaytext_equals_to_notsubmitted_35',
            test='''{{ result('get_timesheetapprovalsatus_34').approvalStatus.displayText == 'Not Submitted' }}''',
            yes_task="putnewtimeentries_36",
            no_task="reopen_timesheet_39",
        )

        putnewtimeentries_36 = rail.RepliconServiceOperator(
            task_id='putnewtimeentries_36',
            endpoint="/services/TimeEntryService3.svc/PutTimeEntry",
            data=request_payload.get_putnewtimeentries_36_payload
        )

        dtna_timesheets_to_submit_add_entry_37 = rail.WriteLogOperator(
            task_id='dtna_timesheets_to_submit_add_entry_37',
            log="{{ result('create_timesheets_to_submit_log') }}",
            message="na",
            severity="Info",
            properties={
                "timesheeturi": "{{ result('get_timesheetapprovalsatus_34').timesheet.uri }}",
                "status": '''{{ result('get_timesheetapprovalsatus_34').approvalStatus.displayText }}'''
            }
        )

        reopen_timesheet_39 = rail.RepliconServiceOperator(
            task_id='reopen_timesheet_39',
            endpoint="/services/TimesheetApprovalService1.svc/Reopen",
            data=lambda: {
                "timesheetUri": rail.result('log_timesheeturi_33'),
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Reopening Timesheet to put time entry"
            }
        )

        putnewtimeentries_40 = rail.RepliconServiceOperator(
            task_id='putnewtimeentries_40',
            endpoint="/services/TimeEntryService3.svc/PutTimeEntry",
            data=request_payload.get_putnewtimeentries_40_payload
        )

        dtna_timesheets_to_submit_add_entry_41 = rail.WriteLogOperator(
            task_id='dtna_timesheets_to_submit_add_entry_41',
            log="{{ result('create_timesheets_to_submit_log') }}",
            message="na",
            severity="Info",
            properties={
                "timesheeturi": "{{ result('get_timesheetapprovalsatus_34').timesheet.uri }}",
                "status": '''{{ result('get_timesheetapprovalsatus_34').approvalStatus.displayText }}'''
            }
        )

        dtna_time_entry_import_logs_add_entry_42 = rail.WriteLogOperator(
            task_id='dtna_time_entry_import_logs_add_entry_42',
            log="{{ result('create_time_entry_import_log') }}",
            message="Time entry imported",
            severity="Success",
            properties={
                "user_name": "{{ dag_run.conf.userid }}",
                "status": "Success",
                "reason": "Time entry imported",
                "entrydate": "{{ dag_run.conf.entrydate_received }}",
                "taskcode": "{{ dag_run.conf.taskcode }}",
                "hoursworked": "{{ dag_run.conf.hoursworked }}"
            }
        )

        dtna_time_entry_import_logs_add_entry_46 = rail.WriteLogOperator(
            task_id='dtna_time_entry_import_logs_add_entry_46',
            log="{{ result('create_time_entry_import_log') }}",
            message="Time entry not imported since task is not available in Replicon.",
            severity="Error",
            properties={
                "user_name": "{{ dag_run.conf.userid }}",
                "status": "Error",
                "reason": "Time entry not imported since task is not available in Replicon.",
                "entrydate": "{{ dag_run.conf.entrydate_received }}",
                "taskcode": "{{ dag_run.conf.taskcode }}",
                "hoursworked": "{{ dag_run.conf.hoursworked }}"
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> create_timesheets_to_submit_log
        create_timesheets_to_submit_log >> create_time_entry_import_log >> getalltaskcodes_getthetaskenabled_56 \
            >> log_taskuri >> if_request_taskuri_present_10
        if_request_taskuri_present_10 >> rail.Label(
            'Yes') >> log_rownumbertopass_5 >> log_minutesworkedtouse_9 >> gettaskdetails_12 >> if_enddate_day_present_15
        if_enddate_day_present_15 >> rail.Label(
            'Yes') >> enddate_day_present >> if_log_enddateintimeformat_17_less_than_datalogger501d791dmessage_18
        if_log_enddateintimeformat_17_less_than_datalogger501d791dmessage_18 >> rail.Label(
            'Yes') >> dtna_time_entry_import_logs_add_entry_19 >> finish
        if_log_enddateintimeformat_17_less_than_datalogger501d791dmessage_18 >> rail.Label(
            'No') >> if_isclosed_to_s_equals_to_true_21
        if_enddate_day_present_15 >> rail.Label(
            'No') >> enddate_day_not_present >> if_isclosed_to_s_equals_to_true_21
        if_isclosed_to_s_equals_to_true_21 >> rail.Label(
            'Yes') >> dtna_time_entry_import_logs_add_entry_22 >> finish
        if_timesheet_uri_blank_27 >> rail.Label(
            'Yes') >> get_timesheet_for_date2_28 >> get_timesheet_details_29 >> get_time_entries_for_user_and_date_range_30 \
            >> delete_time_entry_32 >> log_timesheeturi_33
        if_timesheet_uri_blank_27 >> rail.Label(
            'No') >> log_timesheeturi_33 >> get_timesheetapprovalsatus_34 >> if_approvalstatus_displaytext_equals_to_notsubmitted_35
        if_approvalstatus_displaytext_equals_to_notsubmitted_35 >> rail.Label(
            'Yes') >> putnewtimeentries_36 >> dtna_timesheets_to_submit_add_entry_37 >> dtna_time_entry_import_logs_add_entry_42 >> finish
        if_approvalstatus_displaytext_equals_to_notsubmitted_35 >> rail.Label(
            'No') >> reopen_timesheet_39 >> putnewtimeentries_40 >> dtna_timesheets_to_submit_add_entry_41 >> dtna_time_entry_import_logs_add_entry_42
        if_isclosed_to_s_equals_to_true_21 >> rail.Label(
            'No') >> get_timesheet_for_date2_26 >> if_timesheet_uri_blank_27
        if_request_taskuri_present_10 >> rail.Label(
            'No') >> dtna_time_entry_import_logs_add_entry_46 >> finish

        finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
