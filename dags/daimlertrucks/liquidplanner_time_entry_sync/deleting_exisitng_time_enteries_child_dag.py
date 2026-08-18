
from datetime import timedelta, datetime
import uuid
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'daimlertrucks_deletingexisitngtimeenteries_child_{config.instance}',
        description=f'Live|DTNA_Deleting exisitng time enteries_child {config.instance}',
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

        log_minday_2 = rail.PythonOperator(
            task_id='log_minday_2',
            python_callable=lambda dag_run: int(
                dag_run.conf['mindate'].split("/")[1])
        )

        log_minmonth_3 = rail.PythonOperator(
            task_id='log_minmonth_3',
            python_callable=lambda dag_run: int(
                dag_run.conf['mindate'].split("/")[0])
        )

        log_minyear_4 = rail.PythonOperator(
            task_id='log_minyear_4',
            python_callable=lambda dag_run: int(
                dag_run.conf['mindate'].split("/")[2])
        )

        log_maxday_5 = rail.PythonOperator(
            task_id='log_maxday_5',
            python_callable=lambda dag_run: int(
                dag_run.conf['maxdate'].split("/")[1])
        )

        log_maxmonth_6 = rail.PythonOperator(
            task_id='log_maxmonth_6',
            python_callable=lambda dag_run: int(
                dag_run.conf['maxdate'].split("/")[0])
        )

        log_maxyear_7 = rail.PythonOperator(
            task_id='log_maxyear_7',
            python_callable=lambda dag_run: int(
                dag_run.conf['maxdate'].split("/")[2])
        )

        get_data_existingtimesheets_8 = rail.RepliconServiceOperator(
            task_id='get_data_existingtimesheets_8',
            endpoint="/services/TimesheetListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000",
                "columnUris": [
                    "urn:replicon:timesheet-list-column:timesheet",
                    "urn:replicon:timesheet-list-column:timesheet-owner",
                    "urn:replicon:timesheet-list-column:timesheet-period"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": null,
                            "filterDefinitionUri": "urn:replicon:timesheet-list-filter:timesheet-period-date-range"
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": {
                                "uri": null,
                                "uris": [],
                                "bool": null,
                                "date": null,
                                "money": null,
                                "number": null,
                                "text": null,
                                "time": null,
                                "calendarDayDurationValue": null,
                                "workdayDurationValue": null,
                                "dateRange": {
                                    "startDate": {
                                        "year": "{{result('log_minyear_4')}}",
                                        "month": "{{result('log_minmonth_3')}}",
                                        "day": "{{result('log_minday_2')}}"
                                    },
                                    "endDate": {
                                        "year": "{{result('log_maxyear_7')}}",
                                        "month": "{{result('log_maxmonth_6')}}",
                                        "day": "{{result('log_maxday_5')}}"
                                    },
                                    "relativeDateRangeUri": null,
                                    "relativeDateRangeAsOfDate": null
                                },
                                "dateTimeUtc": null
                            },
                            "filterDefinitionUri": null
                        },
                        "value": null,
                        "filterDefinitionUri": null
                    },
                    "operatorUri": "urn:replicon:filter-operator:and",
                    "rightExpression": {
                        "leftExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": null,
                            "filterDefinitionUri": "urn:replicon:timesheet-list-filter:timesheet-owner"
                        },
                        "operatorUri": "urn:replicon:filter-operator:equal",
                        "rightExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": {
                                "uri": "{{ dag_run.conf.useruri }}",
                                "uris": [],
                                "bool": null,
                                "date": null,
                                "money": null,
                                "number": null,
                                "text": null,
                                "time": null,
                                "calendarDayDurationValue": null,
                                "workdayDurationValue": null,
                                "dateRange": null,
                                "dateTimeUtc": null
                            },
                            "filterDefinitionUri": null
                        },
                        "value": null,
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            }
        )

        foreach_d_9 = rail.ForEachOperator(
            task_id='foreach_d_9',
            items="{{ result('get_data_existingtimesheets_8').rows | to_json }}",
            start_task='accumulate_list_items_10',
            end_task='foreach_d_9_end'
        )

        accumulate_list_items_10 = rail.SetVariableOperator(
            task_id='accumulate_list_items_10',
            name='timesheet_data',
            append=True,
            value=lambda: {
                # pylint: disable=line-too-long
                "timesheeturi": rail.find_first_by_attr_and_get_attr(rail.result('foreach_d_9')['cells'], 'objectType', 'urn:replicon:object-type:timesheet', 'uri'),
                "startdate": rail.find_first_by_attr_and_get_attr(rail.result('foreach_d_9')['cells'], 'dataType', 'urn:replicon:list-type:date-range', 'textValue').split(' - ')[0],
                "enddate": rail.find_first_by_attr_and_get_attr(rail.result('foreach_d_9')['cells'], 'dataType', 'urn:replicon:list-type:date-range', 'textValue').split(' - ')[1],
            }
        )

        foreach_d_9_end = rail.EmptyOperator(
            task_id='foreach_d_9_end',
        )

        def max_min_dates():
            start_date_objects = null
            end_date_objects = null
            if len(rail.result('get_data_existingtimesheets_8')['rows']) > 0:
                start_date_objects = [datetime.strptime(date['startdate'], '%m/%d/%Y')
                                      for date in rail.result('accumulate_list_items_10')['value']]
                end_date_objects = [datetime.strptime(date['enddate'], '%m/%d/%Y')
                                    for date in rail.result('accumulate_list_items_10')['value']]

            return {
                'min_start_date': min(start_date_objects).strftime('%m/%d/%Y') if start_date_objects else null,
                'max_end_date': max(end_date_objects).strftime('%m/%d/%Y') if end_date_objects else null
            }

        get_max_min_date = rail.PythonOperator(
            task_id='get_max_min_date',
            python_callable=max_min_dates
        )

        log_minstartdate_11 = rail.PythonOperator(
            task_id='log_minstartdate_11',
            python_callable=lambda: rail.result('get_max_min_date')[
                'min_start_date']
        )

        log_maxenddate_12 = rail.PythonOperator(
            task_id='log_maxenddate_12',
            python_callable=lambda: rail.result(
                'get_max_min_date')['max_end_date']
        )

        if_log_minstartdate_11_present_13 = rail.IfOperator(
            task_id='if_log_minstartdate_11_present_13',
            test=lambda: rail.result('log_minstartdate_11') is not null and rail.result(
                'log_maxenddate_12') is not null,
            yes_task="log_min_day_14",
            no_task="finish",
        )

        log_min_day_14 = rail.PythonOperator(
            task_id='log_min_day_14',
            python_callable=lambda: int(rail.result(
                'log_minstartdate_11').split('/')[1])
        )

        log_minmonth_15 = rail.PythonOperator(
            task_id='log_minmonth_15',
            python_callable=lambda:  int(rail.result(
                'log_minstartdate_11').split('/')[0])
        )

        log_minyear_16 = rail.PythonOperator(
            task_id='log_minyear_16',
            python_callable=lambda: int(rail.result(
                'log_minstartdate_11').split('/')[2])
        )

        log_max_day_17 = rail.PythonOperator(
            task_id='log_max_day_17',
            python_callable=lambda: int(rail.result(
                'log_maxenddate_12').split('/')[1])
        )

        log_max_month_18 = rail.PythonOperator(
            task_id='log_max_month_18',
            python_callable=lambda: int(rail.result(
                'log_maxenddate_12').split('/')[0])
        )

        log_max_year_19 = rail.PythonOperator(
            task_id='log_max_year_19',
            python_callable=lambda: int(rail.result(
                'log_maxenddate_12').split('/')[2])
        )

        getexistingtimeentries_20 = rail.RepliconServiceOperator(
            task_id='getexistingtimeentries_20',
            endpoint="/services/TimeEntryService3.svc/GetTimeEntriesForUserAndDateRange",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "dateRange": {
                    "startDate": {
                        "year": "{{result('log_minyear_16')}}",
                        "month": "{{result('log_minmonth_15')}}",
                        "day": "{{result('log_min_day_14')}}"
                    },
                    "endDate": {
                        "year": "{{result('log_max_year_19')}}",
                        "month": "{{result('log_max_month_18')}}",
                        "day": "{{result('log_max_day_17')}}"
                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                },
                "asOf": null
            }
        )

        if_response_d_greater_than_0_21 = rail.IfOperator(
            task_id='if_response_d_greater_than_0_21',
            test=lambda: bool(
                len(rail.result('getexistingtimeentries_20')) > 0),
            yes_task="foreach_response_22",
            no_task="finish",
        )

        foreach_response_22 = rail.ForEachOperator(
            task_id='foreach_response_22',
            items="{{ result('getexistingtimeentries_20') | to_json }}",
            start_task='gettimesheetdetails_23',
            end_task='foreach_response_22_end'
        )

        gettimesheetdetails_23 = rail.RepliconServiceOperator(
            task_id='gettimesheetdetails_23',
            endpoint="/services/TimesheetService1.svc/GetTimesheetForDate2",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "date": {
                    "year": "{{result('foreach_response_22').entryDate.year}}",
                    "month": "{{result('foreach_response_22').entryDate.month}}",
                    "day": "{{result('foreach_response_22').entryDate.day}}"
                },
                "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
            }
        )

        get_timesheet_approval_details_24 = rail.RepliconServiceOperator(
            task_id='get_timesheet_approval_details_24',
            endpoint="/services/TimesheetApprovalService1.svc/GetTimesheetApprovalDetails2",
            data={
                "timesheetUri": "{{ result('gettimesheetdetails_23').timesheet.uri }}"
            }
        )

        check_timesheet_tasks_success = rail.IfOperator(
            task_id='check_timesheet_tasks_success',
            test='{{ get_task_state("gettimesheetdetails_23") == "success" and get_task_state("get_timesheet_approval_details_24") == "success"}}',
            yes_task='deletetimeentry_26',
            no_task='gettimesheetdetails_30'
        )

        deletetimeentry_26 = rail.RepliconServiceOperator(
            task_id='deletetimeentry_26',
            endpoint="/services/TimeEntryService3.svc/DeleteTimeEntry",
            data={
                "timeEntryUri": "{{ result('foreach_response_22').uri }}"
            }
        )

        if_approvalstatus_displaytext_equals_to_notsubmitted_27 = rail.IfOperator(
            task_id='if_approvalstatus_displaytext_equals_to_notsubmitted_27',
            test='''{{ result('get_timesheet_approval_details_24').approvalStatus.displayText == 'Not Submitted' }}''',
            yes_task="dtna_timesheets_to_submit_add_entry_28",
            no_task="foreach_response_22_end",
        )

        dtna_timesheets_to_submit_add_entry_28 = rail.WriteLogOperator(
            task_id='dtna_timesheets_to_submit_add_entry_28',
            log="{{ result('create_timesheets_to_submit_log') }}",
            message="na",
            severity="Info",
            properties={
                "timesheeturi": "{{ result('get_timesheet_approval_details_24').timesheet.uri }}",
                "status": "{{ result('get_timesheet_approval_details_24').approvalStatus.displayText }}"
            }
        )

        gettimesheetdetails_30 = rail.RepliconServiceOperator(
            task_id='gettimesheetdetails_30',
            endpoint="/services/TimesheetService1.svc/GetTimesheetForDate2",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "date": {
                    "year": "{{result('foreach_response_22').entryDate.year}}",
                    "month": "{{result('foreach_response_22').entryDate.month}}",
                    "day": "{{result('foreach_response_22').entryDate.day}}"
                },
                "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
            }
        )

        get_timesheet_approval_details_31 = rail.RepliconServiceOperator(
            task_id='get_timesheet_approval_details_31',
            endpoint="/services/TimesheetApprovalService1.svc/GetTimesheetApprovalDetails2",
            data={
                "timesheetUri": "{{ result('gettimesheetdetails_30').timesheet.uri }}"
            }
        )

        dtna_timesheets_to_submit_add_entry_32 = rail.WriteLogOperator(
            task_id='dtna_timesheets_to_submit_add_entry_32',
            log="{{ result('create_timesheets_to_submit_log') }}",
            message="na",
            severity="Info",
            properties={
                "timesheeturi": "{{ result('get_timesheet_approval_details_31').timesheet.uri }}",
                "status": "{{ result('get_timesheet_approval_details_31').approvalStatus.displayText }}"
            }
        )

        reopentimesheet_33 = rail.RepliconServiceOperator(
            task_id='reopentimesheet_33',
            endpoint="/services/TimesheetApprovalService1.svc/Reopen",
            data=lambda: {
                "timesheetUri": rail.result('gettimesheetdetails_30')['timesheet']['uri'],
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Reopening Timesheet to put time entry"
            }
        )

        deletetimeentry_34 = rail.RepliconServiceOperator(
            task_id='deletetimeentry_34',
            endpoint="/services/TimeEntryService3.svc/DeleteTimeEntry",
            data={
                "timeEntryUri": "{{ result('foreach_response_22').uri }}"
            }
        )

        foreach_response_22_end = rail.EmptyOperator(
            task_id='foreach_response_22_end',
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

        create_timesheets_to_submit_log >> log_minday_2 >> log_minmonth_3 >> log_minyear_4 >> log_maxday_5 \
            >> log_maxmonth_6 >> log_maxyear_7 >> get_data_existingtimesheets_8 \
            >> foreach_d_9 >> accumulate_list_items_10 >> foreach_d_9_end
        foreach_d_9 >> foreach_d_9_end >> get_max_min_date >> log_minstartdate_11 >> log_maxenddate_12 >> if_log_minstartdate_11_present_13
        if_log_minstartdate_11_present_13 >> rail.Label(
            'Yes') >> log_min_day_14 >> log_minmonth_15 >> log_minyear_16 >> log_max_day_17 >> log_max_month_18 \
            >> log_max_year_19 >> getexistingtimeentries_20 >> if_response_d_greater_than_0_21
        if_response_d_greater_than_0_21 >> rail.Label(
            'Yes') >> foreach_response_22 >> gettimesheetdetails_23 >> get_timesheet_approval_details_24 >> check_timesheet_tasks_success

        check_timesheet_tasks_success >> rail.Label(
            'Yes') >> deletetimeentry_26 >> if_approvalstatus_displaytext_equals_to_notsubmitted_27
        check_timesheet_tasks_success >> rail.Label(
            'No') >> gettimesheetdetails_30 >> get_timesheet_approval_details_31 \
            >> dtna_timesheets_to_submit_add_entry_32 >> reopentimesheet_33 >> deletetimeentry_34 >> foreach_response_22_end

        if_approvalstatus_displaytext_equals_to_notsubmitted_27 >> rail.Label(
            'Yes') >> dtna_timesheets_to_submit_add_entry_28 >> foreach_response_22_end
        if_approvalstatus_displaytext_equals_to_notsubmitted_27 >> rail.Label(
            'No') >> foreach_response_22_end
        foreach_response_22 >> foreach_response_22_end >> finish

        if_response_d_greater_than_0_21 >> rail.Label(
            'No') >> finish
        if_log_minstartdate_11_present_13 >> rail.Label('No') >> finish

        finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
