
from datetime import timedelta, datetime
import json
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'michaelkorstna_uk_user_import_timesheet_recalculation_child_{config.instance}',
        description=f'MichaelKorsTnA_Child timesheet recalculation v1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='invoke_custom_ruby_code_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='invoke_custom_ruby_code_3',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def get_date_object(datestring):
            dateobj = datetime.strptime(datestring, "%d/%m/%Y")
            return {
                'day': dateobj.day,
                'month': dateobj.month,
                'year': dateobj.year
            }

        invoke_custom_ruby_code_3 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_3',
            python_callable=lambda: get_date_object(
                (datetime.now().replace(day=1)).strftime("%d/%m/%Y"))
        )

        invoke_custom_ruby_code_4 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_4',
            python_callable=lambda: get_date_object(
                datetime.now().strftime("%d/%m/%Y"))
        )

        get_timesheet_data_5 = rail.RepliconServiceOperator(
            task_id='get_timesheet_data_5',
            endpoint="/services/TimesheetListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:timesheet-list-column:timesheet"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
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
                    "operatorUri": "urn:replicon:filter-operator:and",
                    "rightExpression": {
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
                                        "year": "{{ result('invoke_custom_ruby_code_3').year }}",
                                        "month": "{{ result('invoke_custom_ruby_code_3').month }}",
                                        "day": "{{ result('invoke_custom_ruby_code_3').day }}"
                                    },
                                    "endDate": {
                                        "year": "{{ result('invoke_custom_ruby_code_4').year }}",
                                        "month": "{{ result('invoke_custom_ruby_code_4').month }}",
                                        "day": "{{ result('invoke_custom_ruby_code_4').day }}"
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
                    "value": null,
                    "filterDefinitionUri": null
                }
            }
        )

        if_timesheet_data_present = rail.IfOperator(
            task_id = 'if_timesheet_data_present',
            test= lambda: bool(rail.result('get_timesheet_data_5') and rail.result('get_timesheet_data_5')['rows'] and rail.result(
                'get_timesheet_data_5')['rows'][0]['cells']),
            yes_task='log_get_all_timesheets_array_6',
            no_task='finish'
        )
        log_get_all_timesheets_array_6 = rail.PythonOperator(
            task_id='log_get_all_timesheets_array_6',
            python_callable=lambda: [json.loads((json.dumps(rail.result('get_timesheet_data_5')[
                                     'rows'][0]['cells'])).replace("[{", "{").replace("}]", "}"))]
        )

        log_get_all_timesheetstoberecalculated_8 = rail.PythonOperator(
            task_id='log_get_all_timesheetstoberecalculated_8',
            python_callable=lambda: (rail.result(
                'log_get_all_timesheets_array_6'))[0]['uri']
        )

        mark_timesheets_as_out_of_date_9 = rail.RepliconServiceOperator(
            task_id='mark_timesheets_as_out_of_date_9',
            endpoint="/services/TimesheetService1.svc/MarkTimesheetsAsOutOfDate",
            data=lambda:{
                "timesheets": [rail.result('log_get_all_timesheetstoberecalculated_8')]
            }
        )

        foreach_document_10 = rail.ForEachOperator(
            task_id='foreach_document_10',
            items=lambda: rail.result(
                'log_get_all_timesheets_array_6'),
            start_task='enqueue_recalculate_script_data_11',
            end_task='foreach_document_10_end'
        )

        enqueue_recalculate_script_data_11 = rail.RepliconServiceOperator(
            task_id='enqueue_recalculate_script_data_11',
            endpoint="/services/TimesheetService1.svc/EnqueueRecalculateScriptData",
            data={
                "timesheet": {
                    "uri": "{{ result('foreach_document_10').uri }}",
                    "user": null,
                    "date": null
                }
            }
        )

        foreach_document_10_end = rail.EmptyOperator(
            task_id='foreach_document_10_end',
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> invoke_custom_ruby_code_3
        invoke_custom_ruby_code_3 >> invoke_custom_ruby_code_4 >> get_timesheet_data_5
        get_timesheet_data_5 >> if_timesheet_data_present
        if_timesheet_data_present >> rail.Label('Yes') >> log_get_all_timesheets_array_6
        log_get_all_timesheets_array_6 >> log_get_all_timesheetstoberecalculated_8 >> mark_timesheets_as_out_of_date_9
        mark_timesheets_as_out_of_date_9 >> foreach_document_10 >> enqueue_recalculate_script_data_11 >> foreach_document_10_end
        foreach_document_10 >> foreach_document_10_end >> finish >> log_to_sumo
        if_timesheet_data_present >> rail.Label('No') >> finish

    return dag


rail.for_each_instance(create_dag)
