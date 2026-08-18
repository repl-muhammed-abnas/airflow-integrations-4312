from datetime import timedelta, datetime
import rail
from horizonmedia.timesheet_autosubmission_v1.util import data_formatting, request_payload
from horizonmedia.timesheet_autosubmission_v1.send_logs import get_send_logs
from pendulum import datetime as dt

def create_dag(config):
    # pylint: disable=too-many-statements,unnecessary-lambda
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'Horizonmedia_AutoTimesheetSubmit_Master_V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=dt(2022, 1, 1, tz=config.time_zone),
        schedule_interval=config.master_dag_schedule_interval,
        max_active_runs=config.max_active_runs
    ) as dag:

        start = rail.EmptyOperator(task_id='start')
        finish = rail.EmptyOperator(task_id='finish')

        get_tsauto_submission_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_tsauto_submission_report_details',
            report_name=config.base_report_name,
        )

        report_group_entry, report_group_exit = rail.run_report(
            group_id='get_report_details',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_tsauto_submission_report_details').uri }}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        is_report_failed = rail.IfOperator(
            task_id="is_report_failed",
            test='{{result("get_report_details.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_report_generation",
            no_task="report_has_data"
        )

        fail_report_generation = rail.FailOperator(
            task_id="fail_report_generation",
            message="{{result('get_report_details.get_report_result').reportGenerationResults[0].error}}"
        )

        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test="{{ result('get_report_details.get_report_result', 'has_data') }}",
            yes_task='get_tsauto_submission_report_has_expected_columns',
            no_task='finish',
        )

        # pylint: disable=line-too-long
        get_tsauto_submission_report_columns = 'Timesheet Period,User Name,First Day of Leave,Actual last day of Leave,Approval Status,TimesheetPeriodUri,Worker Status,Timesheet End Date,Total Hours (In Period),Duedate,DayDiff,Time Off Hrs,Time Off Type,Scheduled Hrs (In Period)'

        get_tsauto_submission_report_has_expected_columns = rail.IfOperator(
            task_id="get_tsauto_submission_report_has_expected_columns",
            #pylint: disable=consider-using-f-string
            test="{{ result('get_report_details.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % get_tsauto_submission_report_columns,
            no_task='fail_invalid_get_tsauto_submission_report_columns',
            yes_task='get_tsauto_submission_report_payload_to_csv',
        )

        fail_invalid_get_tsauto_submission_report_columns = rail.FailOperator(
            task_id="fail_invalid_get_tsauto_submission_report_columns",
            message="Incorrect columns"
        )

        get_tsauto_submission_report_payload_to_csv = rail.LoadCSVFileOperator(
            task_id="get_tsauto_submission_report_payload_to_csv",
            document='{{result("get_report_details.get_report_result").reportGenerationResults[0].payload}}'
        )

        write_csv_with_date_in_comparable_format = rail.WriteCSVFileOperator(
            task_id = 'write_csv_with_date_in_comparable_format',
            source=lambda: rail.result('get_tsauto_submission_report_payload_to_csv'),
            header=[
                "Timesheet Period",
                "User Name",
                "First Day of Leave",
                "Actual last day of Leave",
                "Approval Status",
                "TimesheetPeriodUri",
                "Worker Status",
                "Timesheet End Date",
                "Total Hours (In Period)",
                # "validation message",
                "Duedate",
                "DayDiff",
                "Time Off Hrs",
                "Scheduled Hrs (In Period)"
            ],
            row=lambda item:[
                item["Timesheet Period"],
                item["User Name"],
                datetime.strptime(item["First Day of Leave"],"%b %d, %Y").strftime('%Y-%m-%d') if item["First Day of Leave"] else '',
                datetime.strptime(item["Actual last day of Leave"],"%b %d, %Y").strftime('%Y-%m-%d') if item["Actual last day of Leave"] else '',
                item["Approval Status"],
                item["TimesheetPeriodUri"],
                item["Worker Status"],
                item["Timesheet End Date"],
                item["Total Hours (In Period)"],
                # item["validation message"],
                datetime.strptime(item["Duedate"],"%b %d, %Y").strftime('%Y-%m-%d') if item["Duedate"] else '',
                item["DayDiff"],
                item["Time Off Hrs"],
                item["Scheduled Hrs (In Period)"]
            ]

        )

        get_tsauto_submission_report_data_collection = rail.CreateCollectionOperator(
            task_id="get_tsauto_submission_report_data_collection",
            name='timesheet_report',
            source='{{result("write_csv_with_date_in_comparable_format")}}',
            columns={
                "Timesheet Period":"Timesheet_Period",
                "User Name":"User_Name",
                "First Day of Leave":"First_Day_of_Leave",
                "Actual last day of Leave":"Actual_last_day_of_Leave",
                "Approval Status":"Approval Status",
                "TimesheetPeriodUri":"TimesheetPeriodUri",
                "Worker Status":"Worker Status",
                "Timesheet End Date":"Timesheet_End_Date",
                "Total Hours (In Period)":"Total_Hrs_In_Period",
                # "validation message":"validation_message",
                "Duedate":"Duedate",
                "DayDiff":"DayDiff",
                "Time Off Hrs":"Time_Off_Hrs",
                "Scheduled Hrs (In Period)":"Scheduled_Hrs_In_Period"
            }
        )

        query_valid_ts = rail.QueryCollectionOperator(
            task_id='query_valid_ts',
            query='SELECT * FROM "timesheet_report" WHERE NULLIF("First_Day_of_Leave","") is NOT NULL AND "DayDiff" < 0 AND ("Duedate" > "First_Day_of_Leave" OR "Duedate"="First_Day_of_Leave") AND ("Time_Off_Hrs" >= "Scheduled_Hrs_In_Period")'
        )

        get_valid_ts_query_data= rail.PythonOperator(
            task_id= 'get_valid_ts_query_data',
            python_callable= lambda: rail.load_all_records(rail.result("query_valid_ts"))
        )

        query_has_data = rail.IfOperator(
            task_id='query_has_data',
            test='{{ result("get_valid_ts_query_data") | length > 0 }}',
            yes_task='recalculate_timesheets',
            no_task='no_timesheet_fail_email',
        )

        no_timesheet_fail_email = rail.EmailOperator(
            task_id='no_timesheet_fail_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Timesheet autosubmission  - No Timesheet found - {{ current_time_in_specified_tz() }}',
            html_content='no_timesheet_fail.html',
        )

        recalculate_timesheets = rail.RepliconServiceOperator(
            task_id='recalculate_timesheets',
            endpoint='/services/TimesheetService1.svc/CreateRecalculateScriptDataBatch2',
            data=lambda: request_payload.get_recalculate_timesheet_payload()
        )

        (process_timedata_batch, wait_for_timedata_batch) = rail.batch_execution(
                group_id='process_timedata_batch',
                creation_task_id=recalculate_timesheets.task_id
        )

        recalculate_successful = rail.IfOperator(
            task_id="recalculate_successful",
            #pylint: disable=consider-using-f-string
            test="{{ result('process_timedata_batch.wait_for_batch')['executionState'] == 'urn:replicon-service-model:batch-execution-state:succeeded' }}",
            no_task='finish',
            yes_task='get_most_recent_validation_result',
        )

        get_most_recent_validation_result = rail.RepliconServiceOperator(
            task_id='get_most_recent_validation_result',
            endpoint='/services/TimesheetService1.svc/BulkGetMostRecentValidationResults',
            data=lambda: request_payload.get_validation_msg_timesheet_payload()
        )

        process_validation_msg = rail.PythonOperator(
            task_id="process_validation_msg",
            python_callable = data_formatting.process_validation_msg_format,
            op_args=["{{ result('get_most_recent_validation_result') | tojson }}", "{{ result('get_valid_ts_query_data') | tojson }}"]
        )

        has_valid_ts_for_submission = rail.IfOperator(
            task_id="has_valid_ts_for_submission",
            #pylint: disable=consider-using-f-string
            test="{{ result('process_validation_msg')['latestvalidattionresults'] | length > 0 }}",
            no_task='has_logs_for_ts',
            yes_task='submit_ts',
        )

        submit_ts = rail.TriggerDagRunForEachItemOperator(
            task_id='submit_ts',
            retries=0,
            items="{{ result('process_validation_msg')['latestvalidattionresults'] | to_json }}",
            trigger_dag_id=config.submit_timesheet_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                'timesheet_uris': '{{ item.timesheeturi }}',
                'username': '{{ item.username }}',
                'timesheetperiod': '{{ item.timesheetperiod }}',
                'callerjobid': "{{dag_run_ecid()}}"
            }
        )

        wait_for_process_submit_ts = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_submit_ts',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("submit_ts") }}',
        )

        send_logs_enter, _ = get_send_logs(config)

        has_logs_for_ts = rail.IfOperator(
            task_id="has_logs_for_ts",
            #pylint: disable=consider-using-f-string
            test="{{ result('process_validation_msg')['forlogging'] | length > 0 }}",
            no_task='finish',
            yes_task='invalid_ts_log',
        )

        invalid_ts_log = rail.WriteLogOperator(
            task_id = "invalid_ts_log",
            severity="Exception",
            items="{{ result('process_validation_msg')['forlogging'] | to_json }} ",
            message="Timesheet not submitted due to Validate error. Message: ",
            properties=lambda item: {
                'username':item['username'],
                'timesheetperiod':item['timesheetperiod'],
                'parentjobid': rail.render_template("{{dag_run_ecid()}}"),
                'jobid': '',
                'status': 'Exception',
                'details': "Timesheet not submitted due to Validate error. Message: " + item['validationmessage']
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
        )

        start >> get_tsauto_submission_report_details >> report_group_entry >> report_group_exit >> is_report_failed
        is_report_failed >> rail.Label("Yes") >> fail_report_generation >> finish

        is_report_failed >> rail.Label("No") >> report_has_data
        report_has_data >> rail.Label("No") >> finish >> catch_and_log_errors

        report_has_data >> rail.Label("Yes") >> get_tsauto_submission_report_has_expected_columns
        get_tsauto_submission_report_has_expected_columns >> rail.Label("No") >> fail_invalid_get_tsauto_submission_report_columns >> finish >> catch_and_log_errors

        get_tsauto_submission_report_has_expected_columns >> rail.Label("Yes") >> get_tsauto_submission_report_payload_to_csv \
        >> write_csv_with_date_in_comparable_format >> get_tsauto_submission_report_data_collection >> query_valid_ts >> get_valid_ts_query_data >> query_has_data
        query_has_data >> rail.Label("No") >> no_timesheet_fail_email >> finish >> catch_and_log_errors

        query_has_data >> rail.Label("Yes") >> recalculate_timesheets >> process_timedata_batch >> wait_for_timedata_batch >> recalculate_successful
        recalculate_successful >> rail.Label("No") >> finish >> catch_and_log_errors

        recalculate_successful >> rail.Label("Yes") >> get_most_recent_validation_result >> process_validation_msg >> has_valid_ts_for_submission
        has_valid_ts_for_submission >> rail.Label("No") >> has_logs_for_ts

        has_valid_ts_for_submission >> rail.Label("Yes") >> submit_ts >> wait_for_process_submit_ts >> has_logs_for_ts

        has_logs_for_ts >> rail.Label("No") >> finish >> send_logs_enter >> catch_and_log_errors

        has_logs_for_ts >> invalid_ts_log >> rail.Label("Yes") >> finish >> send_logs_enter >> catch_and_log_errors

        return dag

rail.for_each_instance(create_dag)
