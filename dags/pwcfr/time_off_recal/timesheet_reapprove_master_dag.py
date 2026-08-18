from datetime import timedelta, datetime
from airflow.models import Variable
import rail
import pytz

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'pwcfr_timesheet_reapprove_master_{config.instance}',
        description=f'Pwcfr_timesheet_reapprove_master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_child, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='log_timenow'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_timenow',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def get_todaydate():
            today = datetime.now(pytz.timezone('Europe/Paris'))
            formatted_date = "{ Day: " + str(today.day) + ", Month: " + str(
                today.month) + ", Year: " + str(today.year) + " }"
            return formatted_date

        log_timenow = rail.PythonOperator(
            task_id='log_timenow',
            python_callable=get_todaydate
        )

        get_all_reports = rail.RepliconServiceOperator(
            task_id='get_all_reports',
            endpoint="/services/reportService1.svc/GetAllReports",
            data=None
        )

        get_reopen_timesheet_report = rail.PythonOperator(
            task_id='get_reopen_timesheet_report',
            python_callable=lambda: rail.smartjoin_by_delim(rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_reports'), 'displayText', '**RIT - Reopen Timesheet report', 'uri', null), "")
        )

        get_open_timesheet_report = rail.PythonOperator(
            task_id='get_open_timesheet_report',
            python_callable=lambda: rail.smartjoin_by_delim(rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_reports'), 'displayText', '***RIT - Open Timesheet report', 'uri', null), "")
        )

        if_reopen_timesheet_not_present = rail.IfOperator(
            task_id='if_reopen_timesheet_not_present',
            test="{{ result('get_reopen_timesheet_report') | is_falsy }}",
            yes_task="stop_job_with_error_message",
            no_task="get_reopen_timesheet_reporturi",
        )

        stop_job_with_error_message = rail.FailOperator(
            task_id='stop_job_with_error_message',
            message='**RIT - Reopen Timesheet report not found'
        )

        get_reopen_timesheet_reporturi = rail.RepliconServiceOperator(
            task_id='get_reopen_timesheet_reporturi',
            endpoint="/services/reportService1.svc/GetReportDetails2",
            data={
                "reportUri": "{{result('get_reopen_timesheet_report')}}"
            }
        )

        get_open_timesheet_reporturi = rail.RepliconServiceOperator(
            task_id='get_open_timesheet_reporturi',
            endpoint="/services/reportService1.svc/GetReportDetails2",
            data={
                "reportUri": "{{result('get_open_timesheet_report')}}"
            }
        )

        get_actionfilter_uri = rail.PythonOperator(
            task_id='get_actionfilter_uri',
            python_callable=lambda: rail.smartjoin_by_delim(rail.find_first_by_attr_and_get_attr(
                rail.result('get_reopen_timesheet_reporturi')['filterConfiguration']['enabledFilters'], 'displayText', 'ActionFilter', 'uri', null), "")
        )

        get_modified_daterange_filter = rail.PythonOperator(
            task_id='get_modified_daterange_filter',
            python_callable=lambda: rail.smartjoin_by_delim(rail.find_first_by_attr_and_get_attr(
                rail.result('get_reopen_timesheet_reporturi')['filterConfiguration']['enabledFilters'], 'displayText', 'ModifiedOnUtcDateRangeFilter', 'uri', null), "")
        )

        get_modified_filter = rail.PythonOperator(
            task_id='get_modified_filter',
            python_callable=lambda: rail.smartjoin_by_delim(rail.find_first_by_attr_and_get_attr(
                rail.result('get_reopen_timesheet_reporturi')['filterConfiguration']['enabledFilters'], 'displayText', 'ModifiedByFilter', 'uri', null), "")
        )

        generate_report = rail.run_report2(
            group_id='generate_report_data',
            report_params={
                "reportParameters": [
                    {
                        "filterValues": [{
                            "reportFilterUri": "{{result('get_actionfilter_uri')}}",
                            "value": "4"
                        },
                            {
                            "reportFilterUri": "{{result('get_modified_daterange_filter')}}",
                            "value": "Today"
                        },
                            {
                            "reportFilterUri": "{{result('get_modified_daterange_filter')}}",
                            "value": "null"
                        },
                            {
                            "reportFilterUri": "{{result('get_modified_daterange_filter')}}",
                            "value": "null"
                        },
                            {
                            "reportFilterUri": "{{result('get_modified_filter')}}",
                            "value": "2"
                        }],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv",
                        "reportUri": "{{result('get_reopen_timesheet_report')}}"
                    }
                ]
            },
            target='artifact',
        )

        if_payload_contains_error = rail.IfOperator(
            task_id='if_payload_contains_error',
            test="{{ (result('generate_report_data.get_report_result')| load_json_artifact).reportGenerationResults[0].error | is_truthy }}",
            yes_task="stop_job_with_error",
            no_task="if_payload_has_data",
        )

        stop_job_with_error = rail.FailOperator(
            task_id='stop_job_with_error',
            message="{{(result('generate_report_data.get_report_result')| load_json_artifact).reportGenerationResults[0].error}}"
        )

        if_payload_has_data = rail.IfOperator(
            task_id='if_payload_has_data',
            test='{{not (result("generate_report_data.get_report_result")| load_json_artifact).reportGenerationResults[0].payload | matches("No Data")}}',
            yes_task="get_entrydate_filter",
            no_task="stop_job"
        )

        stop_job = rail.EmptyOperator(
            task_id='stop_job',
        )

        get_entrydate_filter = rail.PythonOperator(
            task_id='get_entrydate_filter',
            python_callable=lambda: rail.smartjoin_by_delim(rail.find_first_by_attr_and_get_attr(
                rail.result('get_open_timesheet_reporturi')['filterConfiguration']['enabledFilters'], 'displayText', 'EntryDateFilter', 'uri', null), "")
        )

        get_approvalstatus_filter = rail.PythonOperator(
            task_id='get_approvalstatus_filter',
            python_callable=lambda: rail.smartjoin_by_delim(rail.find_first_by_attr_and_get_attr(
                rail.result('get_open_timesheet_reporturi')['filterConfiguration']['enabledFilters'], 'displayText', 'ApprovalStatusFilter', 'uri', null), "")
        )

        generate_report_details = rail.run_report2(
            group_id='run_report_data',
            report_params={
                "reportParameters": [
                    {
                        "filterValues": [{
                            "reportFilterUri": "{{result('get_approvalstatus_filter')}}",
                            "value": "0"
                        },
                            {
                            "reportFilterUri": "{{result('get_entrydate_filter')}}",
                            "value": "null"
                        },
                            {
                            "reportFilterUri": "{{result('get_entrydate_filter')}}",
                            "value": "{{dag_run.conf.start}}"
                        },
                            {
                            "reportFilterUri": "{{result('get_entrydate_filter')}}",
                            "value": "{{dag_run.conf.end}}"
                        }],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv",
                        "reportUri": "{{result('get_open_timesheet_report')}}"
                    }
                ]
            },
            target='artifact',
        )

        if_report_payload_contains_error = rail.IfOperator(
            task_id='if_report_payload_contains_error',
            test="{{ (result('run_report_data.get_report_result')| load_json_artifact).reportGenerationResults[0].error | is_truthy }}",
            yes_task="stop_job_with_failure_message",
            no_task="if_report_payload_has_data",
        )

        stop_job_with_failure_message = rail.FailOperator(
            task_id='stop_job_with_failure_message',
            message="{{(result('run_report_data.get_report_result')| load_json_artifact).reportGenerationResults[0].error}}"
        )

        if_report_payload_has_data = rail.IfOperator(
            task_id='if_report_payload_has_data',
            test='{{not (result("run_report_data.get_report_result")| load_json_artifact).reportGenerationResults[0].payload | matches("No Data")}}',
            yes_task="parse_report_csv",
            no_task="finish_job"
        )

        finish_job = rail.EmptyOperator(
            task_id='finish_job',
        )

        parse_report_csv = rail.LoadCSVFileOperator(
            task_id='parse_report_csv',
            document="{{ (result('generate_report_data.get_report_result')| load_json_artifact).reportGenerationResults[0].payload }}",
        )

        create_timesheet_report_list = rail.CreateCollectionOperator(
            task_id='create_timesheet_report_list',
            source='{{ result("parse_report_csv")}}',
            name="reopen_timesheet_report",
            columns={
                "timesheeturi": "timesheeturi",
                "Login Name": "loginname",
                "Modification Summary": "modificationsummary"

            }
        )

        query_list_entries_for_system_reopened = rail.QueryCollectionOperator(
            task_id='query_list_entries_for_system_reopened',
            name='rit_reopen_timesheet_report_final',
            query="""SELECT * FROM reopen_timesheet_report WHERE reopen_timesheet_report.modificationsummary = '"System reopened because overlapping booking was modified"' """
        )

        if_query_list_entries_for_system_reopened_has_data = rail.IfOperator(
            task_id='if_query_list_entries_for_system_reopened_has_data',
            test="{{result('query_list_entries_for_system_reopened', 'length') > 0 }}",
            yes_task='parse_timesheet_data',
            no_task='finish'
        )

        parse_timesheet_data = rail.LoadCSVFileOperator(
            task_id='parse_timesheet_data',
            document="{{ (result('run_report_data.get_report_result')| load_json_artifact).reportGenerationResults[0].payload }}",
            headers=['Login Name', 'timesheeturi', 'Approval Status']
        )

        create_open_timesheet_list = rail.CreateCollectionOperator(
            task_id='create_open_timesheet_list',
            source='{{ result("parse_timesheet_data")}}',
            name="open_timesheet_report",
            columns={
                "Login Name": "loginname",
                "timesheeturi": "timesheeturi",
                "Approval Status": "timesheetstatus"

            }
        )

        query_list_for_timesheeturi = rail.QueryCollectionOperator(
            task_id='query_list_for_timesheeturi',
            query="""SELECT * FROM rit_reopen_timesheet_report_final WHERE rit_reopen_timesheet_report_final.timesheeturi IN (SELECT DISTINCT open_timesheet_report.timesheeturi FROM open_timesheet_report)""",
        )

        pwctest_timesheet_approval_lookuptable = rail.CreateLogOperator(
            task_id='pwctest_timesheet_approval_lookuptable'
        )

        if_query_list_for_timesheeturi_has_data_present = rail.IfOperator(
            task_id='if_query_list_for_timesheeturi_has_data_present',
            test="{{result('query_list_for_timesheeturi', 'length') > 0 }}",
            yes_task='process_force_approve_timesheet_child',
            no_task='finish'
        )

        process_force_approve_timesheet_child = rail.TriggerDagRunForEachItemOperator(
            task_id='process_force_approve_timesheet_child',
            retries=0,
            items="{{result('query_list_for_timesheeturi')}}",
            trigger_dag_id=f'pwcfr_force_approve_timesheet_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "timesheeturi_list": item,
                "pwctest_lookup_table": rail.result('pwctest_timesheet_approval_lookuptable')
            }
        )

        wait_for_process_force_approve_timesheet_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_force_approve_timesheet_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_force_approve_timesheet_child") }}'
        )

        search_entries_in_pwctest_lookup_table = rail.FilterLogEntriesOperator(
            task_id='search_entries_in_pwctest_lookup_table',
            log="{{result('pwctest_timesheet_approval_lookuptable')}}",
            properties={
                'jobid': "{{ dag_run_ecid() }}",
            }
        )

        create_csv = rail.WriteCSVFileOperator(
            task_id='create_csv',
            source=lambda: rail.result(
                'pwctest_timesheet_approval_lookuptable'),
            delimiter=',',
            header=['loginname',
                    'timesheeturi',
                    'status',
                    'details',
                    'jobid'],
            row=lambda item: [
                item['properties']['loginname'],
                item['properties']['timesheeturi'],
                item['properties']['status'],
                item['properties']['details'],
                item['properties']['jobid']

            ]
        )

        log_start_date = rail.PythonOperator(
            task_id='log_start_date',
            python_callable=lambda: datetime.now(
                pytz.timezone("Europe/Paris")).strftime("%Y-%m-%eT%H:%M%S.%f")
        )

        upload_file_to_s3 = rail.S3UploadFileOperator(
            task_id='upload_file_to_s3',
            aws_conn_id=config.aws_conn_id,
            source="{{ result('create_csv') }}",
            bucket_name=lambda: Variable.get(config.bucket_name),
            key_name=lambda: config.new_file_path + '_' +
            rail.result("log_start_date") + '.csv'
        )

        get_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='get_download_link',
            artifact_name="{{ result('create_csv')}}",
            output_file_name='timesheetreapprove_logs_ecid.csv',
            expires_in_seconds=7*24*60*60,
        )

        get_error_logs = rail.FilterLogEntriesOperator(
            task_id='get_error_logs',
            log="{{result('pwctest_timesheet_approval_lookuptable')}}",
            severity='Error'
        )

        send_import_completion_mail = rail.EmailOperator(
            task_id='send_import_completion_mail',
            to=config.internal_logs_email,
            bcc="{%- if result('get_error_logs', 'length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() }} |  Re-approve Timesheet Post Time Off import - {{" "}} \
                {%- if result("get_error_logs", key="length") > 0 -%} \
                    Completed with error   \
                {%- else -%} \
                    Completed Successfully  \
                {%- endif -%} \
                    {{result("log_start_date")}}',
            html_content="/templates/emails/timesheet_update_completion_mail.html",
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> log_timenow
        log_timenow >> get_all_reports >> get_reopen_timesheet_report >> get_open_timesheet_report
        get_open_timesheet_report >> if_reopen_timesheet_not_present >> rail.Label(
            'Yes') >> stop_job_with_error_message >> finish
        if_reopen_timesheet_not_present >> rail.Label(
            'No') >> get_reopen_timesheet_reporturi >> get_open_timesheet_reporturi >> get_actionfilter_uri
        get_actionfilter_uri >> get_modified_daterange_filter >> get_modified_filter >> generate_report
        generate_report >> if_payload_contains_error
        if_payload_contains_error >> rail.Label(
            'Yes') >> stop_job_with_error >> finish
        if_payload_contains_error >> rail.Label('No') >> if_payload_has_data
        if_payload_has_data >> rail.Label('Yes') >> get_entrydate_filter
        if_payload_has_data >> rail.Label(
            'No') >> stop_job >> get_entrydate_filter >> get_approvalstatus_filter
        get_approvalstatus_filter >> generate_report_details >> if_report_payload_contains_error
        if_report_payload_contains_error >> rail.Label(
            'Yes') >> stop_job_with_failure_message >> finish
        if_report_payload_contains_error >> rail.Label(
            'No') >> if_report_payload_has_data
        if_report_payload_has_data >> rail.Label(
            'Yes') >> parse_report_csv >> create_timesheet_report_list >> query_list_entries_for_system_reopened
        query_list_entries_for_system_reopened >> if_query_list_entries_for_system_reopened_has_data
        if_query_list_entries_for_system_reopened_has_data >> rail.Label(
            'Yes') >> parse_timesheet_data >> create_open_timesheet_list
        create_open_timesheet_list >> query_list_for_timesheeturi >> pwctest_timesheet_approval_lookuptable
        pwctest_timesheet_approval_lookuptable >> if_query_list_for_timesheeturi_has_data_present
        if_query_list_for_timesheeturi_has_data_present >> rail.Label(
            'Yes') >> process_force_approve_timesheet_child >> wait_for_process_force_approve_timesheet_child
        wait_for_process_force_approve_timesheet_child >> search_entries_in_pwctest_lookup_table
        search_entries_in_pwctest_lookup_table >> create_csv >> log_start_date >> upload_file_to_s3
        upload_file_to_s3 >> get_download_link >> get_error_logs >> send_import_completion_mail >> finish
        finish >> log_to_sumo
        if_query_list_for_timesheeturi_has_data_present >> rail.Label(
            'No') >> finish
        if_query_list_entries_for_system_reopened_has_data >> rail.Label(
            'No') >> finish
        if_report_payload_has_data >> rail.Label(
            'No') >> finish_job >> finish

        return dag


rail.for_each_instance(create_dag)
