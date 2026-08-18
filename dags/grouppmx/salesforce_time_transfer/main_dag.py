from pendulum import datetime
from datetime import timedelta, datetime as dt
import rail
from grouppmx.salesforce_time_transfer.utils import custom_methods

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id= config.master_dag_id,
        description=f'Grouppmx Time Transfer To Salesforce Master {config.instance}',
        company_key=config.company_key,
        schedule_interval= config.schedule_interval,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 1, 1, tz= config.timezone),
        max_active_runs=config.master_dag_max_active_runs
    ) as dag:

        get_time_entry_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_time_entry_report_details",
            report_name=config.time_entry_report_name
        )

        generate_time_entry_report = rail.run_report2(
            group_id="generate_base_report",
            report_params=lambda: {
                "reportParameters": [
                    {
                        "reportUri":  rail.result('get_time_entry_report_details')['uri'],
                        "filterValues": custom_methods.get_filter_payload(),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id
        )

        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test=lambda: rail.result(
                "generate_base_report.get_report_result", "has_data"),
            yes_task='report_has_expected_columns',
            no_task="send_no_data_email"
        )

        send_no_data_email = rail.EmailOperator(
            task_id='send_no_data_email',
            to=config.tenant_email,
            # pylint: disable=line-too-long
            subject="{{ get_company_key() }} | Replicon to Salesforce Time Transfer - No Data to process - {{ current_time('%d%m%Y%H%M%S') }}",
            html_content='templates/email/blank_payload.html'
        )

        report_has_expected_columns = rail.IfOperator(
            task_id="report_has_expected_columns",
            #pylint: disable=consider-using-f-string line-too-long
            test="{{ result('generate_base_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % config.expected_time_entry_report_columns,
            yes_task="load_report_data",
            no_task="fail_invalid_report_columns"
        )

        fail_invalid_report_columns = rail.FailOperator(
            task_id="fail_invalid_report_columns",
            message="Base report column does not match"
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('generate_base_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_report_collection = rail.CreateCollectionOperator(
            task_id="create_report_collection",
            source="{{ result('load_report_data') }}",
            name="timeentryreportcollection"
        )

        create_log = rail.CreateLogOperator(
            task_id = 'create_log'
        )

        query_uniq_clients = rail.QueryCollectionOperator(
            task_id = 'query_uniq_clients',
            name= 'uniqclients',
            query= '''SELECT DISTINCT ClientUri FROM timeentryreportcollection WHERE NULLIF(ClientUri, "") IS NOT NULL'''
        )

        process_each_client = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_client',
            trigger_dag_id= config.client_dag_id,
            retries=0,
            items= "{{ result('query_uniq_clients') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                'client_uri': '{{ item.ClientUri }}',
                'log': '{{ result("create_log") }}'
            }
        )

        wait_for_process_client = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_client',
            dag_runs='{{ result("process_each_client") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        gather_client_details = rail.GatherResultsFromDagRunsOperator(
            task_id = 'gather_client_details',
            dag_runs= '{{ result("process_each_client") }}',
            dagrun_task_id= 'log_account_details',
            flatten= True
        )

        query_uniq_projects = rail.QueryCollectionOperator(
            task_id = 'query_uniq_projects',
            name= 'uniqprojects',
            query= '''SELECT DISTINCT ProjectUri FROM timeentryreportcollection WHERE NULLIF(ProjectUri, "") IS NOT NULL'''
        )

        process_each_project = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_project',
            trigger_dag_id= config.project_dag_id,
            retries=0,
            items= "{{ result('query_uniq_projects') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                'project_uri': '{{ item.ProjectUri }}',
                'log': '{{ result("create_log") }}'
            }
        )

        wait_for_process_projects = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_projects',
            dag_runs='{{ result("process_each_project") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        gather_project_details = rail.GatherResultsFromDagRunsOperator(
            task_id = 'gather_project_details',
            dag_runs= '{{ result("process_each_project") }}',
            dagrun_task_id= 'log_project_details',
            flatten= True
        )

        query_uniq_timesheets = rail.QueryCollectionOperator(
            task_id = 'query_uniq_timesheets',
            name= 'uniqtimesheets',
            query= '''SELECT DISTINCT TimesheetPeriodUri FROM timeentryreportcollection WHERE NULLIF(TimesheetPeriodUri, "") IS NOT NULL'''
        )

        load_report_artifact_data = rail.PythonOperator(
            task_id = 'load_report_artifact_data',
            python_callable= lambda: rail.load_all_records(rail.result("create_report_collection"))
        )

        def filter_report_data(artifact,timesheeturi):
            return list(filter(lambda x: x['TimesheetPeriodUri'] == timesheeturi,artifact))

        process_each_timesheet = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_timesheet',
            trigger_dag_id= config.timesheet_dag_id,
            retries=0,
            items= "{{ result('query_uniq_timesheets') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'timesheet_uri': item['TimesheetPeriodUri'],
                'report_data': filter_report_data(rail.result('load_report_artifact_data'),item['TimesheetPeriodUri']),
                'accounts_data': rail.result('gather_client_details'),
                'projects_data': rail.result('gather_project_details'),
                'log': rail.result("create_log")
            }
        )

        wait_for_process_timesheets = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_timesheets',
            dag_runs='{{ result("process_each_timesheet") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        format_logs = rail.PythonOperator(
            task_id="format_logs",
            python_callable= custom_methods.do_format_logs,
            show_return_value_in_logs=False,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result("format_logs"),
            header=[
                'JobID',
                'Contact',
                'Project',
                'Account',
                'Entry Date',
                'Hours Worked',
                'Time Off Hours',
                'Status',
                'Details'],
            row=[
                '{{ item.jobid }}',
                '{{ item.contact }}',
                '{{ item.project }}',
                '{{ item.account }}',
                '{{ item.entrydate }}',
                '{{ item.hoursworked }}',
                '{{ item.timeoffhours }}',
                '{{ item.status }}',
                '{{ item.details }}'
            ],
        )

        get_log_file_name = rail.PythonOperator(
            task_id = 'get_log_file_name',
            python_callable= lambda: "TimeTransfer_Logs_" + dt.now().strftime("%m-%d-%Y-%H-%M-%S") + '- PT.csv'
        )

        generate_downloadable_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_downloadable_link",
            artifact_name="{{result('render_logs_csv')}}",
            output_file_name='{{ result("get_log_file_name") }}',
            expires_in_seconds=30*24*60*60
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            '/'+"{{result('get_log_file_name')}}",
            sftp_conn_id = config.sftp_conn_id
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', key='error_record_count') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='''{{ get_company_key() }} |  Replicon Time Entry Sync - \
                {%- if result("format_logs", key="error_record_count") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("format_logs", key="exception_record_count") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " " + current_time() }}''',
            html_content="templates/email/email_import_complete.html"
        )

        get_time_entry_report_details >> generate_time_entry_report >> report_has_data

        report_has_data >> rail.Label(
            "Yes") >> report_has_expected_columns

        report_has_data >> rail.Label(
            "No") >> send_no_data_email

        report_has_expected_columns >> rail.Label(
            "No") >> fail_invalid_report_columns

        report_has_expected_columns >> rail.Label(
            "Yes") >> load_report_data >> create_report_collection >> create_log >> query_uniq_clients >> process_each_client >> \
            wait_for_process_client >> gather_client_details >> query_uniq_projects >> process_each_project >> wait_for_process_projects >> \
                gather_project_details >> query_uniq_timesheets >> load_report_artifact_data >> process_each_timesheet >> \
                    wait_for_process_timesheets >> format_logs >> render_logs_csv >> get_log_file_name >> generate_downloadable_link >> \
                        upload_log_to_sftp >> send_import_complete_email


    return dag


rail.for_each_instance(create_dag)
