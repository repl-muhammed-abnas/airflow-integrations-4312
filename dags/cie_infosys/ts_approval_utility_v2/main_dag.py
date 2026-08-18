# pylint: disable=line-too-long wildcard-import unused-wildcard-import, too-many-statements line-too-long
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
import pendulum
import rail
from cie_infosys.ts_approval_utility_v2.utils import data_formatting
from cie_infosys.ts_approval_utility_v2.utils import download_from_s3
from cie_infosys.ts_approval_utility_v2.utils import upload_to_s3


# config : https://github.com/replicon/airflow-integrations/main/dags/infosys/sap_time_export/config.py


def create_main_dag(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    dag_id_prefix = f'{config.team_id}_' if config.instance else ''
    location = f'{config.location}_' if config.location else ''
    with rail.create_airflow_dag(
        dag_id=f'{dag_id_prefix}{config.company_key}_timesheet_approval_{location}master_v2{dag_id_postfix}'.lower(),
        description=f'{dag_id_prefix}infosys_timehseet_approval_Master{dag_id_postfix} - V1.0',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        # runs at everytime as per configure minutes
        start_date=pendulum.datetime(2022, 10, 10,  tz=config.timezone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_master_run,
        default_args={
        },
    ) as dag:

        entry_report_name = config.infosys_config['entry_report_name']
        timesheet_report_name = config.infosys_config['timesheet_report_name']
        period_in_months = config.infosys_config['period_in_months']
        now = pendulum.now(config.timezone)
        startDate = (now - relativedelta(months=period_in_months)
                     ).strftime('%b, %d, %Y')
        startDate2 = (now - relativedelta(months=period_in_months, days=7)
                      ).strftime('%b, %d, %Y')
        endDate = now.strftime('%b, %d, %Y')
        ts_notification_period_details = str(datetime.strftime((now - relativedelta(days=7)), "%d/%m/%Y")) +" - "+ str(datetime.strftime((now - relativedelta(days=1)), "%d/%m/%Y"))

        longrunning_task = download_from_s3.DownloadCsvOperator(
            task_id='longrunning_task',
            bucket_name=config.bucket_name,
            file_path=config.file_path,
            file_name=config.file_name,
            expires_in_seconds=7*24*60*60,
        )
        entry_by_request_or_time = rail.PythonOperator(
            task_id="entry_by_request_or_time",
            python_callable=data_formatting.check_for_request,
            op_args=[
                "{{ result('longrunning_task')}}", now, config]
        )
        has_to_run_approval = rail.IfOperator(
            task_id='has_to_run_approval',
            test="{{ result('entry_by_request_or_time').get('to_run') | is_truthy}}",
            yes_task='eligible_for_ts_mail_notification',
            no_task='delete_this_dagrun'
        )

        delete_this_dagrun = rail.EmptyOperator(
            task_id="delete_this_dagrun"
        )#rail.DeleteCurrentDagRunOperator(task_id='delete_this_dagrun')
 

        eligible_for_ts_mail_notification = rail.IfOperator(
            task_id='eligible_for_ts_mail_notification',
            test=config.location.lower() != 'India'.lower() and now.strftime('%A').lower() == 'Monday'.lower(),
            yes_task='download_ts_details_from_file',
            no_task='get_all_report'
        )

        download_ts_details_from_file = download_from_s3.DownloadCsvOperator(
            task_id='download_ts_details_from_file',
            bucket_name=config.bucket_name,
            file_path=config.status_artifacts_file_path,
            file_name=config.ts_status_artifacts_file_name,
            expires_in_seconds=7*24*60*60,
        )

        get_consolidated_ts_approval_count = rail.PythonOperator(
            task_id="get_consolidated_ts_approval_count",
            python_callable=data_formatting.get_ts_data_from_artifacts
        )

        send_ts_task_completion_email = rail.EmailOperator(
            task_id='send_ts_task_completion_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Timesheet Automation Status for the Period : '+ ts_notification_period_details,
            html_content="templates/emails/email_for_timesheet_approval.html",
        )

        remove_ts_data_from_file = upload_to_s3.UploadCsvOperator(
            task_id='remove_ts_data_from_file',
            source="{{ result('get_consolidated_ts_approval_count').get('remove_file_content') }}",
            bucket_name=config.bucket_name,
            file_path=config.status_artifacts_file_path,
            file_name=config.ts_status_artifacts_file_name,
        )

        get_all_report = rail.RepliconServiceOperator(
            task_id="get_all_report",
            endpoint="/services/ReportService1.svc/GetAllReports",
            response_filter=lambda response: data_formatting.findItemByDisplayText(
                response, entry_report_name, timesheet_report_name)
        )
        has_all_report = rail.IfOperator(
            task_id='has_all_report',
            test="{{ result('get_all_report') | is_truthy}}",
            yes_task='get_entry_report_details',
            no_task='finish'
        )

        get_entry_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_entry_report_details',
            report_name=entry_report_name,
        )

        def entry_approval_filter_uri(filter_name):
            return rail.find_first_by_attr_and_get_attr(
                rail.result('get_entry_report_details')['filterConfiguration']['enabledFilters'], 'displayText', filter_name, 'uri')

        def entry_date_filter_uri(filter_name):
            return rail.find_first_by_attr_and_get_attr(
                rail.result('get_entry_report_details')['filterConfiguration']['enabledFilters'], 'displayText', filter_name, 'uri')

        get_report_approval_filter_uri = rail.PythonOperator(
            task_id='get_report_approval_filter_uri',
            python_callable=entry_approval_filter_uri,
            op_args=["TimeEntryStatusFilter"]
        )
        get_report_filter = rail.PythonOperator(
            task_id='get_report_filter',
            python_callable=entry_date_filter_uri,
            op_args=["EntryDateFilter"]
        )
        run_report_for_entry = rail.run_report2(
            group_id='run_report_for_entry',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_all_report').get('entry_report_uri')}}",
                        "filterValues": [

                            {
                                "reportFilterUri": "{{result('get_report_filter')}}",
                                "value": None,
                            },
                            {
                                "reportFilterUri": "{{result('get_report_filter')}}",
                                "value": startDate,
                            },
                            {
                                "reportFilterUri": "{{result('get_report_filter')}}",
                                "value": endDate,
                            },
                            {
                                "reportFilterUri": "{{result('get_report_approval_filter_uri')}}",
                                "value": "1",
                            },
                        ],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            target='artifact',
            replicon_conn_id=config.replicon_conn_id,
        )

        has_report_entry_data = rail.IfOperator(
            task_id='has_report_entry_data',
            test="{{ result('run_report_for_entry.get_report_result','has_data')}}",
            yes_task='get_entry_waiting_for_approval',
            no_task='finish'
        )

        get_entry_waiting_for_approval = rail.PythonOperator(
            task_id="get_entry_waiting_for_approval",
            python_callable=data_formatting.get_formated_entry_data,
            op_args=[
                "{{ result('run_report_for_entry.get_report_result') }}", config]
        )
        process_entry_child = rail.TriggerDagRunForEachItemOperator(
            task_id='process_entry_child',
            items=lambda: rail.result('get_entry_waiting_for_approval'),
            trigger_dag_id=f'{dag_id_prefix}{config.company_key}_process_entry_chunk_{location}child_v2{dag_id_postfix}'.lower(
            ),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )
        wait_for_process_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_child',
            dag_runs='{{ result("process_entry_child") }}',
            execution_timeout=timedelta(days=14),
        )

        gather_entry_child_data = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_entry_child_data',
            dag_runs="{{ result('process_entry_child') }}",
            dagrun_task_id='create_log',
            flatten=True,
        )

        get_merged_entries_logs = rail.PythonOperator(
            task_id='get_merged_entries_logs',
            python_callable=data_formatting.get_entry_errror_logs
        )

        merged_entries_logs_has_data = rail.IfOperator(
            task_id='merged_entries_logs_has_data',
            test="{{ result('get_merged_entries_logs').get('has_data') }}",
            yes_task='get_entry_status_file_from_s3',
            no_task='is_country_india',
        )

        get_entry_status_file_from_s3 = download_from_s3.DownloadCsvOperator(
            task_id='get_entry_status_file_from_s3',
            bucket_name=config.bucket_name,
            file_path=config.status_artifacts_file_path,
            file_name=config.entry_status_artifacts_file_name,
            expires_in_seconds=7*24*60*60,
        )

        create_entry_logs_content = rail.PythonOperator(
            task_id='create_entry_logs_content',
            python_callable=data_formatting.create_entry_logs_str
        )

        update_entry_status_file = upload_to_s3.UploadCsvOperator(
            task_id='update_entry_status_file',
            source="{{ result('create_entry_logs_content') }}",
            bucket_name=config.bucket_name,
            file_path=config.status_artifacts_file_path,
            file_name=config.entry_status_artifacts_file_name,
        )

        is_country_india = rail.IfOperator(
            task_id='is_country_india',
            test=config.location.lower() == 'India'.lower(),
            yes_task='finish',
            no_task='connector'
        )

        connector = rail.EmptyOperator(
            task_id="connector"
        )

        run_report_for_updated_entry = rail.run_report2(
            group_id='run_report_for_updated_entry',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_all_report').get('entry_report_uri')}}",
                        "filterValues": [

                            {
                                "reportFilterUri": "{{result('get_report_filter')}}",
                                "value": None,
                            },
                            {
                                "reportFilterUri": "{{result('get_report_filter')}}",
                                "value": startDate2,
                            },
                            {
                                "reportFilterUri": "{{result('get_report_filter')}}",
                                "value": endDate,
                            },
                        ],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            target='artifact',
            replicon_conn_id=config.replicon_conn_id,
        )

        has_report_updated_entry_data = rail.IfOperator(
            task_id='has_report_updated_entry_data',
            test="{{ result('run_report_for_updated_entry.get_report_result','has_data')}}",
            yes_task='get_timesheet_report_details',
            no_task='finish'
        )
        get_timesheet_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_timesheet_report_details',
            report_name=timesheet_report_name,
        )

        def timesheet_date_filter_uri(filter_name):
            return rail.find_first_by_attr_and_get_attr(
                rail.result('get_timesheet_report_details')['filterConfiguration']['enabledFilters'], 'displayText', filter_name, 'uri')

        get_timesheet_status_filter_uri = rail.PythonOperator(
            task_id='get_timesheet_status_filter_uri',
            python_callable=timesheet_date_filter_uri,
            op_args=["ApprovalStatusFilter"]
        )
        get_timesheet_report_filter = rail.PythonOperator(
            task_id='get_timesheet_report_filter',
            python_callable=timesheet_date_filter_uri,
            op_args=["EntryDateFilter"]
        )
        run_report_for_timesheet = rail.run_report2(
            group_id='run_report_for_timesheet',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_all_report').get('timesheet_report_uri')}}",
                        "filterValues": [

                            {
                                "reportFilterUri": "{{result('get_timesheet_report_filter')}}",
                                "value": None,
                            },
                            {
                                "reportFilterUri": "{{result('get_timesheet_report_filter')}}",
                                "value": startDate,
                            },
                            {
                                "reportFilterUri": "{{result('get_timesheet_report_filter')}}",
                                "value": endDate,
                            },
                            {
                                "reportFilterUri": "{{result('get_timesheet_status_filter_uri')}}",
                                "value": "0",
                            },
                            {
                                "reportFilterUri": "{{result('get_timesheet_status_filter_uri')}}",
                                "value": "1",
                            },
                        ],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            target='artifact',
            replicon_conn_id=config.replicon_conn_id,
        )

        has_report_timesheet_data = rail.IfOperator(
            task_id='has_report_timesheet_data',
            test="{{ result('run_report_for_timesheet.get_report_result','has_data')}}",
            yes_task='get_timesheet_waiting_for_approval',
            no_task='finish'
        )

        get_timesheet_waiting_for_approval = rail.PythonOperator(
            task_id="get_timesheet_waiting_for_approval",
            python_callable=data_formatting.get_formated_timesheet_data,
            op_args=[config]
        )

        process_timesheet_child = rail.TriggerDagRunForEachItemOperator(
            task_id='process_timesheet_child',
            items=lambda: rail.result('get_timesheet_waiting_for_approval'),
            trigger_dag_id=f'{dag_id_prefix}{config.company_key}_process_timesheet_chunk_{location}child_v2{dag_id_postfix}'.lower(
            ),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )
        wait_for_process_timesheet_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_timesheet_child',
            dag_runs='{{ result("process_timesheet_child") }}',
            execution_timeout=timedelta(days=14),
        )

        gather_ts_child_data = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_ts_child_data',
            dag_runs="{{ result('process_timesheet_child') }}",
            dagrun_task_id='create_log',
            flatten=True,
        )

        get_merged_ts_logs = rail.PythonOperator(
            task_id='get_merged_ts_logs',
            python_callable=data_formatting.get_ts_errror_logs
        )

        merged_ts_logs_has_data = rail.IfOperator(
            task_id='merged_ts_logs_has_data',
            test="{{ result('get_merged_ts_logs').get('has_data') }}",
            yes_task='get_ts_status_file_from_s3',
            no_task='update_task_status',
        )

        get_ts_status_file_from_s3 = download_from_s3.DownloadCsvOperator(
            task_id='get_ts_status_file_from_s3',
            bucket_name=config.bucket_name,
            file_path=config.status_artifacts_file_path,
            file_name=config.ts_status_artifacts_file_name,
            expires_in_seconds=7*24*60*60,
        )

        create_ts_logs_content = rail.PythonOperator(
            task_id='create_ts_logs_content',
            python_callable=data_formatting.create_ts_logs_str
        )

        update_ts_status_file = upload_to_s3.UploadCsvOperator(
            task_id='update_ts_status_file',
            source="{{ result('create_ts_logs_content') }}",
            bucket_name=config.bucket_name,
            file_path=config.status_artifacts_file_path,
            file_name=config.ts_status_artifacts_file_name,
        )

        update_task_status = rail.PythonOperator(
            task_id="update_task_status",
            python_callable=data_formatting.update_request_status,
            op_args=["{{ result('longrunning_task') }}"]
        )
        update_longrunning_task = upload_to_s3.UploadCsvOperator(
            task_id='update_longrunning_task',
            source="{{ result('update_task_status') }}",
            bucket_name=config.bucket_name,
            file_path=config.file_path,
            file_name=config.file_name,
        )

        finish = rail.EmptyOperator(
            task_id="finish"
        )

        eligible_for_entry_mail_notification = rail.IfOperator(
            task_id='eligible_for_entry_mail_notification',
            #trigger_rule='all_done',
            test=config.location.lower() != 'India'.lower(),
            yes_task='download_entry_details_from_file',
            no_task='log_completion'
        )

        download_entry_details_from_file = download_from_s3.DownloadCsvOperator(
            task_id='download_entry_details_from_file',
            bucket_name=config.bucket_name,
            file_path=config.status_artifacts_file_path,
            file_name=config.entry_status_artifacts_file_name,
            expires_in_seconds=7*24*60*60,
        )

        get_consolidated_entry_approval_count = rail.PythonOperator(
            task_id="get_consolidated_entry_approval_count",
            python_callable=data_formatting.get_entry_data_from_artifacts
        )

        send_entry_task_completion_email = rail.EmailOperator(
            task_id='send_entry_task_completion_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Time Entry Automation Status For - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/email_for_entry_approval.html",
        )

        remove_entry_data_from_file = upload_to_s3.UploadCsvOperator(
            task_id='remove_entry_data_from_file',
            source="{{ result('get_consolidated_entry_approval_count').get('remove_file_content') }}",
            bucket_name=config.bucket_name,
            file_path=config.status_artifacts_file_path,
            file_name=config.entry_status_artifacts_file_name,
        )

        log_completion = rail.WriteLogOperator(
            task_id='log_completion',
            message='\
                {%- if result("longrunning_task") | length > 0 -%} \
                    {{ "we get data  long running task" if result("longrunning_task") | length > 0  else "There is no file on s3 Bucket" }} \
                        {{ "Approval tool has started" if result("entry_by_request_or_time").get("to_run") | is_truthy  else "No Reuqest to run tool" }} \
                            {%- if result("entry_by_request_or_time").get("to_run") | is_truthy -%}\
                                {{ "tool is running as per schedule reuest" if result("entry_by_request_or_time").get("to_run") | is_truthy and result("entry_by_request_or_time").get("position") | length > 0  else "tool is not running further" }} \
                                    {{ "We got the requested report details" if result("get_all_report") and result("get_all_report").get("entry_report_uri") else "report does not exist" }} \
                                {%- if result("has_all_report") | is_truthy -%}\
                                    {{ "getting filter for entry report details" if result("get_entry_report_details") else "selected filter is not available" }} \
                                        {{ "getting filter uri for selected filter" if result("get_report_filter") else "selected filter uri is not available" }} \
                                            {{ "data is downloaded from report run_report_for_entry" if result("run_report_for_entry.get_report_result",  "has_data") else "report does not have data for selected filters" }} \
                                    {%- if result("has_report_entry_data") | is_truthy -%}\
                                        {{ "checking time entry available for approval" if result("get_entry_waiting_for_approval") else "No time entry is there for approval" }} \
                                            {{ "Approving submitted entries waiting for approval" if result("process_entry_child")  | is_truthy else "No time entry is there for approval" }} \
                                                {{ "get time entry data after approval from report run_report_for_updated_entry" if result("run_report_for_updated_entry.get_report_result",  "has_data") else "No updated time entry data found from report run_report_for_updated_entry" }} \
                                        {%- if result("has_report_updated_entry_data") | is_truthy -%}\
                                            {{ "getting filter details for timehseet report" if result("get_timesheet_report_details") else "selected filter is not available" }} \
                                                {{ "getting filter uri details for timehseet report" if result("get_timesheet_report_filter") | length > 0 else "selected filter uri is not available" }} \
                                                    {{ "data is downloaded from report run_report_for_timesheet" if result("run_report_for_timesheet.get_report_result",  "has_data") else "No time entry found after approval" }} \
                                            {%- if result("has_report_timesheet_data") | is_truthy -%}\
                                                {{ "merging the data of time entry with timesheet and get timesheet for approval" if result("get_timesheet_waiting_for_approval") else "after merging timehseet no timesheet found for approval" }} \
                                                    {{ "Approving all the eligible timesheet" if result("process_timesheet_child") | length > 0 else "No timesheet is there to approve" }} \
                                                        {{ "updating the task status to complete" if result("update_task_status") else "No task is requested manually" }} \
                                                            {{ "Updated the status of completed task to task file in s3" if result("update_longrunning_task") else "No task is requested to be approved" }} \
                                            {%- else -%} \
                                                {{ "Task is ending as report does not have data" }} \
                                            {%- endif -%} \
                                        {%- else -%} \
                                            {{ "Task is ending as report does not have data" }} \
                                        {%- endif -%} \
                                    {%- else -%} \
                                        {{ "Task is ending as report does not have data" }} \
                                    {%- endif -%} \
                                {%- else -%} \
                                    {{ "Task is ending as report does not have data" }} \
                                {%- endif -%} \
                            {%- else -%} \
                                {{ "Task is ending as there is no reuquest to run it" }} \
                            {%- endif -%} \
                {%- else -%} \
                    {{ "LongRunningTask.csv file does not exist in s3 bucket" }} \
                {%- endif -%}',
            properties={
                'timesheetapproval': entry_report_name,
                'projecttype': '{{ dag_run.conf }}',
                'status': '{{ "Success" if result("longrunning_task") | length > 0  else "Exception" }}',
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            properties={
                'reportname': entry_report_name,
                'reporttype': 'Final Errors',
                'status': 'Error',
            }
        )

        send_task_failure_email = rail.EmailOperator(
            task_id='send_task_failure_email',
            # trigger_rule='one_failed',
            to=config.alert_email,
            subject="{{ get_company_key() }} | Timesheet Approval - failed to Approve Timehseet/TimeEntry - {{ current_time_in_specified_tz() }}",
            html_content="templates/emails/failure_email.html",
            params={
                'dag_id': f'{config.company_key}_timesheet_approval_master{dag_id_postfix}'.lower()
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        def final_status(**kwargs):
            for task_instance in kwargs['dag_run'].get_task_instances():
                if task_instance.current_state() == "failed" and \
                        task_instance.task_id != kwargs['task_instance'].task_id:
                    raise Exception(
                        f"Task {task_instance.task_id} failed. Failing this DAG run")

        final_status = rail.PythonOperator(
            task_id='final_status',
            python_callable=final_status,
        )
        longrunning_task >> entry_by_request_or_time >> has_to_run_approval >> rail.Label(
            'Yes') >> eligible_for_ts_mail_notification 
        eligible_for_ts_mail_notification >>  rail.Label(
            'Yes') >> download_ts_details_from_file >> get_consolidated_ts_approval_count >> send_ts_task_completion_email >> remove_ts_data_from_file >> get_all_report
        eligible_for_ts_mail_notification >>  rail.Label(
            'No') >> get_all_report
        get_all_report >> has_all_report
        has_to_run_approval >> rail.Label(
            'No') >> delete_this_dagrun
        has_all_report >> rail.Label(
            'Yes') >> get_entry_report_details >> get_report_approval_filter_uri >> get_report_filter >> run_report_for_entry >> has_report_entry_data
        has_all_report >> rail.Label(
            'No') >> finish
        has_report_entry_data >> rail.Label(
            'No') >> finish
        has_report_entry_data >> rail.Label(
            'Yes') >> get_entry_waiting_for_approval >> process_entry_child >> wait_for_process_child >> gather_entry_child_data >> get_merged_entries_logs >> merged_entries_logs_has_data
        merged_entries_logs_has_data >> rail.Label(
            'Yes') >> get_entry_status_file_from_s3 >> create_entry_logs_content >> update_entry_status_file >> is_country_india
        merged_entries_logs_has_data >> rail.Label(
            'No') >> is_country_india
        is_country_india >> rail.Label(
            'No') >> connector >> run_report_for_updated_entry >> has_report_updated_entry_data
        is_country_india >> rail.Label(
            'Yes') >> finish
        has_report_updated_entry_data >> rail.Label(
            'yes') >> get_timesheet_report_details >> get_timesheet_status_filter_uri >> get_timesheet_report_filter >> run_report_for_timesheet >> has_report_timesheet_data
        has_report_updated_entry_data >> rail.Label(
            'No') >> finish
        has_report_timesheet_data >> rail.Label(
            'No') >> finish
        has_report_timesheet_data >> rail.Label(
            'Yes') >> get_timesheet_waiting_for_approval >> process_timesheet_child >> wait_for_process_timesheet_child >> gather_ts_child_data >> get_merged_ts_logs >> merged_ts_logs_has_data
        merged_ts_logs_has_data >> rail.Label(
            'Yes') >> get_ts_status_file_from_s3 >> create_ts_logs_content >> update_ts_status_file >> update_task_status >> update_longrunning_task >> finish 
        merged_ts_logs_has_data >> rail.Label(
            'No') >> update_task_status >> update_longrunning_task >> finish
        finish >> eligible_for_entry_mail_notification
        eligible_for_entry_mail_notification >> rail.Label(
            'No') >> log_completion
        eligible_for_entry_mail_notification >> rail.Label(
            'Yes') >> download_entry_details_from_file >> get_consolidated_entry_approval_count >> send_entry_task_completion_email >> remove_entry_data_from_file >> log_completion 
        log_completion >> catch_and_log_errors >> send_task_failure_email >> log_to_sumo >> final_status
    return dag


rail.for_each_instance(create_main_dag)
