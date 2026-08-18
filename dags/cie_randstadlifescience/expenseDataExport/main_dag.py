from datetime import timedelta, datetime
import pendulum
import rail
from cie_randstadlifescience.expenseDataExport.utils import download_from_s3, data_formatting, upload_to_s3, delete_files_from_s3
from cie_randstadlifescience.expenseDataExport import payloads
# pylint: disable=unnecessary-lambda,line-too-long,too-many-statements
# config : https://github.com/replicon/airflow-integrations/blob/main/dags/dags/cie_randstadlifescience/timeDataExport/config.py


def create_dag(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    dag_id_prefix = f'{config.team_id}_' if config.instance else ''
    with rail.create_airflow_dag(
        dag_id=f'{dag_id_prefix}expenseDataExport_master{dag_id_postfix}',
        description=f'Expense Data Export - {dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        # schedule_interval=timedelta(minutes=5),
        start_date=pendulum.datetime(2022, 10, 10,  tz=config.instance_tz),
        schedule_interval=config.schedule_interval,
        max_active_runs=1,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:
        # start = rail.EmptyOperator(
        #     task_id="start"
        # )

        is_trigger_time = rail.PythonOperator(
            task_id='is_trigger_time',
            python_callable=data_formatting.check_trigger_time,
            op_args=[config]
        )

        trigger_export = rail.IfOperator(
            task_id='trigger_export',
            test="{{ result('is_trigger_time') | is_truthy  }}",
            yes_task='authenticate_replicon',
            no_task='delete_this_dagrun',
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        authenticate_replicon = rail.RepliconServiceOperator(
            task_id="authenticate_replicon",
            endpoint="/services/UserAccessControlService1.svc/GetMyActualUserIdentity"
        )

        download_all_expense_files = download_from_s3.DownloadAllFilesOperator(
            task_id='download_all_expense_files',
            file_path=config.expense_detail_file_path,
            bucket_name=config.expense_detail_bucket,
            expires_in_seconds=7*24*60*60,
        )

        get_expense_uris = rail.PythonOperator(
            task_id='get_expense_uris',
            python_callable=data_formatting.get_expense_uris,
            op_args=[
                '{{ result("download_all_expense_files") | tojson }}', config]
        )

        process_expense_uris_child = rail.TriggerDagRunForEachItemOperator(
            task_id='process_expense_uris_child',
            items=lambda: rail.result('get_expense_uris'),
            trigger_dag_id=f'{dag_id_prefix}process_expense_uris_chunk_wisechild_dag{dag_id_postfix}',
            # execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        wait_for_expense_uris_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_expense_uris_child',
            dag_runs='{{ result("process_expense_uris_child") }}',
            execution_timeout=timedelta(days=14),
        )

        extract_expense_details_from_variables = rail.PythonOperator(
            task_id='extract_expense_details_from_variables',
            python_callable=data_formatting.extract_expense_details_from_variables,
            op_args=[config]
        )

        get_all_report = rail.RepliconServiceOperator(
            task_id="get_all_report",
            endpoint="/services/ReportService1.svc/GetAllReports",
            response_filter=lambda response: data_formatting.findItemByDisplayText(
                response, config.user_base_report_name)
        )

        user_report_uri = {
            "reportUri": "{{ ti.xcom_pull(task_ids='get_all_report')}}"}

        get_enabled_user_report_filter = rail.RepliconServiceOperator(
            task_id="get_enabled_user_report_filter",
            data=user_report_uri,
            endpoint="/services/ReportService1.svc/GetReportDetails2",
        )

        generate_user_report = rail.RepliconServiceOperator(
            task_id='generate_user_report',
            endpoint="/services/ReportService1.svc/generateReport",
            data=lambda: payloads.get_report_payloads(
                rail.result('get_all_report')),
            response_filter=lambda response: data_formatting.report_str_to_json(
                response)
        )

        user_report_has_data = rail.IfOperator(
            task_id='user_report_has_data',
            test="{{ result('generate_user_report') | length > 0  }}",
            yes_task='grouped_user_details',
            no_task='finish',
        )

        grouped_user_details = rail.PythonOperator(
            task_id='grouped_user_details',
            python_callable=data_formatting.get_grouped_user_details,
            op_args=[
                '{{ result("generate_user_report") | tojson }}']
        )

        # project report
        get_all_report_for_projectdetails = rail.RepliconServiceOperator(
            task_id="get_all_report_for_projectdetails",
            endpoint="/services/ReportService1.svc/GetAllReports",
            response_filter=lambda response: data_formatting.findItemByDisplayText(
                response, config.projectdetails_base_report_name)
        )

        projectdetails_report_uri = {
            "reportUri": "{{ ti.xcom_pull(task_ids='get_all_report_for_projectdetails')}}"}

        get_enabled_projectdetails_report_filter = rail.RepliconServiceOperator(
            task_id="get_enabled_projectdetails_report_filter",
            data=projectdetails_report_uri,
            endpoint="/services/ReportService1.svc/GetReportDetails2",
        )

        generate_projectdetails_report = rail.RepliconServiceOperator(
            task_id='generate_projectdetails_report',
            endpoint="/services/ReportService1.svc/generateReport",
            data=lambda: payloads.get_report_payloads(
                rail.result('get_all_report_for_projectdetails')),
            response_filter=lambda response: data_formatting.report_str_to_json(
                response)
        )

        projectdetails_report_has_data = rail.IfOperator(
            task_id='projectdetails_report_has_data',
            test="{{ result('generate_projectdetails_report') | length > 0  }}",
            yes_task='grouped_project_details',
            no_task='finish',
        )

        grouped_project_details = rail.PythonOperator(
            task_id='grouped_project_details',
            python_callable=data_formatting.get_grouped_project_details,
            op_args=[
                '{{ result("generate_projectdetails_report") | tojson }}']
        )

        convert_expense_chunks_flat_list = rail.PythonOperator(
            task_id='convert_expense_chunks_flat_list',
            python_callable=data_formatting.convert_expense_chunks_flat_list,
            op_args=[
                '{{ result("get_expense_uris") | tojson }}']
        )

        get_expense_approval_detail = rail.RepliconServiceCallForEachItemOperator(
            task_id='get_expense_approval_detail',
            endpoint='/services/ExpenseApprovalService1.svc/GetExpenseSheetApprovalDetails',
            items=lambda: rail.result('convert_expense_chunks_flat_list'),
            execution_timeout=timedelta(days=14),
            data=lambda item: {"expenseUri": item},
            # response_filter=lambda response: data_formatting.get_grouped_expense_approval_detail(
            #    response.json()['d'])
        )

        grouped_expense_approval_detail = rail.PythonOperator(
            task_id='grouped_expense_approval_detail',
            python_callable=data_formatting.get_grouped_expense_approval_detail,
            op_args=[
                '{{ result("get_expense_approval_detail") | tojson }}']
        )

        # get processed ExpenseUris
        get_processed_ExpenseUris = download_from_s3.DownloadFileOperator(
            task_id='get_processed_ExpenseUris',
            file_path=config.processed_expense_uris_file_path,
            file_name=config.processed_expense_uris_file_name,
            bucket_name=config.processed_expense_uris_bucket_name,
            expires_in_seconds=7*24*60*60
        )

        process_expense_extract = rail.PythonOperator(
            task_id='process_expense_extract',
            python_callable=data_formatting.process_expense_extract,
            op_args=[
                '{{ result("extract_expense_details_from_variables") | tojson }}',
                '{{ result("grouped_user_details") | tojson }}',
                '{{ result("grouped_project_details") | tojson }}',
                '{{ result("grouped_expense_approval_detail") | tojson }}',
                '{{ result("get_processed_ExpenseUris") }}',
                config
            ]
        )
        # extract_expense_details_from_variables : expense detail list, grouped_user_details:key is user uri and value is details
        # grouped_project_details:key is project uri and value is details and grouped_expense_approval_detail: expense approval detail with key as expense uri

        extract_has_data = rail.IfOperator(
            task_id='extract_has_data',
            test="{{ result('process_expense_extract') != '' }}",
            yes_task='render_csv',
            no_task='finish',
        )

        col_names = ["SOURCE", "RNA_RPL_IMP_ID", "SEQNBR", "RNA_RPT_PRD_ID", "RNA_TASK_TSH_ID", "RNA_TSH_ENTRY_ID", "RNA_RPL_EMPLID", "EMPLID", "FIRST_NAME",
                     "LAST_NAME", "PAY_END_DT", "DATE_WRK", "TL_QUANTITY", "EXPENSE_TYPE", "RNA_EXPENSE_DATE", "RNA_EXP_PAY_AMT", "SP_EXP_APPROVER", "RNA_RPL_PAY_CODE", "RNA_RPL_ACTIVITY",
                     "RNA_RPL_TASKID", "APPROVAL_STATUS", "RNA_TASK_BILLABLE", "RNA_TSH_BILLABLE", "DTTIME_ADDED", "DTTM_EXPORT", "RNA_RPL_PROJ_ID", "RNA_RPL_TASK_NAME",
                     "RNA_RPL_TASK_CODE", "RNA_RPL_UNITID", "RNA_CLIENT_CODE", "RNA_CLIENT_NAME", "RNA_RPL_NEW_TIME", "VENDOR_ID", "PAY_RATE", "RUN_DTTM", "PROCESS_STATUS",
                     "RECORD_IDENTIFIER", "DTTM_IMPORTED", "EMPLID2", "FIRST_NAME_SRCH", "LAST_NAME_SRCH", "RNA_APPROVER_DTTM"]

        render_csv = rail.WriteCSVFileOperator(
            task_id='render_csv',
            source="{{ result('process_expense_extract') }}",
            delimiter="|",
            header=col_names,
            row=['{{ item["SOURCE"] }}', '{{ item["RNA_RPL_IMP_ID"] }}', '{{ item["SEQNBR"] }}', '{{ item["RNA_RPT_PRD_ID"] }}', '{{ item["RNA_TASK_TSH_ID"] }}',
                    '{{ item["RNA_TSH_ENTRY_ID"] }}', '{{ item["RNA_RPL_EMPLID"] }}',
                    '{{ item["EMPLID"] }}', '{{ item["FIRST_NAME"] }}', '{{ item["LAST_NAME"] }}', '{{ item["PAY_END_DT"] }}', '{{ item["DATE_WRK"] }}', '{{ item["TL_QUANTITY"] }}',
                    '{{ item["EXPENSE_TYPE"] }}', '{{ item["RNA_EXPENSE_DATE"] }}', '{{ item["RNA_EXP_PAY_AMT"] }}', '{{ item["SP_EXP_APPROVER"] }}', '{{ item["RNA_RPL_PAY_CODE"] }}',
                    '{{ item["RNA_RPL_ACTIVITY"] }}', '{{ item["RNA_RPL_TASKID"] }}', '{{ item["APPROVAL_STATUS"] }}', '{{ item["RNA_TASK_BILLABLE"] }}',
                   '{{ item["RNA_TSH_BILLABLE"] }}', '{{ item["DTTIME_ADDED"] }}', '{{ item["DTTM_EXPORT"] }}', '{{ item["RNA_RPL_PROJ_ID"] }}',
                    '{{ item["RNA_RPL_TASK_NAME"] }}', '{{ item["RNA_RPL_TASK_CODE"] }}', '{{ item["RNA_RPL_UNITID"] }}', '{{ item["RNA_CLIENT_CODE"] }}',
                   '{{ item["RNA_CLIENT_NAME"] }}', '{{ item["RNA_RPL_NEW_TIME"] }}', '{{ item["VENDOR_ID"] }}', '{{ item["PAY_RATE"] }}',
                    '{{ item["RUN_DTTM"] }}', '{{ item["PROCESS_STATUS"] }}', '{{ item["RECORD_IDENTIFIER"] }}', '{{ item["DTTM_IMPORTED"] }}',
                 '{{ item["EMPLID2"] }}', '{{ item["FIRST_NAME_SRCH"] }}', '{{ item["LAST_NAME_SRCH"] }}', '{{ item["RNA_APPROVER_DTTM"] }}'],
        )

        upload_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_to_sftp',
            sftp_conn_id=config.sftp_conn_id,
            content="{{ result('render_csv') }}",
            remote_filepath=config.sftp_filepath + config.export_filename +
            datetime.now().strftime(config.output_export_file_timestamp_format) + '.csv',
        )

        create_expense_uri_content = rail.PythonOperator(
            task_id='create_expense_uri_content',
            python_callable=data_formatting.create_expense_uri_str,
            op_args=[
                '{{ result("process_expense_extract") }}', '{{ result("get_processed_ExpenseUris") }}']
        )

        update_expense_uris_file = upload_to_s3.UpdateFileOperator(
            task_id='update_expense_uris_file',
            source="{{ result('create_expense_uri_content') }}",
            bucket_name=config.processed_expense_uris_bucket_name,
            file_path=config.processed_expense_uris_file_path,
            file_name=config.processed_expense_uris_file_name,
        )

        get_s3_keys = rail.PythonOperator(
            task_id='get_s3_keys',
            python_callable=data_formatting.get_s3_keys,
            op_args=[
                '{{ result("download_all_expense_files") | tojson }}']
        )

        delete_files_s3 = delete_files_from_s3.DeleteFilesOperator(
            task_id='delete_files_s3',
            file_path=config.expense_detail_file_path,
            bucket_name=config.expense_detail_bucket,
            s3_keys='{{ result("get_s3_keys") | tojson }}',
            expires_in_seconds=7*24*60*60,
        )

        log_timestamp = '{{ "\n" }}{{ current_time("%d/%m/%YT%H:%M:%S") }}{{ " | INFO | " }}'

        write_replicon_logs = rail.WriteLogOperator(
            task_id='write_replicon_logs',
            message=log_timestamp+'{{ "Process started." }}\
                {%- if result("download_all_expense_files") | length > 0 -%} \
                    '+log_timestamp+'{{ "Downloaded expense files from s3 Bucket '+config.expense_detail_bucket+'." }}\
                        {%- if result("get_expense_uris") | length > 0 -%} \
                            '+log_timestamp+'{{ "Extracting Expense Uris."}}\
                                {%- if result("extract_expense_details_from_variables") | length > 0 -%} \
                                    '+log_timestamp+'{{ "Extracting Expense details."}}\
                                        {%- if result("get_all_report") | is_truthy -%} \
                                            '+log_timestamp+'{{ "Processing Report - '+config.user_base_report_name+'."}}\
                                                {%- if result("generate_user_report") | length > 0 -%} \
                                                    '+log_timestamp+'{{ "User report - '+config.user_base_report_name+' data generated."}}\
                                                        {%- if result("get_expense_approval_detail") | length > 0 -%} \
                                                            '+log_timestamp+'{{ "Approval Detail data generated."}}\
                                                                {%- if result("get_processed_ExpenseUris") | is_truthy -%} \
                                                                    '+log_timestamp+'{{ "Extracted already processed Extracts from s3 Bucket '+config.processed_expense_uris_bucket_name+'."}}\
                                                                    '+log_timestamp+'{{ "Updating file in sftp." }} \
                                                                    '+log_timestamp+'{{ "Updating Expense uris in s3." }} \
                                                                    '+log_timestamp+'{{ "Sending task completion mails" }}\
                                                                    '+log_timestamp+'{{ "Process completed." }}\
                                                                {%- else -%} \
                                                                    '+log_timestamp+'{{ "No already processed Expense Uris found in s3 Bucket '+config.processed_expense_uris_bucket_name+'."}}\
                                                                {%- endif -%}\
                                                        {%- else -%} \
                                                            '+log_timestamp+'{{ "Unable to generate Approval Detail data." }} \
                                                        {%- endif -%}\
                                                {%- else -%} \
                                                    '+log_timestamp+'{{ "User report - '+config.user_base_report_name+', unable to generate data." }} \
                                                {%- endif -%}\
                                        {%- else -%} \
                                            '+log_timestamp+'{{ "Report - '+config.user_base_report_name+' not found." }} \
                                        {%- endif -%}\
                                {%- else -%} \
                                    '+log_timestamp+'{{ "Unable to extract Expense details." }} \
                                {%- endif -%}\
                        {%- else -%} \
                            '+log_timestamp+'{{ "Unable to to extract Expense Uris." }} \
                        {%- endif -%}\
                {%- else -%} \
                    '+log_timestamp+'{{ "Unable to download files from s3 Bucket '+config.expense_detail_bucket+'." }} \
                {%- endif -%}\
                '
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ get_master_log() }}",
            delimiter="|",
            header=[],
            row=['{{ item.message }}'],
        )

        send_task_completion_email = rail.EmailOperator(
            task_id='send_task_completion_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='{{ get_company_key() }} | Expense Data Export - Run Successfully - {{ current_time_in_specified_tz("America/New_York","%m_%d_%Y") }}',
            html_content="templates/expense_export.html",
            # files=[
            #     ("{{ result('render_logs_csv') }}")
            # ],
        )

        send_task_failure_email = rail.EmailOperator(
            task_id='send_task_failure_email',
            trigger_rule='one_failed',
            to=config.alert_email,
            subject='{{ get_company_key() }} | Expense Data Export - failed to create/upload export - {{ current_time_in_specified_tz("America/New_York","%m_%d_%Y") }}',
            html_content='templates/failure_email.html',
            params={
                'dag_id': f'{dag_id_prefix}expenseDataExport_master{dag_id_postfix}'.lower()
            }
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

        finish = rail.EmptyOperator(
            task_id="finish"
        )
        is_trigger_time >> trigger_export
        trigger_export >> rail.Label(
            'No') >> delete_this_dagrun
        trigger_export >> rail.Label('Yes') >> authenticate_replicon >> download_all_expense_files >> get_expense_uris >> process_expense_uris_child \
            >> wait_for_expense_uris_child >> extract_expense_details_from_variables >> get_all_report >> get_enabled_user_report_filter >> generate_user_report >> user_report_has_data

        user_report_has_data >> rail.Label(
            'No') >> finish
        user_report_has_data >> rail.Label('Yes') >> grouped_user_details >> get_all_report_for_projectdetails >> get_enabled_projectdetails_report_filter \
            >> generate_projectdetails_report >> projectdetails_report_has_data

        projectdetails_report_has_data >> rail.Label(
            'No') >> finish
        projectdetails_report_has_data >> rail.Label('Yes') >> grouped_project_details >> convert_expense_chunks_flat_list >> get_expense_approval_detail \
            >> grouped_expense_approval_detail >> get_processed_ExpenseUris >> process_expense_extract >> extract_has_data

        extract_has_data >> rail.Label(
            'No') >> finish
        extract_has_data >> rail.Label('Yes') >> render_csv >> upload_to_sftp >> create_expense_uri_content >> update_expense_uris_file \
            >> get_s3_keys >> delete_files_s3 >> finish
        finish >> write_replicon_logs >> render_logs_csv >> send_task_completion_email >> send_task_failure_email >> final_status
    return dag


rail.for_each_instance(create_dag)
