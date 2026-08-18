from datetime import datetime, timedelta
import rail
from dxctechnology.australia_termination_balance_v1.utils import request_payload

null = None

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_australia_terminated_export_child_{config.instance}_v1',
        description=f'DXC PayrollData_Export_Child Daily Terminated - V1 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        process_start_time = rail.PythonOperator(
            task_id="process_start_time",
            python_callable=lambda: datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        )

        process_start_time_ymd_format = rail.PythonOperator(
            task_id="process_start_time_ymd_format",
            python_callable=lambda:  datetime.utcnow().strftime("%Y%m%d")
        )

        process_start_time_hms_format = rail.PythonOperator(
            task_id="process_start_time_hms_format",
            python_callable=lambda:  datetime.utcnow().strftime("%H%M%S")
        )

        logging_job_start_time = rail.WriteLogOperator(
            task_id="logging_job_start_time",
            log="{{ result('create_log') }}",
            message="{{result('process_start_time')}} - Process started",
            properties={
                "log": "{{result('process_start_time')}} - Process started"
            }
        )

        get_file_name = rail.PythonOperator(
            task_id='get_file_name',
            python_callable=lambda dag_run: config.file_name_prefix +
            "_" + datetime.utcnow().strftime("%Y%m%d%H%M%S")
            + "_AUREPL_RE"+ dag_run.conf['file_diff'] + dag_run.conf['sequence_no'] + "_DUT8G2I"
        )

        create_object_set = rail.RepliconServiceOperator(
            task_id='create_object_set',
            endpoint="/services/UserService1.svc/CreateObjectSet",
            data=lambda: {
                "userUris": rail.get_dag_run_conf()['useruri']
            }
        )

        create_payroll_download_batch = rail.RepliconServiceOperator(
            task_id='create_payroll_download_batch',
            endpoint="/services/PayRunService1.svc/CreatePayrollDownloadBatch",
            data=lambda: request_payload.get_create_payroll_download_batch_payload(config.cutoff_date)
        )

        batchuri = "{{ result('create_payroll_download_batch') }}"
        execute_payroll_download_batch, wait_for_payroll_download_batch = rail.batch_execution(
            'execute_payroll_download_batch', create_payroll_download_batch.task_id)

        get_payroll_run_batch_result = rail.RepliconServiceOperator(
            task_id="get_payroll_run_batch_result",
            endpoint="/services/PayRunService1.svc/GetPayrollDownloadBatchResults",
            data={"payrollDownloadBatchUri": batchuri}
        )

        download_payload_file_from_url = rail.HTTPDownloadFileOperator(
            task_id="download_payload_file_from_url",
            url="{{ result('get_payroll_run_batch_result').downloadUrl }}"
        )

        load_payload_file = rail.LoadCSVFileOperator(
            task_id="load_payload_file",
            document="{{ result('download_payload_file_from_url') }}"
        )

        create_payroll_data_collection = rail.CreateCollectionOperator(
            task_id='create_payroll_data_collection',
            name='payroll_data',
            source="{{ result('load_payload_file') }}"
        )

        has_payroll_data = rail.IfOperator(
            task_id='has_payroll_data',
            test="{{ result('create_payroll_data_collection','length') > 0 }}",
            yes_task='create_payrun_batch',
            no_task='finish_export_no_payroll_data'
        )

        finish_export_no_payroll_data = rail.EmptyOperator(
            task_id='finish_export_no_payroll_data'
        )

        send_email_for_no_payroll_data = rail.EmailOperator(
            task_id='send_email_for_no_payroll_data',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon payroll export is skipped for - Australia - {{dag_run.conf.division_name}} - {{current_time_in_specified_tz()}}',
            html_content="templates/email/es_blank_export.html"
        )

        create_payrun_batch = rail.RepliconServiceOperator(
            task_id='create_payrun_batch',
            endpoint="/services/PayRunService1.svc/CreatePayRunBatch",
            data=lambda: request_payload.get_create_payrun_batch_payload(config.cutoff_date)
        )

        execute_payrun_batch, wait_forpayrun_batch = rail.batch_execution(
            'execute_payrun_batch', create_payrun_batch.task_id)

        get_payrun_batch_result = rail.RepliconServiceOperator(
            task_id="get_payrun_batch_result",
            endpoint="/services/PayRunService1.svc/GetCreatePayRunBatchResults",
            data={"payRunBatchUri": "{{ result('create_payrun_batch') }}"}
        )

        update_payrun_name = rail.RepliconServiceOperator(
            task_id="update_payrun_name",
            endpoint="/services/PayRunService1.svc/UpdatePayRunName",
            data={
                "target": {
                    "uri": "{{ result('get_payrun_batch_result').payRunUri }}",
                },
                "name":  "{{ result('get_file_name')}}"
            }
        )

        create_payrun_download_batch = rail.RepliconServiceOperator(
            task_id="create_payrun_download_batch",
            endpoint="/services/PayRunService1.svc/CreatePayrollDownloadBatch",
            data=request_payload.get_create_payrun_download_batch_payload
        )

        execute_payrun_download_batch, wait_for_payrun_download_batch = rail.batch_execution(
            'execute_payrun_download_batch', create_payrun_download_batch.task_id)

        get_payrun_download_batch_result = rail.RepliconServiceOperator(
            task_id="get_payrun_download_batch_result",
            endpoint="/services/PayRunService1.svc/GetPayrollDownloadBatchResults",
            data={
                "payrollDownloadBatchUri": "{{ result('create_payrun_download_batch') }}"}
        )

        mark_payrun_as_complete = rail.RepliconServiceOperator(
            task_id="mark_payrun_as_complete",
            endpoint="/services/PayRunService1.svc/MarkPayRunAsComplete",
            data={
                "target": {
                    "uri": "{{ result('get_payrun_batch_result').payRunUri }}"
                }
            }
        )

        catch_error = rail.EmptyOperator(
            task_id='catch_error',
            trigger_rule='one_failed'
        )

        download_final_payload_file_from_url = rail.HTTPDownloadFileOperator(
            task_id="download_final_payload_file_from_url",
            url="{{ result('get_payrun_download_batch_result').downloadUrl }}"
        )

        load_final_payload_file = rail.LoadCSVFileOperator(
            task_id="load_final_payload_file",
            document="{{ result('download_final_payload_file_from_url') }}"
        )

        create_final_payroll_data_collection = rail.CreateCollectionOperator(
            task_id='create_final_payroll_data_collection',
            name='finalpayrolldata',
            source="{{ result('load_final_payload_file') }}"
        )

        query_final_payroll_data_without_empid = rail.QueryCollectionOperator(
            task_id='query_final_payroll_data_without_empid',
            query='''SELECT * From finalpayrolldata WHERE NULLIF(CLIID, '') IS NULL OR CLIID=="" '''
        )

        has_empty_empid_data = rail.IfOperator(
            task_id='has_empty_empid_data',
            test="{{ result('query_final_payroll_data_without_empid','length') > 0 }}",
            yes_task='mark_payrun_as_draft',
            no_task='process_regular_payrolldata_export'
        )

        payload = {
            "target": {
                "uri": "{{ result('get_payrun_batch_result').payRunUri }}"
            }
        }

        mark_payrun_as_draft = rail.RepliconServiceOperator(
            task_id="mark_payrun_as_draft",
            endpoint="/services/PayRunService1.svc/MarkPayRunAsDraft",
            data=payload
        )

        cancel_payrun = rail.RepliconServiceOperator(
            task_id="cancel_payrun",
            endpoint="/services/PayRunService1.svc/CancelPayRun",
            data=payload
        )

        fail_export = rail.FailOperator(
            task_id="fail_export",
            message="Employee ID not present for some users. Users available to validate in payrun \
                '{{ result('get_file_name')}}'"
        )

        process_regular_payrolldata_export = rail.TriggerDagRunOperator(
            task_id='process_regular_payrolldata_export',
            retries=0,
            trigger_dag_id=f'dxctechnology_australia_terminated_export_regular_child_{config.instance}_v1',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf= request_payload.get_regular_child_dagrun_conf
        )

        query_users_for_report = rail.QueryCollectionOperator(
            task_id='query_users_for_report',
            query='''SELECT DISTINCT useruri From terminateduserslist WHERE employeeid IN (SELECT DISTINCT CLIID FROM  finalpayrolldata) '''
        )

        get_termination_balance_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_termination_balance_report_details',
            report_name=config.termination_balance_report_name_us
        )

        load_termination_balance_report = rail.run_report(
            group_id='load_termination_balance_report',
            report_params=request_payload.get_run_termination_balance_report_payload
        )

        termination_balance_report_payload_to_csv = rail.LoadCSVFileOperator(
            task_id="termination_balance_report_payload_to_csv",
            document='{{result("load_termination_balance_report.get_report_result").reportGenerationResults[0].payload}}'
        )

        termination_balance_report_data_collection = rail.CreateCollectionOperator(
            task_id="termination_balance_report_data_collection",
            source='{{result("termination_balance_report_payload_to_csv")}}',
            name="terminatedusertimeoffbalance"
        )

        has_timeoff_payroll_data = rail.IfOperator(
            task_id='has_timeoff_payroll_data',
            test="{{ result('termination_balance_report_data_collection','length') > 0 }}",
            yes_task='query_list_terminated_user_balance',
            no_task='finish_export_no_payroll_data'
        )

        query_list_terminated_user_balance = rail.QueryCollectionOperator(
            task_id='query_list_terminated_user_balance',
            name= "terminationbalance",
            query="""SELECT * FROM  terminatedusertimeoffbalance WHERE Employee_ID IN (SELECT DISTINCT CLIID FROM  finalpayrolldata)""",
        )

        query_final_payroll_collection = rail.QueryCollectionOperator(
            task_id="query_final_payroll_collection",
            query="SELECT * FROM terminationbalance WHERE NULLIF(Employee_ID, '') IS NOT NULL",
        )

        query_active_user_balance_data = rail.QueryCollectionOperator(
            task_id="query_active_user_balance_data",
            query='''SELECT DISTINCT(Employee_ID),Time_Off_Balance,Time_Off_Type FROM query_final_payroll_collection WHERE Time_Off_Type == "[AUS] LSL Prorata Accrual" ''',
        )

        query_list_in_final_payroll_collection = rail.QueryCollectionOperator(
            task_id="query_list_in_final_payroll_collection",
            query="SELECT * FROM query_final_payroll_collection WHERE Time_Off_Type != '[AUS] LSL Prorata Accrual' ",
        )

        has_query_final_payroll_collection = rail.IfOperator(
            task_id='has_query_final_payroll_collection',
            test='{{ result("query_list_in_final_payroll_collection", "length") > 0 }}',
            yes_task="final_termination_balance_data_to_csv",
            no_task="finish_export_no_payroll_data"
        )

        no_of_records_size_including_header_footer=rail.PythonOperator(
            task_id="no_of_records_size_including_header_footer",
            python_callable=lambda:  int(rail.result('query_list_in_final_payroll_collection','length')) + 2
        )

        final_termination_balance_data_to_csv = rail.WriteCSVFileOperator(
            task_id="final_termination_balance_data_to_csv",
            source="{{ result('query_list_in_final_payroll_collection') }}",
            header=["RECTY","CLIID","INTCA","ORDNO","IOPER","INFTY","SUBTY","BEGDA",
            "ENDDA","OBJPS","SPRPS","SEQNR","EXTRA","BEGUZ","ENDUZ","KTART","ANZHL","DESTA","DEEND",
            "VTKEN","KVERB", "TDLANGU", "TDSUBLA", "TDTYPE", "QUONR"],
            row=request_payload.get_termination_balance_us_data_row,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        create_document = rail.RenderTemplateOperator(
            task_id='create_document',
            target='artifact',
            template_file='schema/termination_balance_aus.txt',
            dataset="{{ result('final_termination_balance_data_to_csv') }}",
        )

        pgp_encyrpt_item_file = rail.PGPEncryptionOperator(
            task_id="pgp_encyrpt_item_file",
            source="{{ result('create_document') }}",
            pgp_conn_id=config.pgp_conn_id
        )

        upload_encrypted_export_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_encrypted_export_data_to_sftp",
            content='{{result("pgp_encyrpt_item_file")}}',
            remote_filepath=config.output_filepath +
            '{{ result("get_file_name") }}.SAP.pgp'
        )

        upload_export_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_export_data_to_sftp",
            sftp_conn_id=config.secondary_encrypted_sftp_conn_id,
            content="{{ result('pgp_encyrpt_item_file') }}",
            remote_filepath=config.secondary_encrypted_output_filepath +
            "{{ result('get_file_name')}}.SAP.pgp"
        )

        upload_export_data_to_secondary_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_export_data_to_secondary_sftp",
            sftp_conn_id=config.secondary_sftp_conn_id,
            content='{{result("create_document")}}',
            remote_filepath=config.secondary_output_filepath +
            '{{ result("get_file_name")}}.SAP'
        )

        is_upload_data_to_sftp_failed = rail.IfOperator(
            task_id='is_upload_data_to_sftp_failed',
            test=request_payload.is_upload_data_to_sftp_failed,
            yes_task="send_email_for_sftp_failure",
            no_task="fail_sftp_export"
        )

        send_email_for_sftp_failure = rail.EmailOperator(
            task_id='send_email_for_sftp_failure',
            to=config.alert_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Termination balance data export automation - SFTP failure for {{ dag_run.conf.location }} location  on - current_time_in_specified_tz() }}',
            params={
                'output_filepath': config.output_filepath,
            },
            html_content="templates/email/sftp_failure.html",
            files=[
                ('{{ result("get_file_name") }}.SAP.pgp', '{{result("pgp_encyrpt_item_file")}}')]
        )

        logging_no_of_valid_records = rail.WriteLogOperator(
            task_id="logging_no_of_valid_records",
            log="{{ result('create_log') }}",
            message="{{ current_time_in_specified_tz() }} - INFO admin No of records exported = {{result('query_list_in_final_payroll_collection','length')}}",
            properties={
                "log": "{{ current_time_in_specified_tz() }} - INFO admin No of records exported = {{result('query_list_in_final_payroll_collection','length')}}",
            }
        )

        logging_file_creation = rail.WriteLogOperator(
            task_id="logging_file_creation",
            log="{{ result('create_log') }}",
            message="{{ current_time_in_specified_tz() }} - INFO admin Export File_" +
            '{{ result("get_file_name")}}' + " created",
            properties={
                "log": "{{ current_time_in_specified_tz() }} - INFO admin Export File_" +
                    '{{ result("get_file_name")}}' + ".txt"
            }
        )

        process_end_time = rail.PythonOperator(
            task_id="process_end_time",
            python_callable=lambda: datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        )

        logging_job_end_time = rail.WriteLogOperator(
            task_id="logging_job_end_time",
            log="{{ result('create_log') }}",
            message="{{result('process_end_time')}} - Process ended",
            properties={
                "log": "{{result('process_end_time')}} - Process ended"
            }
        )

        log_file_data_to_csv = rail.WriteCSVFileOperator(
            task_id="log_file_data_to_csv",
            source="{{ result('create_log') }}",
            header=None,
            row=[
                '{{ item.properties | attr_or_default("log", "") }}'
            ]
        )

        send_email_for_export_copmpletion = rail.EmailOperator(
            task_id='send_email_for_export_copmpletion',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon payroll export for Australia Termination file completed  on - {{ current_time_in_specified_tz() }}',
            params={
                'output_filepath': config.output_filepath,
                'log_filepath': config.log_filepath

            },
            html_content="templates/email/export_success.html"
        )

        upload_log_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_log_data_to_sftp",
            content='{{result("log_file_data_to_csv")}}',
            remote_filepath=config.log_filepath +
            "log_"+'{{ result("get_file_name")}}' + ".txt"
        )

        is_upload_log_to_sftp_failed = rail.IfOperator(
            task_id='is_upload_log_to_sftp_failed',
            test=request_payload.is_upload_log_to_sftp_failed,
            yes_task="send_email_for_log_upload_failure",
            no_task="fail_export_before_log"
        )

        send_email_for_log_upload_failure = rail.EmailOperator(
            task_id='send_email_for_log_upload_failure',
            to=config.alert_email,
            subject='{{ get_company_key() }} | Replicon payroll export for Australia Termination file - SFTP failure for {{ dag_run.conf.location }} location {{ current_time_in_specified_tz() }}',
            params={
                'log_filepath': config.log_filepath
            },
            html_content="templates/email/log_upload_failure.html",
            files=[
                ("log_"+'{{ result("get_file_name") }}', '{{result("log_file_data_to_csv")}}')]
        )

        catch_sftp_upload_error = rail.EmptyOperator(
            task_id = 'catch_sftp_upload_error',
            trigger_rule='one_failed'
        )

        fail_export_before_log = rail.FailOperator(
            task_id="fail_export_before_log",
            message="termination file export has failed"
        )

        fail_sftp_export = rail.FailOperator(
            task_id = 'fail_sftp_export',
            message="termination file export has failed"
        )

        finish_export = rail.EmptyOperator(
            task_id = 'finish_export'
        )

        create_log >> process_start_time >> process_start_time_ymd_format >> process_start_time_hms_format >> \
            logging_job_start_time >> create_object_set
        create_object_set >> get_file_name >> create_payroll_download_batch
        create_payroll_download_batch >> execute_payroll_download_batch >> wait_for_payroll_download_batch >> \
            get_payroll_run_batch_result >> download_payload_file_from_url >> load_payload_file >> \
            create_payroll_data_collection >> has_payroll_data
        has_payroll_data >> rail.Label(
            'Yes') >> create_payrun_batch >> execute_payrun_batch >> wait_forpayrun_batch >> get_payrun_batch_result
        has_payroll_data >> rail.Label('No') >> finish_export_no_payroll_data >> send_email_for_no_payroll_data
        get_payrun_batch_result >> update_payrun_name >> create_payrun_download_batch >> execute_payrun_download_batch \
            >> wait_for_payrun_download_batch >> get_payrun_download_batch_result >> mark_payrun_as_complete >> rail.Label(
                "on_success") >> download_final_payload_file_from_url
        mark_payrun_as_complete >> rail.Label(
            "on_error") >> catch_error >> cancel_payrun >> fail_export
        download_final_payload_file_from_url >> load_final_payload_file >> create_final_payroll_data_collection

        create_final_payroll_data_collection >> query_final_payroll_data_without_empid >> has_empty_empid_data >> rail.Label(
            'Yes') >> mark_payrun_as_draft >> cancel_payrun >> fail_export

        has_empty_empid_data >> rail.Label(
            "No") >> process_regular_payrolldata_export >> query_users_for_report >> get_termination_balance_report_details >> load_termination_balance_report >> termination_balance_report_payload_to_csv >> \
                termination_balance_report_data_collection >> has_timeoff_payroll_data
        has_timeoff_payroll_data >> rail.Label(
            "Yes") >> query_list_terminated_user_balance
        has_timeoff_payroll_data >> rail.Label(
            "No") >> finish_export_no_payroll_data
        query_list_terminated_user_balance >> query_final_payroll_collection >> query_active_user_balance_data >> query_list_in_final_payroll_collection
        query_list_in_final_payroll_collection >> has_query_final_payroll_collection
        has_query_final_payroll_collection >> rail.Label(
            "Yes") >> final_termination_balance_data_to_csv >> no_of_records_size_including_header_footer >> create_document >> pgp_encyrpt_item_file >> upload_export_data_to_sftp >> upload_encrypted_export_data_to_sftp
        has_query_final_payroll_collection >> rail.Label('No') >> finish_export_no_payroll_data >> send_email_for_no_payroll_data
        upload_encrypted_export_data_to_sftp >> rail.Label(
            "on_success") >>upload_export_data_to_secondary_sftp>> process_end_time >> logging_no_of_valid_records >> logging_file_creation
        upload_encrypted_export_data_to_sftp >> rail.Label("on_error") >> catch_sftp_upload_error >> is_upload_data_to_sftp_failed >> rail.Label("Yes") >> send_email_for_sftp_failure
        is_upload_data_to_sftp_failed >> rail.Label("No") >> fail_sftp_export
        logging_file_creation >> logging_job_end_time >> log_file_data_to_csv >> send_email_for_export_copmpletion >> upload_log_data_to_sftp
        upload_log_data_to_sftp >> rail.Label("on_success") >> finish_export
        upload_log_data_to_sftp >> rail.Label("on_error") >> catch_sftp_upload_error >> is_upload_log_to_sftp_failed >> rail.Label("Yes") >> send_email_for_log_upload_failure
        is_upload_log_to_sftp_failed >> rail.Label(
            "No") >> fail_export_before_log

    return dag

rail.for_each_instance(create_dag)
