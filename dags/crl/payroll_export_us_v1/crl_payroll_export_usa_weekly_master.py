from pendulum import datetime
import pendulum
import rail
from crl.payroll_export_us_v1.utils import request_payload
from crl.payroll_export_us_v1.utils import python_callable
# pylint: disable=no-name-in-module


OPEN_BRACKETS = '{{'
CLOSE_BRACKETS = '}}'

# pylint: disable=too-many-statements


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.weekly_dag_id,
        description=f"CRL Payroll Export Weekly Master USA {config.instance}",
        company_key=config.company_key,
        start_date=datetime(2022, 4, 1, tz=config.time_zone),
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
        default_args={
            "sftp_conn_id": config.sftp_conn_id
        }
    ) as dag:

        def can_process_run_test():
            current_date = pendulum.now(config.time_zone).strftime("%d-%m-%Y")
            curent_hours = int(pendulum.now(config.time_zone).strftime("%H"))
            return bool(list(filter(lambda calendar_mapper: calendar_mapper["payroll_processing_date"] == current_date and calendar_mapper["processing_time"] == curent_hours, config.USA_PAYROLL_CALENDER_MAPPER_TO_USE_WEEKLY)))

        can_process_run = rail.IfOperator(
            task_id="can_process_run",
            test=can_process_run_test,
            yes_task="process_start_time"
        )

        process_start_time = rail.PythonOperator(
            task_id="process_start_time",
            python_callable=python_callable.get_time_in_formats,
            op_args=[config.time_zone]
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        logging_job_start_time = rail.WriteLogOperator(
            task_id="logging_job_start_time",
            log="{{ result('create_log') }}",
            message="{{ result('process_start_time').start_time }} - Process started",
            properties={
                "log": "{{ result('process_start_time').start_time }} - Process started"
            }
        )

        get_adp_payroll_script = rail.RepliconServiceOperator(
            task_id="get_adp_payroll_script",
            endpoint="/services/PayrollDownloadScriptAdministrationService1.svc/GetAllScripts",
            response_filter=lambda response: rail.find_first_by_attr_and_get_attr(response.json()['d'],
                                                                                  'displayText', config.payroll_export_file_format, 'uri')
        )

        is_file_format_script_present = rail.IfOperator(
            task_id='is_file_format_script_present',
            test='{{ result("get_adp_payroll_script") | is_truthy }}',
            yes_task='get_user_report_details'
        )

        get_user_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_user_report_details',
            report_name=config.payroll_export_report_weekly,
        )

        genarate_user_report = rail.run_report2(
            group_id='load_user_report',
            report_params=lambda: request_payload.get_user_report_payload()
        )

        is_report_failed = rail.IfOperator(
            task_id="is_report_failed",
            test='{{result("load_user_report.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_report_generation",
            no_task="has_data"
        )

        fail_report_generation = rail.FailOperator(
            task_id="fail_report_generation",
            message="{{result('load_user_report.get_report_result').reportGenerationResults[0].error}}"
        )

        has_data = rail.IfOperator(
            task_id="has_data",
            test='{{ result("load_user_report.get_report_result", "has_data") }}',
            yes_task='users_report_payload_to_csv'
        )

        users_report_payload_to_csv = rail.LoadCSVFileOperator(
            task_id="users_report_payload_to_csv",
            document='{{result("load_user_report.get_report_result").reportGenerationResults[0].payload}}'
        )

        create_object = rail.TriggerDagRunForEachItemOperator(
            task_id="create_object",
            items=lambda: rail.result('users_report_payload_to_csv'),
            batch_size=10000,
            trigger_dag_id=config.child_dag_id,
            conf=lambda item: {"uri": item},
            retries=0
        )

        wait_create_object = rail.WaitForDagRunsSensor(
            task_id="wait_create_object",
            dag_runs="{{result('create_object')}}"
        )

        create_object_uris = rail.GatherResultsFromDagRunsOperator(
            task_id='create_object_uris',
            dag_runs="{{ result('create_object') }}",
            dagrun_task_id='create_object_set'
        )

        get_file_name = rail.PythonOperator(
            task_id='get_file_name',
            python_callable=lambda: "P" + config.adp_gv_system + config.gv_system_number + "476" + "_" +
            pendulum.now(config.time_zone).strftime(
                "%Y%m%d%H%M%S") + "_" + "USTIME_HRMD02_DUT8G2I"
        )

        # pylint: disable=unnecessary-lambda
        create_payroll_download_batch = rail.RepliconServiceOperator(
            task_id="create_payroll_download_batch",
            endpoint="/services/PayRunService1.svc/CreatePayrollDownloadBatch",
            data=lambda: request_payload.get_create_payroll_download_batch_payload(
                config.time_zone, "WEEKLY")
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
            subject='{{ get_company_key() }} | Replicon Payroll Data Export - No records found - {{ result("process_start_time").start_time }}',
            html_content="/templates/email/blank_export.html"
        )

        # pylint: disable=unnecessary-lambda
        create_payrun_batch = rail.RepliconServiceOperator(
            task_id="create_payrun_batch",
            endpoint="/services/PayRunService1.svc/CreatePayRunBatch",
            data=lambda: request_payload.get_create_payrun_batch_payload(
                config.time_zone, "WEEKLY")
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
        send_invalid_records_email = rail.EmailOperator(
            task_id='send_invalid_records_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Payroll Data Export - Invalid records found - {{ result("process_start_time").start_time }}',
            html_content="templates/email/email_invalid_records_in_export.html"
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
            no_task='get_all_employee_types_from_mapper'
        )

        invalid_records = rail.WriteCSVFileOperator(
            task_id='invalid_records',
            source="{{ result('query_final_payroll_data_without_empid')}}",
            thread_pool_size=config.thread_pool_size_write_csv
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{result('invalid_records')}}",
            output_file_name="Invalid_PayrollExport_records_{{dag_run_ecid()}}_.csv",
            expires_in_seconds=config.expire_time
        )

        mark_payrun_as_draft = rail.RepliconServiceOperator(
            task_id="mark_payrun_as_draft",
            endpoint="/services/PayRunService1.svc/MarkPayRunAsDraft",
            data=request_payload.get_payload
        )

        on_error = rail.EmptyOperator(
            task_id='on_error',
            trigger_rule='one_failed'
        )

        cancel_payrun = rail.RepliconServiceOperator(
            task_id="cancel_payrun",
            endpoint="/services/PayRunService1.svc/CancelPayRun",
            data=request_payload.get_payload
        )

        fail_export = rail.FailOperator(
            task_id="fail_export",
            message="Employee ID not present for some users. Users available to validate in payrun \
                '{{ result('get_file_name')}}'"
        )

        get_all_employee_types_from_mapper = rail.PythonOperator(
            task_id='get_all_employee_types_from_mapper',
            python_callable=lambda: python_callable.get_all_required_employee_types(
                config.REGULAR_EMPLOYEE_TYPES)
        )

        query_list_in_final_payroll_collection = rail.QueryCollectionOperator(
            task_id='query_list_in_final_payroll_collection',
            query=f"""SELECT * FROM finalpayrolldata WHERE finalpayrolldata.SUBTY = "2000" AND finalpayrolldata.Employee_Type_Name IN {config.employee_type}
                    UNION ALL
                SELECT * FROM finalpayrolldata WHERE finalpayrolldata.SUBTY <> "2000" AND finalpayrolldata.SUBTY <> "2602"
                    UNION ALL
                SELECT * FROM finalpayrolldata WHERE finalpayrolldata.SUBTY = "2602" AND finalpayrolldata.Employee_Type_Name NOT IN {config.employee_type}
            """
        )

        has_item_data = rail.IfOperator(
            task_id='has_item_data',
            test="{{ result('query_list_in_final_payroll_collection','length') > 0 }}",
            yes_task='compose_item_payroll_csv_file',
            no_task='finish_export_no_payroll_data'
        )

        compose_item_payroll_csv_file = rail.WriteCSVFileOperator(
            task_id='compose_item_payroll_csv_file',
            source="{{ result('query_list_in_final_payroll_collection') }}",
            header=["RECTY", "CLIID", "INTCA", "ORDNO", "IOPER", "INFTY", "SUBTY", "BEGDA",
                    "ENDDA", "OBJPS", "SPRPS", "SEQNR", "EXTRA", "LGART", "STDAZ", "BEGUZ", "ENDUZ", "BETRG", "WAERS",
                    "ANZHL", "ZEINH", "VTKEN", "BWGRL", "AUFKZ", "ENDOF", "UFLD1", "UFLD2", "UFLD3", "KEYPR", "TRFGR", "TRFST", "PRAKN", "PRAKZ",
                    "OTYPE", "PLANS", "VERSL", "EXBEL", "WTART", "TDLANGU", "TDSUBLA", "TDTYPE"],
            row=request_payload.get_compose_item_payroll_aus_data_row,
            thread_pool_size=config.thread_pool_size_write_csv
        )

        no_of_records_size_including_header_footer = rail.PythonOperator(
            task_id="no_of_records_size_including_header_footer",
            python_callable=lambda:  int(rail.result(
                'query_list_in_final_payroll_collection', 'length')) + 2
        )

        create_document = rail.RenderTemplateOperator(
            task_id='create_document',
            target='artifact',
            template_file='schema/usa_payroll_export_data.txt',
            dataset="{{ result('compose_item_payroll_csv_file') }}",
        )

        pgp_encyrpt_item_file = rail.PGPEncryptionOperator(
            task_id="pgp_encyrpt_item_file",
            source="{{ result('create_document') }}",
            pgp_conn_id=config.pgp_conn_id,
            sign=True
        )

        logging_no_of_records_exported = rail.WriteLogOperator(
            task_id="logging_no_of_records_exported",
            log="{{ result('create_log') }}",
            message="{{ result('process_start_time').start_time }} - INFO admin No of records exported" +
            " = {{result('query_list_in_final_payroll_collection','length')}}",
            properties={
                "log": "{{ result('process_start_time').start_time }} - INFO admin No of records exported" +
                " = {{result('query_list_in_final_payroll_collection','length')}}",
            }
        )

        logging_file_creation = rail.WriteLogOperator(
            task_id="logging_file_creation",
            log="{{ result('create_log') }}",
            message="{{ result('process_start_time').start_time }} - INFO admin Export File : " +
            "log_payroll_export{{ result('process_start_time').start_time }}" + ".txt",

        )

        process_end_time = rail.PythonOperator(
            task_id="process_end_time",
            python_callable=python_callable.get_time_in_formats,
            op_args=[config.time_zone]
        )

        logging_job_end_time = rail.WriteLogOperator(
            task_id="logging_job_end_time",
            log="{{ result('create_log') }}",
            message="{{ result('process_end_time').start_time }} - Process ended",
            properties={
                "log": "{{ result('process_end_time').start_time }} - Process ended"
            }
        )

        log_file_data_to_csv = rail.WriteCSVFileOperator(
            task_id="log_file_data_to_csv",
            source="{{ result('create_log') }}",
            header=None,
            row=[
                '{{ item.properties | attr_or_default("log", "") }}'
            ],
            thread_pool_size=config.thread_pool_size_write_csv
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_log_to_sftp",
            content="{{ result('log_file_data_to_csv') }}",
            remote_filepath=config.output_filepath +
            "/log_payroll_export{{ result('process_start_time').ymd_format }}{{ result('process_start_time').hms_format }}" + ".txt"
        )

        upload_payroll_item_file_secondary_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_payroll_item_file_secondary_sftp",
            sftp_conn_id=config.secondary_encrypted_sftp_conn_id,
            content="{{ result('create_document')}}",
            remote_filepath=config.secondary_output_filepath +
            "/{{ result('get_file_name')}}.SAP"
        )

        upload_encrypted_file_to_secondary_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_encrypted_file_to_secondary_sftp",
            sftp_conn_id=config.secondary_encrypted_sftp_conn_id,
            content="{{ result('pgp_encyrpt_item_file') }}",
            remote_filepath=config.secondary_encrypted_output_filepath +
            "/{{ result('get_file_name')}}.SAP.pgp"
        )

        upload_encrypted_payroll_file_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_encrypted_payroll_file_to_sftp",
            content="{{ result('pgp_encyrpt_item_file') }}",
            remote_filepath=config.output_filepath +
            "/{{ result('get_file_name')}}.SAP.pgp"
        )

        fail_sftp_upload_error = rail.FailOperator(
            task_id='fail_sftp_upload_error',
            message="{{ get_error_message() }}"
        )

        send_email_for_sftp_failure = rail.EmailOperator(
            task_id='send_email_for_sftp_failure',
            trigger_rule='one_failed',
            to=config.alert_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon Payroll Data Export - Failed while uploading the file - {{ result("process_start_time").start_time }}',
            params={
                'output_filepath': config.output_filepath,
            },
            html_content="templates/email/sftp_failure.html",
            files=[
                ("{{ result('get_file_name')}}.SAP.pgp", '{{result("pgp_encyrpt_item_file")}}')]
        )

        send_email_for_export_copmpletion = rail.EmailOperator(
            task_id='send_email_for_export_copmpletion',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon Payroll Data Export completed for ' + \
            config.export_location + \
            ' {{ result("process_start_time").start_time }}',
            params={
                'output_filepath': config.output_filepath
            },
            html_content="/templates/email/export_success.html"
        )

        log_to_sumo_valid_export = rail.SendToSumoOperator(
            task_id="log_to_sumo_valid_export",
            data={
                'job_start_time': '{{ result("process_start_time").start_time }}',
                'job_end_time': f'{OPEN_BRACKETS} current_time_in_specified_tz("{config.time_zone}", "%Y-%m-%dT%H:%M:%S") {CLOSE_BRACKETS}',
                'export_file_name': '{{ result("get_file_name")}}',
                'Location':'US',
                'export_filepath': config.output_filepath,
                'numberofrecords': "{{ result('query_list_in_final_payroll_collection', 'length')}}",
            },
            sumo_conn_id="sumologic-exportlogger"
        )

        can_process_run >> process_start_time >> create_log >> logging_job_start_time >> get_adp_payroll_script >> is_file_format_script_present
        is_file_format_script_present >> rail.Label("Yes") >> get_user_report_details >> genarate_user_report >> is_report_failed >> rail.Label("No") >> has_data >> rail.Label("Yes") >> users_report_payload_to_csv >> create_object\
            >> wait_create_object >> create_object_uris >> get_file_name \
            >> create_payroll_download_batch >> execute_payroll_download_batch >> wait_for_payroll_download_batch \
            >> get_payroll_run_batch_result >> download_payload_file_from_url >> load_payload_file \
            >> create_payroll_data_collection >> has_payroll_data
        has_payroll_data >> rail.Label(
            'Yes') >> create_payrun_batch >> execute_payrun_batch >> wait_forpayrun_batch >> get_payrun_batch_result
        has_payroll_data >> rail.Label('No') >> finish_export_no_payroll_data
        get_payrun_batch_result >> update_payrun_name >> create_payrun_download_batch >> execute_payrun_download_batch \
            >> wait_for_payrun_download_batch >> get_payrun_download_batch_result >> mark_payrun_as_complete >> rail.Label(
                "on_success") >> download_final_payload_file_from_url
        mark_payrun_as_complete >> rail.Label(
            "on_error") >> on_error >> cancel_payrun >> fail_export
        download_final_payload_file_from_url >> load_final_payload_file >> create_final_payroll_data_collection

        is_report_failed >> rail.Label("Yes") >> fail_report_generation

        create_final_payroll_data_collection >> query_final_payroll_data_without_empid >> has_empty_empid_data >> rail.Label(
            'Yes') >> mark_payrun_as_draft >> cancel_payrun >> fail_export >> invalid_records >> generate_download_link >> send_invalid_records_email
        has_empty_empid_data >> rail.Label(
            'No') >> get_all_employee_types_from_mapper >> query_list_in_final_payroll_collection >> has_item_data >> rail.Label(
            'Yes') >> compose_item_payroll_csv_file >> no_of_records_size_including_header_footer >> create_document \
            >> pgp_encyrpt_item_file >> logging_no_of_records_exported >> logging_file_creation >> process_end_time >> logging_job_end_time\
            >> log_file_data_to_csv >> upload_log_to_sftp >> upload_payroll_item_file_secondary_sftp \
            >> upload_encrypted_file_to_secondary_sftp >> upload_encrypted_payroll_file_to_sftp
        has_item_data >> rail.Label(
            'No') >> finish_export_no_payroll_data >> send_email_for_no_payroll_data
        upload_payroll_item_file_secondary_sftp >> rail.Label(
            "on_success") >> send_email_for_export_copmpletion >> log_to_sumo_valid_export
        upload_payroll_item_file_secondary_sftp >> rail.Label(
            "on_error") >> send_email_for_sftp_failure >> fail_sftp_upload_error

    return dag


rail.for_each_instance(create_main_dag)
