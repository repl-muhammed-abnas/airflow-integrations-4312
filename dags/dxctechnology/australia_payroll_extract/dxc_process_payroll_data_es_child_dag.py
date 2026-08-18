# pylint: disable=too-many-statements
from datetime import datetime as dt, timedelta
import rail
from dxctechnology.australia_payroll_extract.utils import request_payload
from dxctechnology.australia_payroll_extract.utils import response_filter


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_australia_payrolldata_export_es_child_{config.instance}',
        description=f'DXC_Australia_PayrollData_Export_ES_Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_pay_groups_data = rail.RepliconServiceOperator(
            task_id="get_pay_groups_data",
            endpoint="/services/ServiceCenterService1.svc/GetAllServiceCenters",
            response_filter=response_filter.get_pay_groups_data
        )

        get_location_child_hierarchy_data = rail.RepliconServiceOperator(
            task_id="get_location_child_hierarchy_data",
            endpoint="/services/LocationListService1.svc/GetChildHierarchyData",
            data=request_payload.get_location_child_hierarchy_param,
            response_filter=response_filter.convert_location_hierarchy
        )

        get_contractor_employee_type_child_hierarchy_data = rail.RepliconServiceOperator(
            task_id="get_contractor_employee_type_child_hierarchy_data",
            endpoint="/services/EmployeeTypeGroupListService1.svc/GetChildHierarchyData",
            data=request_payload.get_employee_type_child_hierarchy_param,
            response_filter=response_filter.convert_employee_type_hierarchy
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        get_file_name = rail.PythonOperator(
            task_id="get_file_name",
            python_callable=lambda: config.file_name_prefix +
            "_" + dt.utcnow().strftime("%Y%m%d%H%M%S")
            + "_AUREPL_RECP" + request_payload.get_sequence_no('sequence_no_for_2010') + "_DUT8G2I"
        )

        process_start_time = rail.PythonOperator(
            task_id="process_start_time",
            python_callable=lambda:  dt.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        )

        process_start_time_ymd_format = rail.PythonOperator(
            task_id="process_start_time_ymd_format",
            python_callable=lambda:  dt.utcnow().strftime("%Y%m%d")
        )

        process_start_time_hms_format = rail.PythonOperator(
            task_id="process_start_time_hms_format",
            python_callable=lambda:  dt.utcnow().strftime("%H%M%S")
        )

        logging_job_start_time = rail.WriteLogOperator(
            task_id="logging_job_start_time",
            log="{{ result('create_log') }}",
            message="{{result('process_start_time')}} - Process started",
            properties={
                "log": "{{result('process_start_time')}} - Process started"
            }
        )

        logging_location = rail.WriteLogOperator(
            task_id="logging_location",
            log="{{ result('create_log') }}",
            message="Territory : {{dag_run.conf.location_name}}",
            properties={
                "log": "Territory : {{dag_run.conf.location_name}}"
            }
        )

        create_payroll_download_batch = rail.RepliconServiceOperator(
            task_id="create_payroll_download_batch",
            endpoint="/services/PayRunService1.svc/CreatePayrollDownloadBatch",
            data=lambda: request_payload.get_create_payroll_download_batch_payload(
                config.duration_days)
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

        create_payrun_batch = rail.RepliconServiceOperator(
            task_id="create_payrun_batch",
            endpoint="/services/PayRunService1.svc/CreatePayRunBatch",
            data=lambda: request_payload.get_create_payrun_batch_payload(
                config.duration_days)
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
            no_task='query_list_in_final_absence_payroll_collection'
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

        query_list_in_final_absence_payroll_collection = rail.QueryCollectionOperator(
            task_id='query_list_in_final_absence_payroll_collection',
            name='final_payroll_item_data',
            query="""SELECT * FROM finalpayrolldata WHERE Pay_Code_Code NOT IN ('2082', '2087', '9034')"""
        )

        get_all_time_off_pay_codes_from_mapper= rail.PythonOperator(
            task_id= 'get_all_time_off_pay_codes_from_mapper',
            python_callable=lambda: request_payload.get_all_required_pacodes(config.time_off_mapper)
        )

        query_list_in_final_payroll_collection = rail.QueryCollectionOperator(
            task_id='query_list_in_final_payroll_collection',
            query='''SELECT * FROM final_payroll_item_data WHERE Pay_Code_Code NOT IN ({{result('get_all_time_off_pay_codes_from_mapper')}})'''
        )

        has_item_data = rail.IfOperator(
            task_id='has_item_data',
            test="{{ result('query_list_in_final_payroll_collection','length') > 0 }}",
            yes_task='process_absence_taken',
            no_task='finish_export_no_payroll_data'
        )

        process_absence_taken = rail.TriggerDagRunOperator(
            task_id='process_absence_taken',
            retries=0,
            trigger_dag_id=f'dxctechnology_australia_payrolldata_export_absence_taken_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.process_absence_taken_es_user_conf
        )

        compose_item_payroll_csv_file = rail.WriteCSVFileOperator(
            task_id='compose_item_payroll_csv_file',
            source="{{ result('query_list_in_final_payroll_collection') }}",
            header=["RECTY", "CLIID", "INTCA", "ORDNO", "IOPER", "INFTY", "SUBTY", "BEGDA",
                    "ENDDA", "OBJPS", "SPRPS", "SEQNR", "EXTRA", "LGART", "STDAZ", "BEGUZ", "ENDUZ", "BETRG", "WAERS",
                    "ANZHL", "ZEINH", "VTKEN", "BWGRL", "AUFKZ", "ENDOF", "UFLD1", "UFLD2", "UFLD3", "KEYPR", "TRFGR", "TRFST", "PRAKN", "PRAKZ",
                    "OTYPE", "PLANS", "VERSL", "EXBEL", "WTART", "TDLANGU", "TDSUBLA", "TDTYPE"],
            row=request_payload.get_compose_item_payroll_aus_data_row,
        )

        logging_no_of_records_exported = rail.WriteLogOperator(
            task_id="logging_no_of_records_exported",
            log="{{ result('create_log') }}",
            message="{{ current_time() }} - INFO admin No of records exported = {{result('query_list_in_final_payroll_collection','length')}}",
            properties={
                "log": "{{ current_time() }} - INFO admin No of records exported = {{result('query_list_in_final_payroll_collection','length')}}",
            }
        )

        no_of_records_size_including_header_footer = rail.PythonOperator(
            task_id="no_of_records_size_including_header_footer",
            python_callable=lambda:  int(rail.result(
                'query_list_in_final_payroll_collection', 'length')) + 2
        )

        create_document = rail.RenderTemplateOperator(
            task_id='create_document',
            target='artifact',
            template_file='schema/aus_payroll_export_data.txt',
            dataset="{{ result('compose_item_payroll_csv_file') }}",
        )

        pgp_encyrpt_item_file = rail.PGPEncryptionOperator(
            task_id="pgp_encyrpt_item_file",
            source="{{ result('create_document') }}",
            pgp_conn_id=config.pgp_conn_id
        )

        upload_payroll_item_file_secondary_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_payroll_item_file_secondary_sftp",
            sftp_conn_id=config.secondary_sftp_conn_id,
            content="{{ result('create_document')}}",
            remote_filepath=config.secondary_output_filepath +
            "{{ result('get_file_name')}}.SAP"
        )

        upload_encrypted_file_to_secondary_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_encrypted_file_to_secondary_sftp",
            sftp_conn_id=config.secondary_encrypted_sftp_conn_id,
            content="{{ result('pgp_encyrpt_item_file') }}",
            remote_filepath=config.secondary_encrypted_output_filepath +
            "{{ result('get_file_name')}}.SAP.pgp"
        )

        upload_encrypted_payroll_item_file_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_encrypted_payroll_item_file_sftp",
            content="{{ result('pgp_encyrpt_item_file') }}",
            remote_filepath=config.output_filepath +
            "{{ result('get_file_name')}}.SAP.pgp"
        )

        fail_sftp_upload_error = rail.FailOperator(
            task_id='fail_sftp_upload_error',
            message=config.error_template
        )

        send_email_for_sftp_failure = rail.EmailOperator(
            task_id='send_email_for_sftp_failure',
            trigger_rule='one_failed',
            to=config.alert_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon payroll export for Australia - SFTP failure for {{dag_run.conf.location_name}}-{{dag_run.conf.division_name}}-{{current_time_in_specified_tz()}}',
            params={
                'output_filepath': config.output_filepath,
            },
            html_content="templates/email/sftp_failure.html",
            files=[
                ("{{ result('get_file_name')}}.SAP.pgp", '{{result("pgp_encyrpt_item_file")}}')]
        )

        logging_file_creation = rail.WriteLogOperator(
            task_id="logging_file_creation",
            log="{{ result('create_log') }}",
            message="{{ current_time() }} - INFO admin Export File_" +
            "{{ result('get_file_name')}}",
            properties={
                "log": " {{ current_time() }} - INFO admin Export File_" +
                "{{ result('get_file_name')}}" + ".txt"
            }
        )

        process_end_time = rail.PythonOperator(
            task_id="process_end_time",
            python_callable=lambda:  dt.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
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

        upload_log_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_log_data_to_sftp",
            content='{{result("log_file_data_to_csv")}}',
            remote_filepath=config.log_filepath + "log_" +
            "{{ result('get_file_name')}}" + ".txt"
        )

        send_email_for_export_copmpletion = rail.EmailOperator(
            task_id='send_email_for_export_copmpletion',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon payroll export for Australia completed for - {{dag_run.conf.location_name}} - {{dag_run.conf.division_name}} - {{current_time_in_specified_tz()}}',
            params={
                'output_filepath': config.output_filepath,
                'log_filepath': config.log_filepath,
            },
            html_content="templates/email/export_success.html"
        )

        fail_log_upload_error = rail.FailOperator(
            task_id='fail_log_upload_error',
            message=config.error_template
        )

        send_email_for_log_upload_failure = rail.EmailOperator(
            task_id='send_email_for_log_upload_failure',
            trigger_rule='one_failed',
            to=config.alert_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon payroll export for Australia - SFTP failure for {{dag_run.conf.location_name}}-{{dag_run.conf.division_name}}-{{current_time_in_specified_tz()}}',
            params={
                'log_filepath': config.log_filepath,
            },
            html_content="templates/email/log_upload_failure.html",
            files=[
                ("log_{{ result('get_file_name')}}", '{{result("log_file_data_to_csv")}}')]
        )

        # pylint: disable=line-too-long
        get_pay_groups_data >> get_location_child_hierarchy_data >> get_contractor_employee_type_child_hierarchy_data
        get_contractor_employee_type_child_hierarchy_data >> create_log >> get_file_name >> process_start_time >> \
            process_start_time_ymd_format >> process_start_time_hms_format >> logging_job_start_time >> logging_location
        logging_location >> create_payroll_download_batch >> execute_payroll_download_batch >> wait_for_payroll_download_batch >> \
            get_payroll_run_batch_result >> download_payload_file_from_url >> load_payload_file >> \
            create_payroll_data_collection >> has_payroll_data
        has_payroll_data >> rail.Label(
            'Yes') >> create_payrun_batch >> execute_payrun_batch >> wait_forpayrun_batch >> get_payrun_batch_result
        has_payroll_data >> rail.Label('No') >> finish_export_no_payroll_data
        get_payrun_batch_result >> update_payrun_name >> create_payrun_download_batch >> execute_payrun_download_batch \
            >> wait_for_payrun_download_batch >> get_payrun_download_batch_result >> mark_payrun_as_complete >> rail.Label(
                "on_success") >> download_final_payload_file_from_url
        mark_payrun_as_complete >> rail.Label(
            "on_error") >> catch_error >> cancel_payrun >> fail_export
        download_final_payload_file_from_url >> load_final_payload_file >> create_final_payroll_data_collection

        create_final_payroll_data_collection >> query_final_payroll_data_without_empid >> has_empty_empid_data >> rail.Label(
            'Yes') >> mark_payrun_as_draft >> cancel_payrun >> fail_export
        has_empty_empid_data >> rail.Label(
            'No') >> query_list_in_final_absence_payroll_collection >> get_all_time_off_pay_codes_from_mapper >> query_list_in_final_payroll_collection >> has_item_data >> rail.Label(
            'Yes') >> process_absence_taken >> compose_item_payroll_csv_file >> logging_no_of_records_exported >> no_of_records_size_including_header_footer >> create_document >>\
            upload_payroll_item_file_secondary_sftp >> pgp_encyrpt_item_file >> upload_encrypted_file_to_secondary_sftp >> upload_encrypted_payroll_item_file_sftp
        has_item_data >> rail.Label('No') >> finish_export_no_payroll_data
        upload_encrypted_payroll_item_file_sftp >> rail.Label(
            "on_success") >> logging_file_creation >> process_end_time >> logging_job_end_time >> log_file_data_to_csv
        log_file_data_to_csv >> upload_log_data_to_sftp >> rail.Label(
            "on_success") >> send_email_for_export_copmpletion
        upload_encrypted_payroll_item_file_sftp >> rail.Label(
            "on_error") >> send_email_for_sftp_failure >> fail_sftp_upload_error
        upload_log_data_to_sftp >> rail.Label(
            "on_error") >> send_email_for_log_upload_failure >> fail_log_upload_error
    return dag


rail.for_each_instance(create_child_dag)
