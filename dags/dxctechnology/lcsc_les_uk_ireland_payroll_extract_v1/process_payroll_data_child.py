# pylint: disable=too-many-statements
import pendulum as pd
import rail
from dxctechnology.lcsc_les_uk_ireland_payroll_extract_v1.utils import request_payload
from dxctechnology.lcsc_les_uk_ireland_payroll_extract_v1.utils import response_filter

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_payroll_data_child_dag_id,
        description=f'DXC Location Company Codewise PayrollData Export Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        process_start_time = rail.PythonOperator(
            task_id="process_start_time",
            python_callable=lambda: pd.now(config.time_zone).strftime("%Y-%m-%dT%H:%M:%S")
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

        create_payrun_batch = rail.RepliconServiceOperator(
            task_id="create_payrun_batch",
            endpoint="/services/PayRunService1.svc/CreatePayRunBatch",
            data=request_payload.get_create_payrun_batch_payload
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
                "name":  "{{ dag_run.conf.file_name }}"
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

        create_complete_payrun_status_batch = rail.RepliconServiceOperator(
            task_id='create_complete_payrun_status_batch',
            endpoint="/services/PayRunService1.svc/CreateMarkPayRunAsCompleteBatch",
            data={
                "target": {
                    "uri": "{{ result('get_payrun_batch_result').payRunUri }}"
                }
            }
        )

        execute_complete_payrun_status_batch, wait_for_complete_payrun_status_batch = rail.batch_execution(
            'complete_payrun_status_batch', create_complete_payrun_status_batch.task_id)

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
            query='SELECT * From finalpayrolldata WHERE NULLIF(CLIID, "") IS NULL'
        )

        has_empty_empid_data = rail.IfOperator(
            task_id='has_empty_empid_data',
            test="{{ result('query_final_payroll_data_without_empid','length') > 0 }}",
            yes_task='mark_payrun_as_draft',
            no_task='get_all_pay_codes_from_mapper'
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

        create_cancel_payrun_status_batch = rail.RepliconServiceOperator(
            task_id='create_cancel_payrun_status_batch',
            endpoint="/services/PayRunService1.svc/CreateModifyPayRunStatusBatch",
            data={
                "target": {
                    "uri": "{{ result('get_payrun_batch_result').payRunUri }}"
                },
                "payRunStatusUri": "urn:replicon:pay-run-status:cancelled"
            }
        )

        execute_cancel_payrun_status_batch, wait_for_cancel_payrun_status_batch = rail.batch_execution(
            'cancel_payrun_status_batch', create_cancel_payrun_status_batch.task_id)

        fail_export = rail.FailOperator(
            task_id="fail_export",
            message="Employee ID not present for some users. Users available to validate in payrun \
                '{{ dag_run.conf.file_name }}'"
        )

        get_all_pay_codes_from_mapper= rail.PythonOperator(
            task_id= 'get_all_pay_codes_from_mapper',
            python_callable=request_payload.get_all_required_paycodes,
            op_args=[config.lcsc_wage_codes_mapper, config.les_wage_codes_mapper]
        )

        query_list_in_final_payroll_collection = rail.QueryCollectionOperator(
            task_id='query_list_in_final_payroll_collection',
            query='''SELECT * FROM finalpayrolldata WHERE Pay_Code_Code IN ({{result('get_all_pay_codes_from_mapper')}})'''
        )

        has_item_data = rail.IfOperator(
            task_id='has_item_data',
            test="{{ result('query_list_in_final_payroll_collection','length') > 0 }}",
            yes_task='compose_item_payroll_csv_file',
            no_task='finish_export_no_payroll_data'
        )

        finish_export_no_payroll_data = rail.EmptyOperator(
            task_id='finish_export_no_payroll_data'
        )

        compose_item_payroll_csv_file = rail.WriteCSVFileOperator(
            task_id='compose_item_payroll_csv_file',
            source="{{ result('query_list_in_final_payroll_collection') }}",
            header=["RECTY","CLIID","INTCA","ORDNO","IOPER","INFTY","paycodecode","BEGDA",
            "ENDDA","OBJPS","SPRPS","SEQNR","EXTRA","paycodecode2","STDAZ","BEGUZ","ENDUZ","BETRG","WAERS",
            "PayCodeHours","ZEINH"],
            row=lambda item: request_payload.get_compose_item_payroll_data_row(
                item, 
                config.lcsc_wage_codes_mapper,
                config.les_wage_codes_mapper
            )
        )

        logging_no_of_records_exported = rail.WriteLogOperator(
            task_id="logging_no_of_records_exported",
            log="{{ result('create_log') }}",
            message="{{ current_time() }} - INFO admin No of records exported = {{result('query_list_in_final_payroll_collection','length')}}",
            properties={
                "log": "{{ current_time() }} - INFO admin No of records exported = {{result('query_list_in_final_payroll_collection','length')}}",
            }
        )

        no_of_records_size_including_header_footer=rail.PythonOperator(
            task_id="no_of_records_size_including_header_footer",
            python_callable=lambda: int(rail.result('query_list_in_final_payroll_collection','length')) + 2
        )

        create_document = rail.RenderTemplateOperator(
            task_id='create_document',
            target='artifact',
            template_file='/schema/lcsc_les_payroll_export_data.txt',
            dataset="{{ result('compose_item_payroll_csv_file') }}",
        )

        pgp_encrypt_item_file = rail.PGPEncryptionOperator(
            task_id="pgp_encrypt_item_file",
            source="{{ result('create_document') }}",
            pgp_conn_id=config.pgp_conn_id
        )

        upload_payroll_item_file_secondary_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_payroll_item_file_secondary_sftp",
            sftp_conn_id=config.secondary_sftp_conn_id,
            content="{{ result('create_document')}}",
            remote_filepath=config.secondary_output_filepath +
            "{{ dag_run.conf.file_name }}.SAP"
        )

        upload_encrypted_payroll_item_file_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_encrypted_payroll_item_file_sftp",
            content="{{ result('pgp_encrypt_item_file') }}",
            remote_filepath=config.output_filepath +
            "{{ dag_run.conf.file_name }}.SAP.pgp"
        )

        can_upload_to_tertiary_sftp = rail.IfOperator(
            task_id = 'can_upload_to_tertiary_sftp',
            test= config.can_upload_to_tertiary_sftp,
            yes_task='pgp_encrypt_for_tertiary_sftp',
            no_task='finish'
        )

        finish = rail.EmptyOperator(
            task_id = "finish"
        )

        # this encryption is for uploading encrypted file to Replicon SFTP(Tertiary SFTP)
        pgp_encrypt_for_tertiary_sftp = rail.PGPEncryptionOperator(
            task_id="pgp_encrypt_for_tertiary_sftp",
            source="{{ result('create_document') }}",
            pgp_conn_id=config.tertiary_pgp_conn_id
        )

        upload_encrypted_payroll_file_tertiary_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_encrypted_payroll_file_tertiary_sftp",
            sftp_conn_id=config.tertiary_sftp_conn_id,
            content="{{ result('pgp_encrypt_for_tertiary_sftp') }}",
            remote_filepath=config.tertiary_encrypted_filepath + "{{ dag_run.conf.location_name }}/" + "{{ dag_run.conf.file_name }}.SAP.pgp"
        )

        fail_tertiary_sftp_upload_error = rail.FailOperator(
            task_id='fail_tertiary_sftp_upload_error',
            trigger_rule='one_failed',
            message=config.error_template
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
            subject='{{ get_company_key() }} | Replicon payroll export for {{ dag_run.conf.region }} {{ dag_run.conf.location_name }} {{ dag_run.conf.division_code }} - SFTP failure - {{ current_time_in_specified_tz() }}',
            params={
                'output_filepath': config.output_filepath,
            },
            html_content="/templates/emails/email_for_sftp_failure.html",
            files=[
                ("{{ dag_run.conf.file_name }}.SAP.pgp", '{{result("pgp_encrypt_item_file")}}')]
        )

        logging_file_creation = rail.WriteLogOperator(
            task_id="logging_file_creation",
            log="{{ result('create_log') }}",
            message="{{ current_time() }} - INFO admin Export File_{{ dag_run.conf.file_name }}.SAP",
            properties={
                "log": "{{ current_time() }} - INFO admin Export File_{{ dag_run.conf.file_name }}.SAP"
            }
        )

        process_end_time = rail.PythonOperator(
            task_id="process_end_time",
            python_callable=lambda: pd.now(config.time_zone).strftime("%Y-%m-%dT%H:%M:%S")
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
            remote_filepath=config.log_filepath + "log_{{ dag_run.conf.file_name }}.txt"
        )

        can_upload_logs_to_tertiary_sftp = rail.IfOperator(
            task_id = 'can_upload_logs_to_tertiary_sftp',
            test= config.can_upload_to_tertiary_sftp,
            yes_task='upload_log_data_to_tertiary_sftp',
            no_task='finish_log'
        )

        finish_log = rail.EmptyOperator(
            task_id = "finish_log"
        )

        upload_log_data_to_tertiary_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_log_data_to_tertiary_sftp",
            sftp_conn_id=config.tertiary_sftp_conn_id,
            content='{{result("log_file_data_to_csv")}}',
            remote_filepath=config.tertiary_log_filepath + "{{ dag_run.conf.location_name }}/logs/" + "log_{{ dag_run.conf.file_name }}.txt"
        )

        fail_tertiary_sftp_log_upload_error = rail.FailOperator(
            task_id='fail_tertiary_sftp_log_upload_error',
            trigger_rule='one_failed',
            message=config.error_template
        )

        send_email_for_export_completion = rail.EmailOperator(
            task_id='send_email_for_export_completion',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon payroll export for {{ dag_run.conf.region }} {{ dag_run.conf.location_name }} {{ dag_run.conf.division_code }} completed successfully - {{ current_time_in_specified_tz() }}',
            params={
                'output_filepath': config.output_filepath,
                'log_filepath': config.log_filepath,
            },
            html_content="/templates/emails/email_for_export_success.html"
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
            subject='{{ get_company_key() }} | Replicon payroll export for {{ dag_run.conf.region }} {{ dag_run.conf.location_name }} {{ dag_run.conf.division_code }} - SFTP failure - {{ current_time_in_specified_tz() }}',
            params={
                'log_filepath': config.log_filepath,
            },
            html_content="/templates/emails/email_for_log_upload_failure.html",
            files=[
                ("log_{{ dag_run.conf.file_name }}.txt", '{{result("log_file_data_to_csv")}}')]
        )

        process_start_time >> get_location_child_hierarchy_data >> get_contractor_employee_type_child_hierarchy_data >> create_log >> \
            logging_job_start_time >> logging_location
        logging_location >> create_payrun_batch >> execute_payrun_batch >> wait_forpayrun_batch >> get_payrun_batch_result
        get_payrun_batch_result >> update_payrun_name >> create_payrun_download_batch >> execute_payrun_download_batch \
            >> wait_for_payrun_download_batch >> get_payrun_download_batch_result \
                >> create_complete_payrun_status_batch >> execute_complete_payrun_status_batch
        wait_for_complete_payrun_status_batch >> rail.Label("on_success") >> download_final_payload_file_from_url
        wait_for_complete_payrun_status_batch >> rail.Label("on_error") >> catch_error >> create_cancel_payrun_status_batch
        download_final_payload_file_from_url >> load_final_payload_file >> create_final_payroll_data_collection

        create_final_payroll_data_collection >> query_final_payroll_data_without_empid >> has_empty_empid_data >> rail.Label(
            'Yes') >> mark_payrun_as_draft >> create_cancel_payrun_status_batch
        create_cancel_payrun_status_batch >> execute_cancel_payrun_status_batch
        wait_for_cancel_payrun_status_batch >> fail_export
        has_empty_empid_data >> rail.Label(
            'No') >> get_all_pay_codes_from_mapper >> query_list_in_final_payroll_collection >> has_item_data >> rail.Label(
            'Yes') >> compose_item_payroll_csv_file >> logging_no_of_records_exported >> no_of_records_size_including_header_footer \
                >> create_document >> upload_payroll_item_file_secondary_sftp>> [pgp_encrypt_item_file, can_upload_to_tertiary_sftp]

        can_upload_to_tertiary_sftp >> rail.Label('Yes') >> pgp_encrypt_for_tertiary_sftp >> upload_encrypted_payroll_file_tertiary_sftp
        upload_encrypted_payroll_file_tertiary_sftp >> rail.Label("on_error") >> fail_tertiary_sftp_upload_error

        can_upload_to_tertiary_sftp >> rail.Label('No') >> finish
        pgp_encrypt_item_file >> upload_encrypted_payroll_item_file_sftp
        has_item_data >> rail.Label('No') >> finish_export_no_payroll_data
        upload_encrypted_payroll_item_file_sftp >> rail.Label(
            "on_success") >> logging_file_creation >> process_end_time >> logging_job_end_time >> log_file_data_to_csv
        log_file_data_to_csv >> [upload_log_data_to_sftp, can_upload_logs_to_tertiary_sftp]

        can_upload_logs_to_tertiary_sftp >> rail.Label('Yes') >> upload_log_data_to_tertiary_sftp
        upload_log_data_to_tertiary_sftp >> rail.Label('on_error') >> fail_tertiary_sftp_log_upload_error
        can_upload_logs_to_tertiary_sftp >> rail.Label('No') >> finish_log

        upload_log_data_to_sftp >> rail.Label(
            "on_success") >> send_email_for_export_completion
        upload_encrypted_payroll_item_file_sftp >> rail.Label("on_error") >> send_email_for_sftp_failure >> fail_sftp_upload_error
        upload_log_data_to_sftp >> rail.Label("on_error") >> send_email_for_log_upload_failure >> fail_log_upload_error
    return dag


rail.for_each_instance(create_child_dag)
