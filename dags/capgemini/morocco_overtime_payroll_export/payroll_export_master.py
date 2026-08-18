from datetime import timedelta
import json
from pendulum import datetime
import pendulum
from capgemini.morocco_overtime_payroll_export.utils import custom_methods, request_payload
from airflow.models import Variable
import rail

# pylint: disable=too-many-statements
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f"Capgemini Morocco Overtime Payroll Export Master {config.instance}",
        company_key=config.company_key,
        start_date=datetime(2024, 9, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        default_args={
            "sftp_conn_id": config.sftp_conn_id,
            'retries': 0
        }
    ) as dag:

        current_date = pendulum.now(config.time_zone)

        is_valid_scheduled_run = rail.IfOperator(
            task_id='is_valid_scheduled_run',
            test=lambda: current_date.strftime("%d/%m/%Y") in config.schedules,
            yes_task='logging_details'
        )

        logging_details = rail.PythonOperator(
            task_id='logging_details',
            python_callable=custom_methods.get_logging_details,
            op_args=[config.time_zone, config.ma01_filename_prefix, config.ma02_ma03_filename_prefix]
        )

        get_morocco_overtime_payroll_script = rail.RepliconServiceOperator(
            task_id="get_morocco_overtime_payroll_script",
            endpoint="/services/PayrollDownloadScriptAdministrationService1.svc/GetAllScripts",
            response_filter=lambda response: rail.find_first_by_attr_and_get_attr(response.json()['d'],
                'displayText', config.payroll_export_file_format, 'uri')
        )

        is_file_format_script_present = rail.IfOperator(
            task_id='is_file_format_script_present',
            test='{{ result("get_morocco_overtime_payroll_script") | is_truthy }}',
            yes_task='get_allowed_location_uris'
        )

        get_allowed_location_uris = rail.RepliconServiceOperator(
            task_id='get_allowed_location_uris',
            endpoint="/services/LocationService1.svc/GetPageOfAvailableLocationsByTextSearch",
            data=request_payload.get_location_uri_payload(config.location),
            data_handler=lambda response: custom_methods.get_location_uri(
                response, config.location)
        )

        get_allowed_costcenter_uris = rail.RepliconServiceCallForEachItemOperator(
            task_id='get_allowed_costcenter_uris',
            endpoint="/services/CostCenterService1.svc/GetPageOfAvailableCostCentersByTextSearch",
            items=[config.ma01_costcenter, config.ma02_costcenter, config.ma03_costcenter],
            data=request_payload.get_cost_center_payload,
            data_handler=lambda response, item: custom_methods.get_costcenter_uri(response, item)
        )

        get_costcenter_hierarchy_data = rail.RepliconServiceOperator(
            task_id='get_costcenter_hierarchy_data',
            endpoint='/services/CostCenterListService1.svc/GetData',
            data={
                "page": "1",
                "pagesize": "10000000",
                "columnUris": [
                    "urn:replicon:cost-center-list-column:cost-center",
                    "urn:replicon:cost-center-list-column:full-path"
                ]
            },
            data_handler=lambda response: custom_methods.filter_costcenter_hierarchy(response, config),
            target='artifact'
        )

        create_payrun_batch = rail.RepliconServiceOperator(
            task_id="create_payrun_batch",
            endpoint="/services/PayRunService1.svc/CreatePayRunBatch",
            data=lambda: request_payload.get_create_payroll_batch_payload(config.time_zone)
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
                "name":  "{{ result('logging_details').payroll_name }}"
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

        download_final_payload_file_from_url = rail.HTTPDownloadFileOperator(
            task_id="download_final_payload_file_from_url",
            url="{{ result('get_payrun_download_batch_result').downloadUrl }}"
        )

        load_final_payload_file = rail.LoadCSVFileOperator(
            task_id="load_final_payload_file",
            document="{{ result('download_final_payload_file_from_url') }}"
        )

        create_payrun_payroll_data_collection = rail.CreateCollectionOperator(
            task_id='create_payrun_payroll_data_collection',
            name='payrunpayrolldata',
            source="{{ result('load_final_payload_file') }}"
        )

        query_blank_emmployeeid_records = rail.QueryCollectionOperator(
            task_id='query_blank_emmployeeid_records',
            query="SELECT * FROM payrunpayrolldata WHERE NULLIF(Employee_ID, '') IS NULL",
            name='invalid_records'
        )

        is_blank_empid_records_exists = rail.IfOperator(
            task_id='is_blank_empid_records_exists',
            test='{{ result("query_blank_emmployeeid_records", "length") > 0 }}',
            yes_task='mark_payrun_as_draft',
            no_task='query_overtime_entries'
        )

        mark_payrun_as_draft = rail.RepliconServiceOperator(
            task_id="mark_payrun_as_draft",
            endpoint="/services/PayRunService1.svc/MarkPayRunAsDraft",
            data={
                "target": {
                    "uri": "{{ result('get_payrun_batch_result').payRunUri }}"
                }
            }
        )

        cancel_blank_empid_payrun = rail.RepliconServiceOperator(
            task_id="cancel_blank_empid_payrun",
            endpoint="/services/PayRunService1.svc/CancelPayRun",
            data=request_payload.get_payload
        )

        fail_blank_empid_payrun = rail.FailOperator(
            task_id="fail_blank_empid_payrun",
            message="Employee ID not present for some users. Users available to validate in payrun '{{ result('logging_details').payroll_name }}'"
        )

        query_overtime_entries = rail.QueryCollectionOperator(
            task_id='query_overtime_entries',
            query=f"SELECT * FROM payrunpayrolldata WHERE Pay_Code_Name IN {config.paycodes}",
            name='overtime_entries'
        )

        is_overtime_entries_exists = rail.IfOperator(
            task_id='is_overtime_entries_exists',
            test='{{ result("query_overtime_entries", "length") > 0 }}',
            yes_task='cost_center_fullpaths_collection',
            no_task='process_blank_payroll_data'
        )

        cost_center_fullpaths_collection = rail.CreateCollectionOperator(
            task_id='cost_center_fullpaths_collection',
            source=lambda: json.dumps(rail.load_all_records(rail.result("get_costcenter_hierarchy_data"))),
            columns=["costcenter", "uri", "fullpath"],
            name='ma01_ma02_ma03_cost_centers'
        )

        payroll_data_with_costcenter_fullpaths = rail.QueryCollectionOperator(
            task_id='payroll_data_with_costcenter_fullpaths',
            query='''SELECT ote.Employee_ID, ote.User, ote.Entry_Date,
                ote.Pay_Code_Name, ote.Pay_Code_Code, ote.Pay_Code_Hours,
                ote.Cost_Center_Name, ccf.fullpath
                FROM overtime_entries ote
                JOIN ma01_ma02_ma03_cost_centers ccf
                ON ote.Cost_Center_Name = ccf.costcenter''',
            name='payroll_data_with_costcenter_fullpaths'
        )

        query_ma01_costcenter_payroll_data = rail.QueryCollectionOperator(
            task_id='query_ma01_costcenter_payroll_data',
            query="SELECT * FROM payroll_data_with_costcenter_fullpaths WHERE fullpath LIKE :ma01_costcenter",
            query_params={
                "ma01_costcenter": f"%{config.ma01_costcenter}%"
            }
        )

        is_ma01_data_exists = rail.IfOperator(
            task_id='is_ma01_data_exists',
            test='{{ result("query_ma01_costcenter_payroll_data", "length") > 0 }}',
            yes_task='write_ma01_costcenter_payroll_data_csv',
            no_task='process_ma01_empty_export'
        )

        write_ma01_costcenter_payroll_data_csv = rail.WriteCSVFileOperator(
            task_id='write_ma01_costcenter_payroll_data_csv',
            source="{{ result('query_ma01_costcenter_payroll_data') }}",
            row=custom_methods.get_payroll_data_rows,
            header=config.export_headers,
            execution_timeout=timedelta(minutes=config.execution_timeout_mins_write_csv),
            thread_pool_size=config.write_csv_thread_pool_size
        )

        create_ma01_payroll_data_xml = rail.RenderTemplateOperator(
            task_id='create_ma01_payroll_data_xml',
            target='artifact',
            template_file='xml_schema/sopra_overtime_payroll.xml',
            dataset='{{ result("write_ma01_costcenter_payroll_data_csv") }}'
        )

        upload_ma01_payroll_extract_to_s3 = rail.S3UploadFileOperator(
            task_id='upload_ma01_payroll_extract_to_s3',
            source="{{ result('create_ma01_payroll_data_xml') }}",
            key_name=config.s3_upload_filepath + '/{{ result("logging_details").ma01_export_filename }}.xml',
            bucket_name=lambda: Variable.get(config.bucket_name),
            aws_conn_id=config.aws_conn_id
        )

        encrypt_ma01_payroll_extract_data_xml = rail.PGPEncryptionOperator(
            task_id='encrypt_ma01_payroll_extract_data_xml',
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('create_ma01_payroll_data_xml') }}"
        )

        upload_ma01_payroll_extract_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_ma01_payroll_extract_to_sftp",
            content='{{ result("encrypt_ma01_payroll_extract_data_xml") }}',
            remote_filepath=config.input_filepath + '/{{ result("logging_details").ma01_export_filename }}.xml.pgp'
        )

        send_ma01_export_complete_email = rail.EmailOperator(
            task_id="send_ma01_export_complete_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon overtime payroll data extract to SOPRA for Morocco for MA01 cost center'
                + ' is completed - {{ current_time_in_specified_tz("' + config.time_zone + '") }}',
            html_content="/templates/emails/email_valid_export_complete.html",
            params={
                'upload_file_path': config.input_filepath,
                'location': config.location,
                'time_zone': config.time_zone,
                'costcenters': config.ma01_costcenter,
                'costcenters_placeholder': "MA01"
            }
        )

        process_blank_payroll_data = rail.EmptyOperator(
            task_id='process_blank_payroll_data'
        )

        process_ma01_empty_export = rail.EmptyOperator(
            task_id='process_ma01_empty_export'
        )

        create_ma01_blank_payroll_data_xml = rail.RenderTemplateOperator(
            task_id='create_ma01_blank_payroll_data_xml',
            target='artifact',
            template_file='xml_schema/sopra_overtime_payroll.xml',
            dataset=custom_methods.get_empty_export_row
        )

        encrypt_ma01_blank_payroll_extract_data_xml = rail.PGPEncryptionOperator(
            task_id='encrypt_ma01_blank_payroll_extract_data_xml',
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('create_ma01_blank_payroll_data_xml') }}"
        )

        upload_ma01_blank_payroll_extract_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_ma01_blank_payroll_extract_to_sftp",
            content='{{ result("encrypt_ma01_blank_payroll_extract_data_xml") }}',
            remote_filepath=config.input_filepath + '/{{ result("logging_details").ma01_export_filename }}.xml.pgp'
        )

        send_ma01_empty_export_email = rail.EmailOperator(
            task_id='send_ma01_empty_export_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon overtime payroll data extract to SOPRA for Morocco for MA01 cost center'
                + ' - {{ current_time_in_specified_tz("' + config.time_zone + '") }}',
            html_content="/templates/emails/email_empty_export.html",
            params={
                'upload_file_path': config.input_filepath,
                'location': config.location,
                'time_zone': config.time_zone,
                'costcenters': config.ma01_costcenter,
                'costcenters_placeholder': "MA01"
            }
        )

        query_ma02_ma03_costcenter_payroll_data = rail.QueryCollectionOperator(
            task_id='query_ma02_ma03_costcenter_payroll_data',
            query="""SELECT * FROM payroll_data_with_costcenter_fullpaths WHERE fullpath LIKE :ma02_costcenter
                OR fullpath LIKE :ma03_costcenter""",
            query_params={
                "ma02_costcenter": f"%{config.ma02_costcenter}%",
                "ma03_costcenter": f"%{config.ma03_costcenter}%"
            }
        )

        is_ma02_ma03_data_exists = rail.IfOperator(
            task_id='is_ma02_ma03_data_exists',
            test='{{ result("query_ma02_ma03_costcenter_payroll_data", "length") > 0 }}',
            yes_task='write_ma02_ma03_costcenter_payroll_data_csv',
            no_task='process_ma02_ma03_empty_export'
        )

        write_ma02_ma03_costcenter_payroll_data_csv = rail.WriteCSVFileOperator(
            task_id='write_ma02_ma03_costcenter_payroll_data_csv',
            source="{{ result('query_ma02_ma03_costcenter_payroll_data') }}",
            row=custom_methods.get_payroll_data_rows,
            header=config.export_headers,
            execution_timeout=timedelta(minutes=config.execution_timeout_mins_write_csv),
            thread_pool_size=config.write_csv_thread_pool_size
        )

        create_ma02_ma03_payroll_data_xml = rail.RenderTemplateOperator(
            task_id='create_ma02_ma03_payroll_data_xml',
            target='artifact',
            template_file='xml_schema/sopra_overtime_payroll.xml',
            dataset='{{ result("write_ma02_ma03_costcenter_payroll_data_csv") }}'
        )

        upload_ma02_ma03_payroll_extract_to_s3 = rail.S3UploadFileOperator(
            task_id='upload_ma02_ma03_payroll_extract_to_s3',
            source="{{ result('create_ma02_ma03_payroll_data_xml') }}",
            key_name=config.s3_upload_filepath + '/{{ result("logging_details").ma02_ma03_export_filename }}.xml',
            bucket_name=lambda: Variable.get(config.bucket_name),
            aws_conn_id=config.aws_conn_id
        )

        encrypt_ma02_ma03_payroll_extract_data_xml = rail.PGPEncryptionOperator(
            task_id='encrypt_ma02_ma03_payroll_extract_data_xml',
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('create_ma02_ma03_payroll_data_xml') }}"
        )

        upload_ma02_ma03_payroll_extract_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_ma02_ma03_payroll_extract_to_sftp",
            content='{{ result("encrypt_ma02_ma03_payroll_extract_data_xml") }}',
            remote_filepath=config.input_filepath + '/{{ result("logging_details").ma02_ma03_export_filename }}.xml.pgp'
        )

        send_ma02_ma03_export_complete_email = rail.EmailOperator(
            task_id="send_ma02_ma03_export_complete_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon overtime payroll data extract to SOPRA for Morocco for MA02 and MA03 cost centers'
                + ' is completed - {{ current_time_in_specified_tz("' + config.time_zone + '") }}',
            html_content="/templates/emails/email_valid_export_complete.html",
            params={
                'upload_file_path': config.input_filepath,
                'location': config.location,
                'time_zone': config.time_zone,
                'costcenters': f'{config.ma02_costcenter} and {config.ma03_costcenter}',
                'costcenters_placeholder': 'MA02 and MA03'
            }
        )

        process_ma02_ma03_empty_export = rail.EmptyOperator(
            task_id='process_ma02_ma03_empty_export'
        )

        create_ma02_ma03_blank_payroll_data_xml = rail.RenderTemplateOperator(
            task_id='create_ma02_ma03_blank_payroll_data_xml',
            target='artifact',
            template_file='xml_schema/sopra_overtime_payroll.xml',
            dataset=custom_methods.get_empty_export_row
        )

        encrypt_ma02_ma03_blank_payroll_extract_data_xml = rail.PGPEncryptionOperator(
            task_id='encrypt_ma02_ma03_blank_payroll_extract_data_xml',
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('create_ma02_ma03_blank_payroll_data_xml') }}"
        )

        upload_ma02_ma03_blank_payroll_extract_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_ma02_ma03_blank_payroll_extract_to_sftp",
            content='{{ result("encrypt_ma02_ma03_blank_payroll_extract_data_xml") }}',
            remote_filepath=config.input_filepath + '/{{ result("logging_details").ma02_ma03_export_filename }}.xml.pgp'
        )

        send_ma02_ma03_empty_export_email = rail.EmailOperator(
            task_id='send_ma02_ma03_empty_export_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon overtime payroll data extract to SOPRA for Morocco for MA02 and MA03 cost centers'
                + ' - {{ current_time_in_specified_tz("' + config.time_zone + '") }}',
            html_content="/templates/emails/email_empty_export.html",
            params={
                'upload_file_path': config.input_filepath,
                'location': config.location,
                'time_zone': config.time_zone,
                'costcenters': f'{config.ma02_costcenter} and {config.ma03_costcenter}',
                'costcenters_placeholder': 'MA02 and MA03'
            }
        )

        finish_payroll_export = rail.EmptyOperator(
            task_id='finish_payroll_export'
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
            message="{{ get_error_message() }}"
        )

        is_valid_scheduled_run >> rail.Label("Yes") >> logging_details >> get_morocco_overtime_payroll_script >> is_file_format_script_present
        is_file_format_script_present >> rail.Label("Yes") >> get_allowed_location_uris >> get_allowed_costcenter_uris \
            >> get_costcenter_hierarchy_data >> create_payrun_batch >> execute_payrun_batch >> wait_forpayrun_batch >> get_payrun_batch_result
        get_payrun_batch_result >> update_payrun_name >> create_payrun_download_batch >> execute_payrun_download_batch \
            >> wait_for_payrun_download_batch >> get_payrun_download_batch_result >> create_complete_payrun_status_batch \
                >> execute_complete_payrun_status_batch
        wait_for_complete_payrun_status_batch >> rail.Label(
                "on_success") >> download_final_payload_file_from_url
        wait_for_complete_payrun_status_batch >> rail.Label(
            "on_error") >> on_error >> cancel_payrun >> fail_export
        download_final_payload_file_from_url >> load_final_payload_file >> create_payrun_payroll_data_collection

        create_payrun_payroll_data_collection >> query_blank_emmployeeid_records >> is_blank_empid_records_exists
        is_blank_empid_records_exists >> rail.Label("Yes") >> mark_payrun_as_draft >> cancel_blank_empid_payrun \
            >> fail_blank_empid_payrun
        is_blank_empid_records_exists >> rail.Label("No") >> query_overtime_entries >> is_overtime_entries_exists
        is_overtime_entries_exists >> rail.Label("Yes") >> cost_center_fullpaths_collection \
            >> payroll_data_with_costcenter_fullpaths
        is_overtime_entries_exists >> rail.Label("No") >> process_blank_payroll_data

        payroll_data_with_costcenter_fullpaths >> query_ma01_costcenter_payroll_data >> is_ma01_data_exists
        is_ma01_data_exists >> rail.Label("Yes") >> write_ma01_costcenter_payroll_data_csv \
            >> create_ma01_payroll_data_xml >> upload_ma01_payroll_extract_to_s3 \
                >> encrypt_ma01_payroll_extract_data_xml >> upload_ma01_payroll_extract_to_sftp \
                    >> send_ma01_export_complete_email >> finish_payroll_export
        process_blank_payroll_data >> process_ma01_empty_export
        is_ma01_data_exists >> rail.Label("No") >> process_ma01_empty_export >> create_ma01_blank_payroll_data_xml \
            >> encrypt_ma01_blank_payroll_extract_data_xml >> upload_ma01_blank_payroll_extract_to_sftp \
                >> send_ma01_empty_export_email >> finish_payroll_export

        payroll_data_with_costcenter_fullpaths >> query_ma02_ma03_costcenter_payroll_data >> is_ma02_ma03_data_exists
        is_ma02_ma03_data_exists >> rail.Label("Yes") >> write_ma02_ma03_costcenter_payroll_data_csv \
            >> create_ma02_ma03_payroll_data_xml >> upload_ma02_ma03_payroll_extract_to_s3 \
                >> encrypt_ma02_ma03_payroll_extract_data_xml >> upload_ma02_ma03_payroll_extract_to_sftp \
                    >> send_ma02_ma03_export_complete_email >> finish_payroll_export
        process_blank_payroll_data >> process_ma02_ma03_empty_export
        is_ma02_ma03_data_exists >> rail.Label("No") >> process_ma02_ma03_empty_export >> create_ma02_ma03_blank_payroll_data_xml\
            >> encrypt_ma02_ma03_blank_payroll_extract_data_xml >> upload_ma02_ma03_blank_payroll_extract_to_sftp \
                >> send_ma02_ma03_empty_export_email >> finish_payroll_export

    return dag


rail.for_each_instance(create_main_dag)
