from datetime import datetime as timedelta
from datetime import timedelta as td
import json
import rail
from airflow.models import Variable
from eisner_amper.time_export_s4hc.utils.custom_methods import logging_details
from eisner_amper.time_export_s4hc.utils import request_payload

OPEN_BRACKETS = '{{'
CLOSE_BRACKETS = '}}'

null = None
# pylint: disable=too-many-statements


def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f"eisner_amper_time_export_child_s4hc_{config.instance}",
        description=f"Eisner Amper Time Export Child S4HC {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_logging_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=td(days=config.execution_timeout_days),
            start_task='get_logging_details',
            end_task='log_to_sumo_valid_export',
        )

        get_logging_details = rail.PythonOperator(
            task_id='get_logging_details',
            python_callable=logging_details,
            op_args=[config.instance]
        )

        # Fetch enrichment data - matching Workato child recipe steps 3-7
        get_all_division_details = rail.RepliconServiceOperator(
            task_id="get_all_division_details",
            endpoint="/services/DivisionService1.svc/GetAllDivisions",
            data={},
            data_handler=lambda res: list(set(map(lambda data: data['uri'], res))) if res else []
        )

        get_bulk_division_details = rail.RepliconServiceOperator(
            task_id="get_bulk_division_details",
            endpoint="/services/DivisionService1.svc/BulkGetDivisionDetails",
            data=lambda: {"divisionUris": rail.result('get_all_division_details')}
        )

        get_all_servicecenter_details = rail.RepliconServiceOperator(
            task_id="get_all_servicecenter_details",
            endpoint="/services/ServiceCenterService1.svc/GetAllServiceCenters",
            data_handler=lambda res: list(set(map(lambda data: data['uri'], res))) if res else []
        )

        get_bulk_servicecenter_details = rail.RepliconServiceOperator(
            task_id="get_bulk_servicecenter_details",
            endpoint="/services/ServiceCenterService1.svc/BulkGetServiceCenterDetails",
            data= lambda: {
                "serviceCenterUris": rail.result('get_all_servicecenter_details')
            }
        )

        get_all_object_extensions_filed_details = rail.RepliconServiceOperator(
            task_id="get_all_object_extensions_filed_details",
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data={"bindingContextUri": "urn:replicon:object-type:time-entry"},
            data_handler=lambda res: rail.find_first_by_attr_and_get_attr(
                res, 'name', 'Work Location', 'uri')
        )

        get_object_extension_tag_definition_details = rail.RepliconServiceOperator(
            task_id="get_object_extension_tag_definition_details",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data=lambda: {
                "objectExtensionTagDefinitionUri": rail.result('get_all_object_extensions_filed_details')
            },
            response_filter=lambda resp: resp.json()['d']['tags']
        )

        compose_intermediate_enriched_csv = rail.WriteCSVFileOperator(
            task_id='compose_intermediate_enriched_csv',
            source="{{ dag_run.conf.postingsdata | to_json }}",
            header=['employeeid', 'companycode', 'timeentrydate', 'receivercostcenter',
                    'roles', 'rolesdescription', 'costcenter', 'lccode', 'slcode',
                    'worklocation', 'worklocationcode'],
            row=lambda item: request_payload.get_enriched_intermediate_csv_row(item, config),
            thread_pool_size=config.thread_pool_size_write_csv,
            chunk_size=config.chunk_size_write_csv,
            execution_timeout=td(days=config.execution_timeout_days)
        )

        create_enriched_collection = rail.CreateCollectionOperator(
            task_id='create_enriched_collection',
            source="{{ result('compose_intermediate_enriched_csv') }}",
            name='enriched_records'
        )

        query_invalid_enriched_records = rail.QueryCollectionOperator(
            task_id='query_invalid_enriched_records',
            query="""SELECT * FROM enriched_records
                WHERE roles IS NULL OR roles = ''
                OR rolesdescription IS NULL OR rolesdescription = ''
                OR costcenter IS NULL OR costcenter = ''
                OR lccode IS NULL OR lccode = ''
                OR slcode IS NULL OR slcode = ''
                OR worklocationcode IS NULL OR worklocationcode = ''"""
        )

        check_for_invalid_records = rail.IfOperator(
            task_id="check_for_invalid_records",
            test=lambda: rail.result('query_invalid_enriched_records', 'length') > 0,
            yes_task='compose_invalid_records_csv',
            no_task='compose_final_output_csv'
        )

        compose_invalid_records_csv = rail.WriteCSVFileOperator(
            task_id="compose_invalid_records_csv",
            source="{{ result('query_invalid_enriched_records') }}",
            header=['employeeid', 'companycode', 'timeentrydate', 'receivercostcenter',
                    'roles', 'rolesdescription', 'costcenter', 'lccode', 'slcode',
                    'worklocation', 'worklocationcode', 'reason'],
            row=lambda item: [
                item.get('employeeid', '') if item.get('employeeid', '') else '""',
                item.get('companycode', '') if item.get('companycode', '') else '""',
                item.get('timeentrydate', '') if item.get('timeentrydate', '') else '""',
                item.get('receivercostcenter', '') if item.get('receivercostcenter', '') else '""',
                item.get('roles', '') if item.get('roles', '') else '""',
                item.get('rolesdescription', '') if item.get('rolesdescription', '') else '""',
                item.get('costcenter', '') if item.get('costcenter', '') else '""',
                item.get('lccode', '') if item.get('lccode', '') else '""',
                item.get('slcode', '') if item.get('slcode', '') else '""',
                item.get('worklocation', '') if item.get('worklocation', '') else '""',
                item.get('worklocationcode', '') if item.get('worklocationcode', '') else '""',
                request_payload.build_error_reason(item)
            ]
        )

        csv_invalid_data_update = rail.PythonOperator(
            task_id="csv_invalid_data_update",
            python_callable=lambda: request_payload.fix_csv_empty_value_quotes("compose_invalid_records_csv")
        )

        upload_invalid_csv_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_invalid_csv_to_sftp',
            content="{{ result('csv_invalid_data_update') }}",
            sftp_conn_id=config.sftp_conn_internal_id,
            remote_filepath=f"{config.invalid_data_export_path}/Error_Timesheet_{{{{ dag_run.conf.index }}}}_{{{{ result('get_logging_details')['file_name_format'] }}}}.csv"
        )

        send_invalid_records_email = rail.EmailOperator(
            task_id='send_invalid_records_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Time Data extract for SAP HANA skipped at {{ result("get_logging_details")["current_date"] }}',
            html_content="template/invalid_records_skipped.html",
            params={
                'error_file_path': config.invalid_data_export_path
            }
        )

        compose_final_output_csv = rail.WriteCSVFileOperator(
            task_id='compose_final_output_csv',
            source="{{ dag_run.conf.postingsdata | to_json }}",
            header=['EmployeeID', 'Companycode', 'SAPGeneratedInternalNumber', 'TimeEntryDate',
                    'SAPEmployeeID', 'TimesheetOperation', 'ControllingArea', 'ReceiverCostCenter',
                    'ActivityType', 'WBSElement', 'BillingControlCategory', 'WorkItem', 'Comments',
                    'Hours', 'HoursUnitofMeasure', 'WorkLocationCode', 'TimeEntryApprovalStatus', 'EntryID'],
            row=lambda item: request_payload.get_final_output_csv_row(item, config),
            thread_pool_size=config.thread_pool_size_write_csv,
            chunk_size=config.chunk_size_write_csv,
            execution_timeout=td(days=config.execution_timeout_days)
        )

        csv_data_update = rail.PythonOperator(
            task_id="csv_data_update",
            python_callable=lambda: request_payload.fix_csv_empty_value_quotes("compose_final_output_csv")
        )

        # Upload final CSV to input folder
        upload_final_csv_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_final_csv_to_sftp',
            content="{{ result('csv_data_update') }}",
            sftp_conn_id=config.sftp_conn_internal_id,
            remote_filepath=f"{config.input_data_export_path}/Timesheet_{{{{ dag_run.conf.index }}}}_{{{{ result('get_logging_details')['file_name_format'] }}}}.csv"
        )

        # Create JSON payload with lowercase keys
        compose_json_payload = rail.PythonOperator(
            task_id='compose_json_payload',
            python_callable=request_payload.create_json_payload_for_s4hc_from_csv
        )

        send_data_to_endpoint = rail.IfOperator(
            task_id="send_data_to_endpoint",
            test=config.send_data_to_endpoint,
            yes_task='send_data_client_endpoint',
            no_task='upload_json_to_valid',
        )

        # Upload JSON to valid folder when not sending to endpoint (testing)
        upload_json_to_valid = rail.SFTPUploadFileOperator(
            task_id='upload_json_to_valid',
            content="{{ result('compose_json_payload') }}",
            sftp_conn_id=config.sftp_conn_internal_id,
            remote_filepath=f"{config.valid_data_export_path}/Timesheet_{{{{ dag_run.conf.index }}}}_{{{{ result('get_logging_details')['file_name_format'] }}}}.json"
        )

        send_data_client_endpoint = rail.SimpleHttpOperator(
            task_id='send_data_client_endpoint',
            method='POST',
            http_conn_id=config.http_conn_id,
            endpoint='/http/Replicon_SAP_Timesheet',
            headers={
                "Content-Type": "application/json; charset=utf-8"
            },
            data="{{ result('compose_json_payload') }}",
            extra_options={
                'verify': False
            },
            retries=0
        )

        # Runs regardless of success/failure — classifies outcome so DAG never fails on timeout
        def handle_endpoint_response():
            error_msg = str(rail.render_template("{{ get_error_message() }}"))
            if not error_msg:
                return "success"
            if "timeout" in error_msg.lower() or "timed out" in error_msg.lower():
                return "timeout"
            return f"error:{error_msg}"

        handle_endpoint_response_task = rail.PythonOperator(
            task_id='handle_endpoint_response',
            trigger_rule='all_done',
            python_callable=handle_endpoint_response
        )

        # Real error → backup + error email; timeout/success → completion email
        check_endpoint_result = rail.IfOperator(
            task_id='check_endpoint_result',
            test=lambda: str(rail.result('handle_endpoint_response')).startswith("error:"),
            yes_task='send_404_error_mail',
            no_task='send_timeout_success_mail'
        )

        # Upload JSON to backup folder on error
        upload_json_to_backup = rail.SFTPUploadFileOperator(
            task_id="upload_json_to_backup",
            content="{{ result('compose_json_payload') }}",
            sftp_conn_id=config.sftp_conn_internal_id,
            remote_filepath=f"{config.valid_data_export_backup_path}/Timesheet_{{{{ dag_run.conf.index }}}}_{{{{ result('get_logging_details')['file_name_format'] }}}}.json"
        )

        # 404 Error - Send error email
        send_404_error_mail = rail.EmailOperator(
            task_id='send_404_error_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Time Data extract for SAP HANA failed at the end point at {{ current_time_in_specified_tz() }}',
            html_content="template/failed_at_endpoint.html"
        )

        # Timeout (Not 404) - Send completion email (expected behavior)
        send_timeout_success_mail = rail.EmailOperator(
            task_id='send_timeout_success_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Time Data extract for SAP HANA completed at the end point at {{ current_time_in_specified_tz() }}',
            html_content="template/completion.html",
            params={
                'note': 'Data was successfully received by S4HC endpoint (timeout is expected behavior)'
            }
        )

        send_completion_data_mail = rail.EmailOperator(
            task_id='send_completion_data_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Time Data extract for SAP HANA completed at the end point at {{ current_time_in_specified_tz() }}',
            html_content="template/completion.html"
        )

        log_to_sumo_valid_export = rail.SendToSumoOperator(
            task_id="log_to_sumo_valid_export",
            data={
                'job_start_time': '{{ result("get_logging_details")["current_date"] }}',
                'job_end_time': f'{OPEN_BRACKETS} current_time_in_specified_tz("{config.time_zone}", "%Y-%m-%dT%H:%M:%S") {CLOSE_BRACKETS}',
                'export_file_name': "Timesheet_{{ result('get_logging_details')['file_name_format'] }}.json",
                'export_filepath': config.valid_data_export_backup_path,
                'numberofrecords': "{{ dag_run.conf.postingsdata | length }}",
            },
            sumo_conn_id="sumologic-exportlogger"
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> log_to_sumo_valid_export
        can_run_batch_task >> rail.Label("No") >> get_logging_details

        # Fetch enrichment data (location and service line use static mappers, not API)
        get_logging_details >> get_all_division_details >> get_bulk_division_details
        get_bulk_division_details >> get_all_servicecenter_details >> get_bulk_servicecenter_details
        get_bulk_servicecenter_details >> get_all_object_extensions_filed_details
        get_all_object_extensions_filed_details >> get_object_extension_tag_definition_details
        get_object_extension_tag_definition_details >> compose_intermediate_enriched_csv

        # STEP 1-3: Intermediate enriched CSV → Collection → Query invalid records
        compose_intermediate_enriched_csv >> create_enriched_collection
        create_enriched_collection >> query_invalid_enriched_records >> check_for_invalid_records

        # STEP 4-5: If invalid → Upload error CSV and send email (then wait for valid query)
        check_for_invalid_records >> rail.Label("Yes") >> compose_invalid_records_csv >> \
            csv_invalid_data_update >> upload_invalid_csv_to_sftp >> send_invalid_records_email

        # STEP 6: Compose final output from original conf data
        send_invalid_records_email >> compose_final_output_csv
        check_for_invalid_records >> rail.Label("No") >> compose_final_output_csv

        # STEP 6 continued: Post-process CSV to quote empty values, create JSON and upload
        compose_final_output_csv >> csv_data_update >> upload_final_csv_to_sftp >> compose_json_payload >> \
            send_data_to_endpoint

        # STEP 7: Send to endpoint or upload to valid folder (testing)
        send_data_to_endpoint >> rail.Label("Yes") >> send_data_client_endpoint >> handle_endpoint_response_task >> upload_json_to_backup >> check_endpoint_result
        send_data_to_endpoint >> rail.Label("No") >> upload_json_to_valid >> send_completion_data_mail >> log_to_sumo_valid_export

        check_endpoint_result >> rail.Label("Yes") >> send_404_error_mail >> log_to_sumo_valid_export
        check_endpoint_result >> rail.Label("No") >> send_timeout_success_mail >> log_to_sumo_valid_export

    return dag

rail.for_each_instance(create_child_dag)
