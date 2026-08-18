import rail
from tsystems.time_export_to_sap_v1.utils.request_payload import get_berlin_timenow_in_fmt, get_final_extract_data_row, create_export_status_complete_batch_payload
from tsystems.time_export_to_sap_v1.utils.response_filter import retrieve_export_uri
from tsystems.time_export_to_sap_v1.task.time_data_export import time_data_export
from tsystems.time_export_to_sap_v1.utils import python_callable
from tsystems.time_export_to_sap_v1.utils import request_payload


# pylint:disable = too-many-statements
def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.timeexport_to_sap_child_dag,
        description=f'Timeexport to SAP {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        group_id = 'time_data_export'

        get_export_data = rail.PythonOperator(
            task_id='get_export_data',
            python_callable=request_payload.get_export_data_from_mapper
        )

        get_oef_field_values = rail.RepliconServiceOperator(
            task_id='get_oef_field_values',
            endpoint='/services/ObjectExtensionTagListService1.svc/GetData',
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:object-extension-tag-list-column:object-extension-tag"
                ],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:object-extension-tag-list-filter:definition"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "value": {
                            "uri": "{{ dag_run.conf.legal_unit_oef_uri }}"
                        }
                    }
                }
            },
            data_handler=python_callable.get_oef_field_values
        )

        get_required_companycode_uris = rail.RepliconServiceOperator(
            task_id='get_required_companycode_uris',
            endpoint='/services/LocationListService1.svc/GetData',
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:location-list-column:code",
                    "urn:replicon:location-list-column:location"
                ]
            },
            data_handler=python_callable.get_required_companycode_uris
        )

        (create_export, write_log_with_no_data,
            finish_time_export_task_group) = time_data_export(group_id)
        
        query_blank_employee_id_records = rail.QueryCollectionOperator(
            task_id="query_blank_employee_id_records",
            query="""SELECT DISTINCT time_entry_id , employee_ID , entry_date , project_ID , task_name FROM validateddata rtd WHERE NULLIF(rtd.employee_ID, '') IS NULL"""
        )

        get_export_uri = rail.RepliconServiceOperator(
            task_id='get_export_uri',
            endpoint='/services/TimeDataExportService1.svc/GetCreateTimeDataExportBatchResults',
            data={
                "timeDataExportBatchUri": "{{ result('" + group_id + ".create_export') }}"
            },
            data_handler=retrieve_export_uri
        )

        has_any_blank_emp_id = rail.IfOperator(
            task_id="has_any_blank_emp_id",
            test="{{ result('query_blank_employee_id_records', 'length') > 0}}",
            yes_task="missing_employeeid_csv",
            no_task="valid_extracted_data"
        )

        missing_employeeid_csv = rail.WriteCSVFileOperator(
            task_id='missing_employeeid_csv',
            source="{{ result('query_blank_employee_id_records') }}",
            header=['time_entry_id', 'employee_ID', 'Entry Date', 'project_ID', 'task_name'],
            row=lambda item: [
                item['time_entry_id'],
                item["employee_ID"],
                item["entry_date"],
                item["project_ID"],
                item["task_name"]
            ]
        )

        generate_download_link_missing_employeeid_records_csv = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link_missing_employeeid_records_csv',
            artifact_name="{{result('missing_employeeid_csv')}}",
            output_file_name="Invalid_TimeExport_records_{{dag_run_ecid()}}.csv",
            expires_in_seconds=7*24*60*60
        )

        send_invalid_records_email = rail.EmailOperator(
            task_id='send_invalid_records_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Time Data Export to SAP - Invalid records found - {{ current_time_in_specified_tz() }}',
            html_content="templates/email_invalid_records_in_export.html"
        )

        valid_extracted_data = rail.QueryCollectionOperator(
            task_id='valid_extracted_data',
            query="""SELECT * FROM validateddata WHERE NULLIF(employee_ID, '') IS NOT NULL AND NULLIF(project_ID, '') IS NOT NULL AND NULLIF(task_name , '') IS NOT NULL AND CAST(hours AS FLOAT) != 0"""
        )

        is_valid_data_present = rail.IfOperator(
            task_id="is_valid_data_present",
            test="{{ result('valid_extracted_data', 'length') > 0 }}",
            yes_task="render_final_extract_data",
            no_task="create_export_status_complete_batch"
        )

        render_final_extract_data = rail.WriteCSVFileOperator(
            task_id='render_final_extract_data',
            source="{{ result('valid_extracted_data') }}",
            delimiter=";",
            header=[
                    'time_entry_id',
                    'employee_ID',
                    'project_ID',
                    'Entry Date',
                    'billing_entry',
                    'billing_rate_name',
                    'hours',
                    'task_name',
                    'task_code',
                    'task_activity_name',
                    'task_description',
                    'sap_activity_type',
                    ' '
                ],
            row=get_final_extract_data_row,
            encoding="ascii"
        )


        log_the_records_count = rail.PythonOperator(
            task_id="log_the_records_count",
            python_callable=lambda: f"{get_berlin_timenow_in_fmt()} - INFO admin No of records exported = {rail.result('valid_extracted_data', 'length')}"
        )

        upload_export_file_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_export_file_to_sftp',
            content="{{ result('render_final_extract_data') }}",
            remote_filepath=config.upload_filepath + "/{{ result('get_export_data').export_file_name }}.csv"
        )

        def create_export_status_batch_payload(status):
            return {
                "target": {
                    "uri": rail.result(get_export_uri.task_id),
                    "name": None
                },
                "statusUri": f"urn:replicon:time-data-export-status:{status}"
            }

        create_export_status_complete_batch = rail.RepliconServiceOperator(
            task_id='create_export_status_complete_batch',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataExportStatusBatch',
            data=lambda: create_export_status_batch_payload("complete")
        )

        execute_export_status_complete_batch, wait_for_export_status_complete_batch = rail.batch_execution(
            group_id='execute_time_export_status_complete_batch',
            creation_task_id=create_export_status_complete_batch.task_id
        )

        mark_timedata_export_draft_error = rail.EmptyOperator(
            task_id='mark_timedata_export_draft_error',
            trigger_rule='one_failed'
        )

        mark_timedata_export_as_draft = rail.RepliconServiceOperator(
            task_id='mark_timedata_export_as_draft',
            endpoint="/services/TimeDataExportService1.svc/MarkTimeDataExportAsDraft",
            data={
                "target": {
                    "uri": "{{ result('get_export_uri') }}"
                }
            }
        )

        create_export_status_cancel_batch = rail.RepliconServiceOperator(
            task_id='create_export_status_cancel_batch',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataExportStatusBatch',
            data=lambda: create_export_status_batch_payload("cancelled")
        )

        execute_export_status_cancel_batch, wait_for_export_status_cancel_batch = rail.batch_execution(
            group_id='execute_time_export_status_cancel_batch',
            creation_task_id=create_export_status_cancel_batch.task_id
        )

        fail_time_export = rail.FailOperator(
            task_id='fail_time_export',
            message='{{ get_error_message() }}'
        )

        def get_export_log_count(dag_run):
            current_time = get_berlin_timenow_in_fmt()
            # pylint: disable=cell-var-from-loop
            logs = [
                {'log': f"{dag_run.conf['process_start_time']} - Process started"},
                {'log': rail.result('log_the_records_count')},
                {'log': f"{current_time} - INFO admin Export File - {rail.result('get_export_data')['export_file_name']}.csv created"},
                {'log': f"{current_time} - Process ended"}
            ]
            logs = list(filter(lambda log: bool(log['log']), logs))
            return {
                'logs': logs,
                'processended': current_time,
                'recordcount': rail.result('valid_extracted_data', 'length')
            }

        get_export_file_log_count = rail.PythonOperator(
            task_id="get_export_file_log_count",
            python_callable=get_export_log_count
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id="render_logs_csv",
            source="{{ result('get_export_file_log_count').logs | to_json }}",
            header=None,
            row=[
                '{{ item.log }}'
            ]
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_log_to_sftp",
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath + '/{{ result("get_export_data").log_filename }}'
        )

        send_valid_export_complete_email = rail.EmailOperator(
            task_id="send_valid_export_complete_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            #pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon Time Data Export to SAP  - {{ dag_run.conf.process_start_time }}',
            html_content="templates/email_valid_import_complete.html",
            params={
                'upload_file_path': config.upload_filepath,
                'log_filepath': config.log_filepath,
            }
        )

        log_to_sumo_valid_export = rail.SendToSumoOperator(
            task_id="log_to_sumo_valid_export",
            data={
                'jobstarttime': '{{ dag_run.conf.process_start_time }}',
                'jobendtime': '{{ current_time_in_specified_tz("Europe/Berlin", "%Y-%m-%dT%H:%M:%S") }}',
                'exportfilename': '{{ result("get_export_data").export_file_name }}.csv',
                'exportfilepath': config.upload_filepath,
                'numberofrecords': "{{ result('get_export_file_log_count').recordcount }}",
                'logfilename': 'Log_{{ result("get_export_data").log_filename }}',
                'logfilepath': config.log_filepath,
            },
            sumo_conn_id=config.sumo_conn_id
        )

        log_to_sumo_no_data = rail.SendToSumoOperator(
            task_id="log_to_sumo_no_data",
            data={
                'jobstarttime': '{{ dag_run.conf.process_start_time }}',
                'jobendtime': '{{ current_time_in_specified_tz("Europe/Berlin", "%Y-%m-%dT%H:%M:%S") }}',
                'exportfilename': '{{ result("get_export_data").export_file_name }}_Nodownloaddata.csv',
                'exportfilepath': config.upload_filepath,
                'numberofrecords': 0,
                'logfilename': None,
                'logfilepath': config.log_filepath,
            },
            sumo_conn_id=config.sumo_conn_id
        )

        create_export_status_for_blank_email_cancel_batch = rail.RepliconServiceOperator(
            task_id='create_export_status_for_blank_email_cancel_batch',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataExportStatusBatch',
            data=lambda: create_export_status_batch_payload("cancelled")
        )

        execute_export_status_for_blank_email_cancel_batch, wait_for_export_status_for_blank_email_cancel_batch = rail.batch_execution(
            group_id='execute_time_export_status_for_blank_email_cancel_batch',
            creation_task_id=create_export_status_for_blank_email_cancel_batch.task_id
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            sumo_conn_id=config.dagrun_log_sumo_conn_id,
            trigger_rule='all_done',
            extra_info={
                'twbrowcount': "{{ result('" + group_id + ".create_timeexport_collection', 'length') or \
                    result('valid_extracted_data', 'length') or 0 }}",
                'filename': "{{ result('get_export_data').export_file_name }}.csv"
            }
        )

        should_fail_dag = rail.IfOperator(
            task_id='should_fail_dag',
            test="{{ get_failed_upstream_task_ids() | length > 0 }}",
            yes_task='fail_time_export',
            no_task='time_export_finish'
        )

        time_export_finish = rail.EmptyOperator(
            task_id='time_export_finish'
        )

        get_export_data >> get_oef_field_values >> get_required_companycode_uris >> create_export

        create_export

        finish_time_export_task_group >> query_blank_employee_id_records >> get_export_uri >> has_any_blank_emp_id

        write_log_with_no_data >> log_to_sumo_no_data >> should_fail_dag

        has_any_blank_emp_id >> rail.Label("Yes") >> missing_employeeid_csv >> generate_download_link_missing_employeeid_records_csv >> send_invalid_records_email >> create_export_status_for_blank_email_cancel_batch
        
        create_export_status_for_blank_email_cancel_batch >> execute_export_status_for_blank_email_cancel_batch >> wait_for_export_status_for_blank_email_cancel_batch >> dagrun_log_to_sumo >> should_fail_dag

        has_any_blank_emp_id >> rail.Label("No") >> valid_extracted_data >> is_valid_data_present

        is_valid_data_present >> rail.Label("Yes") >> render_final_extract_data >> log_the_records_count >> upload_export_file_to_sftp >> create_export_status_complete_batch
        is_valid_data_present >> rail.Label("No") >> create_export_status_complete_batch

        create_export_status_complete_batch >> execute_export_status_complete_batch >> wait_for_export_status_complete_batch >> get_export_file_log_count

        get_export_file_log_count >> render_logs_csv >> upload_log_to_sftp >> send_valid_export_complete_email >> log_to_sumo_valid_export >> should_fail_dag

        get_export_file_log_count >> rail.Label(
            "On Error") >> mark_timedata_export_draft_error >> mark_timedata_export_as_draft >> create_export_status_cancel_batch
        
        create_export_status_cancel_batch >> execute_export_status_cancel_batch >> wait_for_export_status_cancel_batch >> fail_time_export

        should_fail_dag >> rail.Label("Yes") >> fail_time_export
        should_fail_dag >> rail.Label("No") >> time_export_finish

    return dag


rail.for_each_instance(create_child_dag)
