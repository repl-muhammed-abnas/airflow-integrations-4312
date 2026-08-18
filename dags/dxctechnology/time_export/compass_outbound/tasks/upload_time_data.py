from datetime import timedelta
import pendulum
import rail
from dxctechnology.time_export.compass_outbound.utils import custom_methods, response_filters, request_payload
from airflow.models import Variable

#pylint: disable=too-many-arguments

def get_upload_time_data(config, region, code_1, code_2, task_type, compass_oef_name_attr, internal_oef_name,
    internal_oef_uri_attr, unique_id_attr, last_unique_id_attr, division_final_data, export_type):
    with rail.TaskGroup(group_id=f'upload_time_data_{task_type}', prefix_group_id=False) as export_timedata:

        is_unckn_export_extension_field_value_is_processed = rail.IfOperator(
            task_id=f"is_unckn_export_extension_field_value_is_processed_for_{task_type}",
            test=lambda dag_run: response_filters.get_specific_time_export_details(
                rail.result("get_last_time_export_details")["extensionFieldValues"], dag_run.conf[compass_oef_name_attr]),
            yes_task=f'process_acknowledgement_not_received_{task_type}',
            no_task=f'is_unckn_export_extension_field_value_is_sent_for_{task_type}'
        )

        process_acknowledgement_not_received = rail.TriggerDagRunOperator(
            task_id=f'process_acknowledgement_not_received_{task_type}',
            retries=0,
            trigger_dag_id=config.compass_acknowledgement_child_dagid,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                'downloadurl': dag_run.conf["downloadurl"],
                'fileformaturi': dag_run.conf["fileformaturi"],
                'timeexporturi': dag_run.conf["timeexporturi"],
                'twbname': dag_run.conf["twbname"],
                'postdata': dag_run.conf["postdata"],
                'lasttwbname': dag_run.conf["lasttwbname"],
                'lasttwburi': dag_run.conf["lasttwburi"],
                'payload_identifier_replicon_uniqueid': dag_run.conf[unique_id_attr],
                'oefname': dag_run.conf[compass_oef_name_attr],
                'lasttwbuniqueindentifier': dag_run.conf[last_unique_id_attr],
                'twblist': dag_run.conf["twblist"],
                'sender': code_1 if config.company_key.lower() == "dxctechnology" else code_2,
                'erp': "COMPASS"
            }
        )

        is_unckn_export_extension_field_value_is_sent = rail.IfOperator(
            task_id=f"is_unckn_export_extension_field_value_is_sent_for_{task_type}",
            test=lambda: response_filters.get_specific_time_export_details(
                rail.result("get_current_time_export_details")["extensionFieldValues"], internal_oef_name),
            yes_task=f'create_compass_{task_type}_xml',
            no_task=f'finish_{task_type}_export'
        )

        create_compass_xml = rail.RenderTemplateOperator(
            task_id=f'create_compass_{task_type}_xml',
            target='artifact',
            template_file='xml_schema/compass_outbound.xml',
            dataset=lambda: custom_methods.get_compass_xml_data(unique_id_attr, division_final_data, region)
        )

        is_final_data_size_greater_than_record_count = rail.IfOperator(
            task_id=f'is_{task_type}_final_data_size_greater_than_record_count',
            test=lambda: custom_methods.check_final_data_greater_than_limit(task_type, config.record_count_limit),
            yes_task=f'dag_run_log_to_sumo_threshold_{task_type}',
            no_task=f'get_filenames_{task_type}'
        )

        dag_run_log_to_sumo_threshold = rail.DagRunLogToSumoOperator(
            task_id=f'dag_run_log_to_sumo_threshold_{task_type}',
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info=lambda dag_run: {
                "payloadidentifier": dag_run.conf[unique_id_attr],
                "exporttype": "Employee",
                "downstreamapp": f"Compass {code_2}",
                "twbrowcount": rail.result("create_final_time_data_collection", key="length"),
                "twbname": dag_run.conf["twbname"],
                "exportrowcount": custom_methods.get_attr_value(custom_methods.get_data_existence_var_data(), "name", code_1, "count"),
                "exportfilepath": config.output_filepath,
                "jobstarttime": dag_run.conf["process_start_time"],
                "jobendtime": pendulum.now(config.utc_timezone).isoformat(),
                "payloadthresold": "Yes",
                "exportscheduletime": dag_run.conf["process_start_time"],
                "exportfilename": dag_run.conf["twbname"] + ".xml"
            }
        )

        get_filenames = rail.PythonOperator(
            task_id=f'get_filenames_{task_type}',
            python_callable=custom_methods.get_filename,
            op_args=[config, code_1, code_2, task_type, export_type]
        )

        upload_compass_file_to_sftp = rail.SFTPUploadFileOperator(
            task_id=f'upload_compass_{task_type}_file_to_sftp',
            content="{{ result('" + create_compass_xml.task_id + "') }}",
            remote_filepath=config.output_filepath + '/{{ result("' + get_filenames.task_id + '").sftp_filename }}',
        )

        check_sftp_upload_state_failure = rail.IfOperator(
            task_id=f'check_sftp_upload_state_failure_for_{task_type}',
            trigger_rule='all_done',
            test='{{ get_task_state("' + upload_compass_file_to_sftp.task_id + '").lower() == "failed" }}',
            yes_task=f'generate_download_link_for_{task_type}',
            no_task=f'check_sftp_upload_success_for_{task_type}'
        )

        check_sftp_upload_success = rail.IfOperator(
            task_id=f'check_sftp_upload_success_for_{task_type}',
            test='{{ get_task_state("' + upload_compass_file_to_sftp.task_id + '").lower() == "success" }}',
            yes_task=f'process_{task_type}_upload_sftp_success',
            no_task=f'check_for_errors_for_{task_type}'
        )

        check_for_errors = rail.IfOperator(
            task_id=f'check_for_errors_for_{task_type}',
            test='{{ get_error_message() | is_truthy }}',
            yes_task=f'fail_{task_type}_dag',
            no_task=f'finish_{task_type}_export'
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id=f'generate_download_link_for_{task_type}',
            artifact_name="{{ result('" + create_compass_xml.task_id + "')}}",
            output_file_name=config.s3_upload_filepath + '{{ result("' + get_filenames.task_id + '").s3_filename }}',
            expires_in_seconds=config.s3_download_link_expiry
        )

        send_compass_time_data_file_sftp_failed_email = rail.EmailOperator(
            task_id=f'send_compass_time_data_{task_type}_file_sftp_failed_email',
            to=config.alert_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject="{{ get_company_key() }} | Compass Time data export automation - SFTP upload failure for " + code_1 + " - {{ current_time_in_specified_tz() }}",
            html_content="templates/emails/compass_sftp_failure.html",
            params={
                'sftp_path': config.output_filepath,
                'download_link': f'generate_download_link_for_{task_type}'
            }
        )

        process_upload_sftp_success = rail.EmptyOperator(
            task_id=f'process_{task_type}_upload_sftp_success'
        )

        compass_time_data_upload_http = rail.HTTPUploadFileOperator(
            task_id=f'compass_{task_type}_time_data_upload_http',
            method='POST',
            http_conn_id=config.compass_http_conn_id,
            content_type='application/xml',
            content="{{ result('" + create_compass_xml.task_id + "') }}"
        )

        acknowledge_current_export= rail.RepliconServiceOperator(
            task_id=f'acknowledge_current_{task_type}_export',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=lambda dag_run: request_payload.get_update_oef_acknowlegement_payload(dag_run, dag_run.conf[internal_oef_uri_attr])
        )

        if_no_data = rail.IfOperator(
            task_id=f'if_{task_type}_has_no_data',
            test=lambda: custom_methods.get_attr_value(custom_methods.get_data_existence_var_data(), "name", code_1, "type") in [None, "No Data"],
            yes_task=f'send_no_{task_type}_data_email',
            no_task=f'send_compass_{task_type}_export_complete_email'
        )

        send_no_data_email = rail.EmailOperator(
            task_id=f'send_no_{task_type}_data_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Replicon time extract for Compass - No records to export(" + code_2 + ") - {{ current_time_in_specified_tz() }}",
            html_content="templates/emails/compass_no_data.html",
            params={
                'region': region,
                'unique_id_attr': unique_id_attr
            }
        )

        send_compass_export_complete_email = rail.EmailOperator(
            task_id=f'send_compass_{task_type}_export_complete_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject="{{ get_company_key() }} | Replicon time extract for Compass - " + code_2 + " - Completed Successfully - {{ current_time_in_specified_tz() }}",
            html_content="templates/emails/compass_complete.html",
            params={
                'sftp_path': config.output_filepath,
                'code_1': code_1,
                'code_2': code_2,
                'unique_id_attr': unique_id_attr
            }
        )

        dag_run_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id=f'dag_run_log_to_sumo_{task_type}',
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info=lambda dag_run: {
                "payloadidentifier": dag_run.conf[unique_id_attr],
                "exporttype": "Employee",
                "downstreamapp": f"Compass {code_2}",
                "twbrowcount": rail.result("create_final_time_data_collection", key="length"),
                "twbname": dag_run.conf["twbname"],
                "exportrowcount": custom_methods.get_attr_value(custom_methods.get_data_existence_var_data(), "name", code_1, "count"),
                "exportfilepath": config.output_filepath,
                "jobstarttime": dag_run.conf["process_start_time"],
                "jobendtime": pendulum.now(config.utc_timezone).isoformat(),
                "payloadthresold": "No",
                "exportscheduletime": dag_run.conf["process_start_time"],
                "exportfilename": dag_run.conf["twbname"] + ".xml"
            }
        )

        finish_export = rail.EmptyOperator(
            task_id=f'finish_{task_type}_export'
        )

        fail_dag = rail.FailOperator(
            task_id=f'fail_{task_type}_dag',
            message=config.error_template
        )

        is_unckn_export_extension_field_value_is_processed >> rail.Label("Yes") >> process_acknowledgement_not_received \
            >> is_unckn_export_extension_field_value_is_sent
        is_unckn_export_extension_field_value_is_processed >> rail.Label("No") >> is_unckn_export_extension_field_value_is_sent

        is_unckn_export_extension_field_value_is_sent >> rail.Label("Yes") >> create_compass_xml >> is_final_data_size_greater_than_record_count

        is_unckn_export_extension_field_value_is_sent >> rail.Label("No") >> finish_export

        is_final_data_size_greater_than_record_count >> rail.Label("Yes") >> dag_run_log_to_sumo_threshold >> get_filenames >> upload_compass_file_to_sftp >> check_sftp_upload_state_failure
        is_final_data_size_greater_than_record_count >> rail.Label("No") >> get_filenames >> upload_compass_file_to_sftp >> check_sftp_upload_state_failure

        check_sftp_upload_state_failure >> rail.Label("Yes") >> generate_download_link \
            >> send_compass_time_data_file_sftp_failed_email >> process_upload_sftp_success
        check_sftp_upload_state_failure >> rail.Label("No") >> check_sftp_upload_success

        check_sftp_upload_success >> rail.Label("Yes") >> process_upload_sftp_success
        check_sftp_upload_success >> rail.Label("No") >> check_for_errors

        check_for_errors >> rail.Label("Yes") >> fail_dag
        check_for_errors >> rail.Label("No") >> finish_export

        process_upload_sftp_success >> compass_time_data_upload_http

        compass_time_data_upload_http >> acknowledge_current_export >> if_no_data

        if_no_data >> rail.Label("Yes") >> send_no_data_email >> dag_run_log_to_sumo
        if_no_data >> rail.Label("No") >> send_compass_export_complete_email >> dag_run_log_to_sumo

        dag_run_log_to_sumo >> finish_export

    return export_timedata
