import rail
from datetime import timedelta
from alvarezandmarsalholdings.time_export.time_export_s4hc.utils import custom_methods, response_filters

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.time_export_post_to_s4hc_child_dag_id,
        description="Alvarez and Marsal Holdings Time Export post payload to S4HC API endpoint",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.post_to_endpoint_max_active_run,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        query_records_to_post = rail.QueryCollectionOperator(
            task_id = "query_records_to_post",
            query="""SELECT * FROM raw_timeexport_data
                """
        )

        final_export_data = rail.DataAdaptorOperator(
            task_id='final_export_data',
            source="{{result('query_records_to_post')}}",
            data=lambda row: response_filters.translate_rows(row,
                                                            config.TIME_OFF_TYPE_PROJECT_CODE,
                                                            config.SBU_CHAR_CODE,
                                                            config.JOB_CATEGORY_CHAR_CODE,
                                                            config.PROJECT_PROFILE_VALUE)
        )

        create_s4hc_json_payload = rail.PythonOperator(
            task_id="create_s4hc_json_payload",
            python_callable=custom_methods.create_s4hc_json_payload_callable,
            op_args=[final_export_data.task_id]
        )

        upload_s4hc_payload_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_s4hc_payload_data_to_sftp",
            content="{{ result('create_s4hc_json_payload') | load_json_artifact }}",
            remote_filepath=config.timeexport_upload_backup_filepath +
            '/s4hc_{{dag_run.conf.time_export_name.replace(":", "_")}}' + '.json'
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{result('create_s4hc_json_payload')}}",
            output_file_name='/s4hc_{{dag_run.conf.time_export_name.replace(":", "_")}}' + '.json',
            expires_in_seconds=7*24*60*60
        )

        if_instance_trial = rail.IfOperator(
            task_id='if_instance_trial',
            test=lambda: bool(config.instance.lower() in ["trial"]),
            yes_task="send_success_email",
            no_task="send_s4hc_data_to_sap_endpoint",
        )

        send_s4hc_data_to_sap_endpoint = rail.SimpleHttpOperator(
            task_id='send_s4hc_data_to_sap_endpoint',
            method='POST',
            http_conn_id=config.http_conn_id,
            endpoint='',
            headers={
                "Content-Type": 'application/json'
            },
            data="{{ result('create_s4hc_json_payload') | load_json_artifact | to_json }}",
            extra_options={
                'verify': False
            },
            execution_timeout=timedelta(hours=2),
            retries=0
        )

        check_failure_is_post_to_endpoint_failed = rail.IfOperator(
            task_id="check_failure_is_post_to_endpoint_failed",
            trigger_rule="one_failed",
            test="{{ get_task_state('send_s4hc_data_to_sap_endpoint') | lower == 'failed' }}",
            yes_task="send_posting_failed_email"
        )

        send_success_email = rail.EmailOperator(
            task_id='send_success_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='{{ get_company_key() }} | Replicon Time Data Export to S4HC - Completed Successfully - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/email_export_success_s4hc.html",
            params={
                'sftp_upload_path': config.timeexport_upload_backup_filepath
            }
        )

        send_posting_failed_email = rail.EmailOperator(
            task_id='send_posting_failed_email',
            to=config.tenant_email,
            bcc=config.alert_email,
            subject='{{ get_company_key() }} | Replicon Time Data Export to S4HC - Failed while posting to API endpoint - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/post_to_api_failed.html",
            params={
                'sftp_upload_path': config.timeexport_upload_backup_filepath
            }
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            trigger_rule="all_done",
            test="{{ get_error_message() | is_truthy }}",
            yes_task="fail_dag_due_to_error"
        )

        fail_dag_due_to_error = rail.FailOperator(
            task_id="fail_dag_due_to_error",
            message='Failure in processing time export - {{ get_error_message() }}'
        )

        query_records_to_post >> final_export_data >> create_s4hc_json_payload >> upload_s4hc_payload_data_to_sftp >> \
            generate_download_link >> if_instance_trial

        if_instance_trial >> rail.Label("Yes") >> send_success_email
        if_instance_trial >> rail.Label("No") >> send_s4hc_data_to_sap_endpoint >> check_failure_is_post_to_endpoint_failed
        
        check_failure_is_post_to_endpoint_failed >> rail.Label("Yes") >> send_posting_failed_email >> can_fail_dag

        send_s4hc_data_to_sap_endpoint >> send_success_email >> rail.Label("On error") >> can_fail_dag
        
        can_fail_dag >> rail.Label("Yes") >> fail_dag_due_to_error
        
    return dag


rail.for_each_instance(create_main_dag)
