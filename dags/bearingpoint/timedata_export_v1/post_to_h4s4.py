import json
import rail
from bearingpoint.timedata_export_v1.custom_http_operator.CustomSimpleHttpOperator import CustomSimpleHttpOperator

OPEN_BRACKETS = '{{'
CLOSE_BRACKETS = '}}'

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.time_export_post_to_h4s4_dag_id,
        description="Bearingpoint Time Export post payload to H4S4 API endpoint",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.post_to_endpoint_max_active_run,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        if_instance_trial = rail.IfOperator(
            task_id='if_instance_trial',
            test=lambda: bool(config.instance.lower() in ["trial", "dev"]),
            yes_task="upload_h4s4_payload_data_to_sftp",
            no_task="send_h4s4_data_to_sap_endpoint",
        )

        upload_h4s4_payload_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_h4s4_payload_data_to_sftp",
            content="{{ dag_run.conf.data | load_json_artifact }}",
            remote_filepath=config.timeexport_upload_backup_filepath +
            '/h4s4_json_payload_{{dag_run.conf.export_file_time_stamp.replace(":", "_")}}' + '.json'
        )

        send_h4s4_data_to_sap_endpoint = CustomSimpleHttpOperator(
            task_id='send_h4s4_data_to_sap_endpoint',
            method='POST',
            http_conn_id=config.http_conn_id,
            endpoint=config.h4s4_endpoint,
            auth_type=None,
            headers={
                "Content-Type": 'application/json',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            data=lambda dag_run: json.dumps(rail.load_json_artifact(dag_run.conf["data"])).encode("utf-8"),
            extra_options={
                'verify': False
            }
        )

        send_success_email = rail.EmailOperator(
            task_id='send_success_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='{{ get_company_key() }} | Replicon Time Data Export On H4S4 - Completed Successfully - {{ dag_run.conf.process_start_time }}',
            html_content="templates/emails/email_export_success_h4s4.html",
            params={
                'sftp_upload_path': config.timeexport_upload_backup_filepath
            }
        )

        catch_errors = rail.PythonOperator(
            task_id = "catch_errors",
            trigger_rule = "one_failed",
            python_callable=lambda dag_run: {
                "dag_id": dag.dag_id,
                "run_id": dag_run.run_id,
                "error_message": rail.render_template("{{get_error_message()}}")
            }
        )

        if_instance_trial >> rail.Label("Yes") >> upload_h4s4_payload_data_to_sftp >> send_success_email
        if_instance_trial >> rail.Label("No") >> send_h4s4_data_to_sap_endpoint >> send_success_email
        send_success_email >> rail.Label("On error") >> catch_errors
        
    return dag


rail.for_each_instance(create_main_dag)
