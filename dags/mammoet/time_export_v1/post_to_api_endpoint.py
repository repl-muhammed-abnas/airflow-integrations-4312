from json import loads
import rail
from mammoet.time_export_v1.utils import custom_methods

OPEN_BRACKETS = '{{'
CLOSE_BRACKETS = '}}'

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.time_export_post_export_dag_id,
        description="Mammoet Time Export post payload batch to API endpoint",
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
            query="""SELECT * FROM filter_raw_timeexport_data frtd
                    WHERE CAST (frtd.record_id as int) BETWEEN {{dag_run.conf.record_start_index}} AND {{dag_run.conf.record_end_index}}
                """
        )

        format_raw_export_data = rail.DataAdaptorOperator(
            task_id="format_raw_export_data",
            source="{{result('query_records_to_post')}}",
            columns=['record_id', 'sap_counter_id', 'entry_date', 'user', 'employee_id',
                     'activity_name', 'activity_code', 'project_name',
                     'project_code', 'task_name', 'task_code', 'in_time',
                     'out_time', 'short_time_entry_id', 'source_system', 'crane_capacity', 'hours',
                     'time_entry_type', 'account_indicator'],
            data=custom_methods.format_raw_export_data_callable
        )

        final_export_data = rail.DataAdaptorOperator(
            task_id="final_export_data",
            source="{{result('format_raw_export_data')}}",
            columns=[
                'SAP_Counter_ID',
                'Entry_Date',
                'Employee_ID',
                'Activity_Type',
                'Project_Code',
                'Task_ID',
                'In_Time',
                'Out_Time',
                'Entry_ID',
                'Source_system',
                'Crane_capacity',
                'Hours',
                'Time_Entry_Type',
                'Account_Indicator'
            ],
            data=custom_methods.final_export_data_callable
        )

        create_csv_file = rail.WriteCSVFileOperator(
            task_id = "create_csv_file",
            source="{{result('final_export_data')}}",
            header=[
                'SAP_Counter_ID',
                'Entry_Date',
                'Employee_ID',
                'Activity_Type',
                'Project_Code',
                'Task_ID',
                'In_Time',
                'Out_Time',
                'Entry_ID',
                'Source_system',
                'Crane_capacity',
                'Hours',
                'Time_Entry_Type',
                "Account_Indicator"
            ],
            row= [
                "{{ item.SAP_Counter_ID }}",
                "{{ item.Entry_Date }}",
                "{{ item.Employee_ID }}",
                "{{ item.Activity_Type }}",
                "{{ item.Project_Code }}",
                "{{ item.Task_ID }}",
                "{{ item.In_Time }}",
                "{{ item.Out_Time }}",
                "{{ item.Entry_ID }}",
                "{{ item.Source_system }}",
                "{{ item.Crane_capacity }}",
                "{{ item.Hours }}",
                "{{ item.Time_Entry_Type}}",
                "{{ item.Account_Indicator}}"
            ]
        )

        create_json_payload = rail.PythonOperator(
            task_id="create_json_payload",
            python_callable=custom_methods.create_json_payload_callable,
            op_args=[final_export_data.task_id]
        )

        upload_export_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_export_data_to_sftp",
            content="{{result('create_csv_file')}}",
            remote_filepath=config.timeexport_upload_input_filepath +
            '/{{dag_run.conf.time_export_name}}_{{dag_run.conf.batch_index}}' + '.csv'
        )

        #!IF you pass any headers in the below request it will fail with the 401 Unauthorized ERROR
        get_access_token = rail.SimpleHttpOperator(
            task_id='get_access_token',
            method='POST',
            http_conn_id=config.http_conn_id,
            endpoint='/OAuthService/GenerateToken',
            data={
                "grant_type": "client_credentials",
                "client_id": f"{OPEN_BRACKETS}var.json.{config.client_id_secret_variable_name}.client_id {CLOSE_BRACKETS}",
                "client_secret": f"{OPEN_BRACKETS}var.json.{config.client_id_secret_variable_name}.client_secret {CLOSE_BRACKETS}",
            }
        )

        access_token = rail.PythonOperator(
            task_id = "access_token",
            python_callable=lambda: loads(rail.result("get_access_token"))['access_token']
        )

        post_to_target = rail.HTTPUploadFileOperator(
            task_id='post_to_target',
            content_type='application/json',
            endpoint="/Replicon/TimeData",
            http_conn_id=config.http_conn_id,
            content="{{result('create_json_payload')}}",
            retries=0,
            headers={
                "Authorization": "Bearer {{result('access_token')}}",
                "TimeExportName": "{{dag_run.conf.time_export_name}}_{{dag_run.conf.batch_index}}"
            },
            extra_options={
                'verify': False
            }
        )

        is_post_to_endpoint_failed = rail.IfOperator(
            task_id="is_post_to_endpoint_failed",
            trigger_rule="all_done",
            test="{{ get_task_state('post_to_target') | lower == 'failed' }}",
            yes_task="upload_to_backup_path",
            no_task="is_run_failed"
        )

        upload_to_backup_path = rail.SFTPUploadFileOperator(
            task_id="upload_to_backup_path",
            content="{{result('create_json_payload')}}",
            remote_filepath=config.timeexport_upload_backup_filepath +
            '/{{dag_run.conf.time_export_name}}_{{dag_run.conf.batch_index}}' + '.json'
        )

        send_posting_failed_email = rail.EmailOperator(
            task_id='send_posting_failed_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='{{ get_company_key() }} | Replicon Time Data Export - Failed while posting to API endpoint - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/email_post_to_api_failed.html",
            params={
                'sftp_upload_path': config.timeexport_upload_backup_filepath
            }
        )

        is_run_failed = rail.IfOperator(
            task_id="is_run_failed",
            test="{{ get_error_message() | is_truthy}}",
            yes_task="fail_dagrun",
            no_task="send_success_email"
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message="{{get_error_message()}}"
        )

        send_success_email = rail.EmailOperator(
            task_id='send_success_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='{{ get_company_key() }} | Replicon Time Data Export - Completed Successfully - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/email_export_success.html",
            params={
                'sftp_upload_path': config.timeexport_upload_input_filepath
            }
        )

        log_to_sumo_valid_export = rail.SendToSumoOperator(
            task_id="log_to_sumo_valid_export",
            data={
                'job_start_time': '{{ dag_run.conf.process_start_time }}',
                'job_end_time': f'{OPEN_BRACKETS} current_time_in_specified_tz("{config.time_zone}", "%Y-%m-%dT%H:%M:%S") {CLOSE_BRACKETS}',
                'export_type': '{{ dag_run.conf.time_export_run_type }}',
                'export_name': '{{ dag_run.conf.time_export_name }}',
                'export_file_name': '{{ dag_run.conf.time_export_batch_name }}',
                'export_filepath': config.timeexport_upload_input_filepath,
                'export_backup_filepath': config.timeexport_upload_backup_filepath,
                'twb_numberofrecords': "{{ dag_run.conf.twb_numberofrecords }}",
                'export_numberofrecords': "{{ result('format_raw_export_data', 'length')}}"
            },
            sumo_conn_id=config.sumo_conn_id
        )

        query_records_to_post >> format_raw_export_data\
            >> final_export_data >> create_json_payload >> create_csv_file >> upload_export_data_to_sftp >> get_access_token\
            >> access_token >> post_to_target >> is_post_to_endpoint_failed
        is_post_to_endpoint_failed >> rail.Label("No") >> is_run_failed >> rail.Label("No") >> send_success_email\
            >> log_to_sumo_valid_export
        is_run_failed >> rail.Label("Yes") >> fail_dagrun
        is_post_to_endpoint_failed >> rail.Label(
            "Yes") >> upload_to_backup_path >> send_posting_failed_email
        
    return dag


rail.for_each_instance(create_main_dag)
