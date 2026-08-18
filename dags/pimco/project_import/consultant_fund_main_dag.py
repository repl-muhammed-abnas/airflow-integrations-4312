from datetime import timedelta, datetime
import rail
from rail.lib.ecid import get_dagrun_ecid
from pimco.project_import.utils import request_payload

# pylint: disable=too-many-statements
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"pimco_consultant_fund_project_import_master_{config.instance}",
        description=f"PIMCO Fund Consultant Project Import master {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_schedule_interval),
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.consultant_fund_input_filepath,
            soft_fail_timeout=timedelta(minutes=15),
        )

        is_txt = rail.IfOperator(
            task_id='is_txt',
            test='{{ result("new_file_sensor") | file_ext | lower == "txt" }}',
            yes_task='download_file',
            no_task='send_bad_file_format_email',
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon project import- Incorrect File Format for Deal - {{ current_time_in_specified_tz() }}',
            html_content='templates/email/bad_file_format.html',
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}",
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='move_input_file_to_processing',
            no_task='delete_this_dagrun',
        )

        move_input_file_to_processing = rail.SFTPMoveFileOperator(
            task_id='move_input_file_to_processing',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.consultant_fund_processing_filepath +
            "Consultant_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        get_log_file_name = rail.PythonOperator(
            task_id = 'get_log_file_name',
            python_callable= lambda: get_dagrun_ecid(rail.get_current_context()['dag_run']) + '_Project_Import_logs_' +
            datetime.utcnow().strftime("%m%d%YT%H%M%S") + rail.result('new_file_sensor').split('/')[-1] + '.csv'
        )

        load_time_data = rail.LoadCSVFileOperator(
            task_id='load_time_data',
            document="{{ result('download_file') }}",
            delimiter= '|'
        )

        create_time_data_collection = rail.CreateCollectionOperator(
            task_id='create_time_data_collection',
            source="{{ result('load_time_data') }}",
            name="input_data",
            columns={
                'Fund Complex ID': 'Projectcode',
                'Fund Complex Name': 'Projectname',
                'Contractor Eligible / Not Eligible Flag': 'flag'
            }
        )

        has_any_records = rail.IfOperator(
            task_id='has_any_records',
            test="{{ result('create_time_data_collection', 'length') > 0 }}",
            yes_task='get_base_project_details',
            no_task='send_blank_payload_email'
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon project import- No data to process - {{ current_time_in_specified_tz() }}',
            html_content="templates/email/blank_payload.html",
            params={
                'type': 'Fund',
            },
        )

        get_base_project_details = rail.RepliconServiceOperator(
            task_id= 'get_base_project_details',
            endpoint= '/services/ProjectService1.svc/BulkGetProjectDetails3',
            data= {
                "projects": [
                    {
                    "name": config.consultant_base_project_name
                    }
                ]
            },
            response_filter=lambda resp: (resp.json()['d'][0:1] or [
                {"projectDetails": None}])[0]['projectDetails'],
        )

        process_projects = rail.trigger_parallel_dagrun(
            task_id = "process_projects",
            items="{{ result('create_time_data_collection') }}",
            parallel_count=20,
            trigger_dag_id=f"pimco_consultant_project_import_process_each_record_{config.instance}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            conf=lambda item: request_payload.get_trigger_parallel_dagrun_conf(item, "Fund", "Consultant")
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ get_master_log() }}",
            header=['Project code', 'Project Name', 'Active/Inactive flag', 'Status', 'JobId', 'Details'],
            row=['{{ item.properties.Projectcode }}', '{{ item.properties.Projectname }}', '{{ item.properties.flag }}',
                 '{{ item.properties.Status }}', '{{ item.properties.JobId }}', '{{ item.properties.details }}'],
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('render_logs_csv')}}",
            output_file_name='{{ result("get_log_file_name") }}',
            expires_in_seconds=7*24*60*60,
        )

        upload_logs_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_logs_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.consultant_fund_log_filepath +
            '{{ result("get_log_file_name") }}',
        )

        get_errored_logs = rail.FilterLogEntriesOperator(
            task_id='get_errored_logs',
            severity='Error',
        )

        send_completion_mail = rail.EmailOperator(
            task_id='send_completion_mail',
            to=config.tenant_email,
            bcc="{%- if result('get_errored_logs', key='length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Project Import for Consultant Fund is - " }} \
                {%- if result("get_errored_logs", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    completed successfully  \
                {%- endif -%} \
                {{ " " + current_time_in_specified_tz() }}',
            html_content="templates/email/import_complete.html",
            params={
                'log_filepath': config.consultant_fund_log_filepath
            }
        )

        archieve_input_file = rail.SFTPMoveFileOperator(
            task_id='archieve_input_file',
            existing_filename=config.consultant_fund_processing_filepath + 'Consultant_{{ result("new_file_sensor") | file_name }}',
            new_filename=config.archieve_input_filepath +
            "{{ dag_run_ecid() }}_{{ result('new_file_sensor') | file_name }}"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        new_file_sensor >> is_txt >> rail.Label(
            "No") >> send_bad_file_format_email

        is_txt >> rail.Label(
            "Yes") >> download_file >> rail.Label(
            "Always") >> was_new_file_found >> rail.Label("Yes") >> move_input_file_to_processing
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun

        download_file >> get_log_file_name >> load_time_data >> create_time_data_collection >> has_any_records

        has_any_records >> rail.Label(
            "No") >> send_blank_payload_email

        has_any_records >> rail.Label(
            "Yes") >> get_base_project_details >> process_projects>> render_logs_csv

        render_logs_csv >> generate_download_link >> upload_logs_to_sftp >> get_errored_logs >> send_completion_mail >> archieve_input_file >> log_to_sumo >> \
            can_fail_dag >> rail.Label('Yes') >> fail_dagrun

    return dag

rail.for_each_instance(create_main_dag)
