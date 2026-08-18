from datetime import timedelta
import rail
from itvdaytime.user_import.utils import request_payload, custom_methods
from itvdaytime.user_import.tasks.gather_details import get_gather_details

# pylint: disable=too-many-statements


def create_main_dag(config):

    with rail.create_airflow_dag(
        dag_id=f"itvdaytime_user_import_master_{config.instance}",
        description=f"iTV DayTime User Import master {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_schedule_interval),
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
        max_active_runs=config.max_active_runs_master
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=15),

        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" }}',
            yes_task='download_file',
            no_task='send_bad_file_format_email',
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Resource Sync - Incorrect File Format - {{ current_time() }}',
            html_content='templates/emails/bad_file_format.html',
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() }}_{{ result('new_file_sensor') | file_name }}"
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}",
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='archive_file',
            no_task='delete_this_dagrun',
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        decrypt_file = rail.PGPDecryptionOperator(
            task_id="decrypt_file",
            source="{{result('download_file')}}",
            pgp_conn_id=config.pgp_connection_id
        )

        has_file_content = rail.IfOperator(
            task_id='has_file_content',
            test=custom_methods.do_has_file_content,
            yes_task='load_user_import_data',
            no_task='dummy_send_blank_file_email'
        )

        dummy_send_blank_file_email = rail.EmptyOperator(
            task_id="dummy_send_blank_file_email"
        )

        load_user_import_data = rail.LoadCSVFileOperator(
            task_id='load_user_import_data',
            document="{{ result('decrypt_file') }}"
        )

        create_user_import_data_collection = rail.CreateCollectionOperator(
            task_id='create_user_import_data_collection',
            source="{{ result('load_user_import_data') }}",
            name="input_data",
            columns={
                "Person_Number": "employee_number",
                "First Name": "first_name",
                "Last Name": "last_name",
                "Start Date": "start_date",
                "Assignment_Number": "assignment_number",
                "Phone_Number": "phone_number",
                "Department": "department",
                "Location": "location",
                "Job_Role": "job_role",
                "Assignment_Start_Date": "assignment_start_date",
                "Termination_Date": "termination_date",
                "Annual Leave Entitlement": "annual_leave_entitlement",
                "Annual Leave Entitlement Effective Date": "ale_effective_date",
                "Carry Forward": "carry_forward",
                "Carry Forward Effective Date": "carry_forward_effective_date",
                "Relish Purchased Holiday": "relish_purchased_holiday",
                "Relish Start Date": "relish_start_date",
                "Line Manager": "line_manager",
                "User Person Type": "user_person_type",
                "Contract Type": "contract_type"
            }
        )

        has_any_records = rail.IfOperator(
            task_id='has_any_records',
            test="{{result('create_user_import_data_collection', 'length') > 0}}",
            yes_task='process_records',
            no_task='send_blank_payload_email'
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Resource Sync - Blank File - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/blank_payload.html"
        )

        process_records = rail.EmptyOperator(
            task_id="process_records"
        )

        process_contact_type = rail.TriggerDagRunOperator(
            task_id = "process_contact_type",
            trigger_dag_id=f"itvdaytime_user_import_process_contract_types_{config.instance}",
            conf = {"file_name": "{{result('new_file_sensor') | file_name }}"},
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        wait_for_process_contact_type = rail.WaitForDagRunsSensor(
            task_id = "wait_for_process_contact_type",
            dag_runs="{{result('process_contact_type')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        query_invalid_records = rail.QueryCollectionOperator(
            task_id="query_invalid_records",
            name="invalid_records",
            query="""SELECT * FROM input_data WHERE NULLIF(employee_number, '') IS NULL OR NULLIF(first_name, '') IS NULL
                        OR NULLIF(last_name, '') IS NULL OR NULLIF(start_date, '') IS NULL OR NULLIF(assignment_number, '') IS NULL
                        OR NULLIF(department, '') IS NULL OR NULLIF(user_person_type, '') IS NULL"""
        )

        has_any_invalid_records = rail.IfOperator(
            task_id="has_any_invalid_records",
            test="{{result('query_invalid_records', 'length') > 0}}",
            yes_task="log_invalid_data",
        )

        log_invalid_data = rail.WriteLogOperator(
            task_id="log_invalid_data",
            items="{{result('query_invalid_records')}}",
            severity="Skipped",
            message="mandatory field is not present",
            properties=request_payload.get_invalid_logs_property_conf
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id="query_valid_records",
            name="valid_records",
            query="""SELECT * FROM input_data WHERE NULLIF(employee_number, '') IS NOT NULL AND NULLIF(first_name, '') IS NOT NULL
                        AND NULLIF(last_name, '') IS NOT NULL AND NULLIF(start_date, '') IS NOT NULL AND NULLIF(assignment_number, '') IS NOT NULL
                        AND NULLIF(department, '') IS NOT NULL AND NULLIF(user_person_type, '') IS NOT NULL"""
        )

        gather_details_start, gather_details_end = get_gather_details()

        has_any_valid_records = rail.IfOperator(
            task_id="has_any_valid_records",
            test="{{ result('query_valid_records', 'length') > 0 }}",
            yes_task=gather_details_start.task_id
        )

        create_timeoff_details_collection = rail.CreateCollectionOperator(
            task_id="create_timeoff_details_collection",
            source="{{ result('get_timeoff_details') | to_json }}",
            name="timeoff_type_details"
        )

        create_supervisor_log = rail.CreateLogOperator(
            task_id="create_supervisor_log"
        )

        process_user_records = rail.TriggerDagRunForEachItemOperator(
            task_id="process_user_records",
            items="{{result('query_valid_records')}}",
            trigger_dag_id=f"itvdaytime_user_import_process_each_user_record_{config.instance}",
            conf=request_payload.get_process_user_records_conf,
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        wait_process_user_records = rail.WaitForDagRunsSensor(
            task_id="wait_process_user_records",
            dag_runs="{{result('process_user_records')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        get_records_for_supervisor_processing = rail.FilterLogEntriesOperator(
            task_id="get_records_for_supervisor_processing",
            log="{{result('create_supervisor_log')}}"
        )

        def get_supervisor_processing_conf(item):
            if not item:
                return []
            item = item['properties']
            return {k: v if v is not None else '' for k, v in item.items()}

        process_pending_supervisor_update = rail.TriggerDagRunForEachItemOperator(
            task_id="process_pending_supervisor_update",
            items="{{result('get_records_for_supervisor_processing')}}",
            trigger_dag_id=f"itvdaytime_user_import_process_supervisor_assignment_{config.instance}",
            conf=get_supervisor_processing_conf
        )
        wait_for_process_pending_supervisor_update = rail.WaitForDagRunsSensor(
            task_id="wait_for_process_pending_supervisor_update",
            dag_runs="{{result('process_pending_supervisor_update')}}",
        )

        load_master_log = rail.RenderTemplateOperator(
            task_id='load_master_log',
            target='result',
            template="{{ get_master_log() | load_all_records | to_json }}"
        )
        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=custom_methods.do_format_logs
        )

        write_csv_file = rail.WriteCSVFileOperator(
            task_id='write_csv_file',
            source="{{ result('format_logs').final_logs }}",
            header=[
                'Personal Number',
                'User Name',
                'Status',
                'Action',
                'Details',
                'Jobid'],
            row=[
                '{{ item.employee_number }}',
                '{{ item.loginname }}',
                '{{ item.status}}',
                '{{ item.action }}',
                '{{ item.details }}',
                '{{ item.jobid }}'],
            footer=[
                'Number of records found: {{result("create_user_import_data_collection", "length")}}',
                'Number of records processed:{{result("query_valid_records", "length")}}',
                'Number of Successes: {{result("format_logs").get_record_summary.success}}',
                'Number of failures: {{result("format_logs").get_record_summary.failed}}',
                'Number of new users added: {{result("format_logs").get_record_summary.new_users_added}}',
                'Number of user profiles updated: {{result("format_logs").get_record_summary.users_updated}}',
                ''
            ]
        )

        upload_csv_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_csv_to_sftp',
            content="{{ result('write_csv_file') }}",
            remote_filepath=config.log_filepath +
            '/log_{{ ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_base }}.csv'
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs')..get_record_summary.failed == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon Resource Sync - " }} \
                {%- if result("format_logs").get_record_summary.failed > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("format_logs").get_record_summary.exception > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " " + current_time() }}',
            html_content="templates/emails/import_complete.html",
            params={
                'log_filepath': config.log_filepath,
            }
        )

        can_log_to_sumo = rail.IfOperator(
            task_id="can_log_to_sumo",
            trigger_rule="all_done",
            test=lambda:  rail.get_current_context()['dag_run'].get_task_instance(
                delete_this_dagrun.task_id).current_state().lower() != "success",
            yes_task="log_to_sumo",
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info={
                "file_name": "{{result('new_file_sensor') | file_name }}"
            }
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

        new_file_sensor >> is_csv >> rail.Label(
            "No") >> send_bad_file_format_email
        is_csv >> rail.Label("Yes") >> download_file >> rail.Label(
            "Always") >> was_new_file_found >> rail.Label("Yes") >> archive_file
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun

        download_file >> decrypt_file >> has_file_content >> rail.Label("Yes") >> load_user_import_data >> create_user_import_data_collection\
            >> has_any_records >> rail.Label("No") >> send_blank_payload_email

        has_file_content >> rail.Label(
            "No") >> dummy_send_blank_file_email >> send_blank_payload_email
        has_any_records >> rail.Label("Yes") >> process_records

        process_records >> process_contact_type >> wait_for_process_contact_type >> [query_invalid_records, query_valid_records]
        query_invalid_records >> has_any_invalid_records >> rail.Label(
            "Yes") >> log_invalid_data >> load_master_log
        query_valid_records >> has_any_valid_records >> rail.Label(
            "No") >> gather_details_start
        gather_details_end >> create_timeoff_details_collection >> create_supervisor_log >> process_user_records \
            >> wait_process_user_records >> get_records_for_supervisor_processing\
            >> process_pending_supervisor_update >> wait_for_process_pending_supervisor_update\
            >> load_master_log >> format_logs >> write_csv_file >> upload_csv_to_sftp >> send_import_complete_email

        send_import_complete_email >> can_log_to_sumo >> rail.Label(
            "Yes") >> log_to_sumo >> can_fail_dag >> rail.Label("Yes") >> fail_dagrun

    return dag


rail.for_each_instance(create_main_dag)
