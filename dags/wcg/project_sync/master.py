"""
WCG Project Sync v2 - Master DAG
Converted from Workato Integration - January 2026

Original Workato Recipe: live_wcg_netsuite_project_sync_v2_0.recipe.json

This master DAG orchestrates the project synchronization:
1. Monitors SFTP for feed file (delta data from NetSuite)
2. Downloads and parses the CSV feed file
3. Gets project custom field definitions from Replicon
4. Triggers child DAGs for each project in the feed
5. Generates processing log and sends completion email

Note: Unlike the Workato integration that queries NetSuite APIs,
this Airflow integration receives a delta feed file with project data.
"""

from datetime import timedelta
import rail
from wcg.project_sync.utils import custom_methods

null = None

def create_dag(config):
    """
    Master DAG for WCG Project Sync v2.
    Triggered by file arrival on SFTP.
    """
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f"WCG Project Sync v2 - Master {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=30),
        max_active_runs=config.max_active_runs_master,
        default_args={
            "sftp_conn_id": config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        # Set job start time for duration calculation
        set_job_start_time = rail.PythonOperator(
            task_id="set_job_start_time",
            python_callable=lambda: custom_methods.now(config.time_zone).isoformat(),
        )

        create_log = rail.CreateLogOperator(task_id="create_log")

        # ============================================================================
        # PHASE 1: FILE DETECTION AND DOWNLOAD
        # ============================================================================

        wait_for_feed_file = rail.SFTPAnyFileSensor(
            task_id="wait_for_feed_file",
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout),
        )

        download_feed_file = rail.SFTPDownloadFileOperator(
            task_id="download_feed_file",
            remote_filepath='{{ result("wait_for_feed_file") }}',
        )

        archive_feed_file = rail.SFTPMoveFileOperator(
            task_id="archive_feed_file",
            existing_filename='{{ result("wait_for_feed_file") }}',
            new_filename=f'{config.input_archive_filepath}{{{{ result("wait_for_feed_file").split("/")[-1] }}}}_{{{{ ts_nodash }}}}',
        )

        # ============================================================================
        # PHASE 2: PARSE CSV AND CREATE COLLECTION
        # ============================================================================

        parse_feed_csv = rail.LoadCSVFileOperator(
            task_id="parse_feed_csv",
            document='{{ result("download_feed_file") }}',
        )

        create_input_collection = rail.CreateCollectionOperator(
            task_id="create_input_collection",
            source='{{ result("parse_feed_csv") }}',
            name="project_feed",
            columns={
                header: field
                for header, field in config.feed_file_headers.items()
            },
        )

        # ============================================================================
        # PHASE 3: CHECK IF THERE ARE PROJECTS TO PROCESS
        # ============================================================================

        if_has_projects_to_process = rail.IfOperator(
            task_id="if_has_projects_to_process",
            test=lambda: (
                rail.result("create_input_collection")
                and len(rail.result("create_input_collection")) > 0
            ),
            yes_task="get_project_custom_fields_details",
            no_task="log_empty_file",
        )

        log_empty_file = rail.WriteLogOperator(
            task_id="log_empty_file",
            log='{{ result("create_log") }}',
            message="Feed file is empty - no projects to process",
            severity="Warning",
            properties=lambda dag_run: {
                "status": "Warning",
                "details": "Feed file contained no project records",
            },
        )

        # ============================================================================
        # PHASE 4: GET CUSTOM FIELD DEFINITIONS
        # ============================================================================

        get_project_custom_fields_details = rail.RepliconServiceOperator(
            task_id="get_project_custom_fields_details",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={"objectUri": "urn:replicon:object-type:project"},
            data_handler=lambda udfs: {
                "project_subsidiary_uri":rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Project Subsidiary', 'uri'),
                "pl_type_uri":rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'P&L Type', 'uri'),
                "department_uri": rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Project Department', 'uri'),
                "contract_uri": rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Contract', 'uri'),
                "utilization_uri": rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Utilization (Project)', 'uri'),
                "go_live_date_uri": rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Go Live Date', 'uri'),
            },
        )

        def get_all_drop_down_options_filter(response):
            if not response:
                return []
            return list(map(lambda data: {
                "name": data['displayText'],
                "uri": data['uri'],
                'enabled': data['isEnabled']
            }, response))

        get_subsidiary_dropdown_options = rail.RepliconServiceOperator(
            task_id="get_subsidiary_dropdown_options",
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result("get_project_custom_fields_details").get("project_subsidiary_uri")
            },
            data_handler=get_all_drop_down_options_filter
        )

        get_department_dropdown_options = rail.RepliconServiceOperator(
            task_id="get_department_dropdown_options",
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result("get_project_custom_fields_details").get("department_uri")
            },
            data_handler=get_all_drop_down_options_filter
        )

        get_pl_type_dropdown_options = rail.RepliconServiceOperator(
            task_id="get_pl_type_dropdown_options",
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result("get_project_custom_fields_details").get("pl_type_uri")
            },
            data_handler=get_all_drop_down_options_filter
        )

        load_project_template_mapper = rail.PythonOperator(
            task_id="load_project_template_mapper",
            python_callable=lambda: config.project_template_mapper,
        )

        # ============================================================================
        # PHASE 5: TRIGGER CHILD DAGS FOR EACH PROJECT
        # ============================================================================

        trigger_parallel_project_processing = rail.trigger_parallel_dagrun(
            task_id="trigger_parallel_project_processing",
            trigger_dag_id=config.process_project_child_dag_id,
            items="{{ result('create_input_collection') }}",
            conf=lambda item: {
                **item,
                "custom_field_uris": rail.result("get_project_custom_fields_details"),
                #"subsidiary_dropdown_options": rail.result("get_subsidiary_dropdown_options"),
                "project_template_mapper": rail.result("load_project_template_mapper"),
                'subsidiary_drop_uri': rail.find_first_by_attr_and_get_attr
                (rail.result("get_subsidiary_dropdown_options"),'name', (item['subsidiary']),'uri')
                if item['subsidiary'] else null,
                'pl_drop_uri': rail.find_first_by_attr_and_get_attr
                (rail.result("get_pl_type_dropdown_options"),'name', item['pl_type'],'uri')
                if item['pl_type'] else null,
                'department_drop_uri': rail.find_first_by_attr_and_get_attr
                (rail.result("get_department_dropdown_options"),'name', item['department'],'uri')
                if item['department'] else null,
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            parallel_count=config.max_active_runs_child,
        )

        # ============================================================================
        # PHASE 6: LOGGING & COMPLETION
        # ============================================================================

        # Get DAG run IDs from all parallel project processing triggers
        get_project_dag_ids = rail.PythonOperator(
            task_id="get_project_dag_ids",
            python_callable=lambda: custom_methods.get_project_processing_dag_ids(config.max_active_runs_child),
            show_return_value_in_logs=False,
        )

        # Gather logs from all child DAG runs
        gather_child_logs = rail.GatherResultsFromDagRunsOperator(
            task_id="gather_child_logs",
            dag_runs='{{ result("get_project_dag_ids") }}',
            dagrun_task_id="create_log",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            flatten=True,
        )

        # Format logs - load actual log entries from log artifacts
        format_logs = rail.PythonOperator(
            task_id="format_logs",
            python_callable=custom_methods.do_format_logs,
            show_return_value_in_logs=False,
        )

        # Get email and log file details for completion email
        get_email_and_log_file_details = rail.PythonOperator(
            task_id="get_email_and_log_file_details",
            python_callable=lambda: custom_methods.get_email_details_callable_v2(
                rail.result("set_job_start_time"), config.time_zone)
        )

        generate_processing_log = rail.WriteCSVFileOperator(
            task_id="generate_processing_log",
            header=config.log_file_headers,
            source=lambda: rail.result("format_logs"),
            row=lambda item: [
                item.get("projectname", ""),
                item.get("projectcode", ""),
                item.get("customer", ""),
                item.get("status", ""),
                item.get("details", ""),
                item.get("jobid", ""),
            ],
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_log_to_sftp",
            content='{{ result("generate_processing_log") }}',
            remote_filepath=config.logs_filepath + "/{{ result('get_email_and_log_file_details').log_file_name }}",
        )

        generate_downloadable_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_downloadable_link",
            artifact_name="{{ result('generate_processing_log') }}",
            output_file_name="{{ result('get_email_and_log_file_details').log_file_name }}",
            expires_in_seconds=7*24*60*60,
        )

        send_sync_complete_email = rail.EmailOperator(
            task_id="send_sync_complete_email",
            to=config.tenant_email,
            bcc="{%- if result('format_logs', 'error_record_count') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alerts_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | WCG Project Sync " }} \
                {%- if result("format_logs", key="error_record_count") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("format_logs", key="exception_record_count") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time_in_specified_tz() }}',
            html_content="templates/emails/project_sync_complete_mail.html",
            params={
                'log_filepath': config.logs_filepath,
            }
        )

        # ============================================================================
        # TASK DEPENDENCIES
        # ============================================================================

        # File processing flow
        (
            set_job_start_time
            >> create_log
            >> wait_for_feed_file
            >> download_feed_file
            >> archive_feed_file
            >> parse_feed_csv
            >> create_input_collection
            >> if_has_projects_to_process
        )

        # Empty file flow
        if_has_projects_to_process >> rail.Label("No") >> log_empty_file

        # Main processing flow
        (
            if_has_projects_to_process
            >> rail.Label("Yes")
            >> get_project_custom_fields_details
            >> get_subsidiary_dropdown_options
            >> get_department_dropdown_options
            >> get_pl_type_dropdown_options
            >> load_project_template_mapper
            >> trigger_parallel_project_processing
            >> get_project_dag_ids
            >> gather_child_logs
            >> format_logs
            >> get_email_and_log_file_details
            >> generate_processing_log
            >> upload_log_to_sftp
            >> generate_downloadable_link
            >> send_sync_complete_email
        )

        return dag


rail.for_each_instance(create_dag)
