"""
Unisys Cost Center Import Integration - Master DAG

This module defines the master DAG that orchestrates the Cost Center import process
from input files to Replicon division hierarchy.

The DAG workflow:
    1. Monitors SFTP directory for new CSV files
    2. Downloads and optionally decrypts input file
    3. Validates file format and data structure
    4. Retrieves all existing cost centers from Replicon (paginated)
    5. Categorizes cost centers into: add, update, disable
    6. Triggers child DAGs for parallel processing
    7. Generates processing logs and sends email notifications

Key Features:
    - File format validation (CSV only)
    - Optional PGP decryption support
    - Paginated retrieval of existing cost centers
    - Parallel processing via child DAGs
    - Comprehensive error handling and logging
    - Email notifications with processing summary
    - SFTP file operations (download, upload, archive)

Design Reference:
    Based on cost_center_design.txt workflow specifications

Functions:
    create_main_dag(config): Creates and configures the master Airflow DAG
"""

from datetime import timedelta
import rail
from airflow.models import Variable
from unisys.cost_center_import.utils import request_payload
from unisys.cost_center_import.utils import response_filters
from unisys.cost_center_import.utils import custom_methods
null=None
# pylint: disable=too-many-statements


def create_main_dag(config):
    """
    Create the master DAG for Cost Center import integration.

    This function configures and returns the main orchestration DAG that coordinates
    the entire cost center import process from file ingestion to Replicon sync.

    Args:
        config: Configuration object containing instance-specific settings including:
            - master_dag (str): DAG identifier
            - instance (str): Environment instance name
            - company_key (str): Replicon company identifier
            - replicon_conn_id (str): Airflow connection ID for Replicon API
            - sftp_conn_id (str): Airflow connection ID for SFTP server
            - input_filepath (str): SFTP path for input files
            - file_sensor_timeout (int): File sensor timeout in minutes
            - Various email addresses and child DAG identifiers

    Returns:
        airflow.DAG: Configured Airflow DAG object

    Example:
        >>> from unisys.cost_center_import.instances import development
        >>> dag = create_main_dag(development)
    """
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f"Unisys Cost Center Import - Master DAG {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(minutes=30),
        max_active_runs=config.max_active_run_master,
        default_args={
            "sftp_conn_id": config.sftp_conn_id,
            "execution_timeout": timedelta(days=config.execution_timeout_days),
        },
    ) as dag:

        # ============================================================================
        # PHASE 1: FILE ACQUISITION AND VALIDATION
        # ============================================================================

        # Step 1: Check for new files on SFTP
        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id="new_file_sensor",
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout),
        )

        # Step 2: Validate file extension is CSV or PGP
        is_csv_pgp = rail.IfOperator(
            task_id="is_csv_pgp",
            test=lambda: rail.result("new_file_sensor").lower().endswith(('.csv', '.pgp')),
            yes_task="download_file",
            no_task="send_bad_file_format_email",
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id="send_bad_file_format_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Cost Center Import - Invalid File Format - {{ current_time_in_specified_tz() }}",
            html_content="templates/emails/bad_file_format.html",
        )

        # Step 3: Download file from SFTP
        download_file = rail.SFTPDownloadFileOperator(
            task_id="download_file", remote_filepath="{{ result('new_file_sensor') }}"
        )

        # Step 4: Check if file was successfully found and downloaded
        was_new_file_found = rail.IfOperator(
            task_id="was_new_file_found",
            trigger_rule="all_done",
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task="archive_file",
            no_task="delete_this_dagrun",
        )

        # Step 5: Archive the processed file
        archive_file = rail.SFTPMoveFileOperator(
            task_id="archive_file",
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath
            + "/{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') |file_name }}",
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id="delete_this_dagrun"
        )

        # Step 6: Optional PGP decryption
        can_decrypt_file = rail.IfOperator(
            task_id="can_decrypt_file",
            test=lambda: Variable.get(
                config.can_decrypt_file_var_name, default_var="false"
            ).lower()
            == "true",
            yes_task="decrypt_file",
            no_task="get_input_data",
        )

        decrypt_file = rail.PGPDecryptionOperator(
            task_id="decrypt_file",
            source='{{ result("download_file") }}',
            pgp_conn_id=config.pgp_conn_id,
        )

        # Step 7: Get input data (decrypted or raw)
        get_input_data = rail.PythonOperator(
            task_id="get_input_data",
            python_callable=lambda: (
                rail.result("decrypt_file")
                if Variable.get(
                    config.can_decrypt_file_var_name, default_var="false"
                ).lower()
                == "true"
                else rail.result("download_file")
            ),
            show_return_value_in_logs=False,
        )

        # ============================================================================
        # PHASE 2: DATA LOADING AND VALIDATION
        # ============================================================================

        # Step 8: Load CSV file
        load_data = rail.LoadCSVFileOperator(
            task_id="load_data",
            document="{{ result('get_input_data') }}",
            encoding="utf-8-sig",
        )

        create_processing_log = rail.CreateLogOperator(
            task_id="create_processing_log")

        # Step 10: Create collection from input data
        create_input_data_collection = rail.CreateCollectionOperator(
            task_id="create_input_data_collection",
            source="{{ result('load_data') }}",
            name="inputdatacollection",
            columns={
                "COMPANY": "company",
                "COMPANY_NAME": "company_name",
                "COST_CENTER": "cost_center",
                "COST_CENTER_NAME": "cost_center_name",
                "STATUS": "status",
            },
        )

        # Step 11: Check if input has data
        has_input_data = rail.IfOperator(
            task_id="has_input_data",
            test="{{ result('create_input_data_collection','length') > 0 }}",
            yes_task="query_valid_data",
            no_task="send_no_records_email",
        )

        send_no_records_email = rail.EmailOperator(
            task_id="send_no_records_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Cost Center Import - No Records - {{ current_time_in_specified_tz() }}",
            html_content="templates/emails/no_records.html",
        )

        # Step 12: Query valid records (all required fields present)
        query_valid_data = rail.QueryCollectionOperator(
            task_id="query_valid_data",
            name="valid_data",
            query="""SELECT * FROM inputdatacollection WHERE
                NULLIF(TRIM("company"),"") IS NOT NULL AND
                NULLIF(TRIM("company_name"),"") IS NOT NULL AND
                NULLIF(TRIM("cost_center"),"") IS NOT NULL AND
                NULLIF(TRIM("cost_center_name"),"") IS NOT NULL AND
                NULLIF(TRIM("status"),"") IS NOT NULL 
                AND lower(status) IN ('enabled','disabled')""",
        )

        # Step 13: Query invalid records (missing required fields)
        query_invalid_data = rail.QueryCollectionOperator(
            task_id="query_invalid_data",
            name="invalid_data",
            query="""SELECT * FROM inputdatacollection WHERE
                NULLIF(TRIM("company"),"") IS NULL OR
                NULLIF(TRIM("company_name"),"") IS NULL OR
                NULLIF(TRIM("cost_center"),"") IS NULL OR
                NULLIF(TRIM("cost_center_name"),"") IS NULL OR
                NULLIF(TRIM("status"),"") IS NULL 
                OR lower(status) NOT IN ('enabled','disabled')""",
        )

        # Step 14: Log invalid records
        log_invalid_records = rail.WriteLogOperator(
            task_id="log_invalid_records",
            log="{{ result('create_processing_log') }}",
            message="Invalid records found in input (missing required fields)",
            items="{{ result('query_invalid_data') }}",
            severity="Exception",
            properties=lambda item: {
                "company": item.get("company", ""),
                "cost_center": item.get("cost_center", ""),
                "action": "Validate",
                "status": "Exception",
                "details": "Missing required fields in input record",
            },
        )

        # ============================================================================
        # PHASE 3: RETRIEVE EXISTING COST CENTERS FROM REPLICON
        # ============================================================================
        def page_handler(request, result):
            if len(result['rows']) > 0:
                request['page'] += 1
                return request
            return None

        def all_result_data_handler(result):
            # Flatten paginated results with validation
            # Handles cases where result structure may vary
            flattened_rows = []
            if result and isinstance(result, list):
                for item in result:
                    if isinstance(item, dict) and 'rows' in item:
                        flattened_rows.extend(item['rows'])
            return flattened_rows
    
        get_company_page = rail.RepliconServicePageOperator(
            task_id="get_company_page",
            endpoint=config.division_list_service_endpoint,
            data=request_payload.get_hierarchy_data_payload,
            page_handler=page_handler,
            all_result_data_handler=all_result_data_handler
        )

        get_company_result = rail.PythonOperator(
            task_id="get_company_result",
            python_callable=lambda: response_filters.extract_hierarchy_data(rail.result("get_company_page")),
        )

        # ============================================================================
        # PHASE 4: CATEGORIZE COST CENTERS (ADD/UPDATE/DISABLE)
        # ============================================================================

        # Step 20: Create collection of existing cost centers for querying
        create_existing_company_collection = rail.CreateCollectionOperator(
            task_id="create_existing_company_collection",
            source="{{ result('get_company_result') }}",
            name="existing_companies",
            columns={
                "company": "company",
                "company_name": "company_name",
                "cost_center": "cost_center",
                "cost_center_name": "cost_center_name",
                "status": "status",
            },
        )

        # Step 21: Query records with companies not in Replicon
        query_missing_companies = rail.QueryCollectionOperator(
            task_id="query_missing_companies",
            name="missing_companies",
            query="""SELECT DISTINCT
                    v.company,
                    v.company_name
                FROM valid_data v
                WHERE v.company NOT IN (
                    SELECT DISTINCT e.company
                    FROM existing_companies e
                )
            """,
        )

        if_companies_to_create = rail.IfOperator(
            task_id = "if_companies_to_create",
            test = '{{result("query_missing_companies", "length") > 0 }}',
            yes_task = "create_company_code",
            no_task = "query_update_companies"
        )

        create_company_code = rail.EmptyOperator(task_id="create_company_code")

        trigger_create_company_code = rail.trigger_parallel_dagrun(
            task_id="trigger_create_company_code",
            items='{{ result("query_missing_companies") }}',
            trigger_dag_id=config.process_company_code_child_dag_id,
            conf=lambda item: {
                **item,
                "action": "add",
                "processing_log": rail.result("create_processing_log"),
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            parallel_count=config.trigger_parallel_dagrun_count_process_cost_centers,
        )

        query_update_companies = rail.QueryCollectionOperator(
            task_id="query_update_companies",
            name="update_companies",
            query="""SELECT DISTINCT
                    v.company,
                    v.company_name
                FROM valid_data v
                INNER JOIN existing_companies e
                ON e.company = v.company AND lower(e.company_name) != lower(v.company_name)
            """,
        )

        if_companies_to_update = rail.IfOperator(
            task_id = "if_companies_to_update",
            test = '{{result("query_update_companies", "length") > 0 }}',
            yes_task = "update_company_code",
            no_task = "start_cost_center_processing"
        )

        update_company_code = rail.EmptyOperator(task_id="update_company_code")

        trigger_update_company_code = rail.trigger_parallel_dagrun(
            task_id="trigger_update_company_code",
            items='{{ result("query_update_companies") }}',
            trigger_dag_id=config.process_company_code_child_dag_id,
            conf=lambda item: {
                **item,
                "action": "update",
                "processing_log": rail.result("create_processing_log"),
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            parallel_count=config.trigger_parallel_dagrun_count_process_cost_centers,
        )

        start_cost_center_processing = rail.EmptyOperator(task_id="start_cost_center_processing")

        get_cost_center_page = rail.RepliconServicePageOperator(
            task_id="get_cost_center_page",
            endpoint=config.division_list_service_endpoint,
            data=request_payload.get_hierarchy_data_payload,
            page_handler=page_handler,
            all_result_data_handler=all_result_data_handler
        )

        get_cost_center_result = rail.PythonOperator(
            task_id="get_cost_center_result",
            python_callable=lambda: response_filters.extract_hierarchy_data(rail.result("get_cost_center_page")),
        )

        # ============================================================================
        # PHASE 4: CATEGORIZE COST CENTERS (ADD/UPDATE/DISABLE)
        # ============================================================================

        # Step 20: Create collection of existing cost centers for querying
        create_existing_collection = rail.CreateCollectionOperator(
            task_id="create_existing_collection",
            source="{{ result('get_cost_center_result') }}",
            name="existing_cost_centers",
            columns={
                "company": "company",
                "company_name": "company_name",
                "cost_center": "cost_center",
                "cost_center_name": "cost_center_name",
                "status": "status",
            },
        )

        query_new_cost_centers_to_add = rail.QueryCollectionOperator(
            task_id="query_new_cost_centers_to_add",
            name="cost_centers_to_add",
            query="""SELECT
                    v.company,
                    v.company_name,
                    v.cost_center,
                    v.cost_center_name,
                    v.status,
                    'add' as action,
                    v.company || '|' || v.cost_center as key
                FROM valid_data v
                WHERE v.company IN (
                    SELECT DISTINCT e.company
                    FROM existing_cost_centers e
                )
                AND v.company || '|' || v.cost_center NOT IN (
                    SELECT e.company || '|' || e.cost_center
                    FROM existing_cost_centers e
                )
            """,
        )

        # Step 24: Query cost centers to UPDATE
        # Design Reference (Step 14): Cost centers where name changed
        query_cost_centers_to_update = rail.QueryCollectionOperator(
            task_id="query_cost_centers_to_update",
            name="cost_centers_to_update",
            query="""SELECT
                    v.company,
                    v.company_name,
                    v.cost_center,
                    v.cost_center_name,
                    v.status,
                    'update' as action,
                    v.company || '|' || v.cost_center as key
                FROM valid_data v
                INNER JOIN existing_cost_centers e
                    ON v.company = e.company
                    AND v.cost_center = e.cost_center
                WHERE (v.cost_center_name != e.cost_center_name OR LOWER(TRIM(v.status)) != LOWER(TRIM(e.status)))
            """,
        )

        # Step 26: Check if any processing is needed
        has_cost_centers_to_process = rail.IfOperator(
            task_id="has_cost_centers_to_process",
            test="{{ result('query_new_cost_centers_to_add', 'length') > 0 or\
                    result('query_cost_centers_to_update', 'length') > 0 }}",
            yes_task="process_cost_centers",
            no_task="has_invalid_records",
        )

        has_invalid_records = rail.IfOperator(
            task_id="has_invalid_records",
            test='{{result("query_invalid_data", "length") > 0}}',
            yes_task="format_logs",
            no_task="send_no_changes_email"
        )

        send_no_changes_email = rail.EmailOperator(
            task_id="send_no_changes_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Replicon Cost Center Import - No Changes - {{ current_time_in_specified_tz() }}",
            html_content="templates/emails/no_records.html",
        )

        # ============================================================================
        # PHASE 5: TRIGGER CHILD DAGS FOR PARALLEL PROCESSING
        # ============================================================================

        process_cost_centers = rail.EmptyOperator(
            task_id="process_cost_centers")

        # Step 27: Trigger child DAGs for ADD operations
        trigger_add_cost_centers = rail.trigger_parallel_dagrun(
            task_id="trigger_add_cost_centers",
            items=lambda: rail.result("query_new_cost_centers_to_add"),
            trigger_dag_id=config.process_cost_centers_child_dag_id,
            conf=lambda item: {
                **item,
                "processing_log": rail.result("create_processing_log"),
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            parallel_count=config.trigger_parallel_dagrun_count_process_cost_centers,
        )

        # Step 28: Trigger child DAGs for UPDATE operations
        trigger_update_cost_centers = rail.trigger_parallel_dagrun(
            task_id="trigger_update_cost_centers",
            items=lambda: rail.result("query_cost_centers_to_update"),
            trigger_dag_id=config.process_cost_centers_child_dag_id,
            conf=lambda item: {
                **item,
                "processing_log": rail.result("create_processing_log"),
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            parallel_count=config.trigger_parallel_dagrun_count_process_cost_centers,
        )

        process_cost_centers_end = rail.EmptyOperator(task_id="process_cost_centers_end")
        # ============================================================================
        # PHASE 6: LOG GENERATION AND EMAIL NOTIFICATION
        # ============================================================================

        # Step 24: Generate summary log file
        format_logs = rail.PythonOperator(
            task_id="format_logs", python_callable=custom_methods.format_logs_callable
        )

        create_csv_log = rail.WriteCSVFileOperator(
            task_id="create_csv_log",
            source="{{ result('format_logs') }}",
            header=[
                "company",
                "cost_center",
                "action",
                "status",
                "details",
                "ecid"
            ],
            row=[
                "{{ item.properties | attr_or_default('company', '') }}",
                "{{ item.properties | attr_or_default('cost_center', '')  }}",
                "{{ item.properties | attr_or_default('action', '')  }}",
                "{{ item.properties | attr_or_default('status', '')  }}",
                "{{ item.properties | attr_or_default('details', '')  }}",
                "{{ item | attr_or_default('ecid', '') }}",
            ],
        )

        get_log_filename = rail.PythonOperator(
            task_id="get_log_filename",
            python_callable=lambda:rail.render_template(
                "{{get_company_key()}}_{{ current_time_in_specified_tz(fmt='%Y%m%d_%H%M%S') }}_cost_center_import_log.csv")
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('create_csv_log')}}",
            output_file_name="{{ result('get_log_filename') }}",
            expires_in_seconds=7*24*60*60,
        )

        # Step 25: Upload log file to SFTP
        upload_log_file = rail.SFTPUploadFileOperator(
            task_id="upload_log_file",
            content="{{ result('create_csv_log') }}",
            remote_filepath=config.log_filepath
            + "/{{result('get_log_filename')}}",
        )

        # Step 26: Send completion email
        send_completion_email = rail.EmailOperator(
            task_id="send_completion_email",
            to=config.tenant_email,
            bcc="{%- if result('format_logs', 'error_record_count') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon Cost Center Import " }} \
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
            html_content="templates/emails/import_complete.html",
            params={
                "log_filepath": config.log_filepath,
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger'
        )
        # Step 27: Check for errors and fail if needed
        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test="{{ get_error_message() | is_truthy }}",
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun", message="{{ get_error_message() }}"
        )

        # ============================================================================
        # DAG DEPENDENCIES
        # ============================================================================

        # Phase 1: File acquisition
        new_file_sensor >> is_csv_pgp
        is_csv_pgp >> rail.Label("Yes") >> download_file >> was_new_file_found
        is_csv_pgp >> rail.Label("No") >> send_bad_file_format_email
        was_new_file_found >> rail.Label("Yes") >> archive_file
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun

        # Phase 2: Decryption and loading
        download_file >> can_decrypt_file
        can_decrypt_file >> rail.Label("Yes") >> decrypt_file >> get_input_data
        can_decrypt_file >> rail.Label("No") >> get_input_data >> load_data >>\
        create_processing_log >>\
        create_input_data_collection >>\
        has_input_data >> rail.Label("No") >> send_no_records_email
        (
            has_input_data
            >> rail.Label("Yes")
            >> query_valid_data
            >> query_invalid_data
            >> log_invalid_records
        )

        # Phase 4: Get existing cost centers and categorize
        log_invalid_records >> get_company_page >> get_company_result >> create_existing_company_collection >>\
        query_missing_companies >> if_companies_to_create >> rail.Label("Yes") >>\
        create_company_code >> trigger_create_company_code >> query_update_companies
        if_companies_to_create >> rail.Label("No") >> query_update_companies >>\
        if_companies_to_update >> rail.Label("Yes") >> update_company_code >>\
        trigger_update_company_code >> start_cost_center_processing >>\
        get_cost_center_page >> get_cost_center_result >> create_existing_collection >>\
        query_new_cost_centers_to_add
        if_companies_to_update >> rail.Label("No") >>\
        start_cost_center_processing
        query_new_cost_centers_to_add >>\
        query_cost_centers_to_update >> has_cost_centers_to_process
        has_cost_centers_to_process >> rail.Label(
            "No") >> has_invalid_records >> rail.Label("Yes") >> format_logs
        has_invalid_records >> rail.Label("No") >> send_no_changes_email
        has_cost_centers_to_process >> rail.Label(
            "Yes") >> process_cost_centers >> [
            trigger_add_cost_centers,
            trigger_update_cost_centers,
        ] >> process_cost_centers_end >>\
        format_logs >> create_csv_log >> get_log_filename >> generate_download_link >>\
        upload_log_file >> send_completion_email
        send_completion_email >> log_to_sumo >> can_fail_dag
        can_fail_dag >> rail.Label("Yes") >> fail_dagrun

    return dag


# Create DAG instances for each environment
rail.for_each_instance(create_main_dag)
