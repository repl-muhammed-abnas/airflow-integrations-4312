import rail
from datetime import timedelta
from airflow.models import Variable
from unisys.purchase_order_import.utils import request_payload, helpers, response_filters, custom_methods


def create_master_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f"Unisys Purchase Order IDs Import - Master DAG {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(minutes=5),
        max_active_runs=config.max_active_run_master,
        default_args={
            "execution_timeout": timedelta(days=config.execution_timeout_days),
        },
    ) as dag:
        
        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id="new_file_sensor",
            path=config.input_file_path,
            sftp_conn_id=config.sftp_conn_id,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout),
        )

        if_new_file_found = rail.IfOperator(
            task_id="if_new_file_found",
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task="is_csv_pgp",
            no_task="delete_this_dag_run"
        )

        is_csv_pgp = rail.IfOperator(
            task_id="is_csv_pgp",
            test=lambda: rail.result("new_file_sensor").lower().endswith(("csv", "pgp")),
            yes_task="download_file",
            no_task="send_bad_file_format_email"
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id="send_bad_file_format_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Purchase Order Import - Invalid File Format - {{ current_time_in_specified_tz() }}",
            html_content="templates/emails/bad_file_format.html",
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id="download_file",
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath='{{ result("new_file_sensor") }}',
        )

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

        load_data = rail.LoadCSVFileOperator(
            task_id="load_data",
            document="{{ result('get_input_data') }}",
            encoding="utf-8-sig",
        )

        create_processing_log = rail.CreateLogOperator(
            task_id="create_processing_log")
        
        create_input_data_collection = rail.CreateCollectionOperator(
            task_id="create_input_data_collection",
            source="{{ result('load_data') }}",
            name="inputdatacollection",
            columns={
                "Purchase Order ID": "purchase_order_id",
            },
        )

        get_distinct_purchase_order_ids = rail.QueryCollectionOperator(
            task_id="get_distinct_purchase_order_ids",
            name="purchase_order_ids",
            query="""SELECT DISTINCT(purchase_order_id) FROM inputdatacollection
                    WHERE NULLIF(TRIM(purchase_order_id),"") IS NOT NULL;""",
        )

        has_purchase_order_ids = rail.IfOperator(
            task_id="has_purchase_order_ids",
            test="{{ result('get_distinct_purchase_order_ids','length') > 0 }}",
            yes_task="get_all_departments",
            no_task="send_no_purchase_order_ids_email"
        )

        get_all_departments = rail.RepliconServicePageOperator(
            task_id="get_all_departments",
            endpoint=config.get_hierarchy_data_endpoint,
            data=request_payload.get_all_departments_payload,
            page_handler=helpers.page_handler,
            all_result_data_handler=response_filters.extract_department_groups,
        )

        get_level_0_department = rail.PythonOperator(
            task_id="get_level_0_department",
            python_callable=lambda: response_filters.get_department_uri(
            rail.result("get_all_departments"), hierarchy_level=0
            ),
        )

        get_level_1_departments = rail.PythonOperator(
            task_id="get_level_1_departments",
            python_callable=lambda: response_filters.get_all_rows(
            rail.result("get_all_departments"), hierarchy_level=1
            ),
        )

        create_existing_department_collection = rail.CreateCollectionOperator(
            task_id="create_existing_purchase_order_ids_collection",
            source="{{ result('get_level_1_departments') }}",
            name="existing_purchase_order_ids",
            columns={
                "purchase_order_ids": "purchase_order_ids",
            },
        )

        get_new_purchase_order_ids = rail.QueryCollectionOperator(
            task_id="get_new_purchase_order_ids",
            name="new_purchase_order_ids",
            query="""SELECT poi.purchase_order_id FROM purchase_order_ids poi
                    LEFT JOIN existing_purchase_order_ids epi
                    ON LOWER(poi.purchase_order_id) = LOWER(epi.purchase_order_ids)
                    WHERE epi.purchase_order_ids IS NULL;""",
        )

        send_no_purchase_order_ids_email = rail.EmailOperator(
            task_id="send_no_purchase_order_ids_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Purchase Order Import - No Purchase Order IDs found - {{ current_time_in_specified_tz() }}",
            html_content="templates/emails/no_purchase_order_ids_format.html",
        )

        send_no_new_purchase_order_ids_email = rail.EmailOperator(
            task_id="send_no_new_purchase_order_ids_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Purchase Order Import - No New Purchase Order IDs found - {{ current_time_in_specified_tz() }}",
            html_content="templates/emails/no_new_purchase_order_ids_format.html",
        )

        if_new_purchase_order_ids_found = rail.IfOperator(
            task_id="if_new_purchase_order_ids_found",
            test="{{ result('get_new_purchase_order_ids','length') > 0 }}",
            yes_task="process_department_groups",
            no_task="send_no_new_purchase_order_ids_email",
        )

        process_department_groups = rail.EmptyOperator(
            task_id="process_department_groups"
        )

        trigger_add_department_groups = rail.trigger_parallel_dagrun(
            task_id="trigger_add_department_groups",
            items=lambda: rail.result("get_new_purchase_order_ids"),
            trigger_dag_id=config.process_department_groups_child_dag_id,
            conf=lambda item: {
                **item,
                "processing_log": rail.result("create_processing_log"),
                "parent_department": rail.result("get_level_0_department"),
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            parallel_count=config.trigger_parallel_dagrun_count_process_department_groups,
        )

        process_department_groups_end = rail.EmptyOperator(
            task_id="process_department_groups_end"
        )

        format_logs = rail.PythonOperator(
            task_id="format_logs", python_callable=custom_methods.format_logs_callable
        )

        create_csv_log = rail.WriteCSVFileOperator(
            task_id="create_csv_log",
            source="{{ result('format_logs') }}",
            header=[
                "purchase_order_id",
                "status",
                "details",
                "ecid",
            ],
            row=[
                "{{ item.properties.purchase_order_id }}",
                "{{ item.properties.status }}",
                "{{ item.properties.details }}",
                "{{ item.ecid }}",
            ],
            footer=['Number of records found: {{ result("create_input_data_collection","length")}}',
                    'Number of records processed: {{ result("format_logs", key="total_record_count")}}',
                    'Number of success records: {{ result("format_logs", key="success_record_count")}}',
                    'Number of error records: {{ result("format_logs", key="error_record_count") }}',
                    'Number of exception records: 0',
                    ]
        )

        get_log_filename = rail.PythonOperator(
            task_id="get_log_filename",
            python_callable=lambda:rail.render_template(
                "{{get_company_key()}}_{{ current_time_in_specified_tz(fmt='%Y%m%d_%H%M%S') }}_purchase_order_ids_import_log.csv")
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('create_csv_log')}}",
            output_file_name="{{ result('get_log_filename') }}",
            expires_in_seconds=7*24*60*60,
        )

        upload_log_file = rail.SFTPUploadFileOperator(
            task_id="upload_log_file",
            sftp_conn_id=config.sftp_conn_id,
            content="{{ result('create_csv_log') }}",
            remote_filepath=config.log_filepath
            + "/{{result('get_log_filename')}}",
        )

        send_completion_email = rail.EmailOperator(
            task_id="send_completion_email",
            to=config.tenant_email,
            bcc="{%- if result('format_logs', 'error_record_count') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon Purchase Order ID Import " }} \
                {%- if result("format_logs", key="error_record_count") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    completed successfully  \
                {%- endif -%} \
                {{ " - " + current_time_in_specified_tz() }}',
            html_content="templates/emails/import_complete.html",
            params={
                "log_filepath": config.log_filepath,
            }
        )

        move_the_input_file = rail.SFTPMoveFileOperator(
            task_id = "move_the_input_file",
            sftp_conn_id = config.sftp_conn_id,
            existing_filename = '{{ result("new_file_sensor") }}',
            new_filename=config.workday_input_filepath
            + "/{{ result('new_file_sensor') |file_name }}",
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger'
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test="{{ get_error_message() | is_truthy }}",
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun", message="{{ get_error_message() }}"
        )

        delete_this_dag_run = rail.DeleteCurrentDagRunOperator(
            task_id="delete_this_dag_run"
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        new_file_sensor >> if_new_file_found
        if_new_file_found >> rail.Label("Yes") >> is_csv_pgp
        is_csv_pgp >> rail.Label("No") >> send_bad_file_format_email >> move_the_input_file >> finish
        is_csv_pgp >> rail.Label("Yes") >> download_file >> can_decrypt_file
        can_decrypt_file >> rail.Label("Yes") >> decrypt_file >> get_input_data
        can_decrypt_file >> rail.Label("No") >> get_input_data
        get_input_data >> load_data >> create_processing_log >> create_input_data_collection >>\
        get_distinct_purchase_order_ids >> has_purchase_order_ids
        has_purchase_order_ids >> rail.Label("No") >> send_no_purchase_order_ids_email >> move_the_input_file >> finish
        has_purchase_order_ids >> rail.Label("Yes") >> get_all_departments >> get_level_0_department >> get_level_1_departments >> create_existing_department_collection >>\
        get_new_purchase_order_ids >> if_new_purchase_order_ids_found >> rail.Label("No") >> send_no_new_purchase_order_ids_email >> move_the_input_file >> finish
        get_new_purchase_order_ids >> if_new_purchase_order_ids_found >> rail.Label("Yes") >> process_department_groups >>\
        trigger_add_department_groups >> process_department_groups_end >>\
        format_logs >> create_csv_log >> get_log_filename >> generate_download_link >> upload_log_file >> send_completion_email
        send_completion_email >> move_the_input_file >> log_to_sumo >> can_fail_dag
        can_fail_dag >> rail.Label("Yes") >> fail_dagrun >> finish
        
        if_new_file_found >> rail.Label("No") >> delete_this_dag_run >> finish
    
    return dag

rail.for_each_instance(create_master_dag)