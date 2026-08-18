from guidehouse.time_export.time_extract_to_datalake_unapproved_time.utils import custom_methods, request_payload

import rail
import pendulum


def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.process_time_extract_child_dag_id,
        description='GuideHouse Unapproved Time Export to Datalake - Process Time Extract Child DAG',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        create_report_filter = rail.PythonOperator(
            task_id="create_report_filter",
            python_callable=lambda dag_run: custom_methods.make_report_filter(dag_run),
        )

        time_extract_base_report_set_1_entry, time_extract_base_report_set_1_exit = rail.run_report(
            group_id="time_extract_base_report",
            report_params=lambda dag_run: {
                "reportParameters": [
                    {
                        "reportUri": dag_run.conf["report_uri"],
                        "filterValues": rail.result("create_report_filter"),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            target="artifact"
        )

        if_error_in_report_batch_result = rail.IfOperator(
            task_id="if_error_in_report_batch_result",
            test="{{ (result('time_extract_base_report.get_report_result') | load_json_artifact).reportGenerationResults[0].error | is_truthy }}",
            yes_task="fail_due_to_report_generation_error",
            no_task="if_report_has_data"
        )

        fail_due_to_report_generation_error = rail.FailOperator(
            task_id="fail_due_to_report_generation_error",
            message="Report Execution Failed"
        )

        if_report_has_data = rail.IfOperator(
            task_id='if_report_has_data',
            test="{{result('time_extract_base_report.get_report_result','has_data')}}",
            yes_task="has_expected_report_columns",
            no_task="stop_execution_due_to_no_data"
        )

        stop_execution_due_to_no_data = rail.EmptyOperator(
            task_id="stop_execution_due_to_no_data"
        )

        has_expected_report_columns = rail.IfOperator(
            task_id="has_expected_report_columns",
            test="{{ (result('time_extract_base_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload | \
                starts_with('%s') }}" % config.EXPECTED_REPORT_COLUMNS,
            yes_task="load_report_csv_file",
            no_task="fail_no_expected_columns"
        )

        fail_no_expected_columns = rail.FailOperator(
            task_id="fail_no_expected_columns",
            message='Base report column order does not match expected.'
        )

        load_report_csv_file = rail.LoadCSVFileOperator(
            task_id="load_report_csv_file",
            document="{{ (result('time_extract_base_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload }}"
        )

        create_collection_from_time_entry_data_csv = rail.CreateCollectionOperator(
            task_id="create_collection_from_time_entry_data_csv",
            source="{{ result('load_report_csv_file') }}",
            name="unapproved_time_entry_base_report_data",
            columns=config.REPORT_COLUMN_HEADER_MAP
        )

        query_tdg_time_entry_data = rail.QueryCollectionOperator(
            task_id="query_tdg_time_entry_data",
            name="tdg_time_entry_data",
            query="""SELECT * FROM unapproved_time_entry_base_report_data
                    WHERE NULLIF(short_entry_id, '') IS NOT NULL"""
        )

        has_no_tdg_records = rail.IfOperator(
            task_id="has_no_tdg_records",
            test="{{ result('query_tdg_time_entry_data', 'length') == 0 }}",
            yes_task="stop_execution_due_to_no_tdg_records",
            no_task="query_peoplesoft_india_time_entry_records",
        )

        stop_execution_due_to_no_tdg_records = rail.EmptyOperator(
            task_id="stop_execution_due_to_no_tdg_records"
        )

        query_peoplesoft_india_time_entry_records = rail.QueryCollectionOperator(
            task_id="query_peoplesoft_india_time_entry_records",
            name="peoplesoft_india_time_entry_records",
            query="""SELECT * FROM tdg_time_entry_data WHERE financial_system IN ('PeopleSoft', 'India')"""
        )

        transform_peoplesoft_india_time_entry_records = rail.DataAdaptorOperator(
            task_id="transform_peoplesoft_india_time_entry_records",
            source="{{ result('query_peoplesoft_india_time_entry_records') }}",
            columns=config.EXPORT_FILE_LAYOUT,
            data=lambda item, dag_run: custom_methods.transform_peoplesoft_india_time_entry_records(
                item, dag_run, config.TIME_OFF_PROJECT_TASK_MAPPER)
        )

        query_costpoint_time_entry_records = rail.QueryCollectionOperator(
            task_id="query_costpoint_time_entry_records",
            name="costpoint_time_entry_records",
            query="""SELECT * FROM tdg_time_entry_data WHERE financial_system = 'CostPoint'"""
        )

        query_distinct_tasks_per_user = rail.QueryCollectionOperator(
            task_id="query_distinct_tasks_per_user",
            name="distinct_tasks_per_user",
            query="""SELECT DISTINCT login_name, project_code, task_uri, task_name_full_path FROM costpoint_time_entry_records
                    WHERE NULLIF(task_uri, '') is NOT NULL"""
        )

        get_all_project_roles = rail.RepliconServicePageOperator(
            task_id="get_all_project_roles",
            endpoint="/services/ProjectRoleListService1.svc/GetData",
            data=lambda: {
                "page": 1,
                "pagesize": 1000,
                "columnUris": [
                    "urn:replicon:project-role-list-column:description",
                    "urn:replicon:project-role-list-column:project-role",
                ],
                "sort": [],
                "filterExpression": None,
            },
            page_handler=custom_methods.page_handler,
            all_result_data_handler=custom_methods.build_project_role_code_map,
        )

        login_name_task_uri_to_task_role_mapping = rail.RepliconServiceCallForEachItemOperator(
            task_id="login_name_task_uri_to_task_role_mapping",
            endpoint="/services/TaskService1.svc/BulkGetTaskResourceEstimateDetailsForTaskResourceUserAssignmentPairs",
            items="{{ result('query_distinct_tasks_per_user') }}",
            batch_size=10,
            data=lambda items: {
                "taskResourceUserAssignmentPairs": [
                    request_payload.build_task_resource_payload(item) for item in items
                ]
            },
            data_handler=lambda data, items: (items, data),
            flatten=True,
            all_result_data_handler=custom_methods.build_login_task_role_mapping
        )

        transform_costpoint_time_entry_records = rail.DataAdaptorOperator(
            task_id="transform_costpoint_time_entry_records",
            source="{{ result('query_costpoint_time_entry_records') }}",
            columns=config.EXPORT_FILE_LAYOUT,
            data=lambda item, dag_run: custom_methods.transform_costpoint_time_entry_records(
                item, dag_run, config.TIME_OFF_PROJECT_TASK_MAPPER)
        )

        create_transformed_time_entry_collection_start = rail.EmptyOperator(
            task_id="create_transformed_time_entry_collection_start"
        )

        create_transformed_peoplesoft_india_time_entry_records_collection = rail.CreateCollectionOperator(
            task_id="create_transformed_peoplesoft_india_time_entry_records_collection",
            source="{{ result('transform_peoplesoft_india_time_entry_records') }}",
            name="transform_peoplesoft_india_time_entry_records",
            columns=config.EXPORT_FILE_LAYOUT
        )

        create_transformed_costpoint_time_entry_records_collection = rail.CreateCollectionOperator(
            task_id="create_transformed_costpoint_time_entry_records_collection",
            source="{{ result('transform_costpoint_time_entry_records') }}",
            name="transform_costpoint_time_entry_records",
            columns=config.EXPORT_FILE_LAYOUT
        )

        create_transformed_time_entry_collection_end = rail.EmptyOperator(
            task_id="create_transformed_time_entry_collection_end"
        )

        merge_transformed_time_entry_collections = rail.QueryCollectionOperator(
            task_id="merge_transformed_time_entry_collections",
            name="final_transformed_time_entry_data",
            query="""SELECT * FROM transform_peoplesoft_india_time_entry_records
                    UNION ALL
                    SELECT * FROM transform_costpoint_time_entry_records"""
        )

        write_final_transformed_time_entry_data_to_csv = rail.WriteCSVFileOperator(
            task_id="write_final_transformed_time_entry_data_to_csv",
            source="{{ result('merge_transformed_time_entry_collections') }}",
            header=config.EXPORT_FILE_HEADER,
            row=lambda item: [item.get(column, "") for column in config.EXPORT_FILE_LAYOUT],
            delimiter="|"
        )

        get_file_name = rail.PythonOperator(
            task_id="get_file_name",
            python_callable=lambda dag_run: config.FILE_NAME_FORMAT.format(
                week_type=dag_run.conf["week_type"],
                file_name_prefix=dag_run.conf["file_name_prefix"],
                instance=config.instance.upper(),
                timestamp=pendulum.now(dag_run.conf["timezone"]).strftime("%Y%m%d_%H%M%S")
            )
        )

        encrypt_file = rail.PGPEncryptionOperator(
            task_id="encrypt_file",
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('write_final_transformed_time_entry_data_to_csv') }}",
        )

        upload_file_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_file_to_sftp",
            sftp_conn_id=config.sftp_conn_id,
            content="{{ result('encrypt_file') }}",
            remote_filepath=config.sftp_remote_filepath + '/' + "{{ result('get_file_name') }}"
        )

        send_success_email = rail.EmailOperator(
            task_id="send_success_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Replicon Data Lake unapproved time data extract is completed - {{ dag_run.conf.week_type }} - {{ result('get_file_name') }}",
            html_content="/templates/email_valid_export_complete.html",
            params={"upload_file_path": config.sftp_remote_filepath},
        )


        create_report_filter >> time_extract_base_report_set_1_entry >> time_extract_base_report_set_1_exit >> if_error_in_report_batch_result

        if_error_in_report_batch_result >> rail.Label("Yes") >> fail_due_to_report_generation_error
        if_error_in_report_batch_result >> rail.Label("No") >> if_report_has_data

        if_report_has_data >> rail.Label("No") >> stop_execution_due_to_no_data
        if_report_has_data >> rail.Label("Yes") >> has_expected_report_columns

        has_expected_report_columns >> rail.Label("No") >> fail_no_expected_columns
        has_expected_report_columns >> rail.Label("Yes") >> load_report_csv_file >>\
            create_collection_from_time_entry_data_csv >> query_tdg_time_entry_data >> has_no_tdg_records
        has_no_tdg_records >> rail.Label("Yes") >> stop_execution_due_to_no_tdg_records
        has_no_tdg_records >> rail.Label("No") >> query_peoplesoft_india_time_entry_records >> transform_peoplesoft_india_time_entry_records >>\
        query_costpoint_time_entry_records >> query_distinct_tasks_per_user >> get_all_project_roles >> login_name_task_uri_to_task_role_mapping >> transform_costpoint_time_entry_records >>\
        create_transformed_time_entry_collection_start >> [
            create_transformed_peoplesoft_india_time_entry_records_collection,
            create_transformed_costpoint_time_entry_records_collection,
        ] >> create_transformed_time_entry_collection_end >> merge_transformed_time_entry_collections >> write_final_transformed_time_entry_data_to_csv >>\
        get_file_name >> encrypt_file >> upload_file_to_sftp >> send_success_email

        return dag

rail.for_each_instance(create_child_dag)