"""
iPipeline User Import - Master DAG

File-based SFTP processing for iPipeline user data.
Uses file sensor to detect new CSV files, processes them, and triggers individual user DAGs.

Pattern: File Sensor → Download CSV → Parse CSV → Process Users → Archive
Based on frontdoorinc/user_import pattern.
"""

from datetime import timedelta
import json
from pendulum import datetime
from ipipeline.user_import_v2.utils import custom_methods, request_payload
from ipipeline.user_import_v2.tasks import process_user_groups_data, process_user_prerequisites
from airflow.models import Variable
import rail

null = None


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f"iPipeline User Import Master - File Processing Master {config.instance}",
        start_date=datetime(2025, 9, 1, tz=config.time_zone),
        company_key=config.company_key,
        schedule_interval=timedelta(seconds=config.schedule_interval),
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout),
        )

        logging_details = rail.PythonOperator(
            task_id='logging_details',
            python_callable=custom_methods.logging_details,
            op_args=[config.time_zone,
                     config.STANDARD_EMAIL_DATE_FORMAT, config.YMD_DATE_FORMAT]
        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" }}',
            yes_task='download_input_file',
            no_task='send_invalid_format_email',
        )

        send_invalid_format_email = rail.EmailOperator(
            task_id='send_invalid_format_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon User Import - Invalid Input File Format - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/invalid_format_email.html"
        )

        download_input_file = rail.SFTPDownloadFileOperator(
            task_id='download_input_file',
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

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            new_filename=config.archive_filepath +
            'Archive_{{ dag_run_ecid() }}_{{ result("new_file_sensor") | file_name }}',
            existing_filename=config.input_filepath +
            '/{{ result("new_file_sensor") | file_name }}',
        )

        parse_csv = rail.LoadCSVFileOperator(
            task_id="parse_csv",
            document="{{ result('download_input_file') }}"
        )

        can_use_reference_file = rail.IfOperator(
            task_id="can_use_reference_file",
            test=lambda: Variable.get(
                config.can_use_reference_file_var_name, default_var='true').lower() == 'true',
            yes_task="download_reference_file",
            no_task="create_mapped_collection"
        )

        # Download previous reference file (if exists) for hash comparison
        download_reference_file = rail.SFTPDownloadFileOperator(
            task_id='download_reference_file',
            remote_filepath=config.reference_filepath + config.reference_filename
        )

        archive_reference_file = rail.SFTPMoveFileOperator(
            task_id='archive_reference_file',
            new_filename=config.archive_reference_filepath +
            'Archive_{{ dag_run_ecid() }}_' + config.reference_filename,
            existing_filename=config.reference_filepath + config.reference_filename,
        )

        # Parse reference file CSV (if exists)
        parse_reference_csv = rail.LoadCSVFileOperator(
            task_id="parse_reference_csv",
            document="{{ result('download_reference_file') }}"
        )

        # Create reference data collection for comparison
        create_reference_collection = rail.CreateCollectionOperator(
            task_id="create_reference_collection",
            source="{{ result('parse_reference_csv') }}",
            name="reference_data"
        )

        # Create collection from mapped fields
        create_mapped_collection = rail.CreateCollectionOperator(
            task_id="create_mapped_collection",
            source="{{ result('parse_csv') }}",
            columns=custom_methods.get_input_columns(
                config.input_fields_mapper_data),
            name="raw_input_user_data"
        )

        has_records_in_input_file = rail.IfOperator(
            task_id='has_records_in_input_file',
            test='{{ result("create_mapped_collection", "length") > 0 }}',
            yes_task='create_csv_with_hash',
            no_task='send_no_data_email'
        )

        send_no_data_email = rail.EmailOperator(
            task_id='send_no_data_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon User Import - No Data - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/no_records_email.html"
        )

        # Process data with client-provided HASH field and business logic
        create_csv_with_hash = rail.WriteCSVFileOperator(
            task_id='create_csv_with_hash',
            source="{{ result('create_mapped_collection') }}",
            header=[
                'employee_id', 'first_name', 'last_name', 'display_name', 'email', 'start_date', 'end_date',
                'login_name', 'authentication_type', 'authentication_id', 'supervisor', 'language', 'fte',
                'level', 'title', 'location_level_1', 'location_level_2', 'employee_schedule',
                'department_level_1', 'department_level_2', 'employee_category', 'scheduled_hours',
                'elt', 'uksick', 'transfer_date', 'employee_type', 'paygroup', 'project', 'seniority_level',
                'hash_value', 'hash_sha256'
            ],
            row=custom_methods.process_user_row_with_hash
        )

        # Create collection from processed data with hash for comparison
        create_current_data_collection = rail.CreateCollectionOperator(
            task_id="create_current_data_collection",
            source="{{ result('create_csv_with_hash') }}",
            name="current_data"
        )

        # Compare hashes and identify new/updated records
        identify_changed_records = rail.QueryCollectionOperator(
            task_id="identify_changed_records",
            query=request_payload.get_changed_records_query(
                config.can_use_reference_file_var_name),
            name="changed_records"
        )

        # Check if there are any changes to process
        check_for_changes = rail.IfOperator(
            task_id="check_for_changes",
            test='{{result("identify_changed_records", "length") > 0}}',
            yes_task="start_batch_task",
            no_task="upload_reference_file"
        )

        start_batch_task = rail.EmptyOperator(
            task_id='start_batch_task'
        )

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var="true").lower() == "true",
            yes_task="batch_task",
            no_task="process_changed_data"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="process_changed_data",
            end_task="start_user_processing"
        )

        process_changed_data = rail.EmptyOperator(
            task_id='process_changed_data'
        )

        create_users_collection = rail.CreateCollectionOperator(
            task_id="create_users_collection",
            source="{{ result('identify_changed_records') }}",
            name="users_changed_data"
        )

        query_distinct_login_names = rail.QueryCollectionOperator(
            task_id="query_distinct_login_names",
            query="SELECT DISTINCT login_name FROM users_changed_data WHERE NULLIF(login_name, '') IS NOT NULL",
            name="distinct_login_names"
        )

        create_groups_log = rail.CreateLogOperator(
            task_id='create_groups_log'
        )

        create_supervisors_pending_log = rail.CreateLogOperator(
            task_id='create_supervisors_pending_log'
        )

        get_all_groups_data = process_user_groups_data.get_all_groups_data(
            "all")

        get_all_prerequisites_data = process_user_prerequisites.get_all_prerequisites_data(
            config)

        process_groups_creation = rail.EmptyOperator(
            task_id='process_groups_creation'
        )

        # Individual group analysis tasks
        analyze_departments_to_create = rail.PythonOperator(
            task_id='analyze_departments_to_create',
            python_callable=lambda: custom_methods.analyze_departments_to_create(
                config.defaults_mapper_data["root_department"])
        )

        analyze_locations_to_create = rail.PythonOperator(
            task_id='analyze_locations_to_create',
            python_callable=custom_methods.analyze_locations_to_create
        )

        analyze_employee_types_to_create = rail.PythonOperator(
            task_id='analyze_employee_types_to_create',
            python_callable=custom_methods.analyze_employee_types_to_create
        )

        analyze_project_roles_to_create = rail.PythonOperator(
            task_id='analyze_project_roles_to_create',
            python_callable=custom_methods.analyze_project_roles_to_create
        )

        # Conditional check tasks for each group type
        check_if_departments_need_creation = rail.IfOperator(
            task_id='check_if_departments_need_creation',
            test=custom_methods.check_if_departments_need_creation,
            yes_task='process_department_creation',
            no_task='check_if_locations_need_creation'
        )

        check_if_locations_need_creation = rail.IfOperator(
            task_id='check_if_locations_need_creation',
            test=custom_methods.check_if_locations_need_creation,
            yes_task='process_location_creation',
            no_task='check_if_employee_types_need_creation'
        )

        check_if_employee_types_need_creation = rail.IfOperator(
            task_id='check_if_employee_types_need_creation',
            test=custom_methods.check_if_employee_types_need_creation,
            yes_task='process_employeetype_creation',
            no_task='check_if_project_roles_need_creation'
        )

        check_if_project_roles_need_creation = rail.IfOperator(
            task_id='check_if_project_roles_need_creation',
            test=custom_methods.check_if_project_roles_need_creation,
            yes_task='process_projectrole_creation',
            no_task='gather_all_the_run_ids'
        )

        process_department_creation = rail.EmptyOperator(
            task_id='process_department_creation'
        )

        # PWC-style parallel trigger for department hierarchies
        create_departments = rail.TriggerDagRunForEachItemOperator(
            task_id="create_departments",
            items="{{ result('analyze_departments_to_create') | to_json }}",
            trigger_dag_id=config.create_departments_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **item,
                "groups_log_artifact": rail.result("create_groups_log")
            }
        )

        process_location_creation = rail.EmptyOperator(
            task_id='process_location_creation'
        )

        create_locations = rail.TriggerDagRunForEachItemOperator(
            task_id="create_locations",
            items="{{ result('analyze_locations_to_create') | to_json }}",
            trigger_dag_id=config.create_locations_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **item,
                "groups_log_artifact": rail.result("create_groups_log")
            }
        )

        process_employeetype_creation = rail.EmptyOperator(
            task_id='process_employeetype_creation'
        )

        create_employeetypes = rail.TriggerDagRunForEachItemOperator(
            task_id="create_employeetypes",
            items="{{ result('analyze_employee_types_to_create') | to_json }}",
            trigger_dag_id=config.create_employeetypes_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **item,
                "groups_log_artifact": rail.result("create_groups_log")
            }
        )

        process_projectrole_creation = rail.EmptyOperator(
            task_id='process_projectrole_creation'
        )

        create_project_roles = rail.TriggerDagRunForEachItemOperator(
            task_id="create_project_roles",
            items="{{ result('analyze_project_roles_to_create') | to_json }}",
            trigger_dag_id=config.create_projectroles_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "project_roles_to_create": item.get("project_role_name"),
                "groups_log_artifact": rail.result("create_groups_log")
            }
        )

        def gather_all_the_run_ids_callable():
            run_ids = []

            # Get results from all tasks
            results = [
                rail.result(create_departments.task_id),
                rail.result(create_locations.task_id),
                rail.result(create_employeetypes.task_id),
                rail.result(create_project_roles.task_id)
            ]

            # Add all non-empty results
            for result in results:
                if result:  # Skip None and empty lists
                    # result should always be a list here
                    run_ids.extend(result)

            return run_ids

        gather_all_the_run_ids = rail.PythonOperator(
            task_id="gather_all_the_run_ids",
            python_callable=gather_all_the_run_ids_callable
        )

        wait_for_groups_creation = rail.WaitForDagRunsSensor(
            task_id='wait_for_groups_creation',
            dag_runs='{{ result("gather_all_the_run_ids") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        check_error_in_groups_creation = rail.FilterLogEntriesOperator(
            task_id='check_error_in_groups_creation',
            log='{{ result("create_groups_log") }}',
            severity='Error'
        )

        is_any_group_creation_failed = rail.IfOperator(
            task_id='is_any_group_creation_failed',
            test='{{ result("check_error_in_groups_creation", "length") > 0 }}',
            yes_task='fail_user_import_due_to_group_errors',
            no_task='process_updated_groups_data'
        )

        fail_user_import_due_to_group_errors = rail.FailOperator(
            task_id='fail_user_import_due_to_group_errors',
            message='User import failed due to errors in group creation. Check groups log for details.'
        )

        process_updated_groups_data = rail.EmptyOperator(
            task_id='process_updated_groups_data'
        )

        # Get all organizational group data (locations, departments, cost centers, employee types)
        get_updated_groups_data = process_user_groups_data.get_all_groups_data(
            "updated")

        start_user_processing = rail.EmptyOperator(
            task_id='start_user_processing'
        )

        required_timeoff_types_data_artifact = rail.PythonOperator(
            task_id='required_timeoff_types_data_artifact',
            python_callable=lambda: rail.write_artifact(
                json.dumps(rail.result('get_required_time_off_types')))
        )

        # Process individual user records
        process_user_record = rail.trigger_parallel_dagrun(
            task_id="process_user_record",
            items='{{ result("create_users_collection") }}',
            trigger_dag_id=config.process_user_record_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            parallel_count=config.trigger_process_user_record_child_parallel_count,
            conf=lambda item: {
                **custom_methods.create_users_payload_from_variable(item, config),
                "required_timeoff_types_data_artifact": rail.result('required_timeoff_types_data_artifact'),
                "current_date": rail.result('logging_details')['current_date'],
                "log_artifact": rail.result('create_groups_log'),
                "supervisor_log": rail.result('create_supervisors_pending_log'),
                "oef_data": {
                    oef["oef_name"]: rail.result("get_all_user_oefs").get(
                        f"{oef['field_name']}_oef_uri")
                    for oef in config.oef_field_mapper_data
                },
                "all_users_login_names": rail.result("query_distinct_login_names"),
            }
        )

        # =======================
        # 9. GATHER RESULTS AND LOGGING
        # =======================

        get_process_user_record_dag_ids = rail.PythonOperator(
            task_id='get_process_user_record_dag_ids',
            python_callable=lambda: custom_methods.get_process_users_dag_ids(
                config.trigger_process_user_record_child_parallel_count),
            show_return_value_in_logs=False
        )

        gather_user_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_logs',
            dag_runs='{{ result("get_process_user_record_dag_ids") }}',
            dagrun_task_id='user_child_log',
            execution_timeout=timedelta(
                hours=config.gather_logs_timeout_hours),
            flatten=True
        )

        filter_pending_supervisor_records = rail.FilterLogEntriesOperator(
            task_id='filter_pending_supervisor_records',
            log='{{ result("create_supervisors_pending_log")}}',
            severity='Pending'
        )

        if_filtered_pending_supervisor_records = rail.IfOperator(
            task_id='if_filtered_pending_supervisor_records',
            test='{{ result("filter_pending_supervisor_records", "length") > 0 }}',
            yes_task='create_supervisor_assignment_log',
            no_task='format_logs'
        )

        create_supervisor_assignment_log = rail.CreateLogOperator(
            task_id='create_supervisor_assignment_log'
        )

        # Process individual pending supervisor assignment records
        process_pending_supervisor_records = rail.TriggerDagRunForEachItemOperator(
            task_id="process_pending_supervisor_records",
            items='{{ result("filter_pending_supervisor_records") }}',
            trigger_dag_id=config.supervisor_assignment_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **dict(item['properties'].items()),
                "current_date": rail.result('logging_details')['current_date'],
                "supervisor_assign_log": rail.result('create_supervisor_assignment_log')
            }
        )

        format_logs = rail.CreateCollectionOperator(
            task_id='format_logs',
            source=custom_methods.do_format_logs,
            columns=["employeeid", "action", "status", "details", "runid"]
        )

        # =======================
        # 11. LOG GENERATION AND COMPLETION
        # =======================

        has_any_entries_in_log = rail.IfOperator(
            task_id='has_any_entries_in_log',
            test='{{ result("format_logs", "length") > 0 }}',
            yes_task='trigger_log_generation',
            no_task='upload_reference_file'
        )

        trigger_log_generation = rail.TriggerDagRunOperator(
            task_id='trigger_log_generation',
            trigger_dag_id=config.process_log_generation_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: {
                **rail.result("logging_details"),
                'userlogs': rail.result("format_logs") if rail.result("format_logs") else [],
                'success_count': rail.result("format_logs", key="get_logged_success"),
                'error_count': rail.result("format_logs", key="get_logged_errors"),
                'exception_count': rail.result("format_logs", key="get_logged_exceptions"),
                'total_record_count': rail.result("create_users_collection", key="length")
            }
        )

        # =======================
        # 12. UPDATE REFERENCE FILE FOR NEXT RUN
        # =======================

        # Upload current processed file as new reference file for next comparison
        upload_reference_file = rail.SFTPUploadFileOperator(
            task_id='upload_reference_file',
            content="{{ result('create_csv_with_hash') }}",
            remote_filepath=config.reference_filepath + config.reference_filename
        )

        finish_import = rail.EmptyOperator(
            task_id='finish_import'
        )

        # =======================
        # TASK DEPENDENCIES - SEQUENTIAL FLOW
        # =======================

        # 1. File detection and initial setup
        new_file_sensor >> logging_details >> is_csv

        # 2. File validation and download
        is_csv >> rail.Label(
            "Yes") >> download_input_file >> was_new_file_found
        is_csv >> rail.Label("No") >> send_invalid_format_email

        # 3. File processing decision
        was_new_file_found >> rail.Label("Yes") >> archive_file >> parse_csv
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun

        # 5. CSV processing and reference file handling (sequential)
        parse_csv >> can_use_reference_file >> rail.Label(
            "Yes") >> download_reference_file
        can_use_reference_file >> rail.Label("No") >> create_mapped_collection
        download_reference_file >> archive_reference_file >> parse_reference_csv
        parse_reference_csv >> create_reference_collection
        create_reference_collection >> create_mapped_collection
        create_mapped_collection >> has_records_in_input_file
        has_records_in_input_file >> rail.Label("Yes") >> create_csv_with_hash
        has_records_in_input_file >> rail.Label("No") >> send_no_data_email
        create_csv_with_hash >> create_current_data_collection
        create_current_data_collection >> identify_changed_records
        identify_changed_records >> check_for_changes

        # 6. Change processing decision
        check_for_changes >> rail.Label(
            "Yes") >> start_batch_task >> can_run_batch_task
        check_for_changes >> rail.Label("No") >> upload_reference_file

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> start_user_processing
        can_run_batch_task >> rail.Label("No") >> process_changed_data
        # 7. User data processing (sequential)
        process_changed_data >> create_users_collection
        create_users_collection >> query_distinct_login_names >> create_groups_log

        # 8. Groups and prerequisites data (sequential, no parallel)
        create_groups_log >> create_supervisors_pending_log >> get_all_groups_data
        get_all_groups_data >> get_all_prerequisites_data
        get_all_prerequisites_data >> process_groups_creation

        # 9. Groups creation with conditional analysis and creation
        process_groups_creation >> analyze_departments_to_create
        analyze_departments_to_create >> analyze_locations_to_create
        analyze_locations_to_create >> analyze_employee_types_to_create

        # Conditional group creation flow
        analyze_employee_types_to_create >> analyze_project_roles_to_create
        analyze_project_roles_to_create >> check_if_departments_need_creation
        check_if_departments_need_creation >> rail.Label("Yes") >> process_department_creation >> create_departments \
            >> check_if_locations_need_creation
        check_if_departments_need_creation >> rail.Label(
            "No") >> check_if_locations_need_creation
        check_if_locations_need_creation >> rail.Label("Yes") >> process_location_creation >> create_locations \
            >> check_if_employee_types_need_creation
        check_if_locations_need_creation >> rail.Label(
            "No") >> check_if_employee_types_need_creation
        check_if_employee_types_need_creation >> rail.Label("Yes") >> process_employeetype_creation >> create_employeetypes \
            >> check_if_project_roles_need_creation
        check_if_employee_types_need_creation >> rail.Label(
            "No") >> check_if_project_roles_need_creation
        check_if_project_roles_need_creation >> rail.Label("Yes") >> process_projectrole_creation >> create_project_roles \
            >> gather_all_the_run_ids
        check_if_project_roles_need_creation >> rail.Label(
            "No") >> gather_all_the_run_ids

        gather_all_the_run_ids >> wait_for_groups_creation >> check_error_in_groups_creation >> is_any_group_creation_failed

        is_any_group_creation_failed >> rail.Label(
            "No") >> process_updated_groups_data >> get_updated_groups_data >> start_user_processing
        is_any_group_creation_failed >> rail.Label(
            "Yes") >> fail_user_import_due_to_group_errors

        # Groups creation flows directly to get_updated_groups_data (trigger_parallel_dagrun handles waiting)

        # 11. User processing (sequential)
        start_user_processing >> required_timeoff_types_data_artifact >> process_user_record
        process_user_record >> get_process_user_record_dag_ids >> gather_user_logs
        gather_user_logs >> filter_pending_supervisor_records >> if_filtered_pending_supervisor_records
        if_filtered_pending_supervisor_records >> rail.Label(
            "Yes") >> create_supervisor_assignment_log >> process_pending_supervisor_records
        if_filtered_pending_supervisor_records >> rail.Label(
            "No") >> format_logs
        process_pending_supervisor_records >> format_logs >> has_any_entries_in_log

        # 13. Log generation decision
        has_any_entries_in_log >> rail.Label("Yes") >> trigger_log_generation
        has_any_entries_in_log >> rail.Label("No") >> upload_reference_file

        # 14. Final completion (sequential)
        trigger_log_generation >> upload_reference_file
        upload_reference_file >> finish_import

        return dag


# Create the DAG instance
rail.for_each_instance(create_main_dag)
