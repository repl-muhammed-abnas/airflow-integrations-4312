# main.py
from pendulum import datetime as dt, now
import rail
import os
from datetime import timedelta

# Import utilities
from tsystems.cost_center_hierarchy_import_v1.utils import custom_methods, request_payload, response_filter
from tsystems.cost_center_hierarchy_import_v1 import config


def create_cost_center_hierarchy_import_dag(config):
    """
    Creates the main DAG for T-Systems Cost Center Hierarchy Import

    :param config: Configuration module with settings for the instance
    :return: The created DAG
    """
    with rail.create_airflow_dag(
        dag_id=config.mast_dag_id,
        description=f'T-Systems Cost Center Hierarchy Import - Master DAG ({config.instance})',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=30),
        start_date=dt(2025,6,1, tz=config.timezone),
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        log_start_time = rail.PythonOperator(
            task_id = "log_start_time",
            python_callable=lambda: now(config.timezone).strftime("%y-%m-%dT%H:%M:%S%z")
        )

        # Check for new file in SFTP
        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout),
        )

        # Check if file is CSV
        is_csv = rail.IfOperator(
            task_id='is_csv',
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" }}',
            yes_task='download_file',
            no_task='send_invalid_format_email'
        )

        # Send email for invalid file format
        send_invalid_format_email = rail.EmailOperator(
            task_id='send_invalid_format_email',
            to=config.tenant_email,
            cc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Replicon Cost Center Hierarchy Import - Invalid Format | {{ current_time_in_specified_tz() }}",
            html_content="templates/emails/invalid_format_email.html"
        )

        # Archive original file
        archive_file_bad_format = rail.SFTPMoveFileOperator(
            task_id='archive_file_bad_format',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=f"{config.archive_filepath}/{{{{ dag_run_ecid() | replace(':', '-')}}}}_{{{{ result('new_file_sensor') | file_name }}}}"
        )

        # Download file from SFTP
        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}",
        )


        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            no_task='delete_this_dagrun',
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')


        # Archive original file
        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=f"{config.archive_filepath}/{{{{ dag_run_ecid() | replace(':', '-')}}}}_{{{{ result('new_file_sensor') | file_name }}}}"
        )

        # Check if file has content
        def check_file_content():
            with rail.existing_artifact(rail.result('download_file')) as artifact:
                return os.path.getsize(artifact.local_filename) > 0

        has_file_content = rail.IfOperator(
            task_id='has_file_content',
            test=check_file_content,
            yes_task='load_cost_center_data',
            no_task='send_empty_file_email'
        )

        # Load and process CSV file
        load_cost_center_data = rail.LoadCSVFileOperator(
            task_id='load_cost_center_data',
            document="{{ result('download_file') }}",
            delimiter=";",
            encoding="utf-8-sig"
        )

        # Send email if file is empty
        send_empty_file_email = rail.EmailOperator(
            task_id='send_empty_file_email',
            to=config.tenant_email,
            cc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Replicon Cost Center Hierarchy Import - No records |{{ current_time_in_specified_tz() }}",
            html_content="templates/emails/blank_feed_file_email.html"
        )

        # Create collection from CSV data with SHA256 hash added
        create_cost_center_hash = rail.DataAdaptorOperator(
            task_id='create_cost_center_hash',
            source="{{ result('load_cost_center_data') }}",
            columns=['Name', 'Code', 'Description', 'Status', 'Cost Center Manager', 'SHA256', 'CostCenterDetailsSHA256'],
            data=custom_methods.compute_sha256_hash,
        )

        # created the collection
        create_cost_center_collection = rail.CreateCollectionOperator(
            task_id = "create_cost_center_collection",
            source="{{result('create_cost_center_hash')}}",
            name="raw_cost_centers_with_hash",
            columns={
                'Name': 'Name',
                'Code': 'Code',
                'Description': 'Description',
                'Status': 'Status',
                'Cost Center Manager': 'Cost_Center_Manager',
                'SHA256': 'SHA256',
                'CostCenterDetailsSHA256': 'CostCenterDetailsSHA256'
            }
        )

        query_parent_code_fullpath = rail.QueryCollectionOperator(
            task_id = "query_parent_code_fullpath",
            query = """SELECT
                        t1.*,
                        t2.Code AS ParentCode,
                        t2.Name AS ParentFullPath
                    FROM raw_cost_centers_with_hash t1
                    LEFT JOIN raw_cost_centers_with_hash t2
                        ON t2.Name != t1.Name
                        AND t1.Name LIKE t2.Name || '|%'
                        AND NOT EXISTS (
                            SELECT 1
                            FROM raw_cost_centers_with_hash t3
                            WHERE t3.Name != t1.Name
                            AND t1.Name LIKE t3.Name || '|%'
                            AND LENGTH(t3.Name) > LENGTH(t2.Name)
                        )
                    ORDER BY LENGTH(t1.Name), t1.Name;
                    """,
            name="cost_centers_with_hash",
        )

        # Check if there are any records
        has_any_records = rail.IfOperator(
            task_id='has_any_records',
            test="{{ result('query_parent_code_fullpath', 'length') > 0 }}",
            yes_task='create_records_log',
            no_task='send_empty_file_email'
        )

        # Create shared log for all records
        create_records_log = rail.CreateLogOperator(
            task_id='create_records_log'
        )

        # Validate mandatory fields
        find_valid_records = rail.QueryCollectionOperator(
            task_id='find_valid_records',
            query="""SELECT * FROM cost_centers_with_hash
                        WHERE NULLIF("Name", '') IS NOT NULL
                        AND NULLIF("Code", '') IS NOT NULL
                        AND NULLIF("Description", '') IS NOT NULL
                        AND NULLIF("Status", '') IS NOT NULL
                        AND (LENGTH(Name) - LENGTH(REPLACE(Name, '|', '')) + 1) < 8""",
            name="valid_cost_centers"
        )

        # Find invalid records (missing mandatory fields)
        find_invalid_records = rail.QueryCollectionOperator(
            task_id='find_invalid_records',
            query="""SELECT * FROM cost_centers_with_hash
                        WHERE NULLIF("Name", '') IS NULL
                        OR NULLIF("Code", '') IS NULL
                        OR NULLIF("Description", '') IS NULL
                        OR NULLIF("Status", '') IS NULL
                        OR (LENGTH(Name) - LENGTH(REPLACE(Name, '|', '')) + 1) > 7""",
            name="invalid_cost_centers"
        )

        # Log invalid records
        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            log="{{ result('create_records_log') }}",
            severity="Exception",
            items="{{ result('find_invalid_records') }}",
            message="Invalid cost center record - missing mandatory field(s)",
            properties=lambda item: {
                'code': item.get('Code', 'MISSING'),
                'name': item.get('Name', 'MISSING'),
                'description': item.get('Description', 'MISSING'),
                'status': "Exception",
                'action': "Validation",
                'details': custom_methods.get_invalid_log_message(item),
                'manager_id': item.get('Cost_Center_Manager', '')
            }
        )

        # Check if we have valid records
        has_valid_records = rail.IfOperator(
            task_id='has_valid_records',
            test="{{ result('find_valid_records', 'length') > 0 }}",
            yes_task='download_reference_file',
            no_task='generate_logs'
        )

        # Download reference file
        download_reference_file = rail.SFTPDownloadFileOperator(
            task_id='download_reference_file',
            remote_filepath=f"{config.reference_filepath}/reference_cost_centers.csv",
            sftp_conn_id=config.reference_sftp_conn_id
        )

        # Load reference file
        load_reference_file = rail.LoadCSVFileOperator(
            task_id='load_reference_file',
            document="{{ result('download_reference_file') }}",
            delimiter=";",
            encoding="utf-8-sig",
        )

        reference_file = rail.CreateCollectionOperator(
            task_id = "reference_file",
            source = "{{ result('load_reference_file') }}",
            name = "load_reference_file"
        )

        # Find unchanged records (SHA256 in input matches SHA256 in reference)
        # check if the code hash is changed or not
        find_unchanged_records = rail.QueryCollectionOperator(
            task_id='find_unchanged_records',
            query="""SELECT cc.* FROM valid_cost_centers cc
                    JOIN load_reference_file ref ON cc.Code = ref.Code
                    WHERE cc.SHA256 = ref.SHA256
            """,
            name="unchanged_cost_centers"
        )

        # Log unchanged records
        log_unchanged_records = rail.WriteLogOperator(
            task_id='log_unchanged_records',
            log="{{ result('create_records_log') }}",
            severity="Info",
            items="{{ result('find_unchanged_records') }}",
            message="Unchanged cost center record - skipping",
            properties=lambda item: {
                'code': item.get('Code', ''),
                'name': item.get('Name', ''),
                'description': item.get('Description', ''),
                'status': "Exception",
                'action': "Validation",
                'details': "No change in record",
                'manager_id': item.get('Cost_Center_Manager', '')
            }
        )

        # Find changed records - SHA256 in input is different from reference or not in reference
        detect_changes = rail.QueryCollectionOperator(
            task_id='detect_changes',
            query="""SELECT cc.*
            FROM valid_cost_centers cc
            WHERE NOT EXISTS (
                SELECT 1
                FROM load_reference_file ref
                WHERE cc.Code = ref.Code AND cc.SHA256 = ref.SHA256
            )
            """,
            name="changed_cost_centers"
        )

        # Get all departments from Replicon using paged requests
        get_all_departments = rail.RepliconServicePageOperator(
            task_id='get_all_departments',
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data=request_payload.get_departments_payload(),
            page_handler=lambda request, response: {
                'page': request['page'] + 1
            } if response.get('rows', []) and len(response['rows']) >= request['pagesize'] else None,
            all_result_data_handler=response_filter.combine_and_map_departments
        )

        # Create collection from Replicon departments data
        create_departments_collection = rail.CreateCollectionOperator(
            task_id='create_departments_collection',
            source=lambda: rail.result('get_all_departments'),
            name="replicon_departments"
        )

        # Check if there are records to process
        has_changes_to_process = rail.IfOperator(
            task_id='has_changes_to_process',
            test="{{ result('detect_changes', 'length') > 0 }}",
            yes_task=['identify_records_to_add', 'identify_records_to_update', 'identify_managers_to_update', 'query_managers_for_permission_removal'],
            no_task='generate_logs'
        )

        # Identify records to add (not in Replicon)
        identify_records_to_add = rail.QueryCollectionOperator(
            task_id='identify_records_to_add',
            query="""SELECT
                    cc.*,
                    (LENGTH(cc.Name) - LENGTH(REPLACE(cc.Name, '|', '')) + 1) AS hierarchy_level
                FROM changed_cost_centers cc
                LEFT JOIN replicon_departments dept ON cc.Code = dept.Code
                WHERE dept.Code IS NULL
            """,
            name="records_to_add"
        )

        # Identify records to update (exist in Replicon)
        identify_records_to_update = rail.QueryCollectionOperator(
            task_id='identify_records_to_update',
            query="""SELECT DISTINCT
                    c.*,
                    (LENGTH(c.Name) - LENGTH(REPLACE(c.Name, '|', '')) + 1) AS hierarchy_level
                FROM changed_cost_centers c
                INNER JOIN replicon_departments r
                    ON LOWER(c.Code) = LOWER(r.Code)
                LEFT JOIN load_reference_file lrf
                    ON LOWER(c.Code) = LOWER(lrf.Code)
                WHERE 
                    lrf.Code IS NULL
                    OR
                    c.CostCenterDetailsSHA256 != lrf.CostCenterDetailsSHA256
            """,
            name="records_to_update"
        )

        extract_hierarchy_levels_add_update = rail.PythonOperator(
            task_id = "extract_hierarchy_levels_add_update",
            # Taking only the hierarchy_levels which needs to be added or updated, rather than triggering the whole 7 levels
            python_callable=lambda: {
                **{
                    'hierarchy_levels_add': sorted(set([r.get('hierarchy_level', 1) for r in rail.load_all_records(rail.result('identify_records_to_add'))]))
                },
                **{
                    'hierarchy_levels_update': sorted(set([r.get('hierarchy_level', 1) for r in rail.load_all_records(rail.result('identify_records_to_update'))]))
                }
            }
        )

        # Identify unique managers
        identify_managers_to_update = rail.QueryCollectionOperator(
            task_id='identify_managers_to_update',
            query="""SELECT DISTINCT manager_id
                        FROM (
                            -- Managers who lost cost centers (in reference but not in input)
                            SELECT ref.Cost_Center_Manager as manager_id
                            FROM load_reference_file ref
                            LEFT JOIN raw_cost_centers_with_hash r 
                                ON ref.Code = r.Code 
                                AND ref.Cost_Center_Manager = r.Cost_Center_Manager
                            WHERE 
                                NULLIF(ref.Cost_Center_Manager, '') IS NOT NULL
                                AND r.Cost_Center_Manager IS NULL
                            
                            UNION ALL
                            
                            -- Managers who gained cost centers (in input but not in reference)
                            SELECT r.Cost_Center_Manager as manager_id
                            FROM raw_cost_centers_with_hash r
                            LEFT JOIN load_reference_file ref 
                                ON r.Code = ref.Code 
                                AND r.Cost_Center_Manager = ref.Cost_Center_Manager
                            WHERE 
                                NULLIF(r.Cost_Center_Manager, '') IS NOT NULL
                                AND ref.Cost_Center_Manager IS NULL
                        ) changes
            """,
            name="managers"
        )

        # Check if there are new cost centers to add
        has_new_cost_centers = rail.IfOperator(
            task_id='has_new_cost_centers',
            test="{{ result('identify_records_to_add', 'length') > 0 }}",
            yes_task='trigger_add_dag',
            no_task='check_update_cost_centers'
        )

        # Trigger intermediate DAG for adding new cost centers for each hierarchy level
        trigger_add_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_add_dag',
            items=lambda: rail.result('extract_hierarchy_levels_add_update')['hierarchy_levels_add'],
            trigger_dag_id=config.intermediate_dag_id,
            execution_timeout=timedelta(hours=config.child_dag_timeout_hours),
            conf=lambda item: {
                'add_update_cost_centers_collection_name': rail.result('identify_records_to_add', 'table'),
                'hierarchy_level': str(item),
                'operation_type': 'add',
                'replicon_departments': custom_methods.get_updated_departments(get_all_departments.task_id),
                'file_name': rail.result('new_file_sensor'),
                'master_log': rail.result('create_records_log')
            }
        )

        # Wait for all add DAG runs to complete
        wait_for_add_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_add_completion',
            dag_runs="{{ result('trigger_add_dag') }}",
            execution_timeout=timedelta(hours=config.child_dag_timeout_hours)
        )

        # Check if there are cost centers to update
        check_update_cost_centers = rail.IfOperator(
            task_id='check_update_cost_centers',
            test="{{ result('identify_records_to_update', 'length') > 0 }}",
            yes_task='get_all_departments_after_add',
            no_task='check_manager_updates'
        )

        get_all_departments_after_add = rail.RepliconServicePageOperator(
            task_id='get_all_departments_after_add',
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data=request_payload.get_departments_payload(),
            page_handler=lambda request, response: {
                'page': request['page'] + 1
            } if response.get('rows', []) and len(response['rows']) >= request['pagesize'] else None,
            all_result_data_handler=response_filter.combine_and_map_departments
        )

        # Trigger intermediate DAG for updating existing cost centers by hierarchy level
        trigger_update_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_update_dag',
            items=lambda: rail.result('extract_hierarchy_levels_add_update')['hierarchy_levels_update'],
            trigger_dag_id=config.intermediate_dag_id,
            execution_timeout=timedelta(hours=config.child_dag_timeout_hours),
            conf=lambda item: {
                'add_update_cost_centers_collection_name': rail.result('identify_records_to_update', 'table'),
                'hierarchy_level': str(item),
                'operation_type': 'update',
                'replicon_departments': custom_methods.get_updated_departments(get_all_departments_after_add.task_id),
                'file_name': rail.result('new_file_sensor'),
                'master_log': rail.result('create_records_log')
            }
        )

        # Wait for all update DAG runs to complete
        wait_for_update_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_update_completion',
            dag_runs="{{ result('trigger_update_dag') }}",
            execution_timeout=timedelta(hours=config.child_dag_timeout_hours)
        )

        # Check for managers that need permission updates
        check_manager_updates = rail.IfOperator(
            task_id='check_manager_updates',
            test=lambda : ((rail.result('identify_managers_to_update', 'length') > 0) or( rail.result('query_managers_for_permission_removal', 'length') > 0)),
            yes_task='get_all_departments_updated',
            no_task='gather_runids'
        )

        get_all_departments_updated = rail.RepliconServicePageOperator(
            task_id='get_all_departments_updated',
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data=request_payload.get_departments_payload(),
            page_handler=lambda request, response: {
                'page': request['page'] + 1
            } if response.get('rows', []) and len(response['rows']) >= request['pagesize'] else None,
            all_result_data_handler=response_filter.combine_and_map_departments
        )

        get_cost_manager_permission = rail.RepliconServiceOperator(
            task_id='get_cost_manager_permission',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data=request_payload.get_all_permission_sets_payload,
            data_handler=lambda response: {
                'cost_manager': rail.find_first_by_attr_and_get_attr(response,
                                                    'name',
                                                    'Cost Manager (View)',
                                                    default={}),
                "payroll_manager": rail.find_first_by_attr_and_get_attr(response,
                                                    'name',
                                                    'Payroll Manager (View)',
                                                    default={}),
            }
        )

        # Trigger manager permission assignment DAG
        trigger_manager_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_manager_dag',
            items=lambda: rail.result('identify_managers_to_update'),
            trigger_dag_id=config.manager_cost_center_restriction_update_dag_id,
            execution_timeout=timedelta(hours=config.child_dag_timeout_hours),
            conf=lambda item: {
                'managers': item,
                'manager_permission': rail.result('get_cost_manager_permission')['cost_manager'],
                'payroll_manager_permission': rail.result('get_cost_manager_permission')['payroll_manager'],
                'replicon_departments': custom_methods.get_updated_departments(get_all_departments_updated.task_id),
                'file_name': rail.result('new_file_sensor'),
                'master_log': rail.result('create_records_log')
            }
        )

        query_managers_for_permission_removal = rail.QueryCollectionOperator(
            task_id = "query_managers_for_permission_removal",
            query = """SELECT DISTINCT Cost_Center_Manager FROM load_reference_file WHERE NULLIF("Cost_Center_Manager", '') IS NOT NULL AND Cost_Center_Manager NOT IN (SELECT DISTINCT "Cost_Center_Manager" FROM raw_cost_centers_with_hash)""",
        )

        trigger_manager_dag_for_permission_removal = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_manager_dag_for_permission_removal',
            items=lambda: rail.result('query_managers_for_permission_removal'),
            trigger_dag_id=config.manager_cost_center_restriction_update_dag_id,
            execution_timeout=timedelta(hours=config.child_dag_timeout_hours),
            conf=lambda item: {
                'managers': {
                    "manager_id": item['Cost_Center_Manager'],
                },
                "should_permission_removed": "True",
                'manager_permission': rail.result('get_cost_manager_permission')['cost_manager'],
                'payroll_manager_permission': rail.result('get_cost_manager_permission')['payroll_manager'],
                'replicon_departments': custom_methods.get_updated_departments(get_all_departments_updated.task_id),
                'file_name': rail.result('new_file_sensor'),
                'master_log': rail.result('create_records_log')
            }
        )


        # Wait for manager DAG run to complete
        wait_for_manager_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_manager_completion',
            dag_runs=[
                '{{ result("trigger_manager_dag") }}',
                '{{ result("trigger_manager_dag_for_permission_removal") }}'
            ],
            execution_timeout=timedelta(hours=config.child_dag_timeout_hours)
        )

        def gather_runids_callable():
            run_ids = []
            if rail.result('trigger_add_dag'):
                run_ids.extend(rail.result('trigger_add_dag'))
            if rail.result('trigger_update_dag'):
                run_ids.extend(rail.result('trigger_update_dag'))

            return run_ids

        gather_runids = rail.PythonOperator(
            task_id = "gather_runids",
            python_callable=gather_runids_callable
        )

        gather_child_run_ids = rail.GatherResultsFromDagRunsOperator(
            task_id = "gather_child_run_ids",
            dag_runs= "{{result('gather_runids')}}",
            dagrun_task_id="gather_run_ids",
            flatten=True
        )

        combine_cost_center_manager_runids = rail.PythonOperator(
            task_id = "combine_cost_center_manager_runids",
            python_callable=lambda: (
                (rail.result('gather_child_run_ids') if rail.result('gather_child_run_ids') else []) +
                ( rail.result('trigger_manager_dag') if rail.result('trigger_manager_dag') else []) +
                ( rail.result('trigger_manager_dag_for_permission_removal') if rail.result('trigger_manager_dag_for_permission_removal') else [] )
            )
        )

        gather_logs = rail.GatherResultsFromDagRunsOperator(
            task_id = "gather_logs",
            dag_runs= "{{ result('combine_cost_center_manager_runids') }}",
            dagrun_task_id="create_process_log",
            flatten=True
        )

        def get_generate_logs_conf():
            return{
                'logs': rail.result('gather_logs'),
                'exception_logs': rail.result('create_records_log'),
                'total_record_count': rail.result('create_cost_center_collection', 'length'),
                'records_to_add': rail.result('identify_records_to_add', 'length'),
                'records_to_update':rail.result('identify_records_to_update', 'length'),
                'managers': rail.result('identify_managers_to_update', 'length'),
                'process_file_name': rail.render_template("{{ result('new_file_sensor') | file_name }}"),
                'log_file_path': config.log_filepath,
                'job_started_time': rail.result("log_start_time")
            }

        # Trigger log generation DAG
        generate_logs = rail.TriggerDagRunOperator(
            task_id='generate_logs',
            trigger_dag_id=config.log_generation_dag_id,
            execution_timeout=timedelta(hours=1),
            conf=get_generate_logs_conf
        )

        # Archive old reference file
        archive_reference_file = rail.SFTPMoveFileOperator(
            task_id='archive_reference_file',
            sftp_conn_id=config.reference_sftp_conn_id,
            existing_filename=f"{config.reference_filepath}/reference_cost_centers.csv",
            new_filename=f"""{config.reference_archive_filepath}/reference_cost_centers_{{{{ current_time_in_specified_tz(fmt="%Y_%m_%dT%H_%M_%S", tz="Etc/UTC") }}}}.csv""",
        )

        # Create new reference file
        write_new_reference = rail.WriteCSVFileOperator(
            task_id='write_new_reference',
            source="{{ result('create_cost_center_hash') }}",
            delimiter=";",
            row= lambda item: [
                item["Name"],
                item["Code"],
                item["Description"],
                item["Status"],
                item["Cost Center Manager"],
                item["SHA256"],
                item["CostCenterDetailsSHA256"]
            ],
            encoding="utf-8-sig"
        )

        # Upload new reference file
        upload_new_reference = rail.SFTPUploadFileOperator(
            task_id='upload_new_reference',
            content="{{ result('write_new_reference') }}",
            remote_filepath=f"{config.reference_filepath}/reference_cost_centers.csv",
            sftp_conn_id=config.reference_sftp_conn_id
        )

        # Define task dependencies

        # Initial file handling
        log_start_time >> new_file_sensor >> is_csv

        # Handle CSV check results
        is_csv >> rail.Label("Yes") >> download_file
        is_csv >> rail.Label("No") >> send_invalid_format_email >> archive_file_bad_format

        # File download and content check
        download_file >> archive_file >> has_file_content

        download_file >> was_new_file_found >> rail.Label("no") >> delete_this_dagrun

        # Handle file content results
        has_file_content >> rail.Label("Yes") >> load_cost_center_data
        has_file_content >> rail.Label("No") >> send_empty_file_email

        # Process CSV data
        load_cost_center_data >> create_cost_center_hash >> create_cost_center_collection >> query_parent_code_fullpath >> has_any_records

        # Handle records check
        has_any_records >> rail.Label("Yes") >> create_records_log >> [find_valid_records, find_invalid_records]
        has_any_records >> rail.Label("No") >> send_empty_file_email

        # Log invalid records
        find_invalid_records >> log_invalid_records

        # Check for valid records
        find_valid_records >> has_valid_records

        # Handle valid records check
        has_valid_records >> rail.Label("Yes") >> download_reference_file
        has_valid_records >> rail.Label("No") >> generate_logs

        # Compare with reference file
        download_reference_file >> load_reference_file >> reference_file >> [detect_changes, find_unchanged_records]
        find_unchanged_records >> log_unchanged_records >> generate_logs
        detect_changes >> get_all_departments >> create_departments_collection >> has_changes_to_process

        # Handle changes processing
        has_changes_to_process >> rail.Label("Yes") >> [identify_records_to_add, identify_records_to_update, identify_managers_to_update, query_managers_for_permission_removal] >> extract_hierarchy_levels_add_update
        extract_hierarchy_levels_add_update >> has_new_cost_centers
        has_changes_to_process >> rail.Label("No") >> generate_logs

        # Handle add/update branches with proper waiting between steps
        has_new_cost_centers >> rail.Label("Yes") >> trigger_add_dag >> wait_for_add_completion >> check_update_cost_centers
        has_new_cost_centers >> rail.Label("No") >> check_update_cost_centers

        check_update_cost_centers >> rail.Label("Yes") >> get_all_departments_after_add >> trigger_update_dag >> wait_for_update_completion >> check_manager_updates
        check_update_cost_centers >> rail.Label("No") >> check_manager_updates

        check_manager_updates >> rail.Label("Yes") >> get_all_departments_updated >> get_cost_manager_permission >> trigger_manager_dag >> trigger_manager_dag_for_permission_removal >> wait_for_manager_completion >> gather_runids
        gather_runids >> gather_child_run_ids >> combine_cost_center_manager_runids >> gather_logs >> generate_logs
        check_manager_updates >> rail.Label("No") >> gather_runids

        # Final steps
        generate_logs >> archive_reference_file >> write_new_reference >> upload_new_reference

        return dag


# Create DAGs for each instance
rail.for_each_instance(create_cost_center_hierarchy_import_dag)
