from datetime import timedelta
from pendulum import datetime as dt
import rail
from siemens.project_import.utils import custom_methods
from siemens.project_import.tasks.process_log_generation import process_log_task_group
null = None


def create_dag(config):
    """
    Master DAG for Siemens Project Import v1
    Handles file detection, processing, and triggers child DAGs for add/update operations
    """
    with rail.create_airflow_dag(
        dag_id=config.master_dagid,
        description=f"Siemens Portugal Project Import v1 Master {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(minutes=5),
        start_date=dt(2023, 1, 1, tz=config.time_zone),
        max_active_runs=config.max_active_runs_master,
        default_args={
            "sftp_conn_id": config.sftp_conn_id,
        },
    ) as dag:

        # ============================================================================
        # PHASE 1: REAL-TIME FILE DETECTION (REQ-001: 30-second response)
        # ============================================================================

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id="new_file_sensor",
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout),
        )

        # Check if new file was found
        was_new_file_found = rail.IfOperator(
            task_id="was_new_file_found",
            trigger_rule="all_done",
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task="archive_file",
            no_task="delete_this_dagrun",
        )

        # Delete DAG run if no file found
        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id="delete_this_dagrun"
        )

        # ============================================================================
        # PHASE 2: FILE PROCESSING & ARCHIVING
        # ============================================================================

        # Download the detected CSV file
        download_file = rail.SFTPDownloadFileOperator(
            task_id="download_file", remote_filepath="{{result('new_file_sensor')}}"
        )

        # Archive file immediately after download (parallel task)
        archive_file = rail.SFTPMoveFileOperator(
            task_id="archive_file",
            new_filename=config.input_archive_filepath
            + "{{ecid() | replace(':', '-')}}_{{result('new_file_sensor') | file_name}}",
            existing_filename="{{result('new_file_sensor')}}",
        )

        # ============================================================================
        # PHASE 3: CSV PARSING & DATA PREPARATION
        # ============================================================================

        # Parse CSV data
        parse_csv_data = rail.LoadCSVFileOperator(
            task_id="parse_csv_data",
            document="{{ result('download_file') }}",
            encoding="utf-8-sig"
        )

        create_input_collection = rail.CreateCollectionOperator(
            task_id="create_input_collection",
            source="{{ result('parse_csv_data') }}",
            columns=config.reference_file_headers
        )

        # Generate MD5 fingerprints using DataAdaptorOperator
        create_encoded_data = rail.DataAdaptorOperator(
            task_id="create_encoded_data",
            source="{{ result('create_input_collection') }}",
            columns=[
                "type",
                "categorization",
                "projectcode",
                "projectmanager",
                "name",
                "client",
                "startdate",
                "enddate",
                "projectvalue",
                "estimatedengineeringhours",
                "estimatedpmhours",
                "estimatedengineeringcost",
                "estimatedpmcost",
                "underwarranty",
                "deliverydate",
                "encoded",
            ],
            data=custom_methods.add_encoding,
        )

        # Create project collection with all required fields for reference file
        create_project_collection = rail.CreateCollectionOperator(
            task_id="create_project_collection",
            source="{{ result('create_encoded_data') }}",
            name="siemens_projects",
        )

        # ============================================================================
        # PHASE 4: FINGERPRINTING & CHANGE DETECTION (REQ-002)
        # ============================================================================

        list_reference_file = rail.SFTPListFilesOperator(
            task_id="list_reference_file", paths=[config.reference_filepath]
        )

        get_reference_filename = rail.PythonOperator(
            task_id="get_reference_filename",
            python_callable=lambda: (
                rail.result("list_reference_file")[config.reference_filepath][0]["name"]
                if rail.result("list_reference_file")
                else None
            ),
        )

        # Download existing reference file (if exists)
        download_reference_file = rail.SFTPDownloadFileOperator(
            task_id="download_reference_file",
            remote_filepath=config.reference_filepath
            + "{{result('get_reference_filename')}}",
        )

        # Load reference data as collection
        load_reference_csv = rail.LoadCSVFileOperator(
            task_id="load_reference_csv",
            document="{{ result('download_reference_file') }}",
        )

        # Create reference collection
        create_reference_collection = rail.CreateCollectionOperator(
            task_id="create_reference_collection",
            source="{{ result('load_reference_csv') }}",
            name="reference_data",
        )

        # Find new projects using SQL query
        find_new_projects = rail.QueryCollectionOperator(
            task_id="find_new_projects",
            query="""SELECT * FROM siemens_projects
                     WHERE encoded NOT IN (SELECT DISTINCT encoded FROM reference_data)""",
        )

        query_unchanged_projects = rail.QueryCollectionOperator(
             task_id="query_unchanged_projects",
             query = """SELECT * FROM siemens_projects
                     WHERE encoded IN (SELECT DISTINCT encoded FROM reference_data)"""
        )

        create_master_log = rail.CreateLogOperator(task_id="create_master_log")

        write_unchanged_project_log = rail.WriteLogOperator(
            task_id="write_unchanged_project_log",
            log='{{ result("create_master_log") }}',
            items='{{ result("query_unchanged_projects")}}',
            message="No Change in Record",
            properties=lambda item: {
                "projectname": item.get("name"),
                "projectcode": item.get("projectcode"),
                "status": "Skipped",
                "details": "No Change in Record",
            },
        )

        if_new_project_data = rail.IfOperator(
            task_id="if_new_project_data",
            test="{{result('find_new_projects', 'length') > 0}}",
            yes_task="get_default_task_list",
            no_task="log_generation",
        )

        # ============================================================================
        # PHASE 2: MASTER TASK LIST PREPARATION
        # ============================================================================

        # Get Default Task List project for new project task management
        get_default_task_list = rail.RepliconServiceOperator(
            task_id="get_default_task_list",
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data=lambda: {
                "projects": [
                    {
                        "uri": null,
                        "name": "Default Task List",
                        "code": null,
                        "parameterCorrelationId": null,
                    }
                ]
            },
            data_handler=lambda response: (
                response[0]["projectDetails"]["uri"] if response else null
            ),
        )

        # Get all tasks from Default Task List
        get_master_tasks = rail.RepliconServiceOperator(
            task_id="get_master_tasks",
            endpoint="/services/TaskService1.svc/GetDescendantTaskDetails",
            data={"parentUri": '{{result("get_default_task_list")}}'},
            data_handler=lambda response: list(
                map(lambda i: i["task"]["name"], response)
            ),
        )

        get_project_custom_fields_details = rail.RepliconServiceOperator(
            task_id="get_project_custom_fields_details",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={"objectUri": "urn:replicon:object-type:project"},
            data_handler=lambda response: {
                i["displayText"].lower().replace(" ", "") + "uri": i["uri"]
                for i in response
            },
        )

        # ============================================================================
        # PHASE 5: PARALLEL PROJECT PROCESSING
        # ============================================================================

        # Trigger parallel processing for all new projects
        trigger_parallel_project_processing = rail.trigger_parallel_dagrun(
            task_id="trigger_parallel_project_processing",
            trigger_dag_id=config.process_project_dagid,
            items="{{result('find_new_projects')}}",
            conf=lambda item: {
                **item,  # This spreads all project data from the CSV
                "task_list": rail.result("get_master_tasks"),
                **rail.result("get_project_custom_fields_details"),
            },
            execution_timeout=timedelta(days=14),
            parallel_count=config.max_active_runs_child,
        )

        # ============================================================================
        # PHASE 6: REFERENCE FILE GENERATION
        # ============================================================================

        archive_reference_file = rail.SFTPMoveFileOperator(
            task_id="archive_reference_file",
            new_filename=config.reference_archive_filepath
            + "{{ecid() | replace(':', '-')}}_{{result('get_reference_filename')}}",
            existing_filename=config.reference_filepath
            + "{{result('get_reference_filename')}}",
        )

        write_reference_file_csv = rail.WriteCSVFileOperator(
            task_id="write_reference_file_csv",
            source="{{ result('create_encoded_data') }}",
            header=[
                "type",
                "categorization",
                "projectcode",
                "projectmanager",
                "name",
                "client",
                "startdate",
                "enddate",
                "projectvalue",
                "estimatedengineeringhours",
                "estimatedpmhours",
                "estimatedengineeringcost",
                "estimatedpmcost",
                "underwarranty",
                "deliverydate",
                "encoded",
            ],
            row=[
                "{{item.type}}",
                "{{item.categorization}}",
                "{{item.projectcode}}",
                "{{item.projectmanager}}",
                "{{item.name}}",
                "{{item.client}}",
                "{{item.startdate}}",
                "{{item.enddate}}",
                "{{item.projectvalue}}",
                "{{item.estimatedengineeringhours}}",
                "{{item.estimatedpmhours}}",
                "{{item.estimatedengineeringcost}}",
                "{{item.estimatedpmcost}}",
                "{{item.underwarranty}}",
                "{{item.deliverydate}}",
                "{{item.encoded}}",
            ]
        )

        upload_reference_file = rail.SFTPUploadFileOperator(
            task_id="upload_reference_file",
            content="{{ result('write_reference_file_csv') }}",
            remote_filepath=config.reference_filepath
            + "{{ds_nodash}}_Siemens_Reference.csv",
        )

        # ============================================================================
        # PHASE 7: LOGGING & COMPLETION
        # ============================================================================

        # Generate processing summary log
        log_generation = rail.EmptyOperator(task_id="log_generation")

        generate_processing_log = process_log_task_group(config)

        # Final Sumo logging
        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id="log_to_sumo",
            sumo_conn_id="sumologic-dagrunlogger",
            trigger_rule="all_done",
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{get_error_message() | is_truthy}}',
            yes_task="fail_dagrun"
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{get_error_message()}}'
        )

        # ============================================================================
        # TASK DEPENDENCIES
        # ============================================================================

        # File processing flow
        new_file_sensor >> download_file
        download_file >> was_new_file_found >> rail.Label("No") >> delete_this_dagrun
        was_new_file_found >> rail.Label("Yes") >> archive_file

        # Data processing flow
        (
            download_file
            >> parse_csv_data
            >> create_input_collection
            >> create_encoded_data
            >> create_project_collection
        )

        # Reference file and change detection flow
        (
            create_project_collection
            >> list_reference_file
            >> get_reference_filename
            >> download_reference_file
            >> load_reference_csv
            >> create_reference_collection
            >> find_new_projects
            >> query_unchanged_projects
            >> create_master_log
            >> write_unchanged_project_log
            >> if_new_project_data
            >> rail.Label("Yes")
            >> get_default_task_list
            >> get_master_tasks
            >> get_project_custom_fields_details
            >> trigger_parallel_project_processing
            >> log_generation
            >> generate_processing_log
            >> archive_reference_file >> write_reference_file_csv
            >> upload_reference_file >> log_to_sumo >> can_fail_dag >> fail_dagrun
        )
        # No new projects flow
        if_new_project_data >> rail.Label("No") >> log_generation

        return dag


rail.for_each_instance(create_dag)
