from datetime import timedelta
from sasglobal.oef_import.oef_geo_import.tasks.send_logs import get_send_logs
from sasglobal.oef_import.oef_geo_import.utils.response_filters import filter_object_extension_tags, get_filtered_input_data
import rail

null = None

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'sasglobal_oef_geo_import_master_{config.instance}',
        description=f'SaSGlobal OEF GEO Import Master DAG {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval = timedelta(seconds=config.schedule_interval),
        max_active_runs=config.max_active_runs,
        default_args={
            "sftp_conn_id": config.sftp_conn_id
        }
    ) as dag:

        def geo_oef_uri():
            return "urn:replicon-tenant:" + rail.get_tenant_slug() + ":object-extension-tag-definition:14f46628-83b4-458a-93e3-a8d6ad86d38f"

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout),
        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test="{{ result('new_file_sensor') | file_ext | lower == 'csv' }}",
            yes_task='download_file',
            no_task='archive_invalid_file'
        )

        archive_invalid_file = rail.SFTPMoveFileOperator(
            task_id='archive_invalid_file',
            new_filename=config.archive_filepath + '/Skipped_Geo_{{ current_time("%Y%m%d%H%M%S") }}_{{ result("new_file_sensor") | file_name }}',
            existing_filename=config.input_filepath+'/{{ result("new_file_sensor") | file_name }}',
        )

        log_invalid_file = rail.WriteLogOperator(
            task_id='log_invalid_file',
            message="Skipped processing the file {{ result('new_file_sensor') }} due to incorrect file format",
            severity='Skipped',
            properties={
                "name": null,
                "value": null,
                "status": null,
                "processing_status": "Skipped",
                "details": "Skipped processing the file {{ result('new_file_sensor') }} due to incorrect file format"
            }
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id = 'download_file',
            remote_filepath = "{{ result('new_file_sensor') }}",
        )

        process_on_error = rail.IfOperator(
            task_id = 'process_on_error',
            trigger_rule = 'one_failed',
            test = '{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task = 'archive_file_on_error'
        )

        archive_file_on_error = rail.SFTPMoveFileOperator(
            task_id='archive_file_on_error',
            new_filename=config.archive_filepath + '/Skipped_Geo_{{ current_time("%Y%m%d%H%M%S") }}_{{ result("new_file_sensor") | file_name }}',
            existing_filename=config.input_filepath+'/{{ result("new_file_sensor") | file_name }}',
        )

        was_new_file_found = rail.IfOperator(
            task_id = 'was_new_file_found',
            trigger_rule = 'all_done',
            test = '{{ get_task_state("new_file_sensor") == "success" }}',
            no_task = 'delete_this_dagrun'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            new_filename=config.archive_filepath + '/Geo_{{ current_time("%Y%m%d%H%M%S") }}_{{ result("new_file_sensor") | file_name }}',
            existing_filename=config.input_filepath+'/{{ result("new_file_sensor") | file_name }}',
        )

        load_data = rail.LoadCSVFileOperator(
            task_id='load_data',
            document="{{ result('download_file') }}"
        )

        filter_input_data = rail.WriteCSVFileOperator(
            task_id='filter_input_data',
            source='{{ result("load_data") }}',
            header=["name", "value", "status"],
            row=get_filtered_input_data
        )

        create_input_collection = rail.CreateCollectionOperator(
            task_id='create_input_collection',
            source='{{ result("filter_input_data") }}',
            name='inputdata'
        )

        has_input_data = rail.IfOperator(
            task_id='has_input_data',
            test='{{ result("create_input_collection", "length") > 0 }}',
            yes_task='get_object_extension_tags',
            no_task='log_empty_file'
        )

        log_empty_file = rail.WriteLogOperator(
            task_id='log_empty_file',
            message="Blank input file received. Skipped processing the file {{ result('new_file_sensor') }}.",
            severity='Skipped',
            properties={
                "name": null,
                "value": null,
                "status": null,
                "processing_status": "Skipped",
                "details": "Blank input file received. Skipped processing the file {{ result('new_file_sensor') }}."
            }
        )

        get_object_extension_tags = rail.RepliconServiceOperator(
            task_id='get_object_extension_tags',
            endpoint='/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails',
            data=lambda: {
                "objectExtensionTagDefinitionUri": geo_oef_uri()
            },
            response_filter=filter_object_extension_tags
        )

        available_tags_list = rail.CreateCollectionOperator(
            task_id='available_tags_list',
            source='{{ result("get_object_extension_tags") | to_json }}',
            name='availabletagslist'
        )

        query_not_geo_input = rail.QueryCollectionOperator(
            task_id='query_not_geo_input',
            query="SELECT * FROM inputdata WHERE name != 'GEO'"
        )

        log_input_name_not_geo = rail.WriteLogOperator(
            task_id='log_input_name_not_geo',
            items='{{ result("query_not_geo_input") }}',
            message="Name not set to GEO",
            severity='Skipped',
            properties={
                "name": "{{ item.name }}",
                "value": "{{ item.value }}",
                "status": "{{ item.status }}",
                "processing_status": "Skipped",
                "details": "Name not set to GEO"
            }
        )

        query_geo_input = rail.QueryCollectionOperator(
            task_id='query_geo_input',
            query="SELECT * FROM inputdata WHERE name = 'GEO'"
        )

        load_available_tags = rail.PythonOperator(
            task_id='load_available_tags',
            python_callable=lambda: rail.load_all_records(rail.result('available_tags_list'))
        )

        process_valid_objects = rail.TriggerDagRunForEachItemOperator(
            task_id='process_valid_objects',
            items='{{ result("query_geo_input") }}',
            trigger_dag_id=f'sasglobal_oef_geo_import_process_valid_oef_child_{config.instance}',
            conf=lambda item: {
                "geo_oef_uri": geo_oef_uri(),
                "object_data": item,
                "available_tags": rail.result("load_available_tags"),
                "tags_list_artifact": rail.result("create_tags_list")
            }
        )

        wait_for_process_valid_objects = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_valid_objects',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_valid_objects") }}'
        )

        send_logs_entry, send_logs_end = get_send_logs(config)

        new_file_sensor >> is_csv >> rail.Label("Yes") >> download_file >> was_new_file_found
        is_csv >> rail.Label("No") >> archive_invalid_file >> log_invalid_file >> send_logs_entry
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun

        download_file >> rail.Label("On Error") >> process_on_error >> rail.Label("Yes") >> archive_file_on_error

        download_file >> archive_file >> load_data >> filter_input_data >> create_input_collection >> has_input_data
        has_input_data >> rail.Label("No") >> log_empty_file >> send_logs_entry
        has_input_data >> rail.Label("Yes") >> get_object_extension_tags
        get_object_extension_tags >> available_tags_list >> query_not_geo_input >> log_input_name_not_geo \
            >> query_geo_input >> load_available_tags >> process_valid_objects >> wait_for_process_valid_objects >> send_logs_entry
        send_logs_end

    return dag

rail.for_each_instance(create_main_dag)
