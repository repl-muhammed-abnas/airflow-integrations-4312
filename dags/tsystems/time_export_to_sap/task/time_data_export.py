import json
import rail
from tsystems.time_export_to_sap.utils import request_payload, response_filter


def time_data_export(group_id):
    """T-Systems Time Data Export Task Group following PWC pattern."""
    with rail.TaskGroup(group_id=group_id):

        def form_download_parameters(dag_run):
            """Form download parameters for T-Systems CSV export."""
            return json.dumps({
                "columnUris": [],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-data-export"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "value": {
                            "uris": [rail.result(f"{group_id}.get_export_uri")]
                        },
                    },
                },
                "fileFormatScriptUri": dag_run.conf['fileformat_script_uri']
            })

        # Create T-Systems time data export batch
        create_export = rail.RepliconServiceOperator(
            task_id='create_export',
            endpoint=f'/services/TimeDataExportService1.svc/CreateTimeDataExportBatch',
            data=request_payload.get_export_request
        )

        # Execute and wait for time export batch
        (execute_time_export, wait_for_time_export) = rail.batch_execution(
            group_id='execute_time_export',
            creation_task_id=create_export.task_id,
        )

        # Get export URI from batch results
        get_export_uri = rail.RepliconServiceOperator(
            task_id='get_export_uri',
            endpoint='/services/TimeDataExportService1.svc/GetCreateTimeDataExportBatchResults',
            data={
                "timeDataExportBatchUri": "{{ result('" + group_id + ".create_export') }}"
            },
            data_handler=response_filter.retrieve_export_uri
        )

        update_export_name = rail.RepliconServiceOperator(
            task_id="update_export_name",
            endpoint="/services/TimeDataExportService1.svc/UpdateTimeDataExportName",
            data={
                "target": {
                    "uri": "{{ result('" + group_id + ".get_export_uri') }}"
                },
                "name": "{{ result('get_export_data').twb_file_name }}"
            }
        )

        update_export_description = rail.RepliconServiceOperator(
            task_id="update_export_description",
            endpoint="/services/TimeDataExportService1.svc/UpdateTimeDataExportDescription",
            data={
                "target": {
                    "uri": "{{ result('" + group_id + ".get_export_uri') }}"
                },
                "description": "{{ result('get_export_data').export_file_name }}"
            }
        )

        # Create download batch for T-Systems format
        create_download_batch = rail.RepliconServiceOperator(
            task_id='create_download_batch',
            endpoint=f'/services/TimeDataExportService1.svc/CreateTimeDataDownloadBatch',
            data=form_download_parameters
        )

        # Execute and wait for download batch
        (execute_download_batch, wait_for_download_batch) = rail.batch_execution(
            group_id='execute_download_batch',
            creation_task_id=create_download_batch.task_id
        )

        # Get download URL
        get_download_url = rail.RepliconServiceOperator(
            task_id='get_download_url',
            endpoint=f'/services/TimeDataExportService1.svc/GetTimeDataDownloadBatchResults',
            data={
                "timeDataDownloadBatchUri": "{{ result('" + group_id + ".create_download_batch') }}"
            },
            data_handler=response_filter.extract_download_url
        )

        # Download the export file
        download_export = rail.HTTPDownloadFileOperator(
            task_id='download_export',
            url="{{ result('" + group_id + ".get_download_url') }}",
        )

        # Load CSV data
        load_export = rail.LoadCSVFileOperator(
            task_id='load_export',
            document="{{ result('" + group_id + ".download_export') }}"
        )

        # Create collection for time export data
        create_timeexport_collection = rail.CreateCollectionOperator(
            task_id='create_timeexport_collection',
            name='datatoexport',
            source="{{ result('" + group_id + ".load_export') }}"
        )

        # Check if we have any data to export
        has_any_data = rail.IfOperator(
            task_id='has_any_data',
            test="{{ result('" + group_id +
            ".create_timeexport_collection', 'length') > 0 }}",
            yes_task=f"{group_id}.validate_and_filter_data",
            no_task=f"{group_id}.update_export_name_with_nodownloaddata"
        )

        # Handle case when no data is found
        update_export_name_with_nodownloaddata = rail.RepliconServiceOperator(
            task_id="update_export_name_with_nodownloaddata",
            endpoint="/services/TimeDataExportService1.svc/UpdateTimeDataExportName",
            data={
                "target": {
                    "uri": "{{ result('" + group_id + ".get_export_uri') }}"
                },
                "name": "NODATA_{{ result('get_export_data').twb_file_name }}"
            }
        )

        mark_as_completed = rail.RepliconServiceOperator(
            task_id="mark_as_completed",
            endpoint="/services/TimeDataExportService1.svc/MarkTimeDataExportAsComplete",
            data={
                "target": {
                    "uri": "{{ result('" + group_id + ".get_export_uri') }}"
                }
            }
        )

        # Write log for no data scenario
        write_log_with_no_data = rail.WriteLogOperator(
            task_id="write_log_with_no_data",
            severity="Success",
            message="no data in time data export",
            properties={
                "process_started": "{{ dag_run.conf.process_start_time }}",
                "process_end": "{{ current_time_in_specified_tz('Europe/Berlin', '%Y-%m-%dT%H:%M:%S') }}",
                "rowcount": 0,
                "filename": "NA",
                "datapresent": "no",
                "status": "success",
            }
        )

        # Validate and filter data for T-Systems requirements
        validate_and_filter_data = rail.DataAdaptorOperator(
            task_id='validate_and_filter_data',
            source="{{ result('" + group_id + ".load_export') }}",
            columns=['time_entry_id', 'employee_ID', 'project_ID', 'entry_date', 'billing_entry',
                     'billing_rate_name', 'hours', 'task_name', 'task_code', 'task_activity_name',
                     'task_description', 'sap_activity_type','transaction_id'],
            data=response_filter.translate_rows
        )

        # Create collection for validated data
        create_validated_data_collection = rail.CreateCollectionOperator(
            task_id='create_validated_data_collection',
            name='validateddata',
            source="{{ result('" + group_id + ".validate_and_filter_data') }}"
        )

        finish_time_export_task_group = rail.EmptyOperator(
            task_id='finish_time_export_task_group'
        )

        create_export >> execute_time_export
        
        wait_for_time_export >> get_export_uri >> update_export_name >> update_export_description >> \
            create_download_batch >> execute_download_batch

        wait_for_download_batch >> get_download_url >> download_export >> \
            load_export >> create_timeexport_collection >> has_any_data

        has_any_data >> rail.Label("No") >> update_export_name_with_nodownloaddata >> \
            mark_as_completed >> write_log_with_no_data

        has_any_data >> rail.Label("Yes") >> validate_and_filter_data >> \
            create_validated_data_collection >> finish_time_export_task_group

    return (create_export, write_log_with_no_data, finish_time_export_task_group)
