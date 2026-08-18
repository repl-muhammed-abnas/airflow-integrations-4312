import json
import rail
from pwcglobal.time_export_v3 import request_payload, response_filter


def time_data_export(group_id):
    with rail.TaskGroup(group_id=group_id):

        def form_download_parameters(dag_run):
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
                "fileFormatScriptUri": dag_run.conf['file_format_uri']
            })

        create_export = rail.RepliconServiceOperator(
            task_id='create_export',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataExportBatch',
            data=request_payload.get_export_request
        )

        (execute_time_export, wait_for_time_export) = rail.batch_execution(
            group_id='execute_time_export',
            creation_task_id=create_export.task_id,
        )

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
                "name": "{{ dag_run.conf.export_file_name }}"
            }
        )

        create_download_batch = rail.RepliconServiceOperator(
            task_id='create_download_batch',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataDownloadBatch',
            data=form_download_parameters
        )

        (execute_download_batch, wait_for_download_batch) = rail.batch_execution(
            group_id='execute_download_batch',
            creation_task_id=create_download_batch.task_id
        )

        get_download_url = rail.RepliconServiceOperator(
            task_id='get_download_url',
            endpoint='/services/TimeDataExportService1.svc/GetTimeDataDownloadBatchResults',
            data={
                "timeDataDownloadBatchUri": "{{ result('" + group_id + ".create_download_batch') }}"
            },
            data_handler=lambda response: response['downloadUrl']
        )

        download_export = rail.HTTPDownloadFileOperator(
            task_id='download_export',
            url="{{ result('" + group_id + ".get_download_url') }}",
        )

        load_export = rail.LoadCSVFileOperator(
            task_id='load_export',
            document="{{ result('" + group_id + ".download_export') }}"
        )

        create_timeexport_collection = rail.CreateCollectionOperator(
            task_id='create_timeexport_collection',
            name='datatoexport',
            source="{{ result('" + group_id + ".load_export') }}"
        )

        has_any_data = rail.IfOperator(
            task_id='has_any_data',
            test="{{ result('" + group_id +
            ".create_timeexport_collection', 'length') > 0 }}",
            yes_task=f"{group_id}.filter_rows",
            no_task=f"{group_id}.update_export_name_with_nodownloaddata"
        )

        update_export_name_with_nodownloaddata = rail.RepliconServiceOperator(
            task_id="update_export_name_with_nodownloaddata",
            endpoint="/services/TimeDataExportService1.svc/UpdateTimeDataExportName",
            data={
                "target": {
                    "uri": "{{ result('" + group_id + ".get_export_uri') }}"
                },
                "name": "{{ dag_run.conf.export_file_name }}_Nodownloaddata"
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

        write_log_with_no_data = rail.WriteLogOperator(
            task_id="write_log_with_no_data",
            severity="Success",
            message="no data in time data export",
            properties={
                "location": "{{ dag_run.conf.location }}",
                "process_started": "{{ dag_run.conf.process_start_time }}",
                "process_end": "{{ current_time_in_specified_tz('Europe/Paris', '%Y-%m-%dT%H:%M:%S') }}",
                "rowcount": 0,
                "filename": "NA",
                "datapresent": "no",
                "status": "success",
                "extracttype": "{{ dag_run.conf.export_period }}"
            }
        )

        filter_rows = rail.DataAdaptorOperator(
            task_id='filter_rows',
            source="{{ result('" + group_id + ".load_export') }}",
            data=response_filter.translate_rows
        )

        create_validated_data_collection = rail.CreateCollectionOperator(
            task_id='create_validated_data_collection',
            name='validateddata',
            source="{{ result('" + group_id + ".filter_rows') }}"
        )

        finish_time_export_task_group = rail.EmptyOperator(
            task_id='finish_time_export_task_group'
        )

        create_export >> execute_time_export

        wait_for_time_export >> get_export_uri >> update_export_name >> \
            create_download_batch >> execute_download_batch

        wait_for_download_batch >> get_download_url >> download_export >> \
            load_export >> create_timeexport_collection >> has_any_data

        has_any_data >> rail.Label(
            "No") >> update_export_name_with_nodownloaddata >> mark_as_completed >> \
            write_log_with_no_data

        has_any_data >> rail.Label(
            "Yes") >> filter_rows >> create_validated_data_collection >> finish_time_export_task_group

    return (create_export, write_log_with_no_data, finish_time_export_task_group)
