"""
Time data extraction TaskGroup for Azenta Oracle PPM Time Export Integration (FI017)
Pattern: CreateExport → batch_execution → GetResults → UpdateName → MarkComplete
         → CreateDownloadBatch → batch_execution → GetDownloadUrl → Download → LoadCSV
"""
import rail
from azenta.time_export.utils import request_payload


def time_data_export(group_id, get_export_name, approval_filter_mode):

    with rail.TaskGroup(group_id=group_id):

        create_export = rail.RepliconServiceOperator(
            task_id='create_export',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataExportBatch',
            data=lambda: request_payload.create_time_export_payload(approval_filter_mode)
        )

        execute_export, wait_for_export = rail.batch_execution(
            group_id='execute_time_export',
            creation_task_id=create_export.task_id,
            retries=0
        )

        get_export_uri = rail.RepliconServiceOperator(
            task_id='get_export_uri',
            endpoint='/services/TimeDataExportService1.svc/GetCreateTimeDataExportBatchResults',
            data={
                "timeDataExportBatchUri": "{{ result('" + create_export.task_id + "') }}"
            },
            data_handler=request_payload.retrieve_export_uri
        )

        update_export_name = rail.RepliconServiceOperator(
            task_id='update_export_name',
            endpoint='/services/TimeDataExportService1.svc/UpdateTimeDataExportName',
            data={
                "target": {
                    "uri": "{{ result('" + get_export_uri.task_id + "') }}"
                },
                "name": get_export_name
            }
        )

        mark_as_completed = rail.RepliconServiceOperator(
            task_id='mark_as_completed',
            endpoint='/services/TimeDataExportService1.svc/MarkTimeDataExportAsComplete',
            data={
                "target": {
                    "uri": "{{ result('" + get_export_uri.task_id + "') }}"
                }
            }
        )

        create_download_batch = rail.RepliconServiceOperator(
            task_id='create_download_batch',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataDownloadBatch',
            data=lambda: request_payload.create_download_batch_payload(get_export_uri.task_id)
        )

        execute_download_batch, wait_for_download_batch = rail.batch_execution(
            group_id='execute_download_batch',
            creation_task_id=create_download_batch.task_id
        )

        get_download_url = rail.RepliconServiceOperator(
            task_id='get_download_url',
            endpoint='/services/TimeDataExportService1.svc/GetTimeDataDownloadBatchResults',
            data={
                "timeDataDownloadBatchUri": "{{ result('" + create_download_batch.task_id + "') }}"
            },
            data_handler=lambda response: response['downloadUrl']
        )

        download_export = rail.HTTPDownloadFileOperator(
            task_id='download_export',
            url="{{ result('" + get_download_url.task_id + "') }}"
        )

        load_export = rail.LoadCSVFileOperator(
            task_id='load_export',
            document="{{ result('" + download_export.task_id + "') }}",
            delimiter=','
        )

        # pylint: disable=pointless-statement
        create_export >> execute_export >> wait_for_export >> get_export_uri >> update_export_name
        update_export_name >> mark_as_completed >> create_download_batch
        create_download_batch >> execute_download_batch >> wait_for_download_batch >> get_download_url
        get_download_url >> download_export >> load_export

        return (create_export, load_export)
