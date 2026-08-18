import rail
from galaxyusopcoinc.timeoffexport.utils import request_payload


def extract_timeoff(group_id, file_format_uri, export_file_name,time_zone):
    with rail.TaskGroup(group_id=group_id, prefix_group_id=False):

        create_export = rail.RepliconServiceOperator(
            task_id='create_export',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataExportBatch',
            data=request_payload.get_create_export_payload(time_zone)
        )

        (execute_export, wait_for_export) = rail.batch_execution(
            group_id='execute_timeoff_export',
            creation_task_id=create_export.task_id,
            wait_timeout=60*60*5,
        )

        get_export_batch_results = rail.RepliconServiceOperator(
            task_id='get_export_batch_results',
            endpoint='/services/TimeDataExportService1.svc/GetCreateTimeDataExportBatchResults',
            data=request_payload.get_export_batch_results_payload

        )

        has_batch_error = rail.IfOperator(
            task_id='has_batch_error',
            test="{{ result('get_export_batch_results').error | is_truthy }}",
            yes_task="fail_export",
            no_task="update_export_name",
        )

        fail_export = rail.FailOperator(
            task_id='fail_export',
            message='{{ get_error_message() }}',
        )

        update_export_name = rail.RepliconServiceOperator(
            task_id="update_export_name",
            endpoint="/services/TimeDataExportService1.svc/UpdateTimeDataExportName",
            data=lambda: request_payload.get_update_export_name_payload(
                export_file_name)
        )

        create_export_status_complete_batch = rail.RepliconServiceOperator(
            task_id='create_export_status_complete_batch',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataExportStatusBatch',
            data=request_payload.get_mark_as_completed_payload,
        )

        execute_export_status_complete_batch, wait_for_export_status_complete_batch = rail.batch_execution(
            group_id='execute_time_export_status_complete_batch',
            creation_task_id=create_export_status_complete_batch.task_id
        )

        create_download_batch = rail.RepliconServiceOperator(
            task_id='create_download_batch',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataDownloadBatch',
            data=lambda: request_payload.get_download_batch_payload(
                file_format_uri)
        )

        (execute_download_batch, wait_for_download_batch) = rail.batch_execution(
            group_id='execute_download_batch',
            creation_task_id=create_download_batch.task_id,
            wait_timeout=60*60*5,
        )

        get_download_url = rail.RepliconServiceOperator(
            task_id='get_download_url',
            endpoint='/services/TimeDataExportService1.svc/GetTimeDataDownloadBatchResults',
            data=request_payload.get_download_url_payload,
            data_handler=lambda response: response['downloadUrl'],
        )

        download_export = rail.HTTPDownloadFileOperator(
            task_id='download_export',
            url="{{result('get_download_url')}}",
        )

        load_export = rail.LoadCSVFileOperator(
            task_id='load_export',
            document="{{result('download_export')}}"
        )

        catch_dataexport_error = rail.EmptyOperator(
            task_id='catch_dataexport_error',
            trigger_rule='one_failed'
        )

        create_export_status_cancel_batch_1 = rail.RepliconServiceOperator(
            task_id='create_export_status_cancel_batch_1',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataExportStatusBatch',
            data=lambda: request_payload.create_export_status_batch_payload("cancel")
        )

        execute_export_status_cancel_batch_1, wait_for_export_status_cancel_batch_1 = rail.batch_execution(
            group_id='execute_time_export_status_cancel_batch_1',
            creation_task_id=create_export_status_cancel_batch_1.task_id
        )

        fail_timeoff_export = rail.FailOperator(
            task_id='fail_timeoff_export',
            message='{{ get_error_message() }}',
        )

        extract_timeoff_success = rail.EmptyOperator(
            task_id='extract_timeoff_success',
        )

        create_export >> execute_export >> wait_for_export >> get_export_batch_results
        get_export_batch_results >> has_batch_error >> rail.Label(
            'Yes') >> fail_export
        has_batch_error >> rail.Label('No') >> update_export_name
        update_export_name >> create_export_status_complete_batch >> \
        execute_export_status_complete_batch >> wait_for_export_status_complete_batch >> create_download_batch
        create_download_batch >> execute_download_batch >> wait_for_download_batch >> get_download_url
        get_download_url >> download_export >> load_export >> rail.Label(
            'On Error') >> catch_dataexport_error >> create_export_status_cancel_batch_1 >> \
        execute_export_status_cancel_batch_1 >> wait_for_export_status_cancel_batch_1 >> fail_timeoff_export
        load_export >> rail.Label('On Success') >> extract_timeoff_success
    return (create_export, extract_timeoff_success)
