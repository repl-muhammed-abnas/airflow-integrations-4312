import rail
from technicolorg3.time_export_to_ceta.utils import request_payload


def time_export_task(file_format_uri, export_file_name):
    with rail.TaskGroup(group_id="generate_time_export", prefix_group_id=False):

        create_export = rail.RepliconServiceOperator(
            task_id='create_export',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataExportBatch',
            data=request_payload.get_create_export_payload
        )

        (execute_export, wait_for_export) = rail.batch_execution(
            group_id='execute_time_export_batch',
            creation_task_id=create_export.task_id,
            wait_timeout=60*60*5,
        )

        get_export_batch_results = rail.RepliconServiceOperator(
            task_id='get_export_batch_results',
            endpoint='/services/TimeDataExportService1.svc/GetCreateTimeDataExportBatchResults',
            data=lambda : {
                "timeDataExportBatchUri": rail.result('create_export')
            }

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

        mark_as_completed = rail.RepliconServiceOperator(
            task_id="mark_as_completed",
            endpoint="/services/TimeDataExportService1.svc/MarkTimeDataExportAsComplete",
            data=request_payload.get_mark_as_completed_payload,
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
            data=lambda:{
                "timeDataDownloadBatchUri": rail.result('create_download_batch')
            },
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

        cancel_time_export = rail.RepliconServiceOperator(
            task_id='cancel_time_export',
            endpoint="/services/TimeDataExportService1.svc/CancelTimeDataExport",
            data=request_payload.get_cancel_time_export_payload
        )

        fail_time_export = rail.FailOperator(
            task_id='fail_time_export',
            message='{{ get_error_message() }}',
        )

        time_export_success = rail.EmptyOperator(
            task_id='time_export_success',
        )

        create_export >> execute_export >> wait_for_export >> get_export_batch_results
        get_export_batch_results >> has_batch_error >> rail.Label(
            'Yes') >> fail_export
        has_batch_error >> rail.Label('No') >> update_export_name
        update_export_name >> mark_as_completed >> create_download_batch
        create_download_batch >> execute_download_batch >> wait_for_download_batch >> get_download_url
        get_download_url >> download_export >> load_export >> rail.Label(
            'On Error') >> catch_dataexport_error >> cancel_time_export >> fail_time_export
        load_export >> rail.Label('On Success') >> time_export_success
    return (create_export, time_export_success)
