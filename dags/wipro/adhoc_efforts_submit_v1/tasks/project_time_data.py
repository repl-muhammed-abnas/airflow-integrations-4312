from wipro.adhoc_efforts_submit_v1.utils import request_payload
import rail
null = None


def project_time_export(group_id, country_code,country):
    with rail.TaskGroup(
        group_id=group_id,
        prefix_group_id=False
    ) as task_group:
        
        create_time_export_download_batch = rail.RepliconServiceOperator(
            task_id="create_time_export_download_batch",
            endpoint="/services/TimeDataExportService1.svc/CreateTimeDataDownloadBatch",
            data=request_payload.download_time_export_request
        )

        execute_time_export_download_batch, wait_for_time_export_download_batch = rail.batch_execution(
            group_id="execute_time_export_download",
            creation_task_id=create_time_export_download_batch.task_id
        )

        get_time_export_download_batch_results = rail.RepliconServiceOperator(
            task_id="get_time_export_download_batch_results",
            endpoint="/services/TimeDataExportService1.svc/GetTimeDataDownloadBatchResults",
            data=lambda: {
                    "timeDataDownloadBatchUri": rail.result(create_time_export_download_batch.task_id)
            },
            data_handler=lambda response: response["downloadUrl"]
        )

        download_time_export_file = rail.HTTPDownloadFileOperator(
            task_id="download_time_export_file",
            url='{{result("get_time_export_download_batch_results")}}'

        )

        load_time_export_csv = rail.LoadCSVFileOperator(
            task_id="load_time_export_csv",
            document="{{result('download_time_export_file')}}",
        )

        create_time_export_download_batch >>\
        execute_time_export_download_batch >> wait_for_time_export_download_batch >>\
        get_time_export_download_batch_results >> download_time_export_file >>\
        load_time_export_csv

    return task_group
