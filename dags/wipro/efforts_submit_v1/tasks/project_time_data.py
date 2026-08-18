from wipro.efforts_submit_v1.utils import request_payload
import rail
null = None


def project_time_export(group_id, country_code, country):
    with rail.TaskGroup(
        group_id=group_id,
        prefix_group_id=False
    ) as task_group:

        create_time_export_batch = rail.RepliconServiceOperator(
            task_id="create_time_export_batch",
            endpoint="/services/TimeDataExportService1.svc/CreateTimeDataExportBatch",
            data=lambda: request_payload.create_time_export_request(country)
        )

        execute_time_export_batch, wait_for_time_export_batch = rail.batch_execution(
            group_id="execute_time_export",
            creation_task_id=create_time_export_batch.task_id
        )

        get_time_export_batch_results = rail.RepliconServiceOperator(
            task_id="get_time_export_batch_results",
            endpoint="/services/TimeDataExportService1.svc/GetCreateTimeDataExportBatchResults",
            data={
                    "timeDataExportBatchUri": '{{result("create_time_export_batch")}}'
            },
            data_handler=request_payload.get_time_export_uri
        )

        update_time_export_name = rail.RepliconServiceOperator(
            task_id="update_time_export_name",
            endpoint="/services/TimeDataExportService1.svc/UpdateTimeDataExportName",
            data={
                    "target": {
                        "uri": '{{result("get_time_export_batch_results")}}',
                        "name": null
                    },
                "name": f"Time_export_{country_code}_" + '{{ result("process_start_time") }}'
            }
        )

        mark_time_export_as_complete = rail.RepliconServiceOperator(
            task_id="mark_time_export_as_complete",
            endpoint="/services/TimeDataExportService1.svc/MarkTimeDataExportAsComplete",
            data={
                    "target": {
                        "uri": '{{result("get_time_export_batch_results")}}',
                        "name": null
                    }
            }
        )

        catch_time_export_error = rail.EmptyOperator(
            task_id='catch_error',
            trigger_rule='one_failed'
        )

        mark_time_export_as_draft = rail.RepliconServiceOperator(
            task_id="mark_time_export_as_draft",
            endpoint="/services/TimeDataExportService1.svc/MarkTimeDataExportAsDraft",
            data={
                    "target": {
                        "uri": '{{result("get_time_export_batch_results")}}',
                        "name": null
                    }
            }
        )

        cancel_time_export_batch = rail.RepliconServiceOperator(
            task_id="cancel_time_export_batch",
            endpoint="/services/TimeDataExportService1.svc/CancelTimeDataExport",
            data={
                    "target": {
                        "uri": '{{result("get_time_export_batch_results")}}',
                        "name": null
                    }
            }
        )

        update_cancelled_time_export_name = rail.RepliconServiceOperator(
            task_id="update_cancelled_time_export_name",
            endpoint="/services/TimeDataExportService1.svc/UpdateTimeDataExportName",
            data={
                    "target": {
                        "uri": '{{result("get_time_export_batch_results")}}',
                        "name": null
                    },
                "name": f"Time_export_{country_code}_cancelled_" + '{{ result("process_start_time") }}'
            }
        )

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

        create_time_export_batch >>\
            execute_time_export_batch >> wait_for_time_export_batch >>\
            get_time_export_batch_results >> update_time_export_name >>\
            mark_time_export_as_complete >> rail.Label("On failure") >>\
            catch_time_export_error >> mark_time_export_as_draft >> \
            cancel_time_export_batch >> update_cancelled_time_export_name
        mark_time_export_as_complete >> rail.Label("On Sucess") >>\
            create_time_export_download_batch >>\
            execute_time_export_download_batch >> wait_for_time_export_download_batch >>\
            get_time_export_download_batch_results >> download_time_export_file >>\
            load_time_export_csv

    return task_group
