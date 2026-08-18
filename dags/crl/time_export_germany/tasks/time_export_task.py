import rail
from crl.time_export_germany.utils import request_payload, response_filters


def time_data_export(
    group_id,
    generate_request,
    get_export_name,
    file_script_uri,
    retries,
):

    with rail.TaskGroup(group_id=group_id):
        create_export = rail.RepliconServiceOperator(
            task_id='create_export',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataExportBatch',
            data=generate_request,
        )

        execute_export, wait_for_export = rail.batch_execution(
            group_id='execute_time_export',
            creation_task_id=create_export.task_id,
            retries=retries,
        )

        get_export_uri = rail.RepliconServiceOperator(
            task_id='get_export_uri',
            endpoint='/services/TimeDataExportService1.svc/GetCreateTimeDataExportBatchResults',
            data={
                "timeDataExportBatchUri": "{{ result('" + create_export.task_id + "') }}"
            },
            data_handler=response_filters.retrieve_export_uri,
        )

        update_export_name = rail.RepliconServiceOperator(
            task_id="update_export_name",
            endpoint="/services/TimeDataExportService1.svc/UpdateTimeDataExportName",
            data={
                "target": {
                    "uri": "{{ result('" + get_export_uri.task_id + "') }}"
                },
                "name": get_export_name
            },
        )

        create_export_status_complete_batch = rail.RepliconServiceOperator(
            task_id='create_export_status_complete_batch',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataExportStatusBatch',
            data=lambda: request_payload.create_export_status_complete_batch_payload(get_export_uri.task_id)
        )

        execute_export_status_complete_batch, wait_for_export_status_complete_batch = rail.batch_execution(
            group_id='execute_time_export_status_complete_batch',
            creation_task_id=create_export_status_complete_batch.task_id
        )

        create_download_batch = rail.RepliconServiceOperator(
            task_id='create_download_batch',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataDownloadBatch',
            data=lambda: request_payload.form_download_parameters(
                group_id, file_script_uri),
        )

        execute_download_batch, wait_for_download_batch = rail.batch_execution(
            group_id='execute_download_batch',
            creation_task_id=create_download_batch.task_id,
        )

        get_download_url = rail.RepliconServiceOperator(
            task_id='get_download_url',
            endpoint='/services/TimeDataExportService1.svc/GetTimeDataDownloadBatchResults',
            data={
                "timeDataDownloadBatchUri": "{{ result('" + create_download_batch.task_id + "') }}"
            },
            data_handler=lambda response: response['downloadUrl'],
        )

        download_export = rail.HTTPDownloadFileOperator(
            task_id='download_export',
            url="{{ result('" + group_id + ".get_download_url') }}",
        )

        load_export = rail.LoadCSVFileOperator(
            task_id='load_export',
            document="{{ result('" + group_id + ".download_export') }}",
        )

        catch_dataexport_error = rail.EmptyOperator(
            task_id='catch_dataexport_error',
            trigger_rule='one_failed'
        )

        get_export_uri_for_cancel = rail.RepliconServiceOperator(
            task_id='get_export_uri_for_cancel',
            endpoint='/services/TimeDataExportService1.svc/GetCreateTimeDataExportBatchResults',
            data={
                "timeDataExportBatchUri": "{{ result('" + create_export.task_id + "') }}"
            },
            data_handler=response_filters.retrieve_export_uri,
        )

        get_time_export_status = rail.RepliconServiceOperator(
            task_id="get_time_export_status",
            endpoint="/services/TimeDataExportService1.svc/GetTimeDataExportDetails",
            data=lambda: {
                "target": {
                    "uri": rail.result(get_export_uri_for_cancel.task_id),
                    "name": None
                }
            },
            data_handler=lambda response: response['status'] if (response and 'status' in response) else {}
        )

        def create_export_status_batch_payload(status):
            return {
                "target": {
                    "uri": rail.result(get_export_uri_for_cancel.task_id),
                    "name": None
                },
                "statusUri": f"urn:replicon:time-data-export-status:{status}"
            }

        create_export_status_draft_batch = rail.RepliconServiceOperator(
            task_id='create_export_status_draft_batch',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataExportStatusBatch',
            data=lambda: create_export_status_batch_payload("draft")
        )

        execute_export_status_draft_batch, wait_for_export_status_draft_batch = rail.batch_execution(
            group_id='execute_time_export_status_draft_batch',
            creation_task_id=create_export_status_draft_batch.task_id
        )

        create_export_status_cancel_batch = rail.RepliconServiceOperator(
            task_id='create_export_status_cancel_batch',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataExportStatusBatch',
            data=lambda: create_export_status_batch_payload("cancelled")
        )

        execute_export_status_cancel_batch, wait_for_export_status_cancel_batch = rail.batch_execution(
            group_id='execute_time_export_status_cancel_batch',
            creation_task_id=create_export_status_cancel_batch.task_id
        )

        status_is_draft = rail.IfOperator(
            task_id="status_is_draft",
            test=lambda: rail.result(get_time_export_status.task_id).get('displayText', '') == 'draft',
            yes_task=create_export_status_cancel_batch.task_id,
            no_task=create_export_status_draft_batch.task_id
        )

        finish_export_cancel = rail.EmptyOperator(
            task_id='finish_export_cancel'
        )

        fail_timeoff_export = rail.FailOperator(
            task_id='fail_timeoff_export',
            message='{{ get_error_message() }}',
        )

        create_export >> execute_export >> wait_for_export >> get_export_uri >> update_export_name >> create_export_status_complete_batch >> \
            execute_export_status_complete_batch >> wait_for_export_status_complete_batch >> create_download_batch >> execute_download_batch >> \
            wait_for_download_batch >> get_download_url >> download_export >> load_export >> catch_dataexport_error >> rail.Label("On Failure") >> \
            get_export_uri_for_cancel >> get_time_export_status >> status_is_draft >> rail.Label("No") >> create_export_status_draft_batch
        status_is_draft >> rail.Label("Yes") >> create_export_status_cancel_batch
        create_export_status_draft_batch >> execute_export_status_draft_batch >> wait_for_export_status_draft_batch \
            >> create_export_status_cancel_batch >> execute_export_status_cancel_batch >> wait_for_export_status_cancel_batch \
            >> finish_export_cancel
        finish_export_cancel >> fail_timeoff_export

    return (create_export, load_export)
