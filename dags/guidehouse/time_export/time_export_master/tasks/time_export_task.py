import rail
from guidehouse.time_export.time_export_master.utils.request_payload import (
    time_export_generate_request,
)
from guidehouse.time_export.time_export_master.utils.custom_methods import (
    retrieve_export_uri,
)


def time_data_export(group_id, get_export_name):

    with rail.TaskGroup(group_id=group_id):

        create_export = rail.RepliconServiceOperator(
            task_id="create_export",
            endpoint="/services/TimeDataExportService1.svc/CreateTimeDataExportBatch",
            data=lambda dag_run:time_export_generate_request(dag_run),
        )

        execute_export, wait_for_export = rail.batch_execution(
            group_id="execute_time_export",
            creation_task_id=create_export.task_id,
            retries=0,
        )

        get_export_uri = rail.RepliconServiceOperator(
            task_id="get_export_uri",
            endpoint="/services/TimeDataExportService1.svc/GetCreateTimeDataExportBatchResults",
            data={
                "timeDataExportBatchUri": "{{ result('"
                + create_export.task_id
                + "') }}"
            },
            data_handler=retrieve_export_uri,
        )

        update_export_name = rail.RepliconServiceOperator(
            task_id="update_export_name",
            endpoint="/services/TimeDataExportService1.svc/UpdateTimeDataExportName",
            data={
                "target": {"uri": "{{ result('" + get_export_uri.task_id + "') }}"},
                "name": get_export_name,
            },
        )

        mark_as_completed = rail.RepliconServiceOperator(
            task_id="mark_as_completed",
            endpoint="/services/TimeDataExportService1.svc/MarkTimeDataExportAsComplete",
            data={"target": {"uri": "{{ result('" + get_export_uri.task_id + "') }}"}},
        )

        catch_dataexport_error = rail.EmptyOperator(
            task_id="catch_dataexport_error", trigger_rule="one_failed"
        )

        get_export_uri_for_cancel = rail.RepliconServiceOperator(
            task_id="get_export_uri_for_cancel",
            endpoint="/services/TimeDataExportService1.svc/GetCreateTimeDataExportBatchResults",
            data={
                "timeDataExportBatchUri": "{{ result('"
                + create_export.task_id
                + "') }}"
            },
            data_handler=retrieve_export_uri,
        )

        get_time_export_status = rail.RepliconServiceOperator(
            task_id="get_time_export_status",
            endpoint="/services/TimeDataExportService1.svc/GetTimeDataExportDetails",
            data=lambda: {
                "target": {
                    "uri": rail.result(get_export_uri_for_cancel.task_id),
                    "name": None,
                }
            },
            data_handler=lambda response: (
                response["status"] if (response and "status" in response) else {}
            ),
        )

        def create_export_status_batch_payload(status):
            return {
                "target": {
                    "uri": rail.result(get_export_uri_for_cancel.task_id),
                    "name": None,
                },
                "statusUri": f"urn:replicon:time-data-export-status:{status}",
            }

        create_export_status_draft_batch = rail.RepliconServiceOperator(
            task_id="create_export_status_draft_batch",
            endpoint="/services/TimeDataExportService1.svc/CreateTimeDataExportStatusBatch",
            data=lambda: create_export_status_batch_payload("draft"),
        )

        execute_export_status_draft_batch, wait_for_export_status_draft_batch = (
            rail.batch_execution(
                group_id="execute_time_export_status_draft_batch",
                creation_task_id=create_export_status_draft_batch.task_id,
            )
        )

        create_export_status_cancel_batch = rail.RepliconServiceOperator(
            task_id="create_export_status_cancel_batch",
            endpoint="/services/TimeDataExportService1.svc/CreateTimeDataExportStatusBatch",
            data=lambda: create_export_status_batch_payload("cancelled"),
        )

        execute_export_status_cancel_batch, wait_for_export_status_cancel_batch = (
            rail.batch_execution(
                group_id="execute_time_export_status_cancel_batch",
                creation_task_id=create_export_status_cancel_batch.task_id,
            )
        )

        is_time_export_status_draft = rail.IfOperator(
            task_id="is_time_export_status_draft",
            test=lambda: rail.result(get_time_export_status.task_id).get(
                "displayText", ""
            )
            == "draft",
            yes_task=create_export_status_cancel_batch.task_id,
            no_task=create_export_status_draft_batch.task_id,
        )

        fail_timeoff_export = rail.FailOperator(
            task_id="fail_timeoff_export",
            message="{{ get_error_message() }}",
        )

        (
            create_export
            >> execute_export
            >> wait_for_export
            >> get_export_uri
            >> update_export_name
            >> mark_as_completed
            >> catch_dataexport_error
        )
        (
            catch_dataexport_error
            >> rail.Label("OnFailure")
            >> get_export_uri_for_cancel
            >> get_time_export_status
            >> is_time_export_status_draft
            >> rail.Label("Yes")
            >> create_export_status_cancel_batch
        )
        (
            get_time_export_status
            >> is_time_export_status_draft
            >> rail.Label("No")
            >> create_export_status_draft_batch
            >> execute_export_status_draft_batch
            >> wait_for_export_status_draft_batch
            >> create_export_status_cancel_batch
            >> execute_export_status_cancel_batch
            >> wait_for_export_status_cancel_batch
            >> fail_timeoff_export
        )

    return (create_export, mark_as_completed)
