import rail


def cancel_time_export(_group_id, time_export_uri_task_id="get_export_uri_failed"):

    with rail.TaskGroup(group_id=_group_id, prefix_group_id=True):

        def create_export_status_batch_payload(status):
            return {
                "target": {"uri": rail.result(time_export_uri_task_id), "name": None},
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

        finish_export_cancel = rail.EmptyOperator(task_id="finish_export_cancel")

        (
            create_export_status_draft_batch
            >> execute_export_status_draft_batch
            >> wait_for_export_status_draft_batch
            >> create_export_status_cancel_batch
            >> execute_export_status_cancel_batch
            >> wait_for_export_status_cancel_batch
            >> finish_export_cancel
        )

        return (create_export_status_draft_batch, finish_export_cancel)
