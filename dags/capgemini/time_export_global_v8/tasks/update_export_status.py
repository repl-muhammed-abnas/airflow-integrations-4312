import rail
from capgemini.time_export_global_v8.utils import request_payload


def cancel_time_export():

    with rail.TaskGroup(group_id="cancel_export", prefix_group_id=False):

        create_export_status_draft_batch = rail.RepliconServiceOperator(
            task_id='create_export_status_draft_batch',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataExportStatusBatch',
            data=lambda: request_payload.create_export_status_batch_payload("draft")
        )

        execute_export_status_draft_batch, wait_for_export_status_draft_batch = rail.batch_execution(
            group_id='execute_time_export_status_draft_batch',
            creation_task_id=create_export_status_draft_batch.task_id
        )

        create_export_status_cancel_batch = rail.RepliconServiceOperator(
            task_id='create_export_status_cancel_batch',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataExportStatusBatch',
            data=lambda: request_payload.create_export_status_batch_payload("cancelled")
        )

        execute_export_status_cancel_batch, wait_for_export_status_cancel_batch = rail.batch_execution(
            group_id='execute_time_export_status_cancel_batch',
            creation_task_id=create_export_status_cancel_batch.task_id
        )

        finish_export_cancel = rail.EmptyOperator(
            task_id='finish_export_cancel'
        )

        create_export_status_draft_batch >> execute_export_status_draft_batch >> wait_for_export_status_draft_batch \
            >> create_export_status_cancel_batch >> execute_export_status_cancel_batch >> wait_for_export_status_cancel_batch \
                >> finish_export_cancel

        return (create_export_status_draft_batch, finish_export_cancel)
