"""
Cancel export TaskGroup for Azenta Oracle PPM Time Export Integration (FI017)
Transitions a Replicon time-data-export batch from draft → cancelled, so its entries revert to
un-exported and become eligible for the next run's export filter — used both on the extraction-error
path and whenever the batch was extracted successfully but never actually posted to Oracle
(validation failure, posting failure).
"""
import rail
from azenta.time_export.utils import request_payload


def cancel_time_export(export_uri_task_id, group_id='cancel_export'):
    """
    export_uri_task_id: task_id (may be group-prefixed, e.g. 'time_data_export.get_export_uri')
                         whose result is the export batch URI to cancel.
    group_id: unique TaskGroup id for this call — task_ids inside are suffixed with it (per
              prefix_group_id=False, matching dags/adessa/timeoff_sync/tasks/create_timoff_booking.py)
              so this function can be called more than once in the same DAG without id collisions.
    """
    with rail.TaskGroup(group_id=group_id, prefix_group_id=False):

        create_export_status_draft_batch = rail.RepliconServiceOperator(
            task_id=f'create_export_status_draft_batch_{group_id}',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataExportStatusBatch',
            data=lambda: request_payload.create_export_status_batch_payload("draft", export_uri_task_id)
        )

        execute_export_status_draft_batch, wait_for_export_status_draft_batch = rail.batch_execution(
            group_id=f'execute_time_export_status_draft_batch_{group_id}',
            creation_task_id=create_export_status_draft_batch.task_id
        )

        create_export_status_cancel_batch = rail.RepliconServiceOperator(
            task_id=f'create_export_status_cancel_batch_{group_id}',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataExportStatusBatch',
            data=lambda: request_payload.create_export_status_batch_payload("cancelled", export_uri_task_id)
        )

        execute_export_status_cancel_batch, wait_for_export_status_cancel_batch = rail.batch_execution(
            group_id=f'execute_time_export_status_cancel_batch_{group_id}',
            creation_task_id=create_export_status_cancel_batch.task_id
        )

        finish_export_cancel = rail.EmptyOperator(
            task_id=f'finish_export_cancel_{group_id}'
        )

        # pylint: disable=pointless-statement
        create_export_status_draft_batch >> execute_export_status_draft_batch >> wait_for_export_status_draft_batch \
            >> create_export_status_cancel_batch >> execute_export_status_cancel_batch \
            >> wait_for_export_status_cancel_batch >> finish_export_cancel

        return (create_export_status_draft_batch, finish_export_cancel)
