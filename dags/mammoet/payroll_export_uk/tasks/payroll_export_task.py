import rail
from mammoet.payroll_export_uk.utils import request_payload, response_filters


def payroll_data_export(
    group_id,
    generate_request,
    get_export_name,
    file_script_uri,
    retries,
):

    with rail.TaskGroup(group_id=group_id):
        create_export = rail.RepliconServiceOperator(
            task_id='create_export',
            endpoint='/services/PayRunService1.svc/CreatePayRunBatch',
            data=generate_request
        )

        execute_export, wait_for_export = rail.batch_execution(
            group_id='execute_time_export',
            creation_task_id=create_export.task_id,
            retries=retries,
        )

        get_export_uri = rail.RepliconServiceOperator(
            task_id='get_export_uri',
            endpoint='/services/PayRunService1.svc/GetCreatePayRunBatchResults',
            data={
                "payRunBatchUri": "{{ result('" + create_export.task_id + "') }}"
            },
            data_handler=response_filters.retrieve_export_uri,

        )

        update_export_name = rail.RepliconServiceOperator(
            task_id="update_export_name",
            endpoint="/services/PayRunService1.svc/UpdatePayRunName",
            data={
                "target": {
                    "uri": "{{ result('" + get_export_uri.task_id + "') }}"
                },
                "name": get_export_name
            },

        )

        mark_as_completed = rail.RepliconServiceOperator(
            task_id="mark_as_completed",
            endpoint="/services/PayRunService1.svc/MarkPayRunAsComplete",
            data={
                "target": {
                    "uri": "{{ result('" + get_export_uri.task_id + "') }}"
                }
            }
        )

        create_download_batch = rail.RepliconServiceOperator(
            task_id='create_download_batch',
            endpoint='/services/PayRunService1.svc/CreatePayrollDownloadBatch',
            data=lambda: request_payload.form_download_parameters(
                group_id, file_script_uri),
        )

        execute_download_batch, wait_for_download_batch = rail.batch_execution(
            group_id='execute_download_batch',
            creation_task_id=create_download_batch.task_id,
        )

        get_download_url = rail.RepliconServiceOperator(
            task_id='get_download_url',
            endpoint='/services/PayRunService1.svc/GetPayrollDownloadBatchResults',
            data={
                "payrollDownloadBatchUri": "{{ result('" + create_download_batch.task_id + "') }}"
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

        cancel_payroll_export = rail.RepliconServiceOperator(
            task_id='cancel_payroll_export',
            endpoint="/services/PayRunService1.svc/CancelPayRun",
            data=lambda: request_payload.get_revert_draft_or_cancel_payroll_export_payload(
                group_id)
        )

        fail_payroll_export = rail.FailOperator(
            task_id='fail_payroll_export',
            message='{{ get_error_message() }}',
        )

        create_export >> execute_export >> wait_for_export >> get_export_uri >> update_export_name >> mark_as_completed
        mark_as_completed >> create_download_batch >> execute_download_batch >> wait_for_download_batch >> get_download_url
        get_download_url >> download_export >> load_export >> catch_dataexport_error >> rail.Label("On Failure") >>\
            cancel_payroll_export >> fail_payroll_export

    return (create_export, load_export)
