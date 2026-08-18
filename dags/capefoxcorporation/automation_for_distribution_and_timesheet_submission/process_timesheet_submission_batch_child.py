import rail
from rail.task_groups.batch_execution import batch_execution
from capefoxcorporation.automation_for_distribution_and_timesheet_submission.utils import request_payload

null = None


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.child_process_timesheet_submission_batch_dag_id,
        description=f'CapeFoxCorporation Automation For Distribution and Timesheet Submission - Process timesheet submission batch Child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_submission_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        create_log = rail.CreateLogOperator(
            task_id="create_log"
        )

        create_submit_batch = rail.RepliconServiceOperator(
            task_id="create_submit_batch",
            endpoint="/services/TimesheetTimeEntryRevisionGroupApprovalService1.svc/CreateSubmitBatch",
            data=lambda dag_run: {
                "timesheetUris": list(map(lambda x: x['properties']['timesheet_uri'], dag_run.conf['items'])),
                "comments": "Submitted by automation",
                "submitOptions": []
            }
        )

        (batch_enter, batch_exit) = batch_execution(
            group_id='process_submit_batch',
            creation_task_id='create_submit_batch',
            replicon_conn_id=config.replicon_conn_id,
            wait_timeout=60*60*5,
            retries=0
        )

        get_batch_result = rail.RepliconServiceOperator(
            task_id="get_batch_result",
            trigger_rule='all_done',
            endpoint="/services/TimesheetTimeEntryRevisionGroupApprovalService1.svc/GetTimesheetTimeEntryRevisionGroupApprovalBatchResults",
            data={
                "timesheetTimeEntryRevisionGroupApprovalBatchUri": "{{ result('create_submit_batch') }}"
            }
        )

        add_batch_log_entry = rail.WriteLogOperator(
            task_id="add_batch_log_entry",
            log='{{ result("create_log") }}',
            items=lambda: rail.result('get_batch_result')['completedUris'],
            severity="Success",
            properties=lambda item, dag_run: request_payload.get_log_properties(
                item, dag_run, 'success'),
            message="Timesheet submitted",
        )

        log_batch_error = rail.WriteLogOperator(
            task_id="log_batch_error",
            log='{{ result("create_log") }}',
            items=lambda: rail.result('get_batch_result')['errors'],
            severity=lambda item: "Success" if item['timesheetError']['notifications'] and item['timesheetError']['notifications'][0] and
                    item['timesheetError']['notifications'][0]['displayText'] == "Timesheet already submitted." else "Error",
            properties=lambda item, dag_run: request_payload.get_log_properties(
                item, dag_run, 'error'),
            message="na",
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log='{{ result("create_log") }}',
            items=lambda dag_run: dag_run.conf['items'],
            severity='Error',
            message='{{ get_error_message()}}',
            properties=lambda item: {
                **item, **{'status': 'Error', 'details': 'Error'}},
        )

        create_log >> create_submit_batch >> batch_enter >> batch_exit >> get_batch_result >> add_batch_log_entry >> log_batch_error >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
