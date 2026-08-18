import rail
from dxctechnology.adhoc.timeentry_submission import request_payload


def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_timeentry_submission_child_adhoc_{config.instance}',
        description='Time Entry Submission Child Adhoc',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        reopen_timeentry = rail.RepliconServiceOperator(
            task_id="reopen_timeentry",
            endpoint="/services/TimeEntryRevisionGroupApprovalService1.svc/Reopen",
            data=request_payload.reopen_timeentry_payload,
        )

        submit_timeentry = rail.RepliconServiceOperator(
            task_id="submit_timeentry",
            endpoint="/services/TimeEntryRevisionGroupApprovalService1.svc/Submit",
            data=request_payload.submit_timeentry_payload,
        )

        log_successfull = rail.WriteLogOperator(
            task_id='log_successfull',
            message='The entry ID "{{dag_run.conf.timeentryid}}" has been successfully re-opened and submitted',
            severity='Success',
            properties=lambda dag_run: {
                'Employee Id': dag_run.conf['employeeid'],
                'Timeentry Id':  dag_run.conf['timeentryid'],
                'Timesheet Status': dag_run.conf['timesheetstatus'],
                'Timesheet period': dag_run.conf['timesheetperiod'],
                'status': 'Success',
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                'Employee Id': dag_run.conf['employeeid'],
                'Timeentry Id':  dag_run.conf['timeentryid'],
                'Timesheet Status': dag_run.conf['timesheetstatus'],
                'Timesheet period': dag_run.conf['timesheetperiod'],
                'status': 'Error',
            }
        )

        reopen_timeentry >> submit_timeentry >> log_successfull >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag_wbs)
