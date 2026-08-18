import rail
from rail.task_groups.batch_execution import batch_execution


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_c1_timesheet_dag_id,
        description=f'DxcTechnology TimeSheet Auto Submission LOA  - AutoSubmission C1 Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_submit_timesheet_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        create_log = rail.CreateLogOperator(
            task_id="create_log" 
        )

        create_submit_batch = rail.RepliconServiceOperator(
            task_id="create_submit_batch",
            endpoint="/services/TimesheetTimeEntryRevisionGroupApprovalService1.svc/CreateSubmitBatch",
            data=lambda dag_run: {
                "timesheetUris": list(map(lambda x: x['timesheet_period_uri'], dag_run.conf['items'])),
                "comments": "Submitted by Timesheet Autosubmission LOA Integration",
                "submitOptions": []
            }
        )

        (batch_enter, batch_exit) = batch_execution(
            'process_submit_batch', 'create_submit_batch', config.replicon_conn_id)

        get_batch_result = rail.RepliconServiceOperator(
            task_id="get_batch_result",
            endpoint="/services/TimesheetTimeEntryRevisionGroupApprovalService1.svc/GetTimesheetTimeEntryRevisionGroupApprovalBatchResults",
            data={
                "timesheetTimeEntryRevisionGroupApprovalBatchUri": "{{ result('create_submit_batch') }}"
            }
        )

        add_batch_log_entry = rail.WriteLogOperator(
            task_id="add_batch_log_entry",
            items=lambda: rail.result('get_batch_result')['completedUris'],
            log='{{ result("create_log") }}',
            severity="Success",
            properties=lambda item, dag_run: {
                'timesheet_uri': item,
                'employee_id': rail.find_first_by_attr_and_get_attr(dag_run.conf['items'],
                    'timesheet_period_uri', item, 'employee_id'),
                'timesheet_period': rail.find_first_by_attr_and_get_attr(dag_run.conf['items'],
                    'timesheet_period_uri', item, 'timesheet_period'),
                'details': "Timesheet submitted",
                'status': "Success",
            },
            message="Timesheet Submitted Successfully",
        )

        log_batch_error = rail.WriteLogOperator(
            task_id="log_batch_error",
            items="{{ result('get_batch_result').errors | to_json }}",
            log='{{ result("create_log") }}',
            severity="Error",
            properties=lambda item, dag_run: {
                'timesheeturi': item['timesheet']['uri'],
                'employee_id': rail.find_first_by_attr_and_get_attr(dag_run.conf['items'],
                    'timesheet_period_uri', item['timesheet']['uri'], 'employee_id'),
                'timesheet_period': rail.find_first_by_attr_and_get_attr(dag_run.conf['items'],
                    'timesheet_period_uri', item['timesheet']['uri'], 'timesheet_period'),
                'details': "Timesheet submitted",
                'status': "Error",
            },
            # pylint: disable=line-too-long
            message="{{ 'Timesheet submitted' if item['notifications'][0]['displayText']=='Timesheet has already been approved.' else item['notifications'][0]['displayText'] }}",
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

        create_log >> create_submit_batch >> batch_enter >> batch_exit >> get_batch_result >> add_batch_log_entry >> log_batch_error >> \
            catch_and_log_errors
    return dag


rail.for_each_instance(create_dag)
