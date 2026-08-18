import rail
from rail.task_groups.batch_execution import batch_execution


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_timesheet_autosubmission_child_{config.instance}_{config.country}_{config.entity}{config.identifier_dagname}',
        description=f'DXC - Timesheet submission_child_v2 - {config.instance}_{config.country}_{config.entity}{config.identifier_dagname}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        create_submit_batch = rail.RepliconServiceOperator(
            task_id="create_submit_batch",
            endpoint="/services/TimesheetTimeEntryRevisionGroupApprovalService1.svc/CreateSubmitBatch",
            data=lambda dag_run: {
                "timesheetUris": list(map(lambda x: x['timesheeturi'], dag_run.conf['items'])),
                "comments": "Submitted by automation",
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
            severity="Success",
            properties=lambda item, dag_run: {
                'timesheeturi': item,
                'employeeid': rail.find_first_by_attr_and_get_attr(dag_run.conf['items'],
                                                                   'timesheeturi', item, 'employeeid'),
                'username': rail.find_first_by_attr_and_get_attr(dag_run.conf['items'],
                                                                 'timesheeturi', item, 'username'),
                'timesheetperiod': rail.find_first_by_attr_and_get_attr(dag_run.conf['items'],
                                                                        'timesheeturi', item, 'timesheetperiod'),
                'details': "Timesheet submitted",
                'country_type': f'{config.country}-{config.entity}',
                'status': "Success",
            },
            message="Timesheet submitted",
        )

        # to check the error message mapping with items and error list
        log_batch_error = rail.WriteLogOperator(
            task_id="log_batch_error",
            items="{{ result('get_batch_result').errors | to_json }}",
            severity="Error",
            properties=lambda item, dag_run: {
                'timesheeturi': item['timesheet']['uri'],
                'employeeid': rail.find_first_by_attr_and_get_attr(dag_run.conf['items'],
                                                                   'timesheeturi', item['timesheet']['uri'], 'employeeid'),
                'username': rail.find_first_by_attr_and_get_attr(dag_run.conf['items'],
                                                                 'timesheeturi', item['timesheet']['uri'], 'username'),
                'timesheetperiod': rail.find_first_by_attr_and_get_attr(dag_run.conf['items'],
                                                                        'timesheeturi', item['timesheet']['uri'], 'timesheetperiod'),
                'details': "Timesheet submitted",
                'country_type': f'{config.country}-{config.entity}',
                'status': "Error",
            },
            # pylint: disable=line-too-long
            message="{{ 'Timesheet submitted' if item['notifications'][0]['displayText']=='Timesheet has already been approved.' else item['notifications'][0]['displayText'] }}",
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            items=lambda dag_run: dag_run.conf['items'],
            severity='Error',
            message='{{ get_error_message()}}',
            properties=lambda item: {
                **item, **{'status': 'Error', 'details': 'Error', 'country_type': f'{config.country}_{config.entity}'}},
        )

        create_submit_batch >> batch_enter >> batch_exit >> get_batch_result >> add_batch_log_entry >> log_batch_error >> \
            catch_and_log_errors
    return dag


rail.for_each_instance(create_dag)
