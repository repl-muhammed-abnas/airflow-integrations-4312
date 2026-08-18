import rail
from rail.task_groups.batch_execution import batch_execution
# config : https://github.com/replicon/airflow-integrations/blob/main/dags/pwcglobal/user_import/config.py


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'pwcglobal_timesheet_auto_submission_timeoff_hours_v4_child_{config.instance}',
        description=f'PwCGlobal - Timesheet auto submission timeoff hours Child_v4.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        create_submit_batch = rail.RepliconServiceOperator(
            task_id="create_submit_batch",
            endpoint="/services/TimesheetTimeEntryRevisionGroupApprovalService1.svc/CreateSubmitBatch",
            data=lambda: {
                "timesheetUris": list(map(lambda x: x['timesheeturi'], rail.get_current_context()['dag_run'].conf['items'])),
                "comments": "System approved based on automated workflow",
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

        has_batch_error = rail.IfOperator(
            task_id='has_batch_error',
            test="{{  result('get_batch_result').errors | length > 0}}",
            yes_task='log_batch_error',
            no_task='create_forced_approve_batch'
        )

        log_batch_error = rail.WriteLogOperator(
            task_id="log_batch_error",
            severity="Error",
            items="{{ result('get_batch_result').errors | to_json }}",
            properties=lambda item: {
                'timesheeturi': item['timesheetError']['timesheet']['uri'],
                'User_Name': rail.find_first_by_attr_and_get_attr(rail.get_current_context()['dag_run'].conf['items'],
                                                                  'timesheeturi', item['timesheetError']['timesheet']['uri'], 'User_Name'),
                'timesheetperiod': rail.find_first_by_attr_and_get_attr(rail.get_current_context()['dag_run'].conf['items'],
                                                                        'timesheeturi', item['timesheetError']['timesheet']['uri'], 'Timesheet_Start_Date')
                + " - " +
                rail.find_first_by_attr_and_get_attr(rail.get_current_context()['dag_run'].conf['items'],
                                                     'timesheeturi', item['timesheetError']['timesheet']['uri'], 'Timesheet_End_Date'),
                'status': "Success" if item['timesheetError']['notifications'] and item['timesheetError']['notifications'][0] and
                item['timesheetError']['notifications'][0]['displayText'] == "Timesheet already submitted." else "Error",
            },
            # pylint: disable=line-too-long
            message="{{ 'Successfully Approved' if item['timesheetError']['notifications'][0]['displayText']=='Timesheet already submitted.' else item['timesheetError']['notifications'][0]['displayText'] }}",
        )

        create_forced_approve_batch = rail.RepliconServiceOperator(
            task_id="create_forced_approve_batch",
            endpoint="/services/TimesheetApprovalService1.svc/CreateForcedApproveBatch",
            data=lambda: {
                "timesheetUris": rail.result('get_batch_result')['completedUris'],
                "comments": "System approved based on automated workflow"
            }
        )

        execute_timesheet_approvalbatch2 = rail.RepliconServiceOperator(
            task_id="execute_timesheet_approvalbatch2",
            endpoint="/services/TimesheetApprovalService1.svc/ExecuteTimesheetApprovalBatch2",
            data={
                "timesheetApprovalBatchUri": "{{ result('create_forced_approve_batch') }}"
            }
        )

        add_log_entry = rail.WriteLogOperator(
            task_id="add_log_entry",
            items=lambda: rail.result('execute_timesheet_approvalbatch2')[
                'completedUris'],
            severity="Success",
            properties=lambda item: {
                'timesheeturi': item,
                'User_Name': rail.find_first_by_attr_and_get_attr(rail.get_current_context()['dag_run'].conf['items'],
                                                                  'timesheeturi', item, 'User_Name'),
                'timesheetperiod': rail.find_first_by_attr_and_get_attr(rail.get_current_context()['dag_run'].conf['items'],
                                                                        'timesheeturi', item, 'Timesheet_Start_Date')
                + " - " +
                rail.find_first_by_attr_and_get_attr(rail.get_current_context()['dag_run'].conf['items'], 'timesheeturi',
                                                     item, 'Timesheet_End_Date'),
                'status': "Success",
            },
            message="Successfully Approved",
        )

        has_batch2_error = rail.IfOperator(
            task_id='has_batch2_error',
            test="{{  result('execute_timesheet_approvalbatch2').errors | length > 0}}",
            yes_task='log_batch2_error',
        )

        log_batch2_error = rail.WriteLogOperator(
            task_id="log_batch2_error",
            severity="Error",
            items="{{ result('execute_timesheet_approvalbatch2').errors | to_json }}",
            properties=lambda item: {
                'timesheeturi': item['timesheet']['uri'],
                'User_Name': rail.find_first_by_attr_and_get_attr(rail.get_current_context()['dag_run'].conf['items'],
                                                                  'timesheeturi', item['timesheet']['uri'], 'User_Name'),
                'timesheetperiod': rail.find_first_by_attr_and_get_attr(rail.get_current_context()['dag_run'].conf['items'],
                                                                        'timesheeturi', item['timesheet']['uri'], 'Timesheet_Start_Date')
                + " - " +
                rail.find_first_by_attr_and_get_attr(rail.get_current_context()['dag_run'].conf['items'],
                                                     'timesheeturi', item['timesheet']['uri'], 'Timesheet_End_Date'),
                'status': "Success" if item['notifications'] and item['notifications'][0] and
                item['notifications'][0]['displayText'] == "Timesheet has already been approved." else "Error",
            },
            # pylint: disable=line-too-long
            message="{{ 'Successfully Approved' if item['notifications'][0]['displayText']=='Timesheet has already been approved.' else item['notifications'][0]['displayText'] }}",
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity="Error",
            message='{{ get_error_message()}}',
            items=lambda: rail.get_current_context()['dag_run'].conf['items'],
            properties=lambda item: {
                'timesheeturi': item['timesheeturi'],
                'User_Name': item['User_Name'],
                'timesheetperiod': item['Timesheet_Start_Date'] + " - " + item['Timesheet_End_Date'],
                'status': "Error",
            },
        )
        create_submit_batch >> batch_enter >> batch_exit >> get_batch_result >> has_batch_error
        has_batch_error >> rail.Label(
            'Yes') >> log_batch_error >> create_forced_approve_batch
        has_batch_error >> rail.Label('No') >> create_forced_approve_batch
        create_forced_approve_batch >> execute_timesheet_approvalbatch2 >> add_log_entry >> has_batch2_error
        has_batch2_error >> rail.Label(
            'Yes') >> log_batch2_error >> catch_and_log_errors

    return dag


rail.for_each_instance(create_dag)
