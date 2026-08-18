import rail

# config : https://github.com/replicon/airflow-integrations/blob/main/dags/pwcglobal/user_import/config.py


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'pwcglobal_timesheet_auto_submission_zero_hours_v4_child_{config.instance}',
        description=f'PwCGlobal - Timesheet auto submission zero hours LOA Auto_submission_Child_v4.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        create_forced_approve_batch = rail.RepliconServiceOperator(
            task_id="create_forced_approve_batch",
            endpoint="/services/TimesheetApprovalService1.svc/CreateForcedApproveBatch",
            data=lambda: {
                "timesheetUris": list(map(lambda x: x['timesheeturi'], rail.get_current_context()['dag_run'].conf['items'])),
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

        has_batch_error = rail.IfOperator(
            task_id='has_batch_error',
            test="{{  result('execute_timesheet_approvalbatch2').errors | length > 0}}",
            yes_task='log_batch_error'
        )

        log_batch_error = rail.WriteLogOperator(
            task_id="log_batch_error",
            severity="Error",
            items="{{ result('execute_timesheet_approvalbatch2').errors | to_json }}",
            properties=lambda item: {
                'timesheeturi': item['timesheet']['uri'],
                'User_Name': rail.find_first_by_attr_and_get_attr(rail.get_current_context()['dag_run'].conf['items'],
                                                                  'timesheeturi', item['timesheet']['uri'], 'User_Name'),
                'timesheetperiod': rail.find_first_by_attr_and_get_attr(rail.get_current_context()['dag_run'].conf['items'],
                                                                        'timesheeturi', item['timesheet']['uri'], 'Timesheet_Start_Date')
                + " - " +
                rail.find_first_by_attr_and_get_attr(rail.get_current_context()['dag_run'].conf['items'], 'timesheeturi',
                                                     item['timesheet']['uri'], 'Timesheet_End_Date'),
                'status': "Exception" if item['notifications'] and item['notifications'][0] and
                item['notifications'][0]['displayText'] == "Timesheet has already been approved." else "Error",
            },
            message="{{ item.notifications[0].displayText}}",
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

        create_forced_approve_batch >> execute_timesheet_approvalbatch2 >> add_log_entry >> has_batch_error >> \
            rail.Label('Yes') >> log_batch_error >> catch_and_log_errors

    return dag


rail.for_each_instance(create_dag)
