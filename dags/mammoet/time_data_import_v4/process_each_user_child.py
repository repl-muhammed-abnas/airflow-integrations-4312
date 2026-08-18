from datetime import timedelta
import rail
from airflow.models import Variable
from mammoet.time_data_import_v4.utils import custom_methods, request_payload, response_filter

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_each_user_dagid,
        description=f'mammoet Time Entry Sync process users child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_max_active_run,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run_conf",
                                    extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=Variable.get(config.can_run_batch_task,
                              'true').lower() == 'true',
            yes_task="batch_task",
            no_task="create_log"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="create_log",
            end_task="log_timesheet_reopened"
        )

        create_log = rail.CreateLogOperator(
            task_id="create_log"
        )

        create_timesheet_reopened_log = rail.CreateLogOperator(
            task_id="create_timesheet_reopened_log"
        )

        create_timesheet_log_for_recalc = rail.CreateLogOperator(
            task_id="create_timesheet_log_for_recalc"
        )

        get_all_records_for_user = rail.QueryCollectionOperator(
            task_id="get_all_records_for_user",
            query="""SELECT * FROM final_data fd WHERE fd.employeenumber =:EMP_ID""",
            query_params={
                "EMP_ID": "{{ dag_run.conf.employeenumber }}"
            }
        )

        get_required_details = rail.PythonOperator(
            task_id="get_required_details",
            python_callable=custom_methods.get_required_details,
            op_args=[get_all_records_for_user.task_id]
        )

        get_user_timesheet_details = rail.RepliconServiceOperator(
            task_id="get_user_timesheet_details",
            endpoint="/services/TimesheetListService1.svc/GetData",
            data=request_payload.get_all_timesheet_for_user,
            data_handler=response_filter.get_timesheet_details
        )

        map_timesheet_with_user_data = rail.PythonOperator(
            task_id="map_timesheet_with_user_data",
            python_callable=custom_methods.map_timesheet_with_user_data,
            op_args=[get_all_records_for_user.task_id,
                     get_user_timesheet_details.task_id],
            show_return_value_in_logs=False
        )

        reopen_timesheets = rail.RepliconServiceCallForEachItemOperator(
            task_id = "reopen_timesheets",
            items="{{result('map_timesheet_with_user_data', 'timesheet_to_reopen') | to_json}}",
            endpoint= "/services/TimesheetApprovalService1.svc/Reopen",
            data={
                "timesheetUri": "{{ item.ts_uri }}",
                "unitOfWorkId": "{{ item.unit_of_work_id }}",
                "comments": "Timesheet is reopened by Integration (Time Data Import)"
            }
        )

        log_timesheet_reopened = rail.WriteLogOperator(
            task_id="log_timesheet_reopened",
            log="{{ result('create_timesheet_reopened_log') }}",
            items="{{result('map_timesheet_with_user_data', 'timesheet_to_reopen') | to_json}}",
            message="TS is reopened",
            severity=lambda item:"approved" if item['timesheet_status_uri'].endswith('approved') else "waiting",
            properties=lambda item: {
                "ts_uri": item['ts_uri'],
                "timesheet_status_uri": item['timesheet_status_uri'],
                "timesheet_status": item['timesheet_status'],
                "user_login_name": item['user_login_name'],
                "user_uri": item["user_uri"]
            }
        )

        process_users_timesheet = rail.trigger_parallel_dagrun(
            task_id="process_users_timesheet",
            items=lambda: rail.load_json_artifact(
                rail.result('map_timesheet_with_user_data')),
            trigger_dag_id= config.process_each_timeentry_dagid,
            parallel_count= config.parallel_count,
            conf=lambda item, dag_run: {
                **dag_run.conf,
                **item,
                **{
                    "log": rail.result("create_log"),
                    "timesheet_reopened_log": rail.result("create_timesheet_reopened_log"),
                    "recalc_log": rail.result('create_timesheet_log_for_recalc')
                }
            },
            execution_timeout=timedelta(hours=5)
        )

        can_run_batch_task_2 = rail.IfOperator(
            task_id="can_run_batch_task_2",
            test=Variable.get(config.can_run_batch_task,
                              'true').lower() == 'true',
            yes_task="batch_task2",
            no_task="gather_timesheets_to_approve"
        )

        batch_task2 = rail.BatchTaskRunOperator(
            task_id="batch_task2",
            start_task="gather_timesheets_to_approve",
            end_task="start_batch_execution"
        )

        gather_timesheets_to_submit = rail.FilterLogEntriesOperator(
            task_id="gather_timesheets_to_submit",
            log="{{ result('create_timesheet_reopened_log') }}",
            severity="waiting"
        )

        gather_timesheets_to_approve = rail.FilterLogEntriesOperator(
            task_id="gather_timesheets_to_approve",
            log="{{ result('create_timesheet_reopened_log') }}",
            severity="approved"
        )

        has_any_timesheet_to_approve = rail.IfOperator(
            task_id="has_any_timesheet_to_approve",
            test="{{ result('gather_timesheets_to_approve', 'length') > 0 }}",
            yes_task="create_force_approval_batch",
            no_task="gather_timesheets_to_submit"
        )

        def get_create_force_approval_batch():
            ts_uris = list(map(lambda ts_entry: ts_entry['properties']['ts_uri'], rail.load_all_records(rail.result(
                'gather_timesheets_to_approve'))))
            return {
                "timesheetUris": ts_uris,
                "comments": "Forced Approved by Integration (Time Data Import)"
            }

        create_force_approval_batch = rail.RepliconServiceOperator(
            task_id="create_force_approval_batch",
            endpoint="/services/TimesheetApprovalService1.svc/CreateForcedApproveBatch",
            data=get_create_force_approval_batch
        )

        execute_approval_batch, wait_approval_batch = rail.batch_execution(
            group_id="approval_batch_execution",
            creation_task_id=create_force_approval_batch.task_id,
            replicon_conn_id=config.replicon_conn_id,
        )

        has_any_timesheet_to_submit = rail.IfOperator(
            task_id="has_any_timesheet_to_submit",
            test="{{ result('gather_timesheets_to_submit', 'length') > 0 }}",
            yes_task="create_submit_batch",
            no_task="start_batch_execution"
        )

        def get_create_submit_batch():
            ts_uris = list(map(lambda ts_entry: ts_entry['properties']['ts_uri'], rail.load_all_records(rail.result(
                'gather_timesheets_to_submit'))))
            return {
                "timesheetUris": ts_uris,
                "comments": "Submitted by Integration (Time Data Import)"
            }

        create_submit_batch = rail.RepliconServiceOperator(
            task_id="create_submit_batch",
            endpoint="/services/TimesheetApprovalService1.svc/CreateSubmitBatch2",
            data=get_create_submit_batch
        )

        execute_submit_batch, wait_submit_batch = rail.batch_execution(
            group_id="submit_batch_execution",
            creation_task_id=create_submit_batch.task_id,
            replicon_conn_id=config.replicon_conn_id,
        )

        start_batch_execution = rail.EmptyOperator(
            task_id="start_batch_execution"
        )

        has_approval_batch = rail.IfOperator(
            task_id="has_approval_batch",
            test="{{ result('create_force_approval_batch') | is_truthy}}",
            yes_task=execute_approval_batch.task_id,
            no_task="create_recalc_batch"
        )

        has_submit_batch = rail.IfOperator(
            task_id="has_submit_batch",
            test="{{ result('create_submit_batch') | is_truthy}}",
            yes_task=execute_submit_batch.task_id,
            no_task="create_recalc_batch"
        )

        def get_recalc_batch_payload():
            return {
                "timesheets": [ts['properties']['ts_uri'] for ts in rail.load_all_records(rail.result(
                    'create_timesheet_log_for_recalc')) if ts['properties']['ts_uri'] is not None]
            }

        create_recalc_batch = rail.RepliconServiceOperator(
            task_id = "create_recalc_batch",
            endpoint="/services/TimesheetService1.svc/CreateRecalculateScriptDataBatch2",
            data=get_recalc_batch_payload
        )

        execute_recalc_batch, wait_recalc_batch = rail.batch_execution(
            group_id="recalc_batch_execution",
            creation_task_id=create_recalc_batch.task_id,
            replicon_conn_id=config.replicon_conn_id,
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            log="{{result('create_log')}}",
            trigger_rule="one_failed",
            items=lambda: rail.load_json_artifact(
                rail.result('map_timesheet_with_user_data')),
            message="{{ get_error_message() }}",
            severity="Error",
            properties={
                "counter_id": "{{item['counter']}}",
                "employee_id": "{{item['employeenumber']}}",
                'entrydate': "{{ item.workdate }}",
                "status": "Error",
                "action": "Sync",
                "details": "{{ get_error_message() }}"
            }
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> log_timesheet_reopened
        can_run_batch_task >> rail.Label("No") >> create_log
        create_log >> create_timesheet_reopened_log >> create_timesheet_log_for_recalc >> get_all_records_for_user \
            >> get_required_details >> get_user_timesheet_details >> map_timesheet_with_user_data
        map_timesheet_with_user_data >> reopen_timesheets >> log_timesheet_reopened >> process_users_timesheet >> can_run_batch_task_2 >> rail.Label(
            "No") >> gather_timesheets_to_approve
        can_run_batch_task_2 >> rail.Label(
            "Yes") >> batch_task2 >> start_batch_execution

        gather_timesheets_to_approve >> has_any_timesheet_to_approve >> rail.Label(
            "No") >> gather_timesheets_to_submit
        has_any_timesheet_to_approve >> rail.Label(
            "Yes") >> create_force_approval_batch >> gather_timesheets_to_submit
        gather_timesheets_to_submit >> has_any_timesheet_to_submit >> rail.Label(
            "No") >> start_batch_execution
        has_any_timesheet_to_submit >> rail.Label(
            "Yes") >> create_submit_batch >> start_batch_execution

        start_batch_execution >> [has_approval_batch, has_submit_batch]

        has_submit_batch >> rail.Label("Yes") >> execute_submit_batch
        wait_submit_batch >> create_recalc_batch

        has_submit_batch >> rail.Label("No") >> create_recalc_batch
        has_approval_batch >> rail.Label("Yes") >> execute_approval_batch
        wait_approval_batch >> create_recalc_batch

        has_approval_batch >> rail.Label("No") >> create_recalc_batch >> execute_recalc_batch
        wait_recalc_batch >> rail.Label("On Error")>> catch_and_log_error

    return dag


rail.for_each_instance(create_child_dag)
