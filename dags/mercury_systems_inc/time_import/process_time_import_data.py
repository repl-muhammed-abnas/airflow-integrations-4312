from datetime import timedelta
import rail
from airflow.models import Variable
from mercury_systems_inc.time_import.utils import request_payload, response_filters

null = None
SQL_DATEFORMAT = "%Y-%m-%d"

def create_process_time_data_dag(config):
    """Factory function to create tasks for processing each time entry dag_run.con"""
    with rail.create_airflow_dag(
        dag_id=config.process_time_data_dag_id,
        description=f'Mercury Systems Time Import {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.process_child_max_active_run
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dag_run_con"
        )

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.batch_task_variable, default_var="true") == "true",
            yes_task="batch_task",
            no_task="create_time_entry_log"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="create_time_entry_log",
            end_task="catch_and_log_error",
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_time_entry_log = rail.CreateLogOperator(
            task_id="create_time_entry_log"
        )

        query_user_time_entry_records = rail.QueryCollectionOperator(
            task_id="query_user_time_entry_records",
            query="""SELECT * FROM final_valid_records where
              user_uri='{{dag_run.conf.user_uri}}' and entry_date='{{dag_run.conf.entry_date}}' """
        )

        get_or_create_timesheet = rail.RepliconServiceOperator(
            task_id="get_or_create_timesheet",
            endpoint="/services/TimesheetService1.svc/GetTimesheetDetailsForDate",
            data=lambda dag_run: request_payload.get_timesheet_for_date(
                user_uri=dag_run.conf['user_uri'],
                entry_date=rail.parse_date(
                    dag_run.conf['entry_date'], SQL_DATEFORMAT)
            ),
            data_handler=response_filters.filter_timesheet_details
        )

        is_time_sheet_open = rail.IfOperator(
            task_id="is_time_sheet_open",
            test=lambda: bool(rail.result("get_or_create_timesheet").get(
                "status").split(":")[-1] == "open"),
            yes_task="get_time_entry_details",
            no_task="log_timesheet_not_open"
        )

        log_timesheet_not_open = rail.WriteLogOperator(
            task_id="log_timesheet_not_open",
            log='{{result("create_time_entry_log")}}',
            items=lambda: rail.load_all_records(
                rail.result("query_user_time_entry_records")),
            message=lambda: "Time entry not processed as the timesheet is not open.",
            severity="Exception",
            properties=lambda item: {
                "employee_id": item.get('employee_id', ''),
                "entry_date": item.get('entry_date', ''),
                "project_code": item.get('project_code', ''),
                "task_code": item.get('task_code', ''),
                "hours": item.get('total_hours', ''),
                "status": "Exception",
                "action": "Add",
                "details": "Time entry not processed as the timesheet is not open."
            }
        )

        get_time_entry_details = rail.RepliconServiceOperator(
            task_id="get_time_entry_details",
            endpoint="/services/TimeEntryRevisionGroupService1.svc/GetTimeEntryRevisionGroupsForUserAndDateRange",
            data=lambda dag_run: request_payload.get_time_entries_for_user_date_range(
                user_uri=dag_run.conf['user_uri'],
                entry_date=rail.parse_date(
                    dag_run.conf['entry_date'], SQL_DATEFORMAT)
            ),
            data_handler=response_filters.filter_time_entries
        )

        is_time_entry_present = rail.IfOperator(
            task_id="process_time_entry",
            test=lambda: bool(rail.result('get_time_entry_details')),
            yes_task="update_time_entry",
            no_task="add_time_entry"
        )

        # Update existing time entry
        update_time_entry = rail.RepliconServiceCallForEachItemOperator(
            task_id="update_time_entry",
            endpoint="/services/TimeEntryRevisionGroupService1.svc/PutTimeEntryRevisionGroup",
            items=lambda: rail.result('get_time_entry_details'),
            data=lambda item: request_payload.put_time_entry_payload(item)
        )

        # Add new time entry
        add_time_entry = rail.RepliconServiceCallForEachItemOperator(
            task_id="add_time_entry",
            items=lambda: rail.load_all_records(
                rail.result("query_user_time_entry_records")),
            endpoint="/services/TimeEntryRevisionGroupService1.svc/PutTimeEntryRevisionGroup",
            data=lambda item: request_payload.put_time_entry_payload(item)
        )

        # Log success
        log_success = rail.WriteLogOperator(
            task_id="log_success",
            message=lambda: "Time entry Added successfully",
            log='{{result("create_time_entry_log")}}',
            items=lambda: rail.load_all_records(
                rail.result("query_user_time_entry_records")),
            severity="Success",
            properties=lambda item: {
                "employee_id": item.get('employee_id', ''),
                "entry_date": item.get('entry_date', ''),
                "project_code": item.get('project_code', ''),
                "task_code": item.get('task_code', ''),
                "hours": item.get('total_hours', ''),
                "status": "Success",
                "action": f"{'Update' if rail.result('get_time_entry_details') else 'Add'}",
                "details": "Time entry added successfully"
            }
        )

        # Handle errors
        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            trigger_rule="one_failed",
            message="{{ get_error_message() }}",
            log='{{result("create_time_entry_log")}}',
            items=lambda: rail.load_all_records(
                rail.result("query_user_time_entry_records")),
            severity="Error",
            properties=lambda item: {
                "employee_id": item.get('employee_id', ''),
                "entry_date": item.get('entry_date', ''),
                "project_code": item.get('project_code', ''),
                "task_code": item.get('task_code', ''),
                "hours": item.get('total_hours', ''),
                "status": "Error",
                "action": f"{'Update' if rail.result('get_time_entry_details') else 'Add'}",
                "details": rail.render_template('{{get_error_message()}}')
            }
        )

        # Define workflow for this dag_run.conf
        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >>\
            create_time_entry_log >> query_user_time_entry_records >> get_or_create_timesheet >>\
            is_time_sheet_open >> rail.Label("No") >> log_timesheet_not_open
        is_time_sheet_open >> rail.Label("Yes") >>\
            get_time_entry_details >> is_time_entry_present >> rail.Label("Yes") >>\
            update_time_entry >> log_success
        is_time_entry_present >> rail.Label("No") >>\
            add_time_entry >> log_success >> catch_and_log_error

        # Return the final tasks in the workflow
    return dag


rail.for_each_instance(create_process_time_data_dag)
