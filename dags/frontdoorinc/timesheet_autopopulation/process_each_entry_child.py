import time
import rail
from frontdoorinc.timesheet_autopopulation.utils import request_payload

null = None


def create_child_dag_process(config):
    with rail.create_airflow_dag(
        dag_id=f'frontdoorinc_process_timesheets_autopopulation_child_{config.instance}',
        description=f'FrontdoorInc Process Timesheets Auto Population Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_child_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        populate_timesheets = rail.RepliconServiceOperator(
            task_id='populate_timesheets',
            endpoint='/services/TimesheetPopulationService1.svc/ExecuteTimesheetPopulationScript',
            data=request_payload.get_repopulate_timesheet_payload
        )

        wait_for_30_sec = rail.PythonOperator(
            task_id='wait_for_30_sec',
            python_callable=lambda: time.sleep(30)
        )

        enqueue_recalculate_script_data = rail.RepliconServiceOperator(
            task_id='enqueue_recalculate_script_data',
            endpoint='/services/TimesheetService1.svc/EnqueueRecalculateScriptData',
            data=request_payload.get_enqueue_recalculate_script_payload
        )

        log_successfull = rail.WriteLogOperator(
            task_id='log_successfull',
            message="Timesheet auto populated successfully",
            severity="Success",
            properties=lambda dag_run: {
                "timesheet_period": dag_run.conf["timesheet_period"],
                "username": dag_run.conf["username"],
                "status": "Success",
                "details": "Timesheet auto populated successfully"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity="Error",
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                "timesheet_period": dag_run.conf["timesheet_period"],
                "username": dag_run.conf["username"],
                "status": "Error",
                "details": '{{ get_error_message() }}'
            }
        )

        populate_timesheets >> wait_for_30_sec >> enqueue_recalculate_script_data >> log_successfull >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag_process)
