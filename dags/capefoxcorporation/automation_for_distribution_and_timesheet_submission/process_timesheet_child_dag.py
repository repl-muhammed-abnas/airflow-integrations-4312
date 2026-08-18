import time
import rail
from capefoxcorporation.automation_for_distribution_and_timesheet_submission.utils import request_payload

null = None


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.child_process_timesheets_dag_id,
        description=f'CapeFoxCorporation Automation For Distribution and Timesheet Submission - Process timesheet auto-populate Child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
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

        log_successful = rail.WriteLogOperator(
            task_id='log_successful',
            log='{{dag_run.conf.main_log}}',
            message="Timesheet auto populated successfully",
            severity="Success",
            properties=lambda dag_run: {
                "username": dag_run.conf["username"],
                "employee_id": dag_run.conf["employee_id"],
                "timesheet_period": dag_run.conf["timesheet_period"],
                "timesheet_uri": dag_run.conf["timesheet_uri"],
                "status": "Success",
                "details": "Timesheet auto populated successfully",
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log='{{dag_run.conf.main_log}}',
            severity="Error",
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                "username": dag_run.conf["username"],
                "employee_id": dag_run.conf["employee_id"],
                "timesheet_period": dag_run.conf["timesheet_period"],
                "timesheet_uri": dag_run.conf["timesheet_uri"],
                "status": "Error",
                "details": '{{ get_error_message() }}',
            }
        )

        populate_timesheets >> wait_for_30_sec >> enqueue_recalculate_script_data >> log_successful >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
