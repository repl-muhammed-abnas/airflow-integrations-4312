from airflow.exceptions import AirflowFailException
import rail
from datetime import datetime, timedelta
from pendulum import now
from sqlalchemy import desc
from airflow.models import DagRun, TaskInstance
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.timezone import utcnow

null = None

EXPORT_DATE_FORMAT = "%Y-%m-%d"
EXPORT_FILE_TIMESTAMP = "%Y%m%d%H%M%S"
EXPORT_TIME_FORMAT = '%H:%M:%S'
LOGGING_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"
MAINTENANCE_WINDOW_TIME_FORMAT = "%H:%M"


def is_within_maintenance_window(maintenance_window_mapper, default_timezone):
    """Return True if the current time falls inside a configured maintenance
    window (exports suspended); False when the mapper is empty or the current
    time is outside every window. The mapper is the parsed maintenance-window
    Variable, e.g.:

        {
          "timezone": "US/Eastern",
          "day_of_week": [
            {"saturday": {"start_time": "00:00", "end_time": "06:00"}},
            {"sunday":   {"start_time": "21:00", "end_time": "23:59"}},
            {"monday":   {"start_time": "00:00", "end_time": "04:00"}}
          ]
        }

    `timezone` drives the comparison, falling back to `default_timezone` when absent.
    """
    if not isinstance(maintenance_window_mapper, dict):
        return False

    current = now(tz=maintenance_window_mapper.get('timezone', default_timezone))
    current_day = current.strftime("%A").lower()
    current_time = current.time().replace(second=0, microsecond=0)

    for entry in maintenance_window_mapper.get('day_of_week', []):
        window = next((w for day, w in entry.items() if day.lower() == current_day), None)
        if not window:
            continue
        start_time = datetime.strptime(window['start_time'], MAINTENANCE_WINDOW_TIME_FORMAT).time()
        end_time = datetime.strptime(window['end_time'], MAINTENANCE_WINDOW_TIME_FORMAT).time()
        if start_time <= current_time <= end_time:
            return True

    return False


@provide_session
def check_previous_master_dag_runs(config, session=NEW_SESSION):
    """
    Check if the wait_for_dag_runs task has FAILED in any failed master DAG run in the last 7 days.
    - Returns True (can proceed) if no FAILED state found (only SUCCESS/SKIPPED or no history)
    - Returns False (block) if any FAILED state found (means child DAG(s) failed)

    This design handles:
    1. Child DAG failures → wait_for_dag_runs FAILED → Block subsequent runs
    2. Export creation timeouts → wait_for_dag_runs SKIPPED → Allow subsequent runs (for delta processing)
    """

    # Calculate 7 days ago from now
    lookback_date = now() - timedelta(days=config.lookback_days_for_failed_master_dags)

    # Get all failed master DAG runs in the last N days
    recent_failed_dag_runs = (
        session.query(DagRun)
        .filter(
            DagRun.dag_id == config.master_dag_id,
            DagRun.state.in_([DagRunState.FAILED]),
            DagRun.execution_date >= lookback_date
        )
        .order_by(desc(DagRun.execution_date))
        .all()
    )

    if not recent_failed_dag_runs:
        return {
            'check_results': f'No previous run history found in last {config.lookback_days_for_failed_master_dags} days. Allowing execution.',
            'can_process_further': True
        }

    # Check wait_for_dag_runs task state in each of these runs
    failed_runs_with_child_failure = []

    for dag_run in recent_failed_dag_runs:
        task_instance = (
            session.query(TaskInstance)
            .filter(
                TaskInstance.dag_id == config.master_dag_id,
                TaskInstance.run_id == dag_run.run_id,
                TaskInstance.task_id == 'wait_for_dag_runs'
            )
            .first()
        )

        if task_instance:

            # If the task FAILED, it means child DAGs failed
            if task_instance.state == TaskInstanceState.FAILED:
                failed_runs_with_child_failure.append({
                    'run_id': dag_run.run_id,
                    'execution_date': dag_run.execution_date,
                    'state': task_instance.state
                })

    # If any FAILED state found, block execution
    if failed_runs_with_child_failure:
        failure_details = "; ".join([
            f"Run {r['run_id']} ({r['execution_date']}): {r['state']}"
            for r in failed_runs_with_child_failure
        ])

        return {
            'check_results': f'Found {len(failed_runs_with_child_failure)} child DAG failure(s) in last {config.lookback_days_for_failed_master_dags} days.\
                Details: {failure_details}. Blocking execution until child DAG issues are resolved.',
            'can_process_further': False
        }
    else:
        return {
            'check_results': f'Checked {len(recent_failed_dag_runs)} failed master DAG runs in last {config.lookback_days_for_failed_master_dags} days. No child DAG failures found. Proceeding with execution.',
            'can_process_further': True
        }


def retrieve_export_uri(response):
    if response['error'] is not None:
        raise AirflowFailException('Export failed - ' + response)
    return response['timeDataExportUri']


def get_timeexport_fileformat(config, response):
    file_format = rail.find_first_by_attr_and_get_attr(
        response, 'displayText', config.time_export_file_format, 'uri')
    if file_format:
        return file_format
    raise Exception(
        f'Unable to locate script `{config.time_export_file_format}`')
