"""
JIRA Time Sync Integration - Replicon Polaris Child DAG
========================================================

Processes one JIRA worklog and creates/updates/deletes the matching
Replicon Polaris time entry. Project/task come from either a hardcoded
config override or the colon-separated JIRA path field; task resolution
uses a single TaskListService1.svc/GetData full-path match (any depth,
no level-by-level calls). See FLOW_DOCUMENTATION.md for the full diagram.

Trigger: TriggerDagRunOperator from the Master DAG.
"""

from datetime import timedelta
import rail
from airflow.models import Variable

from jira_time_sync_cp_polaris.utils.payload_builders import (
    build_replicon_user_search_payload,
    build_replicon_activity_search_payload,
    build_replicon_timesheet_lookup_payload,
    build_replicon_project_by_name_payload,
    build_replicon_task_search_payload,
    build_replicon_recalculate_payload,
    build_replicon_time_entry_payload,
    build_replicon_search_entry_by_oef_payload,
    build_replicon_delete_entry_payload,
    build_jira_comment,
)
from jira_time_sync_cp_polaris.utils.response_handlers import (
    extract_replicon_user,
    extract_replicon_activity,
    extract_replicon_timesheet,
    extract_replicon_project,
    extract_task_by_full_path,
    extract_existing_time_entry,
)
from jira_time_sync_cp_polaris.utils.transformers import parse_jira_path_field


def create_replicon_child_dag(config):
    """
    Create the Replicon Polaris child DAG for processing time entries.

    Task resolution uses TaskListService1.svc/GetData with the full-path column.
    A single API call returns all tasks for the project with their full hierarchical
    path (cellCollection), enabling dynamic depth matching for any task hierarchy.

    Args:
        config: Instance-specific configuration object
    """
    with rail.create_airflow_dag(
        dag_id=config.replicon_child_dag_id,
        description=f"JIRA to Replicon Time Sync Child - {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.replicon_child_max_active_runs,
        default_args={
            'retries': config.max_retries,
            'retry_delay': timedelta(seconds=config.retry_delay_seconds),
            'jira_conn_id': config.jira_conn_id,
        }
    ) as dag:

        # Standalone debugging aid — no edges, not part of the chain.
        view_config = rail.ViewDagRunConfOperator(
            task_id='view_config'
        )

        parse_path = rail.PythonOperator(
            task_id='parse_path',
            python_callable=lambda dag_run: resolve_project_and_task(dag_run, config)
        )

        # Runs check_needs_activity_lookup..catch_and_log_errors as one Airflow
        # task, avoiding per-task scheduler overhead across this long chain.
        # Toggle via Airflow Variable to fall back to normal task-by-task execution.
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='check_needs_activity_lookup'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='check_needs_activity_lookup',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        check_needs_activity_lookup = rail.IfOperator(
            task_id='check_needs_activity_lookup',
            test=lambda: bool(config.rep_hardcoded_activity_name),
            yes_task='lookup_activity',
            no_task='parse_path'
        )

        lookup_activity = rail.RepliconServiceOperator(
            task_id='lookup_activity',
            endpoint="services/ActivityListService1.svc/GetData",
            data=lambda: build_replicon_activity_search_payload(config.rep_hardcoded_activity_name),
            data_handler=lambda response: extract_replicon_activity(response, config.rep_hardcoded_activity_name)
        )

        lookup_user = rail.RepliconServiceOperator(
            task_id='lookup_user',
            endpoint="services/UserListService1.svc/GetData",
            data=lambda dag_run: build_replicon_user_search_payload(
                email=dag_run.conf['email']
            ),
            data_handler=lambda response, dag_run: extract_replicon_user(response, dag_run.conf['email'])
        )

        check_user_found = rail.IfOperator(
            task_id='check_user_found',
            test=lambda: rail.result('lookup_user') is not None,
            yes_task='check_user_enabled',
            no_task='comment_user_not_found'
        )

        check_user_enabled = rail.IfOperator(
            task_id='check_user_enabled',
            test=lambda: validate_user_status(rail.result('lookup_user'), config),
            yes_task='check_is_delete',
            no_task='comment_user_disabled'
        )

        check_is_delete = rail.IfOperator(
            task_id='check_is_delete',
            test=lambda dag_run: dag_run.conf.get('webhook_event') == 'worklog_deleted',
            yes_task='search_entry_for_delete',
            no_task='search_existing_entry'
        )

        # Delete path: find and remove the existing time entry.
        search_entry_for_delete = rail.RepliconServiceOperator(
            task_id='search_entry_for_delete',
            endpoint="services/TimeEntryRevisionGroupListService1.svc/GetData",
            data=lambda dag_run: build_replicon_search_entry_by_oef_payload(
                dag_run.conf['worklog_id'], dag_run.conf['oef_column_uri'], dag_run.conf['oef_filter_uri']
            ),
            data_handler=lambda response: extract_existing_time_entry(response)
        )

        check_delete_entry_found = rail.IfOperator(
            task_id='check_delete_entry_found',
            test=lambda: rail.result('search_entry_for_delete') is not None and bool(rail.result('search_entry_for_delete').get('entry_uri')),
            yes_task='get_timesheet_for_delete',
            no_task='comment_delete_not_found'
        )

        get_timesheet_for_delete = rail.RepliconServiceOperator(
            task_id='get_timesheet_for_delete',
            endpoint="services/TimesheetService1.svc/GetTimesheetDetailsForDate",
            data=lambda dag_run: build_replicon_timesheet_lookup_payload(
                rail.result('lookup_user')['uri'],
                dag_run.conf['date_string'] or normalize_date(
                    (rail.result('search_entry_for_delete') or {}).get('entry_date', '')
                )
            ),
            data_handler=lambda response: extract_replicon_timesheet(response)
        )

        check_delete_ts_open = rail.IfOperator(
            task_id='check_delete_ts_open',
            test=lambda: rail.result('get_timesheet_for_delete') is not None and rail.result('get_timesheet_for_delete').get('status') == 'open',
            yes_task='delete_worklog_entry',
            no_task='comment_delete_ts_not_open'
        )

        delete_worklog_entry = rail.RepliconServiceOperator(
            task_id='delete_worklog_entry',
            endpoint="services/TimeEntryRevisionGroupService1.svc/DeleteTimeEntryRevisionGroup",
            data=lambda: build_replicon_delete_entry_payload(rail.result('search_entry_for_delete')['entry_uri'])
        )

        recalculate_after_delete = rail.RepliconServiceOperator(
            task_id='recalculate_after_delete',
            endpoint="services/TimesheetService1.svc/EnqueueRecalculateScriptData",
            data=lambda: build_replicon_recalculate_payload(rail.result('get_timesheet_for_delete')['uri'])
        )

        comment_delete_success = rail.JiraAPIOperator(
            task_id='comment_delete_success',
            jira_conn_id=config.jira_conn_id,
            request_method='POST',
            endpoint="/rest/api/3/issue/{{ dag_run.conf.issue_key }}/comment",
            request_body=lambda dag_run: build_jira_comment(
                heading='Time Entry Deleted in Polaris',
                date=dag_run.conf['date_string'],
                hours=dag_run.conf['hours_decimal'],
                user=dag_run.conf['email'],
                project_path=get_project_path(),
                info='Time entry has been successfully removed from Polaris.',
                success=True,
                account_id=dag_run.conf.get('account_id')
            )
        )

        comment_delete_ts_not_open = rail.JiraAPIOperator(
            task_id='comment_delete_ts_not_open',
            jira_conn_id=config.jira_conn_id,
            request_method='POST',
            endpoint="/rest/api/3/issue/{{ dag_run.conf.issue_key }}/comment",
            request_body=lambda dag_run: build_jira_comment(
                heading='Time Entry Sync Failed in Polaris',
                date=dag_run.conf['date_string'],
                hours=dag_run.conf['hours_decimal'],
                user=dag_run.conf['email'],
                project_path=get_project_path(),
                info=f"Cannot remove the time entry — timesheet for {dag_run.conf['date_string']} is not open "
                     f"(Status: {(rail.result('get_timesheet_for_delete') or {}).get('status', 'unknown').replace('_', ' ').title()}). "
                     f"Please consult with your reporting lead/manager.",
                success=False,
                account_id=dag_run.conf.get('account_id')
            )
        )

        comment_delete_not_found = rail.JiraAPIOperator(
            task_id='comment_delete_not_found',
            jira_conn_id=config.jira_conn_id,
            request_method='POST',
            endpoint="/rest/api/3/issue/{{ dag_run.conf.issue_key }}/comment",
            request_body=lambda dag_run: build_jira_comment(
                heading='Time Entry Sync Failed in Polaris',
                date=dag_run.conf['date_string'],
                hours=dag_run.conf['hours_decimal'],
                user=dag_run.conf['email'],
                project_path=get_project_path(),
                info='No matching time entry was found for this worklog. It may have already been deleted.',
                success=False,
                account_id=dag_run.conf.get('account_id')
            )
        )

        # Create/update path: always search for an existing entry by OEF
        # (worklog_id), regardless of the reported webhook_event. This keeps
        # the sync idempotent on worklog_id — a retried/duplicate "Created"
        # delivery updates the existing line instead of creating a duplicate.
        search_existing_entry = rail.RepliconServiceOperator(
            task_id='search_existing_entry',
            endpoint="services/TimeEntryRevisionGroupListService1.svc/GetData",
            data=lambda dag_run: build_replicon_search_entry_by_oef_payload(
                dag_run.conf['worklog_id'], dag_run.conf['oef_column_uri'], dag_run.conf['oef_filter_uri']
            ),
            data_handler=lambda response: extract_existing_time_entry(response)
        )

        check_date_changed = rail.IfOperator(
            task_id='check_date_changed',
            test=lambda dag_run: is_date_changed(dag_run),
            yes_task='delete_existing_entry',
            no_task='get_timesheet_for_date'
        )

        delete_existing_entry = rail.RepliconServiceOperator(
            task_id='delete_existing_entry',
            endpoint="services/TimeEntryRevisionGroupService1.svc/DeleteTimeEntryRevisionGroup",
            data=lambda: build_replicon_delete_entry_payload(rail.result('search_existing_entry')['entry_uri'])
        )

        get_timesheet = rail.RepliconServiceOperator(
            task_id='get_timesheet_for_date',
            endpoint="services/TimesheetService1.svc/GetTimesheetDetailsForDate",
            data=lambda dag_run: build_replicon_timesheet_lookup_payload(
                rail.result('lookup_user')['uri'], dag_run.conf['date_string']
            ),
            data_handler=lambda response: extract_replicon_timesheet(response)
        )

        check_timesheet_open = rail.IfOperator(
            task_id='check_timesheet_open',
            test=lambda: rail.result('get_timesheet_for_date') is not None and rail.result('get_timesheet_for_date').get('status') == 'open',
            yes_task='lookup_project',
            no_task='comment_timesheet_not_open'
        )

        comment_timesheet_not_open = rail.JiraAPIOperator(
            task_id='comment_timesheet_not_open',
            jira_conn_id=config.jira_conn_id,
            request_method='POST',
            endpoint="/rest/api/3/issue/{{ dag_run.conf.issue_key }}/comment",
            request_body=lambda dag_run: build_jira_comment(
                heading='Time Entry Sync Failed in Polaris',
                date=dag_run.conf['date_string'],
                hours=dag_run.conf['hours_decimal'],
                user=dag_run.conf['email'],
                project_path=get_project_path(),
                info=f"Timesheet for {dag_run.conf['date_string']} is not open "
                     f"(Status: {(rail.result('get_timesheet_for_date') or {}).get('status', 'unknown').replace('_', ' ').title()}). "
                     f"Please consult with your reporting lead/manager.",
                success=False,
                account_id=dag_run.conf.get('account_id')
            )
        )

        lookup_project = rail.RepliconServiceOperator(
            task_id='lookup_project',
            endpoint="services/ProjectService1.svc/BulkGetProjectDetails3",
            data=lambda: build_replicon_project_by_name_payload(rail.result('parse_path')['project_name']),
            data_handler=lambda response: extract_replicon_project(response)
        )

        check_project_valid = rail.IfOperator(
            task_id='check_project_valid',
            test=lambda: validate_project_status(rail.result('lookup_project'), config),
            yes_task='lookup_leaf_task',
            no_task='comment_project_error'
        )

        lookup_leaf_task = rail.RepliconServiceOperator(
            task_id='lookup_leaf_task',
            endpoint="services/TaskListService1.svc/GetData",
            data=lambda: build_replicon_task_search_payload(rail.result('lookup_project')['uri']),
            data_handler=lambda response: extract_task_by_full_path(response, rail.result('parse_path')['task_path'])
        )

        check_task_found = rail.IfOperator(
            task_id='check_task_found',
            test=lambda: rail.result('lookup_leaf_task') is not None and bool(rail.result('lookup_leaf_task').get('uri')),
            yes_task='build_time_entry_payload',
            no_task='comment_task_not_found'
        )

        build_payload = rail.PythonOperator(
            task_id='build_time_entry_payload',
            python_callable=lambda dag_run: build_replicon_time_entry_payload(
                jira_data=dag_run.conf,
                user=rail.result('lookup_user'),
                project=rail.result('lookup_project'),
                task=rail.result('lookup_leaf_task'),
                oef_definition_uri=dag_run.conf.get('oef_definition_uri'),
                existing_entry_uri=get_existing_entry_uri(dag_run),
                activity_uri=get_activity_uri(),
                jira_id_oef_definition_uri=dag_run.conf.get('jira_id_oef_definition_uri'),
                default_billable=config.rep_default_billable
            )
        )

        create_time_entry = rail.RepliconServiceOperator(
            task_id='create_time_entry',
            endpoint="services/TimeEntryRevisionGroupService1.svc/PutTimeEntryRevisionGroup",
            data=lambda: rail.result('build_time_entry_payload')
        )

        check_api_response = rail.IfOperator(
            task_id='check_api_response',
            test=lambda: check_replicon_response_success(rail.result('create_time_entry')),
            yes_task='recalculate_timesheet',
            no_task='comment_api_error'
        )

        recalculate_timesheet = rail.RepliconServiceOperator(
            task_id='recalculate_timesheet',
            endpoint="services/TimesheetService1.svc/EnqueueRecalculateScriptData",
            data=lambda: build_replicon_recalculate_payload(rail.result('get_timesheet_for_date')['uri'])
        )

        comment_user_not_found = rail.JiraAPIOperator(
            task_id='comment_user_not_found',
            jira_conn_id=config.jira_conn_id,
            request_method='POST',
            endpoint="/rest/api/3/issue/{{ dag_run.conf.issue_key }}/comment",
            request_body=lambda dag_run: build_jira_comment(
                heading='Time Entry Sync Failed in Polaris',
                date=dag_run.conf['date_string'],
                hours=dag_run.conf['hours_decimal'],
                user=dag_run.conf['email'],
                project_path=get_project_path(),
                info=f"User '{dag_run.conf['email']}' was not found in Polaris.",
                success=False,
                account_id=dag_run.conf.get('account_id')
            )
        )

        comment_user_disabled = rail.JiraAPIOperator(
            task_id='comment_user_disabled',
            jira_conn_id=config.jira_conn_id,
            request_method='POST',
            endpoint="/rest/api/3/issue/{{ dag_run.conf.issue_key }}/comment",
            request_body=lambda dag_run: build_jira_comment(
                heading='Time Entry Sync Failed in Polaris',
                date=dag_run.conf['date_string'],
                hours=dag_run.conf['hours_decimal'],
                user=dag_run.conf['email'],
                project_path=get_project_path(),
                info=f"User '{dag_run.conf['email']}' is currently disabled in Polaris.",
                success=False,
                account_id=dag_run.conf.get('account_id')
            )
        )

        comment_project_error = rail.JiraAPIOperator(
            task_id='comment_project_error',
            jira_conn_id=config.jira_conn_id,
            request_method='POST',
            endpoint="/rest/api/3/issue/{{ dag_run.conf.issue_key }}/comment",
            request_body=lambda dag_run: build_jira_comment(
                heading='Time Entry Sync Failed in Polaris',
                date=dag_run.conf['date_string'],
                hours=dag_run.conf['hours_decimal'],
                user=dag_run.conf['email'],
                project_path=get_project_path(),
                info=f"Project '{(rail.result('parse_path') or {}).get('project_name')}' was not found in Polaris.",
                success=False,
                account_id=dag_run.conf.get('account_id')
            )
        )

        comment_task_not_found = rail.JiraAPIOperator(
            task_id='comment_task_not_found',
            jira_conn_id=config.jira_conn_id,
            request_method='POST',
            endpoint="/rest/api/3/issue/{{ dag_run.conf.issue_key }}/comment",
            request_body=lambda dag_run: build_jira_comment(
                heading='Time Entry Sync Failed in Polaris',
                date=dag_run.conf['date_string'],
                hours=dag_run.conf['hours_decimal'],
                user=dag_run.conf['email'],
                project_path=get_project_path(),
                info=f"Task was not found under '{(rail.result('parse_path') or {}).get('project_name')}' in Polaris.",
                success=False,
                account_id=dag_run.conf.get('account_id')
            )
        )

        comment_api_error = rail.JiraAPIOperator(
            task_id='comment_api_error',
            jira_conn_id=config.jira_conn_id,
            request_method='POST',
            endpoint="/rest/api/3/issue/{{ dag_run.conf.issue_key }}/comment",
            request_body=lambda dag_run: build_jira_comment(
                heading='Time Entry Sync Failed in Polaris',
                date=dag_run.conf['date_string'],
                hours=dag_run.conf['hours_decimal'],
                user=dag_run.conf['email'],
                project_path=get_project_path(),
                info='An unexpected error occurred while syncing with Polaris.',
                success=False,
                account_id=dag_run.conf.get('account_id')
            )
        )

        comment_sync_success = rail.JiraAPIOperator(
            task_id='comment_sync_success',
            jira_conn_id=config.jira_conn_id,
            request_method='POST',
            endpoint="/rest/api/3/issue/{{ dag_run.conf.issue_key }}/comment",
            request_body=lambda dag_run: build_jira_comment(
                heading='Time Entry Updated in Polaris' if had_existing_entry() else 'Time Entry Created in Polaris',
                date=dag_run.conf['date_string'],
                hours=dag_run.conf['hours_decimal'],
                user=dag_run.conf['email'],
                project_path=get_project_path(),
                info=get_sync_info(dag_run),
                success=True,
                account_id=dag_run.conf.get('account_id')
            )
        )

        catch_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log='{{ dag_run.conf.log }}',
            severity='Exception',
            message='Replicon sync failed with unexpected error',
            properties=lambda dag_run: {
                'issue_key': dag_run.conf.get('issue_key'),
                'worklog_id': dag_run.conf.get('worklog_id'),
                'error': '{{ get_error_message() }}',
                'target_system': 'replicon'
            }
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_errors
        can_run_batch_task >> rail.Label('No') >> check_needs_activity_lookup

        check_needs_activity_lookup >> rail.Label('Yes') >> lookup_activity >> parse_path
        check_needs_activity_lookup >> rail.Label('No') >> parse_path

        parse_path >> lookup_user >> check_user_found

        check_user_found >> rail.Label('Yes') >> check_user_enabled
        check_user_found >> rail.Label('No') >> comment_user_not_found >> catch_errors

        check_user_enabled >> rail.Label('Yes') >> check_is_delete
        check_user_enabled >> rail.Label('No') >> comment_user_disabled >> catch_errors

        check_is_delete >> rail.Label('Yes') >> search_entry_for_delete >> check_delete_entry_found
        check_is_delete >> rail.Label('No') >> search_existing_entry

        check_delete_entry_found >> rail.Label('Yes') >> get_timesheet_for_delete >> check_delete_ts_open
        check_delete_ts_open >> rail.Label('Yes') >> delete_worklog_entry >> recalculate_after_delete >> comment_delete_success >> catch_errors
        check_delete_ts_open >> rail.Label('No') >> comment_delete_ts_not_open >> catch_errors

        check_delete_entry_found >> rail.Label('No') >> comment_delete_not_found >> catch_errors

        search_existing_entry >> check_date_changed

        check_date_changed >> rail.Label('No') >> get_timesheet
        check_date_changed >> rail.Label('Yes') >> delete_existing_entry >> get_timesheet

        get_timesheet >> check_timesheet_open

        check_timesheet_open >> rail.Label('Yes') >> lookup_project
        check_timesheet_open >> rail.Label('No') >> comment_timesheet_not_open >> catch_errors

        lookup_project >> check_project_valid

        check_project_valid >> rail.Label('Yes') >> lookup_leaf_task
        check_project_valid >> rail.Label('No') >> comment_project_error >> catch_errors

        lookup_leaf_task >> check_task_found

        check_task_found >> rail.Label('Yes') >> build_payload
        check_task_found >> rail.Label('No') >> comment_task_not_found >> catch_errors

        build_payload >> create_time_entry >> check_api_response

        check_api_response >> rail.Label('Yes') >> recalculate_timesheet >> comment_sync_success >> catch_errors
        check_api_response >> rail.Label('No') >> comment_api_error >> catch_errors

    return dag


def resolve_project_and_task(dag_run, config):
    """
    Resolve the Replicon project name and task path.

    When config.rep_hardcoded_project_name / rep_hardcoded_task_name are set,
    those are used directly (integrations where JIRA sends time entries
    against a single fixed project/task). Otherwise falls back to parsing
    the colon-separated path from jira_project_custom_field.
    """
    if config.rep_hardcoded_project_name and config.rep_hardcoded_task_name:
        return {
            'project_name': config.rep_hardcoded_project_name,
            'task_path': [config.rep_hardcoded_task_name]
        }

    return parse_jira_path_field(dag_run.conf['project_code'])


def get_sync_info(dag_run):
    """
    Build the Info line for the JIRA success comment.
    For creates returns a creation sentence; for updates describes what changed.
    """
    try:
        result = rail.result('search_existing_entry')
    except Exception:
        result = None

    if not result or not result.get('entry_uri'):
        return "Time entry has been successfully recorded in Polaris."

    try:
        existing_date = normalize_date(result.get('entry_date', ''))
        new_date = dag_run.conf.get('date_string', '')
        date_changed = bool(existing_date and existing_date != new_date)

        old_hours = result.get('entry_hours')
        new_hours = dag_run.conf.get('hours_decimal')
        try:
            hours_changed = bool(
                old_hours is not None and new_hours is not None
                and abs(float(old_hours) - float(new_hours)) > 0.001
            )
        except (ValueError, TypeError):
            hours_changed = False

        if date_changed and hours_changed:
            return "Time entry has been updated. Entry date and hours have been revised."

        if date_changed:
            return "Time entry has been updated. Entry date has been changed."

        if hours_changed:
            return "Time entry has been updated. Hours have been revised."

        return "Time entry has been updated."
    except Exception:
        return "Time entry has been updated."


def get_project_path():
    """
    Build the full project path string from parse_path result.
    Returns e.g. 'MyProject > Task L1 > Task L2 > Leaf'.
    """
    try:
        parsed = rail.result('parse_path')

        if not parsed:
            return ""

        name = parsed.get('project_name', '') or ''
        task_path = parsed.get('task_path', [])

        parts = [name] + task_path if name else task_path

        return " > ".join(parts) if parts else ""
    except Exception:
        return ""


def get_activity_uri():
    """
    Get the resolved Activity URI from lookup_activity, if that task ran.
    Returns None when activity lookup was skipped (no hardcoded activity
    configured) or the activity wasn't found.
    """
    try:
        result = rail.result('lookup_activity')
        return result.get('uri') if result else None
    except Exception:
        return None


def is_date_changed(dag_run):
    """
    Check if the worklog date changed compared to the existing time entry.
    Returns True only when an existing entry was found AND its date differs.
    """
    try:
        result = rail.result('search_existing_entry')

        if not result or not result.get('entry_uri'):
            return False

        existing_date = normalize_date(result.get('entry_date', ''))
        new_date = dag_run.conf.get('date_string', '')

        return existing_date != new_date
    except Exception:
        return False


def normalize_date(date_str):
    """
    Normalize a date string to YYYY-MM-DD format for comparison.
    Handles: 'YYYY-MM-DD', 'YYYY-MM-DDTHH:MM:SS...' (Replicon datetime),
             'M/D/YYYY', 'July 23, 2026' (Replicon long-form textValue).
    """
    if not date_str:
        return ""

    # ISO format with optional time component — take the date portion only
    if len(date_str) >= 10 and date_str[4] == "-":
        return date_str[:10]

    from datetime import datetime

    for fmt in ("%m/%d/%Y", "%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass

    return date_str


def get_existing_entry_uri(dag_run):
    """
    Get the existing time entry URI for in-place updates (same date only).
    Returns None when:
    - No existing entry found for this worklog_id (a genuine create)
    - OEF value doesn't match worklog ID
    - Date changed (old entry was deleted, create fresh)

    Deliberately does not check webhook_event — search_existing_entry now
    always runs, so a matching entry is honored regardless of whether JIRA
    reported this delivery as a create or an update (keeps the sync
    idempotent on worklog_id across duplicate/retried deliveries).
    """
    try:
        result = rail.result('search_existing_entry')

        if not result or not result.get('entry_uri'):
            return None

        if str(result.get('oef_value')) != str(dag_run.conf.get('worklog_id')):
            return None

        existing_date = normalize_date(result.get('entry_date', ''))
        new_date = dag_run.conf.get('date_string', '')

        if existing_date != new_date:
            return None

        return result['entry_uri']
    except Exception:
        return None


def had_existing_entry():
    """
    Whether search_existing_entry found a prior time entry for this
    worklog_id — true for both an in-place update and a date-changed
    delete+recreate, false for a genuine first-time create.
    """
    try:
        result = rail.result('search_existing_entry')
        return bool(result and result.get('entry_uri'))
    except Exception:
        return False


def validate_user_status(user_data, config):
    """
    Check if user is enabled in Replicon.
    """
    if not user_data:
        return False

    if config.rep_require_enabled_user:
        return user_data.get('is_enabled', False)

    return True


def validate_project_status(project_data, config):
    """
    Check if project is valid for time entry.
    """
    if not project_data:
        return False

    if not project_data.get('uri'):
        return False

    if not project_data.get('allows_time_entry', True):
        return False

    if project_data.get('is_closed', False):
        return False

    return True


def check_replicon_response_success(response):
    """
    Check if Replicon PutTimeEntryRevisionGroup API response indicates success.
    """
    return bool(response) and not response.get('error')


# Create DAG instances for each configured environment
rail.for_each_instance(create_replicon_child_dag)
