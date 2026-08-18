"""
JIRA Time Sync Integration - Master DAG
========================================

This DAG receives a JIRA system webhook (fired on worklog create/update/delete)
and orchestrates synchronization to Replicon Polaris.

NOTE: Costpoint sync is not part of the current release — see
backup/costpoint_integration_backup.py for its config/DAG if reactivating.

Trigger: JIRA system webhook (webhookEvent: worklog_created/worklog_updated/worklog_deleted)
Output: Triggers the Replicon child DAG

Author: Integration Team
Version: 3.0
"""

from datetime import timedelta
from airflow.models import Variable
import rail

from jira_time_sync_cp_polaris.utils.transformers import extract_jira_worklog_data
from jira_time_sync_cp_polaris.utils.validators import validate_jira_webhook


def create_master_dag(config):
    """
    Create the master DAG for JIRA to Replicon Polaris time sync.

    This DAG:
    1. Receives a worklog event from a JIRA system webhook
    2. Fetches the full issue (custom fields, embedded worklogs for email extraction)
    3. Fetches the author by accountId as a guaranteed email fallback (covers delete events)
    4. Validates the extracted data
    5. Triggers the Replicon child DAG

    Args:
        config: Instance-specific configuration object
    """
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f"JIRA Time Sync Master - {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        webhook_conf=rail.WebhookConf(
            hmac_secret_var=config.hmac_secret_jira
        ),
        default_args={
            'retries': config.max_retries,
            'retry_delay': timedelta(seconds=config.retry_delay_seconds),
            'jira_conn_id': config.jira_conn_id,
        }
    ) as dag:

        # Standalone debugging aid — no edges, not part of the chain.
        view_config = rail.ViewDagRunConfOperator(
            task_id='view_webhook_config'
        )

        # Runs create_sync_log..catch_and_log_errors as one Airflow task,
        # avoiding per-task scheduler overhead. Toggle via Airflow Variable
        # to fall back to normal task-by-task execution for debugging.
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_sync_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_sync_log',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        create_log = rail.CreateLogOperator(
            task_id='create_sync_log'
        )

        check_sync_enabled = rail.IfOperator(
            task_id='check_sync_enabled',
            test=lambda: Variable.get(
                config.sync_enabled_var_name, default_var='true'
            ).lower() == 'true',
            yes_task='validate_webhook_payload',
            no_task='sync_disabled_skip'
        )

        sync_disabled_skip = rail.PythonOperator(
            task_id='sync_disabled_skip',
            python_callable=lambda: "Sync is disabled via Airflow Variable"
        )

        # Resolve definition URIs for both OEFs in one call.
        # Worklog_ID (hidden) is used for idempotency search;
        # JIRA_ID (visible) is written on every entry.
        # Sequential so BatchTaskRunOperator can wrap this chain.
        discover_oef_definitions = rail.RepliconServiceOperator(
            task_id='discover_oef_definitions',
            endpoint="services/ObjectExtensionDefinitionListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:object-extension-tag-definition-list-column:name",
                    "urn:replicon:object-extension-tag-definition-list-column:object-extension-tag-definition"
                ],
                "sort": [],
                "filterExpression": None
            },
            data_handler=lambda res: get_oef_definitions_by_name(
                res, [config.REP_OEF_WORKLOG_ID_NAME, config.REP_OEF_JIRA_ID_NAME])
        )

        validate_webhook = rail.PythonOperator(
            task_id='validate_webhook_payload',
            python_callable=lambda dag_run: validate_webhook_and_raise(dag_run.conf)
        )

        # System webhook carries issueId (numeric) — fetch the full issue to
        # resolve issueKey, summary, and project/task custom fields.
        # For non-delete events email is extracted from the issue's embedded
        # worklogs by matching worklog id.
        fetch_issue = rail.JiraAPIOperator(
            task_id='fetch_issue_details',
            jira_conn_id=config.jira_conn_id,
            request_method='GET',
            endpoint="/rest/api/3/issue/{{ dag_run.conf.webhook.data.worklog.issueId }}"
        )

        # For delete events the worklog is already gone from the issue's
        # embedded worklogs list, so we can't extract email from there.
        # Gate this call to delete events only — for create/update the email
        # is already resolved from the issue's embedded worklogs list.
        check_needs_author_fetch = rail.IfOperator(
            task_id='check_needs_author_fetch',
            test=lambda dag_run: (dag_run.conf.get('webhook') or {}).get('data', {}).get('webhookEvent') == 'worklog_deleted',
            yes_task='fetch_author_email',
            no_task='extract_worklog_data'
        )

        fetch_author_email = rail.JiraAPIOperator(
            task_id='fetch_author_email',
            jira_conn_id=config.jira_conn_id,
            request_method='GET',
            endpoint="/rest/api/3/user?accountId={{ dag_run.conf.webhook.data.worklog.author.accountId }}"
        )

        extract_data = rail.PythonOperator(
            task_id='extract_worklog_data',
            python_callable=lambda dag_run: extract_jira_worklog_data(
                webhook_payload=dag_run.conf,
                issue_response=rail.result('fetch_issue_details'),
                config=config,
                author_response=get_author_response()
            )
        )

        check_required_fields = rail.IfOperator(
            task_id='check_required_fields',
            test=lambda: validate_extracted_data(rail.result('extract_worklog_data')),
            yes_task='trigger_replicon_child',
            no_task='log_missing_fields'
        )

        log_missing_fields = rail.WriteLogOperator(
            task_id='log_missing_fields',
            log='{{ result("create_sync_log") }}',
            severity='Exception',
            message='Missing required fields in JIRA data',
            properties=lambda: {
                'extracted_data': rail.result('extract_worklog_data'),
                'error': 'Missing worklog_id, or missing email/time_spent_seconds/started for non-delete event'
            }
        )

        trigger_replicon = rail.TriggerDagRunOperator(
            task_id='trigger_replicon_child',
            trigger_dag_id=config.replicon_child_dag_id,
            conf=lambda: {
                **rail.result('extract_worklog_data'),
                'log': rail.result('create_sync_log'),
                'target_system': 'replicon',
                'oef_definition_uri': (rail.result('discover_oef_definitions') or {}).get(config.REP_OEF_WORKLOG_ID_NAME, {}).get('definition_uri'),
                'oef_column_uri': (rail.result('discover_oef_definitions') or {}).get(config.REP_OEF_WORKLOG_ID_NAME, {}).get('column_uri'),
                'oef_filter_uri': (rail.result('discover_oef_definitions') or {}).get(config.REP_OEF_WORKLOG_ID_NAME, {}).get('filter_uri'),
                'jira_id_oef_definition_uri': (rail.result('discover_oef_definitions') or {}).get(config.REP_OEF_JIRA_ID_NAME, {}).get('definition_uri'),
            },
            wait_for_completion=False
        )

        log_master_complete = rail.WriteLogOperator(
            task_id='log_master_complete',
            trigger_rule='none_failed_min_one_success',
            log='{{ result("create_sync_log") }}',
            severity='Info',
            message='Master DAG completed - Replicon child DAG triggered',
            properties=lambda: {
                'issue_key': rail.result('extract_worklog_data').get('issue_key'),
                'worklog_id': rail.result('extract_worklog_data').get('worklog_id')
            }
        )

        catch_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log='{{ result("create_sync_log") }}',
            severity='Exception',
            message='Master DAG encountered an error',
            properties=lambda dag_run: {
                'issue_id': dag_run.conf.get('webhook', {}).get('data', {}).get('worklog', {}).get('issueId'),
                'worklog_id': dag_run.conf.get('webhook', {}).get('data', {}).get('worklog', {}).get('id'),
                'error': '{{ get_error_message() }}'
            }
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_errors
        can_run_batch_task >> rail.Label('No') >> create_log

        create_log >> check_sync_enabled

        check_sync_enabled >> rail.Label('Yes') >> validate_webhook
        check_sync_enabled >> rail.Label('No') >> sync_disabled_skip

        validate_webhook >> discover_oef_definitions >> fetch_issue >> check_needs_author_fetch
        check_needs_author_fetch >> rail.Label('Yes') >> fetch_author_email >> extract_data
        check_needs_author_fetch >> rail.Label('No') >> extract_data
        extract_data >> check_required_fields

        check_required_fields >> rail.Label('Yes') >> trigger_replicon
        check_required_fields >> rail.Label('No') >> log_missing_fields >> catch_errors

        trigger_replicon >> log_master_complete >> catch_errors

    return dag


def get_oef_definitions_by_name(response, names):
    """
    Extract OEF URIs from ObjectExtensionDefinitionListService1 GetData response.

    The definition URI contains the GUID shared by the column and filter URIs in
    the list service. Column and filter URIs are derived by replacing the type
    segment — confirmed from actual API responses where GUID is identical across
    all three URI forms (definition / column / filter).

    Returns {oef_name: {definition_uri, column_uri, filter_uri}} for each matched name.
    """
    result = {}
    for row in (response or {}).get('rows', []):
        cells = row.get('cells') or []
        if len(cells) < 2:
            continue
        name = cells[0].get('textValue')
        definition_uri = cells[1].get('uri')
        if name in names and definition_uri:
            column_uri = definition_uri.replace(
                ':object-extension-tag-definition:',
                ':time-entry-revision-group-object-extension-column:'
            )
            filter_uri = definition_uri.replace(
                ':object-extension-tag-definition:',
                ':time-entry-revision-group-object-extension-filter:'
            )
            if column_uri == definition_uri or filter_uri == definition_uri:
                raise ValueError(
                    f"OEF definition URI '{definition_uri}' for '{name}' has unexpected shape "
                    f"— cannot derive column/filter URIs by segment replacement"
                )
            if name in result:
                raise ValueError(
                    f"Duplicate OEF definition rows found for name '{name}' "
                    f"— cannot safely choose one"
                )
            result[name] = {
                'definition_uri': definition_uri,
                'column_uri': column_uri,
                'filter_uri': filter_uri,
            }
    return result


def get_author_response():
    """
    Safely read the fetch_author_email result. Returns None when the task
    was skipped (create/update events where email comes from embedded worklogs).
    """
    try:
        return rail.result('fetch_author_email')
    except Exception:
        return None


def validate_webhook_and_raise(conf):
    """
    Validate webhook payload and raise exception if invalid.
    """
    is_valid, error_message = validate_jira_webhook(conf)
    if not is_valid:
        raise ValueError(f"Webhook validation failed: {error_message}")
    return True


def validate_extracted_data(data):
    """
    Validate that extracted data has all required fields.

    email is always required — the child DAG's lookup_user task needs it
    regardless of event type. For worklog_deleted events it is supplied by
    fetch_author_email (GET /rest/api/3/user?accountId=...) even when the
    worklog is already gone from the issue's embedded worklogs list.

    For worklog_deleted events time_spent_seconds and started are not
    required — the entry is found and deleted by OEF (worklog_id) so
    the original hours/date are irrelevant.

    project_code is intentionally not required: it's only used by the
    Replicon child DAG's path-parsing fallback, which is dormant while the
    hardcoded project/task override is active.
    """
    if not data:
        return False

    if not data.get('worklog_id'):
        return False

    if not data.get('email'):
        return False

    if data.get('webhook_event') == 'worklog_deleted':
        return True

    required_fields = ['time_spent_seconds', 'started']

    for field in required_fields:
        if not data.get(field):
            return False

    return True


# Create DAG instances for each configured environment
rail.for_each_instance(create_master_dag)
