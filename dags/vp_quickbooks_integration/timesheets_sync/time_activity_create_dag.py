"""
Time Activity Create DAG for VP -> QBO Timesheets Sync.

Per-record DAG. Receives {PostSeq, Period, TransType, connections} from
dispatcher, then:
  1. Re-fetches the full PSA Ledger row by composite key.
  2. Defensive billing-transfer guard.
  3. Reads employee + firm lookup tables.
  4. Resolves VP->QBO mappings.
  5. Validates both mappings exist.
  6. Posts up to three QBO TimeActivity records (regular, overtime,
     special overtime) — recipe steps 20-24.

Mirrors the orchestrator portion of `014_503_psa_vantagepoint_timesheet_
exports_to_quickbooks.recipe.json` plus the post recipe `014_503_psa_post_
vantagepoint_timesheet_to_quickbooks.recipe.json`.
"""
# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,import-error
from datetime import timedelta
import rail
from vp_quickbooks_integration.timesheets_sync.utils.python_callable_method import (  # noqa: E501
    resolve_employee_method,
    resolve_firm_method,
    is_billing_transfer_record_method,
    is_employee_mapping_resolved_method,
    is_firm_mapping_resolved_method,
    log_billing_transfer_skipped,
    log_missing_employee_mapping,
    log_missing_firm_mapping,
    build_regular_time_activity_body,
    build_overtime_time_activity_body,
    build_special_overtime_time_activity_body,
    has_overtime_hours_method,
    has_special_overtime_hours_method,
    capture_create_dag_error,
)


def create_dag(config):
    """Per-record DAG: validate + create up to three QBO TimeActivities."""
    with rail.create_airflow_dag(
        dag_id=(
            f'vp_qbo_timesheets_sync_time_activity_create_'
            f'{config.instance}'
        ),
        description=(
            'Resolve mappings and create QBO TimeActivity entries '
            '(regular + overtime + special overtime) for one PSA Ledger row'
        ),
        company_key=config.company_key,
        integration_type='generic',
        multi_tenant=True,
        max_active_runs=config.max_active_runs,
        schedule_interval=None,
        tags=[
            'vantagepoint_quickbooks', 'timesheets_sync',
            'time_activity_create'
        ],
        default_args={
            'execution_timeout': timedelta(
                days=config.execution_timeout_days
            ),
        }
    ) as dag:

        def build_record_filter():
            """
            Polling trigger emits {Period, PostSeq, TransType} only;
            re-fetch the full row by composite key.
            PostSeq is coerced to int and Period is OData-escaped
            (single quotes doubled) to harden the filter against
            unexpected conf values.
            """
            conf = rail.get_current_context()['dag_run'].conf
            try:
                post_seq = int(conf.get('PostSeq'))
            except (TypeError, ValueError):
                post_seq = 0
            period = str(conf.get('Period') or '').replace("'", "''")
            return (
                f"?$filter=PostSeq eq {post_seq}"
                f" and Period eq '{period}'"
            )

        fetch_record_detail = rail.VantagepointPsaledgerOperator(
            task_id='fetch_record_detail',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            filters=build_record_filter,
            trans_type='ts',
            pagination=False
        )

        # Defensive guard — billing-transfer rows should already be filtered
        # by the dispatcher, but the polling endpoint may not return Desc1,
        # so we re-check after the full fetch.
        # IfOperator.no_task only forwards to a single task; we want
        # the mapping lookups to fan out in parallel, so the No branch
        # routes through a passthrough EmptyOperator first.
        is_billing_transfer = rail.IfOperator(
            task_id='is_billing_transfer',
            test=is_billing_transfer_record_method,
            yes_task='log_billing_transfer_skipped_action',
            no_task='start_mapping_resolution'
        )

        start_mapping_resolution = rail.EmptyOperator(
            task_id='start_mapping_resolution'
        )

        log_billing_transfer_skipped_action = rail.PythonOperator(
            task_id='log_billing_transfer_skipped_action',
            python_callable=log_billing_transfer_skipped
        )

        # Mapping resolution — queries the shared mapping_sync S3 collection
        # directly (no separate fetch tasks needed; each resolve callable
        # executes its own targeted S3 query inline).
        resolve_employee = rail.PythonOperator(
            task_id='resolve_employee',
            python_callable=resolve_employee_method
        )

        resolve_firm = rail.PythonOperator(
            task_id='resolve_firm',
            python_callable=resolve_firm_method
        )

        is_employee_mapping_resolved = rail.IfOperator(
            task_id='is_employee_mapping_resolved',
            test=is_employee_mapping_resolved_method,
            yes_task='is_firm_mapping_resolved',
            no_task='log_missing_employee_mapping_action'
        )

        is_firm_mapping_resolved = rail.IfOperator(
            task_id='is_firm_mapping_resolved',
            test=is_firm_mapping_resolved_method,
            yes_task='post_regular_time_activity',
            no_task='log_missing_firm_mapping_action'
        )

        log_missing_employee_mapping_action = rail.PythonOperator(
            task_id='log_missing_employee_mapping_action',
            python_callable=log_missing_employee_mapping
        )

        log_missing_firm_mapping_action = rail.PythonOperator(
            task_id='log_missing_firm_mapping_action',
            python_callable=log_missing_firm_mapping
        )

        # ---- Regular hours (always created) -----------------------------
        # POST /timeactivity is non-idempotent — overriding retries=3 per
        # acceptance criteria, with PrivateNote as soft-dedupe key.
        post_regular_time_activity = rail.QuickBooksTimeActivityOperator(
            task_id='post_regular_time_activity',
            intuit_conn_id="{{ dag_run.conf.connections.intuit }}",
            request_body=build_regular_time_activity_body,
            retries=3,
            retry_exponential_backoff=True,
            retry_delay=timedelta(seconds=10)
        )

        # ---- Overtime hours (conditional) -------------------------------
        has_overtime = rail.IfOperator(
            task_id='has_overtime',
            test=has_overtime_hours_method,
            yes_task='post_overtime_time_activity',
            no_task='has_special_overtime'
        )

        post_overtime_time_activity = rail.QuickBooksTimeActivityOperator(
            task_id='post_overtime_time_activity',
            intuit_conn_id="{{ dag_run.conf.connections.intuit }}",
            request_body=build_overtime_time_activity_body,
            retries=3,
            retry_exponential_backoff=True,
            retry_delay=timedelta(seconds=10)
        )

        # ---- Special overtime hours (conditional) -----------------------
        # has_special_overtime.no_task points directly at the catch task
        # so all happy-path branches converge there (mirrors vendor_sync
        # and customer_sync — no separate `log_success` terminal).
        has_special_overtime = rail.IfOperator(
            task_id='has_special_overtime',
            test=has_special_overtime_hours_method,
            yes_task='post_special_overtime_time_activity',
            no_task='catch_create_dag_error'
        )

        post_special_overtime_time_activity = (
            rail.QuickBooksTimeActivityOperator(
                task_id='post_special_overtime_time_activity',
                intuit_conn_id="{{ dag_run.conf.connections.intuit }}",
                request_body=build_special_overtime_time_activity_body,
                retries=3,
                retry_exponential_backoff=True,
                retry_delay=timedelta(seconds=10)
            )
        )

        # `op_args` is in PythonOperator's template_fields, so each
        # element is Jinja-rendered before being passed in. The
        # `get_error_message` macro is registered globally for every
        # RAIL DAG via rail.dag.get_macros() -> user_defined_macros
        # (see rail/macros.py:121 and rail/dag.py:179). On a happy-path
        # run with no upstream failures, the macro returns an empty
        # string and `capture_create_dag_error` returns None — that's
        # the intended no-op behaviour for the all_done trigger rule.
        # This task is also the topology terminal for all QBO POST
        # branches; the all_done trigger rule means it fires regardless
        # of which combinations of the optional overtime / special-
        # overtime branches ran.
        catch_create_dag_error = rail.PythonOperator(
            task_id='catch_create_dag_error',
            trigger_rule='all_done',
            python_callable=capture_create_dag_error,
            op_args=[
                '{{ dag_run.conf.PostSeq }}',
                '{{ dag_run.conf.Period }}',
                '{{ get_error_message() }}'
            ]
        )

        # --- Validation flow (was router_dag) ----------------------------
        fetch_record_detail >> is_billing_transfer
        (
            is_billing_transfer >> rail.Label('Yes - skip') >>
            log_billing_transfer_skipped_action
        )
        # Employee and Firm lookups are independent — fan out from the
        # passthrough start node and rejoin at is_employee_mapping_resolved.
        # IfOperator.no_task can only point at a single task, hence the
        # EmptyOperator hop.
        (
            is_billing_transfer >> rail.Label('No - process') >>
            start_mapping_resolution
        )
        start_mapping_resolution >> [resolve_employee, resolve_firm]
        [resolve_employee, resolve_firm] >> is_employee_mapping_resolved

        (
            is_employee_mapping_resolved >>
            rail.Label('Employee mapped') >>
            is_firm_mapping_resolved
        )
        (
            is_employee_mapping_resolved >>
            rail.Label('Employee not mapped') >>
            log_missing_employee_mapping_action
        )

        (
            is_firm_mapping_resolved >> rail.Label('Firm mapped') >>
            post_regular_time_activity
        )
        (
            is_firm_mapping_resolved >> rail.Label('Firm not mapped') >>
            log_missing_firm_mapping_action
        )

        # --- QBO POST chain ----------------------------------------------
        post_regular_time_activity >> has_overtime
        (
            has_overtime >> rail.Label('Has overtime') >>
            post_overtime_time_activity >> has_special_overtime
        )
        (
            has_overtime >> rail.Label('No overtime') >>
            has_special_overtime
        )
        (
            has_special_overtime >> rail.Label('Has special overtime') >>
            post_special_overtime_time_activity >> catch_create_dag_error
        )
        # Explicit no-special-overtime edge — required because the
        # IfOperator's no_task='catch_create_dag_error' is only a name
        # reference for branching, not a topology edge. test_if_operator_yes_no_taks_are_valid
        # checks that both yes_task and no_task targets appear in the
        # operator's actual downstream_task_ids.
        (
            has_special_overtime >> rail.Label('No special overtime') >>
            catch_create_dag_error
        )

        # --- Failure path -------------------------------------------------
        # `catch_create_dag_error` has trigger_rule='all_done' and serves
        # as the single terminal for all branches (happy path and
        # validation-log branches alike). The QBO POST tasks reach it
        # transitively via the has_special_overtime IfOperator; we only
        # wire the validation-log branches explicitly.
        log_billing_transfer_skipped_action >> catch_create_dag_error
        log_missing_employee_mapping_action >> catch_create_dag_error
        log_missing_firm_mapping_action >> catch_create_dag_error

        return dag


rail.for_each_instance(create_dag)
