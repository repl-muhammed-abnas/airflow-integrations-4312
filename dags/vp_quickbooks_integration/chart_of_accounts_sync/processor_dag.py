"""
Processor DAG for QBO -> VP Chart of Accounts Sync.

Runs once per QuickBooks account surfaced by the dispatcher. Reproduces the
per-account FOREACH body of the Workato main recipe
`014_503_psa_sync_account_codes` as an IfOperator decision tree:

  ensure_map_row     -> Workato branch 1: add this QBO account to the
                        account_map if it isn't there yet (idempotent).
  match_vp_account   -> Workato join: match the QBO account to a VP account
                        by AcctNum=Account OR Name=Name (against the slim VP
                        index passed in conf by the dispatcher).
  decide_action      -> collapse the Workato branches into one of:
                          create | update | link | skip
  is_create  (yes) -> get_system_formats -> check_account_code_length
                      -> create_account_in_vp (POST /Accounts/)
                      -> link_map_after_create
  is_update  (yes) -> update_account_in_vp (PUT /Accounts/{code})
  is_link    (yes) -> link_map_after_match (no VP write — map bookkeeping)
  (none)           -> log_skip

The two VP-writing branches are the only outcomes that call the VP API; the
rest are pure account_map maintenance — matching the Workato recipe, where a
single account hits exactly one of these outcomes per pass.

No idempotency beyond the account_map's QBOID key + the dispatcher watermark,
which only advances on a fully clean run (any processor failure leaves the
watermark behind so the next dispatcher run re-polls the same window).
"""
# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,import-error
from datetime import timedelta
import rail
from vp_quickbooks_integration.chart_of_accounts_sync.utils.python_callable_method import (  # noqa: E501
    ensure_map_row_method,
    match_vp_account_method,
    decide_action_method,
    is_create_action,
    is_update_action,
    is_link_action,
    system_formats_entity_filter,
    check_account_code_length_method,
    build_create_account_body_method,
    build_update_account_body_method,
    link_account_in_map_method,
    log_skip_method,
    capture_processor_error,
)


def create_dag(config):
    """Per-QBO-account processor DAG."""
    with rail.create_airflow_dag(
        dag_id=f'vp_qbo_chart_of_accounts_sync_processor_{config.instance}',
        description=(
            'Sync one QuickBooks account into the Vantagepoint chart of '
            'accounts (create / update / link / skip)'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        tags=[
            'vantagepoint_quickbooks',
            'chart_of_accounts_sync',
            'processor',
        ],
        default_args={
            'execution_timeout': timedelta(
                days=config.execution_timeout_days
            )
        }
    ) as dag:

        ensure_map_row = rail.PythonOperator(
            task_id='ensure_map_row',
            python_callable=ensure_map_row_method,
        )

        match_vp_account = rail.PythonOperator(
            task_id='match_vp_account',
            python_callable=match_vp_account_method,
        )

        decide_action = rail.PythonOperator(
            task_id='decide_action',
            python_callable=decide_action_method,
        )

        is_create = rail.IfOperator(
            task_id='is_create',
            test=is_create_action,
            yes_task='get_system_formats',
            no_task='is_update',
        )

        # --- create branch (Workato branch 2b-i) ---
        get_system_formats = rail.VantagepointSystemFormatsOperator(
            task_id='get_system_formats',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            request_method='GET',
            filters=system_formats_entity_filter,
        )

        check_account_code_length = rail.PythonOperator(
            task_id='check_account_code_length',
            python_callable=check_account_code_length_method,
        )

        create_account_in_vp = rail.VantagepointChartOfAccountsOperator(
            task_id='create_account_in_vp',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            request_method='POST',
            request_body=build_create_account_body_method,
        )

        link_map_after_create = rail.PythonOperator(
            task_id='link_map_after_create',
            python_callable=link_account_in_map_method,
        )

        is_update = rail.IfOperator(
            task_id='is_update',
            test=is_update_action,
            yes_task='update_account_in_vp',
            no_task='is_link',
        )

        # --- update branch (Workato branch 3) ---
        update_account_in_vp = rail.VantagepointChartOfAccountsOperator(
            task_id='update_account_in_vp',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            request_method='PUT',
            account="{{ result('decide_action')['vp_code'] }}",
            request_body=build_update_account_body_method,
        )

        # Refresh the account_map after the PUT — mirrors the Workato update
        # branch, which follows its VP PUT with a lookup_table.update_entry that
        # writes back VantagepointCode/Name/Type (+ clears Message), leaving the
        # QBO* columns untouched. decide_action carries the new name/code/type
        # for the update action, so link_account_in_map_method writes exactly
        # those columns.
        link_map_after_update = rail.PythonOperator(
            task_id='link_map_after_update',
            python_callable=link_account_in_map_method,
        )

        is_link = rail.IfOperator(
            task_id='is_link',
            test=is_link_action,
            yes_task='link_map_after_match',
            no_task='log_skip',
        )

        # --- link branch (Workato branch 2a-i: map-only, no VP write) ---
        link_map_after_match = rail.PythonOperator(
            task_id='link_map_after_match',
            python_callable=link_account_in_map_method,
        )

        # --- skip branch (Workato branch 5) ---
        log_skip = rail.PythonOperator(
            task_id='log_skip',
            python_callable=log_skip_method,
        )

        catch_processor_dag_error = rail.PythonOperator(
            task_id='catch_processor_dag_error',
            trigger_rule='one_failed',
            python_callable=capture_processor_error,
            op_args=[
                '{{ dag_run.conf.Id }}',
                "{{ dag_run.conf.get('Name') or '' }}",
                '{{ get_error_message() }}',
            ],
        )

        # Decision spine
        (
            ensure_map_row >> match_vp_account >> decide_action >> is_create
        )

        # create branch
        (
            is_create >> rail.Label('Create') >>
            get_system_formats >> check_account_code_length >>
            create_account_in_vp >> link_map_after_create
        )

        # update branch
        is_create >> rail.Label('Not create') >> is_update
        (
            is_update >> rail.Label('Update') >>
            update_account_in_vp >> link_map_after_update
        )

        # link / skip branches
        is_update >> rail.Label('Not update') >> is_link
        is_link >> rail.Label('Link') >> link_map_after_match
        is_link >> rail.Label('Skip') >> log_skip

        # `catch_processor_dag_error` MUST be the SOLE leaf task, for two
        # reasons that together make the child run end SUCCESS on any failure —
        # which is what the dispatcher's WaitForDagRunsSensor requires (it
        # treats a `failed` child run as a hard failure: `failed_states=[]`
        # falls back to the default `['failed']`, so a failed child raises the
        # sensor and skips the gather/fail path):
        #
        #  1. Direct edge from EVERY work task -> catch, because
        #     trigger_rule='one_failed' fires on a `failed` DIRECT upstream but
        #     NOT on `upstream_failed`. A failure anywhere thus gives catch a
        #     `failed` parent so it runs (returns a dict, never raises).
        #  2. `log_skip` funnels into catch too, so catch is the ONLY leaf. If
        #     log_skip stayed a leaf it would go `upstream_failed` when a
        #     PRE-BRANCH task (ensure_map_row/match/decide) fails, making the
        #     child run `failed` even though catch succeeded — tripping the
        #     dispatcher sensor. With catch as the sole leaf the run is SUCCESS
        #     (catch success, or catch skipped on the happy path), the sensor
        #     passes, and fail_chart_of_accounts_sync surfaces the gathered error.
        ensure_map_row >> catch_processor_dag_error
        match_vp_account >> catch_processor_dag_error
        decide_action >> catch_processor_dag_error
        get_system_formats >> catch_processor_dag_error
        check_account_code_length >> catch_processor_dag_error
        create_account_in_vp >> catch_processor_dag_error
        link_map_after_create >> catch_processor_dag_error
        update_account_in_vp >> catch_processor_dag_error
        link_map_after_update >> catch_processor_dag_error
        link_map_after_match >> catch_processor_dag_error
        log_skip >> catch_processor_dag_error

        return dag


rail.for_each_instance(create_dag)
