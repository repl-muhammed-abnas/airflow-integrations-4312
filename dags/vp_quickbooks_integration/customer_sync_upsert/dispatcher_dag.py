"""
Dispatcher DAG for VP -> QBO Customer Upsert.
Polls Vantagepoint for recently updated firms and triggers per-firm router DAG.

Mirrors vendor_sync/dispatcher_dag.py topology (prepare timestamps -> source
poll -> extract list -> if-any -> fanout -> wait -> gather errors -> fail-or-
advance-watermark -> post run details).

Polling: a single `VantagepointFirmOperator` GET with a server-side
filterHash that selects the ModDate window AND ClientInd='Y'. The
half-open window [last_sync, current_sync) gives Workato
`polling_firm_updated` parity (records modified mid-run are picked up
exactly once on the next poll).
"""
from datetime import datetime, timedelta, timezone
from airflow.models import Variable
import rail
from vp_quickbooks_integration.common.python_callable_method import (
    build_customer_variable_key,
)


# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,too-many-locals
def create_dag(config):
    """Per-tenant dispatcher: poll VP firms, dedupe, fan out, gather, advance."""
    with rail.create_airflow_dag(
        dag_id=f'vp_qbo_customer_upsert_dispatcher_{config.instance}',
        description=(
            'Poll Vantagepoint for recently updated firms and trigger '
            'per-firm QBO customer upsert router DAG'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        # Backstop for stuck deferred sensors. Without it, a hung
        # wait_for_router_dag_runs would hold this DAG's only slot
        # forever and back-up the entire scheduling queue.
        dagrun_timeout=timedelta(
            hours=getattr(config, 'dispatcher_dagrun_timeout_hours', 2)
        ),
        tags=['vantagepoint_quickbooks', 'customer_upsert', 'dispatcher'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        def prepare_sync_timestamps():
            """Capture the sync window for this run as a half-open interval
            [last_sync_time, current_sync_time). current_sync_time is written
            back to the per-tenant Variable at the end of a successful run.
            """
            customer_id = (
                rail.get_current_context()['dag_run'].conf
                .get('customerId')
            )
            variable_key = build_customer_variable_key(
                customer_id, 'customer_upsert_last_run'
            )

            current_time = (
                datetime.now(timezone.utc)
                .strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            )

            try:
                last_sync_time = Variable.get(variable_key)
                print(
                    f"Retrieved last sync time from Variable: "
                    f"{last_sync_time}"
                )
            except KeyError:
                last_sync_time = config.initial_sync_time
                print(
                    f"Variable {variable_key} not found, using initial "
                    f"sync time: {last_sync_time}"
                )

            return {
                'last_sync_time': last_sync_time,
                'current_sync_time': current_time
            }

        prepare_timestamps = rail.PythonOperator(
            task_id='prepare_sync_timestamps',
            python_callable=prepare_sync_timestamps
        )

        # VP filter syntax: ?filterHash[i][name]=...&filterHash[i][value]=...
        # &filterHash[i][opp]=<symbol>&filterHash[i][seq]=<index>
        # &filterHash[i][type]=<datetime|date|...>.
        #
        # `opp` accepts URL-encoded comparison symbols: %3D%3D (==),
        # %3E%3D (>=), %3C (<), etc. (There is no `[operator]=<word>`
        # form despite what the API name might suggest.)
        #
        # For ModDate range comparisons `[type]=datetime` is REQUIRED —
        # without it VP silently returns an empty result set. The value
        # MUST carry an explicit UTC offset (the `Z` suffix produced by
        # `prepare_sync_timestamps`); a naive timestamp also returns []
        # because VP rejects it as ambiguous.
        #
        # ModDate >= last_sync AND ModDate < current_sync gives a half-open
        # window matching Workato `polling_firm_updated`. ClientInd='Y'
        # restricts to client/customer firms (equality filter, the only
        # opp the rest of the codebase had precedent for).
        #
        # NOTE: `filters` is NOT a Jinja template_field on
        # VantagepointFirmOperator (verified in
        # rail/operators/vantagepoint/vantagepoint_firm_operator.py:32).
        # The operator DOES support a callable for `filters` and
        # auto-invokes it with task context, so we build the URL there
        # and read the prepare_sync_timestamps XCom directly.
        def build_firm_poll_filter(**context):
            ti = context['ti']
            window = ti.xcom_pull(task_ids='prepare_sync_timestamps')
            lower = window['last_sync_time']
            upper = window['current_sync_time']
            return (
                "?filterHash[0][name]=ModDate"
                f"&filterHash[0][value]={lower}"
                "&filterHash[0][opp]=%3E%3D"
                "&filterHash[0][seq]=0"
                "&filterHash[0][type]=datetime"
                "&filterHash[1][name]=ModDate"
                f"&filterHash[1][value]={upper}"
                "&filterHash[1][opp]=%3C"
                "&filterHash[1][seq]=1"
                "&filterHash[1][type]=datetime"
                "&filterHash[2][name]=ClientInd"
                "&filterHash[2][value]=Y"
                "&filterHash[2][opp]=%3D%3D"
                "&filterHash[2][seq]=2"
            )

        get_recently_changed_firms = rail.VantagepointFirmOperator(
            task_id='get_recently_changed_firms',
            vp_conn_id=(
                "{{ dag_run.conf.connections.vantagepoint }}"
            ),
            request_method='GET',
            filters=build_firm_poll_filter
        )

        def _unwrap_firm_rows(response):
            """Pull the firm-row list out of a VP GET response envelope.

            VP returns either a bare JSON array (verified for /api/firm)
            or a dict wrapper {array|Body|body|rows|firms: [...]} for
            some endpoints. Returns None if the response is unparseable
            so the caller can fail loudly rather than silently skip.
            """
            if isinstance(response, list):
                return response
            if isinstance(response, dict):
                for key in ('array', 'Body', 'body', 'rows', 'firms'):
                    candidate = response.get(key)
                    if isinstance(candidate, list):
                        return candidate
            return None

        def extract_firm_list():
            """Extract the firm list from the VantagepointFirmOperator response.

            filterHash already applied server-side. We just unwrap the
            envelope, filter to rows that carry a ClientID, and raise if
            the response is unparseable (so the dispatcher fails rather
            than silently parks the watermark on a future poll).
            """
            response = rail.result('get_recently_changed_firms')
            rows = _unwrap_firm_rows(response)
            if rows is None:
                raise RuntimeError(
                    "Could not parse VP firm-poll response. "
                    f"type={type(response).__name__}, value={response!r}"
                )
            firms = [
                r for r in rows
                if isinstance(r, dict) and r.get('ClientID')
            ]
            print(f"Found {len(firms)} recently updated firms")
            return firms

        extract_firms = rail.PythonOperator(
            task_id='extract_firm_list',
            python_callable=extract_firm_list
        )

        check_if_firms_exist = rail.IfOperator(
            task_id='check_if_firms_exist',
            test=lambda: len(rail.result('extract_firm_list')) > 0,
            yes_task='process_firms',
            no_task='log_no_firms'
        )

        def log_no_firms_found():
            timestamps = rail.result('prepare_sync_timestamps')
            print(
                f"No recently updated firms found in Vantagepoint "
                f"(query range: {timestamps['last_sync_time']} to "
                f"{timestamps['current_sync_time']})"
            )

        log_no_firms = rail.PythonOperator(
            task_id='log_no_firms',
            python_callable=log_no_firms_found
        )

        process_firms = rail.TriggerDagRunForEachItemOperator(
            task_id='process_firms',
            items=lambda: rail.result('extract_firm_list'),
            trigger_dag_id=(
                f'vp_qbo_customer_upsert_router_{config.instance}'
            ),
            conf=lambda item: {
                **item,
                'connections': (
                    rail.get_current_context()['dag_run'].conf
                    .get('connections')
                ),
                'customerId': (
                    rail.get_current_context()['dag_run'].conf
                    .get('customerId')
                )
            },
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_router_dag_runs = rail.WaitForDagRunsSensor(
            task_id='wait_for_router_dag_runs',
            dag_runs="{{ result('process_firms') }}",
            allowed_states=['success', 'failed'],
            failed_states=[],
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        gather_router_dag_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_router_dag_errors',
            dag_runs="{{ result('process_firms') }}",
            dagrun_task_id='catch_router_dag_error',
            flatten=True
        )

        has_sync_errors = rail.IfOperator(
            task_id='has_sync_errors',
            test="{{ result('gather_router_dag_errors') | length > 0 }}",
            yes_task='fail_customer_upsert',
            no_task='update_last_sync_time'
        )

        fail_customer_upsert = rail.FailOperator(
            task_id='fail_customer_upsert',
            message=(
                "{{ result('gather_router_dag_errors')"
                " | map_to_attr('error') | join(' | ') }}"
            )
        )

        def update_last_sync_time():
            """Advance the watermark using the SAME current_sync_time that
            was used as the upper bound of this run's query, so firms
            updated mid-run are picked up exactly once next run.
            """
            customer_id = (
                rail.get_current_context()['dag_run'].conf
                .get('customerId')
            )
            variable_key = build_customer_variable_key(
                customer_id, 'customer_upsert_last_run'
            )
            timestamps = rail.result('prepare_sync_timestamps')
            current_time = timestamps['current_sync_time']

            Variable.set(variable_key, current_time)
            print(
                f"Updated last sync time Variable '{variable_key}' to: "
                f"{current_time}"
            )
            return current_time

        # trigger_rule='all_done' is critical: the FailOperator on the error
        # branch raises, which would otherwise SKIP this task under the
        # default 'all_success' rule, leaving the watermark un-advanced and
        # causing the next poll to re-process the same window. We always
        # want the watermark to advance once a run has reached a terminal
        # state (success OR partial failure already-captured as errors).
        update_sync_time = rail.PythonOperator(
            task_id='update_last_sync_time',
            python_callable=update_last_sync_time,
            trigger_rule='all_done'
        )

        post_dag_run_details = rail.PostDagRunDetailsToMiddlewareApiOperator(
            task_id='post_dag_run_details',
            middleware_api_base_url=Variable.get(
                'middleware_api_base_url', default_var=''
            ),
            trigger_rule='all_done'
        )

        (
            prepare_timestamps >> get_recently_changed_firms >>
            extract_firms >> check_if_firms_exist
        )

        (
            check_if_firms_exist >> rail.Label('Firms found') >>
            process_firms >> wait_for_router_dag_runs >>
            gather_router_dag_errors >> has_sync_errors
        )
        (
            has_sync_errors >> rail.Label('Yes') >>
            fail_customer_upsert >> update_sync_time
        )
        has_sync_errors >> rail.Label('No') >> update_sync_time

        (
            check_if_firms_exist >> rail.Label('No firms') >>
            log_no_firms >> update_sync_time
        )

        update_sync_time >> post_dag_run_details

        return dag


rail.for_each_instance(create_dag)
