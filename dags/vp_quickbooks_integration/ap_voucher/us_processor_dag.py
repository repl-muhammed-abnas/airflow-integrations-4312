"""
Processor DAG for VP -> QBO AP Voucher Sync — US region.

Runs once per (Period, PostSeq, Voucher) posted AP voucher routed here by the
dispatcher for tenants whose CFG_Region is US. Ports the Workato recipe
`014_503_psa_post_ap_voucher_to_quickbooks_us`:

  GET /PSALedger/AP filtered to this exact (Period, PostSeq, Voucher)
  Load global firm_map + account_map (+ tax_code_map, unused for US)
  Resolve the voucher Vendor code -> VP firm ClientID -> firm_map -> VendorRef
  GET /api/project for the lines' WBS1 codes (batched 10) -> client per line
  Enrich each line with QBO AccountRef (account_map), CustomerRef
    (firm_map via project client), and a binary TaxCodeRef (TAX/NON)
  Validate: every line needs a mapped QBO account; vendor must resolve
  Build one QBO Bill (AccountBasedExpenseLineDetail lines, no TxnTaxDetail)
  POST /bill via QuickBooksBillOperator (create-only, region=US)

No idempotency map — re-run safety comes purely from the dispatcher's
watermark, which only advances on a fully clean run (any processor failure
leaves the watermark behind so the next run re-polls the same window).
"""
# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,import-error
import logging
from datetime import timedelta
import rail
from vp_quickbooks_integration.ap_voucher.config import (
    us_qbo_region,
    us_qbo_currency,
)
from vp_quickbooks_integration.ap_voucher.utils.python_callable_method import (  # noqa: E501
    is_voucher_already_exported_method,
    build_psaledger_period_postseq_ap_filter_method,
    extract_psaledger_lines_method,
    load_lookup_tables_method,
    resolve_firm_vendorref_method,
    extract_unique_wbs1_method,
    get_project_clients_from_vp_method,
    build_project_client_index_method,
    enrich_lines_method,
    validate_enriched_lines_method,
    build_us_bill_body_method,
    record_outstanding_invoices_method,
    capture_processor_error,
)

logger = logging.getLogger(__name__)


def create_dag(config):
    """Per-(Period, PostSeq, Voucher) US AP-voucher processor DAG."""
    with rail.create_airflow_dag(
        dag_id=f'vp_qbo_ap_voucher_sync_us_processor_{config.instance}',
        description=(
            'Build and post one QBO Bill for a single VP AP voucher — '
            'US region'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        tags=[
            'vantagepoint_quickbooks',
            'ap_voucher_sync',
            'us',
            'processor',
        ],
        default_args={
            'execution_timeout': timedelta(
                days=config.execution_timeout_days
            )
        }
    ) as dag:

        # Workato dedup guard (US comment "Voucher has not yet been exported",
        # line 8867): read the Outstanding Purchase Invoices table by
        # Batch+Voucher and skip the entire export if this voucher was already
        # turned into a Bill — prevents a duplicate QBO Bill on any re-poll.
        check_already_exported = rail.IfOperator(
            task_id='check_already_exported',
            test=is_voucher_already_exported_method,
            yes_task='log_skip_already_exported',
            no_task='get_psaledger_lines_for_voucher',
        )

        log_skip_already_exported = rail.PythonOperator(
            task_id='log_skip_already_exported',
            python_callable=lambda: logger.info(
                'AP voucher already exported (present in outstanding-invoices) '
                '— skipping; no duplicate QuickBooks Bill created.'
            ),
        )

        get_psaledger_lines_for_voucher = rail.VantagepointPsaledgerOperator(
            task_id='get_psaledger_lines_for_voucher',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            request_method='GET',
            trans_type='AP',
            filters=build_psaledger_period_postseq_ap_filter_method,
        )

        extract_psaledger_lines = rail.PythonOperator(
            task_id='extract_psaledger_lines',
            python_callable=extract_psaledger_lines_method,
        )

        load_lookup_tables = rail.PythonOperator(
            task_id='load_lookup_tables',
            python_callable=load_lookup_tables_method,
        )

        resolve_firm_vendorref = rail.PythonOperator(
            task_id='resolve_firm_vendorref',
            python_callable=resolve_firm_vendorref_method,
        )

        extract_unique_wbs1 = rail.PythonOperator(
            task_id='extract_unique_wbs1',
            python_callable=extract_unique_wbs1_method,
        )

        get_project_clients_from_vp = rail.PythonOperator(
            task_id='get_project_clients_from_vp',
            python_callable=get_project_clients_from_vp_method,
        )

        build_project_client_index = rail.PythonOperator(
            task_id='build_project_client_index',
            python_callable=build_project_client_index_method,
        )

        enrich_lines = rail.PythonOperator(
            task_id='enrich_lines',
            python_callable=enrich_lines_method,
        )

        validate_enriched_lines = rail.PythonOperator(
            task_id='validate_enriched_lines',
            python_callable=validate_enriched_lines_method,
        )

        build_bill_body = rail.PythonOperator(
            task_id='build_bill_body',
            python_callable=build_us_bill_body_method,
        )

        create_bill_in_qbo = rail.QuickBooksBillOperator(
            task_id='create_bill_in_qbo',
            intuit_conn_id="{{ dag_run.conf.connections.intuit }}",
            region=us_qbo_region,
            currency=us_qbo_currency,
            request_body=lambda: rail.result('build_bill_body'),
        )

        # Workato parity (US step 30-31): after the bill posts, write one
        # outstanding-invoice tracking row per line. Fail-loud (raises on
        # error) to match the recipe; this row is what makes the dedup guard
        # skip the voucher on a re-poll.
        record_outstanding_invoices = rail.PythonOperator(
            task_id='record_outstanding_invoices',
            python_callable=lambda: record_outstanding_invoices_method(
                'create_bill_in_qbo'
            ),
        )

        catch_processor_dag_error = rail.PythonOperator(
            task_id='catch_processor_dag_error',
            trigger_rule='one_failed',
            python_callable=capture_processor_error,
            op_args=[
                '{{ dag_run.conf.Period }}',
                '{{ dag_run.conf.PostSeq }}',
                '{{ get_error_message() }}',
            ],
        )

        # Dedup gate first: already-exported → skip (log only); otherwise run
        # the full export chain.
        check_already_exported >> rail.Label('Already exported') >> log_skip_already_exported
        (
            check_already_exported >> rail.Label('Not yet exported') >>
            get_psaledger_lines_for_voucher >>
            extract_psaledger_lines >>
            load_lookup_tables >>
            resolve_firm_vendorref >>
            extract_unique_wbs1 >>
            get_project_clients_from_vp >>
            build_project_client_index >>
            enrich_lines >>
            validate_enriched_lines >>
            build_bill_body >>
            create_bill_in_qbo >>
            record_outstanding_invoices
        )

        # one_failed fires on `failed` but NOT on `upstream_failed`. Direct
        # edges from every upstream task ensure the catch always runs so the
        # dispatcher gathers an error dict and the watermark is held.
        check_already_exported >> catch_processor_dag_error
        get_psaledger_lines_for_voucher >> catch_processor_dag_error
        extract_psaledger_lines >> catch_processor_dag_error
        load_lookup_tables >> catch_processor_dag_error
        resolve_firm_vendorref >> catch_processor_dag_error
        extract_unique_wbs1 >> catch_processor_dag_error
        get_project_clients_from_vp >> catch_processor_dag_error
        build_project_client_index >> catch_processor_dag_error
        enrich_lines >> catch_processor_dag_error
        validate_enriched_lines >> catch_processor_dag_error
        build_bill_body >> catch_processor_dag_error
        create_bill_in_qbo >> catch_processor_dag_error
        # record_outstanding_invoices is fail-loud (a missing collection raises);
        # wire it to the catch too so its failure is captured + propagated to the
        # dispatcher (otherwise the watermark would wrongly advance and the next
        # poll would create a DUPLICATE Bill, since its outstanding row — the
        # dedup marker — was never written).
        record_outstanding_invoices >> catch_processor_dag_error

        return dag


rail.for_each_instance(create_dag)
