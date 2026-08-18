# dags/vp_xero_integration_v2/ap_voucher_sync/ap_voucher_create_dag.py
"""Per-voucher create DAG for VP -> Xero AP Voucher Sync.

Runs once per (Period, PostSeq, Voucher) posted AP voucher triggered by the
dispatcher. Ports the Workato recipe 014_501_psa_post_ap_voucher_to_xero:

  1. Guard: skip if (Batch, Voucher) already in outstanding_purchase_invoices
  2. Re-fetch all PSALedger lines for this voucher (no AutoEntry/TaxCode filter)
  3. Load map_chart_of_accounts + map_firm + map_tax_code from S3
  4. Resolve Vendor -> Xero ContactID via map_firm
  5. Enrich lines: XeroAccountCode (map_chart_of_accounts), XeroTaxType (map_tax_code)
  6. Validate: every line needs a mapped Xero AccountCode
  7. Build Xero ACCPAY Invoice JSON
  8. POST to Xero /api.xro/2.0/Invoices via XeroAPIOperator
  9. Record outstanding_purchase_invoices rows (dedup marker + InvoiceID)

Failure pattern: QBO-style — catch_processor_dag_error (trigger_rule='one_failed')
with a direct edge from every task except log_skip_already_exported. Any task
failure routes to catch_processor_dag_error, which returns an error dict
consumed by the dispatcher's gather_processor_dag_errors + FailOperator chain.
"""
# pylint: disable=too-many-statements,line-too-long,pointless-statement
# pylint: disable=expression-not-assigned,import-error
import logging
from datetime import timedelta
import rail
from vp_xero_integration_v2.ap_voucher_sync.utils.python_callable_method import (
    is_voucher_already_exported_method,
    build_psaledger_period_postseq_ap_filter_method,
    extract_psaledger_lines_method,
    load_lookup_tables_method,
    resolve_firm_vendorref_method,
    enrich_lines_method,
    validate_enriched_lines_method,
    build_bill_body_method,
    record_outstanding_invoices_method,
    capture_processor_error,
)

logger = logging.getLogger(__name__)


def create_dag(config):
    """Per-(Period, PostSeq, Voucher) AP-voucher Xero Bill create DAG."""
    with rail.create_airflow_dag(
        dag_id=f'vp_xero_ap_voucher_sync_ap_voucher_create_{config.instance}',
        description=(
            'Build and post one Xero ACCPAY Invoice for a single VP AP voucher'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        tags=[
            'vantagepoint_xero',
            'ap_voucher_sync',
            'create',
        ],
        default_args={
            'execution_timeout': timedelta(
                days=config.execution_timeout_days
            )
        },
    ) as dag:

        # Dedup guard: skip if already exported (no duplicate Xero Bill).
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
                '— skipping; no duplicate Xero Bill created.'
            ),
        )

        get_psaledger_lines_for_voucher = rail.VantagepointPsaledgerOperator(
            task_id='get_psaledger_lines_for_voucher',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            request_method='GET',
            trans_type='ap',
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
            python_callable=build_bill_body_method,
        )

        create_xero_bill = rail.XeroInvoiceOperator(
            task_id='create_xero_bill',
            xero_conn_id="{{ dag_run.conf.connections.xero }}",
            operation='create_bill',
            request_body=lambda: rail.result('build_bill_body'),
        )

        record_outstanding_invoices = rail.PythonOperator(
            task_id='record_outstanding_invoices',
            python_callable=lambda: record_outstanding_invoices_method(
                'create_xero_bill'
            ),
        )

        # QBO-style: catch fires on the first task failure; direct edges from
        # every task (except log_skip_already_exported) ensure it always runs
        # when something goes wrong — upstream_failed does NOT fire one_failed,
        # so direct edges are required.
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

        # Dedup gate
        (
            check_already_exported >> rail.Label('Already exported') >>
            log_skip_already_exported
        )

        # Export chain
        (
            check_already_exported >> rail.Label('Not yet exported') >>
            get_psaledger_lines_for_voucher >>
            extract_psaledger_lines >>
            load_lookup_tables >>
            resolve_firm_vendorref >>
            enrich_lines >>
            validate_enriched_lines >>
            build_bill_body >>
            create_xero_bill >>
            record_outstanding_invoices
        )

        # Direct edges to catch_processor_dag_error from every task except
        # log_skip_already_exported (that task is a silent skip, not a failure).
        check_already_exported >> catch_processor_dag_error
        get_psaledger_lines_for_voucher >> catch_processor_dag_error
        extract_psaledger_lines >> catch_processor_dag_error
        load_lookup_tables >> catch_processor_dag_error
        resolve_firm_vendorref >> catch_processor_dag_error
        enrich_lines >> catch_processor_dag_error
        validate_enriched_lines >> catch_processor_dag_error
        build_bill_body >> catch_processor_dag_error
        create_xero_bill >> catch_processor_dag_error
        # Wire record_outstanding_invoices too: if this write fails after a
        # successful Xero Bill create, the outstanding row is never written,
        # so the next poll would create a duplicate Bill. Wiring ensures the
        # watermark holds and the operator is re-run next cycle.
        record_outstanding_invoices >> catch_processor_dag_error

        return dag


rail.for_each_instance(create_dag)
