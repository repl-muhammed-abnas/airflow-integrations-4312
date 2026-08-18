"""
Processor DAG for VP -> QBO Journal Entry Sync.

Runs once per (Period, PostSeq) posted journal entry surfaced by the
dispatcher. Ports the Workato main recipe
`014_503_psa_vantagepoint_journal_entry_exports_to_quickbooks`:

  GET /PSALedger/JE filtered to this exact (Period, PostSeq)
  Load global account_map + firm_map Airflow Variables
  GET /api/project filtered by unique WBS1 codes (batched, 10 per request)
  Enrich each PSALedger line with QBOAccountID, QBOAccountName,
    ClientID, QBOFirmID, IsVendor
  Firm fallback: for unmapped firms with a ClientID, GET /firm/{ClientID}
    and try to populate QBOFirmID from the VP record
  Validate: every line needs QBOAccountName; every line with a ClientID
    needs a QBOFirmID after fallback. Raises on any gap (Workato
    CompoundError halt — per-journal scope so sibling processors continue).
  Build a single balanced QBO JournalEntry payload (flat Line[] array,
    one entry per PSALedger line, PostingType keyed by Amount sign).
  POST /journalentry via QuickBooksJournalEntryOperator (create-only).

No idempotency map — re-run safety comes purely from the dispatcher's
watermark, which only advances on a fully clean run (any processor
failure leaves the watermark behind so the next dispatcher run
re-polls the same window).
"""
# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,import-error
from datetime import timedelta
import rail
from vp_quickbooks_integration.journal_entry_sync.utils.python_callable_method import (  # noqa: E501
    build_psaledger_period_postseq_filter_method,
    extract_psaledger_lines_method,
    load_lookup_tables_method,
    extract_unique_wbs1_method,
    get_project_clients_from_vp_method,
    build_project_client_index_method,
    enrich_lines_method,
    firm_fallback_from_vp_method,
    validate_enriched_lines_method,
    build_journal_entry_body_method,
    capture_processor_error,
)


def create_dag(config):
    """Per-(Period, PostSeq) processor DAG."""
    with rail.create_airflow_dag(
        dag_id=f'vp_qbo_journal_entry_sync_processor_{config.instance}',
        description=(
            'Build and post one balanced QBO JournalEntry for a single VP '
            '(Period, PostSeq) posted journal'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        tags=[
            'vantagepoint_quickbooks',
            'journal_entry_sync',
            'processor',
        ],
        default_args={
            'execution_timeout': timedelta(
                days=config.execution_timeout_days
            )
        }
    ) as dag:

        get_psaledger_lines_for_journal = rail.VantagepointPsaledgerOperator(
            task_id='get_psaledger_lines_for_journal',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            request_method='GET',
            trans_type='JE',
            filters=build_psaledger_period_postseq_filter_method,
        )

        extract_psaledger_lines = rail.PythonOperator(
            task_id='extract_psaledger_lines',
            python_callable=extract_psaledger_lines_method,
        )

        load_lookup_tables = rail.PythonOperator(
            task_id='load_lookup_tables',
            python_callable=load_lookup_tables_method,
        )

        extract_unique_wbs1 = rail.PythonOperator(
            task_id='extract_unique_wbs1',
            python_callable=extract_unique_wbs1_method,
        )

        # Workato `014_503_psa_get_project_clients` helper recipe batches
        # WBS1 codes 10 at a time and iterates VP /api/project per batch.
        # A single `VantagepointProjectOperator` invocation can't loop —
        # so we use `VantagepointHook` directly inside a Python task to
        # preserve the batched-fetch behavior.
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

        firm_fallback_from_vp = rail.PythonOperator(
            task_id='firm_fallback_from_vp',
            python_callable=firm_fallback_from_vp_method,
        )

        validate_enriched_lines = rail.PythonOperator(
            task_id='validate_enriched_lines',
            python_callable=validate_enriched_lines_method,
        )

        build_journal_entry_body = rail.PythonOperator(
            task_id='build_journal_entry_body',
            python_callable=build_journal_entry_body_method,
        )

        create_journal_entry_in_qbo = rail.QuickBooksJournalEntryOperator(
            task_id='create_journal_entry_in_qbo',
            intuit_conn_id="{{ dag_run.conf.connections.intuit }}",
            request_body=lambda: rail.result('build_journal_entry_body'),
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

        (
            get_psaledger_lines_for_journal >>
            extract_psaledger_lines >>
            load_lookup_tables >>
            extract_unique_wbs1 >>
            get_project_clients_from_vp >>
            build_project_client_index >>
            enrich_lines >>
            firm_fallback_from_vp >>
            validate_enriched_lines >>
            build_journal_entry_body >>
            create_journal_entry_in_qbo
        )

        # one_failed fires on `failed` but NOT on `upstream_failed`. Without
        # these direct edges, a failure in any upstream task leaves the
        # tail tasks in upstream_failed — catch never runs, the dispatcher
        # gathers no error dict, and the watermark may advance past a
        # journal we never actually posted.
        get_psaledger_lines_for_journal >> catch_processor_dag_error
        extract_psaledger_lines >> catch_processor_dag_error
        load_lookup_tables >> catch_processor_dag_error
        extract_unique_wbs1 >> catch_processor_dag_error
        get_project_clients_from_vp >> catch_processor_dag_error
        build_project_client_index >> catch_processor_dag_error
        enrich_lines >> catch_processor_dag_error
        firm_fallback_from_vp >> catch_processor_dag_error
        validate_enriched_lines >> catch_processor_dag_error
        build_journal_entry_body >> catch_processor_dag_error
        create_journal_entry_in_qbo >> catch_processor_dag_error

        return dag


rail.for_each_instance(create_dag)
