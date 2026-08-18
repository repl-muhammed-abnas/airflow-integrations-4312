"""
Common utility methods for VP PSA -> QBO Posted AR Invoice Sync.

Translates 13 Workato recipes (014-503 PSA poll trigger + supporting
sub-recipes) into Python callables for the 4-DAG Airflow structure:
  main -> dispatcher -> router -> ar_invoice_create_us
                               -> ar_invoice_create_ca_uk

Regional routing (US vs CA/UK) mirrors the Workato CFG_Region IF branch:
  US:    per-line TaxCodeRef sourced from PSA Ledger TaxCode field.
  CA/UK: IsTaxGroup flag determines tax-group code vs NoTaxCodeID (from QBO).
  Routing decision is made in router_dag; each create DAG is region-specific.

Outstanding Sales Invoices: stored in the shared mapping_sync S3 collection
under the `outstanding_sales_invoices` table (per-customer SQLite).
"""
# pylint: disable=invalid-name,broad-exception-caught
import logging
import re
from datetime import datetime, timezone
from airflow.models import Variable
import rail
from vp_quickbooks_integration.posted_ar_invoice_sync.config import (
    initial_sync_time,
)
from vp_quickbooks_integration.common.python_callable_method import (
    collection_integration,
    collection_single_row,
    collection_rows,
    collection_update,
    watermark_key_template,
)
from vp_quickbooks_integration.common.tables import (
    MAP_FIRM_COLUMNS,
    MAP_FIRM_TABLE_NAME as map_firm_table_name,
    OUTSTANDING_SALES_INVOICES_TABLE_NAME as outstanding_sales_invoices_table_name,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
WATERMARK_VARIABLE_KEY_TEMPLATE = watermark_key_template('ar_invoice_sync')

_CUSTOMER_ID_SAFE_RE = re.compile(r'[^A-Za-z0-9_-]')


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------
def _sanitize_customer_id(customer_id):
    """Strip Airflow-Variable-unsafe chars; fall back to 'default' when empty."""
    if not customer_id:
        return 'default'
    cleaned = _CUSTOMER_ID_SAFE_RE.sub('_', str(customer_id))
    return cleaned or 'default'


def _watermark_variable_key(instance, customer_id):
    return WATERMARK_VARIABLE_KEY_TEMPLATE.format(
        instance=instance,
        customer_id=_sanitize_customer_id(customer_id),
    )


def _utc_now_iso():
    """ISO-8601 millisecond UTC timestamp with 'Z' suffix."""
    return (
        datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
        + 'Z'
    )


def _format_qbo_date(value):
    """Normalize VP date/datetime string to QBO 'YYYY-MM-DD' format."""
    if not value:
        return None
    if isinstance(value, str):
        return value.split('T')[0] if 'T' in value else value[:10]
    return str(value)


def _get_qbo_data(result):
    """
    Extract entity list/dict from a QuickBooksBaseOperator normalized result.
    Returns [] when result is absent or indicates failure.
    """
    if not result or not isinstance(result, dict):
        return []
    if not result.get('success', True):
        return []
    data = result.get('data')
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return [data]
    return []


def _query_mapping_row(task_id, query, query_params, table_name):  # pylint: disable=unused-argument
    return collection_single_row(query, query_params, read_task_id=task_id)


def _load_full_table(table_name):
    return collection_rows(table_name, MAP_FIRM_COLUMNS, '1=1', [])


def _upsert_outstanding_invoice(record):
    """Write one outstanding-sales-invoice record into the S3 collection.

    UPDATE existing row by natural key (DVPInvoice, WBS1, WBS2, WBS3);
    INSERT on miss. Uses collection_update (S3UpdateCollectionOperator) so
    all writes go through the canonical RAIL lock surface.
    """
    tn = outstanding_sales_invoices_table_name
    context = rail.get_current_context()
    result = collection_update(
        tn,
        f'UPDATE {tn} SET Batch=?, InvoiceAmount=?, OutstandingAmount=?, '
        f'TransactionDate=?, QBOInvoice=?, QBOID=? '
        f'WHERE DVPInvoice=? AND WBS1=? AND WBS2=? AND WBS3=?',
        [
            record['Batch'], record['InvoiceAmount'], record['OutstandingAmount'],
            record['TransactionDate'], record['QBOInvoice'], record['QBOID'],
            record['DVPInvoice'], record['WBS1'], record['WBS2'], record['WBS3'],
        ],
        context,
    )
    if not result.get('rows_affected', 0):
        collection_update(
            tn,
            f'INSERT INTO {tn} '
            f'(Batch, DVPInvoice, WBS1, WBS2, WBS3, InvoiceAmount, '
            f'OutstandingAmount, TransactionDate, QBOInvoice, QBOID) '
            f'VALUES (?,?,?,?,?,?,?,?,?,?)',
            [
                record['Batch'], record['DVPInvoice'],
                record['WBS1'], record['WBS2'], record['WBS3'],
                record['InvoiceAmount'], record['OutstandingAmount'],
                record['TransactionDate'], record['QBOInvoice'], record['QBOID'],
            ],
            context,
        )


# ---------------------------------------------------------------------------
# Watermark helpers
# Replaces Workato ModifiedDate-based trigger tracking.
# One Variable per (instance, customerId).
# ---------------------------------------------------------------------------
def prepare_sync_timestamps_method(instance):
    """Capture last sync time + current time for the OData PostDate filter."""
    customer_id = (
        rail.get_current_context()['dag_run'].conf.get('customerId')
    )
    key = _watermark_variable_key(instance, customer_id)
    current_time = _utc_now_iso()
    try:
        last_sync_time = Variable.get(key)
        print(f"Retrieved last sync time from Variable '{key}': {last_sync_time}")
    except KeyError:
        last_sync_time = initial_sync_time
        print(f"Variable '{key}' not found, using initial sync time: {last_sync_time}")
    return {
        'last_sync_time': last_sync_time,
        'current_sync_time': current_time,
    }


def update_last_sync_time_method(instance):
    """
    Persist `current_sync_time` after run completes (trigger_rule='all_done').

    Guard: if prepare_sync_timestamps skipped/failed, leave watermark unchanged.
    """
    try:
        timestamps = rail.result('prepare_sync_timestamps')
    except KeyError:
        timestamps = None
    if not isinstance(timestamps, dict) or not timestamps.get('current_sync_time'):
        print(
            "prepare_sync_timestamps did not produce a current_sync_time "
            "(skipped or failed); leaving watermark Variable unchanged."
        )
        return None

    customer_id = (
        rail.get_current_context()['dag_run'].conf.get('customerId')
    )
    key = _watermark_variable_key(instance, customer_id)
    current_time = timestamps['current_sync_time']
    Variable.set(key, current_time)
    print(f"Updated last sync time Variable '{key}' to: {current_time}")
    return current_time


# ---------------------------------------------------------------------------
# Dispatcher: PSA Ledger poll filter
# Replaces Workato `sales_invoice_created` trigger PostDate window.
# ---------------------------------------------------------------------------
def build_psa_ledger_filter_method():
    """OData filter selecting 'IN' rows within the current watermark window."""
    timestamps = rail.result('prepare_sync_timestamps')
    return (
        f"?$filter=PostDate ge datetime'{timestamps['last_sync_time']}'"
        f" and PostDate le datetime'{timestamps['current_sync_time']}'"
    )


def extract_invoice_batches_method():
    """
    Deduplicate PSA Ledger 'IN' rows to one item per unique Batch number.
    Replaces Workato trigger's per-batch fan-out.
    """
    records = rail.result('poll_psa_ledger') or []
    if not isinstance(records, list):
        return []
    seen = set()
    batches = []
    for r in records:
        batch = str(r.get('Batch') or '').strip()
        if batch and batch not in seen:
            seen.add(batch)
            batches.append({
                'Batch': batch,
                'PostDate': r.get('PostDate') or r.get('TransDate'),
            })
    print(
        f"Extracted {len(batches)} unique batch(es) "
        f"from {len(records)} PSA Ledger rows"
    )
    return batches


# ---------------------------------------------------------------------------
# Create DAG: PSA Ledger batch-specific filter
# ---------------------------------------------------------------------------
def build_invoice_batch_filter():
    """OData filter selecting all 'IN' rows for dag_run.conf.Batch."""
    batch = rail.get_current_context()['dag_run'].conf.get('Batch', '')
    return f"?$filter=Batch eq '{batch}'"


# ---------------------------------------------------------------------------
# Create DAG: VP Project fetch filter
# Mirrors Workato 'Get Project Clients' recipe — batched WBS1 filterHash.
# Matches unit_transaction_sync build_project_filter pattern.
# ---------------------------------------------------------------------------
def build_project_filter():
    """
    Build VP Project API filter for all unique WBS1 codes in the current batch.
    Uses URL-encoded filterHash params: one entry per distinct WBS1 value.
    """
    rows = rail.result('fetch_invoice_batch') or []
    unique_wbs1 = sorted({
        str(r.get('WBS1') or '').strip()
        for r in rows
        if r.get('WBS1')
    })
    if not unique_wbs1:
        print("build_project_filter: no WBS1 values in invoice batch")
        return '?fieldFilter=WBSNumber,WBS1,WBS2,WBS3,Name,ClientID'
    filter_parts = ['?fieldFilter=WBSNumber,WBS1,WBS2,WBS3,Name,ClientID']
    for idx, wbs1 in enumerate(unique_wbs1):
        filter_parts.append(
            f'&filterHash%5B{idx}%5D%5Bname%5D=WBS1'
            f'&filterHash%5B{idx}%5D%5Bvalue%5D={wbs1}'
        )
    print(f"build_project_filter: fetching projects for WBS1={unique_wbs1}")
    return ''.join(filter_parts)


# ---------------------------------------------------------------------------
# Create DAG: firm mapping lookup table
# Replaces Workato customer/firm lookup table for ClientID -> QBO CustomerID.
# Variable schema: list[{FirmID, QBOID, IsVendor (Y|N), Name}]
# ---------------------------------------------------------------------------
def get_firm_mapping_method(instance):
    """Return list of firm mapping rows from the shared mapping_sync S3 collection."""
    return _load_full_table(map_firm_table_name)


# ---------------------------------------------------------------------------
# Create DAG: region configuration
# Replaces Workato CFG_Region account property + IF routing block.
# ---------------------------------------------------------------------------
def fetch_region_config_method(instance):
    """
    Read CFG_Region from the integration config passed via dag_run.conf.
    Returns:
      {'region': 'us' or 'ca_uk', 'region_raw': str, 'no_tax_code_name': str or None}
    """
    config_data = (
        rail.get_current_context()['dag_run'].conf.get('config') or {}
    )
    region_raw = config_data.get('CFG_Region', 'US')
    region = str(region_raw).strip().upper()
    is_ca_uk = region in ('CA', 'UK', 'GB', 'CA_UK')
    no_tax_code_name = (
        config_data.get('CFG_NoTaxCode') or None
    ) if is_ca_uk else None
    print(
        f"Region config: region_raw={region_raw!r}, is_ca_uk={is_ca_uk}, "
        f"no_tax_code_name={no_tax_code_name!r}"
    )
    return {
        'region': 'ca_uk' if is_ca_uk else 'us',
        'region_raw': region,
        'no_tax_code_name': no_tax_code_name,
    }


def is_ca_uk_region_method():
    """True when region config indicates CA or UK (triggers NoTaxCodeID fetch)."""
    region_config = rail.result('fetch_region_config') or {}
    return region_config.get('region') == 'ca_uk'


def resolve_no_tax_code_id_method():
    """
    Extract NoTaxCodeID from QuickBooksTaxCodeOperator result (CA/UK only).
    US path: returns {'no_tax_code_id': None}.
    Replaces Workato 'NoTaxCodeID' variable initialization in CA/UK recipe.
    """
    try:
        result = rail.result('fetch_no_tax_code_id')
    except KeyError:
        return {'no_tax_code_id': None}

    items = _get_qbo_data(result)
    if items:
        no_tax_code_id = items[0].get('Id')
        print(f"Resolved NoTaxCodeID={no_tax_code_id!r} from QBO TaxCode search")
        return {'no_tax_code_id': no_tax_code_id}
    print("Warning: NoTaxCodeID not found in QBO TaxCode search result")
    return {'no_tax_code_id': None}


# ---------------------------------------------------------------------------
# Create DAG: QBO Sales item get-or-create
# Replaces Workato '014-503 PSA QuickBooks Sales Product ID' recipe.
# ---------------------------------------------------------------------------
def check_sales_item_exists_method():
    """True when 'Sales' item was found by search_sales_item."""
    result = rail.result('search_sales_item') or {}
    return len(_get_qbo_data(result)) > 0


def build_sales_item_body():
    """
    Body to create the 'Sales' service item when not already present in QBO.
    Mirrors Workato create_item action: Type=Service, income_account=Services.
    """
    return {
        'Name': 'Sales',
        'Type': 'Service',
        'Description': 'Sales',
        'Active': True,
        'Taxable': True,
        'IncomeAccountRef': {'name': 'Services'},
    }


def resolve_product_id_method():
    """
    Extract ProductID from search or create result.
    trigger_rule='none_failed_min_one_success' — only one branch ran.
    """
    # Try create result first (only present when item was just created).
    for task_id in ('create_sales_item', 'search_sales_item'):
        try:
            result = rail.result(task_id)
            items = _get_qbo_data(result)
            if items:
                item = items[0] if isinstance(items, list) else items
                pid = item.get('Id') if isinstance(item, dict) else None
                if pid:
                    print(f"Resolved ProductID={pid!r} from task '{task_id}'")
                    return {'Id': pid}
        except KeyError:
            continue
    print("Warning: could not resolve ProductID for 'Sales' item")
    return {'Id': None}


# ---------------------------------------------------------------------------
# Create DAG: invoice grouping
# Groups PSA Ledger rows by Invoice into header + lines structures.
# ---------------------------------------------------------------------------
def group_invoices_method():
    """
    Group PSA Ledger 'IN' rows by Invoice number.
    One batch can contain multiple invoices; each invoice has N line rows.
    Returns list of invoice dicts ready for ForEachOperator iteration.
    """
    rows = rail.result('fetch_invoice_batch') or []
    if not isinstance(rows, list):
        rows = [rows] if rows else []

    invoices = {}
    for row in rows:
        invoice_key = str(
            row.get('Invoice') or row.get('InvoiceNumber') or ''
        ).strip()
        if not invoice_key:
            continue
        if invoice_key not in invoices:
            invoices[invoice_key] = {
                'Invoice': invoice_key,
                'InvoiceNumber': str(
                    row.get('InvoiceNumber') or invoice_key
                ).strip(),
                'WBS1': str(row.get('WBS1') or '').strip(),
                'WBS2': str(row.get('WBS2') or '').strip(),
                'WBS3': str(row.get('WBS3') or '').strip(),
                'TransDate': row.get('TransDate'),
                'Period': str(row.get('Period') or '').strip(),
                'PostSeq': str(row.get('PostSeq') or '').strip(),
                'Lines': [],
                '_total': 0.0,
            }
        invoices[invoice_key]['Lines'].append(row)
        invoices[invoice_key]['_total'] += float(
            row.get('TransactionAmount') or 0
        )

    result = []
    for inv in invoices.values():
        inv['IsCreditMemo'] = inv['_total'] < 0
        del inv['_total']
        result.append(inv)

    print(
        f"Grouped {len(rows)} PSA Ledger rows into "
        f"{len(result)} invoice(s)"
    )
    return result


# ---------------------------------------------------------------------------
# Create DAG: per-invoice duplicate check (S3 collection)
# Replaces Workato 'Project Invoice Exists' recipe.
# ---------------------------------------------------------------------------
def check_invoice_exists_method():
    """
    Check the outstanding_sales_invoices S3 table for the current ForEach invoice.
    Returns {'exists': bool, 'qboid': str or None}.
    """
    invoice = rail.result('for_each_invoice') or {}
    dvp_invoice = str(invoice.get('Invoice') or '').strip()
    wbs1 = str(invoice.get('WBS1') or '').strip()
    wbs2 = str(invoice.get('WBS2') or '').strip()
    wbs3 = str(invoice.get('WBS3') or '').strip()

    row = _query_mapping_row(
        task_id='_check_outstanding_invoice',
        query=(
            f'SELECT QBOID FROM {outstanding_sales_invoices_table_name} '
            f'WHERE DVPInvoice = ? AND WBS1 = ? AND WBS2 = ? AND WBS3 = ? LIMIT 1'
        ),
        query_params=[dvp_invoice, wbs1, wbs2, wbs3],
        table_name=outstanding_sales_invoices_table_name,
    )
    if not row:
        return {'exists': False, 'qboid': None}

    qboid = row.get('QBOID') if isinstance(row, dict) else (row[0] if row else None)
    print(
        f"Invoice {dvp_invoice!r} already in outstanding_sales_invoices "
        f"(QBOID={qboid!r})"
    )
    return {'exists': True, 'qboid': qboid}


def is_new_invoice_method():
    """True when the current ForEach invoice is NOT in outstanding_sales_invoices."""
    result = rail.result('check_invoice_exists') or {}
    return not result.get('exists', False)


# ---------------------------------------------------------------------------
# Create DAG: QBO invoice body builders
# Replaces Workato 'Post Invoice to QuickBooks US/CA-UK' body assembly.
# ---------------------------------------------------------------------------
def _find_project(projects, wbs1, wbs2, wbs3):
    """Exact WBS1+WBS2+WBS3 match; falls back to WBS1-only."""
    w1 = str(wbs1 or '').strip()
    w2 = str(wbs2 or '').strip()
    w3 = str(wbs3 or '').strip()
    fallback = None
    for proj in projects:
        p1 = str(proj.get('WBS1') or '').strip()
        p2 = str(proj.get('WBS2') or '').strip()
        p3 = str(proj.get('WBS3') or '').strip()
        if p1 == w1 and p2 == w2 and p3 == w3:
            return proj
        if p1 == w1 and fallback is None:
            fallback = proj
    return fallback


def _find_firm_qboid(firm_map, client_id):
    """Map ClientID (FirmID) to QBO Customer ID via firm-map Variable."""
    if not client_id:
        return None
    cid = str(client_id).strip()
    for row in firm_map:
        if str(row.get('FirmID') or '').strip() == cid:
            return row.get('QBOID')
    return None


def _build_us_line(line, product_id):
    """One QBO SalesItemLine for US region — per-line TaxCodeRef."""
    amount = abs(float(line.get('TransactionAmount') or 0))
    return {
        'Amount': amount,
        'DetailType': 'SalesItemLineDetail',
        'Description': str(line.get('Desc1') or '').strip(),
        'SalesItemLineDetail': {
            'ItemRef': {'value': str(product_id)},
            'UnitPrice': amount,
            'Qty': 1,
            'TaxCodeRef': {
                'value': str(line.get('TaxCode') or 'TAX').strip()
            },
        },
    }


def _build_ca_uk_line(line, product_id, no_tax_code_id):
    """
    One QBO SalesItemLine for CA/UK region.
    IsTaxGroup=true  → use TaxGroupCodes as TaxCodeRef.
    IsTaxGroup=false → use NoTaxCodeID (or fallback 'NON').
    Mirrors Workato CA/UK foreach-line logic.
    """
    amount = abs(float(line.get('TransactionAmount') or 0))
    is_tax_group = str(
        line.get('IsTaxGroup') or 'false'
    ).strip().lower() in ('true', '1', 'yes')
    tax_group_codes = str(line.get('TaxGroupCodes') or '').strip()

    if is_tax_group and tax_group_codes:
        tax_code_value = tax_group_codes
    elif no_tax_code_id:
        tax_code_value = str(no_tax_code_id)
    else:
        tax_code_value = 'NON'

    return {
        'Amount': amount,
        'DetailType': 'SalesItemLineDetail',
        'Description': str(line.get('Desc1') or '').strip(),
        'SalesItemLineDetail': {
            'ItemRef': {'value': str(product_id)},
            'UnitPrice': amount,
            'Qty': 1,
            'TaxCodeRef': {'value': tax_code_value},
        },
    }


def _build_invoice_body(invoice, product_id, firm_map, projects, qbo_lines):
    """Shared invoice body assembly used by both US and CA/UK variants."""
    wbs1 = invoice.get('WBS1', '')
    wbs2 = invoice.get('WBS2', '')
    wbs3 = invoice.get('WBS3', '')

    project = _find_project(projects, wbs1, wbs2, wbs3)
    client_id = (project or {}).get('ClientID')
    qbo_customer_id = _find_firm_qboid(firm_map, client_id)

    if not qbo_customer_id:
        raise ValueError(
            f"No QBO customer found for Invoice={invoice.get('Invoice')!r}, "
            f"WBS1={wbs1!r}, ClientID={client_id!r}. "
            f"Add a row to the map_firm S3 collection for this customer (FirmID={client_id!r})."
        )

    body = {
        '_is_credit_memo': invoice.get('IsCreditMemo', False),
        'CustomerRef': {'value': str(qbo_customer_id)},
        'DocNumber': invoice.get('InvoiceNumber') or invoice.get('Invoice'),
        'TxnDate': _format_qbo_date(invoice.get('TransDate')),
        'Line': qbo_lines,
    }
    if not invoice.get('IsCreditMemo'):
        txn_date = body['TxnDate']
        if txn_date:
            body['DueDate'] = txn_date
    return body


def build_invoice_body_us_method():
    """
    Build QBO invoice/credit-memo body for the US create DAG.

    Reads from upstream task results:
      for_each_invoice      → current invoice header + lines
      fetch_project_clients → WBS -> ClientID project rows
      fetch_firm_mapping    → ClientID -> QBO CustomerID firm rows
      resolve_product_id    → QBO Item Id for 'Sales'

    Each line uses the TaxCode field from the PSA Ledger row directly.
    Returns body dict with internal '_is_credit_memo' flag.
    """
    invoice = rail.result('for_each_invoice') or {}
    product_id = (rail.result('resolve_product_id') or {}).get('Id') or ''
    projects = rail.result('fetch_project_clients') or []
    firm_map = rail.result('fetch_firm_mapping') or []

    qbo_lines = [
        _build_us_line(ln, product_id)
        for ln in (invoice.get('Lines') or [])
    ]
    return _build_invoice_body(invoice, product_id, firm_map, projects, qbo_lines)


def build_invoice_body_ca_uk_method():
    """
    Build QBO invoice/credit-memo body for the CA/UK create DAG.

    Reads from upstream task results:
      for_each_invoice        → current invoice header + lines
      fetch_project_clients   → WBS -> ClientID project rows
      fetch_firm_mapping      → ClientID -> QBO CustomerID firm rows
      resolve_product_id      → QBO Item Id for 'Sales'
      resolve_no_tax_code_id  → NoTaxCodeID resolved from QBO TaxCode search

    IsTaxGroup=true  → TaxGroupCodes used as TaxCodeRef.
    IsTaxGroup=false → NoTaxCodeID (or fallback 'NON') used as TaxCodeRef.
    Returns body dict with internal '_is_credit_memo' flag.
    """
    invoice = rail.result('for_each_invoice') or {}
    product_id = (rail.result('resolve_product_id') or {}).get('Id') or ''
    no_tax_code_id = (
        rail.result('resolve_no_tax_code_id') or {}
    ).get('no_tax_code_id')
    projects = rail.result('fetch_project_clients') or []
    firm_map = rail.result('fetch_firm_mapping') or []

    qbo_lines = [
        _build_ca_uk_line(ln, product_id, no_tax_code_id)
        for ln in (invoice.get('Lines') or [])
    ]
    return _build_invoice_body(invoice, product_id, firm_map, projects, qbo_lines)


def is_credit_memo_method():
    """True when the current invoice has a negative total (IsCreditMemo flag)."""
    body = rail.result('build_invoice_body') or {}
    return bool(body.get('_is_credit_memo', False))


def get_invoice_body_for_create():
    """Strip internal '_is_credit_memo' flag before sending body to QBO operator."""
    body = dict(rail.result('build_invoice_body') or {})
    body.pop('_is_credit_memo', None)
    return body


# ---------------------------------------------------------------------------
# Create DAG: outstanding invoices S3 collection upsert
# Replaces Workato lookup-table add/update after successful QBO invoice post.
# ---------------------------------------------------------------------------
def update_outstanding_invoice_pg_method():
    """
    Upsert current invoice into the outstanding_sales_invoices S3 table after
    a successful QBO post. Reads QBO result from whichever create task ran.
    trigger_rule='none_failed_min_one_success' ensures one branch supplies it.
    """
    conf = rail.get_current_context()['dag_run'].conf
    invoice = rail.result('for_each_invoice') or {}
    batch = str(conf.get('Batch', ''))

    qbo_data = {}
    for task_id in ('create_qbo_invoice', 'create_qbo_credit_memo'):
        try:
            result = rail.result(task_id)
            if result:
                items = _get_qbo_data(result)
                qbo_data = items[0] if isinstance(items, list) and items else {}
                break
        except KeyError:
            continue

    qbo_id = str(qbo_data.get('Id') or '')
    qbo_invoice_number = str(
        qbo_data.get('DocNumber')
        or invoice.get('InvoiceNumber')
        or invoice.get('Invoice')
        or ''
    )
    total_amount = sum(
        abs(float(ln.get('TransactionAmount') or 0))
        for ln in (invoice.get('Lines') or [])
    )
    trans_date = _format_qbo_date(invoice.get('TransDate'))

    dvp_invoice = str(invoice.get('Invoice') or '').strip()
    wbs1 = str(invoice.get('WBS1') or '').strip()
    wbs2 = str(invoice.get('WBS2') or '').strip()
    wbs3 = str(invoice.get('WBS3') or '').strip()

    record = {
        'Batch': batch,
        'DVPInvoice': dvp_invoice,
        'WBS1': wbs1,
        'WBS2': wbs2,
        'WBS3': wbs3,
        'InvoiceAmount': total_amount,
        'OutstandingAmount': total_amount,
        'TransactionDate': trans_date,
        'QBOInvoice': qbo_invoice_number,
        'QBOID': qbo_id,
    }
    _upsert_outstanding_invoice(record)
    print(
        f"Upserted outstanding invoice: Invoice={dvp_invoice!r}, QBOID={qbo_id!r}"
    )


# ---------------------------------------------------------------------------
# Create DAG: revenue recognition JournalEntry
# Replaces Workato '014-503 PSA Vantagepoint Revenue Generation posts to
# QuickBooks' recipe (create_journal_entry_v2 leaf).
# Called once per batch after all invoices are processed.
# ---------------------------------------------------------------------------
def build_revenue_journal_entry_body_method():
    """
    Build QBO JournalEntry body for revenue recognition.

    DocNumber = {Period}-{PostSeq} (mirrors Workato recipe L7819).
    Debit:  AR account (AcctsReceivable from CFGAutoPosting).
    Credit: Uninvoiced Revenue account (UninvoicedRevenue from CFGAutoPosting).
    Amount: sum of absolute TransactionAmounts across all batch rows.

    Returns None when the batch is empty or total is zero (skipped by operator).
    """
    rows = rail.result('fetch_invoice_batch') or []
    if not isinstance(rows, list):
        rows = [rows] if rows else []
    if not rows:
        print("Revenue journal entry: no invoice rows in batch, skipping")
        return None

    accounts = rail.result('fetch_revenue_accounts') or {}
    uninvoiced_revenue_name = (
        accounts.get('UninvoicedRevenueName')
        or accounts.get('UninvoicedRevenue')
        or 'Unbilled Accounts Receivable'
    )
    ar_account_name = (
        accounts.get('AcctsReceivableName')
        or accounts.get('AcctsReceivable')
        or 'Accounts Receivable (A/R)'
    )

    first_row = rows[0]
    period = str(first_row.get('Period') or '').strip()
    post_seq = str(first_row.get('PostSeq') or '').strip()
    trans_date = _format_qbo_date(first_row.get('TransDate')) or ''
    batch = str(first_row.get('Batch') or '').strip()

    if not (period and post_seq):
        print(
            f"Revenue journal entry: missing Period={period!r} or "
            f"PostSeq={post_seq!r} — skipping"
        )
        return None

    total = sum(abs(float(r.get('TransactionAmount') or 0)) for r in rows)
    if total == 0:
        print("Revenue journal entry: total amount is 0, skipping")
        return None

    description = (
        f"AR Revenue recognition — Batch {batch}, "
        f"Period {period}, PostSeq {post_seq}"
    )
    return {
        'TxnDate': trans_date,
        'DocNumber': f"{period}-{post_seq}",
        'PrivateNote': (
            f"vp_psa:Batch={batch};Period={period};PostSeq={post_seq}"
        ),
        'Line': [
            {
                'Amount': total,
                'DetailType': 'JournalEntryLineDetail',
                'Description': description,
                'JournalEntryLineDetail': {
                    'PostingType': 'Debit',
                    'AccountRef': {'Name': ar_account_name},
                },
            },
            {
                'Amount': total,
                'DetailType': 'JournalEntryLineDetail',
                'Description': description,
                'JournalEntryLineDetail': {
                    'PostingType': 'Credit',
                    'AccountRef': {'Name': uninvoiced_revenue_name},
                },
            },
        ],
    }


# ---------------------------------------------------------------------------
# Create DAG: error capture
# Replaces the catch{} block in Workato US/CA-UK export recipes.
# ---------------------------------------------------------------------------
def capture_create_dag_error(batch, fallback_error_message):
    """
    Terminal catch task for ar_invoice_create_us_dag / ar_invoice_create_ca_uk_dag.
    Always returns (never raises) so WaitForDagRunsSensor sees DAG as
    SUCCESS and GatherResultsFromDagRunsOperator can collect the error dict.
    Returns None on a clean run (fallback_error_message is empty).
    """
    if not fallback_error_message:
        return None
    return {
        'error': (
            f"AR Invoice Batch {batch!r} — "
            f"create failed: {fallback_error_message}"
        ),
        'Batch': batch,
    }


# ---------------------------------------------------------------------------
# Router DAG helpers
# ---------------------------------------------------------------------------
def resolve_triggered_runs_method():
    """
    Return the dag_run reference list from whichever regional create DAG
    was triggered (trigger_us_create or trigger_ca_uk_create).
    Used as the dag_runs source for WaitForDagRunsSensor and
    GatherResultsFromDagRunsOperator in the router DAG.
    """
    for task_id in ('trigger_us_create', 'trigger_ca_uk_create'):
        try:
            result = rail.result(task_id)
            if result:
                return result
        except KeyError:
            continue
    return []


def capture_router_dag_error(batch, fallback_error_message):
    """
    Terminal catch task for router_dag.
    Aggregates errors from the nested create DAG run (via
    gather_create_dag_errors) and any router-level failure, then surfaces
    them to the dispatcher's GatherResultsFromDagRunsOperator.
    Returns None on a clean run.
    """
    messages = []
    try:
        gathered = rail.result('gather_create_dag_errors') or []
        for entry in gathered:
            if entry:
                messages.append(entry.get('error', str(entry)))
    except KeyError:
        pass

    if fallback_error_message:
        messages.append(f"Router error: {fallback_error_message}")

    if not messages:
        return None
    return {
        'error': ' | '.join(messages),
        'Batch': batch,
    }
