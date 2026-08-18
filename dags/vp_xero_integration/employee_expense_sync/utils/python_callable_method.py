""" methods for VP -> Xero Employee Expense Sync."""

# pylint: disable=invalid-name,broad-exception-caught
import logging
from urllib.parse import quote
import rail
from vp_xero_integration.common.python_callable_method import (
    collection_rows,
    collection_update,
    unwrap_vp_response,
)
from vp_xero_integration.common.tables import (
    OUTSTANDING_EMPLOYEE_EXPENSES_TABLE_NAME,
    OUTSTANDING_EMPLOYEE_EXPENSES_COLUMNS,
    MAP_EMPLOYEE_TABLE_NAME,
    MAP_EMPLOYEE_COLUMNS,
    MAP_CHART_OF_ACCOUNTS_TABLE_NAME,
    MAP_CHART_OF_ACCOUNTS_COLUMNS,
    MAP_TAX_CODE_TABLE_NAME,
    MAP_TAX_CODE_COLUMNS,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Dispatcher helpers
# ---------------------------------------------------------------------------

def build_vp_expense_poll_filter_method():
    timestamps = rail.result('prepare_sync_timestamps')
    last = quote(timestamps['last_sync_time'], safe='')
    current = quote(timestamps['current_sync_time'], safe='')
    gte = quote('>=', safe='')
    lt = quote('<', safe='')
    return (
        f"?filterHash[0][name]=TransDate"
        f"&filterHash[0][value]={last}"
        f"&filterHash[0][type]=datetime"
        f"&filterHash[0][opp]={gte}"
        f"&filterHash[0][condition]=AND"
        f"&filterHash[0][seq]=0"
        f"&filterHash[1][name]=TransDate"
        f"&filterHash[1][value]={current}"
        f"&filterHash[1][type]=datetime"
        f"&filterHash[1][opp]={lt}"
        f"&filterHash[1][seq]=1"
    )


def extract_expense_vouchers_method():
    """Deduplicate PSA Ledger results into unique (Period, PostSeq, Employee, Voucher) sets."""
    raw = rail.result('poll_expense_ledger')
    records = unwrap_vp_response(raw, strict=True)
    seen = {}
    for r in records:
        if not isinstance(r, dict):
            continue
        period = str(r.get('Period') or '').strip()
        post_seq = str(r.get('PostSeq') or '').strip()
        employee = str(r.get('Employee') or '').strip()
        voucher = str(r.get('Voucher') or '').strip()
        if not all([period, post_seq, employee]):
            continue
        key = (period, post_seq, employee, voucher)
        if key not in seen:
            seen[key] = {
                'Period': period,
                'PostSeq': post_seq,
                'Employee': employee,
                'Voucher': voucher,
                'Org': r.get('Org') or '',
                'TransDate': r.get('TransDate') or '',
            }
    vouchers = list(seen.values())
    return vouchers


def check_if_vouchers_exist_method():
    return len(rail.result('extract_expense_vouchers') or []) > 0


# ---------------------------------------------------------------------------
# Processor helpers
# ---------------------------------------------------------------------------

def build_vp_expense_lines_filter_method(**context):
    """Filter for processor: Period + PostSeq + Employee + Voucher to fetch only this voucher's lines.

    All four fields are required — they form the unique identity keyed by both the
    dispatcher (extract_expense_vouchers_method) and the dedup/result tables
    (check_already_exported_method, record_expense_result_method). Omitting Voucher
    would pull all lines for the employee's entire posting, causing duplicate Xero bills
    when one posting contains more than one expense report voucher.
    """
    conf = context['dag_run'].conf
    period = quote(str(conf.get('Period') or '').strip(), safe='')
    post_seq = quote(str(conf.get('PostSeq') or '').strip(), safe='')
    employee = quote(str(conf.get('Employee') or '').strip(), safe='')
    voucher_raw = str(conf.get('Voucher') or '').strip()
    voucher = quote(voucher_raw, safe='')
    eq = quote('==', safe='')

    if not period or not post_seq or not employee:
        raise RuntimeError(
            "Processor dag_run.conf missing Period/PostSeq/Employee — got "
            f"Period={period!r}, PostSeq={post_seq!r}, Employee={employee!r}. "
            "Refusing to query PSALedger."
        )

    filter_str = (
        f"?filterHash[0][name]=Period"
        f"&filterHash[0][value]={period}"
        f"&filterHash[0][type]=string"
        f"&filterHash[0][opp]={eq}"
        f"&filterHash[0][condition]=AND"
        f"&filterHash[0][seq]=0"
        f"&filterHash[1][name]=PostSeq"
        f"&filterHash[1][value]={post_seq}"
        f"&filterHash[1][type]=string"
        f"&filterHash[1][opp]={eq}"
        f"&filterHash[1][condition]=AND"
        f"&filterHash[1][seq]=1"
        f"&filterHash[2][name]=Employee"
        f"&filterHash[2][value]={employee}"
        f"&filterHash[2][type]=string"
        f"&filterHash[2][opp]={eq}"
        f"&filterHash[2][condition]=AND"
        f"&filterHash[2][seq]=2"
    )
    if voucher_raw:
        filter_str += (
            f"&filterHash[3][name]=Voucher"
            f"&filterHash[3][value]={voucher}"
            f"&filterHash[3][type]=string"
            f"&filterHash[3][opp]={eq}"
            f"&filterHash[3][seq]=3"
        )
    return filter_str


def check_already_exported_method(**context):
    """PythonOperator: return True if this voucher already has an InvoiceID recorded."""
    conf = context['dag_run'].conf
    period = (conf.get('Period') or '').strip()
    post_seq = (conf.get('PostSeq') or '').strip()
    employee = (conf.get('Employee') or '').strip()
    voucher = (conf.get('Voucher') or '').strip()
    rows = collection_rows(
        OUTSTANDING_EMPLOYEE_EXPENSES_TABLE_NAME,
        OUTSTANDING_EMPLOYEE_EXPENSES_COLUMNS,
        "Period = ? AND PostSeq = ? AND Employee = ? AND Voucher = ?",
        [period, post_seq, employee, voucher],
        context,
    )
    already_exported = bool(rows and (rows[0].get('InvoiceID') or '').strip())
    if already_exported:
        logger.info(
            "Skipping already-exported voucher: Period=%s PostSeq=%s "
            "Employee=%s Voucher=%s InvoiceID=%s",
            period, post_seq, employee, voucher, rows[0].get('InvoiceID'),
        )
    return already_exported


def should_skip_if_exported_method():
    return bool(rail.result('check_already_exported'))


def build_xero_bill_body_method(**context):
    conf = context['dag_run'].conf
    employee = (conf.get('Employee') or '').strip()
    voucher = (conf.get('Voucher') or '').strip()
    period = (conf.get('Period') or '').strip()
    post_seq = (conf.get('PostSeq') or '').strip()

    raw = rail.result('get_expense_lines')
    records = unwrap_vp_response(raw, strict=True)

    payable_lines = [
        r for r in records
        if isinstance(r, dict) and not (r.get('BankCode') or '').strip()
    ]

    if not payable_lines:
        logger.info(
            "No employee-payable lines for Period=%s PostSeq=%s "
            "Employee=%s Voucher=%s",
            period, post_seq, employee, voucher,
        )
        return None

    emp_rows = collection_rows(
        MAP_EMPLOYEE_TABLE_NAME,
        MAP_EMPLOYEE_COLUMNS,
        "Employee = ?",
        [employee],
        context,
    )
    if not emp_rows:
        raise ValueError(f"Employee '{employee}' not found in map_employee")
    rows_with_contact = [r for r in emp_rows if (r.get('ContactID') or '').strip()]
    if not rows_with_contact:
        raise ValueError(f"No ContactID mapped for Employee '{employee}'")
    contact_id = rows_with_contact[0]['ContactID'].strip()

    # Build XeroName -> TaxType lookup from the fetch_xero_tax_rates task result.
    tax_rates_raw = rail.result('fetch_xero_tax_rates') or {}
    tax_type_by_name = {
        rate.get('Name', ''): rate.get('TaxType', '')
        for rate in tax_rates_raw.get('TaxRates', [])
        if rate.get('Name')
    }
    logger.info("Loaded %d Xero TaxRates for TaxType resolution", len(tax_type_by_name))

    # Build Xero line items
    first = payable_lines[0]
    line_items = []
    for line in payable_lines:
        xero_account_code = _lookup_account_code(line.get('Account') or '', context)
        xero_tax_type = _lookup_tax_type(line.get('TaxCode') or '', tax_type_by_name, context)

        desc_parts = [
            (line.get('Desc1') or '').strip(),
            (line.get('Desc2') or '').strip(),
        ]
        description = ' '.join(p for p in desc_parts if p) or f"Expense {voucher}"
        amount = float(line.get('Amount') or 0)

        line_item = {
            'Description': description,
            'Quantity': 1.0,
            'UnitAmount': amount,
            'AccountCode': xero_account_code,
        }
        if xero_tax_type:
            line_item['TaxType'] = xero_tax_type
        line_items.append(line_item)

    invoice_date = (first.get('InvoiceDate') or first.get('TransDate') or '').strip()
    due_date = (first.get('DueDate') or invoice_date or '').strip()
    currency = (first.get('TransactionCurrencyCode') or 'USD').strip()

    bill_body = {
        'Type': 'ACCPAY',
        'Contact': {'ContactID': contact_id},
        'Date': invoice_date,
        'DueDate': due_date,
        'Reference': voucher,
        'CurrencyCode': currency,
        'Status': 'AUTHORISED',
        'LineItems': line_items,
    }
    logger.info(
        "Built Xero ACCPAY bill: Employee=%s Voucher=%s Period=%s "
        "ContactID=%s %d line item(s)",
        employee, voucher, period, contact_id, len(line_items),
    )
    return bill_body


def check_has_payable_lines_method():
    return rail.result('build_xero_bill_body') is not None


def record_expense_result_method(**context):
    """Write the Xero InvoiceID back to outstanding_employee_expenses."""
    conf = context['dag_run'].conf
    employee = (conf.get('Employee') or '').strip()
    period = (conf.get('Period') or '').strip()
    post_seq = (conf.get('PostSeq') or '').strip()
    voucher = (conf.get('Voucher') or '').strip()
    org = (conf.get('Org') or '').strip()
    trans_date = (conf.get('TransDate') or '').strip()

    xero_response = rail.result('create_bill_in_xero')
    invoice_id = ''
    messages = ''
    if xero_response:
        invoices = xero_response.get('Invoices') or []
        if invoices:
            invoice_id = (invoices[0].get('InvoiceID') or '').strip()
    if not invoice_id:
        messages = 'Xero bill created but InvoiceID not returned in response'
        logger.warning("No InvoiceID in Xero response for Voucher=%s", voucher)

    # Update existing row or insert a new one
    existing = collection_rows(
        OUTSTANDING_EMPLOYEE_EXPENSES_TABLE_NAME,
        OUTSTANDING_EMPLOYEE_EXPENSES_COLUMNS,
        "Period = ? AND PostSeq = ? AND Employee = ? AND Voucher = ?",
        [period, post_seq, employee, voucher],
        context,
    )
    if existing:
        rowid = existing[0].get('_rowid')
        collection_update(
            OUTSTANDING_EMPLOYEE_EXPENSES_TABLE_NAME,
            f"UPDATE {OUTSTANDING_EMPLOYEE_EXPENSES_TABLE_NAME} "
            "SET InvoiceID = ?, Messages = ? WHERE rowid = ?",
            [invoice_id, messages, rowid],
            context,
        )
        logger.info(
            "Updated outstanding_employee_expenses row: Employee=%s Voucher=%s InvoiceID=%s",
            employee, voucher, invoice_id,
        )
    else:
        collection_update(
            OUTSTANDING_EMPLOYEE_EXPENSES_TABLE_NAME,
            f"INSERT INTO {OUTSTANDING_EMPLOYEE_EXPENSES_TABLE_NAME} "
            "(Period, PostSeq, Employee, Org, TransactionDate, Voucher, "
            "OutstandingAmount, InvoiceID, Messages) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [period, post_seq, employee, org, trans_date, voucher, 0, invoice_id, messages],
            context,
        )
        logger.info(
            "Inserted outstanding_employee_expenses row: Employee=%s Voucher=%s InvoiceID=%s",
            employee, voucher, invoice_id,
        )

    return {'InvoiceID': invoice_id}


def capture_processor_error(employee, voucher, error_message):
    """Return error dict for GatherResultsFromDagRunsOperator aggregation."""
    return {
        'error': (
            f"Employee {employee} Voucher {voucher} - "
            f"expense export failed: {error_message}"
        )
    }

def _normalize_account_code(raw):
    """Normalize VP account codes (VP returns whole numbers as floats)."""
    code = (raw or '').strip()
    if not code:
        return ''
    if '.' in code:
        try:
            f = float(code)
            if f == int(f):
                return str(int(f))
        except ValueError:
            pass
    return code


def _lookup_account_code(vp_account_code, context=None):
    """Resolve VP Account code -> Xero account code from map_chart_of_accounts.

    VP returns account codes as decimal strings (e.g. '625.00'); normalizes to
    '625' before lookup. Raises ValueError when the account is not in the map so
    the data gap is visible — add the account to Xero and re-run mapping_sync.
    """
    code = _normalize_account_code(vp_account_code)
    if not code:
        raise ValueError("VP Account code is blank on expense line")
    rows = collection_rows(
        MAP_CHART_OF_ACCOUNTS_TABLE_NAME,
        MAP_CHART_OF_ACCOUNTS_COLUMNS,
        "VantagepointCode = ?",
        [code],
        context,
    )
    if rows:
        return (rows[0].get('XeroCode') or '').strip()
    raise ValueError(
        f"Account '{code}' not found in map_chart_of_accounts — "
        "add this account to Xero and re-run mapping_sync before retrying"
    )


def _lookup_tax_type(vp_tax_code, tax_type_by_name, context=None):
    """Resolve VP TaxCode -> Xero TaxType API code.

    map_tax_code.XeroCode stores the TaxComponent display name (e.g. 'Purchases Tax'),
    NOT the TaxType identifier the Xero API expects (e.g. 'INPUT'). The correct
    identifier comes from the live TaxRates response passed as tax_type_by_name
    (a dict of {RateName: TaxTypeCode} built from fetch_xero_tax_rates).
    """
    code = (vp_tax_code or '').strip()
    if code:
        rows = collection_rows(
            MAP_TAX_CODE_TABLE_NAME,
            MAP_TAX_CODE_COLUMNS,
            "VantagepointCode = ?",
            [code],
            context,
        )
        if rows:
            xero_name = (rows[0].get('XeroName') or '').strip()
            tax_type = tax_type_by_name.get(xero_name, '')
            if tax_type:
                return tax_type
            logger.warning(
                "TaxCode '%s' -> XeroName '%s' not found in live TaxRates; falling back",
                code, xero_name,
            )
        else:
            logger.warning("TaxCode '%s' not in map_tax_code; falling back", code)

    fallback = tax_type_by_name.get('Tax on Purchases', '')
    if not fallback:
        for name, ttype in tax_type_by_name.items():
            if 'purchase' in name.lower():
                fallback = ttype
                break
    if fallback:
        logger.info("Using fallback ACCPAY TaxType '%s' for VP TaxCode '%s'", fallback, code)
        return fallback
    logger.warning("No purchase-type TaxRate found; TaxType will be omitted for VP TaxCode '%s'", code)
    return ''
