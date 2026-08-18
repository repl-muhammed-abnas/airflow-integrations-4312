from datetime import datetime
from decimal import Decimal, InvalidOperation


def format_decimal(value, blank_if_zero=False):
    """Format decimal per ADP spec: strip trailing zeros, never scientific notation, empty for None/blank."""
    if value is None or value == '':
        return ''
    try:
        d = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return str(value)
    if blank_if_zero and d == 0:
        return ''
    s = format(d, 'f')
    if '.' in s:
        s = s.rstrip('0').rstrip('.')
    return s or '0'


def format_integer(value):
    """Format whole-number fields (e.g. RECCT): no decimals, empty for None/blank."""
    if value is None or value == '':
        return ''
    try:
        return str(int(Decimal(str(value))))
    except (InvalidOperation, ValueError):
        return str(value)


def get_compose_payroll_detail_row(items):
    return [
        items.get('CustRECTY', ''),
        items.get('CustCLIID', ''),
        items.get('CustINTCA', ''),
        items.get('CustORDNO', ''),
        items.get('CustIOPER', ''),
        items.get('CustINFTY', ''),
        items.get('CustSUBTY', ''),
        items.get('CustBEGDA', ''),
        items.get('CustENDDA', ''),
        items.get('CustOBJPS', ''),
        items.get('CustSPRPS', ''),
        items.get('CustSEQNR', ''),
        items.get('CustEXTRA', ''),
        items.get('CustLGART', ''),
        format_decimal(items.get('CustSTDAZ', '')),
        items.get('CustBEGUZ', ''),
        items.get('CustENDUZ', ''),
        format_decimal(items.get('CustBETRG', ''), blank_if_zero=True),
        items.get('CustWAERS', ''),
        format_decimal(items.get('CustANZHL', '')),
        items.get('CustZEINH', ''),
        items.get('CustVTKEN', ''),
        format_decimal(items.get('CustBWGRL', ''), blank_if_zero=True),
        items.get('CustAUFKZ', ''),
        items.get('CustENDOF', ''),
        items.get('CustUFLD1', ''),
        items.get('CustUFLD2', ''),
        items.get('CustUFLD3', ''),
        items.get('CustKEYPR', ''),
        items.get('CustTRFGR', ''),
        items.get('CustTRFST', ''),
        items.get('CustPRAKN', ''),
        items.get('CustPRAKZ', ''),
        items.get('CustOTYPE', ''),
        items.get('CustPLANS', ''),
        items.get('CustVERSL', ''),
        items.get('CustEXBEL', ''),
        items.get('CustWTART', ''),
        items.get('CustTDLANGU', ''),
        items.get('CustTDSUBLA', ''),
        items.get('CustTDTYPE', ''),
    ]


def get_file_name(dag_run):
    return dag_run.conf.get('PayrollFileName', 'payroll_export.SAP')


def export_started_payload(dag_run):
    return {
        'CustExportStarted': datetime.utcnow().isoformat(),
    }


def export_complete_payload(dag_run):
    return {
        'CustExportComplete': datetime.utcnow().isoformat(),
    }


def build_error_payload(error_message, dag_run):
    return {
        'CustErrorMessage': f'{error_message}'[:500] if error_message else '',
    }
