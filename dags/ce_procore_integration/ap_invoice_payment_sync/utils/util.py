import re
from datetime import datetime


def build_unique_key(company_id, check_number, voucher_number):
    return f"COMPANY_{company_id}_CHECK_{check_number}_VOUCHER_{voucher_number}"


_UNIQUE_KEY_RE = re.compile(
    r'^COMPANY_(?P<company>.*?)_CHECK_(?P<check>.*?)_VOUCHER_(?P<voucher>.*)$'
)


def parse_unique_key(unique_key):
    match = _UNIQUE_KEY_RE.match(unique_key or '')
    if not match:
        return ('', '', '')
    return (match.group('company'), match.group('check'), match.group('voucher'))


def convert_date(date_str):
    """
    Convert date from M/D/YYYY or M/D/YY format to YYYY-MM-DD format.

    Args:
        date_str: Date string in M/D/YYYY or M/D/YY format

    Returns:
        Date string in YYYY-MM-DD format, or empty string if invalid/empty
    """
    if not date_str or not isinstance(date_str, str) or date_str.strip() == '':
        return ''

    date_str = date_str.strip()

    # Try different date formats
    for fmt in ['%m/%d/%Y', '%m/%d/%y', '%Y-%m-%d']:
        try:
            parsed_date = datetime.strptime(date_str, fmt)
            return parsed_date.strftime('%Y-%m-%d')
        except ValueError:
            continue

    # If no format matches, return as-is (better than losing data)
    return date_str


def clean_currency(value):
    """
    Remove currency symbols and commas, return clean decimal string.

    Args:
        value: Currency value (string or number)

    Returns:
        Clean decimal string (e.g., "1500.00")
    """
    if value is None or value == '':
        return '0.00'

    try:
        # Convert to string and clean
        value_str = str(value).replace('$', '').replace(',', '').strip()

        # Handle empty string after cleaning
        if value_str == '':
            return '0.00'

        # Try to convert to float and back to ensure valid number
        float_val = float(value_str)
        return f'{float_val:.2f}'
    except (ValueError, TypeError):
        return '0.00'

def get_error_message(err):
    if type(err) == str:
        status = 'Error'
        reason = err
    else:
        status = err['response']['status_code'] \
            if err.get('response') else 'Error'
        reason = err['response']['json']['error']['reason'] \
            if err.get('response') else err
    return {
        'status': status,
        'reason': reason
    }