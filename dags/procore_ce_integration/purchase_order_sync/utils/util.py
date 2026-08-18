from datetime import datetime
from procore_ce_integration.purchase_order_sync.config import CE_FIELD_LENGTHS
from procore_ce_integration.purchase_order_sync.utils.constants import ErrorType
import re


# ComputerEase Field Formatting Utilities
def strip_html_tags(text):
    return re.sub(r'<[^>]+>', '', str(text)).strip() if text else ''


def format_ce_date(date_str):
    """
    Convert ISO date to CE format (MM/DD/YYYY or 2/2/4 format).
    Handles various input formats from Procore API.
    """
    if not date_str:
        return ''

    try:
        if 'T' in str(date_str):
            dt = datetime.fromisoformat(str(date_str).replace('Z', '+00:00'))
        else:
            dt = datetime.strptime(str(date_str), '%Y-%m-%d')
        return dt.strftime('%m/%d/%Y')
    except (ValueError, TypeError, AttributeError):
        return str(date_str) if date_str else ''


def split_description(text, max_lines=10, line_length=30):
    """
    Split long text into multiple lines for CE import.
    Used for item descriptions (itemdes1-10 fields).

    Args:
        text: Text to split
        max_lines: Maximum number of lines (default 10)
        line_length: Max length of each line (default 30 chars)

    Returns:
        Dictionary with itemdes1 through itemdes10 keys, ready to spread into payload
    """
    if not text:
        lines = ['' for _ in range(max_lines)]
    else:
        text = str(text).strip()

        lines = []
        words = text.split()
        current_line = ''

        for word in words:
            test_line = current_line + (' ' if current_line else '') + word

            if len(test_line) <= line_length:
                current_line = test_line
            else:
                if current_line:
                    lines.append(current_line)

                if len(word) > line_length:
                    lines.append(word[:line_length])
                    current_line = ''
                else:
                    current_line = word

            if len(lines) >= max_lines:
                break

        if current_line and len(lines) < max_lines:
            lines.append(current_line)

        while len(lines) < max_lines:
            lines.append('')

        lines = lines[:max_lines]

    return {
        f'itemdes{i+1}': lines[i].strip()
        for i in range(max_lines)
    }


def split_address(address_data, max_lines=4, line_length=30):
    """
    Split address into multiple CE address lines (poshipto1-4).

    Args:
        address_data: Dictionary with address components OR string (HTML or plain text)
        max_lines: Number of address lines (default 4)
        line_length: Max length of each line (default 30)

    Returns:
        Dictionary with poshipto1 through poshipto4 keys, ready to spread into payload
    """
    if not address_data:
        lines = ['' for _ in range(max_lines)]
    else:
        lines = []

        if isinstance(address_data, str):
            clean_address = re.sub(r'<[^>]+>', '', address_data).strip()

            if clean_address:
                address_lines = [
                    line.strip() for line in clean_address.split('\n') if line.strip()]
                lines.extend(address_lines[:max_lines])

        elif isinstance(address_data, dict):
            if address_data.get('address_1'):
                lines.append(str(address_data['address_1']))
            if address_data.get('address_2'):
                lines.append(str(address_data['address_2']))

            city_state_zip = []
            if address_data.get('city'):
                city_state_zip.append(str(address_data['city']))
            if address_data.get('state_code'):
                city_state_zip.append(str(address_data['state_code']))
            if address_data.get('zip'):
                city_state_zip.append(str(address_data['zip']))

            if city_state_zip:
                lines.append(', '.join(city_state_zip))

            if address_data.get('country') and address_data['country'].upper() != 'USA':
                lines.append(str(address_data['country']))

        result_lines = []
        for i in range(max_lines):
            if i < len(lines):
                result_lines.append(lines[i][:line_length] if lines[i] else '')
            else:
                result_lines.append('')

        lines = result_lines

    return {
        f'poshipto{i+1}': lines[i].strip()
        for i in range(max_lines)
    }


def validate_field_length(value, field_name, max_length, errors_list, po_id, line_item_id=None):
    if value is None or value == '':
        return ''

    str_value = str(value).strip()

    if len(str_value) > max_length:
        identifier = f"Line item {line_item_id}: " if line_item_id else ""
        errors_list.append({
            'purchase_order_id': po_id,
            'error_message': f"{identifier}Field '{field_name}' value '{str_value}' exceeds maximum length of {max_length} characters.",
            'error_type': ErrorType.SKIP
        })
        return str_value[:max_length]

    return str_value


def validate_computerease_record(computerease_record):
    ponum = computerease_record.get('ponum', '').strip()
    if not ponum:
        return False, 'Missing PO number'
    if len(ponum) > CE_FIELD_LENGTHS['ponum']:
        return False, 'PO number exceeds character limit'

    povennum = computerease_record.get('povennum', '').strip()
    if not povennum:
        return False, 'Missing vendor number'
    if len(povennum) > CE_FIELD_LENGTHS['povennum']:
        return False, 'Vendor number exceeds character limit'

    return True, None
