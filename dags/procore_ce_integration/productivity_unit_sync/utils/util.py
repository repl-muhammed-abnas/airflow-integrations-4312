from datetime import datetime

from procore_ce_integration.productivity_unit_sync.config import (
    ce_date_format,
    procore_api_date_format
)


def extract_ce_code(origin_id):
    if not origin_id:
        return None

    origin_id_str = str(origin_id)
    if origin_id_str.startswith('CE_'):
        return origin_id_str[3:]
    return origin_id_str


def format_ce_date(date_str):
    if not date_str:
        return None

    try:
        # Try parsing with time component first
        if 'T' in date_str:
            date_obj = datetime.strptime(date_str, procore_api_date_format)
        else:
            # Parse simple date format
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')

        return date_obj.strftime(ce_date_format)
    except ValueError:
        # Return as-is if parsing fails
        return date_str


def validate_field_length(value, field_name, max_length, errors=None, context=None):
    if value is None:
        return True, None

    value_str = str(value)
    if len(value_str) > max_length:
        error_msg = f"Field '{field_name}' exceeds maximum length of {max_length} characters (value: '{value_str}', length: {len(value_str)})"

        if errors is not None and context is not None:
            errors.append({**context, 'error_message': error_msg, 'error_type': 'validation'})

        return False, error_msg

    return True, None


def truncate_notes(notes, max_length=255):
    if not notes:
        return ""

    notes_str = str(notes)
    if len(notes_str) > max_length:
        return notes_str[:max_length]
    return notes_str
