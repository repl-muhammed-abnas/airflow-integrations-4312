"""
Sand Tech Inc - User Import Custom Methods
Helper functions for data transformation and processing
"""

import hashlib
from datetime import datetime


def generate_md5_hash(fields_list):
    """
    Generate MD5 hash from a list of field values
    Used for change detection between file imports
    """
    hash_string = '_'.join([str(f or '').strip() for f in fields_list])
    return hashlib.md5(hash_string.encode()).hexdigest()


def parse_date_string(date_str, date_format='%d/%m/%Y'):
    """
    Parse date string to Replicon date format dict
    Returns None if date_str is empty or invalid
    
    Args:
        date_str: Date string to parse (e.g., "01/03/2025")
        date_format: Format string (default DD/MM/YYYY)
    
    Returns:
        dict with year, month, day keys or None
    """
    if not date_str or not str(date_str).strip():
        return None
    try:
        parsed = datetime.strptime(str(date_str).strip(), date_format)
        return {
            'year': parsed.year,
            'month': parsed.month,
            'day': parsed.day
        }
    except ValueError:
        return None


def format_date_for_display(date_dict):
    """
    Format Replicon date dict to display string
    
    Args:
        date_dict: Dict with year, month, day keys
    
    Returns:
        Formatted date string (DD/MM/YYYY)
    """
    if not date_dict:
        return ''
    try:
        return f"{date_dict['day']:02d}/{date_dict['month']:02d}/{date_dict['year']}"
    except (KeyError, TypeError):
        return ''


def normalize_email(email):
    """
    Normalize email address for comparison
    Strips whitespace and converts to lowercase
    """
    if not email:
        return ''
    return str(email).strip().lower()


def normalize_boolean(value):
    """
    Normalize Yes/No values to boolean
    
    Args:
        value: String like "Yes", "No", "Y", "N", "true", "false"
    
    Returns:
        Boolean True/False or None if invalid
    """
    if not value:
        return None
    val = str(value).strip().lower()
    if val in ('yes', 'y', 'true', '1'):
        return True
    if val in ('no', 'n', 'false', '0'):
        return False
    return None


def get_uri_by_name(items, name_field, name_value, uri_field='uri'):
    """
    Find URI by name in a list of items
    
    Args:
        items: List of dicts to search
        name_field: Field name containing the name to match
        name_value: Value to match (case-insensitive)
        uri_field: Field name containing the URI (default 'uri')
    
    Returns:
        URI string or None if not found
    """
    if not items or not name_value:
        return None
    
    name_lower = str(name_value).strip().lower()
    
    for item in items:
        # Check the specified name field
        if item.get(name_field, ''):
            if str(item[name_field]).strip().lower() == name_lower:
                return item.get(uri_field)
        
        # Also check displayText as fallback (common in Replicon responses)
        if item.get('displayText', ''):
            if str(item['displayText']).strip().lower() == name_lower:
                return item.get('uri')
        
        # Also check name field as fallback
        if item.get('name', ''):
            if str(item['name']).strip().lower() == name_lower:
                return item.get('uri')
    
    return None


def get_item_by_uri(items, uri):
    """
    Find item by URI in a list of items
    
    Args:
        items: List of dicts to search
        uri: URI to match
    
    Returns:
        Item dict or None if not found
    """
    if not items or not uri:
        return None
    
    for item in items:
        if item.get('uri') == uri:
            return item
    
    return None


def build_user_record_from_csv_row(row):
    """
    Transform CSV row dict to standardized user record format
    
    Args:
        row: Dict from CSV with HiBob column names
    
    Returns:
        Dict with standardized field names
    """
    return {
        'employee_id': str(row.get('Employee ID', '') or '').strip(),
        'first_name': str(row.get('First name', '') or '').strip(),
        'last_name': str(row.get('Last name', '') or '').strip(),
        'display_name': str(row.get('Display name', '') or '').strip(),
        'email': str(row.get('Email', '') or '').strip(),
        'start_date': str(row.get('Start date', '') or '').strip(),
        'last_day_of_work': str(row.get('Last day of work', '') or '').strip(),
        'job_title': str(row.get('Job title', '') or '').strip(),
        'job_title_effective_date': str(row.get('Job title/Effective date', '') or '').strip(),
        'manager_email': str(row.get("Manager's email", '') or '').strip(),
        'reports_to_effective_date': str(row.get('Reports to/Effective date', '') or '').strip(),
        'department': str(row.get('Department', '') or '').strip(),
        'department_effective_date': str(row.get('Department/Effective date', '') or '').strip(),
        'site': str(row.get('Site', '') or '').strip(),
        'site_effective_date': str(row.get('Site/Effective date', '') or '').strip(),
        'is_a_manager': str(row.get('Is a manager', '') or '').strip()
    }


def validate_mandatory_fields(record):
    """
    Validate that mandatory fields are present
    
    Args:
        record: User record dict
    
    Returns:
        Tuple (is_valid: bool, missing_fields: list)
    """
    mandatory_fields = ['employee_id', 'first_name', 'last_name', 'email', 'start_date']
    missing = []
    
    for field in mandatory_fields:
        value = record.get(field, '')
        if not value or not str(value).strip():
            missing.append(field)
    
    return (len(missing) == 0, missing)


def generate_user_md5(record):
    """
    Generate MD5 hash for a user record
    Uses all 16 HiBob fields for change detection
    
    Args:
        record: User record dict with standardized field names
    
    Returns:
        MD5 hash string
    """
    fields = [
        record.get('employee_id', ''),
        record.get('first_name', ''),
        record.get('last_name', ''),
        record.get('display_name', ''),
        record.get('email', ''),
        record.get('start_date', ''),
        record.get('last_day_of_work', ''),
        record.get('job_title', ''),
        record.get('job_title_effective_date', ''),
        record.get('manager_email', ''),
        record.get('reports_to_effective_date', ''),
        record.get('department', ''),
        record.get('department_effective_date', ''),
        record.get('site', ''),
        record.get('site_effective_date', ''),
        record.get('is_a_manager', '')
    ]
    return generate_md5_hash(fields)


def determine_login_status(start_date, last_day_of_work):
    """
    Determine if user should be enabled or disabled based on dates
    
    Rules:
    - Start Date present + Last day of work empty = Enable
    - Start Date present + Last day of work has value = Disable
    
    Args:
        start_date: Start date string
        last_day_of_work: Last day of work string
    
    Returns:
        Tuple (should_enable: bool, should_set_end_date: bool)
    """
    has_start = bool(start_date and str(start_date).strip())
    has_end = bool(last_day_of_work and str(last_day_of_work).strip())
    
    if has_start and not has_end:
        return (True, False)  # Enable user, don't set end date
    elif has_start and has_end:
        return (False, True)  # Disable user, set end date
    else:
        return (None, False)  # Can't determine, no changes


def format_log_properties(record, action, status, details, ecid):
    """
    Format standard log properties dict
    
    Args:
        record: User record dict
        action: Action being performed (Add/Update/Supervisor Assignment)
        status: Status (Success/Exception/Error/Ignored)
        details: Detail message
        ecid: DAG run ECID
    
    Returns:
        Dict of log properties
    """
    return {
        "Empid": record.get('employee_id', ''),
        "Username": f"{record.get('first_name', '')} {record.get('last_name', '')}".strip(),
        "Action": action,
        "Status": status,
        "Details": details,
        "Jobid": ecid
    }
