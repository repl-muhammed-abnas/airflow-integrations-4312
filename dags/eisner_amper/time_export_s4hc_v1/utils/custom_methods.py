from datetime import timedelta, datetime
import pendulum


def logging_details(instance, timezone='America/Los_Angeles'):
    current_time = pendulum.now(tz= timezone)
    return {
        "current_date": (current_time).strftime("%d%m%Y%H%M%S"),
        "filter_date": (current_time).strftime("%m/%d/%Y") if instance == "afmig" else (current_time - timedelta(days=1)).strftime("%m/%d/%Y"),
        "file_name_format": (current_time - timedelta(days=1)).strftime("%d%m%Y") + current_time.strftime("%H%M%S")
    }

def get_user_timezone(response):
    if not response:
        return []
    return {
        "time_zone":response[0]['timeZone']['displayText'],
        "user_uri": response[0]['userDetails']['uri']
    }

def safe_parse_date(date_str, input_format='%b %d, %Y', output_format='%Y-%m-%d'):
    if not date_str:
        return ''
    try:
        return datetime.strptime(date_str, input_format).strftime(output_format)
    except Exception as e:
        return ''

def build_lookup_dict(data_list, key_field, value_field):
    if not data_list:
        return {}
    return {
        item.get(key_field, ''): item.get(value_field, '')
        for item in data_list
        if item.get(key_field)
    }

def extract_service_line(cost_center_code, start_idx=4, end_idx=7):
    """Extract service line from cost center code (positions 4-7 by default)"""
    if not cost_center_code or len(cost_center_code) < end_idx:
        return None
    return cost_center_code[start_idx:end_idx]

def extract_location_code(cost_center_code, start_idx=7, end_idx=10):
    """Extract location code from cost center code (positions 7-10 by default)"""
    if not cost_center_code or len(cost_center_code) < end_idx:
        return None
    return cost_center_code[start_idx:end_idx]

def build_mapper_lookup(mapper_list, key_field, value_field):
    if not mapper_list:
        return {}
    return {
        item.get(key_field): item.get(value_field)
        for item in mapper_list
        if item.get(key_field) is not None and item.get(value_field) is not None
    }
