from datetime import datetime


# Convert date format from M/D/YYYY to YYYY-MM-DD
def convert_date(date_str):
    if not date_str:
        return None
    try:
        parsed_date = datetime.strptime(date_str, '%m/%d/%Y')
        return parsed_date.strftime('%Y-%m-%d')
    except:
        return date_str


def clean_currency(value):
    try:
        return str(value or '0.00').replace('$', '').replace(',', '').strip()
    except (ValueError, TypeError) as err:
        print(err)
        return '0.00'

def build_flat_code(phase, category, cost_type):
    if phase and category:
        return f"{phase}-{category}.{cost_type}"
    if phase or category:
        return f"{phase or category}.{cost_type}"
    return None
