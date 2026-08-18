from datetime import datetime


def convert_date(date_str):
    if not date_str:
        return None
    try:
        parsed_date = datetime.strptime(date_str, '%m/%d/%y')
        return parsed_date.strftime('%Y-%m-%d')
    except:
        return date_str


def clean_currency(value):
    try:
        cleaned = str(value or '0.00').replace(
            '$', '').replace(',', '').strip()
        return float(cleaned) if cleaned else 0.00
    except (ValueError, TypeError) as err:
        print(f"Error cleaning currency value '{value}': {err}")
        return 0.00
