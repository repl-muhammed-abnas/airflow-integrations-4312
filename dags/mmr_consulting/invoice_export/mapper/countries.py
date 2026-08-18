"""Geographic routing and currency mappings."""

TRACKING_CATEGORY_NAME = 'BC' # Referenced by countries


COUNTRIES = {
    'us': {
        'client_country': 'United States',
        'line_amount_types': 'Inclusive',
        'tax_type': 'NONE',
        'tracking_category_name': TRACKING_CATEGORY_NAME,
    },
    'canada': {
        'client_country': 'Canada',
        'line_amount_types': 'Exclusive',
        'tax_type': 'CAN020',
        'tracking_category_name': TRACKING_CATEGORY_NAME,
    },
    'india': {
        'client_country': 'India',
        'line_amount_types': 'Exclusive',
        'tax_type': 'TAX006',
        'tracking_category_name': TRACKING_CATEGORY_NAME,
    },
    'singapore': {
        'client_country': 'Singapore',
        'line_amount_types': 'Exclusive',
        'tax_type': 'NONE',
        'tracking_category_name': TRACKING_CATEGORY_NAME,
    },
    'australia': {
        'client_country': 'Australia',
        'line_amount_types': 'Exclusive',
        'tax_type': 'TAX001',
        'tracking_category_name': TRACKING_CATEGORY_NAME,
    },
}

CLIENT_COUNTRY_TO_COUNTRY = {
    meta['client_country']: country for country, meta in COUNTRIES.items()
}

CURRENCY_CODE_BY_NAME = {
    'US Dollar': 'USD',
    'Canadian Dollar': 'CAD',
    'Singapore Dollar': 'SGD',
    'Indian Rupee': 'INR',
    'Australian Dollar': 'AUD',
}
