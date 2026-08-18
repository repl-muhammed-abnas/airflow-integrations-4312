from airflow.models import Variable

import rail
import pycountry
import re
import pendulum


LOOKBACK_TIMESTAMP_FORMAT = re.compile(
    r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}\+0000$'
)


def get_opportunity_lookback_timestamp(variable_name: str) -> str:
    """
    Fetch the opportunity lookback timestamp from an Airflow Variable and
    validate that it is in UTC format: YYYY-MM-DDTHH:MM:SS.mmm+0000

    Args:
        variable_name: The Airflow Variable key to retrieve.

    Returns:
        The validated timestamp string.

    Raises:
        ValueError: If the variable is missing or the format is invalid.
    """
    timestamp = Variable.get(variable_name, default_var=None)

    if not timestamp:
        raise ValueError(
            f"Airflow Variable '{variable_name}' is not set. "
            "Expected format: YYYY-MM-DDTHH:MM:SS.sss+0000"
        )

    if not LOOKBACK_TIMESTAMP_FORMAT.match(timestamp.strip()):
        raise ValueError(
            f"Invalid timestamp format for Variable '{variable_name}': '{timestamp}'. "
            "Expected UTC format: YYYY-MM-DDTHH:MM:SS.mmm+0000 (e.g. 2026-03-17T08:46:36.000+0000)"
        )

    return timestamp.strip()


def get_current_time_in_utc_minus_1_min(time_zone) -> str:
    """
    Returns the current UTC time minus 1 minute in the format:
    YYYY-MM-DDTHH:MM:SS.mmm+0000
    """
    now_utc = pendulum.now(time_zone).subtract(minutes=1)
    return now_utc.format('YYYY-MM-DDTHH:mm:ss.SSS+0000')


def process_product_name(opportunity_product_name, opportunity_name):
    return opportunity_product_name.replace(opportunity_name, "").strip()


def get_billing_contact_name(salesforce_contacts):
    if not salesforce_contacts:
        return None

    first_name = (salesforce_contacts[0].get('FirstName') or '').strip()
    last_name = (salesforce_contacts[0].get('LastName') or '').strip()

    full_name = f"{first_name} {last_name}".strip()
    return full_name or None


def normalize_blank_to_none(val):
    """Return val if it's a non-blank string, else None."""
    return val if val and str(val).strip() else None


def to_country_name(country_code):
    """
    Convert a country code or name to its full country name.
    Handles alpha-2 (US), alpha-3 (USA), and full names (united states).
    Returns None if no match found.
    """
    if not country_code or not str(country_code).strip():
        return None
    val = str(country_code).strip()

    # Try alpha-2 code (e.g. "US")
    if len(val) == 2:
        country = pycountry.countries.get(alpha_2=val.upper())
        if country:
            return country.name

    # Try alpha-3 code (e.g. "USA")
    if len(val) == 3:
        country = pycountry.countries.get(alpha_3=val.upper())
        if country:
            return country.name

    # Try full name match (case-insensitive)
    for country in pycountry.countries:
        if country.name.lower() == val.lower():
            return country.name

    return None


def resolve_country_uri(country_code):
    """Look up the country URI from the display name, or return None."""
    country_name = to_country_name(country_code)
    if not country_code:
        return None
    return rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_countries_in_replicon'), 'displayText', country_name, 'uri', None
    )


def build_client_address(account):
    """Build a clientAddress dict from a Salesforce account record, or None if all fields are blank."""
    country_name = normalize_blank_to_none(account.get('ShippingCountry'))
    country_uri = resolve_country_uri(country_name)

    address = {
        "address": normalize_blank_to_none(account.get('ShippingStreet')),
        "city": normalize_blank_to_none(account.get('ShippingCity')),
        "stateProvince": normalize_blank_to_none(account.get('ShippingState')),
        "country": {"uri": country_uri, "name": None} if country_uri else None,
        "zipPostalCode": normalize_blank_to_none(account.get('ShippingPostalCode')),
        "phoneNumber": normalize_blank_to_none(account.get('Phone')),
        "faxNumber": normalize_blank_to_none(account.get('Fax')),
        "email": None,
        "website": normalize_blank_to_none(account.get('Website')),
    }

    return address if any(address.values()) else None

def build_billing_address(account):
    """Build a clientAddress dict from a Salesforce account record, or None if all fields are blank."""
    country_name = normalize_blank_to_none(account.get('BillingCountry'))
    country_uri = resolve_country_uri(country_name)

    address = {
        "address": normalize_blank_to_none(account.get('BillingStreet')),
        "city": normalize_blank_to_none(account.get('BillingCity')),
        "stateProvince": normalize_blank_to_none(account.get('BillingState')),
        "country": {"uri": country_uri, "name": None} if country_uri else None,
        "zipPostalCode": normalize_blank_to_none(account.get('BillingPostalCode')),
        "phoneNumber": normalize_blank_to_none(account.get('Phone')),
        "faxNumber": normalize_blank_to_none(account.get('Fax')),
        "email": None,
        "website": normalize_blank_to_none(account.get('Website')),
    }

    return address if any(address.values()) else None