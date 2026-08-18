import rail
from datetime import datetime, timedelta, timezone


def extract_salesforce_records(salesforce_result):
    """Return the 'records' array from a Salesforce result (empty list if none)."""
    if not salesforce_result:
        return []
    return salesforce_result.get('records', [])


def last_sync_time(last_sync_var):
    """Read the last-sync watermark Variable (initialises to now-5min on first run)."""
    sync_time = (datetime.now(
                timezone.utc) - timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')
    return rail.get_lastsync_time_variable(
        variable_name=last_sync_var,
        date_format='%Y-%m-%dT%H:%M:%SZ',
        initial_sync_time=sync_time,
        reset_after_threshold=False
        )


def update_last_sync(update_sync_time):
    """Persist this run's current_time as the new last-sync watermark."""
    return rail.set_lastsync_time_variable(
            variable_name=update_sync_time,
            value_to_set=rail.result('get_last_sync_time')['current_time']
        )


def get_clients_list(response):
    """Map a Replicon client-list response to [{client, code, active, uri}]."""
    list_output = []
    if response and response.get('rows'):
        for row in response["rows"]:
            cells = row["cells"]
            client_info = {
                "client": cells[0].get("textValue", None),
                "code": cells[1].get("textValue", None),
                "active": cells[2].get("textValue", None),
                "uri": cells[0].get("uri", None)
            }
            list_output.append(client_info)
    return list_output


def check_uri_presence_result(clients_list_replicon, legacy_id):
    return [
            client["uri"]
            for client in clients_list_replicon
            if int(client["code"]) == int(legacy_id)
        ]


def is_matching_client_active(clients_list_replicon, legacy_id):
    """True if a client with the matching legacy_id exists and is active."""
    if not clients_list_replicon:
        return False
    return any(
        int(entry.get('code', '')) == int(legacy_id)
        and str(entry.get('active', '')).strip().lower() == 'true'
        for entry in clients_list_replicon
    )


def get_uri_if_present(search_user_data, username):
    """URIs from a Replicon user-search result whose textValue matches username."""
    matching_uris = []
    if not search_user_data or not username or "rows" not in search_user_data:
        return matching_uris
    for row in search_user_data.get("rows", []):
        for cell in row.get("cells", []):
            if cell.get("textValue") == username:
                uri = cell.get("uri")
                if uri:
                    matching_uris.append(uri)
    return matching_uris

def optional_field(key, data):
    return {"value": data[key]} if key in data and data[key] else None


def clean_text(value):
    """Recipe sanitization: strip CR/LF/tab, backslash->'/', double->single quote; None if blank."""
    if not value:
        return None
    cleaned = (
        value.replace('\r\n', '')
        .replace('\n', '')
        .replace('\\', '/')
        .replace('"', "'")
        .replace('\t', '')
    )
    return cleaned or None


def optional_clean_field(key, data):
    """optional_field variant that sanitizes the value (see clean_text)."""
    cleaned = clean_text(data.get(key))
    return {"value": cleaned} if cleaned else None


def get_primary_contact(contact_result):
    """First contact's {name, email} from a Salesforce contact result (name = FirstName + LastName)."""
    records = (contact_result or {}).get('records') or []
    if not records:
        return {"name": None, "email": None}
    contact = records[0]
    name = " ".join(
        part for part in [contact.get('FirstName'), contact.get('LastName')] if part
    ).strip() or None
    return {"name": name, "email": contact.get('Email')}


def get_country_uri_replicon(country_name, country_list):
    match = next((c['uri'] for c in country_list if c['name'] == country_name), None)
    return {"value": {"uri": match, "name": None}} if match else None
