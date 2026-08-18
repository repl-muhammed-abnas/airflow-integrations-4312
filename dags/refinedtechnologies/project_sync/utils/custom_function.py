import rail
from datetime import datetime, timedelta, timezone


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


def safe_get_salesforce_record(data, index=0):
    if not data or not isinstance(data, dict):
        return None

    records = data.get('records')
    if not records or not isinstance(records, list) or len(records) <= index:
        return None

    return records[index]

def project_exists(project_detail):
    """True if BulkGetProjectDetails returned a project with a uri."""
    if not project_detail or not project_detail[0].get('projectDetails'):
        return False
    return bool(project_detail[0]['projectDetails'].get('uri'))

def project_name_or_desc_changed(opportunity, existing_project_detail):
    """True if opportunity name/description differ from the stored project (uses write-path cleaning so it doesn't flag a change every run)."""
    project_details = existing_project_detail['projectDetails']
    opp_name = clean_project_name(opportunity.get("Replicon_PID_Description__c")) or ""
    opp_desc = clean_project_description(opportunity.get("Description")) or ""
    proj_name = project_details.get("name") or ""
    proj_desc = project_details.get("description") or ""
    return opp_name != proj_name or opp_desc != proj_desc

def account_name_changed(account_result, existing_project_detail):
    """True when the Account name is present and differs from the project's current client name."""
    account_record = safe_get_salesforce_record(account_result) or {}
    account_name = account_record.get('Name', "")
    client = existing_project_detail.get('client') or {}
    client_name = client.get("name", "")
    return bool(account_name and account_name != client_name)

def check_account_name_legacy_id(account_result):
    """True if Account.Name and Account.Legacy_Id__c are both present (recipe gate)."""
    record = safe_get_salesforce_record(account_result) or {}
    return bool(record.get('Name') and record.get('Legacy_Id__c') is not None)

def extract_uri_from_rows(rows, username):
    uri_list = []
    if not username or not username.get('records') or len(username['records']) == 0:
        return uri_list

    target_username = username['records'][0].get('Username')
    if not target_username:
        return uri_list

    for row in rows.get("rows", []):
        for cell in row.get("cells", []):
            if cell.get("textValue") == target_username:
                uri_list.append(cell.get("uri"))
    return uri_list

def convert_ruby_data_to_list(data):
    output = []
    for row in data.get("rows", []):
        cells = row.get("cells", [])
        if len(cells) >= 3:
            output.append({
                "clientname": cells[0].get("textValue"),
                "clienturi": cells[0].get("uri"),
                "clientcode": cells[1].get("textValue"),
                "status": cells[2].get("boolValue", False)
            })
    return output

def has_matching_client(client_list, legacy_id_salesforce):
    """True if any client's code matches the account code."""
    return bool(_matching_clients(client_list, legacy_id_salesforce))

def _matching_clients(client_list, legacy_id_salesforce):
    """Clients whose code matches RTI_ACCOUNT_ID__c (confirmed to equal Account.Legacy_Id__c)."""
    if not client_list:
        return []
    try:
        legacy_id = int(legacy_id_salesforce['RTI_ACCOUNT_ID__c'])
    except (ValueError, KeyError, TypeError):
        return []
    return [client for client in client_list
            if client.get('clientcode') and int(client['clientcode']) == legacy_id]

def find_matching_client(client_list, legacy_id_salesforce):
    """Return the client dict matching the account code, or None."""
    matches = _matching_clients(client_list, legacy_id_salesforce)
    return matches[0] if matches else None

def build_search_client_reply(client_list, salesforce_record, created_client):
    """Sub-child reply mirroring the recipe's send_reply: clienturi/clientstatus/clientname (+ extras), whether the client existed or was created."""
    matched = find_matching_client(client_list, salesforce_record or {})
    if matched:
        clienturi = matched.get('clienturi')
        clientstatus = matched.get('status')
        clientname = matched.get('clientname')
    elif created_client:
        clienturi = created_client.get('uri')
        clientstatus = True
        clientname = created_client.get('name')
    else:
        clienturi = clientstatus = clientname = None
    return {
        'clienturi': clienturi,
        'clientstatus': clientstatus,
        'clientname': clientname,
        'created_client': created_client,
        'client_data': client_list,
        'status': 'success',
    }

def clean_text(value):
    """Strip CR/LF/tab/backslash and convert double quotes to single quotes (recipe parity)."""
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

def clean_project_name(value):
    """Recipe project-name cleaning: escape double quotes only -> gsub(/"/,'\\"')."""
    if value is None:
        return None
    return value.replace('"', '\\"')

def clean_project_description(value):
    """Recipe project-description cleaning: escape quotes, strip CR/LF/tab, backslash->'/', double->single quote, then slice(0,255)."""
    if not value:
        return None
    cleaned = (
        value.replace('"', '\\"')
        .replace('\r\n', '')
        .replace('\n', '')
        .replace('\\', '/')
        .replace('"', "'")
        .replace('\t', '')
    )
    return cleaned[:255]

def get_primary_contact(contact_result):
    """First contact's display name and email from a Salesforce contact query result."""
    records = (contact_result or {}).get('records') or []
    if not records:
        return {"name": None, "email": None}
    contact = records[0]
    name = " ".join(
        part for part in [contact.get('FirstName'), contact.get('LastName')] if part
    ).strip() or None
    return {"name": name, "email": contact.get('Email')}

def check_facility_legacy_id(data):
    return bool(data.get("Name") and data.get("RTI_ACCOUNT_ID__c"))

def client_uri_check(data):
    return bool(data and data.get("clienturi"))

def optional_field(key, data):
    return {"value": data[key]} if key in data and data[key] else None

def optional_clean_field(key, data):
    """optional_field variant that sanitizes the value (recipe parity for description/street)."""
    cleaned = clean_text(data.get(key))
    return {"value": cleaned} if cleaned else None

def get_country_uri_replicon(country_name, country_list):
    match = next((c['uri'] for c in country_list if c['name'] == country_name), None)
    return {"value": {"uri": match, "name": None}} if match else None