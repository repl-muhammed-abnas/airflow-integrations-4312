"""Response filters for T-Systems Time Import API responses."""
import itertools
import rail

null = None
null_urn = "urn:replicon:list-type:null"

def get_value(item, index, pluck_key):
    return item[index][pluck_key] if item[index]['dataType'] != null_urn else ""

def filter_group_data(res):
    # Get org codes from today's mapper records
    todays_records = rail.result("get_todays_mapper_records") or []
    valid_org_codes = {record['org_code'] for record in todays_records}
    
    # Return only orgs that match today's mapper org codes
    return [
        {
            'name': get_value(data['cells'], 0, 'textValue'),
            'uri': get_value(data['cells'], 0, 'uri'),
            'code': get_value(data['cells'], 1, 'textValue'),
        } 
        for data in res['rows']
        if get_value(data['cells'], 1, 'textValue') in valid_org_codes
    ]

def get_timesheet_details(response):

    if not response:
        return []

    timesheet_status_mapping = {
        'waiting': 'Waiting for Approval',
        'open': 'Not Submitted',
        'rejected': 'Rejected',
        'approved': 'Approved'
    }

    flatten_rows = list(itertools.chain(
        list(map(lambda x: x['timesheet'], response))
    ))

    def fmt(ts):
        status_token = ts['statusUri'].split(':')[-1]
        return {
            "timesheet_status": timesheet_status_mapping.get(status_token, ''),
            "timesheet_status_uri": ts['statusUri'],
            "timesheet_uri": ts['uri'],
            "timesheet_date_range": ts['dateRange'],
            "user_uri": ts['owner']['uri']
        }

    return list(map(fmt, flatten_rows))

def extract_time_entry_revision_group_uris(response):
    if not response:
        return []
    
    time_entries = []
    for group in response.get('timeEntryRevisionGroups', []):
        for revision in group.get('timeEntryRevisions', []):
            task_name = str(revision.get('taskName', '') or '').strip().lower()
            if task_name == 'guessing hours':
                time_entries.append({
                    'timeEntryRevisionGroupUri': group.get('uri') or group.get('timeEntryRevisionGroupUri'),
                    'user_uri': group.get('userUri'),
                    'entry_date': group.get('date'),
                    'hours': revision.get('hours'),
                    **group
                })
                break
    
    return time_entries

def filter_time_entries(response, dag_run):
    """Filter time entries from API response - enhanced for T-Systems with unique ID matching"""
    if not response:
        return []

    entries = []

    for entry in response:
        task_uri = null
        project_uri = null
        activity_uri = null
        comments = null
        row_number = null
        unique_id = null
        is_billable = null
        billing_rate_uri = null

        extension_fields = entry.get('extensionFieldValues', [])
        metadata = entry.get('customMetadata', [])
        time_allocation_types = entry.get('timeAllocationTypeUris', [])

        if 'customMetadata' in entry:
            for meta in entry.get('customMetadata', []):
                key_uri = meta.get('keyUri', '')
                if key_uri == "urn:replicon:time-entry-metadata-key:task":
                    task_uri = meta.get('value', {}).get('uri')
                elif key_uri == "urn:replicon:time-entry-metadata-key:project":
                    project_uri = meta.get('value', {}).get('uri')
                elif key_uri == "urn:replicon:time-entry-metadata-key:activity":
                    activity_uri = meta.get('value', {}).get('uri')
                elif key_uri == "urn:replicon:time-entry-metadata-key:comments":
                    comments = meta.get('value', {}).get('text')
                elif key_uri == "urn:replicon:widget-ui-metadata-key:row-number":
                    row_number = meta.get('value', {}).get('number')
                elif key_uri == "urn:replicon:time-entry-metadata-key:external-id":
                    unique_id = meta.get('value', {}).get('text')
                elif key_uri == "urn:replicon:time-entry-metadata-key:is-billable":
                    is_billable = meta.get('value', {}).get('bool')
                elif key_uri == "urn:replicon:time-entry-metadata-key:billing-rate":
                    billing_rate_uri = meta.get('value', {}).get('uri')

        entries.append({
            'entry_uri': entry.get('uri'),
            'user_uri': entry.get('user', {}).get('uri'),
            'entry_date': entry.get('entryDate'),
            'task_uri': task_uri,
            'project_uri': project_uri,
            'activity_uri': activity_uri,
            'comments': comments,
            'row_number': row_number,
            'unique_id': unique_id,
            'is_billable': is_billable,
            'billing_rate_uri': billing_rate_uri,
            'extension_fields': extension_fields,
            'custom_metadata': metadata,
            'time_allocation_types': time_allocation_types
        })

    if not entries:
        return []

    return entries[0]  
