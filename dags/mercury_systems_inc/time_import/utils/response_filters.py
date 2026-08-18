import rail

null = None


def filter_timesheet_details(response):
    """Extract relevant timesheet details from API response"""
    return {"uri": response.get('timesheet').get('uri') if response.get('timesheet') else None,
            "status": response.get('timesheet').get("statusUri") if response.get('timesheet') else None}


def filter_time_entries(response):
    """Filter time entries from API response"""
    if not response:
        return []

    entries = []
    user_records = rail.load_all_records(
        rail.result("query_user_time_entry_records"))
    for entry in response:
        # Extract project and task URIs from metadata
        task_uri = None
        if 'customMetadata' in entry:
            for meta in entry.get('customMetadata', []):
                if meta.get('keyUri', {}) == "urn:replicon:time-entry-metadata-key:task":
                    task_uri = meta.get('value', {}).get('uri')

        # Extract hours
        hours = 0
        if entry.get("interval",{}) and 'hours' in entry['interval']:
            seconds = entry['interval']['hours'].get('seconds', 0)
            minutes = entry['interval']['hours'].get('minutes', 0)
            hrs = entry['interval']['hours'].get('hours', 0)
            hours = hrs + (minutes / 60) + (seconds / 3600)

        entries.append({
            'entry_uri': entry.get('uri'),
            'user_uri': entry.get('user', {}).get('uri'),
            'entry_date': entry.get('entryDate'),
            'total_hours': round(hours, 2),
            'task_uri': task_uri
        })

    if not entries:
        return []

    for record in user_records:
        for entry in entries:
            if entry.get('task_uri') == record["task_uri"]:
                record["total_hours"] = str(
                    float(record['total_hours'])+float(entry["total_hours"]))
                record["time_entry_uri"] = entry["entry_uri"]

    return user_records
