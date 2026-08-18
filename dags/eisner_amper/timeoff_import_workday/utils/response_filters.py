"""Response filters for Eisner Amper TimeOff Import API responses."""
import itertools
import rail

null = None

def get_timesheet_details(response, item):
    """
    Extract and format timesheet details from Replicon API response.
    
    Processes GetTimesheetDetailsForDate API response to extract timesheet
    status, URI, date range, and user information for timesheet management.
    
    Args:
        response: API response object containing timesheet data
        item: The input record being processed
        
    Returns:
        Dict[str, Any]: Formatted timesheet details with status mappings and input data
    """
    if not response:
        return None

    timesheet_status_mapping = {
        'waiting': 'Waiting For Approval',
        'open': 'Not Submitted',
        'rejected': 'Rejected',
        'approved': 'Approved'
    }
    
    # GetTimesheetDetailsForDate returns a single timesheet object
    timesheet = response.get('timesheet') if isinstance(response, dict) else response
    
    if not timesheet:
        return None
    
    # Extract status from URI
    status_slug = timesheet.get('statusUri', '').split(':')[-1]
    
    return {
        **item,  # Include all input record fields
        "timesheet_status": timesheet_status_mapping.get(status_slug, ''),
        "timesheet_status_uri": timesheet.get('statusUri'),
        "timesheet_uri": timesheet.get('uri'),
        "timesheet_date_range": timesheet.get('dateRange'),
        "user_uri": timesheet.get('owner', {}).get('uri')
    }

def filter_time_entries(response, dag_run):
    """Filter time entries from API response - enhanced for Eisner Amper with booking reference ID matching"""
    if not response:
        return []

    entries = []

    for entry in response:
        task_uri = null
        project_uri = null
        activity_uri = null
        comments = null
        unique_id = null

        if 'customMetadata' in entry:
            for meta in entry.get('customMetadata', []):
                key_uri = meta.get('keyUri', {})
                if key_uri == "urn:replicon:time-entry-metadata-key:task":
                    task_uri = meta.get('value', {}).get('uri')
                elif key_uri == "urn:replicon:time-entry-metadata-key:project":
                    project_uri = meta.get('value', {}).get('uri')
                elif key_uri == "urn:replicon:time-entry-metadata-key:comments":
                    comments = meta.get('value', {}).get('text')
                elif key_uri == "urn:replicon:time-entry-metadata-key:external-id":
                    unique_id = meta.get('value', {}).get('text')

        hours = 0
        if 'interval' in entry and entry['interval'] and 'hours' in entry['interval']:
            seconds = entry['interval']['hours'].get('seconds', 0)
            minutes = entry['interval']['hours'].get('minutes', 0)
            hrs = entry['interval']['hours'].get('hours', 0)
            hours = hrs + (minutes / 60) + (seconds / 3600)

        entries.append({
            'entry_uri': entry.get('uri'),
            'user_uri': entry.get('user', {}).get('uri'),
            'entry_date': entry.get('entryDate'),
            'total_hours': round(hours, 2),
            'task_uri': task_uri,
            'project_uri': project_uri,
            'activity_uri': activity_uri,
            'comments': comments,
            'unique_id': unique_id,
            'approval_status': entry.get('approvalStatus').get('displayText', ''),
        })

    if not entries:
        return []

    target_booking_ref = dag_run.conf["input_data"]["booking_reference_id"]

    for entry in entries:
        if entry.get('unique_id') == target_booking_ref:
            return {
                'entry_uri': entry.get('entry_uri'),
                'approval_status': entry.get('approval_status')
            }
    return null


def format_project_task_details(response, default_task_name):
    return rail.find_first_by_attr_and_get_attr(response, "task.name", default_task_name, "task.uri")


def format_time_entries_for_enrichment(response):
    """
    Format time entries for record enrichment in master.py.
    
    Extracts key fields from time entries needed for matching with input records
    and determining processing requirements.
    
    Args:
        response: API response from GetTimeEntryRevisionGroupsForUserAndDateRange
        
    Returns:
        List[Dict]: Formatted time entries with relevant fields
    """
    if not response:
        return []
    
    formatted_entries = []
    
    for entry in response:
        task_uri = null
        project_uri = null
        unique_id = null
        
        # Extract metadata fields
        if 'customMetadata' in entry:
            for meta in entry.get('customMetadata', []):
                key_uri = meta.get('keyUri', '')
                if key_uri == "urn:replicon:time-entry-metadata-key:task":
                    task_uri = meta.get('value', {}).get('uri')
                elif key_uri == "urn:replicon:time-entry-metadata-key:project":
                    project_uri = meta.get('value', {}).get('uri')
                elif key_uri == "urn:replicon:time-entry-metadata-key:external-id":
                    unique_id = meta.get('value', {}).get('text')
        
        # Calculate hours
        hours = 0
        if 'interval' in entry and entry['interval'] and 'hours' in entry['interval']:
            seconds = entry['interval']['hours'].get('seconds', 0)
            minutes = entry['interval']['hours'].get('minutes', 0)
            hrs = entry['interval']['hours'].get('hours', 0)
            hours = hrs + (minutes / 60) + (seconds / 3600)
        
        # Format entry date as string (MM/DD/YYYY) to match input format
        entry_date_dict = entry.get('entryDate', {})
        if entry_date_dict:
            entry_date = f"{entry_date_dict.get('month', ''):02d}/{entry_date_dict.get('day', ''):02d}/{entry_date_dict.get('year', '')}"
        else:
            entry_date = ''
        
        formatted_entries.append({
            'entry_uri': entry.get('uri'),
            'entry_date': entry_date,
            'total_hours': round(hours, 2),
            'task_uri': task_uri,
            'project_uri': project_uri,
            'unique_id': unique_id,
            'approval_status': entry.get('approvalStatus', {}).get('displayText', '')
        })
    
    return formatted_entries
