import json
import rail
nill = None


def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf


def is_extension_feild(task_value):
    time_export_details = rail.result(task_value)["extensionFieldValues"]
    if time_export_details:
        data = list(filter(lambda x: x["definition"]["displayText"]
                    == "FTP_Payload_Processed", time_export_details))
        if data:
            payload_processed = data[0]["textValue"]
            if payload_processed == "Yes":
                return True
    return False


def get_final_line_data(get_export_name):
    return [{
            'Entryid': '',
            'Uniqueid': json.loads(rail.result(get_export_name))['twbname'],
            'Employeeid': '',
            'Date': '',
            'Wbs': '',
            'Vblen': '',
            'Salesitem': '',
            'Attendancetype': '',
            'Hours': '',
            'Comments': '',
            }
            ]


def get_parent_location(location_full_path):
    """Extract parent location (first segment) from full path.
    
    Example: 'United Kingdom / London' returns 'United Kingdom'
    Example: 'United Kingdom' returns 'United Kingdom'
    """
    if not location_full_path:
        return ''
    return location_full_path.split(' / ')[0].strip()


def get_timeoff_attendance_type(time_off_name, time_off_desc, location_full_path):
    """Determine attendance type for time off bookings for FTP export.
    
    Args:
        time_off_name: Name of the time off type
        time_off_desc: Description containing comma-delimited attendance codes
        location_full_path: Full location path to determine UKI vs non-UKI
    
    Returns:
        Attendance type code (index 2 for UKI, index 1 for non-UKI Holiday, 
        index 2 for [UK]/[IRL] prefixed, or full description for others)
    """
    is_uk_or_irl = time_off_name.startswith(('[UK]', '[IRL]'))
    time_off_desc_split = time_off_desc.split(',') if time_off_desc else []
    
    # Handle Holiday specifically
    if time_off_name == "Holiday" and len(time_off_desc_split) >= 3:
        parent_location = get_parent_location(location_full_path)
        if parent_location in ("United Kingdom", "Ireland"):
            # UKI users: return third element (index 2) for FTP
            return time_off_desc_split[2].strip()
        else:
            # Non-UKI users: return second element (index 1) which is 110
            return time_off_desc_split[1].strip()
    
    # Existing UK/IRL logic
    if is_uk_or_irl and len(time_off_desc_split) > 2:
        return time_off_desc_split[2].strip()
    elif is_uk_or_irl:
        return ''
    else:
        return time_off_desc