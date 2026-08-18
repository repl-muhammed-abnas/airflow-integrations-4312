from datetime import datetime, timedelta
import rail


def get_field_value(payload, *field_names):
    for field_name in field_names:
        value = payload.get(field_name)
        if value not in [None, '', ' ']:
            return value
    return None


def validate_required_fields(payload):
    required_fields_map = {
        'payloadid': ['payloadid', 'payload_id'],
        'userid': ['userid', 'user_id'],
        'assigneeName': ['assigneeName', 'assigneename', 'assignee_name'],
        'assigneeid': ['assigneeid', 'assignee_id'],
        'entrydate': ['entrydate', 'entry_date'],
        'taskcode': ['taskcode', 'task_code'],
        'projectcode': ['projectcode', 'project_code'],
    }

    missing_fields = []
    for field_key, field_variants in required_fields_map.items():
        value = get_field_value(payload, *field_variants)
        if value is None:
            missing_fields.append(field_key)

    if missing_fields:
        error_msg = f"Time entry cannot be processed. The following required fields are missing: {', '.join(missing_fields)}."
        return False, missing_fields, error_msg

    return True, [], ""


def validate_required_oef_fields(payload, mandatory_oef_names):
    oef_list = payload.get('oef', [])
    oef_names_present = {oef.get('name') for oef in oef_list if oef.get('name')}

    missing_oefs = [name for name in mandatory_oef_names if name not in oef_names_present]

    if missing_oefs:
        error_msg = f"Time entry cannot be processed. The following required custom fields are missing from the payload: {', '.join(missing_oefs)}."
        return False, missing_oefs, error_msg

    return True, [], ""


def validate_time_value(time_str, field_name):
    try:
        parts = str(time_str).split(':')
        if len(parts) < 2:
            return False, f"Time entry cannot be processed. The '{field_name}' value '{time_str}' is not in a valid time format (HH:MM or HH:MM:SS)."
        hour = int(parts[0])
        minute = int(parts[1])
        second = int(parts[2]) if len(parts) > 2 else 0
        if not (0 <= hour <= 23):
            return False, f"Time entry cannot be processed. The '{field_name}' value '{time_str}' contains an invalid hour ({hour}). Must be between 0 and 23."
        if not (0 <= minute <= 59):
            return False, f"Time entry cannot be processed. The '{field_name}' value '{time_str}' contains an invalid minute ({minute}). Must be between 0 and 59."
        if not (0 <= second <= 59):
            return False, f"Time entry cannot be processed. The '{field_name}' value '{time_str}' contains an invalid second ({second}). Must be between 0 and 59."
        return True, ""
    except (ValueError, TypeError):
        return False, f"Time entry cannot be processed. The '{field_name}' value '{time_str}' is not a valid time."



def validate_entry_date(entry_date_str, min_days_past=31, max_days_future=4):
    try:
        entry_date = datetime.strptime(entry_date_str, '%Y-%m-%d').date()
    except TypeError:
        return False, "Time entry cannot be processed. The entry date is missing or not a valid text value."
    except ValueError as e:
        error_str = str(e)
        if 'does not match format' in error_str or 'unconverted' in error_str or 'time data' in error_str:
            return False, f"Time entry cannot be processed. The entry date '{entry_date_str}' is not in the required format (YYYY-MM-DD)."
        return False, f"Time entry cannot be processed. The entry date '{entry_date_str}' is not a valid calendar date."

    today = datetime.now().date()
    min_allowed_date = today - timedelta(days=min_days_past)
    max_allowed_date = today + timedelta(days=max_days_future)

    if entry_date < min_allowed_date:
        return False, f"Time entry cannot be processed. The entry date '{entry_date_str}' exceeds the allowed historical limit of {min_days_past} days."

    if entry_date > max_allowed_date:
        return False, f"Time entry cannot be processed. The entry date '{entry_date_str}' exceeds the maximum allowed future date of {max_days_future} days."

    return True, ""


def _get_effective_uri(schedule_list, entry_date, field_key):
    """Return the URI of the schedule entry effective as of entry_date.

    Rules:
    - Entries with effectiveDate <= entry_date are candidates.
    - null effectiveDate = effective since the beginning (lowest priority).
    - Among candidates, the one with the latest effectiveDate wins.
    """
    best_uri = None
    best_date = None  # None means the baseline (null effectiveDate) was used

    for entry in schedule_list:
        obj = entry.get(field_key) or {}
        uri = obj.get('uri')
        if not uri:
            continue

        eff_date_obj = entry.get('effectiveDate')

        if eff_date_obj is None:
            # Baseline entry — only use if nothing better found yet
            if best_uri is None:
                best_uri = uri
                best_date = None
        else:
            try:
                eff_date = datetime(
                    year=eff_date_obj['year'],
                    month=eff_date_obj['month'],
                    day=eff_date_obj['day']
                ).date()
                if eff_date <= entry_date:
                    if best_date is None or eff_date > best_date:
                        best_uri = uri
                        best_date = eff_date
            except (KeyError, TypeError, ValueError):
                pass

    return best_uri


def validate_user_from_report(user_id, user_report_data, entry_date_str):
    matching_users = []
    for user_obj in user_report_data:
        user_details = user_obj.get('userDetails', {})
        if user_details.get('employeeId') == user_id:
            matching_users.append(user_obj)

    if len(matching_users) == 0:
        return False, None, f"Employee '{user_id}' was not found in the system."

    if len(matching_users) > 1:
        return False, None, f"Multiple records found for Employee ID '{user_id}'. Please ensure the employee ID is unique."

    user_obj = matching_users[0]
    user_details = user_obj.get('userDetails', {})

    if not user_details.get('isEnabled', False):
        return False, user_obj, f"Employee '{user_id}' is currently inactive in the system."

    try:
        entry_date = datetime.strptime(entry_date_str, '%Y-%m-%d').date()
    except (ValueError, TypeError):
        return False, user_obj, f"The entry date '{entry_date_str}' could not be parsed."

    employment_date_range = user_details.get('employmentDateRange', {})

    start_date_obj = employment_date_range.get('startDate')
    if start_date_obj:
        try:
            user_start_date = datetime(
                year=start_date_obj.get('year'),
                month=start_date_obj.get('month'),
                day=start_date_obj.get('day')
            ).date()
            if user_start_date > entry_date:
                return False, user_obj, f"Employee '{user_id}' start date ({user_start_date}) is after the entry date ({entry_date_str})."
        except (ValueError, TypeError, KeyError) as e:
            pass

    end_date_obj = employment_date_range.get('endDate')
    if end_date_obj:
        try:
            user_end_date = datetime(
                year=end_date_obj.get('year'),
                month=end_date_obj.get('month'),
                day=end_date_obj.get('day')
            ).date()
            if user_end_date < entry_date:
                return False, user_obj, f"Employee '{user_id}' end date ({user_end_date}) is before the entry date ({entry_date_str})."
        except (ValueError, TypeError, KeyError):
            pass

    today = datetime.now().date()
    cost_center_uri = _get_effective_uri(user_obj.get('costCenterSchedule', []), today, 'costCenter')
    service_center_uri = _get_effective_uri(user_obj.get('serviceCenterSchedule', []), today, 'serviceCenter')

    normalized_user_record = {
        'UserUri': user_details.get('uri'),
        'employeeId': user_details.get('employeeId'),
        'isEnabled': user_details.get('isEnabled'),
        'employmentDateRange': employment_date_range,
        'timesheetTemplate': user_obj.get('timesheetTemplate', {}),
        'costCenterUris': [cost_center_uri] if cost_center_uri else [],
        'serviceCenterUris': [service_center_uri] if service_center_uri else [],
    }

    return True, normalized_user_record, ""


_DEFAULT_TIMESHEET_TEMPLATES = {
    'TSD': ['TSD'],
    'In/Out': ['In/Out'],
    'Punch': ['Punch In/Punch Out', 'Punch In', 'Punch'],
}


def validate_template_and_payload_fields(template_name, payload, template_config=None):
    templates = template_config or _DEFAULT_TIMESHEET_TEMPLATES
    template_name_lower = (template_name or '').lower()

    tsd_keywords = templates.get('TSD', _DEFAULT_TIMESHEET_TEMPLATES['TSD'])
    inout_keywords = templates.get('In/Out', _DEFAULT_TIMESHEET_TEMPLATES['In/Out'])
    punch_keywords = templates.get('Punch', _DEFAULT_TIMESHEET_TEMPLATES['Punch'])

    if any(kw.lower() in template_name_lower for kw in tsd_keywords):
        hours = get_field_value(payload, 'hours')
        if hours is None:
            return False, 'TSD', f"The time entry could not be processed because the Hours field is mandatory for the timesheet template - '{template_name}'"
        return True, 'TSD', ""

    elif any(kw.lower() in template_name_lower for kw in inout_keywords):
        in_time = get_field_value(payload, 'in', 'intime', 'in_time')
        out_time = get_field_value(payload, 'out', 'outtime', 'out_time')

        if in_time is None or out_time is None:
            return False, 'In/Out', f"The time entry could not be processed because In Time /Out Time are mandatory for the timesheet template - '{template_name}'"

        is_valid, error_msg = validate_time_value(in_time, 'intime')
        if not is_valid:
            return False, 'In/Out', error_msg
        is_valid, error_msg = validate_time_value(out_time, 'outtime')
        if not is_valid:
            return False, 'In/Out', error_msg

        return True, 'In/Out', ""

    elif any(kw.lower() in template_name_lower for kw in punch_keywords):
        punch_in = get_field_value(payload, 'punchin', 'punch_in')
        punch_out = get_field_value(payload, 'punchout', 'punch_out')

        if punch_in is None or punch_out is None:
            return False, 'Punch', f"The time entry could not be processed because Punch-In Time /Punch-Out Time are mandatory for the timesheet template - '{template_name}'"

        is_valid, error_msg = validate_time_value(punch_in, 'punchin')
        if not is_valid:
            return False, 'Punch', error_msg
        is_valid, error_msg = validate_time_value(punch_out, 'punchout')
        if not is_valid:
            return False, 'Punch', error_msg

        return True, 'Punch', ""

    else:
        return False, 'Unknown', f"Time entry cannot be processed. The timesheet template '{template_name}' assigned to this employee is not supported by this integration."
