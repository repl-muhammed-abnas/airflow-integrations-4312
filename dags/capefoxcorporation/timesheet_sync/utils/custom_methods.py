from datetime import datetime, timedelta
from airflow.models import Variable
import rail


def is_revert_required(existing_timesheet, get_reversing_record_fn):
    """Check if reverting existing timesheet is required."""
    if len(existing_timesheet['document']['rows']) > 0:
        reversing_record = get_reversing_record_fn(existing_timesheet)
        if reversing_record is not None:
            return True
    return False


def is_data_reversed(row, rows):
    """Check if the data row has already been reversed."""
    for item in rows:
        if item['row']['data']['FY_CD'] == row['row']['data']['FY_CD'] \
                and item['row']['data']['PD_NO'] == row['row']['data']['PD_NO']\
                and item['row']['data']['SUB_PD_NO'] == row['row']['data']['SUB_PD_NO'] \
                and item['row']['data']['TS_HDR_SEQ_NO'] == row['row']['data']['TS_HDR_SEQ_NO'] \
                and item['row']['data']['S_TS_TYPE_CD'] != 'R':
            return item['row']['data']['FY_CD'] == row['row']['data']['FY_CD'] \
                and item['row']['data']['PD_NO'] == row['row']['data']['PD_NO']\
                and item['row']['data']['SUB_PD_NO'] == row['row']['data']['SUB_PD_NO'] \
                and item['row']['data']['TS_HDR_SEQ_NO'] == row['row']['data']['TS_HDR_SEQ_NO']\
                and item['row']['data']['S_TS_TYPE_CD'] != row['row']['data']['S_TS_TYPE_CD'] and \
                (item['row']['data']['S_TS_TYPE_CD'] ==
                 'RV' or item['row']['data']['S_TS_TYPE_CD'] == 'C')
    return False


def is_project_mo(entry, task_details, project_details, config):
    """Check if project is an MO (Manufacturing Order) project."""
    project_uri = get_project_uri_from_entry(entry, task_details)
    if project_uri:
        project_info = list(
            filter(lambda x: x['projectDetails']['uri'] == project_uri, project_details))
        if project_info:
            custom_field_info = project_info[0]['projectDetails']["extensionFieldValues"]
            mo_project_flag = rail.find_first_by_attr_and_get_attr(
                custom_field_info, 'definition.displayText', config.proj_mo_project_flag, 'textValue')
            return bool(mo_project_flag) and mo_project_flag.strip().lower() == 'yes'
    return False


def get_project_uri_from_entry(entry, task_details):
    """Get project URI from an entry using task details."""
    task_uri = rail.find_first_by_attr_and_get_attr(
        entry['customMetadata'], 'keyUri', 'urn:replicon:time-entry-metadata-key:task', 'value.uri')
    task_info = list(
        filter(lambda x: x['uri'] == task_uri, task_details))
    project_uri = task_info[0]['project']['uri'] if task_info else None
    return project_uri


def is_project_allocation_type(entry):
    """Check if entry is a project allocation type."""
    if entry and entry['timeAllocationTypeUris'] \
            and 'urn:replicon:time-allocation-type:project' in entry['timeAllocationTypeUris']:
        return True
    return False


def get_formatted_date(date_object):
    """Format a date object to string format YYYY-MM-DDTHH:MM:SS."""
    return f"{date_object['year']}-{str(date_object['month']).zfill(2)}-{str(date_object['day']).zfill(2)}T00:00:00"


def is_timeoff_paycode(entry, all_pay_codes):
    """Check if entry has 'Time Off' paycode.

    Used to identify entries auto-populated by 'Allocate Timesheet Hours' automation.
    These entries should be excluded from regular time entry processing.
    """
    if entry and all_pay_codes:
        pay_code_uri = rail.find_first_by_attr_and_get_attr(
            entry.get('customMetadata', []), 'keyUri', 'urn:replicon:object-type-uri:pay-code', 'value.uri')
        if pay_code_uri:
            pay_code = rail.find_first_by_attr_and_get_attr(all_pay_codes, 'uri', pay_code_uri)
            if pay_code:
                pay_code_name = pay_code.get('displayText') or pay_code.get('name') or ''
                return pay_code_name.lower() == 'time off'
    return False


def get_reg_hours(time_entries, all_pay_codes, oef_tags, time_offs=None):
    """Calculate total regular hours from time entries and timeoffs.

    Timeoff hours are included in regular hours as per Costpoint requirements.
    Entries with 'Time Off' paycode are excluded (handled via timeoff bookings).
    """
    total_hours = 0.0
    if all_pay_codes:
        for entry in time_entries:
            # Skip timeoff-allocated entries (handled via timeoff bookings API)
            if is_timeoff_paycode(entry, all_pay_codes):
                continue
            if is_project_allocation_type(entry) and entry and entry['interval'] \
                    and is_reg_paycode(entry, all_pay_codes, oef_tags):
                total_hours += get_hours(entry['interval'])

    # Add timeoff hours to regular hours
    if time_offs:
        for time_off in time_offs:
            for entry in time_off.get('entries', []):
                total_hours += get_duration_hours(entry.get('duration', {}))

    return total_hours


def get_total_hours(time_entries, all_pay_codes):
    """Calculate total hours from time entries.

    Entries with 'Time Off' paycode are excluded (handled via timeoff bookings).
    """
    total_hours = 0.0
    if all_pay_codes:
        for entry in time_entries:
            # Skip timeoff-allocated entries (handled via timeoff bookings API)
            if is_timeoff_paycode(entry, all_pay_codes):
                continue
            if is_project_allocation_type(entry) and entry and entry['interval']:
                total_hours += get_hours(entry['interval'])
    return total_hours


def get_other_hours(time_entries, all_pay_codes, oef_tags, time_offs=None):
    """Calculate total other (non-regular) hours from time entries.

    Note: Timeoff hours are NOT included in other hours - they are counted in regular hours only.
    The time_offs parameter is accepted for API consistency but not used.
    Entries with 'Time Off' paycode are excluded (handled via timeoff bookings).
    """
    total_hours = 0.0
    for entry in time_entries:
        # Skip timeoff-allocated entries (handled via timeoff bookings API)
        if is_timeoff_paycode(entry, all_pay_codes):
            continue
        if is_project_allocation_type(entry) and entry and entry['interval'] \
                and entry['interval'] and not is_reg_paycode(entry, all_pay_codes, oef_tags):
            total_hours += get_hours(entry['interval'])
    return total_hours


def is_reg_paycode(entry, all_pay_codes, oef_tags):
    """Check if the entry has a regular pay code (multiplier of 1.0)."""
    pay_code = get_pay_code_from_entry(entry, all_pay_codes, oef_tags)
    if pay_code:
        if float(pay_code['multiplier']) == 1.0:
            return True
        return False
    return True


def get_pay_code_from_entry(entry, all_pay_codes, oef_tags, config=None):
    """Get pay code information from an entry."""
    if entry:
        pay_code_uri = rail.find_first_by_attr_and_get_attr(
            entry['customMetadata'], 'keyUri', 'urn:replicon:object-type-uri:pay-code', 'value.uri')
        if pay_code_uri:
            return rail.find_first_by_attr_and_get_attr(
                all_pay_codes, 'uri', pay_code_uri)
        pay_type_oef_name = 'Pay Type'
        if config:
            pay_type_oef_name = Variable.get(
                config.pay_type_oef_var_name, default_var='Pay Type')
        tag_uri = rail.find_first_by_attr_and_get_attr(
            entry['extensionFieldValues'], 'definition.displayText', pay_type_oef_name, 'tag.uri')
        if tag_uri:
            oef_tag = rail.find_first_by_attr_and_get_attr(
                oef_tags, 'uri', tag_uri)
            if oef_tag:
                return {
                    "code": oef_tag['code'],
                    "multiplier": oef_tag['description'] if oef_tag['description'] else 1.0
                }
    return None


def get_hours(hours_object):
    """Calculate hours from a hours object (supports both hours and timePair formats)."""
    if hours_object['hours']:
        return hours_object['hours']['hours'] + hours_object['hours']['minutes']/60.00 + hours_object['hours']['seconds']/3600.00

    time_pair = hours_object['timePair']
    if time_pair and time_pair['startTime'] and time_pair['endTime']:
        end_date = datetime(
            1900, 1, 1, time_pair['endTime']['hour'], time_pair['endTime']['minute'], time_pair['endTime']['second'])
        start_date = datetime(
            1900, 1, 1, time_pair['startTime']['hour'], time_pair['startTime']['minute'], time_pair['startTime']['second'])
        if start_date > end_date:
            end_date = end_date + timedelta(days=1)
        diff = end_date - start_date
        return diff.total_seconds()/3600.00
    return 0.0


def get_duration_hours(duration):
    """Calculate hours from a duration object (used for timeoff entries)."""
    hours = duration.get('hours', 0) or 0
    minutes = duration.get('minutes', 0) or 0
    seconds = duration.get('seconds', 0) or 0
    return hours + (minutes / 60.0) + (seconds / 3600.0)
