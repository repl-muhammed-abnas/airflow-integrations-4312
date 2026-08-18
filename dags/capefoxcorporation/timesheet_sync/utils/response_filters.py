from airflow.models import Variable
import rail

from capefoxcorporation.timesheet_sync.utils.custom_methods import get_formatted_date


def get_user_company(user_details):
    """Extract user company from user details extension fields."""
    company = rail.find_first_by_attr_and_get_attr(
        user_details['extensionFieldValues'], 'definition.displayText', 'Company', 'textValue')
    return [company]


def get_cost_center_uri(user_details_list):
    """Safely extract cost center URI from user details."""
    if not user_details_list or len(user_details_list) == 0:
        return None
    user = user_details_list[0]
    cost_center_schedule = user.get('costCenterSchedule')
    if not cost_center_schedule or len(cost_center_schedule) == 0:
        return None
    last_schedule = cost_center_schedule[-1]
    cost_center = last_schedule.get('costCenter')
    if not cost_center:
        return None
    return cost_center.get('uri')


def get_project_uris(tasks):
    """Extract unique project URIs from task details."""
    project_uris = []
    projects = []
    if tasks:
        for task in tasks:
            project_uri = task['project']['uri']
            if project_uri not in project_uris:
                projects.append({"uri": project_uri})
                project_uris.append(project_uri)
    return projects


def get_timesheet_header_seq(existing_timesheet):
    """Get next timesheet header sequence number."""
    if not existing_timesheet or \
        not existing_timesheet['document'] \
            or len(existing_timesheet['document']['rows']) == 0:
        return 1
    seq_no = get_max_header_seq(existing_timesheet) + 1
    return seq_no


def get_max_header_seq(existing_timesheet):
    """Get maximum header sequence number from existing timesheet."""
    seq_no = 1
    for row in existing_timesheet['document']['rows']:
        if row['row']['data']['TS_HDR_SEQ_NO'] > seq_no:
            seq_no = row['row']['data']['TS_HDR_SEQ_NO']
    return seq_no


def get_period_number(timesheet):
    """Extract period number (month) from timesheet."""
    return timesheet['dateRange']['endDate']['month']


def get_financial_year(timesheet):
    """Extract financial year from timesheet."""
    return timesheet['dateRange']['endDate']['year']


def get_task_uris(entries):
    """Extract unique task URIs from time entries."""
    task_uris = []
    for entry in entries:
        task_uri = get_task_uri(entry)
        if task_uri and task_uri not in task_uris:
            task_uris.append(task_uri)
    return task_uris


def get_oef_tag_uris(entries, config):
    """Extract unique OEF tag URIs from time entries."""
    tag_uris = []
    pay_type_oef_name = Variable.get(
        config.pay_type_oef_var_name, default_var='Pay Type')
    if pay_type_oef_name:
        for entry in entries:
            tag_uri = rail.find_first_by_attr_and_get_attr(
                entry['extensionFieldValues'], 'definition.displayText', pay_type_oef_name, 'tag.uri')
            if tag_uri and tag_uri not in tag_uris:
                tag_uris.append(tag_uri)
    return tag_uris


def get_timesheet_date(timesheet):
    """Get formatted timesheet date from end date."""
    return get_formatted_date(timesheet['dateRange']['endDate'])


def get_mo_project_id(project_details, task_details, entry, config):
    """Get MO project ID from project extension fields."""
    task_uri = get_task_uri(entry)
    task_info = list(
        filter(lambda x: x['uri'] == task_uri, task_details))
    project_uri = task_info[0]['project']['uri'] if task_info else None
    if project_uri:
        project_info = list(
            filter(lambda x: x['projectDetails']['uri'] == project_uri, project_details))
        if not project_info:
            return ""
        custom_field_info = project_info[0]['projectDetails']["extensionFieldValues"]
        reference_project = rail.find_first_by_attr_and_get_attr(
            custom_field_info, 'definition.displayText', config.proj_reference_project_id, 'textValue')
        return reference_project
    return ""


def get_mo_details(task_details, entry, config):
    """Get MO (Manufacturing Order) details from entry."""
    activity_type = rail.find_first_by_attr_and_get_attr(
        entry['extensionFieldValues'], 'definition.displayText', config.activity_type, 'tag.displayText')
    workcenter = rail.find_first_by_attr_and_get_attr(
        entry['extensionFieldValues'], 'definition.displayText', config.work_center, 'tag.displayText')
    task_uri = get_task_uri(entry)
    for task in task_details:
        if task['uri'] == task_uri:
            return {
                "mo_id": task['project']['code'],
                "seq": task['parent']['task']['code'],
                "step": task['code'],
                "activity_type": activity_type,
                "workcenter": workcenter
            }
    return None


def get_comments(entry):
    """Extract comments from entry custom metadata."""
    if entry and entry['customMetadata']:
        comment = rail.find_first_by_attr_and_get_attr(
            entry['customMetadata'], 'keyUri', 'urn:replicon:time-entry-metadata-key:comments', 'value.text')
        return comment[0:254] if comment else ''
    return ''


def get_line_date(entry):
    """Get formatted line date from entry."""
    return get_formatted_date(entry['entryDate'])


def get_task_codes(entry, task_details):
    """Get project ID and PLC code from entry.

    Integration Spec (PLC-leaf assumption):
    In the capefoxcorporation project_sync integration, PLCs from Costpoint are synced
    to Replicon as the last level child task under the project hierarchy. Users enter
    time against these PLC leaf tasks in Replicon. Therefore:
    - PROJ_ID: parent task code (the actual Costpoint project)
    - PLC (BILL_LAB_CAT_CD): current task code (the billing labor category)

    This assumption is guaranteed by the project_sync integration which creates the
    task hierarchy. If a non-PLC task has a parent, the parent code is still returned
    as PROJ_ID, which is consistent with the Costpoint data model.

    Returns:
        tuple: (project_id, plc_code) - both are strings, empty string if not found
    """
    task_uri = get_task_uri(entry)
    if not task_uri:
        return ("", "")

    for task in task_details:
        if task.get('uri') == task_uri:
            plc_code = task.get('code', "")
            # Get parent task code as PROJ_ID
            parent = task.get('parent')
            if parent and parent.get('task'):
                project_id = parent['task'].get('code', "")
            else:
                # No parent - task itself is the project (not a PLC leaf)
                project_id = plc_code
                plc_code = ""
            return (project_id, plc_code)

    return ("", "")


def get_project_id(entry, task_details):
    """Get project ID from entry.

    In Replicon, PLCs from Costpoint are created as the last level child task.
    The parent task of the PLC is the actual PROJ_ID in Costpoint.
    """
    project_id, _ = get_task_codes(entry, task_details)
    return project_id


def get_plc_code(entry, task_details):
    """Get PLC (Project Labor Category) code from entry.

    In Replicon, PLCs from Costpoint are created as the last level child task.
    The task code of the PLC task is the BILL_LAB_CAT_CD in Costpoint.
    """
    _, plc_code = get_task_codes(entry, task_details)
    return plc_code


def get_task_uri(entry):
    """Get task URI from entry custom metadata."""
    return rail.find_first_by_attr_and_get_attr(
        entry['customMetadata'], 'keyUri', 'urn:replicon:time-entry-metadata-key:task', 'value.uri')


def get_pay_type(entry, all_pay_codes, oef_tags, config):
    """Get pay type from entry."""
    pay_code = get_pay_code(entry, all_pay_codes, oef_tags, config)
    return pay_code['code'] if pay_code and pay_code['code'] else config.regular_pay_type


def is_timeoff_allocated_entry(entry, all_pay_codes, oef_tags, config):
    """Check if a time entry was auto-populated from a timeoff booking.

    Timeoff entries allocated by the 'Allocate Timesheet Hours' automation
    use the 'Time Off' paycode. These should be skipped as we handle timeoff
    via the GetAllOverlappingTimeOffForTimesheet2 API call instead.

    Args:
        entry: Time entry from Replicon
        all_pay_codes: List of all paycodes from Replicon
        oef_tags: OEF tag details
        config: Configuration module

    Returns:
        bool: True if entry has 'Time Off' paycode (is a timeoff-allocated entry)
    """
    pay_code = get_pay_code(entry, all_pay_codes, oef_tags, config)
    if pay_code:
        pay_code_name = pay_code.get('displayText') or pay_code.get('name') or ''
        return pay_code_name.lower() == 'time off'
    return False


def get_pay_code(entry, all_pay_codes, oef_tags, config):
    """Get pay code information from entry."""
    if entry:
        pay_code_uri = rail.find_first_by_attr_and_get_attr(
            entry['customMetadata'], 'keyUri', 'urn:replicon:object-type-uri:pay-code', 'value.uri')
        if pay_code_uri:
            return rail.find_first_by_attr_and_get_attr(
                all_pay_codes, 'uri', pay_code_uri)
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


def get_timeoff_type_uris(time_offs):
    """Extract unique timeoff type URIs from timeoff bookings."""
    unique_timeoff_types = []
    if time_offs:
        for time_off in time_offs:
            time_off_type = time_off.get('timeOffType')
            if time_off_type:
                time_off_type_uri = time_off_type.get('uri')
                if time_off_type_uri and time_off_type_uri not in unique_timeoff_types:
                    unique_timeoff_types.append(time_off_type_uri)
    return unique_timeoff_types


def get_time_off_project_id(employee_id, config):
    """Get project ID for timeoff based on employee ID prefix.

    Cape Fox routes sick leave to different Costpoint leave projects based on
    the first 2 characters of the employee ID:
    - 03, 04, 08 → LEAVE1.SIC
    - 32 → LEAVE3.SIC
    - Other prefixes → empty string (will trigger Costpoint validation error)

    Args:
        employee_id: The employee's Costpoint employee ID
        config: Configuration module containing timeoff_employee_prefix_mapping

    Returns:
        str: The Costpoint PROJ_ID for the leave project, or empty string if unmapped
    """
    if not employee_id or len(employee_id) < 2:
        return ''
    prefix = employee_id[:2]
    mapping = getattr(config, 'timeoff_employee_prefix_mapping', {})
    prefix_config = mapping.get(prefix, {})
    return prefix_config.get('project', '')


def get_time_off_pay_type(employee_id, config):
    """Get pay type for timeoff entries based on employee ID prefix.

    Cape Fox uses pay type from the employee prefix mapping.
    Falls back to 'SIC' if not configured.

    Args:
        employee_id: The employee's Costpoint employee ID
        config: Configuration module containing timeoff_employee_prefix_mapping

    Returns:
        str: The PAY_TYPE code for timeoff entries (default: 'SIC')
    """
    if not employee_id or len(employee_id) < 2:
        return 'SIC'
    prefix = employee_id[:2]
    mapping = getattr(config, 'timeoff_employee_prefix_mapping', {})
    prefix_config = mapping.get(prefix, {})
    return prefix_config.get('pay_type', 'SIC')
