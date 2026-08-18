

"""
URI resolution helpers for the master prereq results.

Each function takes the raw response from the corresponding rail.result(...)
prereq call and finds the URI matching a given input-file text value
(employee type name, department name, etc.).

Returns None when no match is found — callers should treat None as "log
exception and skip the row".
"""

import rail

def find_uri_by_attr(items, attr, value):
    """Linear search; returns the URI of the first item whose `attr` == `value`."""
    if not items or not value:
        return None
    for item in items:
        if isinstance(item, dict) and item.get(attr) == value:
            return item.get('uri')
    return None

def find_uri_by_display_text(items, value):
    """Most Replicon collections expose a `displayText` field (employee types,
    departments, holiday calendars, schedule types, permissions, etc.)."""
    return find_uri_by_attr(items, 'displayText', value)

def find_uri_by_name(items, value):
    """Some collections use `name` instead (timezones, scripts, policy sets)."""
    return find_uri_by_attr(items, 'name', value)

def employee_type_uri(value):
    return find_uri_by_display_text(rail.result('get_employee_type_groups'), value)

def department_uri(value):
    return find_uri_by_display_text(rail.result('get_department_groups'), value)

def company_uri(value):
    """Workato calls these 'service centers'; the input file column is COMPANY."""
    return find_uri_by_display_text(rail.result('get_service_centers'), value)

def _find_uri_any_attr(items, value, attrs=('displayText', 'name', 'slug')):
    """Try multiple attributes (displayText, name, slug) for matching."""
    if not items or not value:
        return None
    v = value.strip() if isinstance(value, str) else value
    for item in items:
        if not isinstance(item, dict):
            continue
        for attr in attrs:
            if item.get(attr) == v and item.get('uri'):
                return item['uri']
    return None

def holiday_calendar_uri(value):
    return _find_uri_any_attr(rail.result('get_all_holiday_calendars'), value)

def office_schedule_uri(value):
    return _find_uri_any_attr(rail.result('get_all_office_schedules'), value)

def supervisor_permission_uri():
    return rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_permission_set'), 'displayText', 'Supervisor', 'uri',
    )

def basic_user_permission_uri():
    return rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_permission_set'), 'displayText', 'Basic User with Reports', 'uri',
    )

def cme_entitlement_option_uri(value):
    """`Yes`/`No` from CME_ENTITLEMENT input column → option URI."""
    return find_uri_by_display_text(rail.result('get_cme_entitlement_options'), value)

def cme_entitlement_field_uri():
    return rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_custom_fields'), 'displayText', 'CME Entitlement', 'uri',
    )

def employee_classification_field_uri():
    return rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_custom_fields'), 'displayText', 'Employee Classification', 'uri',
    )

def _custom_field_uri_by_any(*display_text_candidates):
    """Try multiple displayText spellings against GetAllCustomFields."""
    fields = rail.result('get_all_custom_fields') or []
    for candidate in display_text_candidates:
        for f in fields:
            if isinstance(f, dict) and f.get('displayText') == candidate and f.get('uri'):
                return f['uri']
    return None

def fte_field_uri():
    """Numeric UDF for FTE — name varies per tenant (FTE Total / FTE / FTETotal)."""
    return _custom_field_uri_by_any('FTE Total', 'FTE', 'FTETotal', 'fte total', 'fte')

def fte_effective_date_field_uri():
    return _custom_field_uri_by_any(
        'FTE Effective Date', 'FTE Effective', 'FTE Date', 'Effective Date', 'FTE Change Effective Date'
    )

def timesheet_template_uri(value):
    return find_uri_by_display_text(rail.result('get_all_policy_sets'), value)

def timeoff_template_uri(value):
    return find_uri_by_display_text(rail.result('get_all_policy_sets'), value)

def timesheet_approval_uri(value):
    return find_uri_by_display_text(rail.result('get_timesheet_approval_paths'), value)

def timeoff_approval_uri(value):
    return find_uri_by_display_text(rail.result('get_timeoff_approval_paths'), value)

def timesheet_period_uri(value):
    """GetData-style endpoint — walks rows/cells and tries multiple attribute
    names (displayText, name, slug) for a match. Handles wrapped responses
    ({rows: [...]}, {data: [...]}, or a flat list)."""
    if not value:
        return None
    v = value.strip() if isinstance(value, str) else value
    result = rail.result('get_timesheet_periods')
    if isinstance(result, dict):
        rows = result.get('rows') or result.get('data') or result.get('items') or []
    else:
        rows = result or []
    for row in rows:
        if not isinstance(row, dict):
            continue
        for attr in ('displayText', 'name', 'slug'):
            if row.get(attr) == v and row.get('uri'):
                return row['uri']
        cells = row.get('cells')
        if isinstance(cells, list):
            for cell in cells:
                if not isinstance(cell, dict):
                    continue
                for attr in ('displayText', 'name', 'slug'):
                    if cell.get(attr) == v and cell.get('uri'):
                        return cell['uri']
    return None

def employee_classification_option_for_value(value):
    return find_uri_by_display_text(rail.result('get_employee_classification_options'), value)

_AUTH_TYPE_URIS = {

    'Replicon': 'urn:replicon:user-authentication-type:replicon',
    'SSO': 'urn:replicon:user-authentication-type:sso',
    'LDAP': 'urn:replicon:user-authentication-type:ldap',

}

_WORK_WEEK_URIS = {

    'Sunday': 'urn:replicon:day-of-week:sunday',
    'Monday': 'urn:replicon:day-of-week:monday',
    'Tuesday': 'urn:replicon:day-of-week:tuesday',
    'Wednesday': 'urn:replicon:day-of-week:wednesday',
    'Thursday': 'urn:replicon:day-of-week:thursday',
    'Friday': 'urn:replicon:day-of-week:friday',
    'Saturday': 'urn:replicon:day-of-week:saturday',

}

_SCHEDULE_TYPE_URIS = {

    'Shift': 'urn:replicon:schedule-type:shift',
    'Flex': 'urn:replicon:schedule-type:flex',
    'Flex Schedule': 'urn:replicon:schedule-type:flex',
    'Shift Schedule': 'urn:replicon:schedule-type:shift',

}

def authentication_type_uri(value):
    return _AUTH_TYPE_URIS.get((value or '').strip())

def work_week_start_day_uri(value):
    """The mapper may give either a day name ('Sunday') or the URN directly
    ('urn:replicon:day-of-week:sunday'). Pass-through URNs, look up names."""
    if not value:
        return None
    v = value.strip()
    if v.startswith('urn:'):
        return v
    return _WORK_WEEK_URIS.get(v)

def schedule_type_enum_uri(value):
    return _SCHEDULE_TYPE_URIS.get((value or '').strip())

def activities_for_department(activity_mapper, department):
    """Resolve all activity URIs that apply to the user's department from
    activity_department_mapper rows: {replicon_activity_name, department, check}.
    Looks up names against get_enabled_activities prereq result."""
    if not activity_mapper or not department:
        return []
    dep = department.strip()
    names = []
    for row in activity_mapper:
        if not isinstance(row, dict):
            continue
        if (row.get('department') or '').strip() == dep:
            name = (row.get('replicon_activity_name') or '').strip()
            if name and name not in names:
                names.append(name)
    activities = rail.result('get_enabled_activities') or []
    uris = []
    for name in names:
        for act in activities:
            if isinstance(act, dict) and act.get('displayText') == name and act.get('uri'):
                if act['uri'] not in uris:
                    uris.append(act['uri'])
                break
    return uris

