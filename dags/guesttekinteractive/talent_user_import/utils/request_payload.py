"""
Request Payload Builders - GuestTek Talent User Import Integration

Provides functions to build properly formatted request payloads for various
Replicon service endpoints.

For new users, all assignments (license, timesheet template, time off types,
holiday calendar, schedule, payrule, groups, timezone) are included in the
single CreateUserOrApplyModifications payload.
"""
import pendulum
from datetime import datetime
from uuid import uuid4
import rail
from guesttekinteractive.talent_user_import.mappers.user_sync_mapper import (
    get_mapper_settings, get_licenses_for_user, get_time_off_types_list
)
from guesttekinteractive.talent_user_import.mappers.timezone_mapper import get_timezone_for_location
from guesttekinteractive.talent_user_import import config as base_config

null = None
DATE_FORMAT = "%Y-%m-%d"
INVALID_DATE = "0000-00-00"


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def get_replicon_date(date_str, dt_format=DATE_FORMAT):
    if not date_str or date_str == INVALID_DATE:
        return null
    try:
        dt = datetime.strptime(date_str, dt_format)
        return {"year": dt.year, "month": dt.month, "day": dt.day}
    except (ValueError, TypeError):
        return null


def get_today_date():
    today = pendulum.now()
    return {"year": today.year, "month": today.month, "day": today.day}


# ---------------------------------------------------------------------------
# Prereq list payloads (used by get_user_prereqs task group)
# ---------------------------------------------------------------------------

def get_division_payload():
    return {
        "page": 1, "pagesize": 500,
        "columnUris": [
            "urn:replicon:division-list-column:division",
            "urn:replicon:division-list-column:full-path"
        ],
        "sort": [], "filterExpression": null
    }


def get_location_payload():
    return {
        "page": 1, "pagesize": 500,
        "columnUris": [
            "urn:replicon:location-list-column:location"
        ],
        "sort": [], "filterExpression": null
    }


def get_employeetype_group_payload():
    return {
        "page": 1, "pagesize": 500,
        "columnUris": [
            "urn:replicon:employee-type-group-list-column:employee-type-group",
            "urn:replicon:employee-type-group-list-column:full-path"
        ],
        "sort": [], "filterExpression": null
    }


def get_ts_period_payload():
    return {
        "page": 1, "pagesize": 200,
        "columnUris": [
            "urn:replicon:timesheet-period-list-column:timesheet-period",
            "urn:replicon:timesheet-period-list-column:name"
        ],
        "sort": [], "filterExpression": null
    }


# ---------------------------------------------------------------------------
# User search payloads
# ---------------------------------------------------------------------------

def get_user_data_payload(dag_run):
    return {
        "users": [{
            "uri": null, "loginName": null,
            "employeeId": dag_run.conf['employee_id'],
            "parameterCorrelationId": null
        }],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }


def get_supervisor_data_payload(dag_run):
    return {
        "users": [{
            "uri": null, "loginName": null,
            "employeeId": dag_run.conf.get('supervisor_employee_id', ''),
            "parameterCorrelationId": null
        }],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }


# ---------------------------------------------------------------------------
# Employee type hierarchy creation
# ---------------------------------------------------------------------------

def get_employee_types_hierarchy_payload(dag_run):
    names = dag_run.conf['full_path'].split('|')
    hierarchy = []
    for i, name in enumerate(names):
        item = {}
        if i > 0:
            target = {"parent": {}}
            current = target["parent"]
            for j in reversed(range(i)):
                current["name"] = names[j]
                if j > 0:
                    current["parent"] = {}
                    current = current["parent"]
            item["target"] = target
        item["modificationToApply"] = {"name": name, "isEnabled": True}
        hierarchy.append(item)
    return {
        "hierarchy": hierarchy,
        "modificationOptionUri": "urn:replicon:hierarchy-modification-option:save",
        "unitOfWorkId": str(uuid4())
    }


# ---------------------------------------------------------------------------
# Supervisor assignment
# ---------------------------------------------------------------------------

def get_supervisor_assignment_payload(dag_run, supervisor_uri, user_uri=None):
    return {
        "userUri": user_uri or dag_run.conf.get('useruri'),
        "dateRange": {"startDate": get_today_date(), "endDate": null},
        "supervisorUri": supervisor_uri
    }


def get_update_supervisor_permission_payload(dag_run, supervisor_result_task_id='search_supervisor_in_replicon'):
    """Build payload to assign the Supervisor permission to a user.

    Args:
        dag_run: DAG run context containing supervisor_permission_uri in conf.
        supervisor_result_task_id: Task ID whose result contains supervisor details.

    Returns:
        dict: ApplyUserModifications3 payload.
    """
    return {
        "user": {
            "uri": rail.result(supervisor_result_task_id)[0]["userDetails"]["uri"]
        },
        "modifications": {
            "permissionSetsToApply": {
                "permissionSetUrisToAssign": [dag_run.conf["supervisor_permission_uri"]],
                "policyUrisToRemovePermissionSet": []
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


# ---------------------------------------------------------------------------
# Add-user helper: build schedule-style assignment lists
# ---------------------------------------------------------------------------

def _build_schedule_item(name=null, uri=null, parent_uri=null):
    """Generic schedule item with dateRange=null (effective immediately)."""
    item = {}
    if uri:
        item["uri"] = uri
    if parent_uri:
        item["parentUri"] = parent_uri
    if name:
        item["name"] = name
    if not item:
        return null
    return [{"dateRange": null, "item": item}]


def _get_products_for_add(location_name, department_code):
    """Build products (licenses) block for the create-user payload."""
    license_string = ''
    settings = get_mapper_settings(location_name, department_code)
    if settings:
        license_string = settings.get('license', '')

    if not license_string:
        return []

    license_items = []
    for lic in license_string.split('|'):
        lic = lic.strip()
        uri = base_config.LICENSE_URI_MAP.get(lic)
        if uri:
            license_items.append({"uri": uri, "name": null})

    if not license_items:
        return []

    return [{
        "modificationOptionUri": "urn:replicon:collection-modification-option:add",
        "items": license_items
    }]


def _get_policySets_for_add(location_name, department_code, all_templates, config):
    """Build policySets block - timesheet template + time off template."""
    items = []
    
    # Timesheet template from mapper
    settings = get_mapper_settings(location_name, department_code)
    if settings and settings.get('timesheet_template'):
        template_name = settings['timesheet_template']
        for t in (all_templates or []):
            if t.get('displayText') == template_name:
                items.append({"uri": t.get('uri'), "name": null})
                break
        else:
            items.append({"uri": null, "name": template_name})
    
    # Time Off template (constant)
    items.append({"uri": null, "name": config.default_time_off_template})
    
    if not items:
        return []
    
    return [{
        "modificationOptionUri": "urn:replicon:collection-modification-option:add",
        "items": items
    }]


def _get_timeOffTypes_for_add(location_name, department_code, all_time_off_types):
    """Build timeOffTypes block with default policy."""
    time_off_names = get_time_off_types_list(location_name, department_code)
    if not time_off_names:
        return []

    items = []
    for name in time_off_names:
        tot_entry = {
            "timeOffType": {"uri": null, "name": name},
            "isTimeOffAllowedAgainstThisTimeOffType": "true",
            "applyDefaultTimeOffTypePolicy": "true",
            "defaultTimeOffTypePolicyEffectiveDate": null,
            "policySchedule": []
        }
        # Try to resolve URI from prereqs
        for tot in (all_time_off_types or []):
            if tot.get('displayText') == name:
                tot_entry["timeOffType"] = {"uri": tot.get('uri'), "name": null}
                break
        items.append(tot_entry)

    if not items:
        return []

    return [{
        "modificationOptionUri": "urn:replicon:collection-modification-option:add",
        "items": items
    }]


def _get_holidayCalendarSchedule_for_add(location_name, department_code, all_calendars):
    """Build holidayCalendarSchedule block."""
    settings = get_mapper_settings(location_name, department_code)
    if not settings or not settings.get('holiday_calendar'):
        return []

    calendar_name = settings['holiday_calendar']
    for c in (all_calendars or []):
        if c.get('displayText') == calendar_name:
            return [{"dateRange": null, "item": {"uri": c.get('uri'), "name": null}}]
    # Fallback by name
    return [{"dateRange": null, "item": {"uri": null, "name": calendar_name}}]


def _get_scheduleTypeSchedule_for_add(location_name, department_code, all_schedules):
    """Build scheduleTypeSchedule block."""
    settings = get_mapper_settings(location_name, department_code)
    if not settings or not settings.get('schedule_type'):
        return []

    schedule_name = settings['schedule_type']
    
    # Shift schedule has a different payload structure
    if schedule_name.lower() == 'shift schedule':
        return [{
            "dateRange": null,
            "item": {
                "scheduleTypeUri": "urn:replicon:schedule-type:shift",
                "officeSchedule": null
            }
        }]
    
    return [{
        "dateRange": null,
        "item": {
            "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule",
            "officeSchedule": {"officeScheduleUri": null, "name": schedule_name}
        }
    }]


def _get_payRuleSchedule_for_add(location_name, department_code, all_payrules):
    """Build payRuleSchedule block."""
    settings = get_mapper_settings(location_name, department_code)
    if not settings or not settings.get('payrule'):
        return []

    payrule_name = settings['payrule']
    for p in (all_payrules or []):
        if p.get('displayText') == payrule_name:
            return [{"dateRange": null, "item": {"uri": p.get('uri'), "name": null}}]
    # Fallback by name
    return [{"dateRange": null, "item": {"uri": null, "name": payrule_name}}]


# ---------------------------------------------------------------------------
# Main create / update user payload
# ---------------------------------------------------------------------------

def get_create_update_user_payload(config, dag_run, user_action):
    """
    Build complete payload for creating or updating a user in Replicon.

    For add_user: includes ALL assignments (license, timesheet template,
    time off types, holiday calendar, schedule, payrule, groups, timezone)
    in the single API call.

    For update_user: only basic fields (name, email, dates, status).
    """
    is_add = user_action == 'add_user'
    is_disabled = dag_run.conf.get('user_deactivated', 0) == 1

    location_name = dag_run.conf.get('location_name', '')
    department_code = dag_run.conf.get('department_code', '')

    # Base modifications (common to add and update)
    modifications = {
        "firstName": {"value": dag_run.conf.get('first_name', '')},
        "lastName": {"value": dag_run.conf.get('last_name', '')},
        "emailAddress": {"value": dag_run.conf.get('email', '')},
        "isLoginEnabled": {"value": not is_disabled},
    }

    # Fields only set on add
    if is_add:
        modifications["loginName"] = {"value": dag_run.conf.get('login_name', '')}
        modifications["employeeId"] = {"value": dag_run.conf.get('employee_id', '')}

    # Employment date range
    start_date = dag_run.conf.get('user_hire_date', '')
    if start_date and start_date != INVALID_DATE:
        end_date = dag_run.conf.get('end_date', '')
        modifications["employmentDateRange"] = {
            "value": {
                "startDate": get_replicon_date(start_date),
                "endDate": get_replicon_date(end_date) if end_date and end_date != INVALID_DATE else null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            }
        }

    # --- Add-only assignments below ---
    if is_add:
        # Language
        modifications["displayLanguage"] = {
            "value": {"uri": f"urn:replicon:language:{config.default_language}", "name": null}
        }

        # Timezone
        all_timezones = dag_run.conf.get('replicon_timezones', [])
        timezone_text = get_timezone_for_location(location_name)
        if timezone_text and all_timezones:
            for tz in all_timezones:
                if tz.get('displayText') == timezone_text:
                    modifications["timeZone"] = {"value": {"uri": tz.get('uri'), "IANAName": null}}
                    break

        # Location group
        location_uri = dag_run.conf.get('location_uri')
        if location_uri:
            modifications["locationSchedule"] = [{
                "dateRange": null,
                "item": {"uri": location_uri, "parentUri": null, "name": null}
            }]

        # Department group
        department_uri = dag_run.conf.get('department_uri')
        if department_uri:
            modifications["departmentGroupSchedule"] = [{
                "dateRange": null,
                "item": {"uri": department_uri, "parentUri": null, "name": null}
            }]

        # Employee type group - by name
        emp_type = dag_run.conf.get('employee_work_schedule_value', '')
        if emp_type:
            modifications["employeeTypeGroupSchedule"] = [{
                "dateRange": null,
                "item": {"uri": null, "parent": null, "name": emp_type, "parameterCorrelationId": null}
            }]

        # Products (licenses)
        products = _get_products_for_add(location_name, department_code)
        if products:
            modifications["products"] = products

        # Timesheet template (policySets)
        all_templates = dag_run.conf.get('replicon_timesheet_templates', [])
        policy_sets = _get_policySets_for_add(location_name, department_code, all_templates, config)
        if policy_sets:
            modifications["policySets"] = policy_sets

        # Time off types
        all_time_off_types = dag_run.conf.get('replicon_time_off_types', [])
        time_off_types = _get_timeOffTypes_for_add(location_name, department_code, all_time_off_types)
        if time_off_types:
            modifications["timeOffTypes"] = time_off_types

        # Holiday calendar
        all_calendars = dag_run.conf.get('replicon_holiday_calendars', [])
        holiday_cal = _get_holidayCalendarSchedule_for_add(location_name, department_code, all_calendars)
        if holiday_cal:
            modifications["holidayCalendarSchedule"] = holiday_cal

        # Schedule
        all_schedules = dag_run.conf.get('replicon_schedules', [])
        schedule = _get_scheduleTypeSchedule_for_add(location_name, department_code, all_schedules)
        if schedule:
            modifications["scheduleTypeSchedule"] = schedule

        # Payrule
        all_payrules = dag_run.conf.get('replicon_payrules', [])
        payrule = _get_payRuleSchedule_for_add(location_name, department_code, all_payrules)
        if payrule:
            modifications["payRuleSchedule"] = payrule

        # Project Role (from job_title)
        job_title = dag_run.conf.get('job_title', '')
        if job_title:
            modifications["projectRoleSchedule"] = [{
                "dateRange": null,
                "item": {
                    "projectRole": {
                        "uri": null,
                        "name": job_title
                    },
                    "isPrimary": "true"
                }
            }]

        # Display Name (from additional info preferred name)
        additional_info = rail.result('fetch_additional_info') or {}
        preferred_name = additional_info.get('preferred_name', '')
        if preferred_name:
            modifications["displayName"] = {"value": preferred_name}

        # Custom Fields (LOA and Date of LOA from additional info)
        custom_fields = []
        loa_value = additional_info.get('leave_of_absence', '')
        if loa_value:
            custom_fields.append({
                "value": {
                    "customField": {"uri": null, "name": "Leave Of Absence"},
                    "text": null,
                    "date": null,
                    "dropDownOption": {"uri": null, "name": loa_value},
                    "number": null
                }
            })
        date_of_loa = additional_info.get('date_of_loa', '')
        if date_of_loa:
            custom_fields.append({
                "value": {
                    "customField": {"uri": null, "name": "Date of LOA"},
                    "text": null,
                    "date": get_replicon_date(date_of_loa),
                    "dropDownOption": null,
                    "number": null
                }
            })
        if custom_fields:
            modifications["customFields"] = custom_fields
        
        # Permission Sets
        if config.default_permission_sets:
            modifications["permissionSets"] = [{
                "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                "items": [{
                    "permissionSetPolicy": {"uri": null, "name": perm_name},
                    "groupAccessFilter": null
                } for perm_name in config.default_permission_sets]
            }]

        # Timesheet Period
        modifications["timesheetPeriodSchedule"] = [{
            "dateRange": null,
            "item": {"uri": null, "name": config.default_timesheet_period}
        }]

        # Timesheet Approval Path
        modifications["timesheetApprovalPath"] = {
            "value": {"uri": null, "name": config.default_timesheet_approval_path}
        }

        # --- Update-only assignments below ---
    if not is_add:
        # Get current user data for comparison
        user_data = rail.result('get_user_data')
        current_user = user_data[0] if user_data else {}

        # Location group (only if changed)
        location_uri = dag_run.conf.get('location_uri')
        if location_uri:
            current_location = current_user.get('locationSchedule', [])
            current_location_uri = current_location[-1]['location']['uri'] if current_location else ''
            if location_uri != current_location_uri:
                modifications["locationSchedule"] = [{
                    "dateRange": {"startDate": get_today_date()},
                    "item": {"uri": location_uri, "parentUri": null, "name": null}
                }]

        # Department group (only if changed)
        department_uri = dag_run.conf.get('department_uri')
        if department_uri:
            current_dept = current_user.get('departmentGroupSchedule', [])
            current_dept_uri = current_dept[-1]['departmentGroup']['uri'] if current_dept else ''
            if department_uri != current_dept_uri:
                modifications["departmentGroupSchedule"] = [{
                    "dateRange": {"startDate": get_today_date()},
                    "item": {"uri": department_uri, "parentUri": null, "name": null}
                }]

        # Employee type group (only if changed)
        emp_type = dag_run.conf.get('employee_work_schedule_value', '')
        if emp_type:
            current_emp_type = current_user.get('employeeTypeGroupSchedule', [])
            current_emp_type_name = current_emp_type[-1]['employeeTypeGroup']['displayText'] if current_emp_type else ''
            if emp_type != current_emp_type_name:
                modifications["employeeTypeGroupSchedule"] = [{
                    "dateRange": {"startDate": get_today_date()},
                    "item": {"uri": null, "parent": null, "name": emp_type, "parameterCorrelationId": null}
                }]

        # Custom Fields - LOA (from additional info)
        additional_info = rail.result('fetch_additional_info') or {}
        custom_fields = []
        loa_value = additional_info.get('leave_of_absence', '')
        if loa_value:
            custom_fields.append({
                "value": {
                    "customField": {"uri": null, "name": "Leave Of Absence"},
                    "text": null,
                    "date": null,
                    "dropDownOption": {"uri": null, "name": loa_value},
                    "number": null
                }
            })
        date_of_loa = additional_info.get('date_of_loa', '')
        if date_of_loa:
            custom_fields.append({
                "value": {
                    "customField": {"uri": null, "name": "Date of LOA"},
                    "text": null,
                    "date": get_replicon_date(date_of_loa),
                    "dropDownOption": null,
                    "number": null
                }
            })
        if custom_fields:
            modifications["customFields"] = custom_fields

    # Clean up null values
    modifications = {k: v for k, v in modifications.items() if v is not null}

    payload = {
        "target": {
            "uri": dag_run.conf.get('useruri'),
            "loginName": null, "employeeId": null, "parameterCorrelationId": null
        } if not is_add else null,
        "template": {"templateTarget": null} if is_add else null,
        "modifications": modifications,
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid4())
    }

    return payload


# ---------------------------------------------------------------------------
# License update payload (kept for backward compat, no longer used by add)
# ---------------------------------------------------------------------------

def get_license_update_payload(user_uri, license_uris):
    return {
        "user": {"uri": user_uri},
        "modifications": {
            "productAssignmentsToApply": {
                "productUrisToAssign": license_uris,
                "productUrisToUnassign": []
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }
