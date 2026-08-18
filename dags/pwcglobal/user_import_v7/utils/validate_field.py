import re
from pwcglobal.user_import_v7.utils import request_payload

email_regex = re.compile(
    r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
required = True


def v_emailaddress_add(data):
    email = data.get('emailaddress')
    if not email:
        return 'Email address not present in payload'
    if email and not re.fullmatch(email_regex, email):
        return 'Email not updated since email field received incorrect format'
    return False

def v_enddate_add(data):
    end = data.get('enddate')
    if end and not request_payload.get_replicon_date(end):
        return 'Incorrect date format received for Enddate'
    return False

def v_holidaycalendar_blank(data):
    if not data.get('holidaycalendar'):
        return 'Holiday calendar not assigned since blank value received'
    return False

def v_holidaycalender_uri(data):
    hc = data.get('holidaycalendar')
    hcuri = data.get('holidaycalenderuri')
    if hc and not hcuri:
        return f'Holiday calendar {hc} not available in Replicon'
    return False

def v_linemanager_blank(data):
    if not data.get('linemanagerpartyid'):
        return 'Line manager not assigned since the Line manager is not provided'
    return False

def v_scheduletype_blank(data):
    if not data.get('scheduletype'):
        return 'Schedule type is not assigned since blank value received'
    return False

def v_startdate_add(data):
    start = data.get('startdate')
    if not request_payload.get_replicon_date(start):
        return 'Incorrect date format received for Startdate'
    return False

def v_supervisor_add(data):
    sup = data.get('supervisor')
    if not sup:
        return 'Supervisor not assigned since the Supervisor ID is not provided'
    if sup and '||' in sup:
        parts = sup.split('||')
        if len(parts) >= 2 and parts[0] == parts[1]:
            return 'Supervisor not assigned since the Supervisor partyID and user party ID are the same'
    return False

def v_timesheetapprovalpath_blank(data):
    if not data.get('timesheetapprovalpath'):
        return 'Timesheet approval path not assigned since blank value received'
    return False

def v_timeentryapprovalpath_blank(data):
    if not data.get('timeentryapprovalpath'):
        return 'Timeentry approval path System Approval assigned since blank value received'
    return False

def v_timesheettemplate_blank(data):
    if not data.get('timesheettemplate'):
        return 'Timesheet template not assigned since blank value received'
    return False

def v_timesheetperiodtype_blank(data):
    if not data.get('timesheetperiodtype'):
        return 'Timesheet period type is not assigned since blank value received'
    return False

def v_workdayid_blank(data):
    if not data.get('workdayid'):
        return 'Workdayid is not assigned since blank value received'
    return False

# ---- URI availability validators (ADD) ----
def v_employeetypegroup_uri_add(data):
    et = data.get('employeetype')
    uri = data.get('employeetypegroupuri')
    if et and not uri:
        return f'Employee type {et} not available or is disabled in Replicon'
    return False

def v_companycodegroup_uri_add(data):
    cc = data.get('companycode')
    uri = data.get('companycodegroupuri')
    if cc and not uri:
        return f'Company code {cc} not available in Replicon'
    return False

def v_legalentitygroup_uri_add(data):
    le = data.get('legalentity')
    uri = data.get('legalentitygroupuri')
    if le and not uri:
        return f'Legal entity {le} not available in Replicon'
    return False

def v_countriesgroup_uri_add(data):
    country = data.get('country')
    uri = data.get('countriesgroupuri')
    if country and not uri:
        return f'Country {country} not available in Replicon'
    return False

def v_timesheettemplate_uri_add(data):
    t = data.get('timesheettemplate')
    uri = data.get('timesheettemplateuri')
    if t and not uri:
        return f'Timesheet template {t} not available in Replicon'
    return False

def v_timesheetapprovalpath_uri_add(data):
    t = data.get('timesheetapprovalpath')
    uri = data.get('timesheetapprovalpathuri')
    if t and not uri:
        return f'Timesheet approval path {t} not available in Replicon'
    return False

def v_timezone_uri_add(data):
    tz = data.get('timezone')
    uri = data.get('timezoneuri')
    if tz and not uri:
        return f'timezone {tz} not available in Replicon'
    return False

def v_supervisorlegalentity_uri_add(data):
    sup = data.get('supervisor')
    uri = data.get('supervisorlegalentityuri')
    if sup and not uri:
        return f'supervisor legal entity {sup} not available in Replicon'
    return False

def v_schedule_uri_add(data):
    st = data.get('scheduletype')
    uri = data.get('scheduleuri')
    if st and not uri:
        return f'schedule type {st} not available in Replicon'
    return False

def v_gradedropdown_uri_add(data):
    grade = data.get('grade')
    uri = data.get('gradedropdownuri')
    if grade and not uri:
        return f'Grade {grade} not available in Replicon'
    return False

def v_profilestatusdropdown_uri_add(data):
    ps = data.get('profilestatus')
    uri = data.get('profilestatusdropdownuri')
    if ps and not uri:
        return f'profile status {ps} not available in Replicon'
    return False

def v_permissionset_uri_add(data):
    perm = data.get('adduserpermission')
    uri = data.get('permissionseturi')
    if not uri and perm:
        return f'Permissoin set  {perm} not available in Replicon'
    return False

def v_timeentryapprovalpath_uri_add(data):
    t = data.get('timeentryapprovalpath')
    uri = data.get('timeentryapprovalpathuri')
    if t and not uri:
        return f'Timeentry approval path {t} not available in Replicon'
    return False

def v_payrule_uri_add(data):
    pr = data.get('payrule')
    uri = data.get('payruleuri')
    if pr and not uri:
        return f'payrule {pr} not available in Replicon'
    return False

def v_zerotimeuserpermissionset_uri_add(data):
    perm = data.get('zerotimepermission')
    uri = data.get('zerotimeuserpermissionseturi')
    if not uri and perm:
        return f'{perm} not available in Replicon'
    return False

def v_timeoffapprovalpath_uri_add(data):
    t = data.get('timeoffapprovalpath')
    uri = data.get('timeoffapprovalpathuri')
    if t and not uri:
        return f'Time off approval path {t} not available in Replicon'
    return False

def v_supervisory_org_uri_add(data):
    name = data.get('supervisoryorgname')
    uri = data.get('supervisory_org_uri')
    if name and not uri:
        return f'Supervisory Org {name} not available in Replicon'
    return False


# ---------- UPDATE validators ----------
def v_emailaddress_update(data):
    email = data.get('emailaddress')
    if email and not re.fullmatch(email_regex, email):
        return 'Email not updated since email received is incorrect format'
    return False

def v_enddate_update(data):
    end = data.get('enddate')
    if end and not request_payload.get_replicon_date(end):
        return 'Enddate not updated since Enddate received is incorrect format'
    return False

def v_holidaycalender_uri_update(data):
    hc = data.get('holidaycalendar')
    hcuri = data.get('holidaycalenderuri')
    if hc and not hcuri:
        return f'Holiday calendar not updated since Holiday calendar {hc} not available in Replicon'
    return False

def v_startdate_update(data):
    start = data.get('startdate')
    if not request_payload.get_replicon_date(start):
        return 'Startdate not updated since Startdate received is incorrect format'
    return False

def v_supervisor_update(data):
    sup = data.get('supervisor')
    if sup and '||' in sup:
        parts = sup.split('||')
        if len(parts) >= 2 and parts[0] == parts[1]:
            return 'Supervisor not assigned since the Supervisor partyID and user party ID are the same'
    return False

def v_employeetypegroup_uri_update(data):
    et = data.get('employeetype')
    uri = data.get('employeetypegroupuri')
    if et and not uri:
        return f'Employee type not updated since Employee type {et} not available or is disabled in Replicon'
    return False

def v_companycodegroup_uri_update(data):
    cc = data.get('companycode')
    uri = data.get('companycodegroupuri')
    if cc and not uri:
        return f'Company code not updated since Company code {cc} not available in Replicon'
    return False

def v_legalentitygroup_uri_update(data):
    le = data.get('legalentity')
    uri = data.get('legalentitygroupuri')
    if le and not uri:
        return f'Legal entity not updated since Legal entity {le} not available in Replicon'
    return False

def v_countriesgroup_uri_update(data):
    country = data.get('country')
    uri = data.get('countriesgroupuri')
    if country and not uri:
        return f'Country not updated since Country {country} not available in Replicon'
    return False

def v_timesheettemplate_uri_update(data):
    t = data.get('timesheettemplate')
    uri = data.get('timesheettemplateuri')
    if t and not uri:
        return f'Timesheet template not updated since Timesheet template {t} not available in Replicon'
    return False

def v_timesheetapprovalpath_uri_update(data):
    t = data.get('timesheetapprovalpath')
    uri = data.get('timesheetapprovalpathuri')
    if t and not uri:
        return f'Timesheet approval path not updated since Timesheet approval path {t} not available in Replicon'
    return False

def v_timezone_uri_update(data):
    tz = data.get('timezone')
    uri = data.get('timezoneuri')
    if tz and not uri:
        return f'timezone not updated since timezone {tz} not available in Replicon'
    return False

def v_supervisorlegalentity_uri_update(data):
    sup = data.get('supervisor')
    uri = data.get('supervisorlegalentityuri')
    if sup and not uri:
        return f'supervisor not updated since supervisor legal entity {sup} not available in Replicon'
    return False

def v_schedule_uri_update(data):
    st = data.get('scheduletype')
    uri = data.get('scheduleuri')
    if st and not uri:
        return f'Office Schedule type not updated since Office Schedule type {st} not available in Replicon'
    return False

def v_gradedropdown_uri_update(data):
    grade = data.get('grade')
    uri = data.get('gradedropdownuri')
    if grade and not uri:
        return f'Grade not updated since grade {grade} not available in Replicon'
    return False

def v_profilestatusdropdown_uri_update(data):
    ps = data.get('profilestatus')
    uri = data.get('profilestatusdropdownuri')
    if ps and not uri:
        return f'profile status not updated since profile status {ps} not available in Replicon'
    return False

def v_permissionset_uri_update(data):
    perm = data.get('adduserpermission')
    uri = data.get('permissionseturi')
    if not uri and perm:
        return f'Permission set not updated since Permission set  {perm} not available in Replicon'
    return False

def v_timeentryapprovalpath_uri_update(data):
    t = data.get('timeentryapprovalpath')
    uri = data.get('timeentryapprovalpathuri')
    if t and not uri:
        return f'Timeentry approval path {t} not available in Replicon'
    return False

def v_payrule_uri_update(data):
    pr = data.get('payrule')
    uri = data.get('payruleuri')
    if pr and not uri:
        return f'payrule {pr} not available in Replicon'
    return False

def v_zerotimeuserpermissionset_uri_update(data):
    perm = data.get('zerotimepermission')
    uri = data.get('zerotimeuserpermissionseturi')
    if not uri and perm:
        return f'{perm} not available in Replicon'
    return False

def v_supervisory_org_uri_update(data):
    name = data.get('supervisoryorgname')
    uri = data.get('supervisory_org_uri')
    if name and not uri:
        return f'Supervisory Org {name} not available in Replicon'
    return False


field_config_add = {
    # entry = tuple ( isrequired=Boolean, (optional)custom message - str,method)
    "companycode": (required, None),
    "country": (required, None),
    "emailaddress": (required, v_emailaddress_add),
    "employeeid": (required, None),
    "employeetype": (required, None),
    "enddate": (not required, v_enddate_add),
    "firstname": (required, None),
    "holidaycalendar": (not required, v_holidaycalendar_blank),
    "holidaycalenderuri": (not required, v_holidaycalender_uri),
    "homeofficelocation": (required, None),
    "isloginenabled": (required, None),
    "lastname": (required, None),
    "legalentity": (required, None),
    "loscode": (not required, None),
    "toil": (not required, None),
    "ftepercent": (not required, None),
    "ftepercenteffectivedate": (not required, None),
    "linemanagerpartyid": (not required, v_linemanager_blank),
    "payrule": (not required, None),
    "loginname": (required, None),
    "prefix": (not required, None),
    "grade": (required, None),
    "profilestatus": (not required, None),
    "resourcerole": (not required, None),
    "scheduletype": (not required, v_scheduletype_blank),
    "startdate":  (required, v_startdate_add),
    "supervisor": (not required, v_supervisor_add),
    "timesheetapprovalpath": (not required, v_timesheetapprovalpath_blank),
    "timeentryapprovalpath": (not required, v_timeentryapprovalpath_blank),
    "timesheettemplate": (not required, v_timesheettemplate_blank),
    "timezone": (not required, None),
    "timesheetperiodtype": (not required, v_timesheetperiodtype_blank),
    "workdayid": (not required, v_workdayid_blank),
    "workweek": (not required, None),
    "timeoffpolicyuri": (not required, None),
    "employeetypegroupuri": (required, v_employeetypegroup_uri_add),
    "timeofftypeuri": (not required, None),
    "toiltimeofftypeuri": (not required, None),
    "companycodegroupuri": (required, v_companycodegroup_uri_add),
    "legalentitygroupuri": (required, v_legalentitygroup_uri_add),
    "countriesgroupuri": (required, v_countriesgroup_uri_add),
    "timesheettemplateuri": (not required, v_timesheettemplate_uri_add),
    "timesheetapprovalpathuri": (not required, v_timesheetapprovalpath_uri_add),
    "timezoneuri": (not required, v_timezone_uri_add),
    "supervisorlegalentityuri": (not required, v_supervisorlegalentity_uri_add),
    "scheduleuri": (required, v_schedule_uri_add),
    "gradedropdownuri": (required, v_gradedropdown_uri_add),
    "profilestatusdropdownuri": (not required, v_profilestatusdropdown_uri_add),
    "permissionseturi": (required, v_permissionset_uri_add),
    "timeentryapprovalpathuri": (not required, v_timeentryapprovalpath_uri_add),
    "payruleuri": (not required, v_payrule_uri_add),
    "zerotimeuserpermissionseturi": (not required, v_zerotimeuserpermissionset_uri_add),
    "timeoffapprovalpathuri": (not required, v_timeoffapprovalpath_uri_add),
    "supervisory_org_uri": (not required, v_supervisory_org_uri_add),
}

field_config_update = {
    "emailaddress": (not required, v_emailaddress_update),
    "enddate": (not required, v_enddate_update),
    "holidaycalenderuri": (not required, v_holidaycalender_uri_update),
    "isloginenabled": (required, None),
    "loginname": (required, None),
    "grade": (not required, None),
    "startdate":  (not required, v_startdate_update),
    "supervisor": (not required, v_supervisor_update),
    "employeetypegroupuri": (not required, v_employeetypegroup_uri_update),
    "companycodegroupuri": (not required, v_companycodegroup_uri_update),
    "legalentitygroupuri": (not required, v_legalentitygroup_uri_update),
    "countriesgroupuri": (not required, v_countriesgroup_uri_update),
    "timesheettemplateuri": (not required, v_timesheettemplate_uri_update),
    "timesheetapprovalpathuri": (not required, v_timesheetapprovalpath_uri_update),
    "timezoneuri": (not required, v_timezone_uri_update),
    "supervisorlegalentityuri": (not required, v_supervisorlegalentity_uri_update),
    "scheduleuri": (not required, v_schedule_uri_update),
    "gradedropdownuri": (not required, v_gradedropdown_uri_update),
    "profilestatusdropdownuri": (not required, v_profilestatusdropdown_uri_update),
    "permissionseturi": (not required, v_permissionset_uri_update),
    "toiltimeofftypeuri": (not required, None),
    "timeentryapprovalpathuri": (not required, v_timeentryapprovalpath_uri_update),
    "payruleuri": (not required, v_payrule_uri_update),
    "zerotimeuserpermissionseturi": (not required, v_zerotimeuserpermissionset_uri_update),
    "supervisory_org_uri": (not required, v_supervisory_org_uri_update),
}


# Termination/descope reduced payload: only Login Name, End Date and isLoginEnabled are mandatory; other fields ignored so existing Replicon values are retained.
field_config_termination = {
    "loginname": (required, None),
    "enddate": (required, v_enddate_update),
    "isloginenabled": (required, None),
}


def validate_field(field_config):
    data = request_payload.get_conf()
    errors = []
    for field_name, (is_required, validator) in field_config.items():
        value = data.get(field_name)
        error = None
        if callable(validator):
            error = validator(data)
        if (error is False or error is None) and is_required and not value:
            error = f'{field_name} is not present in payload'

        if error:
            errors.append({
                'field_name': field_name,
                'log_type': 'Exception' if is_required else 'Warning',
                'message': error
            })

    return errors


def validate_field_add():
    """Wrapper function for validating add user fields - picklable."""
    return validate_field(field_config_add)


def validate_field_update():
    """Validate update user fields - picklable. Termination/descope events use the reduced config; other updates use the standard one."""
    if request_payload.get_conf().get('isloginenabled') == 'No':
        return validate_field(field_config_termination)
    return validate_field(field_config_update)
