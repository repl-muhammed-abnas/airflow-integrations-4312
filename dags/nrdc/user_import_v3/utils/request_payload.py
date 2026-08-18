from datetime import datetime, timedelta, timezone
import rail
from rail.lib.ecid import get_dagrun_ecid
null = None

date_format = "%d/%m/%Y"

def get_memberof(dag_run):
    if dag_run.conf['memberof']:
        if 'C3' in dag_run.conf['memberof'] or 'C4' in dag_run.conf['memberof'] or 'Delegate' in dag_run.conf['memberof']:
            return True
        else:
            return False
    return False

def create_disable_profile_payload(dag_run, profile_data, profile_type, disabled_suffix, config):
    """Create payload for disabling a profile"""
    # For disabled profiles: explicitly remove email and set end date
    emailaddress = ""  # Explicitly empty string, not None
    current_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    return {
        'firstname': 'Action Fund' if profile_type == 'C4' else 'Lobbying Timesheet' if profile_type == 'C3' else profile_type,
        'lastname': dag_run.conf['displayname'],
        'emailaddress': emailaddress,  # Empty string for disabled profiles
        'employeeid': dag_run.conf['employeeid'],
        'empnumber': dag_run.conf['empnumber'],
        'whencreated': dag_run.conf['whencreated'],
        'office': dag_run.conf['office'],
        'loginname': profile_data['userloginname'] if profile_data['userloginname'].endswith(disabled_suffix) else profile_data['userloginname'] + disabled_suffix,
        'accountstatus': 'disabled',
        'department': dag_run.conf['department'],
        'memberof': dag_run.conf['memberof'],
        'manager': rail.result('initialize_supervisors')[config['supervisor']] if profile_type in ('C4', 'C3') else None,
        'title': dag_run.conf['title'],
        'leaveofabsence': dag_run.conf['leaveofabsence'],
        'useruri': profile_data['uri'],
        'locationuri': dag_run.conf['locationuri'],
        'type': profile_type,
        'userfullname': dag_run.conf['firstname'] + ' ' + dag_run.conf['lastname'],
        'parentjobid': get_dagrun_ecid(dag_run),
        'force_end_date': True,
        'remove_email': True,
        'enddate': current_date,
        'disable': True
    }


def create_profile_payload(dag_run, profile_type, is_primary, config):
    """Create payload for creating a new profile"""
    profile_suffix_map = {
        'C3': 'lt',
        'C4': 'af',
        'Delegate': 'd'
    }

    # Use base logonname for primary profile, otherwise add suffix
    if is_primary:
        logonname = dag_run.conf['logonname'].split('@')[0]
        remove_email = False
    else:
        # Non-primary profiles get suffix and email removal
        logonname = dag_run.conf['logonname'].split('@')[0] + profile_suffix_map[profile_type]
        remove_email = True

    return {
        'firstname': 'Action Fund' if profile_type == 'C4' else "C3 Lobbying Timesheet" if profile_type == 'C3' else profile_type,
        'lastname': dag_run.conf['displayname'],
        'emailaddress': dag_run.conf['emailaddress'],
        'empid': dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
        'empnumber': dag_run.conf['empnumber'],
        'whencreated': dag_run.conf['whencreated'],
        'office': dag_run.conf['office'],
        'loginname': logonname,
        'accountstatus': dag_run.conf['accountstatus'] if get_memberof(dag_run) else 'disabled',
        'department': dag_run.conf['department'],
        'memberof': dag_run.conf['memberof'],
        'manager': rail.result('initialize_supervisors')[config['supervisor']] if profile_type in ('C4', 'C3') else None,
        'title': dag_run.conf['title'],
        'leaveofabsence': dag_run.conf['leaveofabsence'],
        'locationuri': dag_run.conf['locationuri'],
        'type': profile_type,
        'userfullname': dag_run.conf['firstname'] + ' ' + dag_run.conf['lastname'],
        "primaryuseruri": "NA",
        "timesheettype": "C4 Timesheet" if profile_type == 'C4' else "C3 Lobbying Timesheet" if profile_type == 'C3' else "NA",
        'parentjobid': get_dagrun_ecid(dag_run),
        'is_primary': is_primary,
        'remove_email': remove_email,
        "authtype": "sso" if is_primary else "replicon",
        "status": dag_run.conf['accountstatus'] if get_memberof(dag_run) else 'disabled',
        "profile_suffix_map": profile_suffix_map
    }

def create_update_profile_payload(dag_run, profile_data, profile_type, is_primary, config,existing_primary,primary_uri = None,):
    profile_suffix_map = {
        'C3': 'lt',
        'C4': 'af',
        'Delegate': 'd'
    }

    if is_primary:
        logonname = dag_run.conf['logonname'].split('@')[0]
    else:
        logonname = profile_data['userloginname'] if profile_data['userloginname'].endswith(profile_suffix_map[profile_type]) else profile_data['userloginname'] + profile_suffix_map[profile_type]

    return {
        'loginname': logonname,
        'emailaddress': dag_run.conf['emailaddress'],
        'is_primary': is_primary,
        'remove_email': False,  # Don't remove email in standard updates
        'force_end_date': False,  # Keep profiles active
        'enddate': null,  # No end date for standard updates
        'firstname': 'Action Fund' if profile_type == 'C4' else 'C3 Lobbying Timesheet' if profile_type == 'C3' else profile_type,
        'lastname': dag_run.conf['displayname'],
        "employeeid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
        'empnumber': dag_run.conf['empnumber'],
        'whencreated': dag_run.conf['whencreated'],
        'office': dag_run.conf['office'],
        'status': dag_run.conf['accountstatus'] if get_memberof(dag_run) else 'disabled',
        'accountstatus': dag_run.conf['accountstatus'] if get_memberof(dag_run) else 'disabled',
        'department': dag_run.conf['department'],
        'memberof': dag_run.conf['memberof'],
        'manager': rail.result('initialize_supervisors')[config['supervisor']] if profile_type in ('C4','C3') else None,
        'title': dag_run.conf['title'],
        'leaveofabsence': dag_run.conf['leaveofabsence'],
        'useruri': profile_data['uri'],
        'locationuri': dag_run.conf['locationuri'],
        'type': profile_type,
        'userfullname': dag_run.conf['firstname'] + ' ' + dag_run.conf['lastname'],
        "timesheettype": "C4 Timesheet" if profile_type == 'C4' else "C3 Lobbying Timesheet" if profile_type == 'C3' else "NA",
        "authtype": "sso" if is_primary else "replicon",
        'parentjobid': get_dagrun_ecid(dag_run),
        "profile_suffix_map": profile_suffix_map,
        "existing_primary": existing_primary
    }

def create_rehire_profile_payload(dag_run, profile_type, is_primary, config, existing_primary):
    profile_suffix_map = {
        'C3': 'lt',
        'C4': 'af',
        'Delegate': 'd'
    }
    return {
        'loginname': rail.result('search_users')[0]['loginname'] if not is_primary else dag_run.conf['logonname'].split('@')[0],
        'emailaddress': dag_run.conf['emailaddress'],
        'is_primary': is_primary,
        'remove_email': False,  # Don't remove email in standard updates
        'force_end_date': False,  # Keep profiles active
        'enddate': null,  # No end date for standard updates
        'firstname': 'Action Fund' if profile_type == 'C4' else 'C3 Lobbying Timesheet' if profile_type == 'C3' else profile_type,
        'lastname': dag_run.conf['displayname'],
        "employeeid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
        'empnumber': dag_run.conf['empnumber'],
        'whencreated': dag_run.conf['whencreated'],
        'office': dag_run.conf['office'],
        'status': dag_run.conf['accountstatus'] if get_memberof(dag_run) else 'disabled',
        'accountstatus': dag_run.conf['accountstatus'] if get_memberof(dag_run) else 'disabled',
        'department': dag_run.conf['department'],
        'memberof': dag_run.conf['memberof'],
        'manager': rail.result('initialize_supervisors')[config['supervisor']] if profile_type in ('C4','C3') else None,
        'title': dag_run.conf['title'],
        'leaveofabsence': dag_run.conf['leaveofabsence'],
        'useruri': rail.result('search_users')[0]['useruri'],
        'locationuri': dag_run.conf['locationuri'],
        'type': profile_type,
        'userfullname': dag_run.conf['firstname'] + ' ' + dag_run.conf['lastname'],
        "timesheettype": "C4 Timesheet" if profile_type == 'C4' else "C3 Lobbying Timesheet" if profile_type == 'C3' else "NA",
        "authtype": "sso" if is_primary else "replicon",
        'parentjobid': get_dagrun_ecid(dag_run),
        "profile_suffix_map": profile_suffix_map,
        "existing_primary": existing_primary
    }


def get_user_details():
    return {
        "loginname": "",
        "status": "",
        "employeeid": "",
        "uri": ""
    }


def get_user_data():
    return {
        "loginname": "",
        "status": "",
        "employeeid": "",
        "uri": ""
    }
