import hashlib
from ast import literal_eval
from airflow.models import Variable
import rail


def get_formated_user_row(item):
    return {
        "displayname": item["Display Name"],
        "firstname": item["First Name"],
        "lastname": item["Last Name"],
        "emailaddress": item["Email Address"].lower() if item["Email Address"] else item["Email Address"],
        "empid": item["Employee ID"],
        "empnumber": item["Employee Number"],
        "whencreated": item["When Created"],
        "whenchanged": item["When Changed"],
        "office": item["Office"],
        "logonname": item["Logon Name"],
        "accountstatus": item["Account Status"],
        "department": item["Department"],
        "memberof": item["Member of"],
        "title": item["Title"],
        "leaveofabsence": item["Leave of Absence"],
        "md5": hashlib.md5((
            item['Display Name']+"," +
            item['First Name']+"," +
            item['Last Name']+"," +
            item['Email Address']+"," +
            item['Employee ID']+"," +
            item['Employee Number']+"," +
            item['When Created']+"," +
            item['When Changed']+"," +
            item['Office']+"," +
            item['Logon Name']+"," +
            item['Account Status']+"," +
            item['Department']+"," +
            item['Member of']+"," +
            item['Title']+"," +
            item['Leave of Absence']).encode())
        .hexdigest()
    }.values()

def c3_c4_supervisors_loginname(variable_name):
    supervisors_loginname = literal_eval(Variable.get(variable_name))
    return {
        "c3_supervisor":supervisors_loginname['c3_supervisor_loginname'],
        "c4_supervisor":supervisors_loginname['c4_supervisor_loginname'],
    }

def get_customoef_uri(custom_field_info):
            existing_customoefs = rail.result('get_custom_fieldsforuser_3')
            input_department_info = list(filter(
                lambda item: item['displayText'] == custom_field_info, existing_customoefs))
            return input_department_info[0]['uri'] if input_department_info else None

def get_profile_list(dag_run):
    """Extract profile list from DAG run configuration"""
    profile_list = []
    user_uris = dag_run.conf['useruris']
    loginnames = dag_run.conf['loginnames']
    user_types = dag_run.conf['currenttype']
    user_statuses = dag_run.conf.get('currentstatus', [])

    for x in range(len(user_uris)):
        status = user_statuses[x] if x < len(user_statuses) else 'True'  # Default to enabled if status not provided
        profile_list.append({
            "uri": user_uris[x],
            "userloginname": loginnames[x],
            "type": user_types[x],
            "status": status
        })

    return profile_list

def analyze_profile_requirements(dag_run, existing_profiles):
    """
    Centralized profile analysis function that determines all required actions

    Returns:
    {
        'existing_profiles': [{uri, loginname, type}, ...],
        'input_profiles': ['C3', 'C4', 'Delegate'],
        'primary_profile': 'Delegate|C4|C3',
        'actions': {
            'disable': [{profile_data, reason}, ...],
            'create': [{profile_type, config}, ...],
            'update': [{profile_data, changes}, ...],
            'substitute_assignments': [{primary_uri, target_uri, target_type}, ...]
        },
        'profile_suffix_map': {'C3': 'lt', 'C4': 'af', 'Delegate': 'd'}
    }
    """

    # Parse input profiles from memberof field
    memberof = dag_run.conf.get('memberof', '')
    input_types = set()
    if 'C3' in memberof:
        input_types.add('C3')
    if 'C4' in memberof:
        input_types.add('C4')
    if 'Delegate' in memberof:
        input_types.add('Delegate')

    # Get existing profile types
    existing_types = set()
    existing_profile_map = {}
    for profile in existing_profiles:
        profile_type = profile['type']
        if profile_type == 'Lobbying Timesheet':
            profile_type = 'C3'  # Normalize to C3
        existing_types.add(profile_type)
        existing_profile_map[profile_type] = profile

    # Determine existing_primary (based on existing profiles before input)
    existing_primary = None
    for p in ["Delegate", "C4", "C3"]:
        if p in existing_types:
            existing_primary = p
            break

    # Determine primary profile based on priority: Delegate > C4 > C3
    primary_profile = None
    if 'Delegate' in input_types:
        primary_profile = 'Delegate'
    elif 'C4' in input_types:
        primary_profile = 'C4'
    elif 'C3' in input_types:
        primary_profile = 'C3'

    # Determine required actions
    profiles_to_create = list(input_types - existing_types)
    profiles_to_disable = list(existing_types - input_types)
    profiles_to_update = list(existing_types.intersection(input_types))

    # Profile suffix mapping for disabled profiles
    profile_suffix_map = {
        'C3': 'lt',
        'C4': 'af',
        'Delegate': 'd'
    }

    # Build actions
    actions = {
        'disable': [],
        'create': [],
        'update': [],
        'reenable': [],
        'substitute_assignments': []
    }

    # Disable profiles not in input
    for profile_type in profiles_to_disable:
        if profile_type in existing_profile_map:
            profile_data = existing_profile_map[profile_type]
            actions['disable'].append({
                'profile_data': profile_data,
                'profile_type': profile_type,
                'disabled_suffix': profile_suffix_map[profile_type],
                'reason': f'Profile {profile_type} not in input memberof',
                'config': {
                        'suffix': profile_suffix_map[profile_type],
                        'supervisor': 'c4_supervisor' if profile_type == 'C4' else 'c3_supervisor' if profile_type == 'C3' else None
                    }
            })

    # Create missing profiles
    for profile_type in profiles_to_create:
        actions['create'].append({
            'profile_type': profile_type,
            'is_primary': profile_type == primary_profile,
            'existing_primary': existing_primary,
            'config': {
                'suffix': profile_suffix_map[profile_type],
                'supervisor': 'c4_supervisor' if profile_type == 'C4' else 'c3_supervisor' if profile_type == 'C3' else None
            }
        })

    # Detect disabled profiles that need re-enablement
    profiles_to_reenable = []
    for profile_type in profiles_to_update:
        if profile_type in existing_profile_map:
            profile_data = existing_profile_map[profile_type]
            # Check if profile is disabled (status = 'False' or accountstatus = 'disabled')
            profile_status = profile_data.get('status', 'True')
            if profile_status == 'False' or profile_status.lower() == 'disabled':
                profiles_to_reenable.append(profile_type)
                actions['reenable'].append({
                    'profile_data': profile_data,
                    'profile_type': profile_type,
                    'reason': f'Profile {profile_type} is disabled but present in input - re-enabling',
                    'config': {
                        'suffix': profile_suffix_map[profile_type],
                        'supervisor': 'c4_supervisor' if profile_type == 'C4' else 'c3_supervisor' if profile_type == 'C3' else None
                    }
                })

    # Update existing profiles that are in input (including those that were re-enabled)
    for profile_type in profiles_to_update:
        if profile_type in existing_profile_map:
            profile_data = existing_profile_map[profile_type]
            actions['update'].append({
                'profile_data': profile_data,
                'profile_type': profile_type,
                'existing_primary': existing_primary,
                'is_primary': profile_type == primary_profile,
                'was_reenabled': profile_type in profiles_to_reenable,
                'config': {
                    'suffix': profile_suffix_map[profile_type],
                    'supervisor': 'c4_supervisor' if profile_type == 'C4' else 'c3_supervisor' if profile_type == 'C3' else None
                },
                'changes': ['basic_update']  # Can be extended
            })

    # Determine substitute user assignments
    # Business Rules:
    # - If Delegate is primary → add Delegate as substitute to C3 and C4
    # - If C4 is primary → add C4 as substitute to C3
    # - If C3 is primary → no substitutes needed
    if primary_profile:
        primary_uri = None
        if primary_profile in existing_profile_map:
            primary_uri = existing_profile_map[primary_profile]['uri']

        # Apply business rules for substitute assignments
        target_profiles = []
        if primary_profile == 'Delegate':
            # Delegate primary → add to C3 and C4 if they exist in input
            if 'C3' in input_types:
                target_profiles.append('C3')
            if 'C4' in input_types:
                target_profiles.append('C4')
        elif primary_profile == 'C4':
            # C4 primary → add to C3 if it exists in input
            if 'C3' in input_types:
                target_profiles.append('C3')
        # If C3 is primary → no substitutes needed (empty target_profiles)

        for target_profile in target_profiles:
            target_uri = None
            if target_profile in existing_profile_map:
                target_uri = existing_profile_map[target_profile]['uri']

            if dag_run.conf.get('accountstatus', '').lower() != 'disabled':
                actions['substitute_assignments'].append({
                    'primary_profile': primary_profile,
                    'primary_uri': primary_uri,  # May be None if primary needs to be created
                    'existing_primary': existing_primary,
                    'target_profile': target_profile,
                    'target_uri': target_uri,    # May be None if target needs to be created
                    'assignment_needed': True
                })

    return {
        'existing_profiles': existing_profiles,
        'input_profiles': list(input_types),
        'primary_profile': primary_profile,
        'existing_primary': existing_primary,
        'actions': actions,
        'profile_suffix_map': profile_suffix_map,
        'summary': {
            'total_existing': len(existing_profiles),
            'total_input': len(input_types),
            'to_disable': len(profiles_to_disable),
            'to_create': len(profiles_to_create),
            'to_update': len(profiles_to_update),
            'to_reenable': len(profiles_to_reenable),
            'substitute_assignments': len(actions['substitute_assignments'])
        }
    }