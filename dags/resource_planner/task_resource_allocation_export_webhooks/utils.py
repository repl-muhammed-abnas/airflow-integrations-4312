"""
Shared utilities for webhook-driven task resource allocation processing DAGs.
"""
from datetime import date, datetime, timedelta


def get_current_project_role(project_role_schedule, as_of=None):
    """Pick the primary project role that's currently in effect.

    The user-details API returns ``projectRoleSchedule`` as a list of entries,
    each with an ``effectiveDate`` ({day, month, year}) and a list of
    ``projectRoles`` (each with ``isPrimary`` + the role object). A user's
    "current" role is the primary role on the entry whose effectiveDate is
    the most-recent value <= today.

    Returns the role's ``displayText`` (what `labor_code_map` is keyed on), or
    ``''`` if no applicable entry exists.

    ``as_of`` is injectable for testing; defaults to today (local date).
    """
    today = as_of or date.today()

    applicable = []
    for entry in project_role_schedule or []:
        eff = entry.get('effectiveDate') or {}
        try:
            eff_date = date(int(eff['year']), int(eff['month']), int(eff['day']))
        except (KeyError, TypeError, ValueError):
            continue
        if eff_date <= today:
            applicable.append((eff_date, entry))

    if not applicable:
        return ''

    _, current_entry = max(applicable, key=lambda t: t[0])

    for pr in current_entry.get('projectRoles') or []:
        if pr.get('isPrimary'):
            return (pr.get('projectRole') or {}).get('displayText', '') or ''

    return ''

WEEKDAY_ABBRS = ['mo', 'tu', 'we', 'th', 'fr', 'sa', 'su']

GRAPHQL_QUERY = """query TaskResourceUserAllocationsQuery(
    $projectUri: String!,
    $userUri: String!,
    $taskUris: [String!]!
) {
    taskResourceUserAllocationsForUser(
        filter: {
            projectUri: $projectUri,
            userUri: $userUri,
            taskUris: $taskUris
        }
    ) {
        taskUri
        totalHours
        id
        roleUri
        lastModifiedTimestamp
        scheduleRules {
            dateRange {
                startDate
                endDate
            }
            do
        }
    }
}"""

API_HEADERS = {"Content-Type": "application/json"}


def derive_hours_type(raw_type, billing_status, client_name):
    """Derive the hours_type field from project metadata."""
    if raw_type == 'NCVP' and client_name == 'Deltek':
        return 'Internal Non-Billable'
    elif billing_status == 'Billable':
        return 'Client Project'
    elif billing_status == 'Non-Billable':
        return 'Internal Non-Billable'
    return raw_type


def get_project_custom_field(project_details, field_name):
    """Extract a custom field value from projectDetails.customFields by name."""
    for cf in project_details.get('customFields', []):
        if cf.get('customField', {}).get('name') == field_name:
            return cf.get('text', '')
    return ''


def get_project_client_name(project_details):
    """Extract the primary client name from projectDetails.clients."""
    clients = project_details.get('clients', [])
    if clients:
        return clients[0].get('client', {}).get('displayText', '')
    return ''


def expand_allocations_to_rows(allocations, project_uri):
    """
    Expand allocation schedule rules into individual daily rows.

    Args:
        allocations: List of allocation dicts from GraphQL response
                     (must have _employee_id, _labor_code_id, _users_user_id, _hours_type attached)
        project_uri: Full project URI (last segment used for time_code)

    Returns:
        List of row dicts ready for API submission.
    """
    project_id = project_uri.split(':')[-1]
    expanded_rows = []

    for alloc in allocations:
        allocation_id = alloc['id'].split(':')[-1]
        task_uri = alloc['taskUri']
        task_id = task_uri.split(':')[-1]
        time_code = f"{project_id}~{task_id}"
        user_id = alloc['_users_user_id']

        for rule in alloc.get('scheduleRules', []):
            start_str = rule['dateRange']['startDate'][:10]
            end_str = rule['dateRange']['endDate'][:10]
            start_date = datetime.strptime(start_str, '%Y-%m-%d')
            end_date = datetime.strptime(end_str, '%Y-%m-%d')
            hours = rule['do']['setHours']
            exclude = set(rule['do'].get('excludeWeekdays', []))

            current = start_date
            while current <= end_date:
                weekday_abbr = WEEKDAY_ABBRS[current.weekday()]
                if weekday_abbr not in exclude:
                    expanded_rows.append({
                        'sourceBookingId': allocation_id,
                        'sourceSystem': 'Polaris',
                        'timeCode': time_code,
                        'laborCode': alloc['_labor_code_id'],
                        'usersUserId': user_id,
                        'hours': hours,
                        'workDate': current.strftime('%Y-%m-%d'),
                        'hoursType': alloc['_hours_type'],
                        'lastUpdatedDate': alloc.get('lastModifiedTimestamp', ''),
                        'employeeId': alloc['_employee_id'],
                    })
                current += timedelta(days=1)

    print(f"Expanded {len(allocations)} allocations into {len(expanded_rows)} daily rows")
    return expanded_rows


def build_api_payload(target_table, **kwargs):
    """Build API request payload with optional targetTable."""
    payload = {}
    if target_table:
        payload['targetTable'] = target_table
    payload.update(kwargs)
    return payload
