"""
Helper methods for the C1 LeanstaffAssignment export.

`compute_eligible_project_uris` applies, in bulk, the same eligibility rules the
webhook processor applies per-event (see process_webhooks_data.py), so the
export-side bulk-validation path produces the same project set as the original
per-event validation:

  - project exists and status is 'In Progress'
  - PSA Flag OEF == 'X'
  - WBS Type OEF != 'Opportunity'
  - division is C1 (projects with no division pass, matching the processor)
  - NOT (Project Type 'ES' and name starts with 'E-')
  - NOT (Project Type 'IC' and name starts with 'X-')

Kept pure and side-effect-free so it can be unit tested in isolation.
"""
import rail


def compute_eligible_project_uris(projects, division_code_by_uri):
    """
    projects: list of projectDetails dicts (or None entries) from BulkGetProjectDetails3.
    division_code_by_uri: { division_uri: division_code } from GetDivisionDetails.
    Returns: list of {'project_uri': uri} for projects that pass every rule.
    """
    eligible = []
    for pd in projects:
        if not pd:
            continue
        # Status must be In Progress
        if not (pd.get('status') and pd['status'].get('displayText') == 'In Progress'):
            continue
        efv = pd.get('extensionFieldValues') or []
        # PSA Flag must be 'X'
        psa_item = rail.find_first_by_attr_and_get_attr(efv, 'definition.displayText', 'PSA Flag')
        if not (psa_item and (psa_item.get('tag') or {}).get('displayText') == 'X'):
            continue
        # WBS Type must not be Opportunity
        wbs_item = rail.find_first_by_attr_and_get_attr(efv, 'definition.displayText', 'WBS Type')
        if wbs_item and (wbs_item.get('tag') or {}).get('displayText') == 'Opportunity':
            continue
        # Division must be C1 (projects with no division pass, matching processor)
        div = pd.get('division')
        if div and div.get('uri') and division_code_by_uri.get(div['uri']) != 'C1':
            continue
        # Project Type / name prefix exclusions (ES + 'E-', IC + 'X-')
        ptype_item = rail.find_first_by_attr_and_get_attr(efv, 'tag.definition.displayText', 'Project Type')
        project_type = (ptype_item.get('tag') or {}).get('displayText') if ptype_item else None
        name = pd.get('name') or ''
        if project_type == 'ES' and name.startswith('E-'):
            continue
        if project_type == 'IC' and name.startswith('X-'):
            continue
        eligible.append({'project_uri': pd['uri']})
    return eligible
