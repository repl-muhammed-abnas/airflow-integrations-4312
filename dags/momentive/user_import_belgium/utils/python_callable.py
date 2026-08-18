# pylint: disable=line-too-long
import json
from datetime import datetime
import rail


def get_department_group_list(response):
    """Enabled department groups: name/uri/full path (master pre-fetch)."""
    return list(map(lambda x: {
        'departmentgroupname': x['cells'][0]['textValue'],
        'departmentgroupuri': x['cells'][0]['uri'],
        'fullpath': '/'.join(list(map(lambda c: c['textValue'], x['cells'][2]['cellCollection'])))
    }, response['rows']))


def get_user_data(response):
    """UserList search rows -> {username, useruri, status, enddate, startdate}.

    A login that does not exist yet comes back with no rows at all, which must resolve to
    an empty list (the recipe's search simply returns an empty array and the user is then
    ADDED). Subscripting 'rows' unconditionally made the search task fail for every new
    hire, so the run logged 'Error while searching the user' instead of adding them."""
    if not response or not isinstance(response, dict) or not response.get('rows'):
        return []
    return list(map(lambda x: {
        'username': x['cells'][0]['textValue'],
        'useruri': x['cells'][0]['uri'],
        'status': x['cells'][3]['textValue'],
        'enddate': x['cells'][1]['dateValue'] if 'dateValue' in x['cells'][1] else None,
        'startdate': x['cells'][2]['dateValue'] if 'dateValue' in x['cells'][2] else None,
    }, response['rows']))


def get_req_uris(dag_run):
    """Resolve THIS user's existing Replicon uri/status/dates from the per-user search.

    Runs inside process_each_user. The department/legalentity/paygroup/costcenter uris are
    resolved once by the master and arrive via conf, so no per-user re-fetch of those lists
    is needed."""
    userid = dag_run.conf['userid']
    useruri = ''
    enddate = ''
    startdate = ''
    status = ''
    if rail.result('search_user'):
        useruri = rail.find_first_by_attr_and_get_attr(
            rail.result('search_user'), 'username', userid, 'useruri', '')
        end_date = rail.find_first_by_attr_and_get_attr(
            rail.result('search_user'), 'username', userid, 'enddate', '')
        if end_date:
            enddate = str(end_date['year']) + '-' + str(end_date['month']) + '-' + str(end_date['day'])
        start_date = rail.find_first_by_attr_and_get_attr(
            rail.result('search_user'), 'username', userid, 'startdate', '')
        if start_date:
            startdate = str(start_date['year']) + '-' + str(start_date['month']) + '-' + str(start_date['day'])
        status = rail.find_first_by_attr_and_get_attr(
            rail.result('search_user'), 'username', userid, 'status', '')

    return {
        'useruri': useruri,
        'enddate': enddate,
        'startdate': startdate,
        'status': status,
        'departmentgroupuri': dag_run.conf['departmentgroupuri'],
        'legalentityuri': dag_run.conf['legalentityuri'],
        'paygroupuri': dag_run.conf['paygroupuri'],
        'costcenteruri': dag_run.conf['costcenteruri']
    }


def do_format_logs():
    """Flatten the per-user logs gathered from the process_each_user runs together with the
    master's own entries (validation skips) into the rows the log CSV renders, and publish the
    status counts the completion email uses.

    Each entry carries the ecid of the run that wrote it as 'childjobid'. The 'jobid' column
    (master run ecid) is added by compose_logs_csv from get_master_jobid."""
    log_artifacts = []
    gathered_logs = rail.result('gather_user_logs')
    if gathered_logs:
        if isinstance(gathered_logs, list):
            log_artifacts.extend(gathered_logs)
        else:
            log_artifacts.append(gathered_logs)
    master_log = rail.result('logger_list')
    if master_log:
        log_artifacts.append(master_log)

    log_records = []
    for log in log_artifacts:
        each_log_records = rail.load_all_records(log)
        if each_log_records:
            log_records.extend(each_log_records)

    # Preserve childjobid (ecid) alongside flattened properties for compose_logs_csv.
    final_log_records = list(map(lambda log: {
        'childjobid': log.get('ecid', ''),
        **(log.get('properties') or {}),
    }, log_records))

    rail.set_result(key='error_record_count', val=len(
        list(filter(lambda x: x.get('status') == 'Error', final_log_records))))
    rail.set_result(key='exception_record_count', val=len(
        list(filter(lambda x: x.get('status') == 'Exception', final_log_records))))

    return final_log_records


# ---------------------------------------------------------------------------
# BEL Time off policy update_rehire child helpers (recipe 1362490).
# ---------------------------------------------------------------------------


def past_rehire_policyset_entries(summary, timeoff_uri, start_date):
    """Recipe steps 7-13: from GetUserTimeOffTypePolicySummary, take this time-off type's
    policySetSchedule and keep only entries whose effectiveDate is strictly before the
    (re)start date. Kept raw - the "script" -> "scriptTarget" rename is applied later over
    the whole combined list. Returns [] when none."""
    policies = (summary or {}).get('policiesByTimeOffType') or []
    schedule = rail.find_first_by_attr_and_get_attr(
        policies, 'timeOffType.uri', timeoff_uri, 'policySetSchedule', []) or []
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    kept = []
    for entry in schedule:
        eff = entry.get('effectiveDate') or {}
        if not eff.get('day'):
            continue
        if datetime(eff['year'], eff['month'], eff['day']).date() < start:
            kept.append(entry)
    return kept


def build_bel_rehire_policy_entries(past_entries, default_schedule, start_date):
    """Recipe nodes 14-21: for each default policy-set tier, re-anchor onto the (re)hire
    year (all tiers land on Jan 1). Tier -> effective date:
      offsetValue 0 -> Jan 1 of hire year        (beginning_of_year(startdate))
      offsetValue 2 -> Jan 1 of hire year + 1    (beginning_of_year(startdate + 12 months))
      offsetValue 3 -> Jan 1 of hire year + 2    (beginning_of_year(startdate + 24 months))
    Every re-anchored entry uses description "Added for rehire <dd/mm/yyyy>". Other offsets
    have no branch in the recipe and are ignored. Preserved past entries come first; rename
    "script" -> "scriptTarget" over the whole combined list. Returns [] when nothing built.
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    description = f"Added for rehire {start.strftime('%d/%m/%Y')}"
    tier_year_offset = {0: 0, 2: 1, 3: 2}
    entries = list(past_entries or [])
    for tier in default_schedule or []:
        value = (tier.get('startOffset') or {}).get('offsetValue')
        try:
            offset = int(float(value))
        except (TypeError, ValueError):
            continue
        if offset not in tier_year_offset:
            continue
        eff = {'day': 1, 'month': 1, 'year': start.year + tier_year_offset[offset]}
        entries.append({
            'effectiveDate': eff,
            'policySet': tier.get('policySet'),
            'description': description,
        })
    if not entries:
        return []
    return json.loads(
        json.dumps(entries, ensure_ascii=False).replace('"script"', '"scriptTarget"'))
