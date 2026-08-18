# pylint: disable=line-too-long
import rail


def get_department_group_list(response):
    """Enabled department groups: name/uri/full path (master pre-fetch)."""
    return list(map(lambda x: {
        'departmentgroupname': x['cells'][0]['textValue'],
        'departmentgroupuri': x['cells'][0]['uri'],
        'fullpath': '/'.join(list(map(lambda c: c['textValue'], x['cells'][2]['cellCollection'])))
    }, response['rows']))


def normalize_replicon_userlist(records):
    """Normalize the 'userreferencereport' CSV rows (Workato step 16/41 -> userrinputdetails)
    into the existing-user lookup each foreach iteration searches by login id.
    NOTE: the column keys (userid/useruri/status/startdate/enddate) follow the Workato
    recipe's report schema. Validate against the actual report CSV header row on the first
    trial run and adjust the keys/date format if they differ."""
    return [
        {
            'userid': (record.get('Login Name') or '').strip().lower(),
            'useruri': record.get('useruri'),
            'status': record.get('User Status'),
            'startdate': record.get('User Start Date'),
            'enddate': record.get('User End Date'),
        }
        for record in records
    ]


def get_req_uris(dag_run):
    """Return the current per-user run's existing-user state and resolved uris from conf.

    The master resolves all of these values before triggering (useruri/status/dates from the
    bulk 'userreferencereport' lookup; dept/legal/paygroup/cost from the pre-fetched lists)
    and ships them via conf, so no Replicon calls are needed here."""
    return {
        'useruri': dag_run.conf.get('useruri', ''),
        'enddate': dag_run.conf.get('enddate', ''),
        'startdate': dag_run.conf.get('startdate', ''),
        'status': dag_run.conf.get('status', ''),
        'departmentgroupuri': dag_run.conf.get('departmentgroupuri', ''),
        'legalentityuri': dag_run.conf.get('legalentityuri', ''),
        'paygroupuri': dag_run.conf.get('paygroupuri', ''),
        'costcenteruri': dag_run.conf.get('costcenteruri', ''),
    }


def do_format_logs():
    """Flatten the per-user logs gathered from the process_each_user runs together with the
    master's own entries (validation skips, supervisor-child entries) into the rows the log
    CSV renders, and publish the status counts the completion email uses."""
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

    final_log_records = list(map(lambda log: {
        'jobid': log.get('ecid', ''),
        **(log.get('properties') or {}),
    }, log_records))

    rail.set_result(key='error_record_count', val=len(
        list(filter(lambda x: x.get('status') == 'Error', final_log_records))))
    rail.set_result(key='exception_record_count', val=len(
        list(filter(lambda x: x.get('status') == 'Exception', final_log_records))))

    return final_log_records
