import rail

null = None


def tags_to_add_payload(dag_run):
    data = rail.load_all_records(rail.result('query_assignee_ids_uri_to_add'))
    return rail.write_json_artifact(list(map(lambda item: {
        "target": {
            "uri": item['assigneeuri'],
            "slug": null,
            "tagName": null
        },
        "isEnabled": True,
        "dateRange": {
            "startDate": null,
            "endDate": null,
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        }
    }, data)))


def tags_to_remove_payload(dag_run):
    data = rail.load_all_records(rail.result(
        'query_assignee_ids_uri_to_remove'))
    return rail.write_json_artifact(list(map(lambda item: {
        "uri": item['assigneeuri'],
        "slug": null,
        "tagName": null
    }, data)))

def load_records(log_artifact):
    try:
        logs = rail.load_all_records(log_artifact)
        return logs
    except:  # pylint: disable=bare-except
        return []

# pylint: disable=too-many-branches
def do_format_logs(dag_run):
    log_artifacts = []
    log_records = []

    assigneeadd_errorlog = dag_run.conf['assigneeadd_errorlog']
    assigneeupdate_errorlog = dag_run.conf['assigneeupdate_errorlog']
    #successlog = dag_run.conf['successlog']
    errorlog = dag_run.conf['errorlog']
    exceptionlog = dag_run.conf['exceptionlog']
    skipped_log = dag_run.conf['skipped_log']
    recordexceptionlog = dag_run.conf['recordexceptionlog']


    if assigneeadd_errorlog:
        if isinstance(assigneeadd_errorlog, list):
            log_artifacts.extend(assigneeadd_errorlog)
        else:
            log_artifacts.append(assigneeadd_errorlog)

    if assigneeupdate_errorlog:
        if isinstance(assigneeupdate_errorlog, list):
            log_artifacts.extend(assigneeupdate_errorlog)
        else:
            log_artifacts.append(assigneeupdate_errorlog)

    # if successlog:
    #     if isinstance(successlog, list):
    #         log_artifacts.extend(successlog)
    #     else:
    #         log_artifacts.append(successlog)

    if errorlog:
        if isinstance(errorlog, list):
            log_artifacts.extend(errorlog)
        else:
            log_artifacts.append(errorlog)

    if exceptionlog:
        if isinstance(exceptionlog, list):
            log_artifacts.extend(exceptionlog)
        else:
            log_artifacts.append(exceptionlog)

    if skipped_log:
        if isinstance(skipped_log, list):
            log_artifacts.extend(skipped_log)
        else:
            log_artifacts.append(skipped_log)

    if recordexceptionlog:
        if isinstance(recordexceptionlog, list):
            log_artifacts.extend(recordexceptionlog)
        else:
            log_artifacts.append(recordexceptionlog)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = load_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    return list(map(lambda x: {
        **dict(x['properties'].items()),
       }, log_records))
