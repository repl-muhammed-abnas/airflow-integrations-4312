from ast import literal_eval
import rail

null = None

def get_parent_available(row, current_costcenters):
    row = row.split('|')
    row = [val.strip() for val in row]
    present = True if row[0] in current_costcenters else False
    levels = 0
    for i in range(len(row)):
        val = '|'.join(row[:i+1])
        if present and val in current_costcenters:
            levels = i+1
            continue
        break
    if levels > 0:
        return {
            'parents': '|'.join(row[:levels]),
            'child': '|'.join(row[levels:])
        }
    return {
        'parents': '',
        'child': '|'.join(row)
    }

def get_disable_details(raw_costcenters, current_costcenters):
    """
        Creating two separate list with hardcoded key 'costcenter_path' so that it can be called with trigger_parallel_dagrun function
        and can be run with multi-concurrency other wise it can be optimize with dict comprehension.
        example: {
            level: current_costcenters.get(level, '') for level in raw_costcenters
        }
    """
    path_available = []
    path_unavailable = []
    for row in raw_costcenters:
        if row in current_costcenters and current_costcenters[row]['enabled']:
            path_available.append({
                'costcenter_path':current_costcenters[row]
            })
        else:
            path_unavailable.append({
                'costcenter_path':row
            })
    return {
        'path_available':path_available,
        'path_unavailable':path_unavailable
    }

def get_invalid_data(raw_input_data):
    invalid_data = []
    for item in raw_input_data:
        if not item['Action'] or not item['Supervisory Org']:
            item['Details'] = 'Missing mandatory fields Supervisory Org/Action.'
            invalid_data.append(item)
            continue
        if len(item['Supervisory Org'].split('|')) > 7:
            item['Details'] = 'Supervisory Org length is more than 7.'
            invalid_data.append(item)
            continue
        if item['Action'].lower() not in ('add', 'disable'):
            item['Details'] = "Incorrect action '" + str(item["Action"]) + "' received in feed file."
            invalid_data.append(item)
            continue
    return invalid_data

def get_valid_data(raw_input_data):
    valid_data = []
    for item in raw_input_data:
        if item['Action'] and item['Supervisory Org'] and len(item['Supervisory Org'].split('|')) <= 7:
            valid_data.append(item)
    return valid_data

def get_add_update_costcenters():
    raw_input_data = rail.load_all_records(rail.result('load_csv_content'))
    current_costcenters = rail.result('get_cost_center_hierarchy_data')
    valid_data = get_valid_data(raw_input_data)
    invalid_data = get_invalid_data(raw_input_data)
    add_action = list(map(lambda item:item['Supervisory Org'].strip(), filter(lambda row: row['Action'].lower() == 'add', valid_data)))
    disable_action = list(map(lambda item: item['Supervisory Org'].strip(), filter(lambda row: row['Action'].lower() == 'disable', valid_data)))
    levels_to_disable = get_disable_details(disable_action, current_costcenters)
    levels_to_add = []
    for row in add_action:
        resp = get_parent_available(row, current_costcenters)
        levels_to_add.append(resp)
    return {
        'levels_to_add': levels_to_add,
        'levels_to_disable': levels_to_disable,
        'invalid_data': invalid_data
    }

def get_error_to_ignore():
    return [
        'The specified CostCenter already exists.',
        'Exceptions in AfterSaveAny DomainObjectSavedEvents'
    ]

def get_log_message_details(added_levels, created_levels, error):
    msg = f"Supervisory Org creation failed with error(s) {';'.join(error)}"
    if not error:
        msg = f"Created Supervisory Org {'|'.join(added_levels)}"
    elif created_levels and error:
        msg = f"Partially created Supervisory Org {'|'.join(added_levels)} with error(s) {';'.join(error)}"
    return msg

def prepare_log_to_add(dag_run):
    response = rail.result('create_supervisory_org')
    error = list(set(map(
        lambda item: item['error']['notifications'][0]['displayText'],
        filter(
            lambda row: row['error']['notifications'][0]['displayText'] not in get_error_to_ignore()
            if bool(row['error']) else False,
            response
            )
        )))
    created_levels = list(map(
        lambda item: item['source']['displayText'],
        filter(
            lambda row: not bool(row['error']),
            response
            )
        ))
    parent, child = [], []
    if dag_run.conf['parents']:
        parent = dag_run.conf['parents'].split('|')
    if dag_run.conf['child']:
        child = dag_run.conf['child'].split('|')
    added_levels = parent + child
    return {
        'added_levels': '|'.join(added_levels),
        'status': 'Success' if not error else 'Error',
        'details': get_log_message_details(added_levels, created_levels, error)
    }

def load_records(log_artifact):
    return rail.load_all_records(log_artifact)

def get_status(item, logstatus):
    status = 'status' if item.get('status') else 'Status'
    return item[status].lower() == logstatus

def do_format_logs(dag_run):
    log_artifacts = []
    log_records = []

    supervisory_org_logs = dag_run.conf['supervisory_org_logs']
    otherlogs = dag_run.conf['otherlogs']

    if supervisory_org_logs:
        if isinstance(supervisory_org_logs, list):
            log_artifacts.extend(supervisory_org_logs)
        elif isinstance(supervisory_org_logs, str) and supervisory_org_logs[0] == '[':
            supervisory_org_logs = literal_eval(supervisory_org_logs)
            log_artifacts.extend(supervisory_org_logs)
        else:
            log_artifacts.append(supervisory_org_logs)

    if otherlogs:
        if isinstance(otherlogs, list):
            log_artifacts.extend(otherlogs)
        elif isinstance(otherlogs, str) and otherlogs[0] == '[':
            otherlogs = literal_eval(otherlogs)
            log_artifacts.extend(otherlogs)
        else:
            log_artifacts.append(otherlogs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = load_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    final_log_records = []

    final_log_records = list(map(lambda log: {
        **{"ecid":log['ecid']},
        **dict(log['properties'].items()),
        }, log_records))

    rail.set_result(key="error_record_count",val= len(list(filter(lambda x: get_status(x, 'error'), final_log_records ))))
    rail.set_result(key="success_record_count",val= len(list(filter(lambda x: get_status(x, 'success'), final_log_records ))))
    rail.set_result(key="exception_record_count",val= len(list(filter(lambda x: get_status(x, 'exception'), final_log_records ))))
    rail.set_result(key="skipped_record_count",val= len(list(filter(lambda x: get_status(x, 'skipped'), final_log_records ))))

    return  final_log_records