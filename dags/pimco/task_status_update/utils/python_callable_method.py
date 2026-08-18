import re
import rail



def check_value_of_processed(isClosed):
    entry = rail.load_all_records(rail.result('search_entries_task_status_and_resource_update_lookup'))
    if entry[0]['properties']['processed'] != isClosed:
        return True
    return False

def get_payload_for_child(project):
    entries = rail.load_all_records(rail.result('search_entries_task_status_and_resource_update_lookup'))
    queryresult = rail.load_all_records(rail.result('query_all_projects'))
    task = [ {
            'taskuri': rail.find_first_by_attr_and_get_attr(queryresult, 'taskfullpath',
                       str(project['projectname']) + "-" + re.sub(" - " + entry['properties']['code'], "", entry['properties']['fullpath']),
                       'taskuri'),
            'taskname': entry['properties']['taskname'],
            'taskstatus': entry['properties']['processed']
        } for entry in entries]
    return {
        'projecturi': project['projecturi'],
        'projectname': project['projectname'],
        'task': task
    }


def is_name_for_uri_present(uri):
    entries = rail.load_all_records(rail.result('get_all_entries_pimco_task_table_for_model_project'))
    consultant_entries = rail.load_all_records(rail.result('get_all_entries_pimco_consultant_task_project'))
    for entry in entries:
        if entry['properties']['uri'] == uri and entry['properties']['name']:
            return True
    for entry in consultant_entries:
        if entry['properties']['uri'] == uri and entry['properties']['name']:
            return True
    return False
