import re
import rail

null = None

def get_uri_for_task_name_and_code(taskname, taskcode):
    bulktaskdetails = rail.result('bulk_get_task_details')
    uri=''
    for taskdetail in bulktaskdetails:
        if taskdetail['code'] == taskcode and taskdetail['name'] == taskname:
            uri=taskdetail['uri']
    return uri

def get_payload_for_child(project_data):
    resource = []
    resourceteamassignment = []
    for item in rail.result('get_resourceassignment'):
        resource.append({
            'resourceuri': item['resource'],
            'taskname': item['taskfullpath'],
            'taskcode': item['taskcode']
        })
    for item in rail.result('getresource_assignment'):
        resourceteamassignment.append({
            'resourceuri': item['resourceuri'],
            'taskuri': item['resourceuri']
        })
    return {
        'projecturi': project_data['projecturi'],
        'projectname': project_data['projectname'],
        'resource': resource,
        'resourceteamassignment': resourceteamassignment,
        'alldepartmenturi': [ item['uri'] for item in rail.result('get_enabled_departments')]
    }


def create_resource_assignment():
    entries = rail.load_all_records(rail.result('search_entries_task_status_and_resource_update_lookup'))
    resourceassignment = []
    for entry in entries:
        resourceassignment.append({
            'resource': entry['properties']['taskname'],
            'taskuri': entry['properties']['uri'],
            'taskfullpath': re.sub((" - "+ str( entry['properties']['code'] )),"",entry['properties']['fullpath']),
            'taskcode': entry['properties']['code']
        })
    return resourceassignment

def create_resourceassignment():
    bulkresourceassignments = rail.result('bulk_get_resource_assignments')
    resourceassignment = []
    for resource in bulkresourceassignments:
        resourceassignment.append({
            'resource': [ item['resource']['uri'] for item in resource['assignments'] ],
            'taskuri': resource['taskUri'],
            'taskfullpath': rail.find_first_by_attr_and_get_attr( rail.result('get_resource_assignment'),'taskuri',resource['taskUri'],'resource'),
            'taskcode': rail.find_first_by_attr_and_get_attr( rail.result('get_resource_assignment'),'taskuri',resource['taskUri'],'taskcode')
        })
    return resourceassignment

def createresource_assignment():
    initialteamdata = rail.result('get_initial_team_data')
    resourceassignment = []
    for teamdata in initialteamdata:
        resourceassignment.append({
            'resource': teamdata['resource']['displayText'],
            'resourceuri': teamdata['resource']['uri']
        })
    return resourceassignment

def get_payload_bulk_resource_assignment():
    taskUris = [ item['taskuri'] for item in rail.result('get_resource_assignment')]
    return {
        "taskUris": taskUris,
        "asOfDate": null
    }
