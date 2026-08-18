# pylint: disable=too-many-statements
import json
from pendulum import now
from rail import result, smartjoin_by_delim, find_first_by_attr_and_get_attr


def employee_type_group_source_list():
    rows = result('get_data_employee_type_group_list_service')['rows']
    return [{"code": cell['cells'][0].get("textValue", ""), "textValue": cell['cells'][1]['textValue'], "uri": cell['cells'][1]['uri']} for cell in rows]

def get_policyschedule_entries(response):
    return json.loads(json.dumps(response, ensure_ascii=False).replace(
                'null', '"effective"').replace('"script"', '"scriptTarget"')) if response else None

def get_filtered_user_data(response, dag_run):
    filtered_data = {}
    cells = response['rows'][0]['cells']
    if cells[1]['textValue'] == dag_run.conf['SupervisorID']:
        filtered_data = {
            "supervisor": cells[2]['textValue'],
            "firstname": cells[2]['textValue'].spit(',')[-1].strip(),
            "lastname": cells[2]['textValue'].spit(',')[0].strip(),
            "formattedname": f"{cells[2]['textValue'].spit(',')[0].strip()} {cells[2]['textValue'].spit(',')[-1].strip()}",
            "loginname": cells[0]['textValue'],
        }
    return filtered_data

def get_filtered_user_data_63(response, dag_run):
    if not response['rows']:
        return None
    cells = response['rows'][0]['cells']
    return cells[0]['uri'] if cells[1]['textValue'] == dag_run.conf['SupervisorID'] else None

def get_filtered_user_details(response, dag_run):
    cells = response['rows'][0]['cells']
    supervisor = cells[1]['uri'] if cells[1]['textValue'] == dag_run.conf['supervisorloginname'] else None
    today = now()
    resp = {
        "urioutput":supervisor if supervisor else None,
        "nameoutput":cells[2]['textValue'],
        "statusoutput":cells[0]['textValue'],
        "todaydayoutput":today.day,
        "todaymonthoutput":today.month,
        "todayyearoutput":today.year,
        "todayoutput":today
    }
    return resp if cells[1]['textValue'] == dag_run.conf['SupervisorID'] else None

def get_permission_sets(response):
    return find_first_by_attr_and_get_attr(response, "permissionSet.name", "Supervisor", "permissionSet")

def is_assign_supervisorpermission(response):
    return {
        "managerpermissionset": find_first_by_attr_and_get_attr(response, "permissionSet.name", "Manager", "permissionSet.uri"),
        "enduserpermissionset": find_first_by_attr_and_get_attr(response, "permissionSet.name", "End user with reports view", "permissionSet.uri")
    }

def timeofftypes_to_assign_24(response, dag_run):
    timeofflist = []
    timeoff_string = []
    timeoff = find_first_by_attr_and_get_attr(response, "name", dag_run.conf['Location'], "uri")
    timeoffinput1 = find_first_by_attr_and_get_attr(response, "name", dag_run.conf['Location'], "uri")
    holidaytimeoff = find_first_by_attr_and_get_attr(response, "name", "Holiday", "uri")
    if timeoffinput1:
        timeoff_string.append(timeoffinput1)
    if holidaytimeoff:
        timeoff_string.append(holidaytimeoff)
    if timeoff:
        for uri in timeoff.split("|"):
            timeofflist.append({
                "uri":uri
            })
    return {
        "timeofflist":timeofflist,
        "timeoff_string":timeoff_string
    }

def check_supervisor_id(response, dag_run):
    users_found = response['rows']
    matching_supervisor = list(filter(
        lambda supervisor: supervisor['cells'][1]['textValue'] == dag_run.conf['SupervisorID'], users_found))
    return bool(matching_supervisor)

def get_supervisor_details(response, dag_run):
    user_name = list(filter(
        lambda supervisor: supervisor['cells'][1]['textValue'] == dag_run.conf['SupervisorID'], response['rows'])) if response['rows'] else []
    firstname = user_name[0]['cells'][0]['textValue'].split(',')[0].strip() if user_name else ''
    lastname = user_name[0]['cells'][0]['textValue'].split(',')[1].strip() if user_name else ''
    return {
        "supervisor": bool(user_name),
        "firstname": firstname,
        "lastname": lastname,
        "formattedname": firstname + lastname
    }

def check_if_user_exists(response, dag_run):
    user_uri = ''
    if response['rows']:
        user_resp = list(filter(
        lambda x: x['cells'][1]['textValue'] == dag_run.conf['SupervisorID'], response['rows']))
        user_uri = smartjoin_by_delim(user_resp[0]['cells'][2]['uri'])
    return user_uri

def get_permissions_to_assign_user(response):
    return find_first_by_attr_and_get_attr(
        response, 'permissionSet.name', 'Supervisor', 'permissionSet.uri', '')
