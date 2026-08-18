# pylint: disable=too-many-statements
from hashlib import md5
from pendulum import now
from rail import load_all_records, result, find_first_by_attr_and_get_attr


def get_dag_trigger_time():
    return {
        "dag_trigger_time": now().strftime('%Y%m%dT%H%M%S')
    }

def get_current_date_time():
    return {
        "year": now().year,
        "month": now().month,
        "day": now().day
    }

def get_md5(item):
    return md5((
        item["Login Name"]+ "_"+ item["First Name"]+ "_"+ item["Last Name"]+ "_"+ item["Employee Type"]
        + "_"+ item["Department"]+ "_"+ item["Enabled"]+ "_"+ item["Employee Id"]+ "_"+ item['Start Date']
        + "_"+ item['End Date']+ "_"+ item["Email Address"]+ "_"+ item["Supervisor ID"]+ "_"+ item["Permission Sets"]
        + "_"+ item["Location"]+ "_"+ item["Time Zone"]+ "_"+ item["Work Week"]+ "_"+ item["Holiday Calendar"]
        + "_"+ item["Initial Schedule Name"]+ "_"+ item["Annual Salary"]+ "_"+ item["ELT"]+ "_"+ item["2nd Line Manager"]
        + "_"+ item["Work week Hours"]+ "_"+ item["Business card Title"]+ "_"+ item["Cost Center"]+ "_"+ item["Division"]
    ).encode('utf-8')).hexdigest()

def get_csv_line_items(item):
    return [
        item['Login Name'],
        item['First Name'],
        item['Last Name'],
        item['Employee Type'],
        item['Department'],
        item['Enabled'],
        item['Employee Id'],
        item['Start Date'],
        item['End Date'],
        item['Email Address'],
        item['Supervisor ID'],
        item['Permission Sets'],
        item['Location'],
        item['Time Zone'],
        item['Work Week'],
        item['Holiday Calendar'],
        item['Initial Schedule Name'],
        item['Annual Salary'],
        item['ELT'],
        item['2nd Line Manager'],
        item['Work week Hours'],
        item['Business card Title'],
        item['Cost Center'],
        item['Division'],
        get_md5(item)
    ]

def user_check(employeeid):
    csv_data = load_all_records(result('parse_report_payload'))
    user = list(filter(lambda x: (x['Employee ID'] == employeeid), csv_data))
    return {
        "user":user[0] if user else {},
        "useruri":find_first_by_attr_and_get_attr(csv_data, 'Employee ID', employeeid, 'UserUri')
    }

def get_supervisor_details(supervisorid):
    query_data = load_all_records(result('get_supervisors_from_feed_file'))
    supervisordetails = list(filter(lambda x: (x['employeeid'] == supervisorid), query_data))
    empty_resp =  {
        "loginname":"","firstname":"","lastname": "","employeetype": "","department": "","enabled": "","employeeid": "","startdate": "",
        "enddate": "","emailaddress": "","supervisorid": "","permissionsets": "","location": "","timezone": "","workweek": "","holidaycalendar":"",
        "initialschedulename": "","annualsalary": "","elt": "","secondlinemanager": "","workweekhours": "","businesscardtitle": "","costcenter": "",
        "division":"",
    }
    return {
        "supervisordetails":supervisordetails[0] if supervisordetails else empty_resp
    }

def split_startdate(dag_run):
    return {
        "year" : dag_run.conf['StartDate'].split('/')[2],
        "month" : dag_run.conf['StartDate'].split('/')[0],
        "day" : dag_run.conf['StartDate'].split('/')[1]
    }

def get_required_work_week(dag_run):
    workweek = dag_run.conf['Workweek'].split('-')[0].strip().lower()
    return f"urn:replicon:day-of-week:{workweek}"

def get_all_permissionsets_from_payload(dag_run):
    return dag_run.conf['PermissionSets'].split('|')

def get_permission_uri():
    all_permissions = result('get_all_permissionsets')
    permissions_from_payload = result('get_all_permissionsets_from_payload')
    permission_uri = []
    permissions = []
    for permission in permissions_from_payload:
        uri = find_first_by_attr_and_get_attr(all_permissions, 'name', permission, 'uri')
        if not uri:
            continue
        permissions.append({
            "name":permission,
            "uri": uri
        })
        permission_uri.append(uri)
    return {
        "permissions":permissions,
        "permissiontoassign": permission_uri
    }

def get_timeofftypes_to_assign(dag_run):
    timeoff_string = []
    timeoff_types = result('get_enabled_timeoff_types')
    location_uri = find_first_by_attr_and_get_attr(timeoff_types, 'name', dag_run.conf['Location'], 'uri', '')
    holiday_uri = find_first_by_attr_and_get_attr(timeoff_types, 'name', 'Holiday', 'uri', '')
    if location_uri:
        timeoff_string.append(location_uri)
    if holiday_uri:
        timeoff_string.append(holiday_uri)
    return {
        "timeofflist": [{"uri":item} for item in location_uri.split('|')] if location_uri else [],
        "timeoff_string": timeoff_string
    }
