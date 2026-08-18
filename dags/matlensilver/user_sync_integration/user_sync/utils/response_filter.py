from datetime import datetime
import rail


def get_date_from_replicon_date(replicon_date):
    if not replicon_date:
        return datetime.min
    return datetime(day=replicon_date['day'], month=replicon_date['month'], year=replicon_date['year'])


def get_filtered_user_data(response,dag_run):
    data = response.json()['d']
    return list(filter(lambda x: bool(x['employeeid']) and x['employeeid'] == dag_run.conf['employeeid'], map(lambda row: {
        "name": row['cells'][0]['textValue'],
        'loginname': row['cells'][1]['textValue'],
        "uri": row['cells'][0]['uri'],
        "employeeid": row['cells'][2]['textValue'] if row['cells'][2]['dataType'] != 'urn:replicon:list-type:null' else None,
        "status": row['cells'][3]['textValue']
    }, data['rows'])))


def get_filtered_tag_uri(response):
    replicon_tags = response.json()['d']['tags']
    return {
        'remote_uri':  rail.find_first_by_attr_and_get_attr(replicon_tags, 'name', 'Remote', 'uri'),
        'client_uri': rail.find_first_by_attr_and_get_attr(replicon_tags, 'name', 'Client', 'uri')
    }


def get_filtered_employee_grp(response):
    data = response.json()['d']
    return list(map(lambda row: {
        "name": row['cells'][0]['textValue'],
        'uri': row['cells'][0]['uri'],
        "code": row['cells'][1]['textValue'],
    }, data['rows']))


def map_supervisor_list_data(response, dag_run):
    data = response.json()['d']
    return list(filter(lambda x: x['employeeid'] == dag_run.conf['supervisorcode'], map(lambda item: {
        'name': item['cells'][0]['textValue'],
        'loginname': item['cells'][1]['textValue'],
        'uri': item['cells'][0]['uri'],
        'employeeid': item['cells'][2]['textValue'] if item['cells'][2]['dataType'] != 'urn:replicon:list-type:null' else None,
        'status': item['cells'][3]['textValue'],
        'enddate': get_date_from_replicon_date(item['cells'][4]['dateValue']).strftime("%m-%d-%Y") if item['cells'][4]['textValue'] != "" else None
    }, data['rows'])))


def get_missing_permissions(response, dag_run):
    supervisor_permission = False
    end_user_permission = False
    permissions_to_add = []
    if response:
        supervisor_permission = len(
            [x for x in response if x['permissionSet']['name'] == 'Timesheet Approver']) > 0
        end_user_permission = len([x for x in response if x['permissionSet']
                                  ['name'] == 'End User']) > 0

    if not supervisor_permission:
        permissions_to_add.append(dag_run.conf['timesheetapproveruri'])

    if not end_user_permission:
        permissions_to_add.append(dag_run.conf['enduseruri'])

    return permissions_to_add
