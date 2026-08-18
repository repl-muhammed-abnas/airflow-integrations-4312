import rail

null = None
null_urn = "urn:replicon:list-type:null"


def get_timesheet_oef(response, worktype_mapper):
    data = response.json()['d']
    def get_worktype_from_mapper():
        worktype = set()
        worktype_list = list(map(lambda item: {
            item['taskname'] : rail.find_first_by_attr_and_get_attr(data, 'displayText', item['worktype_name'], 'uri')
        }, worktype_mapper))
        formatted_worktype_list = {k:v for d in worktype_list for k, v in d.items() if k not in worktype and not worktype.add(k)}
        return formatted_worktype_list

    return {
        **get_worktype_from_mapper(),
        **{
            "wdid": rail.find_first_by_attr_and_get_attr(data, 'displayText', 'Time off entry ID', 'uri'),
            "worklocation": rail.find_first_by_attr_and_get_attr(data, 'displayText', 'Work Location', 'uri'),
            "timeentryid": rail.find_first_by_attr_and_get_attr(data, 'displayText', 'TimeentryID', 'uri')
        }
    }


def get_timeentry_column_uri(response):
    data = response.json()['d'][0]['columns']
    return rail.find_first_by_attr_and_get_attr(data, 'displayText', 'Time off entry ID', 'uri') if data else null


def get_timeentry_filter_definition_uri(response):
    data = response.json()['d']
    return rail.find_first_by_attr_and_get_attr(data, 'name', 'Time off entry ID', 'uri') if data else null

def get_value(item, index, pluck_key='textValue'):
    return item[index][pluck_key] if item[index]['dataType'] != null_urn else null

def get_user_details(response, dag_run):
    data = response.json()['d']
    if data['rows']:
        return list(filter(lambda x : x['user_login_name'] == dag_run.conf['userloginname'],(map(lambda row: {
            'user_login_name': get_value(row['cells'], 0, 'textValue'),
            'useruri': get_value(row['cells'], 0, 'uri'),
            'userstatus': get_value(row['cells'], 6, 'textValue'),
            'location': get_value(row['cells'], 3, 'textValue'),
        }, data['rows']))))
    return null


def get_projectdetails(response):
    data = response.json()['d']
    if not data[0]['error']:
        return list(map(lambda project: {
            'projecturi': project['projectDetails']['uri'] if project['projectDetails']['uri'] else null,
            'projectstatus': project['projectDetails']['status']['displayText'] if project['projectDetails']['status']['displayText'] else null,
            'timeentryallowedflag': project['projectDetails']['isTimeEntryAllowed'],
            # pylint: disable=line-too-long
            'extensionfieldvalues': project['projectDetails']['extensionFieldValues'][0]['tag']['displayText'] if project['projectDetails']['extensionFieldValues'] else null
        }, data))
    return []


def get_rounded_duration(durationvalue):
    if not durationvalue:
        return 0.0
    return round((float(durationvalue['hours']) + float(
        durationvalue['minutes'] / 60) + float(durationvalue['seconds'] / 3600)), 2)


def get_timeentries_list(response):
    rows = response.json()['d']['rows']
    return list(map(lambda row: {
        "timeentryrevisiongroup": rail.find_first_by_attr_and_get_attr(row['cells'], 'objectType', 'urn:replicon:object-type:time-entry-revision-group', 'uri'),
        "entrydate": rail.find_first_by_attr_and_get_attr(row['cells'], 'dataType', 'urn:replicon:list-type:date', 'textValue'),
        "hours": rail.find_first_by_attr_and_get_attr(row['cells'], 'dataType', 'urn:replicon:list-type:calendar-day-duration', 'calendarDayDurationValue'),
        "projectname": rail.find_first_by_attr_and_get_attr(row['cells'], 'objectType', 'urn:replicon:object-type:project', 'textValue'),
        "projecturi": rail.find_first_by_attr_and_get_attr(row['cells'], 'objectType', 'urn:replicon:object-type:project', 'uri'),
        "taskname": rail.find_first_by_attr_and_get_attr(row['cells'], 'objectType', 'urn:replicon:object-type:task', 'textValue'),
        "taskuri": rail.find_first_by_attr_and_get_attr(row['cells'], 'objectType', 'urn:replicon:object-type:task', 'uri'),
        "timeentryid": row['cells'][5]['textValue'] if row['cells'][5] and row['cells'][5]['textValue'] else null,
        "comments": row['cells'][6]['textValue'] if row['cells'][6] and row['cells'][6]['dataType'] != 'urn:replicon:list-type:null' else null,
        "duration": get_rounded_duration(
                    rail.find_first_by_attr_and_get_attr(row['cells'], 'dataType', 'urn:replicon:list-type:calendar-day-duration', 'calendarDayDurationValue')),
        "approvalstatus": rail.find_first_by_attr_and_get_attr(row['cells'], 'objectType', 'urn:replicon:object-type:approval-status', 'textValue')
    }, rows)) if rows else []


def get_filtered_tasks(response):
    data = response.json()['d']
    return list(map(lambda d: {
        "taskcode": d['task']['code'],
        "taskname": d['task']['name'],
        "uri": d['task']['uri'],
    }, data)) if response.json()['d'] else []


def get_timesheet_approval_status(response):
    data = response.json()['d']
    if data and data['rows']:
        return list(map(lambda x: {
            'approval_status': rail.find_first_by_attr_and_get_attr(x['cells'], 'objectType', 'urn:replicon:object-type:approval-status', 'textValue'),
            'approval_uri': rail.find_first_by_attr_and_get_attr(x['cells'], 'objectType', 'urn:replicon:object-type:approval-status', 'uri')
        }, data['rows']))
    return []
