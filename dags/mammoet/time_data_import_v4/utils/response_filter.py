import rail
from mammoet.time_data_import_v4.mapper.oef_mapper import OEF_MAPPER

null = None


def get_value(item, index, pluck_key):
    return item[index].get(pluck_key)


def get_timesheet_details(response):
    if not response['rows']:
        return []
    return list(map(lambda ts: {
        "timesheet_status": get_value(ts['cells'], 0, 'textValue'),
        "timesheet_status_uri": get_value(ts['cells'], 0, 'uri'),
        "timesheet_uri": get_value(ts['cells'], 1, 'uri'),
        "timesheet_date_range": get_value(ts['cells'], 2, 'dateRangeValue'),
        "user_uri": get_value(ts['cells'], 3, 'uri')
    }, response['rows']))


def get_rounded_duration(durationvalue):
    if not durationvalue:
        return 0
    return round((float(durationvalue['hours']) + float(
        durationvalue['minutes'] / 60) + float(durationvalue['seconds'] / 3600)), 2)

def get_timeentries_list_replicon_unique_id(response):
    rail.set_result(key="failure", val=False)
    if not response:
        return []
    return {
        "timeentryrevisiongroup": response[0]['uri'],
        "approvalstatus": response[0]['approvalStatus']['displayText']
    }

def get_timeentries_list(response):
    rows = response['rows']
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


def get_timeentry_column_uri(response):
    return {
        'sap_column_uri': rail.find_first_by_attr_and_get_attr(response[0]['columns'], 'displayText', 'SAP Counter ID', 'uri'),
        'replicon_column_uri': rail.find_first_by_attr_and_get_attr(response[0]['columns'], 'displayText', 'Replicon ID', 'uri')
    } if response[0]['columns'] else null


def get_timeentry_filter_definition_uri(response):
    return {
        'sap_filter_uri': rail.find_first_by_attr_and_get_attr(response, 'name', 'SAP Counter ID', 'uri'),
        'replicon_filter_uri': rail.find_first_by_attr_and_get_attr(response, 'name', 'Replicon ID', 'uri')
    } if response else null

def get_activity_type_oef_uri(response):
    oef_list =  list(filter(lambda x: 'Attendance Type' in x['name'], map(lambda item:{
        'name': item['definition']['displayText'],
        'uri': item['tag']['uri'] if item['tag'] else None
    }, response[0]['extensionFieldValues']))) if response else None

    return {
        'activity_uri': [item['uri'] for item in oef_list if item] if oef_list else None,
        'comments': rail.find_first_by_attr_and_get_attr(response[0][
        'customMetadata'],'keyUri','urn:replicon:time-entry-metadata-key:comments', 'value.text')
    }

def get_all_oef_details(res):
    attendence_type_oefs = []

    for index,item in enumerate(OEF_MAPPER):
        attendence_type_oefs.append(item)
        attendence_type_oefs[index]['uri'] = rail.find_first_by_attr_and_get_attr(res, 'name', item['oef_name'], 'uri', None)

    return {
        'sap_counter_id': rail.find_first_by_attr_and_get_attr(res, 'name', 'SAP Counter ID', 'uri'),
        'replicon_id': rail.find_first_by_attr_and_get_attr(res, 'name', 'Replicon ID', 'uri'),
        'time_entry_type': rail.find_first_by_attr_and_get_attr(res, 'name', 'Time Entry Type', 'uri'),
        'account_indicator': rail.find_first_by_attr_and_get_attr(res, 'name', 'Account Indicator', 'uri'),
        'crane_capacity': rail.find_first_by_attr_and_get_attr(res, 'name', 'Crane Capacity', 'uri'),
        'attendence_type_oefs': attendence_type_oefs
    }
