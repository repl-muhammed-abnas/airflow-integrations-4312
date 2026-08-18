from uuid import uuid4
import rail

DATE_FORMAT = "%m/%d/%Y"

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
        "user_uri": get_value(ts['cells'], 3, 'uri'),
        "uuid": str(uuid4())
    }, response['rows']))

def get_timeentries_list(response):
    rows = response['rows']
    return list(map(lambda row: {
        "timeentryrevisiongroup": rail.find_first_by_attr_and_get_attr(row['cells'], 'objectType', 'urn:replicon:object-type:time-entry-revision-group', 'uri'),
        "entrydate": rail.find_first_by_attr_and_get_attr(row['cells'], 'dataType', 'urn:replicon:list-type:date', 'textValue'),
       "approvalstatus": rail.find_first_by_attr_and_get_attr(row['cells'], 'objectType', 'urn:replicon:object-type:approval-status', 'textValue')
    }, rows)) if rows else []