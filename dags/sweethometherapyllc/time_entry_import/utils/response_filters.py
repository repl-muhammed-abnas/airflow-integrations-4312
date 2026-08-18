import itertools
import rail
from typing import Dict, Any, List, Optional

null = None

def get_timesheet_details(response: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not response:
        return []

    timesheet_status_mapping = {
        'waiting': 'Waiting for Approval',
        'open': 'Not Submitted',
        'rejected': 'Rejected',
        'approved': 'Approved'
    }
    flatten_rows = list(itertools.chain(
        list(map(lambda x: x['timesheet'], response))))
    res = []
    for ts in flatten_rows:
        res.append({
        "timesheet_status": timesheet_status_mapping.get(ts['statusUri'].split(':')[-1], ''),
        "timesheet_status_uri": ts['statusUri'],
        "timesheet_uri": ts['uri'],
        "timesheet_date_range": ts['dateRange'],
        "user_uri": ts['owner']['uri']
    })
    return list(map(lambda ts: {
        "timesheet_status": timesheet_status_mapping.get(ts['statusUri'].split(':')[-1], ''),
        "timesheet_status_uri": ts['statusUri'],
        "timesheet_uri": ts['uri'],
        "timesheet_date_range": ts['dateRange'],
        "user_uri": ts['owner']['uri']
    }, flatten_rows))





def get_timesheet_detail_for_item(response, item):
    ts = (response or {}).get('timesheet')
    if not ts:
        return None
    timesheet_status_mapping = {
        'waiting': 'Waiting for Approval',
        'open': 'Not Submitted',
        'rejected': 'Rejected',
        'approved': 'Approved'
    }
    return {
        "timesheet_status": timesheet_status_mapping.get(ts['statusUri'].split(':')[-1], ''),
        "timesheet_uri": ts['uri'],
        "date_of_service": item['date_of_service']
    }


def find_tag_uri_by_name(tags_for_dropdown_oef,oef_value,object_extension_fields,extension_field_name):
    if not tags_for_dropdown_oef:
        return False
    oef_tag_uri = rail.find_first_by_attr_and_get_attr(
        tags_for_dropdown_oef, 'name', oef_value, "uri")
    oef_uri= rail.find_first_by_attr_and_get_attr(
        object_extension_fields, 'name', extension_field_name, 'uri')
    return {"oef_name":extension_field_name,"oef_uri":oef_uri,"oef_value": oef_value,"oef_value_uri": oef_tag_uri}