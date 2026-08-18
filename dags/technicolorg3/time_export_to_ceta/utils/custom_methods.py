from datetime import datetime
import json
from airflow.models import Variable
import rail


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)

def has_data(doc):
    if not doc:
        return None
    return get_data_from_document(doc)

def get_required_details(config):
    time_entry_object_extension_field = rail.find_first_by_attr_and_get_attr(
        rail.result("get_all_columns"), "displayText", "Time Entry Object Extension Field ")

    project = rail.find_first_by_attr_and_get_attr(
        rail.result("get_all_columns"), "displayText", "Project")

    return {
        "resource_schedule_service_id": rail.find_first_by_attr_and_get_attr(time_entry_object_extension_field['columns'],
            "displayText", config.search_attributes['rssid'], 'uri') if time_entry_object_extension_field else None,
        "mill_mpc": rail.find_first_by_attr_and_get_attr(project['columns'],
            "displayText", config.search_attributes['mill_mpc'], 'uri') if project else None,
        "jira": rail.find_first_by_attr_and_get_attr(project['columns'],
            "displayText", config.search_attributes['jira'], 'uri') if project else None,
        "file_format": rail.result("get_ceta_export_script"),
        "employee_type_group": rail.find_first_by_attr_and_get_attr(rail.result("get_all_employee_type_groups"),
            "displayText", config.search_attributes['employee_type'], "uri"),
        "export_name": "CETA Export_" + datetime.now().strftime("%Y%m%dT%H%M%S"),
        "file_name": "_finalexport" + datetime.now().strftime("%Y%m%dT%H%M%S"),
        "can_send_downstream_airflow_var": Variable.get(config.downstream_variable)
    }

def get_rows(item, sub_erp):
    if sub_erp.lower() not in ('mill', 'mpc'):
        return [item['global_id'],item['starttime'],item['endtime'],item['projectname'],item['taskname_fullpath'],item['hours'],item['timeentryid']]

    return [
        item['depot'],
        item['database'],
        item['global_id'],
        item['rss_id'],
        item['starttime'],
        item['endtime'],
        item['project_number'],
        item['title'],
        item['role'],
        item['service'],
        item['description'],
        item['duration'],
    ]


def get_csv_headers(sub_erp):
    if sub_erp in ("mill", "mpc"):
        return ["depot", "database", "global_id", "rss_id", "starttime", "endtime", "project_number", "title", "role", "service", "description", "duration"]
    return ["global ID", "Start time", "End time", "Project Name", "Task Name Full Path", "Hours", "Time Entry ID"]


def get_status():
    if len(rail.result("process_mill_data") if rail.result("process_mill_data") else []) == 0 and\
        len(rail.result("process_mpc_data") if rail.result("process_mpc_data") else []) == 0 and\
            (rail.result("process_skipped_data",'length') if rail.result("process_skipped_data") else []) > 0:
        return "skipped"

    if len(rail.result("process_mill_data") if rail.result("process_mill_data") else []) > 0 or\
        len(rail.result("process_mpc_data") if rail.result("process_mpc_data") else []) > 0 or\
            (rail.result("process_skipped_data",'length') if rail.result("process_skipped_data") else []) > 0:
        return "processed"

    return "no_records"


def get_formatted_date(date):
    if not date:
        return None
    return date.replace("/", "-")


def get_role_service_description(item, index):
    if not item:
        return None
    if len(item.split(" / ")) >= index+1:
        return item.split(" / ")[index]
    return None


def get_processed_data(data, database):
    if not data:
        return []
    report_data = get_data_from_document(
        rail.result("create_report_collection"))

    res = list(map(lambda item:{
        "depot":("".join([x['work_location_full_path'] for x in report_data if x['employeeid'] == item['employeeid']])).replace(" / ", ","),
        "database":database,
        "global_id":int(item['employeeid']) if item['employeeid'] else 0,
        "rss_id":int(item['rssid']) if item['rssid'] else 0,
        "starttime":get_formatted_date(item['entrydate']) if item['entrydate'] else None,
        "endtime":get_formatted_date(item['entrydate']) if item['entrydate'] else None,
        "project_number":item['projectcode'] if item['projectcode'] else None,
        "title":item["projectname"].split('|')[0],
        "role":get_role_service_description(item['taskname_fullpath'], index=0),
        "service":get_role_service_description(item['taskname_fullpath'], index=1),
        "description":get_role_service_description(item['taskname_fullpath'], index=2),
        "duration":float(item['hours'])
    },data))
    return json.dumps(res, separators=(',',':'), ensure_ascii=False)


def get_skipped_data(item):
    if not item:
        return []
    res = {
        "global_id": int(item['employeeid']),
        "starttime": get_formatted_date(item['entrydate']) if item['entrydate'] else None,
        "endtime": get_formatted_date(item['entrydate']) if item['entrydate'] else None,
        "projectname": item['projectname'],
        "taskname_fullpath": item['taskname_fullpath'],
        "hours": item['hours'],
        "timeentryid": item['time_entry_id']
    }
    return {k: v if v is not None else '' for k, v in res.items()}
