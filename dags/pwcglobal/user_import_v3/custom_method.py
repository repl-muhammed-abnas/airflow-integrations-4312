
import json
from datetime import datetime
from hashlib import md5
from dateutil import parser
from pendulum import now
import rail
from pwcglobal.user_import_v3 import request_payload

null=None
def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def map_list_data(res):
    data = res.json()['d']['rows']
    return list(
        map(lambda item:
            {
                'name': item['cells'][0]['textValue'],
                'uri': item['cells'][0]['uri'],
                'code': item['cells'][1].get('textValue'),
            }, data)
    )


def map_timesheetperiod_search_result(res):
    data = res.json()['d']
    timesheetperiodtype = request_payload.get_conf()['timesheetperiodtype']
    timesheet_period = list(
        filter(lambda x: x['displayText'] == timesheetperiodtype, data))
    if len(timesheet_period) == 0:
        rail.set_result(
            f'Timesheet period not updated since {timesheetperiodtype} is not available in Replicon', 'log')
        return None
    return timesheet_period[0]


def map_supervisor_list(res):
    data = res.json()['d']['rows']
    return list(
        filter(lambda x: x['employeeid'] == request_payload.get_conf()['supervisor'].split('||')[0],
               map(lambda item:
                   {
                       'useruri': item['cells'][0]['uri'],
                       'employeeid': item['cells'][1].get('textValue'),
                       'enabled': item['cells'][2].get('boolValue'),
                   }, data))
    )


def map_supervisor_by_legalentity(res):
    data = res.json()['d']['rows']
    return list(
        map(lambda item:
            {
                'useruri': item['cells'][0]['uri'],
                'enabled': item['cells'][2].get('boolValue'),
            }, data)
    )


def map_impersonate_and_create_interactive_session(res):
    data = res.json()['d']
    auth_token = list(
        filter(lambda x: x['name'] == 'AUTHTOKEN', data['sessionCookies']))[0]['value']
    tenant = list(
        filter(lambda x: x['name'] == 'TENANT', data['sessionCookies']))[0]['value']
    return {'cookie': f'AUTHTOKEN={auth_token};TENANT={tenant}', 'Path': '/'}


def do_format_logs():
    master_log = json.loads(rail.result('load_master_log'))
    for log in (rail.result('gather_logs') or []):
        log_records = rail.load_all_records(log)
        if log_records:
            master_log.extend(log_records)
    users = list(
        set(map(lambda x: x['properties'].get('userpartyid', ''), master_log)))
    logs = []
    # pylint: disable=cell-var-from-loop
    for userid in users:
        user_logs = list(
            filter(lambda x: x['properties'].get('userpartyid', '') == userid and x['properties'].get('message', ''), master_log))
        error_logs = list(
            filter(lambda x: x['properties'].get('status') == 'Error', user_logs))
        if len(user_logs) > 0:
            first = user_logs[0]
            if error_logs:
                status = "Error"
            else:
                status = first['properties'].get('status', 'Error')
            logs.append({
                # 2022-04-29 08:20:49
                'Date': parser.parse(first['timestamp']).strftime('%Y-%m-%d %H:%M:%S'),
                'LegalentityID': first['properties'].get('legalentityid'),
                'UserPartyID': userid,
                # formatting WARN EXCEPTION ERROR SUCCESS
                'Status': status.upper(),
                'Details': ', '.join(list(map(lambda x: x['properties'].get('message'), user_logs))),
                'Ecid': first['ecid'],
            })
    return json.dumps(logs, ensure_ascii=False)


def get_existing_blob_md5(item):
    if not item:
        return []
    return {
        "value": str(item["value"]),
        "effectivedate": item["effectivedate"],
        "md5": md5(str(str(item["value"])+"_"+item["effectivedate"]).encode()).hexdigest()
    }


def get_new_records_md5():
    item = str(request_payload.get_conf()["ftepercent"])
    effectivedate = ""
    effectivedate = datetime.strftime(datetime.strptime(
        request_payload.get_timesheet_start_date(), "%Y%m%d"), "%d/%m/%Y")
    if not item:
        return []
    return [{
        "value": item,
        "effectivedate": effectivedate,
        "md5": md5(str(item+"_"+effectivedate).encode()).hexdigest()
    }]

def user_with_legal_entity_id():
    if len(rail.result("get_user_with_line_manager_partyid")) == 1:
        return rail.result("get_user_with_line_manager_partyid")[0]
    if request_payload.get_conf()["linemanagerlegalentityuri"]:
        res_indx = rail.find_index_by_attr(
            rail.result("get_user_with_line_manager_partyid"),
            "legalentityuri",
            request_payload.get_conf()["linemanagerlegalentityuri"]
        )
        return rail.result("get_user_with_line_manager_partyid")[res_indx]
    return null


def do_get_exception_logs():
    logs = []
    location_info = rail.result('get_location_details') or {}
    if not location_info.get('code'):
        logs.append(
            'Display name defaulted since Country code not available in the instance')
    if rail.result('get_timesheetperiodtype_uri', 'log'):
        logs.append(rail.result('get_timesheetperiodtype_uri', 'log'))
    if len(request_payload.get_conf().get('validationlog', [])) > 0:
        logs.extend(list(
            map(lambda item: item['message'], request_payload.get_conf()['validationlog'])))
    if not request_payload.get_conf()["linemanagerpartyid"]:
        logs.append("Line manager party id not specified in the import")
    if not rail.result("get_user_with_line_manager_partyid"):
        logs.append("Line Manager not present in replicon")
    if not request_payload.get_conf()["ftepercent"]:
        logs.append("FTE Percent not specified in the import")
    if not request_payload.get_conf()["country"] and request_payload.get_conf()["toil"] == "Y":
        logs.append("TOIL time off type is not assigned as country is not specified")
    if rail.result(" if_fte_effective_date_in_past") == "get_user_details" and \
        rail.result("if_product_license_present") == "get_exception_logs":
        logs.append("Product licencse not assigned for user hence fte percent with past effective date not updated")
    if rail.result("if_timesheet_template_assigned")  and\
          not rail.result("get_current_timesheet_period"):
        logs.append("Time sheet not assigned for user hence fte percent with past effective date not updated")
    if rail.result("if_line_manager_with_legal_entity_id") == "line_manager_complete":
        logs.append("Mulitple users with same linemanager party id or legal entity id")
    if rail.result("has_valid_linemanager") == "line_manager_complete":
        logs.append("Line manager party id and user party id are the same")
    return logs


def get_user_data(response):
    return list(filter(lambda x: request_payload.get_conf()["linemanagerpartyid"] == x["partyid"],
                    list( map(lambda i: {
                        "displayText": i["cells"][0]["textValue"],
                        "linemanageruri": i["cells"][0]["uri"],
                        "legalentityname": i["cells"][1]["textValue"] if "textValue" in i["cells"][1] else "",
                        "legalentityuri": i["cells"][1]["uri"] if "uri" in i["cells"][1] else "",
                        "linemanagerloginname": i["cells"][2]["textValue"],
                        "partyid": i["cells"][3]["textValue"]
                    }, response["rows"])))) if response else null

def get_permission_set(permission_add):

    user_permission_sets = rail.result("get_assigned_permissionsets")
    user_permission_sets = list(map(lambda i: {"uri":i["permissionSet"]["uri"],
                                         "displayText": i["permissionSet"]["displayText"]
                                         }, user_permission_sets))
    if permission_add == "remove":
        permission_set = list(filter(lambda i: i["displayText"] == "ZT User without collectors", user_permission_sets))
        if not permission_set:
            return []
        permission_set = list(map(lambda i:i["uri"], permission_set))[0]

    elif request_payload.get_conf()["zerotimeuserpermissionseturi"] and permission_add == "add":
        permission_set = list(map(lambda i:i["uri"], user_permission_sets))
        if request_payload.get_conf()["zerotimeuserpermissionseturi"] not in permission_set:
            permission_set.append(request_payload.get_conf()["zerotimeuserpermissionseturi"])

    return permission_set


def check_non_zt_country(dag_run):
    current_country = request_payload.get_attr_value(rail.result("get_effective_user_groupmembership"),
                                    'locations.0.location.location.displayText')
    if current_country in dag_run.conf["zerotime_mapper"] and \
        dag_run.conf["country"] not in dag_run.conf["zerotime_mapper"]:
        return True
    return False

def check_update_zerotime(dag_run):
    current_country = request_payload.get_attr_value(rail.result("get_effective_user_groupmembership"),
                                    'locations.0.location.location.uri')
    if dag_run.conf['country'] and dag_run.conf['countriesgroupuri'] and\
        dag_run.conf['countriesgroupuri'] != current_country and\
        dag_run.conf["zerotimeuserpermissionseturi"] and\
        not rail.find_first_by_attr_and_get_attr(
            rail.result("get_assigned_permissionsets"),
            "permissionSet.displayText",
            dag_run.conf["zerotime_mapper"][dag_run.conf["country"]],
            "uri"):
        return True
    return False

def check_fte_date_within_range(dag_run):
    if dag_run.conf["ftepercenteffectivedate"] and request_payload.check_past_date() > 0:
        today_d = now(tz='Europe/London')
        t_month = today_d.month
        if t_month < 10:
            t_month = "0" + str(t_month)
        today_date = datetime.strptime(str(today_d.year)+str(t_month)+str(today_d.day), "%Y%m%d")
        return (today_date-datetime.strptime(dag_run.conf["ftepercenteffectivedate"], "%Y%m%d")).days <=30
    if dag_run.conf["ftepercenteffectivedate"] and request_payload.check_past_date() <= 0:
        return True
    return null
