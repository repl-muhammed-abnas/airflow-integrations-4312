from datetime import datetime
import functools
import json
import pendulum
import rail
from wikwemikongboard.timeoffbalancetransfer.mapper.timeoff_type_combination import TIMEOFF_TYPES_COMBINATION


def get_user_timeoff_types(dag_run):
    return {
        "userUri": dag_run.conf['uri']
    }


def put_user_timeoff_policy(dag_run):
    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['uri'],
            "timeOffTypeUri": dag_run.conf['sickleavebankeduri'][-1]
        },
        "policySetScheduleEntries": rail.result('get_timeoff_payload')
    }


def get_user_details(dag_run):
    return {
        "users": [
            {
                "uri": dag_run.conf['uri']
            }
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }


def get_timeoff_balance(dag_run):
    return {
        "userUris": [
            dag_run.conf['uri']
        ],
        "timeOffTypeUris": list(map(lambda data: data["uri"],
                                    rail.load_all_records(rail.result('get_user_timeoff_types')))),
        "asOfDate": {
            "year": int(pendulum.now().strftime("%Y")),
            "month": 3,
            "day": 31
        }
    }


def get_all_data(dag_run):
    data = list(map(lambda details: {
        "displayText": dag_run.conf['displayText'],
        "loginName": dag_run.conf['loginName'],
        "uri": dag_run.conf['uri'],
        "timeoffname": details["name"],
        "timeoffuri": details["uri"],
        "timeRemaining": list(map(lambda data: data["balanceSummary"]["timeRemaining"], filter(lambda data: data["timeOffTypeUri"] == details["uri"],
                                                                                        rail.load_all_records(rail.result('get_timeoff_balance_details')))))[0],
        "timeunit": list(map(lambda data: data["balanceSummary"]["measurementUnitUri"], filter(lambda data: data["timeOffTypeUri"] == details["uri"],
                                                                                        rail.load_all_records(rail.result('get_timeoff_balance_details')))))[0],
        "isTimeOffAllowedAgainstThisTimeOffType": details["enabled"],
        "timeoffnameidentifier": str(details["name"]).split("-",maxsplit=1)[0],
        "timeOffTemplate": rail.result('get_user_details')[0]["timeofftemplate"]
    }, rail.load_all_records(rail.result('get_user_timeoff_types'))))

    return data


def get_all_data_from_list(list_data):
    all_data = []

    for sublist in list_data:
        for item in sublist:
            all_data.append(item)

    return all_data

@functools.lru_cache(maxsize=128)
def get_time_off_data():
    return rail.load_all_records(rail.result('create_timeoff_data_collection'))

def process_transfer_timeoff_conf(item):

    data = get_time_off_data()

    filtered_data = [record for record in data if record["uri"] == item["uri"]]

    timeoffname = [record["timeoffname"] for record in filtered_data if bool(
      record["isTimeOffAllowedAgainstThisTimeOffType"])]

    return {

      "displaytext": next(record["displayText"] for record in filtered_data),
      "loginName": item["loginName"],
      "uri": item["uri"],
      "personalleavebalance": [record["timeRemaining"] for record in filtered_data if record["timeoffname"] in
                                ["Personal Leave", "Personal Leave - 10 months", "Personal Leave - Maintenance"] and
                                bool(record["isTimeOffAllowedAgainstThisTimeOffType"])],
      "sickleaveannualbalance": [record["timeRemaining"] for record in filtered_data if record["timeoffname"] in
                                  ["Sick Leave - Annual", "Sick Leave - 10 months - Annual", "Uncertified Sick - Maintenance"] and
                                  bool(record["isTimeOffAllowedAgainstThisTimeOffType"])],
      "sickleavebankedbalance": [record["timeRemaining"] for record in filtered_data if record["timeoffname"] in
                                  ["Sick Leave - Banked", "Sick Leave - 10 months - Banked", "Certified Sick - Maintenance"] and
                                  bool(record["isTimeOffAllowedAgainstThisTimeOffType"])],
      "personalleaveuri": [record["timeoffuri"] for record in filtered_data if record["timeoffname"] in
                            ["Personal Leave", "Personal Leave - 10 months", "Personal Leave - Maintenance"] and
                            bool(record["isTimeOffAllowedAgainstThisTimeOffType"])],
      "sickleaveannualuri": [record["timeoffuri"] for record in filtered_data if record["timeoffname"] in
                              ["Sick Leave - Annual", "Sick Leave - 10 months - Annual", "Uncertified Sick - Maintenance"] and
                              bool(record["isTimeOffAllowedAgainstThisTimeOffType"])],
      "sickleavebankeduri": [record["timeoffuri"] for record in filtered_data if record["timeoffname"] in
                              ["Sick Leave - Banked", "Sick Leave - 10 months - Banked", "Certified Sick - Maintenance"] and
                              bool(record["isTimeOffAllowedAgainstThisTimeOffType"])],
      "Assignedtimeoffs": "|".join(timeoffname),
      "log_artifact": rail.result('log_artifact')
    }


def get_timeoff_combination(dag_run):

    return rail.find_first_by_attr_and_get_attr(TIMEOFF_TYPES_COMBINATION,
                                                "timeoff_types_combinaton", '|'.join(sorted(str(dag_run.conf['Assignedtimeoffs']).split('|'))), "export")


def get_policy_present(policy_list):
    if int(policy_list[-1]["effectiveDate"]["year"]) == int(datetime.now().strftime("%Y")):
        return False
    return True


def get_final_payload(policy_list, dag_run):

    starting_balance = list(filter(lambda i: i["script"] == "Set initial balance for the first day of a policy", map(lambda i: {
        "script": i["script"]["description"],
        "additionalparameters": i["additionalParameters"]
    }, policy_list[-1]["policySet"]["timeOffBalanceEventScripts"])))

    starting_balance_amount = starting_balance[int(len(
        starting_balance))-1]["additionalparameters"][int(len(starting_balance))-1]["value"]["number"]

    existing_balance_amount = {
        "keyUri": "urn:replicon:script-key:parameter:amount", "value": {"number": starting_balance_amount}}

    new_balance_amount = {"keyUri": "urn:replicon:script-key:parameter:amount", "value": {"number": float(
        dag_run.conf['personalleavebalance'][0] if dag_run.conf['personalleavebalance'] and dag_run.conf['personalleavebalance'][0] is not None else 0) +
        float(dag_run.conf['sickleaveannualbalance'][0] if dag_run.conf['sickleaveannualbalance'] and dag_run.conf['sickleaveannualbalance'][0] is not None else 0) +
        float(dag_run.conf['sickleavebankedbalance'][0] if dag_run.conf['sickleavebankedbalance'] and dag_run.conf['sickleavebankedbalance'][0] is not None else 0)}}

    final_data = {
        "effectiveDate": {
            "year": int(datetime.now().strftime("%Y")),
         			"month": 4,
         			"day": 1
        },
        "description": "Effective On "+str(datetime.now().strftime("%Y"))+"-04-01",
      		"policySet": json.loads(json.dumps(policy_list[-1]["policySet"]).replace(json.dumps(existing_balance_amount),
                                                                                                      json.dumps(new_balance_amount)))
    }

    initial_list = rail.result('get_user_timeoff_types')
    initial_list.append(final_data)

    return json.loads(json.dumps(initial_list, ensure_ascii=False).replace('null', '"effective"')
                      .replace('"script"', '"scriptTarget"').replace('\\"', '"').replace('}"}]', '}}]').replace('":"{', '":{'))
