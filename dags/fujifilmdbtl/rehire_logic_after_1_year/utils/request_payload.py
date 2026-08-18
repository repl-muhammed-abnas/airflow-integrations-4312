from datetime import datetime
import pendulum
import json
from fujifilmdbtl.rehire_logic_after_1_year.mappers.fujifilmdbtl_timeoff_balance_mapper import fdt_timeoff_balance_mapper
import rail
from fujifilmdbtl.rehire_logic_after_1_year import config

null = None

def get_datetime_obj(date_str):
    return rail.parse_date(date_str, config.date_format)


def get_run_report_payload():
    return {
            "reportParameters": [
            {
            "filterValues": [
            {
            "reportFilterUri": rail.result('get_filter_uri'),
            "value": null
            },
            {
            "reportFilterUri": rail.result('get_filter_uri'),
            "value": rail.result('process_date_time')
            },
            {
            "reportFilterUri": rail.result('get_filter_uri'),
            "value": rail.result('process_date_time')
            }
        ],
            "outputFormatUri": "urn:replicon:report-output-format-option:csv",
            "reportUri": rail.result('get_report_details')['uri'],
        }
        ]
    }


#sub_child_dag
def get_tenure_method(dag_run):
    today1 = datetime.now()
    today = pendulum.now(tz=config.time_zone)
    adjusted_start_date_dict = rail.parse_date(dag_run.conf['adjustedstartdate'], config.date_format)
    adjusted_start_date = pendulum.datetime(
        adjusted_start_date_dict['year'],
        adjusted_start_date_dict['month'],
        adjusted_start_date_dict['day'],
        tz=config.time_zone
    )
    seconds_diff = (today-adjusted_start_date).total_seconds()
    years_diff = (seconds_diff/86400)/365
    return years_diff


def check_if_policy_is_present_method(dag_run):
    if rail.result('get_default_timeofftype_policy_schedule_for_user') and rail.result('get_default_timeofftype_policy_schedule_for_user')[0]["policySet"]:
        return True
    return False


def get_reset_balance_amount_policy_method(dag_run):
    yearly_reset_policy = rail.find_first_by_attr_and_get_attr(rail.result('get_default_timeofftype_policy_schedule_for_user')[0]['policySet']['timeOffBalanceEventScripts'], 'script.name', "Yearly Reset", "additionalParameters")
    current_reset_amount = rail.find_first_by_attr_and_get_attr(yearly_reset_policy, 'keyUri', "urn:replicon:script-key:parameter:reset-balance-amount", "value.number")
    
    rail.set_result(current_reset_amount, "default_reset_amount")
    if dag_run.conf["timeofftypename"].lower()=="sick leave" and dag_run.conf["ftpt"].lower()=="p":
        current_reset_amount = current_reset_amount/2
    reset_amount_json = {
        "keyUri":"urn:replicon:script-key:parameter:reset-balance-amount",
        "value":{
            "number": current_reset_amount
        }
    }
    return reset_amount_json 


def get_timeoff_balance_from_mapper_method(dag_run):
    month = datetime.strptime(dag_run.conf['startdate'],  config.date_format).strftime('%B')
    balance_amount =  next(
        (item["balance"] for item in fdt_timeoff_balance_mapper
        if item["type"].lower() == dag_run.conf["timeofftypename"].lower() and 
        item["monthofhire"].lower() == month[:3].lower() and
        item["ftpt"].lower() == dag_run.conf["ftpt"].lower()
        ), None)
    rail.set_result(balance_amount,"balance_amount_from_mapper")
    if balance_amount and rail.result('get_default_timeofftype_policy_schedule_for_user') and rail.result('get_default_timeofftype_policy_schedule_for_user')[0]['policySet']:
        return True
    else:
        return False 


def get_default_starting_balance_set_to_policy_method():
    starting_balance_policy = rail.find_first_by_attr_and_get_attr(rail.result("get_default_timeofftype_policy_schedule_for_user")[0]["policySet"]["timeOffBalanceEventScripts"], "script.name", "Starting Balance Set To", "additionalParameters")
    initial_balance_amount = rail.find_first_by_attr_and_get_attr(starting_balance_policy, "keyUri", "urn:replicon:script-key:parameter:amount", "value.number")
    policy_data = {
        "keyUri": "urn:replicon:script-key:parameter:amount",
        "value": {
            "number": initial_balance_amount  
        }
    }
    return policy_data


def get_updated_starting_balance_set_to_policy_method():
    policy_data = {
        "keyUri": "urn:replicon:script-key:parameter:amount",
        "value": {
            "number": rail.result("get_timeoff_balance_from_mapper", "balance_amount_from_mapper") 
        }
    }
    return policy_data


def create_old_policy_schedules_list_method(dag_run):
    old_policies = []
    today = pendulum.now(tz=config.time_zone).date()
    if dag_run.conf["policyset"]: 
        policyset_json = json.loads(dag_run.conf["policyset"])
        for item in policyset_json:
            effective_date = pendulum.date(item["effectiveDate"]["year"], item["effectiveDate"]["month"], item["effectiveDate"]["day"])
            if effective_date < today:
                old_policies.append(item)
    return old_policies


def create_count_of_last_policy_list_method():
    last_policy_data = []
    tenure_data = rail.result('get_tenure')
    for item in rail.result('get_default_policy_from_global_level'):
        if item['startOffset']['offsetValue'] < tenure_data:
            temp_data = {
                "count": item['startOffset']['offsetValue'],
                "diff": tenure_data - item['startOffset']['offsetValue'],
                "policy": item["policySet"]
            }
            last_policy_data.append(temp_data)
    return last_policy_data


def find_the_least_difference_method():
    items = rail.result('create_count_of_last_policy_list')['value']
    return min({item["diff"] for item in items}) if items else None


def create_count_of_new_policy_list_method():
    new_policy_list = []
    if rail.result("find_the_least_difference") is not None and rail.result("create_count_of_last_policy_list")['value'] is not None:
        for item in rail.result("create_count_of_last_policy_list")['value']:
            if item["diff"] == rail.result("find_the_least_difference"):
                temp = {
                    "count": item.get("count"),
                    "policy": item.get("policy")
                }
                new_policy_list.append(temp)
                break
    #update
    if rail.result('get_default_policy_from_global_level'):
        for item in rail.result('get_default_policy_from_global_level'):
            if item.get("startOffset").get("offsetValue") >= rail.result('get_tenure'):
                temp = {
                        "count":  item.get("startOffset").get("offsetValue"),
                        "policy": item.get("policySet")
                    }
                new_policy_list.append(temp)
    return new_policy_list



def create_new_policy_schedule_list_method(dag_run):
    new_policy = []
    if rail.result('create_count_of_new_policy_list'):
        result_data = rail.result('create_count_of_new_policy_list')
        items = result_data.get("value",[])
        for idx,item in enumerate(items):
            if idx==0:
                today = pendulum.now(tz=config.time_zone)
                policy_set = {
                    "effectiveDate": {
                        "day": today.day,
                        "month": today.month,
                        "year": today.year
                    },
                    "description": f"Effective on {today.strftime('%Y-%m-%d')}",
                    "policySet": item["policy"]
                }
                new_policy.append(policy_set)
            else:
                adjusted_start_date = pendulum.parse(dag_run.conf['adjustedstartdate'].strip(), strict=False)
                adjusted_start_date = adjusted_start_date.add(months=item["count"] * 12)
                policy_set = {
                    "effectiveDate": {
                        "day": adjusted_start_date.day,
                        "month": adjusted_start_date.month,
                        "year": adjusted_start_date.year
                    },
                    "description": f"Effective on {adjusted_start_date.strftime('%Y-%m-%d')}",
                    "policySet": item["policy"]
                }
                new_policy.append(policy_set)
    return new_policy



def update_policy_for_list_size_zero():
        last_policy_set = rail.result('get_default_policy_from_global_level')[-1]["policySet"]
        today = pendulum.now(tz=config.time_zone)
        policy_set = {
                    "effectiveDate": {
                        "day": today.day,
                        "month": today.month,
                        "year": today.year
                    },
                    "description": f"Effective on {today.strftime('%Y-%m-%d')}",
                    "policySet": last_policy_set
                }
        return policy_set    


def calculate_existing_TO_policies():
    existing_policy = None
    if rail.result('create_old_policy_schedules_list'):
        existing_policy = json.loads(json.dumps(rail.result('create_old_policy_schedules_list')['value'], ensure_ascii=False)
                                                .replace('null','"effective"')
                                                .replace('"script"', '"scriptTarget"'))
    return existing_policy


def calculate_new_TO_policies():
    new_policy = None
    if rail.result('create_new_policy_schedule_list'):
        new_policy = json.loads(json.dumps(rail.result('create_new_policy_schedule_list')['value'], ensure_ascii=False)
                                        .replace('null','"effective"')
                                        .replace('"script"', '"scriptTarget"')
                                        .replace(json.dumps(rail.result('get_default_starting_balance_set_to_policy')['value']), json.dumps(rail.result('get_updated_starting_balance_set_to_policy')['value']))
                                        .replace('[{"timeOffBalanceEventScripts', '{"timeOffBalanceEventScripts')
                                        .replace("}}]}]}]", "}}]}}]"))
    return new_policy


def calculate_new_policies(dag_run):
    new_policy = rail.result('get_new_time_off_policies')['value']
    if dag_run.conf['timeofftypename'].lower() == "sick leave":
        old_policy_to_replace_step14 = {
            "keyUri":"urn:replicon:script-key:parameter:reset-balance-amount",
            "value":{
                "number": rail.result('get_reset_balance_amount_policy','default_reset_amount')
            }
        }
        new_policy = json.loads(json.dumps(rail.result('get_new_time_off_policies')['value'], ensure_ascii=False)
                            .replace(json.dumps(old_policy_to_replace_step14), json.dumps(rail.result('get_reset_balance_amount_policy'))))
    return new_policy


def check_if_message_67_present():
    if rail.result('get_existing_time_off_policies'):
        return True
    return False


def calculate_create_policyschedules_list(dag_run):
    policies = []
    today = pendulum.now(tz=config.time_zone).date()
    if dag_run.conf["policyset"]: 
        policyset_json = json.loads(dag_run.conf["policyset"])
        for item in policyset_json:
            effective_date = pendulum.date(item["effectiveDate"]["year"], item["effectiveDate"]["month"], item["effectiveDate"]["day"])
            if effective_date < today:
                policies.append(item)
    return policies


def calculate_create_count_of_new_policy_list2():
    policies = []
    if rail.result('get_default_policy_from_global_level'):
        for item in rail.result('get_default_policy_from_global_level'):
            if item.get("startOffset").get("offsetValue") >= rail.result('get_tenure'):
                temp = {
                        "count":  item.get("startOffset").get("offsetValue"),
                        "policy": item.get("policySet")
                    }
                policies.append(temp)
    return policies


def calculate_last_policy_list2():
    policies = []
    if rail.result('get_default_policy_from_global_level'):
        for item in rail.result('get_default_policy_from_global_level'):
            if item.get("startOffset").get("offsetValue") < rail.result('get_tenure'):
                temp = {
                    "count":  item.get("startOffset").get("offsetValue"),
                    "diff": rail.result('get_tenure') - item.get("startOffset").get("offsetValue"),
                    "policy": item.get("policySet")
                }
                policies.append(temp)
    return policies



def find_least_difference2():
    items = rail.result('last_policy_list2')
    if items:
        diffs = [item["diff"] for item in items]
        unique_diffs = set(diffs)
        min_diff = min(unique_diffs)
    else:
        min_diff = None
    return min_diff


def if_diff_equals_least_difference2_method():
    for item in rail.result("last_policy_list2"):
        if item["diff"] == rail.result("find_the_least_difference2"):
            return True
    return False 


def calculate_update_count_of_new_policy2():
    for item in rail.result("last_policy_list2"):
        if item["diff"] == rail.result("find_the_least_difference2"):
            temp = {
                "count": item.get("count"),
                "policy": item.get("policy") 
            }
            return temp 
        


def calculate_update_create_policyschedules_list(dag_run):
    idx = rail.result('foreach_count_of_new_policy2', 'index')
    current_item = rail.result('foreach_count_of_new_policy2')
    today = pendulum.now(tz=config.time_zone)
    if idx==0:
        policy_set = {
            "effectiveDate": {
                "day": today.day,
                "month": today.month,
                "year": today.year
            },
            "description": f"Effective on {today.strftime('%Y-%m-%d')}",
            "policySet": current_item["policy"]
        }
        return policy_set
    else:
        adjusted_start_date = pendulum.parse(dag_run.conf['adjustedstartdate'].strip(), strict=False)
        adjusted_start_date = adjusted_start_date.add(months=current_item["count"] * 12)
        policy_set = {
            "effectiveDate": {
                "day": adjusted_start_date.day,
                "month": adjusted_start_date.month,
                "year": adjusted_start_date.year
            },
            "description": f"Effective on {adjusted_start_date.strftime('%Y-%m-%d')}",
            "policySet": current_item["policy"]
        }
        return policy_set
    

def calculate_update_if_policyschedulelist_size_equals_0():
    last_policy_set =  rail.result('get_default_policy_from_global_level')[-1]["policySet"]
    today = pendulum.now(tz=config.time_zone)
    policy_set = {
                "effectiveDate": {
                    "day": today.day,
                    "month": today.month,
                    "year": today.year
                },
                "description": f"Effective on {today.strftime('%Y-%m-%d')}",
                "policySet": last_policy_set
            }
    return policy_set


def calculate_update_policy_schedule_payload_for_putusertimeoff():
    policySchedules = json.loads(json.dumps(rail.result('get_policyschedules2'), ensure_ascii=False)
                                    .replace('null','"effective"')
                                    .replace('"script"', '"scriptTarget"')
                                    .replace('[{"timeOffBalanceEventScripts', '{"timeOffBalanceEventScripts')
                                    .replace("}}]}]}]", "}}]}}]"))
    return policySchedules
        

