import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
import rail

def create_users_list(response):
    current_year = (int(datetime.strftime(datetime.today()+relativedelta(days=-9), "%Y"))) + 1
    filter_users_without_start_date = list(filter(lambda user: str(
        user['cells'][2]["textValue"]) != "", response['rows']))
    return json.dumps(list(map(lambda user: {
            "name": (user['cells'][0]["textValue"]).replace(",", ""),
            "status":str(user['cells'][1]["boolValue"]),
            "uri": user['cells'][0]["uri"],
            "startdate": str(user['cells'][2]["textValue"]),
            "tenure": str( current_year - (int(datetime.strptime(user['cells'][2]["textValue"], "%Y/%m/%d").strftime("%Y")) + 3)),
            "fiveyearmultiplier":
              str((current_year - (int(datetime.strptime(user['cells'][2]["textValue"], "%Y/%m/%d").strftime("%Y")) + 3)) % 5)
            }, filter_users_without_start_date)))

def get_policy(offset, policy):
    duration = {2:12, 3:24}
    temp = {}
    temp["description"] = f'Added by yearly run on {datetime.strftime(datetime.today()+relativedelta(days=-9), "%d/%m/%Y")}'
    temp["effectiveDate"] = {"day": 1, "month": 1, "year":int(datetime.strftime(
        datetime.today()+relativedelta(days=-9)+relativedelta(months=+duration[offset]), "%Y"))}
    temp.update(json.loads(str(json.dumps(policy)).replace("\"script\"", "\"scriptTarget\"")))
    return temp

def get_time_off_policies(dag_run):

    timeoffpoliciesschedule = []
    user_policies = rail.result('get_user_timeoff_type_policy')
    policy_set = {}

    if "policiesByTimeOffType" in user_policies:
        policy = rail.find_first_by_attr_and_get_attr(user_policies["policiesByTimeOffType"], "timeOffType.uri",dag_run.conf["timeoffuri"],"")
        policy_set = json.loads(str(json.dumps(policy)).replace("\"script\"", "\"scriptTarget\""))

    if 'policySetSchedule' in policy_set:
        for schedule in policy_set['policySetSchedule']:
            schedule_date = str(schedule["effectiveDate"]["day"])+"/"+str(schedule["effectiveDate"]["month"]) + "/" + str(schedule["effectiveDate"]["year"])
            if datetime.strptime(schedule_date, "%d/%m/%Y") < datetime.strptime((datetime.strftime(
                datetime.today()+relativedelta(days=-9)+relativedelta(months=+12),"01/01/%Y")), "%d/%m/%Y"):
                timeoffpoliciesschedule.append(schedule)

    timeoff_type_policies = rail.result("get_default_timeoff_policy_for_timeoff_type")

    off_set_2_policy = rail.find_first_by_attr_and_get_attr(timeoff_type_policies,"startOffset.offsetValue", 2)
    if off_set_2_policy:
        timeoffpoliciesschedule.append(get_policy(offset=2, policy=off_set_2_policy))

    off_set_3_policy = rail.find_first_by_attr_and_get_attr(timeoff_type_policies,"startOffset.offsetValue", 3)
    if off_set_3_policy:
        timeoffpoliciesschedule.append(get_policy(offset=3, policy=off_set_3_policy))
    return timeoffpoliciesschedule
