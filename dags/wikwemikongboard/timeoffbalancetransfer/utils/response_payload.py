
def filter_timeoff_types(response):
    return list(filter(lambda i: (str(i["name"]).lower().startswith("personal leave") or
                                   str(i["name"]).lower().startswith("sick leave") or
                                     str(i["name"]).lower().startswith("certified sick") or
                                       str(i["name"]).lower().startswith("uncertified sick")), map(lambda i: {
                "name": i["timeOffType"]["name"],
                "enabled": i["isTimeOffAllowedAgainstThisTimeOffType"],
                "uri": i["timeOffType"]["uri"],
                "policy": i["policySetSchedule"],
                }, response["policiesByTimeOffType"])))


def get_template(response):
    return list(map(lambda details: {
        "timeofftemplate": details["timeOffTemplate"]["displayText"] if details.get("timeOffTemplate") and details["timeOffTemplate"].get("displayText") else None
    }, response))


def filter_timeoff_types_sick(response, dag_run):
    policy = list(filter(lambda i: i["uri"] == dag_run.conf["sickleavebankeduri"][0] if dag_run.conf["sickleavebankeduri"] is not None else " ", map(lambda i: {
        "name": i["timeOffType"]["name"],
        "enabled": i["isTimeOffAllowedAgainstThisTimeOffType"],
        "uri": i["timeOffType"]["uri"],
        "policy": i["policySetSchedule"]
    }, response["policiesByTimeOffType"])))

    return list(map(lambda i: {
                "effectiveDate": {
                    "year": i["effectiveDate"]["year"],
                   	"month": i["effectiveDate"]["month"],
                   	"day": i["effectiveDate"]["day"]
                },
                "description": i["description"],
                "policySet": i["policySet"]
                }, policy[0]["policy"]))
