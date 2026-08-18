from datetime import datetime
import uuid
import json
import rail

null = None
DATE_FORMAT = "%d.%m.%Y"

def to_datetime(date, date_format=DATE_FORMAT):
    if isinstance(date, dict):
        return datetime(day=date['day'], month=date['month'], year=date['year'])
    elif isinstance(date, str):
        return datetime.strptime(date, date_format)
    return date

def get_activity_type_details_for_update(dag_run):
     if dag_run.conf['activity_type'] and dag_run.conf['effective_date_for_activity_type']\
        and (not (rail.result('get_effective_user_groupmembership') and rail.result(
        'get_effective_user_groupmembership')['divisions'] and rail.result(
        'get_effective_user_groupmembership')['divisions'][0]['division']) or (
        rail.result(
            'get_effective_user_groupmembership')['divisions'][0]['division']['division']['displayText'] != dag_run.conf['activity_type'])):
        return [
                    {
                        "dateRange": {
                        "startDate": rail.get_replicon_date(datetime.strptime(dag_run.conf["effective_date_for_activity_type"], "%d.%m.%Y")),
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                        },
                        "item": {
                        "uri": null,
                        "parentUri": null,
                        "name": dag_run.conf["activity_type"]
                        }
                    }
                ]
     return []

def get_current_value_from_schedule_list_for_user(user_schedule, scrpit_name, required_key,  dag_run):
    current_value = null
    initial_value = null
    current_min_day_diff = "*"
    if 'urn' in json.dumps(user_schedule):
        for item in user_schedule:

            if not item['effectiveDate']:
                initial_value = item
                continue

            daydiff = (datetime.strptime(dag_run.conf['integration_run_date'], DATE_FORMAT).date()) - to_datetime(
                item['effectiveDate'], DATE_FORMAT).date()

            # ignore the future ones
            if daydiff.days < 0:
                continue

            if current_min_day_diff == "*":
                current_value = item
                current_min_day_diff = daydiff
                continue

            if current_min_day_diff > daydiff:
                current_min_day_diff = daydiff
                current_value = item

    return current_value[scrpit_name][required_key] if current_value else (initial_value[scrpit_name][required_key] if initial_value else '')

def get_hourly_rate_details_for_update(dag_run):
    if dag_run.conf['cost_rate'] and dag_run.conf['effective_date_for_cost_rate']:
        current_hourly_cost_for_user = get_current_value_from_schedule_list_for_user(
            rail.result("get_user_details")['costRateSchedule'], 'hourlyRate', 'amount', dag_run)
        new_hourly_cost_for_user = dag_run.conf["cost_rate_amount"]
        if new_hourly_cost_for_user and (not (current_hourly_cost_for_user) or (float(current_hourly_cost_for_user) != float(new_hourly_cost_for_user))):
            return  [
                {
                    "dateRange": {
                    "startDate":rail.get_replicon_date(datetime.strptime(dag_run.conf["effective_date_for_cost_rate"], "%d.%m.%Y")),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                    "hourlyRate": {
                        "amount": dag_run.conf["cost_rate_amount"],
                        "currency": {
                        "uri": null,
                        "name": null,
                        "symbol": dag_run.conf["currency_symbol"]
                        }
                    }
                    }
                }
                ]
    return []

def get_apply_user_modifications_payload(dag_run):
    exception_msg=''
    
    if not dag_run.conf['activity_type'] or not dag_run.conf['effective_date_for_activity_type']:
        if get_hourly_rate_details_for_update(dag_run):
            exception_msg = "Cost Rate Updated Successfully; Activity type not updated since Activity Type/Effective Date not present in feed file"
        else:
            exception_msg = "Activity type not updated since Activity Type/Effective Date not present in feed file; No Update Required for Cost Rate since same cost rate already assinged"   
    if not dag_run.conf['cost_rate'] or not dag_run.conf['effective_date_for_cost_rate']:
        if get_activity_type_details_for_update(dag_run):
            exception_msg = "Activity Type Updated Successfully; Cost rate not updated since Cost Rate/Effective Date not present in feed file"
        else:
            exception_msg = "Cost rate not updated since Cost Rate/Effective Date not present in feed file; No Update Required for Activity Type sincesame activity type already assinged"
    if not exception_msg:
        if not get_hourly_rate_details_for_update(dag_run) and not get_activity_type_details_for_update(dag_run):
            exception_msg = "No Update Required for Activity Type and Cost Rate since same activity type and cost rate already assigned"
        if not get_hourly_rate_details_for_update(dag_run) and get_activity_type_details_for_update(dag_run):
            exception_msg = "Activity Type Updated Successfully; No Update Required for Cost Rate since same cost rate already assigned"
        if not get_activity_type_details_for_update(dag_run) and get_hourly_rate_details_for_update(dag_run):
            exception_msg = "Cost Rate Updated Successfully; No Update Required for Activity Type since same activity type already assigned"

    rail.set_result(key='exception_msg', val=exception_msg)

    return{
        "target": {
            "employeeId": dag_run.conf["employee_id"],
        },
        "template": null,
        "modifications": {
            "divisionSchedule": get_activity_type_details_for_update(dag_run),
            "hourlyRatesSchedule": get_hourly_rate_details_for_update(dag_run),
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
        }



