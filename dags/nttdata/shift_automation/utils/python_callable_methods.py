from datetime import datetime, timedelta
import json
import rail
from dateutil.relativedelta import relativedelta
from nttdata.shift_automation.mapper.shift_schedule_mapper import nttdata_shift_schedule_mapper

null = None

def get_schedule_assignment_list(item):
    schedule_assignment_list = list(map(lambda x: x, filter(lambda data: data["scheduleassignment"] == "yes" , nttdata_shift_schedule_mapper)))
    return list(map(lambda x: x, filter(lambda data: data["country"]=="PAN", schedule_assignment_list))) if item["country"] == "PAN" \
                else list(map(lambda x: x["schedule"], filter(lambda data: data["country"]==item["country"], schedule_assignment_list)))[0]

def get_date(data):
    return datetime.strptime(str(data["date"]["day"]) + "/" + str(data["date"]["month"]) + "/" + str(data["date"]["year"]), "%d/%m/%Y")

def get_assigned_shift_list():
    shift_data = rail.result("get_shift_schedule_summary_foruser")
    return json.dumps(list(map(lambda data: {
        "date": get_date(data).strftime("%d/%m/%Y"),
        "week": get_date(data).isocalendar().week + 1 if get_date(data).weekday() == 6 else get_date(data).isocalendar().week,
        "shift": data["shift"]["displayText"]
    }, shift_data)))

def get_dates_in_month(day_num, dag_run):
    return datetime.strptime(dag_run.conf["startdate"], "%Y-%m-%d") + relativedelta(days=day_num-1)

def get_dates_to_consider_list(dag_run):
    dates_list = []
    startdate = datetime.strptime(dag_run.conf["startdate"], "%Y-%m-%d")
    enddate = datetime.strptime(dag_run.conf["enddate"], "%Y-%m-%d") + timedelta(days=2)

    dates = int(enddate.timestamp() - startdate.timestamp())/86400
    for i in range(1, int(dates)):
        dates_list.append(i)

    return json.dumps(list(map(lambda day_num: {
        "seq": day_num,
        "date": get_dates_in_month(day_num, dag_run).strftime("%Y-%m-%d"),
        "day": (get_dates_in_month(day_num, dag_run)).weekday(),
        "dateday": get_dates_in_month(day_num, dag_run).strftime("%d"),
        "datemonth": get_dates_in_month(day_num, dag_run).strftime("%m"),
        "dateyear": get_dates_in_month(day_num, dag_run).strftime("%Y"),
        "week": get_dates_in_month(day_num, dag_run).isocalendar().week,
    }, dates_list)))

def get_schedule_name(dag_run, day):
    shchedule_name = list(map(lambda data: data["schedule"],
            filter(lambda data: data["default"] == "additional", dag_run.conf["shiftname"]))) if day == '5' else \
                list(map(lambda data: data["schedule"], filter(lambda data: data["default"] == "default", dag_run.conf["shiftname"])))
    return shchedule_name[0]

def get_shift_assignment_list(dag_run):
    return list(map(lambda shift_data: {
        "date": {
            "year": shift_data["dateyear"],
            "month": shift_data["datemonth"],
            "day": shift_data["dateday"]
        },
        "target": {
            "uri": null,
        },
        "shift": {
            "uri": null,
            "name": get_schedule_name(dag_run, shift_data["day"]) if dag_run.conf["country"] == "PAN" else dag_run.conf["shiftname"]
        },
        "user": {
            "uri": dag_run.conf["useruri"],
            "loginName": null,
            "parameterCorrelationId": null
        },
        "startTime": null,
        "endTime": null,
        "publishState": "urn:replicon:shift-assignment-publish-state:published",
        "note": "Published by shift automation"
    }, rail.load_all_records(rail.result("working_days_pan_list")) if dag_run.conf["country"] == "PAN"\
        else rail.load_all_records(rail.result("working_days_other_country_list")) ))
