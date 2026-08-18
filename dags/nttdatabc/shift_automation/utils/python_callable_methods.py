import itertools
import json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import rail
import pendulum

null = None

def get_shift_data(data):
    shift_date = datetime.strptime(str(data["date"]["day"]) + "/" + str(data["date"]["month"]) + "/" + str(data["date"]["year"]), "%d/%m/%Y")
    return {
        "date": shift_date.strftime("%Y-%m-%d"),
        "dateday": shift_date.strftime("%d"),
        "month": shift_date.strftime("%m"),
        "year": shift_date.strftime("%Y"),
        "week": shift_date.isocalendar().week,
        "weekday": shift_date.weekday(),
        "shift": data["shift"]["displayText"]
    }

def get_assigned_shift_list():
    shift_data = rail.result("get_shift_schedule_summary_foruser")
    return json.dumps(list(map(get_shift_data, shift_data)))

def get_dates_in_month(day_num, start_date):
    return {
        "seq": day_num,
        "date": start_date.strftime("%Y-%m-%d"),
        "day": start_date.weekday(),
        "dateday": start_date.strftime("%d"),
        "datemonth": start_date.strftime("%m"),
        "dateyear": start_date.strftime("%Y"),
        "week": start_date.isocalendar().week,
    }

def get_dates_list(dates):
    dates_list = []
    for i in range(1, int(dates)):
        dates_list.append(i)
    return dates_list

def get_reference_month_last_full_week(dag_run):
    startdate = (datetime.strptime(dag_run.conf["startdate"], "%Y-%m-%d"))+relativedelta(day=1)
    enddate = (datetime.strptime(dag_run.conf["enddate"], "%Y-%m-%d")) - relativedelta(months=1, day=31) + timedelta(days=2) \
            if dag_run.conf["usertype"] == "Existing User" \
                    else (datetime.strptime(dag_run.conf["enddate"], "%Y-%m-%d")) - relativedelta(months=13, day=31) + timedelta(days=2)
    dates = int(enddate.timestamp() - startdate.timestamp())/86400
    reference_month_last_full_week = []
    dates_in_month = list(map(lambda day_num: get_dates_in_month(day_num, startdate + relativedelta(days=day_num-1)), get_dates_list(dates)))
    for dates in reversed(dates_in_month):
        if dates["day"] == 5:
            index = dates_in_month.index(dates)
            for i in range(index-6, index+1):
                reference_month_last_full_week.append(dates_in_month[i])
            break
    return reference_month_last_full_week

def get_last_full_week_shifts(dag_run):
    shift_dates_collection = list(map(lambda shift_data: shift_data, filter(lambda data: data["month"] == dag_run.conf["startdatemonth"]
            and data["year"] == dag_run.conf["startdateyear"], rail.load_all_records(rail.result("create_assigned_shift_dates_collection")))))
    return json.dumps(list(itertools.chain(*list(map(lambda full_week: list(map(lambda shift_data: shift_data,
                filter(lambda shift_data: shift_data["date"] == full_week["date"], shift_dates_collection))), get_reference_month_last_full_week(dag_run))))))

def get_dates_to_consider_list(dag_run):
    startdate = (datetime.strptime(dag_run.conf["startdate"], "%Y-%m-%d"))+relativedelta(months=+1, day=1)
    enddate = datetime.strptime(dag_run.conf["enddate"], "%Y-%m-%d") + timedelta(days=2)
    dates = int(enddate.timestamp() - startdate.timestamp())/86400

    return json.dumps(list(map(lambda day_num: get_dates_in_month(day_num, datetime.strptime(dag_run.conf["startdate"], "%Y-%m-%d")
                            + relativedelta(months=+1, days=day_num-1)), get_dates_list(dates))))

def get_schedule_name(day, last_full_week_list):
    shchedule_name = list(map(lambda data: data["shift"], \
            filter(lambda data: data["weekday"] == day, last_full_week_list)))
    return shchedule_name[0]

def get_shift_assignment_list(dag_run):
    last_full_week_list = rail.load_all_records(rail.result("get_last_full_week_shifts"))
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
            "name": get_schedule_name(shift_data["day"], last_full_week_list)
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
    }, rail.load_all_records(rail.result("working_days_list"))))

def get_filename():
    return 'NTTDATABC_Shift_Automation_' + pendulum.now().strftime("%m%d%YT%H%M%S") + '_Log.csv'
