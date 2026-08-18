from datetime import datetime
import time
from pendulum import now
import rail
null=None

def get_user_shifturi(useruri):
    return rail.find_first_by_attr_and_get_attr(
            rail.load_all_records(rail.result("query_enabled_users_with_shift_schedule")),
            "useruri", useruri, "shifturi")

def get_breakuri(useruri):
    shift_uri = get_user_shifturi(useruri)
    return rail.find_first_by_attr_and_get_attr(
        rail.result("get_bulk_shift_schedule_details"),
        "uri", shift_uri, "breakuri"
    )

def get_shift_details(response):
    shift_details = []
    for item in response:
        if item["shiftDetails"]["breakSegments"] and "displayText" in item["shiftDetails"]["breakSegments"][0]["breakType"]:
            shift_details.append({
                "shift": item["shiftDetails"]["name"],
                "uri": item["shiftUri"],
                "break": item["shiftDetails"]["breakSegments"][0]["breakType"]["displayText"],
                "breakuri":item["shiftDetails"]["breakSegments"][0]["breakType"]["uri"],
                "duration":item["shiftDetails"]["breakSegments"][0]["duration"]["hours"] * 60 + item["shiftDetails"]["breakSegments"][0]["duration"]["minutes"],
                "starthour":item["shiftDetails"]["breakSegments"][0]["inTime"]["hour"],
                "startmin":item["shiftDetails"]["breakSegments"][0]["inTime"]["minute"],
            })
        else:
            shift_details.append({
                "shift": item["shiftDetails"]["name"],
                "uri": item["shiftUri"],
                "break": "No Break",
                "breakuri": null,
                "duration": null,
                "starthour": null,
                "startmin": null
            })
    return shift_details

def get_success_log(dag_run):
    start_hour = dag_run.conf["starthour"]
    start_min = dag_run.conf["startmin"]
    end_time, end_hour, end_min = 0, 0, 0
    if start_hour:
        end_time = int(start_hour) * 60 + int(start_min)
        end_time = end_time + int(dag_run.conf["duration"])
        end_hour = end_time//60
        end_min = end_time%60

    start_meridian = time.strftime("%H:%M %p",time.strptime(str(start_hour) + ":" + str(start_min), "%H:%M"))
    end_meridian = time.strftime("%H:%M %p",time.strptime(str(end_hour) + ":" + str(end_min), "%H:%M"))
    msg = "Break Added -  (Break In:"+ str(start_hour) + ":" + str(start_min) + start_meridian + "Break Out:" + str(end_hour) + str(end_min) + end_meridian
    return {
                "Jobid":dag_run.conf["parentecid"],
                "User|date":rail.result("get_first_punch_record")["username"] + "|" +
                                    datetime.strftime(datetime.strptime(
                                    rail.result("get_first_punch_record")["datetime"], "%Y-%m-%d %H:%M:%S"), "%d/%m/%Y"),
                "Status":"Success",
                "Details":msg,
                "Childjobid": rail.render_template('{{ecid()}}')
            }

def get_current_date():
    curr_date = now(tz="US/Central")
    return {
        "year": curr_date.year,
        "month": curr_date.month,
        "day": curr_date.day
    }

def get_punchtime(dag_run, break_type="start"):
    start_hour = dag_run.conf["starthour"]
    start_min = dag_run.conf["startmin"]
    end_time = 0
    if start_hour:
        end_time = int(start_hour) * 60 + int(start_min)
        end_time = end_time + int(dag_run.conf["duration"])

    if break_type == "stop":
        if end_time != 0:
            return {
                "year": now().year,
                "month": now().month,
                "day": now().day,
                "hour": end_time // 60,
                "minute": end_time % 60,
                "second": "00",
                "timeZoneUri": null
            }
        return null

    return {
            "year": now().year,
            "month": now().month,
            "day": now().day,
            "hour": start_hour,
            "minute": start_min,
            "second": "00",
            "timeZoneUri": null
        }
