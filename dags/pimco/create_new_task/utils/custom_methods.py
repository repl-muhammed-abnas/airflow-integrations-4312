from datetime import datetime, timedelta
import rail
import pendulum

null = None


def logging_details(time_zone):
    return {
        "dag_run_start_time": pendulum.now(time_zone).isoformat(),
        "jobdateformatted": str((pendulum.now(time_zone)).strftime("%m_%d_%Y")),
        "time_zone": time_zone
    }


def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf


def read_collection(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        data_collection = list(reader)
    return data_collection


def get_prev_day(time_zone):
    prev_date = pendulum.now(time_zone) - timedelta(days=1)
    return str(prev_date.strftime("%b %e, %Y")) if len(str(prev_date.strftime("%e")).replace(" ", "")) == 2 else str(prev_date.strftime("%b%e, %Y"))


def append_task_level(item):
    if not item:
        return []
    return {
        'tasknamefullpath': item['tasknamefullpath'],
        'taskdescription': item['taskdescription'],
        'taskstatus': item['taskstatus'],
        'created': item['created'],
        'taskcode': item['taskcode'],
        'marketrate': item['marketrate'],
        'orgcosts': item['orgcosts'],
        'luxflag': item['luxflag'],
        'taskuri': item['taskuri'],
        'taskstartdate': item['taskstartdate'],
        'taskenddate': item['taskenddate'],
        'taskname': item['taskname'],
        'entrytype': item['entrytype'],
        'costtype': item['costtype'],
        'esthrs': item['esthrs'],
        'tasklevel': len(item['tasknamefullpath'].split(" / "))
    }


def get_start_date(part, start_date):
    date_str = str(start_date).replace(" ", "")
    if part == "day":
        return datetime.strptime(date_str, "%b%d,%Y").strftime("%e") if date_str is not null and date_str != '' else null
    if part == "month":
        return datetime.strptime(date_str, "%b%d,%Y").strftime("%m") if date_str is not null and date_str != '' else null
    if part == "year":
        return datetime.strptime(date_str, "%b%d,%Y").strftime("%Y") if date_str is not null and date_str != '' else null
    return null


def get_end_date(part, end_date):
    date_str = str(end_date).replace(" ", "")
    if part == "day":
        return datetime.strptime(date_str, "%b%d,%Y").strftime("%e") if date_str is not null and date_str != '' else null
    if part == "month":
        return datetime.strptime(date_str, "%b%d,%Y").strftime("%m") if date_str is not null and date_str != '' else null
    if part == "year":
        return datetime.strptime(date_str, "%b%d,%Y").strftime("%Y") if date_str is not null and date_str != '' else null
    return null


def get_org_cost_uris():
    for row_data in get_dag_run_conf()["custom_fields"]["rows"]:
        for cell_data in row_data["cells"]:
            if cell_data["textValue"] == "Org Costs":
                return cell_data["uri"]
    return null


def get_lux_flag_uris():
    for row_data in get_dag_run_conf()["custom_fields"]["rows"]:
        for cell_data in row_data["cells"]:
            if cell_data["textValue"] == "Lux Flag":
                return cell_data["uri"]
    return null

def get_project_task_data(dag_run):
    task_details = read_collection(dag_run.conf["tasks"])
    return {
        "project_name": dag_run.conf["project_name"],
        "project_uri": dag_run.conf["project_uri"],
        "task_details": [{
            "project_task_fullpath": dag_run.conf['project_name']+"-"+task_record["tasknamefullpath"],
            "parent_task": (str(task_record["tasknamefullpath"]).replace(" / "+task_record["taskname"], "")).rsplit(" / ", maxsplit=1)[-1] \
                if "/" in str(task_record["tasknamefullpath"]) else null,
            "parent_task_uri": null,
            "task_name_full_path": task_record["tasknamefullpath"],
            "task_description": task_record['taskdescription'],
            "task_status": task_record['taskstatus'],
            "created": task_record['created'],
            "task_code": task_record['taskcode'],
            "entry_type": task_record['entrytype'],
            "market_rate": task_record['marketrate'],
            "currency_uri": rail.find_first_by_attr_and_get_attr(get_dag_run_conf()['get_all_currencies'], 'displayText', 'USD$', 'uri'),
            "org_costs": task_record['orgcosts'],
            "lux_flag": task_record['luxflag'],
            "task_uri": task_record['taskuri'],
            "start_date": task_record['taskstartdate'],
            "end_date": task_record['taskenddate'],
            "start_date_day": get_start_date("day", task_record['taskstartdate']),
            "start_date_month": get_start_date("month", task_record['taskstartdate']),
            "start_date_year": get_start_date("year", task_record['taskstartdate']),
            "end_date_day": get_end_date("day", task_record['taskenddate']),
            "end_date_month": get_end_date("month", task_record['taskenddate']),
            "end_date_year": get_end_date("year", task_record['taskenddate']),
            "task_name": task_record['taskname'],
            "resource_assignment": rail.find_first_by_attr_and_get_attr(
                get_dag_run_conf()["resource_assignments"], 'task_uri', task_record['taskuri'], 'resource_uri'),
            "org_cost_uri":get_org_cost_uris(),
            "lux_flag_uri":get_lux_flag_uris(),
            "cost_type": task_record['costtype'],
            "estimated_hours": task_record['esthrs'],
            "is_time_entry_allowed": rail.find_first_by_attr_and_get_attr(
                get_dag_run_conf()["model_task_details"], 'uri', task_record['taskuri'], 'isTimeEntryAllowed')
        } for task_record in task_details]
    }
