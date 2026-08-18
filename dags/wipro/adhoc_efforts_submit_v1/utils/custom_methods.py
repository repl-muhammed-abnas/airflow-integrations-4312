from datetime import datetime
import rail
# for trial instance
from wipro.efforts_submit_v1.country_mapper.ot_code_country_mapper import ot_code_country_list_trial
# for prod instnace
from wipro.efforts_submit_v1.country_mapper.ot_code_country_mapper import ot_code_country_list_prod
DEFAULT_PAYLOAD = {
            "Empid": "",
            "Subty": "",
            "Workdate": "",
            "Projid": "",
            "Hours": ""
        }


def format_timesheet_period_date(date_str):
    return datetime.strftime(datetime.strptime(date_str, "%Y/%m/%d"), "%Y-%m-%d")

def get_it_training_efforts():
    training_entry = rail.load_all_records(
        rail.result("query_it_training_projects"))

    return list(map(lambda data:
                    {
                        "Kkost": "0.00",
                        "Nhours": data["hours_current"],
                        "Endda": format_date(data['entry_date']),
                        "Begda": format_date(data['entry_date']),
                        "Objid": "00000000",
                        "Pernr": data["employee_id"],
                        "Ndays": "0"
                    }, training_entry))


def get_non_proj_efforts():
    non_proj_entry = rail.load_all_records(rail.result("query_it_non_proj"))

    return list(map(lambda data:
                    {
                        "TaskId": data['task_code'],
                        "TaskDesc": data['task_name'],
                        "Pernr": data["employee_id"],
                        "Efforts": data['hours_current']
                    }, non_proj_entry))

def get_in_out_time(time):
    if not time:
        return ""
    return f"{time}:00"

def get_oncall_projects():
    distinct_projects = rail.load_all_records(rail.result('query_distinct_oef_oncall'))
    proj_entry_data = {}

    proj_entry_data["IT_ONCALL1"] = list(map(lambda data: {
            "Empid": data['employee_id'],
            "Subty": "01",
             "Workdate": format_date(data["entry_date"]),
            "Projid": data["project_code"],
            "Hours": data["hours_current"],
        }, distinct_projects)
    )
    return proj_entry_data

def get_callout_projects():
    distinct_projects = rail.load_all_records(rail.result('query_distinct_oef_callout'))
    proj_entry_data = {}

    proj_entry_data["IT_CALLOUT1"] = list(map(lambda data: {
            "Empid": data['employee_id'],
            "Subty": "02",
            "Workdate": format_date(data["entry_date"]),
            "Projid": data["project_code"],
            "Hours": data["hours_current"],
        }, distinct_projects)
    )
    return proj_entry_data

def get_ot_code_mapper(instance):
    return ot_code_country_list_trial if instance in ['trial', 'uat'] else ot_code_country_list_prod


def get_country_key_as_per_mapper(country: str):
    return country.replace("_", " ")

def get_overtime_projects(dag_run, instance):
    ot_code_mapper = get_ot_code_mapper(instance)
    country = get_country_key_as_per_mapper(dag_run.conf.get('cntry', ''))
    distinct_projects = rail.load_all_records(rail.result('query_oef_overtime'))
    proj_entry_data = {
    }

    proj_entry_data["IT_OVERTIME1"] = list(map(lambda data: {
            "Empid": data['employee_id'],
            "Subty": ot_code_mapper.get(country, ''),
            "Workdate": format_date(data["entry_date"]),
            "Projid": data["project_code"],
            "Hours": data["hours_current"]
        }, distinct_projects)
    )

    return proj_entry_data

def get_it_proj_efforts():
    proj_task_list = rail.load_all_records(rail.result("query_per_project"))

    proj_entry_data = {
        "Pernr": proj_task_list[0]["employee_id"],
        "Projectefforts": str(sum(list(map(lambda i: float(i["hours_current"]), proj_task_list)))),
        "Projectname": proj_task_list[0]["project_code"]
    }

    proj_entry_data["IT_TASKS"] = list(map(lambda data:
        {
            "StartDate": data["entry_date"],
            "Employee": data["employee_id"],
            "TaskId": data["task_code"],
            "TaskDescription": data["task_name"],
            "Wbselement": data["project_code"],
            "Comments": data["comments"],
            "Efforts": data["hours_current"],
            "EndDate": data["entry_date"]
        }, proj_task_list
        )
    )

    return proj_entry_data


def format_date(date_str):
    return datetime.strftime(datetime.strptime(date_str, "%Y%m%d"), "%Y-%m-%d")

def get_work_location(country_code):
    location = {
        "Loc1": "",
        "Loc2": ""
    }
    location_ksa_data = rail.load_all_records(rail.result("unique_work_location_ksa"))
    work_location_data = rail.load_all_records(rail.result("unique_work_location"))
    if country_code in ["SA"]:
        if location_ksa_data and len(location_ksa_data) == 1:
            location["Loc1"] = location_ksa_data[0]["work_location_ksa"]
        elif len(location_ksa_data) >= 2:
            location["Loc1"], location["Loc2"] = location_ksa_data[0]["work_location_ksa"], location_ksa_data[1]["work_location_ksa"]
    if country_code in ["PT", "NL", "RO", "PL"]:
        if work_location_data and len(work_location_data) == 1:
            location["Loc1"] = work_location_data[0]["work_location"]
        elif len(work_location_data) >= 2:
            location["Loc1"], location["Loc2"] = work_location_data[0]["work_location"], work_location_data[1]["work_location"]
    return location

def map_time_data_per_day():
    it_proj_data = []
    it_nonproj_data = []
    it_training_data = []

    it_training_data = rail.result("map_it_training_projects") if rail.result("map_it_training_projects") else []
    it_nonproj_data = rail.result("map_it_non_proj") if rail.result("map_it_non_proj") else []
    if rail.result("map_it_project") and "value" in rail.result("map_it_project"):
        it_proj_data = list(filter(None,rail.result("map_it_project").get("value")))
    else:
        it_proj_data = []

    if it_proj_data or it_nonproj_data or it_training_data:
        other_data = rail.load_all_records(
            rail.result("create_collection_per_day"))[0]
        entry_date = format_date(other_data["entry_date"])
        country_code = other_data["country_code"]
        employee_id = other_data["employee_id"]
        data = {
            "d": {
                "Begda": entry_date,
                "Endda": entry_date,
                "Pernr": employee_id,
                "IT_PROJ_EFFORTS": [{
                    "IT_PROJ_DETAILS": it_proj_data,
                    "IT_TRAINING": it_training_data,
                    "IT_OVERTIME": [],
                    "Pernr": employee_id,
                    "MODIF_FLAG": "Y",
                    "Country": country_code,
                    "Workdate": entry_date,
                    "IT_NONPROJ": it_nonproj_data,
                    "Location": "",
                    **get_work_location(country_code)
                }
                ],
                "Retmessage": ""
            }
        }
        return data
    return None

def map_ot_oncall_callout_time_data_per_day():
    oncall_data = rail.result('map_oncall_projects') if rail.result('map_oncall_projects') else {}
    callout_data = rail.result('map_callout_projects') if rail.result('map_callout_projects') else {}
    overtime_data = rail.result('map_overtime_projects') if rail.result('map_overtime_projects') else {}
    data = {**oncall_data, **callout_data, **overtime_data}
    return data

def get_oef_oncall_query(country, query_mapper_for_contry):
    oncall = query_mapper_for_contry[country].get('oncall')
    oncall = oncall if oncall else {}
    condition = [f"{key} IN {val}" for key, val in oncall.items()]
    condition = " OR ".join(condition)
    return condition

def get_oef_callout_query(country, query_mapper_for_contry):
    callout = query_mapper_for_contry[country].get('callout')
    callout = callout if callout else {}
    condition = [f"{key} IN {val}" for key, val in callout.items()]
    condition = " OR ".join(condition)
    return condition

def get_oef_overtime_query(country, query_mapper_for_contry):
    overtime = query_mapper_for_contry[country].get('overtime')
    overtime = overtime if overtime else {}
    condition = [f"{key} IN {val}" for key, val in overtime.items()]
    condition = " OR ".join(condition)
    return condition

def get_oef_distinct_columns_query(country, query_mapper_for_contry):
    oncall = query_mapper_for_contry[country].get('oncall')
    callout = query_mapper_for_contry[country].get('callout')
    overtime = query_mapper_for_contry[country].get('overtime')
    oncall = oncall if oncall else {}
    callout = callout if callout else {}
    overtime = overtime if overtime else {}
    condition = {
        "oncall": ",".join(oncall.keys()),
        "callout": ",".join(callout.keys()),
        "overtime": ",".join(overtime.keys())
    }
    return condition

def get_distint_oef_query(country, collection, query_mapper_for_contry):
    dropdown = query_mapper_for_contry[country]['dropdown']
    dropdown = ",".join(dropdown)
    query = f"{dropdown},entry_date FROM {collection}"
    return query
