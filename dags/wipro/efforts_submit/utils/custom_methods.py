from datetime import datetime
import rail


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
                                           ))

    return proj_entry_data


def format_date(date_str):
    return datetime.strftime(datetime.strptime(date_str, "%Y%m%d"), "%Y-%m-%d")


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
                    "Location": ""
                }
                ],
                "Retmessage": ""
            }
        }
        return data
    return None
