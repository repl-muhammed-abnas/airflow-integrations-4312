from datetime import datetime
from functools import lru_cache
import rail
from airflow.exceptions import AirflowFailException

def retrieve_export_uri(response):
    if response["error"] is not None:
        raise AirflowFailException("Export failed - " + response)
    return response["timeDataExportUri"]

def get_timeexport_fileformat(file_format, response):
    file_format = rail.find_first_by_attr_and_get_attr(
        response, "displayText", file_format, "uri"
    )
    if file_format:
        return file_format
    raise Exception(f"Unable to locate script `{file_format}`")

@lru_cache(maxsize=32)
def get_entry_date(date):
    return datetime.strftime(datetime.strptime(date, "%d/%m/%Y"), "%Y-%m-%d")

def get_peoplesoft_export_rows(item, mapper):
    if not item:
        return []
    time_entry_id = (
        item["short_time_entry_id"]
        if not item["timeoff_type"]
        else item["timeoff_booking_id"]
    )
    project_code = item["project_code"]
    task_name = item["task_name"]
    pay_type = item["pay_type"]
    if item["timeoff_type"] and mapper.get(item["timeoff_type"]):
        if item["fmla"] == "Yes":
            project_code = mapper[item["timeoff_type"]]["fmla_ps_project_code"]
            task_name = mapper[item["timeoff_type"]]["fmla_ps_task_code"]
        else:
            project_code = mapper[item["timeoff_type"]]["ps_project_code"]
            task_name = mapper[item["timeoff_type"]]["ps_task_code"]
        pay_type = mapper[item["timeoff_type"]]["replicon_pay_code"]
    return {
        "employee_id": item["employee_id"],
        "short_time_entry_id": time_entry_id,
        "entry_date": get_entry_date(item["entry_date"]),
        "project_code": project_code,
        "task_name": task_name,
        "hours": f"{float(item['hours'] or 0):.2f}",
        "pay_type": pay_type,
        
    }
