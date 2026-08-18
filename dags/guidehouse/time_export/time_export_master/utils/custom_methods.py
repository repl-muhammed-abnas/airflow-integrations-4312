import re
from datetime import timedelta
from pendulum import now
from sqlalchemy import desc
from airflow.models import DagRun, TaskInstance
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.exceptions import AirflowFailException
from datetime import datetime
from functools import lru_cache

import rail
null = None

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


@provide_session
def check_previous_wrapper_dag_runs(config, session=NEW_SESSION):
    """
    Check if the wait_for_exports task has FAILED in any failed wrapper DAG run in the last 2 hours.
    Returns True if no failures detected, False if blocking failure found.
    """
    lookback_hours = 2
    lookback_date = now() - timedelta(hours=lookback_hours)

    recent_failed_dag_runs = (
        session.query(DagRun)
        .filter(
            DagRun.dag_id == config.master_dag_id,
            DagRun.state.in_([DagRunState.FAILED]),
            DagRun.execution_date >= lookback_date,
        )
        .order_by(desc(DagRun.execution_date))
        .all()
    )

    if not recent_failed_dag_runs:
        return {
            "check_results": f"No previous run history found in last {lookback_hours} hours. Allowing execution.",
            "can_process_further": True,
        }

    failed_runs_with_blocking = []
    for dag_run in recent_failed_dag_runs:
        task_instance = (
            session.query(TaskInstance)
            .filter(
                TaskInstance.dag_id == config.master_dag_id,
                TaskInstance.run_id == dag_run.run_id,
                TaskInstance.task_id == "wait_for_exports",
            )
            .first()
        )

        if task_instance and task_instance.state == TaskInstanceState.FAILED:
            failed_runs_with_blocking.append(
                {
                    "run_id": dag_run.run_id,
                    "execution_date": dag_run.execution_date,
                    "state": task_instance.state,
                }
            )

    if failed_runs_with_blocking:
        failure_details = "; ".join(
            [
                f"Run {r['run_id']} ({r['execution_date']}): {r['state']}"
                for r in failed_runs_with_blocking
            ]
        )
        return {
            "check_results": f"Found {len(failed_runs_with_blocking)} wrapper failure(s) in last {lookback_hours} hours. {failure_details}",
            "can_process_further": False,
        }

    return {
        "check_results": "Previous wrapper runs completed without blocking failures.",
        "can_process_further": True,
    }


def validate_export_results(results):
    """
    Validate PS and India export results.
    Both must have status 'Success' or 'No Data in export' (no errors allowed).
    Returns: {"are_valid": bool, "validation_message": str}
    """
    if not results or len(results) < 2:
        return {
            "are_valid": False,
            "validation_message": f"Expected 2 export results (PS, India), got {len(results) if results else 0}",
        }

    _temp = {}
    _temp["has_data"] = False

    for result in results:
        system_name = result.get("system")
        status = result.get("status", "Unknown")
        batch_uri = result.get("batch_uri")
        _temp[system_name] = batch_uri

        if not status or not isinstance(status, str):
            return {
                "are_valid": False,
                "validation_message": f"{system_name} export returned invalid status: {status}",
            }

        if status.startswith("Error"):
            return {
                "are_valid": False,
                "validation_message": f"{system_name} export failed: {status}",
            }

        if status.startswith("Blank"):
            return {
                "are_valid": False,
                "validation_message": f"{system_name} export validation failed: {status}",
            }

        if not status.startswith("No Data") and batch_uri:
            _temp["has_data"] = True

    return {
        "are_valid": True,
        "validation_message": "All exports completed successfully or have no data.",
        **_temp
    }


def check_datalake_response(results):
    """
    Check DataLake response and determine if it's a download failure.
    Returns: {"is_download_failure": bool, "is_failure": bool}
    """
    if not results:
        return {"is_download_failure": False, "is_failure": True}

    response = results[0] if results else "Unknown"

    if response == "Success":
        return {"is_download_failure": False, "is_failure": False}

    if response == "Download_Failed":
        return {"is_download_failure": True, "is_failure": True}

    return {"is_download_failure": False, "is_failure": True}

def sanitize_free_text(value):
    return re.sub(r"[|\r\n]+", " ", value or "").strip()


@lru_cache(maxsize=32)
def get_entry_date(date):
    return datetime.strftime(datetime.strptime(date, "%d/%m/%Y"), "%Y-%m-%d")

def get_ps_worklocation(current_location_full_path, level2_countries):
    level1 = { data["level1_name"] : data["level1_code"]
                 for data in rail.result("get_all_work_locations") }
    level2 = rail.result("get_level2_work_locations")
    if not current_location_full_path:
        return ""
    current_location_full_path = [ location.strip() for location in current_location_full_path.split("/")]
    country = current_location_full_path[0]
    if country in level2_countries and len(current_location_full_path) > 1:
        return level2.get(f"{country}/{current_location_full_path[1]}", "")
    return level1.get(country, "")

def get_peoplesoft_export_rows(item, mapper, level2_countries):
    if not item:
        return []
    time_entry_id = (
        item["short_time_entry_id"]
        if not item["timeoff_type"]
        else item["timeoff_booking_id"]
    )
    project_code = item["project_code"]
    task_name = item["task_name"]
    pay_type = item["pay_code"]
    item["timeoff_hours"] = ""
    if item["timeoff_type"]:
        if mapper.get(item["timeoff_type"]):
            if item["fmla"] == "Yes":
                project_code = mapper[item["timeoff_type"]]["fmla_ps_project_code"]
                task_name = mapper[item["timeoff_type"]]["fmla_ps_task_code"]
            else:
                project_code = mapper[item["timeoff_type"]]["ps_project_code"]
                task_name = mapper[item["timeoff_type"]]["ps_task_code"]
        pay_type = mapper[item["timeoff_type"]]["replicon_pay_code"]
        item["timeoff_hours"] = item["hours"]
        item["work_location_code"] = get_ps_worklocation(item["location_name"], level2_countries)
    item["project_code"] = project_code
    item["task_code"] = task_name
    item["pay_code"] = pay_type
    item["short_time_entry_id"] = time_entry_id
    item["plc"] = ""
    item["plc_name"] = ""
    item["comments"] = sanitize_free_text(item.get("comments"))
    return item

def get_work_location_code(dag_run, current_location_full_path, level2_countries):
    if not current_location_full_path:
        return ""
    current_location_full_path = [ location.strip() for location in current_location_full_path.split("/")]
    country = current_location_full_path[0]
    if country in level2_countries and len(current_location_full_path) > 1:
        return dag_run.conf.get("level2_locations", {}).get(f"{country}/{current_location_full_path[1]}", "")
    return dag_run.conf.get("level1_locations", {}).get(country, "")

def get_cp_export_rows(item, mapper, dag_run, level2_countries):
    if not item:
        return []
    time_entry_id = (
        item["short_time_entry_id"]
        if not item["timeoff_type"]
        else item.get("timeoff_booking_id","")
    )
    project_code = item["project_code"]
    task_code = item["task_code"]
    pay_type = item.get("pay_code", "")
    item["timeoff_hours"] = ""
    if item["timeoff_type"]:
        if mapper.get(item["timeoff_type"]):
            if item["fmla"] == "Yes":
                project_code = mapper[item["timeoff_type"]]["fmla_cp_project_code"]
                task_code = mapper[item["timeoff_type"]]["fmla_cp_task_code"]
            else:
                project_code = mapper[item["timeoff_type"]]["cp_project_code"]
                task_code = mapper[item["timeoff_type"]]["cp_task_code"]
            pay_type = mapper[item["timeoff_type"]]["replicon_pay_code"]
        item["timeoff_hours"] = item["hours"]
        item["work_location_code"] = get_work_location_code(dag_run,item["location_name"], level2_countries)
    item["pay_code"] = pay_type
    item["project_code"] = project_code
    item["task_code"] = task_code
    item["short_time_entry_id"] = time_entry_id
    item["comments"] = sanitize_free_text(item.get("comments"))
    return {**item}

def apply_plc_to_row(item, TIMEOFF_PROJECT_TASK_MAPPER,dag_run, level2_countries):
    if not item:
        return item
    employee_id = item.get("employee_id", "")
    project_code = item.get("project_code", "")
    task_path = item.get("task_name_full_path", "").split("/")[-1].strip()
    key = f"{employee_id}_{project_code}_{task_path}"
    role = (rail.result("get_user_and_task_role_mapping") or {}).get(key) or {}
    item["plc"] = role.get("plc") or "GENRL"
    item["plc_name"] = role.get("plc_name") or "GENRL"
    item = get_cp_export_rows(item, TIMEOFF_PROJECT_TASK_MAPPER,dag_run, level2_countries)
    
    return {**item}

def project_role_page_handler(request, response):
    if len(response["rows"]) > 0:
        request["page"] += 1
        return request
    return None


def build_project_role_code_map(response):
    role_code_map = {}
    for page in response:
        for row in page.get("rows", []):
            role_code_map[row["cells"][1].get("uri")] = {
                "code": row["cells"][0].get("textValue"),
                "name": row["cells"][1].get("textValue"),
            }
    return role_code_map


def build_plc_mapping(results, default_role="GENRL"):
    role_code_map = rail.result("get_all_project_roles") or {}
    mapping = {}
    for i in results:
        for item, data in zip(i[0], i[1]):
            if not data.get("error") and data.get("estimateDetails"):
                key = item["employee_id"] + "_" + item["project_code"] + "_" + \
                    item["task_name_full_path"].split("/")[-1].strip()
                project_role = data["estimateDetails"].get("projectRole") or {}
                role = role_code_map.get(project_role.get("uri")) or {}
                mapping[key] = {
                    "plc": role.get("code") or default_role,
                    "plc_name": role.get("name") or default_role,
                }
    return mapping