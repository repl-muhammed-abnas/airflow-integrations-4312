import json
from datetime import datetime, timedelta
from io import StringIO
import pytz
import pandas as pd
import numpy as np
from airflow.models import Variable
import pendulum
# pylint: disable=line-too-long, too-many-arguments, consider-using-f-string, too-many-nested-blocks, too-many-branches, too-many-statements, chained-comparison

def extract_data(data):
    result = None
    raw_data = json.loads(data)
    if raw_data:
        result = {
            "expenseuri": [raw_data["expenseSheet"]["uri"]] if "expenseSheet" in raw_data else [],
            "statusuri": [raw_data["approvalStatusUri"]] if "approvalStatusUri" in raw_data else [],
            "actinguser": [raw_data["authority"]["actingUser"]] if "authority" in raw_data else []
        }
    return result

def get_eastern_timenow(config):
    return pendulum.now(config.instance_tz)

def divide_chunks(l, chunk_size):
    for i in range(0, len(l), chunk_size):
        yield l[i:i + chunk_size]

def get_expense_uris(expensedetails, config):
    expensedetails_list = json.loads(expensedetails)
    expense_uris = []
    for expense in expensedetails_list:# iterate multiple expense file content
        expense_lines = expense["data"].split("\n")
        if len(expense_lines) > 0: # get number line contents in particular expense file
            expense_uri_line = [line for line in expense_lines if ":expense-sheet:" in line]
            if len(expense_uri_line) > 0:
                expense_uri = [ line_data for line_data in expense_uri_line[0].split(",") if ":expense-sheet:" in line_data]
                expense_uris.append(expense_uri)
    total_expense_uris = sum(expense_uris, [])
    chunked_expense_uris = list(divide_chunks(total_expense_uris, config.chunk_size))
    if len(chunked_expense_uris) > 0:
        return chunked_expense_uris
    return []

def findItemByDisplayText(response, name):
    for item in response.json()['d']:
        if item['displayText'].lower() == name.lower():
            return item['uri']
    return ''

# def get_specific_filter_uri(filterList, filter_name):
#     return rail.find_first_by_attr_and_get_attr(filterList, 'displayText', filter_name, 'uri')

def report_str_to_json(response):
    report_data = response.json()['d']['payload']
    if not report_data:
        return []
    df = pd.read_csv(StringIO(report_data), sep=",")
    df1 = df.replace(np.nan, "")
    data = df1.to_dict('records')
    return data

def get_grouped_user_details(user_report_details):
    user_details = json.loads(user_report_details)
    grouped_user_dict = {}
    for user in user_details:
        if user["UserUri"]:
            grouped_user_dict[user["UserUri"]] = user
    return grouped_user_dict


def get_grouped_project_details(project_report_details):
    project_details = json.loads(project_report_details)
    grouped_project_dict = {}
    for project in project_details:
        if project["ProjectUri"]:
            grouped_project_dict[project["ProjectUri"]] = project
    return grouped_project_dict

def get_grouped_expense_approval_detail(approval_detail):
    approval_detail_list = json.loads(approval_detail)
    grouped_expense_approval_detail_list = {}
    for details in approval_detail_list:
        # grouped_expense_approval_detail_list.append({"expUri": details["expenseSheet"]["uri"], "expInfo": details})
        grouped_expense_approval_detail_list[details["expenseSheet"]["uri"]] = details
    return grouped_expense_approval_detail_list

def add_expenses_to_variable(exp_details, config):
    details = json.loads(exp_details)
    expense_details_temporary = Variable.get(f"expense_details_temporary_{config.instance}", deserialize_json=True)
    added_list = details + expense_details_temporary
    Variable.set(key=f"expense_details_temporary_{config.instance}", value=json.dumps(added_list))

    return added_list

def extract_expense_details_from_variables(config):
    expense_details = Variable.get(f"expense_details_temporary_{config.instance}", deserialize_json=True)
    Variable.set(key=f"expense_details_temporary_{config.instance}", value=json.dumps([]))
    return expense_details

def convert_expense_chunks_flat_list(expense_uris):
    expense_uris_json = json.loads(expense_uris)
    if len(expense_uris_json) > 0:
        return sum(expense_uris_json, [])
    return []

def get_approver_id(approved):
    approver = ""
    if approved[len(approved) - 1]["authority"]["actingUser"] and approved[len(approved) - 1]["authority"]["actingUser"]["uri"]:
        approver = approved[len(approved) - 1]["authority"]["actingUser"]["uri"]
    return approver

def get_project(grouped_project_data, expense_entry):
    project = None
    if expense_entry["project"]["uri"] in grouped_project_data and grouped_project_data[expense_entry["project"]["uri"]]:
        project = grouped_project_data[expense_entry["project"]["uri"]]
    return project

def get_unique_id(entry):
    unique_id = ""
    uid = ""
    if entry and len(entry["uri"]) > 0:
        uid = entry["uri"].split(":")
    if len(uid) > 1:
        unique_id = uid[-1]
    return unique_id

def get_user(grouped_user_data, expense):
    user = None
    if expense["owner"]["uri"] in grouped_user_data and grouped_user_data[expense["owner"]["uri"]]:
        user = grouped_user_data[expense["owner"]["uri"]]
    return user

def get_client_name(expense_entry):
    client_name = ""
    if expense_entry and expense_entry["client"] and len(expense_entry["client"]["name"]) > 0:
        client_name = expense_entry["client"]["name"]
    return client_name

def update_next_file_number(var,config):
    current_file_number = var["file_number"]
    next_file_number = current_file_number + 1
    var["file_number"] = next_file_number
    Variable.set(key=f"randstad_expensedata_export_variables_{config.instance}", value=json.dumps(var))

def process_expense_extract(expense_details_from_variables, grouped_user_details, grouped_project_details, grouped_expense_approval_detail, processed_ExpenseUris, config):
    expense_info = json.loads(expense_details_from_variables)
    grouped_user_data = json.loads(grouped_user_details)
    grouped_project_data = json.loads(grouped_project_details)
    expense_approval_info = json.loads(grouped_expense_approval_detail)
    csv_object = []
    var = Variable.get(f"randstad_expensedata_export_variables_{config.instance}", deserialize_json=True)
    current_file_number = var["file_number"]
    prev_expense_uris = processed_ExpenseUris
    for expense in expense_info:
        expense_unique_code = "" if len(expense["uri"].split(":")) < 2 else expense["uri"].split(":")[-1]
        if expense_unique_code in prev_expense_uris.split(","):
            continue
        approval_data = None if expense["uri"] not in  expense_approval_info else expense_approval_info[expense["uri"]]
        if not approval_data:
            continue

        submitted = [entry for entry in approval_data["entries"] if entry['action']['uri']  == "urn:replicon:approval-action:submit"]
        submitted.sort(key = lambda entry:datetime.strptime(entry['timestamp']['displayText'], config.based_report_date_format_with_time))

        approved = [entry for entry in approval_data["entries"] if entry['action']['uri']  in ["urn:replicon:approval-action:forced-approve", "urn:replicon:approval-action:approve"]]
        approved.sort(key = lambda entry:datetime.strptime(entry['timestamp']['displayText'], config.based_report_date_format_with_time))
        if len(approved) > 0:
            if len(submitted) > 0:
                submitted_on_obj = datetime.strptime(submitted[len(submitted) - 1]["timestamp"]["displayText"], config.based_report_date_format_with_time)
                submitted_on = submitted_on_obj.strftime(config.export_date_time_format)
            else:
                submitted_on_obj = datetime.strptime(approved[len(approved) - 1]["timestamp"]["displayText"], config.based_report_date_format_with_time)
                submitted_on = submitted_on_obj.strftime(config.export_date_time_format)
            approver_uri = get_approver_id(approved)
            approved_on_obj = datetime.strptime(approved[len(approved) - 1]["timestamp"]["displayText"], config.based_report_date_format_with_time)
            approved_on = approved_on_obj.strftime(config.export_date_time_format)
            approved_by = None if approver_uri == "" or approver_uri not in grouped_user_data else grouped_user_data[approver_uri]

            for expense_entry in expense["entries"]:
                expense_activity = ""
                expense_text = ""
                for custom_data in expense_entry["customFields"]:
                    if custom_data and custom_data["customField"] and (custom_data["customField"]["name"]).lower() == "expense activity":
                        expense_activity = custom_data["text"]
                        if custom_data["text"].lower() == "travel&exp_bill":
                            expense_text = "Travel & Expense Billable"
                        if  custom_data["text"].lower() == "travel&exp_nobl":
                            expense_text = "Travel & Expense Non-Billable"
                project = get_project(grouped_project_data, expense_entry)
                project_code = "" if not project else project["Project Code"]
                client_code = "" if not project else project["Client Code"]
                user = get_user(grouped_user_data, expense)
                project_name = expense_entry["project"]["name"]
                expense_id = get_unique_id(expense_entry["expenseCode"])
                processed_data = {
                            "SOURCE": "PAS",
                            "RNA_RPL_IMP_ID": current_file_number,
                            "SEQNBR": "0",
                            "RNA_RPT_PRD_ID": expense["trackingNumber"],
                            "RNA_TASK_TSH_ID": expense_id,
                            "RNA_TSH_ENTRY_ID": get_unique_id(expense_entry),
                            "RNA_RPL_EMPLID": get_unique_id(expense["owner"]),
                            "EMPLID": "" if not user else user["Employee ID"],
                            "FIRST_NAME": "" if not user else user["User First Name"],
                            "LAST_NAME": "" if not user else user["User Last Name"],
                            "PAY_END_DT": "NULL",
                            "DATE_WRK": "NULL",
                            "TL_QUANTITY": "0",
                            "EXPENSE_TYPE": "",
                            "RNA_EXPENSE_DATE": datetime(expense_entry["incurredDate"]["year"], expense_entry["incurredDate"]["month"], expense_entry["incurredDate"]["day"]).strftime("%m/%d/%Y"),
                            "RNA_EXP_PAY_AMT": expense_entry["reimbursementAmount"]["amount"],
                            "SP_EXP_APPROVER": "" if not approved_by else "Approved by {} {}".format(approved_by["User First Name"], approved_by["User Last Name"]),
                            "RNA_RPL_PAY_CODE": expense_entry["expenseCode"]["name"],
                            "RNA_RPL_ACTIVITY": expense_id,
                            "RNA_RPL_TASKID": expense_id,
                            "APPROVAL_STATUS": "2",
                            "RNA_TASK_BILLABLE": "2",
                            "RNA_TSH_BILLABLE": "2",
                            "DTTIME_ADDED": submitted_on,
                            "DTTM_EXPORT": submitted_on,
                            "RNA_RPL_PROJ_ID": get_unique_id(expense_entry["project"]),
                            "RNA_RPL_TASK_NAME": f"{project_name}/{expense_text}-({project_code}_{expense_activity})",
                            "RNA_RPL_TASK_CODE": f"{project_code}_{expense_activity}",
                            "RNA_RPL_UNITID": get_unique_id(expense_entry["client"]),
                            "RNA_CLIENT_CODE": client_code,
                            "RNA_CLIENT_NAME": f"{get_client_name(expense_entry)}({client_code})",
                            "RNA_RPL_NEW_TIME": "Y",
                            "VENDOR_ID": "" if not user else user["Vendor ID"],
                            "PAY_RATE": "" if not user else user["Pay Rate"],
                            "RUN_DTTM": "",
                            "PROCESS_STATUS": "N",
                            "RECORD_IDENTIFIER": "E",
                            "DTTM_IMPORTED": "",
                            "EMPLID2": "",
                            "FIRST_NAME_SRCH": "",
                            "LAST_NAME_SRCH": "",
                            "RNA_APPROVER_DTTM": approved_on,
                            "expense_unique_code": expense_unique_code
                        }
                csv_object.append(processed_data)
    update_next_file_number(var,config)
    if len(csv_object) > 0:
        curr_timestamp = datetime.now().strftime(config.export_date_time_format)
        for export_entry in csv_object:
            export_entry["DTTM_IMPORTED"] = curr_timestamp
            export_entry["RUN_DTTM"] = curr_timestamp
        return json.dumps(csv_object)
    return ""

def create_expense_uri_str(data, initial_uris):
    content = json.loads(data)
    ts_uris = []
    ts_uris_str = ""
    for d in content:
        if d["expense_unique_code"] not in ts_uris:
            ts_uris.append(d["expense_unique_code"])
    if len(ts_uris) > 0:
        ts_uris_str = initial_uris +",".join(ts_uris)+","
    else:
        ts_uris_str = initial_uris
    return ts_uris_str

def get_s3_keys(expense_file_details):
    expense_s3_keys = json.loads(expense_file_details)
    s3_keys = [ data["s3_key"] for data in expense_s3_keys]
    return s3_keys

def check_trigger_time(config):
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    scheduled_time = config.trigger_time
    current_eastern_time_raw = get_eastern_timenow(config)
    current_eastern_time_str = current_eastern_time_raw.strftime("%Y-%m-%dT%H:%M:%S")
    current_eastern_time = datetime.strptime(current_eastern_time_str, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=pytz.timezone(config.instance_tz))
    for schedule in scheduled_time:
        schedule_day = schedule.split(" ")[0]
        schedule_time = schedule.split(" ")[1]
        schedule_dt_time = current_eastern_time.strftime("%d-%m-%Y") + " " + schedule_time #
        schedule_date_time = datetime.strptime(schedule_dt_time, "%d-%m-%Y %H:%M")
        schedule_date_time_obj = schedule_date_time.replace(tzinfo=pytz.timezone(config.instance_tz))
        scheduletime_after_5min_obj = (schedule_date_time + timedelta(minutes=5)).replace(tzinfo=pytz.timezone(config.instance_tz))
        if schedule_day.lower() == days[current_eastern_time.weekday()].lower():
            if current_eastern_time >= schedule_date_time_obj and scheduletime_after_5min_obj > current_eastern_time:
                return True
    return False
    