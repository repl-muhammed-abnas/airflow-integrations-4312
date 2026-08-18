import json
from datetime import datetime

def process_timesheetwaitingforapproval(res):
    data = json.loads(res)
    rows = data["rows"]
    timesheet_data = {}
    for row in rows:
        cell = row["cells"]
        date_val = cell[4]["textValue"]
        date_val_obj = datetime.strptime(date_val,"%m/%d/%Y")
        current_date = datetime.now()
        opensince = (current_date - date_val_obj).days
        if opensince < 4 or opensince > 11:
            continue
        processed_data = {
            "timesheeturi": cell[0]["uri"],
            "timesheetstatus": cell[1]["textValue"],
            "timesheetowner": cell[2]["textValue"],
            "approvalduedate": cell[3]["textValue"],
            "duedate": cell[4]["textValue"],
            "timesheetperiod": cell[5]["textValue"],
            "opensince": opensince,
            "currentlywaitingonapprover": cell[7]["slug"],
            "supervisoroftimesheetowner": cell[8]["slug"],
            "supervisoroftimesheetowneruri": cell[8]["uri"]
            }
        timesheet_data[cell[0]["uri"]] = processed_data
    return timesheet_data

def check_approver(res):
    processed_timesheet_data = json.loads(res)
    for key in processed_timesheet_data:
        if processed_timesheet_data[key]["currentlywaitingonapprover"] != processed_timesheet_data[key]["supervisoroftimesheetowner"]:
            processed_timesheet_data[key]["get_approver"] = True
        else:
            processed_timesheet_data[key]["get_approver"] = False
        processed_timesheet_data[key]["approvers"] = ""
    return processed_timesheet_data

def format_timesheet_data_with_approvers(timesheet_data, approvers_data):
    timesheet_data_obj = json.loads(timesheet_data)
    approvers_data_obj = json.loads(approvers_data)
    approvers = {}
    for d in approvers_data_obj:
        for key in d:
            if key not in approvers:
                approvers[key] = d[key]
            else:
                approvers[key] = approvers[key] + d[key]

    for key in timesheet_data_obj:
        if key in approvers:
            if timesheet_data_obj[key]["approvers"] == "":
                timesheet_data_obj[key]["approvers"] = ", ".join(approvers[key])
            else:
                if len(approvers[key]) > 0:
                    timesheet_data_obj[key]["approvers"] = timesheet_data_obj[key]["approvers"] + ", " + ", ".join(approvers[key])
    return timesheet_data_obj
