import pendulum as pndlum
import rail


def logging_details(config):
    return {
        "dag_run_start_time": str(pndlum.now(config.time_zone)),
        "log_filename": 'Prod_Timesheet_automatic_submission_logs_' + str((pndlum.now(config.time_zone)).strftime("%m%d%YT%H%M%S")) + '.csv'
    }


def read_collection(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        data_collection = list(reader)
    return data_collection


def check_timesheetperiod():
    timesheets_data = read_collection(
        rail.result("create_timesheet_data"))
    for timesheets in timesheets_data:
        if timesheets['timesheetperiod'] == '' or timesheets['timesheetperiod'] is None:
            return False
    return True


def empty_method():
    return True


def get_expected_approvers_list():
    return "/".join([approver["displayText"] for approver in rail.result('get_expected_approvers')])


def get_timesheet_status(timesheet_history):
    approval_agent = timesheet_history["approvalAgent"]["user"]
    approver_list = get_expected_approvers_list()
    if timesheet_history["action"]["displayText"] == "Submit":
        if approval_agent is not None:
            if approval_agent["displayText"] != "" or approval_agent["displayText"] is not None:
                if approval_agent["displayText"] in approver_list:
                    return "Approve"
    return timesheet_history["action"]["displayText"]


def get_approver_name(timesheet_history):
    approval_agent = timesheet_history["approvalAgent"]["user"]
    if approval_agent is not None:
        if approval_agent["displayText"] != "" or approval_agent["displayText"] is not None:
            return approval_agent["displayText"]
    return "System"


def create_approval_history_list():
    timesheet_history = rail.result(
        "get_timesheet_approval_details")["history"]
    return list(map(lambda item:
                    {
                        "status": get_timesheet_status(item),
                        "name": get_approver_name(item),
                        "reopen": "Yes" if item["action"]["displayText"] == "Reopen" else "No",
                        "comments": item["comments"]
                    }, timesheet_history
                    ))


def get_final_approver_bool(expected_approver, approval_history_list):
    for row in approval_history_list:
        if row["name"] is not None or row["name"] != "":
            if expected_approver == row["name"] and row["status"] == "Approve":
                return "Yes"
    return "No"


def get_final_approver_list():
    expected_approvers = rail.result('get_expected_approvers')
    approval_history_list = rail.result('approval_history_list')
    return list(map(lambda item:
                    {
                        "name": item["displayText"],
                        "finalapprover": get_final_approver_bool(item["displayText"], approval_history_list),
                    }, expected_approvers
                    ))


def get_previous_action():
    approval_list = rail.result('approval_history_list')
    return "Approve" if approval_list[len(approval_list)-2]["status"] == "System Approval" else approval_list[len(approval_list)-2]["status"]


def get_last_approval():
    final_approver_list = rail.result('final_approver_check_list')
    uniq_approver_list = []
    for row in final_approver_list:
        if row["finalapprover"] == "Yes":
            uniq_approver_list.append(row["name"])
    return "Yes" if len(set(uniq_approver_list)) == len(rail.result('get_expected_approvers')) else "No"


def get_status_approve():
    return rail.result('previous_action')


def get_status():
    return "Submit" if rail.result('status_approve') is None else rail.result('status_approve')
