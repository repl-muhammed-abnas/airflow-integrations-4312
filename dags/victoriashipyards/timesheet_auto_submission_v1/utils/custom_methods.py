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

def create_approval_history_list():
    timesheet_history = rail.result(
        "get_timesheet_approval_details")["history"]
    return list(map(lambda item:
                    {
                        "status": item["action"]["displayText"],
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
