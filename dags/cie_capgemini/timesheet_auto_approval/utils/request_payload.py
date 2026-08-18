from cie_capgemini.timesheet_auto_approval import config


def get_timehseet_approve_batch(dag_run):
    if dag_run:
        return {
            "timesheetUris": dag_run.conf["item"],
            "comments": config.timesheet_approve_remarks,
        }
    return None


def execute_batch_timesheet_data(item):
    if item:
        return {
            "timesheetApprovalBatchUri": item,
        }
    return None
