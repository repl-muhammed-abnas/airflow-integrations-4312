import rail
from cie_epiq.ts_approval_utility import config

def get_timehseet_approve_batch():
    timesheetsUri = rail.result('filter_errors_timesheets').get('validTimesheets')
    if timesheetsUri:
        return {
            "timesheetUris": timesheetsUri,
            "comments": config.infosys_config['timesheet_approve_remarks'],
        }
    return None

def get_all_ts_uris(dag_run):
    if dag_run:
        return {
            "timesheetUris": dag_run.conf["item"],
        }
    return None

def execute_batch_timesheet_data(item):
    if item:
        return {
            "timesheetApprovalBatchUri": item,
        }
    return None
