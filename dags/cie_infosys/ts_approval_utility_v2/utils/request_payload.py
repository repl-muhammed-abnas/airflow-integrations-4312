import rail
from cie_infosys.ts_approval_utility_v2 import config


def validate_batch_entry_payload(dag_run):
    if dag_run:
        return {
            "timeEntryRevisionGroupUris": dag_run.conf["item"],
        }
    return None

def create_batch_entry_payload():
    included_uris = rail.result(
        'validate_timeentry_batch').get('included_uris')
    if included_uris:
        return {
            "timeEntryRevisionGroupUris": included_uris,
            "comments": config.infosys_config['entry_approve_remarks'],
        }
    return None


def execute_batch_entry_data(item):
    if item:
        return {
            "batchUri": item,
        }
    return None


def get_timehseet_approve_batch(dag_run):
    if dag_run:
        return {
            "timesheetUris": dag_run.conf["item"],
            "comments": config.infosys_config['timesheet_approve_remarks'],
        }
    return None


def execute_batch_timesheet_data(item):
    if item:
        return {
            "timesheetApprovalBatchUri": item,
        }
    return None

def get_processed_entries_uri():
    processed_uris = rail.result('validate_timeentry_batch')
    if processed_uris and processed_uris.get("included_uris"):
        return {
            "timeEntryRevisionGroupUris": processed_uris.get("included_uris"),
        }

    return None

def get_processed_ts_uri(dag_run):
    if dag_run:
        return {
            "timesheetUris": dag_run.conf["item"],
        }
    return None
