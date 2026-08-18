null = None


def get_process_time_data_records_conf(item):
    return {
        **{k: v if v is not None else '' for k, v in item.items()}
    }


def get_timesheets_payload(dag_run):
    return {
        "timesheets": list(map(lambda item: item['TimesheetPeriodUri'], dag_run.conf['timesheetdetails']))
    }

def get_timesheets_delete_payload(dag_run):
    return {
        "timesheetUris": list(map(lambda item: item['TimesheetPeriodUri'], dag_run.conf['timesheetdetails'])),
        "deleteOptionUri": "urn:replicon:timesheet-delete-option:delete-overlapping-time-and-payable-time-entries"
    }
