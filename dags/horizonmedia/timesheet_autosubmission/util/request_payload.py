import rail

def get_recalculate_timesheet_payload():
    data = rail.result("get_valid_ts_query_data")
    ts_uris = [d['TimesheetPeriodUri'] for d in data if 'TimesheetPeriodUri' in d]
    return {"timesheets": ts_uris}

def get_validation_msg_timesheet_payload():
    data = rail.result("get_valid_ts_query_data")
    ts_uris = [d['TimesheetPeriodUri'] for d in data if 'TimesheetPeriodUri' in d]
    return {"timesheetUris": ts_uris}
