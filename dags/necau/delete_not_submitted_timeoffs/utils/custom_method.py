import rail

def add_timeoff_log_data():
    timeoff_data = rail.result("for_each_timeoff")
    return {
        "useruri": timeoff_data['useruri'] if timeoff_data else None,
        "timeoffbookinguri": timeoff_data['timeoffuri'] if timeoff_data else None,
        "startdate": timeoff_data['startdate'] if timeoff_data else None,
        "enddate": timeoff_data['enddate'] if timeoff_data else None,
        "timeofftypename": timeoff_data['timeofftype'] if timeoff_data else None,
        "executionstatus": "Success"
    }
