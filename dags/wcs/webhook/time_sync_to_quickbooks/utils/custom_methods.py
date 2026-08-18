import rail


def build_child_conf(dag_run):
    ts = rail.result("get_timesheet_details")
    start = ts["dateRange"]["startDate"]
    end = ts["dateRange"]["endDate"]
    return {
        "timesheet_uri": dag_run.conf["webhook"]["data"]["timesheet"]["uri"],
        "timesheet_owner": ts["owner"]["displayText"],
        "timesheet_period": f"{start['year']}/{start['month']}/{start['day']}-{end['year']}/{end['month']}/{end['day']}",
        "user_uri": ts["owner"]["uri"],
    }
