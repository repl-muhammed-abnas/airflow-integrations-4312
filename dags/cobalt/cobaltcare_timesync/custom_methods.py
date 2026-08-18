from datetime import datetime


def get_exception_message(dag_run):
    msg = ""
    if not dag_run.conf["webhook"]["data"]["billing"]:
        msg = "Billing selection not present"
    if not dag_run.conf["webhook"]["data"]["project"]:
        msg += "Project not present"
    if not dag_run.conf["webhook"]["data"]["starttime"]:
        msg += "Start time not present"
    if not dag_run.conf["webhook"]["data"]["endtime"]:
        msg += "End time not present"
    if not dag_run.conf["webhook"]["data"]["ticketid"]:
        msg += "Ticket ID not present"
    return msg


def compare_start_end_time(dag_run):
    start_time = datetime.strptime(
        dag_run.conf["webhook"]["data"]["starttime"], "%Y-%m-%dT%H:%M")
    end_time = datetime.strptime(
        dag_run.conf["webhook"]["data"]["endtime"], "%Y-%m-%dT%H:%M")
    if start_time < end_time:
        return start_time
    return False


def get_time(time_entry):
    time_entry = datetime.strptime(time_entry, "%Y-%m-%dT%H:%M")
    return {
        "hour": time_entry.hour,
        "minute": time_entry.minute,
        "second": time_entry.second
    }
