from datetime import datetime, timedelta
from rail import result


DATE_FORMAT = "%b %d, %Y"
def get_punch_history_data_payload(employeeid, dag_run):
    end = datetime.strptime(dag_run.conf['timesheetenddate'], DATE_FORMAT)
    resp = {
        "eecode": employeeid,
        "startdate": int((datetime.strptime(dag_run.conf['timesheetstartdate'], DATE_FORMAT)).timestamp()),
        "enddate": int((end + timedelta(days=1)).timestamp()),
    }
    return resp


def get_punch_ids():
    response = result("punch_history_data")
    if not response:
        return []
    data = response.get('response', {}).get('data', [])
    return [{"id": rec['punchid']} for rec in data]