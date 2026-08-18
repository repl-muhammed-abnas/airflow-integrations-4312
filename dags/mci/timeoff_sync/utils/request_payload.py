from datetime import datetime, timedelta
from rail import result

DATE_FORMAT = "%b %d, %Y"
def get_punch_history_daterange_payload(employeeid, entrydate):
    start = datetime.strptime(entrydate, DATE_FORMAT)
    resp = {
        "eecode": employeeid,
        "startdate": int(start.timestamp()),
        "enddate": int((start + timedelta(days=1)).timestamp()),
    }
    return resp


def get_punch_ids():
    response = result("punch_history_for_daterange")
    if not response:
        return []
    data = response.get('response', {}).get('data', [])
    return [{"id": data[0]['punchid']}] if data and data[0]['earncode'] != "R" else []
