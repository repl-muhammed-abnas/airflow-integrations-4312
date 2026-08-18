from datetime import datetime
import hashlib
from dateutil.relativedelta import relativedelta
import rail


def check_timeoff_type_assigned_to_user(dag_run):
    user_timeoff_policy_data = rail.result("get_user_info")["timeOffTypePolicySummary"]["policiesByTimeOffType"]
    return rail.find_first_by_attr_and_get_attr(user_timeoff_policy_data, "timeOffType.name", dag_run.conf["time_off_type"])

def is_older_than_6_months(dag_run):
    date = datetime.strptime(dag_run.conf['time_off_date'], '%m/%d/%Y')
    six_months_ago = datetime.now() - relativedelta(months=6)
    return date < six_months_ago

def get_formated_timeoff_row(item):
    if not item:
        return []
    timeoff_md5 = hashlib.md5((
        item["Employee ID"] +
        item["Time off Type"] +
        item['Time Off Date'] ).encode()).hexdigest()
    return { **item, "MD5": timeoff_md5}
