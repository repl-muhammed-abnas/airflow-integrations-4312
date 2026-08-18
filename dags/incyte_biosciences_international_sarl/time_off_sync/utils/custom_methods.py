from datetime import datetime
from dateutil.relativedelta import relativedelta

DATE_FORMAT="%d/%m/%Y"
def get_skipped_log_details(item):
    details = ""
    if not item["employee_id"]:
        details = "employee id is not present"
    if not item["time_off_type"]:
        details += "time off type is not present"
    if not item["start_date"]:
        details += "start date is not present"
    if not item["end_date"]:
        details += "end date is not present"
    if not item["peoplesoft_unique_id"]:
        details += "unique id is not present"
    if not item["status"]:
        details += "status is not present"
    if not item["duration_type"]:
        details += "duration type is not present"
    if not item["duration"]:
        details += "duration is not present"
    return details


def check_valid_dates(dag_run):
    start_date = datetime.strptime(
        dag_run.conf["start_date"].strip(), DATE_FORMAT)
    end_date = datetime.strptime(dag_run.conf["end_date"].strip(), DATE_FORMAT)
    if start_date <= end_date:
        return True
    return False


def check_weekend_timeoff(dag_run):
    start_date = datetime.strptime(
        dag_run.conf["start_date"].strip(), DATE_FORMAT)
    if start_date.weekday() in [5, 6]:
        return True
    return False


def get_valid_multiple_dates(dag_run):
    start_date = datetime.strptime(
        dag_run.conf["start_date"].strip(), DATE_FORMAT)
    end_date = datetime.strptime(dag_run.conf["end_date"].strip(), DATE_FORMAT)
    if start_date == end_date:
        return [datetime.strftime(start_date, DATE_FORMAT)]
    dates_list = []
    while start_date <= end_date:
        if not start_date.weekday() in [5, 6]:
            dates_list.append(datetime.strftime(start_date, DATE_FORMAT))
        start_date = start_date + relativedelta(days=1)
    if len(dates_list) > 0:
        return dates_list
    return []

def get_time_sheet_periods(response):
    timesheet_periods = []
    if response:
        for i in response:
            start_date = str(i["dateRange"]["startDate"]["day"])+"/"+\
                str(i["dateRange"]["startDate"]["month"])+"/"+str(i["dateRange"]["startDate"]["year"])
            end_date =  str(i["dateRange"]["endDate"]["day"])+"/"+\
                str(i["dateRange"]["endDate"]["month"])+"/"+str(i["dateRange"]["endDate"]["year"])
            timesheet_periods.append(start_date + "-" + end_date)
    return timesheet_periods

def create_data(item):
    if not item:
        return []
    return {
        "first_name":item["properties"]["first_name"],
        "employee_id":item["properties"]["employee_id"],
        "time_off_type": item["properties"]["time_off_type"],
        "start_date": item["properties"]["start_date"],
        "end_date": item["properties"]["end_date"],
        "unique_id": item["properties"]["unique_id"],
        "time_off_status": item["properties"]["time_off_status"],
        "time_sheet_period": item["properties"]["time_sheet_period"],
        "status": item["properties"]["status"],
        "details": item["properties"]["details"],
        "email": item["properties"]["email"]
    }
    