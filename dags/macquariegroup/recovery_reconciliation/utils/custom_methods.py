import calendar
import datetime
import hashlib
import json
from dateutil import parser
from dateutil.relativedelta import relativedelta
import rail
from rail.lib.artifact import new_artifact

null = None
null_urn = "urn:replicon:list-type:null"
DATE_FORMAT = "%d/%m/%Y"

def get_replicon_date_from_report_date(dag_run):
    _date = parser.parse(dag_run.conf['user_start_date'])
    return {
        "day" : _date.day,
        "month": _date.month,
        "year": _date.year
    }

def get_today_date():
    now = datetime.datetime.utcnow()
    return {
        'year': now.year,
        'month': now.month,
        'day': now.day
    }


def get_effective_dates_to_apply():
    current_time_sheet_end_date = rail.result(
        'get_users_current_timesheet_end_date')

    if not current_time_sheet_end_date:
        current_time_sheet_end_date = {
            "year": datetime.datetime.now().year,
            "month": datetime.datetime.now().month,
            "day": datetime.datetime.now().day
        }

    date_value = datetime.date(year=current_time_sheet_end_date['year'],
                               month=current_time_sheet_end_date['month'],
                               day=current_time_sheet_end_date['day']) + datetime.timedelta(days=1)
    return {
        "timesheet_period": current_time_sheet_end_date,
        "recovery_enable": {"day": date_value.day, "month": date_value.month, "year": date_value.year}
    }


def get_replicon_date(date_value):
    if not date_value:
        return null
    return {"day": date_value.day, "month": date_value.month, "year": date_value.year}


def filter_get_all_timesheet_periods(response):

    def get_value(item, index, pluck_key):
        return item[index][pluck_key] if item[index]['dataType'] != null_urn else ""

    filtered_data = list(map(lambda item: {
        "name": get_value(item['cells'], 0, "textValue"),
        "status": get_value(item['cells'], 1, "textValue"),
        "uri": get_value(item['cells'], 2, "uri")
    }, response['rows']))

    with new_artifact(mode="w") as replicon_timesheet_period_data:
        replicon_timesheet_period_data.file.write(json.dumps(filtered_data))
        replicon_timesheet_period_data.set_attribute(name="type", value="json")
        return replicon_timesheet_period_data.name


def get_first_timesheet_period_name_from_query(dag_run):
    first_timesheet_period_from_list = rail.load_all_records(rail.result(
        'get_timesheet_period_to_apply_from_feed'))[0]['timesheet_period']

    return {
        "first_timesheet_period_from_feed": first_timesheet_period_from_list,
        "timesheet_period_details": rail.find_first_by_attr_and_get_attr(
            rail.load_all_records(dag_run.conf['replicon_timesheet_period_data']), 'name', first_timesheet_period_from_list)
    }


def get_input_md5_data(item):
    if not item:
        return []
    res = {
        "Employee Type": item["Employee Type"],
        "DEPARTMENT": item["DEPARTMENT"],
        "Costcentre": item["Costcentre"],
        "GROUP": item["GROUP"],
        "OFFICE": item["OFFICE"],
        "Timesheet Period": item['Timesheet Period'],
        "DIVISION": item["DIVISION"],
        "md5": hashlib.md5((
            str(item["Employee Type"]) +
            str(item["DEPARTMENT"]) +
            str(item["Costcentre"])).encode('utf-8')).hexdigest()
    }
    return {k: v if v is not None else '' for k, v in res.items()}


def get_report_data_with_md5(item):
    if not item:
        return []
    res = {
        "login_name": item["Login Name"],
        "cost_center": item["Cost Center (Current)"],
        "department": item["Department (Current)"],
        "employee_type": item["Employee Type (Current)"],
        "groups": item["Group (Current)"],
        "recovery_enabled": item["Recovery Enabled (Current)"],
        "recovery_override": item["Recovery Override"],
        "user_uri": item["useruri"],
        "user_status": item["User Status"],
        "user_start_date": item['User Start Date'],
        "md5": hashlib.md5((str(item["Employee Type (Current)"]) +
                            str(item["Department (Current)"]) +
                            str(item["Cost Center (Current)"])
                            ).encode('utf-8')
                           ).hexdigest()
    }

    return {k: v if v is not None else '' for k, v in res.items()}


def get_invalid_records_conf(item):
    def get_missing_columns():
        missing_fields = []
        if not item['employee_type']:
            missing_fields.append("Employee Type")
        if not item['department']:
            missing_fields.append("Department")
        if not item['cost_centre']:
            missing_fields.append("Cost Centre")
        if not item['groups']:
            missing_fields.append("Group")
        if not item['timesheet_period']:
            missing_fields.append("Timesheet Period")

        return ",".join(missing_fields)

    return {
        "login_name": "",
        "employee_type": item['employee_type'],
        "department": item['department'],
        "cost_centre": item['cost_centre'],
        "action": "Validation",
        "Status": "Skipped",
        "details": get_missing_columns() + " not present in feed file"
    }


def get_skipped_records_log(item, recovery_status):
    return {
        "login_name": item['login_name'],
        "employee_type": item['employee_type'],
        "department": item['department'],
        "cost_centre": item['cost_center'],
        "action": "Update",
        "Status": "Skipped",
        "details": f"User's Recovery Enable flag is already set to {recovery_status}"
    }


def get_str_date(date_value, is_dict=False):
    if is_dict:
        return datetime.date(date_value['year'], date_value['month'], date_value['day']).strftime(DATE_FORMAT)
    return date_value.strftime(DATE_FORMAT)


def get_current_month_start_day():
    now = datetime.datetime.now().replace(day=1)
    return {
        "day": now.day,
        "month": now.month,
        "year": now.year
    }


def get_current_month_end_day(replicon_payload_format=True):
    today = datetime.datetime.now()
    current_month_last_day = today.replace(
        day=calendar.monthrange(today.year, today.month)[1])
    if not replicon_payload_format:
        return current_month_last_day
    return {
        "day": current_month_last_day.day,
        "month": current_month_last_day.month,
        "year": current_month_last_day.year
    }

def get_23rd_of_last_month():
    today = datetime.datetime.now()
    date_value = datetime.date(today.year, today.month, 23) - relativedelta(month = 1)
    return  {
        "day": date_value.day,
        "month": date_value.month,
        "year": date_value.year
    }
# if saturday -1 Day
# if sunday -2 Day
DAYS_TO_REDUCE_MAPPER = {5: -1, 6: -2}

def get_weekday_for_date(date_value):
    return date_value.weekday()

def get_previous_workingday(date_value):
    return (date_value + datetime.timedelta(
        days=DAYS_TO_REDUCE_MAPPER[get_weekday_for_date(date_value)]))

def get_custom_due_date(date_value, employee_type, current_month_holidays, calculate_custom_due_date, timesheet_start_date):
    exception_message = ""
    while True:
        is_timesheet_end_date_working_day = True
        if current_month_holidays:
            if get_str_date(date_value) in current_month_holidays and calculate_custom_due_date[employee_type]:
                date_value -= datetime.timedelta(days=1)
                is_timesheet_end_date_working_day = False
        if date_value <= timesheet_start_date:
            calculate_custom_due_date[employee_type] = False
            exception_message = "The timesheet Doesn't have any Due date"
        if is_timesheet_end_date_working_day:
            break

    return date_value, exception_message

def generate_effective_date_callable():
    def is_day_weekend(date_value):
        if get_weekday_for_date(date_value) >= 5:
            return True
        return False

    today = datetime.datetime.now()

    fmg_timesheet_end_date = datetime.date(today.year, today.month, 23)
    start_of_timesheet_period_fmg = fmg_timesheet_end_date - relativedelta(month = 1)
    start_of_timesheet_period_rmg = datetime.datetime.now().replace(day=1)
    rmg_timesheet_end_date = get_current_month_end_day(
        replicon_payload_format=False)
    current_month_holidays = rail.result("get_holidays_for_current_month")

    # check max loop
    fmg_exception_message = ""
    rmg_exception_message = ""
    calculate_custom_due_date = {"fmg": True, "rmg": True}
    rmg_timesheet_end_date, rmg_exception_message = get_custom_due_date(date_value= rmg_timesheet_end_date,
                                              employee_type='rmg',
                                              current_month_holidays= current_month_holidays,
                                              calculate_custom_due_date=calculate_custom_due_date,
                                              timesheet_start_date= start_of_timesheet_period_rmg
                                              )
    rmg_timesheet_end_date -= datetime.timedelta(days=5)

    while True:
        is_fmg_timesheet_end_date_working_day = True
        is_rmg_timesheet_end_date_working_day = True

        if current_month_holidays:
            if get_str_date(fmg_timesheet_end_date) in current_month_holidays and calculate_custom_due_date['fmg']:
                fmg_timesheet_end_date -= datetime.timedelta(days=1)
                is_fmg_timesheet_end_date_working_day = False

            if get_str_date(rmg_timesheet_end_date) in current_month_holidays and calculate_custom_due_date['rmg']:
                rmg_timesheet_end_date -= datetime.timedelta(days=1)
                is_rmg_timesheet_end_date_working_day = False

        if is_day_weekend(fmg_timesheet_end_date) and calculate_custom_due_date['fmg']:
            fmg_timesheet_end_date = get_previous_workingday(
                fmg_timesheet_end_date)
            is_fmg_timesheet_end_date_working_day = False

        if is_day_weekend(rmg_timesheet_end_date) and calculate_custom_due_date['rmg']:
            rmg_timesheet_end_date = get_previous_workingday(
                rmg_timesheet_end_date)
            is_rmg_timesheet_end_date_working_day = False

        if fmg_timesheet_end_date <= start_of_timesheet_period_fmg :
            calculate_custom_due_date['fmg'] = False
            fmg_exception_message = "The timesheet Doesn't have any Due date"
        if rmg_timesheet_end_date < start_of_timesheet_period_rmg :
            calculate_custom_due_date['rmg'] = False
            rmg_exception_message = "The timesheet Doesn't have any Due date"

        if is_fmg_timesheet_end_date_working_day and is_rmg_timesheet_end_date_working_day:
            break

    return {
        "fmg_timesheet_end_date": fmg_timesheet_end_date.strftime(DATE_FORMAT),
        "rmg_timesheet_end_date": rmg_timesheet_end_date.strftime(DATE_FORMAT),
        "rmg_timesheet_end_date_replicon_date": {
            "day": rmg_timesheet_end_date.day,
            "month": rmg_timesheet_end_date.month,
            "year": rmg_timesheet_end_date.year
        },
        "fmg_timesheet_end_date_replicon_date": {
            "day": fmg_timesheet_end_date.day,
            "month": fmg_timesheet_end_date.month,
            "year": fmg_timesheet_end_date.year
        },
        "fmg_exception_message": fmg_exception_message,
        "rmg_exception_message": rmg_exception_message
    }
