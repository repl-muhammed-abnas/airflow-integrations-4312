# pylint: disable=inconsistent-return-statements
from datetime import datetime
from io import StringIO
from pytz import timezone
import rail
import pandas as pd


def check_for_trigger_day():
    days = {
        "sunday": False,
        "monday": True,
        "tuesday": True,
        "wednesday": True,
        "thursday": True,
        "friday": True,
        "saturday": False,
        "time_zone": "Central Time (US & Canada)",
        "hour": "10",
        "minute": "00"
    }
    time_now = datetime.now()
    central = timezone('US/Central')
    published_cst = time_now.astimezone(central)
    return days.get(published_cst.strftime('%A').lower())


def get_date_today():
    time_now = datetime.now()
    central = timezone('US/Central')
    published_cst = time_now.astimezone(central)
    return published_cst.strftime("%d/%m/%Y")


def get_currrent_or_future_days_list():
    output_list = []
    date_today = datetime.strptime(rail.result("date_today"), "%d/%m/%Y")
    for entry in rail.result("get_holidays_in_date_range"):
        date = entry.get('date', {})  # {'day': 14, 'month': 1, 'year': 2019}
        holiday_date = datetime(date.get('year'),
                                date.get('month'),
                                date.get('day'))
        if date_today <= holiday_date:
            output_list.append({
                "date": holiday_date.strftime("%d/%m/%Y"),
                "duration": entry.get("duration"),
                "durationTypeUri": entry.get("durationTypeUri"),
                "name": entry.get("name"),
                "uri": entry.get("uri")
            })
    output_list.sort(key=date_coverter)
    return output_list


def date_coverter(entry):
    return datetime.strptime(entry.get('date'), "%d/%m/%Y")


def get_coming_date():
    if rail.result("get_holiday_list"):
        details = rail.result('get_holiday_list')[0]
        details["date_difference"] = (datetime.strptime(details.get(
            "date"), "%d/%m/%Y") - datetime.strptime(rail.result("date_today"), "%d/%m/%Y")).days
        return details


def get_errror_logs():
    errored_logs = []
    all_logs = []
    error_logs_info = rail.result('create_log')
    errored_logs_from_child = get_data_from_document(error_logs_info)
    errored_logs += errored_logs_from_child
    for reocrd in errored_logs:
        if reocrd.get('properties'):
            all_logs.append(reocrd.get('properties'))
    return all_logs


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def get_details_for_email():
    output = {"subject": "completed successfully",
              "time_stamp": "{{ current_time_in_specified_tz() }}"
              }
    for log in rail.result('get_all_logs'):
        if log.get('Status') and 'Email Not Sent' in log.get('Status'):
            output['subject'] = 'completed with errors'
            break
    return output


def get_accelerated_payroll_details():
    output = {}
    datetime.today().weekday()
    date = rail.result('get_coming_payroll_details').get('date')
    work_day = datetime.strptime(date, "%d/%m/%Y").weekday()
    output['day'] = work_day
    output['date'] = "Value = " + str(output['day'])
    if 2 <= work_day <= 4:
        output['reminder'] = 2
    elif work_day == 1:
        output['reminder'] = 4
    elif work_day == 0:
        output['reminder'] = 3
    else:
        output['reminder'] = -1
    return output


def get_timesheet_period_formated_detail():
    report_data = rail.result('not_submitted_timesheet_report_generation.get_report_result').get(
        'reportGenerationResults')[0].get('payload')
    csvStringIO = StringIO(report_data)
    df = pd.read_csv(csvStringIO, sep=",")
    supervisor_list = df['User Supervisor Name (Current)'].unique()
    output_list = []
    for supervisor in supervisor_list:
        filtered_frame = df.loc[df['User Supervisor Name (Current)']
                                == supervisor]
        output_list.append({"supervisor": supervisor,
                            "daterangevalue":  ";".join(filtered_frame['Timesheet Period'].unique()),
                            "supervisoremail":  ";".join(map(str, filtered_frame['User Supervisor Email address'].unique())),
                            "user":  ";".join(filtered_frame['User Name'].unique()),
                            "supervisoruri":  ";".join(filtered_frame['supervisoruri'].unique())})
    return output_list


def get_new_list_to_process():
    user_list = rail.result('get_user_list')
    date_range = rail.result('get_date_range_list')[-1]
    output_list = []
    for user in user_list:
        output_list.append({
            "user": user,
            "timesheetperiod": date_range
        })

    return output_list


def get_required_details_for_email(dag_run):
    output = {}
    output['supervisor_first_name'] = dag_run.conf['supervisor'].split(',')[-1]
    if dag_run.conf.get('payrolldate'):
        payroll_date = datetime.strptime(
            dag_run.conf['payrolldate'], "%d/%m/%Y")
        output['payrolldate'] = payroll_date.strftime("%m/%d/%Y")
        output['payrolldate_weekday'] = payroll_date.strftime('%A')
    return output
