# pylint: disable=inconsistent-return-statements
import time
from datetime import datetime, timedelta
from io import StringIO
from pytz import timezone
import rail
import pandas as pd


def check_for_trigger_day():
    days = {
        "sunday": True,
        "monday": True,
        "tuesday": True,
        "wednesday": True,
        "thursday": True,
        "friday": True,
        "saturday": True,
        "time_zone": "Pacific Time (US & Canada)",
        "hour": "10",
        "minute": "00"
    }
    time_now = datetime.now()
    central = timezone('US/Pacific')
    published_cst = time_now.astimezone(central)
    if days.get(published_cst.strftime('%A').lower()):
        return int(published_cst.strftime('%d')) == 1


def get_required_date_data():
    time_now = datetime.now()
    central = timezone('US/Pacific')
    published_cst = time_now.astimezone(central)
    output = {}
    published_cst2 = datetime(published_cst.year, published_cst.month, 1)
    output['month_start_date'] = published_cst.strftime('%Y-%m-%d')
    previous_date = published_cst2 - timedelta(days=1)
    output['previous_date1'] = previous_date.strftime('%Y-%m-%d')
    output['previous_date2'] = previous_date.strftime("%Y%m%d")
    output['previous_date3'] = previous_date.strftime("%m/%d/%Y")

    # date formatfor today's date
    output['date_today_format1'] = published_cst.strftime("%Y%m%d")
    output['date_today_format2'] = published_cst.strftime("%m/%d/%Y")

    # dates for 50 days back
    previous_date_50_days_back = published_cst - timedelta(days=50)
    output['previous_date_50_days_back'] = previous_date_50_days_back.strftime(
        '%Y-%m-%d')
    month_start_date_50_days_back = datetime(
        previous_date_50_days_back.year, previous_date_50_days_back.month, 1)
    output['month_start_date_50_days_back'] = month_start_date_50_days_back.strftime(
        '%Y-%m-%d')
    month_start_weekday_50_days_back = month_start_date_50_days_back.weekday()
    output['month_first_weekday_50_days_back'] = month_start_weekday_50_days_back
    output['month_first_weekday_value_50_days_back'] = f"Value = {month_start_weekday_50_days_back}"
    output['days_till_second_saturday_50_days_back'] = get_value_for_weekday(
        output['month_first_weekday_value_50_days_back'])
    output['date_on_second_saturday_50_days_back'] = (month_start_date_50_days_back +
                                                      timedelta(days=output['days_till_second_saturday_50_days_back'])).strftime('%Y-%m-%d')
    output['sunday_after_second_saturday_50_days_back'] = (month_start_date_50_days_back + timedelta(
        days=output['days_till_second_saturday_50_days_back']) + timedelta(days=1)).strftime('%Y-%m-%d')
    output['timesheet_period_start_date'] = datetime.strptime(
        output['sunday_after_second_saturday_50_days_back'], '%Y-%m-%d').strftime("%m/%d/%Y")

    previous_date_10_days_back = published_cst - timedelta(days=10)
    output['previous_date_10_days_back'] = previous_date_10_days_back.strftime(
        '%Y-%m-%d')
    output['month_start_date_10_days_back'] = datetime(previous_date_10_days_back.year, previous_date_10_days_back.month, 1).strftime(
        '%Y-%m-%d')
    output['month_first_weekday_10_days_back'] = datetime(
        previous_date_10_days_back.year, previous_date_10_days_back.month, 1).weekday()
    output['month_first_weekday_value_10_days_back'] = f"Value = {output['month_first_weekday_10_days_back']}"
    output['days_till_second_saturday_10_days_back'] = get_value_for_weekday(
        output['month_first_weekday_value_10_days_back'])
    output['date_on_second_saturday_10_days_back'] = (datetime(previous_date_10_days_back.year, previous_date_10_days_back.month, 1) +
                                                      timedelta(days=output['days_till_second_saturday_10_days_back'])).strftime('%Y-%m-%d')
    output['timesheet_period_end_date'] = datetime.strptime(
        output['date_on_second_saturday_10_days_back'], '%Y-%m-%d').strftime("%m/%d/%Y")
    return output


def get_date_today():
    time_now = datetime.now()
    central = timezone('S/Pacific')
    published_cst = time_now.astimezone(central)
    previous_date = published_cst - timedelta(days=1)
    return previous_date.strftime("%d/%m/%Y")


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
    return output_list


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


def get_cost_center_list():
    output_list = []
    if rail.result("get_all_costcenters").get("rows"):
        for costcenter in rail.result("get_all_costcenters").get("rows"):
            entry = costcenter.get("cells")
            output_list.append(
                {
                    "name": entry[0]["textValue"],
                    "fullpath": "|".join([x['textValue'] for x in entry[1]["cellCollection"]]),
                    "uri": entry[0]["uri"],
                    "id": entry[0]["uri"].split(":")[-1],
                    "status": entry[2]["textValue"],
                }
            )
    return output_list


def get_cost_center_list_to_process():
    output_list = []
    cost_center_filter = rail.find_first_by_attr_and_get_attr(
        rail.result('get_costcenter_report_details')['filterConfiguration']['enabledFilters'], 'displayText', "CostCenterFilter", 'uri')
    for record in rail.load_all_records(rail.result("query_costcenter_with_dtna_eng")):
        output_list.append({
            "value": record.get('id'),
            "reportFilterUri": cost_center_filter
        })
    return output_list


def get_value_for_weekday(message):
    if "6" in message:
        result = "13"
    elif "0" in message:
        result = "12"
    elif "1" in message:
        result = "11"
    elif "2" in message:
        result = "10"
    elif "3" in message:
        result = "9"
    elif "4" in message:
        result = "8"
    elif "5" in message:
        result = "7"
    else:
        result = None

    result = int(result) if result is not None else None
    return result


def wait_for_batch():
    time.sleep(3)  # Sleep for 3 seconds
    return "wait over"


def get_result_formated():
    data_with_quantitysign_plus = []
    data_with_quantitysign_minus = []
    csv_data = get_data_from_document(
        rail.result('parse_csv_for_batch_results'))
    run_data = rail.result('get_required_data_for_run')
    for data in csv_data:
        if data.get('Time & Expense Entry Type') != "Non-Billable":
            if data.get('Hours Worked'):
                if data.get('Hours Worked') and float(data.get('Hours Worked', "").replace(",", "")) > 0:
                    data_with_quantitysign_plus.append({
                        "sendingsystemid": "DTNA512",
                        "documenttypecode": "2D",
                        "postingdate": run_data.get('previous_date2'),
                        "documentheadertext": " Replicon DAA ENG Hours",
                        "documentdate": run_data.get('date_today_format1'),
                        "debitcostcenterid": data.get('COST_CENTER_NAME (Current)'),
                        "ccreferencefield_1": " ",
                        "debititemtext": "0" + data.get('Project Name').lstrip(),
                        "ioreferencefield_1": "0" + data.get('Project Name').lstrip(),
                        "ioreferencefield_2": data.get('COST_CENTER_NAME (Current)'),
                        "ioreferencefield_3": " ",
                        "quantitysign": "+",
                        "quantity": data.get('Hours Worked').replace(",", ""),
                        "baseunitofmeasure": "HR",
                        "controllingareaid": "4001",
                        "activitytypecode": "102008"
                    })
                if data.get('Hours Worked') and float(data.get('Hours Worked', "").replace(",", "")) < 0:
                    data_with_quantitysign_minus.append({
                        "sendingsystemid": "DTNA512",
                        "documenttypecode": "2D",
                        "postingdate": run_data.get('previous_date2'),
                        "documentheadertext": " Replicon DAA ENG Hours",
                        "documentdate": run_data.get('date_today_format1'),
                        "debitcostcenterid": data.get('COST_CENTER_NAME (Current)'),
                        "ccreferencefield_1": " ",
                        "debititemtext": "0" + data.get('Project Name').lstrip(),
                        "ioreferencefield_1": "0" + data.get('Project Name').lstrip(),
                        "ioreferencefield_2": data.get('COST_CENTER_NAME (Current)'),
                        "ioreferencefield_3": " ",
                        "quantitysign": "-",
                        "quantity": data.get('Hours Worked').replace(",", ""),
                        "baseunitofmeasure": "HR",
                        "controllingareaid": "4001",
                        "activitytypecode": "102008"
                    })
    return {'plus': data_with_quantitysign_plus, 'minus': data_with_quantitysign_minus}


def get_existing_filename(path1, path2):
    file_names = {}
    file_names['existing_filename'] = path1 + "/" + \
        rail.result('foreach_file_in_directory').get('name')
    file_names['new_filename'] = path2 + "/Archived_" + \
        rail.result('foreach_file_in_directory').get(
            'name').replace(".csv", "") + ".csv"
    return file_names
