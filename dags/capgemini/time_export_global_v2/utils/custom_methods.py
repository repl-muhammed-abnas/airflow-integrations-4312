from datetime import datetime as dt, timedelta, date
import functools
import pendulum
from dateutil.relativedelta import relativedelta
import rail

null = None

def get_task_state(task_id):
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()

def get_date_json(date_param):
    date_obj = dt.strptime(date_param, "%Y-%m-%d")
    return {
        "year": date_obj.year,
        "month": date_obj.month,
        "day": date_obj.day
    }

def get_date_in_req_format(date_str, date_format, req_format):
    return dt.strptime(date_str, date_format).strftime(req_format)

def get_export_file_names(export_file_prefix, start_date, end_date, current_time):
    return {
        "time_export_filename": f"{export_file_prefix}_{start_date}_{end_date}_TIMESHEETS_{current_time}",
        "time_export_filename_nodata" : f"{export_file_prefix}_{start_date}_{end_date}_TIMESHEETS_{current_time}N",
        "time_export_filename_cancelled" : f"{export_file_prefix}_{start_date}_{end_date}_TIMESHEETS_{current_time}C",
    }

def get_past_and_current_time_export_date_range_json(config, today):
    current_time = today.strftime('%Y%m%d_%H%M%S')
    past_months_to_export = 3
    past_and_current_months_date_range_list = list(map(lambda month_num: {
        "start_date": (today-relativedelta(months=month_num, day=1)).strftime("%Y-%m-%d"),
        "end_date": (today-relativedelta(months=month_num, day=31)).strftime("%Y-%m-%d")
    }, reversed(range(1, past_months_to_export+1)))) + rail.result("get_timesheet_periods_var")["value"]

    return list(map(lambda daterange: {
        "export_filename": get_export_file_names(config.export_file_prefix,
            get_date_in_req_format(daterange['start_date'], "%Y-%m-%d", "%Y%m%d"),
                get_date_in_req_format(daterange['end_date'], "%Y-%m-%d", "%Y%m%d"), current_time),
        "export_start_date": get_date_in_req_format(daterange['start_date'], "%Y-%m-%d", "%Y/%m/%d"),
        "export_end_date": get_date_in_req_format(daterange['end_date'], "%Y-%m-%d", "%Y/%m/%d"),
        "export_start_date_json": get_date_json(daterange['start_date']),
        "export_end_date_json": get_date_json(daterange['end_date']),
        "time_zone": config.time_zone
    }, past_and_current_months_date_range_list))

def get_export_datetime(response):
    time_in_utc = response["creationDate"]["valueInUtc"]
    return pendulum.datetime(int(time_in_utc["year"]), int(time_in_utc["month"]), int(time_in_utc["day"]),
                int(time_in_utc["hour"]), int(time_in_utc["minute"]), int(time_in_utc["second"])).strftime("%Y%m%d_%H%M%S")

def get_filtered_restricted_location_uris(response):
    if not response['rows']:
        return []
    return list(set(map(lambda data: data['cells'][0]['uri'], response['rows'])))


@functools.lru_cache(maxsize=128)
def get_batch_creation_datetime():
    return rail.result("get_export_creation_datetime")

@functools.lru_cache(maxsize=128)
def get_time_export_uri():
    return (rail.result("time_data_export.get_export_uri")).split(":")[-1]

def get_time_data_csv_rows(item, index):
    if not item:
        return []
    return [
        item['ProjectTime__Project_Time_ID'], #'Short Time Entry ID'
        get_time_export_uri(), #'Batch_name'
        item['ProjectTime_GGID'], #'Employee ID'
        item['ProjectTime_Local_Employee_Number'], #'Local Employee Number'
        item['Cost_Center_Name'], #'Cost Center Name'
        item['Project__CostCenterID_PU_'], #'Cost Center Code'
        item['Market_Unit_Name'], #'Market Unit Name'
        item['Market_Unit_Code'], #'Market Unit Code'
        item['Employee_Type_Name'], #'Employee Type Name'
        item['Employee_Employee_Contract_Type'], #'Employee Type Code'
        item['Location_Name'], #'Location Name'
        item['Employee_Office_City_Code'], #'Location Code'
        item['User_Status'], #'User Status'
        item['Employee_GGID'], #'Login Name'
        item['Employee_Email'], #'Email Address'
        item['Employee_People_Manager_GGID'], #'Current Supervisor'
        item['Employee_Group'], #'Employee Group'
        item['Employee__EmployeeCategory'], #'Employee Category'
        item['Employee_Global_Grade'], #'Employee Grade'
        item['Employee__HRBP_Manager_GGID'], #'People Partner'
        item['ProjectTime_Entrydate'], #'Entry Date'
        item['Timesheet_Period'], #'Timesheet Period'
        item['ProjectTime_ApprovalStatus'], #'Approval Status'
        item['ProjectTime_Hours'], #'Hours'
        item['ProjectTime_Comments'].replace(';', '').replace('\n', ' '), #'Comments'
        item['ProjectTime_ProjectName'], #'Project Name'
        item['ProjectTime_ProjectID'], #'Project Code'
        item['Source_System'], #'Source System'
        item['Project_ProjectType'], #'Project Type'
        item['ProjectTime_TaskName'], #'Task Name'
        item['ProjectTime_TaskCode'], #'Task Code'
        item['Billability'], #'Billing Rate Name'
        item['ProjectTime_ActivityName'], #'Activity Name'
        item['ProjectTime_ActivityCode'], #'Activity Code'
        item['Client_Name'], #'Client Name'
        item['Client_Code'], #'Client Code'
        item['ProjectTime_Absence_Type_Name'], #'Time Off Type Name'
        item['ProjectTime_Absence_Type_Code'], #'Time Off Type Description'
        item['Export_Number'], #'Export Number'
        item['Unit_of_Measure'], #'Unit of Measure'
        get_batch_creation_datetime(), #'Export Creation Datetime'
        index #'Row Number'
    ]

def users_data_with_timesheet_template(response):
    for user_data in response:
        if user_data['timesheetTemplate']:
            return user_data["userDetails"]["uri"]
    return null

def get_specific_location_uri(response, location):
    return rail.find_first_by_attr_and_get_attr(response, "displayText", location, "uri")

def get_updated_start_date():
    prev_timesheet_end_date = rail.result("get_timesheet_details")["timesheet"]["dateRange"]["endDate"]
    return (date(prev_timesheet_end_date["year"], prev_timesheet_end_date["month"], prev_timesheet_end_date["day"])
            + timedelta(days=1)).strftime("%Y-%m-%d")

def get_timesheet_period():
    timesheet_daterange = rail.result("get_timesheet_details")["timesheet"]["dateRange"]
    timesheet_start_date = timesheet_daterange["startDate"]
    timesheet_end_date = timesheet_daterange["endDate"]
    return {
        "start_date": f'{timesheet_start_date["year"]}-{timesheet_start_date["month"]}-{timesheet_start_date["day"]}',
        "end_date": f'{timesheet_end_date["year"]}-{timesheet_end_date["month"]}-{timesheet_end_date["day"]}'
    }
