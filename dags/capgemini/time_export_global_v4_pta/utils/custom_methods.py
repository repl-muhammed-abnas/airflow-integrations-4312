import functools
import pendulum
from dateutil.relativedelta import relativedelta
import rail

def get_task_state(task_id):
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()

def get_date_json(date_obj):
    return {
        "year": date_obj.year,
        "month": date_obj.month,
        "day": date_obj.day
    }

def get_logging_details(config):
    today = pendulum.now(config.time_zone)
    current_time = today.strftime('%Y%m%d_%H%M%S')
    export_end_date = today-relativedelta(months=4, day=31)
    export_start_date = today-relativedelta(months=11, day=1)
    start_date = export_start_date.strftime('%Y%m%d')
    end_date = export_end_date.strftime('%Y%m%d')
    return {
        "current_time": current_time,
        "time_export_filename": f"{config.export_file_prefix}_{start_date}_{end_date}_TIMESHEETS_{current_time}",
        "time_export_filename_nodata" : f"{config.export_file_prefix}_{start_date}_{end_date}_TIMESHEETS_{current_time}N",
        "time_export_filename_cancelled" : f"{config.export_file_prefix}_{start_date}_{end_date}_TIMESHEETS_{current_time}C",
        "export_start_date": export_start_date.strftime("%Y/%m/%d"),
        "export_end_date": export_end_date.strftime("%Y/%m/%d"),
        "export_start_date_json": get_date_json(export_start_date),
        "export_end_date_json": get_date_json(export_end_date),
        "time_zone": config.time_zone
    }

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
        item['Work_Location'], #'Work Location'
        item['Place_Of_Work'], #'Place Of Work'
        item['Place_of_Work__CHE_'], #'Place Of Work (CHE)'
        item['Place_of_Work__ESP_'], #'Place Of Work (ESP)'
        item['Place_of_Work__FRA_'], #'Place Of Work (FRA)'
        item['Place_of_Work__MAR_'], #'Place Of Work (MAR)'
        item['Postal_Code'], #'Postal Code'
        item['Work_Location_CAN'], #'Work Location CAN'
        item['Work_Location_USA'], #'Work Location USA'
        get_batch_creation_datetime(), #'Export Creation Datetime'
        index #'Row Number'
    ]

def get_time_data_log(input_filepath, locations, total_hours, total_records):
    return [
        {
            "processstarted": rail.result("process_start_time"),
            "filename": rail.result("logging_details")["time_export_filename"] + ".csv.pgp",
            "filepath": input_filepath,
            "location": locations,
            "totalhours": total_hours,
            "totalrecords": total_records,
            "daterange": rail.result("logging_details")["export_start_date"] + "-" + rail.result("logging_details")["export_end_date"]
        }
    ]

def get_log_data_rows(item):
    return  [
        item["processstarted"],
        item["filename"],
        item["filepath"],
        item["location"],
        item["totalhours"],
        item["totalrecords"],
        item["daterange"]
    ]
