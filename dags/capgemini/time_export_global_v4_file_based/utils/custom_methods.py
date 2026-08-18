from datetime import datetime
import functools
import pendulum
import rail

null = None

def get_task_state(task_id):
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()

def get_conf():
    return rail.get_current_context()['dag_run'].conf

def get_date_json(date_param):
    date_obj = datetime.strptime(date_param, "%Y-%m-%d")
    return {
        "year": date_obj.year,
        "month": date_obj.month,
        "day": date_obj.day
    }

def get_export_period(dag_run):
    start_date = (dag_run.conf["time_export_period"]).split(" - ")[0]
    end_date = (dag_run.conf["time_export_period"]).split(" - ")[1]
    return {
        "export_start_date": datetime.strptime(start_date, "%b %d, %Y").strftime("%Y/%m/%d"),
        "export_end_date": datetime.strptime(end_date, "%b %d, %Y").strftime("%Y/%m/%d")
    }

def get_date_in_req_format(date_str, date_format, req_format):
    return datetime.strptime(date_str, date_format).strftime(req_format)

def get_export_datetime(response):
    time_in_utc = response["creationDate"]["valueInUtc"]
    return pendulum.datetime(int(time_in_utc["year"]), int(time_in_utc["month"]), int(time_in_utc["day"]),
                int(time_in_utc["hour"]), int(time_in_utc["minute"]), int(time_in_utc["second"])).strftime("%Y%m%d_%H%M%S")

@functools.lru_cache(maxsize=128)
def get_batch_creation_datetime():
    return rail.result("get_export_creation_datetime")

@functools.lru_cache(maxsize=128)
def get_time_export_uri():
    return (get_conf()["time_export_uri"]).split(":")[-1]

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

def get_time_data_log(dag_run, input_filepath, locations, total_hours, total_records):
    return [
        {
            "processstarted": rail.result("process_start_time"),
            "filename": dag_run.conf["time_export_name"] + ".csv.pgp",
            "filepath": input_filepath,
            "location": locations,
            "totalhours": total_hours,
            "totalrecords": total_records,
            "daterange": rail.result("export_period")["export_start_date"] + "-" + rail.result("export_period")["export_end_date"]
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
