import functools
import json
import rail
from airflow.models import Variable
from dateutil.relativedelta import relativedelta
import pendulum

null = None

def get_logging_details(time_zone, export_start_date, timeoff_types_task_codes_mapper):
    current_time = pendulum.now(time_zone).strftime('%Y%m%d%H%M%S')
    return {
        "time_export_filename": f"CPA638_MX01_EXPENDITURE_ITEMS_{current_time}",
        "time_export_filename_nodata" : f"{current_time}_Nodata",
        "time_export_filename_cancelled" : f"{current_time}_Cancelled",
        "export_start_date": export_start_date,
        "export_end_date": str((pendulum.now(time_zone)+relativedelta(day=31)).strftime("%Y/%m/%d")),
        "time_zone": time_zone,
        "timeoff_types_task_codes_mapper": json.loads(Variable.get(timeoff_types_task_codes_mapper))
    }

def get_export_datetime(response):
    time_in_utc = response["creationDate"]["valueInUtc"]
    return pendulum.datetime(int(time_in_utc["year"]), int(time_in_utc["month"]), int(time_in_utc["day"]),
                int(time_in_utc["hour"]), int(time_in_utc["minute"]), int(time_in_utc["second"])).strftime("%Y%m%d_%H%M%S")

def get_filtered_allowed_location_uris(response):
    if not response['rows']:
        return []
    return list(set(map(lambda data: data['cells'][0]['uri'], response['rows'])))

@functools.lru_cache(maxsize=128)
def get_export_uri_uniq_identifier():
    return (rail.result("time_data_export.get_export_uri")).split(":")[-1]

@functools.lru_cache(maxsize=128)
def get_batch_creation_datetime():
    return rail.result("get_export_creation_datetime")

@functools.lru_cache(maxsize=128)
def get_timeoff_types_task_codes_mapper():
    return rail.result("logging_details")["timeoff_types_task_codes_mapper"]

def get_time_data_csv_rows(item, index):
    if not item:
        return []
    return {
        'Reference': item["Reference"],
        'Transaction_source': item["Transaction_source"],
        'Batch_name': get_export_uri_uniq_identifier(),
        'Employee_number': item["Employee_number"],
        'Expenditure_item_date': item["Expenditure_item_date"],
        'Project_number': rail.find_first_by_attr_and_get_attr(get_timeoff_types_task_codes_mapper(), "Time_Off_Type",
                item["Time_Off_Type_Name"], "Project_code") if item["Time_Off_Type_Name"] else item["Project_number"],
        'Task_number': rail.find_first_by_attr_and_get_attr(get_timeoff_types_task_codes_mapper(), "Time_Off_Type",
                item["Time_Off_Type_Name"], "Task_Code") if item["Time_Off_Type_Name"] else item["Task_number"],
        'Expenditure_type': "RC_Time Std" if item["Time_Off_Type_Name"] else item["Expenditure_type"],
        'Non_Labor_resource': item["Non_Labor_resource"],
        'Non_Labor_resource_org_name': item["Non_Labor_resource_org_name"],
        'Organization_name': item["Organization_name"],
        'Quantity': item["Quantity"],
        'Expenditure_comment': item["Expenditure_comment"].replace(';', '').replace('\n', ' '),
        'DFF___Start_date': item["DFF___Start_date"],
        'DFF__End_date': item["DFF__End_date"],
        'Quantity_in_days': item["Quantity_in_days"],
        'External_application_unit_of_measure_for_time_entr': item["External_application_unit_of_measure_for_time_entr"],
        'Attribute3': item["Attribute3"],
        'Attribute4': item["Attribute4"],
        'Attribute5': item["Attribute5"],
        'Attribute6': item["Attribute6"],
        'Attribute7': item["Attribute7"],
        'Attribute8': item["Attribute8"],
        'Attribute9': item["Attribute9"],
        'Nb_hours_sup': item["Nb_hours_sup"],
        'Raw_cost': item["Raw_cost"],
        'Raw_cost_rate': item["Raw_cost_rate"],
        'Billable_Flag': item["Billable_Flag"],
        'Export_Creation_Datetime': get_batch_creation_datetime(),
        'Row_Number': index
    }.values()

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
