from datetime import datetime
import functools
import pendulum
from dateutil.relativedelta import relativedelta
import rail

null = None

CONF_DATE_FORMAT = "%m/%d/%Y"
TWB_DATE_FORMAT = "%Y%m%d"
EXPORT_DATE_FORMAT = "%Y-%m-%d"

def get_conf():
    return rail.get_current_context()['dag_run'].conf

def get_date_json(date_obj):
    return {
        "year": date_obj.year,
        "month": date_obj.month,
        "day": date_obj.day
    }

def get_logging_details(time_zone, export_file_prefix):
    today = pendulum.now(time_zone)
    current_time = today.strftime('%Y%m%d_%H%M%S')
    export_end_date = datetime.strptime(get_conf()["end_date"], CONF_DATE_FORMAT) if get_conf() else today
    export_start_date = datetime.strptime(get_conf()["start_date"], CONF_DATE_FORMAT) if get_conf() else today-relativedelta(months=3)
    return {
        "current_time": current_time,
        "time_export_filename": f"{export_file_prefix}_{current_time}",
        "time_export_filename_nodata" : f"{export_file_prefix}_{current_time}_N",
        "time_export_filename_cancelled" : f"{export_file_prefix}_{current_time}_C",
        "export_start_date": export_start_date.strftime("%Y/%m/%d"),
        "export_end_date": export_end_date.strftime("%Y/%m/%d"),
        "export_start_date_json": get_date_json(export_start_date),
        "export_end_date_json": get_date_json(export_end_date),
        "time_zone": time_zone
    }

@functools.lru_cache(maxsize=128)
def get_time_export_uri_guid():
    return (rail.result("time_data_export.get_export_uri")).split(":")[-1]

def get_time_data_csv_rows(item):
    if not item:
        return []

    # Generate batch ID and description using export URI
    batch_id = f"REPLICON{get_time_export_uri_guid()}"
    batch_description = f"{batch_id}_{item['Export_Creation_Date']}"

    # Expenditure Type conditional logic
    expenditure_type = item['Time_Types_Code']  # Default value

    if item['Time_Types_Name'] == 'Regular' and item['User_Type_Name_Full_Path']:
        user_type_level_1 = item['User_Type_Name_Full_Path'].split("/")[0].strip()
        if user_type_level_1 == 'Employee':
            expenditure_type = 'Unisys Labor'
        elif user_type_level_1 == 'Contingent Worker':
            expenditure_type = 'Contractor Labor T&M'

    entry_date = datetime.strptime(item['Entry_Date'], TWB_DATE_FORMAT).strftime(EXPORT_DATE_FORMAT)

    return [
        'LABOR',  # Transaction Type
        item['Fusion_Business_Unit'],  # Business Unit
        'Replicon',  # Third-Party Application Transaction Source
        'Time Card',  # Document
        '',  # Document Entry
        batch_id,  # Expenditure Batch
        batch_description,  # Batch Description
        entry_date,  # Entry Date
        '',  # Person Name
        item['Employee_ID'],  # Person Number
        '',  # Human Resource Assignment
        '',  # Project Name
        item['Project_Code'],  # Project Number
        '',  # Task Name
        item['Task_Code'],  # Task Number
        expenditure_type,  # Expenditure Type
        '',  # Expenditure Organization
        item['Hours'],  # Quantity
        'Hours',  # Unit of Measure
        item['Time_Entry_ID'],  # Original Transaction Reference
        'PJC_ALL'  # Context Category
    ]
