from functools import lru_cache
from dateutil.parser import parse as date_parser
import pendulum
import rail


EXPORT_DATE_FORMAT = "%Y/%m/%d"
EXPORT_FILE_TIMESTAMP = "%Y%m%d%H%M%S"

PROJECT_CATEGORY = {
    'C': '0800',
    'I': '0081',  # Changed from '0080' to '0081'
    'E': '0080'   # Changed from '0081' to '0080'
}

null = None


def get_timeexport_fileformat(config, response):
    file_format = rail.find_first_by_attr_and_get_attr(
        response, 'displayText', config.time_export_file_format, 'uri')
    if file_format:
        return file_format
    raise Exception(
        f'Unable to locate script `{config.time_export_file_format}`')


def get_time_export_name(config):
    offset_time = pendulum.now(
        config.time_zone).strftime(EXPORT_FILE_TIMESTAMP)
    return {
        "time_export_name": f"Time_Extract_{offset_time}",
        "no_data_time_export_name": f"Time_Extract_{offset_time}_NO_DATA",
        "no_valid_data_time_export_name": f"Time_Extract_{offset_time}_NO_VALID_DATA"
    }


def convert_date_to_export_formate(date_string: str) -> str:
    return date_parser(date_string).strftime(EXPORT_DATE_FORMAT)

@lru_cache(maxsize=8)
def get_cached_billing_rates():
    return rail.result('get_all_billing_rates')

def get_billing_rate_description(billing_name):
    return rail.find_first_by_attr_and_get_attr(
        get_cached_billing_rates(), 'name', billing_name, 'description', '')

def final_export_data_callable(item):
    if not item:
        return []

    return {
        'TimeEntryID': item['timeentry_id'],
        'ControllingArea': item['controlling_area'],
        'UserCostCenter': item['cost_center'],
        'BillingRate': get_billing_rate_description(item['billing_rate']),
        'EmployeeID': item['employee_id'],
        'WorkforceID': item['workforce_id'],
        'TimeEntryDate': convert_date_to_export_formate(item['timeentry_date']),
        'TaskName': item['taskname'],
        'Hours': item['hours'],
        'Comments': item['comments'],
        'UserServiceCenter': item['service_center'],
        'WorkLocation': item['work_location'],
        'Onsite_Remote': item['onsite_remote'],
        'ProjectCode': item['project_code'],
        'BillingControlCategory': item['billing_control_category'],
        'ProjectCategory': item['project_category'],
        'TimeOffTypeName': item['time_off_type_name'],
        'TimeOffTypeDescription': item['time_off_type_description']
    }

def get_workitem(task):
    task = task.split(" / ")
    return task[0] if task and len(task) == 1 else ""

def get_purchase_order_and_item(task):
    task = task.split(" / ")
    res = {
        "PurchaseOrder": "",
        "PurchaseOrderItem": "",
    }
    if len(task) >= 2:
        res = {
        "PurchaseOrder": task[0],
        "PurchaseOrderItem": task[1],
    }
    return res

def get_billing_control_cat(billing_control_cat):
    return "NON_BILL" if billing_control_cat == 'Non-Billable' else ""


def create_s4hc_json_payload_callable(task_id):
    data = rail.load_all_records(rail.result(task_id))
    data = list(filter(lambda x: not bool(x['TimeOffTypeName']), rail.load_all_records(rail.result(task_id))))
    res = [
        {
            "TimeEntryID": item['TimeEntryID'],
            "ControllingArea": item['ControllingArea'],
            "UserCostCenter": item['UserCostCenter'],
            "BillingRate": item['BillingRate'],
            "EmployeeID": item['EmployeeID'],
            "WorkforceID": item['WorkforceID'],
            "TimeSheetDate": item['TimeEntryDate'],
            "WBSElement": item['ProjectCode'],
            "WorkItem": "",
            "RecordedHours": item['Hours'],
            "HoursUnitOfMeasure": "H",
            "TimeSheetStatus": "30",
            "TimeSheetNote": item['Comments'],
            "CompanyCode": item['UserServiceCenter'],
            "TimeSheetWrkLocCode": item['WorkLocation'],
            "YY1_Workplace": item['Onsite_Remote'],
            "PurchaseOrder": get_purchase_order_and_item(item['TaskName'])['PurchaseOrder'],
            "PurchaseOrderItem": get_purchase_order_and_item(item['TaskName'])['PurchaseOrderItem'],
            "BillingControlCategory": get_billing_control_cat(item['BillingControlCategory'])
        } for item in data
    ]
    rail.set_result(key='length', val=len(data))
    return rail.write_json_artifact(res)

def get_absencetype(item):
    if item['TimeOffTypeName']:
        return item['TimeOffTypeDescription']
    task = item['TaskName'].split(" / ")
    task = task[-1] if task else ""
    if item['ProjectCategory'] == "Replicon Internal":
        return task
    return PROJECT_CATEGORY.get(item['ProjectCategory'].upper(), "")

def create_h4s4_json_payload_callable(task_id):
    data = rail.load_all_records(rail.result(task_id))
    res = [
        {
            "TimeEntryID": item['TimeEntryID'],
            "EmployeeID": item['EmployeeID'],
            "WorkforceID": item['WorkforceID'],
            "StartDate": item['TimeEntryDate'],
            "EndDate": item['TimeEntryDate'],
            "WBSElement": item['ProjectCode'],
            "RecordedHours": item['Hours'],
            "HoursUnitOfMeasure": "H",
            "TimeSheetNote": item['Comments'],
            "Attendance_AbsenceType": get_absencetype(item)
        } for item in data
    ]
    rail.set_result(key='length', val=len(res))
    return rail.write_json_artifact(res)
