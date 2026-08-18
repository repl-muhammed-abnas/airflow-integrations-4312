from datetime import datetime
import json
import pendulum
from airflow.models import Variable
import rail

null = None

def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf

def get_logging_details(config):
    today = pendulum.now(config.time_zone)
    current_time = today.strftime('%Y%m%d_%H%M%S')
    timeoff_list_var = list(map(lambda data: data, filter(lambda data: data["allowed"].lower() == "yes",
                        json.loads(Variable.get(config.required_timeoffs)))))
    return {
        "process_start_time": today.strftime('%Y-%m-%dT%H:%M:%S'),
        "current_time": current_time,
        "payroll_export_filename": f"Payroll_Leave_Request_Data_{current_time}",
        "time_zone": config.time_zone,
        "required_timeoffs": f"""('{"','".join([data['timeoff_type'] for data in timeoff_list_var])}')""",
        "required_timeoffs_mapper": timeoff_list_var
    }

def get_time_data_csv_rows(item):
    export_creation_datetime = get_dag_run_conf()["export_creation_datetime"]
    if not item:
        return []
    return [
        null, #'Entity'
        item['ProjectTime_Local_Employee_Number'], #'Local Employee Number'
        item['ProjectTime_GGID'], #'Employee ID'
        "L" if float(item["ProjectTime_Hours"]) > 0 else ("R" if float(item["ProjectTime_Hours"]) < 0 else null),
        datetime.strptime(item['ProjectTime_Entrydate'], "%d%m%Y").strftime("%d/%m/%Y"), #'Entry Date'
        datetime.strptime(item['ProjectTime_Entrydate'], "%d%m%Y").strftime("%d/%m/%Y"), #'Entry Date'
        rail.find_first_by_attr_and_get_attr(rail.result("logging_details")["required_timeoffs_mapper"],
            'replicon_timeoff_code', item['ProjectTime_Absence_Type_Code'], 'expected_timeoff_code'), #'Time Off Type Description'
        datetime.strptime(export_creation_datetime, "%Y%m%d_%H%M%S").strftime("%d/%m/%Y %H:%M:%S"), #'Modified Date'
        null,
        #'Cost Center Name Full Path'
        item['Cost_Center_Name__Full_Path_'].split(' / ')[1] if len(item['Cost_Center_Name__Full_Path_'].split(' / ')) > 1 else null
    ]
