from hashlib import sha256
import json
import pendulum
from airflow.models import Variable
import rail

def get_logging_details(time_zone):
    today = pendulum.now(time_zone)
    current_time = today.strftime('%Y%m%d_%H%M%S')
    return {
        "time_zone": time_zone,
        "process_start_time": today.strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "export_filename": f"victoriashipyards_supervisor_delta_{current_time}"
    }

def get_report_parameters():
    return {
        "reportParameters": [
            {
                "reportUri": rail.result('get_report_details')["uri"],
                "filterValues": [],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }

def get_changed_records_query(can_use_reference_file):
    if Variable.get(can_use_reference_file, default_var='true').lower() == 'true':
        return """SELECT * FROM users_supervisor_sha256_collection WHERE sha256 NOT IN (SELECT DISTINCT sha256 FROM users_supervisor_reference_data)"""
    return """SELECT * FROM users_supervisor_sha256_collection"""

def users_supervisor_csv_data(item):
    record_data = {
        'employeeid': item.get('employeeid', ''),
        'useruri': item.get('useruri'),
        'supervisoremployeeid': item.get('supervisoremployeeid', ''),
        'supervisoruri': item.get('supervisoruri')
    }

    sha256_hash = sha256(json.dumps(record_data, sort_keys=True).encode()).hexdigest()

    return [
        item.get('username', ''),
        item.get('employeeid', ''),
        item.get('supervisorname', ''),
        item.get('supervisoremail', ''),
        item.get('supervisoremployeeid', ''),
        sha256_hash
    ]

def get_users_supervisor_data_rows(item):
    return [
        item["username"],
        item["employeeid"],
        item["supervisorname"],
        item["supervisoremail"],
        item["supervisoremployeeid"]
    ]
