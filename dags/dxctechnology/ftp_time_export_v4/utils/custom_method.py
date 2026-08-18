import json
import rail
nill = None


def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf


def is_extension_feild(task_value):
    time_export_details = rail.result(task_value)["extensionFieldValues"]
    if time_export_details:
        data = list(filter(lambda x: x["definition"]["displayText"]
                    == "FTP_Payload_Processed", time_export_details))
        if data:
            payload_processed = data[0]["textValue"]
            if payload_processed == "Yes":
                return True
    return False


def get_final_line_data(get_export_name):
    return [{
            'Entryid': '',
            'Uniqueid': json.loads(rail.result(get_export_name))['twbname'],
            'Employeeid': '',
            'Date': '',
            'Wbs': '',
            'Vblen': '',
            'Salesitem': '',
            'Attendancetype': '',
            'Hours': '',
            'Comments': '',
            }
            ]
