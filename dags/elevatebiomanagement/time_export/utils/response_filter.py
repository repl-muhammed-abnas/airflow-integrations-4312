import json
from airflow.exceptions import AirflowFailException
import rail

null = None

def get_conf():
    return rail.get_current_context()['dag_run'].conf

def get_extract_file(response):
    data = response.json()["files"]
    return list(filter(lambda x: x['name'] == "Repicon Extract.csv", data))


def get_export_uri(response):
    for timeoff_script in response.json()['d']:
        if timeoff_script['displayText'] == 'Time Export':
            return timeoff_script['uri']
    raise Exception('Unable to locate script Time Off Export')

def load_csv(columns):
    time_data = rail.load_all_records(get_conf()["time_data"])
    csv_data = ','.join(['"'+ x +'"' for x in columns])
    for row in time_data:
        csv_data = csv_data + '\n' + ','.join(['"'+ x +'"' for x in list(row.values())])
    return '"'+csv_data+'"'

def retrieve_export_uri(response):
    if response['error']:
        raise AirflowFailException(response)
    return response['timeDataExportUri']

def get_time_export_status(response):
    return response["status"]["displayText"]

def filter_response(response):
    return json.loads(response.text) if json.loads(response.text) else null
