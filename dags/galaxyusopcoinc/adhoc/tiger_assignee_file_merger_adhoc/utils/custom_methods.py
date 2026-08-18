import hashlib
from functools import lru_cache
from datetime import datetime
from airflow.models import Variable
import rail
from rail.lib.ecid import get_dagrun_ecid

def has_any_file(result_task_id, input_file_path):
    if not result_task_id or not input_file_path:
        raise Exception(
            "Task_id" if not result_task_id else "input path" + "is not provided")
    data = rail.result(result_task_id)
    if not data:
        return False
    return len(data[input_file_path]) > 0

def get_sorted_files(config):
    file_names = []
    file_merge_count = int(Variable.get(config.file_merge_count, default_var='200'))
    files_in_input_dir = rail.result("list_import_files").get(
        config.input_filepath)
    if files_in_input_dir:
        files_in_input_dir.sort(key=lambda s: datetime.strptime(
            s['modify'], '%Y%m%d%H%M%S'), reverse=False)
        index = 1
        for item in files_in_input_dir:
            if index <= file_merge_count:
                file_names.append({
                    "file_name": item["name"],
                    "file_index": index,
                    "modify": item["modify"]
                })
            index += 1
    return file_names

def get_query(dag_run):
    return f"SELECT {dag_run.conf['file_index']} as sequence_number, \
        '{dag_run.conf['file_name']}' as file_name, '{dag_run.conf['file_date_time']}' as file_date_time, * FROM inputdata"

def get_md5_data(raw_data):
    md5_data = []
    for item in raw_data:
        item['md5'] = hashlib.md5(
            (str(item['assigneeid']) + "," + str(item['clientshortname']) +","+ str(item['sequence_number'])).encode('utf-8')).hexdigest()
        item['sourcefilerecordcount'] = len(raw_data)
        md5_data.append(item)
    return md5_data

def get_merged_data():
    combined_data = []
    files_in_input_dir = rail.result("gather_raw_data")
    for file_data in files_in_input_dir:
        raw_file = rail.load_all_records(file_data)
        md5_data = get_md5_data(raw_file)
        combined_data = combined_data + md5_data
    return combined_data

@lru_cache(maxsize=32)
def get_current_file_time():
    return rail.result('get_time_for_file')

def translate_row(item, dag_run):
    return{
        'clientlongname': item['clientlongname'] if item['clientlongname'] else "",
        'clientshortname': item['clientshortname'],
        'assigneeid': item['assigneeid'],
        'firstname': item['firstname'],
        'lastname': item['lastname'],
        'status': item['status'],
        'filedatetime': item['file_date_time'],
        'sourcefilename': item['file_name'],
        'sourcefilerecordcount': item['sourcefilerecordcount'],
        'sequenceno': item['sequence_number'],
        'md5': item['md5'],
        'ignored': item['ignored'],
        'jobid': get_dagrun_ecid(dag_run),
        'mergedfilename': f"Tiger_Assignee_Mergeddata_{get_current_file_time()}.csv"
    }.values()
