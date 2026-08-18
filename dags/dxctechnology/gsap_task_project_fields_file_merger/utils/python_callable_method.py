import hashlib
from datetime import datetime
from airflow.models import Variable
import rail

def get_tasks_dataset():
    query_tasks_data = rail.result("query_tasks_data")
    raw_file = rail.load_all_records(query_tasks_data)
    final_tasks_data = []
    for tasks_info in raw_file:
        final_tasks_data.append(
            {k: v if v is not None else '' for k, v in tasks_info.items()})
    return final_tasks_data


def get_merged_tasks_data():
    tasks_combined_data = []
    files_in_input_dir = rail.result("gather_tasks_raw_data")
    for file_data in files_in_input_dir:
        raw_file = rail.load_all_records(file_data)
        tasks_md5_data = get_tasks_md5_data(raw_file)
        tasks_combined_data = tasks_combined_data + tasks_md5_data
    return tasks_combined_data


def get_tasks_md5_data(tasks_data):
    tasks_md5_data = []
    for data in tasks_data:
        data['md5'] = hashlib.md5(
            (str(data['WBS_Name']) + ","+ str(data['Task_Name']) +
             "," + str(data['sequance_number'])).encode('utf-8')).hexdigest()
        data['sourcefilerecordcount'] = len(tasks_data)
        tasks_md5_data.append(data)
    return tasks_md5_data


def get_query(dag_run):
    return f"SELECT {dag_run.conf['file_index']} as sequance_number, \
        '{dag_run.conf['file_name']}' as file_name, '{dag_run.conf['file_date_time']}' as file_date_time, * FROM inputdata"


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


def get_data_from_compared_tasks_files():
    query_tasks_data = rail.result("query_tasks_data")
    consolidated_data = rail.load_all_records(query_tasks_data)
    raw_tasks_data = rail.result("merge_all_tasks_files")
    for raw_tasks in raw_tasks_data:
        if rail.find_first_by_attr_and_get_attr(consolidated_data, "md5", raw_tasks['md5'], "WBS_Name"):
            raw_tasks['ignored'] = 'No'
        else:
            raw_tasks['ignored'] = 'Yes'
    return raw_tasks_data
