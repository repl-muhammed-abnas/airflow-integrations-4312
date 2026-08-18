import hashlib
from datetime import datetime
from airflow.models import Variable
import rail


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def get_wbs_dataset():
    query_wbs_data = rail.result("query_wbs_data")
    raw_file = get_data_from_document(query_wbs_data)
    final_wbs_data = []
    for wbs_info in raw_file:
        final_wbs_data.append(
            {k: v if v is not None else '' for k, v in wbs_info.items()})
    return final_wbs_data


def get_merged_wbs_data():
    wbs_combined_data = []
    files_in_input_dir = rail.result("gather_wbs_raw_data")
    for file_data in files_in_input_dir:
        raw_file = get_data_from_document(file_data)
        wbs_md5_data = get_wbs_md5_data(raw_file)
        wbs_combined_data = wbs_combined_data + wbs_md5_data
    return wbs_combined_data


def get_wbs_md5_data(wbs_datas):
    wbs_md5_data = []
    for wbs_data in wbs_datas:
        wbs_data['md5'] = hashlib.md5(
            (str(wbs_data['WBS']) + "," + str(wbs_data['sequance_number'])).encode('utf-8')).hexdigest()
        wbs_data['sourcefilerecordcount'] = len(wbs_datas)
        wbs_md5_data.append(wbs_data)
    return wbs_md5_data


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


def get_data_from_compared_wbs_files():
    query_wbs_data = rail.result("query_wbs_data")
    consolidated_wbs_data = get_data_from_document(query_wbs_data)
    raw_wbs_data = rail.result("merge_all_wbs_files")
    for raw_wbs in raw_wbs_data:
        if rail.find_first_by_attr_and_get_attr(consolidated_wbs_data, "md5", raw_wbs['md5'], "WBS"):
            raw_wbs['ignored'] = 'No'
        else:
            raw_wbs['ignored'] = 'Yes'
    return raw_wbs_data
