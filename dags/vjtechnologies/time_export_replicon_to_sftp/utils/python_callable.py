from datetime import datetime
import json
from vjtechnologies.time_export_replicon_to_sftp.mapper import sftp_mapper
import rail

def get_current_date_time():
    return datetime.now().strftime("%Y%m%d%H%M%S")

def get_row(item):
    return [
        datetime.strptime(item["entrydate"], "%m/%d/%Y").strftime("%d-%b-%y"),
        item["companycodecode"],
        item["employeeid"],
        item["projectname"],
        item["projectcode"],
        item["taskname"],
        item["taskcode"],
        item["taskdescription"],
        item["hours"]
    ]

def check_sftp_input_filepath():
    input_filepath = None
    archive_filepath = None
    for i,val in enumerate(sftp_mapper.mapper):
        if val['type'] == "timesync" and val['company_code'] == rail.result('get_division_details')['code'].lower():
            if val['identifier'] == "Input":
                input_filepath = sftp_mapper.mapper[i]['sftp_path']
            if val['identifier'] == "Archive":
                archive_filepath = sftp_mapper.mapper[i]['sftp_path']

    return {
        'input_filepath' : input_filepath,
        'archive_filepath' : archive_filepath
    }

def check_filename(path):
    sftp_file_path_dict = {}
    archive_list = []
    input_file_path = path + rail.result('check_for_sftp_filepath')['input_filepath']
    archive_file_path = path + rail.result('check_for_sftp_filepath')['archive_filepath']
    sftp_file_list = rail.result('list_sftp_files').get(input_file_path)
    if sftp_file_list :
        for i, val in enumerate(sftp_file_list):
            sftp_file_path_dict[i] = {}
            if val['name']:
                sftp_file_path_dict[i]['input_path'] = input_file_path + "/" + sftp_file_list[i]['name']
                sftp_file_path_dict[i]['archive_path'] = archive_file_path + "/archive_" + sftp_file_list[i]['name']

                archive_list.append(sftp_file_path_dict[i])

    return archive_list

def filter_company_slug_list(config, res):
    filtered_data = [element for element in res if element['slug'] not in config.EXCLUDE_COMPANY_SLUG_LIST]
    return json.dumps(filtered_data)
