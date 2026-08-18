# pylint: disable=unused-variable
from datetime import datetime
import os
import rail

# pylint: disable= line-too-long

def check_file_folder():
    file_path = rail.result('new_file_sensor')
    file_name = os.path.split(file_path)[1]
    if file_name not in ('Processing', 'Logs', 'Archive'):
        return True
    return False

def get_filename():
    return "Old_Input_" + rail.result('new_file_sensor_to_process').split('/')[-1].split('.')[0] \
        + "_" + datetime.now().strftime("%m_%d_%Y_T%H_%M_%S") + "_" + ".csv"

def get_today():
    return datetime.now().strftime("%m/%d/%Y")

def get_csv_datalength():
    data = rail.load_all_records(rail.result('load_csv'))
    print(data)
    return len(data)

def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf

def get_user_uri_by_loginname(response, dag_run):
    user_uris = [item['cells'][0]['uri'] for item in response['rows']
                 if item['cells'][0]['textValue'] == dag_run.conf['loginname']] if response['rows'] else []
    return rail.smartjoin_by_delim(user_uris) if user_uris else ''

def get_required_employee_type_uri(dag_run):
    return rail.find_first_by_attr_and_get_attr(rail.result('get_all_employee_type_details'), 'displayText', dag_run.conf['employeetype'], 'uri', '')

def get_dept_uri_data(dag_run):
    data = rail.result('get_enabled_department')
    provided_parent_list = dag_run.conf['department'].split('/')[:-1]
    for i, item in enumerate(data):
        if item['displayText'] == dag_run.conf['department'].split('/')[-1] and \
            len(item['parentDepartments']) == len(provided_parent_list):
            parent_list = []
            for j in range(0,len(item['parentDepartments'])):
                parent_list.append(item['parentDepartments'][j]['displayText'])
            if parent_list == provided_parent_list:
                return item['uri']
    return None

def get_supervisor_uri_by_loginname(response, dag_run):
    supervisor_uris = [item['cells'][0]['uri'] for item in response['rows']
                 if item['cells'][0]['textValue'] == dag_run.conf['initialsupervisorloginname']] if response['rows'] else []
    return rail.smartjoin_by_delim(supervisor_uris) if supervisor_uris else ''

def get_holiday_calender_uri(dag_run):
    return rail.find_first_by_attr_and_get_attr(rail.result('get_all_holiday_calendar'), 'displayText', dag_run.conf['holidaycalendar'], 'uri', '')

def get_daterange_from_profile():
    start_date = rail.result('get_user_details')['employmentDateRange']['startDate']
    end_date = rail.result('get_user_details')['employmentDateRange']['endDate']
    startdate = ''
    enddate = ''
    if end_date is not None:
        enddate = str(end_date['day']) + "/" + str(end_date['month']) + "/" + str(end_date['year'])
    if start_date is not None:
        startdate = str(start_date['day']) + "/" + str(start_date['month']) + "/" + str(start_date['year'])

    return {
        "start_date" : startdate,
        "end_date" : enddate
    }
