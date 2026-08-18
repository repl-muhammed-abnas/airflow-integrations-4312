from airflow.exceptions import AirflowFailException
import rail


null = None

data_types = {
    'object_list_type_uri': 'urn:replicon:list-type:object',
    'null_list_type_uri': 'urn:replicon:list-type:null'
}


def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf


def map_user_by_location(response):
    user_list = []
    if response and response['rows']:
        user_list = [
            {
                'user': rail.find_first_by_attr_and_get_attr(x['cells'], 'dataType', data_types['object_list_type_uri'], 'textValue'),
                'user_uri': rail.find_first_by_attr_and_get_attr(x['cells'], 'dataType', data_types['object_list_type_uri'], 'uri')
            } for x in response['rows'] if x['cells'][1]['dataType'] != data_types['null_list_type_uri']
        ]
    return user_list


def get_first_user_with_timesheet_template(response):
    user_uri = None
    filtered_users = [x['userDetails']
                      for x in response if x['timesheetTemplate'] and len(x['assignedProducts']) > 0]
    if response and len(filtered_users) > 0:
        user_uri = filtered_users[0]['uri']
    return user_uri


def format_user_list_from_batch(batch_result):
    user_list = []
    if batch_result and len([x['cells'][0]['uri'] for x in batch_result if x['cells']]) > 0:
        user_list = [{
            'text_value': x['cells'][0]['textValue'],
            'uri': x['cells'][0]['uri'],
            'user_index': i + 1
        } for i, x in enumerate(batch_result)]
    return user_list


def map_timedata_batch(response):
    time_data_batch = {}
    if response:
        time_data_batch = {
            'error': response.get('error'),
            'user_list': format_user_list_from_batch(response['listData']['rows'])
        }
    return time_data_batch


def retrieve_export_uri(response):
    if response['error']:
        raise AirflowFailException(response)
    return response['timeDataExportUri']

def get_chargecode(row,dag_run_conf):
    if row['TimeEntryId']:
        return row['ChargeCode']
    if ('TimeOffTypeName' in row) and (row['TimeOffTypeName']):
        chargecode_filter_list = list(filter(lambda x:x['location'] == dag_run_conf['location'] and x['timeofftype_name'] == row['TimeOffTypeName'], dag_run_conf['timeofftype_chargecode_mapper']))
        if chargecode_filter_list:
            return chargecode_filter_list[0]['chargecode']

    return dag_run_conf['chargecode']

def translate_rows(row):
    dag_run_conf = get_dag_run_conf()
    ignored_keys = ('TimeEntryId', 'Timesheet Start Date', 'Timesheet End Date',
                    'Comments', 'WorkLocation', 'ChargeCode', 'Time Off Booking ID', 'Entry ID')
    if row:
        return {
            **{k: v for k, v in row.items() if k not in ignored_keys},
            **{
                'TimeEntryId': row['TimeEntryId'] if row['TimeEntryId'] else row['Entry ID'] if (('TimeOffTypeName' in row) and (row['TimeOffTypeName'])) else row['Time Off Booking ID'],
                'Timesheet Start Date': row['Timesheet Start Date'].split("-")[0].strip() if row['Timesheet Start Date'] else null,
                'Timesheet End Date': row['Timesheet End Date'].split("-")[-1].strip() if row['Timesheet Start Date'] and row[
                    'Timesheet End Date'] else null,
                'Comments': row['Comments'].replace('\n', ' ') if row['Comments'] else null,
                'WorkLocation': row['WorkLocation'] if row['TimeEntryId'] else dag_run_conf['location'],
                'ChargeCode': get_chargecode(row,dag_run_conf)
            }
        }
    return None
