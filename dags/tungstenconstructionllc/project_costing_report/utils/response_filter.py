# pylint: disable=too-many-statements
from datetime import datetime
from rail import find_first_by_attr_and_get_attr

def get_file_format_uri(response):
    return find_first_by_attr_and_get_attr(response, 'displayText', 'CSV', 'uri')

def get_start_end_date(dag_run):
    date = (dag_run.conf['webhook']['data'].get('payload',{}).get('daterange', '')).split('-')
    return {
        'start_date': datetime.strptime(date[0], '%m%d%Y').strftime("%m/%d/%Y"),
        'end_date': datetime.strptime(date[1], '%m%d%Y').strftime("%m/%d/%Y"),
    }
