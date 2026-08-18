import itertools
from datetime import datetime
from moodys.monthly_shift_assignment.utils import custom_methods

null = None


def page_handler(request, result):
    if len(result['rows']) > 0:
        request['page'] += 1
        return request
    return null


def all_result_data_handler(result):
    flaten_rows = list(itertools.chain(
        *list(map(lambda x: x['rows'], result))))
    shiftlistoutput = list(map(lambda row: {
        'shift': row['cells'][0]['textValue'],
        'enabled': row['cells'][1]['textValue'] if 'textValue' in row['cells'][1] else null
    }, flaten_rows))
    return bool([shift for shift in shiftlistoutput if shift['shift']
                 == custom_methods.get_dag_run_conf()['shiftname']])


def get_assigned_shift_dates(response):
    shift_list = []
    for shift_detail in response:
        date = datetime(
            shift_detail['date']['year'], shift_detail['date']['month'], shift_detail['date']['day'])
        str_date = date.strftime("%Y/%m/%d")
        week_number = date.isocalendar(
        )[1]+1 if date.weekday() == 6 else date.isocalendar()[1]
        shift = {
            'date': str_date,
            'week_day': date.weekday(),
            'week': week_number,
            'shift': shift_detail['shift']['displayText'],
            'assignmenturi': shift_detail['assignmentUri']
        }
        shift_list.append(shift)
    return shift_list
