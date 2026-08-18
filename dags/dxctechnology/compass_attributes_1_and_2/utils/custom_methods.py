from datetime import date, datetime
import rail

null = None


def get_conf():
    return rail.get_current_context()['dag_run'].conf


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def get_replicon_date(date_str, date_format='%Y%m%d'):
    if not date_str:
        return null
    # date format in 20060401
    try:
        _date = datetime.strptime(date_str, date_format)
        return {
            'year': _date.year,
            'month': _date.month,
            'day': _date.day
        }
    except:  # pylint: disable=bare-except
        return null


def get_start_date(_date, date_format):
    startdate = get_replicon_date(_date, date_format)
    return (str(startdate['year']) + '-'
            + str(startdate['month']) + '-'
            + str(startdate['day'])) if startdate else null


def get_lower_date(first_date, second_date, first_date_format='%Y%m%d', second_date_format='%Y%m%d'):
    if not first_date and not second_date:
        return null

    if not first_date and second_date:
        return get_replicon_date(second_date, second_date_format)

    first_dt = get_replicon_date(first_date, first_date_format)
    second_dt = get_replicon_date(second_date, second_date_format)

    try:
        first_date_obj = date(
            first_dt['year'], first_dt['month'], first_dt['day'])
        second_date_obj = date(
            second_dt['year'], second_dt['month'], second_dt['day'])
        return first_dt if first_date_obj < second_date_obj else second_dt
    except:  # pylint: disable=bare-except
        return null


def get_end_date(project_enddate, attribute_enddate, date_format):
    _date = get_lower_date(
        project_enddate, attribute_enddate, date_format, '%Y%m%d')
    return str(_date['year'])+'-' + \
        str(_date['month'])+'-'+str(_date['day']) if _date else null


def get_userlist(project_details):
    data = rail.result(project_details)
    return list(
        map(lambda x: {
            'uri': x,
            'assignmentstartdate': null,
            'assignmentenddate': null
        }, data)
    )
