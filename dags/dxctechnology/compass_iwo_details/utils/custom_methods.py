from datetime import date, datetime
import rail

null = None


def get_dag_conf():
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


def get_string_date(entrydate):
    return date(entrydate['year'], entrydate['month'], entrydate['day']).strftime('%Y%m%d') if entrydate else null


def check_date_doesnotequal(first_date, second_date, first_date_format='%Y%m%d', second_date_format='%Y%m%d'):
    first_dt = get_replicon_date(first_date, first_date_format)
    second_dt = get_replicon_date(second_date, second_date_format)

    first_date_obj = date(first_dt['year'], first_dt['month'], first_dt['day'])
    second_date_obj = date(
        second_dt['year'], second_dt['month'], second_dt['day'])

    return first_date_obj != second_date_obj


def get_project_team_members(project_team_assingment_results):
    return [
        x['uri'] for x in rail.result(project_team_assingment_results) if x['uri']
    ] if rail.result(project_team_assingment_results) else []


def get_create_existing_blob(item):
    if not item:
        return []
    return {k.lower(): v if v is not null else '' for k, v in item.items()
            if k in ('wbsUri', 'wbsName', 'labourType', 'labourTypeUri', 'startDate', 'endDate')} if item else {}


def check_item_category_present():
    item_category_parent_value = rail.find_first_by_attr_and_get_attr(rail.result('get_parent_project_details')[
        'extensionFieldValues'], 'definition.displayText', 'Item Category', 'tag.displayText') if rail.result('get_parent_project_details')[
        'extensionFieldValues'] else null
    if not item_category_parent_value:
        return False

    item_category_child_value = rail.find_first_by_attr_and_get_attr(rail.result('get_child_project_details')[
        'extensionFieldValues'], 'definition.displayText', 'Item Category', 'tag.displayText') if rail.result('get_child_project_details')[
        'extensionFieldValues'] else null
    return item_category_child_value != item_category_parent_value
