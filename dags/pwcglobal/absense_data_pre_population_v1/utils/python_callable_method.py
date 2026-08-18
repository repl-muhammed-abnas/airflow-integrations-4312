from datetime import datetime
import rail
from pwcglobal.absense_data_pre_population_v1.utils import custom_method

null = None


def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf


def validate_transaction_date(time_entry_item):
    return time_entry_item['TransactionDate']


def validate_hours_quantity(time_entry_item):
    return (float(time_entry_item['HoursQuantity']
                  ) if time_entry_item['HoursQuantity'] else 0) >= 0


def validate_charge_code(time_entry_item):
    return time_entry_item['ChargeCode'] and time_entry_item['ChargeCode']['ChargeCode']


def validate_alternate_identifier_value(time_entry_item):
    partyIdentifierItem = time_entry_item['InternalWorkRelationship']['PartyAlternateIdentifier']
    return rail.find_first_by_attr_and_get_attr(partyIdentifierItem,
                                                'AlternateIdentifierType', 'PwC GUID', 'AlternateIdentifierValue')


def filter_records_of_time_entry_data():
    dag_run_conf = get_dag_run_conf()
    payload = dag_run_conf['webhook']['data']['PersonResourceActualTime']
    valid_time_entry_payloads = []
    invalid_time_entry_payloads = []

    for index, item in enumerate(payload):
        item['record_id']= index
        if (validate_transaction_date(item) and validate_hours_quantity(
                item) and validate_charge_code(item) and validate_alternate_identifier_value(item)):
            valid_time_entry_payloads.append(item)
        else:
            invalid_time_entry_payloads.append(item)

    return {
        "valid_time_entry_data": valid_time_entry_payloads,
        "invalid_time_entry_data": invalid_time_entry_payloads
    }


def get_task_uri(dag_run):
    data = rail.result("get_all_project_tasks")
    if dag_run.conf['ChargeCode']['WorkItem']['WorkItemType'] and \
            rail.find_first_by_attr_and_get_attr(data, 'taskname', dag_run.conf['ChargeCode']['WorkItem']['WorkItemType'], 'uri'):
        return rail.find_first_by_attr_and_get_attr(data, 'taskname', dag_run.conf['ChargeCode']['WorkItem']['WorkItemType'], 'uri')
    return data[0]['uri'] if data and data[0]['uri'] else null


def add_item_to_team_members_list(dag_run):
    projects = rail.result("get_all_project_team_members")
    members_list = []
    for project in projects:
        if (project['resource']['user'] and project['resource']['user']['loginName'] == dag_run.conf['userloginname']):
            members_list.append(project['resource']['user']['uri'])
    return members_list


def update_task_projectmetadata():
    task_or_project_uri = null
    if rail.result("get_task_uri"):
        keyUri = "urn:replicon:time-entry-metadata-key:task"
        task_or_project_uri = rail.result("get_task_uri")
    else:
        keyUri = "urn:replicon:time-entry-metadata-key:project"
        task_or_project_uri = rail.result('search_project_with_code')[0]['projecturi'] if rail.result(
            'search_project_with_code')[0]['timeentryallowedflag'] is True else null

    return {
        "keyUri": keyUri,
        "value": {
            "uri": task_or_project_uri,
            "slug": null,
            "bool": null,
            "date": null,
            "number": null,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null,
            "collection": []
        }
    } if task_or_project_uri else null


def get_oef_for_wdid(dag_run):
    return {
        "definition": {
            "uri": dag_run.conf['wdidoefuri'],
            "name": null
        },
        "textValue": dag_run.conf['TimeEntryID']
    } if dag_run.conf['TimeEntryID'] else null


def get_work_type_oef_uri(dag_run, worktype_mapper):
    oef_tag_definition_uri = null
    if dag_run.conf['WorkType'] and dag_run.conf['worktypeoefuri']:
        position = rail.find_first_by_attr_and_get_attr(worktype_mapper, 'location', rail.result("search_users")[0]['location'], 'position')
        if position:
            oef_tag_definition_uri = (dag_run.conf['worktypeoefuri']).split('|')[position-1]
            return oef_tag_definition_uri

    return oef_tag_definition_uri


def get_comments(dag_run):
    return {
        "keyUri": "urn:replicon:time-entry-metadata-key:comments",
        "value": {
            "uri": null,
            "slug": null,
            "bool": null,
            "date": null,
            "number": null,
            "text": dag_run.conf['Comments'],
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null,
            "collection": []
        }
    } if dag_run.conf['Comments'] else null


def check_existing_teammembers_uri():
    return True if not rail.result('add_item_to_team_members_list') else [rail.result('search_users')[0]['useruri'] not in rail.result(
        'add_item_to_team_members_list')]


def get_log_properties(action, status):
    return {
        'timeentryid': '{{ dag_run.conf.TimeEntryID }}',
        'userpartyid': '{{ dag_run.conf.InternalWorkRelationship.InternalPerson.PartyId }}',
        'action': action,
        'status': status,
        'details': 'Time entry date:{{dag_run.conf.TransactionDate}}|Hours:{{dag_run.conf.HoursQuantity}}\
            |Project-Task:{{dag_run.conf.ChargeCode.ChargeCode}}-{{dag_run.conf.ChargeCode.WorkItem.WorkItemType}}'.replace(" ", ""),
        'unitloggeddatetime': '{{ current_time() }}'
    }


def get_oef_for_work_location(oef_result_by_search):
    dag_run_conf = get_dag_run_conf()
    if rail.result(oef_result_by_search):
        wloc_uri = rail.find_first_by_attr_and_get_attr(
            rail.result(oef_result_by_search),
            'displayText',
            dag_run_conf['WorkLocation'],
            'uri')
        if wloc_uri:
            return {
                'definition': {
                    'uri': dag_run_conf['worklocationoefuri'],
                    'name': null
                },
                'tag': {
                    'uri': wloc_uri
                }
            }
    return null


def get_oef_for_work_type(oef_result_by_customsearch, work_type_oef_uri):
    dag_run_conf = get_dag_run_conf()
    if rail.result(oef_result_by_customsearch):
        worktype_uri = rail.find_first_by_attr_and_get_attr(
            rail.result(oef_result_by_customsearch),
            'displayText',
            dag_run_conf['WorkType'],
            'uri')
        if worktype_uri:
            return {
                'definition': {
                    'uri': rail.result(work_type_oef_uri),
                    'name': null
                },
                'tag': {
                    'uri': worktype_uri
                }
            }
    return null


def add_items_to_oef_values(oef_for_wdid, oef_for_work_loc, oef_for_work_type):
    oef_list = []
    if rail.result(oef_for_wdid):
        oef_list.append(rail.result(oef_for_wdid))
    if rail.result(oef_for_work_loc):
        oef_list.append(rail.result(oef_for_work_loc))
    if rail.result(oef_for_work_type):
        oef_list.append(rail.result(oef_for_work_type))
    return oef_list


def get_time_transaction_details(oef_values_list, team_members_list):
    dag_run_conf = get_dag_run_conf()
    return {
        'entrydate': datetime.strptime(dag_run_conf['TransactionDate'], '%Y%m%d').strftime('%Y-%m-%d'),
        'hours': dag_run_conf['HoursQuantity'].split(".")[0],
        'mins': int(float(dag_run_conf['HoursQuantity'].split(".")[1]) * 60),
        'entrydate_day': custom_method.get_replicon_date(dag_run_conf['TransactionDate'])['day'],
        'entrydate_month': custom_method.get_replicon_date(dag_run_conf['TransactionDate'])['month'],
        'entrydate_year': custom_method.get_replicon_date(dag_run_conf['TransactionDate'])['year'],
        'oefs_to_apply': rail.result(oef_values_list),
        'existingteammembers': rail.result(team_members_list)
    }
