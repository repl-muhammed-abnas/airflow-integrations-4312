from datetime import datetime
import hashlib
import rail

null = None


def get_create_md5_data(item):
    if not item:
        return []
    res = {
        **dict(item.items()),
        **{
            'md5': hashlib.md5((item["projectname"]+","+item["projectcode"]+","+item["projectdescription"]+","
                                + item["status"]+"," + item["allowtimeentry"] +
                                "," + item["startdate"]
                                + item["enddate"]+"," + item["projectmanager"] +
                                "," + item["costtype"]
                                + item["projectleaderapprovalrequired"]+"," +
                                item["invoicecurrency"]+"," +
                                item["tasknamelevel1"]
                                + item["taskcode"]+"," + item["taskstatus"] +
                                "," + item["customfieldregistration"]
                                ).encode('utf-8')).hexdigest()
        }
    }

    return {k: v if v is not None else '' for k, v in res.items()}


def get_process_each_code_conf(item):
    return {
        'item': item,
        'data_artifact': rail.result('get_delta_records'),
        'log': rail.result("create_log")
    }


def get_project_details(dag_run):
    return {
        "page": "1",
        "pagesize": "1000",
        "columnUris": [
            "urn:replicon:project-list-column:project",
            "urn:replicon:project-list-column:code"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:project-list-filter:code"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": null,
                    "uris": [],
                    "bool": null,
                    "date": null,
                    "money": null,
                    "number": null,
                    "text": dag_run.conf['item']['projectcode'],
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def get_update_project_conf(dag_run):
    return {
        'projectcode': dag_run.conf['item']['projectcode'],
        'data': rail.load_all_records(dag_run.conf['data_artifact']),
        'projectdetails': rail.result('get_project_details'),
        'log': dag_run.conf['log']
    }


def get_create_project_conf(dag_run):
    return {
        'projectcode': dag_run.conf['item']['projectcode'],
        'data': rail.load_all_records(dag_run.conf['data_artifact']),
        'log': dag_run.conf['log']
    }


def get_project_data(dag_run):
    return {
        "projectUri": dag_run.conf['projectdetails'][0]['cells'][0]['uri']
    }


def is_name_updated(dag_run):
    current_name = rail.result('get_project_data')['name']
    new_name = rail.result(
        'get_project_records')[-1]['projectname'] + " - " + dag_run.conf['projectcode']
    if current_name != new_name:
        return True
    return False


def get_update_name(dag_run):
    return {
        "projectUri": dag_run.conf['projectdetails'][0]['cells'][0]['uri'],
        "name": rail.result('get_project_records')[0]['projectname'] + " - " + dag_run.conf['projectcode']
    }


def is_start_date_present():
    return bool(rail.result('get_project_records')[-1]['startdate'])


def is_start_date_correct():
    return len(rail.result('get_project_records')[-1]['startdate']) == 8


def is_end_date_correct():
    return len(rail.result('get_project_records')[-1]['enddate']) == 8


def is_end_date_present():
    return bool(rail.result('get_project_records')[-1]['enddate'])


def get_log_start_date_incorrect():
    return {
        'projectcode': rail.result('get_project_records')[-1]['projectcode']+"/"+rail.result('get_project_records')[-1]['projectname'],
        'taskcode': '-',
        'status': 'Failed',
        'details': 'Invalid Start Date'
    }


def get_log_end_date_incorrect():
    return {
        'projectcode': rail.result('get_project_records')[-1]['projectcode']+"/"+rail.result('get_project_records')[-1]['projectname'],
        'taskcode': '-',
        'status': 'Failed',
        'details': 'Invalid End Date'
    }


def is_project_date_present():
    date = rail.result('get_project_data')['timeEntryDateRange']
    if date['endDate'] or date['startDate']:
        return True
    return False


def is_start_end_in_feed_file():
    return is_start_date_present() and is_end_date_present()


def get_replicon_date(date_str):
    if not date_str:
        return None

    try:
        date = datetime.strptime(date_str, '%m%d%Y')
        return {
            'year': date.year,
            'month': date.month,
            'day': date.day
        }
    except:  # pylint: disable=bare-except
        return None


def update_date_range_project(dag_run):
    return {
        "projectUri": dag_run.conf['projectdetails'][0]['cells'][0]['uri'],
        "dateRange": {
            "startDate": get_replicon_date(rail.result('get_project_records')[-1]['startdate']),
            "endDate":  get_replicon_date(rail.result('get_project_records')[-1]['enddate']),
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        }
    }


def is_start_date_feed_file():
    return is_start_date_present() and not is_end_date_present()


def update_start_date_project(dag_run):
    return {
        "projectUri": dag_run.conf['projectdetails'][0]['cells'][0]['uri'],
        "dateRange": {
            "startDate": get_replicon_date(rail.result('get_project_records')[-1]['startdate']),
            "endDate":  null,
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        }
    }


def is_project_description_present():
    return bool(rail.result('get_project_records')[-1]['projectdescription'])


def update_description(dag_run):
    return {
        "projectUri": dag_run.conf['projectdetails'][0]['cells'][0]['uri'],
        "description": rail.result('get_project_records')[-1]['projectdescription']
    }


def is_cutsom_field_registered():
    return bool(rail.result('get_project_records')[-1]['customfieldregistration'] == "Registered")


def is_cutsom_field_non_registered():
    return bool(rail.result('get_project_records')[-1]['customfieldregistration'] == "Non Registered")


def update_dropdown_registered(dag_run):
    data = rail.result('get_enabled_custom_field_dropdown_option')
    for data_uri in data:
        if data_uri['displayText'] == 'Registered':
            registered_uri = data_uri['uri']
    return {
        "objectUri": dag_run.conf['projectdetails'][0]['cells'][0]['uri'],
        "customFieldUri": rail.result('get_registration_udf_uri')[0]['uri'],
        "customFieldDropDownOptionUri": registered_uri
    }


def create_dropdown_registered():
    data = rail.result('get_enabled_custom_field_dropdown_option')
    for data_uri in data:
        if data_uri['displayText'] == 'Registered':
            registered_uri = data_uri['uri']
    return {
        "objectUri": rail.result("create_new_draft"),
        "customFieldUri": rail.result('get_registration_udf_uri')[0]['uri'],
        "customFieldDropDownOptionUri": registered_uri
    }


def update_dropdown_non_registered(dag_run):
    data = rail.result('get_enabled_custom_field_dropdown_option')
    for data_uri in data:
        if data_uri['displayText'] == 'Non Registered':
            non_registered_uri = data_uri['uri']
    return {
        "objectUri": dag_run.conf['projectdetails'][0]['cells'][0]['uri'],
        "customFieldUri": rail.result('get_registration_udf_uri')[0]['uri'],
        "customFieldDropDownOptionUri": non_registered_uri
    }


def create_dropdown_non_registered():
    data = rail.result('get_enabled_custom_field_dropdown_option')
    for data_uri in data:
        if data_uri['displayText'] == 'Non Registered':
            non_registered_uri = data_uri['uri']
    return {
        "objectUri": rail.result("create_new_draft"),
        "customFieldUri": rail.result('get_registration_udf_uri')[0]['uri'],
        "customFieldDropDownOptionUri": non_registered_uri
    }


def is_custom_field_non_present():
    return not rail.result('get_project_records')[-1]['customfieldregistration']


def update_dropdown_non_present(dag_run):
    return {
        "objectUri": dag_run.conf['projectdetails'][0]['cells'][0]['uri'],
        "customFieldUri": rail.result('get_registration_udf_uri')[0]['uri'],
        "customFieldDropDownOptionUri": null
    }


def create_dropdown_non_present():
    return {
        "objectUri": rail.result("create_new_draft"),
        "customFieldUri": rail.result('get_registration_udf_uri')[0]['uri'],
        "customFieldDropDownOptionUri": null
    }


def update_project_status(dag_run):
    return {
        "projectUri": dag_run.conf['projectdetails'][0]['cells'][0]['uri'],
        "projectStatusUri": "urn:replicon:project-status-type:completed"
    }


def log_project_updated():
    return {
        'projectcode': rail.result('get_project_records')[-1]['projectcode']+"/"+rail.result('get_project_records')[-1]['projectname'],
        'taskcode': '-',
        'status': 'Success',
        'details': 'Project Updated'
    }


def log_task_updated(dag_run):
    return {
        'projectcode': dag_run.conf['projectcode'],
        'taskcode': dag_run.conf['taskcode'] + '/' + dag_run.conf['taskname'],
        'status': 'Success',
        'details': 'Task Updated'
    }

def get_children_task_details(dag_run):
    return {
        "parentUri": dag_run.conf['projectdetails'][0]['cells'][0]['uri']
    }


def process_each_task_conf(dag_run, item):
    return {
        'items': item,
        'children_task': rail.result('get_children_task_details'),
        'projectdetails': dag_run.conf['projectdetails'],
        'create': 'N',
        'log': dag_run.conf['log']
    }


def process_create_each_task_conf(dag_run, item):
    return {
        'items': item,
        'children_task': rail.result('get_children_task_details'),
        'projectdetails': rail.result('get_create_project_details'),
        'create': 'Y',
        'log': dag_run.conf['log']
    }


def get_task_details(dag_run):
    data = dag_run.conf['children_task']
    if dag_run.conf['items']['taskcode']:
        return list(filter(lambda x: x['code'] == dag_run.conf['items']['taskcode'], data))
    return []


def is_project_task_present(dag_run):
    return dag_run.conf['items']['taskcode'] and rail.result('get_task_details')


def is_project_task_not_present(dag_run):
    return dag_run.conf['items']['taskcode'] and not rail.result('get_task_details')


def process_update_task_conf(dag_run):
    return {
        'taskuri': rail.result('get_task_details')[0]['uri'],
        'taskname': dag_run.conf['items']['tasknamelevel1'],
        'enddate': dag_run.conf['items']['enddate'],
        'taskstatus': dag_run.conf['items']['taskstatus'],
        'projectcode': dag_run.conf['items']['projectcode'],
        'taskcode': dag_run.conf['items']['taskcode'],
        'log': dag_run.conf['log']
    }


def process_create_task_conf(dag_run):
    return {
        'taskname': dag_run.conf['items']['tasknamelevel1'],
        'enddate': dag_run.conf['items']['enddate'],
        'taskstatus': dag_run.conf['items']['taskstatus'],
        'projectcode': dag_run.conf['items']['projectcode'],
        'taskcode': dag_run.conf['items']['taskcode'],
        'projectUri': dag_run.conf['projectdetails'][0]['cells'][0]['uri'] if dag_run.conf['create'] == "N" else dag_run.conf['projectdetails']['uri'],
        'startdate': dag_run.conf['items']['startdate'],
        'childTask': dag_run.conf['children_task'],
        'projectdetails': dag_run.conf['projectdetails'],
        'log': dag_run.conf['log']
    }


def get_task_info(dag_run):
    return {
        "taskUri": dag_run.conf['taskuri']
    }


def is_task_name_different(dag_run):
    task_name_project = rail.result("get_task_info")['name']
    task_name_field_file = dag_run.conf['taskname'] + \
        " - " + dag_run.conf['taskcode']
    return task_name_project != task_name_field_file


def update_task_name(dag_run):
    return {"taskUri": dag_run.conf['taskuri'], "name": dag_run.conf['taskname'] + " - " + dag_run.conf['taskcode']}


def update_allow_time_entry(dag_run):
    return {
        "taskUri": dag_run.conf['taskuri'],
        "allowTimeEntry": "true"
    }


def is_task_status_open(dag_run):
    return dag_run.conf['taskstatus'] == "Open"


def task_status(dag_run):
    return {
        "taskUri": dag_run.conf['taskuri']
    }


def is_end_date_task_present(dag_run):
    return bool(dag_run.conf['enddate'])


def update_end_allow_time_entry(dag_run):
    return {
        "taskUri": dag_run.conf['taskuri'],
        "allowTimeEntry": "false"
    }


def is_task_status_closed(dag_run):
    return dag_run.conf['taskstatus'] == "Closed"


def create_task_draft(dag_run):
    return {
        "parentUri": dag_run.conf['projectUri']
    }


def create_task_name(dag_run):
    return {"taskUri": rail.result("create_task_draft"), "name": dag_run.conf['taskname'] + " - " + dag_run.conf['taskcode']}


def create_task_code(dag_run):
    return {
        "taskUri": rail.result("create_task_draft"),
        "code": dag_run.conf['taskcode']
    }


def create_allow_time_entry():
    return {
        "taskUri": rail.result("publish_draft")["uri"],
        "allowTimeEntry": "true"
    }


def update_cost_type():
    return {
        "taskUri": rail.result("publish_draft")["uri"],
        "costTypeUri": null
    }


def bulk_update_resource_assignment():
    return {
        "taskUri": rail.result("publish_draft")["uri"],
        "resourceUris": [
            rail.result("get_resource_department_uri")
        ],
        "isAssigned": "true"
    }


def task_create_status():
    return {
        "taskUri": rail.result("publish_draft")["uri"]
    }


def is_create_start_date_present(dag_run):
    return bool(dag_run.conf['startdate'])


def update_timeentry_date_range(dag_run):
    return {
        "taskUri": rail.result("publish_draft")["uri"],
        "dateRange": {
            "startDate": get_replicon_date(dag_run.conf['startdate']),
            "endDate": null
        }
    }


def is_create_end_date_present(dag_run):
    return bool(dag_run.conf['enddate'])


def update_create_allow_time_entry():
    return {
        "taskUri": rail.result("publish_draft")["uri"],
        "allowTimeEntry": "false"
    }
