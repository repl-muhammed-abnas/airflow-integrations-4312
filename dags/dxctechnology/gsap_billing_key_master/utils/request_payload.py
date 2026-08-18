import rail
from dxctechnology.gsap_billing_key_master.utils import custom_methods

null = None


def get_process_each_wbs(item):
    return {
        'wbs': item['wbs'],
        'taskName': item['taskName'],
        'taskCode': item['taskCode'],
        'tasktypeoptionuri':
        rail.find_first_by_attr_and_get_attr(rail.result(
            'get_all_customfield_drop_down_options'), 'displayText', 'GSAP Billing Key', 'uri'),
        'tasktypeuri': rail.find_first_by_attr_and_get_attr(rail.result(
            'get_all_customfields'), 'displayText', 'Task Type', 'uri'),
        'level2tasktypeoptionuri':
        rail.find_first_by_attr_and_get_attr(rail.result(
            'get_all_customfield_drop_down_options'), 'displayText', 'GSAP Task', 'uri'),
    }

def get_process_unique_wbs_conf(item):
    return {
        'wbs': item['wbs'],
        'tasktypeoptionuri':
        rail.find_first_by_attr_and_get_attr(rail.result(
            'get_all_customfield_drop_down_options'), 'displayText', 'GSAP Billing Key', 'uri'),
        'tasktypeuri': rail.find_first_by_attr_and_get_attr(rail.result(
            'get_all_customfields'), 'displayText', 'Task Type', 'uri'),
        'level2tasktypeoptionuri':
        rail.find_first_by_attr_and_get_attr(rail.result(
            'get_all_customfield_drop_down_options'), 'displayText', 'GSAP Task', 'uri'),
    }

def get_create_task_conf(dag_run, item, project_details, team_member_details):
    return {
        'action': item['action'],
        'name': item['taskName'],
        'description': item['taskCode'] if item['taskCode'] else null,
        'tasktypeuri': dag_run.conf['tasktypeuri'],
        'tasktypeoptionuri': dag_run.conf['tasktypeoptionuri'],
        'projecturi': rail.result(project_details)['uri'],
        'userlist': custom_methods.get_userlist(team_member_details),
        'projectname': rail.result(project_details)['name'],
        'enddate': custom_methods.get_replicon_date(rail.result('get_project_date_range')['enddate'], '%d/%m/%Y'),
        'startdate': custom_methods.get_replicon_date(rail.result('get_project_date_range')['startdate'], '%d/%m/%Y'),
        'attribute1uri': item['attribute1uri'],
        'level2tasktypeoptionuri': dag_run.conf['level2tasktypeoptionuri']
    }


def get_put_task_data(dag_run, parent):
    return {
        "project": {"uri": dag_run.conf['projecturi']},
        "task": {
            "target": {
                "name": dag_run.conf['name'],
                "parent": {"uri": dag_run.conf['parenttaskuri']} if parent else null
            },
            "name": dag_run.conf['name'],
            "code": dag_run.conf['description'],
            "timeEntryDateRange": null,
            "customFieldValues": [
                {
                    # `Task Type`
                    "customField": {"uri": dag_run.conf['tasktypeuri']},
                    # `GSAP Billing Key`
                    "dropDownOption": {"uri": dag_run.conf['tasktypeoptionuri']},
                }
            ],
            "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable",
            "percentCompleted": 0,
            "isTimeEntryAllowed": True,
            "isClosed": False,
            "assignedResources": [{'uri': user['uri']} for user in dag_run.conf['userlist']],
        }
    }

def get_child_wbs_payload_new(dag_run):
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:project-list-column:project",
            rail.result('get_all_columns')[0]['uri']
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": rail.result('get_all_filter_defination')[0]['uri']
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
                    "text": dag_run.conf['wbs'],
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null,
                    "dateTimeUtcRange": null,
                    "numberRange": null
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }
def get_child_wbs_payload():
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:project-list-column:project",
            rail.result('get_all_columns')[0]['uri']
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": rail.result('get_all_filter_defination')[0]['uri']
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
                    "text": custom_methods.get_conf()['projectname'],
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null,
                    "dateTimeUtcRange": null,
                    "numberRange": null
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def get_process_child_wbs_config(item):
    return {
        'taskName': custom_methods.get_conf()['name'],
        'taskCode': custom_methods.get_conf()['description'] if custom_methods.get_conf()['description'] else null,
        'tasktypeuri': custom_methods.get_conf()['tasktypeuri'],
        'tasktypeoptionuri': custom_methods.get_conf()['tasktypeoptionuri'],
        'projectname': custom_methods.get_conf()['projectname'],
        'childwbs': item.split(" - ")[0].strip(),
        'level2tasktypeoptionuri': custom_methods.get_conf()['level2tasktypeoptionuri']
    }


def get_put_task_child_data(dag_run, parent):
    return {
        "project": {"uri": rail.result('get_child_project_details_based_on_wbs')['uri']},
        "task": {
            "target": {
                "name": dag_run.conf['taskName'],
                "parent": {"uri": dag_run.conf['parenttaskuri']} if parent else null
            },
            "name": dag_run.conf['taskName'],
            "code": dag_run.conf['taskCode'],
            "timeEntryDateRange": null,
            "customFieldValues": [
                {
                    "customField": {"uri": dag_run.conf['tasktypeuri']},
                    "dropDownOption": {"uri": dag_run.conf['tasktypeoptionuri']},
                }
            ],
            "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable",
            "percentCompleted": 0,
            "isTimeEntryAllowed": True,
            "isClosed": False,
            "assignedResources": [{'uri': user['uri']} for user in custom_methods.get_userlist('get_all_child_project_team_member_details')],
        }
    }


def get_update_task_child_data(dag_run, parent):
    return {
        "project": {"uri": rail.result('get_child_project_details')['uri']},
        "task": {
            "target": {
                "name": dag_run.conf['taskName'],
                "parent": {"uri": dag_run.conf['parenttaskuri']} if parent else null
            },
            "name": dag_run.conf['taskName'],
            "code": dag_run.conf['taskCode'],
            "timeEntryDateRange": null,
            "customFieldValues": [
                {
                    "customField": {"uri": dag_run.conf['tasktypeuri']},
                    "dropDownOption": {"uri": dag_run.conf['tasktypeoptionuri']},
                }
            ],
            "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable",
            "percentCompleted": 0,
            "isTimeEntryAllowed": True,
            "isClosed": False,
            "assignedResources": [{'uri': user['uri']} for user in custom_methods.get_userlist('get_project_team_member_details')],
        }
    }


def get_task_2_conf(dag_run, item):
    return {
        'name': item['name'],
        'description': item['code'] if item['code'] else null,
        'enddate': dag_run.conf['enddate'],
        'startdate': dag_run.conf['startdate'],
        'projecturi': dag_run.conf['projecturi'],
        'taskuri': item['uri'],
        'parenttaskuri': rail.result('put_task_from_wbs')['uri'],
        'tasktypeuri': dag_run.conf['tasktypeuri'],
        'tasktypeoptionuri': dag_run.conf['level2tasktypeoptionuri'],
        'userlist': dag_run.conf['userlist'],
        'projectname': dag_run.conf['projectname']
    }


def get_child_wbs_task_2_conf(dag_run, item):
    return {
        'name': item['name'],
        'description': item['code'] if item['code'] else null,
        'enddate': custom_methods.get_replicon_date(rail.result('get_child_project_date_range')['enddate'], '%d/%m/%Y'),
        'startdate': custom_methods.get_replicon_date(rail.result('get_child_project_date_range')['startdate'], '%d/%m/%Y'),
        'projecturi': rail.result('get_child_project_details_based_on_wbs')['uri'],
        'taskuri': item['uri'],
        'parenttaskuri': rail.result('put_task_from_child_wbs')['uri'],
        'tasktypeuri': dag_run.conf['tasktypeuri'],
        'tasktypeoptionuri': dag_run.conf['level2tasktypeoptionuri'],
        'userlist': custom_methods.get_userlist('get_all_child_project_team_member_details'),
        'projectname': dag_run.conf['childwbs']
    }


def get_update_task_data(dag_run):
    return {
        "project": {"uri": rail.result("get_project_details_based_on_wbs")['uri']},
        "task": {
            "target": {
                "name": rail.result("for_each_billing_key_start")['taskName'],
                "parent": null
            },
            "name": rail.result("for_each_billing_key_start")['taskName'],
            "code": rail.result("for_each_billing_key_start")['taskCode'],
            "timeEntryDateRange": null,
            "customFieldValues": [
                {
                    # `Task Type`
                    "customField": {"uri": dag_run.conf['tasktypeuri']},
                    # `GSAP Billing Key`
                    "dropDownOption": {"uri": dag_run.conf['tasktypeoptionuri']},
                }
            ],
            "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable",
            "percentCompleted": 0,
            "isTimeEntryAllowed": True,
            "isClosed": False,
            "assignedResources": [{'uri': user} for user in rail.result('get_all_project_team_member_details')],
        }
    }
