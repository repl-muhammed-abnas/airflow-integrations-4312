from datetime import datetime
import hashlib
import rail

null = None


def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def get_create_billing_rates_param(
        dag_run, item):
    return {
        "billingRate": {
            "target": {
                "name": dag_run.conf['name'] + item
            },
            "name": dag_run.conf['name'] + item,
            "description": dag_run.conf['description'] if dag_run.conf['description'] else null,
            "isEnabled": True
        }
    }


def get_project_payload(dag_run):
    return {"projects": [{"uri": null, "name": dag_run.conf['wbs'],
                          "code": null, "parameterCorrelationId": null}]}


def get_key_value_from_wbs(key_name_space, key):
    return {"keyNamespace": key_name_space, "key": key}


def put_key_value(key_name_space, key, jsonValue):
    return {"keyNamespace": key_name_space, "keyValue": {
        "key": key, "jsonValue": jsonValue}}


def update_billing_rates_for_team_members(projectUri, billingRateUri):
    return {
        "projectUri": projectUri,
        "billingRateUri": billingRateUri,
        "billingRateAvailableForAssignmentOptionUri": "urn:replicon:billing-rate-available-for-assignment-option:available"
    }


def get_process_billing_rate_wbs_conf(item):
    return {
        'wbs': item['wbs'],
        'billing_rates_from_replicon': rail.result("get_billing_rates_after_create")
        if rail.result("get_billing_rates_after_create") else rail.result("get_billing_rates_before_create"),
        'input_combined_data': rail.result("input_combined_data_collection"),
        'parentwbsuri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_filter_definitions'), 'name', 'Parent WBS', 'uri'),
        'parentwbscolumnuri': rail.result('get_all_columns')
    }


def get_project_list_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:project-list-column:project",
            dag_run.conf["parentwbscolumnuri"]
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": dag_run.conf['parentwbsuri']
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


def get_process_compass_child_conf(item, dag_run):
    return {
        'wbs': item['textValue'],
        'billingrates': rail.result('assignable_billing_rates_for_wbs'),
        'billingratesinreplicon': dag_run.conf['billing_rates_from_replicon'],
        'c1parentname': dag_run.conf['wbs']
    }


def get_data_validation_rows(item):
    def json_to_date(date_type):
        date_range = rail.result('get_project_info_from_project_service')[
            'timeEntryDateRange'][date_type]
        if not date_range:
            return None
        value = str(date_range['month'])+'/' + \
            str(date_range['day'])+'/'+str(date_range['year'])
        return str(datetime.strptime(value, "%m/%d/%Y").strftime("%m/%d/%Y"))

    return[item['name'], item['taskassignmentstartdate'] if item['taskassignmentstartdate'] else json_to_date('startDate'),
           item['taskassignmentenddate'] if item['taskassignmentenddate'] else json_to_date('endDate'), item['blanklabortype']]


def get_new_blob_rows(item):
    dag_run_conf = get_dag_run_conf()
    data = get_data_from_document(rail.result(
        'query_billing_rates_to_assign_validated'))
    return [dag_run_conf['wbs'], item['displayText'], item['uri'],
            datetime.strptime(rail.find_first_by_attr_and_get_attr(
                data, 'name', item['name'], 'taskassignmentstartdate'), "%m/%d/%Y").strftime("%m/%d/%Y"),
            datetime.strptime(rail.find_first_by_attr_and_get_attr(
                data, 'name', item['name'], 'taskassignmentenddate'), "%m/%d/%Y").strftime("%m/%d/%Y"),
            hashlib.md5((dag_run_conf['wbs'] + item['displayText'] +
                         str(datetime.strptime(rail.find_first_by_attr_and_get_attr(data, 'name', item['name'], 'taskassignmentstartdate'),
                                               "%m/%d/%Y").strftime("%m/%d/%Y")) +
                         str(datetime.strptime(rail.find_first_by_attr_and_get_attr(data, 'name', item['name'], 'taskassignmentenddate'),
                                               "%m/%d/%Y").strftime("%m/%d/%Y"))).encode('utf-8')).hexdigest()]


def get_blob_rows(item):
    return [item['wbsName'], item['labourType'], item['labourTypeUri'], item['startDate'], item['endDate'],
            hashlib.md5((item['wbsName'] + item['labourType'] + item['startDate'] + item['endDate']).encode('utf-8')).hexdigest()]


def get_division_payload():
    return {"divisionUri": rail.result('get_project_info_from_project_service')['division']['uri']}
