from datetime import datetime
import rail
null = None


def get_create_billing_rates_param(
        dag_run):
    return {
        "billingRate": {
            "target": {
                "name": dag_run.conf['name']
            },
            "name": dag_run.conf['name'],
            "description": null,
            "isEnabled": True
        }
    }


def get_replicon_date(date_str, format= '%Y-%m-%d'):
    if not date_str:
        return None
    try:
        date = datetime.strptime(date_str, format)
        return {
            'year': date.year,
            'month': date.month,
            'day': date.day
        }
    except:  # pylint: disable=bare-except
        return None


def get_process_billing_rate_wbs_conf(item):
    return {
        'wbs': item['wbs'],
        'billing_rates_from_replicon': rail.write_json_artifact(rail.result("get_billing_rates_after_create")),
        'file_name':rail.render_template('{{result("new_file_sensor") | file_name }}'),
        'no_of_records': rail.result("create_labour_type_data_collection", 'length')
    }


def get_project_payload(dag_run):
    return {"projects": [{"uri": null, "name": dag_run.conf['wbs'],
                          "code": null, "parameterCorrelationId": null}]}


def get_division_detail():
    data = rail.result("get_project_info_from_project_service")[
        'division']['uri']
    return {
        "divisionUri": data
    }

def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf

def get_billing_rate_list_to_assign_to_project(dag_run):
    if rail.find_first_by_attr_and_get_attr(
        rail.result("get_project_info_from_project_service")["extensionFieldValues"],
            "tag.definition.displayText", "COMPASS T&M Indicator", "tag.displayText") != "X":
        return []
    all_labor_types = []
    required_billing_rate_uris = []

    billing_rates_in_replicon = rail.load_all_records(dag_run.conf['billing_rates_from_replicon'])
    assigned_labor_type_to_project = rail.result('get_all_assigned_labor_types_to_project')

    feed_labor_types = rail.load_all_records(rail.result("query_distinct_labor_types_for_project"))
    for item in feed_labor_types:
        if item['role'] and item['role'] not in assigned_labor_type_to_project:
            all_labor_types.append(item['role'])

    if not all_labor_types:
        return []
    
    for item in all_labor_types:
        result = rail.find_first_by_attr_and_get_attr(billing_rates_in_replicon, 'displayText', item,'uri')
        if result:
             required_billing_rate_uris.append(result)

    return required_billing_rate_uris

def get_resource_uris():
    resource_uris = []
    records_to_process = rail.result("assignement_dates_validation","records_to_process")
    for record in records_to_process:
        resource_uris.append(record['user_uri'])
    return resource_uris
