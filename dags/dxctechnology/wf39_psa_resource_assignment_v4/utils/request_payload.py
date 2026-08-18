import json
from datetime import datetime
import rail
null = None
from dxctechnology.wf39_psa_resource_assignment_v4.mapper.item_categories import item_categories_mapper


def get_create_billing_rates_param(
        dag_run, item):
    return {
        "billingRate": {
            "target": {
                "name": dag_run.conf['name'] + item
            },
            "name": dag_run.conf['name'] + item,
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
        'billing_rates_from_replicon':  rail.write_json_artifact(rail.result("get_all_billing_rates")),
       # 'input_combined_data': rail.result("input_combined_data_collection"),
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




def get_resource_uris_enhanced(records):
    """
    Enhanced version that extracts URIs from specific record sets
    Works with both valid_records and records_without_labor_types
    """
    resource_uris = []
    for record in records:
        if record.get('user_uri') and record['user_uri'] not in resource_uris:
            resource_uris.append(record['user_uri'])
    return resource_uris
