import uuid
import rail

null = None


def get_cost_centers():
    return {
        "page": "1",
        "pagesize": "100000000",
        "columnUris": [
            "urn:replicon:cost-center-list-column:cost-center",
            "urn:replicon:cost-center-list-column:full-path",
            "urn:replicon:cost-center-list-column:effectively-enabled"
        ],
        "sort": [],
        "filterExpression": null
    }


def process_psa_cost_centers_conf(item):
    return {
        'costcentername': item['costcentername'],
        'costcenteruri': rail.find_first_by_attr_and_get_attr(rail.result('get_cost_centers'), 'name', item['costcentername'], 'uri'),
        'currentcostcenterparent': rail.find_first_by_attr_and_get_attr(rail.result('get_cost_centers'), 'name', item['costcentername'], 'parent'),
        'psaparenturi': rail.result('psa_parent_cost_center_uri')
    }


def create_cost_center(dag_run):
    return{
        "costCenter": {
            "name": null,
            "uri": null,
            "parent": {
                "name": null,
                "uri": dag_run.conf['psaparenturi'],
                "parent": null,
                "parameterCorrelationId": null
            },
            "parameterCorrelationId": null
        },
        "modifications": {
            "name": dag_run.conf['costcentername'],
            "codeToApply": null,
            "descriptionToApply": null,
            "isEnabled": True
        },
        "unitOfWorkId": str(uuid.uuid4())
    }


def move_under_psa(dag_run):
    return {
        "costCenter": {
            "name": null,
            "uri": dag_run.conf['costcenteruri'],
            "parent": null,
            "parameterCorrelationId": null
        },
        "target": {
            "name": null,
            "uri": dag_run.conf['psaparenturi'],
            "parent": null,
            "parameterCorrelationId": null
        }
    }
