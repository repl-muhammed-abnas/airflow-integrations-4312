import uuid
import rail

null = None


def get_all_org_units_payload():
    return {
        "page": "1",
        "pagesize": "100000000",
        "columnUris": [
          "urn:replicon:department-group-list-column:department-group",
          "urn:replicon:department-group-list-column:full-path",
          "urn:replicon:department-group-list-column:effectively-enabled"
        ],
        "sort": [],
        "filterExpression": null
    }


def process_psa_org_unit_conf(item):
    return {
        'organization_unit_cd': item['organization_unit_cd'],
        'organization_unit_uri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_org_units'), 'name', item['organization_unit_cd'], 'uri'),
        'current_organization_unit_parent': rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_org_units'), 'name', item['organization_unit_cd'], 'parent'),
        'psa_parent_uri': rail.result('psa_parent_org_unit_uri')
    }


def create_organizational_unit(dag_run):
    return {
        "departmentGroup": {
          "name": null,
          "uri": null,
          "parent": {
            "uri": dag_run.conf["psa_parent_uri"],
            "parent": null,
            "name": null,
            "parameterCorrelationId": null
          },
          "parameterCorrelationId": null
        },
        "modifications": {
          "name": dag_run.conf["organization_unit_cd"],
          "codeToApply": null,
          "descriptionToApply": null,
          "isEnabled": null
        },
        "unitOfWorkId": str(uuid.uuid4())
    }


def move_under_psa(dag_run):
    return {
        "departmentGroup": {
            "name": null,
            "uri": dag_run.conf['organization_unit_uri'],
            "parent": null,
            "parameterCorrelationId": null
        },
        "target": {
            "name": null,
            "uri": dag_run.conf["psa_parent_uri"],
            "parent": null,
            "parameterCorrelationId": null
        }
    }
