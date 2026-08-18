import json

import rail


def build_project_create_or_update_request_body(config, d365_project):
    request_body = {}
    request_body['name'] = d365_project.get('msdyn_subject')
    if d365_project.get('msdyn_project'):
        if d365_project.get('msdyn_project').get('ae_projectdescription'):
            request_body['description'] = d365_project.get('msdyn_project').get('ae_projectdescription')
    project_status_from_d365 = d365_project.get('ae_projectstatus')
    pim_project_status_id = config.D365_TO_PIM_PROJECT_STATUS.get(project_status_from_d365)

    if pim_project_status_id:
        request_body['status'] = {
            'id': pim_project_status_id
        }
    else:
        request_body['status'] = {
            'id': config.D365_TO_PIM_PROJECT_STATUS['In Process'] # default to Open/In Progress if status mapping not found
        }
    project_code_from_d365 = d365_project.get('vs360_projectid')
    if project_code_from_d365:
        request_body['code'] = project_code_from_d365

    return json.dumps(request_body)


def build_project_udf_update_request_body(
    d365_project,
    pim_division_id,
    pim_office_id,
    pim_group_id,
):
    request_body = {}
    request_body['id'] = rail.result('parse_get_pim_project_id_from_project_mapper') or rail.result('parse_create_project_in_pim')
    request_body['company'] = {
        'id': rail.result('parse_get_company_mapping')
    }
    if d365_project.get('msdyn_start'):
        request_body['startDate'] = d365_project.get('msdyn_start')
    if d365_project.get('msdyn_finish'):
        request_body['endDate'] = d365_project.get('msdyn_finish')
    if pim_division_id:
        request_body['division'] = {
            'id': pim_division_id[0]
        }
    if pim_office_id:
        request_body['office'] = {
            'id': pim_office_id[0]
        }
    if pim_group_id:
        request_body['group'] = {
            'id': pim_group_id[0]
        }
    if d365_project.get('vs360_totalcontract'):
        request_body['estimatedCost'] = d365_project.get('vs360_totalcontract')

    return json.dumps(request_body)
    

def build_update_entity_contacts_payload(config, pim_project_id, ae_contact_pim_id, project_contact_list):
    seen_ids = set()
    contacts = []

    if ae_contact_pim_id:
        contacts.append({
            'id': ae_contact_pim_id,
            'primaryContact': True,
            'projectManager': False,
        })
        seen_ids.add(ae_contact_pim_id)

    for contact in (project_contact_list or []):
        contact_id = contact.get('pim_contact_id')
        if contact_id and contact_id not in seen_ids:
            contacts.append({
                'id': contact_id,
                'primaryContact': False,
                'projectManager': False,
            })
            seen_ids.add(contact_id)

    return json.dumps({
        'id': pim_project_id,
        'classId': config.ENTITY_CLASS_IDS['PROJECT'],
        'contact': contacts,
    })


def link_external_organization_to_the_pim_project_payload(config):
    return json.dumps({
        "organization": {
            "id": rail.result("parse_get_pim_external_organization_id_from_mapper_after_creation")
        },
        "role": {
            "id": config.PROJECT_EXTERNAL_ORGANIZATION_ROLE_ID
        }
    })