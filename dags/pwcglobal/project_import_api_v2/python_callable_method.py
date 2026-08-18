import rail
from pwcglobal.project_import_api_v2 import custom_method

null = None


def get_task_state(task_id):
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()


def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf


def get_client_name_code_partyid(dag_run):
    client_name = null
    client_code = null
    client_party_id = null
    if dag_run.conf['webhook']['data'] and dag_run.conf['webhook']['data'].get('WorkManagement'):
        work_management_data = dag_run.conf['webhook']['data']['WorkManagement'][0]
        client_data = work_management_data['EngagementPartyRole'][0] if work_management_data.get('EngagementPartyRole') \
            else null
        if client_data:
            client_name = client_data['PrimaryPartyName'] if client_data.get(
                'PrimaryPartyName') else null
            client_code = client_data['PartyAlternateIdentifierValue'].strip() if client_data.get(
                'PartyAlternateIdentifierValue') else null
            client_party_id = client_data['PartyId'] if client_data.get(
                'PartyId') else null
    return {
        'client_name': client_name,
        'client_code': client_code,
        'client_party_id': client_party_id
    }


def do_map_department_list_data(access_scope_mapper, dag_run):

    mapped_company_code_by_codes = [x for x in rail.result(
        'get_department_list') if x['code'] == dag_run.conf['costcentre']['CostCentreCode']]
    mapped_company_code_by_fullpaths = list(
        set({x['fullpath'] for x in mapped_company_code_by_codes}))
    mapped_company_code_by_uris = list(
        set({x['uri'] for x in mapped_company_code_by_codes}))
    mapped_company_code_by_fullpath_uris = list(
        set({x['fullpathuri'] for x in mapped_company_code_by_codes}))

    project_mapper = list(
        filter(lambda x: x['territory'] == ("".join(mapped_company_code_by_fullpaths)).split(" / ")[1] and
               x['Projecttype'] == dag_run.conf['chargecodetype'], access_scope_mapper))
    return {
        "companycodefullpath": "".join(mapped_company_code_by_fullpaths),
        "companycodefullpathuri": "".join(mapped_company_code_by_fullpath_uris),
        "companycodeuri": "".join(mapped_company_code_by_uris),
        "numberofcostcenter": len(mapped_company_code_by_uris),
        "exception1": f"No company code found with the cost center code: {dag_run.conf['costcentre']['CostCentreCode']}"
        if len(mapped_company_code_by_uris) == 0 else null,
        "exception2": f"Multiple company codes found with the cost center code: {dag_run.conf['costcentre']['CostCentreCode']}"
        if len(mapped_company_code_by_uris) > 1 else null,
        "companycodeurilength": len([x['uri'] for x in mapped_company_code_by_codes]),
        "fullpathlength": len("".join(mapped_company_code_by_fullpaths).split(" / ")),
        "secondlevelterritorylookupvalue": "".join(mapped_company_code_by_fullpaths).split(" / ")[1],
        "accessscopemapper": project_mapper[0]['Level'] if (project_mapper and dag_run.conf['confidential_flag'] == "false") else null,
        "leveltoassign": "".join(mapped_company_code_by_fullpaths).split(" / ")[project_mapper[0]['Level'] - 1]
        if (project_mapper and dag_run.conf['confidential_flag'] == "false") and bool("".join(mapped_company_code_by_fullpaths)) else null,
        "companycodeuritoassign": "".join(mapped_company_code_by_fullpath_uris).split(" / ")[project_mapper[0]['Level'] - 1]
        if (project_mapper and dag_run.conf['confidential_flag'] == "false") and bool("".join(mapped_company_code_by_fullpath_uris)) else null,
        "country": "".join(mapped_company_code_by_fullpaths).split(" / ")[1].replace("PwC", "").strip()
        if ("".join(mapped_company_code_by_fullpaths).split(" / ")[1]
            if "".join(mapped_company_code_by_fullpaths) else null) else null
    }


def add_to_respective_user_list(role_types):
    dag_run_conf = get_dag_run_conf()
    user_list = [item for item in rail.result('get_user_list') if item]
    return list(filter(lambda x: x['user_uri'] and (x['assignment_required'] == "Yes" or
                x['project_manager_uri'] == "Yes" or x['project_co_manager_uri'] == "Yes"),
        list(map(lambda item: {
            "internal_person_role_type": item['InternalPersonRoleType'],
            "internal_person_role_type_id": item['InternalPersonRoleTypeId'],
            "party_id": (item['InternalWorkRelationship']['InternalPerson']['PartyId']).lower() if item['InternalWorkRelationship'].get(
                'InternalPerson') and item['InternalWorkRelationship']['InternalPerson'].get('PartyId') else null,
            "pwc_legal_entity": (item['InternalWorkRelationship']['PwCLegalEntity']['PartyId']).lower() if item['InternalWorkRelationship'].get(
                'PwCLegalEntity', {}).get('PartyId') else null,
            "pwc_legal_entity_uri": item['InternalWorkRelationship']['PwCLegalEntity']['pwclegalentityuri'],
            "assignment_required": custom_method.is_assignment_required(dag_run_conf['chargecode'],
                                                                        dag_run_conf['confidential_flag'], item['InternalPersonRoleType'], role_types)
            if role_types[0].endswith("Member") else null,
            "user_uri": "".join(list(set({x['uri'] for x in custom_method.get_user_from_list_by_interperson_id(item, user_list)}))) if
            custom_method.get_user_from_list_by_interperson_id(item, user_list) else null,
            "project_manager_uri": custom_method.match_internal_person_role_types(item['InternalPersonRoleType'], role_types)
            if role_types[0].endswith("Manager") else null,
            "project_co_manager_uri": custom_method.match_internal_person_role_types(item['InternalPersonRoleType'], role_types)
            if role_types[0].endswith("Partner") else null,
            "user_name": "".join(list(set({x['name'] for x in custom_method.get_user_from_list_by_interperson_id(item, user_list)})))
            if (role_types[0].endswith("Partner") and custom_method.get_user_from_list_by_interperson_id(
                item, user_list)) else null
        }, dag_run_conf['internalpersonrole']))))


def add_to_individual_team_member_list():
    team_member_uris = get_user_uris_from_list(
        rail.result('get_team_member_to_assign'))
    project_manager_uris = get_user_uris_from_list(
        rail.result('get_project_manager_to_assign'))
    project_comanager_uris = get_user_uris_from_list(
        rail.result('get_project_co_manager_to_assign'))
    return list(set(team_member_uris + project_manager_uris + project_comanager_uris))


def get_user_uris_from_list(result):
    return [item['user_uri'] for item in result] if result else []


def get_permission_user_uri(manager_type, task_id):
    user_uris = [x['user_uri']
                 for x in rail.result(task_id)] if rail.result(task_id) else null
    permission = get_dag_run_conf()['project_manager_permission'] if \
        manager_type == 'project_manager' else get_dag_run_conf()['project_comanager_permission']
    return {
        'user_uri': user_uris[0],
        'permission': permission
    } if user_uris else null


def log_tasks(item, dag_run, message):
    return {
        'SenderID': f'{dag_run.conf["sender"]} | Task',
        'Project Name|Project Code': 'nil',
        'Client Name|Client Code': 'nil',
        'Task Name|Task Code': f'{item["task"]["name"]} | {item["task"]["code"]}' if item["task"] else '',
        'status': 'Error' if bool(item["error"] and item["error"]["notifications"]) else 'Success',
        'details': f'{item["error"]["notifications"][0]["displayText"]},{message}' if bool(
            item.get("error") and item["error"]["notifications"]) else message,
        'UnitLoggedDateTime': "{{ current_time() }}",
        'Action': 'Add'
    }


def get_multiple_legal_entity_exception(internalpersonrole):
    internal_persontype_work_relationship = [{
        'internalpersonroletype': x['InternalPersonRoleType'],
        'internalworkrelationship': x['InternalWorkRelationship']
    } for x in internalpersonrole if x['InternalWorkRelationship'] and x['InternalPersonRoleType']]

    multiple_legal_entities = [{
        'internalpersonroletype': x['internalpersonroletype'],
        'partyid': x['internalworkrelationship'].get('PwCLegalEntity', {}).get('PartyId')
    } for x in internal_persontype_work_relationship if x.get(
        'internalworkrelationship', {}).get('PwCLegalEntity', {}).get('pwclegalentityuri') and len(
            x['internalworkrelationship']['PwCLegalEntity']['pwclegalentityuri'].split(',')) > 1]

    # pylint: disable=line-too-long
    return [f"{legal_entity['internalpersonroletype']} not added to project since multiple internalpersonrole pwc legal entities found for Party ID {legal_entity['partyid']}"
            for legal_entity in multiple_legal_entities if legal_entity['partyid']] if multiple_legal_entities else None


def do_get_exception_logs_add(dag_run):
    logs = []

    if not rail.result('get_permissionuri_useruri_project_manager'):
        logs.append(
            'Project manager not assigned since user with given party ID was not found')

    party_role_list = dag_run.conf['partyrole']
    party_id_role_list = [x['PartyId']
                          for x in party_role_list if x['PartyId']]
    if len(party_role_list) == 0 and len(party_id_role_list) == 0:
        logs.append(
            'Legal entity not updated as legal entity is not provided')
    else:
        party_id_uri_list = [x['partyiduri']
                             for x in party_role_list if x['partyiduri']]
        if len(party_id_uri_list) > 1 or (bool(party_id_uri_list) and len(party_id_uri_list[0].split(",")) > 1):
            logs.append(
                f'Legal entity not updated as multiple legal entities found with the legalentity ID: {party_id_role_list[0]}')
        elif len(party_id_uri_list) == 0:
            logs.append(
                f'Legal entity not updated as legal entity not found with the legalentity ID: {party_id_role_list[0]}')

    legal_entity_exception = get_multiple_legal_entity_exception(
        dag_run.conf['internalpersonrole'])
    if legal_entity_exception:
        logs.append(','.join(legal_entity_exception))

    client_exception = custom_method.log_client_exception(
        dag_run.conf['client_name'], dag_run.conf['client_code'], dag_run.conf['client_uri'])
    if client_exception:
        logs.append(','.join(client_exception))

    if rail.result('create_project_with_payload', 'error'):
        logs.append(rail.result('create_project_with_payload',
                                'error')['response']['json']['error']['details']['notifications'][0]['displayText'] + dag_run.conf["chargecode"])

    if rail.result('map_department_list_data'):
        custom_method.log_department_list_data_exception(
            rail.result('map_department_list_data'), dag_run, logs)

    return logs


def do_get_success_logs_update():

    logs = []

    success_tasks = list(map(lambda x: x.task_id, filter(lambda x: x.state == 'success',
                                                         rail.get_current_context()['dag_run'].get_task_instances())))

    update_logs_map = {
        'update_project_description': 'Project description updated',
        'update_project_name': 'Project name updated',
        'update_project_manager': 'Project delivery manager updated',
        'update_engagement_party_udf_value': 'Project Engagement partner updated'
    }

    if get_task_state('get_project_date_range_logs') == 'success' and rail.result('get_project_date_range_logs'):
        date_range_error_logs = rail.result(
            'get_project_date_range_logs')['project_date_range_logs']['error'] if rail.result(
            'get_project_date_range_logs')['project_date_range_logs'] else []
        if date_range_error_logs:
            update_logs_map['get_project_date_range_logs'] = date_range_error_logs

    update_logs_map |= {
        'update_text_effective_udf_value': 'Project Mandatory text and Text effective date udf are updated',
        'bulk_update_project_team_members': 'Project team member assignment updated'
    }

    for key in [*update_logs_map]:
        if key in success_tasks:
            logs.extend(update_logs_map[key]) if isinstance(
                update_logs_map[key], list) else logs.append(update_logs_map[key])

    return logs


def do_get_exception_logs_update(dag_run):
    logs = []

    legal_entity_exception = get_multiple_legal_entity_exception(
        dag_run.conf['internalpersonrole'])
    if legal_entity_exception:
        logs.append(','.join(legal_entity_exception))

    if not rail.result('get_permissionuri_useruri_project_manager'):
        logs.append(
            'Project manager not found with given party ID and legal entity ID')

    if not rail.result('get_permissionuri_useruri_project_comanager'):
        logs.append(
            'Project Delivery Partner not found with party ID and legal entity ID')

    if get_task_state('get_project_date_range_logs') == 'success' and rail.result('get_project_date_range_logs'):
        date_range_exception_logs = rail.result(
            'get_project_date_range_logs')['project_date_range_logs']['exception'] if rail.result(
            'get_project_date_range_logs')['project_date_range_logs'] else []
        if date_range_exception_logs:
            logs.extend(date_range_exception_logs)

    client_exception = custom_method.log_client_exception(
        dag_run.conf['client_name'], dag_run.conf['client_code'], dag_run.conf['client_uri'])
    if client_exception:
        logs.append(','.join(client_exception))

    return logs
