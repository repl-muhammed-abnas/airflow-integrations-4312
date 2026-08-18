import hashlib
import json
from dateutil import parser
import rail


def is_assignment_required(charge_code, confidential_flag, internal_person_role_type, role_types):
    if charge_code.startswith("NL"):
        if confidential_flag == "true":
            return "No"
    return match_internal_person_role_types(internal_person_role_type, role_types)


def match_internal_person_role_types(internal_person_role_type, role_types):
    return "Yes" if internal_person_role_type in role_types else "No"


def get_user_from_list_by_interperson_id(item, user_list):
    user = []
    if (item['InternalWorkRelationship']['PwCLegalEntity']['PartyId'] and
            item['InternalWorkRelationship'].get('InternalPerson') and item[
                'InternalWorkRelationship']['InternalPerson'].get('PartyId')):
        user = list(filter(lambda x: x['status'] == "True" and x['code'] ==
                           (item['InternalWorkRelationship']['PwCLegalEntity']['PartyId']).lower() and x['employee_id'] ==
                           (item['InternalWorkRelationship']['InternalPerson']['PartyId']).lower(), user_list))
    return user


def validate_key_from_dagrunconf(conf, key):
    if isinstance(conf.get(key), list):
        return f'{json.dumps(conf[key] if conf.get(key) else [], ensure_ascii=False)},'
    string_value = conf[key] if conf.get(key) else ''
    return f'{string_value},'


def get_source_input_reference(dag_run):
    input_reference = hashlib.md5(
        (
            validate_key_from_dagrunconf(dag_run.conf, 'chargecode') +
            validate_key_from_dagrunconf(dag_run.conf, 'chargecodename') +
            validate_key_from_dagrunconf(dag_run.conf, 'chargecodetype') +
            validate_key_from_dagrunconf(dag_run.conf, 'chargecodetypeid') +
            validate_key_from_dagrunconf(dag_run.conf, 'chargecodestartdate') +
            validate_key_from_dagrunconf(dag_run.conf, 'chargecodeenddate') +
            validate_key_from_dagrunconf(dag_run.conf, 'mandatorytextflag') +
            validate_key_from_dagrunconf(dag_run.conf, 'openfortime') +
            validate_key_from_dagrunconf(dag_run.conf, 'partyrole') +
            validate_key_from_dagrunconf(dag_run.conf, 'internalpersonrole') +
            (f"{dag_run.conf['costcentre']['CostCentreCode']}," if
             dag_run.conf.get('costcentre') and dag_run.conf['costcentre'].get(
                 'CostCentreCode')
             else ',') + validate_key_from_dagrunconf(dag_run.conf, 'workitem') +
            validate_key_from_dagrunconf(dag_run.conf, 'engagementline') +
            validate_key_from_dagrunconf(dag_run.conf, 'confidential_flag') +
            validate_key_from_dagrunconf(dag_run.conf, 'client_name') +
            validate_key_from_dagrunconf(dag_run.conf, 'client_code') +
            (dag_run.conf['project_type'] if dag_run.conf.get(
                'project_type') else '')
        ).encode('utf-8'))
    return input_reference.hexdigest()


def compare_payload_with_sourceinputkey(key_values):
    source_input_reference = rail.find_first_by_attr_and_get_attr(
        key_values, 'keyUri', 'urn:replicon:project-key-value-key:source-input-reference-id', 'value')
    return source_input_reference.get('text') == rail.result('get_md5_from_payload') if source_input_reference else False


def map_impersonate_and_create_interactive_session(res):
    data = res.json()['d']
    auth_token = list(
        filter(lambda x: x['name'] == 'AUTHTOKEN', data['sessionCookies']))[0]['value']
    tenant = list(
        filter(lambda x: x['name'] == 'TENANT', data['sessionCookies']))[0]['value']
    return {'cookie': f'AUTHTOKEN={auth_token};TENANT={tenant}', 'Path': '/'}


def log_client_exception(client_name, client_code, client_uri):
    client_exception = []
    if client_name:
        if not client_uri:
            client_exception.append(
                f'Client not associated with the Project since client {client_name} not found')
        if not client_code:
            client_exception.append(
                'Client not associated with the Project since engagement party role id is blank')
    else:
        client_exception.append(
            'Client not associated with the Project since engagement party role primary party name is blank')

    return client_exception


def log_department_list_data_exception(department_data, dag_run, logs):
    if department_data['companycodeurilength'] > 1:
        logs.append(
            f'Company code not updated as multiple companycodes found with the cost center ID: {dag_run.conf["costcentre"]["CostCentreCode"]}')

    country = department_data['country']
    if country and department_data['secondlevelterritorylookupvalue']:
        country_list_filter = list(
            filter(lambda item: item['displayText'] == country, dag_run.conf['replicon_locations']))
        if len(country_list_filter) < 1:
            logs.append(
                f'Country not updated as no country found with the Country: {country}')
        elif len(country_list_filter) > 1:
            logs.append(
                f'Country not updated as multiple countries found with the Country: {country}')

    if dag_run.conf['confidential_flag'] and \
            dag_run.conf['confidential_flag'] == "false":
        scope_mapper = department_data['accessscopemapper']
        if scope_mapper and not department_data['companycodeuritoassign']:
            logs.append(
                f'Team assignment for {scope_mapper} \
                    access scope not done since required node value not found as per the access scope mapper')


def get_resource_to_assign_by_type(existing_project_team_members, resourceType, payloadType):
    resource_to_assign_by_type = [x['resource']['uri'] for x in existing_project_team_members if
                                  x['resource']['resourceType']['displayText'] == resourceType]
    return list(map(lambda x: {
        f"{payloadType}": {
            "uri": x
        }}, resource_to_assign_by_type)) if resource_to_assign_by_type else []


def get_new_project_team_members():
    return [
        x for x in rail.result('get_individual_team_member_uris') if
        x not in [y['resource']['uri']
                  for y in rail.result('bulk_get_project_team_members')]
    ] if rail.result('get_individual_team_member_uris') else []


def do_format_logs(dag_run):

    def load_records(master_log_artifact):
        try:
            logs = rail.load_all_records(master_log_artifact)
            return logs
        except:  # pylint: disable=bare-except
            return []

    log_artifacts = load_records(dag_run.conf['master_log'])
    if dag_run.conf['child_log']:
        for child_log in dag_run.conf['child_log']:
            child_log_records = load_records(child_log)
            if child_log_records:
                log_artifacts.extend(child_log_records)

    log_records = []

    if log_artifacts:
        add_to_logs(dag_run.conf['identifier'],
                    log_artifacts, log_records, 'Client')
        add_to_logs(dag_run.conf['identifier'],
                    log_artifacts, log_records, 'Project')
        add_to_logs(dag_run.conf['identifier'],
                    log_artifacts, log_records, 'Task')

    return log_records


def add_to_logs(identifier, log_artifacts, log_records, entity_type):
    key = f'{entity_type} Name|{entity_type} Code'

    entity_logs = [x for x in log_artifacts if (
        x['properties']['SenderID'].split(" | ")[1] == entity_type and (x['properties'].get(key) and
                                                                        x['properties'].get(key) != 'nil'))]

    log_records.extend(
        list(map(lambda item: {
            # 2022-04-29 08:20:49.986
            'Date': parser.parse(item['timestamp']).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
            'FileName': identifier,
            'Status': item['properties'].get('status'),
            'Details': item['properties'].get('details'),
            'EntityId': item['properties'].get(key).split(" | ")[1],
            'EntityName': item['properties'].get(key).split(" | ")[0],
            'EntityType': entity_type
        }, entity_logs))
    )
