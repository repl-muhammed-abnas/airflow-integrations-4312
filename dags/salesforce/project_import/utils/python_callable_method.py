import itertools
import json
import rail
import re
import uuid
from salesforce.project_import.utils.request_payload import base_opportunity_query
from salesforce.project_import.utils.util import POLARIS_TO_REPLICON, PROJECT_STATUS_URI, get_project_details, get_project_status


def get_sync_criteria(customSettings):
    condition = customSettings['useANDorORLogicalCombination']
    probability = customSettings['probability']
    basedOnStage = customSettings['basedOnStage']
    basedOnType = customSettings['basedOnType']
    delimiter = customSettings['delimiterCharacter']
    toSyncStatus = customSettings.get('toSyncStatus')

    filter_criteria = []

    if not toSyncStatus and probability and probability.replace('.', '').isdigit():
        filter_criteria.append(f"Probability >= {probability}")

    if basedOnStage and basedOnStage.strip().upper() != 'ALL':
        stages = [(f"'{stage.strip()}'")
                  for stage in basedOnStage.split(delimiter)]
        filter_criteria.append(f"StageName IN ({','.join(stages)})")

    if basedOnType and basedOnType.strip().upper() != 'ALL':
        types = [(f"'{type.strip()}'" if type.strip().upper() !=
                  "NONE" else "''") for type in basedOnType.split(delimiter)]
        filter_criteria.append(f"Type IN ({','.join(types)})")

    return f"{f' {condition} '.join(filter_criteria)}" if filter_criteria else ''


def get_opportunity_where_clause(customSettings):
    condition = customSettings['useANDorORLogicalCombination']
    sync_criteria = get_sync_criteria(customSettings)
    where_clause = sync_criteria if sync_criteria else ''

    result = rail.result('gather_customfield_dag_result')
    customfield_where_clause = result[0]['customfield_where_clause'] if result and result[0] else None
    if customfield_where_clause:
        where_clause = f"{where_clause} {condition} {customfield_where_clause}" if where_clause else customfield_where_clause
    return where_clause


def get_new_updated_opportunity_query(dag_run):
    where_clause = get_opportunity_where_clause(dag_run.conf['customSettings'])
    query_last_modified_records = f"{base_opportunity_query()} WHERE \
        LastModifiedDate >= {rail.result('get_lastsync_time_and_current_time')['last_synctime']}"

    return f"{query_last_modified_records} AND ({where_clause})" if where_clause else query_last_modified_records


def get_new_updated_attachment_query(dag_run):
    opportunity_query = "SELECT Id FROM Opportunity"
    last_synctime = rail.result('get_lastsync_time_and_current_time')[
        'last_synctime']

    where_clause = get_opportunity_where_clause(dag_run.conf['customSettings'])
    if where_clause:
        opportunity_query = f"{opportunity_query} WHERE {where_clause}"

    return f"SELECT ContentDocumentId, Id, LinkedEntityId FROM ContentDocumentLink WHERE IsDeleted = false \
        AND (SystemModstamp >= {last_synctime} OR ContentDocument.LastModifiedDate >= {last_synctime}) AND \
            LinkedEntityId IN ({opportunity_query})"


def is_valid_http_https_url(url):
    regex = re.compile(
        r'^(?:http|https):\/\/(?:[\w\-]+\.)+[a-z]{2,}(?:\/[\w\-\.\/\?\%\&\=]*)?$',
        re.IGNORECASE)
    return re.match(regex, url) is not None


def get_customfield_params_from_type(oef_type, uri, value):
    if oef_type == 'picklist':
        tagDetails = {
            'tagName': {
                'name': value,
                'tagDefinitionUri': 'urn:replicon:object-extension-definition-type:object-extension-type-tag'
            }
        } if value else None
        return {
            'tag': tagDetails,
            'definition': {'uri': uri}
        }

    if oef_type == 'double':
        return {
            'definition': {'uri': uri},
            'numericValue': value,
        }

    if oef_type == 'url':
        return {
            'definition': {'uri': uri},
            'fileValue': {'identityUri': value}
        } if is_valid_http_https_url(value) else None

    if oef_type == 'location':
        coordinates = f"{value['latitude']}:{value['longitude']}"
        value = coordinates if value['latitude'] and value['longitude'] else ''

    if oef_type == 'percent':
        value = f'{value} %'

    return {
        'definition': {'uri': uri},
        'textValue': value
    }


def get_oef_fields_to_apply(dag_run):
    oef_fields_to_apply = []

    for oef in dag_run.conf['oef_list']:
        params = get_customfield_params_from_type(
            oef['type'], oef['uri'], dag_run.conf[oef['code']])
        if params:
            oef_fields_to_apply.append(params)

    return oef_fields_to_apply if oef_fields_to_apply else None


def get_and_apply_customfield_values(dag_run):
    project = get_project_details(
        dag_run.conf['replicon_projects'], dag_run.conf['opportunity_name'])
    project_uri = project['uri'] if project else rail.result(
        'create_project_in_replicon').get('uri')

    return {
        'target': {'uri': project_uri},
        'modifications': {'objectExtensionFieldsToApply': get_oef_fields_to_apply(dag_run)},
        'projectModificationOptionUri': 'urn:replicon:project-modification-option:save',
        'unitOfWorkId': str(uuid.uuid4())
    }


def check_if_sync_required(dag_run):
    if not dag_run.conf.get('account_id'):
        return False

    project_found = get_project_details(
        dag_run.conf['replicon_projects'],
        dag_run.conf['opportunity_name']
    )
    if not project_found:
        return False

    project_status = get_project_status(dag_run.conf)
    if not project_status:
        return False

    new_replicon_status = POLARIS_TO_REPLICON.get(project_status) \
        if dag_run.conf['is_polaris_project'] else project_status

    return project_found['status'] != new_replicon_status


def get_polaris_payload(project_uri, project_status):
    return json.dumps([
        {
            'operationName': 'PutProjectWorkflowState',
            'variables': {
                "projectId": project_uri,
                "projectWorkflowStateId": project_status
            },
            'query': 'mutation PutProjectWorkflowState($projectId: String!, $projectWorkflowStateId: ProjectWorkflowStage!) {\n  putProjectWorkflowState: putProjectWorkflowState3(\n    projectId: $projectId\n    projectWorkflowStateId: $projectWorkflowStateId\n  ) {\n    id\n    uri\n    displayText\n    __typename\n  }\n}\n'
        }
    ])


def get_project_status_update_params(dag_run):
    project_status = get_project_status(dag_run.conf)
    project = get_project_details(
        dag_run.conf['replicon_projects'],
        dag_run.conf['opportunity_name']
    )

    if dag_run.conf['is_polaris_project']:
        return get_polaris_payload(project['uri'], project_status.upper())

    return json.dumps({
        'projectUri': project['uri'],
        'projectStatusUri': PROJECT_STATUS_URI.get(project_status)
    })
