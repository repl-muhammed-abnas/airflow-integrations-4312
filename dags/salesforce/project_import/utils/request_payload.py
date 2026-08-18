from datetime import datetime
import functools
import itertools
import uuid
import rail

from salesforce.project_import.utils.util import get_project_details, get_project_status


@functools.lru_cache(maxsize=128)
def load_all_records_from_collection(collection):
    return rail.load_all_records(collection)


def base_opportunity_query():
    select_part = 'SELECT Id, Name, Type, StageName, Probability, OwnerId, AccountId, Description, CloseDate'
    result = rail.result('gather_customfield_dag_result')
    oef_list = result[0]['oef_list'] if result and result[0] and result[0].get(
        'oef_list') else None
    if not oef_list:
        return select_part + ' FROM Opportunity'
    custom_fields = [oef['code'] for oef in oef_list]
    return f"{select_part}, {', '.join(custom_fields)} FROM Opportunity"


def get_opportunity_from_opportunity_ids():
    opportunity_query = base_opportunity_query()
    opportunity_collection = 'get_no_opportunity_attachment_updated' if rail.render_template(
        "{{ get_task_state('get_no_opportunity_attachment_updated') }}") == 'success' and rail.result(
        'get_no_opportunity_attachment_updated', 'length') > 0 else 'query_distinct_opportunities'
    opportunity_ids = "('" + "','".join(tuple((x['linked_entity_id'] for x in load_all_records_from_collection(
        rail.result(opportunity_collection)))))+"')"
    return f"{opportunity_query} WHERE Id IN {opportunity_ids}"


def get_filtered_opportunities(dag_run):
    filtered_for_status_sync = rail.result(
        'new_updated_opportunity')['records']
    probability = dag_run.conf['customSettings']['probability']
    toSyncStatus = dag_run.conf['customSettings'].get('toSyncStatus')

    filtered = list(filter(lambda x: x['Probability'] >= float(probability),
                           filtered_for_status_sync)) if toSyncStatus else filtered_for_status_sync

    rail.set_result(filtered, 'filtered')
    rail.set_result(filtered_for_status_sync, 'filtered_for_status_sync')


def get_project_status_sync_items():
    opportunities = rail.result(
        'filtered_opportunities', 'filtered_for_status_sync')
    return [{
            'account_id': x['AccountId'],
            'opportunity_name': x['Name'],
            'probability': x['Probability'],
            } for x in opportunities]


def map_response(response):
    records = response.get('records', [])
    opportunity_collection = 'get_no_opportunity_attachment_updated' if rail.render_template(
        "{{ get_task_state('get_no_opportunity_attachment_updated') }}") == 'success' and rail.result(
        'get_no_opportunity_attachment_updated', 'length') > 0 else 'query_distinct_opportunities'
    result = rail.result('gather_customfield_dag_result')
    oef_list = result[0]['oef_list'] if result and result[0] and result[0].get(
        'oef_list') else []
    return list(map(lambda x: {
        **{
            'opportunity_id': x['Id'],
            'opportunity_name': x['Name'],
            'opportunity_type': x['Type'],
            'opportunity_stage': x['StageName'],
            'probability': x['Probability'],
            'owner_id': x['OwnerId'],
            'account_id': x['AccountId'],
            'description': x['Description'],
            'close_date': x['CloseDate']
        },
        **{
            k: v for k, v in x.items() if k in [oef['code'] for oef in oef_list]
        },
        **{
            'content_document_ids': next(iter(filter(lambda y: x['Id'] == y['linked_entity_id'], load_all_records_from_collection(
                rail.result(opportunity_collection)))), '').get('content_document_ids', '')
        }
    }, records)) if records else []


def get_record_items(x, is_skip_attachment):
    result = rail.result('gather_customfield_dag_result')
    oef_list = result[0]['oef_list'] if result and result[0] and result[0].get(
        'oef_list') else []
    return {
        **{
            'opportunity_id': x['Id'],
            'opportunity_name': x['Name'],
            'opportunity_type': x['Type'],
            'opportunity_stage': x['StageName'],
            'probability': x['Probability'],
            'owner_id': x['OwnerId'],
            'account_id': x['AccountId'],
            'description': x['Description'],
            'close_date': x['CloseDate']
        },
        **{
            k: v for k, v in x.items() if k in [oef['code'] for oef in oef_list]
        },
        **({'content_document_ids': x['content_document_ids']} if not is_skip_attachment else {}),
        **{'is_skip_attachment': is_skip_attachment}
    }


def get_project_child_dag_item():
    validated_salesforce_data = rail.result('validate_salesforce_data')

    if validated_salesforce_data == 'trigger_opportunity':
        return rail.result('get_opportunities')

    if validated_salesforce_data == 'skip_attachment':
        records = rail.result('filtered_opportunities', 'filtered')
        return list(map(lambda x: get_record_items(x, True), records))

    project_attachment_items = []

    if rail.result('get_opportunity_attachment_updated', 'length') > 0:
        records = load_all_records_from_collection(
            rail.result('get_opportunity_attachment_updated'))
        project_attachment_items.extend(
            list(map(lambda x: get_record_items(x, False), records)))

    if rail.result('get_opportunity_updated_no_attachment', 'length') > 0:
        records = load_all_records_from_collection(
            rail.result('get_opportunity_updated_no_attachment'))
        project_attachment_items.extend(
            list(map(lambda x: get_record_items(x, True), records)))

    if rail.result('get_no_opportunity_attachment_updated', 'length') > 0:
        project_attachment_items.extend(rail.result('get_opportunities'))

    return project_attachment_items


def get_create_project_payload(dag_run):
    null = None

    project_status = 'Tentative'
    if not dag_run.conf['is_polaris_permissions_present'] \
            and dag_run.conf['customSettings'].get('toSyncStatus'):
        status = get_project_status(dag_run.conf)
        if status:
            project_status = status

    def get_replicon_date(date_str):
        datetime_obj = datetime.strptime(date_str, '%Y-%m-%d')
        return {
            'year': datetime_obj.year,
            'month': datetime_obj.month,
            'day': datetime_obj.day
        }
    return {
        'modifications': {
            'nameToApply': {
                'value': dag_run.conf['opportunity_name']
            },
            'descriptionToApply': {
                'value': dag_run.conf['description']
            } if dag_run.conf['description'] else null,
            'startDateToApply': {
                'date': get_replicon_date(dag_run.conf['close_date'])
            },
            'clientAssignmentsSchedulesToApply': {
                'clients': [
                    {
                        'client': {
                            'uri': rail.result('search_client_in_replicon')
                        },
                        'costAllocationPercentage': '100'
                    }
                ]
            } if rail.result('search_client_in_replicon') else null,
            'statusToApply': {
                'name': project_status
            },
            'billingTypeToApply': {
                'value': 'urn:replicon:billing-type:time-and-material'
            },
            'timeAndMaterials': {
                'timeAndExpenseEntryTypeUri': 'urn:replicon:time-and-expense-entry-type:billable-and-non-billable'
            }
        },
        'projectModificationOptionUri': 'urn:replicon:project-modification-option:save',
        'unitOfWorkId': str(uuid.uuid4())
    }


def get_binaryobject_payload():
    content_version = rail.result(
        'valid_contentversions_in_salesforce')
    version_file_name = f"{content_version['Title']} (v{content_version['VersionNumber']}).{content_version['FileExtension']}"

    return {
        'binaryObject': {
            'correlatedObjectUri': f'urn:replicon-tenant:replicon-inc:psa-attachment:{str(uuid.uuid4())}',
            'base64Content': rail.result('getfile_mimetype_encoding')['base64_content'],
            'mimeType': rail.result('getfile_mimetype_encoding')['mime_type'],
            'storagePolicyUri': 'urn:replicon:binary-object-storage-policy:store-forever',
            'retrievalCacheControlUri': 'urn:replicon:binary-object-retrieval-cache-control:no-cache',
            'keyValues': [
                {
                    'keyUri': 'urn:replicon:binary-object-keyvalue-key:file-name',
                    'value': {
                        'text': content_version['PathOnClient'] if content_version[
                            'VersionNumber'] == '1' else version_file_name
                    }
                },
                {
                    'keyUri': 'urn:replicon:binary-object-keyvalue-key:file-size',
                    'value': {
                        'text': content_version['ContentSize']
                    }
                },
                {
                    'keyUri': 'urn:replicon:binary-object-keyvalue-key:file-uploaded-on',
                    'value': {
                        'text': content_version['CreatedDate']
                    }
                }
            ]
        }}


# pylint:disable = line-too-long
def get_link_attachments_to_objects(dag_run):
    project = get_project_details(
        dag_run.conf['replicon_projects'], dag_run.conf['opportunity_name'])
    project_uri = project['uri'] if project else rail.result(
        'create_project_in_replicon').get('uri')

    return [
        {
            'operationName': 'linkAttachmentsToObject',
            'variables': {
                'objectUri': project_uri,
                'attachments': list(map(lambda x: {
                    'uploadUri': x['uri'],
                    'type': 'urn:replicon:psa-attachment-type:binary-object'
                }, rail.result('gather_s3_attachments')))
            },
            'query': 'mutation linkAttachmentsToObject($objectUri: String!, $attachments: [AttachmentLinkInput!]!) {\n  linkAttachmentsToObject2(\n    input: {attachToUri: $objectUri, attachments: $attachments}\n  ) {\n    attachments: attachments2 {\n      uploadUri\n      __typename\n    }\n    __typename\n  }\n}\n'
        }
    ]


def page_handler(request, response):
    if len(response['rows']) > 0:
        request['page'] += 1
        return request
    return None


def get_uri_from_response(response, value):
    all_rows = list(itertools.chain.from_iterable(
        item['rows'] for item in response))
    matching_uris = [
        row['cells'][0]['uri']
        for row in all_rows
        if row['cells'][1]['textValue'] == value
    ]
    return rail.smartjoin_by_delim(matching_uris) if matching_uris else ''
