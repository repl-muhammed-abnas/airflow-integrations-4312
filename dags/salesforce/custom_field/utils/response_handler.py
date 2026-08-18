import itertools
import json
from rail import get_current_context, result

null = None


def get_matching_customfields_values(dag_run):
    filters = []
    settings = dag_run.conf['customSettings']
    condition = settings['useANDorORLogicalCombination']

    for item in settings['basedOnOpportunityCustomFields']:
        valid_sf_custom_field = next((x for x in result('get_sfobject_customfields')
                                      if x['label'] == item['key'].strip()), None)

        if valid_sf_custom_field:
            custom_field_values = "','".join(list(map(
                str.strip,
                item['value'].split(settings['delimiterCharacter'])
            )))
            filters.append(
                f"{valid_sf_custom_field['name']} IN ('{custom_field_values}')"
            )

    return f"({f' {condition} '.join(filters)})" if filters else ''


def page_handler(request, response):
    if len(response['rows']) > 0:
        request['page'] += 1
        return request
    return null


def filter_object_extension_tags(response):

    null_data_type = 'urn:replicon:list-type:null'

    flatten_rows = list(itertools.chain(
        *list(map(lambda x: x['rows'], response))))

    return list(map(lambda x: {
        'replicon_label': x['cells'][0]['textValue'],
        'replicon_identifier': x['cells'][1]['textValue'] if x['cells'][1]['dataType'] != null_data_type else '',
        'replicon_tag_definition_type_uri': x['cells'][3]['uri'],
        'replicon_tag_definition_uri': x['cells'][4]['uri']
    }, flatten_rows))


def generate_unique_oef_name(sf_field, all_replicon_oefs, new_oefs):
    count = 1
    oef_name = sf_field['label']
    while any(oef_name == oef['name'] for oef in new_oefs) or \
        any(oef_name == oef['replicon_label'] and
            sf_field['name'] != oef['replicon_identifier']
            for oef in all_replicon_oefs):
        oef_name = sf_field['label'] + f'({count})'
        count += 1

    return oef_name


def filter_object_extension_bindings(response):
    new_oefs = []
    modify_oefs = []
    existing_oefs = []
    dag_run_conf = get_current_context()['dag_run'].conf
    sf_customfields = result('get_sfobject_customfields') if result(
        'get_sfobject_customfields') else []
    all_replicon_oefs = result('get_object_extension_tag_list') if result(
        'get_object_extension_tag_list') else []

    delimiter = dag_run_conf['customSettings']['delimiterCharacter']
    customFields = dag_run_conf['customSettings']['customFields']

    already_binded_oef_names = [binded_oef['displayText']
                                for binded_oef in response]
    already_binded_oef_codes = [oef['replicon_identifier']
                                for oef in all_replicon_oefs if oef['replicon_label'] in already_binded_oef_names]

    custom_fields_list = list(
        set(map(str.strip, customFields.split(delimiter))))
    custom_fields_details = [
        customField for customField in sf_customfields if customField['label'] in custom_fields_list]

    for sf_field in custom_fields_details:
        matching_oef = next(
            (oef for oef in all_replicon_oefs if oef['replicon_identifier']
             == sf_field['name']), null
        ) if all_replicon_oefs else null

        updated_name = generate_unique_oef_name(
            sf_field, all_replicon_oefs, new_oefs)
        oef = {
            'name': updated_name,
            'code': sf_field['name'],
            'type': sf_field['type'],
            'dropdown_options': sf_field['picklistValues']
        }
        if matching_oef:
            sf_type = get_replicon_type_from_sf_type(sf_field['type']).lower()
            replicon_type = matching_oef['replicon_tag_definition_type_uri'].split(
                '-')[-1]

            if replicon_type != sf_type:
                print(f"{updated_name} has different datatype! ignoring...")
                continue

            oef['uri'] = matching_oef['replicon_tag_definition_uri']
            oef['is_already_binded'] = True if sf_field['name'] in already_binded_oef_codes else False

            if sf_field['label'] == matching_oef['replicon_label'] or \
                    updated_name == matching_oef['replicon_label']:
                existing_oefs.append(oef)
            else:
                modify_oefs.append(oef)
        else:
            new_oefs.append(oef)

    return {
        'new_oefs': new_oefs,
        'modify_oefs': modify_oefs,
        'existing_oefs': existing_oefs
    }


def get_replicon_type_from_sf_type(oef_type):
    if oef_type == 'picklist':
        return 'Tag'

    if oef_type == 'double':
        return 'Numeric'

    if oef_type == 'url':
        return 'File'

    return 'Text'


def generate_dropdown_options(oef, uri):
    return [{
        'target': {
            'tagName': {
                'name': tag['label'],
                'tagDefinitionUri': uri
            }
        },
        'name': tag['label'],
        'code': tag['value'],
        'isEnabled': tag['active'],
        'description': 'created from Salesforce Connector'
    } for tag in oef['dropdown_options']]


def get_oef_creation_params():
    oef = result('foreach_new_oef')
    oef_type = get_replicon_type_from_sf_type(oef['type'])
    return {
        'url': f'/services/ObjectExtension{oef_type}DefinitionService1.svc/PutObjectExtension{oef_type}Definition',
        'params': json.dumps({
            f'objectExtension{oef_type}Definition': {
                'target': {'name': oef['name']},
                'name': oef['name'],
                'code': oef['code'],
                'description': 'created from Salesforce Connector',
                **({'sourceUris': ['urn:replicon:object-extension-file-definition-source:link']} if oef_type == 'File' else {})
            }
        })
    }


def get_oef_modification_params():
    oef = result('foreach_modify_oef')
    oef_type = get_replicon_type_from_sf_type(oef['type'])
    return {
        'url': f'/services/ObjectExtension{oef_type}DefinitionService1.svc/UpdateName',
        'params': json.dumps({
            'name': oef['name'],
            f'objectExtension{oef_type}DefinitionUri': oef['uri']
        })
    }


def get_final_result(is_custom_fields_present):
    where_clause = result('get_customfield_where_clause') if result(
        'get_customfield_where_clause') else ''
    if is_custom_fields_present == 'False':
        return {'customfield_where_clause': where_clause}

    new_oefs = result('get_all_object_extension_bindings')['new_oefs']
    created_oefs_uri = {item['displayText']: item['uri']
                        for item in result('get_created_oefs_list')['value']}

    for oef in new_oefs:
        oef["uri"] = created_oefs_uri.get(oef["name"], '')

    existing_oefs = result('get_all_object_extension_bindings')[
        'existing_oefs']

    return {
        'customfield_where_clause': where_clause,
        'oef_list': new_oefs + existing_oefs
    }
