"""
Response filters for T-Systems Project Import integration
Handles API response processing and data extraction
"""
import rail

def get_client_data_from_list_service(response,dag_run):
    """Extract client data from ClientListService response"""
    if not response['rows']:
        return []

    response_data = list(filter(lambda item: item['code'] == dag_run.conf['client_code'],map(lambda row: {
        'uri': row['cells'][0].get('uri'),
        'code': row['cells'][1].get('textValue', ''),
        'name': row['cells'][3].get('textValue', ''),
        'client_manager': row['cells'][2].get('textValue', '')
    }, response['rows'])))

    return response_data[0] if response_data else []

def get_project_oef_fields(response):
    oef_mapper = {
        'Accounting Group': 'accounting_group',
        'Control Expert': 'control_expert',
        'Delivery Cost Center': 'delivery_cost_center',
        'Process ID Group': 'process_id_group',
        'Project Classification': 'project_classification',
        'Project Legal Unit': 'project_legal_unit',
        'Project Type': 'project_type'
    }

    if not response or not isinstance(response, list):
        return []
    
    oef_fields = []
    for binding in response:
        field_data = {
            'uri': binding.get('uri'),
            'oef_name': oef_mapper.get(binding.get('displayText'),'')
        }
        oef_fields.append(field_data) if field_data['oef_name'] else None
    
    return oef_fields

def get_dropdown_uris_per_oef(response, oef_name):
    """Extract dropdown values for a specific OEF field"""
    if not response:
        return {oef_name: []}

    dropdown_values = []
    values = response.get('tags', [])

    for value in values:
        dropdown_values.append({
            'name': value.get('name', ''),
            'uri': value.get('uri', '')
        })

    return {oef_name: dropdown_values}

def get_existing_details_of_group(response):
    """Parse Replicon list service response into structured group data"""
    if not response:
        return []

    return [{
        'name': group['cells'][0].get('textValue'),
        'uri': group['cells'][0].get('uri'),
        'fullpath': rail.smartjoin_by_delim([item['textValue'] for item in group['cells'][1]['cellCollection']], '/'),
        'fullpath_code': rail.smartjoin_by_delim([item['textValue'] for item in group['cells'][2]['cellCollection']], '/'),
        'length': len([item['textValue'] for item in group['cells'][1]['cellCollection']]),
        'code': group['cells'][3].get('textValue', ''),
    } for group in response['rows']]
