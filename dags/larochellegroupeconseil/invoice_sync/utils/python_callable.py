import rail
null = None
import json
from airflow.models import Variable
from datetime import datetime, timedelta, timezone


def custom_metadata_list(dag_run):
    # Get data1 from DAG config
    data1 = dag_run.conf.get('customMetadata', [])
    
    # Get data2 from previous task result
    data2_raw = rail.result("bulk_get_project_details")
    
    # Handle all possible data structures safely
    data2 = {}
    if isinstance(data2_raw, list):
        if data2_raw and isinstance(data2_raw[0], dict):
            data2 = data2_raw[0]
    elif isinstance(data2_raw, dict):
        data2 = data2_raw
    
    # Initialize variables
    customMetadata = []
    notes_for_customer = {}
    po_number = {}
    billing_address = {}
    
    data1_exclude_keys = {
        'urn:replicon:invoice-metadata-key:po-number',
        'urn:replicon:invoice-metadata-key:notes-for-customer',
        'urn:replicon:invoice-metadata-key:billing-address'
    }

    # Process data1 (customMetadata from DAG config)
    if isinstance(data1, list):
        for i in data1:
            if isinstance(i, dict) and 'keyUri' in i:
                if i['keyUri'] not in data1_exclude_keys:
                    customMetadata.append({
                        "keyUri": i['keyUri'],
                        "value": i['value']
                    })
                    
                if i['keyUri'] == 'urn:replicon:invoice-metadata-key:notes-for-customer':
                    notes_for_customer = {
                        "keyUri": i['keyUri'],
                        "value": i['value']
                    }

                if i['keyUri'] == 'urn:replicon:invoice-metadata-key:po-number':
                    po_number = {
                        "keyUri": i['keyUri'],
                        "value": i['value']
                    }

                if i['keyUri'] == 'urn:replicon:invoice-metadata-key:billing-address':
                    billing_address = {
                        "keyUri": i['keyUri'],
                        "value": i['value']
                    }
    
    # Safely extract project details
    project_details = {}
    if isinstance(data2, dict):
        project_details = data2.get('projectDetails', {})
    
    # Extract project description safely
    project_description = None
    if isinstance(project_details, dict):
        project_description = project_details.get('description')
    
    if project_description:
        customMetadata.append({
            "keyUri": 'urn:replicon:invoice-metadata-key:notes-for-customer',
            "value": {"text": str(project_description)}
        })
    elif notes_for_customer:
        customMetadata.append(notes_for_customer)
    
    # Extract custom fields safely
    custom_fields = []
    if isinstance(project_details, dict):
        custom_fields_raw = project_details.get('customFields', [])
        if isinstance(custom_fields_raw, list):
            custom_fields = custom_fields_raw
    
    # Extract PO numbers from customFields
    po_numbers = ""
    if isinstance(custom_fields, list):
        po_texts = []
        for item in custom_fields:
            if (isinstance(item, dict) and 
                isinstance(item.get('customField'), dict) and 
                item.get('customField', {}).get('displayText') == "PO Number"):
                po_text = item.get('text', '')
                if po_text:
                    po_texts.append(str(po_text))
        po_numbers = " ".join(po_texts)

    if po_numbers.strip():
        customMetadata.append({
            "keyUri": 'urn:replicon:invoice-metadata-key:po-number',
            "value": {"text": po_numbers}
        })
    elif po_number:
        customMetadata.append(po_number)
        
    # Helper function for getting custom field text
    def get_custom_field_text(display_text):
        if not isinstance(custom_fields, list):
            return ""
        
        texts = []
        for item in custom_fields:
            if (isinstance(item, dict) and 
                isinstance(item.get('customField'), dict) and 
                item.get('customField', {}).get('displayText') == display_text):
                text = item.get('text', '')
                if text:
                    texts.append(str(text))
        return " ".join(texts)

    # Extract billing address from multiple custom fields
    address_parts = []
    for field_name in ["Contact de facturation", "Adresse 1", "Adresse 2", "Adresse 3", "Ville - Province - Code postal", "Pays"]:
        field_text = get_custom_field_text(field_name)
        if field_text.strip():
            address_parts.append(field_text)
    
    getaddressinfo = "\n".join(address_parts)

    if getaddressinfo.strip():
        customMetadata.append({
            "keyUri": 'urn:replicon:invoice-metadata-key:billing-address',
            "value": {"text": getaddressinfo}
        })
    elif billing_address:
        customMetadata.append(billing_address)

    return customMetadata


def get_error_message():
            error_message = rail.render_template("{{get_error_message()}}")
            known_errors = [
                "timed out connecting to server",
				"503 service unavailable",
				"failed to open tcp connection",
				"connection reset by peer",
				"504 gateway timeout",
				"server broke connection",
				"bad gateway"
            ]

            for known_error in known_errors:
                if known_error in error_message:
                    return error_message
            return None


def json_formatter():
    item = rail.result("get_required_invoices")
    return json.dumps(item, indent=4, ensure_ascii=False)

def last_sync_time(last_sync_var):
    sync_time = (datetime.now(
                timezone.utc) - timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')
    return rail.get_lastsync_time_variable(
        variable_name= last_sync_var,
        date_format='%Y-%m-%dT%H:%M:%SZ',
        initial_sync_time= sync_time,
        reset_after_threshold=False
        )

def update_last_sync(update_sync_time):
    return rail.set_lastsync_time_variable(
            variable_name= update_sync_time,
            value_to_set= rail.result('get_last_sync_time')['current_time']
        )