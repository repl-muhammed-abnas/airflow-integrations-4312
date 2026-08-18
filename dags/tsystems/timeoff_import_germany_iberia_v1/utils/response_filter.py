"""
Response filters for T-Systems Germany/Iberia Time Off Import
"""

null = None  # Used to represent null values in responses


def get_hidden_oef_value(response):
    """
    Extract hidden OEF value for transaction ID field
    
    Args:
        response: API response
        
    Returns:
        dict: Hidden OEF value
    """
    return list(filter(lambda x: x['hidden_oef_name'] == 'Transaction ID', map(lambda row: {
        "hidden_oef_name": row['cells'][0]['textValue'],
        "hidden_oef_value": (row['cells'][1]['uri']).split(':')[-1],
    }, response['rows'])))[0]

def get_filtered_timeoff_details(response):
    """
    Filter time off type details
    
    Args:
        response: API response
        
    Returns:
        list: Filtered time off type details
    """
    if not response:
        return []
    
    return list(map(lambda row: {
        "displayText": row['displayText'],
        "description": row['description'],
        "enabled": row['enabled'],
        "uri": row['uri']
    }, response))


def get_filtered_output_empid(response, dag_run):
    """
    Filter user response to get basic user details
    
    Args:
        response: API response
        
    Returns:
        list: Filtered user details
    """
    return list(filter(lambda x: x['employee_id'] is not None and x['employee_id'] == dag_run.conf['employee_id'] and bool(x['status']), map(lambda row: {
        "name": row['cells'][0]['textValue'],
        "uri": row['cells'][0]['uri'],
        "employee_id": row['cells'][1]['textValue']if row['cells'][1]['dataType'] != 'urn:replicon:list-type:null' else null,
        "status": row['cells'][2]['boolValue']
    }, response['rows'])))

def get_filtered_output_user_info(response):
    """
    Filter user bulk response to get detailed user info
    
    Args:
        response: API response
        
    Returns:
        list: Filtered user info
    """
    return list(map(lambda row: {
        "timeoff_template": row['timeOffTemplate'],
        "start_date": row['userDetails']['employmentDateRange']['startDate'],
        "end_date": row['userDetails']['employmentDateRange']['endDate'],
    }, response))

def get_filtered_time_off_details_on_transaction_id(response, dag_run):
    """
    Filter time off records by transaction ID
    
    Args:
        response: API response
        
    Returns:
        list: Filtered time off records
    """
    return list(filter(lambda x: x['transaction_id'] == dag_run.conf['transaction_id'], map(lambda row: {
        "timeoff_uri": row['cells'][0]['uri'],
        "timeoff_type": row['cells'][1]['textValue'],
        "timeoff_type_uri": row['cells'][1]['uri'],
        "transaction_id": row['cells'][2]['textValue'],
        "timeoff_start_date": str(row['cells'][3]['dateValue']['day']).zfill(2)+'.'+str(row['cells'][3]['dateValue']['month']).zfill(2)
        + '.'+ str(row['cells'][3]['dateValue']['year']),
        "timeoff_end_date": str(row['cells'][4]['dateValue']['day']).zfill(2)+'.'+str(row['cells'][4]['dateValue']['month']).zfill(2)
        + '.'+ str(row['cells'][4]['dateValue']['year']),
        "timeoff_start_time":str(row['cells'][5]['timeValue']['hour']).zfill(2)+':'+str(row['cells'][5]['timeValue']['minute']).zfill(2)
            if row['cells'][5]['dataType'] != 'urn:replicon:list-type:null' else null,
        "timeoff_end_time":str(row['cells'][6]['timeValue']['hour']).zfill(2)+':'+str(row['cells'][6]['timeValue']['minute']).zfill(2)
            if row['cells'][6]['dataType'] != 'urn:replicon:list-type:null' else null,
        "hours": row['cells'][7]['numberValue']  if row['cells'][7]['numberValue'] != 'urn:replicon:list-type:null' else null,
    }, response['rows'])))
