from dkpierceassociates.client_sync import config

def search_replicon_client_managers_query(payload):
    if not payload:
        raise ValueError("No payload provided to search_replicon_client_managers_query")

    records = payload.get('records', [])
    if not records:
        raise ValueError("No records in payload for search_replicon_client_managers_query")

    client_manager = records[0].get('Client_Manager__c', '')
    if not client_manager:
        raise ValueError("Client_Manager__c is missing from the account record")

    query = f"""SELECT fields(all) FROM Replicon_Client_Manager__c where Id = '{client_manager}' limit {config.salesforce_client_manager_query_limit}"""
    return query