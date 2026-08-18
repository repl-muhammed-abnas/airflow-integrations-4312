from dkpierceassociates.project_sync import config

def search_account_in_salesforce_query(data):
    records = data.get('records', [])
    if not records:
        raise ValueError("No records provided to search_account_in_salesforce_query")

    account_id = records[0].get('AccountId', '').replace("'", "\\'")
    if not account_id:
        raise ValueError("AccountId is missing from the opportunity record")

    query = f"select fields(all) from account WHERE Id = '{account_id}' limit {config.salesforce_account_query_limit}"
    return query

def searchRepliconProjectManagers_query(payload):
    if not payload:
        raise ValueError("No payload provided to searchRepliconProjectManagers_query")

    records = payload.get('records', [])
    if not records:
        raise ValueError("No records in payload for searchRepliconProjectManagers_query")

    project_manager = records[0].get('Project_Manager__c', '').replace("'", "\\'")
    if not project_manager:
        raise ValueError("Project_Manager__c is missing from the opportunity record")

    query = f"""SELECT fields(all) FROM Replicon_Project_Managers__c where Name = '{project_manager}' limit {config.salesforce_project_manager_query_limit}"""
    return query