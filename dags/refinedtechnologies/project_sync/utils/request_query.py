from refinedtechnologies.project_sync.utils.custom_function import safe_get_salesforce_record

def search_user_in_salesforce_query(data):
    record = safe_get_salesforce_record(data)
    if not record:
        return "SELECT Username FROM User WHERE Id = '' LIMIT 0"

    owner_id = record.get('OwnerId', '').replace("'", "\\'")
    query = f"SELECT Username FROM User WHERE Id = '{owner_id}' LIMIT 150"
    return query

def search_contact_in_salesforce_query(salesforce_record):
    """SOQL: contacts by AccountId. Accepts a full {records:[...]} response or a single record."""
    if isinstance(salesforce_record, dict) and 'records' in salesforce_record:
        record = safe_get_salesforce_record(salesforce_record)
        if not record:
            return "SELECT FirstName, LastName, Email FROM Contact WHERE Id = '' LIMIT 0"
        account_id = record.get('Id', '').replace("'", "\\'")
    else:
        account_id = salesforce_record.get('Id', '').replace("'", "\\'") if salesforce_record else ''

    query = f"SELECT FirstName, LastName, Email FROM Contact WHERE AccountId = '{account_id}' LIMIT 200"
    return query

def specific_account_query(data):
    # Recipe fetches the Account directly by AccountId (facilityid), not by name.
    account_id = data.get('AccountId', '').replace("'", "\\'")
    query = f"""SELECT
        Id,
        Name,
        Legacy_Id__c,
        Description,
        ShippingStreet,
        ShippingCity,
        ShippingState,
        ShippingCountry,
        ShippingPostalCode,
        Phone,
        Fax,
        Website,
        BillingStreet,
        BillingCity,
        BillingState,
        BillingCountry,
        BillingPostalCode,
        OwnerId
    FROM Account
    WHERE Id = '{account_id}'
    LIMIT 1"""
    return query

def account_by_id_query(data):
    """Lightweight Account lookup by the opportunity's AccountId for the name-change check."""
    account_id = data.get('AccountId', '').replace("'", "\\'")
    return (
        "SELECT Id, Name, Legacy_Id__c FROM Account "
        f"WHERE Id = '{account_id}' LIMIT 1"
    )

def search_user_in_salesforce(data):
    owner_id = data.get('OwnerId', '').replace("'", "\\'") 
    query = f"""SELECT
        Id,
        Username,
        FirstName,
        LastName,
        Email,
        IsActive
    FROM User
    WHERE Id = '{owner_id}'
    LIMIT 200"""
    return query