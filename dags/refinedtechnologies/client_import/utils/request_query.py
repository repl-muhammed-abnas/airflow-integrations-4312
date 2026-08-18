
def search_user_in_salesforce_query(salesforce_record):
    """SOQL: the account owner (User) by OwnerId."""
    owner_id = salesforce_record.get('OwnerId', '')
    query = f"SELECT FIELDS(ALL) FROM User WHERE Id = '{owner_id}' LIMIT 150"
    return query


def search_contact_in_salesforce_query(salesforce_record):
    """SOQL: contacts for the account by AccountId."""
    account_id = salesforce_record.get('Id', '')
    query = f"SELECT FirstName, LastName, Email FROM Contact where AccountId = '{account_id}' LIMIT 200"
    return query

