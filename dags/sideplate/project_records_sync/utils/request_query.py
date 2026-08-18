from sideplate.project_records_sync import config

def search_account_in_salesforce_query(data):
    records = data.get('records', [])
    if not records:
        raise ValueError("No records provided to search_account_in_salesforce_query")

    account_id = records[0].get('Account__c', '').replace("'", "\\'")
    if not account_id:
        raise ValueError("Account__c is missing from the opportunity record")

    query = f"""SELECT
          Id,
          Name,
          BillingCity,
          BillingState,
          BillingStreet,
          BillingCountry,
          BillingPostalCode
      FROM Account
      WHERE Id = '{account_id}'"""
    return query

def search_project_in_salesforce_query(payload):
    if not payload:
        raise ValueError("No payload provided to search_project_in_salesforce_query")
    project_id = payload.get('Salesforceprojectid', '').replace("'", "\\'")
    if not project_id:
        raise ValueError("Project_Manager__c is missing from the record")
    query = f"""SELECT
          Project_Number__c,
          Opp_ID__c,
          Opp_Name__c,
          Opp_Category__c,
          Opp_State__c,
          OwnerId,
          Opp_Owner_Name__c,
          R_Value__c,
          Sector__c,
          Software__c,
          PC_Reqd__c,
          Opp_Stage__c,
          Approx_Sq_Ftg_for_Fee_Email__c,
          Primary_design_criteria__c,
          Updated_of_SP_Joints__c,
          Updated_Qty_of_Bldgs__c,
          SP_Bolted__c,
          Fees_Per_Sq_Ft__c,
          Updated_of_Stories__c,
          Why_we_won__c,
          PD_Lead_Engineer__c,
          Project_Engineer__c,
          Sum_of_Project_Amounts__c,
          Milestone_Status__c,
          Active__c,
          Opp_Close_Date__c
      FROM Project__c
      WHERE Id = '{project_id}'"""
    return query

def search_contact_in_salesforce_query(payload):
    if not payload:
        raise ValueError("No payload provided to search_contact_in_salesforce_query")

    records = payload.get('records') or []
    if not records:
        raise ValueError("No records in payload for search_contact_in_salesforce_query")
    record = records[0]
    
    project_id = record.get('PD_Lead_Engineer__c', '').replace("'", "\\'")
    if not project_id:
        raise ValueError("PD_Lead_Engineer__c is missing from the record")

    query = f"""SELECT Full_Name__c FROM Contact where Id = '{project_id}'"""
    return query

def search_project_engineer_contact_in_salesforce_query(payload):
    if not payload:
        raise ValueError("No payload provided to search_project_engineer_contact_in_salesforce_query")

    records = payload.get('records') or []
    if not records:
        raise ValueError("No records in payload for search_contact_in_salesforce_query")
    record = records[0]
    
    project_id = record.get('Project_Engineer__c', '').replace("'", "\\'")
    if not project_id:
        raise ValueError("Project_Engineer__c is missing from the record")

    query = f"""SELECT Full_Name__c FROM Contact where Id = '{project_id}'"""
    return query

def search_pd_engineer_contact_in_salesforce_query(payload):
    if not payload:
        raise ValueError("No payload provided to search_pd_engineer_contact_in_salesforce_query")

    records = payload.get('records') or []
    if not records:
        raise ValueError("No records in payload for search_pd_engineer_contact_in_salesforce_query")
    record = records[0]

    project_id = record.get('PD_Engineer__c', '').replace("'", "\\'")
    if not project_id:
        raise ValueError("PD_Engineer__c is missing from the record")

    query = f"""SELECT Full_Name__c FROM Contact where Id = '{project_id}'"""
    return query