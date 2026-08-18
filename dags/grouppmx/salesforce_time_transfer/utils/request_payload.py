from datetime import datetime as dt
import rail
null = None

def get_client_list_search_param(client_name):
    return {
        "page": 1,
        "pagesize": 100000000,
        "columnUris": [
            "urn:replicon:client-list-column:name",
            "urn:replicon:client-list-column:client"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:client-list-filter:name"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": null,
                    "uris": [],
                    "bool": null,
                    "date": null,
                    "money": null,
                    "number": null,
                    "text": client_name,
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null,
                    "dateTimeUtcRange": null
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }

def create_account_payload():
    client_data = rail.result("search_client_in_replicon")
    return [{
        'Name': client_data['name'],
        'BillingStreet': client_data['billingAddress']['address'],
        'BillingCity': client_data['billingAddress']['city'],
        'BillingCountry': client_data['billingAddress']['country'],
        'BillingState': client_data['billingAddress']['stateProvince'],
        'BillingPostalCode': client_data['billingAddress']['zipPostalCode'],
        'Client_Code__c': client_data['code'],
        'Description': client_data['comment'],
        'Replicon_ID__c': client_data['uri']
        }
    ]

def get_formatted_date(_date):
    return f"{_date['year']}-{_date['month']}-{_date['day']}" if _date else None

def create_project_payload():
    project_data = rail.result("search_project_in_replicon")
    return [{
        'Name': project_data['name'][slice(0,79)],
        'Billing_Type__c': project_data['billingType']['displayText'].split('s')[0],
        'Project_Manager_Replicon__c': project_data['projectLeader']['displayText'] if project_data['projectLeader'] else None,
        'Start_Date__c': get_formatted_date(project_data['timeEntryDateRange']['startDate']),
        'End_Date__c': get_formatted_date(project_data['timeEntryDateRange']['endDate']),
        'Time_And_Expense_Entry_Type__c': 'Only Billable' if 'Billable Only' in project_data[
            'timeAndExpenseEntryType']['displayText'] else project_data['timeAndExpenseEntryType']['displayText'],
        'Replicon_ID__c': project_data['uri'],
        'Full_Project_Name_Replicon__c': project_data['name'],
        'Status__c': project_data['status']['name'],
        'Project_Code__c': project_data['code']
        }
    ]

def convert_date_formats(_date,_format, required_format = '%m/%d/%YT%H:%M %p'):
    return dt.strptime(_date,_format).strftime(required_format)

def update_timesheet_payload():
    project_data = rail.result("get_required_details")
    return [
        {
            "Id": rail.result("search_timesheets_in_salesforce")['records'][0]['Id'],
            "Timesheet_Approved_On__c" : dt.strptime(project_data["approved_on"],'%b %d, %Y %I:%M:%S %p').strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '0000',
            "Timesheet_Submitted_On__c" : dt.strptime(project_data["submitted_on"],'%b %d, %Y').strftime('%Y-%m-%d')
        }
    ]

def create_timesheet_payload(dag_run):
    project_data = rail.result("get_required_details")
    return [
        {
            "Contact_URI__c": project_data['user_uri'],
            "End_Date__c" : dt.strptime(project_data["end_date"],'%b %d, %Y').strftime('%Y-%m-%d'),
            "Replicon_ID__c" : dag_run.conf['timesheet_uri'],
            "Resource__c": project_data["user_name"],
            "Start_Date__c" : dt.strptime(project_data["start_date"],'%b %d, %Y').strftime('%Y-%m-%d'),
            "Status__c": "Approved",
            "Timesheet_Approved_On__c" : dt.strptime(project_data["approved_on"],'%b %d, %Y %I:%M:%S %p').strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '0000',
            "Timesheet_Submitted_On__c" : dt.strptime(project_data["submitted_on"],'%b %d, %Y').strftime('%Y-%m-%d')
        }
    ]

def get_required_resource_details(contactid, data= None):
    return {
        'project_resource': rail.find_first_by_attr_and_get_attr(rail.result(
            "get_project_resources_in_salesforce")['records'],'Resource_Name__c', contactid,'Id'),
        'resource_ts_association': rail.find_first_by_attr_and_get_attr(rail.result(
            "get_project_resources_in_salesforce")['records'],'Project_Resource__c', data,'Id')
    }

def get_required_pto_resource_details(contactid, data= None):
    return {
        'project_resource': rail.find_first_by_attr_and_get_attr(rail.result(
            "get_pto_project_resources")['records'],'Resource_Name__c', contactid,'Id'),
        'resource_ts_association': rail.find_first_by_attr_and_get_attr(rail.result(
            "get_pto_project_resources")['records'],'Project_Resource__c', data,'Id')
    }

def get_create_time_entry_payload(dag_run,task_name= False,type = False):
    return [{
        'Resource_Timesheet__c': rail.result("create_timesheet_resource_association")[0]['id'] if rail.result(
            "create_timesheet_resource_association") else get_required_resource_details(dag_run.conf[
                'contactid'], get_required_resource_details(dag_run.conf['contactid'])['project_resource'])[
                    'resource_ts_association'] if type != 'regular' else rail.result(task_name)[0]['id'],
        'Timesheet__c': dag_run.conf['timesheetid'],
        'Activity__c': dag_run.conf['activity_name'],
        'Billing_Rate__c': dag_run.conf['billingrate_amount'],
        'Billing_Role__c': dag_run.conf['billingrate_name'],
        'Date__c': dt.strptime(dag_run.conf['entry_date'],'%b %d, %Y').strftime('%Y-%m-%d'),
        'Project__c': dag_run.conf['projectid'],
        'Resource__c': dag_run.conf['contact'],
        'Resource_Supervisor__c': dag_run.conf['approver_name'],
        'Duration__c': dag_run.conf['billing_hours'],
        'Bill_Client__c': True,
        'Time_Off_Type__c': dag_run.conf['timeoff_type']
    }] if float(dag_run.conf['billing_hours']) > 0 else [{
        'Resource_Timesheet__c': rail.result("create_timesheet_resource_association")[0]['id'] if rail.result(
            "create_timesheet_resource_association") else get_required_resource_details(dag_run.conf[
                'contactid'], get_required_resource_details(dag_run.conf['contactid'])['project_resource'])[
                    'resource_ts_association'] if type != 'regular' else rail.result(task_name)[0]['id'],
        'Timesheet__c': dag_run.conf['timesheetid'],
        'Activity__c': dag_run.conf['activity_name'],
        'Billing_Rate__c': dag_run.conf['billingrate_amount'],
        'Billing_Role__c': dag_run.conf['billingrate_name'],
        'Date__c': dt.strptime(dag_run.conf['entry_date'],'%b %d, %Y').strftime('%Y-%m-%d'),
        'Project__c': dag_run.conf['projectid'],
        'Resource__c': dag_run.conf['contact'],
        'Resource_Supervisor__c': dag_run.conf['approver_name'],
        'Bill_Client__c': False,
        'Time_Off_Type__c': dag_run.conf['timeoff_type'],
        'Non_Billable_Duration__c': dag_run.conf['nonbillable_hours']
    }]

def get_create_time_off_entry_payload(dag_run,task_name= False,_type = False):
    return [{
        'Resource_Timesheet__c': rail.result("create_timesheet_pto_resource_association")[0]['id'] if rail.result(
            "create_timesheet_pto_resource_association") else get_required_pto_resource_details(dag_run.conf[
                'contactid'], get_required_pto_resource_details(dag_run.conf['contactid'])['project_resource'])[
                    'resource_ts_association'] if _type != 'timeoff' else rail.result(task_name)[0]['id'],
        'Timesheet__c': dag_run.conf['timesheetid'],
        'Activity__c': dag_run.conf['activity_name'],
        'Billing_Rate__c': dag_run.conf['billingrate_amount'],
        'Billing_Role__c': dag_run.conf['billingrate_name'],
        'Date__c': dt.strptime(dag_run.conf['entry_date'],'%b %d, %Y').strftime('%Y-%m-%d'),
        'Project__c': 'a021C00000Tgx8KQAR',
        'Resource__c': dag_run.conf['contact'],
        'Resource_Supervisor__c': dag_run.conf['approver_name'],
        'Time_Off_Duration__c': dag_run.conf['timeoff_hours'],
        'Bill_Client__c': False,
        'Time_Off_Type__c': dag_run.conf['timeoff_type']
    }]
