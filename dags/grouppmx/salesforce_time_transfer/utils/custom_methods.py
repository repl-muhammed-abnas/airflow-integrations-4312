import rail

def get_filter_payload():
    filter_uri_expr = rail.find_first_by_attr_and_get_attr(rail.result('get_time_entry_report_details')[
        'filterConfiguration']['enabledFilters'],'displayText', 'ApprovalDateFilter', 'uri')
    return [
        {
            "reportFilterUri": filter_uri_expr,
            "value": "Yesterday"
        },
        {
            "reportFilterUri": filter_uri_expr,
            "value": None
        },
        {
            "reportFilterUri": filter_uri_expr,
            "value": None
        },
    ]

def get_account_details(dag_run):
    return [{
        'account_id': rail.result("search_accounts_in_salesforce")['account_id'] if rail.result(
            "search_accounts_in_salesforce")['account_id'] else rail.result("create_account_in_salesforce")[0]['id'],
        'account_name': rail.result("search_accounts_in_salesforce")['account_name'] if rail.result(
            "search_accounts_in_salesforce")['account_id'] else rail.result("search_client_in_replicon")['name'],
        'client_uri': dag_run.conf['client_uri']
    }]

def get_project_details(dag_run):
    return [{
        'project_id': rail.result("search_projects_in_salesforce")['project_id'] if rail.result(
            "search_projects_in_salesforce")['project_id'] else rail.result("create_project_in_salesforce")[0]['id'],
        'project_name': rail.result("search_projects_in_salesforce")['project_name'] if rail.result(
            "search_projects_in_salesforce")['project_id'] else rail.result("search_project_in_replicon")['name'],
        'project_uri': dag_run.conf['project_uri']
    }]

def get_csv_lines(item, dag_run):
    return [
        item['TimesheetPeriodUri'],
        item['UserUri'],
        item['ProjectUri'],
        item['Entry_Date'],
        item['User_Supervisor_Name__Current_'],
        item['Activity_Name'],
        item['Billing_Rate_Name'],
        item['Hours_Worked'],
        item['Time_Off_Hrs'],
        item['User_Name'],
        item['Project_Name'],
        item['Client_Name'],
        item['Billing_Rate_Amount'],
        rail.result("search_timesheets_in_salesforce")['records'][0]['Id'] if rail.result(
            "search_timesheets_in_salesforce")['records'] else rail.result("create_timesheet_in_salesforce")[0]['id'],
        rail.find_first_by_attr_and_get_attr(dag_run.conf['accounts_data'],'account_uri',item['ClientUri'],'account_id'),
        rail.find_first_by_attr_and_get_attr(dag_run.conf['projects_data'],'project_uri',item['ProjectUri'],'project_id'),
        rail.result("search_contacts_in_salesforce")['records'][0]['Id'],
        item['Actual_Billable_Hours__Selected_Dates_'],
        item['Actual_Non_Billable_Hours__Selected_Dates_'],
        item['Time_Off_Type'],
        item['Approval_Status'],
        item['Submitted_On'],
        item['Approver_Name'],
        item['Approval_Date_Time']
    ]

def get_required_timesheet_details(dag_run):
    return {
        'user_uri': rail.find_first_by_attr_and_get_attr(
            dag_run.conf['report_data'], 'TimesheetPeriodUri', dag_run.conf['timesheet_uri'], 'UserUri'),
        'start_date': rail.find_first_by_attr_and_get_attr(
            dag_run.conf['report_data'], 'TimesheetPeriodUri', dag_run.conf['timesheet_uri'], 'Timesheet_Start_Date'),
        'end_date': rail.find_first_by_attr_and_get_attr(
            dag_run.conf['report_data'], 'TimesheetPeriodUri', dag_run.conf['timesheet_uri'], 'Timesheet_End_Date'),
        'user_name': rail.find_first_by_attr_and_get_attr(
            dag_run.conf['report_data'], 'TimesheetPeriodUri', dag_run.conf['timesheet_uri'], 'User_Name'),
        'approved_on': rail.find_first_by_attr_and_get_attr(
            dag_run.conf['report_data'], 'TimesheetPeriodUri', dag_run.conf['timesheet_uri'], 'Approval_Date_Time'),
        'submitted_on': rail.find_first_by_attr_and_get_attr(
            dag_run.conf['report_data'], 'TimesheetPeriodUri', dag_run.conf['timesheet_uri'], 'Submitted_On')

    }

def do_format_logs():
    log_artifacts = []
    log_records = []

    logs = rail.result("create_log")

    if logs:
        if isinstance(logs, list):
            log_artifacts.extend(logs)
        else:
            log_artifacts.append(logs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = rail.load_all_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    final_log_records = []

    final_log_records = list(map(lambda log: {
        **{
            'jobid': log['ecid']
        },
        **log['properties'],
        }, log_records))

    rail.set_result(key="error_record_count", val= len(list(filter(lambda x: x['status'] == 'Error', final_log_records ))))
    rail.set_result(key="success_record_count", val= len(list(filter(lambda x: x['status'] == 'Success', final_log_records ))))
    rail.set_result(key="exception_record_count", val= len(list(filter(lambda x: x['status'] == 'Exception', final_log_records ))))
    rail.set_result(key="total_record_count", val=rail.result("query_uniq_timesheets", key="length"))

    return final_log_records
