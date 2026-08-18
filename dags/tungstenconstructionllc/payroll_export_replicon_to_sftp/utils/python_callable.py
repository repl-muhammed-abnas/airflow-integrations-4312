# pylint: disable=unused-variable
from datetime import datetime
import rail

def get_today_date():
    now = datetime.utcnow()
    return {
        'year': now.year,
        'month': now.month,
        'day': now.day
    }

def get_today():
    return str(get_today_date()['day']) + '/' + str(get_today_date()['month']) + '/' + str(get_today_date()['year'])

def get_daterange_data(dag_run):
    start_date = str(datetime.strptime(dag_run.conf['webhook']['data']['dateRange'].split('-')[0], '%m%d%Y').date().strftime('%m/%d/%Y'))
    end_date = str(datetime.strptime(dag_run.conf['webhook']['data']['dateRange'].split('-')[1], '%m%d%Y').date().strftime('%m/%d/%Y'))
    return{
        'start_date' : start_date,
        'end_date' : end_date,
        'daterange_diff' : (datetime.strptime(end_date, "%m/%d/%Y") - datetime.strptime(start_date, "%m/%d/%Y")).days
    }

def add_userids(dag_run):
    reportfilterspayroll = []
    reportfiltersexpense = []
    reportfilterstimesheet = []

    user_ids = dag_run.conf['webhook']['data']['userIds'].split(',')

    for i,user_item in enumerate(user_ids):
        reportfilterspayroll.append({"value" : user_item ,
                                        "reportFilterUri" : rail.result('get_customization_payroll_report_data_uri')['userfilter_uri']
                                    })
        reportfiltersexpense.append({"value" : user_item ,
                                        "reportFilterUri" : rail.result('get_customization_expense_report_data_uri')['userfilter_uri']
                                    })
        reportfilterstimesheet.append({"value" : user_item ,
                                        "reportFilterUri" : rail.result('get_customization_timesheet_comments_report_data_uri')['userfilter_uri']
                                    })
    return {
        "reportfilterspayroll" : reportfilterspayroll,
        "reportfiltersexpense" : reportfiltersexpense,
        "reportfilterstimesheet" : reportfilterstimesheet
    }

def add_entry_dates():
    reportfilterspayroll = []
    reportfiltersexpense = []
    reportfilterstimesheet = []

    if rail.result('add_userids_to_lists'):
        reportfilterspayroll = rail.result('add_userids_to_lists')['reportfilterspayroll']
        reportfiltersexpense = rail.result('add_userids_to_lists')['reportfiltersexpense']
        reportfilterstimesheet = rail.result('add_userids_to_lists')['reportfilterstimesheet']


    null = None
    entry_start_date = rail.result('get_date_range_data')['start_date']
    entry_end_date = rail.result('get_date_range_data')['end_date']

    payroll_entry_date_filter_uri = rail.result('get_customization_payroll_report_data_uri')['entrydatefilter_uri']
    expense_entry_date_filter_uri = rail.result('get_customization_expense_report_data_uri')['daterangefilter_incurreduri']
    timesheet_entry_date_fliter_uri = rail.result('get_customization_timesheet_comments_report_data_uri')['entrydatefilter_uri']

    reportfilterspayroll.extend([{"value" : null , "reportFilterUri" : payroll_entry_date_filter_uri },
                                {"value" : entry_start_date , "reportFilterUri" : payroll_entry_date_filter_uri },
                                {"value" : entry_end_date , "reportFilterUri" : payroll_entry_date_filter_uri }])

    reportfiltersexpense.extend([{"value" : null , "reportFilterUri" : expense_entry_date_filter_uri },
                                {"value" : entry_start_date , "reportFilterUri" : expense_entry_date_filter_uri },
                                {"value" : entry_end_date , "reportFilterUri" : expense_entry_date_filter_uri }])

    reportfilterstimesheet.extend([{"value" : null , "reportFilterUri" : timesheet_entry_date_fliter_uri },
                                    {"value" : entry_start_date , "reportFilterUri" : timesheet_entry_date_fliter_uri },
                                    {"value" : entry_end_date , "reportFilterUri" : timesheet_entry_date_fliter_uri }])

    return {
        "reportfilterspayroll" : reportfilterspayroll,
        "reportfiltersexpense" : reportfiltersexpense,
        "reportfilterstimesheet" : reportfilterstimesheet
    }

def add_approval_status(dag_run):
    reportfilterspayroll = rail.result('add_entry_dates_to_lists')['reportfilterspayroll']
    reportfiltersexpense = rail.result('add_entry_dates_to_lists')['reportfiltersexpense']
    reportfilterstimesheet = rail.result('add_entry_dates_to_lists')['reportfilterstimesheet']

    approval_status_value_dict = {"Not Submitted" : "0" , "Waiting for Approval" : "1" , "Approved" : "2" , "Rejected" : "3" ,
                                    "0" : "0" , "1" : "1" , "2" : "2" , "3" : "3"}

    approval_status_list = dag_run.conf['webhook']['data']['timesheetApprovalStatusIds'].split(',')
    for i, approval_item in enumerate(approval_status_list):
        if approval_item in approval_status_value_dict:
            reportfiltersexpense.append({"value" : approval_status_value_dict[approval_status_list[i]] ,
                                            "reportFilterUri" : rail.result('get_customization_expense_report_data_uri')['approvalstatus_uri'] })
            reportfilterstimesheet.append({"value" : approval_status_value_dict[approval_status_list[i]] ,
                                            "reportFilterUri" : rail.result('get_customization_timesheet_comments_report_data_uri')['approvalstatus_uri'] })
    return {
        "reportfilterspayroll" : reportfilterspayroll,
        "reportfiltersexpense" : reportfiltersexpense,
        "reportfilterstimesheet" : reportfilterstimesheet
    }

def get_payroll_report_params():
    return {
        "reportParameters": [{
            "filterValues": rail.result('add_approval_status_to_lists')['reportfilterspayroll'],
            "outputFormatUri": "urn:replicon:report-output-format-option:csv",
            "reportUri": rail.result('get_report_uri')['payroll_report_uri']
        }
        ]
    }

def get_expense_report_params():
    return {
        "reportParameters": [{
            "filterValues": rail.result('add_approval_status_to_lists')['reportfiltersexpense'],
            "outputFormatUri": "urn:replicon:report-output-format-option:csv",
            "reportUri": rail.result('get_report_uri')['expense_report_uri']
        }
        ]
    }

def get_timesheet_report_params():
    return {
        "reportParameters": [{
            "filterValues": rail.result('add_approval_status_to_lists')['reportfilterstimesheet'],
            "outputFormatUri": "urn:replicon:report-output-format-option:csv",
            "reportUri": rail.result('get_report_uri')['timesheet_report_uri']
        }
        ]
    }

def query_statement_for_payroll_validated_status_data(dag_run):
    query_str = dag_run.conf['webhook']['data']['timesheetApprovalStatusIds'].split(',')
    if len(query_str) == 1 :
        validated_status_data_query = str(tuple(query_str)).replace(',', '')
    else:
        validated_status_data_query = str(tuple(query_str))
    return validated_status_data_query

def get_row(item):
    payroll_distinct_data = rail.load_all_records(rail.result("query_distinct_payroll_data"))
    payroll_input_data = rail.load_all_records(rail.result("query_from_payroll_input"))
    expense_input_data = rail.load_all_records(rail.result("query_from_expense_input"))
    timesheet_input_data = rail.load_all_records(rail.result("query_from_timesheet_comments"))

    firstname = rail.find_first_by_attr_and_get_attr(payroll_input_data, "loginname",item["loginname"], "firstname")
    lastname = rail.find_first_by_attr_and_get_attr(payroll_input_data, "loginname",item["loginname"], "lastname")

    filtered_payroll_list = list(filter(lambda d: d['loginname'] == item["loginname"], payroll_distinct_data))
    filtered_expense_list = list(filter(lambda d: d['loginname'] == item["loginname"], expense_input_data))
    filtered_timesheet_list = list(filter(lambda d: d['loginname'] == item["loginname"], timesheet_input_data))

    timeoff = 0
    regular = 0
    overtime = 0

    for i, payroll_item in enumerate(filtered_payroll_list):
        timeoff = timeoff + float(payroll_item['timeoffhours'])
        regular = regular + float(payroll_item['regularhours'])
        overtime = overtime + float(payroll_item['overtimehours'])

    equipment_cost = rail.find_first_by_attr_and_get_attr(payroll_input_data, "loginname",item["loginname"], "equipmentcost")
    if equipment_cost :
        equipmentcost = "$" + str(float(equipment_cost) * ( regular + overtime))
    else:
        equipmentcost = "Nil"

    comments_list = []
    for j, timesheet_item in enumerate(filtered_timesheet_list):
        comments_list.append(timesheet_item['comments'])

    return [
        firstname + " " + lastname,
        rail.find_first_by_attr_and_get_attr(payroll_input_data, "loginname",item["loginname"], "payrate"),
        equipmentcost,
        timeoff,
        regular,
        overtime,
        regular + overtime,
        rail.find_first_by_attr_and_get_attr(filtered_expense_list, "expensecode","Per Diem", "amount"),
        rail.find_first_by_attr_and_get_attr(filtered_expense_list, "expensecode","Truck Allowance", "amount"),
        rail.find_first_by_attr_and_get_attr(filtered_expense_list, "expensecode","Medical", "amount"),
        rail.find_first_by_attr_and_get_attr(filtered_expense_list, "expensecode","Supplies", "amount"),
        rail.find_first_by_attr_and_get_attr(filtered_expense_list, "expensecode","Loan", "amount"),
        rail.find_first_by_attr_and_get_attr(filtered_expense_list, "expensecode","Per Diem", "units"),
        rail.smartjoin_by_delim(comments_list, " ")
    ]
