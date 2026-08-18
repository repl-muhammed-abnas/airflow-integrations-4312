import json
import rail

def process_validation_msg_format(validation_msg_data, valid_ts_report_data):
    validation_msg_data = json.loads(validation_msg_data)
    valid_ts_report_data = json.loads(valid_ts_report_data)
    result = []
    result = [{
        'timesheeturi': item['objectUri'],
        'Validationrule': "present" if len(item['validationResult']['validationMessages']) > 0 else "notpresent",
        'username': rail.find_first_by_attr_and_get_attr(valid_ts_report_data,'TimesheetPeriodUri',item['objectUri'],'User_Name',''),
        'timesheetperiod': rail.find_first_by_attr_and_get_attr(valid_ts_report_data,'TimesheetPeriodUri',item['objectUri'],'Timesheet_Period',''),
        'validationmessage': item['validationResult']['validationMessages'][0]['displayText'] 
                                if item['validationResult']['validationMessages'] and item['validationResult']['validationMessages'][0]['displayText']
                                else ''
    } for item in validation_msg_data]
    return {
        "latestvalidattionresults": list(filter(lambda x: x['Validationrule'] == 'notpresent',result)),
        "forlogging": list(filter(lambda x: x['Validationrule'] == 'present',result))
    }
