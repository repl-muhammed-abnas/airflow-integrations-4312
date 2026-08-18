import json
from datetime import datetime

null = None

def get_report_filter(date_filter_uri, start_date, end_date):
    return [
        {
            "reportFilterUri": date_filter_uri,
            "value": null
        },
        {
            "reportFilterUri": date_filter_uri,
            "value": datetime.strptime(start_date, '%Y/%m/%d').strftime("%m/%d/%Y")
        },
        {
            "reportFilterUri": date_filter_uri,
            "value": datetime.strptime(end_date, '%Y/%m/%d').strftime("%m/%d/%Y")
        }
    ]


def get_filter_fields(report_filters):
    report_filters = json.loads(report_filters)
    valid_filters = [key_value for filters in report_filters if filters for key_value in filters]

    return valid_filters

def mandatory_data_checks(dag_run):
    return bool(not dag_run.conf['webhook']['data'] or
                dag_run.conf['webhook']['data'].get('requestor') == None or not dag_run.conf['webhook']['data']['requestor'] or
                dag_run.conf['webhook']['data'].get('reportType') == None or not dag_run.conf['webhook']['data']['reportType'] or
                dag_run.conf['webhook']['data'].get('dateRange') == None or
                dag_run.conf['webhook']['data']['dateRange'].get('startDate') == None or not dag_run.conf['webhook']['data']['dateRange']['startDate'] or
                dag_run.conf['webhook']['data']['dateRange'].get('endDate') == None or not dag_run.conf['webhook']['data']['dateRange']['endDate'] or
                dag_run.conf['webhook']['data'].get('timetype') == None or not dag_run.conf['webhook']['data']['timetype'])

def valid_requestor_check(dag_run):
    return bool(str(dag_run.conf['webhook']['data']['requestor']).lower() !='c1' and
                str(dag_run.conf['webhook']['data']['requestor']).lower() !='compass-pj1' and
                str(dag_run.conf['webhook']['data']['requestor']).lower() !='compass-pn1' and
                str(dag_run.conf['webhook']['data']['requestor']).lower() !='compass-p01' and
                str(dag_run.conf['webhook']['data']['requestor']).lower() !='compass' and
                str(dag_run.conf['webhook']['data']['requestor']).lower() !='compass-nt1' and
                str(dag_run.conf['webhook']['data']['requestor']).lower() !='compass-nt2' and
                str(dag_run.conf['webhook']['data']['requestor']).lower() !='compass-nt3' and
                str(dag_run.conf['webhook']['data']['requestor']).lower() !='ftp'
                )

def valid_wbs_check(dag_run):
    return bool(dag_run.conf['webhook']['data'].get('WBS') != None and
                len(dag_run.conf['webhook']['data']['WBS']) > 0 and
                dag_run.conf['webhook']['data']['WBS'][0]['value'])

def valid_user_check(dag_run):
    return bool(dag_run.conf['webhook']['data'].get('users') != None and
                len(dag_run.conf['webhook']['data']['users']) > 0 and
                dag_run.conf['webhook']['data']['users'][0]['value'])

def valid_company_code_check(dag_run):
    return bool(dag_run.conf['webhook']['data'].get('companyCode') != None and
                len(dag_run.conf['webhook']['data']['companyCode']) > 0 and
                dag_run.conf['webhook']['data']['companyCode'][0]['value'])

def valid_clientid_check(dag_run):
    return bool(dag_run.conf['webhook']['data'].get('clientid') != None and
                len(dag_run.conf['webhook']['data']['clientid']) > 0 and
                dag_run.conf['webhook']['data']['clientid'][0]['value'])

def valid_program_check(dag_run):
    return bool(dag_run.conf['webhook']['data'].get('program') != None and
                len(dag_run.conf['webhook']['data']['program']) > 0 and
                dag_run.conf['webhook']['data']['program'][0]['value'])

def valid_soldtoparty_check(dag_run):
    return bool(dag_run.conf['webhook']['data'].get('soldtoparty') != None and
                dag_run.conf['webhook']['data']['soldtoparty'])
