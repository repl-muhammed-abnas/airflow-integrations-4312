# pylint: disable=unused-variable
from datetime import datetime
import rail

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

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
    start_date = str(datetime.strptime(dag_run.conf['startdate'], '%m-%d-%Y').date().strftime('%m/%d/%Y'))
    end_date = str(datetime.strptime(dag_run.conf['enddate'], '%m-%d-%Y').date().strftime('%m/%d/%Y'))
    return{
        'start_date' : start_date,
        'end_date' : end_date,
        'daterange_diff' : (datetime.strptime(end_date, "%m/%d/%Y") - datetime.strptime(start_date, "%m/%d/%Y")).days
    }

def add_entry_dates():
    reportfiltersproject = []
    reportfiltersnotinvoiced = []

    null = None
    start_date = rail.result('get_date_range_data')['start_date']
    end_date = rail.result('get_date_range_data')['end_date']

    project_filter_uri = rail.result('get_project_report_data_uri')['creationdatefilter_uri']
    not_invoiced_filter_uri = rail.result('get_not_invoiced_report_data_uri')['daterangefilter_uri']

    reportfiltersproject.extend([{"value" : null , "reportFilterUri" : project_filter_uri },
                                {"value" : start_date , "reportFilterUri" : project_filter_uri },
                                {"value" : end_date , "reportFilterUri" : project_filter_uri }])

    reportfiltersnotinvoiced.extend([{"value" : null , "reportFilterUri" : not_invoiced_filter_uri },
                                {"value" : start_date , "reportFilterUri" : not_invoiced_filter_uri },
                                {"value" : end_date , "reportFilterUri" : not_invoiced_filter_uri }])

    return {
        "reportfiltersproject" : reportfiltersproject,
        "reportfiltersnotinvoiced" : reportfiltersnotinvoiced
    }

def get_project_report_params():
    return {
        "reportParameters": [{
            "filterValues": rail.result('add_entry_dates_to_lists')['reportfiltersproject'],
            "outputFormatUri": "urn:replicon:report-output-format-option:csv",
            "reportUri": rail.result('get_report_uri')['project_report_uri']
        }
        ]
    }

def get_notinvoiced_report_params():
    return {
        "reportParameters": [{
            "filterValues": rail.result('add_entry_dates_to_lists')['reportfiltersnotinvoiced'],
            "outputFormatUri": "urn:replicon:report-output-format-option:csv",
            "reportUri": rail.result('get_report_uri')['not_invoiced_report_uri']
        }
        ]
    }

def add_project_data():
    finallist = []
    project_data_dict = {}
    project_data = rail.load_all_records(rail.result("query_from_project_input"))
    not_invoiced_data = rail.load_all_records(rail.result("query_from_notinvoiced_input"))

    for i, val in enumerate(project_data):
        # pylint: disable=cell-var-from-loop
        filtered_notinvoiced_list = list(filter(lambda d: d['client'] == val["client"] and \
                                                d['project'] == val["project"] and d['manager'] == val["manager"], not_invoiced_data))
        project_data_dict[i] = {}
        project_data_dict[i]['client'] = val['client']
        project_data_dict[i]['project'] = val['project']
        project_data_dict[i]['manager'] = val['manager']
        project_data_dict[i]['budget'] = val['totalbudget']
        project_data_dict[i]['totalinvoiceamount'] = val['totalinvoiceamount']
        if len(filtered_notinvoiced_list) > 0:
            project_data_dict[i]['notinvoicedamount'] = filtered_notinvoiced_list[0]['notinvoicedamount'].split('$')[-1].replace(',' , '') if is_number(filtered_notinvoiced_list[0]['notinvoicedamount'].split('$')[-1].replace(',' , '')) else 0
            project_data_dict[i]['remainingbudget'] = (float(val['remainingbudget'].replace(',' , '')) if is_number(val['remainingbudget'].replace(',' , '')) else 0) - (float(
                filtered_notinvoiced_list[0]['notinvoicedamount'].split('$')[-1].replace(',' , '')) if is_number(filtered_notinvoiced_list[0]['notinvoicedamount'].split('$')[-1].replace(',' , '')) else 0)
        else:
            project_data_dict[i]['notinvoicedamount'] = 0
            project_data_dict[i]['remainingbudget'] = float(val['remainingbudget'].replace(',' , '')) if is_number(val['remainingbudget'].replace(',' , '')) else 0

        finallist.append(project_data_dict[i])

    return finallist

def add_notinvoiced_data():
    finallist = []
    not_invoiced_distinct_data_dict = {}
    if rail.result('add_project_data_to_final_list'):
        finallist = rail.result('add_project_data_to_final_list')

    not_invoiced_distinct_data = rail.load_all_records(rail.result("query_distinct_not_invoiced"))

    for i, val in enumerate(not_invoiced_distinct_data):
        not_invoiced_distinct_data_dict[i] = {}
        not_invoiced_distinct_data_dict[i]['client'] = val['client']
        not_invoiced_distinct_data_dict[i]['project'] = val['project']
        not_invoiced_distinct_data_dict[i]['manager'] = val['manager']
        not_invoiced_distinct_data_dict[i]['budget'] = val['totalbudget']
        not_invoiced_distinct_data_dict[i]['totalinvoiceamount'] = 0
        if val['notinvoicedamount']:
            not_invoiced_distinct_data_dict[i]['notinvoicedamount'] = val['notinvoicedamount'].split('$')[-1].replace(',' , '') if is_number(val['notinvoicedamount'].split('$')[-1].replace(',' , '')) else 0
            not_invoiced_distinct_data_dict[i]['remainingbudget'] = (float(val['totalbudget'].split('$')[-1].replace(',' , '')) if is_number(val['totalbudget'].split('$')[-1].replace(',' , '')) else 0) - (float(
                val['notinvoicedamount'].split('$')[-1].replace(',' , '')) if is_number(val['notinvoicedamount'].split('$')[-1].replace(',' , '')) else 0)
        else:
            not_invoiced_distinct_data_dict[i]['notinvoicedamount'] = 0
            not_invoiced_distinct_data_dict[i]['remainingbudget'] = float(val['totalbudget'].split('$')[-1].replace(',' , '')) if is_number(val['totalbudget'].split('$')[-1].replace(',' , '')) else 0

        finallist.append(not_invoiced_distinct_data_dict[i])

    return finallist

def get_final_list_data():
    if rail.result('add_not_invoiced_data_to_final_list'):
        return rail.result('add_not_invoiced_data_to_final_list')
    return rail.result('add_project_data_to_final_list')

def get_rows(item):
    return [
        item["client"],
        item["project"],
        item["manager"],
        item["budget"],
        item["totalinvoiceamount"],
        item["notinvoicedamount"],
        item["remainingbudget"]
    ]
