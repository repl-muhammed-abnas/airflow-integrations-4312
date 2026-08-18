# pylint: disable=too-many-statements line-too-long redefined-outer-name
from datetime import datetime
import pendulum
from rail import find_first_by_attr_and_get_attr, result, load_all_records


def email_ids_for_logs(dag_run):
    emailaddress = dag_run.conf['webhook']['data']['emailIds']
    return emailaddress + ',' + result("get_user_details")["emailAddress"] if emailaddress else result("get_user_details")["emailAddress"]


def check_if_daterange_does_not_contains_null(dag_run):
    date = (dag_run.conf['webhook']['data']
                 ['dateRange']).split('-')
    return (len(date) > 1 and len(date[0].strip()) == 8 and len(date[1].strip()) == 8) and (date[0] != 'null' or date[1] != 'null')


def match_column_configuration():
    columns = "Funders Name,Project Name,Time Type Name,Time Type Code,Timesheet Period,Entry Date,Replace,Hours Worked,Work Location,Labor Metrics,Week (Entry Date),Normalization Required?,Employee ID,Employee No.,Login Name,Employee Category"
    config_columns = ''
    for config_value in result('get_report_details')['columnConfiguration']:
        if config_columns:
            config_columns += "," + config_value['column']['displayText']
        else:
            config_columns = config_value['column']['displayText']
    return columns == config_columns


def report_filter_for_payroll_data(conf, enabledFilters):
    daterange = (conf['webhook']['data']['dateRange']).split('-')
    approvalids = (conf['webhook']['data']['timesheetApprovalStatusIds']).split(",")
    report_filter = [
        {
            "reportFilterUri": find_first_by_attr_and_get_attr(enabledFilters, 'displayText', 'TimesheetPeriodFilter', 'uri'),
            "value": None
        },
        {
            "reportFilterUri": find_first_by_attr_and_get_attr(enabledFilters, 'displayText', 'TimesheetPeriodFilter', 'uri'),
            "value": f"{daterange[0][:2]}/{daterange[0][2:4]}/{daterange[0][4:]}"
        },
        {
            "reportFilterUri": find_first_by_attr_and_get_attr(enabledFilters, 'displayText', 'TimesheetPeriodFilter', 'uri'),
            "value": f"{daterange[1][:2]}/{daterange[1][2:4]}/{daterange[1][4:]}"
        }
    ]
    for approvalid in approvalids:
        report_filter.append(
            {
                "reportFilterUri": find_first_by_attr_and_get_attr(enabledFilters, 'displayText', 'ApprovalStatusFilter', 'uri'),
                "value": {
                    "Not Submitted": "0",
                    "Waiting for Approval": "1",
                    "Approved": "2",
                    "Rejected": "3"
                }.get(approvalid, "4")
            }
        )
    return report_filter


def get_date_range(dag_run):
    daterange = (dag_run.conf['webhook']['data']
                ['dateRange']).split('-')
    return f"{daterange[0][:2]}/{daterange[0][2:4]}/{daterange[0][4:]}-{daterange[1][:2]}/{daterange[1][2:4]}/{daterange[1][4:]}"


def get_username_and_time_now(dag_run):
    return {
        "username": dag_run.conf['username'].split("|")[0].strip(),
        "timenow": pendulum.now().strftime('%m/%d/%YT%H:%M:%S'),
        "date_field": dag_run.conf['date_field']
    }


def get_weekentrydate(entrydate):
    date = datetime.strptime(entrydate, '%b %d, %Y')
    return date.isocalendar().week + 1 if date.weekday() == 6 else date.isocalendar().week


def get_csv_row_data(item):
    return [
        item['clientname'],
        item['projectname'],
        item['taskname'],
        item['taskcode'],
        item['timesheetperiod'],
        datetime.strptime(item['entrydate'], '%b %d, %Y').strftime('%Y-%m-%d'),
        item['replace'],
        float(item['hoursworked']),
        item['worklocation'],
        item['labormetrics'],
        get_weekentrydate(item['entrydate']),
        item['normalizationrequired'],
        item['employeeid'],
        int(item['employeeno']),
        item['loginname'],
        item['employeecategory']
    ]


def get_final_data_per_week_data(item, week):
    def get_hrs():
        total_hrs = 0.0
        all_records = load_all_records(result("get_data_related_to_weeks"))
        for record in all_records:
            hoursworked = record['hoursworked'] if record['hoursworked'] else 0.0
            total_hrs += float(hoursworked) if record['taskcode'] == item['taskcode'] and record['replace'] == item[
                'replace'] and record['worklocation'] == item['worklocation'] and record['labormetrics'] == item[
                "labormetrics"] and record['employeeno'] == item['employeeno'] else 0.0
        return total_hrs
    return {
        'employeenumber': item['employeeno'],
        'replace': item['replace'],
        'code': item['taskcode'],
        'hrs': get_hrs(),
        'worklocation': item['worklocation'],
        'labormetrics': item["labormetrics"],
        'week': week
    }


def get_final_data_per_week_data_37_60(item, week):
    return {
        'employeenumber': item['employeeno'],
        'replace': item['replace'],
        'code': item['taskcode'],
        'hrs': float(item['hoursworked']),
        'worklocation': item['worklocation'],
        'labormetrics': item['labormetrics'],
        'week': week
    }

def get_final_data_per_week_data_45(item, week):
    return {
        'employeenumber': item['employeeno'],
        'replace': item['replace'],
        'code': item['taskcode'],
        'hrs': float(result('exhaustive_normalization_per_project')),
        'worklocation': item['worklocation'],
        'labormetrics': item['labormetrics'],
        'week': week
    }


def get_if_normalizationrequired():
    get_data_related_to_weeks = load_all_records(result("get_data_related_to_weeks"))
    return bool(find_first_by_attr_and_get_attr(get_data_related_to_weeks, 'normalizationrequired', 'Yes', 'normalizationrequired'))


def exhaustive_normalization():
    totalhoursinweek_33 = load_all_records(result(
        "total_data_for_distinct_week_with_normalization"))[0]['totalhoursinweek']
    totalhoursinweek_29 = load_all_records(
        result("get_total_hours_for_distinct_weeks"))[0]['totalhoursinweek']
    return (float(totalhoursinweek_33) - (float(totalhoursinweek_29) - 40)) if (float(totalhoursinweek_33) - (float(totalhoursinweek_29) - 40)) > 0 else 0


def exhaustive_normalization_per_project():
    totalhoursinweek_33 = load_all_records(result(
        "total_data_for_distinct_week_with_normalization"))[0]['totalhoursinweek']
    totalhoursinweek_29 = load_all_records(
        result("get_total_hours_for_distinct_weeks"))[0]['totalhoursinweek']
    exhaustive_normalization = result('exhaustive_normalization')
    all_records = len(load_all_records(
        result("final_data_related_to_distinct_week_with_normalization")))
    return (float(exhaustive_normalization)/all_records) if ((float(totalhoursinweek_33) - (float(totalhoursinweek_29) - 40))/all_records) > 0 else 0


def if_totalhoursinweek_is_greater_than_40():
    totalhoursinweek_42 = load_all_records(result(
        "total_data_for_distinct_week_without_normalization"))[0]['totalhoursinweek']
    totalhoursinweek_34 = load_all_records(result(
        "total_data_for_distinct_week_with_excluded_timetypes"))[0]['totalhoursinweek']
    exhaustive_normalization = result('exhaustive_normalization')
    return bool((float(totalhoursinweek_42) + float(totalhoursinweek_34) + float(exhaustive_normalization)) > 40)


def normal_normalization():
    totalhoursinweek_42 = load_all_records(result(
        "total_data_for_distinct_week_without_normalization"))[0]['totalhoursinweek']
    totalhoursinweek_34 = load_all_records(result(
        "total_data_for_distinct_week_with_excluded_timetypes"))[0]['totalhoursinweek']
    exhaustive_normalization = result('exhaustive_normalization')
    return (float(totalhoursinweek_42) + float(exhaustive_normalization)) - (40 - float(totalhoursinweek_34))


def normal_normalization_per_project():
    totalhoursinweek_42 = load_all_records(result(
        "total_data_for_distinct_week_without_normalization"))[0]['totalhoursinweek']
    totalhoursinweek_34 = load_all_records(result(
        "total_data_for_distinct_week_with_excluded_timetypes"))[0]['totalhoursinweek']
    exhaustive_normalization = result('exhaustive_normalization')
    return (40 - float(totalhoursinweek_34)) / (float(totalhoursinweek_42) + float(exhaustive_normalization))


def add_final_data_per_week_lookup_table_51(item, week):
    def get_hrs():
        normal_normalization_per_project = result(
            'normal_normalization_per_project')
        total_hrs = 0
        all_records = load_all_records(
            result("data_related_to_distinct_week_without_normalization"))
        for record in all_records:
            hoursworked = record['hoursworked'] if record['hoursworked'] else 0
            total_hrs += float(hoursworked) if record['taskcode'] == item['taskcode'] and record['replace'] == item[
                'replace'] and record['worklocation'] == item['worklocation'] and record['labormetrics'] == item[
                'labormetrics'] and record['employeeno'] == item['employeeno'] else 0
        return total_hrs * float(normal_normalization_per_project)
    return {
        'employeenumber': item['employeeno'],
        'replace': item['replace'],
        'code': item['taskcode'],
        'hrs': get_hrs(),
        'worklocation': item['worklocation'],
        'labormetrics': item['labormetrics'],
        'week': week
    }


def add_final_data_per_week_lookup_table_55(item, week):
    def get_hrs():
        total_hrs = 0
        all_records = load_all_records(
            result("data_related_to_distinct_week_without_normalization"))
        for record in all_records:
            hoursworked = record['hoursworked'] if record['hoursworked'] else 0
            total_hrs += float(hoursworked) if record['taskcode'] == item['taskcode'] and record['replace'] == item[
                'replace'] and record['worklocation'] == item['worklocation'] and record['labormetrics'] == item[
                'labormetrics'] and record['employeeno'] == item['employeeno'] else 0
        return total_hrs
    return {
        'employeenumber': item['employeeno'],
        'replace': item['replace'],
        'code': item['taskcode'],
        'hrs': get_hrs(),
        'worklocation': item['worklocation'],
        'labormetrics': item['labormetrics'],
        'week': week
    }


def normal_normalization_61():
    totalhoursinweek_29 = load_all_records(
        result("get_total_hours_for_distinct_weeks"))[0]['totalhoursinweek']
    return float(totalhoursinweek_29) - 40


def normal_normalization_per_project_63():
    totalhoursinweek_57 = load_all_records(result(
        "total_data_for_distinct_week_with_excluded_timetypes_else"))[0]['totalhoursinweek']
    totalhoursinweek_29 = load_all_records(
        result("get_total_hours_for_distinct_weeks"))[0]['totalhoursinweek']
    return (40 - float(totalhoursinweek_57)) / (float(totalhoursinweek_29) - float(totalhoursinweek_57))


def add_final_data_per_week_lookup_table_65(item, week):
    def get_hrs():
        normal_normalization_per_project_63 = result(
            'normal_normalization_per_project_63')
        total_hrs = 0
        all_records = load_all_records(result("get_data_related_to_weeks"))
        for record in all_records:
            hoursworked = record['hoursworked'] if record['hoursworked'] else 0
            total_hrs += float(hoursworked) if record['taskcode'] == item['taskcode'] and record['replace'] == item[
                'replace'] and record['worklocation'] == item['worklocation'] and record['labormetrics'] == item[
                'labormetrics'] and record['employeeno'] == item['employeeno'] else 0
        return total_hrs * float(normal_normalization_per_project_63)
    return {
        'employeenumber': item['employeeno'],
        'replace': item['replace'],
        'code': item['taskcode'],
        'hrs': get_hrs(),
        'worklocation': item['worklocation'],
        'labormetrics': item['labormetrics'],
        'week': week
    }


def get_final_data_to_export(item):
    def get_hrs():
        final_data_lookup_table = load_all_records(
            result('create_final_data_per_week_lookup_table'))
        total_hrs = 0
        for entry in final_data_lookup_table:
            total_hrs += float(entry['properties']['hrs']) if entry['properties']['code'] == item['taskcode'] and entry['properties'][
                'replace'] == item['replace'] and entry['properties']['worklocation'] == item['worklocation'] and entry['properties'][
                'labormetrics'] == item['labormetrics'] and entry['properties']['employeenumber'] == item['employeeno'] else 0
        return total_hrs
    return {
        'employeenumber': item['employeeno'],
        'replace': item['replace'],
        'code': item['taskcode'],
        'hrs': get_hrs(),
        'worklocation': item['worklocation'],
        'labormetrics': item['labormetrics']
    }
