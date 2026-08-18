from datetime import datetime, timedelta
from functools import lru_cache
import dateutil
import rail

null = None


def get_all_psa_org_unit():
    return {
        "page": "1",
        "pagesize": "1000000",
        "columnUris": [
            "urn:replicon:department-group-list-column:department-group",
            "urn:replicon:department-group-list-column:full-path"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
            "leftExpression": null,
            "operatorUri": null,
            "rightExpression": null,
            "value": null,
            "filterDefinitionUri": "urn:replicon:department-group-list-filter:effectively-enabled"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
            "leftExpression": null,
            "operatorUri": null,
            "rightExpression": null,
            "value": {
                "uri": null,
                "uris": [],
                "bool": "true",
                "date": null,
                "money": null,
                "number": null,
                "text": null,
                "time": null,
                "calendarDayDurationValue": null,
                "workdayDurationValue": null,
                "dateRange": null,
                "dateTimeUtc": null,
                "dateTimeUtcRange": null,
                "numberRange": null
            },
            "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
        }


def get_division_uris():
    return {
        "page": "1",
        "pagesize": "1000000",
        "columnUris": [
            "urn:replicon:division-list-column:division",
            "urn:replicon:division-list-column:full-path"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:division-list-filter:effectively-enabled"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": null,
                    "uris": [],
                    "bool": True,
                    "date": null,
                    "money": null,
                    "number": null,
                    "text": null,
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


def get_filter_values():
    filterconfiguration = rail.result('get_report_details')['filterConfiguration']['enabledFilters']
    report_filter_uri_department_grp = rail.find_first_by_attr_and_get_attr(filterconfiguration, 'displayText', 'CurrentDepartmentGroupFilter', 'uri')
    replicon_psa_department_grp_list = rail.result('get_all_psa_org_unit')
    department_filter_values = list(map(lambda item: {
        "reportFilterUri": report_filter_uri_department_grp,
        "value": item['uri'].split(':')[-1],
    }, replicon_psa_department_grp_list))

    report_filter_uri_division = rail.find_first_by_attr_and_get_attr(filterconfiguration, 'displayText', 'CurrentDivisionFilter', 'uri')
    data_divison = rail.result('get_division_uris')
    division_filter_values = list(map(lambda item: {
        "reportFilterUri": report_filter_uri_division,
        "value": item['uri'].split(':')[-1],
    }, data_divison))

    report_filter_uri_date_range = rail.find_first_by_attr_and_get_attr(filterconfiguration, 'displayText', 'DateRangeFilter', 'uri')
    date_range = [
        {
            "reportFilterUri": report_filter_uri_date_range,
            "value": null
        },
        {
            "reportFilterUri": report_filter_uri_date_range,
            "value": (datetime.strptime(rail.result('get_export_data')['report_start_date'], "%m/%d/%Y") + timedelta(days=1)).strftime('%m/%d/%Y')
        },
        {
            "reportFilterUri": report_filter_uri_date_range,
            "value": null
        },
    ]

    return department_filter_values + division_filter_values + date_range


def get_filter_params():
    return {
        "reportParameters": [
            {
                "reportUri": rail.result('get_report_details')['uri'],
                "filterValues": get_filter_values(),
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }


def get_all_paycodes():
    data = list(map(lambda item: item['TimeOffTypeUri'], rail.load_all_records(
        rail.result('query_unique_timeoff_uris'))))
    return{
        "timeOffTypeUris": data
    }


def translate_row(row):
    data = get_paycodes()
    paycode_code_data = get_paycode_code()

    @lru_cache(maxsize=32)
    def get_paycode_code_value(timeofftypeuri):
        paycode_uri = rail.find_first_by_attr_and_get_attr(data, 'uri', timeofftypeuri, 'paycodeuri')
        if paycode_uri:
            paycode_code = rail.find_first_by_attr_and_get_attr(paycode_code_data, 'paycodeuri', paycode_uri, 'code')
            if paycode_code:
                return paycode_code
        return ''

    @lru_cache(maxsize=32)
    def leave_date(leavedate):
        leave_date_format = str(dateutil.parser.parse(
            leavedate)).split(' ', maxsplit=1)[0]
        return (((datetime.strptime(leave_date_format, '%Y-%m-%d')).date()).strftime('%Y%m%d'))

    return {
            'employeenumber': row['EmployeeNumber'],
            'absencetype': get_paycode_code_value(row['TimeOffTypeUri']) if row['HomeERP']=='C1' else (
                row['TimeOffTypeDescription'] if row['HomeERP']=='COMPASS' else ''),
            'leavedate': leave_date(row['LeaveDate']),
            'leavehours': float(row['LeaveHours']),
        }.values()

@lru_cache(maxsize=32)
def get_paycode_code():
    return rail.result('get_paycode_codes')

@lru_cache(maxsize=32)
def get_paycodes():
    return rail.result('get_all_paycodes')
