from datetime import timedelta
import datetime
import json
import rail
from dxctechnology.timesheet_autosubmission.location_map import LOCATION_MAP

null = None


def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf


def get_enabled_divisions_company_codes_payload():
    return {
        "page": "1",
        "pagesize": "100000000",
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


def companycode_from_mapper(source, country, employeetype=None):
    if employeetype:
        list_of_codes = list(filter(lambda item: item['source'] == source and item['country']
                             == country and item['employeetype'] == employeetype, LOCATION_MAP))
    else:
        list_of_codes = list(filter(
            lambda item: item['source'] == source and item['country'] == country, LOCATION_MAP))

    return list(map(lambda x: x['companycode'], list_of_codes))

# pylint: disable=consider-using-f-string


def get_report_filter_uris(division_result, get_mappped_companycodes, config, get_report_data, employee_type_data=None):
    data = rail.result(get_report_data)[
        'filterConfiguration']['enabledFilters']
    report_uri = rail.result(get_report_data)['uri']
    period_filter_uri = [x['uri'] for x in data if x['displayText']
                         == config.report_filter_timesheetperiod]
    approval_filter_uri = [
        x['uri'] for x in data if x['displayText'] == config.report_filter_approvalstatus]
    division_filter_uri = [
        x['uri'] for x in data if x['displayText'] == config.report_filter_currentdivision]
    employeetype_filter_uri = [
        x['uri'] for x in data if x['displayText'] == config.report_filter_employeetype]

    filters = [
        {
            "reportFilterUri": period_filter_uri[0],
            "value": null
        },
        {
            "reportFilterUri": period_filter_uri[0],
            "value": f'{(datetime.datetime.utcnow()-timedelta(days=7)).strftime("%m/%d/%Y")}'},
        {
            "reportFilterUri": period_filter_uri[0],
            "value": f'{(datetime.datetime.utcnow()-timedelta(days=0)).strftime("%m/%d/%Y")}'},
        {
            "reportFilterUri": approval_filter_uri[0],
            "value": config.timesheet_status_value}
    ]

    if config.employee_type:
        payload = rail.result(employee_type_data)
        filter_emp_data = list(
            filter(lambda x: x['name'] == config.employee_type, payload))
        employeetype_uri = list(
            map(lambda item: item['uri'].split(':')[-1], filter_emp_data))
        emp_filter = {
            "reportFilterUri": employeetype_filter_uri[0],
            "value": employeetype_uri[0]
        }
        filters.append(emp_filter)

    divsion_data = rail.result(division_result)
    mapped_codes = rail.result(get_mappped_companycodes)
    divsion_names = list(
        filter(lambda x: x['name'] in mapped_codes, divsion_data))
    division_filter = list(map(lambda item: {
        "reportFilterUri": division_filter_uri[0],
        "value": item['uri'].split(':')[-1]
    }, divsion_names))
    final_filters = filters + division_filter

    report_input = {"reportParameters": [
                    {
                        "reportUri": report_uri,
                        "filterValues": final_filters,
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }]}
    return json.dumps(report_input)
