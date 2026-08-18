from datetime import datetime
from hashlib import md5
import rail
null = None


def get_filter_data(param_str, uri):
    param_list = []
    if ',' in param_str:
        param_list = param_str.split(",")
        return list(map(lambda i: {
            "reportFilterUri": uri,
            "value": i
        }, param_list))
    return [{
        "reportFilterUri": uri,
        "value": param_str
    }]


def get_report_params(dag_run):
    report_filter = []
    enabled_filters = rail.result("get_project_hours_report_details")[
        "filterConfiguration"]["enabledFilters"]
    user_filter_uri = rail.find_first_by_attr_and_get_attr(
        enabled_filters, "displayText", "UserFilter", "uri")
    entry_date_uri = rail.find_first_by_attr_and_get_attr(
        enabled_filters, "displayText", "EntryDateFilter", "uri")
    trade_group_uri = rail.find_first_by_attr_and_get_attr(
        enabled_filters, "displayText", "CostCenterFilter", "uri")
    employee_role_uri = rail.find_first_by_attr_and_get_attr(
        enabled_filters, "displayText", "DivisionFilter", "uri")
    date_filter = [{
        "reportFilterUri": entry_date_uri,
        "value": null,
    },
        {
        "reportFilterUri": entry_date_uri,
        "value": str(dag_run.conf['webhook']['data']["daterange"].split("-")[0][:2]) + "/" +
        str(dag_run.conf['webhook']['data']["daterange"].split("-")[0][2:4]) + "/" +
        str(dag_run.conf['webhook']['data']["daterange"].split("-")[0][4:])
    },
        {
        "reportFilterUri": entry_date_uri,
        "value": str(dag_run.conf['webhook']['data']["daterange"].split("-")[1][:2]) + "/" +
        str(dag_run.conf['webhook']['data']["daterange"].split("-")[1][2:4]) + "/" +
        str(dag_run.conf['webhook']['data']["daterange"].split("-")[1][4:])
    }]
    report_filter.extend(date_filter)
    if dag_run.conf['webhook']['data']["username"]:
        report_filter.extend(get_filter_data(
            dag_run.conf['webhook']['data']["username"], user_filter_uri))
    if dag_run.conf['webhook']['data']["tradegroup"]:
        report_filter.extend(get_filter_data(
            dag_run.conf['webhook']['data']["tradegroup"], trade_group_uri))
    if dag_run.conf['webhook']['data']["employeerole"]:
        report_filter.extend(get_filter_data(
            dag_run.conf['webhook']['data']["employeerole"], employee_role_uri))

    return {
        "reportParameters": [
            {
                "reportUri": rail.result("get_project_hours_report_details")["uri"],
                "filterValues": report_filter,
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }


def get_validated_row(item, dag_run):
    supervisor_list = []
    supervisor_check = "Yes"
    if dag_run.conf['webhook']['data']["supervisor"]:
        supervisor_list = list(map(
            lambda i: {"user": i}, dag_run.conf['webhook']['data']["supervisor"].split(',')))
    if supervisor_list:
        if item["supervisoruri"] and item["supervisoruri"].split(":")[-1] in supervisor_list:
            supervisor_check = "Yes"
        else:
            supervisor_check = "No"

    project_list = []
    project_check = "Yes"
    if dag_run.conf['webhook']['data']["projectname"]:
        project_list = list(map(lambda i: {
                            "projectname": i}, dag_run.conf['webhook']['data']["projectname"].split(',')))
    if project_list:
        if item["projecturi"] and item["projecturi"].split(":")[-1] in project_list:
            project_check = "Yes"
        else:
            project_check = "No"

    merge_id = "_".join([item["entrydate"], item["employeeid"],
                         item["projectname"], item["projectcode"], item["taskcode"]]).encode()
    merge_id = md5(merge_id).hexdigest()
    unique_id = "_".join(
        [item["entrydate"], item["username"], item["employeeid"]]).encode()
    unique_id = md5(unique_id).hexdigest()

    return [
        item["entrydate"] or null, item["username"] or null, item["VDCOverrideShift"] or null, item["OverrideShift"] or null,
        item["supervisorname"] or null, item["tradename"] or null, item["taskname"] or null, item["projecthours"] or null,
        item["employeeid"] or null, item["projectname"] or null, item["projectcode"] or null, item["taskcode"] or null,
        item["approvalstatus"] or null, item["supervisoruri"] or null, supervisor_check,
        item["projecturi"] or null, project_check, merge_id, unique_id
    ]

def get_pay_code_row(item):
    item["entrydate"] = datetime.strftime(
        datetime.strptime(item["entrydate"], "%d/%m/%Y"), "%b %d, %Y")
    return [
                item["entrydate"], item["username"], item["tradename"], item["taskname"],
                item["employeeid"], item["projectname"], item["projectcode"], item["taskcode"],
                item["paycodename"], item["paycodecode"], item["paycodehrs"],
                md5(("_".join([item["entrydate"], item["employeeid"],
                               item["projectname"], item["projectcode"], item["taskcode"]])).encode()).hexdigest()
            ]

def get_final_export_data(item):
    item["entrydate1"] = datetime.strftime(
        datetime.strptime(item["entrydate1"], "%b %d, %Y"), "%Y-%m-%d")
    return [
                item["entrydate1"], item["username1"], item["VDCOverrideShift1"], item["OverrideShift1"],
                item["usersupervisornamecurrent"], item["tradegroupcurrentuserudf"], item["taskname1"],
                item["paycodecode1"], item["paycodename1"], item["paycodehrs1"],
                item["eeid"], item["projectname1"], item["projectcode1"], item["taskcode1"], item["approvalstatus"]
            ]
