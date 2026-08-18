from datetime import datetime
from dateutil.relativedelta import relativedelta
import pendulum
from rail import get_current_context
import rail
null = None

TWB_CREATION_DATE_FORMAT = '%d %B %Y %I:%M:%S %p'

def get_dag_run_conf():
    return get_current_context()['dag_run'].conf

def page_handler(request, result):
    if len(result['rows']) > 0:
        request['page'] += 1
        return request
    return null

def get_attr_value(dataset, dataset_key, target, value):
    return rail.find_first_by_attr_and_get_attr(dataset, dataset_key, target, value)

def filter_data_for_c1_reg_time_export(config):
    time_data_formats = rail.result("get_time_download_script")
    current_datetime = pendulum.now(config.utc_timezone)
    prev_90_days = current_datetime - relativedelta(days=90)
    prev_14_days = current_datetime - relativedelta(days=14)
    next_30_days = current_datetime + relativedelta(days=30)
    return {
        "fileformaturi": rail.find_first_by_attr_and_get_attr(time_data_formats, 'displayText', config.c1_reg_time_export_file_format, 'uri'),
        "contractoruri": rail.result("get_employeetype_groups")["contractor_uri"],
        "agencycontractoruri": rail.result("get_employeetype_groups")["agency_contractor_uri"],
        "companycodelist": list(set(map(lambda company_codes_data: company_codes_data["uri"], rail.result("get_all_c1_divisions")))),
        "processingstartdateday": prev_90_days.day,
        "processingstartdatemonth": prev_90_days.month,
        "processingstartdateyear": prev_90_days.year,
        "processingenddateday": next_30_days.day,
        "processingenddatemonth": next_30_days.month,
        "processingenddateyear": next_30_days.year,
        "oeffilter": get_attr_value(rail.result("get_all_filter_definitions"), "name", "Project Type", "uri"),
        "oeffilteroption": get_attr_value(rail.result("get_oef_drop_down_values")["tags"], "name", "ES", "uri"),
        "oeffilteroption1": get_attr_value(rail.result("get_oef_drop_down_values")["tags"], "name", "IC", "uri"),
        "ackdateday": prev_14_days.day,
        "ackdatemonth": prev_14_days.month,
        "ackdateyear": prev_14_days.year
    }

def filter_data_for_c1_iwo_time_export(config):
    time_data_formats = rail.result("get_time_download_script")
    current_datetime = pendulum.now(config.utc_timezone)
    prev_90_days = current_datetime - relativedelta(days=90)
    prev_14_days = current_datetime - relativedelta(days=14)
    next_30_days = current_datetime + relativedelta(days=30)
    return {
        "fileformaturi": rail.find_first_by_attr_and_get_attr(time_data_formats, 'displayText', config.c1_iwo_time_export_file_format, 'uri'),
        "contractoruri": rail.result("get_employeetype_groups")["contractor_uri"],
        "agencycontractoruri": rail.result("get_employeetype_groups")["agency_contractor_uri"],
        "companycodelist": list(set(map(lambda company_codes_data: company_codes_data["uri"], rail.result("get_all_gsap_compass_divisions")))),
        "processingstartdateday": prev_90_days.day,
        "processingstartdatemonth": prev_90_days.month,
        "processingstartdateyear": prev_90_days.year,
        "processingenddateday": next_30_days.day,
        "processingenddatemonth": next_30_days.month,
        "processingenddateyear": next_30_days.year,
        "oeffilter": get_attr_value(rail.result("get_all_filter_definitions"), "name", "IWO Indicator", "uri"),
        "oeffilteroptioncp": get_attr_value(rail.result("get_oef_drop_down_values")["tags"], "name", "X", "uri"),
        "oeffilteroptionc1": get_attr_value(rail.result("get_oef_drop_down_values")["tags"], "name", "C1", "uri"),
        "ackdateday": prev_14_days.day,
        "ackdatemonth": prev_14_days.month,
        "ackdateyear": prev_14_days.year
    }

def filter_data_for_c1_reg_pta_weekly_time_export(config):
    time_data_formats = rail.result("get_time_download_script")
    current_datetime = pendulum.now(config.utc_timezone)
    prev_365_days = current_datetime - relativedelta(days=365)
    prev_90_days = current_datetime - relativedelta(days=90)
    prev_60_days = current_datetime - relativedelta(days=60)
    return {
        "fileformaturi": rail.find_first_by_attr_and_get_attr(time_data_formats, 'displayText', config.c1_reg_time_export_file_format, 'uri'),
        "contractoruri": rail.result("get_employeetype_groups")["contractor_uri"],
        "agencycontractoruri": rail.result("get_employeetype_groups")["agency_contractor_uri"],
        "companycodelist": list(set(map(lambda company_codes_data: company_codes_data["uri"], rail.result("get_all_c1_divisions")))),
        "processingstartdateday": prev_365_days.day,
        "processingstartdatemonth": prev_365_days.month,
        "processingstartdateyear": prev_365_days.year,
        "processingenddateday": prev_90_days.day,
        "processingenddatemonth": prev_90_days.month,
        "processingenddateyear": prev_90_days.year,
        "oeffilter": get_attr_value(rail.result("get_all_filter_definitions"), "name", "Project Type", "uri"),
        "oeffilteroption": get_attr_value(rail.result("get_oef_drop_down_values")["tags"], "name", "ES", "uri"),
        "oeffilteroption1": get_attr_value(rail.result("get_oef_drop_down_values")["tags"], "name", "IC", "uri"),
        "ackdateday": prev_60_days.day,
        "ackdatemonth": prev_60_days.month,
        "ackdateyear": prev_60_days.year
    }

def filter_data_for_c1_iwo_pta_weekly_time_export(config):
    time_data_formats = rail.result("get_time_download_script")
    current_datetime = pendulum.now(config.utc_timezone)
    prev_365_days = current_datetime - relativedelta(days=365)
    prev_90_days = current_datetime - relativedelta(days=90)
    prev_14_days = current_datetime - relativedelta(days=14)
    return {
        "fileformaturi": rail.find_first_by_attr_and_get_attr(time_data_formats, 'displayText', config.c1_iwo_time_export_file_format, 'uri'),
        "contractoruri": rail.result("get_employeetype_groups")["contractor_uri"],
        "agencycontractoruri": rail.result("get_employeetype_groups")["agency_contractor_uri"],
        "companycodelist": list(set(map(lambda company_codes_data: company_codes_data["uri"], rail.result("get_all_gsap_compass_divisions")))),
        "processingstartdateday": prev_365_days.day,
        "processingstartdatemonth": prev_365_days.month,
        "processingstartdateyear": prev_365_days.year,
        "processingenddateday": prev_90_days.day,
        "processingenddatemonth": prev_90_days.month,
        "processingenddateyear": prev_90_days.year,
        "oeffilter": get_attr_value(rail.result("get_all_filter_definitions"), "name", "IWO Indicator", "uri"),
        "oeffilteroptioncp": get_attr_value(rail.result("get_oef_drop_down_values")["tags"], "name", "X", "uri"),
        "oeffilteroptionc1": get_attr_value(rail.result("get_oef_drop_down_values")["tags"], "name", "C1", "uri"),
        "ackdateday": prev_14_days.day,
        "ackdatemonth": prev_14_days.month,
        "ackdateyear": prev_14_days.year
    }

def get_current_export_name(prefix):
    previous_export_name = int(rail.result("get_data_for_all_past_time_exports_for_C1")[0][
        "timeexport"].split('-')[-1])+1
    # pylint: disable=consider-using-f-string
    return prefix+"{:09d}".format(previous_export_name)

def check_ack_date_and_name(dag_run):
    current_creationdatetime = datetime.strptime(rail.result("for_each_time_export")['creationdate'], TWB_CREATION_DATE_FORMAT)
    creationdatetime = datetime.strptime(rail.result("log_twb_creation_time"), TWB_CREATION_DATE_FORMAT)
    return rail.result("for_each_time_export")['twbname'] != dag_run.conf['twbname'] and current_creationdatetime < creationdatetime

def get_all_twb_without_acknowledge():
    return rail.result('get_twb_without_acknowledge_data_var')['value']

def get_hours_for_c1(item):
    hours = float(item["hours"])

    if hours == 0:
        return "0"

    if item["beeperpay"]:
        return item["beeperpay"]

    if item["oncallstandby"]:
        if item["oncallstandby"] == "On Call":
            return "1"
        elif item["oncallstandby"] == "Stand By":
            if 0 < hours <= 8:
                return "1"
            elif 8 < hours <= 16:
                return "2"
            elif 16 < hours <= 24:
                return "3"
            else:
                return round(hours, 2)
        else:
            return round(hours, 2)

    elif item["oncallstandby2"]:
        if 0 < hours <= 8:
            return "1"
        elif 8 < hours <= 16:
            return "2"
        elif 16 < hours <= 24:
            return "3"
        else:
            return round(hours, 2)

    else:
        return round(hours, 2)

def get_write_final_regular_data_for_processing_csv(item):
    return {
        "employeeid": (
            (item['perner'] if item['iwoindicator'] == "X" else (
                item['actualempid'] if item['actualempid'] else item['employeeid'])) if item['iwoindicator'] else (
            item['iapernerid'] if item['internationalassignee'] == '1' else (
                item['actualempid'] if item['actualempid'] else item['employeeid']))),
        "date": item['entrydate'],
        "tasktype": "30" if item['approvalstatus'] == "Approved" else "20",
        "costcenter": "C101099951" if item['iwoindicator'] == "X" else item['costcentercode'],
        "activitytype": "HZD" if item['iwoindicator'] == "X" else item['jobactivitytype'],
        "recwbselement": null if item['wbstype'] == 'Opportunity' else (null if item['parentserviceorder'] else
            item['parentwbs']) if item['iwoindicator'] == "X" else (item['projectname'] if item['masterwbs'] == 'WBS' else null),
        "recorder": null if item['wbstype'] == 'Opportunity' else (item['parentserviceorder'] if item['parentserviceorder'] else
            null) if item['iwoindicator'] == "X" else (item['projectname'] if item['masterwbs'] == 'SO' else null),
        "labortype": item['labortype'].split("|")[0] if item['labortype'] else null,
        "billableindicator": (
            ("X" if "|Billable" in item['labortype'] else null) if item['labortype'] else null),
        "task": null if item['tasktype'] == 'Opportunity' else (
            (null if item['taskname'] == item['projectname'] else item['taskname']) if item['taskname'] else null),
        "hours": get_hours_for_c1(item),
        "attendencetype": '1399' if item['beeperpay'] else ('1010' if item['iwoindicator'] == "X" else
            ('1309' if item['oncallstandby2'] else
            (item['timetype'][0:4] if item['timetype'] else
            (item['timetype2'][0:4] if item['timetype2'] else
            (item['attendancetypecode'] if item['attendancetypecode'] else
            (item['timeofftypedescription'] if item['timeoffbookingid'] else '1010')))))),
        "comments": item['comments'],
        "entryid": (item['timeentryid'] if item['timeentryid2'] else
            str(item['entrydate']) + str(item['timeoffbookingid'])),
        "activitynumber": null,
        "sendingorder": null,
        "sendpoitem": null,
        "deletedentry": "yes" if float(item["hours"]) == 0 else "no",
        "oncallstandby2": item["oncallstandby2"],
        "opp_id": item['taskname'] if item['tasktype'] == 'Opportunity' else null
    }.values()

def get_write_final_iwo_data_for_processing_csv(item):
    return {
        "employeeid": (
            (item["perner"] if item["perner"] else (
                item["actualempid"] if item["actualempid"] else item["employeeid"])) if item["employeetypecode"] == "Contractor" else (
            (item["perner"] if item["iwoindicator"] and item["iwoindicator"] == "X" else item["employeeid"]) if item["companycodecode"] == "COMPASS" else (
            item["actualempid"] if item["actualempid"] else (
                item["iapernerid"] if item["internationalassignee"] == "1" else item["employeeid"])))),
        "date": item['entrydate'],
        "tasktype": "30" if item['approvalstatus'] == "Approved" else "20",
        "costcenter": "C101099951" if item['iwoindicator'] == "X" or item['iwoindicator'] == "C1" else item['costcentercode'],
        "activitytype": "HZD" if item['iwoindicator'] == "X" else item['jobactivitytype'],
        "recwbselement": null if item["wbstype"] == "Opportunity" else (
            (null if item["parentserviceorder"] else item["parentwbs"]) if item["iwoindicator"] == "X" else (
            (null if item["parentserviceorder"] else item["parentwbs"]) if item["iwoindicator"] == "C1" else (
            item["projectname"] if item["masterwbs"] == "WBS" else null))),
        "recorder": null if item["wbstype"] == "Opportunity" else (
            (item["parentserviceorder"] if item["parentserviceorder"] else null) if item["iwoindicator"] == "X" else (
            (item["parentserviceorder"] if item["parentserviceorder"] else null) if item["iwoindicator"] == "C1" else (
            item["projectname"] if item["masterwbs"] == "SO" else null))),
        "labortype": null if item['projecttype'] == "ES" else (item['labortype'].split("|")[0] if item['labortype'] else null),
        "billableindicator": null if item['projecttype'] == "ES" else (
            ("X" if "|Billable" in item['labortype'] else null) if item['labortype'] else null),
        "task": null if item["tasktype"] == "GSAP Billing Key" else (
            null if item["tasktype"] == "Opportunity" else (
            null if item["projecttype"] == "IC" else (
            (null if item["taskname"] == item["projectname"] else item["taskname"]) if item["taskname"] else null))),
        "hours": get_hours_for_c1(item),
        "attendencetype": (
            "1399" if item["beeperpay"] else (
            "1010" if item["iwoindicator"] == "X" else (
            "1010" if item["iwoindicator"] == "C1" else (
            "1309" if item["oncallstandby2"] else (
            item["timetype"][:4] if item["timetype"] else (
            item["timetype2"][:4] if item["timetype2"] else (
            item["attendancetypecode"] if item["attendancetypecode"] else (
            item["timeofftypedescription"] if item["timeoffbookingid"] else "1010")))))))),
        "comments": item['comments'],
        "entryid": (item["timeentryid"] if item["timeentryid2"] else
            str(item["entrydate"]) + str(item["timeoffbookingid"])),
        "activitynumber": null,
        "sendingorder": null,
        "sendpoitem": null,
        "deletedentry": "yes" if float(item["hours"]) == 0 else "no",
        "oncallstandby2": item["oncallstandby2"],
        "opp_id": item['taskname'] if item['tasktype'] == 'Opportunity' else null
    }.values()

def get_c1_start_line_data():
    return [
        {
            'employeeid': '',
            'date': '',
            'tasktype': '',
            'costcenter': '',
            'activitytype': '',
            'recwbselement': '',
            'recorder': '',
            'labortype': '',
            'billableindicator': '',
            'task': '',
            'hours': '',
            'attendencetype': '',
            'comments': '',
            'entryid': get_dag_run_conf()['payload_identifier_replicon_uniqueid'],
            'activitynumber': '',
            'sendingorder': '',
            'sendpoitem': '',
            'deletedentry': '',
            'oncallstandby2': '',
            'opp_id': ''
        }
    ]

def add_starting_line_to_final_data():
    final_time_data = rail.load_all_records(rail.result("final_data_for_processing")) if rail.result("final_data_for_processing") else []
    return [*get_c1_start_line_data(), *final_time_data]

def get_oef_bindings_uri(name):
    return rail.find_first_by_attr_and_get_attr(rail.result('get_all_object_extension_field_bindings'),
        "displayText", name, "uri")
