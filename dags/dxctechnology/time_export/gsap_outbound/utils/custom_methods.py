from datetime import datetime
from dateutil.relativedelta import relativedelta
import pendulum
import rail
null = None

TWB_CREATION_DATE_FORMAT = '%d %B %Y %I:%M:%S %p'

def get_conf():
    return rail.get_current_context()['dag_run'].conf

def page_handler(request, result):
    if len(result['rows']) > 0:
        request['page'] += 1
        return request
    return null

def get_attr_value(dataset, dataset_key, target, value):
    return rail.find_first_by_attr_and_get_attr(dataset, dataset_key, target, value)

def get_all_object_extension_field_bindings():
    return rail.result('get_all_object_extension_field_bindings')

def get_oef_bindings_uri(name):
    return rail.find_first_by_attr_and_get_attr(get_all_object_extension_field_bindings(),
        "displayText", name, "uri")

def filter_data_for_gsap_reg_time_export(config):
    time_data_formats = rail.result("get_time_download_script")
    current_datetime = pendulum.now(config.utc_timezone)
    prev_90_days = current_datetime - relativedelta(days=90)
    prev_14_days = current_datetime - relativedelta(days=14)
    next_30_days = current_datetime + relativedelta(days=30)
    return {
        "fileformaturi": rail.find_first_by_attr_and_get_attr(time_data_formats, 'displayText', config.gsap_c1_cp_time_export_file_format, 'uri'),
        "contractoruri": rail.result("get_employeetype_groups")["contractor_uri"],
        "agencycontractoruri": rail.result("get_employeetype_groups")["agency_contractor_uri"],
        "companycodelist": list(set(map(lambda company_codes_data: company_codes_data["uri"], rail.result("get_all_gsap_divisions")))),
        "processingstartdateday": str(prev_90_days.day),
        "processingstartdatemonth": str(prev_90_days.month),
        "processingstartdateyear": str(prev_90_days.year),
        "processingenddateday": str(next_30_days.day),
        "processingenddatemonth": str(next_30_days.month),
        "processingenddateyear": str(next_30_days.year),
        "oeffilter": get_attr_value(rail.result("get_all_filter_definitions"), "name", "Project Type", "uri"),
        "oeffilter1": get_attr_value(rail.result("get_all_filter_definitions"), "name", "IWO Indicator", "uri"),
        "oeffilteroption": get_attr_value(rail.result("get_oef_drop_down_values_project_type")["tags"], "name", "CP", "uri"),
        "oeffilteroption1": get_attr_value(rail.result("get_oef_drop_down_values_iwo_indicator")["tags"], "name", "C1", "uri"),
        "ackdateday": str(prev_14_days.day),
        "ackdatemonth": str(prev_14_days.month),
        "ackdateyear": str(prev_14_days.year)
    }

def filter_data_for_gsap_iwo_time_export(config):
    time_data_formats = rail.result("get_time_download_script")
    current_datetime = pendulum.now(config.utc_timezone)
    prev_90_days = current_datetime - relativedelta(days=90)
    prev_14_days = current_datetime - relativedelta(days=14)
    next_30_days = current_datetime + relativedelta(days=30)
    return {
        "fileformaturi": rail.find_first_by_attr_and_get_attr(time_data_formats, 'displayText', config.gsap_c1_cp_time_export_file_format, 'uri'),
        "contractoruri": rail.result("get_employeetype_groups")["contractor_uri"],
        "agencycontractoruri": rail.result("get_employeetype_groups")["agency_contractor_uri"],
        "companycodelist": list(set(map(lambda company_codes_data: company_codes_data["uri"], rail.result("get_c1_compass_divisions")))),
        "processingstartdateday": str(prev_90_days.day),
        "processingstartdatemonth": str(prev_90_days.month),
        "processingstartdateyear": str(prev_90_days.year),
        "processingenddateday": str(next_30_days.day),
        "processingenddatemonth": str(next_30_days.month),
        "processingenddateyear": str(next_30_days.year),
        "oeffilter": get_attr_value(rail.result("get_all_filter_definitions"), "name", "Project Type", "uri"),
        "oeffilteroptionc1": get_attr_value(rail.result("get_oef_drop_down_values_project_type")["tags"], "name", "IC", "uri"),
        "oeffilteroptiongs": get_attr_value(rail.result("get_oef_drop_down_values_project_type")["tags"], "name", "GS", "uri"),
        "ackdateday": str(prev_14_days.day),
        "ackdatemonth": str(prev_14_days.month),
        "ackdateyear": str(prev_14_days.year)
    }

def filter_data_for_gsap_reg_pta_weekly_time_export(config):
    time_data_formats = rail.result("get_time_download_script")
    current_datetime = pendulum.now(config.utc_timezone)
    prev_365_days = current_datetime - relativedelta(days=365)
    prev_90_days = current_datetime - relativedelta(days=90)
    prev_14_days = current_datetime - relativedelta(days=14)
    return {
        "fileformaturi": rail.find_first_by_attr_and_get_attr(time_data_formats, 'displayText', config.gsap_c1_cp_time_export_file_format, 'uri'),
        "contractoruri": rail.result("get_employeetype_groups")["contractor_uri"],
        "agencycontractoruri": rail.result("get_employeetype_groups")["agency_contractor_uri"],
        "companycodelist": list(set(map(lambda company_codes_data: company_codes_data["uri"], rail.result("get_all_gsap_divisions")))),
        "processingstartdateday": str(prev_365_days.day),
        "processingstartdatemonth": str(prev_365_days.month),
        "processingstartdateyear": str(prev_365_days.year),
        "processingenddateday": str(prev_90_days.day),
        "processingenddatemonth": str(prev_90_days.month),
        "processingenddateyear": str(prev_90_days.year),
        "oeffilter": get_attr_value(rail.result("get_all_filter_definitions"), "name", "Project Type", "uri"),
        "oeffilter1": get_attr_value(rail.result("get_all_filter_definitions"), "name", "IWO Indicator", "uri"),
        "oeffilteroption": get_attr_value(rail.result("get_oef_drop_down_values_project_type")["tags"], "name", "CP", "uri"),
        "oeffilteroption1": get_attr_value(rail.result("get_oef_drop_down_values_iwo_indicator")["tags"], "name", "C1", "uri"),
        "ackdateday": str(prev_14_days.day),
        "ackdatemonth": str(prev_14_days.month),
        "ackdateyear": str(prev_14_days.year)
    }

def filter_data_for_gsap_iwo_pta_weekly_time_export(config):
    time_data_formats = rail.result("get_time_download_script")
    current_datetime = pendulum.now(config.utc_timezone)
    prev_365_days = current_datetime - relativedelta(days=365)
    prev_90_days = current_datetime - relativedelta(days=90)
    prev_14_days = current_datetime - relativedelta(days=14)
    return {
        "fileformaturi": rail.find_first_by_attr_and_get_attr(time_data_formats, 'displayText', config.gsap_c1_cp_time_export_file_format, 'uri'),
        "contractoruri": rail.result("get_employeetype_groups")["contractor_uri"],
        "agencycontractoruri": rail.result("get_employeetype_groups")["agency_contractor_uri"],
        "companycodelist": list(set(map(lambda company_codes_data: company_codes_data["uri"], rail.result("get_c1_compass_divisions")))),
        "processingstartdateday": str(prev_365_days.day),
        "processingstartdatemonth": str(prev_365_days.month),
        "processingstartdateyear": str(prev_365_days.year),
        "processingenddateday": str(prev_90_days.day),
        "processingenddatemonth": str(prev_90_days.month),
        "processingenddateyear": str(prev_90_days.year),
        "oeffilter": get_attr_value(rail.result("get_all_filter_definitions"), "name", "Project Type", "uri"),
        "oeffilteroptionc1": get_attr_value(rail.result("get_oef_drop_down_values_project_type")["tags"], "name", "IC", "uri"),
        "oeffilteroptiongs": get_attr_value(rail.result("get_oef_drop_down_values_project_type")["tags"], "name", "GS", "uri"),
        "ackdateday": str(prev_14_days.day),
        "ackdatemonth": str(prev_14_days.month),
        "ackdateyear": str(prev_14_days.year)
    }

def get_current_export_name(prefix):
    previous_export_name = int(rail.result("get_past_time_exports_data_for_GSAP")[0][
        "timeexport"].split('-')[-1])+1
    # pylint: disable=consider-using-f-string
    return prefix+"{:09d}".format(previous_export_name)

def check_ack_date_and_name(dag_run, utc_timezone):
    current_creationdatetime = datetime.strptime(rail.result("for_each_time_export")['creationdate'], TWB_CREATION_DATE_FORMAT)
    creationdatetime = (datetime.strptime(rail.result("log_twb_creation_time"), TWB_CREATION_DATE_FORMAT)
        if rail.result("log_twb_creation_time") else pendulum.now(utc_timezone).replace(tzinfo=null))
    return rail.result("for_each_time_export")['timeexport'] != dag_run.conf['twbname'] and current_creationdatetime < creationdatetime

def get_all_twb_without_acknowledge():
    return rail.result('get_twb_without_acknowledge_data_var')['value']

def get_gsap_wbs(projectname, attendancetypecode, timeofftypename, timeoff_types_to_export):
    return (projectname if projectname else ("9061" if attendancetypecode == "2087"
        else rail.find_first_by_attr_and_get_attr(timeoff_types_to_export, "timeoff_type_name", timeofftypename, "project_code")))

def get_write_final_regular_data_for_processing_csv(item):
    timeoff_types_to_export = rail.result("get_timeoff_types_to_export")
    lsl_type_timeoff_types_to_export = list(map(lambda timeoff_type_data: timeoff_type_data["timeoff_type_name"],
        filter(lambda timeoff_type_data: timeoff_type_data["LSL_type"] == "yes", timeoff_types_to_export)))
    return {
        "repliconuniqueid": item['timeentryid'],
        "workdayperner": (item["perner"] if item["perner"] else (item["actualempid"] if item["actualempid"]
            else (item["iapernerid"] if item["internationalassignee"] == "1" else item["employeeid"])))
                if item["employeetypecode"] == "Contractor" else (item["actualempid"] if item["actualempid"]
                    else (item["iapernerid"] if item["internationalassignee"] == "1" else item["employeeid"])),
        "date": item['entrydate'].replace("/", ""),
        "gsapwbs": (get_gsap_wbs(item["projectname"], item['attendancetypecode'], item["timeofftypename"], timeoff_types_to_export).rjust(8, "0")
            if get_gsap_wbs(item["projectname"], item['attendancetypecode'], item["timeofftypename"], timeoff_types_to_export) else ''),
        "billingkey": ((item["taskname"] if item["taskname"] else '') if item["tasktype"] == "GSAP Billing Key"
            else (item["taskfullpath"].split("/")[0].strip() if item["taskfullpath"] else '') if item["tasktype"] == "GSAP Task" else ''),
        "billingindicator": "X" if item["gsapbillableflag"] == "Billable" else '',
        "task": (item["gsaptaskcode"] if item["gsaptask"] else (item["taskfullpath"].split("/")[-1].strip()
            if "/" in item["taskfullpath"] else '')),
        "hours": "0" if float(item["hours"]) == 0 else (f'{float(item["hours"]):0.2f}'),
        "attendanceabsencetype": (item["timetypeauscode"] if item["timetypeauscode"]
            else (("5010" if float(item["hours"]) < 5.0 else "5000")
                if item["timeofftypename"] in lsl_type_timeoff_types_to_export
                    else rail.find_first_by_attr_and_get_attr(timeoff_types_to_export,
                        "timeoff_type_name", item["timeofftypename"], "paycode"))
                            if item["timeofftypename"] else (item["attendancetypecode"]
                                if item["attendancetypecode"] in ["2087", "2850"]
                                    else (item["timetypeauscode"] if item["timetypeauscode"] else "2082"))),
        "status": "30",
        "remarks": item["comments"],
        "referencenumber": item["gsapreferencenumber"],
        "deletedentry": "yes" if float(item["hours"]) == 0 else "no"
    }.values()

def get_write_final_iwo_data_for_processing_csv(item):
    gsap_wbs_value = (item["projectname"] if item["iwoindicator"] == "C1" else (
        item["projectname"] if item["projecttype"] == "CP" else (
            item["parentwbs"] if item["projecttype"] == "IC" else (
                item["parentwbs"] if item["projecttype"] == "GS" else ''
            )
        )
    ))
    return {
        "repliconuniqueid": item['timeentryid'],
        "workdayperner": (
            (item["employeeid"] if item["companycodecode"] == "COMPASS" else (
                item["perner"] if item["perner"] else (
                    item["actualempid"] if item["actualempid"] else item["employeeid"]
                )
            )) if item["employeetypecode"] == "Contractor" else (
                item["employeeid"] if item["companycodecode"] == "COMPASS" else (
                    item["actualempid"] if item["actualempid"] else (
                        item["iapernerid"] if item["internationalassignee"] == "1" else item["employeeid"]
                    )
                )
            )
        ),
        "date": item['entrydate'].replace("/", ""),
        "gsapwbs": gsap_wbs_value.rjust(8, "0") if gsap_wbs_value else '',
        "billingkey": ("00" if item["iwoindicator"] == "C1" else ("00" if item["projecttype"] == "CP"
            else (item["taskname"] if item["tasktype"] == "GSAP Billing Key" and item["taskname"]
                else (item["taskfullpath"].split("/")[0].strip() if item["tasktype"] == "GSAP Task" and item["taskfullpath"]
                    else null)))),
        "billingindicator": ("X" if item["iwoindicator"] == "C1" else ("X" if item["projecttype"] == "CP"
            else ("X" if item["gsapbillableflag"] == "Billable" else null) if item["gsapbillableflag"] else null)),
        "task": ((item["taskname"] if item["taskname"] else null) if item["iwoindicator"] == "C1"
            else (null if item["projecttype"] == "CP" else (item["gsaptaskcode"] if item["gsaptask"]
                else (null if item["tasktype"] == "GSAP Billing Key" else (item["taskname"] if item["taskname"] else null))))),
        "hours": "0" if float(item["hours"]) == 0 else (f'{float(item["hours"]):0.2f}'),
        "attendanceabsencetype": ((item["timetypeauscode"] if item["timetypeauscode"]
            else (item["attendancetypecode"] if item["attendancetypecode"] in ["2087", "2850"]
                else "2082")) if item["companycodecode"] == "GSAP" else "1010"),
        "status": "30",
        "remarks": item["comments"],
        "referencenumber": item["gsapreferencenumber"],
        "deletedentry": "yes" if float(item["hours"]) == 0 else "no"
    }.values()

def get_gsap_start_line_data():
    return [
        {
            "repliconuniqueid": get_conf()["payload_identifier_replicon_uniqueid"],
            "workdayperner": '',
            "date": '',
            "gsapwbs": '',
            "billingkey": '',
            "billingindicator": '',
            "task": '',
            "hours": '',
            "attendanceabsencetype": '',
            "status": '',
            "remarks": '',
            "referencenumber": '',
            "deletedentry": ''
        }
    ]

def add_starting_line_to_final_data():
    final_time_data = rail.load_all_records(rail.result("final_data_for_processing")) if rail.result("final_data_for_processing") else []
    return [*get_gsap_start_line_data(), *final_time_data]

def get_query_to_filter_reg_time_export_data():
    time_off_types_to_export = '"' + "\",\"".join(list(map(lambda timeoff_type_data: timeoff_type_data["timeoff_type_name"],
        rail.result("get_timeoff_types_to_export")))) + '"'
    return """SELECT * FROM finaltimedata
        WHERE (companycodecode = 'GSAP' AND breaktypename NOT IN ('Meal', 'Rest')
            AND (companycodename NOT IN ('3124') OR personnelareacode NOT IN ('AU36'))
            AND timeofftypename IN ('', """ + time_off_types_to_export + """)
            AND (standbyauscode <> 'Stand by' AND timetypebfi <> 'Stand by'
            AND supplementalpay <> 'Stand by'
            AND attendancetypecode IN ('','2850','2087')))
        OR (companycodecode = 'GSAP'
            AND (companycodename NOT IN ('3124') OR personnelareacode NOT IN ('AU36'))
            AND timeofftypename IN (""" + time_off_types_to_export + """))
        ORDER BY CAST(hours AS FLOAT) ASC"""

def get_query_to_filter_iwo_time_export_data():
    return """SELECT * FROM finaltimedata
        WHERE (companycodecode = 'COMPASS'
        AND projecttype= 'GS' AND attendancetypecode
        NOT IN ('499')) OR (companycodecode = 'C1'
        AND projecttype= 'IC' AND projectname
        LIKE 'X%' AND (beeperpay IS NULL OR beeperpay= '')
        AND (oncallstandby2 IS NULL OR oncallstandby2= ''))
        OR (companycodecode = 'GSAP' AND projecttype= 'CP'
        AND breaktypename NOT IN ('Meal', 'Rest')
        AND (companycodename NOT IN ('3124') OR personnelareacode
        NOT IN ('AU36')) AND ( attendancetypecode IN ('','2850','2087'))
        AND timetypebfi <> 'Stand by' AND supplementalpay <> 'Stand by'
        AND standbyauscode <> 'Stand by')
        OR (companycodecode = 'GSAP' AND iwoindicator= 'C1'
        AND breaktypename NOT IN ('Meal', 'Rest')
        AND (companycodename NOT IN ('3124')
        OR personnelareacode NOT IN ('AU36'))
        AND (attendancetypecode IN ('','2850','2087'))
        AND timetypebfi <> 'Stand by' AND supplementalpay <> 'Stand by'
        AND standbyauscode <> 'Stand by') AND standbyauscode <> 'Stand by'
        ORDER BY CAST(hours AS FLOAT) ASC"""
