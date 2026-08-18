from datetime import datetime
from dateutil.relativedelta import relativedelta
from airflow.models import Variable
import pendulum
import rail
null = None

TWB_CREATION_DATE_FORMAT = '%d %B %Y %I:%M:%S %p'

def get_conf():
    return rail.get_current_context()['dag_run'].conf

def get_current_export_name():
    previous_export_name = int(rail.result("get_past_time_exports_data_for_psa")[0][
        "timeexport"].split('-')[-1])+1
    # pylint: disable=consider-using-f-string
    return "REG-GS-"+"{:09d}".format(previous_export_name)

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

def get_final_c1_reg_iwo_time_export_data():
    return [{
        "employeeid": item["actualempid"] if item["actualempid"] else item["employeeid"],
        "date": item["entrydate"],
        "wbs": item["projectname"],
        "attendancetypecode": '1399' if item['beeperpay'] else ('1010' if item['iwoindicator'] == "X" else
            ('1309' if item['oncallstandby2'] else
            (item['timetype'][0:4] if item['timetype'] else
            (item['timetype2'][0:4] if item['timetype2'] else
            (item['attendancetypecode'] if item['attendancetypecode'] else
            (item['timeofftypedescription'] if item['timeoffbookingid'] else '1010')))))),
        "labortype": item["labortype"].split("|")[0] if item["labortype"] else '',
        "billableindicator": item["labortype"].split("|")[-1] if item["labortype"] else '',
        "hours": str(round(float(item["hours"]), 2)),
        "task": item["taskname"] if item["tasktype"] != "GSAP Billing Key" else '',
        "attributecode1": item["attribute1code"],
        "attributecode2": item["attribute2code"],
        "shorttext": item["comments"],
        "billingkey": item["taskname"] if item["tasktype"] == "GSAP Billing Key" else '',
        "gsaptask": item["gsaptaskcode"],
        "gsapbillableflag": item["gsapbillableflag"],
        "repliconuniqueid": item["timeentryid"],
        "homeerp": item["companycodecode"],
        "parentprojecterp": item["companycodecode"],
        "homelocation": item["locationcode"],
        "timeoff": "Y" if item["timeoffbookingid"] else "N",
        "projecttypeflag": item["psaflag"]
    } for item in rail.load_all_records(rail.result("merge_c1_reg_iwo_filtered_and_reversals"))]

def get_final_compass_reg_iwo_time_export_data():
    return [{
        "employeeid": item["actualempid"] if item["actualempid"] else item["employeeid"],
        "date": item["entrydate"],
        "wbs": item["projectname"],
        "attendancetypecode": "400" if item["companycodecode"] == "C1" else (item["attendancetypecode"]
            if item["attendancetypecode"] else (item["timeofftypedescription"] if item["timeoffbookingid"] else '')),
        "labortype": item["labortype"].split("|")[0] if item["labortype"] else '',
        "billableindicator": item["labortype"].split("|")[-1] if item["labortype"] else '',
        "hours": item["hours"],
        "task": item["taskname"] if item["tasktype"] != "GSAP Billing Key" else '',
        "attributecode1": item["attribute1code"],
        "attributecode2": item["attribute2code"],
        "shorttext": item["comments"],
        "billingkey": item["taskname"] if item["tasktype"] == "GSAP Billing Key" else '',
        "gsaptask": item["gsaptask"],
        "gsapbillableflag": item["gsapbillableflag"],
        "repliconuniqueid": item["timeentryid"],
        "homeerp": item["companycodecode"],
        "parentprojecterp": item["companycodecode"],
        "homelocation": item["locationcode"],
        "timeoff": "Y" if item["timeoffbookingid"] else "N",
        "projecttypeflag": item["psaflag"]
    } for item in rail.load_all_records(rail.result("merge_compass_reg_iwo_filtered_and_reversals"))]

def get_final_psa_time_export_data(timeoff_types_to_export):
    all_timeoff_types = Variable.get(timeoff_types_to_export, deserialize_json=True)
    lsl_type_timeoff_types_to_export = list(filter(lambda timeoff_type_data: timeoff_type_data["LSL_type"] == "yes", all_timeoff_types))
    return [{
        "employeeid": item["actualempid"] if item["actualempid"] else item["employeeid"],
        "date": item['entrydate'].replace("/", ""),
        "wbs": (item["parentwbs"] if item["parentwbs"] else item["projectname"]) if item["projectname"] else
            ("9061" if item["attendancetypecode"] == "2087" else
                rail.find_first_by_attr_and_get_attr(all_timeoff_types,
                    "timeoff_type_name", item["timeofftypename"], "project_code")),
        "task": item["gsaptaskcode"] if item["gsaptask"] else '',
        "hours": "0" if float(item["hours"]) <= 0 else (f'{float(item["hours"]):0.2f}'),
        "repliconuniqueid": item["timeentryid"],
        "comments": item["comments"],
        "billingkey": '' if item["timeoffbookingid"] else ("00" if item["projecttype"] == "CP" else
            ("00" if item["iwoindicator"] == "C1" else (
                item["taskname"] if item["tasktype"] == "GSAP Billing Key" else ''))),
        "projectkey": '' if item["timeoffbookingid"] else ("00" if item["gsapbillableflag"] == "Billable" else (
            "01" if item["gsapbillableflag"] == "Non-Billable" else ('' if item["projecttype"] == "ES" else (
                "00" if item["projecttype"] == "CP" else (("00" if item["labortype"].endswith("|Billable") else "01")
                    if item["iwoindicator"] == "X" else (("00" if item["labortype"].endswith("|Billable") else "01")
                        if item["iwoindicator"] == "C1" else '')))))),
        "attendancetypecode": "2082" if item["projecttype"] == "IC" or item["projecttype"] == "GS" or
            item["projecttype"] == "ES" or item["iwoindicator"] == "X" else (item["timetypeauscode"]
                if item["timetypeauscode"] else ((("5010" if float(item["hours"]) < 5.0 else "5000")
                    if rail.find_first_by_attr_and_get_attr(lsl_type_timeoff_types_to_export,
                        "timeoff_type_name", item["timeofftypename"]) else (
                            rail.find_first_by_attr_and_get_attr(all_timeoff_types,
                                "timeoff_type_name", item["timeofftypename"], "paycode"))
                    ) if item["timeofftypename"] else (item["attendancetypecode"] if item["attendancetypecode"] == "2087" or
                        item["attendancetypecode"] == "2850" else (item["timetypeauscode"] if item["timetypeauscode"] else "2082")))),
        "childprojectwbs": item["projectname"] if item["parentwbs"] else '',
        "childprojecterp": item["companycodecode"] if item["parentwbs"] else '',
        "iwowbsflag": '' if item["wbstype"] == "DIWO" else ("X" if item["parentwbs"] else ''),
        "parentprojecterp": "C1" if item["iwoindicator"] == "X" or item["iwoindicator"] == "C1" else (
            "COMPASS" if item["projecttype"] == "ES" or item["projecttype"] == "CP" else (
                "GSAP" if item["projecttype"] == "IC" or item["projecttype"] == "GS" or item["wbstype"] == "DIWO" else (
                    (item["wbstype"] if item["wbstype"] == "GSAP" else '') if item["parentwbs"] else ''
                ))),
        "labortype": (item["labortype"] if item["labortype"] else '') if item["projecttype"] == "CP" else (
            (item["labortype"].split("|")[0] if item["labortype"] else '') if item["iwoindicator"] == "C1" else (
                (item["labortype"] if item["labortype"] else '') if item["projecttype"] == "ES" else (
                    (item["labortype"].split("|")[0] if item["labortype"] else '') if item["iwoindicator"] == "X" else ''
            ))),
        "homeerp": item["companycodecode"],
        "projecttypeflag": "X" if item["psaflag"] == "X" else ''
    } for item in rail.load_all_records(rail.result("merge_filtered_and_reversals"))]

def get_gsap_start_line_data():
    return [
        {
            "employeeid": '',
            "date": '',
            "wbs": '',
            "task": '',
            "hours": '',
            "repliconuniqueid": get_conf()["payload_identifier_replicon_uniqueid"],
            "comments": '',
            "billingkey": '',
            "projectkey": '',
            "attendancetypecode": '',
            "childprojectwbs": '',
            "childprojecterp": '',
            "iwowbsflag": '',
            "parentprojecterp": '',
            "labortype": '',
            "homeerp": '',
            "projecttypeflag": ''
        }
    ]

def get_c1_compass_start_line_data():
    return [
        {
            "employeeid": '',
            "date": '',
            "wbs": '',
            "attendancetypecode": '',
            "labortype": '',
            "billableindicator": '',
            "hours": '',
            "task": '',
            "attributecode1": '',
            "attributecode2": '',
            "shorttext": '',
            "billingkey": '',
            "gsaptask": '',
            "gsapbillableflag": '',
            "repliconuniqueid": get_conf()["payload_identifier_replicon_uniqueid"],
            "homeerp": '',
            "parentprojecterp": '',
            "homelocation": '',
            "timeoff": '',
            "projecttypeflag": ''
        }
    ]

def add_starting_line_to_c1_compass_final_data():
    final_time_data = rail.load_all_records(rail.result("final_data_for_processing")) if rail.result("final_data_for_processing") else []
    return [*get_c1_compass_start_line_data(), *final_time_data]

def add_starting_line_to_gsap_final_data():
    final_time_data = rail.load_all_records(rail.result("final_data_for_processing")) if rail.result("final_data_for_processing") else []
    return [*get_gsap_start_line_data(), *final_time_data]

def get_query_to_filter_c1_compass_time_export_data(timeoff_types_to_exclude):
    psa_org_units_list = '"' + "\",\"".join(rail.result("get_psa_org_unit_child_hierarchy")) + '"'
    timeoff_types_to_exclude = '"' + "\",\"".join(list(map(lambda timeoff_type_data: timeoff_type_data["timeoff_type_name"],
        Variable.get(timeoff_types_to_exclude, deserialize_json=True)))) + '"'
    return {
        "c1_export_query": """SELECT * FROM finaltimedata WHERE ((companycodecode = 'C1'
            AND (attendancetypecode IS NULL OR attendancetypecode NOT LIKE '%799%'))
            OR (iwoindicator = 'X' AND (parentproject IS NULL OR parentproject = '')
            AND attendancetypecode NOT LIKE '%799%' AND ((parentwbs != '')
            OR (parentserviceorder!= '')))) AND ((LOWER(psaflag) = 'x'
            OR orgunitname IN (""" + psa_org_units_list + """)))
            AND standbyauscode <> "Stand by" AND (beeperpay IS NULL
            OR beeperpay= '') AND (oncallstandby2 IS NULL OR oncallstandby2 = '')
            AND timetypebfi <> "Stand by" AND supplementalpay <> "Stand by"
            AND attendancetypecode <> '499' AND profsupplementalpay <> "Stand by" ORDER BY CAST(hours AS FLOAT) ASC""",
        "compass_export_query": """SELECT * FROM finaltimedata WHERE ((companycodecode = 'COMPASS'
            AND attendancetypecode NOT IN ('499', '999', '779') AND breaktypename
            NOT IN ('Meal', 'Rest') AND timeofftypename NOT IN (""" + timeoff_types_to_exclude + """))
            OR (projecttype = 'ES' AND projectname LIKE 'E-%' AND attendancetypecode NOT IN ('499', '999', '779')
            AND breaktypename NOT IN ('Meal', 'Rest'))) AND ((LOWER(psaflag) = 'x'
            OR orgunitname IN (""" + psa_org_units_list + """)))
            AND standbyauscode <> "Stand by" AND (beeperpay IS NULL OR beeperpay = '')
            AND (oncallstandby2 IS NULL OR oncallstandby2 = '') AND timetypebfi <> "Stand by"
            AND supplementalpay <> "Stand by" AND attendancetypecode <> '499'
            AND profsupplementalpay <> "Stand by"
            ORDER BY CAST(hours AS FLOAT) ASC"""
    }

def get_query_to_filter_psa_time_export_data(timeoff_types_to_export):
    psa_org_units_list = '"' + "\",\"".join(rail.result("get_psa_org_unit_child_hierarchy")) + '"'
    psa_cost_centers_list = '"' + "\",\"".join(rail.result("get_psa_cost_center_child_hierarchy")) + '"'
    timeoff_types_to_export = '"' + "\",\"".join(list(map(lambda timeoff_type_data: timeoff_type_data["timeoff_type_name"],
        Variable.get(timeoff_types_to_export, deserialize_json=True)))) + '"'
    return {
        "gsap_regular": """SELECT * FROM finaltimedata WHERE ((companycodecode = 'GSAP'
            AND breaktypename NOT IN ('Meal', 'Rest') AND (companycodename NOT IN ('3124')
            OR personnelareacode NOT IN ('AU36')) AND timeofftypename
            IN ("",""" + timeoff_types_to_export + """) AND (attendancetypecode IN ('','2850','2087')))
            OR (companycodecode = 'GSAP' AND (companycodename NOT IN ('3124')
            OR personnelareacode NOT IN ('AU36')) AND timeofftypename IN (""" + timeoff_types_to_export + """)))
            AND iwoindicator <> "C1" AND projecttype <> "CP" AND (psaflag IN ('x','X')
            OR costcentercode IN (""" + psa_cost_centers_list + """)
            OR orgunitname IN (""" + psa_org_units_list + """))
            AND attendancetypecode NOT LIKE '%799%' AND breaktypename NOT IN ('Meal', 'Rest')
            AND (beeperpay IS NULL OR beeperpay = '') AND (oncallstandby2 IS NULL
            OR oncallstandby2 = '') AND timetypebfi <> "Stand by" AND supplementalpay <> "Stand by"
            AND attendancetypecode <> '499' AND standbyauscode <> "Stand by"
            AND profsupplementalpay <> "Stand by"
            ORDER BY CAST(hours AS FLOAT) ASC""",
        "gsap_iwo": """SELECT * FROM finaltimedata WHERE ((companycodecode = 'COMPASS'
            AND projecttype = 'GS') OR (companycodecode = 'C1' AND projecttype = 'IC'
            AND projectname LIKE 'X%')) AND (psaflag IN ('x','X') OR costcentercode
            IN (""" + psa_cost_centers_list + """) OR orgunitname IN (""" + psa_org_units_list + """))
            AND attendancetypecode NOT LIKE '%799%' AND breaktypename NOT IN ('Meal', 'Rest')
            AND standbyauscode <> "Stand by" AND (beeperpay IS NULL OR beeperpay = '')
            AND (oncallstandby2 IS NULL OR oncallstandby2 = '') AND timetypebfi <> "Stand by"
            AND supplementalpay <> "Stand by" AND attendancetypecode <> '499'
            AND profsupplementalpay <> "Stand by"
            ORDER BY CAST(hours AS FLOAT) ASC""",
        "c1_iwo": """SELECT * FROM finaltimedata WHERE ((iwoindicator = 'X'
            AND (parentproject IS NULL OR parentproject = '')
            AND attendancetypecode NOT LIKE '%799%' AND ((parentwbs != '')
            OR (parentserviceorder != ''))) OR (companycodecode = 'GSAP' AND iwoindicator = "C1"))
            AND (psaflag IN ('x','X') OR costcentercode IN (""" + psa_cost_centers_list + """)
            OR orgunitname IN (""" + psa_org_units_list + """)) AND attendancetypecode NOT LIKE '%799%'
            AND breaktypename NOT IN ('Meal', 'Rest') AND standbyauscode <> "Stand by"
            AND (beeperpay IS NULL OR beeperpay = '') AND (oncallstandby2 IS NULL
            OR oncallstandby2 = '') AND timetypebfi <> "Stand by" AND supplementalpay <> "Stand by"
            AND attendancetypecode <> '499' AND profsupplementalpay <> "Stand by" ORDER BY CAST(hours AS FLOAT) ASC""",
        "compass_iwo": """SELECT * FROM finaltimedata WHERE ((projecttype = 'ES'
            AND projectname LIKE 'E-%' AND attendancetypecode NOT IN ('499', '999', '779')
            AND breaktypename NOT IN ('Meal', 'Rest')) OR (companycodecode = 'GSAP'
            AND projecttype = "CP" AND standbyauscode <> "Stand by"))
            AND (psaflag IN ('x','X') OR costcentercode IN (""" + psa_cost_centers_list + """)
            OR orgunitname IN (""" + psa_org_units_list + """)) AND attendancetypecode NOT LIKE '%799%'
            AND standbyauscode <> "Stand by" AND (beeperpay IS NULL OR beeperpay = '')
            AND (oncallstandby2 IS NULL OR oncallstandby2 = '') AND timetypebfi <> "Stand by"
            AND supplementalpay <> "Stand by" AND attendancetypecode <> '499'
            AND profsupplementalpay <> "Stand by"
            ORDER BY CAST(hours AS FLOAT) ASC""",
        "merge_gsap_reg_iwo_c1_compass_iwo": """SELECT * FROM filtered_gsap_reg_time_export_data
            UNION ALL SELECT * FROM filtered_gsap_iwo_time_export_data
            UNION ALL SELECT * FROM filtered_c1_iwo_time_export_data
            UNION ALL SELECT * FROM filtered_compass_iwo_time_export_data""",
        "psa_data": """SELECT * FROM filtered_gsap_c1_compass_time_export_data
            WHERE filtered_gsap_c1_compass_time_export_data.psaflag IN ('x', 'X')
            OR filtered_gsap_c1_compass_time_export_data.costcentercode IN (""" + psa_cost_centers_list + """)
            OR filtered_gsap_c1_compass_time_export_data.orgunitname IN (""" + psa_org_units_list + """)
            ORDER BY CAST(filtered_gsap_c1_compass_time_export_data.hours AS FLOAT) ASC"""
    }
