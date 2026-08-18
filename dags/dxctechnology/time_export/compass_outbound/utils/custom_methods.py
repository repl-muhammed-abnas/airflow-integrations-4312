from datetime import datetime
import itertools
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

def get_filename(config, code_1, code_2, task_type, export_type):
    if config.company_key.lower() != "dxctechnology":
        return {
            "sftp_filename": f'{code_2}_{get_conf()["twbname"]}.xml',
            "s3_filename": f'{code_1}/{get_conf()["twbname"]}.xml'
        }
    # For DXCTechnology, determine the correct sftp_filename prefix based on task_type and export_type
    # Workato mapping:
    # Regular: p01_nt2 = P01 (code_1), pn1_nt1 = NT1 (code_2), pj1_nt3 = PJ1 (code_1)
    # IWO: p01_nt2 = P01 (code_1), pn1_nt1 = NT1 (code_2), pj1_nt3 = NT3 (code_2)
    if task_type == 'p01_nt2':
        prefix = code_1  # P01
    elif task_type == 'pn1_nt1':
        prefix = code_2  # NT1
    elif task_type == 'pj1_nt3':
        if export_type == 'iwo':
            prefix = code_2  # NT3
        else:
            prefix = code_1  # PJ1 for regular
    else:
        prefix = code_1  # default to code_1
    return {
        "sftp_filename": f'{prefix}_{get_conf()["twbname"]}.xml',
        "s3_filename": f'{code_1}_{get_conf()["twbname"]}.xml'
    }

def get_attr_value(dataset, dataset_key, target, value):
    return rail.find_first_by_attr_and_get_attr(dataset, dataset_key, target, value)

def filter_data_for_compass_reg_time_export(config):
    time_data_formats = rail.result("get_time_download_script")
    current_datetime = pendulum.now(config.utc_timezone)
    prev_90_days = current_datetime - relativedelta(days=90)
    prev_14_days = current_datetime - relativedelta(days=14)
    next_30_days = current_datetime + relativedelta(days=30)
    return {
        "fileformaturi": rail.find_first_by_attr_and_get_attr(time_data_formats, 'displayText', config.compass_reg_time_export_file_format, 'uri'),
        "contractoruri": rail.result("get_employeetype_groups")["contractor_uri"],
        "agencycontractoruri": rail.result("get_employeetype_groups")["agency_contractor_uri"],
        "companycodelist": list(set(list(map(lambda company_codes_data: company_codes_data["uri"], rail.result("get_all_compass_divisions"))))),
        "processingstartdateday": str(prev_90_days.day),
        "processingstartdatemonth": str(prev_90_days.month),
        "processingstartdateyear": str(prev_90_days.year),
        "processingenddateday": str(next_30_days.day),
        "processingenddatemonth": str(next_30_days.month),
        "processingenddateyear": str(next_30_days.year),
        "oeffilter": get_attr_value(rail.result("get_all_filter_definitions"), "name", "IWO Indicator", "uri"),
        "oeffilter1": get_attr_value(rail.result("get_all_filter_definitions"), "name", "Project Type", "uri"),
        "oeffilteroption": get_attr_value(rail.result("get_oef_drop_down_values_iwo_indicator")["tags"], "name", "X", "uri"),
        "oeffilteroption1": get_attr_value(rail.result("get_oef_drop_down_values_project_type")["tags"], "name", "GS", "uri"),
        "ackdateday": str(prev_14_days.day),
        "ackdatemonth": str(prev_14_days.month),
        "ackdateyear": str(prev_14_days.year)
    }

def filter_data_for_compass_iwo_time_export(config):
    current_datetime = pendulum.now(config.utc_timezone)
    prev_90_days = current_datetime - relativedelta(days=90)
    prev_14_days = current_datetime - relativedelta(days=14)
    next_30_days = current_datetime + relativedelta(days=30)
    return {
        "fileformaturi": rail.find_first_by_attr_and_get_attr(rail.result("get_time_download_script"),
            'displayText', config.compass_iwo_time_export_file_format, 'uri'),
        "contractoruri": rail.result("get_employeetype_groups")["contractor_uri"],
        "agencycontractoruri": rail.result("get_employeetype_groups")["agency_contractor_uri"],
        "companycodelist": list(set(list(map(lambda company_codes_data: company_codes_data["uri"], rail.result("get_all_gsap_c1_divisions"))))),
        "processingstartdateday": str(prev_90_days.day),
        "processingstartdatemonth": str(prev_90_days.month),
        "processingstartdateyear": str(prev_90_days.year),
        "processingenddateday": str(next_30_days.day),
        "processingenddatemonth": str(next_30_days.month),
        "processingenddateyear": str(next_30_days.year),
        "oeffilter": get_attr_value(rail.result("get_all_filter_definitions"), "name", "Project Type", "uri"),
        "oeffilteroptionc1": get_attr_value(rail.result("get_oef_drop_down_values_project_type")["tags"], "name", "ES", "uri"),
        "oeffilteroptioncp": get_attr_value(rail.result("get_oef_drop_down_values_project_type")["tags"], "name", "CP", "uri"),
        "ackdateday": str(prev_14_days.day),
        "ackdatemonth": str(prev_14_days.month),
        "ackdateyear": str(prev_14_days.year)
    }

def filter_data_for_compass_reg_pta_weekly_time_export(config):
    time_data_formats = rail.result("get_time_download_script")
    current_datetime = pendulum.now(config.utc_timezone)
    prev_365_days = current_datetime - relativedelta(days=365)
    prev_90_days = current_datetime - relativedelta(days=90)
    prev_14_days = current_datetime - relativedelta(days=14)
    return {
        "fileformaturi": rail.find_first_by_attr_and_get_attr(time_data_formats, 'displayText', config.compass_reg_time_export_file_format, 'uri'),
        "contractoruri": rail.result("get_employeetype_groups")["contractor_uri"],
        "agencycontractoruri": rail.result("get_employeetype_groups")["agency_contractor_uri"],
        "companycodelist": list(set(list(map(lambda company_codes_data: company_codes_data["uri"], rail.result("get_all_compass_divisions"))))),
        "processingstartdateday": str(prev_365_days.day),
        "processingstartdatemonth": str(prev_365_days.month),
        "processingstartdateyear": str(prev_365_days.year),
        "processingenddateday": str(prev_90_days.day),
        "processingenddatemonth": str(prev_90_days.month),
        "processingenddateyear": str(prev_90_days.year),
        "oeffilter": get_attr_value(rail.result("get_all_filter_definitions"), "name", "IWO Indicator", "uri"),
        "oeffilter1": get_attr_value(rail.result("get_all_filter_definitions"), "name", "Project Type", "uri"),
        "oeffilteroption": get_attr_value(rail.result("get_oef_drop_down_values_iwo_indicator")["tags"], "name", "X", "uri"),
        "oeffilteroption1": get_attr_value(rail.result("get_oef_drop_down_values_project_type")["tags"], "name", "GS", "uri"),
        "ackdateday": str(prev_14_days.day),
        "ackdatemonth": str(prev_14_days.month),
        "ackdateyear": str(prev_14_days.year)
    }

def filter_data_for_compass_iwo_pta_weekly_time_export(config):
    current_datetime = pendulum.now(config.utc_timezone)
    prev_365_days = current_datetime - relativedelta(days=365)
    prev_90_days = current_datetime - relativedelta(days=90)
    prev_60_days = current_datetime - relativedelta(days=60)
    return {
        "fileformaturi": rail.find_first_by_attr_and_get_attr(rail.result("get_time_download_script"),
            'displayText', config.compass_iwo_time_export_file_format, 'uri'),
        "contractoruri": rail.result("get_employeetype_groups")["contractor_uri"],
        "agencycontractoruri": rail.result("get_employeetype_groups")["agency_contractor_uri"],
        "companycodelist": list(set(list(map(lambda company_codes_data: company_codes_data["uri"], rail.result("get_all_gsap_c1_divisions"))))),
        "processingstartdateday": str(prev_365_days.day),
        "processingstartdatemonth": str(prev_365_days.month),
        "processingstartdateyear": str(prev_365_days.year),
        "processingenddateday": str(prev_90_days.day),
        "processingenddatemonth": str(prev_90_days.month),
        "processingenddateyear": str(prev_90_days.year),
        "oeffilter": get_attr_value(rail.result("get_all_filter_definitions"), "name", "Project Type", "uri"),
        "oeffilteroptionc1": get_attr_value(rail.result("get_oef_drop_down_values_project_type")["tags"], "name", "ES", "uri"),
        "oeffilteroptioncp": get_attr_value(rail.result("get_oef_drop_down_values_project_type")["tags"], "name", "CP", "uri"),
        "ackdateday": str(prev_60_days.day),
        "ackdatemonth": str(prev_60_days.month),
        "ackdateyear": str(prev_60_days.year)
    }

def get_current_export_name(prefix):
    previous_export_name = int(rail.result("get_past_time_exports_data_for_compass")[0][
        "timeexport"].split('-')[-1])+1
    # pylint: disable=consider-using-f-string
    return prefix+"{:09d}".format(previous_export_name)

def get_all_object_extension_field_bindings():
    return rail.result('get_all_object_extension_field_bindings')

def get_oef_bindings_uri(name):
    return rail.find_first_by_attr_and_get_attr(get_all_object_extension_field_bindings(),
        "displayText", name, "uri")

def check_ack_date_and_name(dag_run, time_zone):
    current_creationdatetime = datetime.strptime(rail.result("for_each_time_export_compass")['creationdate'], TWB_CREATION_DATE_FORMAT)
    creationdatetime = (datetime.strptime(rail.result("log_twb_creation_time"), TWB_CREATION_DATE_FORMAT)
        if rail.result("log_twb_creation_time") else pendulum.now(time_zone).replace(tzinfo=null))
    return rail.result("for_each_time_export_compass")['timeexport'] != dag_run.conf['twbname'] and \
        current_creationdatetime < creationdatetime

def get_all_twb_without_acknowledge():
    return rail.result('get_twb_without_acknowledge_data_var')['value']

def get_data_existence_var_data():
    return list(itertools.chain.from_iterable(rail.result('get_data_existence_var')['value']))

def is_float_in_range(number, start, end):
    return start < number <= end

def get_initial_data_for_processing():
    enabled_company_codes_list = rail.result("get_effectively_enabled_compass_divisions")
    return [
	    {
	    	"companycodecode": item["companycodecode"],
	    	"employeeid": item["actualempid"] if item["actualempid"] else item["employeeid"],
	    	"perner": item["employeeid"] if not item["projecttype"] else (item["perner"] if item["projecttype"] == "ES" else ''),
	    	"approvalstatus": item["approvalstatus"],
	    	"entrydate": item["entrydate"],
	    	"projectname": item["projectname"],
	    	"costcentercode": item["costcentercode"],
	    	"labortype": item["labortype"],
	    	"jobactivitytype": item["jobactivitytype"],
	    	"taskname": item["taskname"],
	    	"timetype": item["timetype"],
	    	"attendancetypecode": item["attendancetypecode"],
	    	"billableindicator": item["billableindicator"],
	    	"hours": item["hours"],
	    	"ratetype": item["ratetype"],
	    	"timeentryid": item["timeentryid"],
	    	"timeoffbookingid": (str(item["entrydate"]) + str(item["timeoffbookingid"])) if item["timeoffbookingid"] else '',
	    	"comments": item["comments"],
	    	"wbstype": item["wbstype"],
	    	"tasktype": item["tasktype"],
	    	"newremainningwork": item["newremainningwork"],
	    	"customer1": item["customer1"],
	    	"customer2": item["customer2"],
	    	"customer3": item["customer3"],
	    	"gsapbillableflag": item["gsapbillableflag"],
	    	"timeofftypedescription": item["timeofftypedescription"],
	    	"masterwbs": item["masterwbs"],
	    	"projecttype": item["projecttype"],
	    	"iwoindicator": item["iwoindicator"],
	    	"parentwbs": '' if item["parentserviceorder"] else item["parentwbs"],
	    	"companycodename": item["companycodename"],
	    	"companycodedesc": rail.find_first_by_attr_and_get_attr(enabled_company_codes_list,
                "companycodename", item["companycodename"], "parent"),
	    	"taskfullpath": item["taskfullpath"],
	    	"length": len(item["taskfullpath"].split(" / ")),
	    	"timeentryid2": item["timeentryid2"],
	    	"parentserviceorder": item["parentserviceorder"],
	    	"internationalassignee": item["internationalassignee"],
	    	"iapernerid": item["iapernerid"],
	    	"iwowbselement": item["iwowbselement"],
	    	"attributecode1": item["attributecode1"],
	    	"attributecode2": item["attributecode2"]
	    } for item in rail.load_all_records(rail.result("query_filter_time_export_data"))
    ]

def get_each_company_final_reg_data(dataset, data_length, task_type):
    if task_type == "p01_nt2":
        return get_p01_nt2_company_final_reg_data(dataset, data_length)
    elif task_type == "pn1_nt1":
        return get_pn1_nt1_company_final_reg_data(dataset, data_length)
    elif task_type == "pj1_nt3":
        return get_pj1_nt3_company_final_reg_data(dataset, data_length)

def get_p01_nt2_company_final_reg_data(dataset, data_length):
    return [{
        "entryid": item["timeentryid2"],
        "shortid": item["timeentryid"] if item["timeentryid2"] else item["timeoffbookingid"],
        "externalsystemidentifier": "REPLICON",
        "perner": item["employeeid"],
        "date": item["entrydate"],
        "projectname": '' if item["wbstype"] == "Opportunity" else (item["iwowbselement"] if item["companycodecode"] == "C1" else item["projectname"]),
        "attendanceabsencetype": "400" if item["companycodecode"] == "C1" else (item["attendancetypecode"]
            if item["attendancetypecode"] else (item["timeofftypedescription"] if item["timeoffbookingid"] else '')),
        "hours": round(float(item["hours"]), 2),
        "comments": item["comments"],
        "iwoexternalsystem": "C1" if item["companycodecode"] == "C1" and item["projecttype"] == "ES" else '',
        "attribute1": '' if item["tasktype"] == "Opportunity" else \
            item["taskname"] if item["tasktype"] == "Attribute 1" else \
            (item["taskfullpath"].split(" / ")[0] if
                (item["tasktype"] == "GSAP Task" and data_length == "4") or \
                (item["tasktype"] == "GSAP Billing Key" and data_length == "3") or \
                (item["tasktype"] == "Attribute 2" and data_length == "2") or \
                ((item["tasktype"] == "PPMC Project & Task") and (data_length in ("2", "3"))) else \
            item["attributecode1"] if item["attributecode1"] else ''),
        "attribute2": '' if item["tasktype"] == "Opportunity" else \
            item["taskname"] if item["tasktype"] == "Attribute 2" else \
            item["attributecode2"] if item["attributecode2"] else \
            (item["taskfullpath"].split(" / ")[1] if
                (item["tasktype"] == "GSAP Task" and data_length == "4") or \
                (item["tasktype"] == "GSAP Billing Key" and data_length == "3") or \
                ((item["tasktype"] == "PPMC Project & Task") and (data_length == "3")) else ''),
        "externalprojecttask": '' if item["tasktype"] == "Opportunity" else
            item["taskname"] if item["tasktype"] and "PPMC" in item["tasktype"] else '',
        "remainingwork": item["newremainningwork"],
        "cfield1": item["customer1"],
        "cfield2": item["customer2"],
        "cfield3": item["customer3"],
        "workorder": '',
        "ratetype": '',
        "tmrole": item["labortype"] if item["labortype"] else '',
        "gsapbillingkey": '' if item["tasktype"] == "Opportunity" else
            item["taskname"] if item["tasktype"] == "GSAP Billing Key" else
            (item["taskfullpath"].split(" / ")[0] if item["tasktype"] == "GSAP Task" and data_length == "2" else
            (item["taskfullpath"].split(" / ")[1] if item["tasktype"] == "GSAP Task" and data_length == "3" else
            (item["taskfullpath"].split(" / ")[2] if item["tasktype"] == "GSAP Task" and data_length == "4" else ''))),
        "gsaptask": '' if item["tasktype"] == "Opportunity" else
            item["taskname"] if item["tasktype"] == "GSAP Task" else '',
        "gsapbillableflag": "X" if item["gsapbillableflag"] and (item["gsapbillableflag"] == "X" or
            item["gsapbillableflag"] == "Billable") else '',
        "bdopportunityid": item["taskname"] if item["tasktype"] == "Opportunity" else ''
    } for item in rail.load_all_records(dataset)]

def get_pn1_nt1_company_final_reg_data(dataset, data_length):
    return [{
        "entryid": item["timeentryid2"],
        "shortid": item["timeentryid"] if item["timeentryid2"] else item["timeoffbookingid"],
        "externalsystemidentifier": "REPLICON",
        "perner": item["employeeid"],
        "date": item["entrydate"],
        "projectname": '' if item["wbstype"] == "Opportunity" else (item["iwowbselement"] if item["companycodecode"] == "C1" else item["projectname"]),
        "attendanceabsencetype": "400" if item["companycodecode"] == "C1" else (item["attendancetypecode"]
            if item["attendancetypecode"] else (item["timeofftypedescription"] if item["timeoffbookingid"] else '')),
        "hours": round(float(item["hours"]), 2),
        "comments": item["comments"],
        "iwoexternalsystem": "C1" if item["companycodecode"] == "C1" and item["projecttype"] == "ES" else '',
        "attribute1": '' if item["tasktype"] == "Opportunity" else \
            item["taskname"] if item["tasktype"] == "Attribute 1" else \
            (item["taskfullpath"].split(" / ")[0] if
                (item["tasktype"] == "GSAP Task" and data_length == "4") or \
                (item["tasktype"] == "GSAP Billing Key" and data_length == "3") or \
                (item["tasktype"] == "Attribute 2" and data_length == "2") or \
                ((item["tasktype"] == "PPMC Project & Task") and (data_length in ("2", "3"))) else \
            item["attributecode1"] if item["attributecode1"] else ''),
        "attribute2": '' if item["tasktype"] == "Opportunity" else \
            item["taskname"] if item["tasktype"] == "Attribute 2" else \
            item["attributecode2"] if item["attributecode2"] else \
            (item["taskfullpath"].split(" / ")[1] if
                (item["tasktype"] == "GSAP Task" and data_length == "4") or \
                (item["tasktype"] == "GSAP Billing Key" and data_length == "3") or \
                ((item["tasktype"] == "PPMC Project & Task") and (data_length == "3")) else ''),
        "externalprojecttask": '' if item["tasktype"] == "Opportunity" else
            item["taskname"] if item["tasktype"] and "PPMC" in item["tasktype"] else '',
        "remainingwork": item["newremainningwork"],
        "cfield1": item["customer1"],
        "cfield2": item["customer2"],
        "cfield3": item["customer3"],
        "workorder": '',
        "ratetype": '',
        "tmrole": item["labortype"] if item["labortype"] else '',
        "gsapbillingkey": '' if item["tasktype"] == "Opportunity" else
            item["taskname"] if item["tasktype"] == "GSAP Billing Key" else
            (item["taskfullpath"].split(" / ")[0] if item["tasktype"] == "GSAP Task" and data_length == "2" else
            (item["taskfullpath"].split(" / ")[1] if item["tasktype"] == "GSAP Task" and data_length == "3" else
            (item["taskfullpath"].split(" / ")[2] if item["tasktype"] == "GSAP Task" and data_length == "4" else ''))),
        "gsaptask": '' if item["tasktype"] == "Opportunity" else
            item["taskname"] if item["tasktype"] == "GSAP Task" else '',
        "gsapbillableflag": "X" if item["gsapbillableflag"] and (item["gsapbillableflag"] == "X" or
            item["gsapbillableflag"] == "Billable") else '',
        "bdopportunityid": item["taskname"] if item["tasktype"] == "Opportunity" else ''
    } for item in rail.load_all_records(dataset)]

def get_pj1_nt3_company_final_reg_data(dataset, data_length):
    return [{
        "entryid": item["timeentryid2"],
        "shortid": item["timeentryid"] if item["timeentryid2"] else item["timeoffbookingid"],
        "externalsystemidentifier": "REPLICON",
        "perner": item["employeeid"],
        "date": item["entrydate"],
        "projectname": '' if item["wbstype"] == "Opportunity" else (item["iwowbselement"] if item["companycodecode"] == "C1" else item["projectname"]),
        "attendanceabsencetype": "400" if item["companycodecode"] == "C1" else (item["attendancetypecode"]
            if item["attendancetypecode"] else (item["timeofftypedescription"] if item["timeoffbookingid"] else '')),
        "hours": round(float(item["hours"]), 2),
        "comments": item["comments"],
        "iwoexternalsystem": "C1" if item["companycodecode"] == "C1" and item["projecttype"] == "ES" else '',
        "attribute1": '' if item["tasktype"] == "Opportunity" else \
            item["taskname"] if item["tasktype"] == "Attribute 1" else \
            (item["taskfullpath"].split(" / ")[0] if
                (item["tasktype"] == "GSAP Task" and data_length == "4") or \
                (item["tasktype"] == "GSAP Billing Key" and data_length == "3") or \
                (item["tasktype"] == "Attribute 2" and data_length == "2") or \
                ((item["tasktype"] == "PPMC Project & Task") and (data_length in ("2", "3"))) else \
            item["attributecode1"] if item["attributecode1"] else ''),
        "attribute2": '' if item["tasktype"] == "Opportunity" else \
            item["taskname"] if item["tasktype"] == "Attribute 2" else \
            item["attributecode2"] if item["attributecode2"] else \
            (item["taskfullpath"].split(" / ")[1] if
                (item["tasktype"] == "GSAP Task" and data_length == "4") or \
                (item["tasktype"] == "GSAP Billing Key" and data_length == "3") or \
                ((item["tasktype"] == "PPMC Project & Task") and (data_length == "3")) else ''),
        "externalprojecttask": '' if item["tasktype"] == "Opportunity" else
            item["taskname"] if item["tasktype"] and "PPMC" in item["tasktype"] else '',
        "remainingwork": item["newremainningwork"],
        "cfield1": item["customer1"],
        "cfield2": item["customer2"],
        "cfield3": item["customer3"],
        "workorder": '',
        "ratetype": '',
        "tmrole": item["labortype"] if item["labortype"] else '',
        "gsapbillingkey": '' if item["tasktype"] == "Opportunity" else
            item["taskname"] if item["tasktype"] == "GSAP Billing Key" else
            (item["taskfullpath"].split(" / ")[0] if item["tasktype"] == "GSAP Task" and data_length == "2" else
            (item["taskfullpath"].split(" / ")[1] if item["tasktype"] == "GSAP Task" and data_length == "3" else
            (item["taskfullpath"].split(" / ")[2] if item["tasktype"] == "GSAP Task" and data_length == "4" else ''))),
        "gsaptask": '' if item["tasktype"] == "Opportunity" else
            item["taskname"] if item["tasktype"] == "GSAP Task" else '',
        "gsapbillableflag": "X" if item["gsapbillableflag"] and (item["gsapbillableflag"] == "X" or
            item["gsapbillableflag"] == "Billable") else '',
        "bdopportunityid": item["taskname"] if item["tasktype"] == "Opportunity" else ''
    } for item in rail.load_all_records(dataset)]

def get_each_company_final_iwo_data(dataset, data_length, task_type):
    if task_type == "p01_nt2":
        return get_p01_nt2_company_final_iwo_data(dataset, data_length)
    elif task_type == "pn1_nt1":
        return get_pn1_nt1_company_final_iwo_data(dataset, data_length)
    elif task_type == "pj1_nt3":
        return get_pj1_nt3_company_final_iwo_data(dataset, data_length)
    else:
        return null

def get_p01_nt2_company_final_iwo_data(dataset, data_length):
    return [{
        "entryid": item["timeentryid2"],
        "shortid": item["timeentryid"] if item["timeentryid2"] else item["timeoffbookingid"],
        "externalsystemidentifier": "REPLICON",
        "perner": item["employeeid"],
        "date": item["entrydate"],
        "projectname": '' if item["wbstype"] == "Opportunity" else (
            item["parentwbs"] if item["companycodecode"] == "C1" else item["projectname"]
        ),
        "attendanceabsencetype": "400" if item["companycodecode"] == "C1" else (
            item["attendancetypecode"] if item["attendancetypecode"] else (
                item["timeofftypedescription"] if item["timeoffbookingid"] else ''
            )
        ),
        "hours": round(float(item["hours"]), 2),
        "comments": item["comments"],
        "iwoexternalsystem": ("C1" if item["projecttype"] == "ES" else '') if item["companycodecode"] == "C1"
            else ("GSAP" if item["companycodecode"] == "GSAP" and item["projecttype"] == "CP" else ''),
        "attribute1": '' if item["tasktype"] == "Opportunity" else \
            item["taskname"] if item["tasktype"] == "Attribute 1" else \
            (item["taskfullpath"].split(" / ")[0] if
                (item["tasktype"] == "GSAP Task" and data_length == "4") or \
                (item["tasktype"] == "GSAP Billing Key" and data_length == "3") or \
                (item["tasktype"] == "Attribute 2" and data_length == "2") or \
                (item["tasktype"] == "PPMC Project & Task" and data_length in ("2", "3")) else \
            item["attributecode1"] if item["attributecode1"] else ''),
        "attribute2": '' if item["tasktype"] == "Opportunity" else \
            item["taskname"] if item["tasktype"] == "Attribute 2" else \
            item["attributecode2"] if item["attributecode2"] else \
            (item["taskfullpath"].split(" / ")[1] if
                (item["tasktype"] == "GSAP Task" and data_length == "4") or \
                (item["tasktype"] == "GSAP Billing Key" and data_length == "3") or \
                (item["tasktype"] == "PPMC Project & Task" and data_length == "3") else ''),
        "externalprojecttask": '' if item["tasktype"] == "Opportunity" else
            item["taskname"] if item["tasktype"] and "PPMC" in item["tasktype"] else '',
        "remainingwork": item["newremainningwork"],
        "cfield1": item["customer1"],
        "cfield2": item["customer2"],
        "cfield3": item["customer3"],
        "workorder": '',
        "ratetype": '',
        "tmrole": item["labortype"] if item["labortype"] else '',
        "gsapbillingkey": '' if item["tasktype"] == "Opportunity" else
            item["taskname"] if item["tasktype"] == "GSAP Billing Key" else
            (item["taskfullpath"].split(" / ")[0] if item["tasktype"] == "GSAP Task" and data_length == "2" else
            (item["taskfullpath"].split(" / ")[1] if item["tasktype"] == "GSAP Task" and data_length == "3" else
            (item["taskfullpath"].split(" / ")[2] if item["tasktype"] == "GSAP Task" and data_length == "4" else ''))),
        "gsaptask": '' if item["tasktype"] == "Opportunity" else
            item["taskname"] if item["tasktype"] == "GSAP Task" else '',
        "gsapbillableflag": "X" if item["gsapbillableflag"] and (item["gsapbillableflag"] == "X" or
            item["gsapbillableflag"] == "Billable") else '',
        "bdopportunityid": item["taskname"] if item["tasktype"] == "Opportunity" else ''
    } for item in rail.load_all_records(dataset)]


def get_pn1_nt1_company_final_iwo_data(dataset, data_length):
    return [{
        "entryid": item["timeentryid2"],
        "shortid": item["timeentryid"] if item["timeentryid2"] else item["timeoffbookingid"],
        "externalsystemidentifier": "REPLICON",
        "perner": item["employeeid"],
        "date": item["entrydate"],
        "projectname": '' if item["wbstype"] == "Opportunity" else (
            item["parentwbs"] if item["companycodecode"] == "C1" else item["projectname"]
        ),
        "attendanceabsencetype": "400" if item["companycodecode"] == "C1" else (
            item["attendancetypecode"] if item["attendancetypecode"] else (
                item["timeofftypedescription"] if item["timeoffbookingid"] else ''
            )
        ),
        "hours": round(float(item["hours"]), 2),
        "comments": item["comments"],
        "iwoexternalsystem": ("C1" if item["projecttype"] == "ES" else '') if item["companycodecode"] == "C1"
            else ("GSAP" if item["companycodecode"] == "GSAP" and item["projecttype"] == "CP" else ''),
        "attribute1": '' if item["tasktype"] == "Opportunity" else \
            item["taskname"] if item["tasktype"] == "Attribute 1" else \
            (item["taskfullpath"].split(" / ")[0] if
                (item["tasktype"] == "GSAP Task" and data_length == "4") or \
                (item["tasktype"] == "GSAP Billing Key" and data_length == "3") or \
                (item["tasktype"] == "Attribute 2" and data_length == "2") or \
                (item["tasktype"] == "PPMC Project & Task" and data_length in ("2", "3")) else \
            item["attributecode1"] if item["attributecode1"] else ''),
        "attribute2": '' if item["tasktype"] == "Opportunity" else \
            item["taskname"] if item["tasktype"] == "Attribute 2" else \
            item["attributecode2"] if item["attributecode2"] else \
            (item["taskfullpath"].split(" / ")[1] if
                (item["tasktype"] == "GSAP Task" and data_length == "4") or \
                (item["tasktype"] == "GSAP Billing Key" and data_length == "3") or \
                (item["tasktype"] == "PPMC Project & Task" and data_length == "3") else ''),
        "externalprojecttask": '' if item["tasktype"] == "Opportunity" else
            item["taskname"] if item["tasktype"] and "PPMC" in item["tasktype"] else '',
        "remainingwork": item["newremainningwork"],
        "cfield1": item["customer1"],
        "cfield2": item["customer2"],
        "cfield3": item["customer3"],
        "workorder": '',
        "ratetype": '',
        "tmrole": item["labortype"] if item["labortype"] else '',
        "gsapbillingkey": '' if item["tasktype"] == "Opportunity" else
            item["taskname"] if item["tasktype"] == "GSAP Billing Key" else
            (item["taskfullpath"].split(" / ")[0] if item["tasktype"] == "GSAP Task" and data_length == "2" else
            (item["taskfullpath"].split(" / ")[1] if item["tasktype"] == "GSAP Task" and data_length == "3" else
            (item["taskfullpath"].split(" / ")[2] if item["tasktype"] == "GSAP Task" and data_length == "4" else ''))),
        "gsaptask": '' if item["tasktype"] == "Opportunity" else
            item["taskname"] if item["tasktype"] == "GSAP Task" else '',
        "gsapbillableflag": "X" if item["gsapbillableflag"] and (item["gsapbillableflag"] == "X" or
            item["gsapbillableflag"] == "Billable") else '',
        "bdopportunityid": item["taskname"] if item["tasktype"] == "Opportunity" else ''
    } for item in rail.load_all_records(dataset)]

def get_pj1_nt3_company_final_iwo_data(dataset, data_length):
    return [{
        "entryid": item["timeentryid2"],
        "shortid": item["timeentryid"] if item["timeentryid2"] else item["timeoffbookingid"],
        "externalsystemidentifier": "REPLICON",
        "perner": item["employeeid"],
        "date": item["entrydate"],
        "projectname": '' if item["wbstype"] == "Opportunity" else (
            item["parentwbs"] if item["companycodecode"] == "C1" else (
                item["parentwbs"] if item["companycodecode"] == "GSAP" else item["projectname"]
            )
        ),
        "attendanceabsencetype": "400" if item["companycodecode"] == "C1" else (
            "400" if item["companycodecode"] == "GSAP" else (
                item["attendancetypecode"] if item["attendancetypecode"] else (
                    item["timeofftypedescription"] if item["timeoffbookingid"] else ''
                )
            )
        ),
        "hours": round(float(item["hours"]), 2),
        "comments": item["comments"],
        "iwoexternalsystem": ("C1" if item["projecttype"] == "ES" else '') if item["companycodecode"] == "C1"
            else ("GSAP" if item["companycodecode"] == "GSAP" and item["projecttype"] == "CP" else ''),
        "attribute1": '' if item["tasktype"] == "Opportunity" else \
            item["taskname"] if item["tasktype"] == "Attribute 1" else \
            (item["taskfullpath"].split(" / ")[0] if
                (item["tasktype"] == "GSAP Task" and data_length == "4") or \
                (item["tasktype"] == "GSAP Billing Key" and data_length == "3") or \
                (item["tasktype"] == "Attribute 2" and data_length == "2") or \
                (item["tasktype"] == "PPMC Project & Task" and data_length in ("2", "3")) else \
            item["attributecode1"] if item["attributecode1"] else ''),
        "attribute2": '' if item["tasktype"] == "Opportunity" else \
            item["taskname"] if item["tasktype"] == "Attribute 2" else \
            item["attributecode2"] if item["attributecode2"] else \
            (item["taskfullpath"].split(" / ")[1] if
                (item["tasktype"] == "GSAP Task" and data_length == "4") or \
                (item["tasktype"] == "GSAP Billing Key" and data_length == "3") or \
                (item["tasktype"] == "PPMC Project & Task" and data_length == "3") else ''),
        "externalprojecttask": '' if item["tasktype"] == "Opportunity" else
            item["taskname"] if item["tasktype"] and "PPMC" in item["tasktype"] else '',
        "remainingwork": item["newremainningwork"],
        "cfield1": item["customer1"],
        "cfield2": item["customer2"],
        "cfield3": item["customer3"],
        "workorder": '',
        "ratetype": '',
        "tmrole": item["labortype"] if item["labortype"] else '',
        "gsapbillingkey": '' if item["tasktype"] == "Opportunity" else
            item["taskname"] if item["tasktype"] == "GSAP Billing Key" else
            (item["taskfullpath"].split(" / ")[0] if item["tasktype"] == "GSAP Task" and data_length == "2" else
            (item["taskfullpath"].split(" / ")[1] if item["tasktype"] == "GSAP Task" and data_length == "3" else
            (item["taskfullpath"].split(" / ")[2] if item["tasktype"] == "GSAP Task" and data_length == "4" else ''))),
        "gsaptask": '' if item["tasktype"] == "Opportunity" else
            item["taskname"] if item["tasktype"] == "GSAP Task" else '',
        "gsapbillableflag": "X" if item["gsapbillableflag"] and (item["gsapbillableflag"] == "X" or
            item["gsapbillableflag"] == "Billable") else '',
        "bdopportunityid": item["taskname"] if item["tasktype"] == "Opportunity" else ''
    } for item in rail.load_all_records(dataset)]

def get_compass_start_line_data(payroll_identifier):
    return [
        {
            "entryid": '',
            "shortid": get_conf()[payroll_identifier],
            "externalsystemidentifier": '',
            "perner": '',
            "date": '',
            "projectname": '',
            "attendanceabsencetype": '',
            "hours": '',
            "comments": '',
            "iwoexternalsystem": '',
            "attribute1": '',
            "attribute2": '',
            "externalprojecttask": '',
            "remainingwork": '',
            "cfield1": '',
            "cfield2": '',
            "cfield3": '',
            "workorder": '',
            "ratetype": '',
            "tmrole": '',
            "gsapbillingkey": '',
            "gsaptask": '',
            "gsapbillableflag": '',
            "bdopportunityid": ''
        }
    ]

def check_final_data_greater_than_limit(task_type, record_count_limit):
    return (rail.result(f'{task_type}_final_data') and (int(rail.result(f'{task_type}_final_data', key='length')) > record_count_limit))

def get_compass_xml_data(unique_id_attr, division_final_data, region):
    first_line_data = get_compass_start_line_data(unique_id_attr)
    final_export_data = rail.load_all_records(rail.result(division_final_data)) if rail.result(division_final_data) else []
    return {
        'records': [*first_line_data, *final_export_data],
        'region': [{'name': region}]
    }
