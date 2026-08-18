import json
from datetime import datetime
from functools import lru_cache
from hashlib import md5
from dateutil.relativedelta import relativedelta
import rail

null = None


def check_c1_or_compass_or_gsap():
    file_name = rail.result("new_file_sensor").split("/")[-1]

    if file_name.startswith("COMPASS_WORK_ORDER") or\
            file_name.startswith("C1_WORK_ORDER") or\
            file_name.startswith("GSAP_WORK_ORDER"):
        return file_name.split("_")[0].lower()
    return False

def get_conf_uris():
    return{
            "Timesheeettemplateuri":
                list(filter(lambda i:i["name"] == "C1 Agency Contractor",rail.result("get_all_policy_sets")))[0]["uri"],
            "WorkOrderID_Customfielduri":
                list(filter(lambda i:i["displayText"] == "Work Order ID",rail.result("get_all_custom_fields")))[0]["uri"],
            "CWF_C1_alternateID_customfielduri":
            list(filter(lambda i:i["displayText"] == "CWF C1 alternate ID",rail.result("get_all_custom_fields")))[0]["uri"]
        }

@lru_cache(maxsize=128)
def get_c1_user_report_data():
    return rail.load_all_records(rail.result("query_all_report_data"))

def get_c1_merged_data(item):
    if not item:
        return []
    user_records = get_c1_user_report_data()
    return {
        **get_conf_uris(),
        "workorderid": item["WorkOrderID"],
        "contingentworkerid": item["ContingentWorkerID"],
        "workorderstartdate": item["WorkOrderStartDate"],
        "workorderenddate": item["WorkOrderEndDate"],
        "workorderstatus": item["WorkOrderStatus"],
        "workerfirstname": item["WorkerFirstName"],
        "workerlastname": item["WorkerLastName"],
        "costcentercode": item["CostCenterCode"],
        "costcentername": item["CostCenterName"],
        "WO_GHR_Personnel_Number": item["WO_GHRPersonnelNumber"],
        "Finance_System": item["FinanceSystem"],
        "useruri": rail.find_first_by_attr_and_get_attr(
            user_records,
            "employeeid",
            item["ContingentWorkerID"],
            "useruri"
        ),
        "timesheettemplate": rail.find_first_by_attr_and_get_attr(
            user_records,
            "employeeid",
            item["ContingentWorkerID"],
            "timesheettemplate"
        ),
        "timesheetapprovalpath": rail.find_first_by_attr_and_get_attr(
            user_records,
            "employeeid",
            item["ContingentWorkerID"],
            "timesheetapprovalpath"
        ),
        "workweek": rail.find_first_by_attr_and_get_attr(
            user_records,
            "employeeid",
            item["ContingentWorkerID"],
            "workweek"
        ),
        "actual_costcenter_value": item["CostCenterName"].split(":")[0],
        "userstatus": rail.find_first_by_attr_and_get_attr(
            user_records,
            "employeeid",
            item["ContingentWorkerID"],
            "status"
        ),
        "employeetype": rail.find_first_by_attr_and_get_attr(
            user_records,
            "employeeid",
            item["ContingentWorkerID"],
            "employeetype"
        ),
        "usercount": len(list(filter(lambda i: i["employeeid"] == item["ContingentWorkerID"],
                                     user_records))),
        "C1PurchaseOrder_value": rail.find_first_by_attr_and_get_attr(
            user_records,
            "employeeid",
            item["ContingentWorkerID"],
            "c1purchaseorder"
        ),
        "WorkOrderID_value": rail.find_first_by_attr_and_get_attr(
            user_records,
            "employeeid",
            item["ContingentWorkerID"],
            "workorderid"
        ),
        "CWFC1alternateID_value": rail.find_first_by_attr_and_get_attr(
            user_records,
            "employeeid",
            item["ContingentWorkerID"],
            "cwfc1alternateid"
        ),
        "timesheetperiod_value": rail.find_first_by_attr_and_get_attr(
            user_records,
            "employeeid",
            item["ContingentWorkerID"],
            "timesheetperiod"
        ),
        "cwf_agency_wbs_customfielduri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_custom_fields"),
            "displayText",
            "CWF Agency WBS",
            "uri"
        ),
    }


def replicon_effective_date():
    effective_date = datetime.today() + relativedelta(days=-
                                                      ({0: 2, 1: 3, 2: 4, 3: 5, 4: 6, 5: 0, 6: 1}[datetime.today().weekday()]))
    return {
        "year": effective_date.year,
        "month": effective_date.month,
        "day": effective_date.day
    }


def replicon_effective_date_compass():
    effective_date = datetime.today() + relativedelta(days=-datetime.today().weekday())
    return {
        "year": effective_date.year,
        "month": effective_date.month,
        "day": effective_date.day
    }

@lru_cache(maxsize=128)
def get_compass_report_data():
    return rail.load_all_records(rail.result("query_compass_all_report_data"))

def get_compass_merged_data(item):
    if not item:
        return []
    user_records_compass = get_compass_report_data()
    return {
        "WorkOrderID": item["WorkOrderID"],
        "RevisionNumber": item["RevisionNumber"],
        "ContingentWorkerID": item["ContingentWorkerID"],
        "WorkOrderStartDate": item["WorkOrderStartDate"],
        "WorkOrderEndDate": item["WorkOrderEndDate"],
        "WorkOrderStatus": item["WorkOrderStatus"],
        "WorkerFirstName": item["WorkerFirstName"],
        "WorkerLastName": item["WorkerLastName"],
        "CostCenterCode": item["CostCenterCode"],
        "BillRateCategory": item["BillRateCategory"],
        "BillRate": item["BillRate"],
        "RateUnit": item["RateUnit"],
        "SiteCountry_usewithWorkerbasedreport": item["SiteCountry_usewithWorkerbasedreport"],
        "TaskCode": item["TaskCode"],
        "WO_CATW": item["WO_CATW"],
        "WO_WorkerType": item["WO_WorkerType"],
        "FinanceSystem": item["FinanceSystem"],
        "RemainingSpend": item["RemainingSpend"],
        "cc_CompanyCode": item["cc_CompanyCode"],
        "useruri": rail.find_first_by_attr_and_get_attr(
            user_records_compass,
            "employeeid",
            item["ContingentWorkerID"],
            "useruri"
        ),
        "timesheettemplate": rail.find_first_by_attr_and_get_attr(
            user_records_compass,
            "employeeid",
            item["ContingentWorkerID"],
            "timesheettemplate"
        ),
        "timesheetapprovalpath": rail.find_first_by_attr_and_get_attr(
            user_records_compass,
            "employeeid",
            item["ContingentWorkerID"],
            "timesheetapprovalpath"
        ),
        "workweek": rail.find_first_by_attr_and_get_attr(
            user_records_compass,
            "employeeid",
            item["ContingentWorkerID"],
            "workweek"
        ),
        "Timesheeettemplateuri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_compass_policy_sets"),
            "displayText",
            {"ES": "COMPASS Agency Contractor", "C1": "C1 Agency Contractor"}[
                item["FinanceSystem"]],
            "uri"
        ),
        "WorkOrderID_Customfielduri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_compass_custom_fields"),
            "displayText",
            "Work Order ID",
            "uri"
        ),
        "CWF_C1_alternateID_customfielduri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_compass_custom_fields"),
            "displayText",
            "CWF C1 alternate ID",
            "uri"
        ),
        "actual_costcenter_value": item["cc_CompanyCode"],
        "userstatus": rail.find_first_by_attr_and_get_attr(
            user_records_compass,
            "employeeid",
            item["ContingentWorkerID"],
            "status"
        ),
        "employeetype": rail.find_first_by_attr_and_get_attr(
            user_records_compass,
            "employeeid",
            item["ContingentWorkerID"],
            "employeetype"
        ),
        "usercount": len(list(
            filter(lambda i:i["employeeid"] == item["ContingentWorkerID"],
            user_records_compass)
            )),
        "loginname": rail.find_first_by_attr_and_get_attr(
            user_records_compass,
            "employeeid",
            item["ContingentWorkerID"],
            "loginname"
        ),
        "timesheettemplatetoassign": {"ES": "COMPASS Agency Contractor", "C1": "C1 Agency Contractor"}[item["FinanceSystem"]],
        "workweektoassign": {"ES": "Monday to Sunday", "C1": "Saturday to Friday"}[item["FinanceSystem"]],
        "timesheetperiod": rail.find_first_by_attr_and_get_attr(
            user_records_compass,
            "employeeid",
            item["ContingentWorkerID"],
            "timesheetperiod"
        ),
        "workorderid_assigned": rail.find_first_by_attr_and_get_attr(
            user_records_compass,
            "employeeid",
            item["ContingentWorkerID"],
            "workorderid"
        ),
        "WorkOrderID_projectoef_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_object_extension_fields"),
            "name",
            'Work Order ID',
            "uri"
        ),
        "remainingspend_projectoef_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_object_extension_fields"),
            "name",
            "Remaining Spend",
            "uri"
        ),
        "cwf_agency_wbs_customfielduri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_compass_custom_fields"),
            "displayText",
            "CWF Agency WBS",
            "uri"
        ),
    }


def get_compass_activity_list(config):
    es_actvity_list = list(
        filter(lambda x: x["type"] == "Activity" and
               x["function"] == "CWF User Integration" and
               x["workertype"] == "Agency Contractor" and
               x["financesystem"] == "ES", config.dxc_cwf_mapper)
    )

    return list(map(lambda i: {
        "uri": null,
        "name": i["value"]
    }, es_actvity_list))


def get_pseudo_contract_conf(item):
    if not item:
        return []
    user_records_compass = get_compass_report_data()
    return {
        "WorkOrderID": item["WorkOrderID"],
        "RevisionNumber": item["RevisionNumber"],
        "ContingentWorkerID": item["ContingentWorkerID"],
        "WorkOrderStartDate": item["WorkOrderStartDate"],
        "WorkOrderEndDate": item["WorkOrderEndDate"],
        "WorkOrderStatus": item["WorkOrderStatus"],
        "WorkerFirstName": item["WorkerFirstName"],
        "WorkerLastName": item["WorkerLastName"],
        "CostCenterCode": item["CostCenterCode"],
        "BillRateCategory": item["BillRateCategory"],
        "BillRate": item["BillRate"],
        "RateUnit": item["RateUnit"],
        "SiteCountry_usewithWorkerbasedreport": item["SiteCountry_usewithWorkerbasedreport"],
        "TaskCode": item["TaskCode"],
        "WO_CATW": item["WO_CATW"],
        "WO_WorkerType": item["WO_WorkerType"],
        "FinanceSystem": item["FinanceSystem"],
        "RemainingSpend": item["RemainingSpend"],
        "cc_CompanyCode": item["cc_CompanyCode"],
        "useruri": rail.find_first_by_attr_and_get_attr(
            user_records_compass,
            "employeeid",
            item["ContingentWorkerID"],
            "useruri"
        ),
        "timesheettemplate": rail.find_first_by_attr_and_get_attr(
            user_records_compass,
            "employeeid",
            item["ContingentWorkerID"],
            "timesheettemplate"
        ),
        "Timesheeettemplateuri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_compass_policy_sets"),
            "displayText",
            "COMPASS SOW Contractor",
            "uri"
        ),
        "employeetype": rail.find_first_by_attr_and_get_attr(
            user_records_compass,
            "employeeid",
            item["ContingentWorkerID"],
            "employeetype"
        ),
        "loginname": rail.find_first_by_attr_and_get_attr(
            user_records_compass,
            "employeeid",
            item["ContingentWorkerID"],
            "loginname"
        ),
        "timesheettemplatetoassign": "COMPASS SOW Contractor",
        "RateunitcustomfieldURI": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_compass_custom_fields"),
            "displayText",
            "Rate Unit",
            "uri"
        ),
        "Rateunitudfcurrentvalue": rail.find_first_by_attr_and_get_attr(
            user_records_compass,
            "employeeid",
            item["ContingentWorkerID"],
            "rateunit"
        ),
        "EmployeetypegroupURI": rail.result("get_all_employee_type_groups"),
        "Timesheetapprovalpath": rail.find_first_by_attr_and_get_attr(
            user_records_compass,
            "employeeid",
            item["ContingentWorkerID"],
            "timesheetapprovalpath"
        ),
        "Timeentryapprovalpath": rail.find_first_by_attr_and_get_attr(
            user_records_compass,
            "employeeid",
            item["ContingentWorkerID"],
            "timeentryapprovalpath"
        ),
        "lookuptable": rail.render_template('{{dag_run.conf.lookuptable}}')
    }

def get_updated_end_date_records(update_records, update_end_date=True):
    return list(map(lambda work_order: {
            "workOrderId": work_order["workOrderId"],
            "revisionNumber": work_order["revisionNumber"],
            "contingentworkerId": work_order["contingentworkerId"],
            "workOrderStartDate": work_order["workOrderStartDate"],
            "workOrderEndDate": get_end_date(work_order) if update_end_date else work_order["workOrderEndDate"],
            "workOrderStatus": work_order["workOrderStatus"],
            "workerFirstName": work_order["workerFirstName"],
            "workerLastName": work_order["workerLastName"],
            "costCenterCode": work_order["costCenterCode"],
            "actualBillRateCategory": work_order["actualBillRateCategory"],
            "billRate": work_order["billRate"],
            "RateUnit": work_order["RateUnit"],
            "siteCountryUseWithWorkerBasedReport": work_order["siteCountryUseWithWorkerBasedReport"],
            "taskCode": work_order["taskCode"],
            "wO_CATW": work_order["wO_CATW"],
            "wO_workerType": work_order["wO_workerType"],
            "financeSystem": work_order["financeSystem"],
            "remainingSpend": work_order["remainingSpend"],
            "ccCompanyCode": work_order["ccCompanyCode"],
            "projectUri": work_order["projectUri"],
            "projectName": work_order["projectName"],
            "userUri": work_order["userUri"],
            "loginName": work_order["loginName"],
            "effectiveDateOfBalance": work_order["effectiveDateOfBalance"],
        },
        update_records))

@lru_cache(maxsize=128)
def get_unique_blob_records():
    return rail.load_all_records(rail.result("query_unique_records_WorkOrderID"))

@lru_cache(maxsize=128)
def get_exisiting_id_blob_records():
    return rail.load_all_records(rail.result("query_new_data_id_in_existing_records"))

@lru_cache(maxsize=128)
def get_new_blob_records():
    return rail.load_all_records(rail.result("query_new_records_blob_WorkOrderID"))


@lru_cache(maxsize=128)
def get_existing_id_in_new_records_blob_records():
    return rail.load_all_records(
                    rail.result("query_existing_id_in_new_records_blob_WorkOrderID"))

def get_json_key_value_blob_compass():
    all_blob_records_for_user = []
    updated_records = []
    new_records = []
    existing_records = []
    if rail.result("query_unique_records_WorkOrderID"):
        existing_records = get_unique_blob_records()
        if existing_records:
            all_blob_records_for_user.extend(existing_records)
            if rail.result("query_new_data_id_in_existing_records"):
                updated_records = get_exisiting_id_blob_records()
                if updated_records:
                    all_blob_records_for_user.extend(get_updated_end_date_records(updated_records))
        else:
            if rail.result("query_existing_id_in_new_records_blob_WorkOrderID"):
                existing_records_with_id_in_new_records = get_existing_id_in_new_records_blob_records()
                if existing_records_with_id_in_new_records:
                    all_blob_records_for_user.extend(get_updated_end_date_records(existing_records_with_id_in_new_records))
    if rail.result("query_new_records_blob_WorkOrderID"):
        new_records = get_new_blob_records()
    if new_records:
        all_blob_records_for_user.extend(get_updated_end_date_records(new_records, False))

    return json.dumps(list(map(lambda work_order: {
            "workOrderId": work_order["workOrderId"],
            "revisionNumber": work_order["revisionNumber"],
            "contingentworkerId": work_order["contingentworkerId"],
            "workOrderStartDate": work_order["workOrderStartDate"],
            "workOrderEndDate": work_order["workOrderEndDate"],
            "workOrderStatus": work_order["workOrderStatus"],
            "workerFirstName": work_order["workerFirstName"],
            "workerLastName": work_order["workerLastName"],
            "costCenterCode": work_order["costCenterCode"],
            "actualBillRateCategory": work_order["actualBillRateCategory"],
            "billRate": work_order["billRate"],
            "RateUnit": work_order["RateUnit"],
            "siteCountryUseWithWorkerBasedReport": work_order["siteCountryUseWithWorkerBasedReport"],
            "taskCode": work_order["taskCode"],
            "wO_CATW": work_order["wO_CATW"],
            "wO_workerType": work_order["wO_workerType"],
            "financeSystem": work_order["financeSystem"],
            "remainingSpend": work_order["remainingSpend"],
            "ccCompanyCode": work_order["ccCompanyCode"],
            "projectUri": work_order["projectUri"],
            "projectName": work_order["projectName"],
            "userUri": work_order["userUri"],
            "loginName": work_order["loginName"],
            "effectiveDateOfBalance": work_order["effectiveDateOfBalance"],
        },
        all_blob_records_for_user)))

@lru_cache(maxsize=128)
def get_valid_user_records_compass():
    return rail.load_all_records(rail.result("query_valid_records_for_user_in_replicon"))

def get_json_new_key_value_blob_compass():
    updated_records = get_valid_user_records_compass()
    _rate = {"DT": "Double Time", "OT": "Overtime", "ST": "Straight Time"}
    return json.dumps(list(map(lambda work_order: {
        "workOrderId": work_order["WorkOrderID"],
        "revisionNumber": work_order["RevisionNumber"],
        "contingentworkerId": work_order["ContingentWorkerID"],
        "workOrderStartDate": work_order["WorkOrderStartDate"],
        "workOrderEndDate": work_order["WorkOrderEndDate"],
        "workOrderStatus": work_order["WorkOrderStatus"],
        "workerFirstName": work_order["WorkerFirstName"],
        "workerLastName": work_order["WorkerLastName"],
        "costCenterCode": work_order["CostCenterCode"],
        "billRateCategory": work_order["BillRateCategory"],
        "billRate": work_order["BillRate"],
        "RateUnit": work_order["RateUnit"],
        "siteCountryUseWithWorkerBasedReport": work_order["SiteCountry_usewithWorkerbasedreport"],
        "taskCode": work_order["TaskCode"],
        "wO_CATW": work_order["WO_CATW"],
        "wO_workerType": work_order["WO_WorkerType"],
        "financeSystem": work_order["FinanceSystem"],
        "remainingSpend": work_order["RemainingSpend"],
        "ccCompanyCode": work_order["cc_CompanyCode"],
        "actualBillRateCategory": _rate.get(work_order["BillRateCategory"], "") ,
        "projectUri": rail.result("get_bulk_project_details"),
        "projectName": work_order["CostCenterCode"],
        "userUri": work_order["useruri"],
        "loginName": work_order["loginname"],
        "effectiveDateOfBalance": work_order["WorkOrderStartDate"]

    },
        updated_records)))

def get_new_key_value_md5_compass(work_order):
    if not work_order:
        return []
    return {
        "workOrderId": work_order["WorkOrderID"],
        "revisionNumber": work_order["RevisionNumber"],
        "contingentworkerId": work_order["ContingentWorkerID"],
        "workOrderStartDate": work_order["WorkOrderStartDate"],
        "workOrderEndDate": work_order["WorkOrderEndDate"],
        "workOrderStatus": work_order["WorkOrderStatus"],
        "workerFirstName": work_order["WorkerFirstName"],
        "workerLastName": work_order["WorkerLastName"],
        "costCenterCode": work_order["CostCenterCode"],
        "billRateCategory": work_order["BillRateCategory"],
        "billRate": work_order["BillRate"],
        "RateUnit": work_order["RateUnit"],
        "siteCountryUseWithWorkerBasedReport": work_order["SiteCountry_usewithWorkerbasedreport"],
        "taskCode": work_order["TaskCode"],
        "wO_CATW": work_order["WO_CATW"],
        "wO_workerType": work_order["WO_WorkerType"],
        "financeSystem": work_order["FinanceSystem"],
        "remainingSpend": work_order["RemainingSpend"],
        "ccCompanyCode": work_order["cc_CompanyCode"],
        "actualBillRateCategory": {"DT": "Double Time", "OT": "Overtime", "ST": "Straight Time"}[work_order["BillRateCategory"]],
        "projectUri":  rail.result("get_bulk_project_details"),
        "projectName": work_order["CostCenterCode"],
        "userUri": work_order["useruri"],
        "loginName": work_order["loginname"],
        "effectiveDateOfBalance": work_order["WorkOrderStartDate"],
        "md5": md5((work_order["WorkOrderID"] + "_" +
                   work_order["RevisionNumber"] + "_" +
                   work_order["ContingentWorkerID"] + "_" +
                   work_order["WorkOrderStartDate"] + "_" +
                   work_order["WorkOrderEndDate"] + "_" +
                   work_order["WorkOrderStatus"] + "_" +
                   work_order["WorkerFirstName"] + "_" +
                   work_order["WorkerLastName"] + "_" +
                   work_order["CostCenterCode"] + "_" +
                   work_order["BillRateCategory"] + "_" +
                   work_order["BillRate"] + "_" +
                   work_order["RateUnit"] + "_" +
                   work_order["SiteCountry_usewithWorkerbasedreport"] + "_" +
                   work_order["TaskCode"] + "_" +
                   work_order["WO_CATW"] + "_" +
                   work_order["WO_WorkerType"] + "_" +
                   work_order["FinanceSystem"] + "_" +
                   work_order["RemainingSpend"] + "_" +
                   work_order["cc_CompanyCode"]).encode()).hexdigest(),
        "uniqueid":  md5((work_order["WorkOrderID"] + "_" +
                         work_order["ContingentWorkerID"] + "_" +
                         work_order["FinanceSystem"]).encode()).hexdigest()
    }

def get_existing_key_value_md5_compass(work_order):
    if not work_order or not work_order.get("billRateCategory"):
        return []

    return {
        "workOrderId": work_order["workOrderId"],
        "revisionNumber": work_order["revisionNumber"],
        "contingentworkerId": work_order["contingentworkerId"],
        "workOrderStartDate": work_order["workOrderStartDate"],
        "workOrderEndDate": work_order["workOrderEndDate"],
        "workOrderStatus": work_order["workOrderStatus"],
        "workerFirstName": work_order["workerFirstName"],
        "workerLastName": work_order["workerLastName"],
        "costCenterCode": work_order["costCenterCode"],
        "billRateCategory": work_order["billRateCategory"],
        "billRate": work_order["billRate"],
        "RateUnit": work_order["RateUnit"],
        "siteCountryUseWithWorkerBasedReport": work_order["siteCountryUseWithWorkerBasedReport"],
        "taskCode": work_order["taskCode"],
        "wO_CATW": work_order["wO_CATW"],
        "wO_workerType": work_order["wO_workerType"],
        "financeSystem": work_order["financeSystem"],
        "remainingSpend": work_order["remainingSpend"],
        "ccCompanyCode": work_order["ccCompanyCode"],
        "actualBillRateCategory": work_order["actualBillRateCategory"],
        "projectUri": work_order["projectUri"],
        "projectName": work_order["projectName"],
        "userUri": work_order["userUri"],
        "loginName": work_order["loginName"],
        "effectiveDateOfBalance": work_order["effectiveDateOfBalance"],
        "md5": md5((work_order["workOrderId"] + "_" +
                   work_order["revisionNumber"] + "_" +
                   work_order["contingentworkerId"] + "_" +
                   work_order["workOrderStartDate"] + "_" +
                   work_order["workOrderEndDate"] + "_" +
                   work_order["workOrderStatus"] + "_" +
                   work_order["workerFirstName"] + "_" +
                   work_order["workerLastName"] + "_" +
                   work_order["costCenterCode"] + "_" +
                   work_order["billRateCategory"] + "_" +
                   work_order["billRate"] +"_"+
                   work_order["RateUnit"] + "_" +
                   work_order["siteCountryUseWithWorkerBasedReport"] + "_" +
                   work_order["taskCode"] + "_" +
                   work_order["wO_CATW"] + "_" +
                   work_order["wO_workerType"] + "_" +
                   work_order["financeSystem"] + "_" +
                   work_order["remainingSpend"] + "_" +
                   work_order["ccCompanyCode"]).encode()).hexdigest(),
        "uniqueid":  md5((work_order["workOrderId"] + "_" +
                         work_order["contingentworkerId"] + "_" +
                         work_order["financeSystem"]).encode()).hexdigest()
    }


def get_end_date(work_order):
    new_record_blob = list(filter(lambda i: i["uniqueid"] == work_order["uniqueid"],
                                  get_new_blob_records()))[0]
    if int(new_record_blob["revisionNumber"]) - int(work_order["revisionNumber"]) == 1:
        if datetime.strptime(work_order["workOrderStartDate"],"%m/%d/%Y") < datetime.strptime(work_order["workOrderEndDate"],"%m/%d/%Y"):
            return work_order["workOrderEndDate"]
        return work_order["workOrderStartDate"]
    if int(new_record_blob["revisionNumber"]) - int(work_order["revisionNumber"]) == 0:
        return work_order["workOrderStartDate"]
    return work_order["workOrderEndDate"]


def check_gsap_workorder_attr(item):
    msg = ""
    msg = ";".join(list(filter(lambda i: item[i] == "" or item[i] is None , item.keys())))
    msg += " value is missing"
    return msg


def check_gsap_workorder_attr_invalid(item):
    msg = ""
    if item["FinanceSystem"] != "GSAP":
        msg = "Finance System is not GSAP"
    elif item["WO_WorkerType"] != "Etes":
        msg = "Worker Type is not Etes"
    else:
        msg = "Bill Rate Categeory is not ST or Daily"
    return msg

@lru_cache(maxsize=128)
def get_valid_user_records_gsap():
    return rail.load_all_records(rail.result("query_valid_records_for_user_in_replicon"))

def get_json_new_key_value_blob_gsap():
    updated_records = get_valid_user_records_gsap()
    return json.dumps(list(map(lambda work_order: {
        "WorkerID": work_order["WorkOrderID"],
        "WorkOrderID": work_order["ContingentWorkerID"],
        "WOStartDate": work_order["WorkOrderStartDate"],
        "WOEndDate": work_order["WorkOrderEndDate"],
        "RateType": work_order["BillRateCategory"],
        "RateUnit": work_order["RateUnit"]
    },
        updated_records)))

@lru_cache(maxsize=128)
def get_gsap_report_data():
    return rail.load_all_records(rail.result("query_gsap_all_report_data"))
def get_gsap_merged_data(item):
    if not item:
        return []
    user_records = get_gsap_report_data()
    return {
        "WorkOrderID": item["WorkOrderID"],
        "ContingentWorkerID": item["ContingentWorkerID"],
        "WorkOrderStartDate": item["WorkOrderStartDate"],
        "WorkOrderEndDate": item["WorkOrderEndDate"],
        "WorkOrderStatus": item["WorkOrderStatus"],
        "WorkerFirstName": item["WorkerFirstName"],
        "WorkerLastName": item["WorkerLastName"],
        "CostCenterCode": item["CostCenterCode"],
        "BillRateCategory": item["BillRateCategory"],
        "BillRate": item["BillRate"],
        "WO_CATW": item["WO_CATW"],
        "WO_WorkerType": item["WO_WorkerType"],
        "FinanceSystem": item["FinanceSystem"],
        "RateUnit": item["RateUnit"],
        "cc_CompanyCode": item["cc_CompanyCode"],
        "useruri": rail.find_first_by_attr_and_get_attr(
            user_records,
            "employeeid",
            item["ContingentWorkerID"],
            "useruri"
        ),
        "timesheettemplate": rail.find_first_by_attr_and_get_attr(
            user_records,
            "employeeid",
            item["ContingentWorkerID"],
            "timesheettemplate"
        ),
        "Timesheeettemplateuri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_gsap_policy_sets"),
            "displayText"
            "GSAP Agency Contractor",
            "uri"
        ),
        "timesheetapprovalpath": rail.find_first_by_attr_and_get_attr(
            user_records,
            "employeeid",
            item["ContingentWorkerID"],
            "timesheetapprovalpath"
        ),
        "workweek": rail.find_first_by_attr_and_get_attr(
            user_records,
            "employeeid",
            item["ContingentWorkerID"],
            "workweek",
        ),
        "WorkOrderID_Customfielduri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_gsap_custom_fields"),
            "displayText",
            "Work Order ID",
            "uri"
        ),
        "CWF_C1_alternateID_customfielduri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_gsap_custom_fields"),
            "displayText",
            "CWF C1 alternate ID",
            "uri"
        ),
        "actual_company_code_value": item["cc_CompanyCode"],
        "userstatus": rail.find_first_by_attr_and_get_attr(
            user_records,
            "employeeid",
            item["ContingentWorkerID"],
            "status"
        ),
        "employeetype": rail.find_first_by_attr_and_get_attr(
            user_records,
            "employeeid",
            item["ContingentWorkerID"],
            "employeetype"
        ),
        "usercount": len(list(
            filter(lambda i:i["employeeid"] == item["ContingentWorkerID"],
            user_records)
            )),
        "loginname": rail.find_first_by_attr_and_get_attr(
            user_records,
            "employeeid",
            item["ContingentWorkerID"],
            "loginname"
        ),
        "timesheettemplatetoassign": "GSAP Agency Contractor",
        "workweektoassign": "Saturday to Friday",
        "timesheetperiod": rail.find_first_by_attr_and_get_attr(
            user_records,
            "employeeid",
            item["ContingentWorkerID"],
            "timesheetperiod"
        ),
        "workorderid_assigned": rail.find_first_by_attr_and_get_attr(
            user_records,
            "employeeid",
            item["ContingentWorkerID"],
            "workorderid"
        ),
        "GHR_personnel_number": item["WO_GHRPersonnelNumber"]
    }


def get_gsap_user_details(response):
    response = response[0]
    custom_fields = list(map(lambda i: {
        "displayText": i["customField"]["displayText"],
        "uri": i["customField"]["uri"],
        "text": i["text"]
    }, response["userDetails"]["customFieldValues"]))
    return {
        "isenabled": response["securityConfiguration"]["isLoginEnabled"],
        "permission_sets": rail.find_first_by_attr_and_get_attr(
            response["permissionSets"],
            "displayText",
            "Contingent Worker/Contractor permission assigned",
            "uri"),
        "pernruri": rail.find_first_by_attr_and_get_attr(
            custom_fields,
            "displayText",
            "PERNER",
            "uri"
        ),
        "perner": rail.find_first_by_attr_and_get_attr(
            custom_fields,
            "displayText",
            "PERNER",
            "text"
        ),
        "workorderiduri": rail.find_first_by_attr_and_get_attr(
            custom_fields,
            "displayText",
            "Work Order ID",
            "uri"
        ),
        "workorderid": rail.find_first_by_attr_and_get_attr(
            custom_fields,
            "displayText",
            "Work Order ID",
            "text"
        )
    }


def get_gsap_activity_list(config):
    es_actvity_list = list(
        filter(lambda x: x["type"] == "Activity" and
               x["function"] == "CWF User Integration" and
               x["workertype"] == "Etes" and
               x["financesystem"] == "GSAP", config.dxc_cwf_mapper)
    )

    return list(map(lambda i: {
        "uri": null,
        "name": i["value"]
    }, es_actvity_list))

@lru_cache(maxsize=128)
def get_unique_blob_records_gsap():
    return rail.load_all_records(rail.result("query_unique_records"))

@lru_cache(maxsize=128)
def get_existing_id_blob_records_gsap():
    return rail.load_all_records(rail.result("query_existing_id_in_new_records_blob"))

@lru_cache(maxsize=128)
def get_new_blob_records_gsap():
    return rail.load_all_records(rail.result("query_new_records_blob"))

def get_json_key_value_blob_gsap():
    all_blob_records_for_user = []
    updated_records = []
    new_records = []
    existing_records = []
    if rail.result("query_unique_records"):
        existing_records = get_unique_blob_records_gsap()
        if existing_records:
            all_blob_records_for_user.extend(existing_records)
    if rail.result("query_existing_records_id_new_data"):
        updated_records = get_existing_id_blob_records_gsap()
        if updated_records:
            all_blob_records_for_user.extend(updated_records)
    if rail.result("query_new_records_blob"):
        new_records = get_new_blob_records_gsap()
        if new_records:
            all_blob_records_for_user.extend(new_records)
    return json.dumps(list(map(lambda work_order: {
        "WorkerID": work_order["WorkerID"],
        "WorkOrderID": work_order["WorkOrderID"],
        "WOStartDate": work_order["WOStartDate"],
        "WOEndDate": work_order["WOEndDate"],
        "RateType": work_order["RateType"],
        "RateUnit": work_order["RateUnit"]
    }, all_blob_records_for_user)))


def get_new_key_value_md5_gsap(work_order):
    if not work_order:
        return []
    return {
        "WorkerID": work_order["ContingentWorkerID"],
        "WorkOrderID": work_order["WorkOrderID"],
        "WOStartDate": work_order["WorkOrderStartDate"],
        "WOEndDate": work_order["WorkOrderEndDate"],
        "RateType": work_order["BillRateCategory"],
        "RateUnit": work_order["RateUnit"],
        "md5": md5((work_order["ContingentWorkerID"]+"_"+work_order["WorkOrderID"]+"_" +
                   work_order["WorkOrderStartDate"]+"_"+work_order["WorkOrderEndDate"]+"_" +
                   work_order["RateUnit"]).encode()).hexdigest(),
        "uniqueid": md5((work_order["ContingentWorkerID"]+"_"+work_order["WorkOrderID"]).encode()).hexdigest()
    }

def get_existing_key_value_md5_gsap(work_order):
    if not work_order:
        return []
    return {
        "WorkerID": work_order["WorkerID"],
        "WorkOrderID": work_order["WorkOrderID"],
        "WOStartDate": work_order["WOStartDate"],
        "WOEndDate": work_order["WOEndDate"],
        "RateType": work_order["RateType"],
        "RateUnit": work_order["RateUnit"],
        "md5": md5((work_order["WorkerID"]+"_"+work_order["WorkOrderID"]+"_" +
                   work_order["WOStartDate"]+"_"+work_order["WOEndDate"]+"_" +
                   work_order["RateUnit"]).encode()).hexdigest(),
        "uniqueid": md5((work_order["WorkerID"]+"_"+work_order["WorkOrderID"]).encode()).hexdigest()
    }
