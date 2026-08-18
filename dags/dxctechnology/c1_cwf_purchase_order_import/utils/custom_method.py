from datetime import datetime
import hashlib
import rail

keys_to_fetch = ('WorkOrderNumber', 'PersonnelNumber', 'FirstName', 'LastName', 'CompanyCode', 'PurchaseOrder', 'POItem', 'Item_StartDate',
                 'Item_EndDate', 'RegularTimeBalance', 'OvertimeBalance', 'DoubleTimeBalance')


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def get_create_md5_data(item):
    if not item:
        return []
    return {
        **{k.lower(): v if v else '' for k, v in item.items()},
        **{"md5": hashlib.md5((str(item['WorkOrderNumber']) + "," + str(item['PersonnelNumber']) + "," +
                               str(item['FirstName']) + "," + str(item['LastName']) + "," +
                               str(item['CompanyCode']) + "," + str(item['PurchaseOrder']) + "," +
                               str(item['POItem']) + "," + str(item['Item_StartDate']) + "," +
                               str(item['Item_EndDate']) + "," + str(item['RegularTimeBalance']) + "," +
                               str(item['OvertimeBalance']) + "," +
                               str(item['DoubleTimeBalance'])
                               ).encode('utf-8')).hexdigest(),
            "id": hashlib.md5((str(item['WorkOrderNumber']) + "_" + str(item['PersonnelNumber']) + "_" +
                              str(item['PurchaseOrder']) +
                               "_" + str(item['POItem'])
                               ).encode('utf-8')).hexdigest()
           }
    }


def get_merged_data(item):
    if not item:
        return []
    report_data = get_data_from_document(
        rail.result('create_report_collection'))
    return {
        **{k.lower(): v if v else '' for k, v in item.items()},
        **{"employee_id": rail.find_first_by_attr_and_get_attr(report_data, "cwf_c1_alternate_id", item['personnelnumber'], "employee_id"),
           "login_name": rail.find_first_by_attr_and_get_attr(report_data, "cwf_c1_alternate_id", item['personnelnumber'], "login_name"),
           "effective_date": datetime.now().strftime("%m/%d/%Y"),
           "user_count": len([x['user_uri'] for x in report_data if x['cwf_c1_alternate_id'] == item['personnelnumber']]),
           "user_uri": rail.find_first_by_attr_and_get_attr(report_data, "cwf_c1_alternate_id", item['personnelnumber'], "user_uri"),
           "md5": item['md5'],
           "id": item['id']
           }
    }


def get_create_existing_blob_md5(item):
    if not item:
        return []
    res = {
        "workordernumber": item['workOrderNumber'],
        "personnelnumber": item['personnelNumber'],
        "employee_id": item['employeeId'],
        "firstname": item['firstName'],
        "lastname": item['lastName'],
        "companycode": item['companyCode'],
        "purchaseorder": item['purchaseOrder'],
        "poitem": item['poItem'],
        "item_startdate": item['itemStartDate'],
        "item_enddate": item['itemEndDate'],
        "regulartimebalance": item['regularTimeBalance'],
        "overtimebalance": item['overtimeBalance'],
        "doubletimebalance": item['doubleTimeBalance'],
        "effective_date": item["effectiveDateOfBalance"],
        "login_name": item['loginName'],
        "md5": hashlib.md5((str(item['workOrderNumber']) + "," + str(item['personnelNumber']) + "," +
                            str(item['firstName']) + "," + str(item['lastName']) + "," +
                            str(item['companyCode']) + "," + str(item['purchaseOrder']) + "," +
                            str(item['poItem']) + "," + str(item['itemStartDate']) + "," +
                            str(item['itemEndDate']) + "," + str(item['regularTimeBalance']) + "," +
                            str(item['overtimeBalance']) + "," + str(item['doubleTimeBalance']) + "," +
                            str(item["effectiveDateOfBalance"])
                            ).encode('utf-8')).hexdigest(),
        "id": hashlib.md5((str(item['workOrderNumber']) + "_" + str(item['personnelNumber']) + "_" +
                           str(item['purchaseOrder']) +
                           "_" + str(item['poItem'])
                           ).encode('utf-8')).hexdigest()
    }
    return {k: v if v is not None else '' for k, v in res.items()}
