import itertools
import json
import os
import rail
from dxctechnology.c1_cwf_purchase_order_import.utils import custom_method


def get_process_update_user_udf_conf(item):
    report_data = custom_method.get_data_from_document(
        rail.result('create_report_collection'))
    merge_data = custom_method.get_data_from_document(
        rail.result('get_valid_merged_records'))
    return {
        "file_name": os.path.split(rail.result("new_file_sensor"))[1],
        "login_name": item['login_name'],

        "work_order_number": rail.find_first_by_attr_and_get_attr(merge_data, 'login_name', item['login_name'], "workordernumber"),
        "personnel_number": rail.find_first_by_attr_and_get_attr(merge_data, 'login_name', item['login_name'], "personnelnumber"),
        "company_code": rail.find_first_by_attr_and_get_attr(merge_data, 'login_name', item['login_name'], "companycode"),
        "user_uri": rail.find_first_by_attr_and_get_attr(report_data, "login_name", item['login_name'], "user_uri"),

        "purchase_order_input_file": rail.find_first_by_attr_and_get_attr(merge_data, 'login_name', item['login_name'], "purchaseorder")
        if rail.find_first_by_attr_and_get_attr(merge_data, 'login_name', item['login_name'], "purchaseorder") else "",

        "purchase_order_user_profile": rail.find_first_by_attr_and_get_attr(report_data, "login_name", item['login_name'], "c1_purchase_order")
        if rail.find_first_by_attr_and_get_attr(report_data, "login_name", item['login_name'], "c1_purchase_order") else "",

        "c1_purchase_order_udf_uri": rail.result('get_c1_purchase_order_custom_fields')
    }


def get_json_value_payload():
    data = custom_method.get_data_from_document(
        rail.result('get_purchaseorders'))
    return json.dumps(list(map(lambda item: {
                "workOrderNumber": item["workordernumber"],
                "personnelNumber": item["personnelnumber"],
                "firstName": item["firstname"],
                "lastName": item["lastname"],
                "companyCode": item["companycode"],
                "purchaseOrder": item["purchaseorder"],
                "poItem": item["poitem"],
                "itemStartDate": item["item_startdate"],
                "itemEndDate": item["item_enddate"],
                "regularTimeBalance": item["regulartimebalance"],
                "overtimeBalance": item["overtimebalance"],
                "doubleTimeBalance": item["doubletimebalance"],
                "loginName": item["login_name"],
                "employeeId": item["employee_id"],
                "effectiveDateOfBalance": item["effective_date"],
                }, data)))


def get_updated_json_key_payload():
    json_value = []

    def add_items(list_to_append):
        data = list(map(lambda item: {
            "workOrderNumber": item["workordernumber"],
            "personnelNumber": item["personnelnumber"],
            "firstName": item["firstname"],
            "lastName": item["lastname"],
            "companyCode": item["companycode"],
            "purchaseOrder": item["purchaseorder"],
            "poItem": item["poitem"],
            "itemStartDate": item["item_startdate"],
            "itemEndDate": item["item_enddate"],
            "regularTimeBalance": item["regulartimebalance"],
            "overtimeBalance": item["overtimebalance"],
            "doubleTimeBalance": item["doubletimebalance"],
            "loginName": item["login_name"],
            "employeeId": item["employee_id"],
            "effectiveDateOfBalance": item["effective_date"],
        }, list_to_append))

        json_value.append(data)

    unique_existing_records = custom_method.get_data_from_document(
        rail.result("get_unique_existing_records"))
    if unique_existing_records:
        add_items(unique_existing_records)

    new_blob_records_to_add = custom_method.get_data_from_document(
        rail.result('get_new_blob_records_to_add'))
    if new_blob_records_to_add:
        add_items(new_blob_records_to_add)

    existing_blob_records_to_update = custom_method.get_data_from_document(
        rail.result('get_existing_blob_records_to_update'))
    if existing_blob_records_to_update:
        add_items(existing_blob_records_to_update)

    return list(itertools.chain(*json_value))
