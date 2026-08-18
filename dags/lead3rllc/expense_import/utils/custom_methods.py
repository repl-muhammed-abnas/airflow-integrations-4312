import rail

MANDATORY_FIELDS_EXPENSE_REPORT = {
    "Concur_Username": "Concur Username",
    "Transaction_Date": "Transaction Date",
    "Project": "Project",
    "Expense_Type": "Expense Type",
    "Amount" : "Amount"
}

MANDATORY_FIELDS_EXPENSE_INVOICE = {
    "Vendor_Name": "Vendor Name",
    "Date_Incurred": "Date Incurred",
    "Project": "Project",
    "Expense_Type": "Expense Type",
    "Amount" : "Amount"
}


def get_missing_field_message_report(item):
    missing_fields = []
    for key, value in MANDATORY_FIELDS_EXPENSE_REPORT.items():
        if not item[key]:
            missing_fields.append(f"{value} not present in the record")
    return rail.smartjoin_by_delim(missing_fields, ";")


def get_missing_field_message_invoice(item):
    missing_fields = []
    for key, value in MANDATORY_FIELDS_EXPENSE_INVOICE.items():
        if not item[key]:
            missing_fields.append(f"{value} not present in the record")
    return rail.smartjoin_by_delim(missing_fields, ";")


def do_format_logs(dag_run):

    log_records = rail.load_all_records(dag_run.conf['expense_import_log'])

    final_log_records = list(map(lambda log: {
        **{
            'jobid': log['ecid']
        },
        **log['properties'],
    }, log_records))

    rail.set_result(key="error_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Error', final_log_records))))
    rail.set_result(key="success_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Success', final_log_records))))
    rail.set_result(key="exception_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Exception', final_log_records))))

    return final_log_records
