import rail


def row_data_for_input_file(item):
    return [
        item['Deal Name'] if item['Deal Name'] != "(No value)" else '',
        item['Deal ID'] if item['Deal ID'] != "(No value)" else '',
        item['Engagement Lead '] if item['Engagement Lead '] != "(No value)" else '',
        item['Deal Type'] if item['Deal Type'] != "(No value)" else '',
        item['NetSuite Project Type'] if item[
            'NetSuite Project Type'] != "(No value)" else '',
        item['Company name'] if item['Company name'] != "(No value)" else '',
        item['Amount in company currency'] if item[
            'Amount in company currency'] != "(No value)" else '',
        item['Contract Start Date (SOW)'] if item['Contract Start Date (SOW)'] != "(No value)" else '',
        item['Contract End Date'] if item[
            'Contract End Date'] != "(No value)" else '',
    ]


MANDATORY_FIELDS = {
    "deal_name": "Deal Name",
    "deal_id": "Deal ID"
}


def get_missing_field_message(item):
    missing_fields = []
    for key, value in MANDATORY_FIELDS.items():
        if not item[key]:
            missing_fields.append(f"{value} not present in the input")
    return rail.smartjoin_by_delim(missing_fields, ";")


def do_format_logs(dag_run):

    log_records = rail.load_all_records(dag_run.conf['project_import_log'])

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
    rail.set_result(key="total_record_count",
                    val=dag_run.conf['input_file_records'])

    return final_log_records
