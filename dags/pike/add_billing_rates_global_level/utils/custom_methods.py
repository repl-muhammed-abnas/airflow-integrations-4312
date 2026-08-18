import rail
from rail.lib.artifact import existing_artifact

null = None

def get_process_billing_rate_payload(item):
    return {
        "billing_rate_uri": rail.find_first_by_attr_and_get_attr(rail.result("get_all_billing_rates"), "displayText", item["Billingratename"], "uri"),
        "item": item
    }

def do_filter_log(log):
    return log['properties']['billing_rate_name'] != '' and log['properties']['billing_rate_name'] != null

def get_txt_file_data_callable():
    with existing_artifact(rail.result('download_email_file'), mode= 'r') as artifact:
        data = artifact.file.read()
        return data
