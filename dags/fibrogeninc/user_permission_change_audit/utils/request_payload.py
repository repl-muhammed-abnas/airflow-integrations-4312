import rail

def get_report_generate_batch_payload():
    return {
        "reportParameters": [
            {
                "reportUri": rail.result("get_report_details")["uri"],
                "filterValues": "",
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }

def get_process_changed_records_payload(item):
    return {
        "useruri": item["UserUri"],
        "permission_name": item["Permission_Name"],
        "change_date": rail.result("get_logging_details")["changed_date_value"],
        "description_custom_field_uri": " ".join(list(map(lambda data: data["uri"],
                                        filter(lambda data: data['displayText'] == "Description",
                                               rail.result("get_all_custom_fields"))))),
        "date_account_last_changed_customfield_uri": " ".join(list(map(lambda data: data["uri"],
                                        filter(lambda data: data['displayText'] == "Date Account Last Changed",
                                               rail.result("get_all_custom_fields"))))),
        "date_account_last_changed": item["Date_Account_Last_Changed"],
        "description": item["Description"],
        "username": item["User_Name"]
    }

def get_update_text_value_account_changed_payload(dag_run):
    return {
        "objectUri": dag_run.conf["useruri"],
        "customFieldUri": dag_run.conf["date_account_last_changed_customfield_uri"],
        "value": dag_run.conf["change_date"]
    }

def get_update_text_value_description_payload(dag_run, value):
    return {
        "objectUri": dag_run.conf["useruri"],
        "customFieldUri": dag_run.conf["description_custom_field_uri"],
        "value": value
    }
