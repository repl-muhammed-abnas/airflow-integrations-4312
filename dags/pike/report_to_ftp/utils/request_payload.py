import rail
null = None

def get_report_generate_batch_payload():
    return {
        "reportParameters": [
            {
                "filterValues": [],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv",
                "reportUri": rail.result('get_report_details')['uri']
            }
        ]
    }

def read_collection():
    with rail.lib.readers.get_data_reader(rail.result('create_report_collection')) as reader:
        data_collection = list(reader)
    return data_collection

def get_index(item):
    return (rail.result("labor_data")).index(item)

def get_csv_rows(item):
    return [
        item['week_end_date'],
        item['user_name'],
        item['billing_rate_name'],
        item['project_name'],
        item['project_code'],
        item['task_code'],
        item['activity_name'],
        item['hours'],
        item['comments'],
        item['user_supervisor_name_current'],
        get_index(item)
    ]
