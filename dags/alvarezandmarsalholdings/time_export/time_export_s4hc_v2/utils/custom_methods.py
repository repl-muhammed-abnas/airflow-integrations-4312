import rail



EXPORT_FILE_TIMESTAMP = "%Y-%m-%dT%H:%M:%S"

null = None


def get_timeexport_fileformat(config, response):
    file_format = rail.find_first_by_attr_and_get_attr(
        response, 'displayText', config.time_export_file_format, 'uri')
    if file_format:
        return file_format
    raise Exception(
        f'Unable to locate script `{config.time_export_file_format}`')

def create_s4hc_json_payload_callable(task_id):
    res = {
        "TimeSheetDataFields":[
            {
                "PersonWorkAgreement": item['PersonWorkAgreement'],
                "CompanyCode": item['CompanyCode'],
                "TimeSheetRecord": item['TimeSheetRecord'],
                "TimeSheetDate": item['TimeSheetDate'],
                "TimeSheetOperation": item['TimeSheetOperation'],
                "ControllingArea": item['ControllingArea'],
                "ReceiverCostCenter": item['ReceiverCostCenter'],
                "ActivityType": item['ActivityType'],
                "WBSElement": item['WBSElement'],
                "WorkItem": item['WorkItem'],
                "BillingControlCategory": item['BillingControlCategory'],
                "TimeSheetNote": item['TimeSheetNote'],
                "RecordedHours": item['RecordedHours'],
                "HoursUnitOfMeasure": item['HoursUnitOfMeasure'],
                "TimeSheetWrkLocCode": item['TimeSheetWrkLocCode'],
                "TimeSheetStatus": item['TimeSheetStatus'],
                "RepliconUniqueNum": item['RepliconUniqueNum']
            } for item in rail.load_all_records(rail.result(task_id))
        ]
    }
    return rail.write_json_artifact(res)

def form_download_parameters(file_script_uri, dag_run):
    return {
        "columnUris": [],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-data-export"
            },
            "operatorUri": "urn:replicon:filter-operator:in",
            "rightExpression": {
                "value": {
                    "uris": [dag_run.conf['time_export_uri']],
                },
            },
        },
        "fileFormatScriptUri": file_script_uri
    }

def get_report_parameters():
    return {
        "reportParameters": [
            {
                "reportUri": rail.result('get_report_details')["uri"],
                "filterValues": [],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }

def check_error():
    send_api_error = rail.result('send_s4hc_data_to_sap_endpoint', "error")

    is_502 = lambda error: error and "502:Bad Gateway" in error.get("exc_message", "")
    is_connection_aborted = lambda error: error and "('Connection aborted.', RemoteDisconnected('Remote end closed connection without response'))" in error.get("exc_message", "")

    return (
        is_502(send_api_error) or is_connection_aborted(send_api_error)
    )
