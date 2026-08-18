import re
import base64
import logging
import requests
from io import BytesIO
import rail
from airflow.exceptions import AirflowException, AirflowFailException
from rail.hooks.vantagepoint_hook import VantagepointHook


def _get_report_filename():
    raw = rail.get_current_context()['dag_run'].conf['webhook']['data']['CustCSRReportName']
    return raw if raw.lower().endswith('.pdf') else f'{raw}.pdf'


_STATUS_LABELS = {0: 'Waiting', 1: 'Running', 2: 'Failed', 3: 'Hold', 4: 'Completed'}


def fetch_process_id(process_data):
    record = process_data[0] if isinstance(process_data, list) else process_data
    return record["ProcessID"]


def poll_report_completion(vp_conn_id):
    process_id = rail.result('get_process_id')
    hook = VantagepointHook(vp_conn_id=vp_conn_id)
    with hook.get_conn() as session:
        url = f'{session.hostname}/api/processDetailJob/{process_id}'
        headers = {
            'Authorization': f'Bearer {session.access_token}',
            'Accept': 'application/json',
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

    record = data[0] if isinstance(data, list) else data
    status = record['ProcessStatus']
    termination_msg = record.get('TerminationMessage', '')
    logging.info(f'ProcessID={process_id} Status={status} ({_STATUS_LABELS.get(status, "Unknown")})')

    if status == 4:
        match = re.search(r'FileID:\s*([a-f0-9-]+)', termination_msg, re.IGNORECASE)
        if not match:
            raise AirflowFailException(f'Could not extract FileID from TerminationMessage: {termination_msg}')
        file_id = match.group(1).strip()
        logging.info(f'Report completed. FileID={file_id}')
        return file_id

    if status == 2:
        raise AirflowFailException(f'Report generation failed: {termination_msg}')

    raise AirflowException(f'Status is {_STATUS_LABELS.get(status, status)} ({status}), retrying')


def submit_report_job(report_path, queue_id):
    webhook_data = rail.get_current_context()['dag_run'].conf['webhook']['data']
    wbs1 = webhook_data['WBS1']
    return {
        'QueueID': queue_id,
        'Description': 'Conflict Search Report',
        'FW_DETAILJOBS': [
            {
                'QueueID': queue_id,
                'Description': 'Conflict Search Report',
                'xmlParams': {
                    'ScheduleReportArgs': {
                        'ReportPath': report_path,
                        'ReportOptions': {
                            "baseAlternateRowColor": "",
                            "baseBottomMargin": 0.5,
                            "baseCulture": "default",
                            "baseDefaultCurrencyFormat": "###T###T###D##;(###T###T###D##);#",
                            "baseDefaultDateFormat": "M/d/yyyy",
                            "baseDefaultHTMLFormatting": "Y",
                            "baseDefaultNumberFormat": "###T###T###D##;-###T###T###D##;#",
                            "baseFont": "Arial",
                            "baseFooterText": "[version] - [options]",
                            "baseGridTable": "",
                            "baseGroupIndent": 0,
                            "baseHeadingEndDate": "",
                            "baseHeadingRowColor": "",
                            "baseHeadingStartDate": "",
                            "baseHideDocumentMap": "Y",
                            "baseHideSingleLineTotals": "N",
                            "baseLeftMargin": 0.5,
                            "defaultPage2Top": 0,
                            "baseOrientation": "automatic",
                            "baseOverrideHeadingDate": "N",
                            "basePageHeight": 11,
                            "basePageSize": "letter",
                            "basePageWidth": 8.5,
                            "baseReportName": "Conflict Search Report",
                            "baseRightMargin": 0.5,
                            "baseShowBorderLines": "N",
                            "baseShowFinalTotals": "N",
                            "baseShowTotalsOnHeader": "N",
                            "baseStartColumnPosition": 0,
                            "baseTopMargin": 0.5,
                            "baseUnitOfMeasure": "in",
                            "baseUseDashpartLayout": "N",
                            "baseUseLookupFilterToGrid": "N",
                            "ReportGroups": [],
                            "ReportColumns": [],
                            "ReportSections": [],
                            "baseCreateActivity": "N",
                            "baseRecordSelection": "",
                            "custWBS1": wbs1,
                        },
                        'SaveToFileStream': 'Y',
                        'FileStreamFileType': 'PDF',
                    }
                }
            }
        ]
    }

def download_pdf(vp_conn_id, file_id):
    hook = VantagepointHook(vp_conn_id=vp_conn_id)
    with hook.get_conn() as session:
        url = f'{session.hostname}/api/Reporting/Files/{file_id}'
        headers = {
            'Authorization': f'Bearer {session.access_token}',
            'Accept': 'application/octet-stream',
        }
        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status()
        pdf_bytes = response.content

    if not pdf_bytes:
        raise RuntimeError(f'PDF download returned empty content for FileID: {file_id}')

    with rail.new_artifact() as artifact:
        artifact.file.write(pdf_bytes)
        artifact.file.flush()
        return artifact.name

def upload_pdf_to_project(vp_conn_id):
    file_name = _get_report_filename()
    artifact_name = rail.result('download_pdf')

    hook = VantagepointHook(vp_conn_id=vp_conn_id)
    with hook.get_conn() as session, rail.existing_artifact(artifact_name) as artifact:
        url = f'{session.hostname}/api/project/fw_files'
        headers = {
            'Authorization': f'Bearer {session.access_token}',
            'Accept': 'application/json',
        }
        with open(artifact.local_filename, 'rb') as pdf_file:
            response = requests.post(
                url,
                files={'file': (file_name, pdf_file, 'application/pdf')},
                headers=headers,
            )
        if not response.ok:
            logging.error(
                f'fw_files upload failed: {response.status_code} {response.reason} '
                f'url={url} body={response.text[:2000]}'
            )
            response.raise_for_status()
        result = response.json()

    record = result[0] if isinstance(result, list) else result
    file_id = record.get('FileID') or record.get('fileID')
    if not file_id:
        raise RuntimeError(f'No FileID in fw_files upload response: {result}')

    return file_id

# def extract_cust_csr_employee_triggered(response):
#     if response:
#         return response[0].get('CustCSREmployeeTriggered')
#     return None

def attach_file_to_project_payload(file_id):
    webhook_data = rail.get_current_context()['dag_run'].conf['webhook']['data']
    wbs1 = webhook_data['WBS1']
    cust_csr_employee = webhook_data['CustCSREmployee']
    file_name = _get_report_filename()
    #cust_csr_employee_triggered_data = extract_cust_csr_employee_triggered(project_response)
    return {
        'CustCSRWarningMessage': '<span style=\'color:#2ecc71\'><strong>Conflict Search Report Published. Please switch to the Files & Links tab to see report.</strong></span>',
        'CustCSREmployee': cust_csr_employee,
        'FW_ATTACHMENTS': [
            {
                'FileID': file_id,
                'Key1': wbs1,
                'Key2': '',
                'Key3': '',
                'CategoryCode': '',
                'FileDescription': file_name,
                '_transType': 'I',
            }
        ]
    }