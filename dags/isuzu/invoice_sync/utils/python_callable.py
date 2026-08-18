# pylint: disable=too-many-statements line-too-long singleton-comparison no-else-return inconsistent-return-statements
from datetime import datetime
from io import StringIO
import csv
import rail
import pandas as pd
import numpy as np
from rail.lib.artifact import existing_artifact, is_artifact_name


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def get_dates(dag_run):
    required_date = datetime.strptime(dag_run.conf['Invoice_Date'], "%Y-%m-%d")
    return {
        "day": required_date.day,
        "month": required_date.month,
        "year": required_date.year,
    }


def get_csv_data_headers_mapped(document, headers):
    mapped_list = []
    if is_artifact_name(document):

        with existing_artifact(document, mode="r", encoding='utf-8') as artifact:
            reader = csv.DictReader(
                artifact.file, delimiter=',', fieldnames=headers)
            # validate_csv_data(reader)
            for row in reader:
                mapped_list.append(row)

    return mapped_list


def validate_csv_data(reader):
    for _ in reader:
        # read the whole file, just to make sure it is valid
        pass


def get_processed_requestid_list():
    records = rail.load_all_records(rail.result("parse_csv_10_10_10"))
    df = pd.DataFrame.from_dict(records)
    list_of_unique_records = df['Internal Notes'].unique()
    return list(list_of_unique_records)


def get_already_present_records():
    report_data = rail.result('invoice_report1_generation.get_report_result').get(
        'reportGenerationResults')[0].get('payload')
    requestid_list = rail.result('get_requestid_list')
    csvStringIO = StringIO(report_data)
    df = pd.read_csv(csvStringIO, sep=",")
    update_df = df.replace(np.nan, "")
    already_present_df = update_df[update_df['Internal Notes'].isin(
        requestid_list) == True]
    return already_present_df.to_dict(orient='records')


def failure_reason(**kwargs):
    for task_instance in kwargs['dag_run'].get_task_instances():
        if task_instance.current_state() == "failed" and \
                task_instance.task_id != kwargs['task_instance'].task_id:
            return f"Task {task_instance.task_id} failure is the reason behind failed Dag run"


def get_invoice_details_tolog():
    if rail.result('get_invoice_details_v2_54_54_18'):
        return rail.result('get_invoice_details_v2_54_54_18')
    elif rail.result('get_invoice_details_v2_54_54_29'):
        return rail.result('get_invoice_details_v2_54_54_29')
    elif rail.result('get_invoice_details_v2_54_54_42'):
        return rail.result('get_invoice_details_v2_54_54_42')
    elif rail.result('get_invoice_details_v2_54_54_54'):
        return rail.result('get_invoice_details_v2_54_54_54')
    else:
        return ""


def get_project_report_data():
    report_data = rail.result('project_report_data_generation.get_report_result').get(
        'reportGenerationResults')[0].get('payload')
    csvStringIO = StringIO(report_data)
    df = pd.read_csv(csvStringIO, sep=",")
    update_df = df.replace(np.nan, "")
    return update_df.to_dict(orient='records')


def get_logs_data():
    all_logs = []
    logs_info = get_data_from_document(rail.result('create_log'))
    for record in logs_info:
        if record.get('properties'):
            all_logs.append(record.get('properties'))
    return all_logs
