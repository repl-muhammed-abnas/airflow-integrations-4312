# pylint: disable=inconsistent-return-statements
from io import StringIO
import rail
import pandas as pd


def find_iten_by_displaytext_reminder(response, report_name1, report_name2):
    report = {}
    report['timesheet_period_template'] = rail.find_first_by_attr_and_get_attr(
        response.json()['d'], 'displayText', report_name1, 'uri')
    report['report_notsubmitted_timesheets_uri'] = rail.find_first_by_attr_and_get_attr(
        response.json()['d'], 'displayText', report_name2, 'uri')
    if report.get('timesheet_period_template') and report.get('report_notsubmitted_timesheets_uri'):
        return report
    raise Exception('Unable to locate reports')


def find_iten_by_displaytext_for_pay(response, report_name1, report_name2):
    report = {}
    report['repor1_uri'] = rail.find_first_by_attr_and_get_attr(
        response.json()['d'], 'displayText', report_name1, 'uri')
    report['repor2_uri'] = rail.find_first_by_attr_and_get_attr(
        response.json()['d'], 'displayText', report_name2, 'uri')
    if report.get('repor1_uri') and report.get('repor2_uri'):
        return report
    raise Exception('Unable to locate reports')


def get_timesheet_period_detail_dict():
    report_data = rail.result(
        'timesheet_period_template_report_generation.get_report_result').get('reportGenerationResults')[0].get('payload')
    csvStringIO = StringIO(report_data)
    df = pd.read_csv(csvStringIO, sep=",")
    if len(df.to_dict(orient='records')) > 0:
        return df.to_dict(orient='records')[-1]


def get_timesheet_period_for_pay_detail_dict1():
    report_data = rail.result(
        'timesheet_period_template_report1_generation.get_report_result').get('reportGenerationResults')[0].get('payload')
    csvStringIO = StringIO(report_data)
    df = pd.read_csv(csvStringIO, sep=",")
    if len(df.to_dict(orient='records')) > 0:
        return df.to_dict(orient='records')[-1]


def get_timesheet_period_for_pay_detail_dict2():
    report_data = rail.result(
        'timesheet_period_template_report2_generation.get_report_result').get('reportGenerationResults')[0].get('payload')
    csvStringIO = StringIO(report_data)
    df = pd.read_csv(csvStringIO, sep=",")
    if len(df.to_dict(orient='records')) > 0:
        return df.to_dict(orient='records')[-1]
