# pylint: disable=broad-exception-raised line-too-long singleton-comparison
from datetime import datetime
import csv
import rail
import pendulum

def findItemByDisplayText(response, report_name):
    report = {}
    report['timesheet_report_uri'] = rail.find_first_by_attr_and_get_attr(
        response.json()['d'], 'displayText', report_name, 'uri')
    if report.get('timesheet_report_uri'):
        return report
    raise Exception('Unable to locate reports')

def get_formated_timesheet_data(config):
    artifact2 = rail.result('run_report_for_timesheet.get_report_result')
    artifact2 = rail.load_json_artifact(artifact2)
    reponse2 = artifact2.get('reportGenerationResults')[0].get('payload')

    curr_time = get_eastern_timenow(config)
    curr_datetime = str(datetime.strftime(curr_time, "%m/%d/%Y"))
    curr_datetime_obj = datetime.strptime(curr_datetime, "%m/%d/%Y")

    splitted_rows2 = reponse2.split('\r\n')
    reader2 = csv.DictReader(splitted_rows2, delimiter=',')
    reader_list2 = list(reader2)
    report_dict2 = dict(enumerate(reader_list2))

    output = []
    not_eligible = set()
    eligible = set()

    for pos in report_dict2:
        if datetime.strptime(report_dict2.get(pos).get('Timesheet Period').split('-')[-1].strip(), "%b %d, %Y") < curr_datetime_obj:
            if report_dict2.get(pos).get('Approval Status') == "Approved":
                not_eligible.add(report_dict2.get(pos).get('TimesheetURI'))
            else:
                eligible.add(report_dict2.get(pos).get('TimesheetURI'))

    final_list = [uri for uri in eligible if uri not in not_eligible]
    for uris in batch(final_list, 100):
        output.append(uris)
    return output

def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


def get_eastern_timenow(config):
    return pendulum.now(config.timezone)

def get_chunk_timesheet_uris(dag_run):
    if dag_run:
        return dag_run.conf["item"]
    return []

def get_validated_ts_uris(response):
    allChunkTimesheetUris = rail.result('get_chunk_timesheets')
    data = response.json()['d']
    invalid_ts = []

    for timesheet in data:
        validationResults = timesheet.get('validationResult', {}).get('validationMessages', [])
        if len(validationResults) != 0:
            if any(item.get('severity') == 'urn:replicon:severity:error' for item in validationResults):
                invalid_ts.append(timesheet.get("objectUri"))

    final_valid_ts = [tsUri for tsUri in allChunkTimesheetUris if tsUri not in invalid_ts]

    if len(final_valid_ts) == 0:
        return {"validTimesheets": final_valid_ts, "InvalidTimesheets": invalid_ts, "has_valid_TS": False}
    return {"validTimesheets": final_valid_ts, "InvalidTimesheets": invalid_ts, "has_valid_TS": True}
