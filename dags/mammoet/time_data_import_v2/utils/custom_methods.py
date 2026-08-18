from datetime import datetime, date
from dateutil.parser import parse as dp
from uuid import uuid4
import rail


null = None

MANDATORY_FIELDS_MAPPING = {'workdate': 'Work Date', 'employeenumber': 'Employee Number',
                            'wbselement': 'Project Code', 'starttime': 'Start Time',
                            'endtime': 'End Time',
                            'sourcesystem': 'Source System'}

REPORT_DATE_FORMAT = "%b %d, %Y"
FEED_ENTRYDATE_DATE_FORMAT = '%Y-%m-%d'


def parse_date(date_value, date_format):
    return datetime.strptime(date_value, date_format)


def parse_date_json(date_value: dict):
    return date(day=date_value['day'], month=date_value['month'], year=date_value['year'])


def get_missing_field_message(item):
    missing_fields = []
    for key, log_value in MANDATORY_FIELDS_MAPPING.items():
        if not item[key]:
            missing_fields.append(f"{log_value} not present in the feed file")
    return rail.smartjoin_by_delim(missing_fields, ";")


def get_log_message_per_item(item, status, action, details):
    return {
        "counter_id": item['counter'],
        "employee_id": item['employeenumber'],
        "entrydate": item['workdate'],
        "status": status,
        "action": action,
        "details": details
    }


def get_entry_date(item):
    return (item['workdate'], dp(item['workdate']))


def map_user_details_with_feed_callable():

    feed_data = rail.load_all_records(rail.result("query_records_to_process"))
    report_user_data = rail.load_json_artifact(
        rail.result("load_available_users"))
    project_report_data = rail.load_all_records(
        rail.result("create_project_report_collection"))
    final_data = []

    def find_max_min_date_for_user_in_feed(emp_id):
        user_records_in_feed = sorted(list((map(get_entry_date, filter(
            lambda item: item['employeenumber'] == emp_id, feed_data)))))
        min_date = null
        max_date = null
        if user_records_in_feed:
            min_date = user_records_in_feed[0][1]
            max_date = user_records_in_feed[-1][1]
        return (min_date, max_date)

    for record in feed_data:
        user_details = rail.find_first_by_attr_and_get_attr(
            report_user_data, "Employee_ID", record['employeenumber'])
        user_min_entry_date, user_max_enrty_date = find_max_min_date_for_user_in_feed(
            record['employeenumber'])
        # pylint: disable=cell-var-from-loop
        # remove level 2 tasks from the project data
        project_data = list(filter(lambda project_data: project_data[
            'Project_Code'] == record['wbselement'], project_report_data))
        task_details_via_code = rail.find_first_by_attr_and_get_attr(
            project_data, "Task_Name__Full_Path_", record['sub_activity'], default={})
        activity_uri = rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_activities"), "code", record['acttype'], 'uri') if record[
                'abs_att_type'] != "BRK" else rail.find_first_by_attr_and_get_attr(rail.result(
                    "get_all_breaks"), "displayText", "Break", 'uri')
        final_data.append({
            **record,
            **{
                "user_login_name": user_details['Login_Name'],
                "user_uri": user_details['UserUri'],
                "user_start_date": user_details['User_Start_Date'],
                "user_end_date": user_details['User_End_Date'],
                "user_login_status": user_details['User_Status'],
                "user_location": user_details['Location__Current___Full_Path_'],
                "feed_user_min_date": user_min_entry_date.strftime(FEED_ENTRYDATE_DATE_FORMAT),
                "feed_user_max_date": user_max_enrty_date.strftime(FEED_ENTRYDATE_DATE_FORMAT),
                "is_valid_dates": test_timeenrty_daterange(user_details['User_Start_Date'],
                                                           user_details['User_End_Date'], record['workdate']),
                "project_uri": project_data[0].get('project_uri') if project_data else null,
                "project_status": project_data[0].get('Project_Status') if project_data else null,
                "task_details": task_details_via_code,
                "task_to_use_uri": task_details_via_code.get('Task_uri'),
                "is_billable": task_details_via_code.get('Task_Time___Expense_Entry_Type') != "Non-Billable",
                "activity_uri": activity_uri
            }
        })
    return rail.write_json_artifact(final_data)


def test_timeenrty_daterange(user_start_date, user_end_date, entry_date):
    user_start_date = parse_date(
        user_start_date, REPORT_DATE_FORMAT) if user_start_date else None
    user_end_date = parse_date(
        user_end_date, REPORT_DATE_FORMAT) if user_end_date else None
    entry_date = parse_date(entry_date, FEED_ENTRYDATE_DATE_FORMAT)

    if (not user_start_date) or (user_start_date > entry_date):
        return False
    if user_end_date and (entry_date > user_end_date):
        return False
    return True


def get_required_details(task_id):
    user_data = rail.load_all_records(rail.result(task_id))[0]
    return {
        "user_uri": user_data['user_uri'],
        "min_entry_date": user_data['feed_user_min_date'],
        "max_entry_date": user_data['feed_user_max_date']
    }


def is_date_in_ts_period(ts_dates, entry_date):
    return parse_date_json(ts_dates['startDate']) <= parse_date(entry_date, FEED_ENTRYDATE_DATE_FORMAT).date() <= parse_date_json(ts_dates['endDate'])


def get_timesheet_data(entry_date, ts_data):
    timesheet_record = list(filter(lambda ts: is_date_in_ts_period(
        ts['timesheet_date_range'], entry_date), ts_data))
    if not timesheet_record:
        return {
            "timesheet_found": "No",
            "timesheet_uri": "na",
            "timesheet_status_uri": "na"
        }
    return {
        **{
            "timesheet_found": "Yes"
        },
        **timesheet_record[0]
    }


def map_timesheet_with_user_data(user_data_task_id, ts_data_task_id):
    user_data = rail.load_all_records(rail.result(user_data_task_id))
    ts_data = rail.result(ts_data_task_id)

    unique_entry_ids = list(set(map(lambda record: record['counter'], user_data)))

    def get_all_data_for_entry_id(item_entry_id):
        data_for_item_entry_id = list(filter(lambda rec: rec['counter']==item_entry_id,user_data))
        return {
            **data_for_item_entry_id[0],}

    user_ts_data_per_unique_entry_id = list(map(get_all_data_for_entry_id, unique_entry_ids))

    def get_map_data(item):
        timesheet_data = get_timesheet_data(item['workdate'], ts_data)
        return {
            **item,
            **timesheet_data
        }

    user_ts_data = list(map(get_map_data, user_ts_data_per_unique_entry_id))
    ts_present_and_to_reopen = filter(lambda record: record['timesheet_found'].lower()=="yes" and
                                      record['timesheet_status_uri'].split(':')[-1] not in ['open', 'rejected'], user_ts_data)
    timesheet_to_reopen = list(map(lambda ts_to_reopen: {
                            "ts_uri": ts_to_reopen['timesheet_uri'],
                            "timesheet_status_uri": ts_to_reopen['timesheet_status_uri'],
                            "timesheet_status": ts_to_reopen['timesheet_status'],
                            "user_login_name": ts_to_reopen['user_login_name'],
                            "user_uri": ts_to_reopen["user_uri"],
                            "unit_of_work_id": str(uuid4())
                        }, ts_present_and_to_reopen))

    rail.set_result(key="timesheet_to_reopen",val= list({v['ts_uri']:v for v in timesheet_to_reopen}.values()))
    return rail.write_json_artifact(user_ts_data)


def get_seconds_from_replicon(replicon_hours):
    if not replicon_hours:
        return 0
    return replicon_hours['seconds'] + (replicon_hours['minutes'] * 60) + (replicon_hours['hours'] * 60 * 60)


def check_timeentry_validations(dag_run):
    if (not rail.result('search_time_entry_by_id')) and (float(dag_run.conf['actual_hours']) < 0):
        return False
    if (not rail.result('search_time_entry_by_id')) and (float(dag_run.conf['actual_hours']) >= 0):
        return True
    return dag_run.conf['actual_hours'] >= 0


def check_timeentry_validations_add(dag_run):
    if (not rail.result('search_time_entry_by_id')) and (float(dag_run.conf['actual_hours']) < 0):
        return False
    if (not rail.result('search_time_entry_by_id')) and (float(dag_run.conf['actual_hours']) >= 0):
        return True
    return dag_run.conf['actual_hours'] > 0


def check_timesheet_is_open():
    if rail.result('get_timesheet_details') and rail.result('get_timesheet_details')['timesheetStatus']:
        timesheet_uri = rail.result('get_timesheet_details')["timesheetStatus"]["uri"]
    elif rail.result('create_timesheet_for_period'):
        return False
    else:
        raise Exception("Timesheet Uri Not found")
    return timesheet_uri.split(':')[-1] not in ['open', 'rejected']


def get_timesheet_status(dag_run):
    return "approved" if dag_run.conf['timesheet_status_uri'].endswith('approved') else "waiting"


def load_records(log_artifact):
    try:
        logs = rail.load_all_records(log_artifact)
        return logs
    except:  # pylint: disable=bare-except
        return []


def do_format_logs(dag_run):
    log_records = []
    logs = dag_run.conf['logs']
    if not logs:
        logs = []
    logs.append(dag_run.conf['main_log'])
    for log in logs:
        each_log_records = load_records(log)
        if each_log_records:
            log_records.extend(each_log_records)

    rail.set_result(key="get_successful_logs", val=len(list(filter(lambda item: item['properties']['status']=="Success", log_records))))
    rail.set_result(key="get_errored_logs", val=len(list(filter(lambda item: item['properties']['status']=="Error", log_records))))
    rail.set_result(key="get_exception_logs", val=len(list(filter(lambda item: item['properties']['status']=="Exception", log_records))))
    rail.set_result(key="get_skipped_logs", val=len(list(filter(lambda item: item['properties']['status']=="Skipped", log_records))))

    return rail.write_json_artifact(log_records)
