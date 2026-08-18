from datetime import datetime
import rail
INVALID_DATES = ["9999-12-31", "0000-00-00"]
null = None
true = "true"
false = "false"


def get_error_message():
    context = rail.get_current_context()
    failed_task_ids = rail.lib.errors.get_failed_task_ids(context)
    error_message = ''
    if failed_task_ids:
        error_key = (context['ti'].xcom_pull(
            failed_task_ids[0], key='error') or 'Unknown error occurred')
        error_message = (error_key.get("response").get("body") if error_key.get("response")
                         else error_key.get('exc_message')) if isinstance(error_key, dict) else error_key

    return error_message

def format_logs(cntry):
    master_log = []
    user_import_logs = []
    if rail.result(f'get_user_import_logs_{cntry}'):
        user_import_logs = rail.result(f'get_user_import_logs_{cntry}')

    if rail.result(f'get_disable_user_import_logs_{cntry}'):
        user_import_logs += rail.result(f'get_disable_user_import_logs_{cntry}')

    for log in user_import_logs:
        log_records = rail.load_all_records(log)
        if log_records:
            master_log.extend(log_records)

    users = list(
        set(map(lambda x: x['properties'].get('employee_id', ''), master_log))
    )

    logs = []

    for userid in users:
        user_logs = list(
            filter(lambda x: x['properties'].get('employee_id', '') == userid, master_log)
        )
        error_logs = list(
            filter(lambda x: x['properties'].get('status') == 'Failed', user_logs)
        )
        exception_logs = list(
            filter(lambda x: x['properties'].get('status') == 'Exception', user_logs)
        )

        if len(user_logs) > 0:
            status = ""
            first = user_logs[0]

            if error_logs:
                status = "Error"
            elif exception_logs:
                status = "Exception"
            else:
                status = first['properties'].get('status', 'Error')

            action = first['properties'].get("action", "")
            details = ', '.join(list(map(lambda x: x['properties'].get('details', ""), user_logs)))
            if action == "Update" and not details:
                details = "No user attribute updated"

            # choose ecid from the actual failed/exception record when applicable
            ecid_out = first.get("ecid", "")

            if status == "Error":
                failed_rec = next((x for x in user_logs if x['properties'].get('status') == 'Failed'), None)
                if failed_rec:
                    ecid_out = failed_rec.get("ecid", "") or ecid_out

            elif status == "Exception":
                exc_rec = next((x for x in user_logs if x['properties'].get('status') == 'Exception'), None)
                if exc_rec:
                    ecid_out = exc_rec.get("ecid", "") or ecid_out

            logs.append({
                "timestamp": first.get("timestamp"),
                "employee_id": first['properties'].get("employee_id", ""),
                "employee_first_name": first['properties'].get("employee_first_name", ""),
                "employee_last_name": first['properties'].get("employee_last_name", ""),
                "country": first['properties'].get("country", ""),
                "company_code": first['properties'].get("company_code", ""),
                "status": status,
                "action": action,
                "details": details,
                "ecid": ecid_out 
            })

    return logs

def get_today_date():
    now_date = datetime.utcnow()
    return {
        'year': now_date.year,
        'month': now_date.month,
        'day': now_date.day
    }


def map_time_off_delete_uri(response):
    time_off_list = []
    data = response.json()['d']['rows']
    for time_off in data:
        time_off_list.append(time_off['cells'][0]['uri'])
    return time_off_list

def get_data_for_all_future_timeoff_after_the_enddate():
    return {
        "page": "1",
        "pagesize": "1000",
        "columnUris": [
            "urn:replicon:time-off-list-column:time-off"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": null,
                    "filterDefinitionUri": "urn:replicon:time-off-list-filter:time-off-date-range"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": {
                        "uri": null,
                        "uris": [],
                        "bool": null,
                        "date": null,
                        "money": null,
                        "number": null,
                        "text": null,
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": {
                            "startDate": get_today_date(),
                            "endDate": null,
                            "relativeDateRangeUri": null,
                            "relativeDateRangeAsOfDate": null
                        },
                        "dateTimeUtc": null,
                        "dateTimeUtcRange": null,
                        "numberRange": null
                    },
                    "filterDefinitionUri": null
                },
                "value": null,
                "filterDefinitionUri": null
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": null,
                    "filterDefinitionUri": "urn:replicon:time-off-list-filter:time-off-owner"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": {
                        "uri": rail.result("for_each_user_delete_timeoff")["useruri"],
                        "uris": [],
                        "bool": null,
                        "date": null,
                        "money": null,
                        "number": null,
                        "text": null,
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "dateTimeUtc": null,
                        "dateTimeUtcRange": null,
                        "numberRange": null
                    },
                    "filterDefinitionUri": null
                },
                "value": null,
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def create_timeOff_delete_batch():
    return {
        "timeOffUris": rail.result("get_data_forall_timeoff_after_the_enddate")
    }


def execute_timeOff_delete_batch():
    return {
        "timeOffDeleteBatchUri": rail.result("create_timeOff_delete_batch")
    }

def get_report_params():
    return {
                "reportParameters": [
                {
                "reportUri": rail.result("get_user_report_details")["uri"],
                "filterValues": [
                    {
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(
                        rail.result("get_user_report_details")["filterConfiguration"]["enabledFilters"],
                        "displayText","CurrentServiceCenterFilter","uri"),
                    "value": rail.result("get_country_uri").split(":")[-1]
                    }
                ],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                }
            ]
            }

def get_all_users_with_enddate_data():
    users_with_enddate = rail.load_all_records(rail.result("query_all_assignee_users_with_enddate"))+\
    rail.load_all_records(rail.result("query_all_local_hire_users_with_enddate"))
    return list(
        map(lambda cell: {
            "useruri": cell["useruri"],
            "enddate": cell["user_end_date"] or cell["onsite_end_date"],
            "employee_id": cell["employee_id"],
            "login_name": cell["login_name"],
            "country": cell["country"],
            "first_name":cell["user_first_name"],
            "last_name":cell["user_last_name"],
        }, users_with_enddate))
