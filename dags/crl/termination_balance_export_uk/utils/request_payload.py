from datetime import datetime as dt
import pendulum
import rail
from dateutil.relativedelta import relativedelta
from crl.termination_balance_export_uk.mapper.time_off_balance_mapper import Time_off_mappper


def format_sql_in_list(items):
    """Convert tuple/list of strings to SQL IN clause format.
    
    Example: ('a', 'b') -> ('a','b')
    """
    if not items:
        return "('')"
    formatted = ','.join(f"'{item}'" for item in items)
    return f"({formatted})"


def get_start_date_begin_of_week(months=6):
    """Return start date string (YYYY-MM-DD) months before now (UTC).

    Uses timezone-aware Pendulum UTC now to avoid deprecated utcnow().
    """
    start_date = (pendulum.now('UTC').date() - relativedelta(months=months))
    return start_date.strftime("%Y-%m-%d")


def get_end_date_begin_of_week():
    """Return end date string (YYYY-MM-DD) for current UTC date.

    Uses timezone-aware Pendulum UTC now to avoid deprecated utcnow().
    """
    return pendulum.now('UTC').strftime("%Y-%m-%d")


def get_utc_now_date_string(config):
    """Return current UTC date/time string formatted per `config.date_time_format`.

    Uses Pendulum to produce a timezone-aware UTC timestamp.
    """
    return pendulum.now('UTC').strftime(config.date_time_format)


def get_run_user_report_payload():
    get_specific_report_details = rail.result('get_user_report_details')

    return {
        "reportParameters": [
            {
                "reportUri": get_specific_report_details['uri'],
                "filterValues": [],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }


def get_run_termination_balance_report_payload():
    get_specific_report_details = rail.result(
        'get_termination_balance_report_details')
    filter_values = []
    users_data = rail.load_all_records(rail.result('query_all_users_data'))
    for item in users_data:
        filter_values.append({
            "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_termination_balance_report_details'
                                                                                )['filterConfiguration']['enabledFilters'], 'displayText', 'UserFilter', 'uri'),
            "value": item['id']
        })

    return {
        "reportParameters": [
            {
                "reportUri": get_specific_report_details['uri'],
                "filterValues": filter_values,
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }



def get_final_users_data_row(items):
    return [items['username'], items['location'],
            items['useruri'],
            items['useruri'].split(':')[-1]
            ]


def get_termination_balance_uk_data_row(items):

    personnelnumber = items['Employee_ID']

    timeoff_balance = items['Time_off_Balance']
    
    timeoff_type = items['Time_Off_Type'] 

    for mapping in Time_off_mappper:
        if mapping['Time off type'] == timeoff_type:
            pay_code = mapping['Quote Type']
            break

    return ["P2010",
            personnelnumber,
            "GB",
            "",
            "INS",
            "2010",
            pay_code,
            dt.strptime(items['User_End_Date'], "%b %d, %Y").strftime(
                "%Y%m%d") if items['User_End_Date'] else "",
            dt.strptime(items['User_End_Date'], "%b %d, %Y").strftime(
                "%Y%m%d") if items['User_End_Date'] else "",
            "",
            "",
            "",
            "",
            pay_code,
            "",
            "",
            "",
            "",
            "GBP",
            timeoff_balance.split("- ")[-1].replace(",", ""),
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            ""
            ]


def get_formated_user_row(item):
    return {
        'username': item['User Name'],
        'location': item['Location (Current)'],
        'useruri': item['UserUri'],
        'userstartdate': dt.strptime(item['User Start Date'], "%b %d, %Y").strftime("%Y-%m-%d") if item['User Start Date'] else None,
        'userenddate': dt.strptime(item['User End Date'], "%b %d, %Y").strftime("%Y-%m-%d") if item['User End Date'] else None,
        'exported': item['Term Exported']
    }.values()


#function to get sequence number based on payroll calendar

# def get_sequence(calendra_mapper, time_zone):
#     current_date = pendulum.now(time_zone).strftime("%d-%m-%Y")
#     return "03" if bool(rail.find_first_by_attr_and_get_attr(
#         calendra_mapper, "payroll_processing_date", current_date)) else "02"


def get_task_state(task_id):
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()


def is_upload_data_to_sftp_failed():
    if get_task_state('upload_encrypted_export_data_to_sftp') == 'failed':
        return True
    return False

def is_upload_log_to_sftp_failed():
    if get_task_state('upload_log_data_to_sftp') == 'failed':
        return True
    return False