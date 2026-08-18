# pylint: disable=line-too-long
from datetime import datetime
import hashlib
import json
from io import StringIO
import pytz
import numpy as np
import pandas as pd
import rail
# import httplib2

def format_amount(amount):
    try:
        # Handle empty strings, None, or whitespace
        if amount is None or str(amount).strip() == '':
            return "0.0"
        
        # Remove commas before converting to float
        clean_amount = str(amount).replace(',', '')
        float_amount = float(clean_amount)
        if float_amount == int(float_amount):
            return f"{float_amount:.1f}"
        
        # Format with 2 decimal places, then remove trailing zeros
        formatted = f"{float_amount:.2f}"
        # Remove trailing zeros after decimal point
        formatted = formatted.rstrip('0').rstrip('.')
        # Ensure at least one decimal place
        if '.' not in formatted:
            formatted += '.0'
        
        return formatted
    except (ValueError, TypeError):
        # Return 0.0 for any invalid values
        return "0.0"
    
def get_process_times(config):

    curr_date = datetime.now(pytz.timezone(config.time_zone))
    process_start_time = curr_date.strftime(config.timestamp)
    # sd = (curr_date - relativedelta(months=config.prev_period_in_months, day = 1)).strftime(config.report_filter_date_format)
    # ed = curr_date.strftime(config.report_filter_date_format)
    return {
           'pst': process_start_time,
        }

def get_extract_file_name(config):
    return config.toil_extract_file_name.format(rail.result('start')['pst']) + config.file_extension

def get_toil_to_types():
    toil_to_types = [x['displayText'] for x in rail.result('get_toil_totypes')]
    return toil_to_types

def format_final_toil_data_in_instance(config):

    # timeoff balance report data parsing and formatting
    to_raw_data_str = rail.load_json_artifact(rail.result("run_to_transaction_report_details.get_report_result"))['reportGenerationResults'][0]['payload']

    if to_raw_data_str.startswith('No Data'):
        to_raw_df = pd.DataFrame(columns=['Time Off Type', 'Date', 'Event Type', 'Amount', 'UserUri'])
        to_raw_df = to_raw_df[to_raw_df['Event Type'] == config.event_type]
    else:
        to_raw_df = pd.read_csv(StringIO(to_raw_data_str), sep=config.report_sep).replace(np.nan, "")

    # timesheet day report data parsing and formatting
    ts_raw_data_str = rail.load_json_artifact(rail.result("run_ts_day_report_details.get_report_result"))['reportGenerationResults'][0]['payload']
    ts_raw_df = pd.read_csv(StringIO(ts_raw_data_str), sep=config.report_sep).replace(np.nan, "")

    toil_to_types = get_toil_to_types()
    toil_df = pd.DataFrame({'Time Off Type': toil_to_types})

    #perform cross join
    ts_to_raw_df = ts_raw_df.merge(toil_df, how='cross')

    # TS and TO report data merging
    to_ts_merge = to_raw_df.merge(ts_to_raw_df, left_on=['UserUri', 'Date', 'Time Off Type'], right_on=['UserUri', 'Date', 'Time Off Type'], how='right').replace(np.nan, 0)

    # user report data formatting
    user_raw_data_str = rail.load_json_artifact(rail.result("run_user_report_details.get_report_result"))['reportGenerationResults'][0]['payload']

    user_raw_df = pd.read_csv(StringIO(user_raw_data_str), sep=config.report_sep, dtype={"Employee ID": "string", "Workday ID": "string", "Legal Entity Code": "string"}).replace(np.nan, '')
    user_raw_df = user_raw_df[user_raw_df['User Start Date'] != '']

    to_ts_data_grpBy_user = to_ts_merge.groupby(['UserUri'])
    total_to_ts_data_users = to_ts_data_grpBy_user.groups.keys()

    user_data_grpBy_user = user_raw_df.groupby(['UserUri'])
    total_user_data_users = user_data_grpBy_user.groups.keys()

    final_data = []
    # pylint: disable=too-many-nested-blocks
    for ts_user in total_to_ts_data_users:
        toil_details = to_ts_data_grpBy_user.get_group(ts_user).to_dict('records')

        for toil_row in toil_details:
            if ts_user in total_user_data_users:
                user_details = user_data_grpBy_user.get_group(ts_user).to_dict('records')

                m_leg_dt, m_pay_dt = None, None
                emp_id, workday_id = '', ''
                le_code, pay_rule = '', ''
                ts_st_dt = datetime.strptime(toil_row['Date'], config.date_format)

                for row in user_details:
                    emp_id, workday_id = row['Employee ID'], row['Workday ID']
                    pay_dt, leg_dt = update_initial_dates(row, config) # row['Pay Rule Effective Date'], row['Legal Entity Effective Date']

                    #Get Legal Entity Code Logic
                    if ts_st_dt >= leg_dt:
                        if m_leg_dt is None or leg_dt >= m_leg_dt:
                            m_leg_dt = leg_dt
                            le_code = row['Legal Entity Code']

                    # Payrule Name Logic
                    if ts_st_dt >= pay_dt:
                        if m_pay_dt is None or pay_dt >= m_pay_dt:
                            m_pay_dt = pay_dt
                            pay_rule = row['Pay Rule Name']


                if emp_id and workday_id:

                    res = {
                        "Party_ID": emp_id,
                        "EMP_ID": workday_id,
                        "Legal_Entity": le_code,
                        "Date": ts_st_dt.strftime(config.output_date_format),
                        "Time_Off_Type": toil_row['Time Off Type'],
                        "Pay_Rule_Name": pay_rule,
                        "Amount": format_amount(toil_row['Amount']),
                        "Units": "Hours",
                        "Timesheet_URI": toil_row['TimesheetUri'],
                        "md5": hashlib.md5(
                            (str(le_code) + "," + str(ts_st_dt.strftime(config.output_date_format)) + "," + str(toil_row['Time Off Type']) + str(pay_rule)
                             + "," + str(format_amount(toil_row['Amount'])) + "," + str(toil_row['TimesheetUri'])).encode('utf-8')).hexdigest()
                    }
                    final_data.append(res)
                else:
                    print('No Employee ID or No Workday ID', toil_row['TimesheetUri'])
 
    return final_data


def update_initial_dates(row, config):

    if row['Pay Rule Effective Date'].strip() == '':
        row['Pay Rule Effective Date'] = row['User Start Date']

    if row['Legal Entity Effective Date'].strip() == '':
        row['Legal Entity Effective Date'] = row['User Start Date']

    return datetime.strptime(row['Pay Rule Effective Date'], config.date_format), datetime.strptime(row['Legal Entity Effective Date'], config.date_format)

def get_filter_timeoff_values(response, config):
    to_types = [to_type.lower() for to_type in config.toil_to_types]
    data = response.json()['d']
    return list(
        filter(
            lambda x: x['displayText'].lower() in to_types, map(
                lambda row: {
                    'value': row['uri'].split(':')[-1],
                    'displayText': row['displayText']
                }, data)
            )
        )

# def send_msg(config, msg):
#     url = config.chat_webhook_url
#     app_message = {"text": msg}
#     message_headers = {"Content-Type": "application/json; charset=UTF-8"}
#     http_obj = httplib2.Http()
#     response = http_obj.request(
#         uri=url,
#         method="POST",
#         headers=message_headers,
#         body=json.dumps(app_message),
#     )

