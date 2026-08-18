# pylint: disable=too-many-statements, too-many-arguments, line-too-long, too-many-branches, chained-comparison, too-many-nested-blocks, unused-variable, unsupported-membership-test, singleton-comparison, no-else-return
from datetime import datetime, timedelta
import csv
import json
from io import StringIO
import pandas as pd
import numpy as np
import rail
import pendulum
from airflow.models import Variable
from rail.lib.artifact import new_artifact
import pytz


def get_eastern_timenow(config):
    return pendulum.now(config.instance_tz)


def findItemByDisplayText(response, report_name1, report_name2,  report_name3):
    report = {}
    report['time_report_uri'] = rail.find_first_by_attr_and_get_attr(
        response.json()['d'], 'displayText', report_name1, 'uri')
    report['audit_report_uri'] = rail.find_first_by_attr_and_get_attr(
        response.json()['d'], 'displayText', report_name2, 'uri')
    report['timeentry_report_uri'] = rail.find_first_by_attr_and_get_attr(
        response.json()['d'], 'displayText', report_name3, 'uri')
    if report.get('time_report_uri') and report.get('audit_report_uri') and report.get('timeentry_report_uri'):
        return report
    raise Exception('Unable to locate reports')


def report_str_to_json(response):
    report_data = response.json()['d']['payload']
    if not report_data:
        return []
    df = pd.read_csv(StringIO(report_data), sep=",",
                     dtype={"Employee ID": "string"})
    df1 = df.replace(np.nan, "")
    data = df1.to_dict('records')
    return data


def report_payload_to_json(response_payload):
    report_data = response_payload
    if not report_data:
        return []
    df = pd.read_csv(StringIO(report_data), sep=",",
                     dtype={"Employee ID": "string"})
    df1 = df.replace(np.nan, "")
    data = df1.to_dict('records')
    return data


def get_specific_filter_uri(filterList, filter_name):
    return rail.find_first_by_attr_and_get_attr(filterList, 'displayText', filter_name, 'uri')


def convert_timedata_export_to_relevant_json(timedata, static_columns, processedDataParam):
    processedData = {}
    processedData['SOURCE'] = static_columns['SOURCE']
    processedData["RNA_RPL_IMP_ID"] = processedDataParam["ser_num"]
    processedData["SEQNBR"] = processedDataParam["seq_num"]
    processedData["RNA_RPT_PRD_ID"] = "" if timedata['TimesheetPeriodUri'] == "" else timedata['TimesheetPeriodUri'].split("timesheet:")[
        1][-12:]
    processedData["RNA_TASK_TSH_ID"] = "" if timedata['TaskUri'] == "" else timedata['TaskUri'].split("task:")[
        1]
    processedData["RNA_TSH_ENTRY_ID"] = processedDataParam["entryID"]
    processedData["RNA_RPL_EMPLID"] = "" if timedata['UserUri'] == "" else timedata['UserUri'].split("user:")[
        1]
    processedData["EMPLID"] = timedata['Employee ID']
    processedData["FIRST_NAME"] = timedata["User First Name"]
    processedData["LAST_NAME"] = timedata["User Last Name"]
    # moment(timedata["Timesheet End Date"], "MMM DD, YYYY").format("MM/DD/YYYY")
    processedData["PAY_END_DT"] = processedDataParam["timesheetEndDate"]
    # moment(timedata["Entry Date"], "MMM DD, YYYY").format("MM/DD/YYYY")
    processedData["DATE_WRK"] = processedDataParam["entryDate"]
    processedData["TL_QUANTITY"] = processedDataParam["hours_worked"]
    processedData['EXPENSE_TYPE'] = static_columns['EXPENSE_TYPE']
    processedData['RNA_EXPENSE_DATE'] = static_columns['RNA_EXPENSE_DATE']
    processedData['RNA_EXP_PAY_AMT'] = static_columns['RNA_EXP_PAY_AMT']
    processedData['SP_EXP_APPROVER'] = static_columns['SP_EXP_APPROVER']
    processedData["RNA_RPL_PAY_CODE"] = processedDataParam["payCode"]
    processedData["RNA_RPL_ACTIVITY"] = "" if timedata['TaskUri'] == "" else timedata['TaskUri'].split("task:")[
        1]
    processedData["RNA_RPL_TASKID"] = "" if timedata['TaskUri'] == "" else timedata['TaskUri'].split("task:")[
        1]
    processedData['APPROVAL_STATUS'] = static_columns['APPROVAL_STATUS']
    processedData['RNA_TASK_BILLABLE'] = static_columns['RNA_TASK_BILLABLE']
    processedData['RNA_TSH_BILLABLE'] = static_columns['RNA_TSH_BILLABLE']
    processedData["DTTIME_ADDED"] = processedDataParam["modifiedOn"]
    processedData["DTTM_EXPORT"] = processedDataParam["submmittedOn"]
    processedData["RNA_RPL_PROJ_ID"] = "" if timedata['ProjectUri'] == "" else timedata['ProjectUri'].split("project:")[
        1]
    processedData[
        "RNA_RPL_TASK_NAME"] = f"{timedata['Project Name']}/{timedata['Task Description']}-({timedata['Project Code']}_{timedata['Task Name']})"
    processedData["RNA_RPL_TASK_CODE"] = f"{timedata['Project Code']}_{timedata['Task Code']}"
    processedData["RNA_RPL_UNITID"] = "" if timedata['ClientUri'] == "" else timedata['ClientUri'].split("client:")[
        1]
    processedData["RNA_CLIENT_CODE"] = timedata['Client Code']
    processedData["RNA_CLIENT_NAME"] = f"{timedata['Client Name']}, {timedata['Client Code']}"
    processedData["RNA_RPL_NEW_TIME"] = static_columns['RNA_RPL_NEW_TIME']
    processedData["VENDOR_ID"] = timedata['Vendor ID']
    processedData["PAY_RATE"] = timedata['Pay Rate']
    processedData["RUN_DTTM"] = processedDataParam["exportDateTime"]
    processedData["PROCESS_STATUS"] = static_columns['PROCESS_STATUS']
    processedData["RECORD_IDENTIFIER"] = static_columns['RECORD_IDENTIFIER']
    processedData["DTTM_IMPORTED"] = processedDataParam["exportDateTime"]
    processedData["EMPLID2"] = static_columns['EMPLID2']
    processedData["FIRST_NAME_SRCH"] = static_columns['FIRST_NAME_SRCH']
    processedData["LAST_NAME_SRCH"] = static_columns['LAST_NAME_SRCH']
    processedData["RNA_APPROVER_DTTM"] = datetime.strftime(datetime.strptime(
        timedata['Approval Date/Time'], "%b %d, %Y %I:%M:%S %p"), "%m/%d/%Y %I.%M.%S.%f %p")

    return processedData
# pylint: disable=too-many-boolean-expressions, inconsistent-return-statements


def filter_same_entry_data(timeEntrygoupedData, userUri, timedata, EntryDate):
    same_entry_data = []
    if (userUri, EntryDate, timedata["TaskUri"], timedata["ProjectUri"], timedata["ClientUri"], timedata["DBC TRC Code"], timedata["Non DBC TRC Code"]) in timeEntrygoupedData.groups.keys():
        for el in timeEntrygoupedData.get_group((userUri, EntryDate, timedata["TaskUri"], timedata["ProjectUri"], timedata["ClientUri"], timedata["DBC TRC Code"], timedata["Non DBC TRC Code"])).to_dict('records'):
            if (el["User Uri"] == userUri and el["Entry Date"] == EntryDate and
                el["Task Uri"] == timedata["TaskUri"] and el["Project Uri"] == timedata["ProjectUri"] and
                el["Client Uri"] == timedata["ClientUri"] and
                el["DBC TRC Code"] == timedata["DBC TRC Code"]
                    and el["Non DBC TRC Code"] == timedata["Non DBC TRC Code"]):
                same_entry_data.append(el)
    return same_entry_data


def get_filtered_data(dag_run, config):
    currentDateTimeStr = dag_run.conf['datetime_str']
    processedTimesheetUris_dict = rail.result("get_processed_TimesheetUris")
    df = pd.DataFrame.from_dict(processedTimesheetUris_dict)
    processedTimesheetUris_string = df.to_string(index=False)

    user_timedata = rail.load_all_records(dag_run.conf['data_artifact'])

    list_to_df = pd.DataFrame(user_timedata)
    df_to_csv_str = list_to_df.to_csv()

    timedata_export = pd.read_csv(StringIO(df_to_csv_str), sep=",",
                                  dtype={"Employee ID": "string"})

    timedata_export = timedata_export.replace(np.nan, "")
    timedata_export = timedata_export.to_dict('records')
    timeaudit_report_artifact = rail.load_json_artifact(rail.result(
        'generate_report_2_in_batch.get_report_result'))
    timeaudit_csv_string = timeaudit_report_artifact.get('reportGenerationResults')[
        0].get('payload')
    timeDataTrailReport = pd.read_csv(StringIO(timeaudit_csv_string), sep=",",
                                      dtype={"Employee ID": "string"})
    timeDataTrailReport = timeDataTrailReport.replace(np.nan, "")
    timeDataTrailReport_groupbytimesheet = timeDataTrailReport.groupby(
        ['timesheetPeriodUri', 'Action'])
    timeDataTrailReport_groupbyuser = timeDataTrailReport.groupby(
        ['userUri', 'timesheetPeriodUri', 'Entry ID', 'Action'])
    timeentry_report_artifact = rail.load_json_artifact(rail.result(
        'generate_base_time_entry_data_report_in_batch.get_report_result'))
    timeentry_csv_string = timeentry_report_artifact.get('reportGenerationResults')[
        0].get('payload')
    timeEntryData = pd.read_csv(StringIO(timeentry_csv_string), sep=",",
                                dtype={"Employee ID": "string"})
    timeEntryData = timeEntryData.replace(np.nan, "")
    timeentrydata_grouped = timeEntryData.groupby(
        ["User Uri", "Entry Date", "Task Uri", "Project Uri", "Client Uri", "DBC TRC Code", "Non DBC TRC Code"])
    timedata_export = sorted(timedata_export, key=lambda i: datetime.strptime(
        i["Approval Date/Time"], "%b %d, %Y %I:%M:%S %p"))
    var = Variable.get(
        f"randstad_timedata_export_variables_{config.instance}", deserialize_json=True)
    ser_num = var["serial"]
    ser_num += 1
    seq_num = 0
    time_data_final_export_data_final = []
    processed_check_list = []
    excluded_timesheet_data = []
    for timedata_chunk in batch(timedata_export, 1000):
        time_data_final_export_data = []
        for timedata in timedata_chunk:
            userUri = timedata["UserUri"]
            EntryDate = timedata["Entry Date"]
            tsheetPeriodUri = timedata["TimesheetPeriodUri"]
            timesheetUriUniqueCode = timedata["TimesheetUri"].split("timesheet:")[
                1]
            if timesheetUriUniqueCode in processedTimesheetUris_string:
                continue

            timesheetEndDate = datetime.strptime(
                timedata["Timesheet End Date"], '%b %d, %Y')
            currentDateTimeET = str(datetime.strftime(
                get_eastern_timenow(config), "%m/%d/%Y %I:%M:%S %p"))
            currentDateTimeET_obj = datetime.strptime(
                currentDateTimeET, "%m/%d/%Y %I:%M:%S %p")
            if timesheetEndDate and timesheetEndDate >= currentDateTimeET_obj:
                if {
                    'timesheet_uri': timesheetUriUniqueCode,
                    'end_date': timesheetEndDate.strftime('%d/%m/%Y'),
                    'approval_date': datetime.strptime(timedata['Approval Date/Time'], "%b %d, %Y %I:%M:%S %p").strftime('%d/%m/%Y')
                } not in excluded_timesheet_data:
                    excluded_timesheet_data.append({
                        'timesheet_uri': timesheetUriUniqueCode,
                        'end_date': timesheetEndDate.strftime('%d/%m/%Y'),
                        'approval_date': datetime.strptime(timedata['Approval Date/Time'], "%b %d, %Y %I:%M:%S %p").strftime('%d/%m/%Y')
                    })
                continue

            sameTimeEntryData = filter_same_entry_data(
                timeentrydata_grouped, userUri, timedata, EntryDate)
            for s in sameTimeEntryData:
                # entryId
                entryID = ""
                rawentryID = ""
                if s:
                    rawentryID = s["Entry ID"]
                    entryID = rawentryID[-12:]

                if entryID in processed_check_list:
                    continue
                if not s["Hours Worked"] or float(s["Hours Worked"]) <= 0:
                    continue
                # SubmittedOn
                submmittedOn = ""
                extractSubmittedOn = timeDataTrailReport_groupbytimesheet.get_group(
                    (tsheetPeriodUri, "Submit")).to_dict('records') if (tsheetPeriodUri, "Submit") in timeDataTrailReport_groupbytimesheet.groups.keys() else []

                if len(extractSubmittedOn) > 0:
                    submmittedOn = (datetime.strptime(
                        extractSubmittedOn[0]['Modified On'], "%b %d, %Y %I:%M:%S %p")).strftime("%m/%d/%Y %I.%M.%S.%f %p")
                # modified on
                newArray = timeDataTrailReport_groupbyuser.get_group(
                    (userUri, tsheetPeriodUri, rawentryID, "Added")).to_dict('records') if (userUri, tsheetPeriodUri, rawentryID, "Added") in timeDataTrailReport_groupbyuser.groups.keys() else []

                modifiedOn = ""
                latestModifiedDateTime = None
                if len(newArray) > 0:
                    for i in newArray:
                        modifiedOnDateTime = datetime.strptime(
                            i["Modified On"], "%b %d, %Y %I:%M:%S %p")
                        if not latestModifiedDateTime:
                            latestModifiedDateTime = modifiedOnDateTime
                        elif modifiedOnDateTime > latestModifiedDateTime:
                            latestModifiedDateTime = modifiedOnDateTime

                if latestModifiedDateTime:  # "MM/DD/YYYY hh.mm.ss.SSS A
                    modifiedOn = latestModifiedDateTime.strftime(
                        "%m/%d/%Y %I.%M.%S.%f %p")

                payCode = ""
                if timedata["DBC TRC Code"] != "":
                    payCode = timedata["DBC TRC Code"]
                else:
                    payCode = timedata["Non DBC TRC Code"]

                if payCode.lower().strip() == "work hours":
                    payCode = "REGULAR"

                timesheetEndDate = datetime.strptime(
                    timedata["Timesheet End Date"], "%b %d, %Y")
                tsDate = timesheetEndDate.strftime("%m/%d/%Y")
                entryDate = datetime.strptime(
                    timedata["Entry Date"], "%b %d, %Y")
                eDate = entryDate.strftime("%m/%d/%Y")
                static_columns = config.static_columns
                processedDataParam = {
                    "entryID": entryID,
                    "modifiedOn": modifiedOn,
                    "submmittedOn": submmittedOn,
                    "ser_num": ser_num,
                    "exportDateTime": str(datetime.strftime(get_eastern_timenow(config), "%m/%d/%Y %I:%M:%S %p")),
                    "payCode": payCode,
                    "seq_num": seq_num,
                    "timesheetEndDate": tsDate,
                    "entryDate": eDate,
                    "hours_worked": s["Hours Worked"]
                }
                processedData = convert_timedata_export_to_relevant_json(
                    timedata, static_columns, processedDataParam)

                if processedData:
                    processedData["timesheetUriUniqueCode"] = timesheetUriUniqueCode
                    time_data_final_export_data.append(processedData)
                    processed_check_list.append(entryID)
                    seq_num += 1
        time_data_final_export_data_final += time_data_final_export_data

    if len(time_data_final_export_data_final) > 0:
        return {"processed_data": json.dumps(time_data_final_export_data_final), "excluded_data": excluded_timesheet_data}
    return {"processed_data": "", "excluded_data": excluded_timesheet_data}


def get_merged_semiprocessed_data():
    time_data_final_export = []
    artifact_list = rail.result('gather_child_processed_data')
    for artifact in artifact_list:
        temp = rail.load_all_records(artifact)
        time_data_final_export.append(pd.DataFrame(temp))
    if time_data_final_export:
        merged_df = pd.concat(time_data_final_export, ignore_index=True)
        merged_df["SEQNBR"] = list(range(len(merged_df)))
        artifact = create_load_csv_artifact(merged_df.to_csv())
    if not time_data_final_export or merged_df.empty:
        return {'artifact': "", 'has_data': False}
    return {'artifact': artifact, 'has_data': True}


def get_final_data_csv(col_names):
    final_data_artifact = rail.result(
        'generate_final_report_data').get('artifact')
    final_data = rail.load_all_records(final_data_artifact)
    final_data_df = pd.DataFrame(final_data)
    csv_string = final_data_df[col_names].to_csv(index=False, sep='|')
    delimeter = "|"
    artifact = create_load_csv_artifact(csv_string, delimeter)
    return artifact


def update_variable_date_and_serial_number(config, currentDateTimeStr):
    has_data = rail.result('generate_final_report_data').get('has_data')
    var = Variable.get(
        f"randstad_timedata_export_variables_{config.instance}", deserialize_json=True)
    var["lastRunDateTime"] = currentDateTimeStr
    if has_data:
        var["serial"] = var["serial"] + 1
    Variable.set(
        key=f"randstad_timedata_export_variables_{config.instance}", value=json.dumps(var))
    return var


def get_merged_excluded_data():
    output = []
    artifact_list = rail.result('gather_child_excluded_data')
    if artifact_list:
        for artifact in artifact_list:
            temp = rail.load_all_records(artifact)
            output.extend(temp)
    return output


def remove_processed_excludedTs(config):
    currentDateTimeET = str(datetime.strftime(
        get_eastern_timenow(config), "%m/%d/%Y %I:%M:%S %p"))
    currentDateTimeET_obj = datetime.strptime(
        currentDateTimeET, "%m/%d/%Y %I:%M:%S %p")

    is_future_TS_available_in_file = rail.result(
        'generate_base_time_data_report_in_batch_exclude.get_report_result')
    updated_dataframe = pd.DataFrame()
    excludedTS_details = rail.result('get_excluded_future_timesheets')

    if excludedTS_details:
        excludedTS_details1 = pd.DataFrame.from_dict(
            rail.result('get_excluded_future_timesheets'))
        if is_future_TS_available_in_file:
            ts_to_remove1 = list(filter(lambda x: (datetime.strptime(
                x["end_date"], "%d/%m/%Y") < currentDateTimeET_obj) and (datetime.strptime(
                    x["approval_date"], "%d/%m/%Y") < currentDateTimeET_obj), excludedTS_details))

            if len(ts_to_remove1) != 0:
                ts_to_remove = pd.DataFrame.from_dict(ts_to_remove1)
                ts_uri_key = ts_to_remove['timesheet_uri'].unique()
                updated_dataframe = excludedTS_details1[excludedTS_details1['timesheet_uri'].isin(
                    ts_uri_key) == False]
            elif not excludedTS_details1.empty:
                updated_dataframe = excludedTS_details1
        else:
            if not excludedTS_details1.empty:
                updated_dataframe = excludedTS_details1
    new_list = rail.result('generate_excluded_report_data')

    if updated_dataframe.empty:
        new_dataframe = pd.DataFrame.from_dict(new_list)
        return new_dataframe.to_json(orient="records")
    else:
        new_dataframe = pd.DataFrame.from_dict(new_list)
        frames = [new_dataframe, updated_dataframe]
        temp_final_df = pd.concat(frames)
        final_df = temp_final_df.drop_duplicates()
        return final_df.to_json(orient="records")


def getApprovedMinMaxDate(config):
    excluded_data = rail.result('get_excluded_future_timesheets')
    currentDateTimeET = str(datetime.strftime(
        get_eastern_timenow(config), "%m/%d/%Y %I:%M:%S %p"))
    currentDateTimeET_obj = datetime.strptime(
        currentDateTimeET, "%m/%d/%Y %I:%M:%S %p")

    filter_excluded_data = list(filter(lambda x: datetime.strptime(
        x["end_date"], "%d/%m/%Y") < currentDateTimeET_obj, excluded_data))
    if filter_excluded_data:
        min_date = min(filter_excluded_data, key=lambda d: datetime.strptime(
            d["approval_date"], '%d/%m/%Y'))

        max_date = max(filter_excluded_data, key=lambda d: datetime.strptime(
            d["approval_date"], '%d/%m/%Y'))

        max_dt = datetime.strptime(
            max_date["approval_date"], '%d/%m/%Y').strftime('%m/%d/%Y')

        min_dt = datetime.strptime(
            min_date["approval_date"], '%d/%m/%Y').strftime('%m/%d/%Y')
        return {"min_date": min_dt, "max_date": max_dt}
    else:
        return None


def get_user_min_max_date(config):
    excluded_data = rail.result('get_excluded_future_timesheets')
    currentDateTimeET = str(datetime.strftime(
        get_eastern_timenow(config), "%m/%d/%Y %I:%M:%S %p"))
    currentDateTimeET_obj = datetime.strptime(
        currentDateTimeET, "%m/%d/%Y %I:%M:%S %p")

    filter_excluded_data = list(filter(lambda x: datetime.strptime(
        x["end_date"], "%d/%m/%Y") < currentDateTimeET_obj, excluded_data))
    if filter_excluded_data:
        min_date = min(filter_excluded_data, key=lambda d: datetime.strptime(
            d["approval_date"], '%d/%m/%Y'))

        max_date = max(filter_excluded_data, key=lambda d: datetime.strptime(
            d["approval_date"], '%d/%m/%Y'))

        max_dt = datetime.strptime(
            max_date["approval_date"], '%d/%m/%Y').strftime('%m/%d/%Y')

        min_dt = datetime.strptime(
            min_date["approval_date"], '%d/%m/%Y').strftime('%m/%d/%Y')
        return {"min_date": min_dt, "max_date": max_dt}
    else:
        return None


def create_timesheet_uri_str():
    previous_uris_df = pd.DataFrame.from_dict(
        rail.result('get_processed_TimesheetUris'))
    date_keys = previous_uris_df['processed_date'].unique(
    ) if not previous_uris_df.empty else []
    current_datetime = datetime.now()
    formated_date = current_datetime.strftime("%d/%m/%Y")
    datetime_limit = current_datetime - timedelta(days=365)
    date_to_remove = filter(lambda x: datetime.strptime(
        x, "%d/%m/%Y") < datetime_limit, date_keys)
    updated_dataframe = previous_uris_df[previous_uris_df['processed_date'].isin(
        date_to_remove) == False]
    # processed_data = json.loads(rail.result("generate_final_report_data"))
    processed_data = rail.load_all_records(rail.result(
        'generate_final_report_data').get('artifact'))
    new_processed_data = pd.DataFrame.from_dict(processed_data)
    uris_list = new_processed_data['timesheetUriUniqueCode'].unique()
    current_date_value = previous_uris_df.loc[previous_uris_df['processed_date'] == formated_date]
    processed_uris = ",".join(current_date_value['processed_uris'])
    ts_uris_str = ""
    if len(uris_list) > 0:
        if processed_uris:
            updated_dataframe = previous_uris_df[previous_uris_df['processed_date'].isin(
                [formated_date]) == False]
            ts_uris_str = processed_uris + "," + ",".join(uris_list)
        else:
            ts_uris_str = ",".join(uris_list)
        new_dataframe = pd.DataFrame.from_dict([
            {'processed_date': formated_date, 'processed_uris': ts_uris_str}])
        final_dataframe = pd.concat([updated_dataframe, new_dataframe])
        return final_dataframe.to_json(orient="records")
    return updated_dataframe.to_json(orient="records")


def check_trigger_time(config):
    days = ['Monday', 'Tuesday', 'Wednesday',
            'Thursday', 'Friday', 'Saturday', 'Sunday']
    scheduled_time = config.trigger_time
    current_eastern_time_raw = get_eastern_timenow(config)
    current_eastern_time_str = current_eastern_time_raw.strftime(
        "%Y-%m-%dT%H:%M:%S")
    current_eastern_time = datetime.strptime(
        current_eastern_time_str, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=pytz.timezone(config.instance_tz))
    for schedule in scheduled_time:
        schedule_day = schedule.split(" ")[0]
        schedule_time = schedule.split(" ")[1]
        schedule_dt_time = current_eastern_time.strftime(
            "%d-%m-%Y") + " " + schedule_time
        schedule_date_time = datetime.strptime(
            schedule_dt_time, "%d-%m-%Y %H:%M")
        schedule_date_time_obj = schedule_date_time.replace(
            tzinfo=pytz.timezone(config.instance_tz))
        scheduletime_after_5min_obj = (
            schedule_date_time + timedelta(minutes=15)).replace(tzinfo=pytz.timezone(config.instance_tz))
        if schedule_day.lower() == days[current_eastern_time.weekday()].lower():
            if current_eastern_time >= schedule_date_time_obj and scheduletime_after_5min_obj > current_eastern_time:
                return True
    return False


def getmin_max():
    unique_dates = rail.load_all_records(
        rail.result("query_unique_dates"))
    min_date = min(unique_dates, key=lambda d: datetime.strptime(
        d["Entry_Date"], '%b %d, %Y'))
    max_date = max(unique_dates, key=lambda d: datetime.strptime(
        d["Entry_Date"], '%b %d, %Y'))
    min_datetime = datetime.strptime(min_date["Entry_Date"], '%b %d, %Y')
    datetime_now = datetime.now()
    datetime_yearback = datetime_now - timedelta(days=365)
    if min_datetime < datetime_yearback:
        min_dt = datetime_yearback.strftime('%m/%d/%Y')
    else:
        min_dt = datetime.strptime(
            min_date["Entry_Date"], '%b %d, %Y').strftime('%m/%d/%Y')
    max_dt = datetime.strptime(
        max_date["Entry_Date"], '%b %d, %Y').strftime('%m/%d/%Y')

    return {"min_date": min_dt, "max_date": max_dt}


def get_last_approval_datetime(config):
    var = Variable.get(
        f"randstad_timedata_export_variables_{config.instance}", deserialize_json=True)
    return var["lastRunDateTime"]


def get_max_users_per_chunk(config):
    var = Variable.get(
        f"randstad_timedata_export_user_chunck_variables_{config.instance}", deserialize_json=True)
    return var["chunck_size"]


def get_user_chunck_entries(dag_run):
    time_data = rail.load_all_records(dag_run.conf['data_artifact'])
    time_data_df = pd.DataFrame(time_data)
    time_data_grouped_df = time_data_df.groupby('UserUri')
    time_data_grouped_dfs = [time_data_grouped_df.get_group(
        user) for user in dag_run.conf['user_list']]
    merged_df = pd.concat(time_data_grouped_dfs, ignore_index=True)

    return merged_df.to_dict('records')


def get_dag_user_data_list():
    output = []
    user_chunk_list = rail.result('get_user_chunck_list')
    time_data = rail.load_all_records(rail.result("load_timedata_csv"))
    time_data_df = pd.DataFrame(time_data)
    time_data_grouped_df = time_data_df.groupby('UserUri')
    for user_chunck in user_chunk_list:
        time_data_grouped_dfs = [time_data_grouped_df.get_group(
            user) for user in user_chunck]
        merged_df = pd.concat(time_data_grouped_dfs, ignore_index=True)
        output.append({'user_list': user_chunck,
                       'user_data': merged_df.to_dict('records')})
    return output


def get_users_min_max_date(dag_run):
    unique_dates = rail.load_all_records(dag_run.conf['data_artifact'])
    min_date = min(unique_dates, key=lambda d: datetime.strptime(
        d["Entry Date"], '%b %d, %Y'))
    max_date = max(unique_dates, key=lambda d: datetime.strptime(
        d["Entry Date"], '%b %d, %Y'))
    min_datetime = datetime.strptime(min_date["Entry Date"], '%b %d, %Y')
    datetime_now = datetime.now()
    datetime_yearback = datetime_now - timedelta(days=365)
    if min_datetime < datetime_yearback:
        min_dt = datetime_yearback.strftime('%m/%d/%Y')
    else:
        min_dt = datetime.strptime(
            min_date["Entry Date"], '%b %d, %Y').strftime('%m/%d/%Y')
    max_dt = datetime.strptime(
        max_date["Entry Date"], '%b %d, %Y').strftime('%m/%d/%Y')

    return {"min_date": min_dt, "max_date": max_dt}


def get_user_chunck_data_artifact_list():
    final_list = []
    user_chunk_list = []
    user_list = rail.load_all_records(
        rail.result("query_unique_users"))
    user_list_df = pd.DataFrame(user_list)
    chunck_size = rail.result("get_max_users_per_chunk")
    for user_chunk in batch(user_list_df['UserUri'].tolist(), chunck_size):
        user_chunk_list.append(user_chunk)
    time_data = rail.load_all_records(rail.result("load_timedata_csv"))
    time_data_df = pd.DataFrame(time_data)
    time_data_grouped_df = time_data_df.groupby('UserUri')
    for user_chunk in user_chunk_list:
        time_data_grouped_dfs = [time_data_grouped_df.get_group(
            user) for user in user_chunk]
        merged_df = pd.concat(time_data_grouped_dfs, ignore_index=True)

        artifact = create_load_csv_artifact(merged_df.to_csv())
        final_list.append({'user_list': user_chunk,
                           'data_artifact': artifact})
    return final_list


def create_load_csv_artifact(df_to_csv_str, delimiter=','):
    reader = csv.DictReader(StringIO(df_to_csv_str),
                            delimiter=delimiter, fieldnames=None)
    validate_csv_data(reader)
    with new_artifact() as new:
        new.file.write(bytes(df_to_csv_str, 'utf-8'))
        set_csv_attributes(new)
    return new.name


def get_user_chunck_filter(dag_run):
    filter_details = []
    timesheet_period_filter_uri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_timesheetaudit_report_details')['filterConfiguration']['enabledFilters'], 'displayText', "TimesheetPeriodFilter", 'uri')
    filter_details.append({
        "reportFilterUri": timesheet_period_filter_uri,
        "value": None,
    })
    filter_details.append({
        "reportFilterUri": timesheet_period_filter_uri,
        "value": (datetime.strptime(rail.result('chunk_min_max_date')["min_date"], '%m/%d/%Y') - timedelta(days=10)).strftime('%m/%d/%Y'),
    })
    filter_details.append({
        "reportFilterUri": timesheet_period_filter_uri,
        "value": (datetime.strptime(rail.result('chunk_min_max_date')["max_date"], '%m/%d/%Y') + timedelta(days=10)).strftime('%m/%d/%Y'),
    })
    user_filter_uri = rail.find_first_by_attr_and_get_attr(rail.result('get_timesheetaudit_report_details')[
        'filterConfiguration']['enabledFilters'], 'displayText', "UserFilter", 'uri')
    new_list = [filter_details.append({"reportFilterUri": user_filter_uri,
                "value": entry.split(':')[-1], }) for entry in dag_run.conf['user_list']]
    return filter_details


def get_user_chunck_filter_entries(dag_run):
    filter_details = []
    timeentry_period_filter_uri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_timeentry_report_details')['filterConfiguration']['enabledFilters'], 'displayText', "EntryDateFilter", 'uri')
    filter_details.append({
        "reportFilterUri": timeentry_period_filter_uri,
        "value": None,
    })
    filter_details.append({
        "reportFilterUri": timeentry_period_filter_uri,
        "value": rail.result('chunk_min_max_date')["min_date"],
    })
    filter_details.append({
        "reportFilterUri": timeentry_period_filter_uri,
        "value": rail.result('chunk_min_max_date')["max_date"],
    })
    user_filter_uri = rail.find_first_by_attr_and_get_attr(rail.result('get_timeentry_report_details')[
        'filterConfiguration']['enabledFilters'], 'displayText', "UserFilter", 'uri')
    new_list = [filter_details.append({"reportFilterUri": user_filter_uri,
                "value": entry.split(':')[-1], }) for entry in dag_run.conf['user_list']]
    return filter_details


def is_timesheet_processed(row, processed_ids):
    timesheet_id_numeric = row['TimesheetUri'].split('timesheet:')[-1]
    return timesheet_id_numeric in processed_ids


def get_report_data_to_csv(config):
    one_year_ago = datetime.now() - timedelta(days=config.days_limit)
    previous_uris = pd.DataFrame.from_dict(
        rail.result('get_processed_TimesheetUris'))
    previous_uris = previous_uris['processed_uris'].tolist() if not previous_uris.empty else [
    ]
    previous_uris = ','.join(previous_uris)

    artifact_futureTS = rail.result(
        'generate_base_time_data_report_in_batch_exclude.get_report_result')
    artifact_pastTS = rail.result(
        'generate_base_time_data_report_in_batch.get_report_result')

    if artifact_futureTS and artifact_pastTS:
        report_output = rail.load_json_artifact(artifact_pastTS)
        csv_string = report_output.get('reportGenerationResults')[
            0].get('payload')
        pastTS_df = pd.read_csv(StringIO(csv_string), sep=",", dtype={
                                "Employee ID": "string"})
        mask = pastTS_df.apply(
            is_timesheet_processed, processed_ids=previous_uris, axis=1)
        pastTS_df = pastTS_df[~mask]
        report_output = rail.load_json_artifact(artifact_futureTS)
        csv_string_futureTS = report_output.get('reportGenerationResults')[
            0].get('payload')

        future_timesheet_df = pd.read_csv(StringIO(csv_string_futureTS), sep=",", dtype={
            "Employee ID": "string"})
        pending_records = rail.result('get_excluded_future_timesheets')
        pending_timesheet_list = ["urn:replicon-tenant:"+rail.get_tenant_slug(
        )+":timesheet:"+entry.get('timesheet_uri') for entry in pending_records]
        futureTS_df = future_timesheet_df[future_timesheet_df['TimesheetUri'].isin(
            pending_timesheet_list)]

        frames = [pastTS_df, futureTS_df]
        final_df = pd.concat(frames)
        df = pd.DataFrame(final_df)
        df['Timesheet End Date'] = pd.to_datetime(
            df['Timesheet End Date'], format='%b %d, %Y')
        filtered_df = df[df['Timesheet End Date'] > one_year_ago]
        filtered_df['Timesheet End Date'] = filtered_df['Timesheet End Date'].dt.strftime(
            '%b %d, %Y')
        df_to_csv_str = filtered_df.to_csv()
        reader = csv.DictReader(StringIO(df_to_csv_str),
                                delimiter=',', fieldnames=None)
        validate_csv_data(reader)
        with new_artifact() as new:
            new.file.write(bytes(df_to_csv_str, 'utf-8'))
            set_csv_attributes(new)
        return new.name
    else:
        if artifact_futureTS:
            report_output = rail.load_json_artifact(artifact_futureTS)
            csv_string_futureTS = report_output.get('reportGenerationResults')[
                0].get('payload')
            future_timesheet_df = pd.read_csv(StringIO(csv_string_futureTS), sep=",", dtype={
                "Employee ID": "string"})
            pending_records = rail.result('get_excluded_future_timesheets')
            pending_timesheet_list = ["urn:replicon-tenant:"+rail.get_tenant_slug(
            )+":timesheet:"+entry.get('timesheet_uri') for entry in pending_records]
            futureTS_df = future_timesheet_df[future_timesheet_df['TimesheetUri'].isin(
                pending_timesheet_list)]
            futureTS_df['Timesheet End Date'] = pd.to_datetime(
                futureTS_df['Timesheet End Date'], format='%b %d, %Y')
            filtered_df = futureTS_df[futureTS_df['Timesheet End Date']
                                      > one_year_ago]
            filtered_df['Timesheet End Date'] = filtered_df['Timesheet End Date'].dt.strftime(
                '%b %d, %Y')
            csv_string = filtered_df.to_csv()
        else:
            report_output = rail.load_json_artifact(artifact_pastTS)
            csv_string_pastTS = report_output.get('reportGenerationResults')[
                0].get('payload')
            pastTS_df = pd.read_csv(StringIO(csv_string_pastTS), sep=",", dtype={
                "Employee ID": "string"})
            mask = pastTS_df.apply(
                is_timesheet_processed, processed_ids=previous_uris, axis=1)
            pastTS_df = pastTS_df[~mask]
            pastTS_df['Timesheet End Date'] = pd.to_datetime(
                pastTS_df['Timesheet End Date'], format='%b %d, %Y')
            filtered_df = pastTS_df[pastTS_df['Timesheet End Date']
                                    > one_year_ago]
            filtered_df['Timesheet End Date'] = filtered_df['Timesheet End Date'].dt.strftime(
                '%b %d, %Y')
            csv_string = filtered_df.to_csv()

        reader = csv.DictReader(StringIO(csv_string),
                                delimiter=',', fieldnames=None)
        validate_csv_data(reader)
        with new_artifact() as new:
            new.file.write(bytes(csv_string, 'utf-8'))
            set_csv_attributes(new)
        return new.name


def set_csv_attributes(artifact, headers=None, delimiter=',', encoding='utf-8'):
    artifact.set_attribute("type", "csv")
    artifact.set_attribute("csv_delimiter", delimiter)
    artifact.set_attribute("csv_encoding", encoding)
    artifact.set_attribute("csv_column", headers)


def validate_csv_data(reader):
    for _ in reader:
        # read the whole file, just to make sure it is valid
        pass


def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]
