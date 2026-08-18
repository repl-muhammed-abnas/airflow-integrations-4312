import calendar
import datetime
import hashlib
import json
from os import path
import os
import rail
from dateutil.relativedelta import relativedelta
from macquariegroup.user_import.utils.request_payload import get_today_date
from rail.lib.artifact import new_artifact

DEPARTMENT_DELIMITER = "^"
DATE_FORMAT = "%d/%m/%Y"
DAYS_TO_REDUCE_MAPPER = {5: -1, 6: -2}


def create_artifact_of_data(data):
    with new_artifact(mode="w") as artifact:
        artifact.file.write(json.dumps(data))
        artifact.set_attribute(name="type", value="json")

        return artifact.name


def get_details(recon_data, group, office, pluck_value):
    res = list(filter(
        lambda item: item['groups'] == group and item['office'] == office, recon_data))
    if not res:
        return ""
    return res[0][pluck_value]


def get_timesheet_period_value_to_assign(recon_data, employee_type, cost_center, department):
    res = list(filter(
        lambda item: item['employee_type'] == employee_type and item['cost_center'] == cost_center and item['department'] == department, recon_data))
    if not res:
        return ""
    return res[0]['timesheet_period']


def get_create_required_fields(config):
    data = rail.load_all_records(rail.result('filter_input_data'))
    last_recon_data = rail.load_all_records(
        rail.result("create_recon_ref_collection"))
    report_data = rail.load_all_records(
        rail.result("create_report_collection"))
    res = []
    for item in data:
        derived_employee_type = get_details(
            last_recon_data, item['groups'], item['office'], 'employee_type')
        derived_cost_center = rail.smartjoin_by_delim([item["mb_gl_rep_entity"], item["mb_gl_bu"],
                                                       item["mb_gl_location"], item["mb_gl_deptid"], item["mb_gl_project"]])
        res.append({
            **{
                "emp_id": item['emp_id'],
                "first_name": item['first_name'],
                "last_name": item['last_name'],
                "email": item['email'],
                "display_name": item['display_name'],
                "login_name": item['login_name'],
                "supervisor": item['supervisor'],
                "groups": item['groups'],
                "division": item['division'],
                "department": item['department'],
                "office": item['office'],
                "mb_gl_rep_entity": item['mb_gl_rep_entity'],
                "mb_gl_bu": item['mb_gl_bu'],
                "mb_gl_location": item['mb_gl_location'],
                "mb_gl_deptid": item['mb_gl_deptid'],
                "mb_gl_project": item['mb_gl_project'],
                "business_title": item['business_title'],
                "grade": item['grade'],
                "region": item['region'],
                "fte": item['fte'],
                "cost_center": derived_cost_center,
                "department_lvl_1": "Macquarie",  # This is root Department
                "department_lvl_2": rail.smartjoin_by_delim(["Macquarie", item['groups']], DEPARTMENT_DELIMITER),
                "department_lvl_3": rail.smartjoin_by_delim(["Macquarie", item['groups'], item['division']], DEPARTMENT_DELIMITER),
                "department_lvl_4": rail.smartjoin_by_delim(["Macquarie", item['groups'], item['division'], item['department']], DEPARTMENT_DELIMITER),
                "default_permission": "Gen3 User – Report Access",
                "default_supervisor_permission": "Gen3 Supervisor",
                "default_work_week": "Sunday to Saturday",
                "default_schedule": "7.5 hours/day, Su, Sa off",
                "employee_type": derived_employee_type,
                "timesheet_period": get_timesheet_period_value_to_assign(recon_data=last_recon_data,
                                                                         employee_type=derived_employee_type,
                                                                         cost_center=derived_cost_center,
                                                                         department=item['department']
                                                                         ),
                "recon_md5": hashlib.md5((str(derived_employee_type) +
                                         str(item['department']) +
                                          str(derived_cost_center)
                                          ).encode('utf-8')
                                         ).hexdigest(),
                "md5": hashlib.md5((str(item['emp_id']) + "," +
                                    str(item['first_name']) + "," +
                                    str(item['last_name']) + "," +
                                    str(item['email']) + "," +
                                    str(item['display_name']) + "," +
                                    str(item['login_name']) + "," +
                                    str(item['supervisor']) + "," +
                                    str(item['groups']) + "," +
                                    str(item['division']) + "," +
                                    str(item['department']) + "," +
                                    str(item['office']) + "," +
                                    str(item['mb_gl_rep_entity']) + "," +
                                    str(item['mb_gl_bu']) + "," +
                                    str(item['mb_gl_location']) + "," +
                                    str(item['mb_gl_deptid']) + "," +
                                    str(item['mb_gl_project']) + "," +
                                    str(item['business_title']) + "," +
                                    str(item['grade']) + "," +
                                    str(item['region']) + "," +
                                    str(item['fte'])).encode('utf-8')).hexdigest()
            },
            **{
                "file_name": os.path.split(rail.result('new_file_sensor'))[1],
                "default_timesheet_approval_path": config.timesheet_approval_mapper.get(get_details(last_recon_data,
                                                                                                    item['groups'], item['office'], 'employee_type')),
                "default_user_permission": "Gen3 User - Report Access",
                "default_timesheet_template": "Gen3 Timesheet - One Validation Rule - RMG/FMG/COG",
                "default_office_schedule": "7.5 hours/day, Su, Sa off",
                "user_uri": rail.find_first_by_attr_and_get_attr(report_data, 'employee_id', item['emp_id'], 'user_uri', default=""),
                "recovery_enabled": rail.find_first_by_attr_and_get_attr(report_data, 'employee_id', item['emp_id'], 'recovery_enabled', default=""),
            }
        })

    return create_artifact_of_data(res)


def get_disable_processing_conf(item):
    return {
        "file_name": path.split(rail.result('new_file_sensor'))[1],
        "username": item['user_name'],
        "user_first_name": item['user_first_name'],
        "user_last_name": item["user_last_name"],
        "emp_id": item['employee_id'],
        "userloginname": item['login_name'],
        "user_uri": item['user_uri'],
        "default_supervisor_uri": rail.result("get_default_supervisor_from_replicon").get('uri', ''),
        "end_date": get_today_date(),
        "startdate": item["user_start_date"],
        "actual_end_date_udf_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_user_custom_fields'), 'name', 'Actual End Date', 'uri'),
        "user_effective_group": item["assigned_groups"]
    }


def is_user_supervisor(emp_id):
    return rail.find_first_by_attr_and_get_attr(
        rail.result("input_final_data_collection"), 'supervisor', emp_id)


def get_add_update_user_conf(item):
    return {
        **{k: v if v is not None else '' for k, v in item.items()},
        **{
            "department_to_assign": rail.find_first_by_attr_and_get_attr(rail.result('get_all_departments'), 'full_path', item['department_lvl_4']),
            "cost_center_to_assign": rail.find_first_by_attr_and_get_attr(rail.result('get_all_cost_centers'), 'name', item['cost_center']),
            "timesheet_period_to_assign": rail.find_first_by_attr_and_get_attr(rail.result('get_all_timesheet_period'), 'name', item['timesheet_period']),
            "employee_type_to_assign": {"name": item['employee_type']} if item['employee_type'] else None,
            # DropDown
            "employee_location_udf": rail.find_first_by_attr_and_get_attr(rail.result('get_all_user_custom_fields'), 'name', 'Employee Location'),
            # Text
            "title_udf": rail.find_first_by_attr_and_get_attr(rail.result('get_all_user_custom_fields'), 'name', 'Title'),
            # Text max length 3
            "grade_udf": rail.find_first_by_attr_and_get_attr(rail.result('get_all_user_custom_fields'), 'name', 'Grade'),
            # Text max length 20
            "region_udf": rail.find_first_by_attr_and_get_attr(rail.result('get_all_user_custom_fields'), 'name', 'Region'),
            # Number
            "fte_udf": rail.find_first_by_attr_and_get_attr(rail.result('get_all_user_custom_fields'), 'name', 'FTE'),

            "recovery_override": rail.find_first_by_attr_and_get_attr(rail.result('get_all_user_oef_fields'), 'name', 'Recovery Override'),
            # Text
            "ea_login_name": rail.find_first_by_attr_and_get_attr(rail.result('get_all_user_oef_fields'), 'name', 'EA Login Name'),
            "is_user_supervisor": bool(is_user_supervisor(item['emp_id'])),
            "supervisor_log": rail.result("create_supervisor_log"),
            "default_supervisor_uri": rail.result("get_default_supervisor_from_replicon").get('uri', ''),
            "recovery_enable_status": ""
        },
        **{
            "custom_due_date": rail.result("generate_effective_date"),
            "rmg_exception_message": rail.result("generate_effective_date")['rmg_exception_message'],
            "fmg_exception_message": rail.result("generate_effective_date")['fmg_exception_message']
        }
    }


def get_is_supervisor_already_assigned():
    if rail.result('get_user_details') and rail.result('get_user_details')[0]['userDetails']['supervisor']:
        return True
    return False


def map_supervisor_list(response, dag_run):
    data = response.json()['d']['rows']
    return list(
        filter(lambda x: x['employeeid'] == dag_run.conf['supervisor'],
               map(lambda item:
                   {
                       'useruri': item['cells'][0]['uri'],
                       'employeeid': item['cells'][1].get('textValue', None),
                       'enabled': item['cells'][2].get('boolValue', None),
                   }, data))
    )


def do_format_logs():

    user_add_logs = rail.result("format_add_logs") or []
    user_update_logs = rail.result("format_update_logs") or []
    user_import_log: list = json.loads(
        rail.result("load_master_log"))
    user_import_log.extend(user_add_logs)
    user_import_log.extend(user_update_logs)

    unique_users = list(
        set(map(lambda item: item['properties'].get(
            "employee_id", ''), user_import_log))
    )

    def get_log_details(user_logs):
        return ";".join(list(filter(bool, (set(map(lambda x: x['properties'].get('details'), user_logs))))))

    def get_status(user_logs):
        available_status = list(
            map(lambda log: log['properties']['status'], user_logs))
        if "Error" in available_status:
            return "Error"
        if "Exception" in available_status:
            return "Exception"
        if "Skipped" in available_status:
            return "Skipped"
        return "Success"
    logs = []
    # pylint: disable= cell-var-from-loop
    for employee_id in unique_users:
        user_logs = list(
            filter(lambda x: x['properties'].get(
                'employee_id', '') == employee_id, user_import_log)
        )
        if len(user_logs) > 0:
            first = user_logs[0]
            user_action = [x['properties'].get('action') for x in user_logs]
            logs.append(
                {
                    "employee_id": employee_id,
                    "user_name": first['properties']['user_name'],
                    "action": first['properties']['action'] if "Disable" not in user_action else "Disable",
                    "status": get_status(user_logs),
                    "ecid": first['ecid'],
                    "details": get_log_details(user_logs)
                }
            )
    return logs


def bool_can_disable_user(dag_run):
    if not dag_run.conf['user_effective_group'] or \
            dag_run.conf['user_effective_group'].lower() not in ('financial management group', 'risk management group'):
        return False
    return True

def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf

def get_log_message(task_id, action):
    added_updated_fields: list = rail.result(task_id, "updated_fields")
    message = ""
    if action == "add":
        message = "User created successfully"
        # Supervisor not present in feed file
        if not bool(get_dag_run_conf()['supervisor']):
            message += ";Supervisor not present in feed file. Default Supervisor assigned"
    if action == "update":
        message = "User updated successfully"
    added_updated_fields.insert(0, message)
    return rail.smartjoin_by_delim(added_updated_fields, ";")


def get_current_month_end_day(replicon_payload_format=True):
    today = datetime.datetime.now()
    current_month_last_day = today.replace(
        day=calendar.monthrange(today.year, today.month)[1])
    if not replicon_payload_format:
        return current_month_last_day
    return {
        "day": current_month_last_day.day,
        "month": current_month_last_day.month,
        "year": current_month_last_day.year
    }


def get_23rd_of_last_month():
    today = datetime.datetime.now()
    date_value = datetime.date(
        today.year, today.month, 23) - relativedelta(month=1)
    return {
        "day": date_value.day,
        "month": date_value.month,
        "year": date_value.year
    }


def get_str_date(date_value, is_dict=False):
    if is_dict:
        return datetime.date(date_value['year'], date_value['month'], date_value['day']).strftime(DATE_FORMAT)
    return date_value.strftime(DATE_FORMAT)


def get_weekday_for_date(date_value):
    return date_value.weekday()


def get_previous_workingday(date_value):
    return (date_value + datetime.timedelta(
        days=DAYS_TO_REDUCE_MAPPER[get_weekday_for_date(date_value)]))


def get_custom_due_date(date_value, employee_type, current_month_holidays, calculate_custom_due_date, timesheet_start_date):
    exception_message = ""
    while True:
        is_timesheet_end_date_working_day = True
        if current_month_holidays:
            if get_str_date(date_value) in current_month_holidays and calculate_custom_due_date[employee_type]:
                date_value -= datetime.timedelta(days=1)
                is_timesheet_end_date_working_day = False
        if date_value <= timesheet_start_date:
            calculate_custom_due_date[employee_type] = False
            exception_message = "The timesheet Doesn't have any Due date"
        if is_timesheet_end_date_working_day:
            break

    return date_value, exception_message


def generate_effective_date_callable():
    def is_day_weekend(date_value):
        if get_weekday_for_date(date_value) >= 5:
            return True
        return False

    today = datetime.datetime.now()

    fmg_timesheet_end_date = datetime.date(today.year, today.month, 23)
    start_of_timesheet_period_fmg = fmg_timesheet_end_date - \
        relativedelta(month=1)
    start_of_timesheet_period_rmg = datetime.datetime.now().replace(day=1)
    rmg_timesheet_end_date = get_current_month_end_day(
        replicon_payload_format=False)
    current_month_holidays = rail.result("get_holidays_for_current_month")

    # check max loop
    fmg_exception_message = ""
    rmg_exception_message = ""
    calculate_custom_due_date = {"fmg": True, "rmg": True}
    rmg_timesheet_end_date, rmg_exception_message = get_custom_due_date(date_value=rmg_timesheet_end_date,
                                                employee_type='rmg',
                                                current_month_holidays=current_month_holidays,
                                                calculate_custom_due_date=calculate_custom_due_date,
                                                timesheet_start_date=start_of_timesheet_period_rmg
                                                )
    rmg_timesheet_end_date -= datetime.timedelta(days=5)

    while True:
        is_fmg_timesheet_end_date_working_day = True
        is_rmg_timesheet_end_date_working_day = True

        if current_month_holidays:
            if get_str_date(fmg_timesheet_end_date) in current_month_holidays and calculate_custom_due_date['fmg']:
                fmg_timesheet_end_date -= datetime.timedelta(days=1)
                is_fmg_timesheet_end_date_working_day = False

            if get_str_date(rmg_timesheet_end_date) in current_month_holidays and calculate_custom_due_date['rmg']:
                rmg_timesheet_end_date -= datetime.timedelta(days=1)
                is_rmg_timesheet_end_date_working_day = False

        if is_day_weekend(fmg_timesheet_end_date) and calculate_custom_due_date['fmg']:
            fmg_timesheet_end_date = get_previous_workingday(
                fmg_timesheet_end_date)
            is_fmg_timesheet_end_date_working_day = False

        if is_day_weekend(rmg_timesheet_end_date) and calculate_custom_due_date['rmg']:
            rmg_timesheet_end_date = get_previous_workingday(
                rmg_timesheet_end_date)
            is_rmg_timesheet_end_date_working_day = False

        if fmg_timesheet_end_date <= start_of_timesheet_period_fmg:
            calculate_custom_due_date['fmg'] = False
            fmg_exception_message = "The timesheet Doesn't have any Due date"
        if rmg_timesheet_end_date < start_of_timesheet_period_rmg:
            calculate_custom_due_date['rmg'] = False
            rmg_exception_message = "The timesheet Doesn't have any Due date"

        if is_fmg_timesheet_end_date_working_day and is_rmg_timesheet_end_date_working_day:
            break

    return {
        "fmg_timesheet_end_date": fmg_timesheet_end_date.strftime(DATE_FORMAT),
        "rmg_timesheet_end_date": rmg_timesheet_end_date.strftime(DATE_FORMAT),
        "rmg_timesheet_end_date_replicon_date": {
            "day": rmg_timesheet_end_date.day,
            "month": rmg_timesheet_end_date.month,
            "year": rmg_timesheet_end_date.year
        },
        "fmg_timesheet_end_date_replicon_date": {
            "day": fmg_timesheet_end_date.day,
            "month": fmg_timesheet_end_date.month,
            "year": fmg_timesheet_end_date.year
        },
        "fmg_exception_message": fmg_exception_message,
        "rmg_exception_message": rmg_exception_message
    }
