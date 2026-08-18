import datetime
import rail
from pwcglobal.user_import_australia.mappers.user_allowance_mapper import user_allowance_mapper
from pwcglobal.user_import_australia.mappers.user_mapping import user_mapping
from pwcglobal.user_import_australia.mappers.australia_classification_records import classification_records
from pwcglobal.user_import_australia.mappers.australia_timezone import australia_timezone

null_urn = "urn:replicon:list-type:null"


def get_conf():
    return rail.get_current_context()['dag_run'].conf


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def get_is_supervisor_already_assigned(caller):
    if ((caller != 'add') and (rail.result('get_user_details') and rail.result('get_user_details')[0]['userDetails']['supervisor'])):
        return True
    return False


def has_any_file(result_task_id, input_file_path):
    if not result_task_id or not input_file_path:
        raise Exception(
            "Task_id" if not result_task_id else "input path" + "is not provided")
    data = rail.result(result_task_id)
    if not data:
        return False
    return len(data[input_file_path]) > 0


def get_user_data(response, dag_run):
    response = response.json()['d']['rows']
    return [{
        "user_uri": record["cells"][0]['uri'],
        "user_guid": record['cells'][0]['textValue'],
        "enabled": record['cells'][1]['textValue']
    } for record in response if record['cells'][0]['textValue'] == dag_run.conf['guid']]


def convert_to_date(date, date_format):
    if not date:
        return None
    if date_format == "json":
        # YYYY-MM-DD
        return datetime.date(date['year'], date['month'], date['day'])

    return datetime.datetime.strptime(date, date_format).date()


def get_payload_format_date(date):
    if not date:
        pass
    return{
        "year": date.year,
        "month": date.month,
        "day": date.day
    }


def bool_enddate_before_startdate(dag_run):
    user_dates = rail.result("get_user_details")['employmentDateRange']
    if not user_dates['startDate']:
        return True
    return (convert_to_date(dag_run.conf['termination_date'], "%d-%m-%Y")-convert_to_date(user_dates["startDate"], "json")).days < 0


def get_allowance_mapper_value(dag_run):
    return rail.find_first_by_attr_and_get_attr(user_allowance_mapper, "compensationelement", dag_run.conf['compensation_element'])


def get_allowance_generic_child_conf(dag_run):
    return {
        "employee_id": dag_run.conf['employee_id'],
        "guid": dag_run.conf['guid'],
        "compensation_plan_effective_date": dag_run.conf['compensation_plan_effective_date'] if dag_run.conf['compensation_plan_effective_date'] else None,
        "expected_end_date": dag_run.conf['expected_end_date'] if dag_run.conf['expected_end_date'] else None,
        "compensation_element": dag_run.conf['compensation_element'],
        "mapper_details": rail.result("get_mapper_value"),
        "file_name": dag_run.conf['file_name'],
        "log": dag_run.conf['log']
    }


def can_record_be_ignored(dag_run):
    if "Ignored" in dag_run.conf['mapper_details']['replicongroup']:
        return True
    return False


def process_allowance_dates(dag_run):
    difference = (convert_to_date(dag_run.conf['expected_end_date'], "%d-%m-%Y") - convert_to_date(
        dag_run.conf['compensation_plan_effective_date'], "%d-%m-%Y")) if dag_run.conf['compensation_plan_effective_date'] else 1
    if difference.days < 0:
        return {
            "log_invalid_dates": "True",
            "expected_end_date": None,
            "compensation_plan_effective_date": None
        }
    return {
        "log_invalid_dates": "False",
        "expected_end_date": dag_run.conf['expected_end_date'] if difference.days > 0
        else (convert_to_date(dag_run.conf['expected_end_date'], "%d-%m-%Y") + datetime.timedelta(days=1)).strftime("%d-%m-%Y"),
        "compensation_plan_effective_date": dag_run.conf['compensation_plan_effective_date']
    }


def get_cost_center_response_filter(response):
    response = response.json()['d']
    return{
        "yes_uri": rail.find_first_by_attr_and_get_attr(response, "displayText", "Yes", "uri"),
        "no_uri": rail.find_first_by_attr_and_get_attr(response, "displayText", "No", "uri")
    }


def get_classification_schedule_for_user_response_filter(response):
    response = response.json()['d']
    if not response:
        return None
    if not response['serviceCenters']:
        return None
    return list(map(lambda item: {
        "classification_display_text": item['serviceCenter']['serviceCenter']['displayText'],
        "classification_uri": item['serviceCenter']['serviceCenter']['uri'],
        "effective_date_json": item['effectiveDate'],
        "effective_date": str(convert_to_date(date=item['effectiveDate'], date_format="json"))
    }, response['serviceCenters']))


def get_schedule_for_user_response_filter(response):
    pluck_value = rail.result("get_details_by_caller_task")[
        'get_details_pluck_value']
    response = response.json()['d']
    if not response:
        return None
    if not response[pluck_value+'s']:
        return None
    return list(map(lambda item: {
        "cost_center_display_text": item[pluck_value][pluck_value]['displayText'],
        "cost_center_uri": item[pluck_value][pluck_value]['uri'],
        "effective_date_json": item['effectiveDate'],
        "effective_date": str(convert_to_date(date=item['effectiveDate'], date_format="json"))
    }, response[pluck_value+'s']))


def get_can_update_status(dag_run):

    effective_date = None
    if dag_run.conf['compensation_plan_effective_date'] and rail.result('get_schedule_for_user'):
        effective_date = (rail.find_first_by_attr_and_get_attr(rail.result('get_schedule_for_user'),
                                                               "cost_center_display_text", "Yes", 'effective_date')
                          == dag_run.conf['compensation_plan_effective_date'])

    end_date = None
    if dag_run.conf['expected_end_date'] and rail.result('get_schedule_for_user'):
        end_date = (rail.find_first_by_attr_and_get_attr(rail.result('get_schedule_for_user'),
                                                         "cost_center_display_text", "No", 'effective_date')
                    == dag_run.conf['expected_end_date'])

    # None = not applicable
    # True = No need to update
    # False = Update
    return{
        "effective_date_update": None if effective_date is None else effective_date,
        "end_date_update": None if end_date is None else end_date
    }


def bool_can_update_cost_center():
    update_status = rail.result("get_can_update_status")
    if update_status['effective_date_update'] in [None, False] or \
            update_status['end_date_update'] in [None, False]:
        return True
    return False


def add_yes(dag_run, cost_center_list, dict_key):
    task_name = "get_enabled_business_units" if dict_key == "division" else "get_enabled_cost_center"
    if dag_run.conf['compensation_plan_effective_date']:
        if rail.result("get_can_update_status") and rail.result("get_can_update_status")['effective_date_update']:
            return
        compensation_date_to_apply = convert_to_date(
            dag_run.conf['compensation_plan_effective_date'], "%d-%m-%Y")
        cost_center_list.append({
            dict_key: {
                "uri": rail.result(f"{task_name}")["yes_uri"],
                "parentUri": None,
                "name": None
            },
            "effectiveDate": {
                "year": compensation_date_to_apply.year,
                "month": compensation_date_to_apply.month,
                "day": compensation_date_to_apply.day
            }
        })


def add_no(cost_center_list, dict_key):
    conf = get_conf()
    task_name = "get_enabled_business_units" if dict_key == "division" else "get_enabled_cost_center"
    if conf['expected_end_date']:
        if rail.result("get_can_update_status") and rail.result("get_can_update_status")['end_date_update']:
            return

        end_date_to_apply = convert_to_date(rail.result('process_expected_end_date')[
                                            'expected_end_date'], "%d-%m-%Y")
        cost_center_list.append({
            dict_key: {
                "uri": rail.result(f"{task_name}")["no_uri"],
                "parentUri": None,
                "name": None
            },
            "effectiveDate": {
                "year": end_date_to_apply.year,
                "month": end_date_to_apply.month,
                "day": end_date_to_apply.day
            }
        })


def get_allowance_log_message(dag_run):
    update_status = rail.result(
        "get_can_update_status")
    if update_status is not None:
        if dag_run.conf['compensation_plan_effective_date'] and dag_run.conf['expected_end_date']:
            if not update_status['effective_date_update'] and not update_status['end_date_update']:
                return "Allowance start and end schedule added"
            if not update_status['effective_date_update'] and update_status['end_date_update']:
                return "Start date allowance schedule added, End date allowance schedule is already present"
            if update_status['effective_date_update'] and not update_status['end_date_update']:
                return "Start date allowance schedule is already present, End date allowance is added"

        if dag_run.conf['compensation_plan_effective_date']:
            return "Start date allowance added"
        if dag_run.conf['expected_end_date']:
            return "End date allowance added"

    return ""


def get_classification_filtered_response(response, dag_run):
    return rail.find_first_by_attr_and_get_attr(response.json()['d'], "displayText", (dag_run.conf['compensation_element'].split("-")[-1]).strip())


def get_classification_can_update_status(dag_run):
    effective_date = None
    if dag_run.conf['compensation_plan_effective_date']:
        effective_date = (rail.find_first_by_attr_and_get_attr(rail.result('get_schedule_for_user'),
                                "classifications_display_text", rail.result("get_enabled_classifications")['displayText'], 'effective_date')
                          == dag_run.conf['compensation_plan_effective_date'])

    # None = not applicable
    # True = No need to update
    # False = Update
    return{
        "effective_date_update": None if effective_date is None else effective_date,
    }


def get_user_import_conf(item, dag_run):
    def is_user_manager():
        collection_to_use = rail.result('get_delta_records') if rail.result(
            'get_reference_variable') else rail.result('get_valid_input_data')
        return rail.find_first_by_attr_and_get_attr(get_data_from_document(collection_to_use), "manager_id", item['employee_id'])
    return {
        "file_name": dag_run.conf['file_name'],
        "employee_id": item['employee_id'],
        "party_id": item['party_id'],
        "guid": item['guid'],
        "staff_code": item['staff_code'],
        "active_status": item['active_status'],
        "id": item['id'],
        "work_email": str(item['work_email']),
        "firstname": item['first_name'],
        "lastname": item['last_name'],
        "hire_date": item['hire_date'],
        "employee_type": item['employee_type'],
        "time_type": item['time_type'],
        "management_level": item['management_level'],
        "line_of_service": item['line_of_service'],
        "manager_id": item['manager_id'],
        "costcenter_id": item['cost_center_id'],
        "costcenter_name": item['cost_center_level_1'],
        "costcenter_level_1": item['cost_center_level_1'],
        "costcenter_level_2": item['cost_center_level_2'],
        "costcenter_level_3": item['cost_center_level_3'],
        "costcenter_level_4": item['cost_center_level_4'],
        "location_level_1": item['location_level_1'],
        "location_level_2": item['location_level_2'],
        "location_level_3": item['location_level_3'],
        "location_level_4": item['location_level_4'],
        "classification": item['classification'],
        "md5": item['md5'],
        "party_id_customfield_uri": rail.result('get_all_user_custom_fields')['party_id_uri'],
        "management_level_customfield_uri": rail.result('get_all_user_custom_fields')['management_level_uri'],
        "line_of_service_customfield_uri": rail.result('get_all_user_custom_fields')['line_of_services_uri'],
        "local_staff_code_customfield_uri": rail.result('get_all_user_custom_fields')['local_staff_code_uri'],
        "manager_user": bool(is_user_manager()),
        "log": rail.result('create_user_import_log'),
        "supervisor_log": rail.result("create_supervisor_processing_log")
    }


def get_entries_from_user_mapper(dag_run):
    return rail.find_first_by_attr_and_get_attr(user_mapping, "employeetype", dag_run.conf['employee_type'] + ' - ' + dag_run.conf['time_type'])


def get_costcenter_fullpath(dag_run):
    return "/ ".join(filter(None, ["PwC", str(dag_run.conf['costcenter_level_4']),
                                   str(dag_run.conf['costcenter_level_3']), str(dag_run.conf['costcenter_level_2']), str(dag_run.conf['costcenter_level_1'])]))


def get_activity_uris(dag_run, method_caller, parent):
    full_path = get_costcenter_fullpath(dag_run)
    cost_center = {
        "cost_center_fullpath": full_path,
        "length": len(full_path.split("/ ")),
        "parent_department": full_path.split("/ ")[2]
    }

    def can_add_uri(item):
        return (cost_center['parent_department'].strip() == "PIC"
                and cost_center['length'] > 2 and item['displayText'] == "Firm Administration (PIC Only)")

    to_ignore = ["3. Annual Leave", "4. Personal (Sick) Leave", "5. Birthday Leave", "6. Leave - Compassionate", "7. Jury Duty",
                 "8. Leave Social Impact", "9.Leave - Long Service", "10. Leave - Study/Exam", "13. Leave - Purchased Additional Annual",
                 "11. Leave without pay", "12. Leave - Public Holidays", "14. Leave - Defence Force (Paid)", "15. Leave Unpaid Sick-Short Term",
                 "16. Leave - Community Day - PwC Foundation", "17. Leave - Defence Force (Unpaid)", "18. Leave Natural Disaster"]

    # for add user
    if parent == "add_user":
        if method_caller == "non_salaried_self_employed":
            return [item['uri'] for item in rail.result("get_all_activities") if can_add_uri(item) or item['displayText'] not in to_ignore]

        if method_caller == "regular_fixed_term":
            return [item['uri'] for item in rail.result("get_all_activities") if can_add_uri(item) or item['displayText']
                    != "Firm Administration (PIC Only)"]

        return []

    # for update user
    if method_caller == "non_salaried_self_employed":
        return [{
            "uri": item['uri']
        } for item in rail.result("get_all_activities") if can_add_uri(item) or item['displayText'] not in to_ignore]

    return [{
        "uri": item['uri']
    } for item in rail.result("get_all_activities") if can_add_uri(item) or item['displayText'] != "Firm Administration (PIC Only)"]


def get_mapper_classification_records(dag_run):
    return rail.find_first_by_attr_and_get_attr(classification_records, 'grade', dag_run.conf['classification'])


def get_mapper_timezone_for_location(dag_run):
    return rail.find_first_by_attr_and_get_attr(australia_timezone, "locationlevel2", dag_run.conf['location_level_2'])


def get_location_full_path(dag_run, location_index):
    locations_received = [str(dag_run.conf['location_level_4']), str(dag_run.conf['location_level_3']),
                          str(dag_run.conf['location_level_2']), str(dag_run.conf['location_level_1'])]
    return "/ ".join(filter(None, (locations_received[:location_index])))


def search_location_group2_by_name_code_response_filter(response, dag_run, location_index):
    response = response.json()['d']
    full_path = get_location_full_path(dag_run, location_index)

    return list(filter(lambda x: x['status'] in [True, 'True'] and x['full_path'] == full_path, map(lambda item: {
        "uri": item['cells'][0]['uri'] if item['cells'][0]['dataType'] != null_urn else None,
        "name": item['cells'][0]['textValue'] if item['cells'][0]['dataType'] != null_urn else None,
        "code": item['cells'][0]['textValue'] if item['cells'][0]['dataType'] != null_urn else None,
        "status": item['cells'][1]['textValue'] if item['cells'][1]['dataType'] != null_urn else None,
        "full_path":  "/ ".join([x['textValue'] for x in item['cells'][-1]['cellCollection']])
                if item['cells'][-1]['cellCollection'] else None
                }, response['rows'])))


def search_department_group_by_name_response_filter(response, dag_run):
    response = response.json()['d']
    if not response:
        return []
    full_path = get_costcenter_fullpath(dag_run)
    return list(filter(lambda x: x['status'] in [True, 'True'] and x['full_path'] == full_path, map(lambda item: {
        "uri": item['cells'][1]['uri'] if item['cells'][1]['dataType'] != null_urn else None,
        "name": item['cells'][1]['textValue'] if item['cells'][1]['dataType'] != null_urn else None,
        "code": item['cells'][0]['textValue'] if item['cells'][0]['dataType'] != null_urn else None,
        "status": item['cells'][2]['textValue'] if item['cells'][2]['dataType'] != null_urn else None,
        "full_path":  "/ ".join([x['textValue'] for x in item['cells'][-1]['cellCollection']]) if item['cells'][-1]['cellCollection'] else None
    }, response['rows'])))


# update_user
def get_update_ignore_reason(dag_run):
    ignore_reason = []
    if not dag_run.conf['employee_id']:
        ignore_reason.append("User employeeID not present")
    if not dag_run.conf['firstname']:
        ignore_reason.append("First_Name not present")
    if not dag_run.conf['lastname']:
        ignore_reason.append("Last_Name not present")
    if not dag_run.conf['hire_date']:
        ignore_reason.append("Hire date not present")
    if not dag_run.conf['guid']:
        ignore_reason.append("GuID not present")
    if not dag_run.conf['employee_type']:
        ignore_reason.append("Employee type not present")
    if not dag_run.conf['active_status']:
        ignore_reason.append("Employee status not present")

    if ignore_reason:
        return ",".join(ignore_reason)
    return []


def can_update_user(dag_run):
    if get_update_ignore_reason(dag_run):
        return False
    return True


def bool_get_can_update(dag_run, input_value, search_value):
    if dag_run.conf[input_value] and (dag_run.conf[input_value] != rail.find_first_by_attr_and_get_attr(
            rail.result("get_user_details")[0]['userDetails']['customFieldValues'], 'customField.displayText', search_value, 'text')):
        return True
    return False


def get_manager_logs(dag_run, logs):
    is_supervisor_updated = None
    is_supervisor_searched = rail.get_current_context()['dag_run'].get_task_instance(
        "search_supervisor_in_replicon").current_state().lower() == "success"
    if not dag_run.conf['manager_id']:
        return
    if dag_run.conf['manager_id'] in [dag_run.conf['guid'], dag_run.conf['employee_id']]:
        logs.append(
            "Supervisor not updated for since user's supervisor can not be same as the user")
    # pylint: disable=line-too-long
    elif is_supervisor_searched and len(rail.result('search_supervisor_in_replicon')) > 1:
        logs.append(
            f"""Supervisor not assigned for user "{dag_run.conf['firstname']} {dag_run.conf['lastname']}" as multiple users have same Employee ID: {dag_run.conf['manager_id']}""")
    elif is_supervisor_searched and (not rail.result('search_supervisor_in_replicon')):
        logs.append(
            f"Supervisor not assigned since supervisor is not found in replicon {dag_run.conf['manager_id']}"
        )
    elif ((not rail.result('search_supervisor_in_replicon')[0]['enabled']) if rail.result('search_supervisor_in_replicon') else []):
        logs.append(
            f"Supervisor not assigned since supervisor is disabled in Replicon {dag_run.conf['manager_id']}"
        )
    elif dag_run.conf['action'].lower() == 'add':
        return
    else:
        is_supervisor_updated = rail.get_current_context()['dag_run'].get_task_instance(
            "update_supervisor_schedule_for_user").current_state().lower() == "success"

    if (bool(rail.result('get_user_details')) and bool(rail.result('get_user_details')[0]['userDetails']['supervisor']) and is_supervisor_updated):
        logs.append("Supervisor updated")

    if (bool(rail.result('get_user_details')) and (not bool(rail.result('get_user_details')[0]['userDetails']['supervisor'])) and is_supervisor_updated):
        logs.append("Initial supervisor added")


def get_update_user_custom_log_message(dag_run, logs):
    if dag_run.conf['active_status'] and rail.result('get_user_details')[0]['userDetails']['isEnabled'] in ['False', False]:
        logs.insert(0, "User enabled in Replicon and end date removed")
    if bool_get_can_update(dag_run, 'management_level', "Management Level") and \
            rail.result('management_level_task.get_managementlevel_enabled_dropdown_option'):
        logs.append("Management level field updated")

    is_supervisor_updated = rail.get_current_context()['dag_run'].get_task_instance(
        "update_supervisor_schedule_for_user").current_state().lower() == "success"

    if (bool(rail.result('get_user_details')) and bool(rail.result('get_user_details')[0]['userDetails']['supervisor']) and is_supervisor_updated):
        logs.append("Supervisor updated")

    if (bool(rail.result('get_user_details')) and (not bool(rail.result('get_user_details')[0]['userDetails']['supervisor'])) and is_supervisor_updated):
        logs.append("Initial supervisor added")


def user_import_cost_center_response_filter(response):

    response = response.json()['d']
    if not response['rows']:
        return None

    object_list_type_uri = 'urn:replicon:list-type:object'

    return list(map(lambda item: {
        "replicon_company_codes_name": rail.find_first_by_attr_and_get_attr(item['cells'], 'dataType', object_list_type_uri, 'textValue'),
        "replicon_company_codes_uri": rail.find_first_by_attr_and_get_attr(item['cells'], 'dataType', object_list_type_uri, 'uri'),
        "full_path": "/ ".join([x['textValue'] for x in item['cells'][-1]['cellCollection']])
                    if item['cells'][-1]['cellCollection'] else None,
                    "length": str(len(item['cells'][1]['cellCollection']))
                    }, response['rows']))


def user_import_location_response_filter(response):
    object_list_type_uri = 'urn:replicon:list-type:object'

    response = response.json()['d']
    if not response['rows']:
        return None

    return list(map(lambda item: {
        "replicon_location_name": rail.find_first_by_attr_and_get_attr(item['cells'], 'dataType', object_list_type_uri, 'textValue'),
        "replicon_location_uri": rail.find_first_by_attr_and_get_attr(item['cells'], 'dataType', object_list_type_uri, 'uri'),
        "full_path": " / ".join([x['textValue'] for x in item['cells'][-1]['cellCollection']])
                    if item['cells'][-1]['cellCollection'] else None,
                    "length": str(len(item['cells'][1]['cellCollection']))
                    }, response['rows']))


def get_custom_fields_payload(custom_field_uri, custom_field_value):
    return {
        "customField": {
            "uri": custom_field_uri,
        },
        "text": custom_field_value,
    }


def get_update_custom_field_payload(dag_run, log_list=None):
    custom_fields = []

    if bool_get_can_update(dag_run, 'line_of_service', "Line of Services"):
        custom_fields.append(get_custom_fields_payload(
            dag_run.conf["line_of_service_customfield_uri"], dag_run.conf['line_of_service']))
    if bool_get_can_update(dag_run, 'party_id', "Party ID"):
        log_list.append("Party ID field updated")
        custom_fields.append(get_custom_fields_payload(
            dag_run.conf["party_id_customfield_uri"], dag_run.conf['party_id']))
    if bool_get_can_update(dag_run, 'staff_code', "Local Staff ID"):
        log_list.append("Staff Code field updated")
        custom_fields.append(get_custom_fields_payload(
            dag_run.conf["staff_code_customfield_uri"], dag_run.conf['staff_code']))

    return custom_fields


def get_add_user_location_update_payload():
    return {
        "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:replace-entire-schedule",
        "replacementLocationSchedule": [
            {
                "location": {
                    "uri": rail.result('search_location_group_by_name')[0]['uri'],
                    "parentUri": None,
                    "name": None
                },
                "effectiveDate": None
            }
        ],
        "updateLocationScheduleOverDateRange": None
    }


def get_user_schedule_update_payload():
    return{
        "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:replace-entire-schedule",
        "replacementSchedule": [
          {
              "schedulePolicy": {
                  "officeScheduleUri": None,
                  "name": None,
                  "officeSchedule": {
                      "officeScheduleUri": rail.result('get_all_office_schedule'),
                      "name": None
                  },
                  "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
              },
              "effectiveDate": None
          }
        ],
        "updateScheduleOverDateRange": None
    }


def get_department_schedule_update_payload():
    return {
        "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:replace-entire-schedule",
        "replacementDepartmentGroupSchedule": [
            {
                "departmentGroup": {
                    "uri": rail.result('search_department_group_by_name')[0]['uri'],
                    "parent": None,
                    "name": None,
                    "parameterCorrelationId": None
                },
                "effectiveDate": None
            }
        ],
        "updateDepartmentGroupScheduleOverDateRange": None
    }


def get_service_center_schedule_payload():
    if rail.result('search_service_center'):
        return {
            "userServiceCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:replace-entire-schedule",
            "replacementServiceCenterSchedule": [
                {
                    "serviceCenter": {
                        "uri": rail.result('search_service_center')[0]['uri'],
                        "parentUri": None,
                        "name": None
                    },
                    "effectiveDate": None
                }
            ],
            "updateServiceCenterScheduleOverDateRange": None
        }
    return None


def get_payroll_rate_payload():
    if rail.result("get_mapper_classification_records") and rail.result("get_mapper_classification_records")["hourly rate"]:
        return {
            "initialHourlyRate": {
                "amount": rail.result("get_mapper_classification_records")["hourly rate"],
                "currency": {
                    "uri": rail.result("get_enabled_currencies")
                }
            },
            "scheduleEntries": []
        }
    return None


def get_calender_payload():
    return{
        "holidayCalendar": {
            "uri": rail.result('get_all_holiday_calender')
        }
    }


def get_timezone_payload():
    return {
        "userTimeZoneModificationOptionUri": "urn:replicon:user-time-zone-modication-option:use-specified-time-zone",
        "timezone": {
            "uri": rail.result('get_timezone_uri')
        }
    }


def get_data_access_scope_payload():
    return {
        "policyDataAccessScopes": [
            {
                "policyUri": "urn:replicon:policy:time-off",
                "locations": [
                    {
                        "location": {
                            "uri": rail.result('search_location_group2_by_name_code')[0]['uri']
                        },
                        "groupSpecificationModeUri": None,
                        "groupDescendantModeUri": "urn:replicon:data-access-scope-group-descendant-mode:do-not-include-descendants"
                    }
                ],
                "divisions": [],
                "costCenters": [],
                "serviceCenters": [],
                "departmentGroups": [],
                "employeeTypeGroups": []
            }
        ]
    }


def get_add_custom_field_payload(dag_run):
    custom_fields = []
    if dag_run.conf['line_of_service']:
        custom_fields.append(get_custom_fields_payload(
            dag_run.conf["line_of_service_customfield_uri"], dag_run.conf['line_of_service']))
    if dag_run.conf['party_id']:
        custom_fields.append(get_custom_fields_payload(
            dag_run.conf["party_id_customfield_uri"], dag_run.conf['party_id']))
    if dag_run.conf['staff_code']:
        custom_fields.append(get_custom_fields_payload(
            dag_run.conf["staff_code_customfield_uri"], dag_run.conf['staff_code']))
    return custom_fields

def do_format_logs():
    user_import_log = rail.load_all_records(
        rail.result("create_user_import_log"))
    unique_users = list(
        set(map(lambda item: item['properties'].get(
            "guid", ''), user_import_log))
    )

    def get_details(user_logs):
        return ";".join(list(filter(bool, (set(map(lambda x: x['properties'].get('details'), user_logs))))))

    def get_status(user_logs):
        available_status = list(
            map(lambda log: log['properties']['status'], user_logs))
        if "Error" in available_status:
            return "Error"
        if "Exception" in available_status:
            return "Exception"
        return "Success"
    logs = []
    # pylint: disable= cell-var-from-loop
    for guid in unique_users:
        user_logs = list(
            filter(lambda x: x['properties'].get(
                'guid', '') == guid, user_import_log)
        )
        if len(user_logs) > 0:
            first = user_logs[0]
            logs.append(
                {
                    "guid": guid,
                    "action": first['properties']['action'],
                    "status": get_status(user_logs),
                    "ecid": first['ecid'],
                    "details": get_details(user_logs)
                }
            )
    return logs
