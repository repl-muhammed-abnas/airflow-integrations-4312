import itertools
from datetime import datetime
from pendulum import now
from hashlib import md5
import json
import rail

null = None

DATE_FORMAT = "%Y-%m-%d"


def get_job_start_timestamps(config):
    _now = now(config.time_zone)
    return {
        'email_timestamp': _now.strftime(config.EMAIL_TIMESTAMP_FORMAT),
        'run_date':  _now.strftime(config.DATE_FORMAT),
        'log_timestamp': _now.strftime(config.LOG_TIMESTAMP)
    }


def get_employee_details_callable(response):
    if not response or not isinstance(response, dict):
        return {}
    for k, v in response.items():
        if v == None:
            response[k] = ''
    return response


def get_process_each_user_payload_dag_ids(parallel_count):
    process_each_user_dag_ids = list(itertools.chain(
        *list(map(lambda x: (rail.result(
            f'trigger_process_each_changed_user_{x+1}') if rail.result(
            f'trigger_process_each_changed_user_{x+1}') else []), range(parallel_count)))))

    return process_each_user_dag_ids


def to_datetime(date, date_format=DATE_FORMAT):
    if isinstance(date, dict):
        return datetime(day=date['day'], month=date['month'], year=date['year'])
    elif isinstance(date, str):
        return datetime.strptime(date, date_format)
    return date


def get_daydiff_data(run_date, list_table_data, date_format):
    today = to_datetime(run_date, date_format).date()
    for table_record in list_table_data:
        if table_record.get('customEffectiveDate3'):
            effective_date = to_datetime(table_record.get(
                'customEffectiveDate3'), date_format).date()
            table_record['daydiff'] = (today - effective_date).days
        else:
            table_record['daydiff'] = 1000

    return list_table_data


def get_min_effectivedate_entry(table_records):
    entry_with_minimum_daydiff = min(
        table_records, key=lambda x: x['daydiff']) if table_records else ""
    return {
        'min_daydiff_effective_date': entry_with_minimum_daydiff['customEffectiveDate3'] if entry_with_minimum_daydiff else '',
        'min_daydiff_entry': entry_with_minimum_daydiff if entry_with_minimum_daydiff else []
    }


def validate_employee_data(employee_data, program_info_table_record, mandatory_fields):
    """Validate required employee fields"""
    missing_fields = []

    for field, field_label in mandatory_fields.items():
        if not employee_data.get(field):
            missing_fields.append(field_label)

    if not (program_info_table_record) or not (program_info_table_record[0].get('customDirectIndirect')):
        missing_fields.append("Labor Level")

    if missing_fields:
        return {
            'is_valid': False,
            'validation_details': f"One or more mandatory field value is missing ({', '.join(missing_fields)})"
        }

    # Validate email format
    email = employee_data.get('workEmail', '')
    if '@' not in email:
        return {
            'is_valid': False,
            'validation_details': "Invalid email address "
        }

    return {
        'is_valid': True,
        'validation_details': ""
    }


def get_record_with_md5(user_record, program_info_table_record_min_daydiff):
    data_fields_for_md5 = {
        "customLaborLevel": program_info_table_record_min_daydiff.get('customDirectIndirect', ''),
        "customTelephonyID": user_record.get('customTelephonyID', ''),
        "customTelephonySystem": user_record.get('customTelephonySystem', ''),
        "middleName": user_record.get('middleName', ''),
        "workEmail": user_record.get('workEmail', ''),
        "employeeNumber": user_record.get('employeeNumber', ''),
        "homeEmail": user_record.get('homeEmail', ''),
        "status":  user_record.get('status', ''),
        "firstName":  user_record.get('firstName', ''),
        "lastName":  user_record.get('lastName', ''),
        "jobTitle":  user_record.get('jobTitle', ''),
        "location": user_record.get('location', ''),
        "supervisorId":  user_record.get('supervisorId', ''),
        "department":  user_record.get('department', ''),
        "supervisor":  user_record.get('supervisor', ''),
        "division": user_record.get('division', ''),
        "id": user_record.get('id', ''),
        "customTrainingBillingType": user_record.get('customTrainingBillingType', ''),
        "customDirectIndirect": program_info_table_record_min_daydiff.get('customDirectIndirect', ''),
        "customProjectName": program_info_table_record_min_daydiff.get('customProjectName', ''),
        "customClientName": program_info_table_record_min_daydiff.get('customClientName', ''),
    }

    values_list = []

    for value in data_fields_for_md5.values():
        values_list.append(str(value))

    data_fields_for_md5.update(
        {'md5': md5(("_".join(values_list)).encode()).hexdigest()})

    return data_fields_for_md5


def md5_value_check(matching_reference_file_record, new_md5_value):
    ref_record_md5 = rail.load_all_records(
        matching_reference_file_record)[0].get('md5', '')

    return new_md5_value != ref_record_md5


def get_current_value_from_schedule_list_for_user(user_schedule, scrpit_name, required_key,  effective_date, config):
    current_value = null
    initial_value = null
    current_min_day_diff = "*"
    if 'urn' in json.dumps(user_schedule):
        for item in user_schedule:

            if not (item['startDate'] if scrpit_name == 'holidayCalendar' else item['effectiveDate']):
                initial_value = item
                continue

            daydiff = (datetime.strptime(effective_date, config.DATE_FORMAT).date()) - to_datetime(
                (item['startDate'] if scrpit_name == 'holidayCalendar' else item['effectiveDate']), config.DATE_FORMAT).date()

            # ignore the future ones
            if daydiff.days < 0:
                continue

            if current_min_day_diff == "*":
                current_value = item
                current_min_day_diff = daydiff
                continue

            if current_min_day_diff > daydiff:
                current_min_day_diff = daydiff
                current_value = item

    return current_value[scrpit_name][required_key] if current_value else (initial_value[scrpit_name][required_key] if initial_value else '')


def get_payrule_to_assign(dag_run, config):
    pay_rule = ''
    matching_entries_based_on_location = list(filter(
        lambda x: x["location"] == dag_run.conf['location'] and x["overtime"] == "Non-exempt", config.PAYRULE_MAPPER))
    if len(matching_entries_based_on_location) > 0:
        pay_rule = matching_entries_based_on_location[0]['payrule']

    matching_entries_based_on_location_overtime = list(filter(
        lambda x: x["location"] == dag_run.conf['location'] and x["overtime"] == dag_run.conf['overtime'], config.PAYRULE_MAPPER))

    if len(matching_entries_based_on_location_overtime) > 0:
        pay_rule = matching_entries_based_on_location_overtime[0]['payrule']

    return pay_rule


def get_holiday_calendar_timezone_to_assign(dag_run, config):
    holiday_calendar = ''
    time_zone = ''
    matching_entries_based_on_location = list(filter(
        lambda x: x["location"] == dag_run.conf['location'], config.HOLIDAY_AND_TIMEZONE_MAPPER))

    if len(matching_entries_based_on_location) > 0:
        holiday_calendar = matching_entries_based_on_location[0]['holidaycalendar']
        time_zone = matching_entries_based_on_location[0]['iananame']

    return {
        'holiday_calendar': holiday_calendar,
        'time_zone': time_zone
    }


def modification_payload_for_fields_present_31_36(dag_run, config):
    modifications = {}
    if dag_run.conf.get('division'):
        modifications.update({
            "departmentGroupSchedule": [{
                "dateRange": null,
                "item": {
                    "parent": {
                        "name": "Transparent BPO"
                    },
                    "name": dag_run.conf['division']
                }
            }]
        })

    if dag_run.conf.get('customTrainingBillingType'):
        modifications.update({
            "divisionSchedule": [
                {
                    "dateRange": {
                        "startDate": rail.parse_date(dag_run.conf['hireDate'], config.DATE_FORMAT)
                    },
                    "item": {
                        "name": dag_run.conf.get('customTrainingBillingType')
                    }
                }
            ]
        })

    if dag_run.conf.get('employmentHistoryStatus'):
        modifications.update({
            "employeeTypeGroupSchedule": [{
                "dateRange": null,
                "item": {
                    "name": dag_run.conf.get('employmentHistoryStatus')
                }
            }]
        })

    return modifications


def get_text_dd_custom_fields_to_update_payload(
        dag_run, dropdown_options_for_telephony_system, dropdown_options_for_overtime, supervisor_creation=None):

    custom_fields_payload = []
    required_custom_field_uris = dag_run.conf['custom_field_uris']
    custom_fields_payload.append({
        "value": {
            "customField": {
                "uri": required_custom_field_uris['bamboo_hr_id_cf_uri'],
                "name": null
            },
            "text": dag_run.conf.get('id') if not (supervisor_creation) else dag_run.conf.get('subordinate_details').get('id')
        }
    })

    if dag_run.conf.get('customTelephonySystem') and required_custom_field_uris['telephony_system_cf_uri']:
        dropdown_option_uri_telephony_system = next((
            option['uri'] for option in dropdown_options_for_telephony_system if (option['displayText'] == dag_run.conf['customTelephonySystem'] and str(
                option['isEnabled']).lower() == 'true')), null)
        if dropdown_option_uri_telephony_system:
            custom_fields_payload.append({
                "value": {
                    "customField": {
                        "uri": required_custom_field_uris['telephony_system_cf_uri'],
                        "name": null
                    },
                    "dropDownOption": {
                        "uri": dropdown_option_uri_telephony_system,
                        "name": null
                    }
                }
            })

    if not (dag_run.conf.get('customTelephonySystem')) and required_custom_field_uris['telephony_system_cf_uri']:
        dropdown_option_uri_telephony_system = next((option['uri'] for option in dropdown_options_for_telephony_system if (option['displayText'] == "None" and str(
            option['isEnabled']).lower() == 'true')), null)
        if dropdown_option_uri_telephony_system:
            custom_fields_payload.append({
                "value": {
                    "customField": {
                        "uri": required_custom_field_uris['telephony_system_cf_uri'],
                        "name": null
                    },
                    "dropDownOption": {
                        "uri": dropdown_option_uri_telephony_system,
                        "name": null
                    }
                }
            })

    if dag_run.conf.get('customTelephonyID') and required_custom_field_uris['telephony_id_cf_uri']:
        custom_fields_payload.append({
            "value": {
                "customField": {
                    "uri": required_custom_field_uris['telephony_id_cf_uri'],
                    "name": null
                },
                "text": dag_run.conf.get('customTelephonyID')
            }
        })

    if dag_run.conf.get('jobTitle') and required_custom_field_uris['job_title_cf_uri']:
        custom_fields_payload.append({
            "value": {
                "customField": {
                    "uri": required_custom_field_uris['job_title_cf_uri'],
                    "name": null
                },
                "text": dag_run.conf.get('jobTitle')
            }
        })

    if not (supervisor_creation):
        if dag_run.conf.get('overtime') and required_custom_field_uris['overtime_cf_uri']:
            dropdown_option_uri_overtime = next((
                option['uri'] for option in dropdown_options_for_overtime if (option['displayText'] == dag_run.conf.get('overtime') and str(
                    option['isEnabled']).lower() == 'true')), null)
            if dropdown_option_uri_overtime:
                custom_fields_payload.append({
                    "value": {
                        "customField": {
                            "uri": required_custom_field_uris['overtime_cf_uri'],
                            "name": null
                        },
                        "dropDownOption": {
                            "uri": dropdown_option_uri_overtime,
                            "name": null
                        }
                    }
                })

        if dag_run.conf.get('customDirectIndirect') and required_custom_field_uris['labor_level_cf_uri']:
            custom_fields_payload.append({
                "value": {
                    "customField": {
                        "uri": required_custom_field_uris['labor_level_cf_uri'],
                        "name": null
                    },
                    "text": dag_run.conf.get('customDirectIndirect')
                }
            })

        if dag_run.conf.get('customClientName') and required_custom_field_uris['client_name_cf_uri']:
            custom_fields_payload.append({
                "value": {
                    "customField": {
                        "uri": required_custom_field_uris['client_name_cf_uri'],
                        "name": null
                    },
                    "text": dag_run.conf.get('customClientName')
                }
            })

        if dag_run.conf.get('customProjectName') and required_custom_field_uris['project_name_cf_uri']:
            custom_fields_payload.append({
                "value": {
                    "customField": {
                        "uri": required_custom_field_uris['project_name_cf_uri'],
                        "name": null
                    },
                    "text": dag_run.conf.get('customProjectName')
                }
            })

    if supervisor_creation:
        if (dag_run.conf.get('ssn') or dag_run.conf.get('customInternationalSSN')) and required_custom_field_uris['ssn_cf_uri']:
            custom_fields_payload.append({
                "value": {
                    "customField": {
                        "uri": required_custom_field_uris['ssn_cf_uri'],
                        "name": null
                    },
                    "text": dag_run.conf.get('ssn') or dag_run.conf.get('customInternationalSSN')
                }
            })

        if dag_run.conf.get('customLaborLevel') and required_custom_field_uris['labor_level_cf_uri']:
            custom_fields_payload.append({
                "value": {
                    "customField": {
                        "uri": required_custom_field_uris['labor_level_cf_uri'],
                        "name": null
                    },
                    "text": dag_run.conf.get('customLaborLevel')
                }
            })

        if dag_run.conf.get('department') and required_custom_field_uris['department_cf_uri']:
            custom_fields_payload.append({
                "value": {
                    "customField": {
                        "uri": required_custom_field_uris['department_cf_uri'],
                        "name": null
                    },
                    "text": dag_run.conf.get('department')
                }
            })

    return custom_fields_payload


def get_relevant_group_assignments_to_update(dag_run, config):
    modifications = {}
    updates = []
    current_assignments = rail.result(
        'get_user_current_group_assignments')

    if dag_run.conf['employmentHistoryStatus']:
        if current_assignments['current_employeetype_name'] and (
                current_assignments['current_employeetype_name'] != dag_run.conf['employmentHistoryStatus']):
            updates.append(
                "Employment History Status group schedule updated")
            modifications.update({
                "employeeTypeGroupSchedule": [
                    {
                        "dateRange": {
                            "startDate": rail.parse_date(dag_run.conf['job_run_date'], config.DATE_FORMAT),
                        },
                        "item": {
                            "name": dag_run.conf['employmentHistoryStatus']
                        }
                    }
                ]
            })

        elif not (current_assignments['current_employeetype_name']):
            updates.append(
                "Employment History Status group assignmened")
            modifications.update({
                "employeeTypeGroupSchedule": [
                    {
                        "dateRange": null,
                        "item": {
                            "name": dag_run.conf['employmentHistoryStatus']
                        }
                    }
                ]
            })

    if dag_run.conf['division']:
        if current_assignments['current_department_name'] and (
                current_assignments['current_department_name'] != dag_run.conf['division']):
            updates.append("Department group schedule updated")
            modifications.update({
                "departmentGroupSchedule": [
                    {
                        "dateRange": {
                            "startDate": rail.parse_date(dag_run.conf['job_run_date'], config.DATE_FORMAT),
                        },
                        "item": {
                            "parent": {
                                "name": "Transparent BPO"
                            },
                            "name": dag_run.conf['division']
                        }
                    }
                ]
            })

        elif not (current_assignments['current_department_name']):
            updates.append("Department group assigned")
            modifications.update({
                "departmentGroupSchedule": [
                    {
                        "dateRange": null,
                        "item": {
                            "parent": {
                                "name": "Transparent BPO"
                            },
                            "name": dag_run.conf['division']
                        }
                    }
                ]
            })

    # Pay rule assignment
    if dag_run.conf['location']:
        pay_rule = ''
        matching_entries_based_on_location = list(filter(
            lambda x: x["location"] == dag_run.conf['location'] and x["overtime"] == "Non-exempt", config.PAYRULE_MAPPER))
        if len(matching_entries_based_on_location) > 0:
            pay_rule = matching_entries_based_on_location[0]['payrule']

        matching_entries_based_on_location_overtime = list(filter(
            lambda x: x["location"] == dag_run.conf['location'] and x["overtime"] == dag_run.conf['overtime'], config.PAYRULE_MAPPER))

        if len(matching_entries_based_on_location_overtime) > 0:
            pay_rule = matching_entries_based_on_location_overtime[0]['payrule']

        if pay_rule:
            if dag_run.conf['currentPayRule'] and pay_rule != dag_run.conf['currentPayRule']:
                updates.append("Pay rule updated")
                modifications.update({
                    "payRuleSchedule": [
                        {
                            "dateRange": {
                                "startDate": rail.parse_date(dag_run.conf['job_run_date'], config.DATE_FORMAT),
                            },
                            "item": {
                                "name": pay_rule
                            }
                        }
                    ]
                })
            elif not(dag_run.conf['currentPayRule']):
                updates.append("Pay rule assigned")
                modifications.update({
                    "payRuleSchedule": [
                        {
                            "dateRange": null,
                            "item": {
                                "name": pay_rule
                            }
                        }
                    ]
                })

    if dag_run.conf['customTrainingBillingType']:
        if current_assignments['current_division_name'] and (
                current_assignments['current_division_name'] != dag_run.conf['customTrainingBillingType']):
            updates.append("Division group schedule updated")
            modifications.update({
                "divisionSchedule": [
                    {
                        "dateRange": {
                            "startDate": rail.parse_date(dag_run.conf['job_run_date'], config.DATE_FORMAT),
                        },
                        "item": {
                            "name": dag_run.conf['customTrainingBillingType']
                        }
                    }
                ]
            })

        elif not (current_assignments['current_division_name']):
            updates.append("Division group assigned")
            modifications.update({
                "divisionSchedule": [
                    {
                        "dateRange": {
                            "startDate": rail.result(
                                'get_user_details')['userDetails']['employmentDateRange']['startDate'],
                        },
                        "item": {
                            "name": dag_run.conf['customTrainingBillingType']
                        }
                    }
                ]
            })

    if dag_run.conf['location']:
        holiday_calendar = ''
        iananame = ''
        matching_entries = list(filter(
            lambda x: x["location"] == dag_run.conf['location'], config.HOLIDAY_AND_TIMEZONE_MAPPER))
        if len(matching_entries) > 0:
            holiday_calendar = matching_entries[0]['holidaycalendar']
            iananame = matching_entries[0]['iananame']

        if holiday_calendar and rail.result('get_user_details').get('holidayCalendar') and holiday_calendar != rail.result(
                'get_user_details').get('holidayCalendar').get('name', ''):

            updates.append("Holiday calendar updated")
            modifications.update({
                "holidayCalendar": {
                    "value": {
                        "name": holiday_calendar
                    }
                }
            })

        if iananame and rail.result('get_user_details').get('timeZone') and iananame != rail.result('get_user_details').get('timeZone').get('ianaName', ''):
            updates.append("Time zone updated")
            modifications.update({
                "timeZone": {
                    "value": {
                        "IANAName": iananame
                    }
                }
            })

        if current_assignments['current_location_name'] and (current_assignments['current_location_name'] != dag_run.conf['location']):
            updates.append("Location group updated")
            modifications.update({
                "locationSchedule": [
                    {
                        "dateRange": {
                            "startDate": rail.parse_date(dag_run.conf['job_run_date'], config.DATE_FORMAT),
                        },
                        "item": {
                            "name": dag_run.conf['location']
                        }
                    }
                ]
            })

        if not (current_assignments['current_location_name']):
            updates.append("Location group assigned")
            modifications.update({
                "locationSchedule": [
                    {
                        "dateRange": null,
                        "item": {
                            "name": dag_run.conf['location']
                        }
                    }
                ]
            })

    return {
        'updates': ', '.join(updates) if updates else '',
        'modifications_payload': modifications
    }


def get_time_off_type_uris_to_assign(timeoff_types_to_assign_from_mapper, all_replicon_timeoffs):
    time_off_type_uris = []
    for item in timeoff_types_to_assign_from_mapper:
        time_off_uri = rail.find_first_by_attr_and_get_attr(
            all_replicon_timeoffs, 'name', item['timeoff'], 'uri')
        if time_off_uri:
            time_off_type_uris.append(time_off_uri)

    return time_off_type_uris


def get_custom_fields_to_update_payload(dag_run, user_existing_data, dropdown_options_for_overtime):
    custom_fields_update_payload = []
    user_custom_field_details = user_existing_data['userDetails']['customFieldValues']
    required_custom_field_uris = dag_run.conf['custom_field_uris']

    if dag_run.conf.get('customClientName') and required_custom_field_uris['client_name_cf_uri']:
        existing_value_client_name_cf = rail.find_first_by_attr_and_get_attr(
            user_custom_field_details, 'customField.displayText', 'Client Name', 'text')
        if not (existing_value_client_name_cf) or existing_value_client_name_cf != dag_run.conf.get('customClientName'):
            custom_fields_update_payload.append({
                "value": {
                    "customField": {
                        "uri": required_custom_field_uris['client_name_cf_uri'],
                        "name": null
                    },
                    "text": dag_run.conf.get('customClientName')
                }
            })

    if dag_run.conf.get('customDirectIndirect') and required_custom_field_uris['labor_level_cf_uri']:
        existing_value_direct_indirect_cf = rail.find_first_by_attr_and_get_attr(
            user_custom_field_details, 'customField.displayText', 'Labor Level', 'text')
        if not (existing_value_direct_indirect_cf) or existing_value_direct_indirect_cf != dag_run.conf.get('customDirectIndirect'):
            custom_fields_update_payload.append({
                "value": {
                    "customField": {
                        "uri": required_custom_field_uris['labor_level_cf_uri'],
                        "name": null
                    },
                    "text": dag_run.conf.get('customDirectIndirect')
                }
            })

    if dag_run.conf.get('customProjectName') and required_custom_field_uris['project_name_cf_uri']:
        existing_value_project_name_cf = rail.find_first_by_attr_and_get_attr(
            user_custom_field_details, 'customField.displayText', 'Project Name', 'text')
        if not (existing_value_project_name_cf) or existing_value_project_name_cf != dag_run.conf.get('customProjectName'):
            custom_fields_update_payload.append({
                "value": {
                    "customField": {
                        "uri": required_custom_field_uris['project_name_cf_uri'],
                        "name": null
                    },
                    "text": dag_run.conf.get('customProjectName')
                }
            })

    if dag_run.conf.get('jobTitle') and required_custom_field_uris['job_title_cf_uri']:
        existing_value_jobtitle_cf = rail.find_first_by_attr_and_get_attr(
            user_custom_field_details, 'customField.displayText', 'Job Title', 'text')
        if not (existing_value_jobtitle_cf) or existing_value_jobtitle_cf != dag_run.conf.get('jobTitle'):
            custom_fields_update_payload.append({
                "value": {
                    "customField": {
                        "uri": required_custom_field_uris['job_title_cf_uri'],
                        "name": null
                    },
                    "text": dag_run.conf.get('jobTitle')
                }
            })

    if dag_run.conf.get('overtime') and required_custom_field_uris['overtime_cf_uri']:
        dropdown_option_uri_overtime = next((
            option['uri'] for option in dropdown_options_for_overtime if (option['displayText'] == dag_run.conf.get('overtime') and str(
                option['isEnabled']).lower() == 'true')), null)
        existing_value_overtime_dd_cf = rail.find_first_by_attr_and_get_attr(
            user_custom_field_details, 'customField.displayText', 'Overtime', 'dropDownOption')
        if dropdown_option_uri_overtime and existing_value_overtime_dd_cf != dropdown_option_uri_overtime:
            custom_fields_update_payload.append({
                "value": {
                    "customField": {
                        "uri": required_custom_field_uris['overtime_cf_uri'],
                        "name": null
                    },
                    "dropDownOption": {
                        "uri": dropdown_option_uri_overtime,
                        "name": null
                    }
                }
            })

    return custom_fields_update_payload


def do_format_logs(dag_run):
    log_artifacts = []
    log_records = []

    userlogs = dag_run.conf['userlogs']
    otherlogs = dag_run.conf['otherlogs']

    if userlogs:
        if isinstance(userlogs, list):
            log_artifacts.extend(userlogs)
        else:
            log_artifacts.append(userlogs)

    if otherlogs:
        if isinstance(otherlogs, list):
            log_artifacts.extend(otherlogs)
        else:
            log_artifacts.append(otherlogs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = rail.load_all_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    final_log_records = []

    final_log_records = list(map(lambda log: {
        **{
            'jobid': log['ecid']
        },
        **log['properties'],
    }, log_records))

    rail.set_result(key="error_record_count", val=len(
        list(filter(lambda x: x['status'] == 'failed', final_log_records))))
    rail.set_result(key="success_record_count", val=len(
        list(filter(lambda x: x['status'] == 'success', final_log_records))))
    rail.set_result(key="total_record_count",
                    val=dag_run.conf['total_records'])

    return final_log_records

def format_reference_logs(gathered_reference_logs, existing_reference_data):
    ref_log_artifacts = []
    reference_records = []

    existing_ref_records = existing_reference_data
    new_ref_logs = gathered_reference_logs

    if existing_ref_records:
        if isinstance(existing_ref_records, list):
            ref_log_artifacts.extend(existing_ref_records)
        else:
            ref_log_artifacts.append(existing_ref_records)

    if new_ref_logs:
        if isinstance(new_ref_logs, list):
            ref_log_artifacts.extend(new_ref_logs)
        else:
            ref_log_artifacts.append(new_ref_logs)

    if ref_log_artifacts:
        for log in ref_log_artifacts:
            each_log_records = rail.load_all_records(log)
            if each_log_records:
                reference_records.extend(each_log_records)

    final_ref_records = []

    final_ref_records = list(map(lambda log: {
        **log['properties']
    }, reference_records))

    return final_ref_records


def get_email_details_callable(dag_run, time_zone):
    _now = now(time_zone)
    return {
        "job_end_time": _now.isoformat(),
        "job_duration": (((_now - datetime.strptime(dag_run.conf['job_start_time'], "%Y-%m-%dT%H:%M:%S%z")).seconds)//60),
        "log_timestamp": _now.strftime("%Y%m%dT%H%M%S"),
        "email_timestamp": _now.isoformat(),
        "log_file_name": f"bamboohrusersynclog_{_now.strftime('%Y%m%dT%H')}.csv"
    }
