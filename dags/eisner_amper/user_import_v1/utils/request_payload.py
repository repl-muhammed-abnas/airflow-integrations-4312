from datetime import datetime
import json
import uuid
from eisner_amper.user_import_v1.mapper.user_import_mapper import EISNER_AMPER_USER_SYNC_MAPPER
import rail
from dateutil.relativedelta import relativedelta
from datetime import timedelta
import pendulum


def logging_details(time_zone):
    current_time = pendulum.now(time_zone)
    return {
        "timerange": (current_time).strftime("%d%m%Y%H%M%S")
    }


def get_employee_type(item):
    # Pre-process the item data for efficiency
    pay_rate_type = item['YY1_EmpDataRepliconType']['PayRateType']
    job_exempt = item['YY1_EmpDataRepliconType']['JobExempt']

    # Use a single filter expression for common cases
    if pay_rate_type.strip() == "" or job_exempt.strip() == "":
        return list(map(lambda mapper: mapper["value"],
                        filter(lambda mapper: mapper["type"] == "Employee Type" and mapper["identifier1"] == "Null" and mapper["identifier2"] == "Null", EISNER_AMPER_USER_SYNC_MAPPER)))
    else:
        return list(map(lambda mapper: mapper["value"],
                    filter(lambda mapper: mapper["type"] == "Employee Type" and mapper["identifier1"] == job_exempt and mapper["identifier2"] == pay_rate_type, EISNER_AMPER_USER_SYNC_MAPPER)))


def get_schedule(item):
    # Pre-process the item data for efficiency
    company_code = item['YY1_EmpDataRepliconType']['CompanyCode']

    # Use a single filter expression for common cases
    if company_code is not None and (list(map(lambda mapper: mapper["value"],
                                              filter(lambda mapper: mapper["type"] == "Schedule" and mapper["identifier1"] == company_code, EISNER_AMPER_USER_SYNC_MAPPER)))) != []:
        return list(map(lambda mapper: mapper["value"],
                        filter(lambda mapper: mapper["type"] == "Schedule" and mapper["identifier1"] == company_code, EISNER_AMPER_USER_SYNC_MAPPER)))

    # Handle the remaining case with a more specific filter
    else:
        return list(map(lambda mapper: mapper["value"],
                    filter(lambda mapper: mapper["type"] == "Schedule" and mapper["identifier1"] == "Default", EISNER_AMPER_USER_SYNC_MAPPER)))


def get_timesheettemplate(item):

    return list(map(lambda mapper: mapper["value"],
                    filter(lambda mapper: mapper["type"] == "Timesheet Template" and mapper["identifier1"] == "Default", EISNER_AMPER_USER_SYNC_MAPPER)))


def get_workweek(item):

    return list(map(lambda mapper: mapper["identifier2"],
                    filter(lambda mapper: mapper["type"] == "Work Week" and mapper["identifier1"] == "Default", EISNER_AMPER_USER_SYNC_MAPPER)))


def get_workuri(item):

    return list(map(lambda mapper: mapper["value"],
                    filter(lambda mapper: mapper["type"] == "Work Week" and mapper["identifier1"] == "Default", EISNER_AMPER_USER_SYNC_MAPPER)))


def get_timesheetperiod(item):

    return list(map(lambda mapper: mapper["value"],
                    filter(lambda mapper: mapper["type"] == "Timesheet Period" and mapper["identifier1"] == "Default", EISNER_AMPER_USER_SYNC_MAPPER)))


def get_timesheetapprovalpath(item):

    return list(map(lambda mapper: mapper["value"],
                    filter(lambda mapper: mapper["type"] == "Timesheet Approval Path" and mapper["identifier1"] == "Default", EISNER_AMPER_USER_SYNC_MAPPER)))


def get_timeentryapprovalpath(item):

    return list(map(lambda mapper: mapper["value"],
                    filter(lambda mapper: mapper["type"] == "Time Entry Approval Path" and mapper["identifier1"] == "Default", EISNER_AMPER_USER_SYNC_MAPPER)))


def get_user_payload(dag_run):

    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:login-name",
            "urn:replicon:user-list-column:enabled"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:user-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": dag_run.conf['username']
                }
            }
        }
    }


def bulk_get_user_payload(dag_run):

    return {
        "users": [
            {
                "uri": dag_run.conf['uri']
            }
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }


def update_employmentdaterange_payload(dag_run):

    end_date = datetime.strptime(
        dag_run.conf['startdate'].split('T')[0].strip(), '%Y-%m-%d').date() - relativedelta(days=1)
    return {
        "userUri": dag_run.conf['uri'],
        "dateRange": {
            "startDate": {
                "year": rail.result('bulk_get_user')[0]['userDetails']['employmentDateRange']['startDate']['year'],
                "month": rail.result('bulk_get_user')[0]['userDetails']['employmentDateRange']['startDate']['month'],
                "day": rail.result('bulk_get_user')[0]['userDetails']['employmentDateRange']['startDate']['day']
            },
            "endDate": {
                "year": end_date.year,
                "month": end_date.month,
                "day": end_date.day
            }
        }
    }


def disable_login_payload(dag_run):
    return {
        "userUri": dag_run.conf['uri']
    }


def enable_login_payload(dag_run):
    return {
        "userUri": dag_run.conf['uri']
    }


def update_employment_date_range_payload(dag_run):

    start_date = datetime.strptime(
        dag_run.conf['startdate'].split('T')[0].strip(), '%Y-%m-%d').date()
    return {
        "userUri": dag_run.conf['uri'],
        "dateRange": {
            "startDate": {
                "year": start_date.year,
                "month": start_date.month,
                "day": start_date.day
            }
        }
    }


def get_effective_user_group_membership_payload(dag_run):
    return {
        "userUri": dag_run.conf['uri']
    }


def update_first_name_user_payload(dag_run):
    return {
        "userUri": dag_run.conf['uri'],
        "firstname": dag_run.conf['firstname']
    }


def update_last_name_user_payload(dag_run):
    return {
        "userUri": dag_run.conf['uri'],
        "lastname": dag_run.conf['lastname']
    }


def update_email_address_user_payload(dag_run):
    return {
        "userUri": dag_run.conf['uri'],
        "email": dag_run.conf['defaultemailaddress']
    }


def put_user_notification_preferences_payload(user_uri):
    return {
        "user": {
            "uri": user_uri
        },
        "preferences": {
            "notificationDeliveryPreferences": [
                {
                    "objectTypeUri": "urn:replicon:object-type:project",
                    "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                },
                {
                    "objectTypeUri": "urn:replicon:object-type:user",
                    "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                },
                {
                    "objectTypeUri": "urn:replicon:object-type:timesheet",
                    "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                },
                {
                    "objectTypeUri": "urn:replicon:object-type:time-entry-revision-group",
                    "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                },
                {
                    "objectTypeUri": "urn:replicon:object-type:expense-sheet",
                    "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                },
                {
                    "objectTypeUri": "urn:replicon:object-type:pay-rule-script",
                    "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                },
                {
                    "objectTypeUri": "urn:replicon:object-type:time-off",
                    "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                },
                {
                    "objectTypeUri": "urn:replicon:object-type:holiday",
                    "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                }
            ],
            "sharedDeliveryPreferenceOptionUris": [
                "urn:replicon:user-shared-delivery-preference-option:always-deliver"
            ]
        }
    }


def update_costcenter_group_payload(dag_run):

    startdate = datetime.strptime(
        dag_run.conf['startdate'].split('T')[0].strip(), '%Y-%m-%d').date()
    return {
        "user": {
            "uri": dag_run.conf['uri']
        },
        "modifications": {
            "costCenterScheduleToApply": {
                "userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementCostCenterSchedule": [],
                "updateCostCenterScheduleOverDateRange": {
                    "replacementCostCenterScheduleEntries": [
                        {
                            "costCenter": {
                                "name": dag_run.conf['costcenterdescription']
                            },
                            "effectiveDate": {
                                "year": startdate.year,
                                "month": startdate.month,
                                "day": startdate.day
                            }
                        }
                    ]
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def update_department_group_payload(dag_run):
    startdate = datetime.strptime(
        dag_run.conf['startdate'].split('T')[0].strip(), '%Y-%m-%d').date()
    return {
        "user": {
            "uri": dag_run.conf['uri']
        },
        "modifications": {
            "departmentGroupScheduleToApply": {
                "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementDepartmentGroupSchedule": [],
                "updateDepartmentGroupScheduleOverDateRange": {
                    "replacementDepartmentGroupScheduleEntries": [
                        {
                            "departmentGroup": {
                                "uri": dag_run.conf['companycodeuri']
                            },
                            "effectiveDate": {
                                "year": startdate.year,
                                "month": startdate.month,
                                "day": startdate.day
                            }
                        }
                    ]
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def update_cost_center_group_current_date_payload(dag_run):
    startdate = datetime.now()
    return {
        "user": {
            "uri": dag_run.conf['uri']
        },
        "modifications": {
            "costCenterScheduleToApply": {
                "userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementCostCenterSchedule": [],
                "updateCostCenterScheduleOverDateRange": {
                    "replacementCostCenterScheduleEntries": [
                        {
                            "costCenter": {
                                "name": dag_run.conf['costcenterdescription']
                            },
                            "effectiveDate": {
                                "year": startdate.year,
                                "month": startdate.month,
                                "day": startdate.day
                            }
                        }
                    ]
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def update_department_group_current_date_payload(dag_run):
    startdate = datetime.now()
    return {
        "user": {
            "uri": dag_run.conf['uri']
        },
        "modifications": {
            "departmentGroupScheduleToApply": {
                "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementDepartmentGroupSchedule": [],
                "updateDepartmentGroupScheduleOverDateRange": {
                    "replacementDepartmentGroupScheduleEntries": [
                        {
                            "departmentGroup": {
                                "uri": dag_run.conf['companycodeuri']
                            },
                            "effectiveDate": {
                                "year": startdate.year,
                                "month": startdate.month,
                                "day": startdate.day
                            }
                        }
                    ]
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def update_employee_type_group_payload(dag_run):
    startdate = datetime.now()
    return {
        "user": {
            "uri": dag_run.conf['uri']
        },
        "modifications": {
            "employeeTypeGroupScheduleToApply": {
                "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementEmployeeTypeGroupSchedule": [],
                "updateEmployeeTypeGroupScheduleOverDateRange": {
                    "replacementEmployeeTypeGroupScheduleEntries": [
                        {
                            "employeeTypeGroup": {
                                "uri": dag_run.conf['employeetypeuri']
                            },
                            "effectiveDate": {
                                "year": startdate.year,
                                "month": startdate.month,
                                "day": startdate.day
                            }
                        }
                    ]
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def update_location_payload(dag_run):
    startdate = datetime.now()
    return {
        "user": {
            "uri": dag_run.conf['uri']
        },
        "modifications": {
            "locationScheduleToApply": {
                "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementLocationSchedule": [],
                "updateLocationScheduleOverDateRange": {
                    "replacementLocationScheduleEntries": [
                        {
                            "location": {
                                "uri": dag_run.conf['worklocationuri']
                            },
                            "effectiveDate": {
                                "year": startdate.year,
                                "month": startdate.month,
                                "day": startdate.day
                            }
                        }
                    ]
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def update_division_payload(dag_run):

    startdate = datetime.strptime(
        dag_run.conf['roleeffectivedate'].split('T')[0].strip(), '%Y-%m-%d').date() if rail.result('bulk_get_user')[0]['userDetails']['isEnabled'] == True else datetime.now()
    return {
        "user": {
            "uri": dag_run.conf['uri']
        },
        "modifications": {
            "divisionScheduleToApply": {
                "userDivisionScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementDivisionSchedule": [],
                "updateDivisionScheduleOverDateRange": {
                    "replacementDivisionScheduleEntries": [
                        {
                            "division": {
                                "uri": dag_run.conf['roleuri']
                            },
                            "effectiveDate": {
                                "year": startdate.year,
                                "month": startdate.month,
                                "day": startdate.day
                            }
                        }
                    ]
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def update_custom_value_payload(dag_run):
    return {
        "user": {
            "uri": dag_run.conf['uri']
        },
        "modifications": {
            "customFieldValuesToApply": json.loads(json.dumps(
                rail.get_dag_run_var('customFieldValues'), ensure_ascii=False).replace('"date":{}', '"date":null').replace(
                '{"year":null,"month":null,"day":null}', '{}')),
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def create_cost_center_or_apply_modification_payload(dag_run):
    return {
        "modifications": {
            "name": dag_run.conf['costcenterdescription'],
            "codeToApply": {
                "value": dag_run.conf['costcenter']
            },
            "isEnabled": "true"
        },
        "unitOfWorkId": str(uuid.uuid4())
    }


def create_company_code_or_apply_modification_payload(dag_run):
    return {
        "departmentGroup": {
            "parent": {
                "uri": dag_run.conf['Companydepturi']
            }
        },
        "modifications": {
            "name": dag_run.conf['companycodename'],
            "codeToApply": {
                "value": dag_run.conf['companycode']
            },
            "isEnabled": "true"
        },
        "unitOfWorkId": str(uuid.uuid4())
    }


def create_work_location_or_apply_modification_payload(dag_run):
    return {
        "location": {
        },
        "modifications": {
            "name": dag_run.conf['worklocation'],
            "codeToApply": {
                "value": dag_run.conf['worklocationid']
            },
            "isEnabled": "true"
        },
        "unitOfWorkId": str(uuid.uuid4())
    }


def create_roles_or_apply_modification_payload(dag_run):
    return {
        "modifications": {
            "name": dag_run.conf['role'],
            "codeToApply": {
                "value": dag_run.conf['roledescription']
            },
            "isEnabled": "true"
        },
        "unitOfWorkId": str(uuid.uuid4())
    }


def create_user_payload(dag_run):
    return {
        "user": {
            "target": {
                "loginName": dag_run.conf['username']
            },
            "firstname": dag_run.conf['firstname'],
            "lastname": dag_run.conf['lastname'],
            "emailAddress": dag_run.conf['defaultemailaddress'],
            "employeeId": dag_run.conf['personexternalid'],
            "schedulePolicySchedule": [
                {
                    "schedulePolicy": {
                        "name": dag_run.conf['schedule'] if dag_run.conf['schedule'] else None,
                        "officeSchedule": {
                            "name": dag_run.conf['schedule'] if dag_run.conf['schedule'] else None
                        },
                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                    }
                }
            ],
            "workWeekStartDayUri": dag_run.conf['workweekuri'] if dag_run.conf['workweekuri'] else None,
            "employmentDateRange": {
                "startDate": rail.parse_date(dag_run.conf['startdate'].split('T')[0].strip(), '%Y-%m-%d')
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                    list(map(lambda mapper: mapper["value"],
                             filter(lambda mapper: mapper["type"] == "Authentication" and mapper["identifier1"] == "Default", EISNER_AMPER_USER_SYNC_MAPPER)))[0]
                ],
                "isLoginEnabled": True if dag_run.conf['workagreementstatus'] == "1" else False,
                "loginName": dag_run.conf['username'],
                "SSOName": dag_run.conf['username']
            },
            "permissionSets": [
                {
                    "name": list(map(lambda mapper: mapper["value"],
                                     filter(lambda mapper: mapper["type"] == "End User Permission" and mapper["identifier1"] == "Default", EISNER_AMPER_USER_SYNC_MAPPER)))[0]
                }
            ],
            "policySets": [
                {
                    "name": dag_run.conf['timesheettemplate'] if dag_run.conf['timesheettemplate'] else None
                }
            ],
            "timesheetApprovalPath": {
                "name": dag_run.conf['timesheetapprovalpath'] if dag_run.conf['timesheetapprovalpath'] else None
            },
            "customFieldValues": [json.loads(json.dumps(
                rail.get_dag_run_var('customFieldValues')[0], ensure_ascii=False).replace('"date":{}', '"date":null').replace(
                '{"year":null,"month":null,"day":null}', '{}'))],
            "divisionSchedule": [
                {
                    "division": {
                        "uri": dag_run.conf['roleuri'] if dag_run.conf['roleuri'] else None
                    }
                }
            ],
            "costCenterSchedule": [
                {
                    "costCenter": {
                        "name": dag_run.conf['costcenterdescription'] if dag_run.conf['costcenterdescription'] else None
                    }
                }
            ],
            "departmentGroupSchedule": [
                {
                    "departmentGroup": {
                        "uri": dag_run.conf['companycodeuri'] if dag_run.conf['companycodeuri'] else None
                    }
                }
            ],
            "employeeTypeGroupSchedule": [
                {
                    "employeeTypeGroup": {
                        "uri": dag_run.conf['employeetypeuri'] if dag_run.conf['employeetypeuri'] else None
                    }
                }
            ],
            "timesheetPeriodSchedule": [
                {
                    "timesheetPeriod": {
                        "name": dag_run.conf['timesheetperiod'] if dag_run.conf['timesheetperiod'] else None
                    }
                }
            ]
        }
    }


def update_location_payload2(dag_run):
    return {
        "userUri": rail.result('create_user'),
        "scheduleEntries": [
            {
                "location": {
                    "uri": dag_run.conf['worklocationuri']
                }
            }
        ]
    }


def update_time_entry_approval_path_for_new_user_payload(dag_run):
    return {
        "user": {
            "uri": rail.result('create_user')
        },
        "modifications": {
            "timeEntryRevisionGroupApprovalPathToApply": {
                "name": dag_run.conf['timesheetapprovalpath'][0] if dag_run.conf['timesheetapprovalpath'][0] else None
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_all_custom_fields_payload():
    return {
        "objectUri": "urn:replicon:object-type:user"
    }


def do_format_logs():
    log_artifacts = []
    log_records = []

    user_shift_logs = rail.result('create_user_log')

    if user_shift_logs:
        if isinstance(user_shift_logs, list):
            log_artifacts.extend(user_shift_logs)
        else:
            log_artifacts.append(user_shift_logs)

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
        list(filter(lambda x: x['status'] == 'Error', final_log_records))))
    rail.set_result(key="success_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Success', final_log_records))))
    rail.set_result(key="skipped_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Success', final_log_records))))
    rail.set_result(key="add_record_count", val=len(
        list(filter(lambda x: x['action'] == 'Add', final_log_records))))
    rail.set_result(key="update_record_count", val=len(
        list(filter(lambda x: x['action'] == 'Update', final_log_records))))
    rail.set_result(key="exception_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Exception', final_log_records))))

    return final_log_records


def is_schedule_present():
    schedule = json.dumps(rail.result('bulk_get_user')[0]['schedulePolicies'])
    if "urn" in schedule:
        return True
    else:
        return False


def get_current_office_schedule():
    schedules = []
    for item in rail.result('bulk_get_user')[0]['schedulePolicies']:
        effective_date = None
        if item.get('effectiveDate'):
            effective_date = item['effectiveDate'].get('day', None) + "/" + item['effectiveDate'].get('month', None) + "/" + item['effectiveDate'].get('year', rail.result('bulk_get_user')[0]['userDetails']['employmentDateRange']
                                                                                                                                                       ['startDate']['day']) + "/" + rail.result('bulk_get_user')[0]['userDetails']['employmentDateRange']['startDate']['month'] + "/" + rail.result('bulk_get_user')[0]['userDetails']['employmentDateRange']['startDate']['year']
        else:
            schedules.append({
                "effective_date": effective_date,
                "displaytext": item['officeSchedule']['displayText'],
                "uri": item['officeSchedule'].get('uri'),
                "schedule_type_uri": item['scheduleTypeUri']
            })
    return schedules


def update_office_schedule_payload(dag_run):
    startdate = datetime.now()
    return {
        "user": {
            "uri": dag_run.conf['uri']
        },
        "modifications": {
            "schedulePolicyToApply": {
                "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementSchedule": [],
                "updateScheduleOverDateRange": {
                    "replacementScheduleEntries": [
                        {
                            "schedulePolicy": {
                                "name": dag_run.conf['schedule'],
                                "officeSchedule": {
                                    "name": dag_run.conf['schedule']
                                },
                                "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                            },
                            "effectiveDate": {
                                "year": startdate.year,
                                "month": startdate.month,
                                "day": startdate.day
                            }
                        }
                    ]
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_trigger_id(config, index):
    batch_num = int(index) % config.BATCH_COUNT
    return f"{config.process_each_user_dag_id}_batch_{batch_num+1}"
