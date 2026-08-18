from datetime import datetime
import itertools
import uuid
import rail
from rail import get_current_context
null = None
EFFECTIVE_DATE_FORMAT_BAMBOOHR = '%Y-%m-%d'

def get_dag_run_conf():
    return get_current_context()['dag_run'].conf

def split_startdate(date_str):
    date_obj = datetime.strptime(date_str, EFFECTIVE_DATE_FORMAT_BAMBOOHR)
    return {
        "year" : date_obj.year,
        "month" : date_obj.month,
        "day" : date_obj.day
    }

def assign_supervisor_permission(dag_run):
    return {
        "user": {
            "uri": rail.result("get_user_supervisor_from_replicon")["userDetails"]["uri"],
            "loginName": rail.result("get_user_supervisor_from_replicon")["securityConfiguration"]["loginName"],
            "employeeId": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "permissionSetsToApply": {
                "permissionSetUrisToAssign": dag_run.conf["supervisor_permission_sets"],
                "policyUrisToRemovePermissionSet": []
            }
        }
    }

def get_oef_details_to_add(dag_run):
    oef_data_to_add = []
    oef_tags_not_present = []
    for oefdata in dag_run.conf["oef_details"]:
        if dag_run.conf["user_details"][oefdata["bamboohr_field"]] is not null:
            oef_tag_uri = rail.find_first_by_attr_and_get_attr(oefdata["oeftags"], "value",
                dag_run.conf["user_details"][oefdata["bamboohr_field"]], "uri")
            if oef_tag_uri is null:
                oef_tags_not_present.append('OEF value "' + dag_run.conf["user_details"][oefdata["bamboohr_field"]]
                    + '" for OEF "' + oefdata["oefname"] + '" is not present in Replicon')
            if oef_tag_uri:
                oef_data_to_add.append(
                    {
                        "definition": {
                            "uri": oefdata["oefuri"],
                            "name": null
                        },
                        "tag": {
                            "uri": oef_tag_uri,
                            "slug": null,
                            "tagName": null
                        },
                        "numericValue": null,
                        "textValue": null,
                        "fileValue": null,
                        "jsonValue": null
                    }
                )
    return {
        "oef_data_to_add": oef_data_to_add,
        "oef_logs": oef_tags_not_present
    }

def get_oef_details_to_update():
    oef_data_to_update = []
    oef_tags_not_present = []
    for oefdata in get_dag_run_conf()["oef_details"]:
        if get_dag_run_conf()["user_details"][oefdata["bamboohr_field"]] is not null:
            oef_tag_uri = rail.find_first_by_attr_and_get_attr(oefdata["oeftags"], "value",
                get_dag_run_conf()["user_details"][oefdata["bamboohr_field"]], "uri")
            if oef_tag_uri is null:
                oef_tags_not_present.append('OEF value "' + get_dag_run_conf()["user_details"][oefdata["bamboohr_field"]]
                    + '" for OEF "' + oefdata["oefname"] + '" is not present in Replicon')
            if oef_tag_uri and get_dag_run_conf()["user_details"][oefdata["bamboohr_field"]] != rail.find_first_by_attr_and_get_attr(
                get_dag_run_conf()["replicon_user_details"]["userDetails"]["extensionFieldValues"],
                    "definition.displayText", oefdata["oefname"], "tag.displayText"):
                oef_data_to_update.append(
                    {
                        "definition": {
                            "uri": oefdata["oefuri"],
                            "name": null
                        },
                        "tag": {
                            "uri": oef_tag_uri,
                            "slug": null,
                            "tagName": null
                        },
                        "numericValue": null,
                        "textValue": null,
                        "fileValue": null,
                        "jsonValue": null
                    }
                )
    return {
        "oef_data_to_update": oef_data_to_update,
        "oef_logs": oef_tags_not_present
    }

def get_create_user_payload(dag_run, user_permission_set):
    return {
        "user": {
            "target": {
                "uri": null,
                "loginName": dag_run.conf['user_details']['workemail'],
                "employeeId": dag_run.conf['user_details']['employeenumber'],
                "parameterCorrelationId": null
            },
            "firstname": dag_run.conf['user_details']['firstname'],
            "lastname": dag_run.conf['user_details']['lastname'],
            "emailAddress": dag_run.conf['user_details']['workemail'],
            "employeeId": dag_run.conf['user_details']['employeenumber'],
            "workWeekStartDayUri": "urn:replicon:day-of-week:monday",
            "employmentDateRange": {
                "startDate": split_startdate(dag_run.conf['user_details']['startdate'])
                    if dag_run.conf['user_details']['startdate'] else null,
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "schedulePolicySchedule": [
                {
                    "schedulePolicy": {
                        "officeScheduleUri": null,
                        "name": "TE - Regular 7.5 Hours per day schedule",
                        "officeSchedule": null,
                        "scheduleTypeUri": null
                    },
                    "effectiveDate": null
                }
            ],
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:sso"
                ],
                "isLoginEnabled": "true",
                "loginName": dag_run.conf['user_details']['workemail'],
                "SSOName": dag_run.conf['user_details']['workemail'],
                "password": null
            },
            "permissionSets": [
                {
                    "uri": null,
                    "name": permission_role
                } for permission_role in user_permission_set],
            "policySets": [
                {
                    "uri": null,
                    "name": "TE - 37.5"
                },
                {
                    "uri": null,
                    "name": "Time Off"
                }
            ],
            "timesheetApprovalPath": {
                "uri": null,
                "name": "System Approval"
            },
            "timesheetPeriodSchedule": [
                {
                    "timesheetPeriod": {
                        "uri": null,
                        "name": "Weekly starting on Monday"
                    },
                    "effectiveDate": null
                }
            ],
            "timeOffPolicy": {
                "bankedTimePolicy": null,
                "applyDefaultTimeOffTypePolicyScheduleForV3": "true",
                "timeOffPoliciesByTimeOffType": [
                    {
                        "timeOffType": {
                            "uri": null,
                            "name": timeoff_type_name
                        },
                        "isTimeOffAllowedAgainstThisTimeOffType": "true",
                        "policySchedule": []
                    } for timeoff_type_name in ["Holiday", "Leave", "Conferences"]
                ]
            },
            "timeOffApprovalPath": {
                "uri": null,
                "name": "Autoapproval"
            },
            "timeZone": {
                "uri": null,
                "IANAName": "Etc/GMT"
            },
            "extensionFieldValues": get_oef_details_to_add(dag_run)["oef_data_to_add"] if get_oef_details_to_add(dag_run)
                and get_oef_details_to_add(dag_run)["oef_data_to_add"] else []
        }
    }

def get_assign_licenses_to_user_payload(licenses):
    return {
        "target": {
            "uri": rail.result("create_user_in_replicon")["uri"]
        },
        "template": null,
        "modifications": {
            "products": [
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                    "items": [
                      {
                        "uri": null,
                        "name": license_name
                      }
                    ]
                } for license_name in licenses
            ],
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_user_details_from_replicon(dag_run):
    return {
        "users": [
            {
                "employeeId": dag_run.conf["user_details"]["employeenumber"],
                "loginName": null,
                "parameterCorrelationId": null
            }
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }

def get_bamboohr_employees_request(data_type):
    last_modified_time = rail.result('get_lastsync_time_and_current_time')[
        'last_synctime']
    required_employee_fields = rail.result("filter_required_employee_fields")
    return {
        "filters": {
            "filters": [
                {
                    "field": "lastChanged",
                    "operator": "gte",
                    "value": last_modified_time
                }
            ],
            "match": "all"
        } if data_type != "All" else {},
        "fields": [field["bamboohr_field"] for field in required_employee_fields if field["bamboohr_field"]]
    }

def get_department_groups_data_payload():
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:department-group-list-column:department-group",
            "urn:replicon:department-group-list-column:effectively-enabled",
            "urn:replicon:department-group-list-column:full-path"
        ],
        "sort": [],
        "filterExpression": null
    }

def filtered_costrate_data(dag_run):
    result = []
    previous_costrate = null
    group_data = dag_run.conf["user_details"]["costratedata"]
    for record in group_data:
        costrate = f'{record["hourlyrate"]} {record["hourlyratecurrency"]}'
        if costrate != previous_costrate:
            result.append(record)
            previous_costrate = costrate
    return result

def get_apply_modifications_user_payload(dag_run):
    supervisors_result = rail.result("get_user_supervisors")["value"] if rail.result("get_user_supervisors") else []
    costrates = filtered_costrate_data(dag_run)
    return {
    	"user": {
    		"uri": rail.result("create_user_in_replicon")["uri"],
    		"loginName": null,
    		"employeeId": null,
    		"parameterCorrelationId": null
    	},
    	"modifications": {
    		"employeeTypeGroupScheduleToApply": {
    			"userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
    			"replacementEmployeeTypeGroupSchedule": [],
    			"updateEmployeeTypeGroupScheduleOverDateRange": {
                    "replacementEmployeeTypeGroupScheduleEntries": rail.result("get_user_employee_type_groups")["value"],
                    "endDate": null
                }
    		} if rail.result("get_user_employee_type_groups") and rail.result("get_user_employee_type_groups")["value"] else null,
            "departmentGroupScheduleToApply": {
    			"userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
    			"replacementDepartmentGroupSchedule": [],
    			"updateDepartmentGroupScheduleOverDateRange": {
                    "replacementDepartmentGroupScheduleEntries": rail.result("get_user_department_groups")["value"],
                    "endDate": null
                }
    		} if rail.result("get_user_department_groups") and rail.result("get_user_department_groups")["value"] else null,
            "costRateScheduleModifications": {
                "scheduleEntriesToPut": [
                    {
                        "hourlyRate": {
                            "amount": costrates[0]["hourlyrate"],
                            "currency": {
                                "uri": null,
                                "name": null,
                                "symbol": costrates[0]["hourlyratecurrency"]
                            }
                        },
                        "effectiveDate": null
                    }
                ] + [
                    {
                        "hourlyRate": {
                            "amount": costrate["hourlyrate"],
                            "currency": {
                                "uri": null,
                                "name": null,
                                "symbol": costrate["hourlyratecurrency"]
                            }
                        },
                        "effectiveDate": split_startdate(costrate["date"])
                    } for costrate in costrates[1:]
                ],
                "scheduleEntriesToAdd": []
            } if dag_run.conf["user_details"]["costratedata"] else null,
            "supervisorsToApply": {
                "initialSupervisor": {
                    "uri": supervisors_result[0]["supervisor"]["uri"],
                    "loginName": null,
                    "employeeId": null,
                    "parameterCorrelationId": null
                } if len(supervisors_result) > 0 else null,
                "supervisorScheduleEntries": supervisors_result[1:] if len(supervisors_result) > 1 else []
    	    } if len(supervisors_result) > 0 else null
        },
    	"userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

def is_supervisor_changed():
    user_supervisor_from_replicon = rail.result("get_user_supervisor_from_replicon")
    if not user_supervisor_from_replicon or not user_supervisor_from_replicon.get("userDetails"):
        return False
    
    current_supervisor_uri = user_supervisor_from_replicon["userDetails"].get("uri", "")
    supervisor_assignment_uri = rail.result("get_supervisor_assignment_details", "supervisor").get("uri", "")
    
    return current_supervisor_uri != supervisor_assignment_uri

def is_department_changed():
    user_details = get_dag_run_conf()["user_details"]
    current_department_uri = user_details.get("department_uri")
    if not current_department_uri:
        return False
    
    replicon_department_uri = rail.result("get_effectiveusergroupmembership_replicon", "department").get("uri", "")
    
    return current_department_uri != replicon_department_uri

def is_employment_type_changed():
    user_details = get_dag_run_conf()["user_details"]
    current_employee_type_uri = user_details.get("employmentstatus_uri")
    if not current_employee_type_uri:
        return False
    
    replicon_employee_type_uri = rail.result("get_effectiveusergroupmembership_replicon", "employeetype").get("uri", "")
    
    return current_employee_type_uri != replicon_employee_type_uri

def is_hourly_rate_updated(date_format):
    user_details = get_dag_run_conf()["user_details"]
    cost_rate_schedule = get_dag_run_conf()["replicon_user_details"]["costRateSchedule"]
    
    if not cost_rate_schedule:
        return True
    
    # Get current date from process start time
    current_date = datetime.strptime(get_dag_run_conf()["process_start_time"], date_format)
    
    # Find the most recent cost rate entry that is effective (effectiveDate <= current_date)
    effective_entries = []
    for entry in cost_rate_schedule:
        if entry.get("effectiveDate"):
            effective_date = datetime(entry["effectiveDate"]["year"], entry["effectiveDate"]["month"], entry["effectiveDate"]["day"])
            if effective_date <= current_date:
                effective_entries.append((effective_date, entry))
    
    # Get the entry with the latest effective date
    user_costrate_latest = max(effective_entries, key=lambda x: x[0])[1] if effective_entries else null
    
    if not user_costrate_latest:
        return True
        
    # Compare rates
    bamboo_rate_str = (str(float(user_details["hourlyrate"])) if user_details["hourlyrate"] else "") + \
                     (user_details["hourlyratecurrency"] or "")
    
    replicon_rate_str = (str(float(user_costrate_latest["hourlyRate"]["amount"])) if user_costrate_latest["hourlyRate"]["amount"] else "") + \
                       (user_costrate_latest["hourlyRate"]["currency"]["symbol"] or "")
    
    return bamboo_rate_str != replicon_rate_str

def get_updated_log(dag_run, date_format):
    msg_list = []
    basic_details_update = [f'{key} updated' for key in rail.result("updated_user_basic_details").keys()
        if rail.result("updated_user_basic_details")[key] is not null]
    login_name_update = (dag_run.conf["replicon_user_details"]["securityConfiguration"]["loginName"]
        != dag_run.conf["user_details"]["workemail"])
    loginname_and_licenses_update = rail.result("update_user_loginname_and_licenses")
    if basic_details_update:
        msg_list.append(basic_details_update)
    if login_name_update:
        msg_list.append(["Login name updated"])
    if loginname_and_licenses_update:
        msg_list.append(["Login name and licenses updated"])
    if is_supervisor_changed():
        msg_list.append(["Supervisor updated"])
    if is_department_changed():
        msg_list.append(["Department updated"])
    if is_employment_type_changed():
        msg_list.append(["Employee type updated"])
    if is_hourly_rate_updated(date_format):
        msg_list.append(["Hourly cost updated"])
    if get_oef_details_to_update() and get_oef_details_to_update()["oef_data_to_update"]:
        msg_list.append(["OEF's updated"])
    return list(itertools.chain.from_iterable(msg_list))

def get_update_modifications_user_payload(dag_run, date_format):
    current_date = datetime.strptime(dag_run.conf["process_start_time"], date_format)
    return {
    	"user": {
    		"uri": get_dag_run_conf()["replicon_user_details"]["userDetails"]["uri"],
    		"loginName": null,
    		"employeeId": null,
    		"parameterCorrelationId": null
    	},
    	"modifications": {
    		"employeeTypeGroupScheduleToApply": {
    		    "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
    		    "replacementEmployeeTypeGroupSchedule": [],
    		    "updateEmployeeTypeGroupScheduleOverDateRange": {
                    "replacementEmployeeTypeGroupScheduleEntries": [{
                        "employeeTypeGroup": {
                            "uri": get_dag_run_conf()["user_details"]["employmentstatus_uri"],
                        },
                        "effectiveDate": rail.parse_date(get_dag_run_conf()["user_details"]["employmentstatuseffectivedate"],
                            EFFECTIVE_DATE_FORMAT_BAMBOOHR)
                    }],
                    "endDate": null
    		    }
            } if is_employment_type_changed() else null,
            "departmentGroupScheduleToApply": {
    			"userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
    			"replacementDepartmentGroupSchedule": [],
    			"updateDepartmentGroupScheduleOverDateRange": {
                    "replacementDepartmentGroupScheduleEntries": [{
                        "departmentGroup": {
                            "uri": get_dag_run_conf()["user_details"]["department_uri"],
                        },
                        "effectiveDate": rail.parse_date(get_dag_run_conf()["user_details"]["jobinfoeffectivedate"],
                            EFFECTIVE_DATE_FORMAT_BAMBOOHR)
                    }],
                    "endDate": null
                }
            } if is_department_changed() else null,
            "costRateScheduleModifications": {
                "scheduleEntriesToAdd": [],
                "scheduleEntriesToPut": get_dag_run_conf()["replicon_user_details"]["costRateSchedule"] + [
                    {
                        "hourlyRate": {
                            "amount": get_dag_run_conf()["user_details"]["hourlyrate"],
                            "currency": {
                                "uri": null,
                                "name": null,
                                "symbol": get_dag_run_conf()["user_details"]["hourlyratecurrency"]
                            }
                        },
                        "effectiveDate": {
                            "year": current_date.year,
                            "month": current_date.month,
                            "day": current_date.day
                        }
                    }
                ],
            } if get_dag_run_conf()["user_details"]["hourlyrate"] and is_hourly_rate_updated(date_format) else null,
            "objectExtensionFieldsToApply": get_oef_details_to_update()["oef_data_to_update"] if
                get_oef_details_to_update() and get_oef_details_to_update()["oef_data_to_update"] else []
        },
    	"userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

def get_update_supervisors_for_user_payload(dag_run):
    supervisors_result = rail.result("get_user_supervisors")["value"] if rail.result("get_user_supervisors") else []
    return {
    	"user": {
    		"uri": dag_run.conf["replicon_user_details"]["userDetails"]["uri"],
    		"loginName": null,
    		"employeeId": null,
    		"parameterCorrelationId": null
    	},
    	"modifications": {
            "supervisorsToApply": {
                "initialSupervisor": {
                    "uri": supervisors_result[0]["supervisor"]["uri"],
                    "loginName": null,
                    "employeeId": null,
                    "parameterCorrelationId": null
                } if len(supervisors_result) > 0 else null,
                "supervisorScheduleEntries": supervisors_result[1:] if len(supervisors_result) > 1 else []
    	    } if len(supervisors_result) > 0 else null
        },
    	"userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

def get_update_user_basic_details_payload(dag_run):
    updated_details = rail.result("updated_user_basic_details")
    start_date = (updated_details["startdate"] if updated_details["startdate"] == "0000-00-00" else (
        split_startdate(updated_details["startdate"]) if updated_details["startdate"] else null))
    end_date = (updated_details["enddate"] if updated_details["enddate"] == "0000-00-00" else (
        split_startdate(updated_details["enddate"]) if updated_details["enddate"] else null))

    return {
        "user": {
            "uri": dag_run.conf["replicon_user_details"]["userDetails"]["uri"],
            "loginName": null,
            "employeeId": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "userDetailsToApply": {
                "firstName": updated_details["firstname"],
                "lastName": updated_details["lastname"],
                "emailAddress": {
                    "emailAddress": updated_details["workemail"]
                } if updated_details["workemail"] else null,
                "employmentStartDate": {
                    "date": null if start_date == "0000-00-00" else start_date,
                } if start_date else null,
                "employmentEndDate": {
                    "date": null if end_date == "0000-00-00" else end_date,
                } if end_date else null
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

def get_datetime_obj(date_str, fmt='%Y-%m-%d'):
    datetime_obj = datetime.strptime(date_str, fmt)
    return {
        'year': datetime_obj.year,
        'month': datetime_obj.month,
        'day': datetime_obj.day
    }

def update_loginname_enddate_licenses(dag_run, licenses):
    return {
        "target": {
            "uri": rail.result('get_user_details_from_replicon')['userDetails']['uri']
        },
        "template": null,
        "modifications": {
            "loginName": {
                "value": rail.result("get_user_details_from_replicon")["securityConfiguration"]["loginName"] + "_"
                    + dag_run.conf['user_details']['terminationdate']
            },
            "employmentDateRange": {
                "value": {
                    "startDate": get_datetime_obj(dag_run.conf['user_details']['startdate']),
                    "endDate": get_datetime_obj(dag_run.conf['user_details']['terminationdate']),
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            } if dag_run.conf['user_details']['terminationdate'] and
                dag_run.conf['user_details']['terminationdate'] != '0000-00-00' else null,
            "securitySettings": {
                "value": {
                    "loginEnabled": null,
                    "forcePasswordChange": null,
                    "ssoName": {
                        "value": rail.result("get_user_details_from_replicon")["securityConfiguration"]["loginName"] + "_"
                            + dag_run.conf['user_details']['terminationdate']
                    }
                }
            },
            "products": [
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:remove",
                    "items": [
                      {
                        "uri": null,
                        "name": license_name
                      }
                    ]
                } for license_name in licenses
            ],
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_update_user_loginname_in_replicon_payload(dag_run):
    return {
        "target": {
            "uri": dag_run.conf['replicon_user_details']['userDetails']['uri']
        },
        "template": null,
        "modifications": {
            "loginName": {
                "value": dag_run.conf["user_details"]["workemail"]
            },
            "securitySettings": {
                "value": {
                    "loginEnabled": null,
                    "forcePasswordChange": null,
                    "ssoName": {
                        "value": dag_run.conf["user_details"]["workemail"]
                    }
                }
            },
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def update_loginname_licenses(dag_run, licenses):
    return {
        "target": {
            "uri": dag_run.conf['replicon_user_details']['userDetails']['uri']
        },
        "template": null,
        "modifications": {
            "loginName": {
                "value": dag_run.conf["user_details"]["workemail"]
            },
            "securitySettings": {
                "value": {
                    "loginEnabled": {
                        "value": "true"
                    },
                    "forcePasswordChange": null,
                    "ssoName": {
                        "value": dag_run.conf["user_details"]["workemail"]
                    }
                }
            },
            "products": [
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                    "items": [
                      {
                        "uri": null,
                        "name": license_name
                      }
                    ]
                } for license_name in licenses
            ],
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_oef_values_payload(oef_uri):
    return {
        "page": "1",
        "pageSize": "10000",
        "objectExtensionTagDefinitionUri": oef_uri,
        "textSearch": null
    }
