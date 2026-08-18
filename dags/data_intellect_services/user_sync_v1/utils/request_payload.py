from datetime import datetime
import functools
import uuid
from data_intellect_services.user_sync_v1.utils import python_callable
from data_intellect_services.user_sync_v1.mapper.time_zones import time_zones_mapper
import rail

null = None

def get_user_details_from_replicon(dag_run):
    return {
        "users": [
            {
                "employeeId": dag_run.conf["user_details"]["employee_id"] or 
                    rail.result("get_user_details_from_hibob")["work"]["employeeIdInCompany"],
                "loginName": null,
                "parameterCorrelationId": null
            }
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }

def get_add_job_title_dropdown_payload():
    updated_dropdown_list = list(map(lambda dropdown_data: {
        "target": {
	    	"uri": dropdown_data["uri"],
	    	"name": dropdown_data["displayText"]
	    },
	    "name": dropdown_data["displayText"],
	    "isEnabled": dropdown_data["isEnabled"]
    }, rail.result("get_job_title_dropdown_options")))
    updated_dropdown_list.append(
        {
            "target": {
		        "uri": null,
		        "name": rail.result("get_human_readable_data_from_hibob")['title']
	        },
	        "name": rail.result("get_human_readable_data_from_hibob")['title'],
	        "isEnabled": "true"
        }
    )
    return {
	    "customFieldUri": rail.result("get_job_title_customfield_uri"),
	    "customFieldDropDownOptionUris": updated_dropdown_list
    }

@functools.lru_cache(maxsize=128)
def get_create_draft_new_role():
    return rail.result("create_draft_new_role_in_replicon")

def get_update_role_name_payload():
    return {
        "projectRoleUri": get_create_draft_new_role(),
        "name": rail.result("get_human_readable_data_from_hibob")["primary_role"]
    }

def get_enable_role_payload():
    return {
        "projectRoleUri": get_create_draft_new_role()
    }

def get_update_isbillable_payload():
    return {
    	"projectRoleUri": get_create_draft_new_role(),
    	"isBillable": "true"
    }

def get_update_cost_rate_payload():
    return {
    	"projectRoleUri": rail.result("publish_draft_new_role")["uri"],
    	"dateRange": null,
    	"rate": {
    		"amount": 0,
    		"currencyUri": rail.result("get_required_currency")
    	}
    }

def get_update_billing_rate_payload():
    return {
    	"projectRoleUri": rail.result("publish_draft_new_role")["uri"],
    	"dateRange": null,
    	"rate": {
    		"amount": 0,
    		"currencyUri": rail.result("get_required_currency")
    	}
    }

def get_publish_draft_new_role():
    return {
	    "draftUri": get_create_draft_new_role()
    }

def assign_role_payload():
    return {
    	"userUri": rail.result("create_user_in_replicon")["uri"],
    	"scheduleEntries": [
    		{
    			"effectiveDate": null,
    			"projectRoles": [
    				{
    					"isPrimary": "true",
    					"projectRole": {
    						"uri": rail.result("get_user_primary_role_from_replicon") or rail.result("publish_draft_new_role")["uri"]
    					}
    				}
    			]
    		}
    	]
    }

def get_time_zone_from_mapper(site):
    return rail.find_first_by_attr_and_get_attr(time_zones_mapper, "location", site, "IANA_name")

def get_create_user_payload(dag_run):
    permission_set = ["Project Resource", "Supervisor"] if  dag_run.conf['user_details']['is_manager'] else ["Project Resource"]
    return {
        "user": {
            "target": {
                "uri": null,
                "loginName": dag_run.conf['user_details']['email'],
                "employeeId": dag_run.conf['user_details']["employee_id"],
                "parameterCorrelationId": null
            },
            "displayNameParameter": {
                "displayName": dag_run.conf['user_details']['firstname'] + " " + dag_run.conf['user_details']['lastname']
            },
            "firstname": dag_run.conf['user_details']['firstname'],
            "lastname": dag_run.conf['user_details']['lastname'],
            "emailAddress": dag_run.conf['user_details']['email'],
            "employeeId": dag_run.conf['user_details']["employee_id"],
            "workWeekStartDayUri": "urn:replicon:day-of-week:monday",
            "employmentDateRange": {
                "startDate": python_callable.split_startdate(dag_run.conf['user_details']['startdate'])
                    if dag_run.conf['user_details']['startdate'] else null,
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:sso"
                ],
                "isLoginEnabled": "true",
                "loginName": dag_run.conf['user_details']['email'],
                "SSOName": dag_run.conf['user_details']['email'],
                "password": null
            },
            "permissionSets": [
                {
                    "uri": null,
                    "name": permission_role
                } for permission_role in permission_set],
            "policySets": [
                {
                    "uri": null,
                    "name": "Timesheet - DISL"
                },
                {
                    "uri": null,
                    "name": "Time Off"
                },
                {
                    "uri": null,
                    "name": "Expenses"
                }
            ],
            "assignedActivities": [
                {
                    "uri": null,
                    "name": "1- Work Week 7am-7pm"
                },
                {
                    "uri": null,
                    "name": "2- Weekend Work"
                },
                {
                    "uri": null,
                    "name": "3- Bank Holiday Work"
                },
                {
                    "uri": null,
                    "name": "4- Night Work 7pm-7am"
                }
            ],
            "timesheetPeriodSchedule": [
                {
                    "timesheetPeriod": {
                        "uri": null,
                        "name": "Weekly starting on a Saturday"
                    },
                    "effectiveDate": null
                }
            ],
        }
    }

def get_parent(path_length, group_full_path):
    if path_length > 0:
        return {
                "uri": null,
        		"name": group_full_path[path_length - 1],
        		"parameterCorrelationId": null,
                "parent": get_parent(path_length - 1, group_full_path)
        }
    return null

def get_department_parents():
    departments_full_path = (rail.result("get_required_department_full_path")["department_full_path"]).split("/") \
        if rail.result("get_required_department_full_path") else null
    return get_parent(len(departments_full_path), departments_full_path)

def get_employee_type_parents():
    employee_type_full_path = (rail.result("get_required_employee_type")["employee_type_full_path"]).split("/") \
        if rail.result("get_required_employee_type") else null
    return get_parent(len(employee_type_full_path), employee_type_full_path)

def get_job_title_dropdown_uri(title):
    return (rail.find_first_by_attr_and_get_attr(
        rail.result("get_job_title_dropdown_options"), 'displayText', rail.result("get_human_readable_data_from_hibob")["title"], 'uri') or \
        rail.find_first_by_attr_and_get_attr(rail.result("get_updated_job_title_dropdown_options"), 'displayText',
        rail.result("get_human_readable_data_from_hibob")["title"], 'uri')) if title else null

def get_apply_modifications_user_payload(dag_run):
    return {
    	"user": {
    		"uri": rail.result("create_user_in_replicon")["uri"],
    		"loginName": null,
    		"employeeId": null,
    		"parameterCorrelationId": null
    	},
    	"modifications": {
    		"timezoneToApply": {
    			"userTimeZoneModificationOptionUri": "urn:replicon:user-time-zone-modication-option:use-specified-time-zone",
                "timezone": {
                    "uri": null,
                    "IANAName": get_time_zone_from_mapper(rail.result("get_human_readable_data_from_hibob")["location"])
                },
    		} if get_time_zone_from_mapper(rail.result("get_human_readable_data_from_hibob")["location"]) else null,
    		"workWeekStartToApply": null,
    		"holidayCalendarToApply": {
    			"holidayCalendar": {
                    "uri": rail.result("get_holiday_calendar_based_on_location")[0]["uri"],
                    "name": null
                },
    		} if rail.result("get_holiday_calendar_based_on_location") else null,
    		"holidayCalendarAssignmentsToApply": null,
    		"schedulePolicyToApply": {
    			"userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
    			"replacementSchedule": [],
    			"updateScheduleOverDateRange": {
                    "replacementScheduleEntries": [
                        {
                            "schedulePolicy": {
                                "officeScheduleUri": null,
                                "name": null,
                                "officeSchedule": {
    						        "officeScheduleUri": null,
    						        "name": "8 hours/day; Mon-Fri",
                                },
                                "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                            },
                            "effectiveDate": null
                        }
                    ],
                    "endDate": null
                }
    		},
            "supervisorsToApply": {
                "initialSupervisor": {
                    "uri": rail.result("get_user_supervisor_from_replicon")["userDetails"]["uri"],
                    "loginName": null,
                    "employeeId": null,
                    "parameterCorrelationId": null
                },
                "supervisorScheduleEntries": []
            } if rail.result("get_user_supervisor_from_replicon") else null,
    		"locationScheduleToApply": {
    			"userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
    			"replacementLocationSchedule": [],
    			"updateLocationScheduleOverDateRange": {
                    "replacementLocationScheduleEntries": [
                        {
                            "location": {
                                "uri": rail.result("get_required_location"),
                                "parentUri": null,
                                "name": null
                            },
                            "effectiveDate": null
                        }
                    ],
                    "endDate": null
                }
    		} if rail.result("get_required_location") else null,
    		"divisionScheduleToApply": null,
    		"costCenterScheduleToApply": {
    			"userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
    			"replacementCostCenterSchedule": [],
    			"updateCostCenterScheduleOverDateRange": {
                    "replacementCostCenterScheduleEntries": [
                        {
                            "costCenter": {
                                "uri": rail.result("get_required_costcenter"),
                                "parentUri": null,
                                "name": null
                            },
                            "effectiveDate": null
                        }
                    ],
                    "endDate": null
                }
    		} if rail.result("get_required_costcenter") else null,
    		"departmentGroupScheduleToApply": {
    			"userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
    			"replacementDepartmentGroupSchedule": [],
    			"updateDepartmentGroupScheduleOverDateRange": {
                    "replacementDepartmentGroupScheduleEntries": [
                        {
                            "departmentGroup": get_department_parents(),
                            "effectiveDate": null
                        }
                    ],
                    "endDate": null
                }
    		} if rail.result("get_required_department_full_path") and get_department_parents() else null,
    		"employeeTypeGroupScheduleToApply": {
    			"userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
    			"replacementEmployeeTypeGroupSchedule": [],
    			"updateEmployeeTypeGroupScheduleOverDateRange": {
                    "replacementEmployeeTypeGroupScheduleEntries": [
                        {
                            "employeeTypeGroup": get_employee_type_parents(),
                            "effectiveDate": null
                        }
                    ],
                    "endDate": null
                }
    		} if rail.result("get_required_employee_type") else null,
    		"projectRoleAssignmentSchedulesToApply": {
    			"projectRoleAssignmentSchedulesToPut": [
    				{
    					"projectRoles": [
    						{
    							"projectRole": {
    								"uri": rail.result("get_user_primary_role_from_replicon") or rail.result("publish_draft_new_role")["uri"],
    								"name": null
    							},
    							"isPrimary": "true"
    						}
    					],
    					"effectiveDate": null
    				}
    			],
    			"modificationUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range"
    		} if rail.result("get_user_primary_role_from_replicon") or rail.result("publish_draft_new_role") else null,
    		"customFieldValuesToApply": [
                {
                    "customField": {
                        "uri": rail.result("get_job_title_customfield_uri"),
                        "name": null,
                        "groupUri": null
                    },
                    "text": null,
                    "date": null,
                    "dropDownOption": {
                        "uri": get_job_title_dropdown_uri(dag_run.conf["user_details"]["title"]),
                        "name": null
                    },
                    "number": null
                }
            ] if rail.result("get_job_title_customfield_uri") and rail.result("get_job_title_dropdown_options") and
                    get_job_title_dropdown_uri(dag_run.conf["user_details"]["title"]) else []
    	},
    	"userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

def get_date_json(effective_date):
    date_obj = datetime.strptime(effective_date, "%Y-%m-%d")
    return {
        "year": date_obj.year,
        "month": date_obj.month,
        "day": date_obj.day
    }

def is_supervisor_changed():
    return rail.result("get_user_supervisor_from_replicon")["userDetails"]["uri"] != rail.result(
        "get_supervisor_assignment_details", "supervisor").get("uri", "")

def is_department_changed():
    return rail.result("get_required_department_full_path") and "uri" in rail.result("get_required_department_full_path") and \
        rail.result("get_required_department_full_path")["uri"] != rail.result(
            "get_effectiveusergroupmembership","department").get("uri", "")

def is_cost_center_changed():
    return rail.result("get_required_costcenter") and rail.result("get_required_costcenter") != rail.result(
        "get_effectiveusergroupmembership","costcenter").get("uri", "")

def is_location_changed():
    return rail.result("get_required_location") and rail.result("get_required_location") != rail.result(
        "get_effectiveusergroupmembership","location").get("uri", "")

def is_employment_type_changed():
    return rail.result("get_required_employee_type") and "uri" in rail.result("get_required_employee_type") and \
        rail.result("get_required_employee_type")["uri"] != rail.result(
            "get_effectiveusergroupmembership","employeetype").get("uri", "")

def is_job_title_changed(dag_run):
    return rail.find_first_by_attr_and_get_attr(dag_run.conf["replicon_user_details"]["userDetails"]["customFieldValues"],
        "customField.uri", rail.result("get_job_title_customfield_uri"), "text") != dag_run.conf["user_details"]["title"]

def get_apply_emp_work_details_user_payload(dag_run):
    return {
    	"user": {
    		"uri": dag_run.conf["replicon_user_details"]["userDetails"]["uri"],
    		"loginName": null,
    		"employeeId": null,
    		"parameterCorrelationId": null
    	},
    	"modifications": {
    		"timezoneToApply": {
    			"userTimeZoneModificationOptionUri": "urn:replicon:user-time-zone-modication-option:use-specified-time-zone",
                "timezone": {
                    "uri": null,
                    "IANAName": get_time_zone_from_mapper(rail.result("get_human_readable_data_from_hibob")["location"])
                },
    		} if is_location_changed() and get_time_zone_from_mapper(rail.result("get_human_readable_data_from_hibob")["location"]) else null,
    		"locationScheduleToApply": {
    			"userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
    			"replacementLocationSchedule": [],
    			"updateLocationScheduleOverDateRange": {
                    "replacementLocationScheduleEntries": [
                        {
                            "location": {
                                "uri": rail.result("get_required_location"),
                                "parentUri": null,
                                "name": null
                            },
                            "effectiveDate": get_date_json(dag_run.conf["user_details"]["effective_date"])
                        }
                    ],
                    "endDate": null
                }
    		} if rail.result("get_required_location") and is_location_changed() else null,
    		"costCenterScheduleToApply": {
    			"userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
    			"replacementCostCenterSchedule": [],
    			"updateCostCenterScheduleOverDateRange": {
                    "replacementCostCenterScheduleEntries": [
                        {
                            "costCenter": {
                                "uri": rail.result("get_required_costcenter"),
                                "parentUri": null,
                                "name": null
                            },
                            "effectiveDate": get_date_json(dag_run.conf["user_details"]["effective_date"])
                        }
                    ],
                    "endDate": null
                }
    		} if rail.result("get_required_costcenter") and is_cost_center_changed() else null,
    		"departmentGroupScheduleToApply": {
    			"userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
    			"replacementDepartmentGroupSchedule": [],
    			"updateDepartmentGroupScheduleOverDateRange": {
                    "replacementDepartmentGroupScheduleEntries": [
                        {
                            "departmentGroup": get_department_parents(),
                            "effectiveDate": get_date_json(dag_run.conf["user_details"]["effective_date"])
                        }
                    ],
                    "endDate": null
                }
    		} if rail.result("get_required_department_full_path") and is_department_changed() and get_department_parents() else null,
            "employeeTypeGroupScheduleToApply": {
    			"userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
    			"replacementEmployeeTypeGroupSchedule": [],
    			"updateEmployeeTypeGroupScheduleOverDateRange": {
                    "replacementEmployeeTypeGroupScheduleEntries": [
                        {
                            "employeeTypeGroup": get_employee_type_parents(),
                            "effectiveDate": get_date_json(dag_run.conf["user_details"]["effectivedateemptype"])
                        }
                    ],
                    "endDate": null
                }
    		} if rail.result("get_required_employee_type") and is_employment_type_changed() and get_employee_type_parents() else null,
    		"customFieldValuesToApply": [
                {
                    "customField": {
                        "uri": rail.result("get_job_title_customfield_uri"),
                        "name": null,
                        "groupUri": null
                    },
                    "text": null,
                    "date": null,
                    "dropDownOption": {
                        "uri": get_job_title_dropdown_uri(dag_run.conf["user_details"]["title"]),
                        "name": null
                    },
                    "number": null
                }
            ] if rail.result("get_job_title_customfield_uri") and rail.result("get_job_title_dropdown_options") and is_job_title_changed(dag_run)
                    and get_job_title_dropdown_uri(dag_run.conf["user_details"]["title"]) else []
    	},
    	"userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

def get_display_name(first_name, last_name, dag_run):
    old_first_name = dag_run.conf["hibob_user_details"]["firstName"]
    old_last_name = dag_run.conf["hibob_user_details"]["surname"]
    return f'{first_name} {last_name}' if first_name and last_name else (f'{old_first_name} {last_name}'
            if first_name is null and last_name else (f'{first_name} {old_last_name}' if first_name and last_name is null
                else null))

def get_date_json_for_hibob(effective_date):
    date_obj = datetime.strptime(effective_date, "%d/%m/%Y")
    return {
        "year": date_obj.year,
        "month": date_obj.month,
        "day": date_obj.day
    }

def get_apply_basic_details_user_payload(dag_run):
    user_basic_details = dag_run.conf["user_details"]
    user_details_from_hibob = dag_run.conf["hibob_user_details"]
    return {
    	"user": {
    		"uri": dag_run.conf["replicon_user_details"]["userDetails"]["uri"],
    		"loginName": null,
    		"employeeId": null,
    		"parameterCorrelationId": null
    	},
    	"modifications": {
            "userDetailsToApply": {
                "firstName": user_details_from_hibob["firstName"] if user_basic_details["firstname"] else null,
                "lastName": user_details_from_hibob["surname"] if user_basic_details["lastname"] else null,
                "employmentStartDate": {
                  "date": get_date_json_for_hibob(user_details_from_hibob["work"]["startDate"])
                } if user_basic_details["startdate"] else null,
                "employmentEndDate": {
                  "date": get_date_json(user_basic_details["enddate"])
                } if user_basic_details["enddate"] else null,
                "employeeId": null,
                "displayNameParameter": {
                    "displayName": get_display_name(user_details_from_hibob["firstName"], user_details_from_hibob["surname"], dag_run)
                } if user_basic_details["firstname"] or user_basic_details["lastname"] else null
            } if user_basic_details else null,
            "securitySettingsToApply": {
                "loginEnabled": "true" if user_basic_details["status"] == "Active"
                    else ("false" if user_basic_details["status"] == "Inactive" else null)
            } if user_basic_details else null,
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }
def get_resource_pools_payload():
    return {
        "resourcePool": {
            "uri": null,
            "slug": null,
            "name": rail.result("get_human_readable_data_from_hibob")["primary_role"]
        }
    }

def get_create_resource_payload():
    return {
        "resourcePool": {
            "target": null,
            "name": rail.result("get_human_readable_data_from_hibob")["primary_role"],
            "code": null,
            "description": null,
            "isEnabled": "true",
            "poolManager": {
                "uri": null,
                "loginName": "david.richardson@dataintellect.com",
                "employeeId": null,
                "parameterCorrelationId": null
            }
        },
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_assign_resource_pool_payload(user_uri):
    return {
        "user": {
            "uri": user_uri
        },
        "resourcePool": {
            "uri": rail.result("get_resource_pool_from_replicon")["uri"] or rail.result("create_resource_pool_in_replicon")["uri"]
        },
        "resourcePoolUserAssignmentOptionUri": "urn:replicon:user-resource-pool-assignment-option:assign"
    }

def get_required_employee_type_payload():
    return {
        "page": "1",
        "pagesize": "1000",
        "columnUris": [
          "urn:replicon:employee-type-group-list-column:full-path"
        ],
        "sort": [],
        "filterExpression": null
    }

def get_required_departments_payload():
    return {
        "page": "1",
        "pagesize": "1000",
        "columnUris": [
            "urn:replicon:department-group-list-column:full-path"
        ],
        "sort": [],
        "filterExpression": null
    }

def get_assign_primary_role_payload(dag_run):
    return {
        "userUri": dag_run.conf["replicon_user_details"]["userDetails"]["uri"],
        "scheduleEntries": [
            {
                "effectiveDate": get_date_json(dag_run.conf["user_details"]["effective_date"]),
                "projectRoles": [
                    {
                        "isPrimary": "true",
                        "projectRole": {
                            "uri": rail.result("get_user_primary_role_from_replicon") or rail.result("publish_draft_new_role")["uri"]
                        }
                    }
                ]
            }
        ]
    }

def put_notification_pref_payload():
    return {
        "user": {
            "uri": rail.result("create_user_in_replicon")["uri"],
            "loginName": null,
            "employeeId": null,
            "parameterCorrelationId": null
        },
        "preferences": {
            "notificationDeliveryPreferences": list(map(lambda notification_pref_data:
                {
                    "objectTypeUri": notification_pref_data["objectTypeUri"],
                    "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
                }, rail.result("get_notification_preferences_for_user")["notificationDeliveryPreferences"])),
            "sharedDeliveryPreferenceOptionUris": [
                "urn:replicon:user-shared-delivery-preference-option:do-not-deliver-on-non-work-days"
            ]
        }
    }
