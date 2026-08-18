from datetime import datetime
import hashlib
import rail

from cohnreznick.user_sync.mapper.user_sync_mapper import user_sync_mapper

CONTRACTORS = "Contractors"

null = None

def get_today_date():
    now = datetime.now()
    return {
        'year': now.year,
        'month': now.month,
        'day': now.day
    }

def get_replicon_date(date_str):
    if not date_str:
        return None

    try:
        date = datetime.strptime(date_str, '%m/%d/%Y')
        return {
            'year': date.year,
            'month': date.month,
            'day': date.day
        }
    except:  # pylint: disable=bare-except
        return None

def get_create_md5_data(item):
    if not item:
        return []
    res = {
        **dict(item.items()),
        **{
        'md5': hashlib.md5((str(item["employeeid"])+","+str(item["employeenumber"])+","+str(item["company"])+","
                            + str(item["preferredfirstname"])+"," + str(item["firstname"]) + "," + str(item["lastname"])+","
                            + str(item["email"])+"," + str(item["startdate"]) +"," + str(item["status"])+"," + str(item["enddate"])+"," +
                            str(item["employeetype"])+"," + str(item["locationcode"])+","+ str(item["locationname"])+"," + str(item["departmentcode"]) +
                            "," + str(item["departmentname"])+","+ str(item["servicecentercode"])+"," +str(item["servicecentername"]) +
                            "," + str(item["costcentercode"])+","+ str(item["costcentername"])+"," + str(item["divisioncode"])+"," +
                            str(item["divisionname"])+","+ str(item["workschedule"])+"," + str(item["timeentrysystem"]) +
                            "," + str(item["activitytypecode"])+","+ str(item["activitytypedescription"])).encode('utf-8')).hexdigest()
        }
    }

    return dict(res.items())

MANDATORY_FIELDS = {
        "employeeid":"EecEEID",
        "employeenumber": "Employee Number",
        "company": "Company",
        "firstname": "First Name",
        "lastname": "Last Name",
        "email": "Employee Email",
        "startdate": "Last Hire Date",
        "status": "Employee Status",
        "employeetype": "Employee Type",
        "locationcode": "Location Code",
        "locationname": "Location Name",
        "departmentcode": "Org Level 3 code",
        "departmentname": "Org Level 3",
        "servicecentercode": "Service Center Code",
        "servicecentername": "Service Center Name",
        "costcentercode": "Cost Center Code",
        "costcentername": "Cost Center Name",
        "divisioncode": "Pay group code",
        "divisionname": "Pay group Name",
        "workschedule": "Work Schedule",
        "timeentrysystem": "Time Entry System",
        "activitytypecode": "Activity type Code",
        "activitytypedescription": "Activity Type Description",
}

def get_mandatory_fields_exception_message(item):
    missing_fields = []
    for payload_key, log_value in MANDATORY_FIELDS.items():
        if not item[payload_key]:
            missing_fields.append(f"{log_value} is not present in payload")
    return rail.smartjoin_by_delim(missing_fields, ";")

def get_costcenter_payload():
    return {
        "page": "1",
        "pagesize": "1000000",
        "columnUris": [
            "urn:replicon:cost-center-list-column:cost-center",
            "urn:replicon:cost-center-list-column:code"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:cost-center-list-filter:effectively-enabled"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": null,
                    "uris": [],
                    "bool": "true",
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
        }
    }

def get_location_payload():
    return {
        "page": "1",
        "pagesize": "1000000",
        "columnUris": [
            "urn:replicon:location-list-column:location",
            "urn:replicon:location-list-column:code"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:location-list-filter:effectively-enabled"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": null,
                    "uris": [],
                    "bool": "true",
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
        }
    }

def get_dept_group_payload():
    return {
        "page": "1",
        "pagesize": "1000000",
        "columnUris": [
            "urn:replicon:department-group-list-column:department-group",
            "urn:replicon:department-group-list-column:code"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:department-group-list-filter:effectively-enabled"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": null,
                    "uris": [],
                    "bool": "true",
                    "date": null,
                    "money": null,
                    "number": null,
                    "text": null,
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }

def get_all_employee_grp_payload():
    return {
        "page": "1",
        "pagesize": "100000000",
        "columnUris": [
            "urn:replicon:employee-type-group-list-column:employee-type-group",
            "urn:replicon:employee-type-group-list-column:code"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:employee-type-group-list-filter:effectively-enabled"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": null,
                    "uris": [],
                    "bool": "true",
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
        }
    }

def get_user_data_payload(dag_run):
    return{
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:login-name",
            "urn:replicon:user-list-column:employee-id",
            "urn:replicon:user-list-column:enabled"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:user-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
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
                    "text": dag_run.conf['employeeid'],
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null,
                    "dateTimeUtcRange": null
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }
def get_timesheet_template_name(locationname, costcentercode, costcentername):
    if "California" in locationname:
        timesheettemplate_mapper = list(filter(lambda x: x['type'] == 'timesheettemplate' and x['location']=='California'
            and x['costcentercode']==costcentercode and x['costcentername']== costcentername, user_sync_mapper))
        if timesheettemplate_mapper:
            return timesheettemplate_mapper[0]['timesheettemplate']
        return list(filter(lambda x: x['type'] == 'timesheettemplate' and x['location']=='California'
            and x['costcentercode']=="other" and x['costcentername']== "other", user_sync_mapper))[0]['timesheettemplate']
    timesheettemplate_mapper = list(filter(lambda x: x['type'] == 'timesheettemplate' and x['location']=="other"
            and x['costcentercode']==costcentercode and x['costcentername']== costcentername, user_sync_mapper))
    if timesheettemplate_mapper:
        return timesheettemplate_mapper[0]['timesheettemplate']
    return list(filter(lambda x: x['type'] == 'timesheettemplate' and x['location']=="other"
            and x['costcentercode']=="other" and x['costcentername']== "other", user_sync_mapper))[0]['timesheettemplate']

def get_payrulescript_name(locationname, employeetype):
    if employeetype in ["Exempt Hourly", "Exempt Salary"]:
        return list(filter(lambda x: x['type'] == 'payrule' and x['location']=='all'
            and x['employeetype']==employeetype, user_sync_mapper))[0]['payrule']

    if "California" in locationname:
        return list(filter(lambda x: x['type'] == 'payrule' and x['location']=='California'
            and x['employeetype']==employeetype, user_sync_mapper))[0]['payrule']
    return list(filter(lambda x: x['type'] == 'payrule' and x['location']=="other"
            and x['employeetype']==employeetype, user_sync_mapper))[0]['payrule']

def is_component_company_contractor(component_company_value):
    return CONTRACTORS == component_company_value

def get_contractors_service_center_details(config):
    service_center_data = rail.result("get_updated_service_centers")
    sub_contractor = list(filter(lambda service_center : service_center['name'] == config.DEFAULT_CONTRACTOR_SERVICE_CENTER_NAME
                and service_center['code'] == config.DEFAULT_CONTRACTOR_SERVICE_CENTER_CODE, service_center_data)
         )
    if sub_contractor:
        return sub_contractor[0]
    return {}

def get_process_users_conf(item, config):

    return {
        **dict(item.items()),
        **{
            'employeenumberdefinitionuri': rail.result('get_user_oefs')['employeenumberdefinitionuri'],
            'timesheettemplatename': get_timesheet_template_name(item['locationname'],item['costcentercode'],item['costcentername']),
            'timesheettemplateuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_policy_sets"),'displayText',get_timesheet_template_name(
                item['locationname'],item['costcentercode'],item['costcentername']),"uri"),
            'timesheetapprovalpathname': rail.find_first_by_attr_and_get_attr(rail.result('get_timesheet_approval_paths'),
                'displayText', 'Client Representative', 'displayText'),
            'timesheetapprovalpathuri': rail.find_first_by_attr_and_get_attr(rail.result('get_timesheet_approval_paths'),
                'displayText', 'Client Representative', 'uri'),
            'timesheetperiod': config.TIMESHEETPERIOD,
            'timeentryapprovalpathname': rail.find_first_by_attr_and_get_attr(rail.result('get_timeentry_approval_paths'),
                'name', 'Automatic Approval', 'name'),
            'timeentryapprovalpathuri': rail.find_first_by_attr_and_get_attr(rail.result('get_timeentry_approval_paths'),
                'name', 'Automatic Approval', 'uri'),
            'payrulescriptname': get_payrulescript_name(item['locationname'], item['employeetype']),
            'payrulescripturi': rail.find_first_by_attr_and_get_attr(rail.result("get_all_payrule_scripts"),'displayText',get_payrulescript_name(
                item['locationname'], item['employeetype']),"uri"),
            'projectresourcewithreportsuri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_set'),
                'displayText', 'Project Resource with Reports', 'uri'),
            'locationuri':rail.find_first_by_attr_and_get_attr(rail.result('get_updated_locations'), 'name', item['locationname'], 'uri'),
            'departmenturi':rail.find_first_by_attr_and_get_attr(rail.result('get_updated_departments'), 'name', item['departmentname'], 'uri'),
            'servicecenteruri':rail.find_first_by_attr_and_get_attr(rail.result('get_updated_service_centers'), 'name', item['servicecentername'], 'uri'),
            'costcenteruri':rail.find_first_by_attr_and_get_attr(rail.result('get_updated_costcenter'), 'name', item['costcentername'], 'uri'),
            'divisionuri':rail.find_first_by_attr_and_get_attr(rail.result('get_updated_divisions'), 'name', item['divisionname'], 'uri'),
            'employeetypeuri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_employeetypes'), 'name', item['employeetype'], 'uri'),
            'componentcompanydefinitionuri': rail.result('get_user_udfs')['companydefinitionuri'],
            'componentcompanydropdownuri': rail.find_first_by_attr_and_get_attr(rail.result('get_updated_componentcompany_udf_dropdown_values'),
                'name', item['company'], 'uri'),
            'activitytypecodedefinitionuri': rail.result('get_user_oefs')['activitytypecodedefinitionuri'],
            'activitytypedescriptiondefinitionuri': rail.result('get_user_oefs')['activitytypedescriptiondefinitionuri'],
            'timeentrysystemdefinitionuri': rail.result('get_user_udfs')['timeentrysystemdefinitionuri'],
        },
        **{
            "can_overwrite_service_center": "Yes" if is_component_company_contractor(item['company']) else "No",
            "contractors_service_center" : get_contractors_service_center_details(config)
        }
    }

def get_process_new_users_conf(dag_run):
    return {
        **dag_run.conf,
        **{
            'user_log' : rail.result('create_user_log')
        }
    }

def get_process_update_users_conf(dag_run):
    return {
        **dag_run.conf,
        **{
            'user_log': rail.result('create_user_log'),
            'useruri': rail.result('get_user_data')[0]['uri'],
            'userstatus':  rail.result('get_user_data')[0]['status'],
            'todays_date': get_today_date()
        }
    }

def test_valid_fields(dag_run):
    startdate = get_replicon_date(dag_run.conf['startdate'])
    if not startdate:
        return False
    if dag_run.conf['enddate']:
        enddate = get_replicon_date(dag_run.conf['enddate'])
        if not enddate:
            return False
    if dag_run.conf['status'] == "Disabled" and not dag_run.conf['enddate']:
        return False
    return True

def get_invalid_fields_message(dag_run):
    log=[]
    startdate = get_replicon_date(dag_run.conf['startdate'])
    if not startdate:
        log.append('Invalid format for Last Hire Date')
    if dag_run.conf['enddate']:
        enddate = get_replicon_date(dag_run.conf['enddate'])
        if not enddate:
            log.append('Invalid format for Termination Date')
    if dag_run.conf['status'] == "Disabled" and not dag_run.conf['enddate']:
        log.append('Employee Status field is Disabled in Feed File but Termination date is blank')
    if dag_run.conf['status'] == "Enabled" and dag_run.conf['enddate']:
        log.append('Employee Status field is Enabled in Feed File but Termination date is present')
    return rail.smartjoin_by_delim(log,";")

def get_authentication_type(dag_run):
    if dag_run.conf['timeentrysystem'] == 'GovUnanet':
        return "urn:replicon:user-authentication-type:replicon"
    return "urn:replicon:user-authentication-type:sso"

def get_oefs(userstatus, dag_run):
    oefs = []
    def add_text_oef(textvalue, definitionuri):
        oefs.append(
            {
                "definition": {
                    "uri": definitionuri,
                    "name": null
                },
                "tag": null,
                "numericValue": null,
                "textValue": textvalue,
                "fileValue": null,
                "jsonValue": null
            }
        )
    if userstatus == 'adduser':
        add_text_oef(dag_run.conf['employeenumber'], dag_run.conf['employeenumberdefinitionuri'])
        add_text_oef(dag_run.conf['activitytypecode'],dag_run.conf['activitytypecodedefinitionuri'])
        add_text_oef(dag_run.conf['activitytypedescription'],dag_run.conf['activitytypedescriptiondefinitionuri'])

    if userstatus == 'updateuser':
        current_employee_number = rail.find_first_by_attr_and_get_attr(rail.result('get_current_oef_values'),
            'definition.displayText', 'Employee Number', 'textValue')
        current_activitytypecode = rail.find_first_by_attr_and_get_attr(rail.result('get_current_oef_values'),
            'definition.displayText', 'Activity Type Code', 'textValue')
        current_activitytypedescription = rail.find_first_by_attr_and_get_attr(rail.result('get_current_oef_values'),
            'definition.displayText', 'Activity Type Description', 'textValue')
        if current_employee_number != dag_run.conf['employeenumber']:
            add_text_oef(dag_run.conf['employeenumber'], dag_run.conf['employeenumberdefinitionuri'])

        if current_activitytypecode != dag_run.conf['activitytypecode']:
            add_text_oef(dag_run.conf['activitytypecode'],dag_run.conf['activitytypecodedefinitionuri'])

        if current_activitytypedescription != dag_run.conf['activitytypedescription']:
            add_text_oef(dag_run.conf['activitytypedescription'],dag_run.conf['activitytypedescriptiondefinitionuri'])

    return oefs

def get_udfs(userstatus, dag_run):
    udfs = []
    def add_dropdown_udf(dropdownuri, definitionuri):
        udfs.append({
        "customField": {
          "uri": definitionuri,
          "name": null,
          "groupUri": null
        },
        "text": null,
        "date": null,
        "dropDownOption": {
          "uri": dropdownuri,
          "name": null
        },
        "numbr": null
      })

    def add_textvalue_udf(textvalue, definitionuri):
        udfs.append({
        "customField": {
          "uri": definitionuri,
          "name": null,
          "groupUri": null
        },
        "text": textvalue,
        "date": null,
        "dropDownOption": null,
        "numbr": null
      })

    if userstatus =='adduser':
        add_dropdown_udf(dag_run.conf['componentcompanydropdownuri'],dag_run.conf['componentcompanydefinitionuri'])
        add_textvalue_udf(dag_run.conf['timeentrysystem'],dag_run.conf['timeentrysystemdefinitionuri'])

    if userstatus == 'updateuser':
        current_componentcompany = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Component Company', 'text')
        current_timeentrysystem = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Time Entry System', 'text')

        if current_componentcompany != dag_run.conf['company']:
            add_dropdown_udf(dag_run.conf['componentcompanydropdownuri'],dag_run.conf['componentcompanydefinitionuri'])

        if current_timeentrysystem != dag_run.conf['timeentrysystem']:
            add_textvalue_udf(dag_run.conf['timeentrysystem'],dag_run.conf['timeentrysystemdefinitionuri'])

    return udfs

def get_servcie_center_schedule_to_add(dag_run):
    if dag_run.conf['can_overwrite_service_center'] == "No":
        return [
            {
                "serviceCenter": {
                "uri": dag_run.conf['servicecenteruri'],
                "parentUri": null,
                "name": null
                },
                "effectiveDate": null
            }
        ]

    if dag_run.conf['can_overwrite_service_center'] == "Yes":
        if dag_run.conf['contractors_service_center']:
            return [
            {
                "serviceCenter": {
                "uri": dag_run.conf['contractors_service_center']['uri'],
                "parentUri": null,
                "name": null
                },
                "effectiveDate": null
            }
        ]
        rail.set_result(key = "exception",val="`SubContractors ServiceCenter` not assigned as it is not found or disabled in Replicon")
    return []

def get_put_user_payload(dag_run,workweek):
    # pylint: disable=too-many-branches
    return {
        "user": {
            "target": {
                "uri": null,
                "loginName": dag_run.conf['email'],
                "parameterCorrelationId": null
            },
            "firstname": dag_run.conf['firstname'] if not dag_run.conf['preferredfirstname'] else
                    dag_run.conf['preferredfirstname'],
            "lastname": dag_run.conf['lastname'],
            "emailAddress": dag_run.conf['email'],
            "employeeId": dag_run.conf['employeeid'],
            "department": null,
            "supervisorAssignmentSchedule": null,
            "schedulePolicySchedule": [
                {
                    "schedulePolicy": {
                        "officeScheduleUri": null,
                        "name": null,
                        "officeSchedule": {
                            "officeScheduleUri": null,
                            "name": dag_run.conf['workschedule']
                        },
                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                    },
                    "effectiveDate": null
                }
            ],
            "workWeekStartDayUri": workweek,
            "employmentDateRange": {
                "startDate": get_replicon_date(dag_run.conf['startdate']),
                "endDate": get_replicon_date(dag_run.conf['enddate']) if dag_run.conf['enddate'] else null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                    get_authentication_type(dag_run)
                ],
                "isLoginEnabled": "true" if dag_run.conf['status'] == "Enabled" else "false",
                "loginName": dag_run.conf['email'],
                "SSOName": null if dag_run.conf['timeentrysystem'] == 'GovUnanet' else dag_run.conf['email'],
                "password": 'Replicon@1234' if dag_run.conf['timeentrysystem'] == 'GovUnanet' else null
            },
            "holidayCalendar": null,
            "timeOffPolicy": null,
            "permissionSets": [
                {
                    "uri": dag_run.conf['projectresourcewithreportsuri'],
                    "name": null
                }
            ],
            "policySets": [
                {
                    "uri": dag_run.conf['timesheettemplateuri'],
                    "name": null
                }
            ],
            "employeeType": null,
            "timesheetPeriodTypeUri": null,
            "costRateSchedule": null,
            "payrollRateSchedule": null,
            "defaultBillingRate": null,
            "timesheetApprovalPath": {
                "uri": dag_run.conf['timesheetapprovalpathuri'],
                "name": null
                },
            "expenseApprovalPath": null,
            "timeOffApprovalPath": null,
            "customFieldValues": get_udfs('adduser', dag_run),
            "assignedActivities": [],
            "timeZone":null,
            "overtimeRuleAssignmentSchedule": null,
            "validationRuleAssignmentSchedule": null,
            "locationSchedule": [
                {
                    "location": {
                        "uri": dag_run.conf['locationuri'],
                        "parentUri": null,
                        "name": null
                    },
                    "effectiveDate": null
                }
            ],
            "divisionSchedule":  [
                {
                    "division": {
                    "uri": dag_run.conf['divisionuri'],
                    "parentUri": null,
                    "name": null
                    },
                    "effectiveDate": null
                }
                ],
            "costCenterSchedule": [
                {
                    "costCenter": {
                    "uri": dag_run.conf['costcenteruri'],
                    "parentUri": null,
                    "name": null
                    },
                    "effectiveDate": null
                }
                ],
            "serviceCenterSchedule": get_servcie_center_schedule_to_add(dag_run),
            "departmentGroupSchedule": [
                {
                    "departmentGroup": {
                        "uri": dag_run.conf['departmenturi'],
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null
                    },
                    "effectiveDate": null
                }
            ],
            "employeeTypeGroupSchedule": [
                {
                    "employeeTypeGroup": {
                        "uri": dag_run.conf['employeetypeuri'],
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null
                    },
                    "effectiveDate": null
                }
            ],
            "timesheetPeriodSchedule": [
                {
                    "timesheetPeriod": {
                        "uri": null,
                        "name": dag_run.conf['timesheetperiod']
                    },
                    "effectiveDate": null
                }
            ],
            "policyDataAccessScopes": [],
            "policyDataAccessScopes2": [],
            "payRuleScriptSchedule": [
                {
                    "payRuleScript": {
                        "uri": dag_run.conf['payrulescripturi'],
                        "name": null
                    },
                    "effectiveDate": null
                }
            ],
            "displayNameParameter": null,
            "decimalSeparatorUri": null,
            "numberGroupSeparatorUri": null,
            "extensionFieldValues": get_oefs('adduser', dag_run)
        }
    }

def get_remove_timeoff_payload():
    return {
        "userUri": rail.result('add_new_user')['uri'],
        "timeOffTypeUris": []
    }

def test_timeentrysystem(dag_run):
    current_timeentrysystem = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
        'customField.displayText', 'Time Entry System', 'text')
    return not bool(current_timeentrysystem == dag_run.conf['timeentrysystem'])


def update_user_details(dag_run):
    user_details = rail.result("get_user_info")['userDetails']
    firstname = dag_run.conf['preferredfirstname'] if dag_run.conf['preferredfirstname'] else dag_run.conf['firstname']
    return {
      "firstName": firstname if user_details['firstName'] != firstname else null,
      "lastName": dag_run.conf['lastname'] if user_details['lastName'] != dag_run.conf['lastname'] else null,
      "emailAddress": {
        "emailAddress": dag_run.conf['email']
      } if user_details['emailAddress'] != dag_run.conf['email'] else null,
      "language": null,
      "employmentDateRange": null,
      "employmentStartDate": {
        "date": get_replicon_date(dag_run.conf['startdate'])
      } if user_details['employmentDateRange']['startDate'] != get_replicon_date(dag_run.conf['startdate']) else null,
      "employmentEndDate": {
        "date": get_replicon_date(dag_run.conf['enddate']) if bool(get_replicon_date(dag_run.conf['enddate'])) else null
      },
      "employeeId": null,
      "displayNameParameter": null
    }

def update_payrule_script(dag_run):
    current_payrulescript = rail.result("get_user_info")['payRuleScriptSchedule']
    if not current_payrulescript:
        return {
            "scheduleEntries": [
                {
                    "payRuleScript": {
                        "uri": dag_run.conf['payrulescripturi'],
                        "name": null
                    },
                    "effectiveDate": dag_run.conf['todays_date']
                }
            ]
        }

    if dag_run.conf['payrulescriptname'] != current_payrulescript[-1]['payRuleScript']['displayText']:
        return {
            "scheduleEntries": [
                {
                    "payRuleScript": {
                        "uri": dag_run.conf['payrulescripturi'],
                        "name": null
                    },
                    "effectiveDate": dag_run.conf['todays_date']
                }
            ]
        }

    return null

def update_location_grp(locationuri, currentlocationuri, dag_run):
    return {
        "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementLocationSchedule": [],
        "updateLocationScheduleOverDateRange": {
            "replacementLocationScheduleEntries": [
                {
                    "location": {
                        "uri": locationuri
                    },
                    "effectiveDate": dag_run.conf['todays_date']
                }
            ],
            "endDate": null
        }
    } if currentlocationuri != locationuri else null

def update_department_grp(departmenturi, currentdepartmenturi, dag_run):
    return {
        "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementDepartmentGroupSchedule": [],
        "updateDepartmentGroupScheduleOverDateRange": {
            "replacementDepartmentGroupScheduleEntries": [
                {
                    "departmentGroup": {
                        "uri": departmenturi
                    },
                    "effectiveDate": dag_run.conf['todays_date']
                }
            ],
            "endDate": null
        }
    } if departmenturi != currentdepartmenturi else null

def update_division_grp(divisionuri, currentdivisionuri, dag_run):
    return {
        "userDivisionScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementDivisionSchedule": [],
        "updateDivisionScheduleOverDateRange": {
            "replacementDivisionScheduleEntries": [
                {
                    "division": {
                        "uri": divisionuri
                    },
                    "effectiveDate": dag_run.conf['todays_date']
                }
            ],
            "endDate": null
        }
    } if divisionuri != currentdivisionuri else null

def update_costcenter_grp(costcenteruri, currentcostcenteruri, dag_run):
    return {
        "userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementCostCenterSchedule": [],
        "updateCostCenterScheduleOverDateRange": {
            "replacementCostCenterScheduleEntries": [
                {
                    "costCenter": {
                        "uri": costcenteruri
                    },
                    "effectiveDate": dag_run.conf['todays_date']
                }
            ],
            "endDate": null
        }
    } if costcenteruri != currentcostcenteruri else null

def update_servicecenter_grp(servicecenteruri, currentservicecenteruri, dag_run):
    if dag_run.conf['can_overwrite_service_center'] == "Yes":
        if dag_run.conf['contractors_service_center']:
            if currentservicecenteruri != dag_run.conf['contractors_service_center']['uri']:
                return {
                    "userServiceCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                    "replacementServiceCenterSchedule": [],
                    "updateServiceCenterScheduleOverDateRange": {
                        "replacementServiceCenterScheduleEntries": [
                        {
                            "serviceCenter": {
                            "uri": dag_run.conf['contractors_service_center']['uri'],
                            "parentUri": null,
                            "name": null
                            },
                            "effectiveDate": dag_run.conf['todays_date']
                        }
                        ],
                        "endDate": null
                    }
                }
            return null
        rail.set_result(key = "exception",val="`SubContractors ServiceCenter` not updated as it is not found or disabled in Replicon")
        return null

    return {
      "userServiceCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
      "replacementServiceCenterSchedule": [],
      "updateServiceCenterScheduleOverDateRange": {
        "replacementServiceCenterScheduleEntries": [
          {
            "serviceCenter": {
              "uri": servicecenteruri,
              "parentUri": null,
              "name": null
            },
            "effectiveDate": dag_run.conf['todays_date']
          }
        ],
        "endDate": null
      }
    } if servicecenteruri != currentservicecenteruri else null

def update_employeetype_grp(employeetypeuri, currentemployeetypeuri, dag_run):
    return {
        "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementEmployeeTypeGroupSchedule": [],
        "updateEmployeeTypeGroupScheduleOverDateRange": {
            "replacementEmployeeTypeGroupScheduleEntries": [
                {
                    "employeeTypeGroup": {
                        "uri": employeetypeuri
                    },
                    "effectiveDate": dag_run.conf['todays_date']
                }
            ],
            "endDate": null
        }
    } if employeetypeuri != currentemployeetypeuri else null

def update_schedule(dag_run):
    current_schedule = rail.result("get_user_info")['schedulePolicies']
    if not current_schedule:
        return  {
            "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
            "replacementSchedule": [],
            "updateScheduleOverDateRange": {
                "replacementScheduleEntries": [
                {
                    "schedulePolicy": {
                    "officeScheduleUri": null,
                    "name": dag_run.conf['workschedule'],
                    "officeSchedule": {
                        "officeScheduleUri": null,
                        "name": dag_run.conf['workschedule']
                    },
                    "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                    },
                    "effectiveDate": dag_run.conf['todays_date']
                }
                ],
                "endDate": null
            }
            }
    if dag_run.conf['workschedule'] != current_schedule[-1]['officeSchedule']['displayText']:
        return  {
            "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
            "replacementSchedule": [],
            "updateScheduleOverDateRange": {
                "replacementScheduleEntries": [
                {
                    "schedulePolicy": {
                    "officeScheduleUri": null,
                    "name": dag_run.conf['workschedule'],
                    "officeSchedule": {
                        "officeScheduleUri": null,
                        "name": dag_run.conf['workschedule']
                    },
                    "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                    },
                    "effectiveDate": dag_run.conf['todays_date']
                }
                ],
                "endDate": null
            }
            }
    return null

def update_permission_set(dag_run):
    if not rail.find_first_by_attr_and_get_attr(rail.result('get_user_info')['permissionSets'],
            'displayText', 'Project Resource with Reports', 'displayText'):
        return {
            "permissionSetUrisToAssign": [
                dag_run.conf['projectresourcewithreportsuri']
            ],
            "policyUrisToRemovePermissionSet": []
        }
    return null

def update_policy_set(dag_run):
    assigned_timesheet_template = rail.result(
        "get_user_info")['timesheetTemplate']
    if not assigned_timesheet_template:
        if dag_run.conf['timesheettemplateuri']:
            return {
                "policySetUrisToAssign": [
                    dag_run.conf['timesheettemplateuri']
                ],
                "policyUrisToRemovePolicySet": []
            }
        return null

    if (dag_run.conf['timesheettemplatename'] and dag_run.conf['timesheettemplateuri']) \
        and (dag_run.conf['timesheettemplatename'] != assigned_timesheet_template['displayText']):
        return {
            "policySetUrisToAssign": [
                dag_run.conf['timesheettemplateuri']
            ],
            "policyUrisToRemovePolicySet": []
        }
    return null

def update_security_settings(dag_run):
    current_status = rail.result("get_user_info")['userDetails']['isEnabled']
    authentication_type_sso = ['Replicon', 'GovUnanet and Replicon']
    current_timeentrysystem =  rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
        'customField.displayText', 'Time Entry System', 'text')
    def is_update_required():
        if current_timeentrysystem == dag_run.conf['timeentrysystem']:
            return False
        if current_timeentrysystem in authentication_type_sso and dag_run.conf['timeentrysystem'] in authentication_type_sso:
            return False
        return True

    def is_email_changed():
        return bool(rail.result("get_user_info")['userDetails']['emailAddress'] != dag_run.conf['email'])

    def is_status_changed():
        return bool(current_status) and dag_run.conf['status'] != "Enabled"

    if is_update_required() or is_email_changed() or is_status_changed():
        return {
            "loginEnabled": "true" if dag_run.conf['status'] == "Enabled" else "false",
            "forcePasswordChange": null if dag_run.conf['timeentrysystem'] in authentication_type_sso else "false",
            "loginName": dag_run.conf['email'],
            "ssoName": dag_run.conf['email'] if dag_run.conf['timeentrysystem'] in authentication_type_sso else null,
            "password": null if dag_run.conf['timeentrysystem'] in authentication_type_sso else 'Replicon@1234',
            "enabledAuthenticationTypeUris": [
                ("urn:replicon:user-authentication-type:sso" if dag_run.conf['timeentrysystem'] in authentication_type_sso else
                    "urn:replicon:user-authentication-type:replicon")
            ],
            "emailMFAResendVerificationEmail": "false",
            "emailMFATryAddMethodFromUsersEmail": "false",
            "clearIsLockedOut": "false"
            }
    return null

def apply_user_modifications_payload(dag_run):
    return {
        "user": {
            "uri": dag_run.conf['useruri']
        },
        "modifications": {
            "schedulePolicyToApply": update_schedule(dag_run),
            "locationScheduleToApply": update_location_grp(dag_run.conf['locationuri'],
                rail.result('get_effective_user_groupmembership','location').get('uri', ''), dag_run),
            "divisionScheduleToApply": update_division_grp(dag_run.conf['divisionuri'],
                rail.result('get_effective_user_groupmembership', 'division').get('uri', ''), dag_run),
            "costCenterScheduleToApply": update_costcenter_grp(dag_run.conf['costcenteruri'],
                rail.result('get_effective_user_groupmembership', 'costcenter').get('uri', ''), dag_run),
            "departmentGroupScheduleToApply": update_department_grp(dag_run.conf['departmenturi'],
                rail.result('get_effective_user_groupmembership', 'department').get('uri', ''), dag_run),
            "employeeTypeGroupScheduleToApply": update_employeetype_grp(dag_run.conf['employeetypeuri'],
                rail.result('get_effective_user_groupmembership', 'employeetype').get('uri', ''), dag_run),
            "serviceCenterScheduleToApply": update_servicecenter_grp(dag_run.conf['servicecenteruri'],
                rail.result('get_effective_user_groupmembership', 'servicecenter').get('uri', ''), dag_run),
            "permissionSetsToApply": update_permission_set(dag_run),
            "policySetsToApply": update_policy_set(dag_run),
            "securitySettingsToApply": update_security_settings(dag_run),
            "customFieldValuesToApply": get_udfs('updateuser', dag_run),
            "userDetailsToApply": update_user_details(dag_run),
            "payRulesScheduleModifications": update_payrule_script(dag_run),
            "objectExtensionFieldsToApply": get_oefs('updateuser', dag_run)
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }
