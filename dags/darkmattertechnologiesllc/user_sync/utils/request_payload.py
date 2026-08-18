# pylint: disable=unused-variable too-many-branches too-many-statements
from datetime import datetime
import rail
import uuid
from dateutil.parser import parse as date_parser
from darkmattertechnologiesllc.user_sync.utils import python_callable


null = None

def get_today_dateformat_payload():
    return get_datetime_obj(datetime.strftime(datetime.now(), "%m/%d/%Y"))

def get_datetime_obj(effectivedate):
    effective_date = datetime.strptime(effectivedate, '%m/%d/%Y')
    return {
        "year": effective_date.year,
        "month": effective_date.month,
        "day": effective_date.day
    }

def get_supervisor_assign_payload(dag_run):
    daterange = {}
    if 'caller' in dag_run.conf:
        daterange = {}
        if dag_run.conf['caller'] == 'update':
            daterange = {"startDate": get_today_dateformat_payload()}
    elif 'useruri' in dag_run.conf:
        daterange = {"startDate": get_today_dateformat_payload()}
    return {
        "userUri": dag_run.conf['useruri'] if 'useruri' in dag_run.conf else rail.result("create_user")['user']['uri'],
        "supervisorUri": rail.result('search_for_supervisor')[0]['userDetails']['uri'],
        "dateRange": daterange
    }

def update_emp_date_for_disableuser(dag_run, config):
    startdate = rail.result('search_user')[0]['userDetails']['employmentDateRange']['startDate']
    return {
        "userUri": rail.result('search_user')[0]['userDetails']['uri'],
        "dateRange": {
            "startDate": {
                "year": startdate['year'],
                "month": startdate['month'],
                "day": startdate['day']
            },
            "endDate": get_datetime_obj(dag_run.conf['firstdayofleave']) if (
                dag_run.conf['employeestatus'] in config.leave_status) else get_datetime_obj(dag_run.conf['enddate'])
        }
    }

def update_emp_daterange_startdate(dag_run):
    user_details = rail.result('get_user_details_for_update')[0]
    previous_status_value = rail.find_first_by_attr_and_get_attr(
        user_details['userDetails']['customFieldValues'], 'customField.displayText', 'Previous Status', 'text')
    user_startdate = str(user_details['userDetails']['employmentDateRange']['startDate']['month']) + '/' + str(
        user_details['userDetails']['employmentDateRange']['startDate']['day']) + '/' +str(
            user_details['userDetails']['employmentDateRange']['startDate']['year'])
    return {
        "userUri": dag_run.conf['useruri'],
        "dateRange": {
            "startDate": get_datetime_obj(dag_run.conf['startdate']) if previous_status_value == 'Terminated' else get_datetime_obj(user_startdate),
            "endDate": get_datetime_obj(dag_run.conf['enddate']) if dag_run.conf['enddate'] else null
        }
    }

def get_custom_field_values(customefield, value):
    if customefield == 'First Day of Leave':
        return {
            "value": {
                "customField": {
                    "name": customefield
                },
                "date": get_datetime_obj(value)
            }
        }
    return {
        "value": {
            "customField": {
                "name": customefield
            },
            "text": value
        }
    }

def get_custom_field_payload(dag_run):
    custom_field_payload = []
    if dag_run.conf['blackknightid']:
        custom_field_payload.append(get_custom_field_values('Black Night ID', dag_run.conf['blackknightid']))
    if dag_run.conf['businesstitle']:
        custom_field_payload.append(get_custom_field_values('Job Profile', dag_run.conf['businesstitle']))
    if dag_run.conf['manager2']:
        custom_field_payload.append(get_custom_field_values('Manager - Level 02', dag_run.conf['manager2']))
    if dag_run.conf['manager3']:
        custom_field_payload.append(get_custom_field_values('Manager - Level 03', dag_run.conf['manager3']))
    if dag_run.conf['manager4']:
        custom_field_payload.append(get_custom_field_values('Manager - Level 04', dag_run.conf['manager4']))
    if dag_run.conf['manager5']:
        custom_field_payload.append(get_custom_field_values('Manager - Level 05', dag_run.conf['manager5']))
    if dag_run.conf['manager6']:
        custom_field_payload.append(get_custom_field_values('Manager - Level 06', dag_run.conf['manager6']))
    if dag_run.conf['manager7']:
        custom_field_payload.append(get_custom_field_values('Manager - Level 07', dag_run.conf['manager7']))
    if dag_run.conf['manager8']:
        custom_field_payload.append(get_custom_field_values('Manager - Level 08', dag_run.conf['manager8']))
    if dag_run.conf['manager9']:
        custom_field_payload.append(get_custom_field_values('Manager - Level 09', dag_run.conf['manager9']))
    if dag_run.conf['manager10']:
        custom_field_payload.append(get_custom_field_values('Manager - Level 10', dag_run.conf['manager10']))
    if dag_run.conf['scheduledweeklyhours']:
        custom_field_payload.append(get_custom_field_values('Scheduled Weekly Hours', dag_run.conf['scheduledweeklyhours']))
    if dag_run.conf['fte']:
        custom_field_payload.append(get_custom_field_values('FTE', dag_run.conf['fte']))
    if dag_run.conf['returndatefromleave']:
        custom_field_payload.append(get_custom_field_values('Return Date from Leave', dag_run.conf['returndatefromleave']))
    if dag_run.conf['firstdayofleave']:
        custom_field_payload.append(get_custom_field_values('First Day of Leave', dag_run.conf['firstdayofleave']))
    if dag_run.conf['employeestatus']:
        custom_field_payload.append(get_custom_field_values('Previous Status', dag_run.conf['employeestatus']))
    return custom_field_payload

def get_createuser_payload(dag_run):
    return {
        "target": null,
        "modifications": {
            "firstName": {
                "value": dag_run.conf['firstname']
            },
            "lastName": {
                "value": dag_run.conf['lastname']
            },
            "loginName": {
                "value": dag_run.conf['loginname']
            },
            "emailAddress": {
                "value": dag_run.conf['loginname']
            },
            "employeeId": {
                "value": dag_run.conf['employeeid']
            },
            "employmentDateRange": {
                "value": {
                    "startDate": get_datetime_obj(dag_run.conf['startdate']),
                    "endDate": get_datetime_obj(dag_run.conf['enddate']) if dag_run.conf['enddate'] else null
                }
            },
            "securitySettings": {
                "value": {
                    "loginEnabled": {
                        "value": "1"
                    },
                    "ssoName": {
                        "value": dag_run.conf['loginname']
                    }
                }
            },
            "timesheetApprovalPath": {
                "value": {
                    "name": "Project Manager"
                }
            },
            "timesheetTemplate": {
                "value": {
                    "name": 'Standard Timesheet'
                }
            },
            "punchEntryPolicy": {
                "value": {
                    "name": "All Devices Access"
                }
            },
            "customFields": get_custom_field_payload(dag_run),
            "permissionSets": [
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                    "items": [
                        {
                            "permissionSetPolicy": {
                                "uri": dag_run.conf['enduser_permission']
                            }
                        }
                    ]
                }
            ],
            "locationSchedule": [{
                "dateRange": null,
                "item": {
                    "uri": dag_run.conf['location_uri']
                }
            }],
            "costCenterSchedule": [{
                "dateRange": null,
                "item": {
                    "uri": dag_run.conf['cost_center_uri']
                }
            }],
            "departmentGroupSchedule": [{
                "dateRange": null,
                "item": {
                    "uri": dag_run.conf['department_uri']
                }
            }],
            "employeeTypeGroupSchedule": [{
                "dateRange": null,
                "item": {
                    "uri": dag_run.conf['employee_type_uri']
                }
            }],
            "timesheetPeriodSchedule": [
                {
                    "dateRange": null,
                    "item": {
                        "name": "Monthly"
                    }
                }
            ]
        },
        "unitOfWorkId": str(uuid.uuid4())
}

def process_supervisor_data(item):
    return {
        "workermanager": item['properties']['workermanager'],
        "employeeid": item['properties']['employeeid'],
        "caller": item['properties']['caller'],
        "useruri": item['properties']['useruri'],
        "logger" : rail.result('user_import_log'),
        'supervisor_assignment_permission': rail.find_first_by_attr_and_get_attr(
            rail.result('get_required_permission_set'), 'displayText', 'Supervisor', 'uri', ''),
    }

def get_emp_type_full_path(item):
    return python_callable.get_full_path([item['workertype'], item['employeetype']])

def get_location_full_path(item):
    return python_callable.get_full_path([item['locationhierarchy'], item['locationname'],item['workstate'], item['workcity']])

def process_each_user_payload(item):
    return {
        'employeeid' : item['employeeid'],
        'blackknightid' : item['blackknightid'],
        'firstname' : item['firstname'],
        'lastname' : item['lastname'],
        'workertype' : item['workertype'],
        'employeetype' : item['employeetype'],
        'businesstitle' : item['businesstitle'],
        'costcenterid' : item['costcenterid'],
        'costcentername' : item['costcentername'],
        'departmentname' : item['departmentname'],
        'workermanager' : item['workermanager'],
        'locationhierarchy' : item['locationhierarchy'],
        'locationname' : item['locationname'],
        'workstate' : item['workstate'],
        'workcity' : item['workcity'],
        'scheduledweeklyhours' : item['scheduledweeklyhours'],
        'fte' : item['fte'],
        'startdate' : item['startdate'],
        'enddate' : item['enddate'],
        'loginname' : item['loginname'],
        'manager2' : item['manager2'],
        'manager3' : item['manager3'],
        'manager4' : item['manager4'],
        'manager5' : item['manager5'],
        'manager6' : item['manager6'],
        'manager7' : item['manager7'],
        'manager8' : item['manager8'],
        'manager9' : item['manager9'],
        'manager10' : item['manager10'],
        'employeestatus' : item['employeestatus'],
        'firstdayofleave' : item['firstdayofleave'],
        'returndatefromleave' : item['returndatefromleave'],
        'logger' : rail.result('user_import_log'),
        'supervisor_logger' : rail.result('supervisor_assignment_log'),
        'enduser_permission': rail.find_first_by_attr_and_get_attr(
            rail.result('get_required_permission_set'), 'displayText', 'Project Resource with Reports', 'uri', ''),
        'supervisor_assignment_permission': rail.find_first_by_attr_and_get_attr(
            rail.result('get_required_permission_set'), 'displayText', 'Supervisor', 'uri', ''),

        "employee_type_full_path": get_emp_type_full_path(item),
        "employee_type_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_updated_employee_types_from_replicon'),
                                                                    'full_path', get_emp_type_full_path(item), 'uri'),
        "location_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_updated_locations'),
                                                                'full_path', get_location_full_path(item), 'uri'),
        "location_full_path": get_location_full_path(item),
        "department_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_updated_departments'), 'name', item['departmentname'], 'uri'),
        "cost_center_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_updated_costcenters'), 'name', item['costcentername'], 'uri'),
    }

def process_update_user_payload(dag_run):
    return {
        **dag_run.conf,
        **{
            'logger' : rail.result('create_user_log'),
            'useruri' : rail.result('search_user')[0]['userDetails']['uri']
        }
    }

def process_add_user_payload(dag_run):
    return {
        **dag_run.conf,
        **{
            'logger' : rail.result('create_user_log')
        }
    }

MANDATORY_FIELDS = {
    "employeeid":"employeeid",
    "firstname": "firstname",
    "lastname": "lastname",
    "workertype": "workertype",
    "employeetype": "employeetype",
    "businesstitle": "businesstitle",
    "costcenterid": "costcenterid",
    "costcentername": "costcentername",
    "departmentname": "departmentname",
    "workermanager": "workermanager",
    "locationhierarchy": "locationhierarchy",
    "locationname": "locationname",
    "workstate": "workstate",
    "workcity": "workcity",
    "scheduledweeklyhours": "scheduledweeklyhours",
    "fte": "fte",
    "startdate": "startdate",
    "loginname": "loginname",
    "employeestatus": "employeestatus"
}

def get_mandatory_fields_exception_message(item):
    missing_fields = []
    for payload_key, log_value in MANDATORY_FIELDS.items():
        if not item[payload_key]:
            missing_fields.append(f"{log_value} is not present in payload")
    return rail.smartjoin_by_delim(missing_fields, ";")

def get_invalid_record(item):
    details = get_mandatory_fields_exception_message(item)
    return {
        "employeeid": item['employeeid'],
        "action": "Validation",
        "status": "Exception",
        "details": details
    }

def is_group_changed(group, group_value):
    return rail.result('get_effectivegroup_membership').get(group, {}).get('uri', '') != group_value

def get_group_value_to_update_payload(dag_run, logger, val, val_uri):
    if is_group_changed(val, dag_run.conf[val_uri]):
        logger.append(val + 'updated')
        return [{
            "dateRange": {
                "startDate": get_today_dateformat_payload()
            },
            "item": {
                "uri": dag_run.conf[val_uri]
            }
        }]
    return []

def get_employeetype_details_from_replicon():
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:employee-type-group-list-column:name",
            "urn:replicon:employee-type-group-list-column:employee-type-group",
            "urn:replicon:employee-type-group-list-column:full-path"
        ],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:employee-type-group-list-filter:effectively-enabled"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "value": {
                    "bool": "1"
                }
            }
        }
    }

def get_location_payload():
    return {
        "page": "1",
        "pagesize": "1000000",
        "columnUris": [
            "urn:replicon:location-list-column:name",
            "urn:replicon:location-list-column:location",
            "urn:replicon:location-list-column:full-path"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:location-list-filter:effectively-enabled"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "value": {
                    "bool": "true"
                }
            }
        }
    }

def get_enddate_payload(dag_run, config):
    if dag_run.conf['employeestatus'].lower() == 'active':
        if dag_run.conf['enddate']:
            return get_datetime_obj(dag_run.conf['enddate'])
    elif dag_run.conf['employeestatus'].lower() == 'terminated':
        return get_datetime_obj(dag_run.conf['enddate'])
    elif dag_run.conf['employeestatus'] in config.leave_status:
        return get_datetime_obj(dag_run.conf['firstdayofleave'])

    return null

def get_update_user_payload(dag_run, config):
    user_details = rail.result('get_user_details_for_update')[0]
    previous_status_value = rail.find_first_by_attr_and_get_attr(
        user_details['userDetails']['customFieldValues'], 'customField.displayText', 'Previous Status', 'text')
    user_startdate = str(user_details['userDetails']['employmentDateRange']['startDate']['month']) + '/' + str(
        user_details['userDetails']['employmentDateRange']['startDate']['day']) + '/' +str(
            user_details['userDetails']['employmentDateRange']['startDate']['year'])
    logger = []    
    payload = {
        "target": {
            "uri": user_details['userDetails']['uri']
        },
        "modifications": {
            "firstName": {
                "value": dag_run.conf['firstname']
            } if user_details['userDetails']['firstName'] != dag_run.conf['firstname'] else null,
            "lastName": {
                "value": dag_run.conf['lastname']
            } if user_details['userDetails']['lastName'] != dag_run.conf['lastname'] else null,
            "loginName": {
                "value": dag_run.conf['loginname']
            } if user_details['securityConfiguration']['loginName'] != dag_run.conf['loginname'] else null,
            "emailAddress": {
                "value": dag_run.conf['loginname']
            } if user_details['userDetails']['emailAddress'] != dag_run.conf['loginname'] else null,
            "employeeId": null,
            "employmentDateRange": {
                "value": {
                    "startDate": get_datetime_obj(dag_run.conf['startdate']) if previous_status_value == 'Terminated' else get_datetime_obj(user_startdate),
                    "endDate": get_enddate_payload(dag_run, config)
                }
            },
            "customFields": get_custom_field_payload(dag_run),
            "locationSchedule": get_group_value_to_update_payload(dag_run, logger,'location', 'location_uri'),
            "costCenterSchedule": get_group_value_to_update_payload(dag_run, logger,'costcenter', 'cost_center_uri'),
            "departmentGroupSchedule": get_group_value_to_update_payload(dag_run, logger,'departmentname', 'department_uri'),
            "employeeTypeGroupSchedule": get_group_value_to_update_payload(dag_run, logger,'employeetype', 'employee_type_uri')
        },
        "unitOfWorkId": str(uuid.uuid4())
    }
    rail.set_result(key='log', val=rail.smartjoin_by_delim(logger, ';'))
    return payload

def get_search_user_param(dag_run):
    return {
        'page': '1',
        'pagesize': '100',
        'columnUris': [
            'urn:replicon:user-list-column:user',
            "urn:replicon:user-list-column:enabled",
            "urn:replicon:user-list-column:login-name"
        ],
        'sort': [],
        'filterExpression': {
            'leftExpression': {
                'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
            },
            'operatorUri': 'urn:replicon:filter-operator:text-search',
            'rightExpression': {
                'value': {
                    'text': dag_run.conf['workermanager']
                }
            }
        }
    }

def get_workermanager_detail(response):
    if response['rows']:
        return {
            'uri': response['rows'][0]['cells'][0]['uri'],
            'enabled': response['rows'][0]['cells'][1]['textValue'],
            'loginname': response['rows'][0]['cells'][2]['textValue']
        }
    return None

def user_import_csv_data(item):
    return [
        item['Employee ID'], 
        item['Black Knight ID'],
        item['Perferred Name - First Name'],
        item['Perferred Name - Last Name'],
        item['Worker Type'],
        item['Employee Type'],
        item['Business Title'],
        item['Cost Center - ID'],
        item['Cost Center - Name'],
        item['Department Name'],
        item["Worker's Manager"],
        item['Location Hierarchy'],
        item['Location - Name'],
        item['Work State'],
        item['Work City'].title(),
        item['Scheduled Weekly Hours'],
        item['FTE %'],
        item['Continuous Service Date'],
        item['Termination Date'],
        item['Email - Primary Work'],
        item['Manager - Level 02'],
        item['Manager - Level 03'],
        item['Manager - Level 04'],
        item['Manager - Level 05'],
        item['Manager - Level 06'],
        item['Manager - Level 07'],
        item['Manager - Level 08'],
        item['Manager - Level 09'],
        item['Manager - Level 10'],
        item['Employee Status'],
        item['First Day of Leave'],
        item['Return Date from Leave']
    ]
