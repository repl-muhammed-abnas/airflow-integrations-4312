from datetime import datetime
from functools import lru_cache
import uuid
import hashlib
from cryptography.fernet import Fernet
from airflow.models import Variable
import rail

from lanter_delivery_systems.user_import.user_import_integration.config import PASSWORD_ENCRYPTION_VARIABLE

null = None

DATE_FORMAT = "%m/%d/%Y"

def get_task_state(task_id):
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()

def get_today_date():
    now = datetime.utcnow()
    return {
        'year': now.year,
        'month': now.month,
        'day': now.day
    }

@lru_cache(maxsize=32)
def get_key_from_airflow_var():
    return bytes(Variable.get(PASSWORD_ENCRYPTION_VARIABLE), "utf-8")

def get_create_md5_data(item):
    if not item:
        return []
    item['password'] = (Fernet(get_key_from_airflow_var()).encrypt((item['password'].encode('utf-8')))).decode()
    return {
        **item,
        **{
        'md5': hashlib.md5((str(item["loginname"])+","+str(item["firstname"])+","+str(item["lastname"])+","
                            + str(item["authtype"])+"," + str(item["enabled"]) + "," + str(item["employeetype"])+","
                            + str(item["employeeid"])+"," + str(item["department"]) +"," + str(item["startdate"])+"," + str(item["enddate"])+"," +
                            str(item["authid"])+"," + str(item["licenses"])+"," + str(item["supervisorusername"]) +
                            "," + str(item["locationname"])+","+ str(item["permisssionset"])+"," +str(item["timesheettemplate"]) +
                            "," + str(item["timesheetapprovalpath"])+","+ str(item["timezone"])+"," + str(item["currency"])+"," +
                            str(item["payrate"])+","+ str(item["punchentrypolicy"])+"," + str(item["payrulename"]) +
                            "," + str(item["district"])+","+ str(item["costcenter"])+","+ str(item["cid"])+","+
                            str(item["locationaddress"])+","+ str(item["locationcity"])
                            +","+ str(item["locationstate"])+","+ str(item["glstring"]) + ","+ str(item["accountingcode"])+","+
                            str(item["worktype"])+","+ str(item["accountingcodedescription"])
                            +","+ str(item["agency"])+","+ str(item["markup"])).encode('utf-8')).hexdigest()
        }
    }

MANDATORY_FIELDS = {
        "loginname":"Login Name",
        "firstname": "First Name",
        "lastname": "Last Name",
        "authtype": "Authentication Type",
        "enabled": "Last Name",
        "employeetype": "Employee Type",
        "employeeid": "Employee ID",
        "department": "Department",
        "startdate": "Start Date",
        "password": "Password",
        "licenses": "Licenses",
        "locationname": "Location",
}

def get_mandatory_fields_exception_message(item):
    missing_fields = []
    for payload_key, log_value in MANDATORY_FIELDS.items():
        if not item[payload_key]:
            missing_fields.append(f"{log_value} is not present in payload")
    return rail.smartjoin_by_delim(missing_fields, ";")

def get_add_department_payload(dag_run):
    return {
        "departmentGroup": {
            "parent": {
                "uri": rail.result("get_parent_department_details")[0]['uri']
            },
        },
        "modifications": {
            "name": dag_run.conf['department_name'],
            "isEnabled": "1"
        },
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_add_employeetype_payload(dag_run):
    return {
        "employeeTypeGroup": {
            "parent": {
                "uri": rail.result("get_parent_employee_type_details")[0]['uri']
            },
        } if rail.result('get_parent_employee_type_details') else null,
        "modifications": {
            "name": dag_run.conf['employeetype_name'],
            "isEnabled": "1"
        },
        "unitOfWorkId": str(uuid.uuid4())
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

def get_data_for_supervisor_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:login-name",
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
                    "text": dag_run.conf['supervisorusername'],
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

def get_replicon_date(date_str):
    if not date_str:
        return None

    try:
        date = datetime.strptime(date_str, DATE_FORMAT)
        return {
            'year': date.year,
            'month': date.month,
            'day': date.day
        }
    except:  # pylint: disable=bare-except
        return None

def validate_supervisor_changed():
    if not rail.result('get_effective_supervisor_of_user'):
        return True
    if rail.result('search_supervisor_in_replicon') and rail.result('get_effective_supervisor_of_user') and \
        rail.result('search_supervisor_in_replicon')[0]['loginname'] == rail.result('get_effective_supervisor_of_user')['supervisor']['user']['loginName']:
        return False
    return True

def get_supervisor_status(dag_run):
    if get_task_state('log_supervisor_not_present') == 'success' \
        or get_task_state('log_supervisor_disabled_in_replicon') == 'success' or dag_run.conf['exception_logs']:
        return 'Exception'
    return 'Success'

def get_supervisor_message(action, dag_run):
    # pylint: disable=too-many-return-statements
    exception_log = dag_run.conf['exception_logs'] if dag_run.conf['exception_logs'] else []
    if get_task_state('log_supervisor_not_present') == 'success':
        return ("User Partially Added" if action == 'Add' else "User Partially Updated") + \
            ',Supervisor not present in replicon;'+ rail.smartjoin_by_delim(exception_log, ";")
    if get_task_state('log_supervisor_disabled_in_replicon') == 'success':
        return ("User Partially Added" if action == 'Add' else "User Partially Updated") + ',Supervisor is disabled in replicon;'
    return f"""User {('Added' if action=='add' else 'Updated')
        if not exception_log else ('Partially Added,'if action=='add' else 'Partially Updated,') + rail.smartjoin_by_delim(exception_log, ";")}"""

def get_user_data_payload(dag_run):
    return{
    "users": [
        {
        "uri": null,
        "loginName": dag_run.conf['loginname'],
        "employeeId": null,
        "parameterCorrelationId": null
        }
    ]
}

def test_valid_fields(dag_run):
    if not get_replicon_date(dag_run.conf['startdate']):
        return False
    if dag_run.conf['enddate']:
        if not  get_replicon_date(dag_run.conf['enddate']):
            return False
    return True

def get_invalid_fields_message(dag_run):
    log=[]
    if not get_replicon_date(dag_run.conf['startdate']):
        log.append('Invalid Date format for Start Date')
    if dag_run.conf['enddate']:
        if not get_replicon_date(dag_run.conf['enddate']):
            log.append('Invalid Date format for End Date')
    return rail.smartjoin_by_delim(log,";")

def get_process_users_conf(item):
    get_user_udfs = rail.result('get_user_udfs')

    def get_all_productlicenseuri(item):
        licenses = (item['licenses']).split('|')
        replicon_products = rail.result('get_all_products_available_for_user_assignment')
        return list(map(lambda data: {
            'name': data,
            'uri' : rail.find_first_by_attr_and_get_attr(replicon_products,'displayText',data.lower(),'uri')
            }, licenses))

    def get_all_permissionseturis(item):
        permission_sets = (item['permisssionset']).split('|')
        replicon_permission_set = rail.result('get_all_permission_set')
        return list(map(lambda data: {
            'name': data,
            'uri': rail.find_first_by_attr_and_get_attr(replicon_permission_set,'displayText',data,'uri')
            }, permission_sets))
    return {
        **item,
        **{
            'districtdefinitionuri': get_user_udfs['districtdefinitionuri'],
            'costcenterdefinitionuri': get_user_udfs['costcenterdefinitionuri'],
            'ciddefinitionuri': get_user_udfs['ciddefinitionuri'],
            'locationaddressdefinitionuri': get_user_udfs['locationaddressdefinitionuri'],
            'locationcitydefinitionuri': get_user_udfs['locationcitydefinitionuri'],
            'locationstatedefinitionuri': get_user_udfs['locationstatedefinitionuri'],
            'accountingcodedefinitionuri': get_user_udfs['accountingcodedefinitionuri'],
            'accountingcodedescriptionfinitionuri': get_user_udfs['accountingcodedescriptionfinitionuri'],
            'glstringdefinitionuri':get_user_udfs['glstringdefinitionuri'],
            'worktypedefinitionuri':get_user_udfs['worktypedefinitionuri'],
            'worktypedropdownuri': rail.find_first_by_attr_and_get_attr(rail.result("get_work_type_udf_dropdown_values"),'name', item['worktype'],'uri')
                if item['worktype'] else null,
            'agencydefinitionuri': get_user_udfs['agencydefinitionuri'],
            'markupdefinitionuri': get_user_udfs['markupdefinitionuri'],
            'departmenturi':rail.find_first_by_attr_and_get_attr(rail.result('get_updated_departments'), 'full_path', item['department'], 'uri'),
            'employeetypeuri':rail.find_first_by_attr_and_get_attr(rail.result('get_updated_employee_types'), 'full_path', item['employeetype'], 'uri'),
            'locationuri':rail.find_first_by_attr_and_get_attr(rail.result('get_updated_locations'), 'name', item['locationname'], 'uri'),
            'productlicenceuri': get_all_productlicenseuri(item),
            'permissionsetdetails': get_all_permissionseturis(item) if item['permisssionset'] else null,
            'timesheettemplateuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_policy_sets"),'displayText',item['timesheettemplate'],"uri")
                if item['timesheettemplate'] else null,
            'timesheetapprovalpathuri': rail.find_first_by_attr_and_get_attr(rail.result("get_timesheet_approval_paths"),'displayText',
                item['timesheetapprovalpath'],"uri") if item['timesheetapprovalpath'] else null,
            'timezoneuri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_timezones'), 'displayText', item['timezone'], 'uri')
                if item['timezone'] else null,
            'currencyuri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_currencies'), 'displayText', item['currency'], 'uri')
                if item['currency'] else null,
            'punchentrypolicyuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_policy_sets"),'displayText',item['punchentrypolicy'],"uri")
                if item['punchentrypolicy'] else null,
            'payrulescripturi': rail.find_first_by_attr_and_get_attr(rail.result("get_all_payrule_scripts"),'displayText',item['payrulename'],"uri")
                if item['payrulename'] else null,
            'supervisor_log' : rail.result('create_supervisor_log'),
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
            'todaysdate': (datetime.now()).strftime(DATE_FORMAT)
        }
    }

def add_permission_sets(log, dag_run):
    all_permission_sets = dag_run.conf['permissionsetdetails']

    if not all_permission_sets:
        return null

    permission_set_uri_not_available = list(filter(lambda x:x['uri']== null, all_permission_sets))
    if len(permission_set_uri_not_available) > 0:
        log.append(f"""Permission set - {rail.smartjoin_by_delim([item['name'] for item in permission_set_uri_not_available], ";")
            } not available in Replicon""")

    permission_set_uri_available = list(filter(lambda x:x['uri']!= null, all_permission_sets))
    if len(permission_set_uri_available) > 0:
        return list(map(lambda item:{
             "uri": item['uri'],
             "name": null
        }, permission_set_uri_available))

    return null

def get_policy_sets(log, dag_run):
    policy_set = []
    if dag_run.conf['timesheettemplate'] and not dag_run.conf['timesheettemplateuri']:
        log.append(f"Timesheet Template - {dag_run.conf['timesheettemplate']} is not available in Replicon")
    if dag_run.conf['punchentrypolicy'] and not dag_run.conf['punchentrypolicyuri']:
        log.append(f"Punch Entry Policy - {dag_run.conf['punchentrypolicy']} is not available in Replicon")

    if not dag_run.conf['timesheettemplateuri'] and not dag_run.conf['punchentrypolicyuri']:
        return null

    if dag_run.conf['timesheettemplateuri']:
        policy_set.append({
                    "uri": dag_run.conf['timesheettemplateuri'],
                    "name": null
                })
    if dag_run.conf['punchentrypolicyuri']:
        policy_set.append({
                    "uri": dag_run.conf['punchentrypolicyuri'],
                    "name": null
                })
    return policy_set

def get_timesheet_approvalpath(log, dag_run):
    if not dag_run.conf['timesheetapprovalpath']:
        return null
    if dag_run.conf['timesheetapprovalpath'] and not dag_run.conf['timesheetapprovalpathuri']:
        log.append(f"Timesheet Approval Path - {dag_run.conf['timesheetapprovalpath']} is not available in Replicon")
        return null
    return {
            "uri": dag_run.conf['timesheetapprovalpathuri'],
            "name": null
        }

def get_timezone(log, dag_run):
    if not dag_run.conf['timezone']:
        return null
    if dag_run.conf['timezone'] and not dag_run.conf['timezoneuri']:
        log.append(f"Time Zone - {dag_run.conf['timezone']} is not available in Replicon")
        return null
    return {
            "uri": dag_run.conf['timezoneuri'],
            "IANAName": null
        }

def get_payrule(log, dag_run):
    if not dag_run.conf['payrulename']:
        return null
    if dag_run.conf['payrulename'] and not dag_run.conf['payrulescripturi']:
        log.append(f"Payrule - {dag_run.conf['payrulename']} is not available in Replicon")
        return null
    return [
            {
                "payRuleScript": {
                    "uri": dag_run.conf['payrulescripturi'],
                    "name": null
                },
                "effectiveDate": null
            }
        ]

def get_payrate(log, dag_run):
    if dag_run.conf['currency'] and not dag_run.conf['currencyuri']:
        log.append(f"Pay Rate Currency Name - {dag_run.conf['currency']} is not available in Replicon")

    if not dag_run.conf['currencyuri'] or not dag_run.conf['payrate']:
        return null

    return {
            "initialHourlyRate": {
                "amount": float(dag_run.conf['payrate']),
                "currency": {
                "uri": dag_run.conf['currencyuri'],
                "name": null,
                "symbol": null
                }
            },
            "scheduleEntries": []
        }

def get_udfs(userstatus, dag_run):
    # pylint: disable=too-many-branches
    # pylint: disable=too-many-statements
    udfs = []
    def add_udf_field_values(definitionuri, dropdownuri = null, textvalue = null , number = null):
        udfs.append({
        "customField": {
          "uri": definitionuri,
          "name": null,
          "groupUri": null
        },
        "text": textvalue,
        "date": null,
        "dropDownOption": {
          "uri": dropdownuri,
          "name": null
        } if dropdownuri != null else null,
        "number": number
      })

    if userstatus =='adduser':
        if dag_run.conf['worktype']:
            add_udf_field_values(definitionuri = dag_run.conf['worktypedefinitionuri'], dropdownuri= dag_run.conf['worktypedropdownuri'])
        if dag_run.conf['district']:
            add_udf_field_values(definitionuri = dag_run.conf['districtdefinitionuri'], textvalue = dag_run.conf['district'])
        if dag_run.conf['costcenter']:
            add_udf_field_values(definitionuri = dag_run.conf['costcenterdefinitionuri'], textvalue = dag_run.conf['costcenter'])
        if dag_run.conf['cid']:
            add_udf_field_values(definitionuri = dag_run.conf['ciddefinitionuri'], textvalue = dag_run.conf['cid'])
        if dag_run.conf['locationaddress']:
            add_udf_field_values(definitionuri = dag_run.conf['locationaddressdefinitionuri'], textvalue = dag_run.conf['locationaddress'])
        if dag_run.conf['locationcity']:
            add_udf_field_values(definitionuri = dag_run.conf['locationcitydefinitionuri'], textvalue = dag_run.conf['locationcity'])
        if dag_run.conf['locationstate']:
            add_udf_field_values(definitionuri = dag_run.conf['locationstatedefinitionuri'], textvalue = dag_run.conf['locationstate'])
        if dag_run.conf['accountingcode']:
            add_udf_field_values(definitionuri = dag_run.conf['accountingcodedefinitionuri'], textvalue = dag_run.conf['accountingcode'])
        if dag_run.conf['accountingcodedescription']:
            add_udf_field_values(definitionuri = dag_run.conf['accountingcodedescriptionfinitionuri'], textvalue = dag_run.conf['accountingcodedescription'])
        if dag_run.conf['agency']:
            add_udf_field_values(definitionuri = dag_run.conf['agencydefinitionuri'], textvalue = dag_run.conf['agency'])
        if dag_run.conf['markup']:
            add_udf_field_values(definitionuri = dag_run.conf['markupdefinitionuri'], number = dag_run.conf['markup'])
        if dag_run.conf['glstring']:
            add_udf_field_values(definitionuri = dag_run.conf['glstringdefinitionuri'], textvalue = dag_run.conf['glstring'])

    if userstatus == 'updateuser':
        current_worktype = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'work_type', 'text')
        current_district = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'District', 'text')
        current_costcenter = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Cost Center', 'text')
        current_cid = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'CID', 'text')
        current_locationadress = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Location Address Line 1', 'text')
        current_locationcity = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Location City', 'text')
        current_locationstate = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Location State/Territory', 'text')
        current_accountingcode = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'accounting_code:gl_string', 'text')
        current_accountingcodedescription = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'accounting_code:gl_description', 'text')
        current_agency = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Agency', 'text')
        current_markup = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'markup %', 'text')
        current_glstring = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'GL String', 'text')

        if dag_run.conf['worktype'] and dag_run.conf['worktype'] != current_worktype:
            add_udf_field_values(definitionuri = dag_run.conf['worktypedefinitionuri'], dropdownuri= dag_run.conf['worktypedropdownuri'])
        if dag_run.conf['district'] and dag_run.conf['district'] != current_district:
            add_udf_field_values(definitionuri = dag_run.conf['districtdefinitionuri'], textvalue = dag_run.conf['district'])
        if dag_run.conf['costcenter'] and dag_run.conf['district'] != current_costcenter:
            add_udf_field_values(definitionuri = dag_run.conf['costcenterdefinitionuri'], textvalue = dag_run.conf['costcenter'])
        if dag_run.conf['cid'] and dag_run.conf['cid'] != current_cid:
            add_udf_field_values(definitionuri = dag_run.conf['ciddefinitionuri'], textvalue = dag_run.conf['cid'])
        if dag_run.conf['locationaddress'] and dag_run.conf['locationaddress'] != current_locationadress:
            add_udf_field_values(definitionuri = dag_run.conf['locationaddressdefinitionuri'], textvalue = dag_run.conf['locationaddress'])
        if dag_run.conf['locationcity'] and dag_run.conf['locationcity'] != current_locationcity:
            add_udf_field_values(definitionuri = dag_run.conf['locationcitydefinitionuri'], textvalue = dag_run.conf['locationcity'])
        if dag_run.conf['locationstate'] and dag_run.conf['locationstate'] != current_locationstate:
            add_udf_field_values(definitionuri = dag_run.conf['locationstatedefinitionuri'], textvalue = dag_run.conf['locationstate'])
        if dag_run.conf['accountingcode'] and dag_run.conf['accountingcode'] != current_accountingcode:
            add_udf_field_values(definitionuri = dag_run.conf['accountingcodedefinitionuri'], textvalue = dag_run.conf['accountingcode'])
        if dag_run.conf['accountingcodedescription'] and dag_run.conf['accountingcodedescription'] != current_accountingcodedescription:
            add_udf_field_values(definitionuri = dag_run.conf['accountingcodedescriptionfinitionuri'], textvalue = dag_run.conf['accountingcodedescription'])
        if dag_run.conf['agency'] and dag_run.conf['agency'] != current_agency:
            add_udf_field_values(definitionuri = dag_run.conf['agencydefinitionuri'], textvalue = dag_run.conf['agency'])
        if dag_run.conf['markup'] and dag_run.conf['markup'] != current_markup:
            add_udf_field_values(definitionuri = dag_run.conf['markupdefinitionuri'], number = dag_run.conf['markup'])
        if dag_run.conf['glstring'] and dag_run.conf['glstring'] != current_glstring:
            add_udf_field_values(definitionuri = dag_run.conf['glstringdefinitionuri'], textvalue = dag_run.conf['glstring'])

    return udfs

def get_put_user_payload(dag_run):
    log=[]
    put_user_payload = {
        "user": {
            "target": {
                "uri": null,
                "loginName": dag_run.conf['loginname'],
            },
            "firstname": dag_run.conf['firstname'],
            "lastname": dag_run.conf['lastname'],
            "employeeId": dag_run.conf['employeeid'],
            "employmentDateRange": {
                "startDate": get_replicon_date(dag_run.conf['startdate']),
                "endDate": get_replicon_date(dag_run.conf['enddate']) if dag_run.conf['enddate'] else null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                   "urn:replicon:user-authentication-type:replicon"
                ],
                "isLoginEnabled": "true" if dag_run.conf['enabled'] == "Yes" else "false",
                "loginName": dag_run.conf['loginname'],
                "password": Fernet(get_key_from_airflow_var()).decrypt(bytes(dag_run.conf['password'],"utf-8")).decode()
            },
            "permissionSets": add_permission_sets(log, dag_run),
            "policySets": get_policy_sets(log, dag_run),
            "payrollRateSchedule": get_payrate(log, dag_run),
            "timesheetApprovalPath": get_timesheet_approvalpath(log,dag_run),
            "customFieldValues": get_udfs('adduser', dag_run),
            "assignedActivities": [],
            "timeZone": get_timezone(log, dag_run),
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
            "policyDataAccessScopes": [],
            "policyDataAccessScopes2": [],
            "payRuleScriptSchedule": get_payrule(log, dag_run),
            "displayNameParameter": null,
            "decimalSeparatorUri": null,
            "numberGroupSeparatorUri": null,
            "extensionFieldValues": []
        }
    }

    rail.set_result(key="exception_logs",val= log)

    return put_user_payload

def get_remove_timeoff_payload():
    return {
        "userUri": rail.result('add_new_user')['uri'],
        "timeOffTypeUris": []
    }

def get_add_user_message():
    # pylint: disable=too-many-return-statements
    if get_task_state('log_supervisor_not_present') == 'success':
        return ""
    exception_logs = rail.result('add_new_user','exception_logs')
    if not exception_logs:
        if get_task_state('log_supervisor_disabled_in_replicon') == 'success':
            return 'User Partially Added, Supervisor is disabled in replicon'
        return "User Added"
    if get_task_state('log_supervisor_disabled_in_replicon') == 'success':
        return 'User Partially Added, Supervisor is disabled in replicon;'+ rail.smartjoin_by_delim(exception_logs, ";")
    return "User Partially Added,"+ rail.smartjoin_by_delim(exception_logs, ";")


def get_add_user_severity():
    if get_task_state('log_supervisor_not_present') == 'success'\
        or get_task_state('log_supervisor_disabled_in_replicon') == 'success' or rail.result('add_new_user','exception_logs'):
        return 'Exception'
    return 'Success'

def get_group_value(data, key):
    if not data:
        return {}
    return data[0].get(key, {}).get(key, {})

def get_effective_user_groupmembership_filter(response):
    group_list = ['location', 'department', 'employeeType']
    for group in group_list:
        rail.set_result(key=group.lower(), val=get_group_value(
            response.get(f'{group}s'), group))

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
                    "effectiveDate": get_replicon_date(dag_run.conf['todaysdate'])
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
                    "effectiveDate": get_replicon_date(dag_run.conf['todaysdate'])
                }
            ],
            "endDate": null
        }
    } if departmenturi != currentdepartmenturi else null

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
                    "effectiveDate": get_replicon_date(dag_run.conf['todaysdate'])
                }
            ],
            "endDate": null
        }
    } if employeetypeuri != currentemployeetypeuri else null


def update_permission_set(log, dag_run):
    permission_set_uris = []
    all_permission_sets = dag_run.conf['permissionsetdetails']

    if not all_permission_sets:
        return null

    permission_set_uri_not_available = list(filter(lambda x:x['uri']== null, all_permission_sets))
    if len(permission_set_uri_not_available) > 0:
        log.append(f"""Permission set - {rail.smartjoin_by_delim([item['name'] for item in permission_set_uri_not_available], ";")
            } not available in Replicon""")

    permission_set_uri_available = list(filter(lambda x:x['uri']!= null, all_permission_sets))
    if len(permission_set_uri_available) > 0:
        for item in permission_set_uri_available:
            if not rail.find_first_by_attr_and_get_attr(rail.result('get_user_info')['permissionSets'],
            'displayText', item['name'], 'displayText'):
                permission_set_uris.append(item['uri'])
    return {
            "permissionSetUrisToAssign": permission_set_uris,
            "policyUrisToRemovePermissionSet": []
        } if permission_set_uris else null

def update_payrule_script(log, dag_run):
    if dag_run.conf['payrulename'] and not dag_run.conf['payrulescripturi']:
        log.append(f"Payrule - {dag_run.conf['payrulename']} is not available in Replicon")
        return null

    current_payrulescript = rail.result("get_user_info")['payRuleScriptSchedule']
    if not current_payrulescript:
        return {
            "scheduleEntries": [
                {
                    "payRuleScript": {
                        "uri": dag_run.conf['payrulescripturi'],
                        "name": null
                    },
                    "effectiveDate": get_replicon_date(dag_run.conf['todaysdate'])
                }
            ]
        }

    if dag_run.conf['payrulescripturi'] != current_payrulescript[-1]['payRuleScript']['uri']:
        return {
            "scheduleEntries": [
                {
                    "payRuleScript": {
                        "uri": dag_run.conf['payrulescripturi'],
                        "name": null
                    },
                    "effectiveDate": get_replicon_date(dag_run.conf['todaysdate'])
                }
            ]
        }

    return null

def update_payrate(log, dag_run):
    if dag_run.conf['currency'] and not dag_run.conf['currencyuri']:
        log.append(f"Pay Rate Currency Name - {dag_run.conf['currency']} is not available in Replicon")
    if not dag_run.conf['currencyuri'] or not dag_run.conf['payrate']:
        return null
    return {
            "initialHourlyRate": {
                "amount": float(dag_run.conf['payrate']),
                "currency": {
                "uri": dag_run.conf['currencyuri'],
                "name": null,
                "symbol": null
                }
            },
            "scheduleEntries": []
        }

def update_user_details(dag_run):
    user_details = rail.result("get_user_info")['userDetails']
    return {
      "firstName": dag_run.conf['firstname'] if user_details['firstName'] != dag_run.conf['firstname'] else null,
      "lastName": dag_run.conf['lastname'] if user_details['lastName'] != dag_run.conf['lastname'] else null,
      "language": null,
      "employmentDateRange": null,
      "employmentStartDate": {
        "date": get_replicon_date(dag_run.conf['startdate'])
      } if user_details['employmentDateRange']['startDate'] != get_replicon_date(dag_run.conf['startdate']) else null,
       "employmentEndDate": {
         "date": get_replicon_date(dag_run.conf['enddate']) if bool(get_replicon_date(dag_run.conf['enddate'])) else null
       },
    }

def update_policy_set(log, dag_run):
    assigned_timesheet_template = rail.result("get_user_info")['timesheetTemplate']
    policy_set = []

    if dag_run.conf['timesheettemplate'] and not dag_run.conf['timesheettemplateuri']:
        log.append(f"Timesheet Template - {dag_run.conf['timesheettemplate']} is not available in Replicon")
    if dag_run.conf['punchentrypolicy'] and not dag_run.conf['punchentrypolicyuri']:
        log.append(f"Punch Entry Policy - {dag_run.conf['punchentrypolicy']} is not available in Replicon")

    if dag_run.conf['timesheettemplateuri']:
        if not assigned_timesheet_template or (assigned_timesheet_template and (
            dag_run.conf['timesheettemplate'] != assigned_timesheet_template['displayText'])):
            policy_set.append(dag_run.conf['timesheettemplateuri'])

    if dag_run.conf['punchentrypolicyuri']:
        policy_set.append(dag_run.conf['punchentrypolicyuri'])
    return {
            "policySetUrisToAssign": policy_set,
            "policyUrisToRemovePolicySet": []
        } if policy_set else null

def update_timesheet_approvalpath(log, dag_run):
    if dag_run.conf['timesheetapprovalpath'] and not dag_run.conf['timesheetapprovalpathuri']:
        log.append(f"Timesheet Approval Path - {dag_run.conf['timesheetapprovalpath']} is not available in Replicon")
    current_timesheet_approvalpath = rail.result('get_user_info')['timesheetApprovalPath']

    if dag_run.conf['timesheetapprovalpathuri']:
        if not current_timesheet_approvalpath or (current_timesheet_approvalpath and (
            dag_run.conf['timesheetapprovalpath'] != current_timesheet_approvalpath['displayText'])):
            return {
                    "uri": dag_run.conf['timesheetapprovalpathuri'],
                    "name": null
                }
    return null

def get_payrollrate_modifications(log, dag_run):
    if dag_run.conf['currency'] and not dag_run.conf['currencyuri']:
        log.append(f"Pay Rate Currency Name - {dag_run.conf['currency']} is not available in Replicon")

    assigned_payroll_rate = rail.result("get_user_info")['payrollRateSchedule']

    if not assigned_payroll_rate and dag_run.conf['currencyuri'] and dag_run.conf['payrate']:
        return {
            "scheduleEntriesToAdd": [
                {
                "hourlyRate": {
                    "amount": float(dag_run.conf['payrate']),
                    "currency": {
                    "uri": dag_run.conf['currencyuri'],
                    "name": null,
                    "symbol": null
                    }
                },
                "effectiveDate": get_replicon_date(dag_run.conf['todaysdate'])
                }
            ],
            "scheduleEntriesToPut": []
            }

    current_amount = float(assigned_payroll_rate[-1]['hourlyRate']['amount']) if assigned_payroll_rate else null
    current_currency_uri = assigned_payroll_rate[-1]['hourlyRate']['currency']['uri'] if assigned_payroll_rate else null

    if dag_run.conf['currencyuri'] and dag_run.conf['payrate'] and (
        current_amount!= float(dag_run.conf['payrate']) or current_currency_uri != dag_run.conf['currencyuri']):
        return {
            "scheduleEntriesToAdd": [
                {
                "hourlyRate": {
                    "amount": float(dag_run.conf['payrate']),
                    "currency": {
                    "uri": dag_run.conf['currencyuri'],
                    "name": null,
                    "symbol": null
                    }
                },
                "effectiveDate": get_replicon_date(dag_run.conf['todaysdate'])
                }
            ],
            "scheduleEntriesToPut": []
            }
    return null

def apply_user_modifications_payload(dag_run):
    log = []
    update_user_payload =  {
        "user": {
            "uri": dag_run.conf['useruri']
        },
        "modifications": {
            "timezoneToApply": {
                "userTimeZoneModificationOptionUri": "urn:replicon:user-time-zone-modication-option:use-specified-time-zone",
                "timezone": get_timezone(log, dag_run)
            },
            "locationScheduleToApply": update_location_grp(dag_run.conf['locationuri'],
                rail.result('get_effective_user_groupmembership','location').get('uri', ''), dag_run),
           "departmentGroupScheduleToApply": update_department_grp(dag_run.conf['departmenturi'],
                rail.result('get_effective_user_groupmembership', 'department').get('uri', ''), dag_run),
            "employeeTypeGroupScheduleToApply": update_employeetype_grp(dag_run.conf['employeetypeuri'],
                rail.result('get_effective_user_groupmembership', 'employeetype').get('uri', ''), dag_run),
            "permissionSetsToApply": update_permission_set(log, dag_run),
            "policySetsToApply": update_policy_set(log, dag_run),
            "timesheetApprovalPathToApply": update_timesheet_approvalpath(log, dag_run),
            "customFieldValuesToApply": get_udfs('updateuser', dag_run),
            "userDetailsToApply": update_user_details(dag_run),
            "payRulesScheduleModifications": update_payrule_script(log, dag_run),
            "payrollRatesModifications": get_payrollrate_modifications(log, dag_run),
            },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

    rail.set_result(key="exception_logs",val= log)

    return update_user_payload

def get_update_user_message():
    # pylint: disable=too-many-return-statements
    if get_task_state('log_supervisor_not_present') == 'success':
        return ""
    exception_logs = rail.result('apply_user_modifications', 'exception_logs')
    if not exception_logs:
        if get_task_state('log_supervisor_disabled_in_replicon') == 'success':
            return 'User Partially Updated, Supervisor is disabled in replicon'
        return "User Updated"
    if get_task_state('log_supervisor_disabled_in_replicon') == 'success':
        return 'User Partially Updated, Supervisor is disabled in replicon'+ rail.smartjoin_by_delim(exception_logs, ";")
    return "User Partially Updated,"+ rail.smartjoin_by_delim(exception_logs, ";")

def get_update_user_severity():
    if get_task_state('log_supervisor_not_present') == 'success'\
        or get_task_state('log_supervisor_disabled_in_replicon') == 'success' or rail.result('apply_user_modifications', 'exception_logs'):
        return 'Exception'
    return 'Success'

def get_product_license_payload(dag_run):
    license_uris = list(map(lambda item: item['uri'], dag_run.conf['productlicenceuri']))
    return{
        "userUri": rail.result('add_new_user')['uri'],
        "productUris": license_uris
    }
