# pylint: disable=unused-variable too-many-branches too-many-statements
from hashlib import md5
import rail
from rail.lib.ecid import get_dagrun_ecid
from assuranceagency.user_import_hortongroup.utils.python_callable import get_today
from assuranceagency.user_import_hortongroup.mappers.assuranceagency_timezone_mapper import timezone_mapper
from assuranceagency.user_import_hortongroup.mappers.assuranceagency_timeofftype_mapper import timeoff_type_mapper
from assuranceagency.user_import_hortongroup.mappers.assuranceagency_employeetype_mapper import employee_type_mapper
from assuranceagency.user_import_hortongroup.mappers.assuranceagency_workweek_mapper import workweek_mapper

def get_today_date_format():
    return {
        "year": get_today().split('/')[2],
        "month": get_today().split('/')[0],
        "day": get_today().split('/')[1]
    }
def user_import_csv_data(item):
    return [
        item['Login Name'],
        item['First Name'],
        item['Last Name'],
        item['Employee Type'],
        item['Department'],
        item['Location'],
        item['Authentication Type'],
        item['Enabled'],
        item['Employee ID'],
        item['Start Date'],
        item['End Date'],
        item['Email Address'],
        item['Initial Supervisor LoginName'],
        item['Permission Sets'],
        item['Timesheet Template'],
        item['Timesheet Period Type'],
        item['Timesheet Approval Path'],
        item['Time Zone'],
        item['Work Week'],
        item['Holiday Calendar'],
        item['Initial Schedule Name '],
        item['TimeOff Template'],
        item['TimeOff Approval Path'],
        item['Initial Payrule Name'],
        item['Work day ID'],
        item['Position'],
        item['Worker Category'],
        item['Manager'],
        item['Business Unit'],

        md5(",".join([item['Login Name'],item['First Name'],item['Last Name'],item['Employee Type'],item['Department'],
                     item['Location'],item['Authentication Type'],item['Enabled'],item['Employee ID'],item['Start Date'],
                     item['End Date'],item['Email Address'],item['Initial Supervisor LoginName'],item['Permission Sets'],
                     item['Timesheet Template'],item['Timesheet Period Type'],item['Timesheet Approval Path'],item['Time Zone'],
                     item['Work Week'],item['Holiday Calendar'],item['Initial Schedule Name '],item['TimeOff Template'],
                     item['TimeOff Approval Path'],item['Initial Payrule Name'],item['Work day ID'],item['Position'],
                     item['Worker Category'],item['Manager'],item['Business Unit']]).encode()).hexdigest()
    ]



def get_userlist_report_params():

    return {
        "reportParameters": [{
            "filterValues": [],
            "outputFormatUri": "urn:replicon:report-output-format-option:csv",
            "reportUri": rail.result('get_report_uri')['userlist_report_uri']
        }
        ]
    }

def get_enabled_timesheet_period():
    return {
            "page": "1",
            "pagesize": "10000000",
            "columnUris": [
                "urn:replicon:timesheet-period-list-column:timesheet-period",
                "urn:replicon:timesheet-period-list-column:enabled"
            ],
            "sort": [],
            "filterExpression": {
                "leftExpression": {
                "filterDefinitionUri": "urn:replicon:timesheet-period-list-filter:enabled"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                "value": {
                    "bool": "true"
                }
                }
            }
        }

def get_enabled_locations():
    return {
            "page": "1",
            "pagesize": "10000",
            "columnUris": [
                "urn:replicon:location-list-column:location",
                "urn:replicon:location-list-column:code"
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

def process_user_to_disable_with_enddate(item):
    user_data = rail.load_all_records(rail.result('query_replicon_userdata'))
    return {
        "parentjobid": get_dagrun_ecid(rail.get_current_context()['dag_run']),
        "userloginname": item['loginname'],
        "useruri": rail.find_first_by_attr_and_get_attr(user_data, 'loginname', item['loginname'], 'useruri', ''),
        "startdate": item['startdate'],
        "username" : str(item['firstname']) + " " + str(item['lastname']),
        "emplid" : item['employeeid'],
        "enddate": item['enddate'],
        "logger" : rail.result('logger_list')
    }

def process_user_to_add(item):
    departmenturi = rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_enabled_dept_list'),
          'displayText', item['department'], 'uri', '')

    businessunit = item['businessunit']
    location_data = rail.result('get_all_enabled_location')
    locationuri = ''
    locationname = ''
    for i, row in enumerate(location_data['rows']):
        if row['cells'][1]['textValue'] == item['location']:
            locationuri = row['cells'][0]['uri']
            locationname = row['cells'][0]['textValue']

    supervisor_data = rail.load_all_records(rail.result('query_replicon_userdata'))
    initial_supervisor_uri = rail.find_first_by_attr_and_get_attr(
        supervisor_data, 'loginname', item['initialsupervisorloginname'], 'useruri', '')

    enduseruri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_permission_set'),
          'name', item['permissionsets'], 'uri', '')

    supervisorpermissionuri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_permission_set'),
          'displayText', 'Manager', 'uri', '')

    timesheettemplateuri = ''
    if item['timesheettemplate']:
        timesheettemplateuri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_policy_sets'),
          'name', item["timesheettemplate"], 'uri', '')
    if timesheettemplateuri == '':
        timesheettemplateuri = rail.find_first_by_attr_and_get_attr(
            rail.result('get_all_policy_sets'),
            'name', 'Standard Timesheet', 'uri', '')

    timesheetperiod_data = rail.result('get_all_enabled_timesheet_period')
    timesheetperioduri = ''
    for j, row in enumerate(timesheetperiod_data['rows']):
        if row['cells'][0]['textValue'] == item['timesheetperiodtype']:
            timesheetperioduri = row['cells'][0]['uri']
    if timesheetperioduri == '':
        for i, row in enumerate(timesheetperiod_data['rows']):
            if row['cells'][0]['textValue'] == 'Weekly starting on Sunday':
                timesheetperioduri = row['cells'][0]['uri']

    timesheetapprovalpathuri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_timesheet_approval'), 'displayText', item['timesheetapprovalpath'], 'uri', '')

    if item['location']:
        timezoneuri = rail.find_first_by_attr_and_get_attr(timezone_mapper, 'identifier', item["location"], 'uri', '')
        if not timezoneuri:
            timezoneuri = rail.find_first_by_attr_and_get_attr(timezone_mapper, 'value', item["timezone"], 'uri', '')
    else:
        timezoneuri = rail.find_first_by_attr_and_get_attr(timezone_mapper, 'value', item["timezone"], 'uri', '')

    employeetypename = None
    for k,emp in enumerate(employee_type_mapper):
        if emp['employeetype'] == item["employeetype"] and \
        emp['worker_category'] == item["workercategory"] and \
        emp['manager'] == item["manager"] :
            employeetypename = emp['value']
    employeetypeuri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_employee_type'), 'displayText', employeetypename, 'uri', '')

    workweek_start_day = 'sunday'
    if item["workweek"]:
        workweek_start_day = item["workweek"].split(" ")[0].lower()
    workweekuri = rail.find_first_by_attr_and_get_attr(workweek_mapper, 'value', workweek_start_day, 'uri', '')

    holidaycalendaruri = ''
    if item['holidaycalendar']:
        holidaycalendaruri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_holiday_calendars'),
          'displayText', item["holidaycalendar"], 'uri', '')
    if holidaycalendaruri == '':
        holidaycalendaruri = rail.find_first_by_attr_and_get_attr(
            rail.result('get_all_holiday_calendars'),
            'displayText', 'Assurance Agency Holiday', 'uri', '')

    officescheduleuri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_office_schedules'), 'displayText', item['initialschedulename'], 'uri', '')

    defaultofficescheduleuri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_office_schedules'), 'displayText', '40', 'uri', '')

    timeofftemplateuri = ''
    if item['timeofftemplate']:
        timeofftemplateuri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_policy_sets'),
          'name', item["timeofftemplate"], 'uri', '')
    if timeofftemplateuri == '':
        timeofftemplateuri = rail.find_first_by_attr_and_get_attr(
            rail.result('get_all_policy_sets'),
            'name', 'Time Off', 'uri', '')

    timeoffapprovalpathuri = ''
    if item['timeoffapprovalpath']:
        timeoffapprovalpathuri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_timeoff_approval'),
          'displayText', item["timeoffapprovalpath"], 'uri', '')
    if timeoffapprovalpathuri == '':
        timeoffapprovalpathuri = rail.find_first_by_attr_and_get_attr(
            rail.result('get_all_timeoff_approval'),
            'displayText', 'Automatic Approval', 'uri', '')

    payruleuri = ''
    if item['initialpayrulename']:
        payruleuri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_payrule_scripts'),
          'displayText', item["initialpayrulename"], 'uri', '')
    if payruleuri == '':
        payruleuri = rail.find_first_by_attr_and_get_attr(
            rail.result('get_all_payrule_scripts'),
            'displayText', 'MMA-Assurance Agency', 'uri', '')

    workdayidudfuri = rail.result('get_custom_field_data_uri')['workdayidudfuri']

    positionudf = rail.result('get_custom_field_data_uri')['positionudf']

    managerudfuri = rail.result('get_custom_field_data_uri')['manageruri']

    timeofftype_mapper_list = []
    for tm, tm_off in enumerate(timeoff_type_mapper):
        if tm_off['check'] == 'yes':
            timeofftype_mapper_list.append(tm_off['value'])
    timeoffuri = []
    timeoff_data = rail.result('get_all_timeoff_type')
    for t, t_off in enumerate(timeoff_data):
        if t_off['displayText'] in timeofftype_mapper_list:
            timeoffuri.append(t_off['uri'])

    enduserpermissionformanager = rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_permission_set'),
          'displayText', 'End user with reports view', 'uri', '')

    return {
        "loginname" : item["loginname"],
        "firstname" : item["firstname"],
        "lastname" : item["lastname"],
        "employeetype" : item["employeetype"],
        "department" : item["department"],
        "location" : item["location"],
        "authenticationtype" : item["authenticationtype"],
        "enabled" : item["enabled"],
        "employeeid" : item["employeeid"],
        "startdate" : item["startdate"],
        "enddate" : item["enddate"],
        "emailaddress" : item["emailaddress"],
        "initialsupervisorloginname" : item["initialsupervisorloginname"],
        "permissionsets" : item["permissionsets"],
        "timesheettemplate" : item["timesheettemplate"],
        "timesheetperiodtype" : item["timesheetperiodtype"],
        "timesheetapprovalpath" : item["timesheetapprovalpath"],
        "timezone" : item["timezone"],
        "workweek" : item["workweek"],
        "holidaycalendar" : item["holidaycalendar"],
        "initialschedulename" : item["initialschedulename"],
        "timeofftemplate" : item["timeofftemplate"],
        "timeoffapprovalpath" : item["timeoffapprovalpath"],
        "initialpayrulename" : item["initialpayrulename"],
        "workdayid" : item["workdayid"],
        "position" : item["position"],
        "workercategory" : item["workercategory"],
        "manager" : item["manager"],
        "logger" : rail.result('logger_list'),
        "supervisor_logger" : rail.result('supervisor_logger_list'),


        "departmenturi" : departmenturi,
        "locationuri" : locationuri,
        "supervisoruri" : initial_supervisor_uri,
        "enduseruri" : enduseruri,
        "supervisorpermissionuri" : supervisorpermissionuri,
        "timesheettemplateuri" : timesheettemplateuri,
        "timesheetperioduri" : timesheetperioduri,
        "timesheetapprovalpathuri" : timesheetapprovalpathuri,
        "timezoneuri" : timezoneuri,
        "employeetypeuri" : employeetypeuri,
        "workweekuri" : workweekuri,
        "holidaycalendaruri" : holidaycalendaruri,
        "officescheduleuri" : officescheduleuri,
        "defaultofficescheduleuri" : defaultofficescheduleuri,
        "timeofftemplateuri" : timeofftemplateuri,
        "timeoffapprovalpathuri" : timeoffapprovalpathuri,
        "payruleuri" : payruleuri,
        "workdayidudfuri" : workdayidudfuri,
        "positionudf" : positionudf,
        "managerudfuri" : managerudfuri,
        "timeoffuri" : timeoffuri,
        "type" : "New",
        "locationname" : locationname,
        "employeetypename" : employeetypename,
        "enduserpermissionformanager" : enduserpermissionformanager,
        "businessunit" : businessunit
    }

def create_user_payload(dag_run):

    officescheduleuri = dag_run.conf['defaultofficescheduleuri']
    if dag_run.conf['officescheduleuri']:
        officescheduleuri = dag_run.conf['officescheduleuri']

    holidaycalendar = ''
    if dag_run.conf['holidaycalendaruri']:
        holidaycalendar = {"uri": dag_run.conf['holidaycalendaruri']}

    permissionsets = []
    if dag_run.conf['enduseruri']:
        permissionsets = [{"uri": dag_run.conf['enduseruri']}]

    timesheetapprovalpath = None
    if dag_run.conf['timesheetapprovalpathuri']:
        timesheetapprovalpath = {"uri": dag_run.conf['timesheetapprovalpathuri']}

    timeoffapprovalpath = None
    if dag_run.conf['timeoffapprovalpathuri']:
        timeoffapprovalpath = {"uri": dag_run.conf['timeoffapprovalpathuri']}

    timezone = None
    if dag_run.conf['timezoneuri']:
        timezone = {"uri": dag_run.conf['timezoneuri']}

    departmentgroupschedule = []
    if dag_run.conf['departmenturi']:
        departmentgroupschedule = [{"departmentGroup": {"uri": dag_run.conf['departmenturi']}}]

    locationschedule = []
    if dag_run.conf['locationuri']:
        locationschedule = [{"location": {"uri": dag_run.conf['locationuri']}}]

    employeetypegroupschedule = []
    if dag_run.conf['employeetypeuri']:
        employeetypegroupschedule = [{"employeeTypeGroup": {"uri": dag_run.conf['employeetypeuri']}}]

    timesheetperiodschedule = []
    if dag_run.conf['timesheetperioduri']:
        timesheetperiodschedule = [{"timesheetPeriod": {"uri": dag_run.conf['timesheetperioduri']}}]

    payrulescriptschedule = []
    if dag_run.conf['payruleuri']:
        payrulescriptschedule = [{"payRuleScript": {"uri": dag_run.conf['payruleuri']}}]

    policyseturi = []
    policysettimesheetdict = {}
    policysettimeoffdict = {}
    if dag_run.conf['timesheettemplateuri'] and dag_run.conf['manager'].lower() == 'no':
        policysettimesheetdict['uri'] = dag_run.conf['timesheettemplateuri']
        policyseturi.append(policysettimesheetdict)
    if dag_run.conf['timeofftemplateuri'] and dag_run.conf['manager'].lower() == 'no':
        policysettimeoffdict['uri'] = dag_run.conf['timeofftemplateuri']
        policyseturi.append(policysettimeoffdict)

    custom_fieldvalues = []
    workdayid_dict = {}
    workdayid_dict['customField'] = {}
    position_dict = {}
    position_dict['customField'] = {}
    manager_dict = {}
    manager_dict['customField'] = {}
    manager_dict['dropDownOption'] = {}
    if dag_run.conf['workdayid']:
        workdayid_dict['customField']['uri'] = dag_run.conf['workdayidudfuri']
        workdayid_dict['text'] = dag_run.conf['workdayid']
        custom_fieldvalues.append(workdayid_dict)
    if dag_run.conf['position']:
        position_dict['customField']['uri'] = dag_run.conf['positionudf']
        position_dict['text'] = dag_run.conf['position']
        custom_fieldvalues.append(position_dict)
    if dag_run.conf['manager']:
        manager_dict['customField']['uri'] = dag_run.conf['managerudfuri']
        manager_dict['dropDownOption']['name'] = dag_run.conf['manager']
        custom_fieldvalues.append(manager_dict)

    return {
            "user": {
                "target": {
                    "loginName": dag_run.conf['loginname']
                },
                "firstname": dag_run.conf['firstname'],
                "lastname": dag_run.conf['lastname'],
                "emailAddress": dag_run.conf['emailaddress'],
                "employeeId": dag_run.conf['employeeid'],
                "schedulePolicySchedule": [
                {
                    "schedulePolicy": {
                        "officeScheduleUri": officescheduleuri,
                        "officeSchedule": {
                            "officeScheduleUri": officescheduleuri
                        },
                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                    }
                }
                ],
                "workWeekStartDayUri": dag_run.conf['workweekuri'],
                "employmentDateRange": {
                    "startDate": {
                        "year": rail.result('split_start_date')['year'],
                        "month": rail.result('split_start_date')['month'],
                        "day": rail.result('split_start_date')['day']
                    }
                },
                "securityConfiguration": {
                    "enabledAuthenticationTypeUris": [
                        "urn:replicon:user-authentication-type:sso"
                    ],
                    "isLoginEnabled": "true",
                    "loginName": dag_run.conf['loginname'],
                    "SSOName": dag_run.conf['loginname']
                },
                "holidayCalendar": holidaycalendar,
                "permissionSets": permissionsets,
                "policySets": policyseturi,
                "timesheetApprovalPath": timesheetapprovalpath,
                "timeOffApprovalPath": timeoffapprovalpath,
                "customFieldValues": custom_fieldvalues,
                "timeZone": timezone,
                "locationSchedule": locationschedule,
                "costCenterSchedule": [
                {
                    "costCenter": {
                    "uri": None,
                    "parentUri": None,
                    "name": dag_run.conf['businessunit']
                    },
                    "effectiveDate": None
                }
                ],
                "departmentGroupSchedule": departmentgroupschedule,
                "employeeTypeGroupSchedule": employeetypegroupschedule,
                "timesheetPeriodSchedule": timesheetperiodschedule,
                "payRuleScriptSchedule": payrulescriptschedule
            }
        }

def get_supervisor_data(dag_run):
    return {
        "users": [
            {
                "uri": dag_run.conf['supervisoruri']
            }
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }

def update_emp_date(dag_run):
    return {
            "userUri": dag_run.conf['useruri'],
            "dateRange": {
                "startDate": {
                    "year": dag_run.conf['startdate'].split('/')[2],
                    "month": dag_run.conf['startdate'].split('/')[1],
                    "day": dag_run.conf['startdate'].split('/')[0],
                },
                "endDate": {
                    "year": dag_run.conf['enddate'].split('/')[2],
                    "month": dag_run.conf['enddate'].split('/')[1],
                    "day": dag_run.conf['enddate'].split('/')[0]
                }
            }
        }


def process_user_to_update(item):
    departmenturi = rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_enabled_dept_list'),
          'displayText', item['department'], 'uri', '')
    businessunit = item['businessunit']
    location_data = rail.result('get_all_enabled_location')
    locationuri = ''
    locationname = ''
    for i, row in enumerate(location_data['rows']):
        if row['cells'][1]['textValue'] == item['location']:
            locationuri = row['cells'][0]['uri']
            locationname = row['cells'][0]['textValue']

    supervisor_data = rail.load_all_records(rail.result('query_replicon_userdata'))
    initial_supervisor_uri = rail.find_first_by_attr_and_get_attr(
        supervisor_data, 'loginname', item['initialsupervisorloginname'], 'useruri', '')

    enduseruri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_permission_set'),
          'name', item['permissionsets'], 'uri', '')

    supervisorpermissionuri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_permission_set'),
          'displayText', 'Manager', 'uri', '')

    timesheettemplateuri = ''
    if item['timesheettemplate']:
        timesheettemplateuri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_policy_sets'),
          'name', item["timesheettemplate"], 'uri', '')

    timesheetperiod_data = rail.result('get_all_enabled_timesheet_period')
    timesheetperioduri = ''
    for j, row in enumerate(timesheetperiod_data['rows']):
        if row['cells'][0]['textValue'] == item['timesheetperiodtype']:
            timesheetperioduri = row['cells'][0]['uri']

    timesheetapprovalpathuri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_timesheet_approval'), 'displayText', item['timesheetapprovalpath'], 'uri', '')

    if item['location']:
        timezoneuri = rail.find_first_by_attr_and_get_attr(timezone_mapper, 'identifier', item["location"], 'uri', '')
        if not timezoneuri:
            timezoneuri = rail.find_first_by_attr_and_get_attr(timezone_mapper, 'value', item["timezone"], 'uri', '')
    else:
        timezoneuri = rail.find_first_by_attr_and_get_attr(timezone_mapper, 'value', item["timezone"], 'uri', '')

    employeetypename = None
    for k,emp in enumerate(employee_type_mapper):
        if emp['employeetype'] == item["employeetype"] and \
        emp['worker_category'] == item["workercategory"] and \
        emp['manager'] == item["manager"] :
            employeetypename = emp['value']
    employeetypeuri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_employee_type'), 'displayText', employeetypename, 'uri', '')

    workweek_start_day = ''
    if item["workweek"]:
        workweek_start_day = item["workweek"].split(" ")[0].lower()
    workweekuri = rail.find_first_by_attr_and_get_attr(workweek_mapper, 'value', workweek_start_day, 'uri', '')

    holidaycalendaruri = ''
    if item['holidaycalendar']:
        holidaycalendaruri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_holiday_calendars'),
          'displayText', item["holidaycalendar"], 'uri', '')

    officescheduleuri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_office_schedules'), 'displayText', item['initialschedulename'], 'uri', '')

    defaultofficescheduleuri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_office_schedules'), 'displayText', '40', 'uri', '')

    timeofftemplateuri = ''
    if item['timeofftemplate']:
        timeofftemplateuri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_policy_sets'),
          'name', item["timeofftemplate"], 'uri', '')

    timeoffapprovalpathuri = ''
    if item['timeoffapprovalpath']:
        timeoffapprovalpathuri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_timeoff_approval'),
          'displayText', item["timeoffapprovalpath"], 'uri', '')

    payruleuri = ''
    if item['initialpayrulename']:
        payruleuri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_payrule_scripts'),
          'displayText', item["initialpayrulename"], 'uri', '')

    workdayidudfuri = rail.result('get_custom_field_data_uri')['workdayidudfuri']

    positionudf = rail.result('get_custom_field_data_uri')['positionudf']

    managerudfuri = rail.result('get_custom_field_data_uri')['manageruri']

    timeofftype_mapper_list = []
    for tm, tm_off in enumerate(timeoff_type_mapper):
        if tm_off['check'] == 'yes':
            timeofftype_mapper_list.append(tm_off['value'])
    timeoffuri = []
    timeoff_data = rail.result('get_all_timeoff_type')
    for t, t_off in enumerate(timeoff_data):
        if t_off['displayText'] in timeofftype_mapper_list:
            timeoffuri.append(t_off['uri'])

    userdata = rail.load_all_records(rail.result('query_replicon_userdata'))
    useruri = rail.find_first_by_attr_and_get_attr(userdata, 'employeeid', item['employeeid'], 'useruri', '')

    managerudfvalueuri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_custom_fields_for_required_group'), 'displayText', item['manager'], 'uri', '')

    enduserpermissionformanager = rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_permission_set'),
          'displayText', 'End user with reports view', 'uri', '')

    return {
        "loginname" : item["loginname"],
        "firstname" : item["firstname"],
        "lastname" : item["lastname"],
        "employeetype" : item["employeetype"],
        "department" : item["department"],
        "location" : item["location"],
        "authenticationtype" : item["authenticationtype"],
        "enabled" : item["enabled"],
        "employeeid" : item["employeeid"],
        "startdate" : item["startdate"],
        "enddate" : item["enddate"],
        "emailaddress" : item["emailaddress"],
        "initialsupervisorloginname" : item["initialsupervisorloginname"],
        "permissionsets" : item["permissionsets"],
        "timesheettemplate" : item["timesheettemplate"],
        "timesheetperiodtype" : item["timesheetperiodtype"],
        "timesheetapprovalpath" : item["timesheetapprovalpath"],
        "timezone" : item["timezone"],
        "workweek" : item["workweek"],
        "holidaycalendar" : item["holidaycalendar"],
        "initialschedulename" : item["initialschedulename"],
        "timeofftemplate" : item["timeofftemplate"],
        "timeoffapprovalpath" : item["timeoffapprovalpath"],
        "initialpayrulename" : item["initialpayrulename"],
        "workdayid" : item["workdayid"],
        "position" : item["position"],
        "workercategory" : item["workercategory"],
        "manager" : item["manager"],
        "logger" : rail.result('logger_list'),
        "supervisor_logger" : rail.result('supervisor_logger_list'),

        "departmenturi" : departmenturi,
        "locationuri" : locationuri,
        "supervisoruri" : initial_supervisor_uri,
        "enduseruri" : enduseruri,
        "supervisorpermissionuri" : supervisorpermissionuri,
        "timesheettemplateuri" : timesheettemplateuri,
        "timesheetperioduri" : timesheetperioduri,
        "timesheetapprovalpathuri" : timesheetapprovalpathuri,
        "timezoneuri" : timezoneuri,
        "employeetypeuri" : employeetypeuri,
        "workweekuri" : workweekuri,
        "holidaycalendaruri" : holidaycalendaruri,
        "officescheduleuri" : officescheduleuri,
        "defaultofficescheduleuri" : defaultofficescheduleuri,
        "timeofftemplateuri" : timeofftemplateuri,
        "timeoffapprovalpathuri" : timeoffapprovalpathuri,
        "payruleuri" : payruleuri,
        "workdayidudfuri" : workdayidudfuri,
        "positionudf" : positionudf,
        "managerudfuri" : managerudfuri,
        "timeoffuri" : timeoffuri,
        "type" : "update",
        "useruri" : useruri,
        "managerudfvalueuri" : managerudfvalueuri,
        "locationname" : locationname,
        "employeetypename" : employeetypename,
        "enduserpermissionformanager" : enduserpermissionformanager,
        "businessunit" : businessunit
    }

def process_supervisor_mapper_data(item):
    return {
        "userloginname" : item['userloginname'],
        "useruri" : item['useruri'],
        "username" : item['username'],
        "supervisorloginname" : item['supervisorloginname'],
        "emplid" : item['emplid'],
        "action" : item['action'],
        "status" : item['status'],
        "supervisorpermissionuri" : rail.find_first_by_attr_and_get_attr(
            rail.result('get_all_permission_set'), 'displayText', 'Manager', 'uri', ''),
        "enduserpermissionformanager" : rail.find_first_by_attr_and_get_attr(
            rail.result('get_all_permission_set'), 'displayText', 'End user with reports view', 'uri', ''),
        "logger" : rail.result('logger_list'),
    }

def get_bulk_user_data(dag_run):
    return {
        "users": [
            {
            "uri": dag_run.conf['useruri']
            }
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
        }

def update_userdata_to_old(dag_run):
    return {
        "user": {
            "uri": dag_run.conf['useruri']
        },
        "modifications": {
            "securitySettingsToApply": {
                "loginEnabled": "false",
                "forcePasswordChange": "false",
                "loginName": "Old_" + dag_run.conf['loginname'],
                "ssoName": "Old_" + dag_run.conf['loginname'],
                "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:sso"
                ],
                "emailMFAResendVerificationEmail": "false",
                "emailMFATryAddMethodFromUsersEmail": "false",
                "isMFAMethodRequired": "false",
                "clearIsLockedOut": "false"
            },
            "userDetailsToApply": {

                "emailAddress": {
                    "emailAddress": "Old_" + dag_run.conf['emailaddress']
                },
                "employeeId": {
                    "employeeId": "Old_" + dag_run.conf['employeeid']
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
        }

def add_user_from_update(dag_run):
    return {
        "loginname" : dag_run.conf['loginname'],
        "firstname" : dag_run.conf["firstname"],
        "lastname" : dag_run.conf["lastname"],
        "employeetype" : dag_run.conf["employeetype"],
        "department" : dag_run.conf["department"],
        "location" : dag_run.conf["location"],
        "authenticationtype" : dag_run.conf["authenticationtype"],
        "enabled" : dag_run.conf["enabled"],
        "employeeid" : dag_run.conf["employeeid"],
        "startdate" : dag_run.conf["startdate"],
        "enddate" : dag_run.conf["enddate"],
        "emailaddress" : dag_run.conf["emailaddress"],
        "initialsupervisorloginname" : dag_run.conf["initialsupervisorloginname"],
        "permissionsets" : dag_run.conf["permissionsets"],
        "timesheettemplate" : dag_run.conf["timesheettemplate"],
        "timesheetperiodtype" : dag_run.conf["timesheetperiodtype"],
        "timesheetapprovalpath" : dag_run.conf["timesheetapprovalpath"],
        "timezone" : dag_run.conf["timezone"],
        "workweek" : dag_run.conf["workweek"],
        "holidaycalendar" : dag_run.conf["holidaycalendar"],
        "initialschedulename" : dag_run.conf["initialschedulename"],
        "timeofftemplate" : dag_run.conf["timeofftemplate"],
        "timeoffapprovalpath" : dag_run.conf["timeoffapprovalpath"],
        "initialpayrulename" : dag_run.conf["initialpayrulename"],
        "workdayid" : dag_run.conf["workdayid"],
        "position" : dag_run.conf["position"],
        "workercategory" : dag_run.conf["workercategory"],
        "manager" : dag_run.conf["manager"],
        "logger" : dag_run.conf["logger"],
        "supervisor_logger" : dag_run.conf["supervisor_logger"],

        "departmenturi" : dag_run.conf["departmenturi"],
        "locationuri" : dag_run.conf["locationuri"],
        "supervisoruri" : dag_run.conf["supervisoruri"],
        "enduseruri" : dag_run.conf["enduseruri"],
        "supervisorpermissionuri" : dag_run.conf["supervisorpermissionuri"],
        "timesheettemplateuri" : dag_run.conf["timesheettemplateuri"],
        "timesheetperioduri" : dag_run.conf["timesheetperioduri"],
        "timesheetapprovalpathuri" : dag_run.conf["timesheetapprovalpathuri"],
        "timezoneuri" : dag_run.conf["timezoneuri"],
        "employeetypeuri" : dag_run.conf["employeetypeuri"],
        "workweekuri" : dag_run.conf["workweekuri"],
        "holidaycalendaruri" : dag_run.conf["holidaycalendaruri"],
        "officescheduleuri" : dag_run.conf["officescheduleuri"],
        "defaultofficescheduleuri" : dag_run.conf["defaultofficescheduleuri"],
        "timeofftemplateuri" : dag_run.conf["timeofftemplateuri"],
        "timeoffapprovalpathuri" : dag_run.conf["timeoffapprovalpathuri"],
        "payruleuri" : dag_run.conf["payruleuri"],
        "workdayidudfuri" : dag_run.conf["workdayidudfuri"],
        "positionudf" : dag_run.conf["positionudf"],
        "managerudfuri" : dag_run.conf["managerudfuri"],
        "timeoffuri" : dag_run.conf["timeoffuri"],
        "type" : "rehire",
        "locationname" : dag_run.conf["locationname"],
        "employeetypename" : dag_run.conf["employeetypename"],
        "enduserpermissionformanager" : dag_run.conf["enduserpermissionformanager"],
        "businessunit" : dag_run.conf["businessunit"]
    }

def update_user_loginname(dag_run):
    return {
        "user": {
            "uri": dag_run.conf['useruri']
        },
        "modifications": {
            "securitySettingsToApply": {
                "loginEnabled": "true",
                "forcePasswordChange": "false",
                "loginName": dag_run.conf['loginname'],
                "ssoName": dag_run.conf['loginname'],
                "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:sso"
                ],
                "emailMFAResendVerificationEmail": "false",
                "emailMFATryAddMethodFromUsersEmail": "false",
                "isMFAMethodRequired": "false",
                "clearIsLockedOut": "false"
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
        }

def payrule_update_payload(dag_run):
    return {
        "user": {
            "uri": dag_run.conf['useruri']
        },
        "modifications": {
            "payRulesScheduleModifications": {
                "scheduleEntries": [
                    {
                        "payRuleScript": {
                            "uri": dag_run.conf['payruleuri']
                        },
                        "effectiveDate": get_today_date_format()
                    }
                ]
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
        }

def timesheet_period_update_payload(dag_run):
    return {
        "user": {
            "uri": dag_run.conf['useruri']
        },
        "modifications": {
            "timesheetPeriodScheduleToApply": {
                "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementTimesheetPeriodSchedule": [],
                "updateTimesheetPeriodScheduleOverDateRange": {
                    "replacementTimesheetPeriodScheduleEntries": [
                        {

                            "timesheetPeriod": {
                                "uri":dag_run.conf['timesheetperioduri']
                            },
                            "effectiveDate": get_today_date_format()
                        }
                    ]
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
}

def officeschedule_update_payload(dag_run):
    return {
        "user": {
            "uri": dag_run.conf['useruri']
        },
        "modifications": {

            "schedulePolicyToApply": {
                "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementSchedule": [],
                "updateScheduleOverDateRange": {
                    "replacementScheduleEntries": [

                        {
                            "schedulePolicy": {
                            "officeScheduleUri": dag_run.conf['officescheduleuri'],
                            "officeSchedule": {
                                "officeScheduleUri": dag_run.conf['officescheduleuri']
                            },
                            "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                            },
                            "effectiveDate": get_today_date_format()
                        }
                    ]
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

def department_update_payload(dag_run):
    return {
        "user": {
            "uri": dag_run.conf['useruri']
        },
        "modifications": {
            "departmentGroupScheduleToApply": {
                "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementDepartmentGroupSchedule": [],
                "updateDepartmentGroupScheduleOverDateRange": {
                    "replacementDepartmentGroupScheduleEntries": [
                        {
                            "departmentGroup": {
                                "uri": dag_run.conf['departmenturi']
                            },
                            "effectiveDate": get_today_date_format()
                        }
                    ]
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
        }

def location_update_payload(dag_run):
    return {
        "user": {
            "uri": dag_run.conf['useruri']
        },
        "modifications": {
            "locationScheduleToApply": {
                "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementLocationSchedule": [],
                "updateLocationScheduleOverDateRange": {
                    "replacementLocationScheduleEntries": [
                        {
                            "location": {
                                "uri": dag_run.conf['locationuri']
                            },
                            "effectiveDate": get_today_date_format()
                        }
                    ]
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
        }

def costcenter_update_payload(dag_run):
    return {
        "user": {
            "uri": dag_run.conf['useruri']
        },
        "modifications": {
            "costCenterScheduleToApply": {
                "userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementCostCenterSchedule": [],
                "updateCostCenterScheduleOverDateRange": {
                    "replacementCostCenterScheduleEntries": [
                        {
                            "costCenter": {
                                "name": dag_run.conf['businessunit']
                            },
                            "effectiveDate": get_today_date_format()
                        }
                    ]
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
        }

def employeetype_update_payload(dag_run):
    return {
        "user": {
            "uri": dag_run.conf['useruri']
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
                            "effectiveDate": get_today_date_format()
                        }
                    ]
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
        }

def get_supervisordetails(dag_run):
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
            "urn:replicon:user-list-column:login-name",
            "urn:replicon:user-list-column:enabled"
        ],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:user-list-filter:login-name"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": dag_run.conf['supervisorloginname']
                }
            }
        }
    }

def user_import_log_csv_data(item):
    return [
        item['username'],
        item['login_name'],
        item['emplid'],
        item['action'],
        item['status'],
        item['details']
    ]
