from hashlib import sha256
from rail.lib.ecid import get_dagrun_ecid
import rail
from assuredpartnersinc.user_import_v3.utils import python_callable

null = None


def row_data_for_input_file(item):
    return [
        item["EE Status"].strip() if item["EE Status"] else null,
        item["Empl ID/ Login"].strip() if item["Empl ID/ Login"] else null,
        item["First Name"].strip() if item["First Name"] else null,
        item["Last Name"].strip() if item["Last Name"]else null,
        item["EE Type"].strip() if item["EE Type"] else null,
        item["Job Code"].strip() if item["Job Code"] else null,
        item["Job Title"].strip() if item["Job Title"] else null,
        item["FLSA Status"].strip() if item["FLSA Status"] else null,
        item["Service Date"].strip() if item["Service Date"] else null,
        item["Termination Date"].strip() if item["Termination Date"] else null,
        # Agency (Org 2) field renamed in Feed via CR v5.3
        item["Business Unit Code"].strip(
        ) if item["Business Unit Code"] else null,
        # Agency Description field renamed in Feed via CR v5.3
        item["Business Unit Description"].strip(
        ) if item["Business Unit Description"] else null,
        item["Supervisor ID"].strip() if item["Supervisor ID"] else null,
        item["Supervisor Name"].strip() if item["Supervisor Name"] else null,
        item["E-Mail"].strip() if item["E-Mail"] else null,
        item["Hourly Rate"].strip() if item["Hourly Rate"] else null,
        item["Weekly STD Hrs"].strip() if item["Weekly STD Hrs"] else null,
        item["Schedule"].strip() if item["Schedule"] else null,
        item["PTO Seniority Date"].strip(
        ) if item["PTO Seniority Date"] else null,
        item["Profit Center"].strip() if item["Profit Center"] else null,
        item["Profit Center Description"].strip(
        ) if item["Profit Center Description"] else null,
        item["Cpny Code"].strip() if item["Cpny Code"] else null,
        item["Pay Group Code"].strip() if item["Pay Group Code"] else null,
        item["Pay Group"].strip() if item["Pay Group"] else null,
        item["PTO-1"].strip() if item["PTO-1"] else null,
        item["PTO-Bereavement"].strip() if item["PTO-Bereavement"] else null,
        item["PTO-Jury Duty"].strip() if item["PTO-Jury Duty"] else null,
        item["Holiday Type"].strip() if item["Holiday Type"] else null,
        item["Illness"].strip() if item["Illness"] else null,
        item["Change Effective Date"].strip(
        ) if item["Change Effective Date"] else null,
        item["VTO"].strip() if item["VTO"] else null,
        item["Emergency Sick"].strip() if item["Emergency Sick"] else null,
        item["Pay Rules"].strip() if item["Pay Rules"] else null,
        item["Timesheet Template"].strip(
        ) if item["Timesheet Template"] else null,
        item["Time Off Template"].strip() if item["Time Off Template"] else null,
        item["Holiday Calendars"].strip() if item["Holiday Calendars"] else null,
        item["Time Zone"].strip() if item["Time Zone"] else null,
        item["Work Week"].strip() if item["Work Week"] else null,
        item["Location Code (Work)"].strip(
        ) if item["Location Code (Work)"] else null,
        item["Dept (Org 4)"].strip() if item["Dept (Org 4)"] else null,
        item["Dept (Org 4 Desc)"].strip(
        ) if item["Dept (Org 4 Desc)"] else null,
        item["Core Supervisor ID"].strip(
        ) if item["Core Supervisor ID"] else null,
        item["Core Supervisor Name"].strip(
        ) if item["Core Supervisor Name"] else null,
        item["LOA Suspend PTO Start"].strip(
        ) if item["LOA Suspend PTO Start"] else null,
        item["LOA Suspend PTO End"].strip(
        ) if item["LOA Suspend PTO End"] else null,
        item["Activity"].strip() if item["Activity"] else null,
        item["Make-up Time PTO"].strip() if item["Make-up Time PTO"] else null,
        item["Punch Entry Policy"].strip(
        ) if item["Punch Entry Policy"] else null,
        item["Payroll Grouping"].strip() if item["Payroll Grouping"] else null,
        item["Payroll Permission"].strip(
        ) if item["Payroll Permission"] else null,
        item["Admin Permission"].strip() if item["Admin Permission"] else null,
        item["Condition- Restrict"].strip() if item["Condition- Restrict"] else null,
        item["Payroll Grouping Groups"].strip(
        ) if item["Payroll Grouping Groups"] else null,
        item["Profit Center Groups"].strip(
        ) if item["Profit Center Groups"] else null,
        item["Agency Groups"].strip() if item["Agency Groups"] else null,
        item["Pay Group Groups"].strip() if item["Pay Group Groups"] else null,
        item["Location Groups"].strip() if item["Location Groups"] else null,
        item["Department Groups"].strip() if item["Department Groups"] else null,
        item["Additional Time Off Types"].strip(
        ) if item["Additional Time Off Types"] else null,
        item["Replicon TS Date"].strip() if item["Replicon TS Date"] else null,
        item["Daily Hours"].strip() if item["Daily Hours"] else null,
        item["Illness PTO"].strip() if item["Illness PTO"] else null,
        item["Assignment Number"].strip() if item["Assignment Number"] else null,
        sha256((str(item["EE Status"]) + "," + str(item["Empl ID/ Login"]) + "," + str(item["First Name"]) + "," + str(item["Last Name"]) + "," + str(item["EE Type"]) + "," +
                str(item["Job Code"]) + "," + str(item["Job Title"]) + "," + str(item["FLSA Status"]) + "," + str(item["Service Date"]) + "," + str(item["Termination Date"]) + "," +
                str(item["Business Unit Code"]) + "," + str(item["Business Unit Description"]) + "," + str(item["Supervisor ID"]) + "," + str(item["Supervisor Name"]) + "," +
                str(item["E-Mail"]) + "," + str(item["Hourly Rate"]) + "," + str(item["Weekly STD Hrs"]) + "," + str(item["Schedule"]) + "," + str(item["PTO Seniority Date"]) + "," +
                str(item["Profit Center"]) + "," + str(item["Profit Center Description"]) + "," + str(item["Cpny Code"]) + "," + str(item["Pay Group Code"]) + "," +
                str(item["Pay Group"]) + "," + str(item["PTO-1"]) + "," + str(item["PTO-Bereavement"]) + "," + str(item["PTO-Jury Duty"]) + "," + str(item["Holiday Type"]) + "," +
                str(item["Illness"]) + "," + str(item["VTO"]) + "," + str(item["Emergency Sick"]) + "," + str(item["Pay Rules"]) + "," +
                str(item["Timesheet Template"]) + "," + str(item["Time Off Template"]) + "," + str(item["Holiday Calendars"]) + "," + str(item["Time Zone"]) + "," +
                str(item["Work Week"]) + "," + str(item["Location Code (Work)"]) + "," + str(item["Dept (Org 4)"]) + "," + str(item["Dept (Org 4 Desc)"]) + "," +
                str(item["Core Supervisor ID"]) + "," + str(item["Core Supervisor Name"]) + "," + str(item["LOA Suspend PTO Start"]) + "," + str(item["LOA Suspend PTO End"]) + "," +
                str(item["Activity"]) + "," + str(item["Make-up Time PTO"]) + "," + str(item["Punch Entry Policy"]) + "," + str(item["Payroll Grouping"]) + "," +
                str(item["Payroll Permission"]) + "," + str(item["Admin Permission"]) + "," + str(item["Condition- Restrict"]) + "," + str(item["Payroll Grouping Groups"]) + "," +
                str(item["Profit Center Groups"]) + "," + str(item["Agency Groups"]) + "," + str(item["Pay Group Groups"]) + "," + str(item["Location Groups"]) + "," +
                str(item["Department Groups"]) + "," + str(item["Additional Time Off Types"]) + "," + str(item["Replicon TS Date"]) + "," +
                str(item["Daily Hours"]) + "," + str(item["Illness PTO"]) + "," + str(item["Assignment Number"])).encode()).hexdigest()
    ]


def process_each_user_payload(item, custom_field_uris):
    return {
        "parentjobid": get_dagrun_ecid(rail.get_current_context()['dag_run']),
        "EEStatus": item["EEStatus"],
        "EmplID_Login": item["EmplID_Login"],
        "FirstName": item["FirstName"],
        "LastName": item["LastName"],
        "EEType": item["EEType"],
        "JobCode": item["JobCode"],
        "JobTitle": item["JobTitle"],
        "FLSAStatus": item["FLSAStatus"],
        "ServiceDate": item["ServiceDate"],
        "TerminationDate": item["TerminationDate"],
        "Agency_Org2": item["Agency_Org2"],
        "AgencyDescription": item["AgencyDescription"],
        "SupervisorID": item["SupervisorID"],
        "SupervisorName": item["SupervisorName"],
        "E_Mail": item["E_Mail"],
        "HourlyRate": item["HourlyRate"],
        "WeeklySTDHrs": item["WeeklySTDHrs"],
        "Schedule": item["Schedule"],
        "PTOSeniorityDate": item["PTOSeniorityDate"],
        "ProfitCenter": item["ProfitCenter"],
        "ProfitCenterDescription": item["ProfitCenterDescription"],
        "CpnyCode": item["CpnyCode"],
        "PayGroupCode": item["PayGroupCode"],
        "PayGroup": item["PayGroup"],
        "PTO_1": item["PTO_1"],
        "PTO_Bereavement": item["PTO_Bereavement"],
        "PTO_JuryDuty": item["PTO_JuryDuty"],
        "HolidayType": item["HolidayType"],
        "Illness": item["Illness"],
        "ChangeEffectiveDate": item["ChangeEffectiveDate"],
        "VTO": item["VTO"],
        "EmergencySick": item["EmergencySick"],
        "PayRules": item["PayRules"],
        "TimesheetTemplate": item["TimesheetTemplate"],
        "TimeOffTemplate": item["TimeOffTemplate"],
        "HolidayCalendars": item["HolidayCalendars"],
        "TimeZone": item["TimeZone"],
        "WorkWeek": item["WorkWeek"],
        "PayrollGrouping": item["PayrollGrouping"],
        "LocationCode_Work": item["LocationCode_Work"],
        "Dept_Org4": item["Dept_Org4"],
        "Dept_Org4Desc": item["Dept_Org4Desc"],
        "CoreSupervisorID": item["CoreSupervisorID"],
        "CoreSupervisorName": item["CoreSupervisorName"],
        "LOASuspendPTOStart": item["LOASuspendPTOStart"],
        "LOASuspendPTOEnd": item["LOASuspendPTOEnd"],
        "PayrollPermission": item["PayrollPermission"],
        "AdminPermission": item["AdminPermission"],
        "ConditionRestrict": item["ConditionRestrict"],
        "PayrollGroupingGroups": item["PayrollGroupingGroups"],
        "ProfitCenterGroups": item["ProfitCenterGroups"],
        "AgencyGroups": item["AgencyGroups"],
        "PayGroupGroups": item["PayGroupGroups"],
        "LocationGroups": item["LocationGroups"],
        "DepartmentGroups": item["DepartmentGroups"],
        "AdditionalTimeOffTypes": item["AdditionalTimeOffTypes"],
        "RepliconTSDate": item["RepliconTSDate"],
        "DailyHours": item["DailyHours"],
        "illnesspto": item["illnesspto"],
        "activity": item["activity"],
        "makeuptimepto": item['makeuptimepto'],
        "punch_entry_policy": item['punchentrypolicy'],
        'AssignmentNumber': item['AssignmentNumber'],
        "assignmentnumber_udf_uri": custom_field_uris["assignmentnumber_udf_uri"],
        "time_administrator_grouping_division_uri": null,
        "companyjobdata_udf_uri": null,
        "eetype_udf_uri": custom_field_uris["eetype_udf_uri"],
        "job_code_udf_uri": custom_field_uris["job_code_udf_uri"],
        "flsastatus_udf_uri": custom_field_uris["flsastatus_udf_uri"],
        "agencyorg2_udf_uri": custom_field_uris["agencyorg2_udf_uri"],
        "hourlyrate_udf_uri": custom_field_uris["hourlyrate_udf_uri"],
        "cpnycode_udf_uri": custom_field_uris["cpnycode_udf_uri"],
        "pay_group_code_udf_uri": custom_field_uris["pay_group_code_udf_uri"],
        "location_code_work_udf_uri": custom_field_uris["location_code_work_udf_uri"],
        "dept_org4_desc_udf_uri": custom_field_uris["dept_org4_desc_udf_uri"],
        "core_supervisorID_udf_uri": custom_field_uris["core_supervisorID_udf_uri"],
        "core_supervisor_name_udf_uri": custom_field_uris["core_supervisor_name_udf_uri"],
        "EEstatusuri": custom_field_uris["EEstatusuri"],
        "loastartdateuri": custom_field_uris["loastartdateuri"],
        "loaenddateuri": custom_field_uris["loaenddateuri"],
        "dailyhoursudfuri": custom_field_uris["dailyhoursudfuri"],
        "replicontsdateudfuri": custom_field_uris["replicontsdateudfuri"],
        "enddateudfuri": custom_field_uris["enddateudfuri"],
        "pto_seniority_date_udf_uri": custom_field_uris["pto_seniority_date_udf_uri"],
        "change_effective_date_udf_uri": custom_field_uris["change_effective_date_udf_uri"],
        "agency_org2_department_uri": rail.find_first_by_attr_and_get_attr(rail.result(
            "get_departmentdata_50"), 'fullpath', "AssuredPartnersInc/" + item['Agency_Org2'], 'uri') if rail.result(
                "get_departmentdata_50") else null,
        "deptorg4desc_employeetype_uri": rail.find_first_by_attr_and_get_attr(rail.result(
            'get_all_employee_type_groups_dept_org4_desc_53'), 'displayText', item['Dept_Org4Desc'], 'uri') if rail.result(
            'get_all_employee_type_groups_dept_org4_desc_53') else null,
        "profitcenter_division_uri": rail.find_first_by_attr_and_get_attr(rail.result(
            'get_all_service_centers_profit_center_55'), 'displayText', item['ProfitCenter'], 'uri') if rail.result(
            'get_all_service_centers_profit_center_55') else null,
        "pay_group_code_location_uri": rail.find_first_by_attr_and_get_attr(rail.result(
            'get_all_locations_pay_group_code_54'), 'displayText', item['PayGroupCode'], 'uri') if rail.result(
            'get_all_locations_pay_group_code_54') else null,
        "location_code_work_division_uri": rail.find_first_by_attr_and_get_attr(rail.result(
            'get_all_divisions_location_code_work_52'), 'displayText', item['LocationCode_Work'], 'uri') if rail.result(
            'get_all_divisions_location_code_work_52') else null,
        "payroll_grouping_cost_center_uri": rail.find_first_by_attr_and_get_attr(rail.result(
            'get_all_cost_centers_payroll_grouping_51'), 'displayText', item['PayrollGrouping'], 'uri') if rail.result(
            'get_all_cost_centers_payroll_grouping_51') else null,
        "officeschedule_uri": rail.find_first_by_attr_and_get_attr(rail.result(
            'get_all_office_schedules_57'), 'displayText', item['Schedule'], 'uri'),
        "timezoneuri": rail.find_first_by_attr_and_get_attr(rail.result(
            'get_all_time_zones_58'), 'displayText', item['TimeZone'], 'uri') if rail.result('get_all_time_zones_58') else null,
        "agency_grouping_department_uri": rail.find_first_by_attr_and_get_attr(rail.result(
            "get_departmentdata_50"), 'fullpath', "AssuredPartnersInc/" + item['Agency_Org2'], 'uri') if rail.result(
                "get_departmentdata_50") else null,
        "supervisor_assignment_log": rail.result("supervisor_assignment_log"),
        "integration_run_date": rail.result('log_integration_run_date')
    }


def get_add_update_dag_conf(dag_run, action, config):

    conf = {
        'parentjobid': dag_run.conf['parentjobid'],
        'EEStatus': dag_run.conf['EEStatus'],
        'EmplID_Login': dag_run.conf['EmplID_Login'],
        'FirstName': dag_run.conf['FirstName'],
        'LastName': dag_run.conf['LastName'],
        'EEType': dag_run.conf['EEType'],
        'JobCode': dag_run.conf['JobCode'],
        'JobTitle': dag_run.conf['JobTitle'],
        'FLSAStatus': dag_run.conf['FLSAStatus'],
        'ServiceDate': dag_run.conf['ServiceDate'],
        'TerminationDate': dag_run.conf['TerminationDate'],
        'Agency_Org2': dag_run.conf['Agency_Org2'],
        'AgencyDescription': dag_run.conf['AgencyDescription'],
        'SupervisorID': dag_run.conf['SupervisorID'],
        'SupervisorName': dag_run.conf['SupervisorName'],
        'E_Mail': dag_run.conf['E_Mail'],
        'HourlyRate': dag_run.conf['HourlyRate'],
        **python_callable.parse_hourly_rate(dag_run.conf['HourlyRate'], config.payroll_rate_currency_mapper),
        'WeeklySTDHrs': dag_run.conf['WeeklySTDHrs'],
        'Schedule': dag_run.conf['Schedule'],
        'PTOSeniorityDate': dag_run.conf['PTOSeniorityDate'],
        'ProfitCenter': dag_run.conf['ProfitCenter'],
        'ProfitCenterDescription': dag_run.conf['ProfitCenterDescription'],
        'CpnyCode': dag_run.conf['CpnyCode'],
        'PayGroupCode': dag_run.conf['PayGroupCode'],
        'PayGroup': dag_run.conf['PayGroup'],
        'PTO_1': dag_run.conf['PTO_1'],
        'PTO_Bereavement': dag_run.conf['PTO_Bereavement'],
        'PTO_JuryDuty': dag_run.conf['PTO_JuryDuty'],
        'HolidayType': dag_run.conf['HolidayType'],
        'Illness': dag_run.conf['Illness'],
        'ChangeEffectiveDate': dag_run.conf['ChangeEffectiveDate'],
        'VTO': dag_run.conf['VTO'],
        'EmergencySick': dag_run.conf['EmergencySick'],
        'PayRules': dag_run.conf['PayRules'],
        'TimesheetTemplate': dag_run.conf['TimesheetTemplate'],
        'TimeOffTemplate': dag_run.conf['TimeOffTemplate'],
        'HolidayCalendars': dag_run.conf['HolidayCalendars'],
        'TimeZone': dag_run.conf['TimeZone'],
        'WorkWeek': dag_run.conf['WorkWeek'],
        'PayrollGrouping': dag_run.conf['PayrollGrouping'],
        'LocationCode_Work': dag_run.conf['LocationCode_Work'],
        'Dept_Org4': dag_run.conf['Dept_Org4'],
        'Dept_Org4Desc': dag_run.conf['Dept_Org4Desc'],
        'CoreSupervisorID': dag_run.conf['CoreSupervisorID'],
        'CoreSupervisorName': dag_run.conf['CoreSupervisorName'],
        'LOASuspendPTOStart': dag_run.conf['LOASuspendPTOStart'],
        'LOASuspendPTOEnd': dag_run.conf['LOASuspendPTOEnd'],
        'PayrollPermission': dag_run.conf['PayrollPermission'],
        'AdminPermission': dag_run.conf['AdminPermission'],
        'ConditionRestrict': dag_run.conf['ConditionRestrict'],
        'PayrollGroupingGroups': dag_run.conf['PayrollGroupingGroups'],
        'ProfitCenterGroups': dag_run.conf['ProfitCenterGroups'],
        'AgencyGroups': dag_run.conf['AgencyGroups'],
        'PayGroupGroups': dag_run.conf['PayGroupGroups'],
        'LocationGroups': dag_run.conf['LocationGroups'],
        'DepartmentGroups': dag_run.conf['DepartmentGroups'],
        'AdditionalTimeOffTypes': dag_run.conf['AdditionalTimeOffTypes'],
        'RepliconTSDate': dag_run.conf['RepliconTSDate'],
        'DailyHours': dag_run.conf['DailyHours'],
        'illnesspto': dag_run.conf['illnesspto'],
        'activity': dag_run.conf['activity'],
        'makeuptimepto': dag_run.conf['makeuptimepto'],
        'punch_entry_policy': dag_run.conf['punch_entry_policy'],
        'AssignmentNumber': dag_run.conf['AssignmentNumber'],
        'assignmentnumber_udf_uri' : dag_run.conf['assignmentnumber_udf_uri'],
        'time_administrator_grouping_division_uri': dag_run.conf['time_administrator_grouping_division_uri'],
        'companyjobdata_udf_uri': dag_run.conf['companyjobdata_udf_uri'],
        'eetype_udf_uri': dag_run.conf['eetype_udf_uri'],
        'job_code_udf_uri': dag_run.conf['job_code_udf_uri'],
        'flsastatus_udf_uri': dag_run.conf['flsastatus_udf_uri'],
        'agencyorg2_udf_uri': dag_run.conf['agencyorg2_udf_uri'],
        'hourlyrate_udf_uri': dag_run.conf['hourlyrate_udf_uri'],
        'cpnycode_udf_uri': dag_run.conf['cpnycode_udf_uri'],
        'pay_group_code_udf_uri': dag_run.conf['pay_group_code_udf_uri'],
        'location_code_work_udf_uri': dag_run.conf['location_code_work_udf_uri'],
        'dept_org4_desc_udf_uri': dag_run.conf['dept_org4_desc_udf_uri'],
        'core_supervisorID_udf_uri': dag_run.conf['core_supervisorID_udf_uri'],
        'core_supervisor_name_udf_uri': dag_run.conf['core_supervisor_name_udf_uri'],
        'EEstatusuri': dag_run.conf['EEstatusuri'],
        'loastartdateuri': dag_run.conf['loastartdateuri'],
        'loaenddateuri': dag_run.conf['loaenddateuri'],
        'dailyhoursudfuri': dag_run.conf['dailyhoursudfuri'],
        'replicontsdateudfuri': dag_run.conf['replicontsdateudfuri'],
        'enddateudfuri': dag_run.conf['enddateudfuri'],
        'pto_seniority_date_udf_uri': dag_run.conf['pto_seniority_date_udf_uri'],
        'change_effective_date_udf_uri': dag_run.conf['change_effective_date_udf_uri'],
        'agency_org2_department_uri': dag_run.conf['agency_org2_department_uri'],
        'deptorg4desc_employeetype_uri': dag_run.conf['deptorg4desc_employeetype_uri'],
        'profitcenter_division_uri': dag_run.conf['profitcenter_division_uri'],
        'pay_group_code_location_uri': dag_run.conf['pay_group_code_location_uri'],
        'location_code_work_division_uri': dag_run.conf['location_code_work_division_uri'],
        'payroll_grouping_cost_center_uri': dag_run.conf['payroll_grouping_cost_center_uri'],
        'officeschedule_uri': dag_run.conf['officeschedule_uri'],
        'timezoneuri': dag_run.conf['timezoneuri'],
        'agency_grouping_department_uri': dag_run.conf['agency_grouping_department_uri'],
        'supervisor_assignment_log': dag_run.conf['supervisor_assignment_log'],
        'integration_run_date': dag_run.conf['integration_run_date'],
        'user_import_log': rail.result("create_user_log"),
    }

    if action == 'update':
        conf.update({"useruri": rail.result(
            'get_user_data_based_on_login_name')[0]['userDetails']['uri']})

    return conf


def get_department_data_payload():
    return {
        "page": "1",
        "pagesize": "1000000",
        "columnUris": [
            "urn:replicon:department-group-list-column:department-group",
            "urn:replicon:department-group-list-column:full-path"
        ],
        "sort": [],
        "filterExpression": null
    }


def search_supervisor_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
            "urn:replicon:user-list-column:employee-id",
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
                    "text": dag_run.conf['SupervisorID']
                }
            }
        }
    }


def payload_for_assigning_no_timesheet_template(dag_run):
    return {
        "user": {
            "uri": dag_run.conf['useruri']
        },
        "modifications": {
            "timesheetPeriodScheduleToApply": {
                "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementTimesheetPeriodSchedule": [],
                "updateTimesheetPeriodScheduleOverDateRange": {
                    "replacementTimesheetPeriodScheduleEntries": [{
                        "timesheetPeriod": null,
                        "effectiveDate": python_callable.get_split_date(
                            dag_run.conf['loastart'], 'int') if dag_run.conf['loastart'] else python_callable.get_split_date(dag_run.conf['integration_run_date'], 'int')
                    }]
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }
