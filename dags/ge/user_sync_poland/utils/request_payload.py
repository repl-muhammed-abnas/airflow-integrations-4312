import rail

null = None


def get_formated_user_row(item):
    return {
        "EmployeeFirstName": item["Employee First Name"].strip() if item["Employee First Name"] else "",
        "EmployeeLastName": item["Employee Last Name"].strip() if item["Employee Last Name"] else "",
        "EmployeeEmailAddress": item["Employee Email Address"].strip() if item["Employee Email Address"] else "",
        "OHRID": item["OHR ID"].strip() if item["OHR ID"] else "",
        "LegalEntityHireDate": item["Legal Entity Hire Date"].strip() if item["Legal Entity Hire Date"] else "",
        "LegacyPayrollID": item["Legacy Payroll ID"].strip() if item["Legacy Payroll ID"] else "",
        "Job/PositionTitle": item["Job/Position Title"].strip() if item["Job/Position Title"] else "",
        "SupervisorSSOID": item["Supervisor SSO ID"].strip() if item["Supervisor SSO ID"] else "",
        "SupervisorName": item["Supervisor Name"].strip() if item["Supervisor Name"] else "",
        "DWSStartDate": item["DWS Start Date"].strip() if item["DWS Start Date"] else "",
        "DWSMonday": item["DWS - Monday"].strip() if item["DWS - Monday"] else "",
        "DWSTuesday": item["DWS - Tuesday"].strip() if item["DWS - Tuesday"] else "",
        "DWSWednesday": item["DWS - Wednesday"].strip() if item["DWS - Wednesday"] else "",
        "DWSThursday": item["DWS - Thursday"].strip() if item["DWS - Thursday"] else "",
        "DWSFriday": item["DWS - Friday"].strip() if item["DWS - Friday"] else "",
        "DWSSaturday": item["DWS - Saturday"].strip() if item["DWS - Saturday"] else "",
        "DWSSunday": item["DWS - Sunday"].strip() if item["DWS - Sunday"] else "",
        "TerminationEffectiveDate": item["Termination Effective Date"].strip() if item["Termination Effective Date"] else "",
        "IndustryFocusGroup": item["Industry Focus Group"].strip() if item["Industry Focus Group"] else "",
        "LegalEntity": item["Legal Entity"].strip() if item["Legal Entity"] else "",
        "ContractID": item["Contract ID"].strip() if item["Contract ID"] else "",
        "ContractType": item["Contract Type"].strip() if item["Contract Type"] else "",
        "RadiationFlag": item["Radiation Flag"].strip() if item["Radiation Flag"] else "",
        "PositionCapacity": item["Position Capacity"].strip() if item["Position Capacity"] else "",
        "PreviousExperience": item["Previous Experience"].strip() if item["Previous Experience"] else "",
        "OvertimeEligibility": item["Overtime Eligibility"].strip() if item["Overtime Eligibility"] else "",
        "SuspendAssignmentCategory": item["Suspend Assignment Category"].strip() if item["Suspend Assignment Category"] else "",
        "Payroll": item["Payroll"].strip() if item["Payroll"] else "",
        "Healthcare Product Line EIT": item["Healthcare Product Line EIT"].strip() if item["Healthcare Product Line EIT"] else "",
        "JobType": item["Job Type"].strip() if item["Job Type"] else "",
        "CareerBand": item["Career Band"].strip() if item["Career Band"] else "",
        "AdjustedServiceDate": item["Adjusted Service Date"].strip() if item["Adjusted Service Date"] else "",
        "Work": item["Work"].strip() if item["Work"] else "",
        "HRMSSOID": item["HRM SSO ID"].strip() if item["HRM SSO ID"] else "",
        "HRMName": item["HRM Name"].strip() if item["HRM Name"] else "",
        "SpecialWorkSchedule": item["Special Work Schedule"].strip() if item["Special Work Schedule"] else "",
        "EducationLevel": item["Education Level"].strip() if item["Education Level"] else "",
        "WorkLocation": item["Work Location"].strip() if item["Work Location"] else "",
        "AssignmentEffectiveDate": item["Assignment Effective Date"].strip() if item["Assignment Effective Date"] else "",
        "HireEffectiveDate": item["Hire Effective Date"].strip() if item["Hire Effective Date"] else "",
        "RevTermEffectiveDate": item["Rev Term Effective Date"].strip() if item["Rev Term Effective Date"] else ""
    }.values()


def get_process_each_user_payload(item):
    return {
        "EmployeeFirstName": item['EmployeeFirstName'] if item['EmployeeFirstName'] else '',
        "EmployeeLastName": item['EmployeeLastName'] if item['EmployeeLastName'] else '',
        "EmployeeEmailAddress": item['EmployeeEmailAddress'] if item['EmployeeEmailAddress'] else '',
        "OHRID": item['OHRID'] if item['OHRID'] else '',
        "LegalEntityHireDate": item['LegalEntityHireDate'] if item['LegalEntityHireDate'] else '',
        "LegacyPayrollID": item['LegacyPayrollID'] if item['LegacyPayrollID'] else '',
        "SupervisorSSOID": item['SupervisorSSOID'] if item['SupervisorSSOID'] else '',
        "SupervisorName": item['SupervisorName'] if item['SupervisorName'] else '',
        "Job_PositionTitle": item['Job_PositionTitle'] if item['Job_PositionTitle'] else '',
        "DWSStartDate": item['DWSStartDate'] if item['DWSStartDate'] else '',
        "DWSMonday": item['DWSMonday'] if item['DWSMonday'] else '',
        "DWSTuesday": item['DWSTuesday'] if item['DWSTuesday'] else '',
        "DWSWednesday": item['DWSWednesday'] if item['DWSWednesday'] else '',
        "DWSThursday": item['DWSThursday'] if item['DWSThursday'] else '',
        "DWSFriday": item['DWSFriday'] if item['DWSFriday'] else '',
        "DWSSaturday": item['DWSSaturday'] if item['DWSSaturday'] else '',
        "DWSSunday": item['DWSSunday'] if item['DWSSunday'] else '',
        "TerminationEffectiveDate": item['TerminationEffectiveDate'] if item['TerminationEffectiveDate'] else '',
        "IndustryFocusGroup": item['IndustryFocusGroup'] if item['IndustryFocusGroup'] else '',
        "LegalEntity": item['LegalEntity'] if item['LegalEntity'] else '',
        "ContractID": item['ContractID'] if item['ContractID'] else '',
        "RadiationFlag": item['RadiationFlag'] if item['RadiationFlag'] else '',
        "PositionCapacity": item['PositionCapacity'] if item['PositionCapacity'] else '',
        "OvertimeEligibility": item['OvertimeEligibility'] if item['OvertimeEligibility'] else '',
        "SuspendAssignmentCategory": item['SuspendAssignmentCategory'] if item['SuspendAssignmentCategory'] else '',
        "Payroll": item['Payroll'] if item['Payroll'] else '',
        "HealthcareProductLineEIT": item['HealthcareProductLineEIT'] if item['HealthcareProductLineEIT'] else '',
        "JobType": item['JobType'] if item['JobType'] else '',
        "CareerBand": item['CareerBand'] if item['CareerBand'] else '',
        "AdjustedServiceDate": item['AdjustedServiceDate'] if item['AdjustedServiceDate'] else '',
        "Work": item['Work'] if item['Work'] else '',
        "HRMSSOID": item['HRMSSOID'] if item['HRMSSOID'] else '',
        "HRMName": item['HRMName'] if item['HRMName'] else '',
        "SpecialWorkSchedule": item['SpecialWorkSchedule'] if item['SpecialWorkSchedule'] else '',
        "EducationLevel": item['EducationLevel'] if item['EducationLevel'] else '',
        "ContractType": item["ContractType"] if item["ContractType"] else '',
        "LocationName": item['WorkLocation'] if item['WorkLocation'] else '',
        "AssignmentEffectiveDate": item['AssignmentEffectiveDate'] if item['AssignmentEffectiveDate'] else '',
        "HireEffectiveDate": item['HireEffectiveDate'] if item['HireEffectiveDate'] else '',
        "RevTermEffectiveDate": item['RevTermEffectiveDate'] if item['RevTermEffectiveDate'] else '',
        "PreviousExperience": item['PreviousExperience'] if item['PreviousExperience'] else '',
        "service_center_uri": rail.find_first_by_attr_and_get_attr(rail.result(
            'get_all_service_centers'), 'fullpath', (item['LegacyPayrollID'] + '/' + item['HRMSSOID']), 'uri', ''),
        "Departmenturi": rail.result('log_required_department_uri'),
        "supervisor_log": rail.result('supervisor_log'),
        "integration_run_date": rail.result('log_integration_run_date')
    }


def get_add_update_dag_conf(dag_run, action):
    conf = {
        'EmployeeFirstName': dag_run.conf['EmployeeFirstName'],
        'EmployeeLastName': dag_run.conf['EmployeeLastName'],
        'EmployeeEmailAddress': dag_run.conf['EmployeeEmailAddress'],
        'OHRID': dag_run.conf['OHRID'],
        'LegalEntityHireDate': dag_run.conf['LegalEntityHireDate'],
        'LegacyPayrollID': dag_run.conf['LegacyPayrollID'],
        "EmployeeGender": '',
        "MaritalStatus": '',
        'JobPositionTitle': dag_run.conf['Job_PositionTitle'],
        'SupervisorSSOID': dag_run.conf['SupervisorSSOID'],
        'SupervisorName': dag_run.conf['SupervisorName'],
        'AssignmentCategory': dag_run.conf['SuspendAssignmentCategory'],
        'DWSStartDate': dag_run.conf['DWSStartDate'],
        'DWSEndDate': '',
        'DWSMonday': dag_run.conf['DWSMonday'],
        'DWSTuesday': dag_run.conf['DWSTuesday'],
        'DWSWednesday': dag_run.conf['DWSWednesday'],
        'DWSThursday': dag_run.conf['DWSThursday'],
        'DWSFriday': dag_run.conf['DWSFriday'],
        'DWSSaturday': dag_run.conf['DWSSaturday'],
        'DWSSunday': dag_run.conf['DWSSunday'],
        'TerminationEffectiveDate': dag_run.conf['TerminationEffectiveDate'],
        'IndustryFocusGroup': dag_run.conf['IndustryFocusGroup'],
        'LegalEntity': dag_run.conf['LegalEntity'],
        'ContractID': dag_run.conf['ContractID'],
        'RadiationFlag': dag_run.conf['RadiationFlag'],
        'PositionCapacity': dag_run.conf['PositionCapacity'],
        'EducationPeriods_StartDate': '',
        'EducationPeriods_EndDate': '',
        'PreviousEmploymentsPeriods_StartDate': '',
        'PreviousEmploymentsPeriodsEndDate': '',
        'Department_Alstom': '',
        'Salary_Basis': '',
        'OvertimeEligibility': dag_run.conf['OvertimeEligibility'],
        'SuspendAssignmentCategory': dag_run.conf['SuspendAssignmentCategory'],
        'DateofBirth': '',
        'Payroll': dag_run.conf['Payroll'],
        'HealthcareProductLineEIT': dag_run.conf['HealthcareProductLineEIT'],
        'JobType': dag_run.conf['JobType'],
        'CareerBand': dag_run.conf['CareerBand'],
        'AdjustedServiceDate': dag_run.conf['AdjustedServiceDate'],
        'Work': dag_run.conf['Work'],
        'HRMSSOID': dag_run.conf['HRMSSOID'],
        'HRMName': dag_run.conf['HRMName'],
        'SpecialWorkSchedule': dag_run.conf['SpecialWorkSchedule'],
        'EducationLevel': dag_run.conf['EducationLevel'],
        'Sub_Biz': '',
        'LocationName': dag_run.conf['LocationName'],
        'AssignmentEffectiveDate': dag_run.conf['AssignmentEffectiveDate'],
        'HireEffectiveDate': dag_run.conf['HireEffectiveDate'],
        'RevTermEffectiveDate': dag_run.conf['RevTermEffectiveDate'],
        'Departmenturi': dag_run.conf['Departmenturi'],
        'ContractType': dag_run.conf['ContractType'],
        'supervisor_log': dag_run.conf['supervisor_log'],
        'integration_run_date': dag_run.conf['integration_run_date'],
        'user_import_log': rail.result("create_user_log"),
    }

    if action == 'update':
        conf.update({
            'useruri': rail.result(
                'get_user_data_based_on_login_name')[0]['userDetails']['uri'],
            'WorktimeSystem': '',
            'ContractattributeAnnualvacationeligibility': '',
            'type': 'Update',
            'PreviousExperience': dag_run.conf['PreviousExperience'],
            'legacypayroll_service_center_uri': dag_run.conf['service_center_uri']
        })

    if action == 'add':
        conf.update({
            'type': 'Add',
            'previousemployment': dag_run.conf['PreviousExperience'],
            'servicecenteruri_hrmssoid': dag_run.conf['service_center_uri']
        })

    if action == 'rehire_add':
        conf.update({
            'type': 'Rehire',
            'previousemployment': dag_run.conf['PreviousExperience'],
            'servicecenteruri_hrmssoid': dag_run.conf['service_center_uri']
        })

    return conf


def get_user_timeoff_booking_details_payload(date_default_format, dag_run):
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
            "urn:replicon:time-off-list-column:total-effective-hours"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": null,
                    "filterDefinitionUri": "urn:replicon:time-off-list-filter:time-off-owner"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": {
                        "uri": dag_run.conf['useruri']
                    },
                    "filterDefinitionUri": null
                },
                "value": null,
                "filterDefinitionUri": null
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:time-off-list-filter:time-off-type"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": dag_run.conf['timeoffuri']
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                },
                "operatorUri": "urn:replicon:filter-operator:and",
                "rightExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:time-off-list-filter:time-off-date-range"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "dateRange": {
                                "startDate": rail.parse_date(rail.result('get_past_timeoff_policy_lines_and_required_date_16_29')['accrual_date'], '%d/%B/%Y'),
                                "endDate": rail.parse_date(dag_run.conf['disabledate'], date_default_format)
                            }
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                },
                "value": null,
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }
