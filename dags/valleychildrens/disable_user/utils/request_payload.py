from datetime import datetime
import json


def get_user_payload(dag_run):
    return {
        "userUri": dag_run.conf['item']['UserUri']
    }


def update_timesheet_period(dag_run):
    end_date = datetime.strptime(
        dag_run.conf['item']['User_End_Date'].split('T')[0].strip(), '%b %d, %Y')
    return {
        "user": {
            "uri": dag_run.conf['item']['UserUri']
        },
        "modifications": {
            "timesheetPeriodScheduleToApply": {
                "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementTimesheetPeriodSchedule": [],
                "updateTimesheetPeriodScheduleOverDateRange": {
                    "replacementTimesheetPeriodScheduleEntries": [
                        {
                            "timesheetPeriod": {
                                "name": "No timesheet period"
                            },
                            "effectiveDate": {
                                "year": end_date.year,
                                "month": end_date.month,
                                "day": end_date.day
                            }
                        }
                    ]
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def update_employee_type(dag_run):
    end_date = datetime.strptime(
        dag_run.conf['item']['User_End_Date'].split('T')[0].strip(), '%b %d, %Y')
    return {
        "user": {
            "uri": dag_run.conf['item']['UserUri']
        },
        "modifications": {
            "employeeTypeGroupScheduleToApply": {
                "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementEmployeeTypeGroupSchedule": [],
                "updateEmployeeTypeGroupScheduleOverDateRange": {
                    "replacementEmployeeTypeGroupScheduleEntries": [
                        {
                            "employeeTypeGroup": {
                                "name": "Term - Final"
                            },
                            "effectiveDate": {
                                "year": end_date.year,
                                "month": end_date.month,
                                "day": end_date.day
                            }
                        }
                    ]
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def put_time_off_policy(dag_run):
    end_date = datetime.strptime(
        dag_run.conf['item']['Terminationdate'].split('T')[0].strip(), '%b %d, %Y')
    effective_date = datetime.strptime(
        str(dag_run.conf['item']['Policyset'][0]['effectiveDate']['day'])+"/" +
        str(dag_run.conf['item']['Policyset'][0]['effectiveDate']['month'])+"/" +
        str(dag_run.conf['item']['Policyset'][0]['effectiveDate']['year']), '%d/%m/%Y')
    if effective_date < end_date:
        policy_data = [{
            "effectiveDate": {
                "day": dag_run.conf['item']['Policyset'][0]['effectiveDate']['day'],
                "month": dag_run.conf['item']['Policyset'][0]['effectiveDate']['month'],
                "year": dag_run.conf['item']['Policyset'][0]['effectiveDate']['year']
            },
            "description": dag_run.conf['item']['Policyset'][0]['description'],
            "policySet": dag_run.conf['item']['Policyset'][0]['policySet']
        },
            {
            "effectiveDate": {
                "year": end_date.year,
                "month": end_date.month,
                "day": end_date.day
            },
		"description": "Added by integration on "+str(end_date.day)+"-"+str(end_date.month)+"-"+str(end_date.year)+"",
		"policySet": {
                    "timeOffBalanceEventScripts": [],
             			"timeOffValidationScripts": []
            }
	}
        ]
    else:
        policy_data = [
            {
                "effectiveDate": {
                    "year": end_date.year,
                    "month": end_date.month,
                    "day": end_date.day
                },
		"description": "Added by integration on "+str(end_date.day)+"-"+str(end_date.month)+"-"+str(end_date.year)+"",
		"policySet": {
                    "timeOffBalanceEventScripts": [],
                 			"timeOffValidationScripts": []
                }
            }
        ]

    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['item']['Useruri'],
            "timeOffTypeUri": dag_run.conf['item']['Timeoffuri']
        },
        "policySetScheduleEntries": json.loads(json.dumps(policy_data).replace("null", '"effective"')
                                               .replace('"script"', '"scriptTarget"')
                                               .replace('":{"additionalParameters', '":[{"additionalParameters')
                                               .replace(':{"keyUri"', ':[{"keyUri"').replace('}},"scriptTarget"', '}}],"scriptTarget"')
                                               .replace('}},"timeOffValidationScripts', '}}],"timeOffValidationScripts')
                                               .replace('}}},"description', '}}]},"description'))
    }
