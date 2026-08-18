import rail
from datetime import datetime
from pendulum import now
import json
from momentive.user_import_japan.utils import python_callable

null = None


def get_user_by_search_payload(text_search_term):
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
            "urn:replicon:user-list-column:login-name",
            "urn:replicon:user-list-column:end-date",
            "urn:replicon:user-list-column:start-date",
            "urn:replicon:user-list-column:enabled",
            "urn:replicon:user-list-column:employee-type"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:user-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": text_search_term
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def conf_payload(action):
    conf = {
        "parentjobid": rail.render_template("{{ dag_run_ecid() }}"),
        "userid": rail.result('foreach_query_list_usershereloginnameispresent_22_31')['User_ID'],
        "Worker_Reference_Employee_ID": rail.result('foreach_query_list_usershereloginnameispresent_22_31')['Worker_Reference_Employee_ID'],
        "emailaddress": rail.result('foreach_query_list_usershereloginnameispresent_22_31')['Email_Address'],
        "firstname": rail.result('foreach_query_list_usershereloginnameispresent_22_31')['First_Name'],
        "lastname": rail.result('foreach_query_list_usershereloginnameispresent_22_31')['Last_Name'],
        "workertype": rail.result('foreach_query_list_usershereloginnameispresent_22_31')['Worker_Type'],
        "effective_date_of_worker_type": rail.result('foreach_query_list_usershereloginnameispresent_22_31')['Effective_Date_of_Worker_Type'] if rail.result('foreach_query_list_usershereloginnameispresent_22_31')['Effective_Date_of_Worker_Type'] else null,
        "exemptionstatus": "Yes" if "1" in str(rail.result('foreach_query_list_usershereloginnameispresent_22_31')['Exemption_Status']) else "No",
        "exemption_eff_date": rail.result('foreach_query_list_usershereloginnameispresent_22_31')['Exemption_Eff_Date'] if rail.result('foreach_query_list_usershereloginnameispresent_22_31')['Exemption_Eff_Date'] else null,
        "gender": rail.result('foreach_query_list_usershereloginnameispresent_22_31')['Gender'],
        "hiredate": rail.result('foreach_query_list_usershereloginnameispresent_22_31')['Hire_Date'] if rail.result('foreach_query_list_usershereloginnameispresent_22_31')['Hire_Date'] else null,
        "terminationdate": rail.result('foreach_query_list_usershereloginnameispresent_22_31')['Termination_Date'] if rail.result('foreach_query_list_usershereloginnameispresent_22_31')['Termination_Date'] else null,
        "active": rail.result('foreach_query_list_usershereloginnameispresent_22_31')['Active'],
        "function": rail.result('foreach_query_list_usershereloginnameispresent_22_31')['Function'],
        "function_change_effective_date": rail.result('foreach_query_list_usershereloginnameispresent_22_31')['Function_Change_Effective_Date'] if rail.result('foreach_query_list_usershereloginnameispresent_22_31')['Function_Change_Effective_Date'] else null,
        "businesstitle": rail.result('foreach_query_list_usershereloginnameispresent_22_31')['Business_Title'] or null,
        "CF_LRV_Business_Title_Change_Eff_Date": rail.result('foreach_query_list_usershereloginnameispresent_22_31')['CF_LRV_Business_Title_Change_Eff_Date'] if rail.result('foreach_query_list_usershereloginnameispresent_22_31')['CF_LRV_Business_Title_Change_Eff_Date'] else null,
        "fieldhr": rail.result('foreach_query_list_usershereloginnameispresent_22_31')['Field_HR'],
        "manager_id": rail.result('foreach_query_list_usershereloginnameispresent_22_31')['Manager_ID'],
        "effective_date_of_manager_change": rail.result('foreach_query_list_usershereloginnameispresent_22_31')['Effective_Date_of_Manager_Change'] if rail.result('foreach_query_list_usershereloginnameispresent_22_31')['Effective_Date_of_Manager_Change'] else null,
        "workshift": rail.result('foreach_query_list_usershereloginnameispresent_22_31')['Work_Shift'],
        "work_shift_change_effective_date": rail.result('foreach_query_list_usershereloginnameispresent_22_31')['Work_Shift_Change_Effective_Date'] if rail.result('foreach_query_list_usershereloginnameispresent_22_31')['Work_Shift_Change_Effective_Date'] else null,
        "location": rail.result('foreach_query_list_usershereloginnameispresent_22_31')['Location'],
        "CF_LRV_Location_Change_Effective_Date": rail.result('foreach_query_list_usershereloginnameispresent_22_31')['CF_LRV_Location_Change_Effective_Date'] if rail.result('foreach_query_list_usershereloginnameispresent_22_31')['CF_LRV_Location_Change_Effective_Date'] else null,
        "country": rail.result('foreach_query_list_usershereloginnameispresent_22_31')['Country'],
        "CF_Date_of_Birth_MM_DD_YYYY": rail.result('foreach_query_list_usershereloginnameispresent_22_31')['CF_Date_of_Birth_MM_DD_YYYY'] if rail.result('foreach_query_list_usershereloginnameispresent_22_31')['CF_Date_of_Birth_MM_DD_YYYY'] else null,
        "CF_LRV_Manager_Email": rail.result('foreach_query_list_usershereloginnameispresent_22_31')['CF_LRV_Manager_Email'],
        "CF_LRV_Manager_First_Name": rail.result('foreach_query_list_usershereloginnameispresent_22_31')['CF_LRV_Manager_First_Name'],
        "CF_LRV_Manager_Last_Name": rail.result('foreach_query_list_usershereloginnameispresent_22_31')['CF_LRV_Manager_Last_Name'],
        "legalentity": rail.result('foreach_query_list_usershereloginnameispresent_22_31')['Legal_entity'],
        "worker_subtype": rail.result('foreach_query_list_usershereloginnameispresent_22_31')['Worker_subType'] or null,
        "costcenter": rail.result('foreach_query_list_usershereloginnameispresent_22_31')['Cost_center'],
        "eff_date_cost_center": rail.result('foreach_query_list_usershereloginnameispresent_22_31')['Worker_cc_change_date'],
        "years_of_service": rail.result('foreach_query_list_usershereloginnameispresent_22_31')['Year_of_service'],
        "paygroup": rail.result('foreach_query_list_usershereloginnameispresent_22_31')['Paygroup'],
        "continous_service_date": rail.result('foreach_query_list_usershereloginnameispresent_22_31')['continous_service_date'],
        "timeoff_service_date": rail.result('foreach_query_list_usershereloginnameispresent_22_31')['timeoff_service_date'],
        "departmentgroupuri": rail.result('log_ifuserexistsuseruri_and_departmentgroupuri_36_37')['departmentgroupuri'],
        "legalentityuri": rail.result('log_legalentity_paygroup_and_costcenter_uris_38_39_40')['legalentityuri'],
        "paygroupuri": rail.result('log_legalentity_paygroup_and_costcenter_uris_38_39_40')['paygroupuri'],
        "costcenteruri": rail.result('log_legalentity_paygroup_and_costcenter_uris_38_39_40')['costcenteruri'],
        "Japan_flag": "Yes" if "1" in str(rail.result('foreach_query_list_usershereloginnameispresent_22_31')['Japan_special_schedule_flag']) else "No",
        "user_import_logs": rail.result('create_log_momentive_user_import_log'),
        "supervisor_assignment_logs": rail.result('create_log_momentive_supervisor_assignment'),
        "supervisor_restriction_logs": rail.result('create_log_momentive_supervisor_restriction'),
        "date_of_birth_uri": rail.result('get_required_user_customfields')['date_of_birth_uri'],
        "title_uri": rail.result('get_required_user_customfields')['title_uri'],
        "worker_subtypeuri": rail.result('get_required_user_customfields')['workersubtypeuri'],
        "years_of_service_uri": rail.result('get_required_user_customfields')['years_of_service_uri'],
        "hrm_uri": rail.result('get_required_user_customfields')['hrm_uri'],
        "continous_years_of_service_uri": rail.result('get_required_user_customfields')['continous_years_of_service_uri'],
        "timeoff_service_date_uri": rail.result('get_required_user_customfields')['timeoffservicedate_uri'],
        "gender_uri": rail.result('get_required_user_customfields')['gender_uri'],
        "function_uri": rail.result('get_required_user_customfields')['function_uri'],
        "workshift_uri": rail.result('get_required_user_customfields')['workshift_uri'],
        "workertype_uri": rail.result('get_required_user_customfields')['workertype_uri'],
        "basic_user_with_report_uri": rail.result('get_all_permissionsets')['basic_user_with_report_uri'],
        "supervisor": rail.result('get_all_permissionsets')['supervisor'],
        "schedule_manager": rail.result('get_all_permissionsets')['schedule_manager']
    }

    if action == 'rehire':
        conf.update({"rehire_update": "rehire", "useruri": rail.result(
            'log_ifuserexistsuseruri_and_departmentgroupuri_36_37')['useruri']})
    elif action == 'update' or action == 'disable':
        conf.update({"rehire_update": "update", "useruri": rail.result(
            'log_ifuserexistsuseruri_and_departmentgroupuri_36_37')['useruri']})
    elif action == 'disablewithenddate':
        conf.update({"rehire_update": "update", "useruri": rail.result(
            'log_ifuserexistsuseruri_and_departmentgroupuri_36_37')['useruri'], "terminationdate": rail.result('log_enddatepresent_and_userstatus_42_43')['enddatepresent']})

    return conf


def search_supervisor_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:user-list-column:employee-id",
            "urn:replicon:user-list-column:login-name"
        ],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:user-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": dag_run.conf['manager_id']
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }

def seachfor_supervisor_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:user-list-column:employee-id",
            "urn:replicon:user-list-column:login-name"
        ],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:user-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": dag_run.conf['supervisorloginname']
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }

def add_missing_supervisor_permission_payload(dag_run):
    return {
        'userUri': rail.result('search_for_user_with_empid_167')[0]['uri'],
        'permissionSetUri': dag_run.conf['supervisor']
    }

def get_formated_user_row(item):
    return {
        'useruri': item['useruri'],
        'useremail': item['User Email'],
        'department': item['Department (Current)']
    }.values()

def add_missing_supervisor_permission_payload_2(dag_run):
    return {
        'userUri': rail.result('search_for_user_with_empid')[0]['uri'],
        'permissionSetUri': dag_run.conf['supervisor']
    }

def supervisor_assignment_log_payload(dag_run):
    return {
        # Master's fan-out (FilterLogEntriesOperator) matches on property
        # 'parentjobid' — the key name MUST be parentjobid, not jobid, or the
        # master finds no entries and never triggers the supervisor-assignment DAG.
        "parentjobid": dag_run.conf['parentjobid'],
        "childjobid": rail.render_template("{{ dag_run_ecid() }}"),
        "loginid": dag_run.conf['userid'],
        "supervisorempid": dag_run.conf['manager_id'],
        "useruri": dag_run.conf['useruri'] if 'useruri' in dag_run.conf else rail.result('create_user_105')['uri'],
        'type': "update" if 'useruri' in dag_run.conf else "add",
        "sup_email": dag_run.conf['CF_LRV_Manager_Email'] if dag_run.conf['CF_LRV_Manager_Email'] else '',
        "sup_firstname": dag_run.conf['CF_LRV_Manager_First_Name'] if dag_run.conf['CF_LRV_Manager_First_Name'] else '',
        "sup_lastname": dag_run.conf['CF_LRV_Manager_Last_Name'] if dag_run.conf['CF_LRV_Manager_Last_Name'] else '',
        "sup_change_effective_date": dag_run.conf['effective_date_of_manager_change']
        if dag_run.conf['effective_date_of_manager_change'] else str(datetime.strftime(datetime.now().date(), '%Y-%m-%d')),
    }


def trigger_timeoff_add_new_user(dag_run):
    return {
        "parentjobid": dag_run.conf['parentjobid'],
        "firstname": dag_run.conf['firstname'],
        "lastname": dag_run.conf['lastname'],
        "loginname": dag_run.conf['userid'],
        "employeeid": dag_run.conf['Worker_Reference_Employee_ID'],
        "supervisor": dag_run.conf['manager_id'],
        "emailaddress": dag_run.conf['emailaddress'],
        "startdate": dag_run.conf['hiredate'],
        "useruri": rail.result('create_user_105')['uri'],
        "terminationdate": dag_run.conf['terminationdate'],
        "workertype": dag_run.conf['workertype'],
        "effective_date_of_worker_type": dag_run.conf['effective_date_of_worker_type'],
        "exemptionstatus": dag_run.conf['exemptionstatus'],
        "exemption_eff_date": dag_run.conf['exemption_eff_date'],
        "gender": dag_run.conf['gender'],
        "active": dag_run.conf['active'],
        "function": dag_run.conf['function'],
        "function_change_effective_date": dag_run.conf['function_change_effective_date'],
        "businesstitle": dag_run.conf['businesstitle'],
        "businesstitle_change_effective_date": dag_run.conf['CF_LRV_Business_Title_Change_Eff_Date'],
        "fieldhr": dag_run.conf['fieldhr'],
        "workshift": dag_run.conf['workshift'], 
        "work_shift_change_effective_date": dag_run.conf['work_shift_change_effective_date'],
        "location": dag_run.conf['location'],
        "location_change_effective_date": dag_run.conf['CF_LRV_Location_Change_Effective_Date'],
        "birthdate": dag_run.conf['CF_Date_of_Birth_MM_DD_YYYY'],
        "sup_email": dag_run.conf['CF_LRV_Manager_Email'],
        "sup_firstname": dag_run.conf['CF_LRV_Manager_First_Name'],
        "sup_lastname": dag_run.conf['CF_LRV_Manager_Last_Name'],
        "continous_service_date": dag_run.conf['continous_service_date'],
        "timeoff_service_date": dag_run.conf['timeoff_service_date'],
        "timeofftypes": rail.result('log_timofftypes_tobeassigned_206'),
        "rehire": 'add'
    }

def get_data_sup_emp_grp_dept_grp(dag_run):
    return {
  "page": "1",
  "pagesize": "100000",
  "columnUris": [
    "urn:replicon:user-list-column:department-group",
    "urn:replicon:user-list-column:employee-type-group",
    "urn:replicon:user-list-column:supervisor"
  ],
  "sort": [],
  "filterExpression": {
    "leftExpression": {
      "leftExpression": null,
      "operatorUri": null,
      "rightExpression": null,
      "value": null,
      "filterDefinitionUri": "urn:replicon:user-list-filter:user"
    },
    "operatorUri": "urn:replicon:filter-operator:equal",
    "rightExpression": {
      "leftExpression": null,
      "operatorUri": null,
      "rightExpression": null,
      "value": {
        "uri": dag_run.conf['useruri'],
        "uris": [],
        "bool": null,
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


def get_manager_details_payload():
    return {
        "users": [
            {
                "uri": rail.result('search_for_user_with_empid')[0]['uri']
            }
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }

def get_timesheet_for_date2_payload(dag_run):
    return {
        "userUri": dag_run.conf['useruri'],
        "date": python_callable.split_date_string(datetime.now().strftime('%Y-%m-%d')),
        "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
    }

def update_employeetypegrp_payload(dag_run):
    return {
  "user": {
    "uri": dag_run.conf['useruri'],
    "loginName": null,
    "parameterCorrelationId": null
  },
  "modifications": {
  "employeeTypeGroupScheduleToApply": {
      "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
      "replacementEmployeeTypeGroupSchedule": [],
      "updateEmployeeTypeGroupScheduleOverDateRange": {
        "replacementEmployeeTypeGroupScheduleEntries": [
          {
            "employeeTypeGroup": {
              "uri": rail.result('get_all_employee_type_details'),
              "parent": null,
              "name": null,
              "parameterCorrelationId": null
            },
            "effectiveDate":  python_callable.split_date_string(rail.result('get_startdate_of_next_timesheet'))
          }
        ],
        "endDate": null
      }
    },
    "projectRolesToApply": null
  },
  "userModificationOptionUri": "urn:replicon:user-modification-option:save"
}

def update_service_center_payload(dag_run):
    return {
  "user": {
    "uri": dag_run.conf['useruri'],
    "loginName": null,
    "parameterCorrelationId": null
  },
  "modifications": {
  "serviceCenterScheduleToApply": {
      "userServiceCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
      "replacementServiceCenterSchedule": [],
      "updateServiceCenterScheduleOverDateRange": {
        "replacementServiceCenterScheduleEntries": [
          {
            "serviceCenter": {
              "uri": dag_run.conf['paygroupuri'],
              "parent": null,
              "name": null,
              "parameterCorrelationId": null
            },
            "effectiveDate": python_callable.split_date_string(datetime.now().strftime('%Y-%m-%d'))
          }
        ],
        "endDate": null
   }
    }
    },
  "userModificationOptionUri": "urn:replicon:user-modification-option:save"
}

def get_costcenter_group_data_payload(dag_run):
    return {
  "page": "1",
  "pagesize": "100000",
  "columnUris": [
    "urn:replicon:cost-center-list-column:cost-center",
    "urn:replicon:cost-center-list-column:full-path"
  ],
  "sort": [],
  "filterExpression": {
    "leftExpression": {
      "leftExpression": null,
      "operatorUri": null,
      "rightExpression": null,
      "value": null,
      "filterDefinitionUri": "urn:replicon:cost-center-list-filter:text"
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
        "text": dag_run.conf['costcenter'],
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

def update_costcenter_group(dag_run):
    return {
  "user": {
    "uri": dag_run.conf['useruri'],
    "loginName": null,
    "parameterCorrelationId": null
  },
  "modifications": {
  "costCenterScheduleToApply": {
      "userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
      "replacementCostCenterSchedule": [],
      "updateCostCenterScheduleOverDateRange": {
        "replacementCostCenterScheduleEntries": [
          {
            "costCenter": {
              "uri":  dag_run.conf['costcenteruri'],
              "parent": null,
              "name": null,
              "parameterCorrelationId": null
            },
            "effectiveDate": rail.result('log_costcenter_changeeffdate')
          }
        ],
        "endDate": null
     }
    }
    },
  "userModificationOptionUri": "urn:replicon:user-modification-option:save"
}

def get_division_group_data_payload(dag_run):
    return {
  "page": "1",
  "pagesize": "100000",
  "columnUris": [
    "urn:replicon:division-list-column:division",
    "urn:replicon:division-list-column:full-path"
  ],
  "sort": [],
  "filterExpression": {
    "leftExpression": {
      "leftExpression": null,
      "operatorUri": null,
      "rightExpression": null,
      "value": null,
      "filterDefinitionUri": "urn:replicon:division-list-filter:text"
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
        "text": dag_run.conf['legalentity'],
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


def update_division_group_payload(dag_run):
    return {
  "user": {
    "uri": dag_run.conf['useruri'],
    "loginName": null,
    "parameterCorrelationId": null
  },
  "modifications": {
  "divisionScheduleToApply": {
      "userDivisionScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
      "replacementDivisionSchedule": [],
      "updateDivisionScheduleOverDateRange": {
        "replacementDivisionScheduleEntries": [
          {
            "division": {
              "uri": dag_run.conf['legalentityuri'],
              "parent": null,
              "name": null,
              "parameterCorrelationId": null
            },
            "effectiveDate": python_callable.split_date_string(datetime.now().strftime('%Y-%m-%d'))
          }
        ],
        "endDate": null
        }
    }
    },
  "userModificationOptionUri": "urn:replicon:user-modification-option:save"
}

def update_department_group_payload(dag_run):
    return {
  "user": {
    "uri": dag_run.conf['useruri'],
    "loginName": null,
    "parameterCorrelationId": null
  },
  "modifications":{
  "departmentGroupScheduleToApply": {
      "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
      "replacementDepartmentGroupSchedule": [],
      "updateDepartmentGroupScheduleOverDateRange": {
        "replacementDepartmentGroupScheduleEntries": [
          {
            "departmentGroup": {
              "uri": dag_run.conf['departmentgroupuri'],
              "parent": null,
              "name": null,
              "parameterCorrelationId": null
            },
            "effectiveDate": rail.result('log_location_change_eff_date')
          }
        ],
        "endDate": null
      }
    }
    },
  "userModificationOptionUri": "urn:replicon:user-modification-option:save"
}

def update_payrule_for_user_payload(dag_run):
    return {
  "user": {
    "uri": dag_run.conf['useruri'],
    "loginName": null,
    "employeeId": null,
    "parameterCorrelationId": null
  },
  "modifications": {
    "timezoneToApply": null,
    "workWeekStartToApply": null,
    "holidayCalendarToApply": null,
    "holidayCalendarAssignmentsToApply": null,
    "schedulePolicyToApply": null,
    "locationScheduleToApply": null,
    "divisionScheduleToApply": null,
    "costCenterScheduleToApply": null,
    "departmentGroupScheduleToApply": null,
    "employeeTypeGroupScheduleToApply": null,
    "timesheetPeriodScheduleToApply": null,
    "serviceCenterScheduleToApply": null,
    "totalBusinessCostScheduleToApply": null,
    "permissionSetsToApply": null,
    "policySetsToApply": null,
    "policySetsScheduleToApply": [],
    "policyDataAccessScopesToApply": null,
    "policyDataAccessScopesToApply2": null,
    "notificationPreferencesToApply": null,
    "timesheetPeriodTypeToApply": null,
    "timesheetApprovalPathToApply": null,
    "timeEntryRevisionGroupApprovalPathToApply": null,
    "validationRuleToApply": null,
    "activitiesToApply": [],
    "activitiesToApply2": null,
    "defaultActivityToApply": null,
    "defaultActivityToApply2": null,
    "defaultTimeOffTypeForBookingsToApply": null,
    "expenseApprovalPathToApply": null,
    "expenseDefaultReimbursementCurrencyToApply": null,
    "timeOffApprovalPathToApply": null,
    "productAssignmentsToApply": null,
    "timeBankPolicyToApply": null,
    "securitySettingsToApply": null,
    "supervisorsToApply": null,
    "supervisorsModifications": null,
    "payrollRatesToApply": null,
    "payrollRatesModifications": null,
    "overtimeRulesToApply": null,
    "overtimeRulesModifications": null,
    "customFieldValuesToApply": [],
    "departmentToApply": null,
    "employeeTypeToApply": null,
    "userDetailsToApply": null,
    "payRulesToApply": null,
    "payRulesScheduleModifications": {
      "scheduleEntries": [
        {
          "payRuleScript": {
            "uri": rail.result('get_req_payrule_script'),
            "name": null
          },
          "effectiveDate": python_callable.split_date_string(rail.result('get_startdate_of_next_timesheet'))
        }
      ]
    },
    "payRatesModifications": null,
    "placeAssignmentsModifications": null,
    "resourceAllocationAfterUserEndDateOptionUri": null,
    "projectRolesToApply": null,
    "projectRoleAssignmentSchedulesToApply": null,
    "decimalSeparatorToApply": null,
    "numberGroupSeparatorToApply": null,
    "dateFormatToApply": null,
    "clockFormatToApply": null,
    "hoursFormatToApply": null,
    "timeZoneFormatToApply": null,
    "objectExtensionFieldsToApply": [],
    "costRateScheduleModifications": null,
    "workAuthorizationApprovalPathToApply": null,
    "displayNameFormatSettingsToApply": null,
    "timePunchTimeZoneDisplayOptionToApply": null,
    "defaultTimesheetToDisplayOptionToApply": null,
    "reportSettingsToApply": null,
    "timeOffBalancePayoutApprovalPathToApply": null,
    "workCompliancePolicyAssignmentScheduleToApply": null,
    "userConsentModificationsToApply": null
  },
  "userModificationOptionUri": "urn:replicon:user-modification-option:save"
}


def dict_to_datetime(dict_date):
    """Convert dictionary with day, month, year keys to datetime object."""
    return datetime(day=dict_date['day'], month=dict_date['month'], year=dict_date['year'])


def get_current_value_from_schedule_list_for_user(user_schedule, scrpit_name, required_key):
    """
    Extract current active value from a schedule list using day-difference selection.
    
    Algorithm:
    1. Iterate schedule entries
    2. Capture initial entry (null effectiveDate) for fallback
    3. For each dated entry:
       a. Calculate day_diff = today - effectiveDate
       b. Ignore negative day_diffs (future entries)
       c. Track entry with smallest day_diff
    4. Return required_key from current entry, or fallback to initial, or empty string
    
    Returns:
    - Current active entry's required_key value (e.g., displayText of pay-rule)
    - If no current entry, returns initial entry's value
    - If no valid entries, returns empty string
    """
    current_value = null
    initial_value = null
    current_min_day_diff = "*"
    
    if 'urn' in json.dumps(user_schedule):
        for item in user_schedule:
            # Capture initial value (null effectiveDate)
            if not item['effectiveDate']:
                initial_value = item
                continue

            # Calculate days between entry effective date and today
            daydiff = (now().date()) - dict_to_datetime(item['effectiveDate']).date()

            # Skip future entries (day-diff < 0)
            if daydiff.days < 0:
                continue

            # First dated entry becomes current
            if current_min_day_diff == "*":
                current_value = item
                current_min_day_diff = daydiff
                continue

            # Update if this entry is more recent (smaller day-diff)
            if current_min_day_diff > daydiff:
                current_min_day_diff = daydiff
                current_value = item

    # Return required key from current entry, fall back to initial, or empty string
    return current_value[scrpit_name][required_key] if current_value else (
        initial_value[scrpit_name][required_key] if initial_value else '')


def get_current_schedule_policy_from_list(schedule_policies):
    """
    Extract current active schedule policy using day-difference selection.
    
    Equivalent to Workato Ruby:
    list1 = input["schedulePolicies"].where(daydiff: input["schedulePolicies"].pluck('daydiff').min)[0]
    
    Logic:
    1. Iterate schedule policies
    2. Calculate day_diff for each: today - effectiveDate (use hire-date if null)
    3. Find entry with smallest (non-negative) day_diff
    4. Return full entry structure (displayText, uri, scheduleTypeUri, effectiveDate, endDate)
    
    Returns:
        Dictionary with current active schedule policy:
        {
            "displayText": "8 hours/day, Su, Sa off",
            "uri": "urn:replicon-tenant:...:schedule-policy-entry:0e82ab53-...",
            "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule",
            "effectiveDate": {...},
            "endDate": {...},
            "daydiff": 45
        }
        Returns empty dict if no valid policy found
    """
    if not schedule_policies or 'urn' not in json.dumps(schedule_policies):
        return {}
    
    current_policy = None
    min_daydiff = None
    
    for policy in schedule_policies:
        # Skip entries without URN
        if 'uri' not in policy or not policy.get('uri'):
            continue
        
        # Calculate day-diff based on effectiveDate
        if policy.get('effectiveDate'):
            policy_date = dict_to_datetime(policy['effectiveDate']).date()
        else:
            # If no effectiveDate, this is a base/initial policy
            # Skip it if there are dated entries; will use as fallback
            continue
        
        daydiff = (now().date() - policy_date).days
        
        # Ignore future entries (daydiff < 0)
        if daydiff < 0:
            continue
        
        # Track entry with smallest daydiff
        if min_daydiff is None or daydiff < min_daydiff:
            min_daydiff = daydiff
            current_policy = policy
    
    # If no dated entry found, look for base policy (null effectiveDate)
    if not current_policy:
        for policy in schedule_policies:
            if 'uri' in policy and not policy.get('effectiveDate'):
                current_policy = policy
                break
    
    # Return policy with daydiff field added
    if current_policy:
        result = current_policy.copy()
        result['daydiff'] = min_daydiff if min_daydiff is not None else 0
        return result
    
    return {}



def put_policy_schedule_for_user_payload(dag_run):
    return {
  "user": {
    "uri": dag_run.conf['useruri'],
    "loginName": null,
    "employeeId": null,
    "parameterCorrelationId": null
  },
  "modifications": {
    "timezoneToApply": null,
    "workWeekStartToApply": null,
    "holidayCalendarToApply": null,
    "holidayCalendarAssignmentsToApply": null,
    "schedulePolicyToApply": {
      "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
      "replacementSchedule": [],
      "updateScheduleOverDateRange": {
        "replacementScheduleEntries": [
          {
            "schedulePolicy": {
              "officeScheduleUri": rail.result('get_req_schedule_script'),
              "name": null,
              "officeSchedule": {
                "officeScheduleUri": rail.result('get_req_schedule_script'),
                "name": null
              },
              "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
            },
            "effectiveDate": rail.result('log_schedule_policy_change_eff_date')
          }
        ],
        "endDate": null
      }
    },
    "locationScheduleToApply": null,
    "divisionScheduleToApply": null,
    "costCenterScheduleToApply": null,
    "departmentGroupScheduleToApply": null,
    "employeeTypeGroupScheduleToApply": null,
    "timesheetPeriodScheduleToApply": null,
    "serviceCenterScheduleToApply": null,
    "totalBusinessCostScheduleToApply": null,
    "permissionSetsToApply": null,
    "policySetsToApply": null,
    "policySetsScheduleToApply": [],
    "policyDataAccessScopesToApply": null,
    "policyDataAccessScopesToApply2": null,
    "notificationPreferencesToApply": null,
    "timesheetPeriodTypeToApply": null,
    "timesheetApprovalPathToApply": null,
    "timeEntryRevisionGroupApprovalPathToApply": null,
    "validationRuleToApply": null,
    "activitiesToApply": [],
    "activitiesToApply2": null,
    "defaultActivityToApply": null,
    "defaultActivityToApply2": null,
    "defaultTimeOffTypeForBookingsToApply": null,
    "expenseApprovalPathToApply": null,
    "expenseDefaultReimbursementCurrencyToApply": null,
    "timeOffApprovalPathToApply": null,
    "productAssignmentsToApply": null,
    "timeBankPolicyToApply": null,
    "securitySettingsToApply": null,
    "supervisorsToApply": null,
    "supervisorsModifications": null,
    "payrollRatesToApply": null,
    "payrollRatesModifications": null,
    "overtimeRulesToApply": null,
    "overtimeRulesModifications": null,
    "customFieldValuesToApply": [],
    "departmentToApply": null,
    "employeeTypeToApply": null,
    "userDetailsToApply": null,
    "payRulesToApply": null,
    "payRulesScheduleModifications": null,
    "payRatesModifications": null,
    "placeAssignmentsModifications": null,
    "resourceAllocationAfterUserEndDateOptionUri": null,
    "projectRolesToApply": null,
    "projectRoleAssignmentSchedulesToApply": null,
    "decimalSeparatorToApply": null,
    "numberGroupSeparatorToApply": null,
    "dateFormatToApply": null,
    "clockFormatToApply": null,
    "hoursFormatToApply": null,
    "timeZoneFormatToApply": null,
    "objectExtensionFieldsToApply": [],
    "costRateScheduleModifications": null,
    "workAuthorizationApprovalPathToApply": null,
    "displayNameFormatSettingsToApply": null,
    "timePunchTimeZoneDisplayOptionToApply": null,
    "defaultTimesheetToDisplayOptionToApply": null,
    "reportSettingsToApply": null,
    "timeOffBalancePayoutApprovalPathToApply": null,
    "workCompliancePolicyAssignmentScheduleToApply": null,
    "userConsentModificationsToApply": null
  },
  "userModificationOptionUri": "urn:replicon:user-modification-option:save"
}

def update_office_schedule_payload(dag_run):
    return {
  "user": {
    "uri": dag_run.conf['useruri'],
    "loginName": null,
    "employeeId": null,
    "parameterCorrelationId": null
  },
  "modifications": {
    "timezoneToApply": null,
    "workWeekStartToApply": null,
    "holidayCalendarToApply": null,
    "holidayCalendarAssignmentsToApply": null,
    "schedulePolicyToApply": {
      "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
      "replacementSchedule": [],
      "updateScheduleOverDateRange": {
        "replacementScheduleEntries": [
          {
            "schedulePolicy": {
              "officeScheduleUri": rail.result('get_0hrs_office_schedule_uri')['0hrs_schedule'],
              "name": null,
              "officeSchedule": {
                "officeScheduleUri": rail.result('get_0hrs_office_schedule_uri')['0hrs_schedule'],
                "name": null
              },
              "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
            },
            "effectiveDate": rail.result('get_split_start_and_end_dates')['enddate_plus_one_day']
          }
        ],
        "endDate": null
      }
    },
    "locationScheduleToApply": null,
    "divisionScheduleToApply": null,
    "costCenterScheduleToApply": null,
    "departmentGroupScheduleToApply": null,
    "employeeTypeGroupScheduleToApply": null,
    "timesheetPeriodScheduleToApply": null,
    "serviceCenterScheduleToApply": null,
    "totalBusinessCostScheduleToApply": null,
    "permissionSetsToApply": null,
    "policySetsToApply": null,
    "policySetsScheduleToApply": [],
    "policyDataAccessScopesToApply": null,
    "policyDataAccessScopesToApply2": null,
    "notificationPreferencesToApply": null,
    "timesheetPeriodTypeToApply": null,
    "timesheetApprovalPathToApply": null,
    "timeEntryRevisionGroupApprovalPathToApply": null,
    "validationRuleToApply": null,
    "activitiesToApply": [],
    "activitiesToApply2": null,
    "defaultActivityToApply": null,
    "defaultActivityToApply2": null,
    "defaultTimeOffTypeForBookingsToApply": null,
    "expenseApprovalPathToApply": null,
    "expenseDefaultReimbursementCurrencyToApply": null,
    "timeOffApprovalPathToApply": null,
    "productAssignmentsToApply": null,
    "timeBankPolicyToApply": null,
    "securitySettingsToApply": null,
    "supervisorsToApply": null,
    "supervisorsModifications": null,
    "payrollRatesToApply": null,
    "payrollRatesModifications": null,
    "overtimeRulesToApply": null,
    "overtimeRulesModifications": null,
    "customFieldValuesToApply": [],
    "departmentToApply": null,
    "employeeTypeToApply": null,
    "userDetailsToApply": null,
    "payRulesToApply": null,
    "payRulesScheduleModifications": null,
    "payRatesModifications": null,
    "placeAssignmentsModifications": null,
    "resourceAllocationAfterUserEndDateOptionUri": null,
    "projectRolesToApply": null,
    "projectRoleAssignmentSchedulesToApply": null,
    "decimalSeparatorToApply": null,
    "numberGroupSeparatorToApply": null,
    "dateFormatToApply": null,
    "clockFormatToApply": null,
    "hoursFormatToApply": null,
    "timeZoneFormatToApply": null,
    "objectExtensionFieldsToApply": [],
    "costRateScheduleModifications": null,
    "workAuthorizationApprovalPathToApply": null,
    "displayNameFormatSettingsToApply": null,
    "timePunchTimeZoneDisplayOptionToApply": null,
    "defaultTimesheetToDisplayOptionToApply": null,
    "reportSettingsToApply": null,
    "timeOffBalancePayoutApprovalPathToApply": null,
    "workCompliancePolicyAssignmentScheduleToApply": null,
    "userConsentModificationsToApply": null
  },
  "userModificationOptionUri": "urn:replicon:user-modification-option:save"
}

def update_shift_schedule_for_user_payload(dag_run):
    return {
  "user": {
    "uri": dag_run.conf['useruri'],
    "loginName": null,
    "employeeId": null,
    "parameterCorrelationId": null
  },
  "modifications": {
    "timezoneToApply": null,
    "workWeekStartToApply": null,
    "holidayCalendarToApply": null,
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
                "name": null
              },
              "scheduleTypeUri": "urn:replicon:schedule-type:shift"
            },
            "effectiveDate": rail.result('log_schedule_policy_change_eff_date')
          }
        ],
        "endDate": null
      }
    },
    "locationScheduleToApply": null,
    "divisionScheduleToApply": null,
    "costCenterScheduleToApply": null,
    "departmentGroupScheduleToApply": null,
    "employeeTypeGroupScheduleToApply": null,
    "timesheetPeriodScheduleToApply": null,
    "serviceCenterScheduleToApply": null,
    "totalBusinessCostScheduleToApply": null,
    "permissionSetsToApply": null,
    "policySetsToApply": null,
    "policySetsScheduleToApply": [],
    "policyDataAccessScopesToApply": null,
    "policyDataAccessScopesToApply2": null,
    "notificationPreferencesToApply": null,
    "timesheetPeriodTypeToApply": null,
    "timesheetApprovalPathToApply": null,
    "timeEntryRevisionGroupApprovalPathToApply": null,
    "validationRuleToApply": null,
    "activitiesToApply": [],
    "activitiesToApply2": null,
    "defaultActivityToApply": null,
    "defaultActivityToApply2": null,
    "defaultTimeOffTypeForBookingsToApply": null,
    "expenseApprovalPathToApply": null,
    "expenseDefaultReimbursementCurrencyToApply": null,
    "timeOffApprovalPathToApply": null,
    "productAssignmentsToApply": null,
    "timeBankPolicyToApply": null,
    "securitySettingsToApply": null,
    "supervisorsToApply": null,
    "supervisorsModifications": null,
    "payrollRatesToApply": null,
    "payrollRatesModifications": null,
    "overtimeRulesToApply": null,
    "overtimeRulesModifications": null,
    "customFieldValuesToApply": [],
    "departmentToApply": null,
    "employeeTypeToApply": null,
    "userDetailsToApply": null,
    "payRulesToApply": null,
    "payRulesScheduleModifications": null,
    "payRatesModifications": null,
    "placeAssignmentsModifications": null,
    "resourceAllocationAfterUserEndDateOptionUri": null,
    "projectRolesToApply": null,
    "projectRoleAssignmentSchedulesToApply": null,
    "decimalSeparatorToApply": null,
    "numberGroupSeparatorToApply": null,
    "dateFormatToApply": null,
    "clockFormatToApply": null,
    "hoursFormatToApply": null,
    "timeZoneFormatToApply": null,
    "objectExtensionFieldsToApply": [],
    "costRateScheduleModifications": null,
    "workAuthorizationApprovalPathToApply": null,
    "displayNameFormatSettingsToApply": null,
    "timePunchTimeZoneDisplayOptionToApply": null,
    "defaultTimesheetToDisplayOptionToApply": null,
    "reportSettingsToApply": null,
    "timeOffBalancePayoutApprovalPathToApply": null,
    "workCompliancePolicyAssignmentScheduleToApply": null,
    "userConsentModificationsToApply": null
  },
  "userModificationOptionUri": "urn:replicon:user-modification-option:save"
}

def trigger_updateuser_timeoff(dag_run):
    strt_date = rail.result('get_user_data_14')[
        0]['userDetails']['employmentDateRange']['startDate']
    return {
        "parentjobid": dag_run.conf['parentjobid'],
        "userid": dag_run.conf['userid'],
        "hiredate": dag_run.conf['hiredate'],
        "terminationdate": dag_run.conf['terminationdate'],
        "active": dag_run.conf['active'],
        "rehire": dag_run.conf['rehire_update'],
        "timeofftypes": rail.result('log_timeofftypes_tobeassigned'),
        "old_startdate": str(strt_date['year']) + '-' + str(strt_date['month']) + '-' + str(strt_date['day']),
        "useruri": dag_run.conf['useruri'],
        "workshift_change_effective_date": dag_run.conf['work_shift_change_effective_date'],
        "continious_service_date": dag_run.conf['continous_service_date'],
        "timeoff_service_date": dag_run.conf['timeoff_service_date']  
    }


def final_policyset_schedule_entry(dag_run):
    final_entries = rail.result("get_past_policysetschedule_entries")
    end_date_string_split = {
        'day': dag_run.conf['terminationdate'].split("/")[0],
        'month': dag_run.conf['terminationdate'].split("/")[1],
        'year': dag_run.conf['terminationdate'].split("/")[2]
    }
    final_entries.append({
        "effectiveDate": end_date_string_split,
        "description": "Effective on " + end_date_string_split['month'] + "/" + end_date_string_split['day'] + "/" + end_date_string_split['year'],
        "policySet": {
            "timeOffBalanceEventScripts": [
                {
                    "scriptTarget": {
                        "uri": dag_run.conf['startingbalancesettouri'],
                        "slug": null,
                        "name": null
                    },
                    "additionalParameters": [
                        {
                            "keyUri": "urn:replicon:script-key:parameter:amount",
                            "value": {
                                "uri": null,
                                "slug": null,
                                "bool": null,
                                "date": null,
                                "number": rail.get_dag_run_var('balance_amount'),
                                "text": null,
                                "time": null,
                                "calendarDayDurationValue": null,
                                "workdayDurationValue": null,
                                "dateRange": null,
                                "collection": []
                            }
                        },
                        {
                            "keyUri": "urn:replicon:script-key:parameter:precedence",
                            "value": {
                                "uri": null,
                                "slug": null,
                                "bool": null,
                                "date": null,
                                "number": "20",
                                "text": null,
                                "time": null,
                                "calendarDayDurationValue": null,
                                "workdayDurationValue": null,
                                "dateRange": null,
                                "collection": []
                            }
                        }
                    ]
                }
            ],
            "timeOffValidationScripts": []
        }
    })

    return final_entries


def create_supervisor_payload(dag_run):
    return {
  "user": {
    "target": {
      "uri": null,
      "loginName": dag_run.conf['sup_email'],
      "employeeId": null,
      "parameterCorrelationId": null
    },
    "firstname": dag_run.conf['sup_firstname'],
    "lastname": dag_run.conf['sup_lastname'],
    "emailAddress": dag_run.conf['sup_email'],
    "employeeId": dag_run.conf['supervisorloginname'],
    "department": null,
    "supervisorAssignmentSchedule": null,
    "schedulePolicySchedule": [],
    "workWeekStartDayUri": "urn:replicon:day-of-week:monday",
    "employmentDateRange": {
      "startDate": rail.result('get_split_dates')['sup_eff_date'],
      "endDate": null,
      "relativeDateRangeUri": null,
      "relativeDateRangeAsOfDate": null
    },
    "securityConfiguration": {
      "enabledAuthenticationTypeUris": [
        "urn:replicon:user-authentication-type:sso"
      ],
      "isLoginEnabled": True,
      "loginName": dag_run.conf['sup_email'],
      "SSOName": dag_run.conf['sup_email'],
      "password": "Replicon@12"
    },
    "holidayCalendar": null,
    "holidayCalendarAssignmentSchedule": null,
    "timeOffPolicy": null,
    "permissionSets": [
      {
        "uri": dag_run.conf['supervisor'],
        "name": null
      }
    ],
    "policySets": [],
    "policySetsSchedule": [],
    "employeeType": null,
    "timesheetPeriodTypeUri": null,
    "costRateSchedule": null,
    "payrollRateSchedule": null,
    "defaultBillingRate": null,
    "timesheetApprovalPath": null,
    "expenseApprovalPath": null,
    "expenseDefaultReimbursementCurrency": null,
    "timeOffApprovalPath": null,
    "workAuthorizationApprovalPath": null,
    "timeOffBalancePayoutApprovalPath": null,
    "customFieldValues": [],
    "assignedActivities": [],
    "timeZone": null,
    "overtimeRuleAssignmentSchedule": null,
    "validationRuleAssignmentSchedule": null,
    "locationSchedule": [],
    "divisionSchedule": [],
    "costCenterSchedule": [],
    "serviceCenterSchedule": [],
    "departmentGroupSchedule": [
      {
        "departmentGroup": {
          "uri": null,
          "parent": null,
          "name": "Momentive",
          "parameterCorrelationId": null
        },
        "effectiveDate": null
      }
    ],
    "employeeTypeGroupSchedule": [
      {
        "employeeTypeGroup": {
          "uri": null,
          "parent": null,
          "name": "Foreign Supervisors",
          "parameterCorrelationId": null
        },
        "effectiveDate": null
      }
    ],
    "timesheetPeriodSchedule": [],
    "policyDataAccessScopes": [],
    "policyDataAccessScopes2": [],
    "payRuleScriptSchedule": [],
    "displayNameParameter": null,
    "decimalSeparatorUri": null,
    "numberGroupSeparatorUri": null,
    "extensionFieldValues": [],
    "workCompliancePolicyAssignmentSchedule": []
  }
}


def generate_report_per_supervisor_payload(dag_run):
    return {
        "reportUri": dag_run.conf['reporturi'],
        "filterValues": [
            {
                "reportFilterUri": dag_run.conf['userfilteruri'],
                "value": rail.result('extract_supervisor_user_id')
            }
        ],
        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
    }

def restrict_supervisor_schedule_payload(dag_run):
    return {
  "userUri": dag_run.conf['supervisoruri'],
  "policyDataAccessScopes": [
    {
      "policyUri": "urn:replicon:policy:schedule-management",
      "locations": [],
      "divisions": [],
      "costCenters": [],
      "serviceCenters": [],
      "departmentGroups": [
        {
          "departmentGroup": {
            "uri": rail.result('get_enabled_department_groups'),
            "parent": null,
            "name": null,
            "parameterCorrelationId": null
          },
          "groupSpecificationModeUri": null,
          "groupDescendantModeUri": null
        }
      ],
      "employeeTypeGroups": [],
      "scopeObjectTypeUri": null
    }
  ]
}

def assign_timeoff_types_payload(dag_run):
    matched_types = rail.get_dag_run_var("matched_timeoff_uris")
    uris = [item['uri'] for item in matched_types] if matched_types else []
    return {
      "userUri": dag_run.conf['useruri'],
      "timeOffTypeUris": uris
    }


def _or_null(value):
    return None if value == "" else value


def create_user_payload(dag_run):
    policy_sets = [p for p in (rail.get_dag_run_var("policysets") or []) if p.get("name") or p.get("uri")]
    pay_rule_scripts = [p for p in (rail.get_dag_run_var("payruletoassign") or []) if p.get("payRuleScript", {}).get("uri")]

    return {
        "user": {
            "target": {
                "uri": null,
                "loginName": dag_run.conf['userid'],
                "employeeId": null,
                "parameterCorrelationId": null
            },
            "firstname": dag_run.conf['firstname'],
            "lastname": dag_run.conf['lastname'],
            "emailAddress": dag_run.conf['emailaddress'],
            "employeeId": dag_run.conf['Worker_Reference_Employee_ID'],
            "department": null,
            "supervisorAssignmentSchedule": null,
            "schedulePolicySchedule": rail.get_dag_run_var("schedule"),
            "workWeekStartDayUri": "urn:replicon:day-of-week:sunday",
            "employmentDateRange": {
                "startDate": rail.result('log_hiredate_47'),
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:sso"
                ],
                "isLoginEnabled": "true",
                "loginName": dag_run.conf['userid'],
                "SSOName": dag_run.conf['userid'],
                "password": null
            },
            "holidayCalendar": _or_null(rail.get_dag_run_var("holidaycalendar")),
            "holidayCalendarAssignmentSchedule": null,
            "timeOffPolicy": null,
            "permissionSets": [
                {
                    "uri": dag_run.conf['basic_user_with_report_uri'],
                    "name": null
                }
            ],
            "policySets": policy_sets,
            "policySetsSchedule": [],
            "employeeType": null,
            "timesheetPeriodTypeUri": null,
            "costRateSchedule": null,
            "payrollRateSchedule": null,
            "defaultBillingRate": null,
            "timesheetApprovalPath": _or_null(rail.get_dag_run_var("timesheetapprovalpath")),
            "expenseApprovalPath": null,
            "expenseDefaultReimbursementCurrency": null,
            "timeOffApprovalPath": _or_null(rail.get_dag_run_var("timeoffapprovalpath")),
            "workAuthorizationApprovalPath": null,
            "timeOffBalancePayoutApprovalPath": null,
            "customFieldValues": [],
            "assignedActivities": [],
            "timeZone": {
                "uri": null,
                "IANAName": "Asia/Tokyo"
            },
            "overtimeRuleAssignmentSchedule": null,
            "validationRuleAssignmentSchedule": null,
            "locationSchedule": [],
            "divisionSchedule": rail.get_dag_run_var("legalentity_division"),
            "costCenterSchedule": rail.get_dag_run_var("costcenter"),
            "serviceCenterSchedule": rail.get_dag_run_var("paygroup_servicecenter"),
            "departmentGroupSchedule": [
                {
                    "departmentGroup": {
                        "uri": dag_run.conf["departmentgroupuri"],
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
                        "uri": rail.result('get_required_employeetype_uri_40'),
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null
                    },
                    "effectiveDate": null
                }
            ],
            "timesheetPeriodSchedule": [],
            "policyDataAccessScopes": [],
            "policyDataAccessScopes2": [],
            "payRuleScriptSchedule": pay_rule_scripts,
            "displayNameParameter": null,
            "decimalSeparatorUri": null,
            "numberGroupSeparatorUri": null,
            "extensionFieldValues": [],
            "workCompliancePolicyAssignmentSchedule": []
        }
    }