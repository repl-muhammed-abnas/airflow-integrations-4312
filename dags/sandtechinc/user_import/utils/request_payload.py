"""
Sand Tech Inc - User Import Request Payloads
Builders for Replicon API request payloads
"""

import uuid

null = None


def build_user_search_payload(search_text, page=1, pagesize=100):
    """
    Build payload for UserListService GetData with text search
    
    Args:
        search_text: Text to search (email or employee ID)
        page: Page number (default 1)
        pagesize: Page size (default 100)
    
    Returns:
        Request payload dict
    """
    return {
        'page': page,
        'pagesize': pagesize,
        'columnUris': [
            'urn:replicon:user-list-column:user',
            'urn:replicon:user-list-column:login-name',
            'urn:replicon:user-list-column:employee-id',
            'urn:replicon:user-list-column:enabled'
        ],
        'filterExpression': {
            'leftExpression': {
                'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
            },
            'operatorUri': 'urn:replicon:filter-operator:text-search',
            'rightExpression': {
                'value': {
                    'text': search_text
                }
            }
        }
    }


def build_bulk_get_users_payload(user_uri):
    """
    Build payload for ImportService BulkGetUsers3
    
    Args:
        user_uri: User URI to fetch
    
    Returns:
        Request payload dict
    """
    return {
        "users": [{
            "uri": user_uri,
            "loginName": null,
            "parameterCorrelationId": null
        }],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }


def build_create_user_payload(config, user_data, parsed_dates, permission_uris, metadata_uris):
    """
    Build payload for ImportService PutUser3 (create new user)
    
    Args:
        config: Instance configuration
        user_data: User record dict
        parsed_dates: Dict with parsed start_date and end_date
        permission_uris: List of permission set URIs
        metadata_uris: Dict with resolved URIs for department, location, etc.
    
    Returns:
        Request payload dict
    """
    return {
        "user": {
            "target": {
                "uri": null,
                "loginName": user_data['email'],
                "parameterCorrelationId": null
            },
            "firstname": user_data['first_name'],
            "lastname": user_data['last_name'],
            "displayNameParameter": user_data.get('display_name') or null,
            "emailAddress": user_data['email'],
            "employeeId": user_data['employee_id'],
            "department": null,
            "supervisorAssignmentSchedule": null,
            "schedulePolicySchedule": [{
                "schedulePolicy": {
                    "officeScheduleUri": null,
                    "name": config.default_office_schedule,
                    "officeSchedule": {
                        "officeScheduleUri": null,
                        "name": config.default_office_schedule
                    },
                    "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                },
                "effectiveDate": null
            }],
            "workWeekStartDayUri": config.default_work_week,
            "employmentDateRange": {
                "startDate": parsed_dates.get('start_date'),
                "endDate": parsed_dates.get('end_date'),
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": ["urn:replicon:user-authentication-type:sso"],
                "isLoginEnabled": "true" if not parsed_dates.get('end_date') else "false",
                "loginName": user_data['email'],
                "SSOName": user_data['email'],
                "password": null
            },
            "holidayCalendar": {"uri": metadata_uris.get('holiday_calendar_uri'), "name": null} if metadata_uris.get('holiday_calendar_uri') else null,
            "timeOffPolicy": null,
            "permissionSets": [{"uri": uri, "name": null} for uri in permission_uris] if permission_uris else [],
            "policySets": build_policy_sets_list(metadata_uris),
            "employeeType": null,
            "costRateSchedule": null,
            "payrollRateSchedule": null,
            "timesheetPeriodTypeUri": null,
            "defaultBillingRate": null,
            "timesheetApprovalPath": {"uri": metadata_uris.get('timesheet_approval_path_uri'), "name": null} if metadata_uris.get('timesheet_approval_path_uri') else null,
            "expenseApprovalPath": null,
            "timeOffApprovalPath": {"uri": metadata_uris.get('timeoff_approval_path_uri'), "name": null} if metadata_uris.get('timeoff_approval_path_uri') else null,
            "customFieldValues": null,
            "assignedActivities": null,
            "timeZone": {"uri": metadata_uris.get('timezone_uri'), "IANAName": null} if metadata_uris.get('timezone_uri') else null,
            "overtimeRuleAssignmentSchedule": null,
            "validationRuleAssignmentSchedule": null,
            "locationSchedule": build_location_schedule(metadata_uris.get('location_uri')),
            "divisionSchedule": null,
            "costCenterSchedule": null,
            "serviceCenterSchedule": null,
            "departmentGroupSchedule": build_department_schedule(metadata_uris.get('department_uri')),
            "employeeTypeGroupSchedule": build_employee_type_schedule(metadata_uris.get('employee_type_uri')),
            "timesheetPeriodSchedule": build_timesheet_period_schedule(metadata_uris.get('timesheet_period_uri')),
            "policyDataAccessScopes": [],
            "policyDataAccessScopes2": [],
            "payRuleScriptSchedule": null
        }
    }


def build_policy_sets_list(metadata_uris):
    """Build policy sets list from URIs"""
    policy_sets = []
    if metadata_uris.get('timesheet_template_uri'):
        policy_sets.append({"uri": metadata_uris['timesheet_template_uri'], "name": null})
    if metadata_uris.get('timeoff_template_uri'):
        policy_sets.append({"uri": metadata_uris['timeoff_template_uri'], "name": null})
    return policy_sets if policy_sets else null


def build_department_schedule(department_uri):
    """Build department schedule entry"""
    if not department_uri:
        return null
    return [{
        "departmentGroup": {
            "uri": department_uri,
            "parent": null,
            "name": null
        },
        "effectiveDate": null
    }]


def build_location_schedule(location_uri):
    """Build location schedule entry"""
    if not location_uri:
        return null
    return [{
        "location": {
            "uri": location_uri,
            "parentUri": null,
            "name": null
        },
        "effectiveDate": null
    }]


def build_employee_type_schedule(employee_type_uri):
    """Build employee type schedule entry"""
    if not employee_type_uri:
        return null
    return [{
        "employeeTypeGroup": {
            "uri": employee_type_uri,
            "parent": null,
            "name": null
        },
        "effectiveDate": null
    }]


def build_timesheet_period_schedule(timesheet_period_uri):
    """Build timesheet period schedule entry"""
    if not timesheet_period_uri:
        return null
    return [{
        "timesheetPeriod": {
            "uri": timesheet_period_uri,
            "name": null
        },
        "effectiveDate": null
    }]


def build_assign_role_payload(user_uri, role_uri, effective_date=None):
    """
    Build payload for assigning primary role to user
    
    Args:
        user_uri: User URI
        role_uri: Project role URI
        effective_date: Optional effective date dict
    
    Returns:
        Request payload dict
    """
    return {
        "user": {
            "uri": user_uri,
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "projectRolesToApply": {
                "userProjectRoleModificationOptionUri": "urn:replicon:user-project-role-modification-option:replace-all",
                "userProjectRoles": [{
                    "projectRole": {
                        "uri": role_uri,
                        "name": null
                    },
                    "isPrimary": True,
                    "effectiveDate": effective_date
                }]
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def build_create_role_payload(role_name):
    """
    Build payload for creating a new project role
    
    Args:
        role_name: Name of the role to create
    
    Returns:
        Request payload dict
    """
    return {
        "target": null,
        "modifications": {
            "name": role_name,
            "descriptionToApply": null,
            "isArchivedToApply": False,  # Status = Enabled
            "isBillableToApply": True,   # Billable = Yes
            "billingRateScheduleToApply": null,  # Billing Rate = $0.00 (default)
            "costRateScheduleToApply": null      # Cost Rate = $0.00 (default)
        },
        "projectRoleModificationOptionUri": "urn:replicon:project-role-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def build_assign_permission_payload(user_uri, permission_set_uri):
    """
    Build payload for assigning permission to user
    
    Args:
        user_uri: User URI
        permission_set_uri: Permission set URI
    
    Returns:
        Request payload dict
    """
    return {
        "userUri": user_uri,
        "permissionSetUri": permission_set_uri
    }


def build_supervisor_assignment_payload(user_uri, supervisor_uri, date_range=None):
    """
    Build payload for updating supervisor assignment
    
    Args:
        user_uri: User URI to update
        supervisor_uri: Supervisor user URI
        date_range: Optional date range dict
    
    Returns:
        Request payload dict
    """
    return {
        "userUri": user_uri,
        "supervisorUri": supervisor_uri,
        "dateRange": date_range
    }


def build_initial_supervisor_payload(user_uri, supervisor_uri):
    """
    Build payload for setting initial supervisor (new user)
    
    Args:
        user_uri: User URI
        supervisor_uri: Supervisor user URI
    
    Returns:
        Request payload dict
    """
    return {
        "userUri": user_uri,
        "initialSupervisorUri": supervisor_uri,
        "scheduleEntries": []
    }


def build_update_department_payload(user_uri, department_uri, effective_date=None):
    """
    Build payload for updating user department
    
    Args:
        user_uri: User URI
        department_uri: Department URI
        effective_date: Optional effective date dict
    
    Returns:
        Request payload dict
    """
    return {
        "user": {
            "uri": user_uri,
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "departmentGroupScheduleToApply": {
                "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementDepartmentGroupSchedule": [],
                "updateDepartmentGroupScheduleOverDateRange": {
                    "replacementDepartmentGroupScheduleEntries": [{
                        "departmentGroup": {
                            "uri": department_uri,
                            "parent": null,
                            "name": null,
                            "parameterCorrelationId": null
                        },
                        "effectiveDate": effective_date
                    }],
                    "endDate": null
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def build_update_location_payload(user_uri, location_uri, effective_date=None):
    """
    Build payload for updating user location
    
    Args:
        user_uri: User URI
        location_uri: Location URI
        effective_date: Optional effective date dict
    
    Returns:
        Request payload dict
    """
    return {
        "user": {
            "uri": user_uri,
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "locationScheduleToApply": {
                "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementLocationSchedule": [],
                "updateLocationScheduleOverDateRange": {
                    "replacementLocationScheduleEntries": [{
                        "location": {
                            "uri": location_uri,
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": effective_date
                    }],
                    "endDate": null
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def build_update_holiday_calendar_payload(user_uri, holiday_calendar_uri):
    """
    Build payload for updating user holiday calendar
    
    Args:
        user_uri: User URI
        holiday_calendar_uri: Holiday calendar URI
    
    Returns:
        Request payload dict
    """
    return {
        "userUri": user_uri,
        "holidayCalendarUri": holiday_calendar_uri
    }


def build_employment_date_range_payload(user_uri, start_date, end_date):
    """
    Build payload for updating employment date range
    
    Args:
        user_uri: User URI
        start_date: Start date dict
        end_date: End date dict (can be None)
    
    Returns:
        Request payload dict
    """
    return {
        "userUri": user_uri,
        "dateRange": {
            "startDate": start_date,
            "endDate": end_date,
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        }
    }


def build_update_security_settings_payload(user_uri, email):
    """
    Build payload for updating login name and SSO name
    
    Args:
        user_uri: User URI
        email: New email/login name
    
    Returns:
        Request payload dict
    """
    return {
        "user": {
            "uri": user_uri,
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "securitySettingsToApply": {
                "loginEnabled": "1",
                "loginName": email,
                "ssoName": email,
                "password": null,
                "enabledAuthenticationTypeUris": ["urn:replicon:user-authentication-type:sso"],
                "emailMFAResendVerificationEmail": "false",
                "emailMFATryAddMethodFromUsersEmail": "false",
                "clearIsLockedOut": "false"
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }
