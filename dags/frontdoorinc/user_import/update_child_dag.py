
from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'frontdoorinc_user_import_update_user_child_{config.instance}',
        description=f'Frontdoorinc_user_import_update_user_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='bulk_get_users3_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='bulk_get_users3_3',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        bulk_get_users3_3 = rail.RepliconServiceOperator(
            task_id='bulk_get_users3_3',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": "{{ dag_run.conf.useruri }}",
                        "loginName": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        if_userdetails_isenabled_is_not_true_5 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_not_true_5',
            test='''{{ result('bulk_get_users3_3')[0].userDetails.isEnabled | is_falsy }}''',
            yes_task="enable_login_6",
            no_task="if_userdetails_isenabled_is_true_8",
        )

        enable_login_6 = rail.RepliconServiceOperator(
            task_id='enable_login_6',
            endpoint="/services/securityService1.svc/EnableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        apply_user_modifications2_updatestartdate_7 = rail.RepliconServiceOperator(
            task_id='apply_user_modifications2_updatestartdate_7',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": {
                        "year": datetime.strptime(dag_run.conf['hiredate'], "%Y-%m-%d").year,
                        "month": datetime.strptime(dag_run.conf['hiredate'], "%Y-%m-%d").month,
                        "day": datetime.strptime(dag_run.conf['hiredate'], "%Y-%m-%d").day,
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_userdetails_isenabled_is_true_8 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_true_8',
            test='''{{ result('bulk_get_users3_3')[0].userDetails.isEnabled | is_truthy }}''',
            yes_task="if_request_terminationdate_present_9",
            no_task="if_request_emailaddress_present_15",
        )

        if_request_terminationdate_present_9 = rail.IfOperator(
            task_id='if_request_terminationdate_present_9',
            test='''{{ dag_run.conf.terminationdate | is_truthy }}''',
            yes_task="apply_user_modifications2_updatestartdate_11",
            no_task="if_request_emailaddress_present_15",
        )

        apply_user_modifications2_updatestartdate_11 = rail.RepliconServiceOperator(
            task_id='apply_user_modifications2_updatestartdate_11',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": {
                        "year": datetime.strptime(dag_run.conf['hiredate'], "%Y-%m-%d").year,
                        "month": datetime.strptime(dag_run.conf['hiredate'], "%Y-%m-%d").month,
                        "day": datetime.strptime(dag_run.conf['hiredate'], "%Y-%m-%d").day,
                    },
                    "endDate":  {
                        "year": datetime.strptime(dag_run.conf['terminationdate'], "%Y-%m-%d").year,
                        "month": datetime.strptime(dag_run.conf['terminationdate'], "%Y-%m-%d").month,
                        "day": datetime.strptime(dag_run.conf['terminationdate'], "%Y-%m-%d").day,
                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        disable_login_12 = rail.RepliconServiceOperator(
            task_id='disable_login_12',
            endpoint="/services/securityService1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        frontdoorinc_user_import_logs_add_entry_13 = rail.WriteLogOperator(
            task_id='frontdoorinc_user_import_logs_add_entry_13',
            log="{{dag_run.conf.lookuptable}}",
            message="na",
            severity="success",
            properties=lambda dag_run: {
                "username": str(dag_run.conf['firstname']) + " " + str(dag_run.conf['lastname']),
                "employeeid": dag_run.conf['employeeid'],
                "action": "update",
                "status": "success",
                "details": "User successfully disabled",
                "jobid": dag_run.conf['job_id'],
                "childjob": rail.render_template("{{ dag_run_ecid() }}")
            }
        )

        if_request_emailaddress_present_15 = rail.IfOperator(
            task_id='if_request_emailaddress_present_15',
            test='''{{ dag_run.conf.emailaddress | is_truthy }}''',
            yes_task="if_securityconfiguration_loginname_not_equals_to_dataworkato_service2693ef48requestemailaddress_16",
            no_task="get_effective_user_group_membership_18",
        )

        if_securityconfiguration_loginname_not_equals_to_dataworkato_service2693ef48requestemailaddress_16 = rail.IfOperator(
            task_id='if_securityconfiguration_loginname_not_equals_to_dataworkato_service2693ef48requestemailaddress_16',
            test='''{{ result('bulk_get_users3_3')[0].securityConfiguration.loginName != dag_run.conf.emailaddress }}''',
            yes_task="set_s_s_o_authentication_for_user_updateloginname_17",
            no_task="get_effective_user_group_membership_18",
        )

        set_s_s_o_authentication_for_user_updateloginname_17 = rail.RepliconServiceOperator(
            task_id='set_s_s_o_authentication_for_user_updateloginname_17',
            endpoint="/services/SecurityService1.svc/SetSSOAuthenticationForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "loginName": "{{ dag_run.conf.emailaddress }}"
            }
        )

        get_effective_user_group_membership_18 = rail.RepliconServiceOperator(
            task_id='get_effective_user_group_membership_18',
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "dateRange": null
            }
        )

        def get_adminmodified():
            data = rail.result('bulk_get_users3_3') if rail.result(
                'bulk_get_users3_3') else null
            for item in data:
                for record in item['userDetails']['customFieldValues']:
                    if record['customField']['name'] == 'Admin Modified':
                        return record
            return None

        get_adminmodified_details = rail.PythonOperator(
            task_id='get_adminmodified_details',
            python_callable=get_adminmodified
        )

        if_customfield_name_equals_to_adminmodified_22 = rail.IfOperator(
            task_id='if_customfield_name_equals_to_adminmodified_22',
            test='''{{ result('get_adminmodified_details').customField.name == 'Admin Modified' }}''',
            yes_task="if_customfield_displaytext_equals_to_yes_23",
            no_task="if_request_emailaddress_present_34",
        )

        # Keeping the condition in parity with workato
        if_customfield_displaytext_equals_to_yes_23 = rail.IfOperator(
            task_id='if_customfield_displaytext_equals_to_yes_23',
            test='''{{ result('get_adminmodified_details').customField.displayText == 'Yes' }}''',
            yes_task="if_request_timetype_present_24",
            no_task="if_request_emailaddress_present_34",
        )

        if_request_timetype_present_24 = rail.IfOperator(
            task_id='if_request_timetype_present_24',
            test=lambda dag_run: dag_run.conf['timetype'] and ((dag_run.conf['employeetypeuri']) != (rail.result('get_effective_user_group_membership_18')['employeeTypes'][0]['employeeType']['employeeType']['uri'] if rail.result(
                'get_effective_user_group_membership_18') and rail.result('get_effective_user_group_membership_18')['employeeTypes'] and rail.result('get_effective_user_group_membership_18')['employeeTypes'][0] and rail.result('get_effective_user_group_membership_18')['employeeTypes'][0]['employeeType'] else null)),
            yes_task="apply_user_modifications2employee_type_group_schedule_to_apply_25",
            no_task="frontdoorinc_user_import_logs_add_entry_29",
        )

        apply_user_modifications2employee_type_group_schedule_to_apply_25 = rail.RepliconServiceOperator(
            task_id='apply_user_modifications2employee_type_group_schedule_to_apply_25',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=lambda dag_run: {
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
                                        "uri": dag_run.conf['employeetypeuri'],
                                        "parentUri": null,
                                        "name": null
                                    },
                                    "effectiveDate": {
                                        "year":  datetime.today().strftime("%Y-%m-%d").split('-')[0],
                                        "month":  datetime.today().strftime("%Y-%m-%d").split('-')[1],
                                        "day":  datetime.today().strftime("%Y-%m-%d").split('-')[2],
                                    }
                                }
                            ],
                            "endDate": null
                        }
                    },
                    "userModificationOptionUri": "urn:replicon:user-modification-option:save"
                }
            }
        )

        if_d_errors_present_26 = rail.IfOperator(
            task_id='if_d_errors_present_26',
            test='''{{ result('apply_user_modifications2employee_type_group_schedule_to_apply_25').errors | is_truthy }}''',
            yes_task="frontdoorinc_user_import_logs_add_entry_27",
            no_task="frontdoorinc_user_import_logs_add_entry_29",
        )

        frontdoorinc_user_import_logs_add_entry_27 = rail.WriteLogOperator(
            task_id='frontdoorinc_user_import_logs_add_entry_27',
            log="{{dag_run.conf.lookuptable}}",
            message="na",
            severity="error",
            properties=lambda dag_run: {
                "username": str(dag_run.conf['firstname']) + " " + str(dag_run.conf['lastname']),
                "employeeid": dag_run.conf['employeeid'],
                "action": "update",
                "status": "error",
                "details": rail.result('apply_user_modifications2employee_type_group_schedule_to_apply_25')['errors'],
                "jobid": dag_run.conf['job_id'],
                "childjob": rail.render_template("{{ dag_run_ecid() }}")
            }
        )

        frontdoorinc_user_import_logs_add_entry_29 = rail.WriteLogOperator(
            task_id='frontdoorinc_user_import_logs_add_entry_29',
            log="{{dag_run.conf.lookuptable}}",
            message="na",
            severity="exception",
            properties=lambda dag_run: {
                "username": str(dag_run.conf['firstname']) + " " + str(dag_run.conf['lastname']),
                "employeeid": dag_run.conf['employeeid'],
                "action": "update",
                "status": "exception",
                "details": "No updates performed as the Admin Modified value is set to Yes",
                "jobid": dag_run.conf['job_id'],
                "childjob": rail.render_template("{{ dag_run_ecid() }}")
            }
        )

        if_request_emailaddress_present_34 = rail.IfOperator(
            task_id='if_request_emailaddress_present_34',
            test='''{{ dag_run.conf.emailaddress | is_truthy }}''',
            yes_task="if_request_emailaddress_not_equals_to_datarestbulk_get_users3_3responsedfirstuserdetailsemailaddress_35",
            no_task="if_request_company_not_equals_to_datarestget_effective_user_37",
        )

        if_request_emailaddress_not_equals_to_datarestbulk_get_users3_3responsedfirstuserdetailsemailaddress_35 = rail.IfOperator(
            task_id='if_request_emailaddress_not_equals_to_datarestbulk_get_users3_3responsedfirstuserdetailsemailaddress_35',
            test='''{{ (dag_run.conf.emailaddress != result('bulk_get_users3_3')[0].userDetails.emailAddress)  or (dag_run.conf.firstname != result('bulk_get_users3_3')[0].userDetails.firstName)  or (dag_run.conf.lastname != result('bulk_get_users3_3')[0].userDetails.lastName) }}''',
            yes_task="adhoc_http_action_36",
            no_task="if_request_company_not_equals_to_datarestget_effective_user_37",
        )

        adhoc_http_action_36 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_36',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=lambda dag_run: {
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
                    "userDetailsToApply": {
                        "firstName": null if dag_run.conf['firstname'] == rail.result('bulk_get_users3_3')[0]['userDetails']['firstName'] else dag_run.conf['firstname'],
                        "lastName": null if dag_run.conf['lastname'] == rail.result('bulk_get_users3_3')[0]['userDetails']['lastName'] else dag_run.conf['lastname'],
                        "emailAddress": {
                            "emailAddress": (rail.result('bulk_get_users3_3')[0]['userDetails']['emailAddress'] if (dag_run.conf['emailaddress'] == rail.result('bulk_get_users3_3')[0]['userDetails']['emailAddress']) else dag_run.conf['emailaddress']) if rail.result('bulk_get_users3_3')[0]['userDetails']['emailAddress'] else rail.result('bulk_get_users3_3')[0]['userDetails']['emailAddress'],
                        },
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": null,
                        "employeeId": null,
                        "displayNameParameter": null
                    },
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
                    "reportSettingsToApply": null
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_request_company_not_equals_to_datarestget_effective_user_37 = rail.IfOperator(
            task_id='if_request_company_not_equals_to_datarestget_effective_user_37',
            test=lambda dag_run: dag_run.conf['company'] != (
                rail.result('get_effective_user_group_membership_18')['departments'][0]['department']['department']['displayText']
                if rail.result('get_effective_user_group_membership_18') and
                rail.result('get_effective_user_group_membership_18').get('departments')
                else None
            ),
            yes_task="if_request_departmenturi_present_38",
            no_task="if_request_timezone_present_40",
        )

        if_request_departmenturi_present_38 = rail.IfOperator(
            task_id='if_request_departmenturi_present_38',
            test='''{{ dag_run.conf.departmenturi | is_truthy }}''',
            yes_task="apply_user_modifications2department_group_schedule_to_apply_39",
            no_task="if_request_timezone_present_40",
        )

        apply_user_modifications2department_group_schedule_to_apply_39 = rail.RepliconServiceOperator(
            task_id='apply_user_modifications2department_group_schedule_to_apply_39',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri'],
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "departmentGroupScheduleToApply": {
                        "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementDepartmentGroupSchedule": [],
                        "updateDepartmentGroupScheduleOverDateRange": {
                            "replacementDepartmentGroupScheduleEntries": [
                                {
                                    "departmentGroup": {
                                        "uri": dag_run.conf['departmenturi'],
                                        "parent": null,
                                        "name": null,
                                        "parameterCorrelationId": null
                                    },
                                    "effectiveDate": {
                                        "year":  datetime.today().strftime("%Y-%m-%d").split('-')[0],
                                        "month":  datetime.today().strftime("%Y-%m-%d").split('-')[1],
                                        "day":  datetime.today().strftime("%Y-%m-%d").split('-')[2],
                                    }
                                }
                            ],
                            "endDate": null
                        }
                    },
                    "projectRolesToApply": null
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_request_timezone_present_40 = rail.IfOperator(
            task_id='if_request_timezone_present_40',
            test='''{{ dag_run.conf.timezone | is_truthy  and dag_run.conf.timezone != result('bulk_get_users3_3')[0].timeZone.ianaName }}''',
            yes_task="apply_user_modifications2timezone_to_apply_41",
            no_task="if_request_locationuri_present_42",
        )

        apply_user_modifications2timezone_to_apply_41 = rail.RepliconServiceOperator(
            task_id='apply_user_modifications2timezone_to_apply_41',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": {
                        "userTimeZoneModificationOptionUri": "urn:replicon:user-time-zone-modication-option:use-specified-time-zone",
                        "timezone": {
                            "uri": null,
                            "IANAName": "{{ dag_run.conf.timezone }}"
                        }
                    },
                    "userModificationOptionUri": "urn:replicon:user-modification-option:save"
                }
            }
        )

        if_request_locationuri_present_42 = rail.IfOperator(
            task_id='if_request_locationuri_present_42',
            test=lambda dag_run: dag_run.conf['locationuri'] and (
                (rail.result('get_effective_user_group_membership_18')['locations'][0]['location']['location']['uri']
                 if rail.result('get_effective_user_group_membership_18') and
                 rail.result('get_effective_user_group_membership_18').get('locations')
                 else None) != dag_run.conf['locationuri']
            ),
            yes_task="apply_user_modifications2location_schedule_to_apply_43",
            no_task="if_request_costcenterid_present_44",
        )

        apply_user_modifications2location_schedule_to_apply_43 = rail.RepliconServiceOperator(
            task_id='apply_user_modifications2location_schedule_to_apply_43',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri'],
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "locationScheduleToApply": {
                        "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementLocationSchedule": [],
                        "updateLocationScheduleOverDateRange": {
                            "replacementLocationScheduleEntries": [
                                {
                                    "location": {
                                        "uri": dag_run.conf['locationuri'],
                                        "parentUri": null,
                                        "name": null
                                    },
                                    "effectiveDate": {
                                        "year":  datetime.today().strftime("%Y-%m-%d").split('-')[0],
                                        "month":  datetime.today().strftime("%Y-%m-%d").split('-')[1],
                                        "day":  datetime.today().strftime("%Y-%m-%d").split('-')[2],
                                    }
                                }
                            ],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_request_costcenterid_present_44 = rail.IfOperator(
            task_id='if_request_costcenterid_present_44',
            test=lambda dag_run: dag_run.conf['costcenterid'] and (
                dag_run.conf['costcenterid'] != (
                    rail.result('get_effective_user_group_membership_18')['costCenters'][0]['costCenter']['costCenter']['uri']
                    if rail.result('get_effective_user_group_membership_18') and
                    rail.result('get_effective_user_group_membership_18').get('costCenters')
                    else None)
            ),
            yes_task="apply_user_modifications2cost_center_45",
            no_task="if_request_timetype_present_46",
        )

        apply_user_modifications2cost_center_45 = rail.RepliconServiceOperator(
            task_id='apply_user_modifications2cost_center_45',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=lambda dag_run: {
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
                                        "uri": dag_run.conf['costcenterid'],
                                        "parentUri": null,
                                        "name": null
                                    },
                                    "effectiveDate": {
                                        "year":  datetime.today().strftime("%Y-%m-%d").split('-')[0],
                                        "month":  datetime.today().strftime("%Y-%m-%d").split('-')[1],
                                        "day":  datetime.today().strftime("%Y-%m-%d").split('-')[2],
                                    }
                                }
                            ],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_request_timetype_present_46 = rail.IfOperator(
            task_id='if_request_timetype_present_46',
            test=lambda dag_run: dag_run.conf['timetype'] and ((dag_run.conf['employeetypeuri']) != (rail.result('get_effective_user_group_membership_18')['employeeTypes'][0]['employeeType']['employeeType']['uri'] if rail.result(
                'get_effective_user_group_membership_18') and rail.result('get_effective_user_group_membership_18')['employeeTypes'] and rail.result('get_effective_user_group_membership_18')['employeeTypes'][0] and rail.result('get_effective_user_group_membership_18')['employeeTypes'][0]['employeeType'] else null)),
            yes_task="apply_user_modifications2employee_type_group_schedule_to_apply_47",
            no_task="foreach_userdetails_51",
        )

        apply_user_modifications2employee_type_group_schedule_to_apply_47 = rail.RepliconServiceOperator(
            task_id='apply_user_modifications2employee_type_group_schedule_to_apply_47',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=lambda dag_run: {
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
                                        "uri": dag_run.conf['employeetypeuri'],
                                        "parentUri": null,
                                        "name": null
                                    },
                                    "effectiveDate": {
                                        "year":  datetime.today().strftime("%Y-%m-%d").split('-')[0],
                                        "month":  datetime.today().strftime("%Y-%m-%d").split('-')[1],
                                        "day":  datetime.today().strftime("%Y-%m-%d").split('-')[2]
                                    }
                                }
                            ],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_d_errors_present_48 = rail.IfOperator(
            task_id='if_d_errors_present_48',
            test='''{{ result('apply_user_modifications2employee_type_group_schedule_to_apply_47').errors | is_truthy }}''',
            yes_task="stop_49",
            no_task="foreach_userdetails_51",
        )

        stop_49 = rail.FailOperator(
            task_id='stop_49',
            message='''{{ result('apply_user_modifications2employee_type_group_schedule_to_apply_47').errors }}'''
        )

        foreach_userdetails_51 = rail.ForEachOperator(
            task_id='foreach_userdetails_51',
            items="{{ result('bulk_get_users3_3')[0].userDetails.customFieldValues | to_json}}",
            start_task='if_customfield_name_equals_to_jobprofilecode_52',
            end_task='foreach_userdetails_51_end'
        )

        if_customfield_name_equals_to_jobprofilecode_52 = rail.IfOperator(
            task_id='if_customfield_name_equals_to_jobprofilecode_52',
            test='''{{ result('foreach_userdetails_51').customField.name == 'Job Profile Code' }}''',
            yes_task="if_request_jobprofilecode_present_53",
            no_task="if_customfield_name_equals_to_jobprofilename_55",
        )

        if_request_jobprofilecode_present_53 = rail.IfOperator(
            task_id='if_request_jobprofilecode_present_53',
            test='''{{ dag_run.conf.jobprofilecode | is_truthy  and result('foreach_userdetails_51').text != dag_run.conf.jobprofilecode }}''',
            yes_task="update_numeric_value_jobprofilecode_54",
            no_task="if_customfield_name_equals_to_jobprofilename_55",
        )

        update_numeric_value_jobprofilecode_54 = rail.RepliconServiceOperator(
            task_id='update_numeric_value_jobprofilecode_54',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.customfielduri_jobprofilecode }}",
                "value": "{{ dag_run.conf.jobprofilecode }}"
            }
        )

        if_customfield_name_equals_to_jobprofilename_55 = rail.IfOperator(
            task_id='if_customfield_name_equals_to_jobprofilename_55',
            test='''{{ result('foreach_userdetails_51').customField.name == 'Job Profile Name' }}''',
            yes_task="if_request_jobprofilename_present_56",
            no_task="foreach_userdetails_51_end",
        )

        if_request_jobprofilename_present_56 = rail.IfOperator(
            task_id='if_request_jobprofilename_present_56',
            test='''{{ dag_run.conf.jobprofilename | is_truthy  and result('foreach_userdetails_51').text != dag_run.conf.jobprofilename }}''',
            yes_task="update_text_value_jobprofilename_57",
            no_task="foreach_userdetails_51_end",
        )

        update_text_value_jobprofilename_57 = rail.RepliconServiceOperator(
            task_id='update_text_value_jobprofilename_57',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.customfielduri_jobprofilename }}",
                "value": "{{ dag_run.conf.jobprofilename }}"
            }
        )

        foreach_userdetails_51_end = rail.EmptyOperator(
            task_id='foreach_userdetails_51_end',
        )

        if_request_hourlyrate_present_58 = rail.IfOperator(
            task_id='if_request_hourlyrate_present_58',
            test='''{{ dag_run.conf.hourlyrate | is_truthy  and dag_run.conf.hourlyrate != result('bulk_get_users3_3')[0].costRateSchedule[0].hourlyRate.amount }}''',
            yes_task="update_user_cost_rate_schedule_over_date_range_59",
            no_task="if_request_managerid_present_60",
        )

        update_user_cost_rate_schedule_over_date_range_59 = rail.RepliconServiceOperator(
            task_id='update_user_cost_rate_schedule_over_date_range_59',
            endpoint="/services/ResourceService1.svc/UpdateUserCostRateScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "hourlyRate": {
                    "amount": dag_run.conf['hourlyrate'],
                    "currencyUri": dag_run.conf['basecurrencyuri']
                },
                "dateRange": {
                    "startDate": {
                        "year":  datetime.today().strftime("%Y-%m-%d").split('-')[0],
                        "month":  datetime.today().strftime("%Y-%m-%d").split('-')[1],
                        "day":  datetime.today().strftime("%Y-%m-%d").split('-')[2]
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_request_managerid_present_60 = rail.IfOperator(
            task_id='if_request_managerid_present_60',
            test='''{{ dag_run.conf.managerid | is_truthy }}''',
            yes_task="search_users_61",
            no_task="add_success_entry",
        )

        search_users_61 = rail.RepliconServiceOperator(
            task_id='search_users_61',
            endpoint='/services/UserListService1.svc/GetData',
            data=lambda dag_run: {
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:enabled",
                    "urn:replicon:user-list-column:employee-id"
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
                            "text": dag_run.conf['managerid'],
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
        )

        def get_useruri(dag_run):
            result = rail.result('search_users_61')['rows'][0] if rail.result('search_users_61') and rail.result('search_users_61')['rows'] and rail.result('search_users_61')['rows'][0] and rail.result(
                'search_users_61')['rows'][0]['cells'] and rail.result('search_users_61')['rows'][0]['cells'][0] and rail.result('search_users_61')['rows'][0]['cells'][0]['uri'] else null
            if result and result['cells'][3].get('textValue') == dag_run.conf['managerid']:
                return rail.smartjoin_by_delim(result['cells'][0]['uri'], "")
            return None

        log_checkifuserexist_62 = rail.PythonOperator(
            task_id='log_checkifuserexist_62',
            python_callable=lambda dag_run: get_useruri(dag_run) if rail.result(
                'search_users_61') and rail.result('search_users_61')['rows'] and rail.result('search_users_61')['rows'][0] else null
        )


        if_enabled_boolvalue_is_true_63 = rail.IfOperator(
            task_id='if_enabled_boolvalue_is_true_63',
            test=lambda: rail.result('search_users_61')['rows'][0]['cells'][2]['boolValue'] if rail.result('search_users_61') and rail.result('search_users_61')['rows'] and rail.result('search_users_61')['rows'][0] and rail.result('search_users_61')['rows'][0]['cells'] and rail.result('search_users_61')['rows'][0]['cells'][2] and rail.result('search_users_61')['rows'][0]['cells'][2]['boolValue'] else null ,
            yes_task="get_data_getcurrentsupervisor_64",
            no_task="add_success_entry",
        )


        get_data_getcurrentsupervisor_64 = rail.RepliconServiceOperator(
            task_id='get_data_getcurrentsupervisor_64',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:user-list-column:supervisor",
                    "urn:replicon:user-list-column:user"
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
                            "uri": "{{ dag_run.conf.useruri }}",
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
        )

        log_current_supervisor_uri_65 = rail.PythonOperator(
            task_id='log_current_supervisor_uri_65',
            python_callable=lambda:  rail.result('get_data_getcurrentsupervisor_64')[
                'rows'][0]['cells'][0]['uri'] if rail.result('get_data_getcurrentsupervisor_64') and rail.result('get_data_getcurrentsupervisor_64')[
                'rows'] and rail.result('get_data_getcurrentsupervisor_64')[
                'rows'][0] and rail.result('get_data_getcurrentsupervisor_64')[
                'rows'][0]['cells'] and rail.result('get_data_getcurrentsupervisor_64')[
                'rows'][0]['cells'][0] and rail.result('get_data_getcurrentsupervisor_64')[
                'rows'][0]['cells'][0].get('uri') else null
        )

        if_log_current_supervisor_uri_65_present_currentsupervisorisassigned_66 = rail.IfOperator(
            task_id='if_log_current_supervisor_uri_65_present_currentsupervisorisassigned_66',
            test='''{{ result('log_current_supervisor_uri_65') | is_truthy }}''',
            yes_task="if_log_checkifuserexist_62_present_67",
            no_task="if_log_checkifuserexist_62_present_83",
        )

        if_log_checkifuserexist_62_present_67 = rail.IfOperator(
            task_id='if_log_checkifuserexist_62_present_67',
            test='''{{ result('log_checkifuserexist_62') | is_truthy }}''',
            yes_task="if_log_checkifuserexist_62_not_equals_to_dataloggerlog_current_supervisor_uri_65message_68",
            no_task="process_supervisor_child1",
        )

        if_log_checkifuserexist_62_not_equals_to_dataloggerlog_current_supervisor_uri_65message_68 = rail.IfOperator(
            task_id='if_log_checkifuserexist_62_not_equals_to_dataloggerlog_current_supervisor_uri_65message_68',
            test='''{{ result('log_checkifuserexist_62') != result('log_current_supervisor_uri_65') }}''',
            yes_task="get_assigned_permission_sets_for_user2_69",
            no_task="add_success_entry",
        )

        get_assigned_permission_sets_for_user2_69 = rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets_for_user2_69',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('log_checkifuserexist_62') }}"
            }
        )

        log_checkifsupervisorpermissionsetisassigned_70 = rail.PythonOperator(
            task_id='log_checkifsupervisorpermissionsetisassigned_70',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_assigned_permission_sets_for_user2_69'), 'name', 'Supervisor', 'uri', "")
        )

        if_log_checkifsupervisorpermissionsetisassigned_70_present_71 = rail.IfOperator(
            task_id='if_log_checkifsupervisorpermissionsetisassigned_70_present_71',
            test='''{{ result('log_checkifsupervisorpermissionsetisassigned_70') | is_truthy }}''',
            yes_task="update_supervisor_assignment_schedule_over_date_range_72",
            no_task="get_all_permission_sets_74",
        )

        update_supervisor_assignment_schedule_over_date_range_72 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_72',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": rail.result('log_checkifuserexist_62'),
                "dateRange": {
                    "startDate": {
                        "year":  datetime.today().strftime("%Y-%m-%d").split('-')[0],
                        "month":  datetime.today().strftime("%Y-%m-%d").split('-')[1],
                        "day":  datetime.today().strftime("%Y-%m-%d").split('-')[2]
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        get_all_permission_sets_74 = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets_74',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data=None
        )

        log_get_supervisorpermissionuri_75 = rail.PythonOperator(
            task_id='log_get_supervisorpermissionuri_75',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_permission_sets_74'), 'name', 'Supervisor', 'uri', '')
        )

        assign_permission_set_to_user_76 = rail.RepliconServiceOperator(
            task_id='assign_permission_set_to_user_76',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('log_checkifuserexist_62') }}",
                "permissionSetUri": "{{ result('log_get_supervisorpermissionuri_75') }}"
            }
        )

        update_supervisor_assignment_schedule_over_date_range_77 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_77',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": rail.result('log_checkifuserexist_62'),
                "dateRange": {
                    "startDate": {
                        "year":  datetime.today().strftime("%Y-%m-%d").split('-')[0],
                        "month":  datetime.today().strftime("%Y-%m-%d").split('-')[1],
                        "day":  datetime.today().strftime("%Y-%m-%d").split('-')[2]
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        process_supervisor_child1 = rail.TriggerDagRunOperator(
            task_id='process_supervisor_child1',
            retries=0,
            trigger_dag_id=f'frontdoorinc_user_import_create_supervisor_child_{config.instance}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "lastname": dag_run.conf['mangaerdetails']['lastname'],
                "firstname": dag_run.conf['mangaerdetails']['firstname'],
                "timezone": dag_run.conf['mangaerdetails']['timezone'],
                "employeeid": dag_run.conf['mangaerdetails']['employeeid'],
                "company": dag_run.conf['mangaerdetails']['company'],
                "departmenturi": dag_run.conf['mangaerdetails']['departmenturi'],
                "hiredate": dag_run.conf['mangaerdetails']['hiredate'],
                "jobprofilecode": dag_run.conf['mangaerdetails']['jobprofilecode'],
                "timetype": dag_run.conf['mangaerdetails']['timetype'],
                "employeetypeuri": dag_run.conf['mangaerdetails']['employeetypeuri'],
                "managerid": dag_run.conf['mangaerdetails']['managerid'],
                "terminationdate": dag_run.conf['mangaerdetails']['terminationdate'],
                "emailaddress": dag_run.conf['mangaerdetails']['emailaddress'],
                "jobprofilename": dag_run.conf['mangaerdetails']['jobprofilename'],
                "costcenterid": dag_run.conf['mangaerdetails']['costcenterid'],
                "statelocation": dag_run.conf['mangaerdetails']['statelocation'],
                "locationuri": dag_run.conf['mangaerdetails']['managerlocationuri'],
                "costcentername": dag_run.conf['mangaerdetails']['costcentername'],
                "hourlyrate": dag_run.conf['mangaerdetails']['hourlyrate'],
                "customfielduri_jobprofilename": dag_run.conf['mangaerdetails']['customfielduri_jobprofilename'],
                "customfielduri_jobprofilecode": dag_run.conf['mangaerdetails']['customfielduri_jobprofilecode'],
                "customfielduri_adminmodified": dag_run.conf['mangaerdetails']['customfielduri_adminmodified'],
                "jobid": dag_run.conf['job_id'],
                "basecurrencyuri": dag_run.conf['basecurrencyuri'],
                "lookuptable": dag_run.conf['lookuptable'],
            }
        )

        wait_for_process_supervisor_child1 = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_supervisor_child1',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            dag_runs='{{result("process_supervisor_child1")}}'
        )

        if_log_checkifuserexist_62_present_83 = rail.IfOperator(
            task_id='if_log_checkifuserexist_62_present_83',
            test='''{{ result('log_checkifuserexist_62') | is_truthy }}''',
            yes_task="get_assigned_permission_sets_for_user2_84",
            no_task="if_mangaerdetails_employeeid_present_94",
        )

        get_assigned_permission_sets_for_user2_84 = rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets_for_user2_84',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('log_checkifuserexist_62') }}"
            }
        )

        def get_permissionset1():
            record = rail.result('get_assigned_permission_sets_for_user2_84') if rail.result(
                'get_assigned_permission_sets_for_user2_84') else null
            for d in record:
                if d['permissionSet']['name'] == "Supervisor":
                    return d['permissionSet']
            return None

        log_checkifsupervisorpermissionsetisassigned_85 = rail.PythonOperator(
            task_id='log_checkifsupervisorpermissionsetisassigned_85',
            python_callable=lambda: get_permissionset1() if rail.result(
                'get_assigned_permission_sets_for_user2_84') else null
        )

        if_log_checkifsupervisorpermissionsetisassigned_85_present_86 = rail.IfOperator(
            task_id='if_log_checkifsupervisorpermissionsetisassigned_85_present_86',
            test='''{{ result('log_checkifsupervisorpermissionsetisassigned_85') | is_truthy }}''',
            yes_task="update_supervisor_assignment_schedule_over_date_range_87",
            no_task="get_all_permission_sets_89",
        )

        update_supervisor_assignment_schedule_over_date_range_87 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_87',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": rail.result('log_checkifuserexist_62'),
                "dateRange": {
                    "startDate": {
                        "year":  datetime.today().strftime("%Y-%m-%d").split('-')[0],
                        "month":  datetime.today().strftime("%Y-%m-%d").split('-')[1],
                        "day":  datetime.today().strftime("%Y-%m-%d").split('-')[2]
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        get_all_permission_sets_89 = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets_89',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets"
        )

        log_get_supervisorpermissionuri_90 = rail.PythonOperator(
            task_id='log_get_supervisorpermissionuri_90',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_permission_sets_89'), 'name', 'Supervisor', 'uri', "")
        )

        assign_permission_set_to_user_91 = rail.RepliconServiceOperator(
            task_id='assign_permission_set_to_user_91',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('log_checkifuserexist_62') }}",
                "permissionSetUri": "{{ result('log_get_supervisorpermissionuri_90') }}"
            }
        )

        update_supervisor_assignment_schedule_over_date_range_92 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_92',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": rail.result('log_checkifuserexist_62'),
                "dateRange": {
                    "startDate": {
                        "year":  datetime.today().strftime("%Y-%m-%d").split('-')[0],
                        "month":  datetime.today().strftime("%Y-%m-%d").split('-')[1],
                        "day":  datetime.today().strftime("%Y-%m-%d").split('-')[2]
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_mangaerdetails_employeeid_present_94 = rail.IfOperator(
            task_id='if_mangaerdetails_employeeid_present_94',
            test='''{{ dag_run.conf.mangaerdetails.employeeid | is_truthy }}''',
            yes_task="process_supervisor_child1",
            no_task="add_success_entry",
        )

        gather_listdata1 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_listdata1',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('process_supervisor_child1') }}",
            dagrun_task_id='get_supervisor_uri',
            flatten=True
        )

        gather_listdata2 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_listdata2',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('process_supervisor_child1') }}",
            dagrun_task_id='get_supervisoruri',
            flatten=True
        )

        get_response = rail.PythonOperator(
            task_id='get_response',
            python_callable=lambda: rail.smartjoin_by_delim(rail.result(
                'gather_listdata1'), "") or rail.smartjoin_by_delim(rail.result('gather_listdata2'), "")
        )

        if_output_has_uri_present = rail.IfOperator(
            task_id='if_output_has_uri_present',
            test="{{result('get_response') | is_truthy}}",
            yes_task="update_supervisor_assignment_schedule_over_date_range",
            no_task="add_success_entry"
        )

        update_supervisor_assignment_schedule_over_date_range = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": rail.result('get_response'),
                "dateRange": {
                    "startDate": {
                        "year":  datetime.today().strftime("%Y-%m-%d").split('-')[0],
                        "month":  datetime.today().strftime("%Y-%m-%d").split('-')[1],
                        "day":  datetime.today().strftime("%Y-%m-%d").split('-')[2]
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        add_success_entry = rail.WriteLogOperator(
            task_id='add_success_entry',
            log="{{ dag_run.conf.lookuptable }}",
            message="na",
            severity="success",
            properties=lambda dag_run: {
                "username": str(dag_run.conf['firstname']) + " " + str(dag_run.conf['lastname']),
                "employeeid": dag_run.conf['employeeid'],
                "action": "update",
                "status": "success",
                "details": " ",
                "jobid": dag_run.conf['job_id'],
                "childjob": rail.render_template("{{ dag_run_ecid() }}")
            }
        )

        catch_98 = rail.EmptyOperator(
            task_id='catch_98',
            trigger_rule='one_failed',
        )

        frontdoorinc_user_import_logs_add_entry_99 = rail.WriteLogOperator(
            task_id='frontdoorinc_user_import_logs_add_entry_99',
            log="{{dag_run.conf.lookuptable}}",
            message="na",
            severity="failed",
            properties=lambda dag_run: {
                "username": str(dag_run.conf['firstname']) + " " + str(dag_run.conf['lastname']),
                "employeeid": dag_run.conf['employeeid'],
                "action": "update",
                "status": "failed",
                "details": rail.render_template("{{ get_error_message() }}"),
                "jobid": dag_run.conf['job_id'],
                "childjob": rail.render_template("{{ dag_run_ecid() }}")
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> bulk_get_users3_3
        bulk_get_users3_3 >> if_userdetails_isenabled_is_not_true_5
        if_userdetails_isenabled_is_not_true_5 >> rail.Label(
            'Yes') >> enable_login_6 >> apply_user_modifications2_updatestartdate_7 >> if_userdetails_isenabled_is_true_8
        if_userdetails_isenabled_is_not_true_5 >> rail.Label(
            'No') >> if_userdetails_isenabled_is_true_8
        if_userdetails_isenabled_is_true_8 >> rail.Label(
            'Yes') >> if_request_terminationdate_present_9
        if_request_terminationdate_present_9 >> rail.Label(
            'Yes') >> apply_user_modifications2_updatestartdate_11 >> disable_login_12
        disable_login_12 >> frontdoorinc_user_import_logs_add_entry_13 >> catch_98
        if_request_terminationdate_present_9 >> rail.Label(
            'No') >> if_request_emailaddress_present_15
        if_userdetails_isenabled_is_true_8 >> rail.Label(
            'No') >> if_request_emailaddress_present_15
        if_request_emailaddress_present_15 >> rail.Label(
            'Yes') >> if_securityconfiguration_loginname_not_equals_to_dataworkato_service2693ef48requestemailaddress_16
        if_securityconfiguration_loginname_not_equals_to_dataworkato_service2693ef48requestemailaddress_16 >> rail.Label(
            'Yes') >> set_s_s_o_authentication_for_user_updateloginname_17 >> get_effective_user_group_membership_18
        if_securityconfiguration_loginname_not_equals_to_dataworkato_service2693ef48requestemailaddress_16 >> rail.Label(
            'No') >> get_effective_user_group_membership_18
        if_request_emailaddress_present_15 >> rail.Label(
            'No') >> get_effective_user_group_membership_18 >> get_adminmodified_details >> if_customfield_name_equals_to_adminmodified_22
        if_customfield_name_equals_to_adminmodified_22 >> rail.Label(
            'Yes') >> if_customfield_displaytext_equals_to_yes_23
        if_customfield_displaytext_equals_to_yes_23 >> rail.Label(
            'Yes') >> if_request_timetype_present_24
        if_request_timetype_present_24 >> rail.Label(
            'Yes') >> apply_user_modifications2employee_type_group_schedule_to_apply_25 >> if_d_errors_present_26
        if_d_errors_present_26 >> rail.Label(
            'Yes') >> frontdoorinc_user_import_logs_add_entry_27 >> catch_98
        if_d_errors_present_26 >> rail.Label(
            'No') >> frontdoorinc_user_import_logs_add_entry_29
        if_request_timetype_present_24 >> rail.Label(
            'No') >> frontdoorinc_user_import_logs_add_entry_29 >> if_request_emailaddress_present_34
        if_customfield_displaytext_equals_to_yes_23 >> rail.Label(
            'No') >> if_request_emailaddress_present_34
        if_customfield_name_equals_to_adminmodified_22 >> rail.Label(
            'No') >> if_request_emailaddress_present_34
        if_request_emailaddress_present_34 >> rail.Label(
            'Yes') >> if_request_emailaddress_not_equals_to_datarestbulk_get_users3_3responsedfirstuserdetailsemailaddress_35
        if_request_emailaddress_not_equals_to_datarestbulk_get_users3_3responsedfirstuserdetailsemailaddress_35 >> rail.Label(
            'Yes') >> adhoc_http_action_36 >> if_request_company_not_equals_to_datarestget_effective_user_37
        if_request_emailaddress_not_equals_to_datarestbulk_get_users3_3responsedfirstuserdetailsemailaddress_35 >> rail.Label(
            'No') >> if_request_company_not_equals_to_datarestget_effective_user_37
        if_request_emailaddress_present_34 >> rail.Label(
            'No') >> if_request_company_not_equals_to_datarestget_effective_user_37
        if_request_company_not_equals_to_datarestget_effective_user_37 >> rail.Label(
            'Yes') >> if_request_departmenturi_present_38
        if_request_departmenturi_present_38 >> rail.Label(
            'Yes') >> apply_user_modifications2department_group_schedule_to_apply_39 >> if_request_timezone_present_40
        if_request_departmenturi_present_38 >> rail.Label(
            'No') >> if_request_timezone_present_40
        if_request_company_not_equals_to_datarestget_effective_user_37 >> rail.Label(
            'No') >> if_request_timezone_present_40
        if_request_timezone_present_40 >> rail.Label(
            'Yes') >> apply_user_modifications2timezone_to_apply_41 >> if_request_locationuri_present_42
        if_request_timezone_present_40 >> rail.Label(
            'No') >> if_request_locationuri_present_42
        if_request_locationuri_present_42 >> rail.Label(
            'Yes') >> apply_user_modifications2location_schedule_to_apply_43 >> if_request_costcenterid_present_44
        if_request_locationuri_present_42 >> rail.Label(
            'No') >> if_request_costcenterid_present_44
        if_request_costcenterid_present_44 >> rail.Label(
            'Yes') >> apply_user_modifications2cost_center_45 >> if_request_timetype_present_46
        if_request_costcenterid_present_44 >> rail.Label(
            'No') >> if_request_timetype_present_46
        if_request_timetype_present_46 >> rail.Label(
            'Yes') >> apply_user_modifications2employee_type_group_schedule_to_apply_47 >> if_d_errors_present_48
        if_d_errors_present_48 >> rail.Label('Yes') >> stop_49 >> catch_98
        if_d_errors_present_48 >> rail.Label('No') >> foreach_userdetails_51
        if_request_timetype_present_46 >> rail.Label(
            'No') >> foreach_userdetails_51 >> if_customfield_name_equals_to_jobprofilecode_52
        if_customfield_name_equals_to_jobprofilecode_52 >> rail.Label(
            'Yes') >> if_request_jobprofilecode_present_53
        if_request_jobprofilecode_present_53 >> rail.Label(
            'Yes') >> update_numeric_value_jobprofilecode_54 >> if_customfield_name_equals_to_jobprofilename_55
        if_request_jobprofilecode_present_53 >> rail.Label(
            'No') >> if_customfield_name_equals_to_jobprofilename_55
        if_customfield_name_equals_to_jobprofilecode_52 >> rail.Label(
            'No') >> if_customfield_name_equals_to_jobprofilename_55
        if_customfield_name_equals_to_jobprofilename_55 >> rail.Label(
            'Yes') >> if_request_jobprofilename_present_56
        if_request_jobprofilename_present_56 >> rail.Label(
            'Yes') >> update_text_value_jobprofilename_57 >> foreach_userdetails_51_end
        if_request_jobprofilename_present_56 >> rail.Label(
            'No') >> foreach_userdetails_51_end
        if_customfield_name_equals_to_jobprofilename_55 >> rail.Label(
            'No') >> foreach_userdetails_51_end
        foreach_userdetails_51 >> foreach_userdetails_51_end >> if_request_hourlyrate_present_58
        if_request_hourlyrate_present_58 >> rail.Label(
            'Yes') >> update_user_cost_rate_schedule_over_date_range_59 >> if_request_managerid_present_60
        if_request_hourlyrate_present_58 >> rail.Label(
            'No') >> if_request_managerid_present_60
        if_request_managerid_present_60 >> rail.Label(
            'Yes') >> search_users_61 >> log_checkifuserexist_62 >> if_enabled_boolvalue_is_true_63
        if_enabled_boolvalue_is_true_63 >> rail.Label(
            'Yes') >> get_data_getcurrentsupervisor_64 >> log_current_supervisor_uri_65
        log_current_supervisor_uri_65 >> if_log_current_supervisor_uri_65_present_currentsupervisorisassigned_66
        if_log_current_supervisor_uri_65_present_currentsupervisorisassigned_66 >> rail.Label(
            'Yes') >> if_log_checkifuserexist_62_present_67
        if_log_current_supervisor_uri_65_present_currentsupervisorisassigned_66 >> rail.Label(
            'No') >> if_log_checkifuserexist_62_present_83
        if_log_checkifuserexist_62_present_67 >> rail.Label(
            'Yes') >> if_log_checkifuserexist_62_not_equals_to_dataloggerlog_current_supervisor_uri_65message_68
        if_log_checkifuserexist_62_not_equals_to_dataloggerlog_current_supervisor_uri_65message_68 >> rail.Label(
            'Yes') >> get_assigned_permission_sets_for_user2_69 >> log_checkifsupervisorpermissionsetisassigned_70
        log_checkifsupervisorpermissionsetisassigned_70 >> if_log_checkifsupervisorpermissionsetisassigned_70_present_71
        if_log_checkifsupervisorpermissionsetisassigned_70_present_71 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_72 >> add_success_entry
        add_success_entry >> catch_98
        if_log_checkifsupervisorpermissionsetisassigned_70_present_71 >> rail.Label(
            'No') >> get_all_permission_sets_74 >> log_get_supervisorpermissionuri_75
        log_get_supervisorpermissionuri_75 >> assign_permission_set_to_user_76
        assign_permission_set_to_user_76 >> update_supervisor_assignment_schedule_over_date_range_77 >> add_success_entry
        add_success_entry >> catch_98
        if_log_checkifuserexist_62_not_equals_to_dataloggerlog_current_supervisor_uri_65message_68 >> rail.Label(
            'No') >> add_success_entry
        if_log_checkifuserexist_62_present_67 >> rail.Label(
            'No') >> process_supervisor_child1 >> wait_for_process_supervisor_child1
        if_log_checkifuserexist_62_present_83 >> rail.Label(
            'Yes') >> get_assigned_permission_sets_for_user2_84 >> log_checkifsupervisorpermissionsetisassigned_85
        log_checkifsupervisorpermissionsetisassigned_85 >> if_log_checkifsupervisorpermissionsetisassigned_85_present_86
        if_log_checkifsupervisorpermissionsetisassigned_85_present_86 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_87 >> add_success_entry
        if_log_checkifsupervisorpermissionsetisassigned_85_present_86 >> rail.Label(
            'No') >> get_all_permission_sets_89 >> log_get_supervisorpermissionuri_90
        log_get_supervisorpermissionuri_90 >> assign_permission_set_to_user_91
        assign_permission_set_to_user_91 >> update_supervisor_assignment_schedule_over_date_range_92 >> add_success_entry
        add_success_entry >> catch_98
        if_log_checkifuserexist_62_present_83 >> rail.Label(
            'No') >> if_mangaerdetails_employeeid_present_94
        if_mangaerdetails_employeeid_present_94 >> rail.Label(
            'Yes') >> process_supervisor_child1 >> wait_for_process_supervisor_child1 >> gather_listdata1
        gather_listdata1 >> gather_listdata2 >> get_response
        get_response >> if_output_has_uri_present >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range >> add_success_entry >> catch_98
        if_output_has_uri_present >> rail.Label(
            'No') >> add_success_entry >> catch_98
        if_mangaerdetails_employeeid_present_94 >> rail.Label(
            'No') >> add_success_entry >> catch_98
        if_enabled_boolvalue_is_true_63 >> rail.Label(
            'No') >> add_success_entry >> catch_98
        if_request_managerid_present_60 >> rail.Label(
            'No') >> add_success_entry >> catch_98 >> frontdoorinc_user_import_logs_add_entry_99 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
