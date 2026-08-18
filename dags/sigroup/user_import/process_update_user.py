from datetime import datetime, timedelta
from sigroup.user_import.utils import custom_methods, request_payload
import rail
null = None

# pylint: disable=too-many-statements
def create_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.sigroup_update_user_dag_id,
       description="sigroup user import update child",
        max_active_runs=config.child_max_active_runs,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        create_timeoffchange_variable = rail.SetVariableOperator(
            task_id="create_timeoffchange_variable",
            name="timeoff_change_var",
            value=""
        )

        get_exception_logs = rail.SetVariableOperator(
            task_id="get_exception_logs",
            name="excpetion_logs",
            value="",
            append=True
        )

        get_effective_date = rail.PythonOperator(
            task_id="get_effective_date",
            python_callable=lambda dag_run: rail.parse_date(
                dag_run.conf["actioneffectivedate"], "%m/%d/%Y")
            if dag_run.conf["actioneffectivedate"] else
            rail.parse_date(datetime.strftime(datetime.today(), "%m/%d/%Y"), "%m/%d/%Y")
        )

        bulk_get_users = rail.RepliconServiceOperator(
            task_id="bulk_get_users",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                    "users": [
                        {
                            "uri": '{{dag_run.conf.useruri}}',
                            "loginName": null,
                            "parameterCorrelationId": null
                        }
                    ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response:
            {"loginname": response[0]["securityConfiguration"]["loginName"],
                "firstname": response[0]["userDetails"]["firstName"],
                "lastname": response[0]["userDetails"]["lastName"],
                "emailaddress": response[0]["userDetails"]["emailAddress"],
                "displayname": response[0]["userDetails"]["displayText"],
                "isenabled": response[0]["userDetails"]["isEnabled"],
                "timezone": response[0]["timeZone"]["uri"],
                "customfieldvalues": response[0]["userDetails"]["customFieldValues"],
                "payrollrateschedule": response[0]["payrollRateSchedule"],
                "costrateschedule": response[0]["costRateSchedule"],
                "actvities": response[0]["assignedActivities"],
                "payrulescriptschedule": response[0]["payRuleScriptSchedule"],
                "timesheetperiod": response[0]["timesheetPeriodSchedule"],
                "schedulepolicies": response[0]["schedulePolicies"],
                "timeofftemplate": response[0]["timeOffTemplate"],
                "timesheettemplate": response[0]["timesheetTemplate"],
                "timesheetapproval": response[0]["timesheetApprovalPath"],
                "timeoffapproval": response[0]["timeOffApprovalPath"],
                "holidaycalendar": response[0]["holidayCalendar"],
                "uri": response[0]['userDetails']["uri"]
             } if response else null
        )

        get_effective_group_membership = rail.RepliconServiceOperator(
            task_id="get_effective_group_membership",
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data={
                "userUri": '{{dag_run.conf.useruri}}',
                "dateRange": null
            },
            data_handler=lambda response: {
                "departmentgroupschedule": custom_methods.get_effective_group_value(response, "departments", "department"),
                "payrollrateschedule": custom_methods.get_effective_group_value(response, "employeeTypes", "employeeType"),
                "businessunitschedule": custom_methods.get_effective_group_value(response, "divisions", "division"),
                "costcenterschedule": custom_methods.get_effective_group_value(response, "costCenters", "costCenter"),
                "locationschedule": custom_methods.get_effective_group_value(response, "locations", "location"),
                "legalemployersschedule": custom_methods.get_effective_group_value(response, "serviceCenters", "serviceCenter"),
            } if response else null
        )

        is_user_enabled_and_active = rail.IfOperator(
            task_id="is_user_enabled_and_active",
            test=lambda dag_run: bool(rail.result("bulk_get_users")[
                                      "uri"] and rail.result("bulk_get_users")['isenabled'] and
                                      dag_run.conf["status"].lower() == "active"),
            yes_task="if_start_date_and_no_enddate",
            no_task="enable_login"
        )

        enable_login = rail.RepliconServiceOperator(
            task_id="enable_login",
            endpoint="/services/securityService1.svc/EnableLogin",
            data={
                "userUri": '{{dag_run.conf.useruri}}'
            }
        )

        update_timeoffchange_variable = rail.SetVariableOperator(
            task_id="update_timeoffchange_variable",
            name='{{result("create_timeoffchange_variable").name}}',
            value="rehire"
        )

        if_start_date_and_no_enddate = rail.IfOperator(
            task_id="if_start_date_and_no_enddate",
            test=lambda dag_run: bool(
                dag_run.conf["startdate"] and not dag_run.conf["enddate"]),
            yes_task="update_employment_date_range",
            no_task="update_employment_with_end_date"
        )

        update_employment_date_range = rail.RepliconServiceOperator(
            task_id="update_employment_date_range",
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf["useruri"],
                "dateRange": {
                    "startDate": rail.result("get_effective_date"),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        update_employment_with_end_date = rail.RepliconServiceOperator(
            task_id="update_employment_with_end_date",
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf["useruri"],
                "dateRange": {
                    "startDate": rail.result("get_effective_date"),
                    "endDate": rail.parse_date(dag_run.conf["enddate"], "%m/%d/%Y"),
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_new_loginname = rail.IfOperator(
            task_id="if_new_loginname",
            test=lambda dag_run: bool(
                dag_run.conf["loginname"] != rail.result("bulk_get_users")["loginname"]),
            yes_task="if_enabled_auth_uri",
            no_task="if_any_user_basic_attribute_update"
        )

        if_enabled_auth_uri = rail.IfOperator(
            task_id="if_enabled_auth_uri",
            test=lambda dag_run: bool(
                "sso" in dag_run.conf["authenticationtype"]),
            yes_task="enable_sso_auth",
            no_task="enable_replicon_auth"
        )

        enable_sso_auth = rail.RepliconServiceOperator(
            task_id="enable_sso_auth",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data={
                    "user": {
                        "uri": '{{dag_run.conf.useruri}}',
                        "loginName": null,
                        "parameterCorrelationId": null
                    },
                "modifications": {
                        "securitySettingsToApply": {
                            "loginEnabled": "true",
                            "forcePasswordChange": "false",
                            "loginName": '{{dag_run.conf.loginname}}',
                            "ssoName": '{{dag_run.conf.loginname}}',
                            "password": null,
                            "enabledAuthenticationTypeUris": [
                                "urn:replicon:user-authentication-type:sso"
                            ],
                            "userSSONameModificationOptionUri": "urn:replicon:sso-name-modification-option:login-name"
                        }},
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        enable_replicon_auth = rail.RepliconServiceOperator(
            task_id="enable_replicon_auth",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data={
                    "user": {
                        "uri": '{{dag_run.conf.useruri}}',
                        "loginName": null,
                        "parameterCorrelationId": null
                    },
                "modifications": {
                        "securitySettingsToApply": {
                            "loginEnabled": "false",
                            "forcePasswordChange": "false",
                            "loginName": '{{dag_run.conf.loginname}}',
                            "enabledAuthenticationTypeUris": [
                                "urn:replicon:user-authentication-type:replicon"
                            ]
                        }
                        },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_any_user_basic_attribute_update = rail.IfOperator(
            task_id="if_any_user_basic_attribute_update",
            test=lambda dag_run: bool(
                custom_methods.get_user_basic_attribute_update(dag_run)),
            yes_task="update_user_basic_attribute",
            no_task="get_custom_field_values"
        )

        update_user_basic_attribute = rail.RepliconServiceOperator(
            task_id="update_user_basic_attribute",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=custom_methods.get_user_basic_attribute_update
        )

        get_custom_field_values = rail.PythonOperator(
            task_id="get_custom_field_values",
            python_callable=custom_methods.get_custom_fields
        )

        if_custom_text_fields_update = rail.IfOperator(
            task_id="if_custom_text_fields_update",
            test=lambda dag_run: bool(
                custom_methods.get_user_custom_fields_text_update(dag_run)),
            yes_task="update_custom_text_fields",
            no_task="if_custom_date_fields_update"
        )

        update_custom_text_fields = rail.RepliconServiceOperator(
            task_id="update_custom_text_fields",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=custom_methods.get_user_custom_fields_text_update
        )

        if_custom_date_fields_update = rail.IfOperator(
            task_id="if_custom_date_fields_update",
            test=lambda dag_run: bool(
                custom_methods.get_user_custom_date_fields_update(dag_run)),
            yes_task="update_custom_date_fields",
            no_task="if_custom_dropdown_fields_update"
        )

        update_custom_date_fields = rail.RepliconServiceOperator(
            task_id="update_custom_date_fields",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=custom_methods.get_user_custom_date_fields_update
        )

        if_custom_dropdown_fields_update = rail.IfOperator(
            task_id="if_custom_dropdown_fields_update",
            test=lambda dag_run: bool(
                custom_methods.get_user_custom_dropdown_fields_update(dag_run)),
            yes_task="update_custom_dropdown_fields",
            no_task="if_custom_field_fte_update"
        )

        update_custom_dropdown_fields = rail.RepliconServiceOperator(
            task_id="update_custom_dropdown_fields",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=custom_methods.get_user_custom_dropdown_fields_update
        )


        update_custom_field_exception = rail.SetVariableOperator(
            task_id="update_custom_field_exception",
            name='{{result("get_exception_logs").name}}',
            value=custom_methods.get_user_custom_field_exception
        )

        if_custom_field_fte_update = rail.IfOperator(
            task_id="if_custom_field_fte_update",
            test=lambda dag_run: bool(dag_run.conf["fte"] and dag_run.conf["fte"] != rail.result(
                "get_custom_field_values")["fte"]),
            yes_task="update_custom_field_fte",
            no_task="if_department_update"
        )

        update_custom_field_fte = rail.RepliconServiceOperator(
            task_id="update_custom_field_fte",
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data={
                "objectUri": '{{dag_run.conf.useruri}}',
                "customFieldUri": '{{dag_run.conf.fteuri}}',
                "value": '{{dag_run.conf.fte}}'
            }
        )

        if_department_update = rail.IfOperator(
            task_id="if_department_update",
            test=lambda dag_run: bool(
                dag_run.conf["department"] and dag_run.conf["departmentcode"] and dag_run.conf["department"] !=
                custom_methods.get_group_display_text(
                    rail.result("get_effective_group_membership")["departmentgroupschedule"])),
            yes_task="update_department",
            no_task="if_paygroup_update"
        )

        update_department = rail.RepliconServiceOperator(
            task_id="update_department",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=request_payload.get_department_update_payload
        )

        if_paygroup_update = rail.IfOperator(
            task_id="if_paygroup_update",
            test=lambda dag_run: bool(
                dag_run.conf["paygroupcode"] and dag_run.conf["paygroup"] and dag_run.conf["paygroup"] !=
                custom_methods.get_group_display_text(
                    rail.result("get_effective_group_membership")["payrollrateschedule"])
            ),
            yes_task="update_paygroup",
            no_task="if_business_units_update"
        )

        update_paygroup = rail.RepliconServiceOperator(
            task_id="update_paygroup",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=request_payload.get_paygroup_update_payload
        )

        if_business_units_update = rail.IfOperator(
            task_id="if_business_units_update",
            test=lambda dag_run: bool(
                dag_run.conf["businessunit"] and dag_run.conf["businessunitcode"] and dag_run.conf["businessunit"] !=
                custom_methods.get_group_display_text(
                    rail.result("get_effective_group_membership")["businessunitschedule"])
            ),
            yes_task="update_business_units",
            no_task="if_location_update"
        )

        update_business_units = rail.RepliconServiceOperator(
            task_id="update_business_units",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=request_payload.get_business_units_update_payload
        )

        if_location_update = rail.IfOperator(
            task_id="if_location_update",
            test=lambda dag_run: bool(
                dag_run.conf["location"] and dag_run.conf["locationcode"] and dag_run.conf["location"] !=
                custom_methods.get_group_display_text(
                    rail.result("get_effective_group_membership")["locationschedule"])
            ),
            yes_task="update_location",
            no_task="if_finance_costcenter_update"
        )

        update_location = rail.RepliconServiceOperator(
            task_id="update_location",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=request_payload.get_location_update_payload
        )

        update_timeoffchange_variable1 = rail.SetVariableOperator(
            task_id="update_timeoffchange_variable1",
            name='{{result("create_timeoffchange_variable").name}}',
            value="yes"
        )

        if_finance_costcenter_update = rail.IfOperator(
            task_id="if_finance_costcenter_update",
            test=lambda dag_run: bool( dag_run.conf["financecostcentercode"] and
                dag_run.conf["financecostcenter"] and dag_run.conf["financecostcenter"] !=
                custom_methods.get_group_display_text(
                    rail.result("get_effective_group_membership")["costcenterschedule"])
            ),
            yes_task="update_finance_costcenter",
            no_task="if_legal_employer_update"
        )

        update_finance_costcenter = rail.RepliconServiceOperator(
            task_id="update_finance_costcenter",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=request_payload.get_finance_costcenter_update_payload
        )

        if_legal_employer_update = rail.IfOperator(
            task_id="if_legal_employer_update",
            test=lambda dag_run: bool(
                dag_run.conf["legalemployer"] and dag_run.conf["legalemployercode"] and dag_run.conf["legalemployer"] !=
                custom_methods.get_group_display_text(
                    rail.result("get_effective_group_membership")["legalemployersschedule"])
            ),
            yes_task="update_legal_employer",
            no_task="if_initial_supervisor_loginname"
        )

        update_legal_employer = rail.RepliconServiceOperator(
            task_id="update_legal_employer",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=request_payload.get_legal_employer_update_payload
        )

        update_groups_exception = rail.SetVariableOperator(
            task_id="update_groups_exception",
            name='{{result("get_exception_logs").name}}',
            value=custom_methods.get_user_update_groups_exceptions
        )

        if_initial_supervisor_loginname = rail.IfOperator(
            task_id="if_initial_supervisor_loginname",
            test=lambda dag_run: bool(dag_run.conf["initialsupervisorloginname"] and
                                      dag_run.conf["initialsupervisorloginname"] != dag_run.conf["loginname"]),
            yes_task="get_supervisor_assignments",
            no_task="if_time_zone_update"
        )

        get_supervisor_assignments = rail.RepliconServiceOperator(
            task_id="get_supervisor_assignments",
            endpoint="/services/UserService1.svc/GetSupervisorAssignmentDetails",
            data=lambda dag_run: {
                "userUri": dag_run.conf["useruri"],
                "asOfDate": rail.result("get_effective_date")
            },
            data_handler=lambda response: {
                "supervisorloginname": response["supervisor"]["user"]["loginName"]
            } if response and response.get("supervisor") and
            response["supervisor"].get("user") else null
        )

        if_supervisor_update = rail.IfOperator(
            task_id="if_supervisor_update",
            test=lambda dag_run: bool(not rail.result("get_supervisor_assignments") or(
                rail.result("get_supervisor_assignments") and
                                      rail.result("get_supervisor_assignments")["supervisorloginname"] !=
                                      dag_run.conf["initialsupervisorloginname"])),
            yes_task="bulk_get_supervisor_details",
            no_task="if_time_zone_update"
        )

        bulk_get_supervisor_details = rail.RepliconServiceOperator(
            task_id="bulk_get_supervisor_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": null,
                        "loginName": '{{dag_run.conf.initialsupervisorloginname}}',
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: {"uri": response[0]["userDetails"]["uri"],
                                           "isenabled": response[0]["userDetails"]["isEnabled"],
                                           "permissionsets": response[0]["permissionSets"]}
            if response and "userDetails" in response[0] and "permissionSets" in response[0] else null
        )

        if_user_enabled = rail.IfOperator(
            task_id="if_user_enabled",
            test=lambda: bool(rail.result("bulk_get_supervisor_details") and
                              rail.result("bulk_get_supervisor_details")["uri"] and
                              rail.result("bulk_get_supervisor_details")["isenabled"]),
            yes_task="if_supervisor_permission_assigned",
            no_task="write_pending_supervisor_log"
        )

        if_supervisor_permission_assigned = rail.IfOperator(
            task_id="if_supervisor_permission_assigned",
            test=lambda: bool(rail.find_first_by_attr_and_get_attr(
                rail.result("bulk_get_supervisor_details")["permissionsets"],
                "displayText",
                "Supervisor",
                "uri"
            )),
            yes_task="assign_supervisor_for_user",
            no_task="assign_supervisor_permission"
        )

        assign_supervisor_permission = rail.RepliconServiceOperator(
            task_id="assign_supervisor_permission",
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                    "userUri": '{{result("bulk_get_supervisor_details").uri}}',
                    "permissionSetUri": '{{dag_run.conf.supervisorpermissionuri}}'
            }
        )

        assign_supervisor_for_user = rail.RepliconServiceOperator(
            task_id="assign_supervisor_for_user",
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                    "userUri": '{{dag_run.conf.useruri}}',
                    "supervisorUri": '{{result("bulk_get_supervisor_details").uri}}',
                    "dateRange": null
            }
        )

        write_pending_supervisor_log = rail.WriteLogOperator(
            task_id="write_pending_supervisor_log",
            log='{{dag_run.conf.lookuptable}}',
            message="Pending supervisor logs",
            severity="Pending",
            properties=lambda dag_run: {
                **dag_run.conf
            }
        )

        if_time_zone_update = rail.IfOperator(
            task_id="if_time_zone_update",
            test=lambda dag_run: bool(dag_run.conf["timezone"] and dag_run.conf["timezone"] !=
                                      rail.result("bulk_get_users")["timezone"]),
            yes_task="update_time_zone",
            no_task="if_payrate_schedule_present"
        )

        update_time_zone = rail.RepliconServiceOperator(
            task_id="update_time_zone",
            endpoint="/services/InternationalizationService1.svc/UpdateTimeZoneForUser",
            data={
                "userUri": '{{dag_run.conf.useruri}}',
                "timeZoneUri": '{{dag_run.conf.timezone}}'
            }
        )

        if_payrate_schedule_present = rail.IfOperator(
            task_id="if_payrate_schedule_present",
            test=lambda dag_run: bool(dag_run.conf["hourlypayrate"]),
            yes_task="if_payrate_schedule_update",
            no_task="if_costrate_schedule_present"
        )

        if_payrate_schedule_update = rail.IfOperator(
            task_id="if_payrate_schedule_update",
            test=lambda dag_run: bool(custom_methods.is_rate_changed(
                dag_run.conf["hourlypayrate"],
                rail.result("bulk_get_users")["payrollrateschedule"])),
            yes_task="if_payrate_data_present",
            no_task="if_costrate_schedule_present"
        )

        if_payrate_data_present = rail.IfOperator(
            task_id="if_payrate_data_present",
            test=lambda dag_run: bool(
                dag_run.conf["hourlypayratecurrency"] and dag_run.conf["hourlypayeffectivedate"]),
            yes_task="update_payrate_schedule",
            no_task="if_costrate_schedule_present"

        )
        update_payrate_schedule = rail.RepliconServiceOperator(
            task_id="update_payrate_schedule",
            endpoint="/services/PayrollService1.svc/UpdateUserPayrollRateScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf["useruri"],
                "hourlyRate": {
                    "amount": dag_run.conf["hourlypayrate"],
                    "currencyUri": dag_run.conf["hourlypayratecurrency"]
                },
                "dateRange": {
                    "startDate": rail.parse_date(dag_run.conf["hourlypayeffectivedate"], "%m/%d/%Y"),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_costrate_schedule_present = rail.IfOperator(
            task_id="if_costrate_schedule_present",
            test=lambda dag_run: bool(dag_run.conf["hourlycostamount"]),
            yes_task="if_costrate_schedule_update",
            no_task="if_admin_not_modified"
        )

        if_costrate_schedule_update = rail.IfOperator(
            task_id="if_costrate_schedule_update",
            test=lambda dag_run: bool(custom_methods.is_rate_changed(
                dag_run.conf["hourlycostamount"],
                rail.result("bulk_get_users")["costrateschedule"])),
            yes_task="if_costrate_data_present",
            no_task="if_admin_not_modified"
        )

        if_costrate_data_present = rail.IfOperator(
            task_id="if_costrate_data_present",
            test=lambda dag_run: bool(
                dag_run.conf["hourlycostcurrency"] and dag_run.conf["hourlycosteffectivedate"]),
            yes_task="update_costrate_schedule",
            no_task="if_admin_not_modified"

        )
        update_costrate_schedule = rail.RepliconServiceOperator(
            task_id="update_costrate_schedule",
            endpoint="/services/ResourceService1.svc/UpdateUserCostRateScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf["useruri"],
                "hourlyRate": {
                    "amount": dag_run.conf["hourlycostamount"],
                    "currencyUri": dag_run.conf["hourlycostcurrency"]
                },
                "dateRange": {
                    "startDate": rail.parse_date(dag_run.conf["hourlycosteffectivedate"], "%m/%d/%Y"),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_admin_not_modified = rail.IfOperator(
            task_id="if_admin_not_modified",
            test=lambda dag_run: bool(
                not rail.result("get_custom_field_values").get("adminmodified") or
                rail.result("get_custom_field_values").get("adminmodified") ==
                dag_run.conf["adminmodified"]),
            yes_task="if_punchentry_policy_present",
            no_task="if_activity_present"
        )
        if_punchentry_policy_present = rail.IfOperator(
            task_id="if_punchentry_policy_present",
            test=lambda dag_run: bool(dag_run.conf["punchentrypolicy"]),
            yes_task="get_assigned_policy_set",
            no_task="if_activity_present"
        )

        get_assigned_policy_set = rail.RepliconServiceOperator(
            task_id="get_assigned_policy_set",
            endpoint="/services/PolicySetService1.svc/GetAssignedPolicySetsForUser",
            data={
                    "userUri": '{{dag_run.conf.useruri}}'
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response,
                "policyUri",
                "urn:replicon:policy:time-punch",
                "policySet"
            )
        )

        if_punchentry_policy_update = rail.IfOperator(
            task_id="if_punchentry_policy",
            test=lambda dag_run: bool(
                not rail.result("get_assigned_policy_set") or
                rail.result("get_assigned_policy_set")["uri"] !=
                dag_run.conf["punchentrypolicy"]),
            yes_task="update_punchentry_policy",
            no_task="if_activity_present"
        )

        update_punchentry_policy = rail.RepliconServiceOperator(
            task_id="update_punchentry_policy",
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                    "userUri": '{{dag_run.conf.useruri}}',
                    "policySetUri": '{{dag_run.conf.punchentrypolicy}}'
            }
        )

        if_activity_present = rail.IfOperator(
            task_id="if_activity_present",
            test=lambda dag_run: bool(dag_run.conf["activity"] and rail.result(
                "bulk_get_users")["actvities"]),
            yes_task="update_activity_list",
            no_task="if_payrule_script_update"
        )

        update_activity_list = rail.RepliconServiceOperator(
            task_id="update_activity_list",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=request_payload.get_activities_payload
        )

        if_payrule_script_update = rail.IfOperator(
            task_id="if_payrule_script_update",
            test=lambda dag_run: (bool((not rail.result("bulk_get_users")["payrulescriptschedule"] and dag_run.conf["payrule"]) or(
                dag_run.conf["payrule"] and dag_run.conf["payrule"] !=
                  rail.result("bulk_get_users")["payrulescriptschedule"][0]["payRuleScript"]["uri"]))),
            yes_task="update_payrule",
            no_task="if_timesheet_period_present"
        )

        update_payrule = rail.RepliconServiceOperator(
            task_id="update_payrule",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=request_payload.get_payrule_update_schedule
        )

        if_timesheet_period_present = rail.IfOperator(
            task_id="if_timesheet_period_present",
            test=lambda dag_run: bool(dag_run.conf["timesheetperiod"] and (
                not rail.result("bulk_get_users")["timesheetperiod"] or
                not rail.result("bulk_get_users")["timesheetperiod"][0]["timesheetPeriod"]["uri"] or
                rail.result("bulk_get_users")["timesheetperiod"][0]["timesheetPeriod"]["uri"] != dag_run.conf["timesheetperiod"] or
                rail.result("create_timeoffchange_variable")["value"] == "rehire")),
            yes_task="update_timesheet_period",
            no_task="if_timesheet_period_blank"
        )

        if_timesheet_period_blank = rail.IfOperator(
            task_id="if_timesheet_period_blank",
            test=lambda dag_run: bool(
                "timesheetperiod" in dag_run.conf.get("mapperblankfields", []) and
                rail.result("bulk_get_users")["timesheetperiod"] and
                rail.result("bulk_get_users")["timesheetperiod"][0]["timesheetPeriod"]["uri"]),
            yes_task="clear_timesheet_period",
            no_task="if_schedule_policy_update"
        )

        clear_timesheet_period = rail.RepliconServiceOperator(
            task_id="clear_timesheet_period",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf["useruri"],
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timesheetPeriodScheduleToApply": {
                        "userTimesheetPeriodScheduleModificationOptionUri":
                            "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementTimesheetPeriodSchedule": [],
                        "updateTimesheetPeriodScheduleOverDateRange": {
                            "replacementTimesheetPeriodScheduleEntries": [
                                {
                                    "timesheetPeriod": {
                                        "uri": null,
                                        "name": "No timesheet period"
                                    },
                                    "effectiveDate": rail.result("get_effective_date")
                                }
                            ]
                        },
                        "projectRolesToApply": null
                    },
                    "userModificationOptionUri": "urn:replicon:user-modification-option:save"
                }
            }
        )

        update_timesheet_period = rail.RepliconServiceOperator(
            task_id="update_timesheet_period",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=request_payload.get_timesheet_period_update
        )

        if_schedule_policy_update = rail.IfOperator(
            task_id="if_schedule_policy_update",
            test=lambda dag_run: bool(dag_run.conf["scheduletypeuri"] and
                                      dag_run.conf["scheduletypeuri"] !=
                                      custom_methods.get_current_schedule_uri()),
            yes_task="if_location_107",
            no_task="if_timeofftemplate_update"
        )

        if_location_107 = rail.IfOperator(
            task_id="if_location_107",
            test=lambda dag_run: (
                dag_run.conf["location_code"] != "107" and dag_run.conf["scheduletype"] == "Shift Schedule"),
            yes_task="update_shift_schedule",
            no_task="if_office_schedule_update"
        )

        update_shift_schedule = rail.RepliconServiceOperator(
            task_id="update_shift_schedule",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=request_payload.get_shift_schedule_update_payload
        )

        if_office_schedule_update = rail.IfOperator(
            task_id="if_office_schedule_update",
            test=lambda dag_run: (
                dag_run.conf["location_code"] != "107" and dag_run.conf["scheduletypeuri"] and
                not dag_run.conf["scheduletypeuri"].endswith("shift") and
                dag_run.conf["scheduletypeuri"] != custom_methods.get_current_schedule_uri()),
            yes_task="update_office_schedule",
            no_task="if_timeofftemplate_update"
        )

        update_office_schedule = rail.RepliconServiceOperator(
            task_id="update_office_schedule",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=request_payload.get_update_office_schedule_payload
        )

        update_schedule_exception = rail.SetVariableOperator(
            task_id="update_schedule_exception",
            name='{{result("get_exception_logs").name}}',
            value=custom_methods.get_user_update_schedule_exceptions
        )

        if_timeofftemplate_update = rail.IfOperator(
            task_id="if_timeofftemplate_update",
            test=lambda dag_run: bool(dag_run.conf["timeofftemplate"] and (
                                  not rail.result("bulk_get_users")["timeofftemplate"] or
                                  dag_run.conf["timeofftemplate"] != rail.result("bulk_get_users")["timeofftemplate"]["uri"])),
            yes_task="update_timeoff_template",
            no_task="if_timesheettemplate_update"
        )

        update_timeoff_template = rail.RepliconServiceOperator(
            task_id="update_timeoff_template",
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                "userUri": '{{dag_run.conf.useruri}}',
                "policySetUri": '{{dag_run.conf.timeofftemplate}}'
            }
        )

        if_timesheettemplate_update = rail.IfOperator(
            task_id="if_timesheettemplate_update",
            test=lambda dag_run: bool(dag_run.conf["timesheettemplate"] and (
                                  not rail.result("bulk_get_users")["timesheettemplate"] or
                                  dag_run.conf["timesheettemplate"] != rail.result("bulk_get_users")["timesheettemplate"]["uri"])),
            yes_task="update_timesheet_template",
            no_task="if_timesheet_approval"
        )

        update_timesheet_template = rail.RepliconServiceOperator(
            task_id="update_timesheet_template",
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                "userUri": '{{dag_run.conf.useruri}}',
                "policySetUri": '{{dag_run.conf.timesheettemplate}}'
            }
        )

        if_timesheet_approval = rail.IfOperator(
            task_id="if_timesheet_approval",
            test=lambda dag_run: bool(dag_run.conf["timesheetapproval"] and (
                                  not rail.result("bulk_get_users")["timesheetapproval"] or
                                  dag_run.conf["timesheetapproval"] != rail.result("bulk_get_users")["timesheetapproval"]["uri"])),
            yes_task="update_timesheet_approval",
            no_task="if_timeoff_approval"
        )

        update_timesheet_approval = rail.RepliconServiceOperator(
            task_id="update_timesheet_approval",
            endpoint="/services/TimesheetApprovalService1.svc/UpdateApprovalPathForUser",
            data={
                "userUri": '{{dag_run.conf.useruri}}',
                "approvalPathUri": '{{dag_run.conf.timesheetapproval}}'
            }
        )

        if_timeoff_approval = rail.IfOperator(
            task_id="if_timeoff_approval",
            test=lambda dag_run: bool(dag_run.conf["timeoffapproval"] and (
                                  not rail.result("bulk_get_users")["timeoffapproval"] or
                                  dag_run.conf["timeoffapproval"] != rail.result("bulk_get_users")["timeoffapproval"]["uri"])),
            yes_task="update_timeoff_approval",
            no_task="if_holiday_calendar_update"
        )

        update_timeoff_approval = rail.RepliconServiceOperator(
            task_id="update_timeoff_approval",
            endpoint="/services/TimeOffApprovalService1.svc/UpdateApprovalPathForUser",
            data={
                "userUri": '{{dag_run.conf.useruri}}',
                "approvalPathUri": '{{dag_run.conf.timeoffapproval}}'
            }
        )

        if_holiday_calendar_update = rail.IfOperator(
            task_id="if_holiday_calendar_update",
            test=lambda dag_run: bool(dag_run.conf["holidaycalendar"] and (
                                      not rail.result("bulk_get_users")["holidaycalendar"] or
                                      dag_run.conf["holidaycalendar"] !=
                                      rail.result("bulk_get_users")["holidaycalendar"]["uri"])),
            yes_task="update_holiday_calendar",
            no_task="if_timeoff_change_yes"
        )

        update_holiday_calendar = rail.RepliconServiceOperator(
            task_id="update_holiday_calendar",
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
                "userUri": '{{dag_run.conf.useruri}}',
                "holidayCalendarUri": '{{dag_run.conf.holidaycalendar}}'
            }
        )

        if_timeoff_change_yes = rail.IfOperator(
            task_id="if_timeoff_change_yes",
            test=lambda: bool(rail.result("update_timeoffchange_variable1") and
                              rail.result("update_timeoffchange_variable1")["value"] == "yes"),
            yes_task="start_timeoff_assignment",
            no_task="end_timeoff_change"
        )

        start_timeoff_assignment = rail.EmptyOperator(task_id="start_timeoff_assignment")
        process_update_timeoff_assignment = rail.TriggerDagRunOperator(
            task_id="process_update_timeoff_assignment",
            trigger_dag_id=config.sigroup_user_import_timeoff_type_for_update_user,
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run:{
                **dag_run.conf
            }
        )
        end_timeoff_change = rail.EmptyOperator(task_id="end_timeoff_change")

        if_timeoff_change_rehire = rail.IfOperator(
            task_id="if_timeoff_change_rehire",
            test=lambda:bool(rail.result("update_timeoffchange_variable") and
                             rail.result("update_timeoffchange_variable")["value"] == "rehire"),
            yes_task="start_timeoff_assignment_rehire",
            no_task="end_timeoff_assignment_rehire"
        )
        start_timeoff_assignment_rehire = rail.EmptyOperator(task_id="start_timeoff_assignment_rehire")

        process_rehire_timeoff_assignment = rail.TriggerDagRunOperator(
            task_id="process_rehire_timeoff_assignment",
            trigger_dag_id=config.sigroup_user_import_timeoff_type_for_rehire_user,
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run:{
                **dag_run.conf
            }
        )

        end_timeoff_assignment_rehire = rail.EmptyOperator(task_id="end_timeoff_assignment_rehire")

        if_exception_logs = rail.IfOperator(
            task_id="if_exception_logss",
            test=lambda: bool("".join(rail.result("get_exception_logs")["value"])),
            yes_task="write_log_exceptions_log",
            no_task="write_log_user_update_successful"
        )

        write_log_exceptions_log = rail.WriteLogOperator(
            task_id="write_log_exceptions_log",
            log='{{dag_run.conf.lookuptable}}',
            message="Exceptions for user attributes",
            severity="Exception",
            properties=lambda dag_run: {
                
                "EmployeeId": dag_run.conf["employeeid"],
                "Username": dag_run.conf["firstname"] + dag_run.conf["lastname"],
                "Action": "Update",
                "Status": "Exception",
                "Details": "User updated partially - " + ";".join(rail.result("get_exception_logs")["value"]),
                
            }
        )

        write_log_user_update_successful = rail.WriteLogOperator(
            task_id="write_log_user_update_successful",
            log='{{dag_run.conf.lookuptable}}',
            message="User updated successfully",
            properties=lambda dag_run: {
                
                "EmployeeId": dag_run.conf["employeeid"],
                "Username": dag_run.conf["firstname"] + dag_run.conf["lastname"],
                "Action": "Update",
                "Status": "Success",
                "Details": "User updated successfully",
                
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{dag_run.conf.lookuptable}}',
            message="User update failed",
            severity="Error",
            trigger_rule="one_failed",
            properties=lambda dag_run: {
                
                "EmployeeId": dag_run.conf["employeeid"],
                "Username": dag_run.conf["firstname"] + dag_run.conf["lastname"],
                "Action": "Update",
                "Status": "Error",
                "Details": rail.render_template('{{get_error_message()}}'),
                
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id="log_to_sumo",
            sumo_conn_id="sumologic-dagrunlogger",
            trigger_rule="all_done"
        )

        create_timeoffchange_variable >>\
        get_exception_logs>>\
            get_effective_date >> bulk_get_users >> get_effective_group_membership >>\
            is_user_enabled_and_active >> rail.Label("Yes") >>\
            enable_login >> update_timeoffchange_variable >> if_start_date_and_no_enddate
        is_user_enabled_and_active >> rail.Label("No") >>\
            if_start_date_and_no_enddate >> rail.Label("Yes") >>\
            update_employment_date_range >> if_new_loginname
        if_start_date_and_no_enddate >> rail.Label("No") >>\
            update_employment_with_end_date >>\
            if_new_loginname >> rail.Label("Yes") >>\
            if_enabled_auth_uri >> rail.Label("Yes") >> enable_sso_auth >>\
            if_any_user_basic_attribute_update
        if_enabled_auth_uri >> rail.Label(
            "No") >> enable_replicon_auth >> if_any_user_basic_attribute_update
        if_new_loginname >> rail.Label("No") >>\
            if_any_user_basic_attribute_update >> rail.Label("Yes") >>\
            update_user_basic_attribute >> get_custom_field_values
        if_any_user_basic_attribute_update >> rail.Label("No") >>\
            get_custom_field_values >>\
            if_custom_text_fields_update >> rail.Label(
                "Yes") >> update_custom_text_fields >> if_custom_date_fields_update
        if_custom_text_fields_update >> rail.Label("No") >>\
            if_custom_date_fields_update >> rail.Label(
                "Yes") >> update_custom_date_fields >> if_custom_dropdown_fields_update
        if_custom_date_fields_update >> rail.Label("No") >>\
            if_custom_dropdown_fields_update >> rail.Label(
                "Yes") >> update_custom_dropdown_fields >>\
        update_custom_field_exception >> if_custom_field_fte_update
        if_custom_dropdown_fields_update >> rail.Label("No") >> \
            if_custom_field_fte_update >> rail.Label(
                "Yes") >> update_custom_field_fte >> if_department_update
        if_custom_field_fte_update >> rail.Label("No") >>\
            if_department_update >> rail.Label(
                "Yes") >> update_department >> if_paygroup_update
        if_department_update >> rail.Label("No") >>\
            if_paygroup_update >> rail.Label(
                "Yes") >> update_paygroup >> if_business_units_update
        if_paygroup_update >> rail.Label("No") >>\
            if_business_units_update >> rail.Label(
                "Yes") >> update_business_units >> if_location_update
        if_business_units_update >> rail.Label("No") >>\
            if_location_update >> rail.Label(
                "Yes") >> update_location >> update_timeoffchange_variable1 >> if_finance_costcenter_update
        if_location_update >> rail.Label("No") >>\
            if_finance_costcenter_update >> rail.Label(
                "Yes") >> update_finance_costcenter >> if_legal_employer_update
        if_finance_costcenter_update >> rail.Label("No") >>\
            if_legal_employer_update >> rail.Label(
                "Yes") >> update_legal_employer >> update_groups_exception >>\
        if_initial_supervisor_loginname
        if_legal_employer_update >> rail.Label("No") >>\
            if_initial_supervisor_loginname >> rail.Label(
                "No") >> if_time_zone_update
        if_initial_supervisor_loginname >> rail.Label("Yes") >>\
            get_supervisor_assignments >>\
            if_supervisor_update >> rail.Label("No") >> if_time_zone_update
        if_supervisor_update >> rail.Label("Yes") >>\
            bulk_get_supervisor_details >>\
            if_user_enabled >> rail.Label(
                "Yes") >> if_supervisor_permission_assigned
        if_user_enabled >> rail.Label("No") >> write_pending_supervisor_log >> if_time_zone_update
        if_supervisor_permission_assigned >> rail.Label("Yes") >>\
            assign_supervisor_for_user >> if_time_zone_update
        if_supervisor_permission_assigned >> rail.Label("No") >>\
            assign_supervisor_permission >> assign_supervisor_for_user >>\
            if_time_zone_update >> rail.Label("Yes") >> update_time_zone >>\
            if_payrate_schedule_present
        if_time_zone_update >> rail.Label("No") >>\
            if_payrate_schedule_present >> rail.Label("No") >>\
            if_costrate_schedule_present
        if_payrate_schedule_present >> rail.Label("Yes") >>\
            if_payrate_schedule_update >> rail.Label("Yes") >>\
            if_payrate_data_present >> rail.Label("No") >>\
            if_costrate_schedule_present
        if_payrate_data_present >> rail.Label("Yes") >>\
            update_payrate_schedule >> if_costrate_schedule_present
        if_payrate_schedule_update >> rail.Label("No") >>\
            if_costrate_schedule_present >> rail.Label(
                "No") >> if_admin_not_modified
        if_costrate_schedule_present >> rail.Label("Yes") >>\
        if_costrate_schedule_update >> rail.Label(
            "No") >> if_admin_not_modified
        if_costrate_schedule_update >> rail.Label("Yes") >>\
        if_costrate_data_present >> rail.Label("Yes") >>\
        update_costrate_schedule >> if_admin_not_modified
        if_costrate_data_present >> rail.Label("No") >>\
        if_admin_not_modified >> rail.Label("No") >> if_activity_present
        if_admin_not_modified >> rail.Label("Yes") >>\
        if_punchentry_policy_present >> rail.Label("No") >>\
        if_activity_present
        if_punchentry_policy_present >>\
        rail.Label("Yes") >> get_assigned_policy_set >>\
        if_punchentry_policy_update >> rail.Label("Yes") >> update_punchentry_policy >>\
        if_activity_present
        if_punchentry_policy_update >> rail.Label("No") >>\
        if_activity_present >> rail.Label("No") >>\
        if_payrule_script_update
        if_activity_present >> rail.Label("Yes") >> update_activity_list >>\
        if_payrule_script_update >> rail.Label("Yes") >>\
        update_payrule >> if_timesheet_period_present
        if_payrule_script_update >> rail.Label("No") >>\
        if_timesheet_period_present >> rail.Label("No") >> if_timesheet_period_blank
        if_timesheet_period_blank >> rail.Label("Yes") >> clear_timesheet_period >>\
        if_schedule_policy_update
        if_timesheet_period_blank >> rail.Label("No") >> if_schedule_policy_update
        if_timesheet_period_present >> rail.Label("Yes") >> update_timesheet_period >>\
        if_schedule_policy_update >> rail.Label("No") >>\
        if_timeofftemplate_update
        if_schedule_policy_update >> rail.Label("Yes") >>\
        if_location_107 >> rail.Label("No") >> if_office_schedule_update
        if_location_107 >> rail.Label("Yes") >> update_shift_schedule >>\
        if_timeofftemplate_update
        if_office_schedule_update >> rail.Label("Yes") >>\
        update_office_schedule >> update_schedule_exception >> if_timeofftemplate_update
        if_office_schedule_update >> rail.Label("No") >>\
        if_timeofftemplate_update >> rail.Label("Yes") >> update_timeoff_template >>\
        if_timesheettemplate_update
        if_timeofftemplate_update >> rail.Label("No") >>\
        if_timesheettemplate_update >> rail.Label("Yes") >>\
        update_timesheet_template >> if_timesheet_approval
        if_timesheettemplate_update >> rail.Label("No") >>\
        if_timesheet_approval >> rail.Label("Yes") >>\
        update_timesheet_approval >> if_timeoff_approval
        if_timesheet_approval >> rail.Label("No") >>\
        if_timeoff_approval >> rail.Label("Yes") >>\
        update_timeoff_approval >> if_holiday_calendar_update
        if_timeoff_approval >> rail.Label("No") >>\
        if_holiday_calendar_update >> rail.Label("Yes") >>\
        update_holiday_calendar >> if_timeoff_change_yes
        if_holiday_calendar_update >> rail.Label("No") >>\
        if_timeoff_change_yes >> rail.Label("Yes") >>\
        start_timeoff_assignment>>\
        process_update_timeoff_assignment >> end_timeoff_change
        if_timeoff_change_yes >> rail.Label("No") >>\
        end_timeoff_change>>\
        if_timeoff_change_rehire >> rail.Label("Yes") >>\
        start_timeoff_assignment_rehire>>\
        process_rehire_timeoff_assignment >> end_timeoff_assignment_rehire
        if_timeoff_change_rehire >> rail.Label("No") >>\
        end_timeoff_assignment_rehire>>\
        if_exception_logs >> rail.Label("Yes") >> write_log_exceptions_log >>\
        catch_and_log_errors
        if_exception_logs >> rail.Label("No") >> write_log_user_update_successful >>\
        catch_and_log_errors >> log_to_sumo
        return dag


rail.for_each_instance(create_airflow_dag)
