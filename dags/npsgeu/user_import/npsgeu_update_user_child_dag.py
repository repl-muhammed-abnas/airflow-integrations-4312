
from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'npsgeu_user_import_update_user_child_{config.instance}',
        description=f'NPSGEU_update_user V1.0 {config.instance}',
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
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='bulk_get_users3_4'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='bulk_get_users3_4',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        bulk_get_users3_4 = rail.RepliconServiceOperator(
            task_id='bulk_get_users3_4',
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

        if_userdetails_isenabled_is_not_true_6 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_not_true_6',
            test='''{{ result('bulk_get_users3_4')[0].userDetails.isEnabled | is_falsy }}''',
            yes_task="enable_login_7",
            no_task="if_userdetails_isenabled_is_true_9",
        )

        enable_login_7 = rail.RepliconServiceOperator(
            task_id='enable_login_7',
            endpoint="/services/securityService1.svc/EnableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        apply_user_modifications2_updatestartdate_8 = rail.RepliconServiceOperator(
            task_id='apply_user_modifications2_updatestartdate_8',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ dag_run.conf.startdate_DS.year }}",
                        "month": "{{ dag_run.conf.startdate_DS.month }}",
                        "day": "{{ dag_run.conf.startdate_DS.day }}"
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_userdetails_isenabled_is_true_9 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_true_9',
            test='''{{ result('bulk_get_users3_4')[0].userDetails.isEnabled | is_truthy }}''',
            yes_task="if_request_enddate_present_10",
            no_task="get_effective_user_group_membership_15",
        )

        if_request_enddate_present_10 = rail.IfOperator(
            task_id='if_request_enddate_present_10',
            test='''{{ dag_run.conf.enddate | is_truthy }}''',
            yes_task="apply_user_modifications2_updatestartdate_12",
            no_task="get_effective_user_group_membership_15",
        )

        apply_user_modifications2_updatestartdate_12 = rail.RepliconServiceOperator(
            task_id='apply_user_modifications2_updatestartdate_12',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ dag_run.conf.startdate_DS.year }}",
                        "month": "{{ dag_run.conf.startdate_DS.month }}",
                        "day": "{{ dag_run.conf.startdate_DS.day }}"
                    },
                    "endDate":  {
                        "year": "{{ dag_run.conf.enddate_DS.year }}",
                        "month": "{{ dag_run.conf.enddate_DS.month }}",
                        "day": "{{ dag_run.conf.enddate_DS.day }}"
                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        get_effective_user_group_membership_15 = rail.RepliconServiceOperator(
            task_id='get_effective_user_group_membership_15',
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "dateRange": null
            }
        )

        if_request_email_present_21 = rail.IfOperator(
            task_id='if_request_email_present_21',
            test='''{{ dag_run.conf.email | is_truthy }}''',
            yes_task="if_request_email_not_equals_to_datarestbulk_get_users3_4responsedfirstuserdetailsemailaddress_22",
            no_task="if_locationuri_present",
        )

        if_request_email_not_equals_to_datarestbulk_get_users3_4responsedfirstuserdetailsemailaddress_22 = rail.IfOperator(
            task_id='if_request_email_not_equals_to_datarestbulk_get_users3_4responsedfirstuserdetailsemailaddress_22',
            #pylint: disable = line-too-long
            test='''{{ dag_run.conf.email != result('bulk_get_users3_4')[0].userDetails.emailAddress  or dag_run.conf.firstname != result('bulk_get_users3_4')[0].userDetails.firstName  or dag_run.conf.lastname != result('bulk_get_users3_4')[0].userDetails.lastName }}''',
            yes_task="adhoc_http_action_23",
            no_task="if_locationuri_present",
        )

        adhoc_http_action_23 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_23',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri']
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save",
                "modifications": {
                    "userDetailsToApply": {
                        "firstName": null if dag_run.conf['firstname'] == rail.result(
                            'bulk_get_users3_4')[0]['userDetails']['firstName'] else dag_run.conf['firstname'],
                        "emailAddress": {
                            "emailAddress": ((rail.result('bulk_get_users3_4')[0]['userDetails']['emailAddress'] if dag_run.conf[
                                'email'] == rail.result('bulk_get_users3_4')[0]['userDetails']['emailAddress'] else dag_run.conf[
                                'email']) if rail.result('bulk_get_users3_4')[0]['userDetails']['emailAddress'] else rail.result(
                                'bulk_get_users3_4')[0]['userDetails']['emailAddress'])
                        },
                        "lastName": null if dag_run.conf['lastname'] == rail.result('bulk_get_users3_4')[0]['userDetails']['lastName'] else dag_run.conf[
                            'lastname']
                    }
                }
            }
        )

        if_locationuri_present = rail.IfOperator(
            task_id='if_locationuri_present',
            test='''{{ dag_run.conf.locationuri | is_truthy }}''',
            yes_task="if_location_uri_not_equals_to_dataworkato_service2693ef48requestlocationuri_25",
            no_task="if_request_holidaycalendar_present_27",
        )

        if_location_uri_not_equals_to_dataworkato_service2693ef48requestlocationuri_25 = rail.IfOperator(
            task_id='if_location_uri_not_equals_to_dataworkato_service2693ef48requestlocationuri_25',
            test=lambda dag_run: dag_run.conf['locationuri'] != (rail.result(
                'get_effective_user_group_membership_15')['locations'][0]['location']['location']['uri'] if rail.result(
                'get_effective_user_group_membership_15') and rail.result(
                'get_effective_user_group_membership_15')['locations'] and rail.result(
                'get_effective_user_group_membership_15')['locations'][0]['location'] and rail.result(
                'get_effective_user_group_membership_15')['locations'][0]['location']['location']['uri'] else ''),
            yes_task="apply_user_modifications2_locationupdate_26",
            no_task="if_request_holidaycalendar_present_27",
        )

        apply_user_modifications2_locationupdate_26 = rail.RepliconServiceOperator(
            task_id='apply_user_modifications2_locationupdate_26',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
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
                                        "uri": "{{ dag_run.conf.locationuri }}",
                                        "parentUri": null,
                                        "name": null
                                    },
                                    "effectiveDate": {
                                        "year": "{{ dag_run.conf.today_DS.year }}",
                                        "month": "{{ dag_run.conf.today_DS.month }}",
                                        "day": "{{ dag_run.conf.today_DS.day }}"
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

        if_request_holidaycalendar_present_27 = rail.IfOperator(
            task_id='if_request_holidaycalendar_present_27',
            test=lambda dag_run: dag_run.conf['holidaycalendar'] and dag_run.conf['holidaycalendar'] != (rail.result(
                'bulk_get_users3_4')[0]['holidayCalendar']['name'] if rail.result('bulk_get_users3_4') and rail.result(
                'bulk_get_users3_4')[0]['holidayCalendar'] else ''),
            yes_task="apply_user_modifications2holiday_calendar_to_apply_28",
            no_task="if_request_timezone_present_29",
        )

        apply_user_modifications2holiday_calendar_to_apply_28 = rail.RepliconServiceOperator(
            task_id='apply_user_modifications2holiday_calendar_to_apply_28',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "holidayCalendarToApply": {
                        "holidayCalendar": {
                            "uri": null,
                            "name": "{{ dag_run.conf.holidaycalendar }}"
                        }
                    },
                    "userModificationOptionUri": "urn:replicon:user-modification-option:save"
                }
            }
        )

        if_request_timezone_present_29 = rail.IfOperator(
            task_id='if_request_timezone_present_29',
            test=lambda dag_run: dag_run.conf['timezone'] and dag_run.conf['timezone'] != (rail.result(
                'bulk_get_users3_4')[0]['timeZone']['ianaName'] if rail.result('bulk_get_users3_4') and rail.result(
                'bulk_get_users3_4')[0]['timeZone'] and rail.result('bulk_get_users3_4')[0]['timeZone']['ianaName'] else ''),
            yes_task="apply_user_modifications2timezone_to_apply_30",
            no_task="foreach_response_31",
        )

        apply_user_modifications2timezone_to_apply_30 = rail.RepliconServiceOperator(
            task_id='apply_user_modifications2timezone_to_apply_30',
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

        foreach_response_31 = rail.ForEachOperator(
            task_id='foreach_response_31',
            items=lambda: rail.result('bulk_get_users3_4'),
            start_task='foreach_userdetails_32',
            end_task='foreach_response_31_end'
        )

        foreach_userdetails_32 = rail.ForEachOperator(
            task_id='foreach_userdetails_32',
            items=lambda: rail.result('foreach_response_31')[
                'userDetails']['customFieldValues'],
            start_task='if_customfield_name_equals_to_division_33',
            end_task='foreach_userdetails_32_end'
        )

        if_customfield_name_equals_to_division_33 = rail.IfOperator(
            task_id='if_customfield_name_equals_to_division_33',
            test='''{{ result('foreach_userdetails_32').customField.name == 'Division' }}''',
            yes_task="if_request_division_present_34",
            no_task="if_customfield_name_equals_to_position_36",
        )

        if_request_division_present_34 = rail.IfOperator(
            task_id='if_request_division_present_34',
            test='''{{ dag_run.conf.division | is_truthy  and result('foreach_userdetails_32').text != dag_run.conf.division }}''',
            yes_task="update_dropdown_value_division_35",
            no_task="if_customfield_name_equals_to_position_36",
        )

        update_dropdown_value_division_35 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_division_35',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.udfuri_division }}",
                "customFieldDropDownOptionUri": "{{ dag_run.conf.divisionuri }}"
            }
        )

        if_customfield_name_equals_to_position_36 = rail.IfOperator(
            task_id='if_customfield_name_equals_to_position_36',
            test='''{{ result('foreach_userdetails_32').customField.name == 'Position' }}''',
            yes_task="if_request_position_present_37",
            no_task="if_customfield_name_equals_to_employeestate_39",
        )

        if_request_position_present_37 = rail.IfOperator(
            task_id='if_request_position_present_37',
            test='''{{ dag_run.conf.position | is_truthy  and result('foreach_userdetails_32').text != dag_run.conf.position }}''',
            yes_task="update_dropdown_value_position_38",
            no_task="if_customfield_name_equals_to_employeestate_39",
        )

        update_dropdown_value_position_38 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_position_38',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.udfuri_position }}",
                "customFieldDropDownOptionUri": "{{ dag_run.conf.positionuri }}"
            }
        )

        if_customfield_name_equals_to_employeestate_39 = rail.IfOperator(
            task_id='if_customfield_name_equals_to_employeestate_39',
            test='''{{ result('foreach_userdetails_32').customField.name == 'Employee state' }}''',
            yes_task="if_request_employeestate_present_40",
            no_task="if_customfield_name_equals_to_employeecity_42",
        )

        if_request_employeestate_present_40 = rail.IfOperator(
            task_id='if_request_employeestate_present_40',
            test='''{{ dag_run.conf.employeestate | is_truthy  and result('foreach_userdetails_32').text != dag_run.conf.employeestate }}''',
            yes_task="update_dropdown_value_employeestate_41",
            no_task="if_customfield_name_equals_to_employeecity_42",
        )

        update_dropdown_value_employeestate_41 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_employeestate_41',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.udfuri_employeestate }}",
                "customFieldDropDownOptionUri": "{{ dag_run.conf.employeestateuri }}"
            }
        )

        if_customfield_name_equals_to_employeecity_42 = rail.IfOperator(
            task_id='if_customfield_name_equals_to_employeecity_42',
            test='''{{ result('foreach_userdetails_32').customField.name == 'Employee City' }}''',
            yes_task="if_request_employeecity_present_43",
            no_task="foreach_userdetails_32_end",
        )

        if_request_employeecity_present_43 = rail.IfOperator(
            task_id='if_request_employeecity_present_43',
            test='''{{ dag_run.conf.employeecity | is_truthy  and result('foreach_userdetails_32').text != dag_run.conf.employeecity }}''',
            yes_task="update_text_value_employeecity_44",
            no_task="foreach_userdetails_32_end",
        )

        update_text_value_employeecity_44 = rail.RepliconServiceOperator(
            task_id='update_text_value_employeecity_44',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.udfuri_employeecity }}",
                "value": "{{ dag_run.conf.employeecity }}"
            }
        )

        foreach_userdetails_32_end = rail.EmptyOperator(
            task_id='foreach_userdetails_32_end',
        )

        foreach_response_31_end = rail.EmptyOperator(
            task_id='foreach_response_31_end',
        )

        if_request_timesheettemplate_present_45 = rail.IfOperator(
            task_id='if_request_timesheettemplate_present_45',
            test=lambda dag_run: dag_run.conf['timesheettemplate'] and dag_run.conf['timesheettemplate'] != (rail.result(
                'bulk_get_users3_4')[0]['timesheetTemplate']['uri'] if rail.result('bulk_get_users3_4') and rail.result(
                'bulk_get_users3_4')[0]['timesheetTemplate'] else '') ,
            yes_task="assign_policy_set_to_user_46",
            no_task="if_request_payrule_present_47",
        )

        assign_policy_set_to_user_46 = rail.RepliconServiceOperator(
            task_id='assign_policy_set_to_user_46',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "policySetUri": "{{ dag_run.conf.timesheettemplate }}"
            }
        )

        if_request_payrule_present_47 = rail.IfOperator(
            task_id='if_request_payrule_present_47',
            test=lambda dag_run: dag_run.conf['payrule'] and dag_run.conf['payrule'] != (rail.result(
                'bulk_get_users3_4')[0]['payRuleScriptSchedule'][0]['payRuleScript']['displayText'] if rail.result(
                'bulk_get_users3_4') and rail.result('bulk_get_users3_4')[0]['payRuleScriptSchedule'] and rail.result(
                'bulk_get_users3_4')[0]['payRuleScriptSchedule'][0]['payRuleScript'] else ''),
            yes_task="apply_user_modifications2_48",
            no_task="if_request_departmenturi_present_49",
        )

        apply_user_modifications2_48 = rail.RepliconServiceOperator(
            task_id='apply_user_modifications2_48',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri'],
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "payRulesScheduleModifications": {
                        "scheduleEntries": [
                            {
                                "payRuleScript": {
                                    "uri": null,
                                    "name": dag_run.conf['payrule']
                                },
                                "effectiveDate": {
                                    "year": dag_run.conf['effective_DS']['year'] if dag_run.conf['effective_DS']['year'] else dag_run.conf[
                                        'today_DS']['year'],
                                    "month": dag_run.conf['effective_DS']['month'] if dag_run.conf['effective_DS']['month'] else dag_run.conf[
                                        'today_DS']['month'],
                                    "day": dag_run.conf['effective_DS']['day'] if dag_run.conf['effective_DS']['day'] else dag_run.conf[
                                        'today_DS']['day']
                                }
                            }
                        ]
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_request_departmenturi_present_49 = rail.IfOperator(
            task_id='if_request_departmenturi_present_49',
            test=lambda dag_run: dag_run.conf['departmenturi'] and dag_run.conf['departmenturi'] != (rail.result(
                'bulk_get_users3_4')[0]['userDetails']['department']['uri'] if rail.result('bulk_get_users3_4') and rail.result(
                'bulk_get_users3_4')[0]['userDetails'] and rail.result('bulk_get_users3_4')[0]['userDetails']['department'] and rail.result(
                'bulk_get_users3_4')[0]['userDetails']['department']['uri'] else ''),
            yes_task="update_department_for_user_50",
            no_task="if_request_employeetypeuri_present_51",
        )

        update_department_for_user_50 = rail.RepliconServiceOperator(
            task_id='update_department_for_user_50',
            endpoint="/services/DepartmentService1.svc/UpdateDepartmentForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "departmentUri": "{{ dag_run.conf.departmenturi }}"
            }
        )

        if_request_employeetypeuri_present_51 = rail.IfOperator(
            task_id='if_request_employeetypeuri_present_51',
            test=lambda dag_run: dag_run.conf['employeetypeuri'] and dag_run.conf['employeetypeuri'] != (rail.result(
                'bulk_get_users3_4')[0]['employeeType']['uri'] if rail.result('bulk_get_users3_4') and rail.result(
                'bulk_get_users3_4')[0]['employeeType'] and rail.result('bulk_get_users3_4')[0]['employeeType']['uri'] else ''),
            yes_task="update_employee_type_for_user_52",
            no_task="if_request_supervisorid_present_53",
        )

        update_employee_type_for_user_52 = rail.RepliconServiceOperator(
            task_id='update_employee_type_for_user_52',
            endpoint="/services/EmployeeTypeService1.svc/UpdateEmployeeTypeForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "employeeTypeUri": "{{ dag_run.conf.employeetypeuri }}"
            }
        )

        if_request_supervisorid_present_53 = rail.IfOperator(
            task_id='if_request_supervisorid_present_53',
            test='''{{ dag_run.conf.supervisorid | is_truthy }}''',
            yes_task="search_users_54",
            no_task="if_request_primaryroles_present_87",
        )

        def get_supervisoruser_details(response, dag_run):
            users_found = response['rows']
            required_user = {}
            for user in users_found:
                if user['cells'][1]['textValue'] == dag_run.conf['supervisorid']:
                    required_user = user
                    break
            return {
                'user': required_user if users_found and required_user else '',
                'useruri': required_user['cells'][0]['uri'] if users_found and required_user else '',
                'enabled': required_user['cells'][2]['boolValue'] if users_found and required_user else False,
            }

        search_users_54 = rail.RepliconServiceOperator(
            task_id='search_users_54',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:employee-id",
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
                            "text": "{{dag_run.conf.supervisorid}}"
                        }
                    }
                }
            },
            data_handler=get_supervisoruser_details
        )

        if_log_checkifuserexist_55_present_56 = rail.IfOperator(
            task_id='if_log_checkifuserexist_55_present_56',
            test='''{{ result('search_users_54').useruri | is_truthy }}''',
            yes_task="get_data_getcurrentsupervisor_57",
            no_task="if_request_primaryroles_present_87",
        )

        get_data_getcurrentsupervisor_57 = rail.RepliconServiceOperator(
            task_id='get_data_getcurrentsupervisor_57',
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
            },
            data_handler=lambda response: response['rows'][0]['cells'][
                0].get('uri') if response and response['rows'] else ''
        )

        if_log_current_supervisor_uri_58_present_currentsupervisorisassigned_59 = rail.IfOperator(
            task_id='if_log_current_supervisor_uri_58_present_currentsupervisorisassigned_59',
            test='''{{ result('get_data_getcurrentsupervisor_57') | is_truthy }}''',
            yes_task="if_log_checkifuserexist_55_present_60",
            no_task="if_log_checkifuserexist_55_present_74",
        )

        if_log_checkifuserexist_55_present_60 = rail.IfOperator(
            task_id='if_log_checkifuserexist_55_present_60',
            test='''{{ result('search_users_54').useruri | is_truthy }}''',
            yes_task="if_log_checkifuserexist_55_not_equals_to_dataloggerlog_current_supervisor_uri_58message_61",
            no_task="npsg_supervisor_check_add_entry_72",
        )

        if_log_checkifuserexist_55_not_equals_to_dataloggerlog_current_supervisor_uri_58message_61 = rail.IfOperator(
            task_id='if_log_checkifuserexist_55_not_equals_to_dataloggerlog_current_supervisor_uri_58message_61',
            test='''{{ result('search_users_54').useruri != result('get_data_getcurrentsupervisor_57') }}''',
            yes_task="get_assigned_permission_sets_for_user2_62",
            no_task="if_request_primaryroles_present_87",
        )

        get_assigned_permission_sets_for_user2_62 = rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets_for_user2_62',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_users_54').useruri }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'name', 'Supervisor', 'permissionSet', '')
        )

        if_log_checkifsupervisorpermissionsetisassigned_63_present_64 = rail.IfOperator(
            task_id='if_log_checkifsupervisorpermissionsetisassigned_63_present_64',
            test='''{{ result('get_assigned_permission_sets_for_user2_62') | is_truthy }}''',
            yes_task="update_supervisor_assignment_schedule_over_date_range_65",
            no_task="get_all_permission_sets_67",
        )

        update_supervisor_assignment_schedule_over_date_range_65 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_65',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "supervisorUri": "{{ result('search_users_54').useruri }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ dag_run.conf.today_DS.year }}",
                        "month": "{{ dag_run.conf.today_DS.month }}",
                        "day": "{{ dag_run.conf.today_DS.day }}"
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        get_all_permission_sets_67 = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets_67',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data_handler=lambda repsonse: rail.find_first_by_attr_and_get_attr(
                repsonse, 'name', 'Supervisor', 'uri', '')
        )

        assign_permission_set_to_user_69 = rail.RepliconServiceOperator(
            task_id='assign_permission_set_to_user_69',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('search_users_54').useruri }}",
                "permissionSetUri": "{{ result('get_all_permission_sets_67') }}"
            }
        )

        update_supervisor_assignment_schedule_over_date_range_70 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_70',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "supervisorUri": "{{ result('search_users_54').useruri }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ dag_run.conf.today_DS.year }}",
                        "month": "{{ dag_run.conf.today_DS.month }}",
                        "day": "{{ dag_run.conf.today_DS.day }}"
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        npsg_supervisor_check_add_entry_72 = rail.WriteLogOperator(
            task_id='npsg_supervisor_check_add_entry_72',
            log="{{ dag_run.conf.supervisorlookup}}",
            message="na",
            severity="na",
            properties=lambda dag_run: {
                "jobid": dag_run.conf['callerjobid'],
                "userempid": dag_run.conf['employeeid'],
                "useruri": dag_run.conf['useruri'],
                "username": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "supervisorempid": dag_run.conf['supervisorid'],
                "action": "Update",
                "childjobid": rail.render_template("{{ dag_run_ecid() }}"),
                "status": '',
                "effectivedate": datetime.now().strftime("%m/%d/%Y")
            }
        )

        if_log_checkifuserexist_55_present_74 = rail.IfOperator(
            task_id='if_log_checkifuserexist_55_present_74',
            test='''{{ result('search_users_54').useruri | is_truthy }}''',
            yes_task="get_assigned_permission_sets_for_user2_75",
            no_task="if_request_supervisorid_present_85",
        )

        get_assigned_permission_sets_for_user2_75 = rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets_for_user2_75',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_users_54').useruri }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'name', 'Supervisor', 'permissionSet', '')
        )

        if_log_checkifsupervisorpermissionsetisassigned_76_present_77 = rail.IfOperator(
            task_id='if_log_checkifsupervisorpermissionsetisassigned_76_present_77',
            test='''{{ result('get_assigned_permission_sets_for_user2_75') | is_truthy }}''',
            yes_task="update_supervisor_assignment_schedule_over_date_range_78",
            no_task="get_all_permission_sets_80",
        )

        update_supervisor_assignment_schedule_over_date_range_78 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_78',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "supervisorUri": "{{ result('search_users_54').useruri }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ dag_run.conf.today_DS.year }}",
                        "month": "{{ dag_run.conf.today_DS.month }}",
                        "day": "{{ dag_run.conf.today_DS.day }}"
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        get_all_permission_sets_80 = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets_80',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data_handler=lambda repsonse: rail.find_first_by_attr_and_get_attr(
                repsonse, 'name', 'Supervisor', 'uri', '')
        )

        assign_permission_set_to_user_82 = rail.RepliconServiceOperator(
            task_id='assign_permission_set_to_user_82',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('search_users_54').useruri }}",
                "permissionSetUri": "{{ result('get_all_permission_sets_80') }}"
            }
        )

        update_supervisor_assignment_schedule_over_date_range_83 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_83',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "supervisorUri": "{{ result('search_users_54').useruri }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ dag_run.conf.today_DS.year }}",
                        "month": "{{ dag_run.conf.today_DS.month }}",
                        "day": "{{ dag_run.conf.today_DS.day }}"
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_request_supervisorid_present_85 = rail.IfOperator(
            task_id='if_request_supervisorid_present_85',
            test='''{{ dag_run.conf.supervisorid | is_truthy }}''',
            yes_task="npsg_supervisor_check_add_entry_86",
            no_task="if_request_primaryroles_present_87",
        )

        npsg_supervisor_check_add_entry_86 = rail.WriteLogOperator(
            task_id='npsg_supervisor_check_add_entry_86',
            log="{{ dag_run.conf.supervisorlookup}}",
            message="na",
            severity="na",
            properties=lambda dag_run: {
                "jobid": dag_run.conf['callerjobid'],
                "userempid": dag_run.conf['employeeid'],
                "useruri": dag_run.conf['useruri'],
                "username": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "supervisorempid": dag_run.conf['supervisorid'],
                "action": "Update",
                "childjobid": rail.render_template("{{ dag_run_ecid() }}"),
                "effectivedate": datetime.now().strftime("%m/%d/%Y")
            }
        )

        if_request_primaryroles_present_87 = rail.IfOperator(
            task_id='if_request_primaryroles_present_87',
            test='''{{ dag_run.conf.primaryroles | is_truthy  or dag_run.conf.additionalroles | is_truthy }}''',
            yes_task="get_project_role_assignment_schedule_for_user_88",
            no_task="npsg_user_import_logs_add_entry_98",
        )

        get_project_role_assignment_schedule_for_user_88 = rail.RepliconServiceOperator(
            task_id='get_project_role_assignment_schedule_for_user_88',
            endpoint="/services/ResourceService1.svc/GetProjectRoleAssignmentScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )


        def get_project_roles(dag_run):
            primaryroles = [dag_run.conf['primaryroles']
                            ] if dag_run.conf['primaryroles'] else []
            additionalroles = list(set((rail.smartjoin_by_delim((dag_run.conf['additionalroles']).split(
                "|"), "|")).split("|"))) if dag_run.conf['additionalroles'] else []
            existingrole = ([role['projectRole']['name'] for role in rail.result(
                'get_project_role_assignment_schedule_for_user_88')[0]['projectRoles']]) if len(
                rail.result('get_project_role_assignment_schedule_for_user_88')[0]['projectRoles']) > 0 else ''
            existing_primaryrole = rail.find_first_by_attr_and_get_attr(rail.result(
                'get_project_role_assignment_schedule_for_user_88')[0]['projectRoles'], 'isPrimary', 'true', 'projectRole.name', '') if len(
                rail.result('get_project_role_assignment_schedule_for_user_88')[0]['projectRoles']) > 0 else ''
            merge_list = primaryroles + additionalroles
            existing_roles_count = 0 if (
                existingrole is None) else len(existingrole)

            if existing_roles_count > 0:
                new_roles_to_assign = list(
                    set(merge_list).difference(existingrole))
            else:
                new_roles_to_assign = list(set(merge_list))


            return {"projectroles": list(map(lambda x: {"name": x}, merge_list)),
                    "count_of_roles_toassign": len(new_roles_to_assign),
                    "is_primaryrole_same": (existing_primaryrole == primaryroles[0]),
                    "new_roles_toassign": list(map(lambda x: {"name": x}, new_roles_to_assign))
            }

        invoke_custom_py_code_89 = rail.PythonOperator(
            task_id='invoke_custom_py_code_89',
            python_callable=get_project_roles
        )

        if_output_is_primaryrole_same_is_true_90 = rail.IfOperator(
            task_id='if_output_is_primaryrole_same_is_true_90',
            test=lambda: rail.result('invoke_custom_py_code_89')['is_primaryrole_same'] == 'true' or rail.result(
                'invoke_custom_py_code_89')['count_of_roles_toassign'] > 0,
            yes_task="adhoc_http_action_91",
            no_task="if_request_primaryroles_present_93",
        )

        adhoc_http_action_91 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_91',
            endpoint="/services/ResourceService1.svc/PutProjectRoleAssignmentScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": [
                    {
                        "projectRoles": [{
                            "projectRole": {
                                "name": projectrole['name']
                            },
                            "isPrimary": "true" if projectrole['name'] == dag_run.conf['primaryroles'] else "false"
                        } for projectrole in rail.result('invoke_custom_py_code_89')['projectroles']]
                    }
                ]
            }
        )

        if_request_primaryroles_present_93 = rail.IfOperator(
            task_id='if_request_primaryroles_present_93',
            test=lambda dag_run: dag_run.conf['primaryroles'] and rail.result(
                'invoke_custom_py_code_89')['is_primaryrole_same'] != 'true',
            yes_task="adhoc_http_action_94",
            no_task="npsg_user_import_logs_add_entry_98",
        )

        adhoc_http_action_94 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_94',
            endpoint="/services/ResourceService1.svc/PutProjectRoleAssignmentScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": [
                    {
                        "projectRoles": [{
                            "projectRole": {
                                "name": projectrole['name']
                            },
                            "isPrimary": "true" if projectrole['name'] == dag_run.conf['primaryroles'] else "false"
                        } for projectrole in rail.result('invoke_custom_py_code_89')['projectroles']],
                        "effectiveDate": {
                            "year": dag_run.conf['today_DS']['year'],
                            "month": dag_run.conf['today_DS']['month'],
                            "day": dag_run.conf['today_DS']['day']
                        }
                    }
                ]
            }
        )

        npsg_user_import_logs_add_entry_98 = rail.WriteLogOperator(
            task_id='npsg_user_import_logs_add_entry_98',
            log="{{ dag_run.conf.userimportlogtable}}",
            message="na",
            severity="success",
            properties=lambda dag_run: {
                "empid": dag_run.conf['employeeid'],
                "username": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "action": "Updated",
                "status": "success",
                "details": "User updated successfully" + ("User's end date has been updated" if dag_run.conf['enddate'] else ''),
                "parentjob": "{{ dag_run.conf.callerjobid }}",
                "childjob": "{{ dag_run_ecid() }}"
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="error",
            properties={
                "empid": "{{dag_run.conf.employeeid}}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "action": "Updated",
                "status": "error",
                "details": "{{get_error_message()}}",
                "parentjob": "{{dag_run.conf.callerjobid}}",
                "childjob": "{{ dag_run_ecid() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> bulk_get_users3_4
        bulk_get_users3_4 >> if_userdetails_isenabled_is_not_true_6
        if_userdetails_isenabled_is_not_true_6 >> rail.Label(
            'Yes') >> enable_login_7 >> apply_user_modifications2_updatestartdate_8 >> if_userdetails_isenabled_is_true_9
        if_userdetails_isenabled_is_not_true_6 >> rail.Label(
            'No') >> if_userdetails_isenabled_is_true_9
        if_userdetails_isenabled_is_true_9 >> rail.Label(
            'Yes') >> if_request_enddate_present_10
        if_request_enddate_present_10 >> rail.Label(
            'Yes') >> apply_user_modifications2_updatestartdate_12 >> get_effective_user_group_membership_15
        if_request_enddate_present_10 >> rail.Label(
            'No') >> get_effective_user_group_membership_15
        if_userdetails_isenabled_is_true_9 >> rail.Label(
            'No') >> get_effective_user_group_membership_15 >> if_request_email_present_21
        if_request_email_present_21 >> rail.Label(
            'Yes') >> if_request_email_not_equals_to_datarestbulk_get_users3_4responsedfirstuserdetailsemailaddress_22
        if_request_email_not_equals_to_datarestbulk_get_users3_4responsedfirstuserdetailsemailaddress_22 >> rail.Label(
            'Yes') >> adhoc_http_action_23 >> if_locationuri_present
        if_request_email_not_equals_to_datarestbulk_get_users3_4responsedfirstuserdetailsemailaddress_22 >> rail.Label(
            'No') >> if_locationuri_present
        if_request_email_present_21 >> rail.Label(
            'No') >> if_locationuri_present
        if_locationuri_present >> rail.Label(
            'Yes') >> if_location_uri_not_equals_to_dataworkato_service2693ef48requestlocationuri_25
        if_location_uri_not_equals_to_dataworkato_service2693ef48requestlocationuri_25 >> rail.Label(
            'Yes') >> apply_user_modifications2_locationupdate_26 >> if_request_holidaycalendar_present_27
        if_location_uri_not_equals_to_dataworkato_service2693ef48requestlocationuri_25 >> rail.Label(
            'No') >> if_request_holidaycalendar_present_27
        if_locationuri_present >> rail.Label(
            'No') >> if_request_holidaycalendar_present_27
        if_request_holidaycalendar_present_27 >> rail.Label(
            'Yes') >> apply_user_modifications2holiday_calendar_to_apply_28 >> if_request_timezone_present_29
        if_request_holidaycalendar_present_27 >> rail.Label(
            'No') >> if_request_timezone_present_29
        if_request_timezone_present_29 >> rail.Label(
            'Yes') >> apply_user_modifications2timezone_to_apply_30 >> foreach_response_31
        if_request_timezone_present_29 >> rail.Label(
            'No') >> foreach_response_31 >> foreach_userdetails_32 >> if_customfield_name_equals_to_division_33
        if_customfield_name_equals_to_division_33 >> rail.Label(
            'Yes') >> if_request_division_present_34
        if_request_division_present_34 >> rail.Label(
            'Yes') >> update_dropdown_value_division_35 >> if_customfield_name_equals_to_position_36
        if_request_division_present_34 >> rail.Label(
            'No') >> if_customfield_name_equals_to_position_36
        if_customfield_name_equals_to_division_33 >> rail.Label(
            'No') >> if_customfield_name_equals_to_position_36
        if_customfield_name_equals_to_position_36 >> rail.Label(
            'Yes') >> if_request_position_present_37
        if_request_position_present_37 >> rail.Label(
            'Yes') >> update_dropdown_value_position_38 >> if_customfield_name_equals_to_employeestate_39
        if_request_position_present_37 >> rail.Label(
            'No') >> if_customfield_name_equals_to_employeestate_39
        if_customfield_name_equals_to_position_36 >> rail.Label(
            'No') >> if_customfield_name_equals_to_employeestate_39
        if_customfield_name_equals_to_employeestate_39 >> rail.Label(
            'Yes') >> if_request_employeestate_present_40
        if_request_employeestate_present_40 >> rail.Label(
            'Yes') >> update_dropdown_value_employeestate_41 >> if_customfield_name_equals_to_employeecity_42
        if_request_employeestate_present_40 >> rail.Label(
            'No') >> if_customfield_name_equals_to_employeecity_42
        if_customfield_name_equals_to_employeestate_39 >> rail.Label(
            'No') >> if_customfield_name_equals_to_employeecity_42
        if_customfield_name_equals_to_employeecity_42 >> rail.Label(
            'Yes') >> if_request_employeecity_present_43
        if_request_employeecity_present_43 >> rail.Label(
            'Yes') >> update_text_value_employeecity_44 >> foreach_userdetails_32_end
        if_request_employeecity_present_43 >> rail.Label(
            'No') >> foreach_userdetails_32_end
        if_customfield_name_equals_to_employeecity_42 >> rail.Label(
            'No') >> foreach_userdetails_32_end >> foreach_response_31_end
        foreach_userdetails_32 >> foreach_userdetails_32_end >> foreach_response_31_end
        foreach_response_31 >> foreach_response_31_end >> if_request_timesheettemplate_present_45
        if_request_timesheettemplate_present_45 >> rail.Label(
            'Yes') >> assign_policy_set_to_user_46 >> if_request_payrule_present_47
        if_request_timesheettemplate_present_45 >> rail.Label(
            'No') >> if_request_payrule_present_47
        if_request_payrule_present_47 >> rail.Label(
            'Yes') >> apply_user_modifications2_48 >> if_request_departmenturi_present_49
        if_request_payrule_present_47 >> rail.Label(
            'No') >> if_request_departmenturi_present_49
        if_request_departmenturi_present_49 >> rail.Label(
            'Yes') >> update_department_for_user_50 >> if_request_employeetypeuri_present_51
        if_request_departmenturi_present_49 >> rail.Label(
            'No') >> if_request_employeetypeuri_present_51
        if_request_employeetypeuri_present_51 >> rail.Label(
            'Yes') >> update_employee_type_for_user_52 >> if_request_supervisorid_present_53
        if_request_employeetypeuri_present_51 >> rail.Label(
            'No') >> if_request_supervisorid_present_53
        if_request_supervisorid_present_53 >> rail.Label(
            'Yes') >> search_users_54 >> if_log_checkifuserexist_55_present_56
        if_log_checkifuserexist_55_present_56 >> rail.Label(
            'Yes') >> get_data_getcurrentsupervisor_57 >> if_log_current_supervisor_uri_58_present_currentsupervisorisassigned_59
        if_log_current_supervisor_uri_58_present_currentsupervisorisassigned_59 >> rail.Label(
            'Yes') >> if_log_checkifuserexist_55_present_60
        if_log_checkifuserexist_55_present_60 >> rail.Label(
            'Yes') >> if_log_checkifuserexist_55_not_equals_to_dataloggerlog_current_supervisor_uri_58message_61
        if_log_checkifuserexist_55_not_equals_to_dataloggerlog_current_supervisor_uri_58message_61 >> rail.Label(
            'Yes') >> get_assigned_permission_sets_for_user2_62 >> if_log_checkifsupervisorpermissionsetisassigned_63_present_64
        if_log_checkifsupervisorpermissionsetisassigned_63_present_64 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_65 >> if_request_primaryroles_present_87
        if_log_checkifsupervisorpermissionsetisassigned_63_present_64 >> rail.Label(
            'No') >> get_all_permission_sets_67 >> assign_permission_set_to_user_69 >> update_supervisor_assignment_schedule_over_date_range_70
        update_supervisor_assignment_schedule_over_date_range_70 >> if_request_primaryroles_present_87
        if_log_checkifuserexist_55_not_equals_to_dataloggerlog_current_supervisor_uri_58message_61 >> rail.Label(
            'No') >> if_request_primaryroles_present_87
        if_log_checkifuserexist_55_present_60 >> rail.Label(
            'No') >> npsg_supervisor_check_add_entry_72 >> if_request_primaryroles_present_87
        if_log_current_supervisor_uri_58_present_currentsupervisorisassigned_59 >> rail.Label(
            'No') >> if_log_checkifuserexist_55_present_74
        if_log_checkifuserexist_55_present_74 >> rail.Label(
            'Yes') >> get_assigned_permission_sets_for_user2_75 >> if_log_checkifsupervisorpermissionsetisassigned_76_present_77
        if_log_checkifsupervisorpermissionsetisassigned_76_present_77 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_78 >> if_request_supervisorid_present_85
        if_log_checkifsupervisorpermissionsetisassigned_76_present_77 >> rail.Label(
            'No') >> get_all_permission_sets_80 >> assign_permission_set_to_user_82 >> update_supervisor_assignment_schedule_over_date_range_83
        update_supervisor_assignment_schedule_over_date_range_83 >> if_request_primaryroles_present_87
        if_log_checkifuserexist_55_present_74 >> rail.Label(
            'No') >> if_request_supervisorid_present_85
        if_request_supervisorid_present_85 >> rail.Label(
            'Yes') >> npsg_supervisor_check_add_entry_86 >> if_request_primaryroles_present_87
        if_request_supervisorid_present_85 >> rail.Label(
            'No') >> if_request_primaryroles_present_87
        if_log_checkifuserexist_55_present_56 >> rail.Label(
            'No') >> if_request_primaryroles_present_87
        if_request_supervisorid_present_53 >> rail.Label(
            'No') >> if_request_primaryroles_present_87
        if_request_primaryroles_present_87 >> rail.Label(
            'Yes') >> get_project_role_assignment_schedule_for_user_88 >> invoke_custom_py_code_89 >> if_output_is_primaryrole_same_is_true_90
        if_output_is_primaryrole_same_is_true_90 >> rail.Label(
            'Yes') >> adhoc_http_action_91 >> npsg_user_import_logs_add_entry_98
        if_output_is_primaryrole_same_is_true_90 >> rail.Label(
            'No') >> if_request_primaryroles_present_93
        if_request_primaryroles_present_93 >> rail.Label(
            'Yes') >> adhoc_http_action_94 >> npsg_user_import_logs_add_entry_98
        if_request_primaryroles_present_93 >> rail.Label(
            'No') >> npsg_user_import_logs_add_entry_98
        if_request_primaryroles_present_87 >> rail.Label(
            'No') >> npsg_user_import_logs_add_entry_98 >> catch_and_log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
