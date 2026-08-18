
import itertools
from datetime import timedelta, datetime
import pendulum
from airflow.models import Variable
from rail.lib.ecid import get_dagrun_ecid
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'intercontinentalexchange_userupdate_v2_{config.instance}',
        description=f'IntercontinentalExchange_User Update V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
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
            no_task='declare_list_2'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='declare_list_2',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        declare_list_2 = rail.SetVariableOperator(
            task_id='declare_list_2',
            append=False,
            name='Exception',
            value=[]
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
            },
            data_handler=lambda response: response[0] if response else None
        )

        get_effective_user_group_membership_5 = rail.RepliconServiceOperator(
            task_id='get_effective_user_group_membership_5',
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "dateRange": null
            }
        )

        date_split_today_6 = rail.EmptyOperator(
            task_id='date_split_today_6',
        )

        if_userdetails_isenabled_is_not_true_7 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_not_true_7',
            test='''{{ result('bulk_get_users3_4').userDetails.isEnabled | is_falsy  and dag_run.conf.employeestatus | matches('Active') | is_falsy }}''',
            yes_task="intercontinentalexchange_user_import_logs_add_entry_8",
            no_task="if_userdetails_isenabled_is_not_true_10",
        )

        intercontinentalexchange_user_import_logs_add_entry_8 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_user_import_logs_add_entry_8',
            message="User already in Inactive status",
            severity="Exception",
            properties={
                "Empid": "{{ dag_run.conf.employeeid }}",
                "Username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "Action": "Update",
                "Status": "Exception",
                "Details": "User already in Inactive status",
                "Jobid": "{{ dag_run_ecid() }}"
            }
        )

        if_userdetails_isenabled_is_not_true_10 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_not_true_10',
            test='''{{ result('bulk_get_users3_4').userDetails.isEnabled | is_falsy }}''',
            yes_task="enable_login_enablelogin_11",
            no_task="invoke_custom_ruby_code_12",
        )

        enable_login_enablelogin_11 = rail.RepliconServiceOperator(
            task_id='enable_login_enablelogin_11',
            endpoint="/services/securityService1.svc/EnableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        def get_replicon_date(date_str):
            effective_date = pendulum.now(config.pacific_timezone)
            if date_str:
                effective_date = datetime.strptime(date_str, '%Y%m%d')
            return {
                'year': effective_date.year,
                'month': effective_date.month,
                'day': effective_date.day
            }

        update_enddate = rail.RepliconServiceOperator(
            task_id='update_enddate',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": get_replicon_date(dag_run.conf['effective_date'])
                }
            }
        )

        def get_custom_value(custom_field_name):
            existing_custom_fields = rail.result('bulk_get_users3_4')[
                'userDetails']['customFieldValues']
            custom_infos = list(filter(
                lambda x: x['customField']['displayText'] == custom_field_name, existing_custom_fields))
            return custom_infos[0]['text'] if custom_infos else None

        invoke_custom_ruby_code_12 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_12',
            python_callable=lambda: {
                "weekhours": get_custom_value("Week Hours"),
                "node": get_custom_value("Node"),
                "adminmodified": get_custom_value("Admin Modified")
            }
        )

        if_output_adminmodified_equals_to_yes_13 = rail.IfOperator(
            task_id='if_output_adminmodified_equals_to_yes_13',
            test='''{{ result('invoke_custom_ruby_code_12').adminmodified == 'Yes' }}''',
            yes_task="intercontinentalexchange_user_import_logs_add_entry_14",
            no_task="date_split_startdate_effectivedate_19",
        )

        intercontinentalexchange_user_import_logs_add_entry_14 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_user_import_logs_add_entry_14',
            message="User not updated as the Admin modified setting is set to 'Yes'.",
            severity="Exception",
            properties={
                "Empid": "{{ dag_run.conf.employeeid }}",
                "Username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "Action": "Update",
                "Status": "Exception",
                "Details": "User not updated as the Admin modified setting is set to 'Yes'.",
                "Jobid": "{{ dag_run_ecid() }}"
            }
        )

        date_split_startdate_effectivedate_19 = rail.PythonOperator(
            task_id='date_split_startdate_effectivedate_19',
            python_callable=lambda dag_run: get_replicon_date(
                dag_run.conf['effective_date'])
        )

        if_request_effective_date_present_20 = rail.IfOperator(
            task_id='if_request_effective_date_present_20',
            test='''{{ dag_run.conf.effective_date | is_truthy  and dag_run.conf.actual_termination_date | is_falsy }}''',
            yes_task="else_22",
            no_task="if_request_actual_termination_date_present_23",
        )

        else_22 = rail.EmptyOperator(
            task_id='else_22',
        )

        if_request_actual_termination_date_present_23 = rail.IfOperator(
            task_id='if_request_actual_termination_date_present_23',
            test='''{{ dag_run.conf.actual_termination_date | is_truthy }}''',
            yes_task="date_split_enddate_24",
            no_task="if_request_firstname_present_dataworkato_servicereceive_requestrequestemployeefirstnamedowncase_32",
        )

        date_split_enddate_24 = rail.EmptyOperator(
            task_id='date_split_enddate_24',
        )

        update_employment_date_rangeforenddate_updateenddatewithnewstartdate_25 = rail.RepliconServiceOperator(
            task_id='update_employment_date_rangeforenddate_updateenddatewithnewstartdate_25',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": {
                        "year": rail.result('bulk_get_users3_4')['userDetails']['employmentDateRange']['startDate']['year'],
                        "month": rail.result('bulk_get_users3_4')['userDetails']['employmentDateRange']['startDate']['month'],
                        "day": rail.result('bulk_get_users3_4')['userDetails']['employmentDateRange']['startDate']['day']
                    },
                    "endDate": {
                        "year":  datetime.strptime(dag_run.conf['actual_termination_date'], '%Y%m%d').year,
                        "month": datetime.strptime(dag_run.conf['actual_termination_date'], '%Y%m%d').month,
                        "day":  datetime.strptime(dag_run.conf['actual_termination_date'], '%Y%m%d').day,
                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_request_firstname_present_dataworkato_servicereceive_requestrequestemployeefirstnamedowncase_32 = rail.IfOperator(
            task_id='if_request_firstname_present_dataworkato_servicereceive_requestrequestemployeefirstnamedowncase_32',
            test='''{{ dag_run.conf.firstname | is_truthy  and result('bulk_get_users3_4').userDetails.firstName | lower != dag_run.conf.firstname | lower }}''',
            yes_task="update_first_name_33",
            no_task="if_request_lastname_present_dataworkato_servicereceive_requestrequestlastnamedowncase_34",
        )

        update_first_name_33 = rail.RepliconServiceOperator(
            task_id='update_first_name_33',
            endpoint="/services/userService1.svc/UpdateFirstName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "firstname": "{{ dag_run.conf.firstname }}"
            }
        )

        if_request_lastname_present_dataworkato_servicereceive_requestrequestlastnamedowncase_34 = rail.IfOperator(
            task_id='if_request_lastname_present_dataworkato_servicereceive_requestrequestlastnamedowncase_34',
            test='''{{ dag_run.conf.lastname | is_truthy and result('bulk_get_users3_4').userDetails.lastName | lower != dag_run.conf.lastname | lower }}''',
            yes_task="update_last_name_35",
            no_task="if_request_work_email_present_36",
        )

        update_last_name_35 = rail.RepliconServiceOperator(
            task_id='update_last_name_35',
            endpoint="/services/userService1.svc/UpdateLastName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "lastname": "{{ dag_run.conf.lastname }}"
            }
        )

        if_request_work_email_present_36 = rail.IfOperator(
            task_id='if_request_work_email_present_36',
            test='''{{ dag_run.conf.work_email | is_truthy and result('bulk_get_users3_4').userDetails.emailAddress | lower != dag_run.conf.work_email | lower }}''',
            yes_task="update_email_37",
            no_task="declare_variable_39",
        )

        update_email_37 = rail.RepliconServiceOperator(
            task_id='update_email_37',
            endpoint="/services/userService1.svc/UpdateEmail",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "email": "{{ dag_run.conf.work_email }}"
            }
        )

        apply_user_modifications2_38 = rail.RepliconServiceOperator(
            task_id='apply_user_modifications2_38',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "securitySettingsToApply": {
                        "loginEnabled": "1",
                        "loginName": "{{ dag_run.conf.work_email }}",
                        "ssoName": "{{ dag_run.conf.work_email }}",
                        "password": null,
                        "enabledAuthenticationTypeUris": [
                            "urn:replicon:user-authentication-type:sso"
                        ],
                        "emailMFAResendVerificationEmail": "false",
                        "emailMFATryAddMethodFromUsersEmail": "false",
                        "clearIsLockedOut": "false"
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        declare_variable_39 = rail.SetVariableOperator(
            task_id='declare_variable_39',
            append=False,
            name='None',
            value=None
        )

        if_request_week_hours_present_40 = rail.IfOperator(
            task_id='if_request_week_hours_present_40',
            test='''{{ dag_run.conf.week_hours | is_truthy  and result('invoke_custom_ruby_code_12').weekhours != dag_run.conf.week_hours }}''',
            yes_task="update_numeric_value_weekhours_41",
            no_task="if_request_location_node_present_42",
        )

        update_numeric_value_weekhours_41 = rail.RepliconServiceOperator(
            task_id='update_numeric_value_weekhours_41',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.weeklyhoursudfuri }}",
                "value": "{{ dag_run.conf.week_hours }}"
            }
        )

        if_request_location_node_present_42 = rail.IfOperator(
            task_id='if_request_location_node_present_42',
            test='''{{ dag_run.conf.location_node | is_truthy  and result('invoke_custom_ruby_code_12').node != dag_run.conf.location_node }}''',
            yes_task="update_text_value_businesstitle_43",
            no_task="if_request_department_present_44",
        )

        update_text_value_businesstitle_43 = rail.RepliconServiceOperator(
            task_id='update_text_value_businesstitle_43',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.nodeudfuri }}",
                "value": "{{ dag_run.conf.location_node }}"
            }
        )

        if_request_department_present_44 = rail.IfOperator(
            task_id='if_request_department_present_44',
            test='''{{ dag_run.conf.department | is_truthy and (result('get_effective_user_group_membership_5') | length == 0 or dag_run.conf.department != result('get_effective_user_group_membership_5').departments[0].department.department.uri) }}''',
            yes_task="if_request_department_blank_45",
            no_task="if_request_employeetypeuri_present_49",
        )

        if_request_department_blank_45 = rail.IfOperator(
            task_id='if_request_department_blank_45',
            test='''{{ dag_run.conf.department | is_falsy }}''',
            yes_task="insert_to_list_46",
            no_task="update_department_group_48",
        )

        insert_to_list_46 = rail.SetVariableOperator(
            task_id='insert_to_list_46',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Department '{{dag_run.conf.department_id}}' is not available in Replicon"
            }
        )

        update_department_group_48 = rail.RepliconServiceOperator(
            task_id='update_department_group_48',
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
                                        "uri": dag_run.conf['department'],
                                        "parent": null,
                                        "name": null,
                                        "parameterCorrelationId": null
                                    },
                                    "effectiveDate": rail.result('date_split_startdate_effectivedate_19')
                                }
                            ],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_request_employeetypeuri_present_49 = rail.IfOperator(
            task_id='if_request_employeetypeuri_present_49',
            test='''{{ dag_run.conf.employeetypeuri | is_truthy  and (result('get_effective_user_group_membership_5').employeeTypes | length == 0 or dag_run.conf.employeetypeuri != result('get_effective_user_group_membership_5').employeeTypes[0].employeeType.employeeType.uri) }}''',
            yes_task="if_request_employeetypeuri_blank_51",
            no_task="if_request_location_present_55",
        )

        if_request_employeetypeuri_blank_51 = rail.IfOperator(
            task_id='if_request_employeetypeuri_blank_51',
            test='''{{ dag_run.conf.employeetypeuri | is_falsy }}''',
            yes_task="insert_to_list_52",
            no_task="update_employeetype_group_54",
        )

        insert_to_list_52 = rail.SetVariableOperator(
            task_id='insert_to_list_52',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Employee type '{{dag_run.conf.worker_type}}' not available in Replicon"
            }
        )

        update_employeetype_group_54 = rail.RepliconServiceOperator(
            task_id='update_employeetype_group_54',
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
                                        "parent": null,
                                        "name": null,
                                        "parameterCorrelationId": null
                                    },
                                    "effectiveDate": rail.result('date_split_startdate_effectivedate_19')
                                }
                            ],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_request_location_present_55 = rail.IfOperator(
            task_id='if_request_location_present_55',
            test='''{{ dag_run.conf.location | is_truthy  and (result('get_effective_user_group_membership_5').locations | length == 0 or dag_run.conf.locationuri != result('get_effective_user_group_membership_5').locations[0].location.location.uri) }}''',
            yes_task="if_request_locationuri_blank_56",
            no_task="if_request_legal_entity_id_present_61",
        )

        if_request_locationuri_blank_56 = rail.IfOperator(
            task_id='if_request_locationuri_blank_56',
            test='''{{ dag_run.conf.locationuri | is_falsy }}''',
            yes_task="insert_to_list_57",
            no_task="update_location_60",
        )

        insert_to_list_57 = rail.SetVariableOperator(
            task_id='insert_to_list_57',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Location '{{dag_run.conf.location}}' is not available in Replicon"
            }
        )

        update_location_60 = rail.RepliconServiceOperator(
            task_id='update_location_60',
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
                                    "effectiveDate": rail.result('date_split_startdate_effectivedate_19')
                                }
                            ],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_request_legal_entity_id_present_61 = rail.IfOperator(
            task_id='if_request_legal_entity_id_present_61',
            test='''{{ dag_run.conf.legal_entity_id | is_truthy  and (result('get_effective_user_group_membership_5').divisions | length == 0 or dag_run.conf.legal_entity_name != result('get_effective_user_group_membership_5').divisions[0].division.division.uri) }}''',
            yes_task="if_request_legal_entity_name_blank_62",
            no_task="if_request_reporting_entity_id_present_66",
        )

        if_request_legal_entity_name_blank_62 = rail.IfOperator(
            task_id='if_request_legal_entity_name_blank_62',
            test='''{{ dag_run.conf.legal_entity_name | is_falsy }}''',
            yes_task="insert_to_list_63",
            no_task="update_division_65",
        )

        insert_to_list_63 = rail.SetVariableOperator(
            task_id='insert_to_list_63',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Legal Entity '{{dag_run.conf.legal_entity_id}}' not available in Replicon"
            }
        )

        update_division_65 = rail.RepliconServiceOperator(
            task_id='update_division_65',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=lambda dag_run: {
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
                                        "uri": dag_run.conf['legal_entity_name'],
                                        "parentUri": null,
                                        "name": null
                                    },
                                    "effectiveDate": rail.result('date_split_startdate_effectivedate_19')
                                }
                            ],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_request_reporting_entity_id_present_66 = rail.IfOperator(
            task_id='if_request_reporting_entity_id_present_66',
            test='''{{ dag_run.conf.reporting_entity_id | is_truthy  and (result('get_effective_user_group_membership_5').costCenters | length == 0 or dag_run.conf.reporting_entity_name != result('get_effective_user_group_membership_5').costCenters[0].costCenter.costCenter.uri) }}''',
            yes_task="if_request_reporting_entity_name_blank_67",
            no_task="if_request_timezoneuri_present_72",
        )

        if_request_reporting_entity_name_blank_67 = rail.IfOperator(
            task_id='if_request_reporting_entity_name_blank_67',
            test='''{{ dag_run.conf.reporting_entity_name | is_falsy }}''',
            yes_task="insert_to_list_68",
            no_task="update_costcenter_71",
        )

        insert_to_list_68 = rail.SetVariableOperator(
            task_id='insert_to_list_68',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Reproting Entity '{{dag_run.conf.reporting_entity_id}}' not available in Replicon"
            }
        )

        update_costcenter_71 = rail.RepliconServiceOperator(
            task_id='update_costcenter_71',
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
                                        "uri": dag_run.conf['reporting_entity_name'],
                                        "parentUri": null,
                                        "name": null
                                    },
                                    "effectiveDate": rail.result('date_split_startdate_effectivedate_19')
                                }
                            ],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_request_timezoneuri_present_72 = rail.IfOperator(
            task_id='if_request_timezoneuri_present_72',
            test='''{{ dag_run.conf.timezoneuri | is_truthy }}''',
            yes_task="if_request_timezoneuri_not_equals_to_datarestbulk_get_users3_4responsedfirsttimezoneuri_73",
            no_task="if_request_holidaycalendaruri_present_75",
        )

        if_request_timezoneuri_not_equals_to_datarestbulk_get_users3_4responsedfirsttimezoneuri_73 = rail.IfOperator(
            task_id='if_request_timezoneuri_not_equals_to_datarestbulk_get_users3_4responsedfirsttimezoneuri_73',
            test='''{{ result('bulk_get_users3_4').timeZone | is_falsy or (result('bulk_get_users3_4').timeZone | is_truthy and dag_run.conf.timezoneuri != result('bulk_get_users3_4').timeZone.uri) }}''',
            yes_task="update_time_zone_for_user_74",
            no_task="if_request_holidaycalendaruri_present_75",
        )

        update_time_zone_for_user_74 = rail.RepliconServiceOperator(
            task_id='update_time_zone_for_user_74',
            endpoint="/services/InternationalizationService1.svc/UpdateTimeZoneForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "timeZoneUri": "{{ dag_run.conf.timezoneuri }}"
            }
        )

        if_request_holidaycalendaruri_present_75 = rail.IfOperator(
            task_id='if_request_holidaycalendaruri_present_75',
            test='''{{ dag_run.conf.holidaycalendaruri | is_truthy and ((result('bulk_get_users3_4').holidayCalendar | is_truthy and dag_run.conf.holidaycalendaruri != result('bulk_get_users3_4').holidayCalendar.uri) or result('bulk_get_users3_4').holidayCalendar | is_falsy) }}''',
            yes_task="update_holiday_calendar_for_user_76",
            no_task="if_request_work_schedule_present_77",
        )

        update_holiday_calendar_for_user_76 = rail.RepliconServiceOperator(
            task_id='update_holiday_calendar_for_user_76',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "holidayCalendarUri": "{{ dag_run.conf.holidaycalendaruri }}"
            }
        )

        if_request_work_schedule_present_77 = rail.IfOperator(
            task_id='if_request_work_schedule_present_77',
            test='''{{ dag_run.conf.work_schedule | is_truthy }}''',
            yes_task="if_schedulepolicies_to_json_contains_urn_78",
            no_task="get_all_permission_sets_86",
        )

        if_schedulepolicies_to_json_contains_urn_78 = rail.IfOperator(
            task_id='if_schedulepolicies_to_json_contains_urn_78',
            test='''{{ result('bulk_get_users3_4').schedulePolicies | is_truthy }}''',
            yes_task="invoke_custom_ruby_code_80",
            no_task="if_schedulepolicies_displaytext_blank_dataworkato_servicereceive_requestrequestinitialschedulename_81",
        )

        def get_effective_date(schedulePolicy):
            start_date = rail.result('bulk_get_users3_4')[
                'userDetails']['employmentDateRange']['startDate']
            effective_date = str(schedulePolicy["effectiveDate"]['day']) + "/" + str(schedulePolicy["effectiveDate"]['month']) + "/" + str(
                schedulePolicy["effectiveDate"]['year']) if schedulePolicy["effectiveDate"] else str(start_date['day']) + "/" + str(start_date['month']) + "/" + str(start_date['year'])
            return effective_date

        def get_day_diff(schedulePolicy):
            todays_date = pendulum.now(config.pacific_timezone)
            todays_date_s = todays_date.strftime("%d/%m/%Y")
            current_pst_date = datetime.strptime(todays_date_s, "%d/%m/%Y")
            start_date = get_effective_date(schedulePolicy)
            from_start = datetime.strptime(start_date, '%d/%m/%Y')
            return (current_pst_date - from_start).days

        def get_schedule_policy():
            schedule_polieces = []
            schedulepolicies = rail.result('bulk_get_users3_4')[
                'schedulePolicies']
            for schedulePolicy in schedulepolicies:
                schedule_polieces.append({
                    "effectivedate": get_effective_date(schedulePolicy),
                    "displayText": schedulePolicy["officeSchedule"]["displayText"],
                    "uri": schedulePolicy["officeSchedule"]["uri"],
                    "scheduletypeuri": schedulePolicy["scheduleTypeUri"],
                    "daydiff": get_day_diff(schedulePolicy)
                })

            return min(schedule_polieces, key=lambda x: x['daydiff'])

        invoke_custom_ruby_code_80 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_80',
            # pylint: disable=unnecessary-lambda
            python_callable=lambda: get_schedule_policy()
        )

        if_schedulepolicies_displaytext_blank_dataworkato_servicereceive_requestrequestinitialschedulename_81 = rail.IfOperator(
            task_id='if_schedulepolicies_displaytext_blank_dataworkato_servicereceive_requestrequestinitialschedulename_81',
            test='''{{ result('invoke_custom_ruby_code_80').displayText | is_falsy or dag_run.conf.work_schedule != result('invoke_custom_ruby_code_80').displayText }}''',
            yes_task="if_request_work_schedule_equals_to_shiftschedule_82",
            no_task="get_all_permission_sets_86",
        )

        if_request_work_schedule_equals_to_shiftschedule_82 = rail.IfOperator(
            task_id='if_request_work_schedule_equals_to_shiftschedule_82',
            test='''{{ dag_run.conf.work_schedule == 'Shift Schedule' }}''',
            yes_task="updateofficeschedule_83",
            no_task="updateofficeschedule_85",
        )

        updateofficeschedule_83 = rail.RepliconServiceOperator(
            task_id='updateofficeschedule_83',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri'],
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "schedulePolicyToApply": {
                        "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementSchedule": [],
                        "updateScheduleOverDateRange": {
                            "replacementScheduleEntries": [
                                {
                                    "schedulePolicy": {
                                        "officeScheduleUri": null,
                                        "name": null,
                                        "officeSchedule": null,
                                        "scheduleTypeUri": "urn:replicon:schedule-type:shift"
                                    },
                                    "effectiveDate": {
                                        "year": pendulum.now(config.pacific_timezone).year,
                                        "month": pendulum.now(config.pacific_timezone).month,
                                        "day": pendulum.now(config.pacific_timezone).day
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

        updateofficeschedule_85 = rail.RepliconServiceOperator(
            task_id='updateofficeschedule_85',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri'],
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "schedulePolicyToApply": {
                        "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementSchedule": [],
                        "updateScheduleOverDateRange": {
                            "replacementScheduleEntries": [
                                {
                                    "schedulePolicy": {
                                        "officeScheduleUri": null,
                                        "name": dag_run.conf['work_schedule'],
                                        "officeSchedule": {
                                            "officeScheduleUri": null,
                                            "name": dag_run.conf['work_schedule']
                                        },
                                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                                    },
                                    "effectiveDate": {
                                        "year": pendulum.now(config.pacific_timezone).year,
                                        "month": pendulum.now(config.pacific_timezone).month,
                                        "day": pendulum.now(config.pacific_timezone).day
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

        get_all_permission_sets_86 = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets_86',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data=None
        )

        if_request_line_manager_present_87 = rail.IfOperator(
            task_id='if_request_line_manager_present_87',
            test='''{{ dag_run.conf.line_manager | is_truthy }}''',
            yes_task="search_users_88",
            no_task="if_request_line_manager_blank_109",
        )

        def page_handler(request, result):
            if len(result['rows']) > 0:
                request['page'] += 1
                return request
            return None

        def get_supervisor_info(result, employeeid):
            flaten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], result))))
            existing_user = list(filter(lambda x: x['employeeid'] == employeeid, map(lambda row: {
                'username': row['cells'][0]['textValue'] if 'textValue' in row['cells'][0] else None,
                'employeeid': row['cells'][2]['textValue'] if 'textValue' in row['cells'][2] else None,
                'status': row['cells'][3]['textValue'] if 'textValue' in row['cells'][3] else None,
                'loginname': row['cells'][1]['textValue'],
                'useruri': row['cells'][1]['uri']
            }, flaten_rows)))

            return existing_user if existing_user else []

        search_users_88 = rail.RepliconServicePageOperator(
            task_id="search_users_88",
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda dag_run: {
                'page': 1,
                'pagesize': 100,
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
                            'text': dag_run.conf['line_manager']
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda result, dag_run: get_supervisor_info(
                result, dag_run.conf['line_manager'])
        )

        if_search_users_88_users_greater_than_1_89 = rail.IfOperator(
            task_id='if_search_users_88_users_greater_than_1_89',
            test='''{{ result('search_users_88') | length > 1 }}''',
            yes_task="insert_to_list_90",
            no_task="log_supervisorcheck_92",
        )

        insert_to_list_90 = rail.SetVariableOperator(
            task_id='insert_to_list_90',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Supervisor not updated as there are multiple users with the ID '{{ dag_run.conf.line_manager }}' in Replicon."
            }
        )

        log_supervisorcheck_92 = rail.PythonOperator(
            task_id='log_supervisorcheck_92',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result('search_users_88'),
                                                                                 'employeeid', dag_run.conf['line_manager'], 'useruri')
        )

        if_log_supervisorcheck_92_blank_93 = rail.IfOperator(
            task_id='if_log_supervisorcheck_92_blank_93',
            test='''{{ result('log_supervisorcheck_92') | is_falsy }}''',
            yes_task="ice_supervisor_check_add_entry_94",
            no_task="bulk_get_users3_96",
        )

        ice_supervisor_check_add_entry_94 = rail.WriteLogOperator(
            task_id='ice_supervisor_check_add_entry_94',
            log="{{ dag_run.conf.supervisor_processing_log }}",
            message="na",
            severity="Update",
            properties=lambda dag_run: {
                "employeeid": dag_run.conf['employeeid'],
                "userloginname": dag_run.conf['work_email'],
                "useruri": dag_run.conf['useruri'],
                "username": dag_run.conf['firstname']+" "+dag_run.conf['lastname'],
                "supervisorempid": dag_run.conf['line_manager'],
                "childjobid": get_dagrun_ecid(dag_run),
                "action": "Update",
                "status": "",
                "effectivedate": pendulum.now(config.pacific_timezone).strftime('%m_%d_%Y')
            }
        )

        bulk_get_users3_96 = rail.RepliconServiceOperator(
            task_id='bulk_get_users3_96',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": "{{ result('log_supervisorcheck_92') }}",
                        "loginName": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: response[0] if response else None
        )

        if_request_line_manager_equals_to_dataworkato_servicereceive_requestrequestemployeeid_97 = rail.IfOperator(
            task_id='if_request_line_manager_equals_to_dataworkato_servicereceive_requestrequestemployeeid_97',
            test='''{{ dag_run.conf.line_manager == dag_run.conf.employeeid }}''',
            yes_task="insert_to_list_98",
            no_task="get_supervisor_assignment_detailsforuser_100",
        )

        insert_to_list_98 = rail.SetVariableOperator(
            task_id='insert_to_list_98',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Supervisor not updated  - Supervisor's employee id is same as User's employee id"
            }
        )

        get_supervisor_assignment_detailsforuser_100 = rail.RepliconServiceOperator(
            task_id='get_supervisor_assignment_detailsforuser_100',
            endpoint="/services/UserService1.svc/GetSupervisorAssignmentDetails",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "asOfDate": {
                    "year": pendulum.now(config.pacific_timezone).year,
                    "month": pendulum.now(config.pacific_timezone).month,
                    "day": pendulum.now(config.pacific_timezone).day
                }
            }
        )

        if_user_loginname_blank_101 = rail.IfOperator(
            task_id='if_user_loginname_blank_101',
            test='''{{ result('get_supervisor_assignment_detailsforuser_100') | is_falsy or result('get_supervisor_assignment_detailsforuser_100').supervisor.user.loginName | is_falsy or result('get_supervisor_assignment_detailsforuser_100').supervisor.user.loginName | lower != result('bulk_get_users3_96').securityConfiguration.loginName | lower }}''',
            yes_task="if_userdetails_isenabled_is_true_102",
            no_task="if_request_line_manager_blank_109",
        )

        if_userdetails_isenabled_is_true_102 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_true_102',
            test='''{{ result('bulk_get_users3_96').userDetails.isEnabled == true }}''',
            yes_task="log_checkifmanagerpermissionisassigned_103",
            no_task="ice_supervisor_check_add_entry_108",
        )

        log_checkifmanagerpermissionisassigned_103 = rail.PythonOperator(
            task_id='log_checkifmanagerpermissionisassigned_103',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'bulk_get_users3_96')['permissionSets'], 'name', "Supervisor", 'uri')
        )

        if_log_checkifmanagerpermissionisassigned_103_blank_104 = rail.IfOperator(
            task_id='if_log_checkifmanagerpermissionisassigned_103_blank_104',
            test='''{{ result('log_checkifmanagerpermissionisassigned_103') | is_falsy }}''',
            yes_task="assign_permission_set_to_user_manager_105",
            no_task="update_supervisor_assignment_schedule_over_date_range_106",
        )

        assign_permission_set_to_user_manager_105 = rail.RepliconServiceOperator(
            task_id='assign_permission_set_to_user_manager_105',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('bulk_get_users3_96').userDetails.uri }}",
                "permissionSetUri": "{{ dag_run.conf.supervisorpermissionuri }}"
            }
        )

        update_supervisor_assignment_schedule_over_date_range_106 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_106',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": rail.result('bulk_get_users3_96')['userDetails']['uri'],
                "dateRange": {
                    "startDate": rail.result('date_split_startdate_effectivedate_19'),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        ice_supervisor_check_add_entry_108 = rail.WriteLogOperator(
            task_id='ice_supervisor_check_add_entry_108',
            message="na",
            severity="Update",
            log="{{ dag_run.conf.supervisor_processing_log }}",
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "userloginname": "{{ dag_run.conf.work_email }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "supervisorempid": "{{ dag_run.conf.line_manager }}",
                "childjobid": "{{ dag_run_ecid() }}",
                "action": "Update",
                "status": "",
                "effectivedate": "{{ current_time('%m_%d_%Y') }}"
            }
        )

        if_request_line_manager_blank_109 = rail.IfOperator(
            task_id='if_request_line_manager_blank_109',
            test='''{{ dag_run.conf.line_manager | is_falsy }}''',
            yes_task="insert_to_list_110",
            no_task="log_exceptions_111",
        )

        insert_to_list_110 = rail.SetVariableOperator(
            task_id='insert_to_list_110',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Supervisor ID was not present in the Input file."
            }
        )

        def get_exception_info(list_name):
            exeption_info = rail.get_dag_run_var(
                rail.result(list_name)['name'])
            exceptions = [exception['value'] for exception in exeption_info]
            return rail.smartjoin_by_delim(exceptions, '|') if exceptions else None

        log_exceptions_111 = rail.PythonOperator(
            task_id='log_exceptions_111',
            python_callable=lambda:  get_exception_info('declare_list_2')
        )

        intercontinentalexchange_user_import_logs_add_entry_111 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_user_import_logs_add_entry_111',
            message="update",
            severity=lambda: "Exception" if rail.result(
                'log_exceptions_111') else "Success",
            properties=lambda dag_run: {
                "Empid": dag_run.conf['employeeid'],
                "Username": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "Action": "update",
                "Status": "Exception" if rail.result(
                    'log_exceptions_111') else "Success",
                "Details": "Updated with exceptions - " + rail.result('log_exceptions_111') if rail.result('log_exceptions_111') else "updated successfully",
                "Jobid": get_dagrun_ecid(dag_run)
            }
        )

        intercontinentalexchange_user_import_logs_add_entry_113 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_user_import_logs_add_entry_113',
            message='{{ get_error_message() }}',
            severity="Error",
            trigger_rule='one_failed',
            properties={
                "Empid": "{{ dag_run.conf.employeeid }}",
                "Username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "Action": "update",
                "Status": "Error",
                "Details": "{{ get_error_message() }}",
                "Jobid": "{{ dag_run_ecid() }}"
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> declare_list_2
        declare_list_2 >> bulk_get_users3_4 >> get_effective_user_group_membership_5 >> date_split_today_6 >> if_userdetails_isenabled_is_not_true_7
        if_userdetails_isenabled_is_not_true_7 >> rail.Label(
            'Yes') >> intercontinentalexchange_user_import_logs_add_entry_8 >> finish
        if_userdetails_isenabled_is_not_true_7 >> rail.Label(
            'No') >> if_userdetails_isenabled_is_not_true_10
        if_userdetails_isenabled_is_not_true_10 >> rail.Label(
            'Yes') >> enable_login_enablelogin_11 >> update_enddate >> invoke_custom_ruby_code_12
        if_userdetails_isenabled_is_not_true_10 >> rail.Label(
            'No') >> invoke_custom_ruby_code_12 >> if_output_adminmodified_equals_to_yes_13
        if_output_adminmodified_equals_to_yes_13 >> rail.Label(
            'Yes') >> intercontinentalexchange_user_import_logs_add_entry_14 >> finish
        if_output_adminmodified_equals_to_yes_13 >> rail.Label(
            'No') >> date_split_startdate_effectivedate_19 >> if_request_effective_date_present_20
        if_request_effective_date_present_20 >> rail.Label(
            'Yes') >> else_22 >> if_request_actual_termination_date_present_23
        if_request_effective_date_present_20 >> rail.Label(
            'No') >> if_request_actual_termination_date_present_23
        if_request_actual_termination_date_present_23 >> rail.Label(
            'Yes') >> date_split_enddate_24 >> update_employment_date_rangeforenddate_updateenddatewithnewstartdate_25 >> \
            if_request_firstname_present_dataworkato_servicereceive_requestrequestemployeefirstnamedowncase_32
        if_request_actual_termination_date_present_23 >> rail.Label(
            'No') >> if_request_firstname_present_dataworkato_servicereceive_requestrequestemployeefirstnamedowncase_32
        if_request_firstname_present_dataworkato_servicereceive_requestrequestemployeefirstnamedowncase_32 >> rail.Label(
            'Yes') >> update_first_name_33 >> if_request_lastname_present_dataworkato_servicereceive_requestrequestlastnamedowncase_34
        if_request_firstname_present_dataworkato_servicereceive_requestrequestemployeefirstnamedowncase_32 >> rail.Label(
            'No') >> if_request_lastname_present_dataworkato_servicereceive_requestrequestlastnamedowncase_34
        if_request_lastname_present_dataworkato_servicereceive_requestrequestlastnamedowncase_34 >> rail.Label(
            'Yes') >> update_last_name_35 >> if_request_work_email_present_36
        if_request_lastname_present_dataworkato_servicereceive_requestrequestlastnamedowncase_34 >> rail.Label(
            'No') >> if_request_work_email_present_36
        if_request_work_email_present_36 >> rail.Label(
            'Yes') >> update_email_37 >> apply_user_modifications2_38 >> declare_variable_39
        if_request_work_email_present_36 >> rail.Label(
            'No') >> declare_variable_39 >> if_request_week_hours_present_40
        if_request_week_hours_present_40 >> rail.Label(
            'Yes') >> update_numeric_value_weekhours_41 >> if_request_location_node_present_42
        if_request_week_hours_present_40 >> rail.Label(
            'No') >> if_request_location_node_present_42
        if_request_location_node_present_42 >> rail.Label(
            'Yes') >> update_text_value_businesstitle_43 >> if_request_department_present_44
        if_request_location_node_present_42 >> rail.Label(
            'No') >> if_request_department_present_44
        if_request_department_present_44 >> rail.Label(
            'Yes') >> if_request_department_blank_45
        if_request_department_blank_45 >> rail.Label(
            'Yes') >> insert_to_list_46 >> if_request_employeetypeuri_present_49
        if_request_department_blank_45 >> rail.Label(
            'No') >> update_department_group_48 >> if_request_employeetypeuri_present_49
        if_request_department_present_44 >> rail.Label(
            'No') >> if_request_employeetypeuri_present_49
        if_request_employeetypeuri_present_49 >> rail.Label(
            'Yes') >> if_request_employeetypeuri_blank_51
        if_request_employeetypeuri_blank_51 >> rail.Label(
            'Yes') >> insert_to_list_52 >> if_request_location_present_55
        if_request_employeetypeuri_blank_51 >> rail.Label(
            'No') >> update_employeetype_group_54 >> if_request_location_present_55
        if_request_employeetypeuri_present_49 >> rail.Label(
            'No') >> if_request_location_present_55
        if_request_location_present_55 >> rail.Label(
            'Yes') >> if_request_locationuri_blank_56
        if_request_locationuri_blank_56 >> rail.Label(
            'Yes') >> insert_to_list_57 >> if_request_legal_entity_id_present_61
        if_request_locationuri_blank_56 >> rail.Label(
            'No') >> update_location_60 >> if_request_legal_entity_id_present_61
        if_request_location_present_55 >> rail.Label(
            'No') >> if_request_legal_entity_id_present_61
        if_request_legal_entity_id_present_61 >> rail.Label(
            'Yes') >> if_request_legal_entity_name_blank_62
        if_request_legal_entity_name_blank_62 >> rail.Label(
            'Yes') >> insert_to_list_63 >> if_request_reporting_entity_id_present_66
        if_request_legal_entity_name_blank_62 >> rail.Label(
            'No') >> update_division_65 >> if_request_reporting_entity_id_present_66
        if_request_legal_entity_id_present_61 >> rail.Label(
            'No') >> if_request_reporting_entity_id_present_66
        if_request_reporting_entity_id_present_66 >> rail.Label(
            'Yes') >> if_request_reporting_entity_name_blank_67
        if_request_reporting_entity_name_blank_67 >> rail.Label(
            'Yes') >> insert_to_list_68 >> if_request_timezoneuri_present_72
        if_request_reporting_entity_name_blank_67 >> rail.Label(
            'No') >> update_costcenter_71 >> if_request_timezoneuri_present_72
        if_request_reporting_entity_id_present_66 >> rail.Label(
            'No') >> if_request_timezoneuri_present_72
        if_request_timezoneuri_present_72 >> rail.Label(
            'Yes') >> if_request_timezoneuri_not_equals_to_datarestbulk_get_users3_4responsedfirsttimezoneuri_73
        if_request_timezoneuri_not_equals_to_datarestbulk_get_users3_4responsedfirsttimezoneuri_73 >> rail.Label(
            'Yes') >> update_time_zone_for_user_74 >> if_request_holidaycalendaruri_present_75
        if_request_timezoneuri_not_equals_to_datarestbulk_get_users3_4responsedfirsttimezoneuri_73 >> rail.Label(
            'No') >> if_request_holidaycalendaruri_present_75
        if_request_timezoneuri_present_72 >> rail.Label(
            'No') >> if_request_holidaycalendaruri_present_75
        if_request_holidaycalendaruri_present_75 >> rail.Label(
            'Yes') >> update_holiday_calendar_for_user_76 >> if_request_work_schedule_present_77
        if_request_holidaycalendaruri_present_75 >> rail.Label(
            'No') >> if_request_work_schedule_present_77
        if_request_work_schedule_present_77 >> rail.Label(
            'Yes') >> if_schedulepolicies_to_json_contains_urn_78
        if_schedulepolicies_to_json_contains_urn_78 >> rail.Label(
            'Yes') >> invoke_custom_ruby_code_80 >> \
            if_schedulepolicies_displaytext_blank_dataworkato_servicereceive_requestrequestinitialschedulename_81
        if_schedulepolicies_to_json_contains_urn_78 >> rail.Label(
            'No') >> if_schedulepolicies_displaytext_blank_dataworkato_servicereceive_requestrequestinitialschedulename_81
        if_schedulepolicies_displaytext_blank_dataworkato_servicereceive_requestrequestinitialschedulename_81 >> rail.Label(
            'Yes') >> if_request_work_schedule_equals_to_shiftschedule_82
        if_request_work_schedule_equals_to_shiftschedule_82 >> rail.Label(
            'Yes') >> updateofficeschedule_83 >> get_all_permission_sets_86
        if_request_work_schedule_equals_to_shiftschedule_82 >> rail.Label(
            'No') >> updateofficeschedule_85 >> get_all_permission_sets_86
        if_schedulepolicies_displaytext_blank_dataworkato_servicereceive_requestrequestinitialschedulename_81 >> rail.Label(
            'No') >> get_all_permission_sets_86
        if_request_work_schedule_present_77 >> rail.Label(
            'No') >> get_all_permission_sets_86 >> if_request_line_manager_present_87
        if_request_line_manager_present_87 >> rail.Label(
            'No') >> if_request_line_manager_blank_109
        if_request_line_manager_present_87 >> rail.Label(
            'Yes') >> search_users_88 >> if_search_users_88_users_greater_than_1_89
        if_search_users_88_users_greater_than_1_89 >> rail.Label(
            'Yes') >> insert_to_list_90 >> if_request_line_manager_blank_109
        if_search_users_88_users_greater_than_1_89 >> rail.Label(
            'No') >> log_supervisorcheck_92 >> if_log_supervisorcheck_92_blank_93
        if_log_supervisorcheck_92_blank_93 >> rail.Label(
            'Yes') >> ice_supervisor_check_add_entry_94 >> if_request_line_manager_blank_109
        if_log_supervisorcheck_92_blank_93 >> rail.Label(
            'No') >> bulk_get_users3_96 >> if_request_line_manager_equals_to_dataworkato_servicereceive_requestrequestemployeeid_97
        if_request_line_manager_equals_to_dataworkato_servicereceive_requestrequestemployeeid_97 >> rail.Label(
            'Yes') >> insert_to_list_98 >> if_request_line_manager_blank_109
        if_request_line_manager_equals_to_dataworkato_servicereceive_requestrequestemployeeid_97 >> rail.Label(
            'No') >> get_supervisor_assignment_detailsforuser_100 >> if_user_loginname_blank_101
        if_user_loginname_blank_101 >> rail.Label(
            'Yes') >> if_userdetails_isenabled_is_true_102
        if_userdetails_isenabled_is_true_102 >> rail.Label(
            'Yes') >> log_checkifmanagerpermissionisassigned_103 >> if_log_checkifmanagerpermissionisassigned_103_blank_104
        if_userdetails_isenabled_is_true_102 >> rail.Label(
            'No') >> ice_supervisor_check_add_entry_108 >> if_request_line_manager_blank_109
        if_log_checkifmanagerpermissionisassigned_103_blank_104 >> rail.Label(
            'Yes') >> assign_permission_set_to_user_manager_105 >> update_supervisor_assignment_schedule_over_date_range_106
        if_log_checkifmanagerpermissionisassigned_103_blank_104 >> rail.Label(
            'No') >> update_supervisor_assignment_schedule_over_date_range_106 >> if_request_line_manager_blank_109
        if_log_checkifmanagerpermissionisassigned_103_blank_104 >> rail.Label(
            'No') >> update_supervisor_assignment_schedule_over_date_range_106 >> if_request_line_manager_blank_109
        if_user_loginname_blank_101 >> rail.Label(
            'No') >> if_request_line_manager_blank_109
        if_request_line_manager_blank_109 >> rail.Label(
            'Yes') >> insert_to_list_110 >> log_exceptions_111
        if_request_line_manager_blank_109 >> rail.Label(
            'No') >> log_exceptions_111 >> intercontinentalexchange_user_import_logs_add_entry_111 >> intercontinentalexchange_user_import_logs_add_entry_113 >> finish

    return dag


rail.for_each_instance(create_dag)
