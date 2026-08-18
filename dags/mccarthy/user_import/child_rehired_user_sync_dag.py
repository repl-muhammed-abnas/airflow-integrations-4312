from datetime import timedelta, datetime
import json
from airflow.models import Variable
import rail


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/mccarthy/user_import/config.py


# pylint: disable=too-many-statements
def create_rehireuser_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'mccarthy_user_import_rehired_user_sync_child_{config.instance}',
        description=f'LIVE | Rehired User Sync_Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='bulk_get_users3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='bulk_get_users3',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        bulk_get_users3 = rail.RepliconServiceOperator(
            task_id='bulk_get_users3',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": "{{ dag_run.conf.useruri }}"
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: response[0] if response else ''
        )

        should_update_firstname = rail.IfOperator(
            task_id='should_update_firstname',
            test="{{ dag_run.conf.Firstname | is_truthy and result('bulk_get_users3').userDetails.firstName | lower \
                != dag_run.conf.Firstname | lower }}",
            yes_task="update_first_name",
            no_task="should_update_lastname"
        )

        update_first_name = rail.RepliconServiceOperator(
            task_id='update_first_name',
            endpoint="/services/UserService1.svc/UpdateFirstName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "firstname": "{{ dag_run.conf.Firstname }}"
            }
        )

        should_update_lastname = rail.IfOperator(
            task_id='should_update_lastname',
            test="{{ dag_run.conf.Lastname | is_truthy and result('bulk_get_users3').userDetails.lastName | lower \
                != dag_run.conf.Lastname | lower }}",
            yes_task="update_lastname",
            no_task="is_email_changed"
        )

        update_lastname = rail.RepliconServiceOperator(
            task_id='update_lastname',
            endpoint="/services/UserService1.svc/UpdateLastName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "lastname": "{{ dag_run.conf.Lastname }}"
            }
        )

        def is_email_changed_test(dag_run):
            if dag_run.conf['Email']:
                email_address = dag_run.conf['Email'].lower()
                replicon_user_email_address = rail.result(
                    'bulk_get_users3')['userDetails']['emailAddress'].lower() if rail.result(
                    'bulk_get_users3')['userDetails']['emailAddress'] else None
                return email_address != replicon_user_email_address
            return False
        is_email_changed = rail.IfOperator(
            task_id='is_email_changed',
            test=is_email_changed_test,
            yes_task="update_email",
            no_task="is_enabledauthenticationtypeuri_sso"
        )

        update_email = rail.RepliconServiceOperator(
            task_id='update_email',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "email": "{{ dag_run.conf.Email }}"
            }
        )

        is_enabledauthenticationtypeuri_sso = rail.IfOperator(
            task_id='is_enabledauthenticationtypeuri_sso',
            test="{{ result('bulk_get_users3').securityConfiguration.enabledAuthenticationTypeUris | first_or_default \
                !='urn:replicon:user-authentication-type:sso' }}",
            yes_task="update_authentication_type",
            no_task="put_permission_sets"
        )

        update_authentication_type = rail.RepliconServiceOperator(
            task_id='update_authentication_type',
            endpoint="/services/SecurityService1.svc/SetSSOAuthenticationForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "loginName": "{{ dag_run.conf.Loginname }}"
            }
        )

        def get_putpermissionset_assignments(dag_run):
            permission_set_uris = [dag_run.conf['Permissionsuri']]
            supervisor_permission_seturi = rail.find_first_by_attr_and_get_attr(
                rail.result('bulk_get_users3')['permissionSets'], 'slug', 'supervisor-supervisor', 'uri', '')
            if supervisor_permission_seturi:
                permission_set_uris.append(supervisor_permission_seturi)
            return {
                "userUri": dag_run.conf['useruri'],
                "permissionSetUris": permission_set_uris
            }
        put_permission_sets = rail.RepliconServiceOperator(
            task_id='put_permission_sets',
            endpoint="/services/PermissionSetService1.svc/PutPermissionSetAssignmentsForUser",
            data=get_putpermissionset_assignments
        )

        def get_customfields_to_update():
            dag_run_conf = rail.get_current_context()['dag_run'].conf
            custom_fields_to_update = []

            current_user_payrollname = rail.find_first_by_attr_and_get_attr(
                rail.result('bulk_get_users3')[
                    'userDetails']['customFieldValues'],
                'customField.displayText', "Payroll Name", 'text')
            payroll_name = dag_run_conf['Payrollname']
            if payroll_name and payroll_name != current_user_payrollname:
                custom_fields_to_update.append({
                    "customField": {
                        "uri": dag_run_conf['Payrollnameuri']
                    },
                    "dropDownOption": {
                        "name": payroll_name
                    }
                })
            current_user_employeecategory = rail.find_first_by_attr_and_get_attr(
                rail.result('bulk_get_users3')[
                    'userDetails']['customFieldValues'],
                'customField.displayText', "Employee Category", 'text')
            employeecategory = dag_run_conf['Employeecategory']
            if employeecategory and employeecategory != current_user_employeecategory:
                custom_fields_to_update.append({
                    "customField": {
                        "uri": dag_run_conf['Employeecategoryuri']
                    },
                    "dropDownOption": {
                        "name": employeecategory
                    }
                })
            current_user_employeeworkstate = rail.find_first_by_attr_and_get_attr(
                rail.result('bulk_get_users3')[
                    'userDetails']['customFieldValues'],
                'customField.displayText', "Employee Work State", 'text')
            employeeworkstate = dag_run_conf['Employeeworkstate']
            if employeeworkstate and employeeworkstate != current_user_employeeworkstate:
                custom_fields_to_update.append({
                    "customField": {
                        "uri": dag_run_conf['Employeeworkstateuri']
                    },
                    "dropDownOption": {
                        "name": employeeworkstate
                    }
                })
            current_user_legalentity = rail.find_first_by_attr_and_get_attr(
                rail.result('bulk_get_users3')[
                    'userDetails']['customFieldValues'],
                'customField.displayText', "Legal Entity", 'text')
            legalentity = dag_run_conf['Legalentity']
            if legalentity and legalentity != current_user_legalentity:
                custom_fields_to_update.append({
                    "customField": {
                        "uri": dag_run_conf['Legalentityuri']
                    },
                    "dropDownOption": {
                        "name": legalentity
                    }
                })
            current_user_jobtitle = rail.find_first_by_attr_and_get_attr(
                rail.result('bulk_get_users3')[
                    'userDetails']['customFieldValues'],
                'customField.displayText', "Job Title", 'text')
            jobtitle = dag_run_conf['Jobtitle']
            if jobtitle and jobtitle != current_user_jobtitle:
                custom_fields_to_update.append({
                    "customField": {
                        "uri": dag_run_conf['Jobtitleuri']
                    },
                    "text": jobtitle
                })
            current_user_organization = rail.find_first_by_attr_and_get_attr(
                rail.result('bulk_get_users3')[
                    'userDetails']['customFieldValues'],
                'customField.displayText', "Organization", 'text')
            organization = dag_run_conf['Organization']
            if organization and organization != current_user_organization:
                custom_fields_to_update.append({
                    "customField": {
                        "uri": dag_run_conf['Organizationuri']
                    },
                    "text": organization
                })
            return custom_fields_to_update
        get_user_customfields_to_update = rail.PythonOperator(
            task_id='get_user_customfields_to_update',
            python_callable=get_customfields_to_update
        )

        is_customfields_to_update = rail.IfOperator(
            task_id='is_customfields_to_update',
            test="{{ result('get_user_customfields_to_update') | length > 0 }}",
            yes_task="update_user_customfields",
            no_task="is_supervisor_assign_pending"
        )

        update_user_customfields = rail.RepliconServiceOperator(
            task_id='update_user_customfields',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri']
                },
                "modifications": {
                    "customFieldValuesToApply": rail.result('get_user_customfields_to_update')
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        is_supervisor_assign_pending = rail.IfOperator(
            task_id='is_supervisor_assign_pending',
            test="{{ dag_run.conf.Supervisorid | is_truthy and dag_run.conf.Supervisorid != \
                result('bulk_get_users3') | attr_or_default('supervisorAssignmentSchedule') | \
                    first_or_default | attr_or_default('supervisor.user.loginName') and \
                        dag_run.conf.Supervisorid != dag_run.conf.Employeeid }}",
            yes_task="write_supervisor_pending_log",
            no_task="get_effective_user_group_membership"
        )

        write_supervisor_pending_log = rail.WriteLogOperator(
            task_id='write_supervisor_pending_log',
            log='{{ dag_run.conf.supervisor_log }}',
            message="na",
            severity="Pending",
            properties=lambda dag_run: {
                "userloginname": dag_run.conf['Loginname'],
                "useruri": dag_run.conf['useruri'],
                "supervisorloginname": dag_run.conf['Supervisorid'],
                "action": "Update",
                "status": "Pending",
                "emplid": dag_run.conf['Employeeid'],
                "effective_date": dag_run.conf['Supervisoreffectivedate'],
                "user_log": dag_run.conf['log']
            }
        )

        get_effective_user_group_membership = rail.RepliconServiceOperator(
            task_id='get_effective_user_group_membership',
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        is_department_present = rail.IfOperator(
            task_id='is_department_present',
            test="{{ dag_run.conf.Departmenturi | sn | is_truthy and dag_run.conf.Departmenturi != \
                result('get_effective_user_group_membership').departments | first_or_default | \
                    attr_or_default('department.department.uri') }}",
            yes_task="update_department_group",
            no_task="get_locationgroup_payload"
        )

        def get_replicon_date(date_str, fmt='%m/%d/%Y'):
            datetime_obj = datetime.strptime(date_str, fmt)
            return {
                'year': datetime_obj.year,
                'month': datetime_obj.month,
                'day': datetime_obj.day
            }
        update_department_group = rail.RepliconServiceOperator(
            task_id='update_department_group',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri']
                },
                "modifications": {
                    "departmentGroupScheduleToApply": {
                        "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "updateDepartmentGroupScheduleOverDateRange": {
                            "replacementDepartmentGroupScheduleEntries": [
                                {
                                    "departmentGroup": {
                                        "uri": dag_run.conf['Departmenturi']
                                    },
                                    "effectiveDate": get_replicon_date(dag_run.conf['Startdate'])
                                }
                            ]
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        def get_update_location_group():
            dag_run_conf = rail.get_current_context()['dag_run'].conf
            basic_user_permission_uri = ''
            supervisor_permission_uri = ''
            location_uri = ''
            if rail.result('bulk_get_users3')['permissionSets']:
                basic_user_permission_uri = rail.find_first_by_attr_and_get_attr(
                    rail.result('bulk_get_users3')['permissionSets'], 'slug', 'project-resource-with-reports', 'uri', '')
                supervisor_permission_uri = rail.find_first_by_attr_and_get_attr(
                    rail.result('bulk_get_users3')['permissionSets'], 'slug', 'supervisor-supervisor', 'uri', '')
            current_user_location = rail.result(
                'bulk_get_users3')['locationSchedule'][0]['location']['displayText'] if rail.result(
                    'bulk_get_users3')['locationSchedule'] else None
            if basic_user_permission_uri and current_user_location != 'Basic User' and not supervisor_permission_uri:
                location_uri = dag_run_conf['locationuri']
            elif current_user_location != 'Supervisor' and supervisor_permission_uri:
                location_uri = dag_run_conf['locationforsupervisor']
            return {
                "userUri": dag_run_conf['useruri'],
                "scheduleEntries": [
                    {
                        "location": {
                            "uri": location_uri
                        }
                    }
                ]
            } if location_uri else ''

        get_locationgroup_payload = rail.PythonOperator(
            task_id='get_locationgroup_payload',
            python_callable=get_update_location_group
        )

        is_updatelocationgroup_present = rail.IfOperator(
            task_id='is_updatelocationgroup_present',
            test="{{ result('get_locationgroup_payload') | is_truthy }}",
            yes_task="update_location_group",
            no_task="should_update_employeetype"
        )

        update_location_group = rail.RepliconServiceOperator(
            task_id='update_location_group',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data=lambda: rail.result('get_locationgroup_payload')
        )

        should_update_employeetype = rail.IfOperator(
            task_id='should_update_employeetype',
            test="{{ dag_run.conf.Employeetypeuri | is_truthy and dag_run.conf.Employeetypeuri \
                != result('get_effective_user_group_membership').employeeTypes | first_or_default | \
                    attr_or_default('employeeType.employeeType.uri') }}",
            yes_task="update_employeetype_group",
            no_task="assign_timesheet_template"
        )

        update_employeetype_group = rail.RepliconServiceOperator(
            task_id='update_employeetype_group',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri']
                },
                "modifications": {
                    "employeeTypeGroupScheduleToApply": {
                        "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "updateEmployeeTypeGroupScheduleOverDateRange": {
                            "replacementEmployeeTypeGroupScheduleEntries": [
                                {
                                    "employeeTypeGroup": {
                                        "uri": dag_run.conf['Employeetypeuri']
                                    },
                                    "effectiveDate": get_replicon_date(dag_run.conf['Startdate'])
                                }
                            ]
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        assign_timesheet_template = rail.RepliconServiceOperator(
            task_id='assign_timesheet_template',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "policySetUri": dag_run.conf['Timesheettemplateuri'] if dag_run.conf['Timesheettemplateuri'] else dag_run.conf['Defaulttimesheettemplate']
            }
        )

        is_timeoff_templateuri_present = rail.IfOperator(
            task_id='is_timeoff_templateuri_present',
            test="{{ dag_run.conf.Timeofftemplateuri | is_truthy }}",
            yes_task="assign_timeoff_template",
            no_task="is_activities_present"
        )

        assign_timeoff_template = rail.RepliconServiceOperator(
            task_id='assign_timeoff_template',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "policySetUri": "{{ dag_run.conf.Timeofftemplateuri }}"
            }
        )

        is_activities_present = rail.IfOperator(
            task_id='is_activities_present',
            test="{{ dag_run.conf.Activities | length > 0 }}",
            yes_task="update_activity_assignments_user",
            no_task="get_enabled_timeoff_type_uris"
        )

        update_activity_assignments_user = rail.RepliconServiceOperator(
            task_id='update_activity_assignments_user',
            endpoint="/services/ActivityService1.svc/UpdateActivityAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "activityUris": dag_run.conf['Activities']
            }
        )

        def get_enabled_timeofftype_uris(response):
            return [x['uri'] for x in response if x['uri']]
        get_enabled_timeoff_type_uris = rail.RepliconServiceOperator(
            task_id='get_enabled_timeoff_type_uris',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
            data_handler=get_enabled_timeofftype_uris
        )

        is_timeoffuris_present = rail.IfOperator(
            task_id='is_timeoffuris_present',
            test="{{ result('get_enabled_timeoff_type_uris') | length > 0 }}",
            yes_task="put_timeoff_type_assignments",
            no_task="is_workweekstartday_monday"
        )

        put_timeoff_type_assignments = rail.RepliconServiceOperator(
            task_id='put_timeoff_type_assignments',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUris": rail.result('get_enabled_timeoff_type_uris')
            }
        )

        for_each_timeoff_uri = rail.ForEachOperator(
            task_id='for_each_timeoff_uri',
            items=lambda: rail.result('get_enabled_timeoff_type_uris'),
            start_task='get_default_timeoff_type_policy_schedule',
            end_task='for_each_timeoff_uri_end'
        )

        get_default_timeoff_type_policy_schedule = rail.RepliconServiceOperator(
            task_id='get_default_timeoff_type_policy_schedule',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ result('for_each_timeoff_uri') }}"
                }
            },
            data_handler=lambda response: json.loads(json.dumps([x for x in response if x['policySet']], ensure_ascii=False).replace(
                'null', '"effective"').replace('"script"', '"scriptTarget"')) if response and response[0] and response[0]['policySet'] else ''
        )

        is_policy_present = rail.IfOperator(
            task_id='is_policy_present',
            test="{{ result('get_default_timeoff_type_policy_schedule') | is_truthy }}",
            yes_task="put_user_timeoff_account_policyschedule",
            no_task="for_each_timeoff_uri_end"
        )

        put_user_timeoff_account_policyschedule = rail.RepliconServiceOperator(
            task_id='put_user_timeoff_account_policyschedule',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('for_each_timeoff_uri')
                },
                "policySetScheduleEntries": rail.result('get_default_timeoff_type_policy_schedule')
            }
        )

        for_each_timeoff_uri_end = rail.EmptyOperator(
            task_id='for_each_timeoff_uri_end'
        )

        is_workweekstartday_monday = rail.IfOperator(
            task_id='is_workweekstartday_monday',
            test="{{ result('bulk_get_users3').userDetails.workWeekStartDay.displayText != 'Monday' }}",
            yes_task="reassign_workweek_monday",
            no_task="is_holidaycalendar_mccarthyholidays"
        )

        reassign_workweek_monday = rail.RepliconServiceOperator(
            task_id='reassign_workweek_monday',
            endpoint="/services/UserService1.svc/UpdateWorkWeekStartDayForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "dayOfWeekUri": "urn:replicon:day-of-week:monday"
            }
        )

        is_holidaycalendar_mccarthyholidays = rail.IfOperator(
            task_id='is_holidaycalendar_mccarthyholidays',
            test="{{ result('bulk_get_users3').holidayCalendar.displayText != 'McCarthy Holidays' }}",
            yes_task="reassign_holiday_calendar_mccarthyholidays",
            no_task="is_officeschedule_defaultschedule"
        )

        reassign_holiday_calendar_mccarthyholidays = rail.RepliconServiceOperator(
            task_id='reassign_holiday_calendar_mccarthyholidays',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}"
                },
                "modifications": {
                    "holidayCalendarToApply": {
                        "holidayCalendar": {
                            "name": "McCarthy Holidays"
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"}
        )

        is_officeschedule_defaultschedule = rail.IfOperator(
            task_id='is_officeschedule_defaultschedule',
            test="{{ result('bulk_get_users3').schedulePolicies | first_or_default | \
                attr_or_default('officeSchedule.displayText') != 'Default Schedule' }}",
            yes_task="reassign_defaultschedule_officeschedule",
            no_task="enable_all_notifications"
        )

        reassign_defaultschedule_officeschedule = rail.RepliconServiceOperator(
            task_id='reassign_defaultschedule_officeschedule',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=lambda dag_run: {
                "userModificationOptionUri": "urn:replicon:user-modification-option:save",
                "user": {
                    "loginName": dag_run.conf['Loginname']
                },
                "modifications": {
                    "schedulePolicyToApply": {
                        "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "updateScheduleOverDateRange": {
                            "replacementScheduleEntries": [
                                {
                                    "schedulePolicy": {
                                        "officeSchedule": {
                                            "name": "Default Schedule"
                                        },
                                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                                    },
                                    "effectiveDate": get_replicon_date(dag_run.conf['Startdate'])
                                }
                            ]
                        }
                    }
                }
            }
        )

        enable_all_notifications = rail.RepliconServiceOperator(
            task_id='enable_all_notifications',
            endpoint="/services/NotificationScriptAdministrationService1.svc/PutUserNotificationPreferences",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}"
                },
                "preferences": {
                    "notificationDeliveryPreferences": [
                        {
                            "objectTypeUri": "urn:replicon:object-type:timesheet",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
                        },
                        {
                            "objectTypeUri": "urn:replicon:object-type:user",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
                        },
                        {
                            "objectTypeUri": "urn:replicon:object-type:time-entry-revision-group",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
                        },
                        {
                            "objectTypeUri": "urn:replicon:object-type:pay-rule-script",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
                        },
                        {
                            "objectTypeUri": "urn:replicon:object-type:time-off",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
                        },
                        {
                            "objectTypeUri": "urn:replicon:object-type:holiday",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
                        },
                        {
                            "objectTypeUri": "urn:replicon:object-type:project",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
                        }
                    ],
                    "sharedDeliveryPreferenceOptionUris": [
                        "urn:replicon:user-shared-delivery-preference-option:workday-deliver"
                    ]
                }
            }
        )

        def get_rehireuser_exception():
            dag_run_conf = rail.get_current_context()['dag_run'].conf
            supervisor_assignment_schedule = rail.result(
                'bulk_get_users3')['supervisorAssignmentSchedule']
            current_supervisor = supervisor_assignment_schedule[0][
                'supervisor']['user']['loginName'] if supervisor_assignment_schedule else ''
            if dag_run_conf['Supervisorid'] and dag_run_conf['Supervisorid'] != current_supervisor and \
                    dag_run_conf['Supervisorid'] == dag_run_conf['Employeeid']:
                return 'supervisor could not be assigned as the supervisor ID received is same as user employee id'
            return ''
        get_rehireuser_exception_logs = rail.PythonOperator(
            task_id='get_rehireuser_exception_logs',
            python_callable=get_rehireuser_exception
        )

        write_rehireuser_log = rail.WriteLogOperator(
            task_id='write_rehireuser_log',
            log="{{ dag_run.conf.log }}",
            message='\
                    {%- if result("get_rehireuser_exception_logs") | is_truthy -%} \
                        Updated partially - {{ result("get_rehireuser_exception_logs") }}\
                    {%- else -%} \
                        Updated successfully\
                    {%- endif -%}',
            severity='\
                    {%- if result("get_rehireuser_exception_logs") | is_truthy -%} \
                        Exception\
                    {%- else -%} \
                        Success\
                    {%- endif -%}',
            properties={
                'loginname': '{{ dag_run.conf.Loginname }}',
                'email': '{{ dag_run.conf.Email }}',
                'action': 'Update',
                'status': '\
                    {%- if result("get_rehireuser_exception_logs") | is_truthy -%} \
                        Exception\
                    {%- else -%} \
                        Success\
                    {%- endif -%}',
                'details': '\
                    {%- if result("get_rehireuser_exception_logs") | is_truthy -%} \
                        Updated partially - {{ result("get_rehireuser_exception_logs") }}\
                    {%- else -%} \
                        Updated successfully\
                    {%- endif -%}'
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ dag_run.conf.log }}",
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity="Error",
            properties={
                'loginname': '{{ dag_run.conf.Loginname }}',
                'email': '{{ dag_run.conf.Email }}',
                'action': 'Update',
                'status': 'Error',
                'details': '{{ get_error_message() }}'
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            'No') >> bulk_get_users3 >> should_update_firstname
        should_update_firstname >> rail.Label(
            'Yes') >> update_first_name >> should_update_lastname
        should_update_firstname >> rail.Label(
            'No') >> should_update_lastname
        should_update_lastname >> rail.Label(
            'Yes') >> update_lastname >> is_email_changed
        should_update_lastname >> rail.Label(
            'No') >> is_email_changed
        is_email_changed >> rail.Label(
            'Yes') >> update_email >> is_enabledauthenticationtypeuri_sso
        is_email_changed >> rail.Label(
            'No') >> is_enabledauthenticationtypeuri_sso
        is_enabledauthenticationtypeuri_sso >> rail.Label(
            'Yes') >> update_authentication_type >> put_permission_sets
        is_enabledauthenticationtypeuri_sso >> rail.Label(
            'No') >> put_permission_sets
        put_permission_sets >> get_user_customfields_to_update >> is_customfields_to_update
        is_customfields_to_update >> rail.Label(
            'Yes') >> update_user_customfields >> is_supervisor_assign_pending
        is_customfields_to_update >> rail.Label(
            'No') >> is_supervisor_assign_pending
        is_supervisor_assign_pending >> rail.Label(
            'Yes') >> write_supervisor_pending_log >> get_effective_user_group_membership
        is_supervisor_assign_pending >> rail.Label(
            'No') >> get_effective_user_group_membership
        get_effective_user_group_membership >> is_department_present
        is_department_present >> rail.Label(
            'Yes') >> update_department_group >> get_locationgroup_payload
        is_department_present >> rail.Label(
            'No') >> get_locationgroup_payload
        get_locationgroup_payload >> is_updatelocationgroup_present
        is_updatelocationgroup_present >> rail.Label(
            'Yes') >> update_location_group >> should_update_employeetype
        is_updatelocationgroup_present >> rail.Label(
            'No') >> should_update_employeetype
        should_update_employeetype >> rail.Label(
            'Yes') >> update_employeetype_group >> assign_timesheet_template
        should_update_employeetype >> rail.Label(
            'No') >> assign_timesheet_template
        assign_timesheet_template >> is_timeoff_templateuri_present
        is_timeoff_templateuri_present >> rail.Label(
            'Yes') >> assign_timeoff_template >> is_activities_present
        is_timeoff_templateuri_present >> rail.Label(
            'No') >> is_activities_present
        is_activities_present >> rail.Label(
            'Yes') >> update_activity_assignments_user >> get_enabled_timeoff_type_uris
        is_activities_present >> rail.Label(
            'No') >> get_enabled_timeoff_type_uris >> is_timeoffuris_present
        is_timeoffuris_present >> rail.Label(
            'Yes') >> put_timeoff_type_assignments >> for_each_timeoff_uri
        for_each_timeoff_uri >> get_default_timeoff_type_policy_schedule >> is_policy_present
        is_policy_present >> rail.Label(
            'Yes') >> put_user_timeoff_account_policyschedule >> for_each_timeoff_uri_end
        is_policy_present >> rail.Label(
            'No') >> for_each_timeoff_uri_end
        for_each_timeoff_uri >> for_each_timeoff_uri_end
        for_each_timeoff_uri_end >> is_workweekstartday_monday
        is_timeoffuris_present >> rail.Label(
            'No') >> is_workweekstartday_monday
        is_workweekstartday_monday >> rail.Label(
            'Yes') >> reassign_workweek_monday >> is_holidaycalendar_mccarthyholidays
        is_workweekstartday_monday >> rail.Label(
            'No') >> is_holidaycalendar_mccarthyholidays
        is_holidaycalendar_mccarthyholidays >> rail.Label(
            'Yes') >> reassign_holiday_calendar_mccarthyholidays >> is_officeschedule_defaultschedule
        is_holidaycalendar_mccarthyholidays >> rail.Label(
            'No') >> is_officeschedule_defaultschedule
        is_officeschedule_defaultschedule >> rail.Label(
            'Yes') >> reassign_defaultschedule_officeschedule >> enable_all_notifications
        is_officeschedule_defaultschedule >> rail.Label(
            'No') >> enable_all_notifications

        enable_all_notifications >> get_rehireuser_exception_logs >> write_rehireuser_log >> catch_and_log_errors

        catch_and_log_errors >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_rehireuser_dag)
