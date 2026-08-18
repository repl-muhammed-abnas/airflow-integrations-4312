from datetime import datetime, timedelta
import json
from airflow.models import Variable
import rail


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/oxfordfinancial/user_import/config.py


# pylint: disable=too-many-statements
def create_adduser_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'oxfordfinancial_user_import_create_users_{config.instance}',
        description=f'Create User {config.instance}',
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
            no_task='is_loginname_employeetype_present'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='is_loginname_employeetype_present',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        is_loginname_employeetype_present = rail.IfOperator(
            task_id='is_loginname_employeetype_present',
            test="{{ dag_run.conf.Active_Directory_Login | sn | is_truthy and \
                dag_run.conf.Employee_Type | sn | is_truthy }}",
            yes_task="create_user",
            no_task="write_add_user_log"
        )

        def get_create_user_payload(dag_run):
            def get_replicon_datetime_obj(date_str, fmt='%m/%d/%Y'):
                datetime_obj = datetime.strptime(date_str, fmt)
                return {
                    'year': datetime_obj.year,
                    'month': datetime_obj.month,
                    'day': datetime_obj.day
                }
            return {
                "user": {
                    "target": {
                        "loginName": dag_run.conf['Active_Directory_Login']
                    },
                    "firstname": dag_run.conf['First_Name'],
                    "lastname": dag_run.conf['Last_Name'],
                    "emailAddress": dag_run.conf['Email'],
                    "department": {
                        "name": "oxford"
                    },
                    "employmentDateRange": {
                        "startDate": get_replicon_datetime_obj(dag_run.conf['Start_Date'])
                    },
                    "securityConfiguration": {
                        "enabledAuthenticationTypeUris": [
                            "urn:replicon:user-authentication-type:sso"
                        ],
                        "isLoginEnabled": "true",
                        "loginName": dag_run.conf['Active_Directory_Login']
                    },
                    "permissionSets": [
                        {
                            "uri": dag_run.conf['permission_set_uri']
                        }
                    ],
                    "policySets": [
                        {
                            "name": "Time Off"
                        }
                    ],
                    "employeeType": {
                        "name": dag_run.conf['Employee_Type']
                    }
                }
            }
        create_user = rail.RepliconServiceOperator(
            task_id='create_user',
            endpoint="/services/ImportService1.svc/PutUser3",
            data=get_create_user_payload
        )

        update_sf_id = rail.RepliconServiceOperator(
            task_id='update_sf_id',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ dag_run.conf.sfid_uri }}",
                "value": "{{ dag_run.conf.SF_18_Digit_ID }}"
            }
        )

        update_middle_name = rail.RepliconServiceOperator(
            task_id='update_middle_name',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ dag_run.conf.middle_name_uri }}",
                "value": "{{ dag_run.conf.Middle_Name }}"
            }
        )

        is_location_uri = rail.IfOperator(
            task_id='is_location_uri',
            test='{{ dag_run.conf.location_uri | is_truthy }}',
            yes_task='put_location_schedule_for_user',
            no_task='update_timeoff_approval_path'
        )

        put_location_schedule_for_user = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "scheduleEntries": [
                    {
                        "location": {
                            "uri": "{{ dag_run.conf.location_uri }}"
                        }
                    }
                ]
            }
        )

        update_timeoff_approval_path = rail.RepliconServiceOperator(
            task_id='update_timeoff_approval_path',
            endpoint="/services/TimeOffApprovalService1.svc/UpdateApprovalPathForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "approvalPathUri": "urn:replicon-tenant:{{ get_tenant_slug() }}:approval-path:6"
            }
        )

        update_timesheet_approval_path = rail.RepliconServiceOperator(
            task_id='update_timesheet_approval_path',
            endpoint="/services/TimesheetApprovalService1.svc/UpdateApprovalPathForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "approvalPathUri": "urn:replicon-tenant:{{ get_tenant_slug() }}:approval-path:4"
            }
        )

        update_holiday_calendar = rail.RepliconServiceOperator(
            task_id='update_holiday_calendar',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "holidayCalendarUri": "urn:replicon-tenant:{{ get_tenant_slug() }}:holiday-calendar:10"
            }
        )

        update_time_zone = rail.RepliconServiceOperator(
            task_id='update_time_zone',
            endpoint="/services/InternationalizationService1.svc/UpdateTimeZoneForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "timeZoneUri": "urn:replicon:time-zone:america-new-york"
            }
        )

        update_default_workweek = rail.RepliconServiceOperator(
            task_id='update_default_workweek',
            endpoint="/services/UserService1.svc/UpdateWorkWeekStartDayForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "dayOfWeekUri": "urn:replicon:day-of-week:sunday"
            }
        )

        add_payrule = rail.RepliconServiceOperator(
            task_id='add_payrule',
            endpoint="/services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "scheduleEntries": [
                    {
                        "payRuleScript": {
                            "uri": "urn:replicon-tenant:{{ get_tenant_slug() }}:script:ed3b3c8c-5caa-4e8f-b170-8685482aeb09"
                        }
                    }
                ]
            }
        )

        put_notification_preferences = rail.RepliconServiceOperator(
            task_id='put_notification_preferences',
            endpoint="/services/NotificationScriptAdministrationService1.svc/PutUserNotificationPreferences",
            data={
                "user": {
                    "uri": "{{ result('create_user').uri }}"
                },
                "preferences": {
                    "notificationDeliveryPreferences": [
                        {
                            "objectTypeUri": "urn:replicon:object-type:user",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
                        },
                        {
                            "objectTypeUri": "urn:replicon:object-type:timesheet",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
                        },
                        {
                            "objectTypeUri": "urn:replicon:object-type:pay-rule-script",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                        },
                        {
                            "objectTypeUri": "urn:replicon:object-type:time-off",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
                        }
                    ]
                }
            }
        )

        assign_timeoff_template = rail.RepliconServiceOperator(
            task_id='assign_timeoff_template',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "policySetUri": "urn:replicon-tenant:{{ get_tenant_slug() }}:policy-set:a5fd7f68-0728-496f-b314-91593461a168"
            }
        )

        get_timeoff_policy_user = rail.RepliconServiceOperator(
            task_id='get_timeoff_policy_user',
            endpoint="/services/TimeOffService1.svc/GetTimeOffPolicyForUser2",
            data={
                "userUri": "{{ result('create_user').uri }}"
            }
        )

        for_each_timeoff_policies = rail.ForEachOperator(
            task_id='for_each_timeoff_policies',
            items=lambda: rail.result('get_timeoff_policy_user')[
                'timeOffPoliciesByTimeOffType'],
            start_task='get_default_timeoff_policy_schedule',
            end_task='for_each_timeoff_policies_end'
        )

        def get_timeoff_schedule(response):
            if response:
                effective_day = response[0].get('effectiveDate', {}).get(
                    'day') if response[0] else None
                if effective_day:
                    return json.loads(json.dumps(response, ensure_ascii=False).replace(
                        'null', '"effective"').replace('"script"', '"scriptTarget"'))
            return ''
        get_default_timeoff_policy_schedule = rail.RepliconServiceOperator(
            task_id='get_default_timeoff_policy_schedule',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ result('create_user').uri }}",
                    "timeOffTypeUri": "{{ result('for_each_timeoff_policies').timeOffType.uri }}"
                }
            },
            data_handler=get_timeoff_schedule
        )

        should_assign_policy = rail.IfOperator(
            task_id='should_assign_policy',
            test="{{ result('get_default_timeoff_policy_schedule') | is_truthy }}",
            yes_task="put_user_timeoff_account_policyset_schedule",
            no_task="for_each_timeoff_policies_end"
        )

        put_user_timeoff_account_policyset_schedule = rail.RepliconServiceOperator(
            task_id='put_user_timeoff_account_policyset_schedule',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda: {
                "timeOffAccount": {
                    "userUri": rail.result('create_user')['uri'],
                    "timeOffTypeUri": rail.result('for_each_timeoff_policies')['timeOffType']['uri']
                },
                "policySetScheduleEntries": rail.result('get_default_timeoff_policy_schedule')
            }
        )

        for_each_timeoff_policies_end = rail.EmptyOperator(
            task_id='for_each_timeoff_policies_end'
        )

        is_supervisor_present = rail.IfOperator(
            task_id='is_supervisor_present',
            test="{{ dag_run.conf.Supervisor | sn | is_truthy }}",
            yes_task="get_supervisor_useruri",
            no_task="write_add_user_log"
        )

        def get_supervisor_uri(response, dag_run):
            filtered_supervisor = list(filter(lambda x: x['cells'][0]['textValue'] == dag_run.conf['Supervisor'],
                                              response['rows'])) if response['rows'] else []
            if filtered_supervisor:
                return rail.smartjoin_by_delim([x['cells'][0]['uri'] for x in filtered_supervisor], ' ') if response['rows'] else ''
            return ''
        get_supervisor_useruri = rail.RepliconServiceOperator(
            task_id='get_supervisor_useruri',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:user-list-column:user-name"
                ]
            },
            data_handler=get_supervisor_uri
        )

        is_supervisor_present_in_replicon = rail.IfOperator(
            task_id='is_supervisor_present_in_replicon',
            test="{{ result('get_supervisor_useruri') | is_truthy }}",
            yes_task="update_user_supervisor",
            no_task="write_add_user_log"
        )

        update_user_supervisor = rail.RepliconServiceOperator(
            task_id='update_user_supervisor',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "supervisorUri": "{{ result('get_supervisor_useruri') }}"
            }
        )

        write_add_user_log = rail.WriteLogOperator(
            task_id='write_add_user_log',
            log="{{ dag_run.conf.log }}",
            message="Created",
            severity="Success",
            properties={
                "loginname": "{{ dag_run.conf.Active_Directory_Login }}",
                "sf18digitid": "{{ dag_run.conf.SF_18_Digit_ID }}",
                "status": "Success",
                "reason": "Created"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ dag_run.conf.log }}",
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error',
            properties={
                "loginname": "{{ dag_run.conf.Active_Directory_Login }}",
                "sf18digitid": "{{ dag_run.conf.SF_18_Digit_ID }}",
                "status": "Error",
                "reason": '{{ get_error_message() }}'
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            'No') >> is_loginname_employeetype_present
        is_loginname_employeetype_present >> rail.Label(
            'Yes') >> create_user >> update_sf_id >> update_middle_name >> is_location_uri
        is_location_uri >> rail.Label(
            'Yes') >> put_location_schedule_for_user >> update_timeoff_approval_path
        is_location_uri >> rail.Label(
            'No') >> update_timeoff_approval_path
        update_timeoff_approval_path >> update_timesheet_approval_path >> update_holiday_calendar >> \
            update_time_zone >> update_default_workweek >> add_payrule >> put_notification_preferences >> \
            assign_timeoff_template >> get_timeoff_policy_user >> for_each_timeoff_policies
        for_each_timeoff_policies >> get_default_timeoff_policy_schedule >> \
            should_assign_policy
        should_assign_policy >> rail.Label(
            'Yes') >> put_user_timeoff_account_policyset_schedule >> for_each_timeoff_policies_end
        should_assign_policy >> rail.Label(
            'No') >> for_each_timeoff_policies_end
        for_each_timeoff_policies >> for_each_timeoff_policies_end
        for_each_timeoff_policies_end >> is_supervisor_present
        is_supervisor_present >> rail.Label(
            'Yes') >> get_supervisor_useruri >> is_supervisor_present_in_replicon
        is_supervisor_present_in_replicon >> rail.Label(
            'Yes') >> update_user_supervisor >> write_add_user_log
        is_supervisor_present_in_replicon >> rail.Label(
            'No') >> write_add_user_log
        is_supervisor_present >> rail.Label(
            'No') >> write_add_user_log
        is_loginname_employeetype_present >> rail.Label(
            'No') >> write_add_user_log

        write_add_user_log >> catch_and_log_errors

        catch_and_log_errors >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_adduser_dag)
