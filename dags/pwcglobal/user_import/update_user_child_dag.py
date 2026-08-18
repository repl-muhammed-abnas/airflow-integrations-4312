from datetime import timedelta
import rail
from pwcglobal.user_import import request_payload
from pwcglobal.user_import.task.update_country import get_update_country
from pwcglobal.user_import.task.update_timesheet_period import get_update_timesheet_period
from pwcglobal.user_import.task.update_supervisor import get_update_supervisor
from pwcglobal.user_import.task.update_timesheet_template import get_update_timesheet_template
from pwcglobal.user_import.task.update_custom_field import get_update_custom_field
from pwcglobal.user_import.task.update_timeoff_policy import get_update_timeoff_policy


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'pwcglobal_user_import_update_user_child_{config.instance}',
        description=f'PwCGlobal_User_Import_Child_User Update {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.user_dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        user_uri = '{{ dag_run.conf.useruri }}'

        null = None

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='can_update_login_name',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        can_update_login_name = rail.IfOperator(
            task_id='can_update_login_name',
            test=lambda: bool(request_payload.get_conf().get(
                'loginnameupdated', False)),
            yes_task='update_login_name',
            no_task='bulk_get_user3',
        )

        update_login_name = rail.RepliconServiceOperator(
            task_id='update_login_name',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data={
                "user": {
                    "uri": user_uri,
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications":  {
                    "securitySettingsToApply": {
                        "loginName": "{{dag_run.conf.loginname}}",
                        "ssoName": "{{dag_run.conf.loginname}}",
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        bulk_get_user3 = rail.RepliconServiceOperator(
            task_id='bulk_get_user3',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data={
                "users": [
                    {
                        "uri": user_uri,
                        "loginName": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            response_filter=lambda res: res.json()['d'][0]
        )

        get_assigned_policysets_for_user = rail.RepliconServiceOperator(
            task_id='get_assigned_policysets_for_user',
            endpoint='/services/policySetService1.svc/GetAssignedPolicySetsForUser',
            data={
                "userUri": user_uri,
            }
        )

        is_start_date_changed = rail.IfOperator(
            task_id='is_start_date_changed',
            test=lambda: request_payload.get_conf()['startdate'] and request_payload.get_replicon_date(request_payload.get_conf()['startdate']) and
            request_payload.get_date_from_replicon_date(request_payload.get_replicon_date(request_payload.get_conf()['startdate'])) !=
            request_payload.get_date_from_replicon_date(rail.result(
                'bulk_get_user3')['userDetails']['employmentDateRange']['startDate']),
            yes_task='update_start_date',
            no_task='is_rehire',
        )

        update_start_date = rail.RepliconServiceOperator(
            task_id='update_start_date',
            endpoint='/services/UserService1.svc/UpdateEmploymentDateRange',
            data=lambda: {
                "userUri": request_payload.get_user_uri(),
                "dateRange": {
                    "startDate": request_payload.get_replicon_date(request_payload.get_conf()['startdate']),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        update_timesheet_periodtype = rail.RepliconServiceOperator(
            task_id='update_timesheet_periodtype',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data=request_payload.get_timesheet_periodtype
        )

        is_rehire = rail.IfOperator(
            task_id='is_rehire',
            test=lambda: not rail.result(bulk_get_user3.task_id)['userDetails']['isEnabled'] and
            request_payload.get_conf()['isloginenabled'] == 'Yes',
            yes_task='enable_login',
            no_task='is_user_already_disabled',
        )

        enable_login = rail.RepliconServiceOperator(
            task_id='enable_login',
            endpoint='/services/securityservice1.svc/EnableLogin',
            data={
                "userUri": user_uri
            }
        )

        remove_enddate = rail.RepliconServiceOperator(
            task_id='remove_enddate',
            endpoint='/services/UserService1.svc/UpdateEmploymentDateRange',
            data=lambda: {
                "userUri": request_payload.get_user_uri(),
                "dateRange": {
                    "startDate": request_payload.get_replicon_date(request_payload.get_conf()['startdate']),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        is_user_already_disabled = rail.IfOperator(
            task_id='is_user_already_disabled',
            test=lambda: not rail.result(bulk_get_user3.task_id)['userDetails']['isEnabled'] and
            request_payload.get_conf()['enddate'] and request_payload.get_conf()[
                'isloginenabled'] == 'No',
            yes_task='log_user_already_disabled',
            no_task='has_end_date',
        )

        log_user_already_disabled = rail.WriteLogOperator(
            task_id='log_user_already_disabled',
            log="{{ dag_run.conf.log }}",
            message='User already disabled',
            severity='Success',
            properties={
                'userpartyid': '{{dag_run.conf.employeeid}}',
                'username': '{{dag_run.conf.firstname}} {{dag_run.conf.lastname}}',
                'legalentityid': '{{dag_run.conf.legalentity}}',
                'status': 'Success',
                'message': 'User already disabled',
                'action': 'Update',
            }
        )

        has_end_date = rail.IfOperator(
            task_id='has_end_date',
            test=lambda: bool(request_payload.get_replicon_date(
                request_payload.get_conf()['enddate'])),
            yes_task='update_end_date',
            no_task='is_user_disabled',
        )

        update_end_date = rail.RepliconServiceOperator(
            task_id='update_end_date',
            endpoint='/services/UserService1.svc/UpdateEmploymentDateRange',
            data=lambda: {
                "userUri": request_payload.get_user_uri(),
                "dateRange": {
                    "startDate": request_payload.get_replicon_date(request_payload.get_conf()['startdate'])
                    if request_payload.get_replicon_date(request_payload.get_conf()['startdate'])
                    else rail.result(bulk_get_user3.task_id)['userDetails']['employmentDateRange']['startDate'],
                    "endDate": request_payload.get_replicon_date(request_payload.get_conf()['enddate']),
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        is_user_disabled = rail.IfOperator(
            task_id='is_user_disabled',
            test=lambda: rail.result(bulk_get_user3.task_id)['userDetails']['isEnabled'] and
            request_payload.get_conf()['enddate'] and request_payload.get_conf()[
                'isloginenabled'] == 'No',
            yes_task='disable_login',
            no_task='is_firstname_changed',
        )

        disable_login = rail.RepliconServiceOperator(
            task_id='disable_login',
            endpoint='/services/securityservice1.svc/DisableLogin',
            data={
                "userUri": user_uri
            }
        )

        log_user_disabled = rail.WriteLogOperator(
            task_id='log_user_disabled',
            log="{{ dag_run.conf.log }}",
            message='User disabled',
            severity='Success',
            properties={
                'userpartyid': '{{dag_run.conf.employeeid}}',
                'username': '{{dag_run.conf.firstname}} {{dag_run.conf.lastname}}',
                'legalentityid': '{{dag_run.conf.legalentity}}',
                'status': 'Success',
                'message': 'User disabled',
                'action': 'Update',
            }
        )

        is_firstname_changed = rail.IfOperator(
            task_id='is_firstname_changed',
            test=lambda: request_payload.get_conf()['firstname'] and
            request_payload.get_conf()['firstname'] != rail.result(
                bulk_get_user3.task_id)['userDetails']['firstName'],
            yes_task='update_firstname',
            no_task='is_lastname_changed',
        )

        update_firstname = rail.RepliconServiceOperator(
            task_id='update_firstname',
            endpoint='/services/userService1.svc/UpdateFirstName',
            data={
                "userUri": user_uri,
                "firstname": "{{ dag_run.conf.firstname}}"
            }
        )

        is_lastname_changed = rail.IfOperator(
            task_id='is_lastname_changed',
            test=lambda: request_payload.get_conf()['lastname'] and
            request_payload.get_conf()['lastname'] != rail.result(
                bulk_get_user3.task_id)['userDetails']['lastName'],
            yes_task='update_lastname',
            no_task='is_email_changed',
        )

        update_lastname = rail.RepliconServiceOperator(
            task_id='update_lastname',
            endpoint='/services/userService1.svc/UpdateLastName',
            data={
                "userUri": user_uri,
                "lastname": "{{ dag_run.conf.lastname}}"
            }
        )

        is_email_changed = rail.IfOperator(
            task_id='is_email_changed',
            test=lambda: request_payload.get_conf()['emailaddress'] and
            '@' in request_payload.get_conf()['emailaddress'] and
            request_payload.get_conf()['emailaddress'] != rail.result(
                bulk_get_user3.task_id)['userDetails']['emailAddress'],
            yes_task='update_email',
            no_task='is_empid_changed',
        )

        update_email = rail.RepliconServiceOperator(
            task_id='update_email',
            endpoint='/services/userService1.svc/UpdateEmail',
            data={
                "userUri": user_uri,
                "email": "{{ dag_run.conf.emailaddress}}"
            }
        )

        is_empid_changed = rail.IfOperator(
            task_id='is_empid_changed',
            test=lambda: request_payload.get_conf()['employeeid'] and
            request_payload.get_conf()['employeeid'] != rail.result(
                bulk_get_user3.task_id)['userDetails']['employeeId'],
            yes_task='update_empid',
            no_task='process_custom_field',
        )

        update_empid = rail.RepliconServiceOperator(
            task_id='update_empid',
            endpoint='/services/userService1.svc/UpdateEmployeeId',
            data={
                "userUri": user_uri,
                "employeeId": "{{ dag_run.conf.employeeid}}"
            }
        )

        update_partyid_udf = rail.RepliconServiceOperator(
            task_id='update_partyid_udf',
            endpoint='/services/CustomFieldService1.svc/UpdateTextValue',
            data={
                "objectUri": user_uri,
                "customFieldUri": "{{ dag_run.conf.customfielduri.partyid}}",
                "value": "{{ dag_run.conf.employeeid}}"
            }
        )
        process_custom_field = rail.EmptyOperator(
            task_id='process_custom_field'
        )

        update_custom_field = get_update_custom_field()

        get_effective_user_groupmembership = rail.RepliconServiceOperator(
            task_id='get_effective_user_groupmembership',
            endpoint='/services/UserGroupService1.svc/GetEffectiveUserGroupMembership',
            data={
                "userUri": user_uri,
                "dateRange": null
            }
        )

        is_emptype_changed = rail.IfOperator(
            task_id='is_emptype_changed',
            test=lambda: request_payload.get_conf()['employeetype'] and request_payload.get_conf()['employeetypegroupuri'] and
            request_payload.get_conf()['employeetype'] !=
            request_payload.get_attr_value(rail.result(get_effective_user_groupmembership.task_id),
                                           'employeeTypes.0.employeeType.employeeType.displayText'),
            yes_task='update_emptype',
            no_task='is_companycode_changed',
        )

        update_emptype = rail.RepliconServiceOperator(
            task_id='update_emptype',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data=request_payload.get_emptype_update_param
        )

        is_companycode_changed = rail.IfOperator(
            task_id='is_companycode_changed',
            test=lambda: request_payload.get_conf()['companycode'] and request_payload.get_conf()['companycodegroupuri'] and
                    request_payload.get_conf()['companycodegroupuri'] !=
                    request_payload.get_attr_value(rail.result(get_effective_user_groupmembership.task_id),
                                                   'departments.0.department.department.uri'),
            yes_task='update_companycode',
            no_task='is_businessunit_changed',
        )

        update_companycode = rail.RepliconServiceOperator(
            task_id='update_companycode',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data=request_payload.get_update_company_code_param
        )

        is_businessunit_changed = rail.IfOperator(
            task_id='is_businessunit_changed',
            test=lambda: request_payload.get_conf()['legalentity'] and request_payload.get_conf()['legalentitygroupuri'] and
                    request_payload.get_conf()['legalentitygroupuri'] !=
                    request_payload.get_attr_value(rail.result(get_effective_user_groupmembership.task_id),
                                                   'divisions.0.division.division.uri'),
            yes_task='update_businessunit',
            no_task='is_country_present',
        )

        update_businessunit = rail.RepliconServiceOperator(
            task_id='update_businessunit',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data=request_payload.get_update_businessunit_param
        )

        is_country_present = rail.IfOperator(
            task_id='is_country_present',
            test=lambda: bool(request_payload.get_conf()['country']),
            yes_task='get_location_details',
            no_task='process_timeoff_policy',
        )

        get_location_details = rail.RepliconServiceOperator(
            task_id='get_location_details',
            endpoint='/services/LocationService1.svc/GetLocationDetails',
            data={
                "locationUri": "{{ dag_run.conf.countriesgroupuri }}"
            }
        )

        is_country_changed = rail.IfOperator(
            task_id='is_country_changed',
            test=lambda: request_payload.get_conf()['country'] and request_payload.get_conf()['countriesgroupuri'] and
                    request_payload.get_conf()['countriesgroupuri'] !=
            request_payload.get_attr_value(rail.result(get_effective_user_groupmembership.task_id),
                                           'locations.0.location.location.uri'),
            yes_task='process_country',
            no_task='process_timeoff_policy',
        )

        process_country = rail.EmptyOperator(
            task_id='process_country'
        )

        update_country_task = get_update_country()

        process_timeoff_policy = rail.EmptyOperator(
            task_id='process_timeoff_policy'
        )
        update_timeoff_policy_task = get_update_timeoff_policy()

        can_update_displayname = rail.IfOperator(
            task_id='can_update_displayname',
            test=lambda: len(
                {update_firstname.task_id,
                    update_lastname.task_id,
                    update_email.task_id,
                    'update_loscode_udf',
                    'update_prefix_udf',
                    'update_country'}.
                intersection(set(map(lambda x: x.task_id,
                                     filter(lambda x: x.state == 'success',
                                            rail.get_current_context()['dag_run'].get_task_instances()))))) > 0,
            yes_task='update_displayname',
            no_task='has_timesheetperiod',
        )

        update_displayname = rail.RepliconServiceOperator(
            task_id='update_displayname',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data=request_payload.get_update_displayname_param
        )

        has_timesheetperiod = rail.IfOperator(
            task_id='has_timesheetperiod',
            test=lambda: request_payload.get_conf()
            ['timesheetperiodtype'] and not (request_payload.get_conf()['startdate'] and
                                             request_payload.get_replicon_date(request_payload.get_conf()['startdate']) and
                                             request_payload.get_date_from_replicon_date(
                request_payload.get_replicon_date(request_payload.get_conf()['startdate'])) !=
                request_payload.get_date_from_replicon_date(rail.result(
                    'bulk_get_user3')['userDetails']['employmentDateRange']['startDate'])),
            yes_task='process_timesheet_period',
            no_task='get_assigned_permissionsets',
        )

        process_timesheet_period = rail.EmptyOperator(
            task_id='process_timesheet_period',
        )

        update_timesheet_period_task, _ = get_update_timesheet_period()

        get_assigned_permissionsets = rail.RepliconServiceOperator(
            task_id='get_assigned_permissionsets',
            endpoint='/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2',
            data={
                "userUri": user_uri
            }
        )

        update_supervisor_task, update_supervisor_assignmentschedule_overdaterange = get_update_supervisor(
            user_uri)

        is_timezone_changed = rail.IfOperator(
            task_id='is_timezone_changed',
            test=lambda: request_payload.get_conf()['timezone'] and request_payload.get_conf()['timezoneuri'] and
            request_payload.get_conf()['timezone'] !=
            rail.result(bulk_get_user3.task_id)['timeZone']['ianaName'],
            yes_task='update_timezone',
            no_task='process_timesheet_template'
        )

        update_timezone = rail.RepliconServiceOperator(
            task_id='update_timezone',
            endpoint='/services/InternationalizationService1.svc/UpdateTimeZoneForUser',
            data={
                "userUri": user_uri,
                "timeZoneUri": "{{ dag_run.conf.timezoneuri }}",
            }
        )

        process_timesheet_template = rail.EmptyOperator(
            task_id='process_timesheet_template'
        )

        update_timesheet_template = get_update_timesheet_template()

        is_timesheet_path_changed = rail.IfOperator(
            task_id='is_timesheet_path_changed',
            test=lambda: request_payload.get_conf()['timesheetapprovalpath'] and
            request_payload.get_conf()['timesheetapprovalpathuri'] and
            request_payload.get_conf()['timesheetapprovalpath'] !=
            (rail.result(bulk_get_user3.task_id)[
                'timesheetApprovalPath'] or {}).get('displayText', None),
            yes_task='update_timesheet_path',
            no_task='is_holiday_calendar_changed'
        )

        update_timesheet_path = rail.RepliconServiceOperator(
            task_id='update_timesheet_path',
            endpoint='/services/TimesheetApprovalService1.svc/UpdateApprovalPathForUser',
            data={
                "userUri": user_uri,
                "approvalPathUri": "{{ dag_run.conf.timesheetapprovalpathuri }}",
            }
        )

        is_holiday_calendar_changed = rail.IfOperator(
            task_id='is_holiday_calendar_changed',
            test=lambda: request_payload.get_conf()['holidaycalendar'] and
            request_payload.get_conf()['holidaycalenderuri'] and
            request_payload.get_conf()['holidaycalendar'] !=
            (rail.result(bulk_get_user3.task_id)[
                'holidayCalendar'] or {}).get('displayText', None),
            yes_task='update_holiday_calendar',
            no_task='is_office_schedule_changed'
        )

        update_holiday_calendar = rail.RepliconServiceOperator(
            task_id='update_holiday_calendar',
            endpoint='/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser',
            data={
                "userUri": user_uri,
                "holidayCalendarUri": "{{ dag_run.conf.holidaycalenderuri }}",
            }
        )

        is_office_schedule_changed = rail.IfOperator(
            task_id='is_office_schedule_changed',
            test=lambda: request_payload.get_conf()['scheduletype'] and request_payload.get_conf()['scheduleuri'] and
            request_payload.get_current_schedule(
                rail.result(bulk_get_user3.task_id)['schedulePolicies'])
            and request_payload.get_current_schedule(rail.result(bulk_get_user3.task_id)['schedulePolicies'])
            ['officeSchedule']['displayText'] != request_payload.get_conf()[
                'scheduletype'],
            yes_task='update_office_schedule',
            no_task='map_logs'
        )

        update_office_schedule = rail.RepliconServiceOperator(
            task_id='update_office_schedule',
            endpoint='/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser',
            data=request_payload.get_office_schedule_param
        )

        logs_map = {
            update_start_date.task_id: 'Start Date updated',
            update_timesheet_periodtype.task_id: 'Timesheet period overwritten with new hire date',
            enable_login.task_id: 'User re-enabled',
            update_end_date.task_id: 'End Date updated',
            update_firstname.task_id: 'First name updated',
            update_lastname.task_id: 'Last name updated',
            update_email.task_id: 'Email updated',
            update_partyid_udf.task_id: 'Party ID updated',
            'update_workdayid_udf': 'Workday ID(UDF) updated',
            'update_profilestatus_udf': 'Profile Status updated',
            'update_resourcerole_udf': 'Resource role updated',
            'update_homeofficelocation_udf': 'Home office location updated',
            'update_prefix_udf': 'Prefix updated',
            'update_grade_udf': 'Grade updated',
            'update_loscode_udf': 'Los Code updated',
            update_emptype.task_id: 'Employee type updated',
            update_companycode.task_id: 'Agencies (Department group) updated',
            'update_country': 'Country updated',
            'assign_timeoff_policy': 'Time off template updated',
            'remove_timeoff_policy': 'Time off template removed',
            update_displayname.task_id: 'Display name updated',
            'put_timesheetperiodschedule_for_user': 'Timesheet period updated',
            update_supervisor_assignmentschedule_overdaterange.task_id: 'Supervisor updated',
            update_timezone.task_id: 'Time zone updated',
            'update_timesheet_template': 'Timesheet template updated',
            update_timesheet_path.task_id: 'Timesheet approval path updated',
            update_holiday_calendar.task_id: 'Holiday calendar updated',
            update_office_schedule.task_id: 'Office schedule updated',
            update_login_name.task_id: 'Login name updated',
        }

        def do_map_logs():
            logs = []
            if len(request_payload.get_conf().get('validationlog', [])) > 0:
                logs.extend(list(
                    map(lambda item: item['message'], request_payload.get_conf()['validationlog'])))
            if rail.result('get_timesheetperiodtype_uri', 'log'):
                logs.append(rail.result('get_timesheetperiodtype_uri', 'log'))
            if rail.result(get_location_details.task_id) and not rail.result(get_location_details.task_id).get('code'):
                logs.append('Display name defaulted without territory code')
            if rail.result('get_timesheetperiodtype_uri', 'log'):
                logs.append(rail.result('get_timesheetperiodtype_uri', 'log'))
            success_tasks = list(map(lambda x: x.task_id, filter(lambda x: x.state == 'success',
                                                                 rail.get_current_context()['dag_run'].get_task_instances())))
            for key in [*logs_map]:
                if key in success_tasks:
                    logs.append(logs_map[key])
            return logs

        map_logs = rail.PythonOperator(
            task_id='map_logs',
            python_callable=do_map_logs
        )

        write_update_logs = rail.WriteLogOperator(
            task_id='write_update_logs',
            log="{{ dag_run.conf.log }}",
            message='''{%- if dag_run.conf.validationlog | length > 0 -%}
                    User partially updated {{ result("map_logs") | to_json }}
                {%- elif result("map_logs") -%}
                    Successfully updated {{ result("map_logs") | to_json }}
                {%- else -%}
                    No change to the user record in Replicon
                {%- endif -%}''',
            severity='Success',
            properties={
                'userpartyid': '{{dag_run.conf.employeeid}}',
                'username': '{{dag_run.conf.firstname}} {{dag_run.conf.lastname}}',
                'legalentityid': '{{dag_run.conf.legalentity}}',
                'message': '''{%- if dag_run.conf.validationlog | length > 0 -%}
                    User partially updated {{ result("map_logs") | to_json }}
                {%- elif result("map_logs") -%}
                    Successfully updated {{ result("map_logs") | to_json }}
                {%- else -%}
                    No change to the user record in Replicon
                {%- endif -%}''',
                'status': 'Success',
                'action': 'Update',
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ dag_run.conf.log }}",
            trigger_rule='one_failed',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'userpartyid': '{{dag_run.conf.employeeid}}',
                'username': '{{dag_run.conf.firstname}} {{dag_run.conf.lastname}}',
                'legalentityid': '{{dag_run.conf.legalentity}}',
                'status': 'Error',
                'action': 'Update',
                'message': '{{ get_error_message() }}',

            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )
        batch_task >> can_update_login_name
        batch_task >> catch_and_log_errors
        can_update_login_name >> rail.Label(
            'Yes') >> update_login_name >> bulk_get_user3
        can_update_login_name >> rail.Label('No') >> bulk_get_user3

        bulk_get_user3 >> get_assigned_policysets_for_user >> is_start_date_changed

        is_start_date_changed >> rail.Label('Yes') >> update_start_date >> update_timesheet_periodtype \
            >> is_rehire
        is_start_date_changed >> rail.Label('No') >> is_rehire

        is_rehire >> rail.Label('Yes') >> enable_login >> remove_enddate \
            >> is_user_already_disabled
        is_rehire >> rail.Label(
            'No') >> is_user_already_disabled >> catch_and_log_errors

        is_user_already_disabled >> rail.Label(
            'Yes') >> log_user_already_disabled
        is_user_already_disabled >> rail.Label('No') >> has_end_date

        has_end_date >> rail.Label(
            'Yes') >> update_end_date >> is_user_disabled
        has_end_date >> rail.Label(
            'No') >> is_user_disabled >> catch_and_log_errors

        is_user_disabled >> rail.Label(
            'Yes') >> disable_login >> log_user_disabled
        is_user_disabled >> rail.Label('No') >> is_firstname_changed

        is_firstname_changed >> rail.Label('Yes') >> update_firstname >> \
            is_lastname_changed
        is_firstname_changed >> rail.Label('No') >> is_lastname_changed
        is_lastname_changed >> rail.Label('Yes') >> update_lastname >> \
            is_email_changed
        is_lastname_changed >> rail.Label('No') >> is_email_changed
        is_email_changed >> rail.Label('Yes') >> update_email >> \
            is_empid_changed
        is_email_changed >> rail.Label('No') >> is_empid_changed
        is_empid_changed >> rail.Label(
            'Yes') >> update_empid >> update_partyid_udf >> process_custom_field
        is_empid_changed >> rail.Label('No') >> process_custom_field
        process_custom_field >> update_custom_field >> get_effective_user_groupmembership >> is_emptype_changed

        is_emptype_changed >> rail.Label(
            'Yes') >> update_emptype >> is_companycode_changed
        is_emptype_changed >> rail.Label('No') >> is_companycode_changed

        is_companycode_changed >> rail.Label(
            'Yes') >> update_companycode >> is_businessunit_changed
        is_companycode_changed >> rail.Label('No') >> is_businessunit_changed

        is_businessunit_changed >> rail.Label(
            'Yes') >> update_businessunit >> is_country_present >> rail.Label('Yes') >> get_location_details >> is_country_changed

        is_country_present >> rail.Label('No') >> process_timeoff_policy
        is_businessunit_changed >> rail.Label('No') >> is_country_present

        is_country_changed >> rail.Label(
            'Yes') >> process_country >> update_country_task >> process_timeoff_policy
        is_country_changed >> rail.Label('No') >> process_timeoff_policy

        process_timeoff_policy >> update_timeoff_policy_task >> can_update_displayname

        can_update_displayname >> rail.Label(
            'Yes') >> update_displayname >> has_timesheetperiod
        can_update_displayname >> rail.Label('No') >> has_timesheetperiod

        has_timesheetperiod >> rail.Label(
            'Yes') >> process_timesheet_period >> update_timesheet_period_task >> get_assigned_permissionsets
        has_timesheetperiod >> rail.Label('No') >> get_assigned_permissionsets

        get_assigned_permissionsets >> update_supervisor_task >> is_timezone_changed
        is_timezone_changed >> rail.Label(
            'yes') >> update_timezone >> process_timesheet_template
        is_timezone_changed >> rail.Label('no') >> process_timesheet_template
        process_timesheet_template >> update_timesheet_template >> is_timesheet_path_changed

        is_timesheet_path_changed >> rail.Label(
            'Yes') >> update_timesheet_path >> is_holiday_calendar_changed
        is_timesheet_path_changed >> rail.Label(
            'No') >> is_holiday_calendar_changed

        is_holiday_calendar_changed >> rail.Label(
            'Yes') >> update_holiday_calendar >> is_office_schedule_changed
        is_holiday_calendar_changed >> rail.Label(
            'No') >> is_office_schedule_changed

        is_office_schedule_changed >> rail.Label(
            'Yes') >> update_office_schedule >> map_logs >> write_update_logs
        is_office_schedule_changed >> rail.Label(
            'No') >> map_logs >> write_update_logs
        write_update_logs >> catch_and_log_errors
        catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
