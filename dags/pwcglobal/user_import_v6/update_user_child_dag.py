from datetime import date, datetime, timedelta
from pendulum import now
import rail
from pwcglobal.user_import_v6.utils import request_payload, custom_method
from pwcglobal.user_import_v6.task.update_country import get_update_country
from pwcglobal.user_import_v6.task.update_timesheet_period import get_update_timesheet_period
from pwcglobal.user_import_v6.task.update_supervisor import get_update_supervisor
from pwcglobal.user_import_v6.task.update_timesheet_template import get_update_timesheet_template
from pwcglobal.user_import_v6.task.update_custom_field import get_update_custom_field
from pwcglobal.user_import_v6.task.update_timeoff_policy import get_update_timeoff_policy
from pwcglobal.user_import_v6.task.update_ftepercent_blob import update_ftepercent_blob
from pwcglobal.user_import_v6.task.assign_default_toil_to_policy import add_toil_default_policy


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.user_update_dag_id,
        description=f'PwCGlobal_User_Import_Child_User Update',
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
            retries=1,
            retry_delay=timedelta(minutes=5)
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

        get_assigned_permissionsets = rail.RepliconServiceOperator(
            task_id='get_assigned_permissionsets',
            endpoint='/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2',
            data={
                "userUri": user_uri
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
            test=lambda: config.company_key.lower() == 'pwc' and
            request_payload.get_conf()['emailaddress'] and
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
            no_task='is_sup_org_changed',
        )

        update_businessunit = rail.RepliconServiceOperator(
            task_id='update_businessunit',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data=request_payload.get_update_businessunit_param
        )

        is_sup_org_changed = rail.IfOperator(
            task_id='is_sup_org_changed',
            test=lambda: request_payload.get_conf()['supervisoryorgname'] and request_payload.get_conf()['supervisory_org_uri'] and
                    request_payload.get_conf()['supervisory_org_uri'] !=
                    request_payload.get_attr_value(rail.result(get_effective_user_groupmembership.task_id),
                                                   'costCenters.0.costCenter.costCenter.uri'),
            yes_task='update_sup_org',
            no_task='is_workcompliancepolicyassignment_changed',
        )

        update_sup_org = rail.RepliconServiceOperator(
            task_id='update_sup_org',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data=request_payload.get_update_supervisory_org_param
        )

        is_workcompliancepolicyassignment_changed = rail.IfOperator(
            task_id='is_workcompliancepolicyassignment_changed',
            test=lambda: request_payload.get_conf()['work_compliance_policy'] and request_payload.get_conf(
            )['work_compliance_policy'] != request_payload.get_current_workcomplianceassignmentpolicy(rail.result(
                bulk_get_user3.task_id)['workCompliancePolicyAssignmentSchedule']),
            yes_task='update_workcomplaincepolicy',
            no_task='is_country_present',
        )

        update_workcomplaincepolicy = rail.RepliconServiceOperator(
            task_id='update_workcomplaincepolicy',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data=request_payload.get_update_workcomplaincepolicy_param
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
        update_timeoff_policy_task = get_update_timeoff_policy(config)

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
            no_task='time_sheet_period_end',
        )

        process_timesheet_period = rail.EmptyOperator(
            task_id='process_timesheet_period',
        )

        update_timesheet_period_task, _ = get_update_timesheet_period()

        time_sheet_period_end = rail.EmptyOperator(
            task_id="time_sheet_period_end")

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
            no_task='has_toil_time_off_type_changed'
        )

        update_office_schedule = rail.RepliconServiceOperator(
            task_id='update_office_schedule',
            endpoint='/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser',
            data=request_payload.get_office_schedule_param
        )

        has_toil_time_off_type_changed = rail.IfOperator(
            task_id="has_toil_time_off_type_changed",
            test=lambda: bool(request_payload.get_conf()["toil"] == "Y" and request_payload.get_conf()['toil'] !=
                              rail.find_first_by_attr_and_get_attr(
                    rail.result('bulk_get_user3')[
                        'userDetails']['customFieldValues'],
                    'customField.displayText',
                    'TOIL',
                    'text') and request_payload.get_conf()['toiltimeofftypeuri']),
            yes_task="get_all_timeofftypes",
            no_task="if_time_entry_approval_path"
        )

        get_all_timeofftypes = rail.RepliconServiceOperator(
            task_id='get_all_timeofftypes',
            endpoint='/services/TimeOffService1.svc/GetAllTimeOffTypes',
        )

        get_user_location_uri = rail.RepliconServiceOperator(
            task_id="get_user_location_uri",
            endpoint="/services/LocationService1.svc/GetLocationScheduleForUser",
            data={
                "userUri": user_uri
            },
            data_handler=lambda response: list(
                map(lambda i: i["location"]["displayText"], response))[-1] if response else null
        )

        get_all_time_off_types_for_user = rail.RepliconServiceOperator(
            task_id="get_all_time_off_types_for_user",
            endpoint="/services/TimeOffService1.svc/GetTimeOffTypeAssignmentsForUser",
            data={
                    "userUri": user_uri
            },
            data_handler=lambda response: list(
                map(lambda i: i["uri"], response)) if response else null
        )

        update_timeofftypes = rail.RepliconServiceOperator(
            task_id="update_timeofftypes",
            endpoint='/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser',
            data=lambda: request_payload.get_update_time_off_req(config)
        )

        process_toil_default = add_toil_default_policy(user_uri)

        if_time_entry_approval_path = rail.IfOperator(
            task_id="if_time_entry_approval_path",
            test=lambda: bool(request_payload.get_conf()
                              ["timeentryapprovalpath"] and request_payload.get_conf()["timeentryapprovalpathuri"]),
            yes_task="get_time_entry_path_for_user",
            no_task="put_system_approval_time_entry_path"
        )

        put_system_approval_time_entry_path = rail.RepliconServiceOperator(
            task_id="put_system_approval_time_entry_path",
            endpoint="/services/TimeEntryRevisionGroupApprovalService1.svc/UpdateApprovalPathForUser",
            data=lambda: {
                    "userUri": request_payload.get_conf()['useruri'],
                    "approvalPathUri": request_payload.get_conf()['systemapprovalpathuri']
            }
        )

        get_time_entry_path_for_user = rail.RepliconServiceOperator(
            task_id="get_time_entry_path_for_user",
            endpoint="/services/TimeEntryRevisionGroupApprovalService1.svc/GetApprovalPathForUser",
            data={
                "userUri": user_uri
            },
        )

        has_time_entry_changed = rail.IfOperator(
            task_id="has_time_entry_changed",
            test=lambda: bool(request_payload.get_conf()["timeentryapprovalpathuri"] and
                              rail.result("get_time_entry_path_for_user")[
                    "uri"] != request_payload.get_conf()["timeentryapprovalpathuri"]
            ),
            yes_task="put_time_entry_approval_path",
            no_task="is_ftepercent_blob_changed"
        )

        put_time_entry_approval_path = rail.RepliconServiceOperator(
            task_id="put_time_entry_approval_path",
            endpoint="/services/TimeEntryRevisionGroupApprovalService1.svc/UpdateApprovalPathForUser",
            data=lambda: {
                    "userUri": request_payload.get_conf()['useruri'],
                    "approvalPathUri": request_payload.get_conf()['timeentryapprovalpathuri']
            }
        )

        is_ftepercent_blob_changed = rail.IfOperator(
            task_id="is_ftepercent_blob_changed",
            test=lambda: request_payload.get_conf()['ftepercent'] and request_payload.get_conf()["customfielduri"]['ftepercenturi'] and
                    str(request_payload.get_conf()['ftepercent']) != rail.find_first_by_attr_and_get_attr(
                        rail.result('bulk_get_user3')['userDetails']['customFieldValues'], 'customField.displayText', 'FTE Percent', 'text'),
            yes_task="update_fte_blob_start",
            no_task="has_payrule"
        )

        update_fte_blob_start = rail.EmptyOperator(
            task_id="update_fte_blob_start")
        update_fte_blob = update_ftepercent_blob(config)

        has_payrule = rail.IfOperator(
            task_id="has_payrule",
            test=lambda: bool(request_payload.get_conf()[
                              "payrule"] and request_payload.get_conf()["payruleuri"]),
            yes_task="if_payrules_exists_for_user",
            no_task="update_zerotime_start"
        )

        if_payrules_exists_for_user = rail.IfOperator(
            task_id="if_payrules_exists_for_user",
            test=lambda: bool(rail.result("bulk_get_user3")
                              ["payRuleScriptSchedule"]),
            yes_task="if_new_payrules_for_user",
            no_task="assign_initial_payrule_for_user"
        )

        assign_initial_payrule_for_user = rail.RepliconServiceOperator(
            task_id="assign_initial_payrule_for_user",
            endpoint="services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data=lambda dag_run: {
                    "userUri": dag_run.conf["useruri"],
                    "scheduleEntries": [
                        {
                            "effectiveDate": null,
                            "payRuleScript": {
                                "uri": request_payload.get_conf()["payruleuri"]
                            }
                        }
                    ]
            }
        )

        if_new_payrules_for_user = rail.IfOperator(
            task_id="if_new_payrules_for_user",
            test=lambda dag_run: not custom_method.is_current_payrule_matching(
                rail.result("bulk_get_user3")["payRuleScriptSchedule"],
                dag_run.conf["payruleuri"]
            ),
            yes_task="if_product_license_present_payrule",
            no_task="update_zerotime_start"
        )

        if_product_license_present_payrule = rail.IfOperator(
            task_id="if_product_license_present_payrule",
            test=lambda: bool(
                len(rail.result("bulk_get_user3")["assignedProducts"]) > 0),
            yes_task="if_timesheet_template_assigned_payrule",
            no_task="update_validation_log_for_product"
        )

        update_validation_log_for_product = rail.PythonOperator(
            task_id="update_validation_log_for_product",
            python_callable=lambda: request_payload.get_conf().get('validationlog', []).append(
                {"message": "Product license not assigned for user hence payrule not updated"})
        )

        if_timesheet_template_assigned_payrule = rail.IfOperator(
            task_id="if_timesheet_template_assigned_payrule",
            test=lambda: bool(rail.result("bulk_get_user3")
                              ["timesheetTemplate"]),
            yes_task="get_current_timesheet_period_payrule",
            no_task="update_validation_log_for_ts"
        )

        update_validation_log_for_ts = rail.PythonOperator(
            task_id="update_validation_log_for_ts",
            python_callable=lambda: request_payload.get_conf().get('validationlog', []).append(
                {"message": "Time sheet not assigned for user hence payrule not updated"})
        )

        get_current_timesheet_period_payrule = rail.RepliconServiceOperator(
            task_id="get_current_timesheet_period_payrule",
            endpoint="/services/TimesheetService1.svc/GetTimesheetForDate",
            data=lambda dag_run: {
                    "userUri": dag_run.conf["useruri"],
                    "date": rail.parse_date(datetime.strftime(now(tz="Europe/London"), "%m/%d/%Y"), "%m/%d/%Y"),
                    "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
            },
            data_handler=lambda response: rail.parse_date(
                response["displayText"].split("/")[1], "%Y-%m-%d") if response else null
        )

        assign_payrules_for_user = rail.RepliconServiceOperator(
            task_id="assign_payrules_for_user",
            endpoint="services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data=request_payload.get_update_payrule_request
        )

        update_zerotime_start = rail.EmptyOperator(
            task_id="update_zerotime_start")

        if_user_belongs_to_zt_country = rail.IfOperator(
            task_id="if_user_belongs_to_zt_country",
            test=custom_method.check_update_zerotime,
            yes_task="assign_zerotime_license_and_permission",
            no_task="if_non_zt_country_update"
        )

        assign_zerotime_license_and_permission = rail.RepliconServiceOperator(
            task_id='assign_zerotime_license_and_permission',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data=lambda: {
                    "user": {
                        "uri": request_payload.get_user_uri()
                    },
                "modifications": {
                        "permissionSetsToApply": {
                            "permissionSetUrisToAssign": custom_method.get_permission_set("add"),
                        },
                        "productAssignmentsToApply": {
                            "productUrisToAssign": [
                                "urn:replicon-saas:product:time-intelligence"],
                        },
                    },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_non_zt_country_update = rail.IfOperator(
            task_id="if_non_zt_country_update",
            test=custom_method.check_non_zt_country,
            yes_task="unassign_zerotime_license",
            no_task="end_zerotime"
        )

        unassign_zerotime_license = rail.RepliconServiceOperator(
            task_id='unassign_zerotime_license',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data=lambda: {
                    "user": {
                        "uri": request_payload.get_user_uri()
                    },
                "modifications": {
                        "productAssignmentsToApply": {
                            "productUrisToUnassign": [
                                "urn:replicon-saas:product:time-intelligence"
                            ]
                        },
                    },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        has_any_zerotime_permission_to_remove = rail.IfOperator(
            task_id = "has_any_zerotime_permission_to_remove",
            test=lambda: custom_method.get_permission_set("remove"),
            yes_task="unassign_zerotime_permission",
            no_task="end_zerotime"
        )

        unassign_zerotime_permission = rail.RepliconServiceOperator(
            task_id='unassign_zerotime_permission',
            endpoint='/services/PermissionSetService1.svc/RemovePermissionSetAssignmentFromUser',
            data=lambda: {
                "userUri": request_payload.get_user_uri(),
                "permissionSetUri": custom_method.get_permission_set("remove")
            }
        )

        end_zerotime = rail.EmptyOperator(task_id="end_zerotime")

        map_logs = rail.PythonOperator(
            task_id='map_logs',
            python_callable=custom_method.do_map_logs
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

        batch_task >> can_update_login_name
        batch_task >> catch_and_log_errors
        can_update_login_name >> rail.Label(
            'Yes') >> update_login_name >> bulk_get_user3
        can_update_login_name >> rail.Label('No') >> bulk_get_user3

        bulk_get_user3 >> get_assigned_policysets_for_user >> get_assigned_permissionsets >>\
            is_start_date_changed

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
            'Yes') >> update_businessunit >> is_sup_org_changed
        is_businessunit_changed >> rail.Label('No') >> is_sup_org_changed

        is_sup_org_changed >> rail.Label(
            'Yes') >> update_sup_org >> is_workcompliancepolicyassignment_changed
        is_sup_org_changed >> rail.Label(
            'No') >> is_workcompliancepolicyassignment_changed

        is_workcompliancepolicyassignment_changed >> rail.Label(
            'Yes') >> update_workcomplaincepolicy >> is_country_present
        is_workcompliancepolicyassignment_changed >> rail.Label(
            'No') >> is_country_present
        is_country_present >> rail.Label("No") >> process_timeoff_policy
        is_country_present >> rail.Label("Yes") >> get_location_details >>\
        is_country_changed >> rail.Label(
            'Yes') >> process_country >> update_country_task >> process_timeoff_policy
        is_country_changed >> rail.Label('No') >> process_timeoff_policy

        process_timeoff_policy >> update_timeoff_policy_task >> can_update_displayname

        can_update_displayname >> rail.Label(
            'Yes') >> update_displayname >> has_timesheetperiod
        can_update_displayname >> rail.Label('No') >> has_timesheetperiod

        has_timesheetperiod >> rail.Label(
            'Yes') >> process_timesheet_period >> update_timesheet_period_task >> time_sheet_period_end
        has_timesheetperiod >> rail.Label('No') >> time_sheet_period_end >>\
            update_supervisor_task >> is_timezone_changed
        is_timezone_changed >> rail.Label(
            'yes') >> update_timezone >> process_timesheet_template
        is_timezone_changed >> rail.Label('no') >> process_timesheet_template
        process_timesheet_template >> update_timesheet_template >>\
            is_timesheet_path_changed >> rail.Label(
                'Yes') >> update_timesheet_path >> is_holiday_calendar_changed
        is_timesheet_path_changed >> rail.Label(
            'No') >>\
            is_holiday_calendar_changed >> rail.Label(
            'Yes') >> update_holiday_calendar >>\
            is_office_schedule_changed >> rail.Label(
            'No') >> has_toil_time_off_type_changed
        is_office_schedule_changed >> rail.Label(
            'Yes') >> update_office_schedule >>\
            has_toil_time_off_type_changed >> rail.Label("Yes") >>\
            get_all_timeofftypes >> get_user_location_uri >>\
            get_all_time_off_types_for_user >> update_timeofftypes >> process_toil_default >> if_time_entry_approval_path
        has_toil_time_off_type_changed >> rail.Label("No") >>\
            if_time_entry_approval_path >> rail.Label("Yes") >> get_time_entry_path_for_user >>\
            has_time_entry_changed >> rail.Label(
                "Yes") >> put_time_entry_approval_path >> is_ftepercent_blob_changed
        has_time_entry_changed >> rail.Label(
            "No") >> is_ftepercent_blob_changed
        if_time_entry_approval_path >> rail.Label("No") >>\
            put_system_approval_time_entry_path >> is_ftepercent_blob_changed >>\
            rail.Label("Yes") >> update_fte_blob_start >> \
            update_fte_blob >> has_payrule
        is_ftepercent_blob_changed >>\
            rail.Label("No") >> has_payrule
        is_holiday_calendar_changed >> rail.Label(
            'No') >> is_office_schedule_changed
        has_payrule >>\
            rail.Label("Yes") >> if_payrules_exists_for_user >> rail.Label("Yes") >> if_new_payrules_for_user >> rail.Label("Yes") >>\
            if_product_license_present_payrule >> rail.Label("Yes") >>\
            if_timesheet_template_assigned_payrule >> rail.Label("Yes") >>\
            get_current_timesheet_period_payrule >>\
            assign_payrules_for_user >> update_zerotime_start
        if_timesheet_template_assigned_payrule >> rail.Label(
            "No") >> update_validation_log_for_ts >> update_zerotime_start
        if_product_license_present_payrule >> rail.Label(
            "No") >> update_validation_log_for_product >> update_zerotime_start
        if_payrules_exists_for_user >> rail.Label(
            "No") >> assign_initial_payrule_for_user >> update_zerotime_start
        if_new_payrules_for_user >> rail.Label(
            "No") >> update_zerotime_start
        has_payrule >> rail.Label("No") >>\
            update_zerotime_start >>\
            if_user_belongs_to_zt_country >> rail.Label("Yes") >>\
            assign_zerotime_license_and_permission >> end_zerotime
        if_user_belongs_to_zt_country >> rail.Label("No") >>\
            if_non_zt_country_update >> rail.Label("Yes") >>\
            unassign_zerotime_license >> has_any_zerotime_permission_to_remove
        has_any_zerotime_permission_to_remove >> rail.Label("Yes") >> unassign_zerotime_permission >> end_zerotime
        has_any_zerotime_permission_to_remove >> rail.Label("No") >> end_zerotime
        if_non_zt_country_update >> rail.Label("No") >> end_zerotime >>\
            map_logs >> write_update_logs >>\
            catch_and_log_errors

    return dag


rail.for_each_instance(create_dag)
