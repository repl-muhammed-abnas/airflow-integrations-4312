
from datetime import timedelta, datetime
import json
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'baylorcollegeofmedicine_user_update_child_{config.instance}',
        description=f'BaylorCollegeOfMedicine User Update {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_user,
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
            no_task='declare_list_2'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='declare_list_2',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        declare_list_2 = rail.SetVariableOperator(
            task_id='declare_list_2',
            append=False,
            name='logs',
            value=[]
        )

        declare_list_3 = rail.SetVariableOperator(
            task_id='declare_list_3',
            append=False,
            name='Exception',
            value=[]
        )

        bulk_get_users3_5 = rail.RepliconServiceOperator(
            task_id='bulk_get_users3_5',
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

        if_firstno_downcase_equals_to_yes_6 = rail.IfOperator(
            task_id='if_firstno_downcase_equals_to_yes_6',
            test=lambda: (rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_5')[0][
                          'userDetails']['customFieldValues'], 'customField.displayText', 'Admin Use Only', 'text', 'No')).lower() == 'yes',
            yes_task="log_user_only_for_admin_use",
            no_task="if_securityconfiguration_isloginenabled_is_not_true_rehire_9",
        )

        log_user_only_for_admin_use = rail.WriteLogOperator(
            task_id='log_user_only_for_admin_use',
            log="{{ dag_run.conf.userimportlogslookup }}",
            message="na",
            severity="Skipped",
            properties={
                "loginname": "{{dag_run.conf.loginname}}",
                "action": "Update",
                "status": "Skipped",
                "details": '"Admin Use Only" custom field is set to "Yes"',
                "jobid": "{{dag_run.conf.callerjobid}}",
                "childjobid": "{{ dag_run_ecid() }}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "lastname": "{{ dag_run.conf.lastname }}"
            }
        )

        if_securityconfiguration_isloginenabled_is_not_true_rehire_9 = rail.IfOperator(
            task_id='if_securityconfiguration_isloginenabled_is_not_true_rehire_9',
            test=lambda: not((rail.result('bulk_get_users3_5')[
                0]['securityConfiguration']['isLoginEnabled'])),
            yes_task="enable_login_10",
            no_task="if_request_firstname_present_12",
        )

        enable_login_10 = rail.RepliconServiceOperator(
            task_id='enable_login_10',
            endpoint="/services/SecurityService1.svc/EnableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        put_policy_set_assignments_for_user_assigntimesheettemplateandpunchentrypolicy_11 = rail.RepliconServiceOperator(
            task_id='put_policy_set_assignments_for_user_assigntimesheettemplateandpunchentrypolicy_11',
            endpoint="/services/PolicySetService1.svc/PutPolicySetAssignmentsForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "policySetUris": [
                    "{{ dag_run.conf.timesheettemplateuri }}",
                    "{{ dag_run.conf.punchentrypolicyuri }}"
                ]
            }
        )

        if_request_firstname_present_12 = rail.IfOperator(
            task_id='if_request_firstname_present_12',
            test=lambda dag_run: dag_run.conf['firstname'] and ((dag_run.conf['firstname']).lower() != ((rail.result('bulk_get_users3_5')[
                0]['userDetails']['firstName']).lower() if rail.result('bulk_get_users3_5')[0]['userDetails']['firstName'] else '')),
            yes_task="update_first_name_13",
            no_task="if_request_lastname_present_14",
        )

        update_first_name_13 = rail.RepliconServiceOperator(
            task_id='update_first_name_13',
            endpoint="/services/userService1.svc/UpdateFirstName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "firstname": "{{ dag_run.conf.firstname }}"
            }
        )

        if_request_lastname_present_14 = rail.IfOperator(
            task_id='if_request_lastname_present_14',
            test=lambda dag_run: dag_run.conf['lastname'] and ((dag_run.conf['lastname']).lower() != ((rail.result('bulk_get_users3_5')[
                0]['userDetails']['lastName']).lower() if rail.result('bulk_get_users3_5')[0]['userDetails']['lastName'] else '')),
            yes_task="update_last_name_15",
            no_task="if_request_emailaddress_present_16",
        )

        update_last_name_15 = rail.RepliconServiceOperator(
            task_id='update_last_name_15',
            endpoint="/services/userService1.svc/UpdateLastName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "lastname": "{{ dag_run.conf.lastname }}"
            }
        )

        if_request_emailaddress_present_16 = rail.IfOperator(
            task_id='if_request_emailaddress_present_16',
            test=lambda dag_run: dag_run.conf['emailaddress'] and ((dag_run.conf['emailaddress']).lower() != ((rail.result('bulk_get_users3_5')[
                0]['userDetails']['emailAddress']).lower() if rail.result('bulk_get_users3_5')[0]['userDetails']['emailAddress'] else '')),
            yes_task="update_email_17",
            no_task="if_request_employeeid_present_18",
        )

        update_email_17 = rail.RepliconServiceOperator(
            task_id='update_email_17',
            endpoint="/services/userService1.svc/UpdateEmail",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "email": "{{ dag_run.conf.emailaddress }}"
            }
        )

        if_request_employeeid_present_18 = rail.IfOperator(
            task_id='if_request_employeeid_present_18',
            test=lambda dag_run: dag_run.conf['employeeid'] and ((dag_run.conf['employeeid']) != ((rail.result('bulk_get_users3_5')[
                0]['userDetails']['employeeId']) if rail.result('bulk_get_users3_5')[0]['userDetails']['employeeId'] else '')),
            yes_task="update_employee_id_19",
            no_task="if_startdate_day_blank_20",
        )

        update_employee_id_19 = rail.RepliconServiceOperator(
            task_id='update_employee_id_19',
            endpoint="/services/UserService1.svc/UpdateEmployeeId",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "employeeId": "{{ dag_run.conf.employeeid }}"
            }
        )

        if_startdate_day_blank_20 = rail.IfOperator(
            task_id='if_startdate_day_blank_20',
            test=lambda dag_run: not (rail.result('bulk_get_users3_5')[0]['userDetails']['employmentDateRange']['startDate'] and rail.result(
                'bulk_get_users3_5')[0]['userDetails']['employmentDateRange']['startDate']['day']) or (datetime.strptime((str(rail.result('bulk_get_users3_5')[
                0]['userDetails']['employmentDateRange']['startDate']['year']) + '-' + str(rail.result(
                'bulk_get_users3_5')[0]['userDetails']['employmentDateRange']['startDate']['month']) + '-' + str(rail.result(
                'bulk_get_users3_5')[0]['userDetails']['employmentDateRange']['startDate']['day'])),'%Y-%m-%d') != datetime.strptime(
                dag_run.conf['userstartdate'],"%Y-%m-%d")),
            yes_task="update_employment_date_range_21",
            no_task="if_request_supervisor_present_22",
        )

        update_employment_date_range_21 = rail.RepliconServiceOperator(
            task_id='update_employment_date_range_21',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ dag_run.conf.startdate.year }}",
                        "month": "{{ dag_run.conf.startdate.month }}",
                        "day": "{{ dag_run.conf.startdate.day }}"
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_request_supervisor_present_22 = rail.IfOperator(
            task_id='if_request_supervisor_present_22',
            test='''{{ dag_run.conf.supervisor | is_truthy }}''',
            yes_task="if_request_loginname_not_equals_to_dataworkato_servicereceive_requestrequestsupervisor_23",
            no_task="get_effective_user_group_membership_45",
        )

        if_request_loginname_not_equals_to_dataworkato_servicereceive_requestrequestsupervisor_23 = rail.IfOperator(
            task_id='if_request_loginname_not_equals_to_dataworkato_servicereceive_requestrequestsupervisor_23',
            test='''{{ dag_run.conf.loginname != dag_run.conf.supervisor }}''',
            yes_task="search_users_searchsupervisorbyloginname_24",
            no_task="if_request_loginname_equals_to_dataworkato_servicereceive_requestrequestsupervisor_43",
        )

        def get_supervisor_uri_and_status(response, dag_run):
            users_found = response['rows']
            supervisor = {}
            for user in users_found:
                if user['cells'][0]['textValue'] == dag_run.conf['supervisor']:
                    supervisor = user
                    break
            return {
                'uri': supervisor['cells'][0]['uri'] if supervisor else '',
                'status': supervisor['cells'][1]['textValue'] if supervisor else ''
            }

        search_users_searchsupervisorbyloginname_24 = rail.RepliconServiceOperator(
            task_id='search_users_searchsupervisorbyloginname_24',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000",
                "columnUris": [
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:user-list-filter:login-name"
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
                            "text": "{{ dag_run.conf.supervisor }}",
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
            data_handler=get_supervisor_uri_and_status
        )

        if_search_users_searchsupervisorbyloginname_24_users_less_than_1_25 = rail.IfOperator(
            task_id='if_search_users_searchsupervisorbyloginname_24_users_less_than_1_25',
            test=lambda: not bool(rail.result(
                'search_users_searchsupervisorbyloginname_24')['uri']),
            yes_task="add_to_supervisor_assignment_queue",
            no_task="if_search_users_searchsupervisorbyloginname_24_users_greater_than_0_27",
        )

        add_to_supervisor_assignment_queue = rail.WriteLogOperator(
            task_id='add_to_supervisor_assignment_queue',
            log="{{ dag_run.conf.supervisorlookup }}",
            message="na",
            severity="Queued",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "username": "{{ dag_run.conf.loginname }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "supervisorloginname": "{{ dag_run.conf.supervisor }}",
                "action": "Update",
                "childjobid": "{{ dag_run_ecid() }}",
                "status": "Queued"
            }
        )

        if_search_users_searchsupervisorbyloginname_24_users_greater_than_0_27 = rail.IfOperator(
            task_id='if_search_users_searchsupervisorbyloginname_24_users_greater_than_0_27',
            test=lambda: bool(rail.result(
                'search_users_searchsupervisorbyloginname_24')['uri']),
            yes_task="get_supervisor_assignment_details_28",
            no_task="if_request_loginname_equals_to_dataworkato_servicereceive_requestrequestsupervisor_43",
        )

        get_supervisor_assignment_details_28 = rail.RepliconServiceOperator(
            task_id='get_supervisor_assignment_details_28',
            endpoint="/services/UserService1.svc/GetSupervisorAssignmentDetails",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "asOfDate": null
            }
        )

        if_response_d_present_29 = rail.IfOperator(
            task_id='if_response_d_present_29',
            test=lambda: rail.result('get_supervisor_assignment_details_28') and (rail.result('get_supervisor_assignment_details_28')[
                'supervisor']['uri'] != rail.result('search_users_searchsupervisorbyloginname_24')['uri']),
            yes_task="get_assigned_permission_sets_for_user2_30",
            no_task="if_response_d_blank_36",
        )

        get_assigned_permission_sets_for_user2_30 = rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets_for_user2_30',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{result('search_users_searchsupervisorbyloginname_24').uri}}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', 'urn:replicon:policy:supervision', 'permissionSet.name', '') if response and response[0]['policyUri'] else ''
        )

        if_pluckname_smart_joinnil_present_31 = rail.IfOperator(
            task_id='if_pluckname_smart_joinnil_present_31',
            test=lambda: bool(rail.result(
                'get_assigned_permission_sets_for_user2_30')),
            yes_task="update_supervisor_assignment_schedule_over_date_range_32",
            no_task="if_pluckname_smart_joinnil_blank_33",
        )

        update_supervisor_assignment_schedule_over_date_range_32 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_32',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "supervisorUri": "{{result('search_users_searchsupervisorbyloginname_24').uri}}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ dag_run.conf.rundate.year }}",
                        "month": "{{ dag_run.conf.rundate.month }}",
                        "day": "{{ dag_run.conf.rundate.day }}"
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_pluckname_smart_joinnil_blank_33 = rail.IfOperator(
            task_id='if_pluckname_smart_joinnil_blank_33',
            test=lambda: not bool(rail.result(
                'get_assigned_permission_sets_for_user2_30')),
            yes_task="assign_permission_set_to_user_supervisor_34",
            no_task="if_response_d_blank_36",
        )

        assign_permission_set_to_user_supervisor_34 = rail.RepliconServiceOperator(
            task_id='assign_permission_set_to_user_supervisor_34',
            endpoint="/services/PermissionSetService1.svc/PutPermissionSetAssignmentsForUser",
            data={
                "userUri": "{{result('search_users_searchsupervisorbyloginname_24').uri}}",
                "permissionSetUris": [
                    "{{ dag_run.conf.supervisorpermissionuri }}",
                    "{{ dag_run.conf.basicwithreportpermissionuri }}"
                ]
            }
        )

        update_supervisor_assignment_schedule_over_date_range_35 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_35',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "supervisorUri": "{{result('search_users_searchsupervisorbyloginname_24').uri}}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ dag_run.conf.rundate.year }}",
                        "month": "{{ dag_run.conf.rundate.month }}",
                        "day": "{{ dag_run.conf.rundate.day }}"
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_response_d_blank_36 = rail.IfOperator(
            task_id='if_response_d_blank_36',
            test='''{{result('get_supervisor_assignment_details_28') | is_falsy }}''',
            yes_task="_adhoc_http_action_37",
            no_task="if_request_loginname_equals_to_dataworkato_servicereceive_requestrequestsupervisor_43",
        )

        _adhoc_http_action_37 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_37',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{result('search_users_searchsupervisorbyloginname_24').uri}}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', 'urn:replicon:policy:supervision', 'permissionSet.name', '') if response and response[0]['policyUri'] else ''
        )

        if_pluckname_smart_joinnil_present_38 = rail.IfOperator(
            task_id='if_pluckname_smart_joinnil_present_38',
            test=lambda: bool(rail.result('_adhoc_http_action_37')),
            yes_task="update_supervisor_assignment_schedule_over_date_range_39",
            no_task="if_pluckname_smart_joinnil_blank_40",
        )

        update_supervisor_assignment_schedule_over_date_range_39 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_39',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "supervisorUri": "{{result('search_users_searchsupervisorbyloginname_24').uri}}",
                "dateRange": {
                    "startDate": null,
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_pluckname_smart_joinnil_blank_40 = rail.IfOperator(
            task_id='if_pluckname_smart_joinnil_blank_40',
            test=lambda: not bool(rail.result('_adhoc_http_action_37')),
            yes_task="assign_permission_set_to_user_supervisor_41",
            no_task="if_request_loginname_equals_to_dataworkato_servicereceive_requestrequestsupervisor_43",
        )

        assign_permission_set_to_user_supervisor_41 = rail.RepliconServiceOperator(
            task_id='assign_permission_set_to_user_supervisor_41',
            endpoint="/services/PermissionSetService1.svc/PutPermissionSetAssignmentsForUser",
            data={
                "userUri": "{{result('search_users_searchsupervisorbyloginname_24').uri}}",
                "permissionSetUris": [
                    "{{ dag_run.conf.supervisorpermissionuri }}",
                    "{{ dag_run.conf.basicwithreportpermissionuri }}"
                ]
            }
        )

        update_supervisor_assignment_schedule_over_date_range_42 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_42',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "supervisorUri": "{{result('search_users_searchsupervisorbyloginname_24').uri}}",
                "dateRange": {
                    "startDate": null,
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_request_loginname_equals_to_dataworkato_servicereceive_requestrequestsupervisor_43 = rail.IfOperator(
            task_id='if_request_loginname_equals_to_dataworkato_servicereceive_requestrequestsupervisor_43',
            test='''{{ dag_run.conf.loginname == dag_run.conf.supervisor }}''',
            yes_task="insert_to_list_44",
            no_task="get_effective_user_group_membership_45",
        )

        insert_to_list_44 = rail.SetVariableOperator(
            task_id='insert_to_list_44',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "value": "Supervisor not assigned/updated since the user and supervisor are same"
            }
        )

        get_effective_user_group_membership_45 = rail.RepliconServiceOperator(
            task_id='get_effective_user_group_membership_45',
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "dateRange": null
            }
        )

        if_request_employeetype_present_46 = rail.IfOperator(
            task_id='if_request_employeetype_present_46',
            test=lambda dag_run: dag_run.conf['employeetype'] and (not (rail.result('get_effective_user_group_membership_45') and rail.result(
                'get_effective_user_group_membership_45')['employeeTypes'] and rail.result(
                'get_effective_user_group_membership_45')['employeeTypes'][0]['employeeType']) or (
                rail.result(
                'get_effective_user_group_membership_45')['employeeTypes'][0]['employeeType']['employeeType']['displayText'] != dag_run.conf['employeetype'])),
            yes_task="update_employee_type_group_47",
            no_task="if_request_timeapprover_blank_48",
        )

        update_employee_type_group_47 = rail.RepliconServiceOperator(
            task_id='update_employee_type_group_47',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
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
                                        "uri": "{{ dag_run.conf.employeetypeuri }}",
                                        "parent": null,
                                        "name": null,
                                        "parameterCorrelationId": null
                                    },
                                    "effectiveDate": {
                                        "year": "{{ dag_run.conf.rundate.year }}",
                                        "month": "{{ dag_run.conf.rundate.month }}",
                                        "day": "{{ dag_run.conf.rundate.day }}"
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

        if_request_timeapprover_blank_48 = rail.IfOperator(
            task_id='if_request_timeapprover_blank_48',
            test='''{{ dag_run.conf.timeapprover | is_falsy }}''',
            yes_task="insert_to_list_49",
            no_task="if_request_timeapprover_present_50",
        )

        insert_to_list_49 = rail.SetVariableOperator(
            task_id='insert_to_list_49',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "value": "time approver not updated since it is not present in the feed file."
            }
        )

        if_request_timeapprover_present_50 = rail.IfOperator(
            task_id='if_request_timeapprover_present_50',
            test=lambda dag_run: dag_run.conf['timeapprover'] and (not (rail.result('get_effective_user_group_membership_45') and rail.result(
                'get_effective_user_group_membership_45')['locations'] and rail.result(
                'get_effective_user_group_membership_45')['locations'][0]['location']) or (
                rail.result(
                    'get_effective_user_group_membership_45')['locations'][0]['location']['location']['displayText'] != dag_run.conf['timeapprover'])),
            yes_task="update_time_approver_assignment_51",
            no_task="if_request_departmentfullpath_present_52",
        )

        update_time_approver_assignment_51 = rail.RepliconServiceOperator(
            task_id='update_time_approver_assignment_51',
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
                                        "uri": "{{ dag_run.conf.timeapproveruri }}",
                                        "parentUri": null,
                                        "name": null
                                    },
                                    "effectiveDate": {
                                        "year": "{{ dag_run.conf.rundate.year }}",
                                        "month": "{{ dag_run.conf.rundate.month }}",
                                        "day": "{{ dag_run.conf.rundate.day }}"
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

        if_request_departmentfullpath_present_52 = rail.IfOperator(
            task_id='if_request_departmentfullpath_present_52',
            test=lambda dag_run: dag_run.conf['departmentfullpath'] and (not (rail.result('get_effective_user_group_membership_45') and rail.result(
                'get_effective_user_group_membership_45')['departments'] and rail.result(
                'get_effective_user_group_membership_45')['departments'][0]['department']) or (
                rail.result(
                    'get_effective_user_group_membership_45')['departments'][0]['department']['department']['uri'] != dag_run.conf['departmentgroupuri'])),
            yes_task="update_department_group_53",
            no_task="if_schedulepolicies_to_json_contains_urn_54",
        )

        update_department_group_53 = rail.RepliconServiceOperator(
            task_id='update_department_group_53',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
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
                                        "uri": "{{ dag_run.conf.departmentgroupuri }}",
                                        "parent": null,
                                        "name": null,
                                        "parameterCorrelationId": null
                                    },
                                    "effectiveDate": {
                                        "year": "{{ dag_run.conf.rundate.year }}",
                                        "month": "{{ dag_run.conf.rundate.month }}",
                                        "day": "{{ dag_run.conf.rundate.day }}"
                                    }
                                }
                            ],
                            "endDate": null
                        }
                    },
                    "objectExtensionFieldsToApply": []
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_schedulepolicies_to_json_contains_urn_54 = rail.IfOperator(
            task_id='if_schedulepolicies_to_json_contains_urn_54',
            test=lambda: 'urn' in json.dumps(rail.result(
                'bulk_get_users3_5')[0]['schedulePolicies']),
            yes_task="invoke_custom_ruby_code_56",
            no_task="if_request_officeschedule_present_57",
        )

        def get_datestring(dateobj):
            return str(dateobj['day']) + "/" + str(dateobj['month']) + '/' + str(dateobj['year'])

        def get_current_office_schedule():
            officeschedule = rail.result('bulk_get_users3_5')[
                0]['schedulePolicies']
            schedule_array = [{
                'effectivedate': get_datestring(schedule['effectiveDate']) if (
                    schedule['effectiveDate'] and schedule['effectiveDate']['day']) else get_datestring(rail.result(
                    'bulk_get_users3_5')[0]['userDetails']['employmentDateRange']['startDate']),
                'displayText': schedule['officeSchedule']['displayText'],
                'uri': schedule['officeSchedule']['uri'],
                'scheduletypeuri': schedule['scheduleTypeUri'],
                'daydiff': (datetime.strptime(datetime.now().strftime("%d%m%Y"), '%d%m%Y') - datetime.strptime(get_datestring(
                    schedule['effectiveDate'] if schedule['effectiveDate'] and schedule['effectiveDate']['day'] else rail.result(
                    'bulk_get_users3_5')[0]['userDetails']['employmentDateRange']['startDate']), "%d/%m/%Y")).days
            } for schedule in officeschedule]
            return min(schedule_array, key=lambda x: x['daydiff'])

        invoke_custom_ruby_code_56 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_56',
            python_callable=get_current_office_schedule
        )

        if_request_officeschedule_present_57 = rail.IfOperator(
            task_id='if_request_officeschedule_present_57',
            test='''{{ dag_run.conf.officeschedule | is_truthy }}''',
            yes_task="if_schedulepolicies_displaytext_present_58",
            no_task="get_place_assignment_schedule_for_user_63",
        )

        if_schedulepolicies_displaytext_present_58 = rail.IfOperator(
            task_id='if_schedulepolicies_displaytext_present_58',
            test=lambda dag_run: bool(rail.result('invoke_custom_ruby_code_56')['displayText'] and (
                dag_run.conf['officeschedule'] != rail.result('invoke_custom_ruby_code_56')['displayText'])),
            yes_task="if_request_officescheduleuri_present_59",
            no_task="get_place_assignment_schedule_for_user_63",
        )

        if_request_officescheduleuri_present_59 = rail.IfOperator(
            task_id='if_request_officescheduleuri_present_59',
            test='''{{ dag_run.conf.officescheduleuri | is_truthy }}''',
            yes_task="update_office_schedule_60",
            no_task="if_request_officescheduleuri_blank_61",
        )

        update_office_schedule_60 = rail.RepliconServiceOperator(
            task_id='update_office_schedule_60',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
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
                                        "name": "{{ dag_run.conf.officeschedule }}",
                                        "officeSchedule": {
                                            "officeScheduleUri": null,
                                            "name": "{{ dag_run.conf.officeschedule }}"
                                        },
                                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                                    },
                                    "effectiveDate": {
                                        "year": "{{ dag_run.conf.rundate.year }}",
                                        "month": "{{ dag_run.conf.rundate.month }}",
                                        "day": "{{ dag_run.conf.rundate.day }}"
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

        if_request_officescheduleuri_blank_61 = rail.IfOperator(
            task_id='if_request_officescheduleuri_blank_61',
            test='''{{ dag_run.conf.officescheduleuri | is_falsy }}''',
            yes_task="insert_to_list_62",
            no_task="get_place_assignment_schedule_for_user_63",
        )

        insert_to_list_62 = rail.SetVariableOperator(
            task_id='insert_to_list_62',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "value": 'Office schedule "{{ dag_run.conf.officeschedule }}" not available in Replicon'
            }
        )

        get_place_assignment_schedule_for_user_63 = rail.RepliconServiceOperator(
            task_id='get_place_assignment_schedule_for_user_63',
            endpoint="/services/PlaceService1.svc/GetPlaceAssignmentScheduleForUser",
            data={
                "userTarget": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                }
            }
        )

        if_first_displaytext_blank_64 = rail.IfOperator(
            task_id='if_first_displaytext_blank_64',
            test=lambda: not bool(rail.result('get_place_assignment_schedule_for_user_63') and rail.result('get_place_assignment_schedule_for_user_63')[
                                  0]['places'] and rail.result('get_place_assignment_schedule_for_user_63')[0]['places'][0]['displayText']),
            yes_task="update_place_65",
            no_task="if_first_displaytext_present_66",
        )

        update_place_65 = rail.RepliconServiceOperator(
            task_id='update_place_65',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "placeAssignmentsModifications": {
                        "placeAssignmentScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementPlaceAssignmentSchedule": [],
                        "updatePlaceAssignmentScheduleOverDateRange": {
                            "replacementPlaceAssignmentScheduleEntries": [
                                {
                                    "effectiveDate": {
                                        "year": "{{ dag_run.conf.rundate.year }}",
                                        "month": "{{ dag_run.conf.rundate.month }}",
                                        "day": "{{ dag_run.conf.rundate.day }}"
                                    },
                                    "places": [
                                        {
                                            "uri": "{{ dag_run.conf.placeuri }}",
                                            "name": null
                                        }
                                    ]
                                }
                            ],
                            "endDate": null
                        }
                    },
                    "objectExtensionFieldsToApply": []
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_first_displaytext_present_66 = rail.IfOperator(
            task_id='if_first_displaytext_present_66',
            test=lambda: bool(rail.result('get_place_assignment_schedule_for_user_63') and rail.result('get_place_assignment_schedule_for_user_63')[
                              0]['places'] and rail.result('get_place_assignment_schedule_for_user_63')[0]['places'][0]['displayText']),
            yes_task="invoke_custom_ruby_code_68",
            no_task="if_request_place_present_69",
        )

        def get_current_place():
            placeschedule = rail.result(
                'get_place_assignment_schedule_for_user_63')
            place_schedule = [{
                'effectivedate': get_datestring(place['effectiveDate']) if place['effectiveDate'] and place['effectiveDate']['day'] else get_datestring(
                    rail.result('bulk_get_users3_5')[0]['userDetails']['employmentDateRange']['startDate']),
                'uri': place['places'][0]['uri'],
                'displaytext': place['places'][0]['displayText'],
                'daydiff': (datetime.strptime(datetime.now().strftime("%d%m%Y"), '%d%m%Y') - datetime.strptime(get_datestring(
                    place['effectiveDate'] if place['effectiveDate'] and place['effectiveDate']['day'] else rail.result(
                    'bulk_get_users3_5')[0]['userDetails']['employmentDateRange']['startDate']), "%d/%m/%Y")).days
            } for place in placeschedule]
            return min(place_schedule, key=lambda x: x['daydiff'])

        invoke_custom_ruby_code_68 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_68',
            python_callable=get_current_place
        )

        if_request_place_present_69 = rail.IfOperator(
            task_id='if_request_place_present_69',
            test='''{{ dag_run.conf.place | is_truthy }}''',
            yes_task="if_placelist_displaytext_present_70",
            no_task="add_final_log_for_updated_user",
        )

        if_placelist_displaytext_present_70 = rail.IfOperator(
            task_id='if_placelist_displaytext_present_70',
            test=lambda dag_run: bool(rail.result('invoke_custom_ruby_code_68')['displaytext'] and (
                dag_run.conf['place'] != rail.result('invoke_custom_ruby_code_68')['displaytext'])),
            yes_task="if_request_placeuri_present_71",
            no_task="add_final_log_for_updated_user",
        )

        if_request_placeuri_present_71 = rail.IfOperator(
            task_id='if_request_placeuri_present_71',
            test='''{{ dag_run.conf.placeuri | is_truthy }}''',
            yes_task="update_place_72",
            no_task="if_request_placeuri_blank_73",
        )

        update_place_72 = rail.RepliconServiceOperator(
            task_id='update_place_72',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "placeAssignmentsModifications": {
                        "placeAssignmentScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementPlaceAssignmentSchedule": [],
                        "updatePlaceAssignmentScheduleOverDateRange": {
                            "replacementPlaceAssignmentScheduleEntries": [
                                {
                                    "effectiveDate": {
                                        "year": "{{ dag_run.conf.rundate.year }}",
                                        "month": "{{ dag_run.conf.rundate.month }}",
                                        "day": "{{ dag_run.conf.rundate.day }}"
                                    },
                                    "places": [
                                        {
                                            "uri": "{{ dag_run.conf.placeuri }}",
                                            "name": null
                                        }
                                    ]
                                }
                            ],
                            "endDate": null
                        }
                    },
                    "objectExtensionFieldsToApply": []
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_request_placeuri_blank_73 = rail.IfOperator(
            task_id='if_request_placeuri_blank_73',
            test='''{{ dag_run.conf.placeuri | is_falsy }}''',
            yes_task="insert_to_list_74",
            no_task="add_final_log_for_updated_user",
        )

        insert_to_list_74 = rail.SetVariableOperator(
            task_id='insert_to_list_74',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "value": 'Place "{{ dag_run.conf.place }}" not available in Replicon'
            }
        )

        add_final_log_for_updated_user = rail.WriteLogOperator(
            task_id='add_final_log_for_updated_user',
            log="{{ dag_run.conf.userimportlogslookup }}",
            message="na",
            severity=lambda: "Exception" if rail.get_dag_run_var(
                'Exception') else "Success",
            properties=lambda dag_run: {
                "loginname": dag_run.conf['loginname'],
                "action": "Update",
                "status": "Exception" if rail.get_dag_run_var('Exception') else "Success",
                "details": "Partialy Updated - " + ';'.join([excpetion['value'] for excpetion in rail.get_dag_run_var('Exception')]) if rail.get_dag_run_var(
                    'Exception') else "Successfully updated",
                "jobid": dag_run.conf['callerjobid'],
                "childjobid": rail.render_template("{{ dag_run_ecid() }}"),
                "firstname": dag_run.conf['firstname'],
                "lastname": dag_run.conf['lastname']
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            log="{{ dag_run.conf.userimportlogslookup }}",
            trigger_rule='one_failed',
            message="na",
            severity="Error",
            properties={
                "loginname": "{{dag_run.conf.loginname}}",
                "action": "Update",
                "status": "Error",
                "details": "{{get_error_message()}}",
                "jobid": "{{dag_run.conf.callerjobid}}",
                "childjobid": "{{ dag_run_ecid() }}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "lastname": "{{ dag_run.conf.lastname }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> declare_list_2
        declare_list_2 >> declare_list_3 >> bulk_get_users3_5 >> if_firstno_downcase_equals_to_yes_6
        if_firstno_downcase_equals_to_yes_6 >> rail.Label(
            'Yes') >> log_user_only_for_admin_use >> catch_and_log_error
        if_firstno_downcase_equals_to_yes_6 >> rail.Label(
            'No') >> if_securityconfiguration_isloginenabled_is_not_true_rehire_9
        if_securityconfiguration_isloginenabled_is_not_true_rehire_9 >> rail.Label(
            'Yes') >> enable_login_10 >> put_policy_set_assignments_for_user_assigntimesheettemplateandpunchentrypolicy_11 >> if_request_firstname_present_12
        if_securityconfiguration_isloginenabled_is_not_true_rehire_9 >> rail.Label(
            'No') >> if_request_firstname_present_12
        if_request_firstname_present_12 >> rail.Label(
            'Yes') >> update_first_name_13 >> if_request_lastname_present_14
        if_request_firstname_present_12 >> rail.Label(
            'No') >> if_request_lastname_present_14
        if_request_lastname_present_14 >> rail.Label(
            'Yes') >> update_last_name_15 >> if_request_emailaddress_present_16
        if_request_lastname_present_14 >> rail.Label(
            'No') >> if_request_emailaddress_present_16
        if_request_emailaddress_present_16 >> rail.Label(
            'Yes') >> update_email_17 >> if_request_employeeid_present_18
        if_request_emailaddress_present_16 >> rail.Label(
            'No') >> if_request_employeeid_present_18
        if_request_employeeid_present_18 >> rail.Label(
            'Yes') >> update_employee_id_19 >> if_startdate_day_blank_20
        if_request_employeeid_present_18 >> rail.Label(
            'No') >> if_startdate_day_blank_20
        if_startdate_day_blank_20 >> rail.Label(
            'Yes') >> update_employment_date_range_21 >> if_request_supervisor_present_22
        if_startdate_day_blank_20 >> rail.Label(
            'No') >> if_request_supervisor_present_22
        if_request_supervisor_present_22 >> rail.Label(
            'Yes') >> if_request_loginname_not_equals_to_dataworkato_servicereceive_requestrequestsupervisor_23
        if_request_loginname_not_equals_to_dataworkato_servicereceive_requestrequestsupervisor_23 >> rail.Label(
            'Yes') >> search_users_searchsupervisorbyloginname_24 >> if_search_users_searchsupervisorbyloginname_24_users_less_than_1_25
        if_search_users_searchsupervisorbyloginname_24_users_less_than_1_25 >> rail.Label(
            'Yes') >> add_to_supervisor_assignment_queue >> if_search_users_searchsupervisorbyloginname_24_users_greater_than_0_27
        if_search_users_searchsupervisorbyloginname_24_users_less_than_1_25 >> rail.Label(
            'No') >> if_search_users_searchsupervisorbyloginname_24_users_greater_than_0_27
        if_search_users_searchsupervisorbyloginname_24_users_greater_than_0_27 >> rail.Label(
            'Yes') >> get_supervisor_assignment_details_28 >> if_response_d_present_29
        if_response_d_present_29 >> rail.Label(
            'Yes') >> get_assigned_permission_sets_for_user2_30 >> if_pluckname_smart_joinnil_present_31
        if_pluckname_smart_joinnil_present_31 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_32 >> if_pluckname_smart_joinnil_blank_33
        if_pluckname_smart_joinnil_present_31 >> rail.Label(
            'No') >> if_pluckname_smart_joinnil_blank_33
        if_pluckname_smart_joinnil_blank_33 >> rail.Label(
            'Yes') >> assign_permission_set_to_user_supervisor_34 >> update_supervisor_assignment_schedule_over_date_range_35 >> if_response_d_blank_36
        if_pluckname_smart_joinnil_blank_33 >> rail.Label(
            'No') >> if_response_d_blank_36
        if_response_d_present_29 >> rail.Label('No') >> if_response_d_blank_36
        if_response_d_blank_36 >> rail.Label(
            'Yes') >> _adhoc_http_action_37 >> if_pluckname_smart_joinnil_present_38
        if_pluckname_smart_joinnil_present_38 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_39 >> if_pluckname_smart_joinnil_blank_40
        if_pluckname_smart_joinnil_present_38 >> rail.Label(
            'No') >> if_pluckname_smart_joinnil_blank_40
        if_pluckname_smart_joinnil_blank_40 >> rail.Label(
            'Yes') >> assign_permission_set_to_user_supervisor_41 >> update_supervisor_assignment_schedule_over_date_range_42
        update_supervisor_assignment_schedule_over_date_range_42 >> if_request_loginname_equals_to_dataworkato_servicereceive_requestrequestsupervisor_43
        if_pluckname_smart_joinnil_blank_40 >> rail.Label(
            'No') >> if_request_loginname_equals_to_dataworkato_servicereceive_requestrequestsupervisor_43
        if_response_d_blank_36 >> rail.Label(
            'No') >> if_request_loginname_equals_to_dataworkato_servicereceive_requestrequestsupervisor_43
        if_search_users_searchsupervisorbyloginname_24_users_greater_than_0_27 >> rail.Label(
            'No') >> if_request_loginname_equals_to_dataworkato_servicereceive_requestrequestsupervisor_43
        if_request_loginname_not_equals_to_dataworkato_servicereceive_requestrequestsupervisor_23 >> rail.Label(
            'No') >> if_request_loginname_equals_to_dataworkato_servicereceive_requestrequestsupervisor_43
        if_request_loginname_equals_to_dataworkato_servicereceive_requestrequestsupervisor_43 >> rail.Label(
            'Yes') >> insert_to_list_44 >> get_effective_user_group_membership_45
        if_request_loginname_equals_to_dataworkato_servicereceive_requestrequestsupervisor_43 >> rail.Label(
            'No') >> get_effective_user_group_membership_45
        if_request_supervisor_present_22 >> rail.Label(
            'No') >> get_effective_user_group_membership_45 >> if_request_employeetype_present_46
        if_request_employeetype_present_46 >> rail.Label(
            'Yes') >> update_employee_type_group_47 >> if_request_timeapprover_blank_48
        if_request_employeetype_present_46 >> rail.Label(
            'No') >> if_request_timeapprover_blank_48
        if_request_timeapprover_blank_48 >> rail.Label(
            'Yes') >> insert_to_list_49 >> if_request_timeapprover_present_50
        if_request_timeapprover_blank_48 >> rail.Label(
            'No') >> if_request_timeapprover_present_50
        if_request_timeapprover_present_50 >> rail.Label(
            'Yes') >> update_time_approver_assignment_51 >> if_request_departmentfullpath_present_52
        if_request_timeapprover_present_50 >> rail.Label(
            'No') >> if_request_departmentfullpath_present_52
        if_request_departmentfullpath_present_52 >> rail.Label(
            'Yes') >> update_department_group_53 >> if_schedulepolicies_to_json_contains_urn_54
        if_request_departmentfullpath_present_52 >> rail.Label(
            'No') >> if_schedulepolicies_to_json_contains_urn_54
        if_schedulepolicies_to_json_contains_urn_54 >> rail.Label(
            'Yes') >> invoke_custom_ruby_code_56 >> if_request_officeschedule_present_57
        if_schedulepolicies_to_json_contains_urn_54 >> rail.Label(
            'No') >> if_request_officeschedule_present_57
        if_request_officeschedule_present_57 >> rail.Label(
            'Yes') >> if_schedulepolicies_displaytext_present_58
        if_schedulepolicies_displaytext_present_58 >> rail.Label(
            'Yes') >> if_request_officescheduleuri_present_59
        if_request_officescheduleuri_present_59 >> rail.Label(
            'Yes') >> update_office_schedule_60 >> if_request_officescheduleuri_blank_61
        if_request_officescheduleuri_present_59 >> rail.Label(
            'No') >> if_request_officescheduleuri_blank_61
        if_request_officescheduleuri_blank_61 >> rail.Label(
            'Yes') >> insert_to_list_62 >> get_place_assignment_schedule_for_user_63
        if_request_officescheduleuri_blank_61 >> rail.Label(
            'No') >> get_place_assignment_schedule_for_user_63
        if_schedulepolicies_displaytext_present_58 >> rail.Label(
            'No') >> get_place_assignment_schedule_for_user_63
        if_request_officeschedule_present_57 >> rail.Label(
            'No') >> get_place_assignment_schedule_for_user_63 >> if_first_displaytext_blank_64
        if_first_displaytext_blank_64 >> rail.Label(
            'Yes') >> update_place_65 >> if_first_displaytext_present_66
        if_first_displaytext_blank_64 >> rail.Label(
            'No') >> if_first_displaytext_present_66
        if_first_displaytext_present_66 >> rail.Label(
            'Yes') >> invoke_custom_ruby_code_68 >> if_request_place_present_69
        if_first_displaytext_present_66 >> rail.Label(
            'No') >> if_request_place_present_69
        if_request_place_present_69 >> rail.Label(
            'Yes') >> if_placelist_displaytext_present_70
        if_placelist_displaytext_present_70 >> rail.Label(
            'Yes') >> if_request_placeuri_present_71
        if_request_placeuri_present_71 >> rail.Label(
            'Yes') >> update_place_72 >> if_request_placeuri_blank_73
        if_request_placeuri_present_71 >> rail.Label(
            'No') >> if_request_placeuri_blank_73
        if_request_placeuri_blank_73 >> rail.Label(
            'Yes') >> insert_to_list_74 >> add_final_log_for_updated_user
        if_request_placeuri_blank_73 >> rail.Label(
            'No') >> add_final_log_for_updated_user
        if_placelist_displaytext_present_70 >> rail.Label(
            'No') >> add_final_log_for_updated_user
        if_request_place_present_69 >> rail.Label(
            'No') >> add_final_log_for_updated_user >> catch_and_log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
