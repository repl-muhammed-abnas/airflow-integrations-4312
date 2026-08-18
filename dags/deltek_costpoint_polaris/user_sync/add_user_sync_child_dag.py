from datetime import timedelta
from datetime import datetime
import json
from airflow.models import Variable
from deltek_costpoint_polaris.user_sync.utils.mapper_helpers import as_name_list
import rail

# pylint:disable = too-many-statements
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_costpoint_add_user_sync_{config.instance}',
        description=f'deltek_costpoint_add_user_sync_{config.instance}',
        company_key=config.company_key,
        max_active_runs=config.child_dag_max_active_runs,
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'deltek_costpoint_conn_id': config.deltek_cospoint_conn_id,
        }
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_super_users_permission'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_super_users_permission',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def get_superuser_uris(dag_run):
            supervisor_user_uris = []
            for superuser in dag_run.conf['userhistory']:
                if superuser['superuseruri'] and superuser['superuseruri'] not in supervisor_user_uris:
                    supervisor_user_uris.append(superuser['superuseruri'])
            return supervisor_user_uris

        get_super_users_permission = rail.RepliconServiceOperator(
            task_id='get_super_users_permission',
            endpoint='/services/PermissionSetService1.svc/BulkGetAssignedPermissionSetsForUsers',
            data=lambda dag_run: {
                "userUris": get_superuser_uris(dag_run)
            }
        )

        foreach_supervisor_assignment = rail.ForEachOperator(
            task_id='foreach_supervisor_assignment',
            items="{{ dag_run.conf.userhistory | to_json }}",
            start_task='if_supervisor_assignment_present',
            end_task='foreach_supervisor_assignment_end'
        )

        def is_not_supervisor_permission_present():
            supervisor_permission = list(filter(lambda x: x['user']['uri'] == rail.result(
                'foreach_supervisor_assignment')['superuseruri'], rail.result('get_super_users_permission')))
            permissionset = rail.find_first_by_attr_and_get_attr(
                supervisor_permission, 'policyUri', 'urn:replicon:policy:supervision', 'permissionSet')
            if permissionset or not rail.result('foreach_supervisor_assignment')['superuseruri']:
                return False
            return True

        if_supervisor_assignment_present = rail.IfOperator(
            task_id='if_supervisor_assignment_present',
            test=is_not_supervisor_permission_present,
            yes_task="put_permissions_user",
            no_task="foreach_supervisor_assignment_end",
        )

        put_permissions_user = rail.RepliconServiceOperator(
            task_id='put_permissions_user',
            endpoint="/services/PermissionSetService1.svc/PutPermissionSetAssignmentsForUser",
            data=lambda dag_run: {
                'userUri': rail.result('foreach_supervisor_assignment')['superuseruri'],
                "permissionSetUris": [uri for uri in
                                      [dag_run.conf['supervisorpermissionuri'], dag_run.conf['reportuserpermissionuri']] if uri != None]
            }
        )

        foreach_supervisor_assignment_end = rail.EmptyOperator(
            task_id='foreach_supervisor_assignment_end',
        )

        def get_oefs(dag_run):
            oefs = []

            def add_tag_oef(tagUri, definitionuri):
                oefs.append(
                    {
                        "definition": {
                            "uri": definitionuri,
                            "name": null
                        },
                        "tag": {
                            "uri": tagUri,
                            "slug": null,
                            "tagName": null
                        },
                        "numericValue": null,
                        "textValue": null,
                        "fileValue": null,
                        "jsonValue": null
                    }
                )

            def add_text_oef(definitionuri, textValue):
                oefs.append(
                    {
                        "definition": {
                            "uri": definitionuri,
                            "name": null
                        },
                        "tag": null,
                        "numericValue": null,
                        "textValue": textValue,
                        "fileValue": null,
                        "jsonValue": null
                    }
                )

            if dag_run.conf['generallabourcategoriestaguri']:
                add_tag_oef(dag_run.conf['generallabourcategoriestaguri'],
                            dag_run.conf['generallabourcategoriesuri'])
            if dag_run.conf['paytypetaguri']:
                add_tag_oef(dag_run.conf['paytypetaguri'],
                            dag_run.conf['paytypeuri'])
            if dag_run.conf['oeftaxableentitytaguri']:
                add_tag_oef(dag_run.conf['oeftaxableentitytaguri'],
                            dag_run.conf['oeftaxableentityuri'])
            if dag_run.conf['oefemployeeclasstaguri']:
                add_tag_oef(dag_run.conf['oefemployeeclasstaguri'],
                            dag_run.conf['oefemployeeclassuri'])
            if dag_run.conf['oefflsaexempttaguri']:
                add_tag_oef(dag_run.conf['oefflsaexempttaguri'],
                            dag_run.conf['oefflsaexempturi'])
            if dag_run.conf['projectlaborcategorytaguri']:
                add_tag_oef(dag_run.conf['projectlaborcategorytaguri'],
                            dag_run.conf['projectlaborcategoryuri'])
            if dag_run.conf['contractfltaguri']:
                add_tag_oef(dag_run.conf['contractfltaguri'],
                            dag_run.conf['contractfluri'])
            add_text_oef(dag_run.conf['companyOefUri'],
                         dag_run.conf['companyCode'])

            return oefs

        def add_location_history(dag_run):
            location_history = []
            for location in dag_run.conf['userhistory']:
                effective_date = datetime.strptime(
                    location['effectivedate'], config.costpoint_date_format) if location['effectivedate'] else None
                if effective_date and location.get('locationuri'):
                    location_history.append({
                        "location": {
                            "uri": location['locationuri'],
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": {
                            "year": effective_date.year,
                            "month": effective_date.month,
                            "day": effective_date.day
                        }
                    })
            return location_history

        def add_department_history(dag_run):
            department_history = []
            for dpt in dag_run.conf['userhistory']:
                effective_date = datetime.strptime(
                    dpt['effectivedate'], config.costpoint_date_format) if dpt['effectivedate'] else None
                if effective_date and dpt.get('departmenturi'):
                    department_history.append({
                        "departmentGroup": {
                            "uri": dpt['departmenturi'],
                            "parent": null,
                            "name": null,
                            "parameterCorrelationId": null
                        },
                        "effectiveDate": {
                            "year": effective_date.year,
                            "month": effective_date.month,
                            "day": effective_date.day
                        }
                    })
            return department_history

        def add_employeetype_history(dag_run):
            employeetype_history = []
            for emptype in dag_run.conf['userhistory']:
                effective_date = datetime.strptime(
                    emptype['effectivedate'], config.costpoint_date_format) if emptype['effectivedate'] else None
                if effective_date and emptype.get('employeetypeuri'):
                    employeetype_history.append({
                        "employeeTypeGroup": {
                            "uri": emptype['employeetypeuri'],
                            "parent": null,
                            "name": null,
                            "parameterCorrelationId": null
                        },
                        "effectiveDate": {
                            "year": effective_date.year,
                            "month": effective_date.month,
                            "day": effective_date.day
                        }
                    })
            return employeetype_history

        def add_division_history(dag_run):
            division_history = []
            for div in dag_run.conf['userhistory']:
                effective_date = datetime.strptime(
                    div['effectivedate'], config.costpoint_date_format) if div['effectivedate'] else None
                if effective_date and div.get('divisionuri'):
                    division_history.append({
                        "division": {
                            "uri": div.get('divisionuri'),
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": {
                            "year": effective_date.year,
                            "month": effective_date.month,
                            "day": effective_date.day
                        }
                    })
            return division_history

        def add_supervisor_history(dag_run):
            supervisor_history = []
            for usersuper in dag_run.conf['userhistory']:
                effective_date = datetime.strptime(
                    usersuper['effectivedate'], config.costpoint_date_format) if usersuper['effectivedate'] else None
                if effective_date and usersuper.get('superuseruri'):
                    supervisor_history.append({
                        "supervisor": {
                            "uri": usersuper['superuseruri'],
                            "loginName": null,
                            "employeeId": null,
                            "parameterCorrelationId": null
                        },
                        "effectiveDate": {
                            "year": effective_date.year,
                            "month": effective_date.month,
                            "day": effective_date.day
                        }
                    })
            if supervisor_history:
                return {
                    "initialSupervisor": null,
                    "supervisorScheduleEntries": supervisor_history
                }
            return null

        def get_user_status(dag_run):
            if dag_run.conf['employeestatus'].lower() in ['act']:
                return True
            return False

        def get_schedule_history(dag_run):
            if config.schedule_from_cp:
                user_schedule_histories = []
                for schedule in dag_run.conf['userhistory']:
                    effective_date = datetime.strptime(
                        schedule['effectivedate'], config.costpoint_date_format) if schedule['effectivedate'] else None
                    if schedule['workschedule_uri']:
                        user_schedule_histories.append({
                            "schedulePolicy": {
                                "officeScheduleUri": schedule['workschedule_uri'],
                                "name": null,
                                "officeSchedule": {
                                    "officeScheduleUri": schedule['workschedule_uri'],
                                    "name": null
                                },
                                "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                            },
                            "effectiveDate": {
                                "year": effective_date.year,
                                "month": effective_date.month,
                                "day": effective_date.day
                            }
                        })
                return user_schedule_histories
            return [
                {
                    "schedulePolicy": {
                        "officeScheduleUri": null,
                        "name": dag_run.conf['office_schedule'],
                        "officeSchedule": {
                            "officeScheduleUri": null,
                            "name": dag_run.conf['office_schedule']
                        },
                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                    },
                    "effectiveDate": null
                }
            ]
        
        def get_holiday_request(dag_run):
            if config.holiday_calendar_from_cp:
                holiday_uri = dag_run.conf.get('holidaycalanderuri')
                if holiday_uri:
                    return {
                        "uri": holiday_uri,
                        "name": null
                    }
                return null

            return {
                "uri": null,
                "name": dag_run.conf['holiday_calendar']
            }

        def get_put_user_req(dag_run):
            hiredate = datetime.strptime(
                dag_run.conf['hiredate'], config.costpoint_date_format) if dag_run.conf['hiredate'] else datetime.now()
            terminationdate = datetime.strptime(
                dag_run.conf['employeeterminationdate'], config.costpoint_date_format) if dag_run.conf['employeeterminationdate'] else None
            return {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": dag_run.conf['loginname'],
                        "employeeId": null,
                        "parameterCorrelationId": null
                    },
                    "firstname": dag_run.conf['firstname'],
                    "lastname": dag_run.conf['lastname'],
                    "emailAddress": dag_run.conf['emailaddress'],
                    "employeeId": dag_run.conf['employeeId'],
                    "supervisorAssignmentSchedule": add_supervisor_history(dag_run),
                    "schedulePolicySchedule": get_schedule_history(dag_run),
                    "workWeekStartDayUri": dag_run.conf['workweek'],
                    "employmentDateRange": {
                        "startDate": {
                            "year": hiredate.year,
                            "month": hiredate.month,
                            "day": hiredate.day
                        },
                        "endDate": {
                            "year": terminationdate.year,
                            "month": terminationdate.month,
                            "day": terminationdate.day
                        } if terminationdate else null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "securityConfiguration": {
                        "enabledAuthenticationTypeUris": [
                            "urn:replicon:user-authentication-type:sso"
                        ] if config.is_sso_enabled else [
                            "urn:replicon:user-authentication-type:replicon"
                        ],
                        "isLoginEnabled": get_user_status(dag_run),
                        "loginName": dag_run.conf['loginname'],
                        **({} if config.is_sso_enabled else {
                            "SSOName": null,
                            "password": dag_run.conf['default_password']
                        })
                    },
                    "holidayCalendar": get_holiday_request(dag_run),
                    "holidayCalendarAssignmentSchedule": null,
                    "timeOffPolicy": null,
                    "permissionSets": [
                        {
                            "uri": null,
                            "name": dag_run.conf['user_permission']
                        }],
                    "policySets": [
                        {
                            "uri": null,
                            "name": dag_run.conf['time_off_template']
                        },
                        {
                            "uri": null,
                            "name": dag_run.conf['timesheet_template']
                        }
                    ],
                    "timesheetPeriodTypeUri": dag_run.conf['timesheet_period_type'],
                    "costRateSchedule": null,
                    "payrollRateSchedule": null,
                    "defaultBillingRate": null,
                    "timesheetApprovalPath": {
                        "uri": null,
                        "name": dag_run.conf['timesheet_approval_path']
                    },
                    "expenseApprovalPath": {
                        "uri": null,
                        "name": dag_run.conf['expenses_approval_path']
                    } if dag_run.conf.get('expenses_approval_path') else null,
                    "timeOffApprovalPath": {
                        "uri": null,
                        "name": dag_run.conf['timeoff_approval_path']
                    },
                    "workAuthorizationApprovalPath": null,
                    "customFieldValues": [],
                    "assignedActivities": [
                        {"name": name}
                        for name in as_name_list(dag_run.conf.get('activities'))
                    ],
                    "timeZone": {
                        "uri": dag_run.conf['timeZone']
                    },
                    "overtimeRuleAssignmentSchedule": null,
                    "validationRuleAssignmentSchedule": null,
                    "locationSchedule": add_location_history(dag_run),
                    "divisionSchedule": add_division_history(dag_run),
                    "costCenterSchedule": [{
                        "costCenter": {
                            "uri": dag_run.conf["costcenteruri"],
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": null
                    }] if dag_run.conf['costcenteruri'] else [],
                    "serviceCenterSchedule": [{
                        "serviceCenter": {
                            "uri": dag_run.conf["servicecenteruri"],
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": null
                    }] if dag_run.conf['servicecenteruri'] else [],
                    "departmentGroupSchedule": add_department_history(dag_run),
                    "employeeTypeGroupSchedule": add_employeetype_history(dag_run),
                    "timesheetPeriodSchedule": [
                        {
                            "timesheetPeriod": {
                                "uri": dag_run.conf['timesheetperioduri'],
                                "name": null
                            },
                            "effectiveDate": null
                        }
                    ] if dag_run.conf['timesheetperioduri'] else [],
                    "policyDataAccessScopes": [],
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": [
                        {
                            "payRuleScript": {
                                "name": dag_run.conf['payrule']
                            }
                        }
                    ] if dag_run.conf['payrule'] else [],
                    "displayNameParameter": {
                        "displayName": dag_run.conf['displayname']
                    },
                    "decimalSeparatorUri": null,
                    "numberGroupSeparatorUri": null,
                    "extensionFieldValues": get_oefs(dag_run)
                }
            }

        sync_users = rail.RepliconServiceOperator(
            task_id='sync_users',
            endpoint="/services/importservice1.svc/PutUser3",
            data=get_put_user_req,
        )

        assign_primary_role_to_user = rail.RepliconServiceOperator(
            task_id='assign_primary_role_to_user',
            endpoint='/services/ResourceService1.svc/PutProjectRoleAssignmentScheduleForUser',
            data=lambda dag_run: {
                "userUri": rail.result('sync_users')['uri'],
                "scheduleEntries": [
                    {
                        "projectRoles": [
                            {
                                "projectRole": {
                                    "uri": projectrole['plc_uri']
                                },
                                "isPrimary": "true"
                            }
                        ] if projectrole['plc_uri'] else [],
                        "effectiveDate": {
                            "year": datetime.strptime(
                                projectrole['effectivedate'], config.costpoint_date_format).year,
                            "month": datetime.strptime(
                                projectrole['effectivedate'], config.costpoint_date_format).month,
                            "day": datetime.strptime(
                                projectrole['effectivedate'], config.costpoint_date_format).day
                        }
                        if projectrole['effectivedate'] else None
                    }
                    for projectrole in dag_run.conf['userhistory']]
                if dag_run.conf['userhistory'] else []
            }
        )

        def is_missing_supervisor_assignment(dag_run):
            for usersuper in dag_run.conf['userhistory']:
                if not usersuper['superuseruri']:
                    return True
            return True

        if_supervisor_assignment_missing = rail.IfOperator(
            task_id='if_supervisor_assignment_missing',
            test=is_missing_supervisor_assignment,
            yes_task="add_supervisor_assignment_table",
            no_task="assign_required_timeofftypes",
        )

        def get_missing_supervisor_assignment(dag_run):
            supervisor_assignment_info = []
            for usersuper in dag_run.conf['userhistory']:
                if usersuper['superuseruri']:
                    supervisor_assignment_info.append({
                        "supervisor": usersuper['supervisor'],
                        "effectivedate": usersuper['effectivedate'],
                        "superuseruri": usersuper['superuseruri']
                    })
            return supervisor_assignment_info

        add_supervisor_assignment_table = rail.WriteLogOperator(
            task_id='add_supervisor_assignment_table',
            log="{{ dag_run.conf.supervisor_processing_log }}",
            message="na",
            severity="queued",
            properties=lambda dag_run: {
                "loginname": dag_run.conf['loginname'],
                "employeeid": dag_run.conf['employeeId'],
                "useruri": rail.result('sync_users')['uri'],
                "supervisorpermissionuri": dag_run.conf['supervisorpermissionuri'],
                "supervisorassignment": get_missing_supervisor_assignment(dag_run),
                "action": "Add",
                "status": "queued"
            }
        )

        def get_timeoff_uris(dag_run):
            if dag_run.conf['timeoffassigneduris']:
                return list(dag_run.conf['timeoffassigneduris'].split(','))
            return []

        assign_required_timeofftypes = rail.RepliconServiceOperator(
            task_id='assign_required_timeofftypes',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": rail.result('sync_users')['uri'],
                "timeOffTypeUris": get_timeoff_uris(dag_run)
            }
        )

        if_timeoff_present_skip_timeoff_assignment = rail.IfOperator(
            task_id='if_timeoff_present_skip_timeoff_assignment',
            test='''{{ dag_run.conf.timeoffassigneduris | is_truthy }}''',
            yes_task="foreach_timeoff_assignment",
            no_task="user_import_logs_add_entry",
        )

        foreach_timeoff_assignment = rail.ForEachOperator(
            task_id='foreach_timeoff_assignment',
            items=lambda dag_run: list(
                dag_run.conf['timeoffassigneduris'].split(',')),
            start_task='get_default_time_off_type_policy_schedule_for_user',
            end_task='foreach_timeoff_assignment_end'
        )

        get_default_time_off_type_policy_schedule_for_user = rail.RepliconServiceOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ result('sync_users').uri }}",
                    "timeOffTypeUri": "{{ result('foreach_timeoff_assignment') }}"
                }
            }
        )

        log_policyto_assign = rail.PythonOperator(
            task_id='log_policyto_assign',
            python_callable=lambda: json.loads(json.dumps(
                    rail.result('get_default_time_off_type_policy_schedule_for_user'), ensure_ascii=False).replace('null', '"effective"').replace(
                        '"script"', '"scriptTarget"'))
        )

        if_log_policyto_assign_present = rail.IfOperator(
            task_id='if_log_policyto_assign_present',
            test='''{{ result('log_policyto_assign') | is_truthy }}''',
            yes_task="put_user_time_off_account_policy_set_schedule",
            no_task="foreach_timeoff_assignment_end",
        )

        put_user_time_off_account_policy_set_schedule = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda: {
                "timeOffAccount": {
                    "userUri": rail.result('sync_users')['uri'],
                    "timeOffTypeUri": rail.result('foreach_timeoff_assignment')
                },
                "policySetScheduleEntries": rail.result('log_policyto_assign')
            }
        )

        foreach_timeoff_assignment_end = rail.EmptyOperator(
            task_id='foreach_timeoff_assignment_end',
        )

        def get_warning_message(dag_run):
            warning = []
            if not dag_run.conf['timesheetperioduri']:
                warning.append("Timesheet period is not available in replicon")
            if warning:
                combined_warnings = rail.smartjoin_by_delim(warning, ';')
                return "New user profile added partially as " + combined_warnings
            return "User Added Successfully"

        user_import_logs_add_entry = rail.WriteLogOperator(
            task_id='user_import_logs_add_entry',
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "employeeid": dag_run.conf['employeeId'],
                "action": "Add",
                "status": "Succeeded",
                "reason": get_warning_message(dag_run)
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            message="{{ get_error_message() }}",
            severity="Error",
            properties={
                "employeeid": "{{ dag_run.conf.employeeId }}",
                "action": "Add",
                "status": "Error",
                "reason": "{{ get_error_message() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> get_super_users_permission >> \
            foreach_supervisor_assignment >> if_supervisor_assignment_present
        if_supervisor_assignment_present >> rail.Label(
            'No') >> foreach_supervisor_assignment_end
        if_supervisor_assignment_present >> rail.Label(
            'Yes') >> put_permissions_user >> foreach_supervisor_assignment_end
        foreach_supervisor_assignment >> foreach_supervisor_assignment_end >> \
            sync_users >> assign_primary_role_to_user >> if_supervisor_assignment_missing
        if_supervisor_assignment_missing >> rail.Label(
            'Yes') >> add_supervisor_assignment_table >> assign_required_timeofftypes >> if_timeoff_present_skip_timeoff_assignment
        if_supervisor_assignment_missing >> rail.Label(
            'No') >> assign_required_timeofftypes
        if_timeoff_present_skip_timeoff_assignment >> rail.Label(
            'No') >> user_import_logs_add_entry
        if_timeoff_present_skip_timeoff_assignment >> rail.Label(
            'Yes') >> foreach_timeoff_assignment >> \
            get_default_time_off_type_policy_schedule_for_user >> log_policyto_assign >> \
            if_log_policyto_assign_present
        if_log_policyto_assign_present >> rail.Label(
            'Yes') >> put_user_time_off_account_policy_set_schedule >> foreach_timeoff_assignment_end
        if_log_policyto_assign_present >> rail.Label(
            'No') >> foreach_timeoff_assignment_end
        foreach_timeoff_assignment >> foreach_timeoff_assignment_end >> \
            user_import_logs_add_entry >> catch_and_log_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
