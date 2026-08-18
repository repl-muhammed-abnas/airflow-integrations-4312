from datetime import timedelta
from datetime import datetime
from airflow.models import Variable
from deltek_costpoint_polaris.user_sync.utils.mapper_helpers import as_name_list
import rail

# pylint:disable = too-many-statements
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_costpoint_update_user_sync_{config.instance}',
        description=f'deltek_costpoint_update_user_sync_{config.instance}',
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
            no_task='bulk_get_user'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='bulk_get_user',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def get_replicon_date(date_str):
            if not date_str:
                todays_date = datetime.now()
                return {
                    'year': todays_date.year,
                    'month': todays_date.month,
                    'day': todays_date.day
                }

            try:
                date = datetime.strptime(
                    date_str, config.costpoint_date_format)
                return {
                    'year': date.year,
                    'month': date.month,
                    'day': date.day
                }
            except:  # pylint: disable=bare-except
                return None

        def update_location(dag_run):
            location_history = []
            for location in dag_run.conf['employeehistory']:
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
            if location_history:
                return {
                    "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                    "replacementLocationSchedule": [],
                    "updateLocationScheduleOverDateRange": {
                        "replacementLocationScheduleEntries": location_history,
                        "endDate": null
                    }
                }
            return {
                "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:replace-entire-schedule",
                "replacementLocationSchedule": [],
            }

        def update_department(dag_run):
            department_history = []
            for dpt in dag_run.conf['employeehistory']:
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
            return {
                "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementDepartmentGroupSchedule": [],
                "updateDepartmentGroupScheduleOverDateRange": {
                    "replacementDepartmentGroupScheduleEntries": department_history,
                    "endDate": null
                }
            } if department_history else null

        def update_employeetype(dag_run):
            employeetype_history = []
            for emptype in dag_run.conf['employeehistory']:
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
            if employeetype_history:
                return {
                    "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                    "replacementEmployeeTypeGroupSchedule": [],
                    "updateEmployeeTypeGroupScheduleOverDateRange": {
                        "replacementEmployeeTypeGroupScheduleEntries": employeetype_history,
                        "endDate": null
                    }
                }
            return {
                "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:replace-entire-schedule",
                "replacementEmployeeTypeGroupSchedule": [],
            }

        def update_division(dag_run):
            division_history = []
            for div in dag_run.conf['employeehistory']:
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
            return {
                "userDivisionScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementDivisionSchedule": [],
                "updateDivisionScheduleOverDateRange": {
                    "replacementDivisionScheduleEntries": division_history,
                    "endDate": null
                }
            } if division_history else null

        def update_costcenter(costcenteruri, currentcostcenter, dag_run):
            if costcenteruri:
                return {
                    "userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:replace-entire-schedule",
                    "replacementCostCenterSchedule": [
                        {
                            "costCenter": {
                                "uri": costcenteruri,
                                "parentUri": null,
                                "name": null
                            },
                            "effectiveDate": null
                        }
                    ]
                } if currentcostcenter != costcenteruri else null
            return {
                "userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:replace-entire-schedule",
                "replacementCostCenterSchedule": []
            }

        def update_servicecenter(servicecenteruri, currentservicecenteruri, dag_run):
            if servicecenteruri:
                return {
                    "userServiceCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:replace-entire-schedule",
                    "replacementServiceCenterSchedule": [
                        {
                            "serviceCenter": {
                                "uri": servicecenteruri,
                                "parentUri": null,
                                "name": null
                            },
                            "effectiveDate": null
                        }
                    ],
                } if currentservicecenteruri != servicecenteruri else null
            return {
                "userServiceCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:replace-entire-schedule",
                "replacementServiceCenterSchedule": []
            }

        def update_user_details(dag_run):
            user_details = rail.result("bulk_get_user")[0]['userDetails']

            def update_first_name(dag_run):
                if user_details['firstName'] != dag_run.conf['firstname']:
                    return dag_run.conf['firstname']
                return null

            return {
                "firstName": update_first_name(dag_run),
                "lastName": dag_run.conf['lastname'] if user_details['lastName'] != dag_run.conf['lastname'] else null,
                "emailAddress": {
                    "emailAddress": dag_run.conf['emailaddress']
                } if user_details['emailAddress'] != dag_run.conf['emailaddress'] else null,
                "language": null,
                "employmentDateRange": null,
                "employmentStartDate": {
                    "date": get_replicon_date(dag_run.conf['hiredate'])
                } if user_details['employmentDateRange']['startDate'] != get_replicon_date(dag_run.conf['hiredate']) else null,
                "employmentEndDate": get_employment_end_date(user_details, dag_run.conf['employeeterminationdate']),
                "displayNameParameter": {
                    "displayName": dag_run.conf['displayname']
                } if dag_run.conf['displayname'] != user_details['customDisplayName'] else null,
            }

        def get_employment_end_date(user_details, employee_termination_date):
            endDate = null
            if employee_termination_date:
                if user_details['employmentDateRange']['endDate'] is None or\
                        user_details['employmentDateRange']['endDate'] != get_replicon_date(employee_termination_date):
                    endDate = {
                        "date": get_replicon_date(employee_termination_date)
                    }
            elif user_details['employmentDateRange']['endDate'] is not None:
                endDate = {}
            else:
                endDate = null
            return endDate

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

        def add_supervisor_history(dag_run):
            supervisor_history = []
            for usersuper in dag_run.conf['userhistory']:
                effective_date = datetime.strptime(
                    usersuper['effectivedate'], config.costpoint_date_format) if usersuper['effectivedate'] else None
                if not effective_date:
                    continue
                effective_date_dict = {
                    "year": effective_date.year,
                    "month": effective_date.month,
                    "day": effective_date.day
                }
                if usersuper.get('superuseruri'):
                    supervisor_history.append({
                        "supervisor": {
                            "uri": usersuper['superuseruri'],
                            "loginName": null,
                            "employeeId": null,
                            "parameterCorrelationId": null
                        },
                        "effectiveDate": effective_date_dict
                    })
                elif not usersuper.get('supervisor'):
                    # CP cleared the supervisor field -- emit an explicit
                    # unassignment so Replicon clears the previously synced supervisor.
                    supervisor_history.append({
                        "supervisor": null,
                        "effectiveDate": effective_date_dict
                    })
            if supervisor_history:
                return {
                    "scheduleEntriesToPut": supervisor_history
                }
            return null

        bulk_get_user = rail.RepliconServiceOperator(
            task_id='bulk_get_user',
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

                return {
                    "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:replace-entire-schedule",
                    "replacementSchedule": user_schedule_histories,
                    "updateScheduleOverDateRange": null
                }
            elif dag_run.conf['office_schedule']:
                return {
                    "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:replace-entire-schedule",
                    "replacementSchedule": [
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
                    ],
                    "updateScheduleOverDateRange": null
                }
            return null

        update_user_modifications = rail.RepliconServiceOperator(
            task_id="update_user_modifications",
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri'],
                    "loginName": null,
                    "employeeId": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": {
                        "userTimeZoneModificationOptionUri": "urn:replicon:user-time-zone-modication-option:use-specified-time-zone",
                        "timezone": {
                            "uri": dag_run.conf['timeZone'],
                            "IANAName": null
                        }
                    },
                    "workWeekStartToApply": {
                        "workWeekStartDayUri": dag_run.conf['workweek']
                    },
                    "holidayCalendarToApply": null,
                    "holidayCalendarAssignmentsToApply": null,
                    "schedulePolicyToApply": get_schedule_history(dag_run),
                    "locationScheduleToApply": update_location(dag_run),
                    "divisionScheduleToApply": update_division(dag_run),
                    "costCenterScheduleToApply": update_costcenter(dag_run.conf['costcenteruri'], "", dag_run),
                    "departmentGroupScheduleToApply": update_department(dag_run),
                    "employeeTypeGroupScheduleToApply": update_employeetype(dag_run),
                    "timesheetPeriodScheduleToApply": {
                        "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:replace-entire-schedule",
                        "replacementTimesheetPeriodSchedule": [
                            {
                                "timesheetPeriod": {
                                    "uri": dag_run.conf['timesheetperioduri'],
                                },
                                "effectiveDate": null
                            }
                        ],
                        "updateTimesheetPeriodScheduleOverDateRange": null
                    } if dag_run.conf['timesheetperioduri'] else null,
                    "serviceCenterScheduleToApply": update_servicecenter(dag_run.conf['servicecenteruri'], "", dag_run),
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
                    "activitiesToApply": [
                        {"uri": null, "name": name}
                        for name in as_name_list(dag_run.conf.get('activities'))
                    ] or null,
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "defaultTimeOffTypeForBookingsToApply": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": {
                        "loginEnabled": get_user_status(dag_run),
                    },
                    "supervisorsToApply": null,
                    "supervisorsModifications": add_supervisor_history(dag_run),
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": update_user_details(dag_run),
                    "payRulesToApply": null,
                    "payRulesScheduleModifications": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null,
                    "resourceAllocationAfterUserEndDateOptionUri": null,
                    "projectRolesToApply": null,
                    "projectRoleAssignmentSchedulesToApply": {
                        "projectRoleAssignmentSchedulesToPut": [
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
                                } if projectrole['effectivedate'] else None
                            }
                            for projectrole in dag_run.conf['userhistory']],
                        "modificationUri": "urn:replicon:schedule-modification-option:replace-entire-schedule"
                    } if dag_run.conf['userhistory'] else null,
                    "decimalSeparatorToApply": null,
                    "numberGroupSeparatorToApply": null,
                    "dateFormatToApply": null,
                    "clockFormatToApply": null,
                    "hoursFormatToApply": null,
                    "timeZoneFormatToApply": null,
                    "objectExtensionFieldsToApply": get_oefs(dag_run),
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

        def get_assigned_timeoffuris(response):
            timeoff_uris = []
            for item1 in response:
                timeoff_types = item1.get(
                    'timeOffTypeAssignmentsDetails', {}).get('timeOffTypes')
                if timeoff_types:
                    for item2 in timeoff_types:
                        timeoff_uris.append(item2['uri'])

            return timeoff_uris

        get_assigned_timeoffuri_list = rail.RepliconServiceOperator(
            task_id='get_assigned_timeoffuri_list',
            endpoint="/services/TimeOffService1.svc/BulkGetTimeOffTypeAssignmentsForUsers",
            data={
                "userUris": [
                    "{{ dag_run.conf.useruri }}"
                ]
            },
            data_handler=get_assigned_timeoffuris
        )

        def get_timeoff_uris(dag_run):
            timeoffuris = rail.result('get_assigned_timeoffuri_list')
            if dag_run.conf['timeoffassigneduris']:
                assigned_timeoffuris = list(
                    dag_run.conf['timeoffassigneduris'].split(','))
                for assigned_uri in assigned_timeoffuris:
                    if assigned_uri not in timeoffuris:
                        timeoffuris.append(assigned_uri)
            return timeoffuris

        assign_required_timeofftypes = rail.RepliconServiceOperator(
            task_id='assign_required_timeofftypes',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUris": get_timeoff_uris(dag_run)
            }
        )

        user_import_logs_add_entry = rail.WriteLogOperator(
            task_id='user_import_logs_add_entry',
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "employeeid": dag_run.conf['employeeId'],
                "action": "Update",
                "status": "Succeeded",
                "reason": "User Updated Successfully"
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            message="{{ get_error_message() }}",
            severity="Error",
            properties={
                "employeeid": "{{ dag_run.conf.employeeId }}",
                "action": "Update",
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
            'No') >> bulk_get_user >> get_super_users_permission >> \
            foreach_supervisor_assignment >> if_supervisor_assignment_present
        if_supervisor_assignment_present >> rail.Label(
            'No') >> foreach_supervisor_assignment_end
        if_supervisor_assignment_present >> rail.Label(
            'Yes') >> put_permissions_user >> foreach_supervisor_assignment_end
        foreach_supervisor_assignment >> foreach_supervisor_assignment_end >> \
            update_user_modifications >> get_assigned_timeoffuri_list >> \
            assign_required_timeofftypes >> \
            user_import_logs_add_entry >> catch_and_log_error >> log_to_sumo
        return dag


rail.for_each_instance(create_dag)
