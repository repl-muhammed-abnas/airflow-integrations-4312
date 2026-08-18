
from datetime import timedelta, datetime, date as dt
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'mci_user_import_update_user_child_{config.instance}',
        description=f'MCIUSA_UserSync_Update_User_Child {config.instance}',
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
                config.can_run_batch_task_child, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='bulk_get_users'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='bulk_get_users',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        bulk_get_users=rail.RepliconServiceOperator(
            task_id='bulk_get_users',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
              "users": [
                {
                  "uri": "{{dag_run.conf.useruri}}",
                  "loginName": null,
                  "parameterCorrelationId": null
                }
              ],
              "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        if_employee_status_not_equal_active=rail.IfOperator(
            task_id='if_employee_status_not_equal_active',
            test=lambda dag_run: bool( dag_run.conf['terminationdate'] != '00/00/0000' and
                                    (rail.result('bulk_get_users')[0]['userDetails']['isEnabled']) and
                                    dag_run.conf['employeestatus'] != 'Active' ),
            yes_task="apply_user_modifications",
            no_task="create_a_mapper",
        )

        apply_user_modifications=rail.RepliconServiceOperator(
            task_id='apply_user_modifications',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
             data={
                "user": {
                  "uri": "{{dag_run.conf.useruri}}",
                  "loginName": null,
                  "parameterCorrelationId": null
                },
                "modifications": {
                  "timezoneToApply": null,
                  "userDetailsToApply": {
                    "firstName": null,
                    "lastName": null,
                    "emailAddress": null,
                    "language": null,
                    "employmentDateRange": null,
                    "employmentStartDate": null,
                    "employmentEndDate": {
                      "date": {
                          "year": "{{dag_run.conf.enddate.year}}",
                          "month": "{{dag_run.conf.enddate.month}}",
                          "day":"{{dag_run.conf.enddate.day}}"
                      }
                    },
                    "employeeId": null,
                    "displayNameParameter": null
                  },
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
              }
        )

        is_update_failed = rail.IfOperator(
            task_id="is_update_failed",
            test="{{result('apply_user_modifications').errors | is_truthy}}",
            yes_task="update_user_failed",
            no_task="disable_login"
        )

        update_user_failed = rail.FailOperator(
            task_id="update_user_failed",
            message="{{result('apply_user_modifications').errors}}"
        )

        disable_login=rail.RepliconServiceOperator(
            task_id='disable_login',
            endpoint="/services/SecurityService1.svc/DisableLogin",
            data={
              "userUri": "{{dag_run.conf.useruri}}"
            }
        )

        add_entry_user_disabled=rail.WriteLogOperator(
            task_id='add_entry_user_disabled',
            log="{{ dag_run.conf.lookuptable }}",
            message="na",
            severity="Success",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "username": "{{dag_run.conf.legalfirstname}} {{dag_run.conf.legallastname}}",
                "loginname": "{{dag_run.conf.workemail }}",
                "action": "Update",
                "status": "Success",
                "details": "User disabled successfully",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        def create_mapper(dag_run):
            details = rail.result('bulk_get_users')[0]
            userdetails = details['userDetails']
            legalfirstname = dag_run.conf['legalfirstname']
            legallastname = dag_run.conf['legallastname']
            employeestatus = dag_run.conf['employeestatus']
            worklocation = dag_run.conf['worklocation']
            day = dag_run.conf['enddate']['day']
            month = dag_run.conf['enddate']['month']
            year = dag_run.conf['enddate']['year']
            currently_assigned_supervisor_uri = details['supervisorAssignmentSchedule'][0]['supervisor']['uri']
            currently_assigned_departmenturi = details['departmentGroupSchedule'][0]['departmentGroup']['uri']
            current_location = details['locationSchedule'][0]['location']['displayText']
            date = userdetails['employmentDateRange']['endDate']
            current_date = date['day'] if date else null
            current_month = date['month'] if date else null
            current_year = date['year'] if date else null
            return {
                "Update": {
                    "firstname": ( legalfirstname if legalfirstname != userdetails['firstName'] else null)
                                  if legalfirstname else null,
                    "lastname": ( legallastname if legallastname != userdetails['lastName'] else null)
                                  if legallastname else null,
                    "displayname": ( (legalfirstname + " " + legallastname) if (legalfirstname + " " + legallastname != userdetails['displayText']) else null )
                                  if (legalfirstname + legallastname) else null,
                    "enddate": ( True  if dt(int(year), int(month), int(day)) != dt(int(current_year), int(current_month), int(current_date)) else null )
                                    if current_month else null,
                    "employeestatus": ( employeestatus if employeestatus != userdetails['isEnabled'] else null )  if employeestatus else null,
                    "supervisor": ( dag_run.conf['supervisoruri'] if dag_run.conf['supervisoruri'] != currently_assigned_supervisor_uri else null)
                                    if dag_run.conf['supervisorprimary'] else null,
                    "department": ( dag_run.conf['departmenturi'] if dag_run.conf['departmenturi'] != currently_assigned_departmenturi else null)
                                    if dag_run.conf['department'] else null,
                    "location": ( worklocation if worklocation != current_location else null) if worklocation else null
                }
            }

        create_a_mapper=rail.PythonOperator(
            task_id='create_a_mapper',
            python_callable=create_mapper
        )

        def is_change_in_user_present():
            update = rail.result('create_a_mapper')['Update']
            #pylint: disable=too-many-boolean-expressions
            if (update['firstname'] or update['lastname'] or update['displayname']) or update['supervisor'] or update['department'] or update['location']:
                return True
            return False

        if_change_present=rail.IfOperator(
            task_id='if_change_present',
            test=is_change_in_user_present,
            yes_task="applyuser_modifications",
            no_task="log_update_skipped",
        )

        applyuser_modifications=rail.RepliconServiceOperator(
            task_id='applyuser_modifications',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=lambda dag_run:{
              "user": {
                "uri": dag_run.conf['useruri'],
                "loginName": null,
                "parameterCorrelationId": null
              },
              "modifications": {
                "timezoneToApply": null,
                "workWeekStartToApply": null,
                "holidayCalendarToApply": null,
                "schedulePolicyToApply": null,
                "locationScheduleToApply": {
                    "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                    "replacementLocationSchedule": [],
                    "updateLocationScheduleOverDateRange": {
                        "replacementLocationScheduleEntries": [
                            {
                                "location": {
                                    "uri": null,
                                    "parenturi": null,
                                    "name": rail.result('create_a_mapper')['Update']['location']
                                },
                                "effectiveDate": {
                                    "year": datetime.now().strftime("%Y"),
                                    "month": datetime.now().strftime("%m"),
                                    "day": datetime.now().strftime("%d")
                                }
                            }
                        ],
                        "endDate": null
                    }
                } if rail.result('create_a_mapper')['Update']['location'] else null,
                "divisionScheduleToApply": null,
                "costCenterScheduleToApply": null,
                "departmentGroupScheduleToApply": {
                    "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                    "replacementDepartmentGroupSchedule": [],
                    "updateDepartmentGroupScheduleOverDateRange": {
                        "replacementDepartmentGroupScheduleEntries": [
                            {
                                "departmentGroup": {
                                    "uri": rail.result('create_a_mapper')['Update']['department'],
                                    "parenturi": null,
                                    "name": null,
                                    "parameterCorrelationId": null
                                },
                                "effectiveDate": {
                                    "year": datetime.now().strftime("%Y"),
                                    "month": datetime.now().strftime("%m"),
                                    "day": datetime.now().strftime("%d")
                                }
                            }
                        ],
                        "endDate": null
                    }
                } if rail.result('create_a_mapper')['Update']['department'] else null,
                "employeeTypeGroupScheduleToApply": null,
                "timesheetPeriodScheduleToApply": null,
                "serviceCenterScheduleToApply": null,
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
                "activitiesToApply": [],
                "activitiesToApply2": null,
                "defaultActivityToApply": null,
                "defaultActivityToApply2": null,
                "expenseApprovalPathToApply": null,
                "timeOffApprovalPathToApply": null,
                "productAssignmentsToApply": null,
                "timeBankPolicyToApply": null,
                "securitySettingsToApply": null,
                "supervisorsToApply": null,
                "supervisorsModifications": {
                    "scheduleEntriesToAdd": [
                        {
                            "supervisor": {
                                "uri": rail.result('create_a_mapper')['Update']['supervisor'],
                                "loginName": null,
                                "parameterCorrelationId": null
                            },
                            "effectiveDate": {
                                    "year": datetime.now().strftime("%Y"),
                                    "month": datetime.now().strftime("%m"),
                                    "day": datetime.now().strftime("%d")
                            }
                        }
                    ],
                    "scheduleEntriesToPut": []
                } if rail.result('create_a_mapper')['Update']['supervisor'] else null,
                "payrollRatesToApply": null,
                "payrollRatesModifications": null,
                "overtimeRulesToApply": null,
                "overtimeRulesModifications": null,
                "customFieldValuesToApply": [],
                "departmentToApply": null,
                "employeeTypeToApply": null,
                "userDetailsToApply": {
                  "firstName": rail.result('create_a_mapper')['Update']['firstname'] if rail.result('create_a_mapper')['Update']['firstname'] else null,
                  "lastName": rail.result('create_a_mapper')['Update']['lastname'] if rail.result('create_a_mapper')['Update']['lastname'] else null,
                  "emailAddress": null,
                  "language": null,
                  "employmentDateRange": null,
                  "employmentStartDate": null,
                  "employmentEndDate": {
                      "date": {
                          "year": dag_run.conf['enddate']['year'],
                          "month": dag_run.conf['enddate']['month'],
                          "day": dag_run.conf['enddate']['day']
                      }
                  } if rail.result('create_a_mapper')['Update']['enddate'] else null,
                  "employeeId": null,
                  "displayNameParameter": {
                    "displayName": rail.result('create_a_mapper')['Update']['displayname'] if rail.result('create_a_mapper')['Update']['displayname'] else null
                  }
                },
                "payRulesToApply": null,
                "payRulesScheduleModifications": null,
                "payRatesModifications": null,
                "placeAssignmentsModifications": null,
                "resourceAllocationAfterUserEndDateOptionUri": null,
                "projectRolesToApply": null,
                "projectRoleAssignmentSchedulesToApply": null,
                "decimalSeparatorToApply": null,
                "numberGroupSeparatorToApply": null,
                "objectExtensionFieldsToApply": []
              },
              "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        has_update_failed = rail.IfOperator(
            task_id="has_update_failed",
            test="{{result('applyuser_modifications').errors | is_truthy}}",
            yes_task="fail_update_user",
            no_task="log_user_updated"
        )

        fail_update_user = rail.FailOperator(
            task_id="fail_update_user",
            message="{{result('applyuser_modifications').errors}}"
        )

        log_user_updated=rail.WriteLogOperator(
            task_id='log_user_updated',
            log="{{ dag_run.conf.lookuptable }}",
            message="na",
            severity="Success",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "username": "{{dag_run.conf.legalfirstname}} {{dag_run.conf.legallastname}}",
                "loginname": "{{dag_run.conf.workemail }}",
                "action": "Update",
                "status": "Success",
                "details": "User updated successfully",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        log_update_skipped=rail.WriteLogOperator(
            task_id='log_update_skipped',
            log="{{ dag_run.conf.lookuptable }}",
            message="na",
            severity="Skipped",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "username": "{{dag_run.conf.legalfirstname}} {{dag_run.conf.legallastname}}",
                "loginname": "{{dag_run.conf.workemail }}",
                "action": "Update",
                "status": "Skipped",
                "details": "No data to update",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        catch_and_log_error=rail.WriteLogOperator(
            task_id='catch_and_log_error',
            log="{{ dag_run.conf.lookuptable }}",
            trigger_rule='one_failed',
            message="na",
            severity="Error",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "username": "{{dag_run.conf.legalfirstname}} {{dag_run.conf.legallastname}}",
                "loginname": "{{dag_run.conf.workemail }}",
                "action": "Update",
                "status": "Error",
                "details": "{{ get_error_message()}}",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )


        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> bulk_get_users
        bulk_get_users >> if_employee_status_not_equal_active
        if_employee_status_not_equal_active >> rail.Label('Yes')  >> apply_user_modifications >> is_update_failed >> rail.Label('No') >> disable_login
        disable_login >> add_entry_user_disabled >> catch_and_log_error
        if_employee_status_not_equal_active >> rail.Label('No') >> create_a_mapper >> if_change_present
        if_change_present >> rail.Label('Yes')  >> applyuser_modifications >> has_update_failed >> rail.Label(
            "No") >> log_user_updated >> catch_and_log_error
        has_update_failed >> rail.Label("Yes") >> fail_update_user >> catch_and_log_error
        is_update_failed >> rail.Label('Yes') >> update_user_failed >> catch_and_log_error
        if_change_present >> rail.Label('No') >> log_update_skipped >> catch_and_log_error

    return dag

rail.for_each_instance(create_dag)
