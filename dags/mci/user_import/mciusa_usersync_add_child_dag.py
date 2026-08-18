
from datetime import timedelta
import json
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'mci_user_import_add_user_child_{config.instance}',
        description=f'MCIUSA_UserSync_Add_User_Child {config.instance}',
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
            no_task='if_workemail_not_present'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_workemail_not_present',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_workemail_not_present=rail.IfOperator(
            task_id='if_workemail_not_present',
            test="{{ dag_run.conf.workemail | is_falsy}}",
            yes_task="log_email_not_present",
            no_task="create_user",
        )

        log_email_not_present=rail.WriteLogOperator(
            task_id='log_email_not_present',
            log="{{ dag_run.conf.lookuptable }}",
            message="na",
            severity="Failed",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "username": "{{dag_run.conf.legalfirstname}} {{dag_run.conf.legallastname}}",
                "loginname": "{{dag_run.conf.workemail }}",
                "action": "Add",
                "status": "Failed",
                "details": "Email address is not present",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        create_user=rail.RepliconServiceOperator(
            task_id='create_user',
            endpoint="/services/importservice1.svc/PutUser3",
            data=lambda dag_run:{
              "user": {
                "target": {
                  "uri": null,
                  "loginName": dag_run.conf['mapper']['Add']['email'],
                  "parameterCorrelationId": null
                },
                "firstname": dag_run.conf['mapper']['Add']['firstname'],
                "lastname": dag_run.conf['mapper']['Add']['lastname'],
                "emailAddress": dag_run.conf['mapper']['Add']['email'],
                "employeeId": dag_run.conf['employeecode'],
                "department": null,
                "supervisorAssignmentSchedule": {
                    "initialSupervisor": {
                        "uri": dag_run.conf['supervisoruri'],
                        "loginName": null,
                        "parameterCorrelationId": null
                    },
                    "supervisorScheduleEntries":[]
                } if dag_run.conf['supervisoruri'] else null,
                "schedulePolicySchedule": [
                  {
                    "schedulePolicy": {
                      "officeScheduleUri": null,
                      "name": dag_run.conf['mapper']['Add']['schedule'],
                      "officeSchedule": null,
                      "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                    },
                    "effectiveDate": null
                  }
                ],
                "workWeekStartDayUri": "urn:replicon:day-of-week:saturday",
                "employmentDateRange": {
                  "startDate": {
                    "year": dag_run.conf['mapper']['Add']['startdate']['year'],
                    "month": dag_run.conf['mapper']['Add']['startdate']['month'],
                    "day": dag_run.conf['mapper']['Add']['startdate']['day']
                  },
                  "endDate": {
                    "year": dag_run.conf['mapper']['Add']['enddate']['year'],
                    "month": dag_run.conf['mapper']['Add']['enddate']['month'],
                    "day": dag_run.conf['mapper']['Add']['enddate']['day']
                  } if dag_run.conf['mapper']['Add']['enddate']['day'] else null,
                  "relativeDateRangeUri": null,
                  "relativeDateRangeAsOfDate": null
                },
                "securityConfiguration": {
                  "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:sso"
                  ],
                  "isLoginEnabled": "true",
                  "loginName": dag_run.conf['mapper']['Add']['email'],
                  "SSOName": null,
                  "password": null
                },
                "holidayCalendar": {
                  "uri": null,
                  "name": "Holiday Calendar MCI USA"
                },
                "timeOffPolicy": null,
                "permissionSets": [
                  {
                    "uri": null,
                    "name": "End User with Report"
                  }
                ],
                "policySets": [
                  {
                    "uri": null,
                    "name": "Time Off"
                  },
                  {
                    "uri": null,
                    "name": dag_run.conf['mapper']['Add']['timesheettemplate']
                  }
                ],
                "employeeType": null,
                "timesheetPeriodTypeUri": null,
                "costRateSchedule": null,
                "payrollRateSchedule": null,
                "defaultBillingRate": null,
                "timesheetApprovalPath": {
                  "uri": null,
                  "name": "Supervisor"
                },
                "expenseApprovalPath": null,
                "timeOffApprovalPath": {
                  "uri": null,
                  "name": "Supervisor"
                },
                "customFieldValues": [],
                "assignedActivities": [],
                "timeZone": {
                  "uri": null,
                  "IANAName": "America/New_York"
                },
                "overtimeRuleAssignmentSchedule": null,
                "validationRuleAssignmentSchedule": null,
                "locationSchedule": [
                  {
                    "location": {
                      "uri": null,
                      "parentUri": null,
                      "name": dag_run.conf['mapper']['Add']['location']
                    },
                    "effectiveDate": null
                  }
                ] if dag_run.conf['mapper']['Add']['location'] else [],
                "divisionSchedule": [],
                "costCenterSchedule": [],
                "serviceCenterSchedule": [],
                "departmentGroupSchedule": [
                {
                    "departmentGroup": {
                      "uri": dag_run.conf['mapper']['Add']['department'],
                      "parent": null,
                      "name": null,
                      "parameterCorrelationId": null
                    },
                    "effectiveDate": null
                  }
                ] if dag_run.conf['mapper']['Add']['department'] else [],
                "employeeTypeGroupSchedule": [
                  {
                    "employeeTypeGroup": {
                      "uri": null,
                      "parent": null,
                      "name": dag_run.conf['payclass'],
                      "parameterCorrelationId": null
                    },
                    "effectiveDate": null
                  }
                ] if dag_run.conf['payclass'] else [],
                "timesheetPeriodSchedule": [
                  {
                    "timesheetPeriod": {
                      "uri": null,
                      "name": "Bi- Weekly starting on Saturday"
                    },
                    "effectiveDate": null
                  }
                ],
                "policyDataAccessScopes": [],
                "policyDataAccessScopes2": [],
                "payRuleScriptSchedule": [],
                "displayNameParameter": null,
                "decimalSeparatorUri": null,
                "numberGroupSeparatorUri": null,
                "extensionFieldValues": []
              }
            }
        )

        put_product_assignments_for_user=rail.RepliconServiceOperator(
            task_id='put_product_assignments_for_user',
            endpoint="/services/AccountManagementService1.svc/PutProductAssignmentsForUser",
            data={
              "userUri": "{{ result('create_user').uri }}",
              "productUris": [
                "urn:replicon-saas:product:time-off-plus",
                "urn:replicon-saas:product:time-bill-plus"
              ]
            }
        )

        def get_final_timeoffassignment(dag_run):
            timeoffs = (list(filter(lambda item: item['field'] == 'timeoff type' and
                      item['identifier'] == (dag_run.conf['accrualleave']).split(" ")[0],config.mapper))) if dag_run.conf['accrualleave'] else (list
                      (filter(lambda item: item['field'] == 'timeoff type' and item['identifier'] == "default",config.mapper)))
            timeoff_assignments = timeoffs[0]['value'].split("|") if timeoffs and timeoffs[0] and timeoffs[0]['value'] else []
            replicon_timeoff_list = rail.load_json_artifact(dag_run.conf['timeoff']['d'])
            return [ {
                "name": item,
                "uri": rail.find_first_by_attr_and_get_attr(replicon_timeoff_list,'name',item,'uri','')
            } for item in timeoff_assignments ]

        get_final_timeoff_assignment=rail.PythonOperator(
            task_id='get_final_timeoff_assignment',
            python_callable= get_final_timeoffassignment
        )

        has_any_timeoff_type_to_assign=rail.IfOperator(
            task_id='has_any_timeoff_type_to_assign',
            test=lambda dag_run: bool (len(rail.result('get_final_timeoff_assignment')) > 0 and dag_run.conf['accrualleave'] != '0 percent (ineligible)'),
            yes_task="put_time_off_type_assignments_for_user",
            no_task="put_timeoff_typeassignments_for_user",
        )

        put_time_off_type_assignments_for_user=rail.RepliconServiceOperator(
            task_id='put_time_off_type_assignments_for_user',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda: {
              "userUri": rail.result('create_user')['uri'],
              "timeOffTypeUris": [ item['uri'] for item in rail.result('get_final_timeoff_assignment') if item['uri']]
            }
        )

        for_each_item_in_timeoff_assignment=rail.ForEachOperator(
            task_id='for_each_item_in_timeoff_assignment',
            items="{{ result('get_final_timeoff_assignment') | to_json }}",
            start_task = 'get_default_timeoff_type_policy_schedule_for_user',
            end_task = 'for_each_item_in_timeoff_assignment_end'
        )

        get_default_timeoff_type_policy_schedule_for_user=rail.RepliconServiceOperator(
            task_id='get_default_timeoff_type_policy_schedule_for_user',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                  "userUri": "{{result('create_user').uri}}",
                  "timeOffTypeUri": "{{result('for_each_item_in_timeoff_assignment').uri}}"
                }
            }
        )

        if_has_response=rail.IfOperator(
            task_id='if_has_response',
            test=lambda: len(rail.result('get_default_timeoff_type_policy_schedule_for_user')) > 0,
            yes_task="put_user_time_off_account_policy_set_schedule",
            no_task="for_each_item_in_timeoff_assignment_end",
        )

        put_user_time_off_account_policy_set_schedule=rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda: {
              "timeOffAccount": {
                "userUri": rail.result('create_user')['uri'],
                "timeOffTypeUri": rail.result('for_each_item_in_timeoff_assignment')['uri']
              },
              "policySetScheduleEntries": json.loads(
                                            json.dumps(
                                            rail.result('get_default_timeoff_type_policy_schedule_for_user')).replace(
                                            'null','"effective"').replace('"script"','"scriptTarget"'))
                                            if rail.result('get_default_timeoff_type_policy_schedule_for_user')[0]['policySet'] else null
            }
        )

        for_each_item_in_timeoff_assignment_end=rail.EmptyOperator(
            task_id='for_each_item_in_timeoff_assignment_end',
        )

        put_timeoff_typeassignments_for_user=rail.RepliconServiceOperator(
            task_id='put_timeoff_typeassignments_for_user',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data={
              "userUri": "{{result('create_user').uri}}",
              "timeOffTypeUris": []
            }
        )

        log_user_created=rail.WriteLogOperator(
            task_id='log_user_created',
            log="{{ dag_run.conf.lookuptable }}",
            message="na",
            severity="Success",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "username": "{{dag_run.conf.legalfirstname}} {{dag_run.conf.legallastname}}",
                "loginname": "{{dag_run.conf.workemail }}",
                "action": "Add",
                "status": "Success",
                "details": "User Created Successfully",
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
                "action": "Add",
                "status": "Error",
                "details": "{{get_error_message()}}",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )


        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> if_workemail_not_present
        if_workemail_not_present >> rail.Label('Yes')  >> log_email_not_present >> catch_and_log_error
        if_workemail_not_present >> rail.Label('No') >> create_user >> put_product_assignments_for_user >> get_final_timeoff_assignment
        get_final_timeoff_assignment >> has_any_timeoff_type_to_assign
        has_any_timeoff_type_to_assign >> rail.Label('Yes')  >> put_time_off_type_assignments_for_user >> for_each_item_in_timeoff_assignment
        for_each_item_in_timeoff_assignment >> get_default_timeoff_type_policy_schedule_for_user >> if_has_response
        if_has_response >> rail.Label('Yes') >> put_user_time_off_account_policy_set_schedule >> for_each_item_in_timeoff_assignment_end
        if_has_response >> rail.Label('No') >> for_each_item_in_timeoff_assignment_end
        for_each_item_in_timeoff_assignment >> for_each_item_in_timeoff_assignment_end >> log_user_created
        has_any_timeoff_type_to_assign >> rail.Label(
            'No') >> put_timeoff_typeassignments_for_user >> log_user_created >> catch_and_log_error

    return dag

rail.for_each_instance(create_dag)
