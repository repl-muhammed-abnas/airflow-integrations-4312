
from datetime import timedelta, datetime
import uuid
from rail.lib.ecid import get_dagrun_ecid
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.create_projects_dag,
        description=f'Mccarthy - Creating projects in Replicon Child {config.instance} V1',
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
            no_task='if_projectname_or_taskname_not_present'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_projectname_or_taskname_not_present',
            end_task='add_failure_entries',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_projectname_or_taskname_not_present=rail.IfOperator(
            task_id='if_projectname_or_taskname_not_present',
            test=lambda dag_run: not bool(dag_run.conf['Projectname'] and
                                      dag_run.conf['Tasklist'] and
                                      dag_run.conf['Regionname'] and
                                      dag_run.conf['Tasklist'][0]['Taskname']),
            yes_task="log_fail_entry",
            no_task="get_project",
        )

        def get_error_message(dag_run):
            return ( ( ( ( ( ( null if dag_run.conf['Tasklist'][0]['Taskstartdate']
                        else "Task startdate is blank") if dag_run.conf['Tasklist'][0]['Taskcode']
                        else "Task code is blank") if dag_run.conf['Tasklist'] and dag_run.conf['Tasklist'][0]['Taskname']
                        else "Task name is blank" ) if dag_run.conf['Projectstartdate']
                        else "Project start is blank" ) if dag_run.conf['Projectcode']
                        else "Project code is blank" ) if dag_run.conf['Regionname']
                        else "Region name is blank" ) if dag_run.conf['Projectname'] else "Project Name is blank"

        log_fail_entry=rail.WriteLogOperator(
            task_id='log_fail_entry',
            log="{{ dag_run.conf.lookuptable }}",
            message="na",
            severity=lambda dag_run: "Error: " + get_error_message(dag_run),
            properties=lambda dag_run:{
                "projectname": dag_run.conf['Projectname'],
                "projectcode": dag_run.conf['Projectcode'],
                "taskname": dag_run.conf['Tasklist'][0]['Taskname'] if dag_run.conf['Tasklist'] else '',
                "taskcode": dag_run.conf['Tasklist'][0]['Taskcode'] if dag_run.conf['Tasklist'] else '',
                "jobid": dag_run.conf['JobID'],
                "status": "Error: " + get_error_message(dag_run),
                "childjobid": get_dagrun_ecid(rail.get_current_context()['dag_run'])
            }
        )

        get_project=rail.RepliconServiceOperator(
            task_id='get_project',
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data=lambda dag_run: {
              "projects": [
                {
                  "uri": null,
                  "name": dag_run.conf['Projectname'],
                  "code": null,
                  "parameterCorrelationId": null
                }
              ]
          }
        )

        if_projectdetails_uri_blank=rail.IfOperator(
            task_id='if_projectdetails_uri_blank',
            test=lambda: not bool( rail.result('get_project')[0]['projectDetails'] and
                            rail.result('get_project')[0]['projectDetails']['uri']),
            yes_task="create_project",
            no_task="add_failure_entries",
        )

        def get_date_object(date):
            date_obj = datetime.strptime(date,'%m/%d/%Y') if date else ''
            return {
                "date": {
                    "year": date_obj.year,
                    "month": date_obj.month,
                    "day": date_obj.day
                }
            } if date_obj else null

        create_project=rail.RepliconServiceOperator(
            task_id='create_project',
            retries=1,
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=lambda dag_run: {
              "modifications": {
                "nameToApply": {
                  "value": dag_run.conf['Projectname']
                },
                "codeToApply": { "value": dag_run.conf['Projectcode']} if dag_run.conf['Projectcode'] else null,
                "descriptionToApply": { "value": dag_run.conf['Projectdescription']} if dag_run.conf['Projectdescription'] else null,
                "startDateToApply": get_date_object(dag_run.conf['Projectstartdate']),
                "endDateToApply": get_date_object(dag_run.conf['Projectenddate']),
                "billingTypeToApply": {
                  "value": "urn:replicon:billing-type:time-and-material"
                },
                "clientAssignmentsSchedulesToApply": {
                  "clients": [
                    {
                      "client": {
                        "name": dag_run.conf['Regionname']
                      },
                      "costAllocationPercentage": "100"
                    }
                  ]
                },
                "statusToApply": {
                  "name": 'Completed' if dag_run.conf['Projectenddate'] else 'In Progress'
                },
                "isTimeEntryAllowed": "false",
                "timeAndMaterials": {
                  "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:non-billable"
                },
                "keyValuesToApply": [
                  {
                    "keyUri": "urn:replicon:project-key-value-key:project-team-member-assignment-type",
                    "value": {
                      "uri": "urn:replicon:project-team-member-assignment-type:automatically-assign-task"
                    }
                  }
                ]
              },
              "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
              "unitOfWorkId": str(uuid.uuid4())
            }
        )

        bulk_update_project_team_members_assignment=rail.RepliconServiceOperator(
            task_id='bulk_update_project_team_members_assignment',
            endpoint="/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment",
            data={
              "projectUri": "{{result('create_project').uri}}",
              "resourceUri": [
                "urn:replicon-tenant:"+ "{{ get_tenant_slug()}}" +":department:1"
              ],
              "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
            }
        )

        update_billing_rate_is_available_for_assignment_to_team_members=rail.RepliconServiceOperator(
            task_id='update_billing_rate_is_available_for_assignment_to_team_members',
            endpoint="/services/TimeAndMaterialsProjectService1.svc/UpdateBillingRateIsAvailableForAssignmentToTeamMembers",
            data={
              "projectUri": "{{result('create_project').uri}}",
              "billingRateUri": "urn:replicon:project-specific-billing-rate",
              "billingRateAvailableForAssignmentOptionUri": "urn:replicon:billing-rate-available-for-assignment-option:available"
            }
        )

        put_project_team_member_billing_rates_allowed_for_billing_time3=rail.RepliconServiceOperator(
            task_id='put_project_team_member_billing_rates_allowed_for_billing_time3',
            endpoint="/services/TimeAndMaterialsProjectService1.svc/PutProjectTeamMemberBillingRatesAllowedForBillingTime3",
            data={
              "projectTeamMemberBillingRate": {
                "projectUri": "{{result('create_project').uri}}",
                "resourceUri": "urn:replicon-tenant:"+ "{{get_tenant_slug()}}" +":department:1",
                "billingRateUris": [
                  "urn:replicon:project-specific-billing-rate"
                ],
                "billingRateCopyOptionUri": "urn:replicon:billing-rate-copy-option:do-not-copy-billing-rates-from-client"
              }
            }
        )

        foreach_item_in_tasklist=rail.ForEachOperator(
            task_id='foreach_item_in_tasklist',
            items=lambda dag_run: dag_run.conf['Tasklist'],
            start_task = 'create_task',
            end_task = 'foreach_item_in_tasklist_end'
        )

        create_task=rail.RepliconServiceOperator(
            task_id='create_task',
            endpoint="/services/TaskService1.svc/CreateTaskOrApplyModifications",
            data=lambda dag_run: {
              "project": {
                "uri": rail.result('create_project')['uri']
              },
              "modifications": {
                "name": rail.result('foreach_item_in_tasklist')['Taskname'],
                "codeToApply": {
                  "value": rail.result('foreach_item_in_tasklist')['Taskcode'] if rail.result('foreach_item_in_tasklist')['Taskcode'] else null
                },
                "isClosed": 'true' if rail.result('foreach_item_in_tasklist')['Taskenddate'] else 'false',
                "timeEntryStartDateToApply": get_date_object(rail.result('foreach_item_in_tasklist')['Taskstartdate']),
                "timeEntryEndDateToApply": get_date_object(rail.result('foreach_item_in_tasklist')['Taskenddate']),
                "timeAndExpenseEntryTypeToApply": {
                  "value": "urn:replicon:time-and-expense-entry-type:non-billable"
                },
                "resourceAssignmentModifications": {
                  "resourcesToAdd": [
                    {
                      "department": {
                        "uri": "urn:replicon-tenant:"+ rail.get_tenant_slug() +":department:1"
                      }
                    }
                  ]
                },
                "isTimeEntryAllowed": "true"
              },
              "unitOfWorkId": str(uuid.uuid4())
            }
        )

        add_success_log_entries=rail.WriteLogOperator(
            task_id='add_success_log_entries',
            log="{{ dag_run.conf.lookuptable }}",
            message="na",
            severity="Success",
            properties=lambda dag_run:{
                "projectname": dag_run.conf['Projectname'],
                "projectcode": dag_run.conf['Projectcode'],
                "taskname": rail.result('foreach_item_in_tasklist')['Taskname'],
                "taskcode": rail.result('foreach_item_in_tasklist')['Taskcode'],
                "jobid": dag_run.conf['JobID'],
                "status": "Success",
                "childjobid": get_dagrun_ecid(rail.get_current_context()['dag_run'])
            }
        )

        foreach_item_in_tasklist_end=rail.EmptyOperator(
            task_id='foreach_item_in_tasklist_end',
        )

        add_failure_entries=rail.WriteLogOperator(
            task_id='add_failure_entries',
            log="{{ dag_run.conf.lookuptable }}",
            trigger_rule='one_failed',
            message="na",
            severity="Error:{{get_error_message()}}",
            properties={
                "projectname": "{{dag_run.conf.Projectname}}",
                "projectcode": "{{dag_run.conf.Projectcode}}",
                "taskname": "{{ dag_run.conf.Tasklist[0].Taskname if (dag_run.conf.Tasklist | is_truthy) else ''}}",
                "taskcode": "{{ dag_run.conf.Tasklist[0].Taskcode if (dag_run.conf.Tasklist | is_truthy) else ''}}",
                "jobid": "{{dag_run.conf.JobID}}",
                "status": "Error:" + "{{get_error_message()}}",
                "childjobid": "{{dag_run_ecid()}}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                "file_name": "{{ dag_run.conf.inputfilename}}",
            }
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> add_failure_entries
        can_run_batch_task >> rail.Label('No') >> if_projectname_or_taskname_not_present
        if_projectname_or_taskname_not_present >> rail.Label('Yes')  >> log_fail_entry >> add_failure_entries
        if_projectname_or_taskname_not_present >> rail.Label('No') >> get_project >> if_projectdetails_uri_blank
        if_projectdetails_uri_blank >> rail.Label('Yes')  >> create_project >> bulk_update_project_team_members_assignment
        bulk_update_project_team_members_assignment >> update_billing_rate_is_available_for_assignment_to_team_members
        update_billing_rate_is_available_for_assignment_to_team_members >> put_project_team_member_billing_rates_allowed_for_billing_time3
        put_project_team_member_billing_rates_allowed_for_billing_time3 >> foreach_item_in_tasklist >> create_task
        create_task >> add_success_log_entries >> foreach_item_in_tasklist_end
        foreach_item_in_tasklist >> foreach_item_in_tasklist_end >> add_failure_entries
        if_projectdetails_uri_blank >> rail.Label('No') >> add_failure_entries
        add_failure_entries >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
