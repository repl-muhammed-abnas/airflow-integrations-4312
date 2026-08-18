
from datetime import timedelta, datetime
import uuid
from rail.lib.ecid import get_dagrun_ecid
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'mccarthy_project_import_update_projects_in_replicon_child_{config.instance}',
        description=f'Mccarthy - Update projects in Replicon Child {config.instance}',
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
            no_task='if_projectname_or_projecturi_or_taskname_not_present'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_projectname_or_projecturi_or_taskname_not_present',
            end_task='add_failure_entry',
            retry_delay_secs= 60,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_projectname_or_projecturi_or_taskname_not_present=rail.IfOperator(
            task_id='if_projectname_or_projecturi_or_taskname_not_present',
            test=lambda dag_run: not bool(dag_run.conf['Projectname'] and dag_run.conf['Taskname'] and dag_run.conf['ProjectURI'] and dag_run.conf['Regionname']),
            yes_task="log_fail_entry",
            no_task="update_project",
        )

        def get_error_message(dag_run):
            return ( ( ( ( ( ( null if dag_run.conf['Taskstartdate']
                        else "Task startdate is blank") if dag_run.conf['Taskcode']
                        else "Task code is blank") if dag_run.conf['Taskname']
                        else "Task name is blank" ) if dag_run.conf['Projectstartdate']
                        else "Project start is blank" ) if dag_run.conf['Projectcode']
                        else "Project code is blank" ) if dag_run.conf['Regionname']
                        else "Region name is blank" ) if dag_run.conf['Projectname'] else "Project Name is blank"

        log_fail_entry=rail.WriteLogOperator(
            task_id='log_fail_entry',
            log="{{ dag_run.conf.lookuptable }}",
            message="na",
            severity=lambda dag_run: "Failed while updating project: " + get_error_message(dag_run),
            properties=lambda dag_run:{
                "projectname": dag_run.conf['Projectname'],
                "projectcode": dag_run.conf['Projectcode'],
                "taskname": dag_run.conf['Taskname'],
                "taskcode": dag_run.conf['Taskcode'],
                "jobid": dag_run.conf['JobID'],
                "status": "Failed while updating project: " + get_error_message(dag_run),
                "childjobid": get_dagrun_ecid(rail.get_current_context()['dag_run'])
            }
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

        update_project=rail.RepliconServiceOperator(
            task_id='update_project',
            retries=1,
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=lambda dag_run:{
              "target": {
                "uri": dag_run.conf['ProjectURI']
              },
              "modifications": {
                "codeToApply": {
                    "value": dag_run.conf['Projectcode']
                } if dag_run.conf['Projectcode'] else null,
                "descriptionToApply": {
                    "value": dag_run.conf['Projectdescription']
                } if dag_run.conf['Projectdescription'] else null,
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
                  "name": "Completed" if dag_run.conf['Projectenddate'] else "In Progress"
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
                "projectUri": "{{dag_run.conf.ProjectURI}}",
                "resourceUri": [
                    "urn:replicon-tenant:"+ "{{get_tenant_slug()}}" +":department:1"
                ],
                "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
            }
        )

        update_billing_rate_is_available_for_assignment_to_team_members=rail.RepliconServiceOperator(
            task_id='update_billing_rate_is_available_for_assignment_to_team_members',
            endpoint="/services/TimeAndMaterialsProjectService1.svc/UpdateBillingRateIsAvailableForAssignmentToTeamMembers",
            data={
                "projectUri": "{{dag_run.conf.ProjectURI}}",
                "billingRateUri": "urn:replicon:project-specific-billing-rate",
                "billingRateAvailableForAssignmentOptionUri": "urn:replicon:billing-rate-available-for-assignment-option:available"
            }
        )

        get_project=rail.RepliconServiceOperator(
            task_id='get_project',
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data={
                "projects": [
                    {
                    "uri": null,
                    "name": "{{dag_run.conf.Projectname}}",
                    "code": null,
                    "parameterCorrelationId": null
                    }
                ]
            }
        )

        if_project_status_not_equal_to_completed=rail.IfOperator(
            task_id='if_project_status_not_equal_to_completed',
            test=lambda dag_run: bool( rail.result('get_project')[0]['projectDetails']['status']['displayText'] != 'Completed' and
                                    not dag_run.conf['Projectenddate']) ,
            yes_task="get_bulktaskdeatils",
            no_task="add_success_log_entry",
        )

        get_bulktaskdeatils=rail.RepliconServiceOperator(
            task_id='get_bulktaskdeatils',
            endpoint="/services/ProjectService1.svc/BulkGetTaskDetails2",
            data={
                "pageIndex": "1",
                "pageSize": "5000",
                "projectUris": [
                    "{{dag_run.conf.ProjectURI}}"
                ]
            }
        )

        create_or_updatetask=rail.RepliconServiceOperator(
            task_id='create_or_updatetask',
            endpoint="/services/TaskService1.svc/CreateTaskOrApplyModifications",
            data=lambda dag_run:{
                "target": {
                    "uri": rail.find_first_by_attr_and_get_attr(rail.result('get_bulktaskdeatils'),'name',dag_run.conf['Taskname'],'uri',null)
                } if rail.find_first_by_attr_and_get_attr(rail.result('get_bulktaskdeatils'),'name',dag_run.conf['Taskname'],'name','') else null,
                "project": {
                    "uri": dag_run.conf['ProjectURI']
                },
                "modifications": {
                    "name": dag_run.conf['Taskname'],
                "codeToApply": {
                    "value": dag_run.conf['Taskcode']
                } if dag_run.conf['Taskcode'] else null,
                "isClosed": "true" if dag_run.conf['Taskenddate'] else "false",
                "timeEntryStartDateToApply": get_date_object(dag_run.conf['Taskstartdate']),
                "timeEntryEndDateToApply": get_date_object(dag_run.conf['Taskenddate']),
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

        add_success_log_entry=rail.WriteLogOperator(
            task_id='add_success_log_entry',
            log="{{ dag_run.conf.lookuptable }}",
            message="na",
            severity="Success",
            properties=lambda dag_run:{
                "projectname": dag_run.conf['Projectname'],
                "projectcode": dag_run.conf['Projectcode'],
                "taskname": dag_run.conf['Taskname'],
                "taskcode": dag_run.conf['Taskcode'],
                "jobid": dag_run.conf['JobID'],
                "status": "Success",
                "childjobid": get_dagrun_ecid(rail.get_current_context()['dag_run'])
            }
        )

        add_failure_entry=rail.WriteLogOperator(
            task_id='add_failure_entry',
            log="{{ dag_run.conf.lookuptable }}",
            trigger_rule='one_failed',
            message="na",
            severity="Failed while updating project: {{ get_error_message()}}",
            properties={
                "projectname": "{{dag_run.conf.Projectname}}",
                "projectcode": "{{dag_run.conf.Projectcode}}",
                "taskname": "{{dag_run.conf.Taskname}}",
                "taskcode": "{{dag_run.conf.Taskcode}}",
                "jobid": "{{dag_run.conf.JobID}}",
                "status": "Failed while updating project:" + "{{get_error_message()}}",
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

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> add_failure_entry
        can_run_batch_task >> rail.Label('No') >> if_projectname_or_projecturi_or_taskname_not_present
        if_projectname_or_projecturi_or_taskname_not_present >> rail.Label('Yes')  >> log_fail_entry >> add_failure_entry
        if_projectname_or_projecturi_or_taskname_not_present >> rail.Label('No') >> update_project >> bulk_update_project_team_members_assignment
        bulk_update_project_team_members_assignment >> update_billing_rate_is_available_for_assignment_to_team_members
        update_billing_rate_is_available_for_assignment_to_team_members >> get_project >> if_project_status_not_equal_to_completed
        if_project_status_not_equal_to_completed >> rail.Label('Yes')  >> get_bulktaskdeatils >> create_or_updatetask >> add_success_log_entry
        if_project_status_not_equal_to_completed >> rail.Label('No') >> add_success_log_entry >> add_failure_entry
        add_failure_entry >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
