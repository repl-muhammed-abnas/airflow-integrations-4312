
from datetime import timedelta, datetime
import uuid
from rail.lib.ecid import get_dagrun_ecid
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.update_projects_dag,
        description=f'Mccarthy - Update projects in Replicon Child {config.instance} V1',
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
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_projectname_or_projecturi_or_taskname_not_present=rail.IfOperator(
            task_id='if_projectname_or_projecturi_or_taskname_not_present',
            test=lambda dag_run: not bool(dag_run.conf['Projectname'] and dag_run.conf['ProjectURI'] and dag_run.conf['Regionname']),
            yes_task="log_fail_entry",
            no_task="update_project",
        )

        def get_error_message(dag_run):
            return ( ( ( null if dag_run.conf['Projectstartdate']
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
                "taskname": '',
                "taskcode": '',
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

        query_existing_projects_details=rail.QueryCollectionOperator(
            task_id='query_existing_projects_details',
            query="""Select RegionName,ProjectName,ProjectCode,ProjectDescription,ProjectStartDate,
                   ProjectEndDate,TaskName,TaskCode,TaskStartDate,TaskEndDate from 
                    inputfile where ProjectName =  '{{dag_run.conf.Projectname}}'""",
        )

        def get_payload_update_child(dag_run, item):
            return  {
                'Regionname': item['RegionName'],
                'Projectname': item['ProjectName'],
                'Projectcode': item['ProjectCode'],
                'Projectdescription': item['ProjectDescription'],
                'Projectstartdate': item['ProjectStartDate'],
                'Projectenddate': item['ProjectEndDate'],
                'Taskname': item['TaskName'],
                'Taskcode':item['TaskCode'],
                'Taskstartdate': item['TaskStartDate'],
                'Taskenddate': item['TaskEndDate'],
                'JobID': dag_run.conf['JobID'],
                'ProjectURI': dag_run.conf['ProjectURI'],
                'Projectstatus': dag_run.conf['Projectstatus'],
                'lookuptable': dag_run.conf['lookuptable'],
                'inputfilename': dag_run.conf['inputfilename'],
                'project_status': rail.result('get_project')[0]['projectDetails']['status']['displayText'],
                'task_uri': rail.find_first_by_attr_and_get_attr(rail.result('get_bulktaskdeatils'),'name',item['TaskName'],'uri',null)
            }
        
        trigger_create_update_task_child=rail.TriggerDagRunForEachItemOperator(
            task_id = 'trigger_create_update_task_child',
            retries = 0,
            items= "{{result('query_existing_projects_details')}}",
            trigger_dag_id=config.create_update_task_dag,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run, item: get_payload_update_child(dag_run, item)
        )

        wait_for_create_update_task_child=rail.WaitForDagRunsSensor(
            task_id='wait_for_create_update_task_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('trigger_create_update_task_child')}}"
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
                "taskname": "",
                "taskcode": "",
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
        update_billing_rate_is_available_for_assignment_to_team_members >> get_project >> get_bulktaskdeatils
        get_bulktaskdeatils >> query_existing_projects_details >> trigger_create_update_task_child
        trigger_create_update_task_child >> wait_for_create_update_task_child >> add_failure_entry
        add_failure_entry >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
