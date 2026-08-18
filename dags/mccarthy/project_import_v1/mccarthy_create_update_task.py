
from datetime import timedelta, datetime
import uuid
from rail.lib.ecid import get_dagrun_ecid
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.create_update_task_dag,
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
            no_task='if_taskname_not_present'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_taskname_not_present',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_taskname_not_present=rail.IfOperator(
            task_id='if_taskname_not_present',
            test=lambda dag_run: not bool(dag_run.conf['Taskname']),
            yes_task="log_fail_entry",
            no_task="if_project_status_not_equal_to_completed",
        )

        def get_error_message(dag_run):
            return ( ( null if dag_run.conf['Taskstartdate']
                        else "Task startdate is blank") if dag_run.conf['Taskcode']
                        else "Task code is blank") if dag_run.conf['Taskname'] else "Task name is blank" 

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

        if_project_status_not_equal_to_completed=rail.IfOperator(
            task_id='if_project_status_not_equal_to_completed',
            test=lambda dag_run: bool( dag_run.conf['project_status'] != 'Completed' and
                                    not dag_run.conf['Projectenddate']) ,
            yes_task="create_or_updatetask",
            no_task="add_success_log_entry",
        )

        create_or_updatetask=rail.RepliconServiceOperator(
            task_id='create_or_updatetask',
            endpoint="/services/TaskService1.svc/CreateTaskOrApplyModifications",
            data=lambda dag_run:{
                "target": {
                    "uri": dag_run.conf['task_uri']
                } if dag_run.conf['task_uri'] else null,
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
        
        finish = rail.EmptyOperator(
            task_id= 'finish'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> if_taskname_not_present
        if_taskname_not_present >> rail.Label('Yes')  >> log_fail_entry >> finish
        if_taskname_not_present >> rail.Label('No') >> if_project_status_not_equal_to_completed
        if_project_status_not_equal_to_completed >> rail.Label('Yes') >> create_or_updatetask >> add_success_log_entry
        if_project_status_not_equal_to_completed >> rail.Label('No') >> add_success_log_entry >> finish

    return dag

rail.for_each_instance(create_dag)
