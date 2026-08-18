from zaloragroup.zendesk_task_import.utils import request_payload
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'zaloragroup_create_new_task_in_replicon_child_{config.instance}',
        description=f'ZaloraGroup New tickets in Zendesk will create new Task in Replicon {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        create_project_task=rail.RepliconServiceOperator(
            task_id='create_project_task',
            endpoint='/services/ProjectService1.svc/PutTask',
            data=request_payload.get_add_project_task_payload
        )

        get_all_project_team_members=rail.RepliconServiceOperator(
            task_id='get_all_project_team_members',
            endpoint="/services/ProjectService1.svc/GetAllProjectTeamMembers",
            data={
                "projectUri": "{{ dag_run.conf.parenturi }}"
            }
        )

        bulk_update_resource_assignments=rail.RepliconServiceOperator(
            task_id='bulk_update_resource_assignments',
            endpoint="/services/TaskService1.svc/BulkUpdateResourceAssignments",
            data=request_payload.get_bulk_resource_assignments_payload
        )

        log_create_task_successful = rail.WriteLogOperator(
            task_id='log_create_task_successful',
            severity='Skipped',
            message='Task Code already exists',
            properties= {
                "Projectname": '{{ dag_run.conf.projectname }}',
                "Ticketnumber": '{{ dag_run.conf.taskcode }}',
                "Ticketdescription": '{{ dag_run.conf.ticketdescription }}',
                "Status": 'Success',
                "Reason": 'Task Created Successfully'
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties= {
                "Projectname": '{{ dag_run.conf.projectname }}',
                "Ticketnumber": '{{ dag_run.conf.taskcode }}',
                "Ticketdescription": '{{ dag_run.conf.ticketdescription }}',
                "Status": 'Error',
                "Reason": '{{ get_error_message() }}'
            }
        )

        create_project_task >> get_all_project_team_members >> bulk_update_resource_assignments >> log_create_task_successful >> catch_and_log_errors

    return dag

rail.for_each_instance(create_dag)
