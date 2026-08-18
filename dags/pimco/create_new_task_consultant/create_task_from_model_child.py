import rail
from pimco.create_new_task_consultant.utils import request_payload

# pylint: disable=too-many-statements


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"pimco_create_task_from_model_to_all_consultant_projects_child_dag_{config.instance}",
        description=f"PIMCO Create task from Model to all consultant projects child dag {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_create_tasks_child_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_task_details = rail.RepliconServiceOperator(
            task_id='get_task_details',
            endpoint='/services/ProjectService1.svc/BulkGetTaskDetails2',
            data=request_payload.get_bulk_tasks_payload,
        )

        create_task = rail.RepliconServiceOperator(
            task_id='create_task',
            endpoint='/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications',
            data=request_payload.get_add_task_payload,
        )

        is_create_task_successfull = rail.IfOperator(
            task_id='is_create_task_successfull',
            test="{{result('create_task') | filter_by_attr('error', 'does-not-equal', None)|is_falsy}}",
            yes_task='create_task_successful',
            no_task='fail_create_task'
        )

        create_task_successful = rail.EmptyOperator(
            task_id='create_task_successful'
        )

        fail_create_task = rail.FailOperator(
            task_id="fail_create_task",
            message="Create Task Unsucessfull",
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'projectname': '{{dag_run.conf.project_task_data.project_name}}',
                'runid': '{{dag_run.run_id}}',
                'status': 'Error',
            },
        )

        get_task_details >> create_task >> is_create_task_successfull
        is_create_task_successfull >> rail.Label("Yes") >> create_task_successful >> catch_and_log_errors
        is_create_task_successfull >> rail.Label("No") >> fail_create_task >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
