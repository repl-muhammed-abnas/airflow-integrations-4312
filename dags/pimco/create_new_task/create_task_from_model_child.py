import itertools
import rail
from pimco.create_new_task.utils import request_payload
from pimco.create_new_task.utils import custom_methods

# pylint: disable=too-many-statements


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"pimco_create_task_from_model_to_all_projects_child_dag_{config.instance}",
        description=f"PIMCO Create task from Model to all projects child dag {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_create_tasks_child_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        project_task_data = rail.PythonOperator(
            task_id='project_task_data',
            python_callable=custom_methods.get_project_task_data
        )

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
            yes_task='log_task_creation_success',
            no_task='log_task_creation_errors'
        )

        log_task_creation_success = rail.WriteLogOperator(
            task_id='log_task_creation_success',
            severity='Success',
            message="Task created successfully",
            properties={
                'projectname': '{{dag_run.conf.project_name}}',
                'runid': '{{dag_run.run_id}}',
                'status': 'Success',
            }
        )

        log_task_creation_errors = rail.WriteLogOperator(
            task_id='log_task_creation_errors',
            severity='Error',
            message=lambda: ", ".join(list(itertools.chain.from_iterable(list(map(lambda rows:
                list(map(lambda errors: errors["displayText"], rows["error"]["notifications"])), rail.result("create_task")))))),
            properties={
                'projectname': '{{dag_run.conf.project_name}}',
                'runid': '{{dag_run.run_id}}',
                'status': 'Error',
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'projectname': '{{dag_run.conf.project_name}}',
                'runid': '{{dag_run.run_id}}',
                'status': 'Error',
            },
        )

        project_task_data >> get_task_details >> create_task >> is_create_task_successfull
        is_create_task_successfull >> rail.Label("Yes") >> log_task_creation_success >> catch_and_log_errors
        is_create_task_successfull >> rail.Label("No") >> log_task_creation_errors >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
