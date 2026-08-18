from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'broadridge_project_import_update_project_and_task_code_child_{config.instance}',
        description=f'Broadridge_project_import_update_project_and_task_code_child {config.instance}',
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        max_active_runs=config.max_active_runs_child1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_request_object_equals_to_project_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_request_object_equals_to_project_3',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_request_object_equals_to_project_3 = rail.IfOperator(
            task_id='if_request_object_equals_to_project_3',
            test='''{{ dag_run.conf.object == 'project'  and dag_run.conf.task_items.metisprojectuid | is_truthy }}''',
            yes_task="update_codeproject_4",
            no_task="if_request_object_not_equals_to_project_5",
        )

        update_codeproject_4 = rail.RepliconServiceOperator(
            task_id='update_codeproject_4',
            endpoint="/services/ProjectService1.svc/UpdateCode",
            data={
                "projectUri": "{{ dag_run.conf.task_items.projecturi }}",
                "code": "{{ dag_run.conf.task_items.metisprojectuid }}"
            }
        )

        add_success_entries_for_project = rail.WriteLogOperator(
            task_id='add_success_entries_for_project',
            log="{{ dag_run.conf.task_lookuptable }}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "projectname": dag_run.conf['task_items']['projectname'],
                "previouscode": dag_run.conf['task_items']['projectcode'],
                "newcode": dag_run.conf['task_items']['metisprojectuid'],
                "taskname": '',
                "previoustaskcode": '',
                "newtaskcode": '',
                "jobid": dag_run.conf['job_id']
            }
        )

        if_request_object_not_equals_to_project_5 = rail.IfOperator(
            task_id='if_request_object_not_equals_to_project_5',
            test='''{{ dag_run.conf.object != 'project'  and dag_run.conf.taskmetisuid | is_truthy }}''',
            yes_task="updatecode_task_6",
            no_task="finish",
        )

        updatecode_task_6 = rail.RepliconServiceOperator(
            task_id='updatecode_task_6',
            endpoint="/services/taskService1.svc/UpdateCode",
            data={
                "taskUri": "{{ dag_run.conf.taskuri }}",
                "code": "{{ dag_run.conf.taskmetisuid }}"
            }
        )

        add_success_entries_for_task = rail.WriteLogOperator(
            task_id='add_success_entries_for_task',
            log="{{ dag_run.conf.task_lookuptable }}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "projectname": dag_run.conf['task_items']['projectname'],
                "previouscode": dag_run.conf['task_items']['projectcode'],
                "newcode": dag_run.conf['task_items']['metisprojectuid'],
                "taskname": dag_run.conf['task_items']['taskname'],
                "previoustaskcode": dag_run.conf['task_items']['taskname'],
                "newtaskcode": dag_run.conf['task_items']['taskmetisuid'],
                "jobid": dag_run.conf['job_id']
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> if_request_object_equals_to_project_3
        if_request_object_equals_to_project_3
        if_request_object_equals_to_project_3 >> rail.Label(
            'Yes') >> update_codeproject_4 >> add_success_entries_for_project
        add_success_entries_for_project >> if_request_object_not_equals_to_project_5
        if_request_object_equals_to_project_3 >> rail.Label(
            'No') >> if_request_object_not_equals_to_project_5
        if_request_object_not_equals_to_project_5 >> rail.Label(
            'Yes') >> updatecode_task_6 >> add_success_entries_for_task >> finish
        if_request_object_not_equals_to_project_5 >> rail.Label(
            'No') >> finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
