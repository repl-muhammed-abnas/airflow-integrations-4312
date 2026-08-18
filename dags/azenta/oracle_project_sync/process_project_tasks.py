"""
Task-creation child DAG - build the whole Oracle WBS in Replicon Polaris in one pass.

Triggered once per project by process_project_child (with wait_for_completion) after the
managed project is upserted. Receives the project's flat Oracle task list plus the already
upserted project uri, orders every task parent-before-child (TaskLevel ascending), and
upserts them all in a single PutTask ForEach pass - parents resolved by name, so no per-level
loop and no cross-iteration uri accumulator (the design that only ever created level 1).

"""
from datetime import timedelta

import rail
from airflow.models import Variable

from azenta.oracle_project_sync.utils import custom_methods, request_payload

# pylint: disable=expression-not-assigned,pointless-statement


def create_process_project_tasks_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_project_tasks_dag_id,
        description=f'Azenta Oracle->Polaris create project tasks ({config.instance})',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id='view_dagrun_conf')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='build_task_worklist',
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='build_task_worklist',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        
        build_task_worklist = rail.PythonOperator(
            task_id='build_task_worklist',
            python_callable=lambda dag_run: custom_methods.build_ordered_task_worklist(
                dag_run.conf.get('tasks')),
        )

        has_tasks = rail.IfOperator(
            task_id='has_tasks',
            test=lambda: len(rail.result('build_task_worklist')) > 0,
            yes_task='get_task_oef_details',
            no_task='end',
        )

        # -- Task OEF definitions (uri needed to look up existing tasks by Oracle Task Id) --
        get_task_oef_details = rail.RepliconServiceOperator(
            task_id='get_task_oef_details',
            endpoint='/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails',
            data={'bindingContextUri': 'urn:replicon:object-type:task'},
        )

        # -- Per-task: lookup by Oracle Task Id OEF, then upsert, then set OEF on new tasks --
        for_each_task = rail.ForEachOperator(
            task_id='for_each_task',
            items=lambda: rail.result('build_task_worklist'),
            start_task='get_task_by_oracle_id',
            end_task='end_task_iteration',
        )

        get_task_by_oracle_id = rail.RepliconServiceOperator(
            task_id='get_task_by_oracle_id',
            endpoint='/services/TaskListService1.svc/GetData',
            data=lambda: request_payload.get_task_by_oracle_task_id_payload(rail.result('for_each_task')),

            data_handler=custom_methods.pick_task_from_list_response,
        )

        put_task = rail.RepliconServiceOperator(
            task_id='put_task',
            endpoint='/services/ProjectService1.svc/PutTask',
            data=lambda dag_run: request_payload.get_task_payload(rail.result('for_each_task'), dag_run),
        )

        is_new_task = rail.IfOperator(
            task_id='is_new_task',
            test=lambda: rail.result('get_task_by_oracle_id') is None,
            yes_task='set_task_oracle_id_oef',
            no_task='end_task_iteration',
        )

        set_task_oracle_id_oef = rail.RepliconServiceOperator(
            task_id='set_task_oracle_id_oef',
            endpoint='graphql',
            app='polaris',
            data=lambda: request_payload.put_task_oracle_id_oef_mutation(
                rail.result('for_each_task'), (rail.result('put_task') or {}).get('uri')),
        )

        end_task_iteration = rail.EmptyOperator(
            task_id='end_task_iteration',
            trigger_rule='none_failed_min_one_success',
        )

        end = rail.EmptyOperator(
            task_id='end',
            trigger_rule='none_failed_min_one_success',
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log="{{ dag_run.conf.log }}",
            severity='Error',
            message='Project task sync failed: {{ get_error_message() }}',
            properties=request_payload.error_log_properties(),
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        # -- Wiring -----------------------------------------------------------
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> build_task_worklist >> has_tasks

        has_tasks >> rail.Label('Yes') >> get_task_oef_details >> for_each_task
        has_tasks >> rail.Label('No') >> end

        for_each_task >> get_task_by_oracle_id >> put_task >> is_new_task
        is_new_task >> rail.Label('Yes') >> set_task_oracle_id_oef >> end_task_iteration
        is_new_task >> rail.Label('No') >> end_task_iteration
        for_each_task >> end_task_iteration >> end

        end >> catch_and_log_errors >> log_to_sumo

        return dag


rail.for_each_instance(create_process_project_tasks_dag)
