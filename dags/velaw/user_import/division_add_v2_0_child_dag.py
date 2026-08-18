
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'velaw_user_import_velawg3_child_division_add_v2_0_{config.instance}',
        description=f'VelawG3 Child_Division add V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='is_division_present'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='is_division_present',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        is_division_present = rail.IfOperator(
            task_id='is_division_present',
            test='''{{ dag_run.conf.division | is_truthy }}''',
            yes_task="create_division_or_apply_modification_3",
            no_task="log_to_sumo",
        )

        create_division_or_apply_modification_3 = rail.RepliconServiceOperator(
            task_id='create_division_or_apply_modification_3',
            endpoint="/services/DivisionService1.svc/CreateDivisionOrApplyModification",
            data={
                "division": {
                    "name": null,
                    "uri": null,
                    "parent": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "name": "{{ dag_run.conf.division }}",
                    "codeToApply": null,
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": "{{ dag_run_ecid() }}"
            }
        )

        def get_downstreamtasks_error(error_message):
            return {
                'error': error_message
            }

        catch_group_error = rail.PythonOperator(
            task_id='catch_group_error',
            trigger_rule='one_failed',
            python_callable=get_downstreamtasks_error,
            op_args=['{{ get_error_message() }}']
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> is_division_present
        is_division_present >> rail.Label(
            'Yes') >> create_division_or_apply_modification_3 >> catch_group_error >> log_to_sumo
        is_division_present >> rail.Label('No') >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
