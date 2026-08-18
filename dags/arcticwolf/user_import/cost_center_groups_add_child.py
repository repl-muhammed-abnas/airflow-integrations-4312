
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.cost_center_groups_add_child_dagid,
        description=f'Arctic wolf Cost Center add child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_group,
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
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_costcenter_or_apply_modification'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_costcenter_or_apply_modification',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_costcenter_or_apply_modification = rail.RepliconServiceOperator(
            task_id='create_costcenter_or_apply_modification',
            endpoint="/services/CostCenterService1.svc/CreateCostCenterOrApplyModification",
            data={
                "costCenter": null,
                "modifications": {
                    "name": "{{ dag_run.conf.cost_center }}",
                    "codeToApply": {"value": "{{ dag_run.conf.cost_center_code }}"},
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": "{{ dag_run_ecid() }}"
            }
        )

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=lambda: 'Error' +
            rail.render_template("{{get_error_message()}}")
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error
        can_run_batch_task >> rail.Label(
            'No') >> create_costcenter_or_apply_modification
        create_costcenter_or_apply_modification >> catch_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
