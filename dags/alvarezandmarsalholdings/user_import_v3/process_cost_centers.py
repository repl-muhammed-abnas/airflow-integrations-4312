
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.process_cost_centers_dagid,
        description=f'{config.company_key} User Import - Process Cost Centers',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.cost_center_max_active_run_child,
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
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_add_cost_center'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_add_cost_center',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_add_cost_center = rail.IfOperator(
            task_id='if_add_cost_center',
            test=lambda dag_run: dag_run.conf['type'] == 'add',
            yes_task='create_cost_center',
            no_task='update_cost_center'
        )

        create_cost_center = rail.RepliconServiceOperator(
            task_id='create_cost_center',
            endpoint="/services/CostCenterService1.svc/CreateCostCenterOrApplyModification",
            data={
                "costCenter": null,
                "modifications": {
                    "name": "{{ dag_run.conf.name }}",
                    "codeToApply": {"value": "{{ dag_run.conf.code }}"},
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": "{{ dag_run_ecid() }}"
            }
        )

        update_cost_center = rail.RepliconServiceOperator(
            task_id='update_cost_center',
            endpoint="/services/CostCenterService1.svc/CreateCostCenterOrApplyModification",
            data={
                "costCenter":  {
                    "name": "{{ dag_run.conf.name }}"
                },
                "modifications": {
                    "name": "{{ dag_run.conf.updatedname }}",
                    "codeToApply": null,
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

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error
        can_run_batch_task >> rail.Label(
            'No') >> if_add_cost_center
        if_add_cost_center >> rail.Label(
            'Yes') >> update_cost_center >> catch_error
        if_add_cost_center >> rail.Label(
            'No') >> create_cost_center >> catch_error

    return dag


rail.for_each_instance(create_dag)
