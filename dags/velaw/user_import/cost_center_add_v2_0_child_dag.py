
from datetime import timedelta
import uuid
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'velaw_user_import_velawg3_child_cost_center_add_v2_0_{config.instance}',
        description=f'VelawG3 Child_cost center add V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='is_costcenter_present'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='is_costcenter_present',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        is_costcenter_present = rail.IfOperator(
            task_id='is_costcenter_present',
            test='''{{ dag_run.conf.costcenter | is_truthy }}''',
            yes_task="create_cost_center_or_apply_modification_level1_3",
            no_task="log_to_sumo",
        )

        create_cost_center_or_apply_modification_level1_3 = rail.RepliconServiceOperator(
            task_id='create_cost_center_or_apply_modification_level1_3',
            endpoint="/services/CostCenterService1.svc/CreateCostCenterOrApplyModification",
            data={
                "costCenter": {
                    "name": null,
                    "uri": null,
                    "parent": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "name": "{{ dag_run.conf.costcenter }}",
                    "codeToApply": null,
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        if_request_type_present_costcenter_6 = rail.IfOperator(
            task_id='if_request_type_present_costcenter_6',
            test=lambda dag_run: bool(dag_run.conf.get('type', False)),
            yes_task="enable_8",
            no_task="log_to_sumo",
        )

        enable_8 = rail.RepliconServiceOperator(
            task_id='enable_8',
            endpoint="/services/{{ dag_run.conf.type }}Service1.svc/Enable",
            data={
                "{{ dag_run.conf.type }}Uri": "{{ dag_run.conf.uri }}"
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
            'No') >> is_costcenter_present
        is_costcenter_present >> rail.Label(
            'Yes') >> create_cost_center_or_apply_modification_level1_3 >> if_request_type_present_costcenter_6
        is_costcenter_present >> rail.Label('No') >> log_to_sumo
        if_request_type_present_costcenter_6 >> rail.Label(
            'Yes') >> enable_8 >> catch_group_error >> log_to_sumo
        if_request_type_present_costcenter_6 >> rail.Label(
            'No') >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
