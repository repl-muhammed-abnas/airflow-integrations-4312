from datetime import timedelta
from airflow.models import Variable
import rail
from dxctechnology.gsap_cost_center.utils import request_payload


# pylint: disable=too-many-statements
def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_gsap_cost_center_process_cost_centers_{config.instance}',
        description='DXC_GSAP_COST_CENTER Process Cost center',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_cost_centers,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='is_cost_center_available'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='is_cost_center_available',
            end_task='catch_and_log_errors',
        )

        is_cost_center_available = rail.IfOperator(
            task_id='is_cost_center_available',
            test=lambda dag_run: bool(dag_run.conf['costcenteruri']),
            yes_task='is_cost_center_under_psa',
            no_task='create_cost_center'
        )

        create_cost_center = rail.RepliconServiceOperator(
            task_id="create_cost_center",
            endpoint="/services/CostCenterService1.svc/CreateCostCenterOrApplyModification",
            data=request_payload.create_cost_center,
        )

        is_cost_center_under_psa = rail.IfOperator(
            task_id='is_cost_center_under_psa',
            test=lambda dag_run: dag_run.conf['currentcostcenterparent'] == 'PSA Cost Center',
            yes_task='log_completion',
            no_task='move_under_psa'
        )

        move_under_psa = rail.RepliconServiceOperator(
            task_id="move_under_psa",
            endpoint="/services/CostCenterService1.svc/MoveCostCenter",
            data=request_payload.move_under_psa,
        )

        log_completion = rail.WriteLogOperator(
            task_id='log_completion',
            message=lambda dag_run: 'Cost Center Added to PSA Heirarchy' if dag_run.conf[
                'currentcostcenterparent'] != 'PSA Cost Center' else "Cost Center already exists as part of PSA Cost Center",
            severity='Success',
            properties=lambda dag_run: {
                'costcentername': dag_run.conf['costcentername'],
                'status': 'Success',
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                'costcentername': dag_run.conf['costcentername'],
                'status': 'Error',
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> is_cost_center_available

        is_cost_center_available >> rail.Label(
            'No') >> create_cost_center >> log_completion
        is_cost_center_available >> rail.Label(
            'Yes') >> is_cost_center_under_psa >> rail.Label('Yes') >> log_completion
        is_cost_center_under_psa >> rail.Label(
            'No') >> move_under_psa >> log_completion >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag_wbs)
