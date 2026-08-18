from datetime import timedelta
from airflow.models import Variable
import rail
from dxctechnology.gsap_iwo_resource_assignment.utils import request_payload

null = None


def create_attribute_1_process_child_wbs_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_gsab_iwo_resource_assign_child_resource_task_{config.dag_id_postfix}',
        description=f'DXC_GSAB IWO Resource Child - GSAP Child Resource Assignment {config.dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='update_date_to_parent_child_task'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='update_date_to_parent_child_task',
            end_task='catch_and_log_errors',
        )

        update_date_to_parent_child_task = rail.RepliconServiceOperator(
            task_id="update_date_to_parent_child_task",
            endpoint="/services/ResourceService1.svc/PutResourceTaskAllocationsForTask",
            data=request_payload.get_update_date_to_parent_child_task
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log='{{ dag_run.conf.log_artifact }}',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'empid': '{{ dag_run.conf.empid }}',
                'status': 'Error',
                'details': '{{ get_error_message() }}',
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            "No") >> update_date_to_parent_child_task >> catch_and_log_errors

        catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_attribute_1_process_child_wbs_dag)
