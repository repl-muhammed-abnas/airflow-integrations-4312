from datetime import timedelta
import rail
from airflow.models import Variable

from salesforce.project_import.utils.python_callable_method import check_if_sync_required, get_project_status_update_params


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"standard_salesforce_{config.region.replace('-', '_')}_project_import_status_sync_dag_{config.instance}",
        description=f'Salesforce {config.region} Project Import Status DAG {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='is_status_update_required'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='is_status_update_required',
            end_task='catch_project_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        is_status_update_required = rail.IfOperator(
            task_id='is_status_update_required',
            test=check_if_sync_required,
            yes_task='project_status_update_params',
            no_task='catch_project_error'
        )

        project_status_update_params = rail.PythonOperator(
            task_id='project_status_update_params',
            python_callable=get_project_status_update_params
        )

        is_polaris_project = rail.IfOperator(
            task_id='is_polaris_project',
            test="{{ dag_run.conf.is_polaris_project | is_truthy }}",
            yes_task='update_polaris_project_status',
            no_task='update_core_project_status'
        )

        update_polaris_project_status = rail.RepliconServiceOperator(
            task_id='update_polaris_project_status',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            endpoint='/graphql',
            app='polaris',
            data="{{ result('project_status_update_params') }}"
        )

        update_core_project_status = rail.RepliconServiceOperator(
            task_id='update_core_project_status',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            endpoint='/services/ProjectService1.svc/UpdateStatus',
            data="{{ result('project_status_update_params') }}"
        )

        def get_downstreamtasks_error(opportunity_name, error_message):
            return {
                'error': f'Error with {opportunity_name} - {error_message}'
            }
        catch_project_error = rail.PythonOperator(
            task_id='catch_project_error',
            trigger_rule='one_failed',
            python_callable=get_downstreamtasks_error,
            op_args=['{{ dag_run.conf.opportunity_name }}',
                     '{{ get_error_message() }}']
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> rail.Label(
                'on Error') >> catch_project_error

        can_run_batch_task >> rail.Label(
            'No') >> is_status_update_required

        is_status_update_required >> rail.Label(
            'Yes') >> project_status_update_params >> is_polaris_project
        is_status_update_required >> rail.Label(
            'No') >> catch_project_error

        is_polaris_project >> rail.Label(
            'Yes') >> update_polaris_project_status >> catch_project_error
        is_polaris_project >> rail.Label(
            'No') >> update_core_project_status >> catch_project_error

    return dag


rail.for_each_instance(create_child_dag)
