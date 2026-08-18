from datetime import timedelta
from airflow.models import Variable
import rail
from accenture.user_sync.utils.python_callable_methods import (
    get_employee_lookup_filter,
    get_direct_deposit_filters,
    get_create_payload,
    get_update_payload,
    make_build_status_payload,
    get_error_summary,
)


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_employee_child_dag_id,
        description='Accenture Employee Sync MRDR — child',
        integration_type='generic',
        company_key=config.company_key,
        replicon_conn_id=None,
        max_active_runs=config.max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id='view_dag_run_conf')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true'
            ).lower() == 'true',
            yes_task='batch_task',
            no_task='lookup_employee',
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='lookup_employee',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        lookup_employee = rail.VantagepointEmployeeOperator(
            task_id='lookup_employee',
            vp_conn_id='{{ dag_run.conf.vantagepoint_conn_id }}',
            endpoint='/employee',
            request_method='GET',
            filters=get_employee_lookup_filter,
        )

        check_employee_exists = rail.IfOperator(
            task_id='check_employee_exists',
            test=lambda: len(rail.result('lookup_employee') or []) > 0,
            yes_task='lookup_direct_deposits',
            no_task='create_employee',
        )

        create_employee = rail.VantagepointEmployeeOperator(
            task_id='create_employee',
            vp_conn_id='{{ dag_run.conf.vantagepoint_conn_id }}',
            endpoint='/employee',
            request_method='POST',
            request_body=get_create_payload,
        )

        lookup_direct_deposits = rail.VantagepointAPIOperator(
            task_id='lookup_direct_deposits',
            vp_conn_id='{{ dag_run.conf.vantagepoint_conn_id }}',
            endpoint='{{ "/employee/" + dag_run.conf.employee + "/DirectDeposit" }}',
            request_method='GET',
            filters=get_direct_deposit_filters,
            pagination=False,
        )

        update_employee = rail.VantagepointEmployeeOperator(
            task_id='update_employee',
            vp_conn_id='{{ dag_run.conf.vantagepoint_conn_id }}',
            endpoint='{{ "/employee/" + dag_run.conf.employee + ("%7C" + dag_run.conf.CustEmployeeCompany if dag_run.conf.get("CustEmployeeCompany") else "") }}',
            request_method='PUT',
            request_body=get_update_payload,
        )

        build_row_status = rail.PythonOperator(
            task_id='build_row_status',
            trigger_rule='all_done',
            python_callable=make_build_status_payload(config.employee_integration_table),
            op_args=['{{ get_error_message() }}'],
        )

        update_row_status = rail.VantagepointHubDataTablesOperator(
            task_id='update_row_status',
            vp_conn_id='{{ dag_run.conf.vantagepoint_conn_id }}',
            request_method='POST',
            endpoint=f'/{config.vantagepoint_hub}',
            filters='?startWorkflow=N',
            request_body=lambda: rail.result('build_row_status'),
        )

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=get_error_summary,
            op_args=['{{ get_error_message() }}'],
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error
        can_run_batch_task >> rail.Label('No') >> lookup_employee

        lookup_employee >> check_employee_exists
        check_employee_exists >> rail.Label('Employee exists') >> lookup_direct_deposits >> update_employee >> build_row_status
        check_employee_exists >> rail.Label('Employee not found') >> create_employee >> build_row_status
        build_row_status >> update_row_status >> catch_error

        return dag


rail.for_each_instance(create_dag)
