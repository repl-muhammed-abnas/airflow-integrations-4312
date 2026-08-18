"""Processor DAG for VP -> Xero Employee Sync Upsert (one employee per run).
"""

# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,import-error
from datetime import timedelta
import rail
from vp_xero_integration.employee_sync_upsert import config as sync_config
from vp_xero_integration.employee_sync_upsert.utils.python_callable_method import (
    get_employee_from_map_method,
    check_vp_returned_employee_method,
    should_skip_employee_method,
    map_row_needs_xero_create_method,
    map_row_is_active_for_update_method,
    map_row_present_for_archive_method,
    build_xero_create_contact_body_method,
    build_xero_update_contact_body_method,
    build_xero_archive_contact_body_method,
    build_vp_single_employee_filter_method,
    write_map_row_after_create_method,
    refresh_map_row_after_update_method,
    mark_map_row_archived_method,
    log_result_method,
    capture_processor_error,
)

_XERO_CONN_ID = (
    "{{ dag_run.conf.get('connections', {}).get('xero', 'xero_default') }}"
)


def create_dag(config):
    """Per-employee processor DAG."""
    with rail.create_airflow_dag(
        dag_id=f'{sync_config.processor_dag_id_prefix}_{config.instance}',
        description=sync_config.processor_dag_description,
        integration_type=sync_config.integration_type,
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        tags=sync_config.processor_dag_tags,
        default_args={
            'execution_timeout': timedelta(
                days=config.execution_timeout_days
            )
        }
    ) as dag:
        # Recipe step 3: fetch this employee via the LIST endpoint
        get_single_employee_from_vp = rail.VantagepointEmployeeOperator(
            task_id='get_single_employee_from_vp',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            request_method='GET',
            filters=build_vp_single_employee_filter_method,
        )

        get_employee_from_map = rail.PythonOperator(
            task_id='get_employee_from_map',
            python_callable=get_employee_from_map_method,
        )

        check_if_vp_employee_exists = rail.IfOperator(
            task_id='check_if_vp_employee_exists',
            test=check_vp_returned_employee_method,
            yes_task='check_if_employee_should_be_synced',
            no_task='check_if_contact_exists_to_archive',
        )

        check_if_employee_should_be_synced = rail.IfOperator(
            task_id='check_if_employee_should_be_synced',
            test=should_skip_employee_method,
            yes_task='skip_terminated_or_unapproved_employee',
            no_task='check_if_xero_contact_needs_creation',
        )

        skip_terminated_or_unapproved_employee = rail.PythonOperator(
            task_id='skip_terminated_or_unapproved_employee',
            python_callable=log_result_method,
        )

        check_if_xero_contact_needs_creation = rail.IfOperator(
            task_id='check_if_xero_contact_needs_creation',
            test=map_row_needs_xero_create_method,
            yes_task='create_contact_in_xero',
            no_task='check_if_active_contact_to_update',
        )

        create_contact_in_xero = rail.XeroContactOperator(
            task_id='create_contact_in_xero',
            xero_conn_id=_XERO_CONN_ID,
            operation='create',
            request_body=build_xero_create_contact_body_method,
        )

        write_map_row_after_create = rail.PythonOperator(
            task_id='write_map_row_after_create',
            python_callable=write_map_row_after_create_method,
            trigger_rule='all_done',
        )

        check_if_active_contact_to_update = rail.IfOperator(
            task_id='check_if_active_contact_to_update',
            test=map_row_is_active_for_update_method,
            yes_task='update_contact_in_xero',
            no_task='skip_update_for_inactive_contact',
        )

        skip_update_for_inactive_contact = rail.PythonOperator(
            task_id='skip_update_for_inactive_contact',
            python_callable=log_result_method,
        )

        update_contact_in_xero = rail.XeroContactOperator(
            task_id='update_contact_in_xero',
            xero_conn_id=_XERO_CONN_ID,
            operation='update',
            request_body=build_xero_update_contact_body_method,
        )

        refresh_map_row_after_update = rail.PythonOperator(
            task_id='refresh_map_row_after_update',
            python_callable=refresh_map_row_after_update_method,
        )

        check_if_contact_exists_to_archive = rail.IfOperator(
            task_id='check_if_contact_exists_to_archive',
            test=map_row_present_for_archive_method,
            yes_task='archive_contact_in_xero',
            no_task='skip_archive_no_mapping_exists',
        )

        skip_archive_no_mapping_exists = rail.PythonOperator(
            task_id='skip_archive_no_mapping_exists',
            python_callable=log_result_method,
        )

        archive_contact_in_xero = rail.XeroContactOperator(
            task_id='archive_contact_in_xero',
            xero_conn_id=_XERO_CONN_ID,
            operation='update',
            request_body=build_xero_archive_contact_body_method,
        )

        mark_map_row_archived = rail.PythonOperator(
            task_id='mark_map_row_archived',
            python_callable=mark_map_row_archived_method,
        )

        log_result = rail.PythonOperator(
            task_id='log_result',
            python_callable=log_result_method,
            trigger_rule='none_failed_min_one_success',
        )

        catch_processor_dag_error = rail.PythonOperator(
            task_id='catch_processor_dag_error',
            trigger_rule='one_failed',
            python_callable=capture_processor_error,
            op_args=[
                '{{ dag_run.conf.Employee }}',
                "{{ dag_run.conf.get('Name') or '' }}",
                '{{ get_error_message() }}',
            ],
        )

        (
            get_single_employee_from_vp
            >> get_employee_from_map
            >> check_if_vp_employee_exists
        )

        (
            check_if_vp_employee_exists >> rail.Label('VP returned employee')
            >> check_if_employee_should_be_synced
        )
        (
            check_if_employee_should_be_synced >> rail.Label('Skip')
            >> skip_terminated_or_unapproved_employee >> log_result
        )
        (
            check_if_employee_should_be_synced >> rail.Label('Process')
            >> check_if_xero_contact_needs_creation
        )

        (
            check_if_xero_contact_needs_creation >> rail.Label('Create')
            >> create_contact_in_xero
            >> write_map_row_after_create >> log_result
        )
        (
            check_if_xero_contact_needs_creation
            >> rail.Label('Existing map row')
            >> check_if_active_contact_to_update
        )

        (
            check_if_active_contact_to_update >> rail.Label('ACTIVE')
            >> update_contact_in_xero
            >> refresh_map_row_after_update >> log_result
        )
        (
            check_if_active_contact_to_update >> rail.Label('Non-active')
            >> skip_update_for_inactive_contact >> log_result
        )

        # Deletion branch.
        (
            check_if_vp_employee_exists
            >> rail.Label('VP returned nothing')
            >> check_if_contact_exists_to_archive
        )
        (
            check_if_contact_exists_to_archive
            >> rail.Label('Map row present')
            >> archive_contact_in_xero
            >> mark_map_row_archived >> log_result
        )
        (
            check_if_contact_exists_to_archive >> rail.Label('No map row')
            >> skip_archive_no_mapping_exists >> log_result
        )

        # Error catcher -- any task failing routes here.
        get_single_employee_from_vp >> catch_processor_dag_error
        get_employee_from_map >> catch_processor_dag_error
        create_contact_in_xero >> catch_processor_dag_error
        update_contact_in_xero >> catch_processor_dag_error
        archive_contact_in_xero >> catch_processor_dag_error
        write_map_row_after_create >> catch_processor_dag_error
        refresh_map_row_after_update >> catch_processor_dag_error
        mark_map_row_archived >> catch_processor_dag_error

        return dag


rail.for_each_instance(create_dag)
