# dags/vp_xero_integration_v2/employee_sync_upsert/dispatcher_dag.py
"""Dispatcher DAG for VP -> Xero Employee Sync Upsert (V2 IPA GitSync architecture).

Per-tenant: applies the polling watermark, polls VP /employee for records
modified since the last run, and triggers the per-employee processor DAG to
upsert each employee as a Xero Contact. Gathers child errors; holds the
watermark on any failure.

V2 changes from V1:
  - schedule_interval from config.schedule_interval (not None)
  - vp_conn_id from get_connections(config) (not Jinja dag_run.conf)
  - connections/customerId in build_processor_dag_conf from config (not dag_run.conf)
  - middleware_api_base_url via Jinja var.value.get (not parse-time Variable.get)
  - check_disabled_flag / skip_run removed (RAIL handles disabled=True at parse time)
  - trigger_rule='all_done' on update_last_sync_time
"""
# pylint: disable=too-many-statements,line-too-long,pointless-statement
# pylint: disable=expression-not-assigned,import-error
import logging
from datetime import timedelta
import rail
from vp_xero_integration_v2.employee_sync_upsert.config import (
    watermark_variable_key_template,
)
from vp_xero_integration_v2.common.python_callable_method import (
    get_connections,
    prepare_sync_timestamps,
    update_last_sync_time,
    has_sync_errors_method,
)
from vp_xero_integration_v2.employee_sync_upsert.utils.python_callable_method import (
    build_vp_employee_filter_method,
    extract_employee_list_method,
    check_if_employees_exist_method,
)

_log = logging.getLogger(__name__)


def create_dag(config):
    """Per-tenant dispatcher: poll VP employees, fan out, gather errors, advance watermark."""
    connections = get_connections(config)
    with rail.create_airflow_dag(
        dag_id=f'vp_xero_employee_sync_upsert_v2_dispatcher_{config.instance}',
        description=(
            'Poll VP /employee for changed records and trigger per-employee '
            'Xero Contact upsert processor'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
        dagrun_timeout=timedelta(hours=2),
        tags=['vantagepoint_xero', 'employee_sync_upsert', 'dispatcher'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        prepare_timestamps = rail.PythonOperator(
            task_id='prepare_sync_timestamps',
            python_callable=lambda: prepare_sync_timestamps(
                config.instance,
                watermark_variable_key_template,
                config.initial_sync_time,
            )
        )

        get_changed_employees_from_vp = rail.VantagepointEmployeeOperator(
            task_id='get_changed_employees_from_vp',
            vp_conn_id=connections['vantagepoint'],
            request_method='GET',
            filters=build_vp_employee_filter_method,
        )

        extract_employees = rail.PythonOperator(
            task_id='extract_employee_list',
            python_callable=extract_employee_list_method,
        )

        check_if_employees_exist = rail.IfOperator(
            task_id='check_if_employees_exist',
            test=check_if_employees_exist_method,
            yes_task='process_employees',
            no_task='log_no_employees',
        )

        log_no_employees = rail.PythonOperator(
            task_id='log_no_employees',
            python_callable=lambda: _log.info(
                'No changed VP employees in this poll window.'
            )
        )

        def build_processor_dag_conf(item):
            return {
                'Employee': item.get('Employee'),
                'ModDate': item.get('ModDate'),
                'Name': item.get('Name') or '',
                'connections': connections,
                'customerId': config.customer_id,
            }

        process_employees = rail.TriggerDagRunForEachItemOperator(
            task_id='process_employees',
            items=lambda: rail.result('extract_employee_list'),
            trigger_dag_id=(
                f'vp_xero_employee_sync_upsert_v2_processor_{config.instance}'
            ),
            conf=build_processor_dag_conf,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_processor_dag_runs = rail.WaitForDagRunsSensor(
            task_id='wait_for_processor_dag_runs',
            dag_runs="{{ result('process_employees') }}",
            allowed_states=['success', 'failed', 'upstream_failed', 'removed'],
            failed_states=[],
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        gather_processor_dag_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_processor_dag_errors',
            dag_runs="{{ result('process_employees') }}",
            dagrun_task_id='catch_processor_dag_error',
            flatten=True
        )

        has_sync_errors = rail.IfOperator(
            task_id='has_sync_errors',
            test=has_sync_errors_method,
            yes_task='fail_employee_sync_upsert',
            no_task='update_last_sync_time',
        )

        fail_employee_sync_upsert = rail.FailOperator(
            task_id='fail_employee_sync_upsert',
            message=(
                "{{ result('gather_processor_dag_errors')"
                " | map_to_attr('error') | join(' | ') }}"
            )
        )

        update_sync_time = rail.PythonOperator(
            task_id='update_last_sync_time',
            trigger_rule='all_done',
            python_callable=lambda: update_last_sync_time(
                config.instance,
                watermark_variable_key_template,
            )
        )

        post_dag_run_details = rail.PostDagRunDetailsToMiddlewareApiOperator(
            task_id='post_dag_run_details',
            middleware_api_base_url="{{ var.value.get('middleware_api_base_url', '') }}",
            trigger_rule='all_done'
        )

        prepare_timestamps >> get_changed_employees_from_vp >> extract_employees >> check_if_employees_exist

        (
            check_if_employees_exist >> rail.Label('No employees') >>
            log_no_employees >> update_sync_time
        )

        (
            check_if_employees_exist >> rail.Label('Employees found') >>
            process_employees >> wait_for_processor_dag_runs >>
            gather_processor_dag_errors >> has_sync_errors
        )

        has_sync_errors >> rail.Label('No') >> update_sync_time
        (
            has_sync_errors >> rail.Label('Yes') >>
            fail_employee_sync_upsert >> post_dag_run_details
        )

        update_sync_time >> post_dag_run_details

        return dag


rail.for_each_instance(create_dag)
