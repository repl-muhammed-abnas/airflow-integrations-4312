"""Processor DAG for VP -> Xero Tax Code Schedule.
"""
# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned
from datetime import timedelta
import rail
from vp_xero_integration.tax_code_schedule import config as sync_config
from vp_xero_integration.tax_code_schedule.utils.python_callable_method import (
    build_xero_tax_rates_staging,
    prepare_vp_tax_codes_staging,
    read_map_tax_code_safe,
    sync_xero_tax_codes_to_vp,
    capture_dag_error,
    COMPILE_TAX_CODES_SQL,
    XERO_TAX_COMPONENTS_COLLECTION,
    XERO_TAX_COMPONENTS_STAGING_COLUMNS,
    VP_TAX_CODES_COLLECTION,
    VP_TAX_CODES_STAGING_COLUMNS,
    TAX_CODE_MAP_COLLECTION,
    TAX_CODE_MAP_STAGING_COLUMNS,
    COMPILED_TAX_CODES_COLLECTION,
)


def create_dag(config):
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
            'execution_timeout': timedelta(days=config.execution_timeout_days),
        },
    ) as dag:

        fetch_xero_tax_rates = rail.XeroTaxRateOperator(
            task_id='fetch_xero_tax_rates',
            xero_conn_id=(
                "{{ dag_run.conf.get('connections', {}).get('xero', 'xero_default') }}"
            ),
            operation='list',
        )

        fetch_vp_tax_codes = rail.VantagepointTaxCodesOperator(
            task_id='fetch_vp_tax_codes',
            vp_conn_id=(
                "{{ dag_run.conf.get('connections', {}).get('vantagepoint', 'vantagepoint_default') }}"
            ),
            request_method='GET',
            pagination=True,
        )

        create_xero_tax_components = rail.CreateCollectionOperator(
            task_id='create_xero_tax_components',
            name=XERO_TAX_COMPONENTS_COLLECTION,
            source=build_xero_tax_rates_staging,
            columns=XERO_TAX_COMPONENTS_STAGING_COLUMNS,
        )

        create_vp_tax_codes = rail.CreateCollectionOperator(
            task_id='create_vp_tax_codes',
            name=VP_TAX_CODES_COLLECTION,
            source=prepare_vp_tax_codes_staging,
            columns=VP_TAX_CODES_STAGING_COLUMNS,
        )

        create_tax_code_map = rail.CreateCollectionOperator(
            task_id='create_tax_code_map',
            name=TAX_CODE_MAP_COLLECTION,
            source=read_map_tax_code_safe,
            columns=TAX_CODE_MAP_STAGING_COLUMNS,
        )

        query_compiled_tax_codes = rail.QueryCollectionOperator(
            task_id='query_compiled_tax_codes',
            name=COMPILED_TAX_CODES_COLLECTION,
            query=COMPILE_TAX_CODES_SQL,
        )

        process_xero_tax_codes = rail.PythonOperator(
            task_id='process_xero_tax_codes',
            python_callable=sync_xero_tax_codes_to_vp,
            op_args=[config.instance],
        )

        catch_tax_code_sync_error = rail.PythonOperator(
            task_id='catch_tax_code_sync_error',
            trigger_rule='one_failed',
            python_callable=capture_dag_error,
            op_args=[
                'tax_code_sync',
                "{{ dag_run.conf.get('customerId') or '' }}",
                '{{ get_error_message() }}',
            ],
        )

        (
            fetch_xero_tax_rates
            >> fetch_vp_tax_codes
            >> create_xero_tax_components
            >> create_vp_tax_codes
            >> create_tax_code_map
            >> query_compiled_tax_codes
            >> process_xero_tax_codes
            >> catch_tax_code_sync_error
        )

        fetch_xero_tax_rates >> catch_tax_code_sync_error
        fetch_vp_tax_codes >> catch_tax_code_sync_error
        create_xero_tax_components >> catch_tax_code_sync_error
        create_vp_tax_codes >> catch_tax_code_sync_error
        create_tax_code_map >> catch_tax_code_sync_error
        query_compiled_tax_codes >> catch_tax_code_sync_error

        return dag


rail.for_each_instance(create_dag)
