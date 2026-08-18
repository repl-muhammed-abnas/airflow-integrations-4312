# pylint: disable=missing-module-docstring,line-too-long,pointless-statement,expression-not-assigned,import-error
import logging
from datetime import timedelta
from pendulum import datetime as dt
import rail
from vp_ukgpro_integration_v2.payroll_sync.utils.integration_api import (
    get_oauth2_credentials,
    parse_integration_config
)
from vp_ukgpro_integration_v2.payroll_sync.utils.error_handler import (
    capture_webhook_receiver_error
)


def create_webhook_dag(config):
    dag_id = f'vp_ukgpro_payroll_sync_v2_webhook_{config.instance}'

    # Build the enriched conf passed to the processor DAG.
    def build_processor_conf(dag_run):
        dag_conf = dag_run.conf

        # Extract webhook payload from middleware structure
        if 'webhook_payload' in dag_conf:
            # New middleware webhook flow
            webhook_data = dag_conf['webhook_payload']
            logging.info("Processing webhook from middleware: webhook_id=%s, customer_id=%s",
                        dag_conf.get('webhook_id'), dag_conf.get('customer_id'))
        else:
            # Fallback: Legacy direct webhook (should not happen in production)
            webhook_data = dag_conf
            logging.warning("Received direct webhook (legacy flow) - expected middleware webhook")

        # Check if middleware already provided connection IDs and config
        if 'vp_conn_id' in dag_conf and 'ukgpro_conn_id' in dag_conf:
            # Middleware sent conn_ids directly - prefer them over API response
            integration_config = {
                'integration_id': dag_conf.get('integration_id'),
                'customer_id': dag_conf.get('customer_id'),
                'vp_conn_id': dag_conf.get('vp_conn_id'),
                'ukgpro_conn_id': dag_conf.get('ukgpro_conn_id'),
                'clientId': dag_conf.get('clientId'),
            }
            logging.info("Using connection IDs from middleware payload: vp_conn_id=%s, ukgpro_conn_id=%s",
                        dag_conf.get('vp_conn_id'), dag_conf.get('ukgpro_conn_id'))

            # Still fetch from API for validation/backup
            api_config = rail.result('parse_integration_config')
            logging.info("API backup config retrieved: vp_conn_id=%s, ukgpro_conn_id=%s",
                        api_config.get('vp_conn_id'), api_config.get('ukgpro_conn_id'))
        else:
            # Use Integration Platform API as primary source
            integration_config = rail.result('parse_integration_config')
            logging.info("Using integration config from API (no conn_ids in payload)")

        # Merge webhook data with integration config
        return {
            **integration_config,
            'webhook': {'data': webhook_data},
            'original_webhook_data': webhook_data,
            'webhook_id': dag_conf.get('webhook_id'),  # Preserve webhook metadata
            'triggered_by': dag_conf.get('triggered_by', 'webhook'),
            'customerId': dag_conf.get('customer_id'),  # For PostDagRunDetailsToMiddlewareApiOperator
        }

    webhook_conf = [rail.WebhookConf(
        query_access_token_var='vantagepoint_webhook_token',
    )]

    default_args = {
        'owner': config.instance,
        'retries': 0,
        'execution_timeout': timedelta(days=config.execution_timeout_days),
    }

    with rail.create_airflow_dag(
        dag_id=dag_id,
        description='Receives VantagePoint timesheet webhooks and triggers processing',
        integration_type='generic',
        company_key=config.company_key,
        start_date=dt(2025, 1, 1),
        max_active_runs=10,
        tags=['vantagepoint_ukgpro', 'payroll_sync', 'webhook'],
        default_args=default_args,
        catchup=False,
        webhook_conf=webhook_conf,
    ) as dag:
        # Get OAuth2 credentials
        get_oauth_credentials = rail.PythonOperator(
            task_id='get_oauth_credentials',
            python_callable=get_oauth2_credentials
        )

        # Get OAuth2 bearer token from Integration Platform API
        get_oauth_token = rail.SimpleHttpOperator(
            task_id='get_oauth_token',
            method='POST',
            http_conn_id=config.airflow_connector_ui_connid,
            endpoint='api/v1/oauth/token',
            headers={
                'Content-Type': 'application/x-www-form-urlencoded'
            },
            data=(
                "grant_type=client_credentials&"
                "client_id={{ result('get_oauth_credentials').client_id }}&"
                "client_secret={{ result('get_oauth_credentials').client_secret }}"
            )
        )

        # Fetch integration config from Integration Platform API using bearer token
        # Filter by customer_id to get the correct integration for this customer
        get_integration_config = rail.SimpleHttpOperator(
            task_id='get_integration_config',
            method='GET',
            http_conn_id=config.airflow_connector_ui_connid,
            endpoint=(
                'api/v1/integrations?'
                'customer_id={{ dag_run.conf.customer_id }}&'
                'integration_type=vp_ukgpro_payroll_sync_v2&'
                'status=enabled'
            ),
            headers={
                'Content-Type': 'application/json',
                'Authorization': (
                    "Bearer {{ (result('get_oauth_token') | from_json)['access_token'] }}"
                )
            }
        )

        # Parse integration config
        parse_config = rail.PythonOperator(
            task_id='parse_integration_config',
            python_callable=lambda: parse_integration_config(config.instance)
        )

        trigger_processing_dag = rail.TriggerDagRunOperator(
            task_id='trigger_processing_dag',
            trigger_dag_id=f'vp_ukgpro_payroll_sync_v2_processor_{config.instance}',
            conf=build_processor_conf,
        )

        # Wait for the processor DAG to complete
        wait_for_processor_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_processor_dag',
            dag_runs="{{ [result('trigger_processing_dag')] | tojson }}",
            allowed_states=['success', 'failed'],
            failed_states=['_unreachable_state_'],
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        # Gather errors from the processor DAG if any
        gather_processor_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_processor_errors',
            dag_runs="{{ [result('trigger_processing_dag')] | tojson }}",
            dagrun_task_id='log_failure',
            flatten=True
        )

        # Check if there were any processing errors
        has_processor_errors = rail.IfOperator(
            task_id='has_processor_errors',
            test="{{ result('gather_processor_errors') | length > 0 }}",
            yes_task='fail_webhook_processing',
            no_task='log_webhook_success'
        )

        # Fail if processor had errors
        fail_webhook_processing = rail.FailOperator(
            task_id='fail_webhook_processing',
            message=(
                "Payroll sync processing failed. Errors: "
                "{{ result('gather_processor_errors') | "
                "map_to_attr('reason') | join(' | ') }}"
            )
        )

        # Log success message
        def log_success():
            logging.info(
                "Webhook processing completed successfully for "
                "dag_run: %s",
                rail.result('trigger_processing_dag')
            )
            return {"status": "success"}

        log_webhook_success = rail.PythonOperator(
            task_id='log_webhook_success',
            python_callable=log_success
        )

        # Post DAG run details to Middleware API
        post_to_middleware = rail.PostDagRunDetailsToMiddlewareApiOperator(
            task_id='post_to_middleware',
            middleware_api_base_url=(
                "{{ var.value.get('middleware_api_base_url', '') }}"
            ),
            trigger_rule='all_done'
        )

        catch_webhook_receiver_error = rail.PythonOperator(
            task_id='catch_webhook_receiver_error',
            trigger_rule='all_done',
            python_callable=capture_webhook_receiver_error
        )

        # Task dependencies
        # Always fetch from Integration API for validation and backup
        (
            get_oauth_credentials >>
            get_oauth_token >>
            get_integration_config >>
            parse_config >>
            trigger_processing_dag
        )

        # After triggering processor, wait and check results
        (
            trigger_processing_dag >>
            wait_for_processor_dag >>
            gather_processor_errors >>
            has_processor_errors
        )

        (
            has_processor_errors >>
            rail.Label('Errors found') >>
            fail_webhook_processing >>
            post_to_middleware
        )

        (
            has_processor_errors >>
            rail.Label('No errors') >>
            log_webhook_success >>
            post_to_middleware
        )

        post_to_middleware >> catch_webhook_receiver_error

    return dag


rail.for_each_instance(create_webhook_dag)
