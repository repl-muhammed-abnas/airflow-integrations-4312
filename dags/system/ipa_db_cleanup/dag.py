from datetime import datetime, timedelta
import json
from airflow.models import Variable
from airflow.providers.http.hooks.http import HttpHook
import logging
import rail
from system.ipa_db_cleanup import config


with rail.create_airflow_dag(
    dag_id='system_ipa_db_cleanup',
    description='IPA Database Cleanup - Automated maintenance task',
    company_key='system',
    schedule_interval='0 12 * * 6',  # run weekly on Saturdays at 12 PM UTC
    start_date=datetime(2026, 4, 27),
    catchup=False,
    tags=['ipa_db_cleanup', 'system', 'maintenance'],
    max_active_runs=config.max_active_runs,
    default_args={
        'owner': 'system',
        'depends_on_past': False,
        'execution_timeout': timedelta(hours=config.execution_timeout_hours)
    }
) as dag:

    # Tables to cleanup
    tables = [
        ("integration_runs", "created_at")
    ]

    # Define function to get configuration at runtime
    def get_cleanup_config():
        """Retrieve configuration from Airflow Variables at runtime."""
        max_db_entry_age_in_days = int(
            Variable.get(
                config.max_db_entry_age_var_name,
                config.default_max_db_entry_age_days))
        
        chunk_size = int(
            Variable.get(
                config.chunk_size_var_name,
                config.default_chunk_size
            ))
        
        admin_token = Variable.get(config.admin_token_var_name)
        
        logging.info(f"Configuration loaded: max_age={max_db_entry_age_in_days}, "
                    f"chunk_size={chunk_size}")
        
        return {
            'max_db_entry_age_in_days': max_db_entry_age_in_days,
            'chunk_size': chunk_size,
            'admin_token': admin_token
        }

    # Define preparation function
    def prepare_and_cleanup(table_name, date_column, **context):
        cfg = get_cleanup_config()

        logging.info(f"Preparing cleanup for table: {table_name}, "
                     f"max_retention_days={cfg['max_db_entry_age_in_days']}, chunk_size={cfg['chunk_size']}")

        payload = {
            'table_name': table_name,
            'date_column': date_column,
            'max_retention_days': cfg['max_db_entry_age_in_days'],
            'chunk_size': cfg['chunk_size'],
        }

        hook = HttpHook(http_conn_id=config.middleware_conn_id, method='POST')
        response = hook.run(
            endpoint=config.maintenance_api_endpoint,
            data=json.dumps(payload),
            headers={
                'Content-Type': 'application/json',
                'X-Admin-Token': cfg['admin_token'],
            },
        )

        response_data = response.json()
        if 'deleted_count' not in response_data or 'table_name' not in response_data:
            raise ValueError(f"Invalid response format: {response_data}")

        logging.info(f"Successfully cleaned {response_data.get('deleted_count', 0)} records from {table_name}")

        return {
            'table_name': table_name,
            'deleted_count': response_data.get('deleted_count', 0),
            'max_retention_days': cfg['max_db_entry_age_in_days'],
            'chunk_size': cfg['chunk_size'],
        }

    for table_name, date_column in tables:

        cleanup_integration_runs = rail.PythonOperator(
            task_id=f'cleanup_{table_name}',
            python_callable=prepare_and_cleanup,
            op_kwargs={
                'table_name': table_name,
                'date_column': date_column
            },
            retries=config.retries,
            retry_delay=timedelta(minutes=config.retry_delay_minutes)
        )
        
        # Log success
        log_cleanup_success_integration_runs = rail.WriteLogOperator(
            task_id=f'log_cleanup_success_{table_name}',
            trigger_rule='all_success',
            severity='Info',
            message=f'Successfully cleaned up table {table_name}',
            properties={
                'table_name': table_name,
                'date_column': date_column,
                'status': 'Success'
            }
        )
        
        # Catch and log errors
        catch_cleanup_error_integration_runs = rail.WriteLogOperator(
            task_id=f'catch_cleanup_error_{table_name}',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'table_name': table_name,
                'date_column': date_column,
                'status': 'Error'
            }
        )
        
        cleanup_integration_runs >> [log_cleanup_success_integration_runs, catch_cleanup_error_integration_runs]
