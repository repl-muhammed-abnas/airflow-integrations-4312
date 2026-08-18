"""
Configuration helper utilities for VP UKG Pro Payroll Sync.
Handles dynamic configuration extraction and management.
"""
import logging


def extract_dynamic_config_from_dag_run(dag_run, static_config):
    """
    Extract dynamic configuration from dag_run.conf.
    
    Args:
        dag_run: Airflow DagRun object
        static_config: Static configuration object with fallback values
        
    Returns:
        dict: Dynamic configuration with connection IDs and settings
    """
    dag_conf = dag_run.conf if dag_run else {}

    # Extract config with defaults from static config
    dynamic_config = {
        'ukgpro_conn_id': dag_conf.get('ukgpro_conn_id', 'ukgpro_default_conn'),
        'vp_conn_id': dag_conf.get('vp_conn_id', 'vp_default_conn'),
        'customer_id': dag_conf.get('customer_id'),
        'clientId': dag_conf.get('clientId'),
        'integration_id': dag_conf.get('integration_id'),
        'batch_size': dag_conf.get('config', {}).get('batch_size', static_config.batch_size),
        'validate_employees': dag_conf.get('config', {}).get(
            'validate_employees_in_ukgpro',
            static_config.validate_employees_in_ukgpro
        ),
        'ukgpro_source': dag_conf.get('config', {}).get(
            'ukgpro_source',
            static_config.ukgpro_source
        ),
    }

    logging.info(
        "Dynamic config extracted: ukgpro_conn_id=%s, customer_id=%s, "
        "integration_id=%s, batch_size=%s",
        dynamic_config['ukgpro_conn_id'],
        dynamic_config['customer_id'],
        dynamic_config['integration_id'],
        dynamic_config['batch_size']
    )

    return dynamic_config
