"""
Configuration helper utilities for VP UKG Pro Employee Sync v2.
Handles dynamic configuration extraction from dag_run.conf with
fallback to the per-instance module (`extras`) and static config.
"""
import logging


def extract_dynamic_config_from_dag_run(dag_run, static_config):
    """
    Extract dynamic configuration for an employee_sync run.

    Precedence (highest first):
      1. dag_run.conf top-level or nested under `config`
      2. static_config.extras (from per-customer instance file)
      3. static_config attribute fallback
    """
    dag_conf = dag_run.conf if dag_run else {}
    if not isinstance(dag_conf, dict):
        dag_conf = {}
    extras = getattr(static_config, 'extras', {}) or {}
    nested_config = dag_conf.get('config') if isinstance(dag_conf.get('config'), dict) else {}
    conf_connections = dag_conf.get('connections') if isinstance(dag_conf.get('connections'), dict) else {}

    def pick(key, default=None):
        if key in dag_conf:
            return dag_conf[key]
        if key in nested_config:
            return nested_config[key]
        if key in extras:
            return extras[key]
        return default

    dynamic_config = {
        'ukgpro_conn_id': (
            conf_connections.get('ukgpro')
            or getattr(static_config, 'ukgpro_conn_id', None)
        ),
        'vp_conn_id': (
            conf_connections.get('vantagepoint')
            or getattr(static_config, 'vantagepoint_conn_id', None)
        ),
        'customer_id': pick('customer_id') or pick('customerId'),
        'integration_id': pick('integration_id') or pick('integrationId'),
        'client_id': pick('client_id') or pick('clientId'),
        'company_id': pick('company_id') or pick('companyID'),
        'employee_id': pick('employee_id') or pick('employeeID'),
        'employee_number': pick('employee_number') or pick('employeeNumber'),
        'employee_status_code': pick('employeeStatusCode'),
        'extras': extras,
    }

    logging.info(
        "Employee v2 dynamic config: ukgpro_conn_id=%s, vp_conn_id=%s, "
        "customer_id=%s, integration_id=%s, employee_number=%s",
        dynamic_config['ukgpro_conn_id'],
        dynamic_config['vp_conn_id'],
        dynamic_config['customer_id'],
        dynamic_config['integration_id'],
        dynamic_config['employee_number'],
    )

    return dynamic_config
