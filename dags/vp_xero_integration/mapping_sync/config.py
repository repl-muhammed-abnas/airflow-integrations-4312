"""
Configuration settings for VantagePoint-Xero Mapping Sync.

Top-level constants are shared defaults consumed by every instance file under
`instances/{trial,dev,qa,devops}.py`. `region` and `environment` are read at
module-import time by the production deployment tooling — removing them breaks
prod deploys, so they stay at module scope even though per-instance files
override them.

`IntegrationConfig` below holds the S3 / conn-id / DAG-id helpers used at
runtime by the DAGs themselves. Mirrors the QuickBooks
`vp_quickbooks_integration/mapping_sync/config.py`, re-keyed for Xero.
"""
# pylint: disable=invalid-name

# Production-deployment expectations (read at module import time by RAIL deploy
# tooling; mirrors the QBO mapping_sync/config.py).
region = 'us-east-1'
environment = 'pre-production'

# DAG Execution Settings (shared across all instances)
execution_timeout_days = 2
max_active_runs_master = 1
max_active_runs_child = 10

# Email Configuration (shared across all instances). tenant_email is
# per-instance and lives in each instance file because it varies by
# environment-specific Airflow Variable.
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'


class IntegrationConfig:
    """Central configuration for the VantagePoint-Xero integration"""

    # S3 Collection Operator Configuration
    # Used as the `integration` parameter for S3*CollectionOperator (forms the S3 path).
    S3_INTEGRATION_NAME = 'vp_xero_integration'
    # Fallback `customer` value when no instance company_key is available in runtime context.
    S3_DEFAULT_CUSTOMER = 'default'
    # Jinja template for the S3 `customer` field on operators whose customer
    # value can't be resolved at DAG-build time. Mirrors the Python resolver
    # `get_s3_customer` exactly: prefer customerId, then company_key, then
    # customer, then dag.dag_id.
    S3_CUSTOMER_TEMPLATE = (
        "{{ dag_run.conf.get('customerId') "
        "or dag_run.conf.get('company_key') "
        "or dag_run.conf.get('customer') "
        "or dag.dag_id }}"
    )
    # Jinja template for the S3 `integration_type` field. Threads through
    # main_dag -> dispatcher -> child DAGs via dag_run.conf['integrationType']
    # (camelCase matches what main_dag.py sets from the middleware
    # `integration_type` field). When the conf key is absent the template
    # renders to empty string, which the RAIL operators treat as None ->
    # legacy 2-segment S3 path.
    S3_INTEGRATION_TYPE_TEMPLATE = (
        "{{ dag_run.conf.get('integrationType') or '' }}"
    )

    # Connection IDs (fallback defaults used by get_conn_ids when the
    # dag_run.conf doesn't override them).
    VANTAGEPOINT_CONN_ID = 'vantagepoint_default'
    XERO_CONN_ID = 'xero_default'

    # Child DAG ID templates (formatted with the instance suffix).
    # Used by dispatcher_dag.py and main_dag.py to reference the child DAG IDs by name.
    DAG_ID_PREFIX = 'vp_xero_mapping_sync'

    # Shared Airflow Variable controlling BatchTaskRunOperator opt-out across
    # every child DAG. Defaults to 'true' inside each child's IfOperator gate
    # (immediate batch perf win on rollout); operators flip to 'false' to fall
    # back to per-task Airflow scheduling for diagnosis / canarying. Single
    # Variable, shared across all child DAGs by design.
    CAN_RUN_BATCH_VARIABLE_NAME = 'vp_xero_mapping_sync_can_run_batch'

    @classmethod
    def get_s3_customer(cls, context) -> str:
        """Resolve the `customer` parameter for S3 collection operators from runtime context.

        Order of resolution:
        1. dag_run.conf['customerId'] (set by main_dag -> dispatcher -> child DAG chain).
        2. dag_run.conf['company_key'] or dag_run.conf['customer'] (manual triggers).
        3. The dag_id (encodes the instance for per-instance DAGs).
        4. The S3_DEFAULT_CUSTOMER fallback.

        `customerId` takes precedence so every child DAG in a single customer
        population writes to the same S3 prefix regardless of which child DAG's
        dag_id we're running under.
        """
        dag_run = context.get('dag_run') if context else None
        if dag_run is not None and getattr(dag_run, 'conf', None):
            customer = (
                dag_run.conf.get('customerId')
                or dag_run.conf.get('company_key')
                or dag_run.conf.get('customer')
            )
            if customer:
                return str(customer)
        dag = context.get('dag') if context else None
        if dag is not None:
            return dag.dag_id
        return cls.S3_DEFAULT_CUSTOMER

    @classmethod
    def get_s3_integration_type(cls, context) -> str:
        """Resolve the `integration_type` parameter for S3 collection operators.

        Reads `dag_run.conf['integrationType']` (camelCase — the field name used
        by main_dag.py when triggering the dispatcher from the middleware
        integrations payload). Returns `None` (not empty string) when the key is
        missing or empty so the lib treats it as "not provided" and falls back
        to the legacy 2-segment S3 path.
        """
        dag_run = context.get('dag_run') if context else None
        if dag_run is not None and getattr(dag_run, 'conf', None):
            value = dag_run.conf.get('integrationType')
            if value:
                return str(value)
        return None

    @classmethod
    def get_cfg(cls, context, key: str, default=None):
        """Resolve a CFG_* value from the middleware integration payload.

        Reads `dag_run.conf['config'][key]` — the middleware ships per-tenant
        CFG values (e.g. `CFG_Region`, `CFG_UpgradeDataSync`) under a nested
        `config` object on the integration payload. Callers use this as the
        first-choice resolution source, falling back to Airflow Variables for
        backwards compatibility.

        Returns `default` (not empty string) when the key is missing, empty, or
        the value is falsy — so callers can chain `get_cfg(...) or
        Variable.get(...)` without spurious overrides.
        """
        dag_run = context.get('dag_run') if context else None
        if dag_run is not None and getattr(dag_run, 'conf', None):
            config = dag_run.conf.get('config') or {}
            value = config.get(key)
            if value not in (None, ''):
                return value
        return default

    @classmethod
    def get_conn_ids(cls, context) -> dict:
        """Resolve VP (target) and Xero (source) connection IDs from runtime context.

        Order: dag_run.conf.connections.{vantagepoint,xero} -> class defaults.
        Returns a dict with 'vp_conn_id' and 'xero_conn_id' keys.
        """
        vp_conn = cls.VANTAGEPOINT_CONN_ID
        xero_conn = cls.XERO_CONN_ID
        dag_run = context.get('dag_run') if context else None
        if dag_run is not None and getattr(dag_run, 'conf', None):
            connections = dag_run.conf.get('connections') or {}
            vp_conn = connections.get('vantagepoint') or vp_conn
            xero_conn = connections.get('xero') or xero_conn
        return {'vp_conn_id': vp_conn, 'xero_conn_id': xero_conn}

    @classmethod
    def dag_id(cls, dag_type: str, instance: str) -> str:
        """Build a DAG ID following the vp_xero_mapping_sync_<type>_{instance} convention."""
        return f'{cls.DAG_ID_PREFIX}_{dag_type}_{instance}'
