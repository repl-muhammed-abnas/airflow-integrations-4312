"""
Placeholder main DAG for the vp_quickbooks_integration `common` package.

`common` is a shared-code home (utils, config) reused across the
vp_quickbooks_integration workflow folders — it is not a real workflow. This
minimal per-instance DAG just anchors the folder in the same shape as the other
integrations (main_dag + instances + utils) so deploy/instance plumbing matches.
"""
# pylint:disable=pointless-statement,expression-not-assigned,import-error
from datetime import timedelta
import rail


def create_dag(config):
    """Create the placeholder per-instance DAG (no tasks beyond a no-op)."""
    with rail.create_airflow_dag(
        dag_id=f'vp_qbo_common_main_{config.instance}',
        description=(
            'Placeholder DAG for vp_quickbooks_integration shared code (common)'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs_master,
        tags=['vantagepoint_quickbooks', 'common', 'main'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        rail.EmptyOperator(task_id='noop')

        return dag


rail.for_each_instance(create_dag)
