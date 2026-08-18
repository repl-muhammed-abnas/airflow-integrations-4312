"""
D365 + PIM OAuth Token Refresh DAG.

Runs on schedule to fetch fresh access tokens for both D365
(client_credentials grant) and PIM (password grant), then stores
them in Airflow Variables so all entity sync DAGs can use them
without managing their own tokens.

Variables written:
  - ``associatedengineeringgroup_d365_access_token_<instance>``
  - ``associatedengineeringgroup_pim_access_token_<instance>``
"""
# pylint: disable=line-too-long,pointless-statement,expression-not-assigned
from datetime import timedelta

import rail
from airflow.models import Variable
from associatedengineeringgroup.deltek_pim_d365_integration.config import (
    D365_TOKEN_VAR_PREFIX,
    PIM_TOKEN_VAR_PREFIX,
)


def _store_d365_token():
    """Read the D365 token from the upstream task and persist it as a Variable."""
    ctx = rail.get_current_context()
    instance = ctx['dag_run'].dag_id.rsplit('_', 1)[-1]
    token = rail.result('fetch_d365_token')
    Variable.set(f'{D365_TOKEN_VAR_PREFIX}_{instance}', token)


def _store_pim_token():
    """Read the PIM token from the upstream task and persist it as a Variable."""
    ctx = rail.get_current_context()
    instance = ctx['dag_run'].dag_id.rsplit('_', 1)[-1]
    token = rail.result('fetch_pim_token')
    Variable.set(f'{PIM_TOKEN_VAR_PREFIX}_{instance}', token)


def create_dag(config):
    """Create the token refresh DAG for a given instance."""
    with rail.create_airflow_dag(
        dag_id=config.token_refresh_dag_id,
        description='Refresh D365 and PIM OAuth tokens',
        integration_type='generic',
        company_key=config.company_key,
        schedule_interval=config.token_refresh_schedule_interval,
        max_active_runs=1,
        tags=['pim_d365', 'token', 'auth'],
        default_args={
            'execution_timeout': timedelta(minutes=5),
            'retries': 3,
            'retry_delay': timedelta(minutes=1),
        }
    ) as dag:

        # ── D365 token (client_credentials) ──────────────────────────
        fetch_d365_token = rail.SimpleHttpOperator(
            task_id='fetch_d365_token',
            method='POST',
            http_conn_id=config.d365_auth_conn_id,
            endpoint=f"/{{{{ conn.{config.d365_auth_conn_id}.extra_dejson.d365_tenant_id }}}}/oauth2/v2.0/token",
            headers={
                'Content-Type': 'application/x-www-form-urlencoded'
            },
            data={
                'grant_type': 'client_credentials',
                'client_id': f"{{{{ conn.{config.d365_auth_conn_id}.extra_dejson.d365_client_id }}}}",
                'client_secret': f"{{{{ conn.{config.d365_auth_conn_id}.extra_dejson.d365_client_secret }}}}",
                'scope': f"{{{{ conn.{config.d365_auth_conn_id}.extra_dejson.d365_scope }}}}",
            },
            response_filter=lambda r: r.json()['access_token']
        )

        store_d365_token = rail.PythonOperator(
            task_id='store_d365_token',
            python_callable=_store_d365_token
        )

        # ── PIM token (password grant) ───────────────────────────────
        fetch_pim_token = rail.SimpleHttpOperator(
            task_id='fetch_pim_token',
            method='POST',
            http_conn_id=config.pim_conn_id,
            endpoint='/XWeb/oauth/token',
            headers={
                'Content-Type': 'application/x-www-form-urlencoded'
            },
            data={
                'grant_type': 'password',
                'username': f"{{{{ conn.{config.pim_conn_id}.extra_dejson.pim_username }}}}",
                'password': f"{{{{ conn.{config.pim_conn_id}.extra_dejson.pim_password }}}}",
                'client_id': f"{{{{ conn.{config.pim_conn_id}.extra_dejson.pim_client_id }}}}",
                'scope': 'API',
            },
            response_filter=lambda r: r.json()['access_token']
        )

        store_pim_token = rail.PythonOperator(
            task_id='store_pim_token',
            python_callable=_store_pim_token
        )

        # Both token fetches run in parallel, each followed by its store
        fetch_d365_token >> store_d365_token
        fetch_pim_token >> store_pim_token

        return dag


rail.for_each_instance(create_dag)
