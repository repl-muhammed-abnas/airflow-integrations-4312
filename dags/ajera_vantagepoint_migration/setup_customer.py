"""
setup_customer.py
-----------------
One-time per-customer setup DAG triggered by the master DAG before migration work begins.

Creates the per-customer source SFTP connection (delete → recreate) and the
Windows backup directory for .bak files. See create_setup_customer_dag() docstring
for the full dag_run.conf schema.
"""

import rail
from rail.operators.connections.create_airflow_connection import CreateAirflowConnection
from rail.operators.connections.delete_airflow_connection import DeleteAirflowConnection
from airflow.providers.ssh.operators.ssh import SSHOperator
from ajera_vantagepoint_migration.utils.custom_methods import build_sftp_conn_attrs

def create_setup_customer_dag(config):
    """
    One-time customer setup DAG triggered by the master DAG before any work begins.

    Tasks:
      1. delete_sftp_input_conn — removes any existing {customer_id}_sftp_input_{instance}
         connection so all fields are fully refreshed on re-setup.
      2. create_sftp_input_conn — creates the per-customer source SFTP connection from
         UI-supplied credentials (via CreateAirflowConnection / build_sftp_conn_attrs).
      3. setup_directories — creates G:\\SQLBackups\\{customer_id} on the Windows server
         via SSH. SFTP directories are created automatically by SFTPUploadFileOperator
         (create_intermediate_dirs=True) on first file upload.

    All other connections (SQL Server, Windows SSH, output SFTP) are shared/fixed and
    used directly — no per-customer cloning required.

    Expects dag_run.conf:
    {
        "customer_id": "acme_corp",
        "instance": "trial",                        # optional, defaults to "production"
        "connection_attributes": {                  # source SFTP — supplied by UI
            "conn_type": "sftp",
            "host": "...",
            "login": "...",
            "password": "...",
            "port": 22,
            "extra": "{}"
        },
        "base_ssh_conn_id": "ASHV2299_SSH"          # optional, default shown
    }
    """

    with rail.create_airflow_dag(
        dag_id=f"ajera_vantagepoint_migration_setup_customer_{config.instance}",
        description="Create per-customer source SFTP connection and directories",
        company_key=config.company_key,
        integration_type="generic",
        max_active_runs=10,
        schedule_interval=None,
        catchup=False,
    ) as dag:

        
        delete_sftp_input_conn = DeleteAirflowConnection(
            task_id='delete_sftp_input_conn',
            conn_id="{{ dag_run.conf['customer_id'] }}_sftp_ajera_to_vp_input_{{ dag_run.conf.get('instance', 'production') }}",
        )

        create_sftp_input_conn = CreateAirflowConnection(
            task_id='create_sftp_input_conn',
            connection_attributes=build_sftp_conn_attrs,
        )


        setup_directories_task = SSHOperator(
            task_id='setup_directories',
            ssh_conn_id="ssh_ajera_to_vp_apoorv",
            command='if not exist "G:\\SQLBackups\\ajera_vantagepoint_migration\\{{ dag_run.conf["customer_id"] }}" mkdir "G:\\SQLBackups\\ajera_vantagepoint_migration\\{{ dag_run.conf["customer_id"] }}"',
        )

        delete_sftp_input_conn >> create_sftp_input_conn >> setup_directories_task

        return dag


rail.for_each_instance(create_setup_customer_dag)
