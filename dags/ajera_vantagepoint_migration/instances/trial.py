"""
instances/trial.py
------------------
Trial environment configuration for the Ajera → VantagePoint migration pipeline.

Imports base config from config.py and defines trial-specific overrides:
  - Connection IDs: sftp_local (source SFTP), sql_conn, sql_conn_db, ASHV2299_SSH
  - SFTP paths for input .bak files, crosswalk Excel, CSV output, and reports
  - Windows destination backup path (G:/SQLBackups/AJindal)
  - DAG IDs for all pipeline stages
  - Webhook and Airflow base URLs for the trial Airflow environment

Note: bearer_token_variable 'Ajeera_VP_Migration_token' (intentional typo 'Ajeera')
must match exactly the Airflow Variable name set in the UI.
"""

from ajera_vantagepoint_migration.config import *


instance = "trial"
environment = 'pre-production'

company_key = "Repliconpincstream6dev"
replicon_conn_id = "Repliconpincstream6dev"

bearer_token_variable = "Ajeera_VP_Migration_token"


disabled=True
