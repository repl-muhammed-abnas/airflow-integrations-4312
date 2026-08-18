region = 'all'
environment = 'all'
# This is required for WriteCSVFileOperator
replicon_conn_id = 'airflow-replicon-admin'

TO_EMAIL_ADDR = "custom.integrations@deltek.com"
CC_EMAIL_ADDR = "dice.developers@deltek.com, DPS-dice.leads@deltek.com"
FROM_EMAIL_ADDR = "dice.alerts@deltek.com"

DISABLE_INACTIVE_DAGS_PREVIOUSLY_CHANGED_FILEPATHS_DETAILS_VAR_NAME = "airflow_disable_inactive_dags_previously_changed_filepaths_details"
DISABLE_INACTIVE_DAGS_IGNORE_INTEGRATIONS_VAR_NAME = "airflow_disable_inactive_dags_ignore_integrations_details"
GITHUB_USER_TOKEN_VAR_NAME = "airflow_disable_inactive_dags_github_user_access_token"
INACTIVE_DAGS_CHECK_DAYS_VAR_NAME = "inactive_dags_last_execution_date_in_days"

NOT_FOUND_MSG = "Not Found"
REPO_OWNER = "replicon"
REPO_NAME = "airflow-integrations"
BASE_BRANCH = "main"
NEW_CONTENT_TO_ADD = "disabled=True\n"
NEW_BRANCH_BASE_NAME = "IP2-4361_disable_inactive_dags"
PR_TITLE = "IP2-4361 | Disable Airflow inactive dags/integrations | "
COMMIT_DATE_FORMAT = "%Y-%m-%dT%H:%M:%Sz"

ORG = "replicon"
TEAM_SLUG = "dice-team"
API_BASE_URL = "https://api.github.com/repos/"
REQUEST_TIMEOUT = 20

DEFAULT_PR_BODY = """
This PR is raised by an automated process to disable inactive dags/integrations.
"""

