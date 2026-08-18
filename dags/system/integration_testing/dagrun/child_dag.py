from datetime import datetime, timedelta
import rail
from system.integration_testing import config


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/system/integration_testing/config.py


with rail.create_airflow_dag(
    dag_id="system_integration_testing_dagrun_operators_child",
    description="System Integration Testing Dagrun Operators Child",
    company_key=config.company_key,
    start_date=datetime(2022, 1, 1),
    group='system',
    max_active_runs=10,
    is_paused_upon_creation=True,
    default_args={
        'owner': 'system',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
        'doc': __doc__
    }
) as dag:

    rail.ViewDagRunConfOperator(
        task_id="view_dagrun_config"
    )

    result = rail.PythonOperator(
        task_id='result',
        python_callable=lambda dag_run: [dag_run.conf['string']]
    )

    result
