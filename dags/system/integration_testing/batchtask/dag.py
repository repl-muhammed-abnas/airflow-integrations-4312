"""
### System Integration Testing Batch TaskRun Operators

#### Purpose:
- This DAG tests all the operators under the <u>[rail/operators](https://github.com/replicon/replicon-airflow-library/tree/main/rail/rail/operators)</u> folder
- PWD state link: <u>https://pwd.rplcn.co/plid=2812</u>

#### Test Cases:
- Added tests for checking batch task
"""

from datetime import datetime, timedelta
import rail
from system.integration_testing import config
from system.integration_testing.batchtask import python_callable_method

null = None

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/system/integration_testing/config.py


with rail.create_airflow_dag(
    dag_id="system_integration_testing_batch_taskrun_operators",
    description="System Integration Testing Batch Task Run Operators",
    company_key=config.company_key,
    start_date=datetime(2022, 1, 1),
    group="system",
    max_active_runs=10,
    is_paused_upon_creation=True,
    default_args={
        "owner": "system",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "doc": __doc__,
    },
) as dag:

    rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

    batch_task_operator = rail.BatchTaskRunOperator(
        task_id="batch_task_operator",
        start_task="get_data",
        end_task="assert_success_batch_state",
        execution_timeout=timedelta(hours=config.execution_timeout_hours),
    )

    get_data = rail.PythonOperator(
        task_id='get_data',
        python_callable=lambda dag_run: dag_run.conf['test_string']
    )

    error_message = "Batch task state triggered Error for run id: {{ dag_run.run_id }}"
    assert_success_batch_state = rail.PythonOperator(
        task_id='assert_success_batch_state',
        python_callable=python_callable_method.assert_batch_state,
        op_args=["{{get_task_state('get_data')}}", error_message,
                 "{{get_task_state('batch_task_operator')}}"]
    )

    delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
        task_id="delete_this_dagrun", trigger_rule="none_failed"
    )

    batch_task_operator >> rail.Label("Test batch task") >> get_data
    get_data >> assert_success_batch_state
    assert_success_batch_state >> delete_this_dagrun

    batch_task_operator >> rail.Label(
        "Mark DAGRun for deletion") >> assert_success_batch_state
