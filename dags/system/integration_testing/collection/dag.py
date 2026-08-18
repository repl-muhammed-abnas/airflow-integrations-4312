"""
### System Integration Testing Collection Operators

#### Purpose:
- This DAG tests all the operators under the <u>[rail/operators/replicon](https://github.com/replicon/replicon-airflow-library/tree/main/rail/rail/operators/collections)</u> folder
- PWD state link: <u>https://pwd.rplcn.co/plid=2812</u>

#### Test Cases:
Added test for create collection and map columns
Added test for query collection
Added test for query collection filter and single-row
"""

from datetime import datetime, timedelta
import rail
from system.integration_testing import config
from system.integration_testing.collection import python_callable_method

null = None

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/system/integration_testing/config.py


with rail.create_airflow_dag(
    dag_id="system_integration_testing_collection_operators",
    description="System Integration Testing Collection Operators",
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
        "doc": __doc__
    }
) as dag:

    batch_task_operator = rail.BatchTaskRunOperator(
        task_id="batch_task_operator",
        start_task="create_collection",
        end_task="delete_this_dagrun",
        execution_timeout=timedelta(
            hours=config.execution_timeout_hours
        )
    )

    create_collection = rail.CreateCollectionOperator(
        task_id='create_collection',
        name='new_collection',
        source=lambda dag_run: dag_run.conf['records'],
        columns={
            'Id': 'Project Id',
            'Name': 'Project Name',
            'Type': 'Project Type'
        }
    )

    query_created_collection = rail.QueryCollectionOperator(
        task_id='query_created_collection',
        name='Opportunity',
        query='SELECT * FROM new_collection'
    )

    test_created_collection = rail.PythonOperator(
        task_id="test_created_collection",
        python_callable=python_callable_method.assert_collection_creation,
        op_args=[
            "collection creation data mismatch for run id: {{ dag_run_ecid() }}"]
    )

    query_filtered_collection = rail.QueryCollectionOperator(
        task_id='query_filtered_collection',
        mode='single-row',
        query='SELECT * FROM Opportunity WHERE Project_Name=:name',
        query_params={'name': 'Burlington Textiles'}
    )

    test_filtered_query = rail.PythonOperator(
        task_id='test_filtered_query',
        python_callable=python_callable_method.assert_collection_query,
        op_args=[
            "collection query data mismatch data for run id: {{ dag_run_ecid() }}"]
    )

    delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
        task_id="delete_this_dagrun",
        trigger_rule="none_failed"
    )

    (
        batch_task_operator
        >> rail.Label("Test operators")
        >> create_collection
        >> query_created_collection
        >> test_created_collection
        >> query_filtered_collection
        >> test_filtered_query
        >> delete_this_dagrun
    )
    (
        batch_task_operator
        >> rail.Label("Mark DAG run for deletion")
        >> delete_this_dagrun
    )
