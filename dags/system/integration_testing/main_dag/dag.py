"""
### System Integration Testing

#### Purpose:
- This DAG is designed for System Integration Testing
- It includes tasks for running the <u>[replicon-airflow-library](https://github.com/replicon/replicon-airflow-library)</u> operator integration tests
- The rail documentation can be found here: <u>[rail-docs](https://rail-docs.replicondev.net/)</u>
- This DAG is run on the <u>[airflow-platform](https://cd.replicondev.net/#/pipeline/airflow-platform)</u> pipeline and the asserted results are validated

#### DAG Tasks:
##### Main Task Group:
- Triggers another DAG(s) parellely based on the rail operator type that we need to perform system testing on. It is expected to wait for this to complete.
##### Validation Task:
- Based on the main task group test results, we monitor this task and accordingly either pass or fail the code pipeline.
"""


from datetime import timedelta
from airflow.exceptions import AirflowFailException
from airflow.utils.state import TaskInstanceState
from system.integration_testing import config
from system.integration_testing.main_dag.task_group import main_dag_task_group
import rail


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/system/integration_testing/config.py


with rail.create_airflow_dag(
    dag_id="system_integration_testing",
    description="System Integration Testing",
    company_key=config.company_key,
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
    },
    webhook_conf=rail.WebhookConf(
        bearer_token_var='system-test-webhook-secret')
) as dag:

    main_task_group = main_dag_task_group()

    def do_validate_system_integration_tests(dag_run):
        def get_failed_operators(dag_run):
            failed_dagrun_ids = [rail.result(
                ti.task_id, 'trigger_run_id') for ti in dag_run.get_task_instances(
                state=TaskInstanceState.FAILED)]
            return failed_dagrun_ids if failed_dagrun_ids else []
        failed_operators = get_failed_operators(dag_run)
        if failed_operators:
            error_message = f"Few System Integration test operators have failed with the following DAG Runs: \n{', '.join(failed_operators)}"
            # There is no efficient way of doing this, hence setting this result as return_value for the pipeline to poll for result!
            rail.set_result(error_message)
            rail.set_result(failed_operators, 'failed_dag_runs')
            raise AirflowFailException(error_message)
        return 'System Integrations tests have passed'
    validate_system_integration_tests = rail.PythonOperator(
        task_id='validate_system_integration_tests',
        trigger_rule='all_done',
        python_callable=do_validate_system_integration_tests
    )

    delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
        task_id='delete_this_dagrun')

    main_task_group >> rail.Label(
        'All Done') >> validate_system_integration_tests >> rail.Label(
        'Mark DAGRun for deletion') >> delete_this_dagrun
