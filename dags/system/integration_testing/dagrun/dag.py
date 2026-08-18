"""
### System Integration Testing Dagrun Operators

#### Purpose:
- This DAG tests all the operators under the
<u>[rail/operators/dagruns](https://github.com/replicon/replicon-airflow-library/tree/main/rail/rail/operators/dagruns)</u> folder
- PWD state link: <u>https://pwd.rplcn.co/plid=2812</u>

#### Test Cases:
- Added tests for comparing view dag run configuration
- Added tests for Trigger Dag Run For Each Item
- Added tests for Wait Trigger Dag Run For Each Item
- Added test for GatherResultsFromDagRunsOperator for result with flatten=False
- Added test for GatherResultsFromDagRunsOperator for artifact with flatten=True
- Added tests for Trigger Single Dag Run
- Added tests for Wait for Single Trigger Dag Run
"""

from datetime import datetime, timedelta
import rail
from system.integration_testing import config
from system.integration_testing.dagrun import python_callable_method


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/system/integration_testing/config.py


with rail.create_airflow_dag(
    dag_id="system_integration_testing_dagrun_operators",
    description="System Integration Testing Dagrun Operators",
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
        task_id='view_dagrun_config')

    log_message = "add message for DAG Run ECID {{ dag_run_ecid() }}"

    batch_task_operator = rail.BatchTaskRunOperator(
        task_id='batch_task_operator',
        start_task='assert_dagrun_conf',
        end_task='delete_this_dagrun',
        execution_timeout=timedelta(hours=config.execution_timeout_hours)
    )

    failure_message = "Mismatched Dagrun Conf for run id: {{ dag_run.run_id }}"
    assert_dagrun_conf = rail.PythonOperator(
        task_id='assert_dagrun_conf',
        python_callable=python_callable_method.do_assert_dagrun_conf,
        op_args=[
            '{{ dag_run.conf.test_string }}',
            '{{ dag_run.conf.test_integer }}',
            failure_message
        ]
    )

    item_list = rail.PythonOperator(
        task_id='item_list',
        python_callable=lambda: [
            {
                "string": "string 1",
                "integer": 1
            },
            {
                "string": "string 2",
                "integer": 2
            }
        ]
    )

    trigger_test_child_dags = rail.TriggerDagRunForEachItemOperator(
        task_id='trigger_test_child_dags',
        retries=0,
        items='{{ result("item_list") | to_json }}',
        trigger_dag_id='system_integration_testing_dagrun_operators_child',
        execution_timeout=timedelta(hours=config.execution_timeout_hours),
        conf=lambda item: {
            "string": item['string'],
            "integer": item['integer']
        }
    )

    failure_message = "Child Dags Triggered Error for run id: {{ dag_run.run_id }}"
    assert_triggered_dagruns = rail.PythonOperator(
        task_id='assert_triggered_dagruns',
        python_callable=python_callable_method.do_assert_trigger_dagrun,
        op_args=['{{ result("trigger_test_child_dags") }}', 2, failure_message]
    )

    wait_for_trigger_test_child_dags = rail.WaitForDagRunsSensor(
        task_id='wait_for_trigger_test_child_dags',
        dag_runs="{{ result('trigger_test_child_dags') }}",
        execution_timeout=timedelta(hours=config.execution_timeout_hours)
    )

    gather_child_dag_result = rail.GatherResultsFromDagRunsOperator(
        task_id='gather_child_dag_result',
        dag_runs='{{ result("trigger_test_child_dags") }}',
        dagrun_task_id='result'
    )

    test_gather_result = rail.PythonOperator(
        task_id='test_gather_result',
        python_callable=lambda: python_callable_method.assert_gathered_result(
            [['string 1'], ['string 2']],
            rail.result('gather_child_dag_result'),
            'Gathered result data mismatch for run id: {{ dag_run_ecid() }}'
        )
    )

    gather_child_dag_result_to_artifact = rail.GatherResultsFromDagRunsOperator(
        task_id='gather_child_dag_result_to_artifact',
        dag_runs='{{ result("trigger_test_child_dags") }}',
        dagrun_task_id='result',
        target='artifact',
        flatten=True
    )

    test_gather_artifact = rail.PythonOperator(
        task_id='test_gather_artifact',
        python_callable=lambda: python_callable_method.assert_gathered_result(
            ['string 1', 'string 2'],
            rail.load_json_artifact(rail.result(
                'gather_child_dag_result_to_artifact')),
            'Gathered artifact data mismatch for run id: {{ dag_run_ecid() }}'
        )
    )

    failure_message = "Wait for Child Dags Triggered Error for run id: {{ dag_run.run_id }}"
    assert_wait_triggered_dagruns = rail.PythonOperator(
        task_id='assert_wait_triggered_dagruns',
        python_callable=python_callable_method.do_assert_wait_for_dagruns,
        op_args=[
            '{{ get_task_state("wait_for_trigger_test_child_dags")  }}', failure_message]
    )

    trigger_test_child_dag = rail.TriggerDagRunOperator(
        task_id='trigger_test_child_dag',
        retries=0,
        trigger_dag_id='system_integration_testing_dagrun_operators_child',
        execution_timeout=timedelta(hours=config.execution_timeout_hours),
        conf={
            "string": '{{ dag_run.conf.test_string }}',
            "integer": '{{ dag_run.conf.test_integer }}'
        }
    )

    failure_message = "Child Dags Triggered Error for run id: {{ dag_run.run_id }}"
    assert_triggered_dagrun = rail.PythonOperator(
        task_id='assert_triggered_dagrun',
        python_callable=python_callable_method.do_assert_trigger_dagrun,
        op_args=['{{ result("trigger_test_child_dag") }}', 1, failure_message]
    )

    wait_for_trigger_test_child_dag = rail.WaitForDagRunsSensor(
        task_id='wait_for_trigger_test_child_dag',
        dag_runs="{{ result('trigger_test_child_dag') }}",
        execution_timeout=timedelta(hours=config.execution_timeout_hours)
    )

    failure_message = "Wait for Child Dag Triggered Error for run id: {{ dag_run.run_id }}"
    assert_wait_triggered_dagrun = rail.PythonOperator(
        task_id='assert_wait_triggered_dagrun',
        python_callable=python_callable_method.do_assert_wait_for_dagruns,
        op_args=[
            '{{ get_task_state("wait_for_trigger_test_child_dags")  }}', failure_message]
    )

    delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
        task_id='delete_this_dagrun',
        trigger_rule='none_failed'
    )

    batch_task_operator >> rail.Label(
        'Comparing View Dag Run Configuration') >> assert_dagrun_conf >> item_list >> trigger_test_child_dags

    trigger_test_child_dags >> rail.Label(
        'Trigger Child Dags') >> assert_triggered_dagruns >> wait_for_trigger_test_child_dags

    wait_for_trigger_test_child_dags >> gather_child_dag_result >> test_gather_result >> \
        gather_child_dag_result_to_artifact >> test_gather_artifact >> rail.Label(
            'Wait for Triggered Child Dags') >> assert_wait_triggered_dagruns >> trigger_test_child_dag

    trigger_test_child_dag >> rail.Label(
        'Trigger Child Dag') >> assert_triggered_dagrun >> wait_for_trigger_test_child_dag

    wait_for_trigger_test_child_dag >> rail.Label(
        'Wait for Triggered Child Dag') >> assert_wait_triggered_dagrun >> delete_this_dagrun

    batch_task_operator >> rail.Label(
        'Mark DAGRun for deletion') >> delete_this_dagrun
