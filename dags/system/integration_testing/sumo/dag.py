"""
### System Integration Testing Sumo Operators

#### Purpose:
- This DAG tests all the operators under the <u>[rail/operators/sumo](https://github.com/replicon/replicon-airflow-library/tree/main/rail/rail/operators/sumo)</u> folder
- PWD state link: <u>https://pwd.rplcn.co/plid=2812</u>

#### Test Cases:
- Added tests for log to sumo
"""

from datetime import datetime, timedelta
import rail
from system.integration_testing import config
from system.integration_testing.sumo import python_callable_method

null = None

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/system/integration_testing/config.py


with rail.create_airflow_dag(
    dag_id="system_integration_testing_sumo_operators",
    description="System Integration Testing Sumo Operators",
    company_key=config.company_key,
    start_date=datetime(2022, 1, 1),
    max_active_runs=10,
    group="system",
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
        start_task="start",
        end_task="delete_this_dagrun",
        execution_timeout=timedelta(hours=config.execution_timeout_hours),
    )

    start = rail.EmptyOperator(task_id="start")

    log_to_sumo = rail.DagRunLogToSumoOperator(
        task_id="log_to_sumo",
        sumo_conn_id='{{dag_run.conf.sumo_conn_id}}',
    )

    failure_message = (
        "Logging to Sumo Triggered Error for run id: {{ dag_run.run_id }}"
    )
    assert_log_to_sumo = rail.PythonOperator(
        task_id="assert_log_to_sumo",
        python_callable=python_callable_method.assert_log_to_sumo,
        op_args=[
            '{{ get_task_state("log_to_sumo") }}',
            failure_message,
        ],
    )

    log_to_send_sumo = rail.SendToSumoOperator(
        task_id="log_to_send_sumo",
        data='{{dag_run.conf.employeeDetail}}',
        sumo_conn_id='{{dag_run.conf.sumo_conn_id}}'
    )
    assert_log_to_send_sumo = rail.PythonOperator(
        task_id="assert_log_to_send_sumo",
        python_callable=python_callable_method.assert_log_to_sumo,
        op_args=[
            '{{ get_task_state("log_to_send_sumo")  }}',
            failure_message,
        ],
    )

    log_dic_to_send_sumo = rail.SendToSumoOperator(
        task_id="log_dic_to_send_sumo",
        data='{{dag_run.conf.employeeDetails}}',
        sumo_conn_id='{{dag_run.conf.sumo_conn_id}}'
    )
    assert_log_dic_to_send_sumo = rail.PythonOperator(
        task_id="assert_log_dic_to_send_sumo",
        python_callable=python_callable_method.assert_log_to_sumo,
        op_args=[
            '{{ get_task_state("log_to_send_sumo")  }}',
            failure_message,
        ],
    )

    delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
        task_id="delete_this_dagrun", trigger_rule="none_failed"
    )

    batch_task_operator >> rail.Label("Test sumo data") >> start
    start >> log_to_sumo >> assert_log_to_sumo \
        >> log_to_send_sumo >> assert_log_to_send_sumo \
        >> log_dic_to_send_sumo >> assert_log_dic_to_send_sumo >> delete_this_dagrun

    batch_task_operator >> rail.Label(
        "Mark DAGRun for deletion") >> delete_this_dagrun
