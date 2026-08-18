"""
### System Integration Testing Business Replicon Report Details Operators

#### Purpose:
- This DAG tests all the operators under the <u>[rail/operators](https://github.com/replicon/replicon-airflow-library/tree/main/rail/rail/operators)</u> folder
- PWD state link: <u>https://pwd.rplcn.co/plid=2812</u>

#### Test Cases:
- Added tests for report_name
"""

from datetime import datetime, timedelta
import rail
from system.integration_testing import config
from system.integration_testing.business_replicon_report_details import python_callable_method


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/system/integration_testing/config.py


with rail.create_airflow_dag(
    dag_id="system_integration_testing_business_replicon_report_details_operators",
    description="System Integration Testing Business Replicon Report Details Operators",
    company_key=config.company_key,
    replicon_conn_id=config.replicon_conn_id,
    start_date=datetime(2022, 1, 1),
    group='system',
    max_active_runs=10,
    is_paused_upon_creation=True,
    default_args={
        'owner': 'system',
        'replicon_conn_id': config.replicon_conn_id,
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

    batch_task_operator = rail.BatchTaskRunOperator(
        task_id="batch_task_operator",
        start_task="get_user_report_details",
        end_task="delete_this_dagrun",
        execution_timeout=timedelta(hours=config.execution_timeout_hours),
    )

    get_user_report_details = rail.RepliconReportDetailsOperator(
        task_id="get_user_report_details",
        report_name='{{dag_run.conf.report_name}}',
        replicon_conn_id=config.replicon_conn_id,
        log_response=True,
    )

    error_message = "Response data mismatch for run id:{{ dag_run_ecid() }} "

    assert_report_response = rail.PythonOperator(
        task_id="assert_report_response",
        python_callable=python_callable_method.assert_response,
        op_args=[error_message],
    )

    delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
        task_id="delete_this_dagrun", trigger_rule="none_failed"
    )

    (
        batch_task_operator
        >> rail.Label("Test user details reports")
        >> get_user_report_details
        >> assert_report_response
        >> delete_this_dagrun
    )

    batch_task_operator >> rail.Label(
        "DAGrun for deletion") >> delete_this_dagrun
