"""
### System Integration Testing Business Get Groups Matching Filter Operators

#### Purpose:
- This DAG tests all the operators under the <u>[rail/operators](https://github.com/replicon/replicon-airflow-library/tree/main/rail/rail/operators)</u> folder
- PWD state link: <u>https://pwd.rplcn.co/plid=2812</u>

#### Test Cases:
- Added tests for text search for the group type
"""

from datetime import datetime, timedelta
import rail
from system.integration_testing import config
from system.integration_testing.business_get_groups_matching_filter import python_callable_method


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/system/integration_testing/config.py


with rail.create_airflow_dag(
    dag_id="system_integration_testing_business_get_groups_matching_filter_operators",
    description="System Integration Testing Business Get Groups Matching Filter Operators",
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
        start_task="get_company_department",
        end_task="delete_this_dagrun",
        execution_timeout=timedelta(hours=config.execution_timeout_hours),
    )

    get_company_department = rail.GetGroupsMatchingFilterOperator(
        task_id='get_company_department',
        group_type='Department',
        text_search='Company'
    )

    error_message = "Response data mismatch for run id:{{ dag_run_ecid() }} "

    assert_department_response = rail.PythonOperator(
        task_id="assert_department_response",
        python_callable=python_callable_method.assert_department_response,
        op_args=[error_message],
    )

    get_ftp_divisions = rail.GetGroupsMatchingFilterOperator(
        task_id='get_ftp_divisions',
        group_type='Division',
        text_search='FTP'
    )

    assert_division_response = rail.PythonOperator(
        task_id="assert_division_response",
        python_callable=python_callable_method.assert_division_response,
        op_args=[error_message]
    )

    get_salaried_employeetype = rail.GetGroupsMatchingFilterOperator(
        task_id='get_salaried_employeetype',
        group_type='EmployeeType',
        text_search='Salaried'
    )

    assert_employeetype_response = rail.PythonOperator(
        task_id="assert_employeetype_response",
        python_callable=python_callable_method.assert_employeetype_response,
        op_args=[error_message]
    )

    delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
        task_id="delete_this_dagrun", trigger_rule="none_failed"
    )

    (
        batch_task_operator
        >> rail.Label("Test user details reports")
        >> get_company_department
        >> assert_department_response
        >> get_ftp_divisions
        >> assert_division_response
        >> get_salaried_employeetype
        >> assert_employeetype_response
        >> delete_this_dagrun
    )

    batch_task_operator >> rail.Label(
        "DAGrun for deletion") >> delete_this_dagrun
