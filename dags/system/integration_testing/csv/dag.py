"""
### System Integration Testing File Format Operators

#### Purpose:
- This DAG tests all the operators under the <u>[rail/operators/file_formats](https://github.com/replicon/replicon-airflow-library/tree/main/rail/rail/operators/file_formats)</u> folder
- PWD state link: <u>https://pwd.rplcn.co/plid=2812</u>

#### Test Cases:
- Added tests for write csv
- Added tests for load csv
- Added tests for write csv with footer
"""
import csv
from datetime import datetime, timedelta
import rail
from system.integration_testing import config
from system.integration_testing.csv import python_callable_method

null = None

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/system/integration_testing/config.py


with rail.create_airflow_dag(
    dag_id="system_integration_testing_csv_operators",
    description="System Integration Testing CSV Operators",
    company_key=config.company_key,
    start_date=datetime(2022, 1, 1),
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
        start_task="write_a_csv_file",
        end_task="delete_this_dagrun",
        execution_timeout=timedelta(hours=config.execution_timeout_hours),
    )

    write_a_csv_file = rail.WriteCSVFileOperator(
        task_id="write_a_csv_file",
        source='{{ dag_run.conf.employeeDetails | to_json }}',
        delimiter=",",
        encoding="utf-8",
        quoting=csv.QUOTE_ALL,
        header=['JobId', 'EmployeeName', 'EmployeeLocation'],
        row=['{{ item.JobId }}', '{{ item.EmployeeName }}',
             '{{ item.EmployeeLocation }}'],
        lineterminator="\n",
    )

    error_message = "Data mismatch for run id:{{ dag_run_ecid() }} "
    assert_csv_data = rail.PythonOperator(
        task_id="assert_csv_data",
        python_callable=python_callable_method.assert_csv_data,
        op_args=[error_message, "{{result('write_a_csv_file')}}"],
    )

    write_new_csv_file = rail.WriteCSVFileOperator(
        task_id="write_new_csv_file",
        source='{{ dag_run.conf.employeeDetails | to_json }}',
        delimiter=",",
        encoding="utf-8",
        quoting=csv.QUOTE_ALL,
        header=['JobId', 'EmployeeName', 'EmployeeLocation'],
        row=['{{ item.JobId }}', '{{ item.EmployeeName }}',
             '{{ item.EmployeeLocation }}'],
        lineterminator="\n",
        thread_pool_size=config.write_csv_thread_pool_size,
    )

    error_message = "Invalid data for run id:{{ dag_run_ecid() }} "
    assert_new_csv_data = rail.PythonOperator(
        task_id="assert_new_csv_data",
        python_callable=python_callable_method.assert_csv_data,
        op_args=[error_message, "{{result('write_new_csv_file')}}"],
    )

    parse_csv = rail.LoadCSVFileOperator(
        task_id="parse_csv",
        document="{{ result('write_a_csv_file') }}",
        delimiter=",",
        encoding="utf-8",
        headers=['JobId', 'EmployeeName', 'EmployeeLocation'],
    )

    error_message = "Invalid data mismatch for run id:{{ dag_run_ecid() }} "
    assert_load_csv_data = rail.PythonOperator(
        task_id="assert_load_csv_data",
        python_callable=python_callable_method.assert_csv_data,
        op_args=[error_message, "{{result('parse_csv')}}"],
    )

    write_a_csv_file_with_footer = rail.WriteCSVFileOperator(
        task_id="write_a_csv_file_with_footer",
        source='{{ dag_run.conf.employeeDetails | to_json }}',
        delimiter=",",
        encoding="utf-8",
        quoting=csv.QUOTE_ALL,
        header=['JobId', 'EmployeeName', 'EmployeeLocation'],
        row=['{{ item.JobId }}', '{{ item.EmployeeName }}',
             '{{ item.EmployeeLocation }}'],
        footer=['Number of records found: 3',
                'Number of records processed: 3'
                ],
        lineterminator="\n",
    )

    error_message = "Data mismatch for run id:{{ dag_run_ecid() }} "
    assert_csv_data_with_footer = rail.PythonOperator(
        task_id="assert_csv_data_with_footer",
        python_callable=python_callable_method.assert_csv_data_with_footer,
        op_args=[error_message, "{{result('write_a_csv_file_with_footer')}}"],
    )

    delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
        task_id="delete_this_dagrun", trigger_rule="none_failed"
    )

    (
        batch_task_operator
        >> rail.Label("Test file responses")
        >> write_a_csv_file
        >> assert_csv_data >> write_new_csv_file >> assert_new_csv_data
        >> parse_csv >> assert_load_csv_data
        >> write_a_csv_file_with_footer >> assert_csv_data_with_footer
        >> delete_this_dagrun
    )

    batch_task_operator >> rail.Label(
        "Mark DAGRun for deletion") >> delete_this_dagrun
