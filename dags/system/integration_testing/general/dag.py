"""
### System Integration Testing General Operators

#### Purpose:
- This DAG tests all the operators under the <u>[rail/operators](https://github.com/replicon/replicon-airflow-library/tree/main/rail/rail/operators)</u> folder
- PWD state link: <u>https://pwd.rplcn.co/plid=2812</u>

#### Test Cases:
- Added tests for jinja templating for test
- Added tests for bool value to be string as 'True' or 'False'
- Added tests for mode
- Added tests for parameter must be callable if provided as a function
"""

from datetime import datetime, timedelta
import rail
from system.integration_testing import config
from system.integration_testing.general import python_callable_method


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/system/integration_testing/config.py


with rail.create_airflow_dag(
    dag_id="system_integration_testing_general_operators",
    description="System Integration Testing General Operators",
    company_key=config.company_key,
    start_date=datetime(2022, 1, 1),
    max_active_runs=10,
    group='system',
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
        task_id="batch_task_operator",
        start_task="start",
        end_task="delete_this_dagrun",
        execution_timeout=timedelta(hours=config.execution_timeout_hours),
    )

    start = rail.EmptyOperator(task_id="start")

    if_test_is_correct = rail.IfOperator(
        task_id="if_test_is_correct",
        test=lambda dag_run: isinstance(dag_run.conf['test_string'], str),
        yes_task="assert_bool_value",
        no_task="if_test_is_in_templateformat",
    )

    error_message = "Test should return bool value for run id:{{ dag_run_ecid() }} "
    assert_bool_value = rail.PythonOperator(
        task_id="assert_bool_value",
        python_callable=python_callable_method.assert_true_value,
        op_args=[
            "{{result('if_test_is_correct')}}",
            error_message,
            "assert_bool_value",
        ],
    )

    if_test_is_in_templateformat = rail.IfOperator(
        task_id="if_test_is_in_templateformat",
        test="{{dag_run.conf.test_string | is_truthy}}",
        yes_task="assert_true_value",
        no_task="if_test_is_callable",
    )

    error_message = "Test should return true value for run id:{{ dag_run_ecid() }} "
    assert_true_value = rail.PythonOperator(
        task_id="assert_true_value",
        python_callable=python_callable_method.assert_true_value,
        op_args=[
            "{{result('if_test_is_in_templateformat')}}",
            error_message,
            "assert_true_value",
        ],
    )

    def get_value(dag_run):
        return isinstance(dag_run.conf['test_string'], str)

    if_test_is_callable = rail.IfOperator(
        task_id="if_test_is_callable",
        test=get_value,
        yes_task="assert_function_call",
        no_task="create_csv_for_multiple_row",
    )

    error_message = "function must be callable for run id:{{ dag_run_ecid() }} "
    assert_function_call = rail.PythonOperator(
        task_id="assert_function_call",
        python_callable=python_callable_method.assert_true_value,
        op_args=[
            "{{result('if_test_is_callable')}}",
            error_message,
            "assert_function_call",
        ],
    )

    delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
        task_id="delete_this_dagrun", trigger_rule="none_failed"
    )

    def get_source():
        return [{
            'key': item
        } for item in range(1, 5)]

    create_csv_for_multiple_row = rail.WriteCSVFileOperator(
        task_id='create_csv_for_multiple_row',
        source=get_source,
        header=['taskname', 'firstname', 'lastname'],
        row=['{{ item.key }}Task', '{{ item.key}}FirstName',
             '{{ item.key }}LastName'],
    )

    data_adaptor_operator_1 = rail.DataAdaptorOperator(
        task_id="data_adaptor_operator_1",
        source="{{result('create_csv_for_multiple_row')}}",
        mode='dataset',
        columns=['taskname', 'fullname'],
        data_format='auto',
        data=lambda item: {
                'taskname': item['taskname'],
                'fullname': item['firstname'] + item['lastname']
        } if item else None
    )

    error_message = "Mismatch converted data for run id:{{ dag_run_ecid() }} "
    assert_data_adaptor_operator_1_response = rail.PythonOperator(
        task_id="assert_data_adaptor_operator_1_response",
        python_callable=python_callable_method.assert_data_adaptor_operator_1,
        op_args=[error_message],
    )

    def get_source2():
        return [{
            'key': item
        } for item in range(1, 2)]

    create_csv_for_single_row = rail.WriteCSVFileOperator(
        task_id='create_csv_for_single_row',
        source=get_source2,
        header=['employeeid', 'firstname', 'lastname'],
        row=['{{ item.key }}EmployeeId', '{{ item.key}}FirstName',
             '{{ item.key }}LastName'],
    )

    data_adaptor_operator_2 = rail.DataAdaptorOperator(
        task_id="data_adaptor_operator_2",
        source="{{result('create_csv_for_single_row')}}",
        mode='single-row',
        columns=['employeeid', 'fullname'],
        data_format='object',
        data=lambda item: {
                'employeeid': item['employeeid'],
                'fullname': item['firstname'] + item['lastname']
        } if item else None
    )

    assert_data_adaptor_operator_2_response = rail.PythonOperator(
        task_id="assert_data_adaptor_operator_2_response",
        python_callable=python_callable_method.assert_data_adaptor_operator_2,
        op_args=[error_message],
    )

    batch_task_operator >> rail.Label(
        "Test bool value") >> start >> if_test_is_correct

    (
        if_test_is_correct
        >> rail.Label("Yes")
        >> assert_bool_value
        >> if_test_is_in_templateformat
    )
    if_test_is_correct >> rail.Label("No") >> if_test_is_in_templateformat

    (
        if_test_is_in_templateformat
        >> rail.Label("Yes")
        >> assert_true_value
        >> if_test_is_callable
    )
    if_test_is_in_templateformat >> rail.Label("No") >> if_test_is_callable

    (
        if_test_is_callable
        >> rail.Label("Yes")
        >> assert_function_call
        >> rail.Label("Test for dataset mode")
        >> create_csv_for_multiple_row
        >> data_adaptor_operator_1
        >> assert_data_adaptor_operator_1_response
        >> rail.Label("Test for single-row mode")
        >> create_csv_for_single_row
        >> data_adaptor_operator_2
        >> assert_data_adaptor_operator_2_response
        >> delete_this_dagrun
    )

    if_test_is_callable >> rail.Label("No") >> create_csv_for_multiple_row

    batch_task_operator >> rail.Label(
        'Mark DAGRun for deletion') >> delete_this_dagrun
