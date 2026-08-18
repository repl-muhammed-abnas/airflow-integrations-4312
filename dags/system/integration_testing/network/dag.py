"""
### System Integration Testing Network Operators

#### Purpose:
- This DAG tests all the operators under the <u>[rail/operators](https://github.com/replicon/replicon-airflow-library/tree/main/rail/rail/operators)</u> folder
- PWD state link: <u>https://pwd.rplcn.co/plid=2812</u>

#### Test Cases:
- Added tests for method
- Added tests for response_check
- Added tests for parameter must be callable if provided as a function
- Added tests for s3 operators
"""

from datetime import datetime, timedelta
import rail
from system.integration_testing import config
from system.integration_testing.network import python_callable_method


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/system/integration_testing/config.py


with rail.create_airflow_dag(
    dag_id="system_integration_testing_network_operators",
    description="System Integration Testing Network Operators",
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

    batch_task_operator = rail.BatchTaskRunOperator(
        task_id="batch_task_operator",
        start_task="post_request_body",
        end_task="delete_this_dagrun",
        execution_timeout=timedelta(hours=config.execution_timeout_hours),
    )

    post_request_body = rail.PythonOperator(
        task_id='post_request_body',
        python_callable=lambda: {"title": "foo", "body": "bar", "userId": 1}
    )

    # endpoint https://jsonplaceholder.typicode.com/posts
    post_simple_http_operator = rail.SimpleHttpOperator(
        task_id="post_simple_http_operator",
        method="POST",
        http_conn_id=config.http_conn_id,
        endpoint="/posts",
        log_response=False,
        headers={"Content-Type": "application/json"},
        data="{{result('post_request_body') | to_json}}",
    )

    error_message = "Mismatch in the response of NetworkOperator for run id:{{ dag_run_ecid() }} "
    assert_post_response = rail.PythonOperator(
        task_id="assert_post_response",
        python_callable=python_callable_method.assert_post_method,
        op_args=[error_message]
    )

    # endpoint https://jsonplaceholder.typicode.com/users
    get_simple_http_operator = rail.SimpleHttpOperator(
        task_id="get_simple_http_operator",
        method="GET",
        http_conn_id=config.http_conn_id,
        endpoint="/users",
        headers={"Content-Type": "application/json"},
    )

    assert_get_response = rail.PythonOperator(
        task_id="assert_get_response",
        python_callable=python_callable_method.assert_get_method,
        op_args=[error_message],
    )

    create_csv_file = rail.WriteCSVFileOperator(
        task_id="create_csv_file",
        source=lambda: [{"value": "1"}, {"value": "2"}],
        header=["value", "id"],
        row=["{{ item.value }}", "{{item.value}}"],
    )

    upload_reference_s3_file = rail.S3UploadFileOperator(
        task_id="upload_reference_s3_file",
        aws_conn_id=config.aws_conn_id,
        source="{{ result('create_csv_file') }}",
        bucket_name="{{dag_run.conf.aws_s3_bucket}}",
        key_name="{{dag_run.conf.network_filepath}}"
        + "{{dag_run_ecid()}}_{{dag_run.conf.file_name}}",
    )

    download_reference_s3_file = rail.S3DownloadFileOperator(
        task_id="download_reference_s3_file",
        bucket_name="{{dag_run.conf.aws_s3_bucket}}",
        key_name="{{dag_run.conf.network_filepath}}"
        + "{{dag_run_ecid()}}_{{dag_run.conf.file_name}}",
        aws_conn_id=config.aws_conn_id,
    )

    load_csv_file = rail.LoadCSVFileOperator(
        task_id="load_csv_file",
        headers=["value", "id"],
        document='{{result("download_reference_s3_file")}}'
    )
    assert_s3_response = rail.PythonOperator(
        task_id="assert_s3_response",
        python_callable=python_callable_method.assert_s3_method,
        op_args=[error_message],
    )

    list_s3_reference_files = rail.S3ListKeysOperator(
        task_id="list_s3_reference_files",
        bucket_name="{{dag_run.conf.aws_s3_bucket}}",
        prefix="{{dag_run.conf.network_filepath}}",
        aws_conn_id=config.aws_conn_id,
    )

    assert_s3listkey_response = rail.PythonOperator(
        task_id="assert_s3listkey_response",
        python_callable=python_callable_method.assert_s3listkey_method,
        op_args=[error_message]
    )

    move_old_s3_reference_file = rail.S3MoveFileOperator(
        task_id="move_old_s3_reference_file",
        source_bucket_name="{{dag_run.conf.aws_s3_bucket}}",
        existing_key_name="{{dag_run.conf.network_filepath}}"
        + "{{dag_run_ecid()}}_{{dag_run.conf.file_name}}",
        new_key_name="{{dag_run.conf.new_network_filepath}}"
        + "{{dag_run_ecid()}}_{{dag_run.conf.new_file_name}}",
        aws_conn_id=config.aws_conn_id,
    )

    download_reference_s3move_file = rail.S3DownloadFileOperator(
        task_id="download_reference_s3move_file",
        bucket_name="{{dag_run.conf.aws_s3_bucket}}",
        key_name="{{dag_run.conf.new_network_filepath}}"
        + "{{dag_run_ecid()}}_{{dag_run.conf.new_file_name}}",
        aws_conn_id=config.aws_conn_id,
    )

    load_new_csv_file = rail.LoadCSVFileOperator(
        task_id="load_new_csv_file",
        headers=["value", "id"],
        document='{{result("download_reference_s3move_file")}}'
    )

    assert_s3move_response = rail.PythonOperator(
        task_id="assert_s3move_response",
        python_callable=python_callable_method.assert_s3movefile_method,
        op_args=[error_message]
    )

    delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
        task_id="delete_this_dagrun", trigger_rule="none_failed"
    )

    (
        batch_task_operator
        >> rail.Label("Test POST Request")
        >> post_request_body
        >> post_simple_http_operator
        >> assert_post_response
        >> rail.Label("Test GET Request")
        >> get_simple_http_operator
        >> assert_get_response
        >> rail.Label("Test S3 operators")
        >> create_csv_file
        >> upload_reference_s3_file
        >> download_reference_s3_file
        >> load_csv_file
        >> assert_s3_response
        >> list_s3_reference_files
        >> assert_s3listkey_response
        >> move_old_s3_reference_file
        >> download_reference_s3move_file
        >> load_new_csv_file
        >> assert_s3move_response
        >> delete_this_dagrun
    )

    batch_task_operator >> rail.Label(
        "DAGRun for deletion") >> delete_this_dagrun
