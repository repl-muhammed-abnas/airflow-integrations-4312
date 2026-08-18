"""
### System Integration Testing Network Operators

#### Purpose:
- This DAG tests all the operators under the <u>[rail/operators/network](https://github.com/replicon/replicon-airflow-library/tree/main/rail/rail/operators/network)</u> folder
- PWD state link: <u>https://pwd.rplcn.co/plid=2812</u>

#### Test Cases:
- Added tests for SFTPUploadFileOperator
- Added tests for SFTPDownloadFileOperator
- Added tests for SFTPListFilesOperator
- Added tests for SFTPMoveFileOperator
- Added tests for SFTPDeleteFileOperator
"""

from datetime import datetime, timedelta
import rail
from system.integration_testing import config
from system.integration_testing.sftp import python_callable_method

null = None

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/system/integration_testing/config.py


with rail.create_airflow_dag(
    dag_id="system_integration_testing_sftp_network_operators",
    description="System Integration Testing SFTP Network Operators",
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

    log_message = "add message for DAG Run ECID {{ dag_run_ecid() }}"

    batch_task_operator = rail.BatchTaskRunOperator(
        task_id="batch_task_operator",
        start_task="write_a_csv_file",
        end_task="delete_this_dagrun",
        execution_timeout=timedelta(hours=config.execution_timeout_hours),
    )

    write_a_csv_file = rail.WriteCSVFileOperator(
        task_id="write_a_csv_file",
        source=lambda: [{"value": "1"}, {"value": "2"}],
        header=["value", "id"],
        row=["{{ item.value }}", "{{item.value}}"],
    )

    upload_log_to_sftp = rail.SFTPUploadFileOperator(
        task_id="upload_log_to_sftp",
        content="{{ result('write_a_csv_file') }}",
        sftp_conn_id=config.sftp_conn_id,
        remote_filepath="{{ dag_run.conf.log_filepath }}"
        + "{{ dag_run.conf.file_name }}",
    )

    failure_message = (
        "Triggered Error while uploading file for run id: {{ dag_run.run_id }}"
    )
    assert_upload_log_to_sftp = rail.PythonOperator(
        task_id="assert_upload_log_to_sftp",
        python_callable=python_callable_method.get_task_state,
        op_args=[
            '{{ get_task_state("upload_log_to_sftp")  }}',
            failure_message,
        ],
    )

    download_file = rail.SFTPDownloadFileOperator(
        task_id="download_file",
        sftp_conn_id=config.sftp_conn_id,
        remote_filepath="{{ dag_run.conf.log_filepath }}"
        + "{{ dag_run.conf.file_name }}",
    )

    parse_csv = rail.LoadCSVFileOperator(
        task_id="parse_csv",
        document="{{ result('download_file') }}",
        headers=["value", "id"],
    )

    error_message = (
        "Triggered Error while downloading file for run id: {{ dag_run.run_id }}"
    )
    assert_file_download = rail.PythonOperator(
        task_id="assert_file_download",
        python_callable=python_callable_method.assert_file_download,
        op_args=[error_message],
    )

    list_log_files = rail.SFTPListFilesOperator(
        task_id="list_log_files",
        sftp_conn_id=config.sftp_conn_id,
        paths=["{{dag_run.conf.log_filepath}}"],
    )

    failure_message = "Triggered Error while listing files for run id: {{ dag_run.run_id }}"
    assert_list_log_files = rail.PythonOperator(
        task_id="assert_list_log_files",
        python_callable=python_callable_method.get_list_log_files,
        op_args=[failure_message],
    )

    archive_file = rail.SFTPMoveFileOperator(
        task_id="archive_file",
        sftp_conn_id=config.sftp_conn_id,
        new_filename="{{ dag_run.conf.archive_filepath }}"
        + "{{ dag_run.conf.file_name }}",
        existing_filename="{{ dag_run.conf.log_filepath }}"
        + "{{ dag_run.conf.file_name }}",
    )

    failure_message = (
        "Triggered Error while moving file from one folder to other folder for run id: {{ dag_run.run_id }}"
    )
    assert_archive_file = rail.PythonOperator(
        task_id="assert_archive_file",
        python_callable=python_callable_method.get_task_state,
        op_args=[
            '{{ get_task_state("archive_file")  }}',
            failure_message,
        ],
    )

    delete_file = rail.SFTPDeleteFileOperator(
        task_id="delete_file",
        sftp_conn_id=config.sftp_conn_id,
        existing_filename="{{ dag_run.conf.archive_filepath }}"
        + "{{ dag_run.conf.file_name }}",
    )

    failure_message = (
        "Triggered Error while deleting a file for run id: {{ dag_run.run_id }}"
    )
    assert_delete_file = rail.PythonOperator(
        task_id="assert_delete_file",
        python_callable=python_callable_method.get_task_state,
        op_args=[
            '{{ get_task_state("delete_file")  }}',
            failure_message,
        ],
    )

    delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
        task_id="delete_this_dagrun", trigger_rule="none_failed"
    )

    (
        batch_task_operator
        >> rail.Label("Test files ")
        >> write_a_csv_file
        >> upload_log_to_sftp
        >> assert_upload_log_to_sftp
        >> download_file
        >> parse_csv
        >> assert_file_download
        >> list_log_files
        >> assert_list_log_files
        >> archive_file
        >> assert_archive_file
        >> delete_file
        >> assert_delete_file
        >> delete_this_dagrun
    )

    batch_task_operator >> rail.Label(
        "Mark DAGRun for deletion") >> delete_this_dagrun
