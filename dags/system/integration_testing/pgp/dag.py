"""
### System Integration Testing PGP Operators

#### Purpose:
- This DAG tests all the operators under the <u>[rail/operators/pgp](https://github.com/replicon/replicon-airflow-library/tree/main/rail/rail/operators/pgp)</u> folder
- PWD state link: <u>https://pwd.rplcn.co/plid=2812</u>

#### Test Cases:
- Added tests for PGPEncryptionOperator
- Added tests for PGPDecryptionOperator
"""

from datetime import datetime, timedelta
import rail
from system.integration_testing import config
from system.integration_testing.pgp import python_callable_method
null = None

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/system/integration_testing/config.py


with rail.create_airflow_dag(
    dag_id="system_integration_testing_pgp_operators",
    description="System Integration Testing PGP Operators",
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
        source=lambda: [{"value": "1"}],
        delimiter=",",
        encoding="utf-8",
        header=["value"],
        row=["{{ item.value }}"],
    )

    encrypt_csv_data = rail.PGPEncryptionOperator(
        task_id="encrypt_csv_data",
        pgp_conn_id=config.pgp_conn_id,
        source="{{ result('write_a_csv_file') }}",
    )

    decrypt_file = rail.PGPDecryptionOperator(
        task_id="decrypt_file",
        source="{{ result('encrypt_csv_data') }}",
        pgp_conn_id=config.pgp_conn_id,
    )

    parse_file = rail.LoadCSVFileOperator(
        task_id="parse_file",
        document="{{ result('decrypt_file') }}",
    )

    error_message = "Data mismatch after decryption for run id: {{ dag_run_ecid() }}"
    assert_decrypted_file = rail.PythonOperator(
        task_id="assert_decrypted_file",
        python_callable=python_callable_method.assert_decrypted_file,
        op_args=[error_message, "{{result('parse_file')}}"],
    )

    encrypt_signed_data = rail.PGPEncryptionOperator(
        task_id="encrypt_signed_data",
        pgp_conn_id=config.pgp_conn_id,
        sign=True,
        source="{{ result('write_a_csv_file') }}",
    )

    decrypt_signed_file = rail.PGPDecryptionOperator(
        task_id="decrypt_signed_file",
        source="{{ result('encrypt_signed_data') }}",
        pgp_conn_id=config.pgp_conn_id,
    )

    parse_decrypted_file = rail.LoadCSVFileOperator(
        task_id="parse_decrypted_file",
        document="{{ result('decrypt_signed_file') }}",
    )

    error_message = "Mismatch in the data after decryption for run id: {{ dag_run_ecid() }}"
    assert_decrypt_signed_file = rail.PythonOperator(
        task_id="assert_decrypt_signed_file",
        python_callable=python_callable_method.assert_decrypted_file,
        op_args=[error_message, "{{result('parse_decrypted_file')}}"],
    )
    delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
        task_id="delete_this_dagrun", trigger_rule="none_failed"
    )

    (
        batch_task_operator
        >> rail.Label("Test decrypted files")
        >> write_a_csv_file
        >> encrypt_csv_data
        >> decrypt_file
        >> parse_file
        >> assert_decrypted_file
        >> encrypt_signed_data
        >> decrypt_signed_file
        >> parse_decrypted_file
        >> assert_decrypt_signed_file
        >> delete_this_dagrun
    )

    batch_task_operator >> rail.Label(
        "Mark DAGRun for deletion") >> delete_this_dagrun
