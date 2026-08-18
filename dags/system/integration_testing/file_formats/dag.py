"""
### System Integration Testing File Format Operators
#### Purpose:
- This DAG tests all the operators under the <u>[rail/operators/file_formats](https://github.com/replicon/replicon-airflow-library/tree/main/rail/rail/operators/file_formats)</u> folder
- PWD state link: <u>https://pwd.rplcn.co/plid=2812</u>
#### Test Cases:
- Added tests for target
- Added tests for adaptor
- Added tests for xsd-document
"""

from datetime import datetime, timedelta
import rail
from system.integration_testing import config
from system.integration_testing.file_formats import python_callable_method

null = None

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/system/integration_testing/config.py


with rail.create_airflow_dag(
    dag_id="system_integration_testing_file_format_operators",
    description="System Integration Testing File Format Operators",
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

    batch_task_operator = rail.BatchTaskRunOperator(
        task_id="batch_task_operator",
        start_task="download_file",
        end_task="delete_this_dagrun",
        execution_timeout=timedelta(hours=config.execution_timeout_hours),
    )

    download_file = rail.SFTPDownloadFileOperator(
        task_id="download_file",
        sftp_conn_id=config.sftp_conn_id,
        remote_filepath="{{ dag_run.conf.input_filepath }}"
        + "{{ dag_run.conf.xml_file_name }}",
    )

    parse_xml = rail.LoadXMLFileOperator(
        task_id='parse_xml',
        document="{{ result('download_file') }}",
        xsd_document="./dags/system/integration_testing/file_formats/xml_schema/data.xsd"
    )

    load_artifact_xml_data = rail.XMLAdaptorOperator(
        task_id="load_artifact_xml_data",
        source='{{ result("parse_xml") }}',
        target='artifact',
        adaptor=[
                'book',
                {
                    'author': "author/text()",
                    'title': "title/text()",
                    "genre": "genre/text()",
                    "price": "price/text()"
                }
        ]
    )

    error_message = "Mismatch the response values for run id:{{ dag_run_ecid() }}"
    assert_response = rail.PythonOperator(
        task_id="assert_response",
        python_callable=python_callable_method.assert_response,
        op_args=[error_message]
    )

    load_result_xml_data = rail.XMLAdaptorOperator(
        task_id="load_result_xml_data",
        source='{{ result("parse_xml") }}',
        target='result',
        adaptor=[
                'book[price > 44.00]',
                {
                    'author': "author/text()",
                    'title': "title/text()",
                    "genre": "genre/text()",
                    "price": "price/text()"
                }
        ]
    )

    assert_method = rail.PythonOperator(
        task_id="assert_method",
        python_callable=python_callable_method.assert_target_response,
        op_args=[error_message]
    )

    delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
        task_id="delete_this_dagrun", trigger_rule="none_failed"
    )

    (
        batch_task_operator
        >> rail.Label("Test xml file")
        >> download_file
        >> parse_xml
        >> load_artifact_xml_data
        >> assert_response
        >> load_result_xml_data
        >> assert_method
        >> delete_this_dagrun
    )

    batch_task_operator >> rail.Label(
        "DAGRun for deletion") >> delete_this_dagrun
