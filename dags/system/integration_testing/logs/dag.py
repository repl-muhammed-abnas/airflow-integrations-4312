"""
### System Integration Testing Log Operators

#### Purpose:
- This DAG tests all the operators under the <u>[rail/operators/logs](https://github.com/replicon/replicon-airflow-library/tree/main/rail/rail/operators/logs)</u> folder
- PWD state link: <u>https://pwd.rplcn.co/plid=2812</u>

#### Test Cases:
- Added tests for create and filter Master Log artifact
- Added tests for create, write and filter Tenantwide Log artifact
- Added tests for create, write and filter DAG Run Log artifact
"""

from datetime import datetime, timedelta
import rail
from system.integration_testing import config
from system.integration_testing.logs import python_callable_method


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/system/integration_testing/config.py


with rail.create_airflow_dag(
    dag_id="system_integration_testing_log_operators",
    description="System Integration Testing Log Operators",
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
        start_task='write_masterlog',
        end_task='delete_this_dagrun',
        execution_timeout=timedelta(hours=config.execution_timeout_hours)
    )

    write_masterlog = rail.WriteLogOperator(
        task_id='write_masterlog',
        severity='Info',
        message=log_message,
        properties=lambda dag_run: dag_run.conf
    )

    filter_masterlog_entries = rail.FilterLogEntriesOperator(
        task_id='filter_masterlog_entries',
        severity='Info',
        properties={
            'test_string': '{{ dag_run.conf.test_string }}'
        },
        remove_filtered_entries=True
    )

    failure_message = "Master Log Error for run id: {{ dag_run.run_id }}"
    assert_masterlog_entries = rail.PythonOperator(
        task_id='assert_masterlog_entries',
        python_callable=python_callable_method.do_assert_log_entries,
        op_args=['filter_masterlog_entries', failure_message]
    )

    create_tenantwide_log = rail.CreateLogOperator(
        task_id='create_tenantwide_log',
        tenant_wide_name='system_integration_testing_logs',
        existing_log_mode='append'
    )

    write_tenantwide_log = rail.WriteLogOperator(
        task_id='write_tenantwide_log',
        log="{{ result('create_tenantwide_log') }}",
        severity='Info',
        message=log_message,
        properties=lambda dag_run: dag_run.conf
    )

    def do_filter_callable(log, dag_run):
        return log['properties']['test_string'] == dag_run.conf['test_string']
    filter_tenantwide_log = rail.FilterLogEntriesOperator(
        task_id='filter_tenantwide_log',
        log="{{ result('create_tenantwide_log') }}",
        severity='Info',
        filter_callable=do_filter_callable,
        remove_filtered_entries=True
    )

    failure_message = "Tenant Wide Log Error for run id: {{ dag_run.run_id }}"
    assert_tenantwide_entries = rail.PythonOperator(
        task_id='assert_tenantwide_entries',
        python_callable=python_callable_method.do_assert_log_entries,
        op_args=['filter_tenantwide_log', failure_message]
    )

    create_log = rail.CreateLogOperator(
        task_id='create_log'
    )

    write_log_operator = rail.WriteLogOperator(
        task_id='write_log_operator',
        log="{{ result('create_log') }}",
        severity='Info',
        message=log_message,
        properties=lambda dag_run: dag_run.conf
    )

    filter_log = rail.FilterLogEntriesOperator(
        task_id='filter_log',
        log="{{ result('create_log') }}",
        severity='Info',
        properties={
            'test_string': '{{ dag_run.conf.test_string }}'
        }
    )

    failure_message = "DAG Run Log Error for run id: {{ dag_run.run_id }}"
    assert_log_entries = rail.PythonOperator(
        task_id='assert_log_entries',
        python_callable=python_callable_method.do_assert_log_entries,
        op_args=['filter_log', failure_message]
    )

    delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
        task_id='delete_this_dagrun',
        trigger_rule='none_failed'
    )

    batch_task_operator >> rail.Label(
        'Write Master Log Artifact') >> write_masterlog >> filter_masterlog_entries >> assert_masterlog_entries >> rail.Label(
        'Write Tenant Wide Artifact') >> create_tenantwide_log

    create_tenantwide_log >> write_tenantwide_log >> filter_tenantwide_log >> assert_tenantwide_entries >> rail.Label(
        'Write Log Artifact') >> create_log

    create_log >> write_log_operator >> filter_log >> assert_log_entries >> rail.Label(
        'Mark DAGRun for deletion') >> delete_this_dagrun

    batch_task_operator >> rail.Label(
        'Mark DAGRun for deletion') >> delete_this_dagrun
