"""
PGP Key Expiry Monitor DAG

This DAG monitors PGP key expiry dates from Airflow connections and sends
email alerts when keys are expired or expiring within 15-30 days.

Schedule: 1st and 15th of every month at 8 AM (0 8 1,15 * *)
Max Active Runs: 1

Tasks:
1. check_pgp_key_expiry - Fetch PGP connections, parse keys, check expiry
2. send_expiry_alert - Send email alert if there are flagged keys
"""
from datetime import datetime, timedelta

import rail
from airflow.models import Variable

from system.pgp_expiry_monitor import config
from system.pgp_expiry_monitor.utils.pgp_utils import get_pgp_key_expiry_status


# DAG Definition
with rail.create_airflow_dag(
    dag_id=config.dag_id,
    description='PGP Key Expiry Monitor - Monitors PGP key expiry dates and sends alerts',
    company_key='system',
    schedule_interval=config.schedule,
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=['system', 'monitoring', 'pgp', 'security'],
    default_args=config.default_args,
    max_active_runs=config.max_active_runs
) as dag:

    # Check if batch task should run
    can_run_batch_task = rail.IfOperator(
        task_id='can_run_batch_task',
        test=lambda: Variable.get(
            config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
        yes_task='batch_task',
        no_task='check_pgp_key_expiry'
    )

    # Batch task wrapper
    batch_task = rail.BatchTaskRunOperator(
        task_id='batch_task',
        start_task='check_pgp_key_expiry',
        end_task='finish',
        execution_timeout=timedelta(hours=1)
    )

    # Task 1: Fetch PGP connections, parse keys, and check for expiry
    check_pgp_key_expiry = rail.PythonOperator(
        task_id='check_pgp_key_expiry',
        python_callable=get_pgp_key_expiry_status,
        op_kwargs={
            'warning_days_min': config.expiry_warning_days_min,
            'warning_days_max': config.expiry_warning_days_max,
            'excluded_conn_ids_var_name': config.excluded_pgp_conn_ids_var_name
        }
    )

    # Task 2: Conditional - should we send alert?
    should_send_alert = rail.IfOperator(
        task_id='should_send_alert',
        test=lambda: rail.result('check_pgp_key_expiry')['should_send_alert'],
        yes_task='send_expiry_alert',
        no_task='finish'
    )

    # Task 3: Send email alert if there are flagged keys
    # Note: RAIL's EmailOperator resolves html_content relative to DAG directory
    send_expiry_alert = rail.EmailOperator(
        task_id='send_expiry_alert',
        to=config.alert_email,
        subject="[Action Required]: PGP Key Expiry Alert | "
                "{{ result('check_pgp_key_expiry').region }} | "
                "{{ result('check_pgp_key_expiry').environment }} | "
                "{{ result('check_pgp_key_expiry').expired_count }} Expired | "
                "{{ result('check_pgp_key_expiry').expiring_count }} "
                "Expiring in {{ result('check_pgp_key_expiry').warning_days_min }}-"
                "{{ result('check_pgp_key_expiry').warning_days_max }} Days | "
                "{{ current_time_in_specified_tz() }}",
        html_content='email_template.html'
    )

    # Finish task
    finish = rail.EmptyOperator(task_id='finish')

    # Task dependencies
    can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
    can_run_batch_task >> rail.Label('No') >> check_pgp_key_expiry
    check_pgp_key_expiry >> should_send_alert
    should_send_alert >> rail.Label('Yes') >> send_expiry_alert >> finish
    should_send_alert >> rail.Label('No') >> finish
