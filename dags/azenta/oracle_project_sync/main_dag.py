"""
master DAG - Azenta Oracle Fusion -> Replicon Polaris project sync.

Runs every 30 minutes: reads the delta watermark, pulls ACTIVE + CLOSED CUSP-POC
projects changed since then, fans out one child DAG per project, emails the generated
run log to the Azenta DL, then advances the watermark to this run's start timestamp.

Scope: F1013 only (Oracle -> Polaris).
"""
from datetime import timedelta

import pendulum
import rail
from airflow.models import Variable

from azenta.oracle_project_sync.utils import custom_methods, request_payload

# pylint: disable=expression-not-assigned,pointless-statement


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'Azenta Oracle->Polaris project sync master ({config.instance})',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        # start_date=pendulum.datetime(2026, 7, 1, tz=config.timezone_iana),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs_master,
    ) as dag:

        # -- Batch-task toggle (house pattern) --------------------------------
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_log',
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_log',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # -- Run log (emailed at the end; the "report" is logs only) ----------
        create_log = rail.CreateLogOperator(task_id='create_log')

        # -- Watermark: read stored value + stamp "now" (plain Airflow Variable)
        def read_watermark_and_stamp_now():
            now_dt = pendulum.now('UTC')
            stored = Variable.get(config.watermark_var_name, default_var=None)
            return {
                'query_watermark': custom_methods.compute_query_watermark(stored, now_dt),
                'begin_timestamp': now_dt.strftime(config.WATERMARK_DATE_FORMAT),
            }

        read_watermark = rail.PythonOperator(
            task_id='read_watermark_and_stamp_now',
            python_callable=read_watermark_and_stamp_now,
        )

        # -- Oracle delta queries ------
        get_active_projects = rail.PythonOperator(
            task_id='get_active_projects',
            retries=config.oracle_api_retries,
            python_callable=lambda: custom_methods.fetch_oracle_paginated(
                config.oracle_conn_id,
                request_payload.oracle_projects_delta_endpoint(
                    config.ORACLE_API_BASE, 'ACTIVE',
                    rail.result('read_watermark_and_stamp_now')['query_watermark'])),
        )

        get_closed_projects = rail.PythonOperator(
            task_id='get_closed_projects',
            retries=config.oracle_api_retries,
            python_callable=lambda: custom_methods.fetch_oracle_paginated(
                config.oracle_conn_id,
                request_payload.oracle_projects_delta_endpoint(
                    config.ORACLE_API_BASE, 'CLOSED',
                    rail.result('read_watermark_and_stamp_now')['query_watermark'])),
        )

        build_project_worklist = rail.PythonOperator(
            task_id='build_project_worklist',
            python_callable=lambda: custom_methods.build_worklist(
                rail.result('get_active_projects'),
                rail.result('get_closed_projects'),
            ),
        )

        has_projects = rail.IfOperator(
            task_id='has_projects',
            test=lambda: len(rail.result('build_project_worklist')) > 0,
            yes_task='trigger_process_project',
            no_task='update_watermark',
        )

        # -- Fan out one child DAG per project --------------------------------
        trigger_process_project = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_process_project',
            trigger_dag_id=config.process_project_dag_id,
            retries=0,
            items=lambda: rail.result('build_project_worklist'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **item,
                'log': rail.result('create_log'),
            },
        )

        wait_for_children = rail.WaitForDagRunsSensor(
            task_id='wait_for_children',
            dag_runs="{{ result('trigger_process_project') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        trigger_log_generation = rail.TriggerDagRunOperator(
            task_id='trigger_log_generation',
            trigger_rule='none_failed_min_one_success',
            trigger_dag_id=config.process_log_generation,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: {
                'log': rail.result('create_log'),
                'start_time': rail.result('read_watermark_and_stamp_now')['begin_timestamp'],
            },
        )

        # -- Advance the watermark to this run's start timestamp --------------
        def update_watermark():
            begin_timestamp = rail.result('read_watermark_and_stamp_now')['begin_timestamp']
            Variable.set(config.watermark_var_name, begin_timestamp)
            return begin_timestamp

        update_watermark_task = rail.PythonOperator(
            task_id='update_watermark',
            trigger_rule='all_done',
            python_callable=update_watermark,
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log="{{ result('create_log') }}",
            severity='Error',
            message='Master sync run failed: {{ get_error_message() }}',
            properties={'jobid': '{{ dag_run_ecid() }} | {{ dag_run.run_id }}'},
        )

        # -- Wiring -----------------------------------------------------------
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> create_log

        create_log >> read_watermark >> get_active_projects >> get_closed_projects \
            >> build_project_worklist >> has_projects

        has_projects >> rail.Label('Yes') >> trigger_process_project \
            >> wait_for_children >> trigger_log_generation >> update_watermark_task
        has_projects >> rail.Label('No') >> update_watermark_task

        update_watermark_task >> catch_and_log_errors

        return dag


rail.for_each_instance(create_main_dag)
