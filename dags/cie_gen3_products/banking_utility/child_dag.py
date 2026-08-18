# pylint: disable=line-too-long wildcard-import redefined-outer-name unused-wildcard-import
from datetime import timedelta
from airflow.models import Variable
import rail
from cie_gen3_products.banking_utility.utils.python_callable import *


def create_child_dag_wbs(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    with rail.create_airflow_dag(
        dag_id=f'cie_{config.company_key}_process_each_user_timeoff_child{dag_id_postfix}'.lower(),
        description='Process list of user uri for particular timeoff type',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_start_end_date'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_start_end_date',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )
        get_start_end_date = rail.PythonOperator(
            task_id='get_start_end_date',
            python_callable=get_min_max_date,
        )

        get_accrual_history = rail.RepliconServiceOperator(
            task_id='get_accrual_history',
            endpoint='/services/TimeOffService1.svc/GetManualAccrualAdjustmentDetailsForDateRange',
            data=get_accrual_history_payload
        )

        get_data_to_be_processed = rail.PythonOperator(
            task_id='get_data_to_be_processed',
            python_callable=get_data_for_processing,
        )
        foreach_file_entry = rail.ForEachOperator(
            task_id='foreach_file_entry',
            items=lambda: rail.result('get_data_to_be_processed'),
            start_task='perform_accrual',
            end_task='foreach_file_entry_end'
        )

        perform_accrual = rail.RepliconServiceOperator(
            task_id='perform_accrual',
            endpoint='/services/TimeOffService1.svc/PutManualAccrualAdjustment',
            data=get_accrual_params
        )

        check_for_log = rail.IfOperator(
            task_id='check_for_log',
            trigger_rule='all_done',
            test=task_state,
            yes_task='write_logs_for_success',
            no_task='write_logs_for_failure'
        )

        write_logs_for_success = rail.WriteLogOperator(
            task_id='write_logs_for_success',
            log="{{ dag_run.conf.logid }}",
            message="na",
            severity="success",
            properties={
                "user_name": "{{ result('foreach_file_entry').user_name }}",
                "accrual_date": "{{ result('foreach_file_entry').accrual_date }}",
                "hour_to_accrue": "{{ result('foreach_file_entry').hours_to_accrue }}",
                "time_off_type": "{{ result('foreach_file_entry').time_off_type }}",
                "status": "success",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        write_logs_for_failure = rail.WriteLogOperator(
            task_id='write_logs_for_failure',
            log="{{ dag_run.conf.logid }}",
            message="na",
            severity="failed",
            properties={
                "user_name": "{{ result('foreach_file_entry').user_name }}",
                "accrual_date": "{{ result('foreach_file_entry').accrual_date }}",
                "hour_to_accrue": "{{ result('foreach_file_entry').hours_to_accrue }}",
                "time_off_type": "{{ result('foreach_file_entry').time_off_type }}",
                "status": "failed",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        foreach_file_entry_end = rail.EmptyOperator(
            task_id='foreach_file_entry_end',
        )

        finish = rail.EmptyOperator(
            task_id="finish"
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ dag_run.conf.logid }}",
            trigger_rule='one_failed',
            severity='Error',
            message='failed to process user timeoff accrual',
            properties={
                "user_name": "",
                "accrual_date": "",
                "hour_to_accrue": "",
                "time_off_type": "",
                'status': 'error',
                "childjobid": "{{ dag_run.conf.item }}|{{ dag_run_ecid() }}"
            },
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> get_start_end_date
        get_start_end_date >> get_accrual_history >> get_data_to_be_processed >> foreach_file_entry
        foreach_file_entry >> perform_accrual >> check_for_log
        check_for_log >> rail.Label(
            'Yes') >> write_logs_for_success >> foreach_file_entry_end
        check_for_log >> rail.Label(
            'No') >> write_logs_for_failure >> foreach_file_entry_end
        foreach_file_entry >> foreach_file_entry_end >> finish
        finish >> catch_and_log_errors
    return dag


rail.for_each_instance(create_child_dag_wbs)
