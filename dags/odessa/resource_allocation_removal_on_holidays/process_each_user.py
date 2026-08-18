from datetime import timedelta
import rail
from odessa.resource_allocation_removal_on_holidays.utils import python_callable

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'odessa_remove_allocation_process_each_user_child_{config.instance}',
        description=f'odessa_remove_allocation_process_each_user_child_ {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        create_child_log = rail.CreateLogOperator(
            task_id = 'create_child_log'
        )

        get_holiday_calander_uris = rail.PythonOperator(
            task_id='get_holiday_calander_uris',
            python_callable=python_callable.get_holiday_uri
        )

        process_each_child = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_child',
            retries=0,
            items= '{{ result("get_holiday_calander_uris") | to_json }}',
            trigger_dag_id=f'odessa_remove_allocation_on_holiday_dates_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "holidayname": "{{ item.holidayname }}",
                "holidaydate": "{{ item.holidaydate }}",
                "holidayday": "{{ item.holidayday }}",
                "holidaymonth": "{{ item.holidaymonth }}",
                "holidayyear": "{{ item.holidayyear }}",
                "holidayuri": "{{ item.holidayuri }}",
                "holidaycalendarname": "{{ item.holidaycalendarname }}",
                "holidaycalendaruri": "{{ item.holidaycalendaruri }}",
                "resourceUri": "{{ dag_run.conf.useruri }}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "child_log" : "{{ result('create_child_log') }}"
            }
        )

        wait_for_process_each_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{result('process_each_child')}}"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger'
        )

        create_child_log >> get_holiday_calander_uris >> process_each_child >> wait_for_process_each_child >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
