from datetime import timedelta
from pendulum import datetime
from data_intellect_services.timeoff_sync_v1.utils import custom_methods
from data_intellect_services.timeoff_sync_v1.utils import response_filters
from data_intellect_services.timeoff_sync_v1.utils import request_payload
from data_intellect_services.timeoff_sync_v1.tasks.send_logs import get_send_logs
from airflow.models import Variable
import rail

open_bracket = '{{'
close_bracket = '}}'

null=None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag,
        description=f'Data Intellect Timeoff Sync Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2023, 6, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_master_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='logging_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='logging_details',
            end_task='dagrun_log_to_sumo',
        )

        logging_details = rail.PythonOperator(
            task_id='logging_details',
            python_callable=custom_methods.get_logging_details,
            op_args=[config]
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        get_timeoff_bookings_from_hibob = rail.SimpleHttpOperator(
            task_id='get_timeoff_bookings_from_hibob',
            method='GET',
            http_conn_id=config.http_conn_id,
            endpoint='timeoff/requests/changes?since={{ result("logging_details").prev_2_hours_encoded }}',
            headers={
                'accept': 'application/json',
                # 'Authorization': f'{open_bracket}var.value.{config.access_token}{close_bracket}'
            },
            response_filter=response_filters.filter_timeoff_data
        )

        if_timeoff_bookings_exists = rail.IfOperator(
            task_id='if_timeoff_bookings_exists',
            test=lambda: rail.result("get_timeoff_bookings_from_hibob")['created'] or \
                rail.result("get_timeoff_bookings_from_hibob")['canceled_deleted'],
            yes_task='get_all_users_payload',
            no_task='send_no_new_timeoff_bookings_email'
        )

        send_no_new_timeoff_bookings_email = rail.EmailOperator(
            task_id='send_no_new_timeoff_bookings_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Timeoff Sync from HIBOB to Replicon is completed at {{ result("logging_details").process_start_time }}',
            html_content='/templates/emails/no_new_timeoff_bookings.html'
        )

        get_all_users_payload = rail.PythonOperator(
            task_id='get_all_users_payload',
            python_callable=request_payload.get_all_users_payload
        )

        get_all_users_from_hibob = rail.SimpleHttpOperator(
            task_id='get_all_users_from_hibob',
            method='POST',
            http_conn_id=config.http_conn_id,
            endpoint='people/search',
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                # 'Authorization': f'{open_bracket}var.value.{config.access_token}{close_bracket}'
            },
            data='{{ result("get_all_users_payload") }}',
            response_filter=response_filters.filter_users_data
        )

        get_booking_id_oef_value = rail.RepliconServiceOperator(
            task_id='get_booking_id_oef_value',
            endpoint='/services/ObjectExtensionDefinitionListService1.svc/GetData',
            data=request_payload.get_booking_id_oef_value_payload,
            data_handler=response_filters.get_booking_id_oef_value
        )

        book_timeoff = rail.TriggerDagRunForEachItemOperator(
            task_id='book_timeoff',
            items='{{ result("get_timeoff_bookings_from_hibob").created | to_json }}',
            trigger_dag_id=config.timeoff_booking_child,
            conf=lambda item: {
                "booking_data": item,
                "log_artifact": rail.result("create_log"),
                "actual_employee_id": custom_methods.get_actual_employee_id(item["employeeId"]),
                "booking_id_oef_value": rail.result('get_booking_id_oef_value')['booking_id_oef_value']
            },
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_bookings = rail.WaitForDagRunsSensor(
            task_id="wait_for_bookings",
            dag_runs="{{result('book_timeoff')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        delete_timeoff = rail.TriggerDagRunForEachItemOperator(
            task_id='delete_timeoff',
            items='{{ result("get_timeoff_bookings_from_hibob").canceled_deleted | to_json }}',
            trigger_dag_id=config.timeoff_delete_child,
            conf=lambda item: {
                "booking_data": item,
                "log_artifact": rail.result("create_log"),
                "actual_employee_id": custom_methods.get_actual_employee_id(item["employeeId"]),
                "booking_id_oef_value": rail.result('get_booking_id_oef_value')['booking_id_oef_value']
            },
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_delete_bookings = rail.WaitForDagRunsSensor(
            task_id="wait_for_delete_bookings",
            dag_runs="{{result('delete_timeoff')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        process_logs = rail.EmptyOperator(
            task_id='process_logs'
        )

        send_logs_enter, send_logs_exit = get_send_logs(config)

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> dagrun_log_to_sumo
        can_run_batch_task >> rail.Label("No") >> logging_details
        logging_details >> create_log >> get_timeoff_bookings_from_hibob >> if_timeoff_bookings_exists
        if_timeoff_bookings_exists >> rail.Label("Yes") >> get_all_users_payload \
            >> get_all_users_from_hibob >> get_booking_id_oef_value >> book_timeoff >> wait_for_bookings \
            >> delete_timeoff >> wait_for_delete_bookings >> process_logs >> send_logs_enter
        send_logs_exit >> dagrun_log_to_sumo
        if_timeoff_bookings_exists >> rail.Label("No") >> send_no_new_timeoff_bookings_email >> dagrun_log_to_sumo

    return dag

rail.for_each_instance(create_dag)
