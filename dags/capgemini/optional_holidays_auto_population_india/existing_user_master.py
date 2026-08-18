from datetime import timedelta
from pendulum import datetime
import pendulum
from capgemini.optional_holidays_auto_population_india.utils import custom_methods
from capgemini.optional_holidays_auto_population_india.utils import response_filter
from capgemini.optional_holidays_auto_population_india.utils import request_payload
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'capgemini_auto_population_of_optional_holidays_india_existing_users_master_{config.instance}',
        description=f'Capgemini Auto Population of Optional Holidays India for Existing Users Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2023, 7, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
            'retries': 0
        },
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
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
            op_args=[config, "existing_user"]
        )

        get_all_holiday_calendars = rail.RepliconServiceOperator(
            task_id='get_all_holiday_calendars',
            endpoint='/services/HolidayCalendarService1.svc/GetAllHolidayCalendars',
            data_handler=response_filter.get_holiday_calendar_list
        )

        get_allowed_location_uris = rail.RepliconServiceOperator(
            task_id='get_allowed_location_uris',
            endpoint='/services/LocationService1.svc/GetEnabledLocations',
            data_handler=response_filter.get_allowed_locations_uris
        )

        get_specfic_time_off_type = rail.RepliconServiceOperator(
            task_id='get_specfic_time_off_type',
            endpoint='/services/TimeOffService1.svc/GetAllTimeOffTypes',
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', config.optional_holiday_timeoff_type_name, 'uri')
        )

        is_optional_holiday_present = rail.IfOperator(
            task_id='is_optional_holiday_present',
            test='{{ result("get_specfic_time_off_type") | is_truthy }}',
            yes_task='get_states_not_available',
            no_task='send_no_optional_holiday_timeoff_email'
        )

        send_no_optional_holiday_timeoff_email = rail.EmailOperator(
            task_id='send_no_optional_holiday_timeoff_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | The Auto population of Optional holiday booking is skipped - {{ result("logging_details").process_start_time }}',
            html_content='/templates/emails/not_available_in_replicon.html',
            params={
                "type": "timeoff_type",
                "timeoff_type": config.optional_holiday_timeoff_type_name
            }
        )

        get_states_not_available = rail.PythonOperator(
            task_id='get_states_not_available',
            python_callable=custom_methods.get_unavailable_states
        )

        is_not_available_states_exist = rail.IfOperator(
            task_id='is_not_available_states_exist',
            test=lambda: len(rail.result("get_states_not_available")) > 0,
            yes_task='send_states_not_available_email',
            no_task='get_holiday_cal_not_available'
        )

        send_states_not_available_email = rail.EmailOperator(
            task_id='send_states_not_available_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | The Auto population of Optional holiday booking is skipped for some states - \
                {{ " " + result("logging_details").process_start_time }}',
            html_content='/templates/emails/not_available_in_replicon.html',
            params={
                "type": "states"
            }
        )

        get_holiday_cal_not_available = rail.PythonOperator(
            task_id='get_holiday_cal_not_available',
            python_callable=custom_methods.get_unavailable_holiday_calendars
        )

        is_not_available_holiday_cal_exist = rail.IfOperator(
            task_id='is_not_available_holiday_cal_exist',
            test=lambda: len(rail.result("get_holiday_cal_not_available")) > 0,
            yes_task='send_holiday_cals_not_available_email',
            no_task='process_optional_holidays'
        )

        send_holiday_cals_not_available_email = rail.EmailOperator(
            task_id='send_holiday_cals_not_available_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | The Auto population of Optional holiday booking is skipped for some states - \
                {{ " " + result("logging_details").process_start_time }}',
            html_content='/templates/emails/not_available_in_replicon.html',
            params={
                "type": "holiday_cal"
            }
        )

        process_optional_holidays = rail.TriggerDagRunForEachItemOperator(
            task_id='process_optional_holidays',
            items='{{ result("get_allowed_location_uris") | to_json }}',
            trigger_dag_id=f'capgemini_process_existing_users_optional_holidays_child_{config.instance}',
            conf=lambda item, index: {
                "master_index": index,
                "properties": {
                    "optional_holiday_timeoff_type_name": config.optional_holiday_timeoff_type_name,
                    "optional_holiday_timeoff_uri": rail.result("get_specfic_time_off_type"),
                    "state_name": item["state_name"],
                    "state_uri": item["state_uri"],
                    "optional_holiday_calendar_name": item["optional_holiday_cal_name"],
                    "optional_holiday_calendar_uri": item["optional_holiday_cal_uri"],
                    "schedule": "E1" if pendulum.now(config.time_zone).strftime("%m/%d") == config.e1_schedule
                        else ("E2" if pendulum.now(config.time_zone).strftime("%m/%d") == config.e2_schedule else null),
                    "time_zone": config.time_zone,
                    "process_start_time": rail.result("logging_details")["process_start_time"]
                }
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> dagrun_log_to_sumo
        can_run_batch_task >> rail.Label("No") >> logging_details

        logging_details >> get_all_holiday_calendars >> get_allowed_location_uris >> get_specfic_time_off_type \
            >> is_optional_holiday_present

        is_optional_holiday_present >> rail.Label(
            "Yes") >> get_states_not_available >> is_not_available_states_exist

        is_not_available_states_exist >> rail.Label("Yes") >> send_states_not_available_email >> get_holiday_cal_not_available \
            >> is_not_available_holiday_cal_exist
        is_not_available_states_exist >> rail.Label(
            "No") >> get_holiday_cal_not_available

        is_not_available_holiday_cal_exist >> rail.Label(
            "Yes") >> send_holiday_cals_not_available_email >> process_optional_holidays
        is_not_available_holiday_cal_exist >> rail.Label(
            "No") >> process_optional_holidays >> dagrun_log_to_sumo

        is_optional_holiday_present >> rail.Label(
            "No") >> send_no_optional_holiday_timeoff_email >> dagrun_log_to_sumo

        dagrun_log_to_sumo >> can_fail_dag >> rail.Label("Yes") >> fail_dagrun

    return dag


rail.for_each_instance(create_dag)
