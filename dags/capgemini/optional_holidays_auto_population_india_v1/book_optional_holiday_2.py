from datetime import timedelta
from capgemini.optional_holidays_auto_population_india_v1.utils import request_payload, custom_methods
from capgemini.optional_holidays_auto_population_india_v1.utils.response_filter import get_user_timeoff_bookings, get_timeoff_booking_uri, get_user_current_holiday_calendar
from airflow.models import Variable
from rail.lib.ecid import get_dagrun_ecid
import rail

null = None

# pylint: disable=too-many-statements
def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.booking_child_dagid_2,
        description=f'Capgemini Auto Population of Optional Holidays India Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_holiday_booking_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
            'retries': 0
        }
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_booking_child_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='view_dagrun_config'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='view_dagrun_config',
            end_task='batch_end',
        )

        view_dagrun_config = rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config")

        get_user_holiday_calendar = rail.RepliconServiceOperator(
            task_id='get_user_holiday_calendar',
            endpoint="/services/HolidayCalendarService2.svc/GetHolidayCalendarAssignmentScheduleForUserAndDateRange",
            data=lambda dag_run: request_payload.get_user_holiday_cal_payload(dag_run, config),
            data_handler=lambda response: get_user_current_holiday_calendar(
                response, config.states_optional_holiday_calendars)
        )

        is_optional_holiday_calendar = rail.IfOperator(
            task_id='is_optional_holiday_calendar',
            test='{{ result("get_user_holiday_calendar") | is_truthy }}',
            yes_task='get_timeoff_booking_for_booking_date',
            no_task='log_not_optional_holiday_calendar'
        )

        log_not_optional_holiday_calendar = rail.WriteLogOperator(
            task_id='log_not_optional_holiday_calendar',
            log='{{ dag_run.conf.log_artifact }}',
            message='User is not assigned with optional holiday calendar',
            severity='Skipped',
            properties=lambda dag_run: {
                "state": dag_run.conf["properties"]["state_name"] if dag_run.conf["properties"]["state_name"] else '',
                "username": dag_run.conf["user_data"]["username"] if dag_run.conf["user_data"]["username"] else '',
                "employee_id": dag_run.conf["user_data"]["employee_id"] if dag_run.conf["user_data"]["employee_id"] else '',
                "booking_date": dag_run.conf["optional_holiday_booking_date"],
                "status": "Skipped",
                "comments": 'User is not assigned with optional holiday calendar'
            }
        )

        get_timeoff_booking_for_booking_date = rail.RepliconServiceOperator(
            task_id='get_timeoff_booking_for_booking_date',
            endpoint="/services/TimeOffService1.svc/GetTimeOffDetailsForUserAndDateRange2",
            data=request_payload.get_timeoff_booking_payload,
            data_handler=lambda response: get_user_timeoff_bookings(
                response, config.excepted_timeoff_types_mapper)
        )

        is_bookings_available_for_given_timeoff_types = rail.IfOperator(
            task_id='is_bookings_available_for_given_timeoff_types',
            test=lambda: len(rail.result(
                "get_timeoff_booking_for_booking_date")) > 0,
            yes_task='log_booking_already_present',
            no_task='put_and_submit_timeoff_booking_for_user'
        )

        log_booking_already_present = rail.WriteLogOperator(
            task_id='log_booking_already_present',
            log='{{ dag_run.conf.log_artifact }}',
            message='Time Off Booking already available for the time off type "{{ result("get_timeoff_booking_for_booking_date")[0].timeoff_type }}"'
                + ' on "{{ dag_run.conf.optional_holiday_booking_date }}"',
            severity='Skipped',
            properties=lambda dag_run: {
                "state": dag_run.conf["properties"]["state_name"] if dag_run.conf["properties"]["state_name"] else '',
                "username": dag_run.conf["user_data"]["username"] if dag_run.conf["user_data"]["username"] else '',
                "employee_id": dag_run.conf["user_data"]["employee_id"] if dag_run.conf["user_data"]["employee_id"] else '',
                "booking_date": dag_run.conf["optional_holiday_booking_date"],
                "status": "Skipped",
                "comments": 'Time Off Booking already available for the time off type "'
                    + rail.result("get_timeoff_booking_for_booking_date")[0]["timeoff_type"]
                + '" on "' +
                    dag_run.conf["optional_holiday_booking_date"] + '"'
            }
        )

        put_and_submit_timeoff_booking_for_user = rail.RepliconServiceOperator(
            task_id='put_and_submit_timeoff_booking_for_user',
            endpoint="/services/TimeOffApprovalService1.svc/PutAndSubmitTimeOff",
            data=request_payload.get_put_holiday_payload
        )
        process_timeoff_booking = rail.IfOperator(
            task_id='process_timeoff_booking',
            trigger_rule='all_done',
            test=custom_methods.check_status,
            yes_task='get_timeoff_uri'
        )
        get_timeoff_uri = rail.PythonOperator(
            task_id='get_timeoff_uri',
            python_callable=get_timeoff_booking_uri
        )
        is_put_submit_success = rail.IfOperator(
            task_id='is_put_submit_success',
            test="{{ get_task_state('put_and_submit_timeoff_booking_for_user').lower() == 'success' }}",
            yes_task='log_booking_successful',
            no_task='is_put_submit_failed'
        )
        is_put_submit_failed = rail.IfOperator(
            task_id='is_put_submit_failed',
            test="{{ get_task_state('put_and_submit_timeoff_booking_for_user').lower() == 'failed' }}",
            yes_task='is_timeoff_uri_present'
        )
        is_timeoff_uri_present = rail.IfOperator(
            task_id='is_timeoff_uri_present',
            test="{{ result('get_timeoff_uri') | is_truthy }}",
            yes_task='log_booking_failure_reason',
            no_task='fail_task'
        )
        fail_task = rail.FailOperator(
            task_id='fail_task',
            message=lambda: rail.result(
                "put_and_submit_timeoff_booking_for_user", key='error')
        )
        log_booking_failure_reason = rail.WriteLogOperator(
            task_id='log_booking_failure_reason',
            log='{{ dag_run.conf.log_artifact }}',
            message=custom_methods.get_failure_reason,
            severity='Error',
            properties=lambda dag_run: {
                "state": dag_run.conf["properties"]["state_name"] if dag_run.conf["properties"]["state_name"] else '',
                "username": dag_run.conf["user_data"]["username"] if dag_run.conf["user_data"]["username"] else '',
                "employee_id": dag_run.conf["user_data"]["employee_id"] if dag_run.conf["user_data"]["employee_id"] else '',
                "booking_date": dag_run.conf["optional_holiday_booking_date"],
                "status": "Error",
                "comments": custom_methods.get_failure_reason()
            }
        )

        log_booking_successful = rail.WriteLogOperator(
            task_id='log_booking_successful',
            log='{{ dag_run.conf.log_artifact }}',
            message='Optional Holiday Booked Successfully',
            severity='Success',
            properties=lambda dag_run: {
                "state": dag_run.conf["properties"]["state_name"] if dag_run.conf["properties"]["state_name"] else '',
                "username": dag_run.conf["user_data"]["username"] if dag_run.conf["user_data"]["username"] else '',
                "employee_id": dag_run.conf["user_data"]["employee_id"] if dag_run.conf["user_data"]["employee_id"] else '',
                "booking_date": dag_run.conf["optional_holiday_booking_date"],
                "status": "Success",
                "comments": 'Optional Holiday Booked Successfully'
            }
        )

        empty_task = rail.EmptyOperator(
            task_id='empty_task'
        )

        batch_end = rail.EmptyOperator(
            task_id='batch_end'
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            trigger_rule='all_done',
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
            no_task='catch_and_log_errors'
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf.log_artifact }}',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error',
            properties=lambda dag_run: {
                "state": dag_run.conf["properties"]["state_name"] if dag_run.conf["properties"]["state_name"] else '',
                "username": dag_run.conf["user_data"]["username"] if dag_run.conf["user_data"]["username"] else '',
                "employee_id": dag_run.conf["user_data"]["employee_id"] if dag_run.conf["user_data"]["employee_id"] else '',
                "booking_date": dag_run.conf["optional_holiday_booking_date"],
                "status": "Error",
                "comments": '{{ get_error_message() }}'
            }
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> batch_end
        can_run_batch_task >> rail.Label(
            "No") >> view_dagrun_config >> get_user_holiday_calendar >> is_optional_holiday_calendar

        is_optional_holiday_calendar >> rail.Label("Yes") >> get_timeoff_booking_for_booking_date
        is_optional_holiday_calendar >> rail.Label("No") >> log_not_optional_holiday_calendar >> batch_end

        get_timeoff_booking_for_booking_date >> is_bookings_available_for_given_timeoff_types
        is_bookings_available_for_given_timeoff_types >> rail.Label(
        "No") >> put_and_submit_timeoff_booking_for_user \
            >> process_timeoff_booking >> rail.Label("Yes") >> get_timeoff_uri >> is_put_submit_success
        is_put_submit_success >> rail.Label(
            "Yes") >> log_booking_successful >> empty_task
        is_put_submit_success >> rail.Label("No") >> is_put_submit_failed
        is_put_submit_failed >> rail.Label("Yes") >> is_timeoff_uri_present
        is_timeoff_uri_present >> rail.Label(
            "Yes") >> log_booking_failure_reason >> empty_task
        is_timeoff_uri_present >> rail.Label(
            "No") >> fail_task >> empty_task
        empty_task >> batch_end

        is_bookings_available_for_given_timeoff_types >> rail.Label(
            "Yes") >> log_booking_already_present >> batch_end

        batch_end >> can_fail_dag >> rail.Label("Yes") >> fail_dagrun >> catch_and_log_errors
        can_fail_dag >> rail.Label("No") >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
