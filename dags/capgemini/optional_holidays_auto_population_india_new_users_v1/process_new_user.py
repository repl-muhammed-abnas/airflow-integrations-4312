from datetime import timedelta
from capgemini.optional_holidays_auto_population_india_new_users_v1.utils import response_filter
from capgemini.optional_holidays_auto_population_india_new_users_v1.utils import request_payload
from capgemini.optional_holidays_auto_population_india_new_users_v1.utils import python_callable
from airflow.models import Variable
from rail.lib.ecid import get_dagrun_ecid

import rail

null = None

# pylint:disable = too-many-statements


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_new_users_dagid,
        description=f'Capgemini Auto Population of Optional Holidays India for New Users process v1 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_new_users,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
            'retries': 0
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_user_info'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_user_info',
            end_task='catch_and_log_errors',
        )

        get_user_info = rail.RepliconServiceOperator(
            task_id='get_user_info',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data={
                "users": [
                    {
                        "uri": '{{ dag_run.conf.user_uri }}',
                        "loginName": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": null
            },
            data_handler=lambda res: res[0] if len(
                res) > 0 and res[0]["userDetails"]["uri"] else null
        )

        is_user_present = rail.IfOperator(
            task_id='is_user_present',
            test='{{ result("get_user_info") | is_truthy }}',
            yes_task='is_holiday_calendar_assigned',
            no_task='log_user_not_present'
        )

        log_user_not_present = rail.WriteLogOperator(
            task_id='log_user_not_present',
            log='{{ dag_run.conf.log_artifact}}',
            message="User not present in Replicon",
            severity='Skipped',
            properties=lambda dag_run: {
                "state": null,
                "username": dag_run.conf["user_name"],
                "employee_id": null,
                "booking_date": null,
                "status": "Skipped",
                "comments": "User not present in Replicon"
            }
        )

        is_holiday_calendar_assigned = rail.IfOperator(
            task_id='is_holiday_calendar_assigned',
            test='{{ result("get_user_info").holidayCalendar | is_truthy }}',
            yes_task='is_start_date_present',
            no_task='log_hoilday_calendar_not_assigned'
        )

        log_hoilday_calendar_not_assigned = rail.WriteLogOperator(
            task_id='log_hoilday_calendar_not_assigned',
            log='{{ dag_run.conf.log_artifact}}',
            message="Holiday calendar is not assigned to the user",
            severity='Skipped',
            properties=lambda: {
                "state": null,
                "username": rail.result("get_user_info")["userDetails"]["displayText"],
                "employee_id": rail.result("get_user_info")["userDetails"]["employeeId"],
                "booking_date": null,
                "status": "Skipped",
                "comments": "Holiday calendar is not assigned to the user"
            }
        )

        is_start_date_present = rail.IfOperator(
            task_id='is_start_date_present',
            test='{{ result("get_user_info").userDetails.employmentDateRange.startDate | is_truthy }}',
            yes_task='get_schedule_based_on_daterange',
            no_task='log_start_date_not_present'
        )

        log_start_date_not_present = rail.WriteLogOperator(
            task_id='log_start_date_not_present',
            log='{{ dag_run.conf.log_artifact}}',
            message="User's start date not present",
            severity='Skipped',
            properties=lambda: {
                "state": null,
                "username": rail.result("get_user_info")["userDetails"]["displayText"],
                "employee_id": rail.result("get_user_info")["userDetails"]["employeeId"],
                "booking_date": null,
                "status": "Skipped",
                "comments": "User's start date not present"
            }
        )

        get_schedule_based_on_daterange = rail.PythonOperator(
            task_id='get_schedule_based_on_daterange',
            python_callable=lambda: python_callable.get_schedule_on_daterange(
                config)
        )

        is_user_start_date_in_feb_to_june = rail.IfOperator(
            task_id='is_user_start_date_in_feb_to_june',
            test='{{ result("get_schedule_based_on_daterange").schedule == "E1" }}',
            yes_task='get_specfic_time_off_type',
            no_task='is_user_start_date_in_aug_to_dec'
        )

        is_user_start_date_in_aug_to_dec = rail.IfOperator(
            task_id='is_user_start_date_in_aug_to_dec',
            test='{{ result("get_schedule_based_on_daterange").schedule == "E2" }}',
            yes_task='get_specfic_time_off_type',
            no_task='log_start_date_not_in_daterange'
        )

        log_start_date_not_in_daterange = rail.WriteLogOperator(
            task_id='log_start_date_not_in_daterange',
            log='{{ dag_run.conf.log_artifact}}',
            message=lambda: python_callable.get_log_message(
                "start_date_not_in_range"),
            severity='Skipped',
            properties=lambda: {
                "state": null,
                "username": rail.result("get_user_info")["userDetails"]["displayText"],
                "employee_id": rail.result("get_user_info")["userDetails"]["employeeId"],
                "booking_date": null,
                "status": "Skipped",
                "comments": python_callable.get_log_message("start_date_not_in_range")
            }
        )

        get_specfic_time_off_type = rail.RepliconServiceOperator(
            task_id='get_specfic_time_off_type',
            endpoint='/services/TimeOffService1.svc/GetAllTimeOffTypes',
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', config.optional_holiday_timeoff_type_name, 'uri')
        )

        is_optional_holiday_timeoff_type_present = rail.IfOperator(
            task_id='is_optional_holiday_timeoff_type_present',
            test='{{ result("get_specfic_time_off_type") | is_truthy }}',
            yes_task='is_user_location_present',
            no_task='log_optional_holiday_not_present'
        )

        log_optional_holiday_not_present = rail.WriteLogOperator(
            task_id='log_optional_holiday_not_present',
            log='{{ dag_run.conf.log_artifact}}',
            message='Time Off type "'+config.optional_holiday_timeoff_type_name +
                '" not available in Replicon',
            severity='Skipped',
            properties=lambda: {
                "state": null,
                "username": rail.result("get_user_info")["userDetails"]["displayText"],
                "employee_id": rail.result("get_user_info")["userDetails"]["employeeId"],
                "booking_date": null,
                "status": "Skipped",
                "comments": 'Time Off type "'+config.optional_holiday_timeoff_type_name+'" not available in Replicon'
            }
        )

        is_user_location_present = rail.IfOperator(
            task_id='is_user_location_present',
            test=lambda: len(rail.result("get_user_info")
                             ["locationSchedule"]) > 0,
            yes_task='get_user_parent_location',
            no_task='log_no_user_location'
        )

        log_no_user_location = rail.WriteLogOperator(
            task_id='log_no_user_location',
            log='{{ dag_run.conf.log_artifact}}',
            message='User location not found',
            severity='Skipped',
            properties=lambda: {
                "state": null,
                "username": rail.result("get_user_info")["userDetails"]["displayText"],
                "employee_id": rail.result("get_user_info")["userDetails"]["employeeId"],
                "booking_date": null,
                "status": "Skipped",
                "comments": "User location not found"
            }
        )

        get_user_parent_location = rail.RepliconServiceOperator(
            task_id='get_user_parent_location',
            endpoint='/services/LocationListService1.svc/GetHierarchyData',
            data=lambda: request_payload.get_location_hierarchy_payload(rail.result(
                "get_user_info")["locationSchedule"][-1]["location"]["displayText"]),
            data_handler=response_filter.get_user_parent_location
        )

        check_if_india_is_parent_location = rail.IfOperator(
            task_id='check_if_india_is_parent_location',
            test=lambda: len(rail.result("get_user_parent_location")) > 0,
            yes_task='get_user_holiday_calendar',
            no_task='log_user_not_india'
        )

        log_user_not_india = rail.WriteLogOperator(
            task_id='log_user_not_india',
            log='{{ dag_run.conf.log_artifact}}',
            message='User does not belong to India',
            severity='Skipped',
            properties=lambda: {
                "state": null,
                "username": rail.result("get_user_info")["userDetails"]["displayText"],
                "employee_id": rail.result("get_user_info")["userDetails"]["employeeId"],
                "booking_date": null,
                "status": "Skipped",
                "comments": "User does not belong to India"
            }
        )

        get_user_holiday_calendar = rail.RepliconServiceOperator(
            task_id='get_user_holiday_calendar',
            endpoint="/services/HolidayCalendarService2.svc/GetHolidayCalendarAssignmentScheduleForUserAndDateRange",
            data=lambda dag_run: request_payload.get_user_holiday_cal_payload(dag_run, config),
            data_handler=lambda response: response_filter.get_user_current_holiday_calendar(
                response, config.states_optional_holiday_calendars)
        )

        is_optional_holiday_calendar_exists_for_assigned = rail.IfOperator(
            task_id='is_optional_holiday_calendar_exists_for_assigned',
            test='{{ result("get_user_holiday_calendar") | is_truthy }}',
            yes_task='get_all_holiday_calendars',
            no_task='log_not_optional_holiday_calendar'
        )

        log_not_optional_holiday_calendar = rail.WriteLogOperator(
            task_id='log_not_optional_holiday_calendar',
            log='{{ dag_run.conf.log_artifact }}',
            message='User is not assigned with optional holiday calendar',
            severity='Skipped',
            properties=lambda: {
                "state": null,
                "username": rail.result("get_user_info")["userDetails"]["displayText"],
                "employee_id": rail.result("get_user_info")["userDetails"]["employeeId"],
                "booking_date": null,
                "status": "Skipped",
                "comments": 'User is not assigned with optional holiday calendar'
            }
        )

        get_all_holiday_calendars = rail.RepliconServiceOperator(
            task_id='get_all_holiday_calendars',
            endpoint='/services/HolidayCalendarService1.svc/GetAllHolidayCalendars',
            data_handler=response_filter.get_optional_holiday_for_user
        )

        is_optional_holiday_calendar_present = rail.IfOperator(
            task_id='is_optional_holiday_calendar_present',
            test=lambda: len(rail.result("get_all_holiday_calendars")) > 0,
            yes_task='is_booking_allowed_on_holiday_calendar',
            no_task='log_no_optional_holiday_calendar'
        )

        log_no_optional_holiday_calendar = rail.WriteLogOperator(
            task_id='log_no_optional_holiday_calendar',
            log='{{ dag_run.conf.log_artifact}}',
            message='Optional holiday calendar \"{{ result("get_user_info").holidayCalendar.name }}_Optional\" not available in Replicon',
            severity='Skipped',
            properties=lambda: {
                "state": null,
                "username": rail.result("get_user_info")["userDetails"]["displayText"],
                "employee_id": rail.result("get_user_info")["userDetails"]["employeeId"],
                "booking_date": null,
                "status": "Skipped",
                "comments": 'Optional holiday calendar \"{{ result("get_user_info").holidayCalendar.name }}_Optional\" not available in Replicon'
            }
        )

        is_booking_allowed_on_holiday_calendar = rail.IfOperator(
            task_id='is_booking_allowed_on_holiday_calendar',
            test=python_callable.is_booking_allowed,
            yes_task='get_timeoff_balance',
            no_task='log_booking_not_allowed'
        )

        log_booking_not_allowed = rail.WriteLogOperator(
            task_id='log_booking_not_allowed',
            log='{{ dag_run.conf.log_artifact}}',
            message='User not allowed for optional booking',
            severity='Skipped',
            properties=lambda: {
                "state": null,
                "username": rail.result("get_user_info")["userDetails"]["displayText"],
                "employee_id": rail.result("get_user_info")["userDetails"]["employeeId"],
                "booking_date": null,
                "status": "Skipped",
                "comments": 'User not allowed for optional booking'
            }
        )

        get_timeoff_balance = rail.RepliconServiceOperator(
            task_id='get_timeoff_balance',
            endpoint='/services/TimeOffService2.svc/GetBalanceSummaryForAccount',
            data=lambda: request_payload.get_timeoff_balance_payload(
                config.time_zone)
        )

        check_enough_balance_to_book = rail.IfOperator(
            task_id='check_enough_balance_to_book',
            test=lambda: rail.result("get_timeoff_balance")["timeRemaining"] > (
                1 if rail.result("get_schedule_based_on_daterange")["schedule"] == "E1" else 0),
            yes_task='get_bookable_holidays_in_date_range',
            no_task='log_user_holiday_balance'
        )

        # pylint:disable = comparison-of-constants
        log_user_holiday_balance = rail.WriteLogOperator(
            task_id='log_user_holiday_balance',
            log='{{ dag_run.conf.log_artifact}}',
            message='User\'s holiday balance is not greater than 1. Already booked holiday for the daterange'
            if '{{ result("get_schedule_based_on_daterange").schedule }}' == "E1"
            else 'User\'s holiday balance is not greater than 0. So, No balance available to book holiday',
            severity='Skipped',
            properties=lambda: {
                "state": null,
                "username": rail.result("get_user_info")["userDetails"]["displayText"],
                "employee_id": rail.result("get_user_info")["userDetails"]["employeeId"],
                "booking_date": null,
                "status": "Skipped",
                "comments": 'User\'s holiday balance is not greater than 1. Already booked holiday for the daterange'
                if '{{ result("get_schedule_based_on_daterange").schedule }}' == "E1"
                else 'User\'s holiday balance is not greater than 0. So, No balance available to book holiday'
            }
        )

        get_bookable_holidays_in_date_range = rail.RepliconServiceOperator(
            task_id='get_bookable_holidays_in_date_range',
            endpoint='/services/HolidayCalendarService2.svc/GetHolidaysInDateRange',
            data=lambda: request_payload.new_user_holiday_bookings_in_daterange_payload(
                config),
            data_handler=response_filter.get_holidays_list
        )

        check_for_bookable_holidays = rail.IfOperator(
            task_id='check_for_bookable_holidays',
            test=lambda: len(rail.result(
                "get_bookable_holidays_in_date_range")) > 0,
            yes_task='check_for_multiple_bookable_holidays',
            no_task='log_no_bookable_holiday'
        )

        log_no_bookable_holiday = rail.WriteLogOperator(
            task_id='log_no_bookable_holiday',
            log='{{ dag_run.conf.log_artifact}}',
            message=lambda: python_callable.get_log_message(
                "no_bookable_holiday"),
            severity='Skipped',
            properties=lambda: {
                "state": null,
                "username": rail.result("get_user_info")["userDetails"]["displayText"],
                "employee_id": rail.result("get_user_info")["userDetails"]["employeeId"],
                "booking_date": null,
                "status": "Skipped",
                "comments": python_callable.get_log_message("no_bookable_holiday")
            }
        )

        check_for_multiple_bookable_holidays = rail.IfOperator(
            task_id='check_for_multiple_bookable_holidays',
            test=lambda: len(rail.result(
                "get_bookable_holidays_in_date_range")) > 1,
            yes_task='log_multiple_bookable_holiday',
            no_task='is_bookable_date_is_less_than_start_date'
        )

        log_multiple_bookable_holiday = rail.WriteLogOperator(
            task_id='log_multiple_bookable_holiday',
            log='{{ dag_run.conf.log_artifact}}',
            message=lambda: python_callable.get_log_message(
                "multiple_bookable_holiday"),
            severity='Skipped',
            properties=lambda: {
                "state": null,
                "username": rail.result("get_user_info")["userDetails"]["displayText"],
                "employee_id": rail.result("get_user_info")["userDetails"]["employeeId"],
                "booking_date": null,
                "status": "Skipped",
                "comments": python_callable.get_log_message("multiple_bookable_holiday")
            }
        )

        is_bookable_date_is_less_than_start_date = rail.IfOperator(
            task_id='is_bookable_date_is_less_than_start_date',
            test=python_callable.check_if_bookable_date_is_less_than_start_date,
            yes_task='log_bookable_date_less_than_start_date',
            no_task='is_bookable_date_is_start_date'
        )

        log_bookable_date_less_than_start_date = rail.WriteLogOperator(
            task_id='log_bookable_date_less_than_start_date',
            log='{{ dag_run.conf.log_artifact}}',
            message='System booked holiday date is before the user\'s start date',
            severity='Skipped',
            properties=lambda: {
                "state": null,
                "username": rail.result("get_user_info")["userDetails"]["displayText"],
                "employee_id": rail.result("get_user_info")["userDetails"]["employeeId"],
                "booking_date": null,
                "status": "Skipped",
                "comments": "System booked holiday date is before the user\'s start date"
            }
        )

        is_bookable_date_is_start_date = rail.IfOperator(
            task_id='is_bookable_date_is_start_date',
            test=python_callable.check_if_bookable_date_is_start_date,
            yes_task='log_start_date_is_same_as_bookable',
            no_task='book_optional_holiday'
        )

        log_start_date_is_same_as_bookable = rail.WriteLogOperator(
            task_id='log_start_date_is_same_as_bookable',
            log='{{ dag_run.conf.log_artifact}}',
            message='User start date and system booked holiday is same',
            severity='Skipped',
            properties=lambda: {
                "state": null,
                "username": rail.result("get_user_info")["userDetails"]["displayText"],
                "employee_id": rail.result("get_user_info")["userDetails"]["employeeId"],
                "booking_date": null,
                "status": "Skipped",
                "comments": "User start date and system booked holiday is same"
            }
        )

        book_optional_holiday = rail.TriggerDagRunOperator(
            task_id='book_optional_holiday',
            trigger_dag_id=config.booking_child_dagid,
            conf=lambda dag_run: {
                "properties": {
                    "optional_holiday_timeoff_uri": rail.result("get_specfic_time_off_type"),
                    "state_name": null
                },
                "user_data": {
                    "user_uri": rail.result("get_user_info")["userDetails"]["uri"],
                    "username": rail.result("get_user_info")["userDetails"]["displayText"],
                    "employee_id": rail.result("get_user_info")["userDetails"]["employeeId"]
                },
                "optional_holiday_booking_date_json": request_payload.get_optional_holiday_booking_date(),
                "optional_holiday_booking_date": rail.result("get_bookable_holidays_in_date_range")[0]["holiday_date"],
                "log_artifact": dag_run.conf["log_artifact"]
            }
        )

        wait_for_booking = rail.WaitForDagRunsSensor(
            task_id="wait_for_booking",
            dag_runs="{{result('book_optional_holiday')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf.log_artifact}}',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error',
            properties=lambda: {
                "state": null,
                "username": rail.result("get_user_info")["userDetails"]["displayText"],
                "employee_id": rail.result("get_user_info")["userDetails"]["employeeId"],
                "booking_date": null,
                "status": "Error",
                "comments": '{{ get_error_message() }}'
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> get_user_info

        get_user_info >> is_user_present
        is_user_present >> rail.Label("Yes") >> is_holiday_calendar_assigned
        is_user_present >> rail.Label(
            "No") >> log_user_not_present >> catch_and_log_errors
        
        is_holiday_calendar_assigned >> rail.Label(
            "Yes") >> is_start_date_present
        is_holiday_calendar_assigned >> rail.Label(
            "No") >> log_hoilday_calendar_not_assigned >> catch_and_log_errors

        is_start_date_present >> rail.Label(
            "Yes") >> get_schedule_based_on_daterange >> is_user_start_date_in_feb_to_june
        is_start_date_present >> rail.Label(
            "No") >> log_start_date_not_present >> catch_and_log_errors

        is_user_start_date_in_feb_to_june >> rail.Label(
            "Yes") >> get_specfic_time_off_type >> is_optional_holiday_timeoff_type_present
        is_user_start_date_in_feb_to_june >> rail.Label(
            "No") >> is_user_start_date_in_aug_to_dec

        is_user_start_date_in_aug_to_dec >> rail.Label(
            "Yes") >> get_specfic_time_off_type
        is_user_start_date_in_aug_to_dec >> rail.Label(
            "No") >> log_start_date_not_in_daterange >> catch_and_log_errors

        is_optional_holiday_timeoff_type_present >> rail.Label(
            "Yes") >> is_user_location_present
        is_optional_holiday_timeoff_type_present >> rail.Label(
            "No") >> log_optional_holiday_not_present >> catch_and_log_errors

        is_user_location_present >> rail.Label(
            "Yes") >> get_user_parent_location >> check_if_india_is_parent_location
        is_user_location_present >> rail.Label(
            "No") >> log_no_user_location >> catch_and_log_errors

        check_if_india_is_parent_location >> rail.Label(
            "Yes") >> get_user_holiday_calendar
        get_user_holiday_calendar >> is_optional_holiday_calendar_exists_for_assigned
        is_optional_holiday_calendar_exists_for_assigned >> rail.Label("Yes") >> get_all_holiday_calendars
        is_optional_holiday_calendar_exists_for_assigned >> rail.Label("No") >> log_not_optional_holiday_calendar >> catch_and_log_errors
        check_if_india_is_parent_location >> rail.Label(
            "No") >> log_user_not_india >> catch_and_log_errors

        get_all_holiday_calendars >> is_optional_holiday_calendar_present

        is_optional_holiday_calendar_present >> rail.Label(
            "Yes") >> is_booking_allowed_on_holiday_calendar
        is_optional_holiday_calendar_present >> rail.Label(
            "No") >> log_no_optional_holiday_calendar >> catch_and_log_errors

        is_booking_allowed_on_holiday_calendar >> rail.Label(
            "Yes") >> get_timeoff_balance >> check_enough_balance_to_book
        is_booking_allowed_on_holiday_calendar >> rail.Label(
            "No") >> log_booking_not_allowed >> catch_and_log_errors

        check_enough_balance_to_book >> rail.Label(
            "Yes") >> get_bookable_holidays_in_date_range >> check_for_bookable_holidays
        check_enough_balance_to_book >> rail.Label(
            "No") >> log_user_holiday_balance >> catch_and_log_errors

        check_for_bookable_holidays >> rail.Label(
            "Yes") >> check_for_multiple_bookable_holidays
        check_for_bookable_holidays >> rail.Label(
            "No") >> log_no_bookable_holiday >> catch_and_log_errors

        check_for_multiple_bookable_holidays >> rail.Label(
            "Yes") >> log_multiple_bookable_holiday >> catch_and_log_errors
        check_for_multiple_bookable_holidays >> rail.Label(
            "No") >> is_bookable_date_is_less_than_start_date

        is_bookable_date_is_less_than_start_date >> rail.Label(
            "Yes") >> log_bookable_date_less_than_start_date >> catch_and_log_errors
        is_bookable_date_is_less_than_start_date >> rail.Label(
            "No") >> is_bookable_date_is_start_date

        is_bookable_date_is_start_date >> rail.Label(
            "Yes") >> log_start_date_is_same_as_bookable >> catch_and_log_errors
        is_bookable_date_is_start_date >> rail.Label(
            "No") >> book_optional_holiday >> wait_for_booking >> catch_and_log_errors
        catch_and_log_errors >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_child_dag)
