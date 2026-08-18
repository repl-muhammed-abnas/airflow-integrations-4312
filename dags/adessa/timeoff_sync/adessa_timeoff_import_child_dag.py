from adessa.timeoff_sync.utils import request_payload
from adessa.timeoff_sync.tasks.create_timoff_booking import get_create_timeoff
from adessa.timeoff_sync.utils import custom_methods
import rail

null = None

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'adessa_timeoff_sync_import_child_{config.instance}',
        description=f'Adessa Timeoff import Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_child_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        if_timeoff_hrs_less_than_0_1 = rail.IfOperator(
            task_id='if_timeoff_hrs_less_than_0_1',
            test=lambda dag_run: float(dag_run.conf["timeoff_hrs"]["hours"] + "." + dag_run.conf["timeoff_hrs"]["minutes"]) < 0.1,
            yes_task="log_timeoff_duration_not_present_skipped",
            no_task="if_request_useruri_not_present",
        )

        log_timeoff_duration_not_present_skipped = rail.WriteLogOperator(
            task_id='log_timeoff_duration_not_present_skipped',
            log="{{ dag_run.conf.log }}",
            message="Skipped | Time off duration not present",
            severity="Skipped",
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employee_id }}",
                "timeofftype": "{{ dag_run.conf.timeoff_type }}",
                "startdate": "{{ dag_run.conf.start_date }}",
                "action": "{{ dag_run.conf.action }}",
                "status": "Skipped | Time off duration not present"
            }
        )

        if_request_useruri_not_present = rail.IfOperator(
            task_id='if_request_useruri_not_present',
            test='{{ dag_run.conf.user_uri | is_falsy }}',
            yes_task="log_user_not_present_skipped",
            no_task="if_request_timeoffuri_not_present",
        )

        log_user_not_present_skipped = rail.WriteLogOperator(
            task_id='log_user_not_present_skipped',
            log="{{ dag_run.conf.log }}",
            message="Skipped | User not present in Replicon",
            severity="Skipped",
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employee_id }}",
                "timeofftype": "{{ dag_run.conf.timeoff_type }}",
                "startdate": "{{ dag_run.conf.start_date }}",
                "action": "{{ dag_run.conf.action }}",
                "status": "Skipped | User not present in Replicon"
            }
        )

        if_request_timeoffuri_not_present = rail.IfOperator(
            task_id='if_request_timeoffuri_not_present',
            test='{{ dag_run.conf.timeoff_uri | is_falsy }}''',
            yes_task="log_timeoffuri_not_present_skipped",
            no_task="if_request_action_not_present",
        )

        log_timeoffuri_not_present_skipped = rail.WriteLogOperator(
            task_id='log_timeoffuri_not_present_skipped',
            log="{{ dag_run.conf.log }}",
            message="Skipped | Time Off Type not present/disabled in Replicon",
            severity="Skipped",
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employee_id }}",
                "timeofftype": "{{ dag_run.conf.timeoff_type }}",
                "startdate": "{{ dag_run.conf.start_date }}",
                "action": "{{ dag_run.conf.action }}",
                "status": "Skipped | Time Off Type not present/disabled in Replicon "
            }
        )

        if_request_action_not_present = rail.IfOperator(
            task_id='if_request_action_not_present',
            test='{{ dag_run.conf.action | is_falsy }}',
            yes_task="log_action_not_present_skipped",
            no_task="if_request_action_equals_to_c",
        )

        log_action_not_present_skipped = rail.WriteLogOperator(
            task_id='log_action_not_present_skipped',
            log="{{ dag_run.conf.log }}",
            message="Skipped | Time Off action is not present",
            severity="Skipped",
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employee_id }}",
                "timeofftype": "{{ dag_run.conf.timeoff_type }}",
                "startdate": "{{ dag_run.conf.start_date }}",
                "action": "{{ dag_run.conf.action }}",
                "status": "Skipped | Time Off action is not present"
            }
        )

        if_request_action_equals_to_c = rail.IfOperator(
            task_id='if_request_action_equals_to_c',
            test='{{ dag_run.conf.action == "C" }}',
            yes_task="get_time_off_type_details",
            no_task="if_request_action_equals_to_d",
        )

        get_time_off_type_details = rail.RepliconServiceOperator(
            task_id='get_time_off_type_details',
            endpoint="/services/TimeOffService1.svc/GetTimeOffTypeDetails",
            data={
                "timeOffTypeUri": "{{ dag_run.conf.timeoff_uri }}"
            }
        )

        get_time_off_type_assignments_for_user = rail.RepliconServiceOperator(
            task_id='get_time_off_type_assignments_for_user',
            endpoint="/services/TimeOffService1.svc/GetTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ dag_run.conf.user_uri }}"
            }
        )

        if_request_timeoffuri_present = rail.IfOperator(
            task_id='if_request_timeoffuri_present',
            test=lambda dag_run: bool(rail.find_first_by_attr_and_get_attr(rail.result("get_time_off_type_assignments_for_user"),
                    "uri", dag_run.conf["timeoff_uri"], "uri")),
            yes_task="get_time_off_details_for_user_and_date_range_1",
            no_task="log_timeoff_booking_not_allowed",
        )

        get_time_off_details_for_user_and_date_range_1 = rail.RepliconServiceOperator(
            task_id='get_time_off_details_for_user_and_date_range_1',
            endpoint="/services/TimeOffService1.svc/GetTimeOffDetailsForUserAndDateRange2",
            data=request_payload.get_time_off_details_for_user_and_date_range_payload
        )

        if_timeoff_booking_already_present = rail.IfOperator(
            task_id='if_timeoff_booking_already_present',
            test=custom_methods.check_if_timeoff_present,
            yes_task="log_timeoff_booking_already_present_skipped",
            no_task="if_timeoff_display_formaturi_equals_to_timeoff_measurement_unit_workdays",
        )

        log_timeoff_booking_already_present_skipped = rail.WriteLogOperator(
            task_id='log_timeoff_booking_already_present_skipped',
            log="{{ dag_run.conf.log }}",
            message="Skipped | Time Off booking is already present for this day.",
            severity="Skipped",
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employee_id }}",
                "timeofftype": "{{ dag_run.conf.timeoff_type }}",
                "startdate": "{{ dag_run.conf.start_date }}",
                "action": "{{ dag_run.conf.action }}",
                "status": "Skipped | Time Off booking is already present for this day."
            }
        )

        if_timeoff_display_formaturi_equals_to_timeoff_measurement_unit_workdays = rail.IfOperator(
            task_id='if_timeoff_display_formaturi_equals_to_timeoff_measurement_unit_workdays',
            test='{{ result("get_time_off_type_details").timeOffDisplayFormatUri == "urn:replicon:time-off-measurement-unit:work-days" }}',
            yes_task="if_request_type_equals_to_f",
            no_task="if_request_type_equals_to_n",
        )

        if_request_type_equals_to_f = rail.IfOperator(
            task_id='if_request_type_equals_to_f',
            test='{{ dag_run.conf.type == "F" }}',
            yes_task="create_timeoff_booking_for_user_f",
            no_task="if_request_type_equals_to_p",
        )

        create_timeoff_booking_for_user_f = rail.EmptyOperator(
            task_id='create_timeoff_booking_for_user_f'
        )

        create_timeoff_booking_type_f, approve_timeoff_booking_type_f = get_create_timeoff("f")

        log_timeoff_f_created_success = rail.WriteLogOperator(
            task_id='log_timeoff_f_created_success',
            log="{{ dag_run.conf.log }}",
            message="Success | The time off is added successfully.",
            severity="Success",
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employee_id }}",
                "timeofftype": "{{ dag_run.conf.timeoff_type }}",
                "startdate": "{{ dag_run.conf.start_date }}",
                "action": "{{ dag_run.conf.action }}",
                "status": "Success | The time off is added successfully."
            }
        )

        if_request_type_equals_to_p = rail.IfOperator(
            task_id='if_request_type_equals_to_p',
            test='{{ dag_run.conf.type == "P" }}',
            yes_task="create_timeoff_booking_for_user_p",
            no_task="catch_and_log_errors",
        )

        create_timeoff_booking_for_user_p = rail.EmptyOperator(
            task_id='create_timeoff_booking_for_user_p',
        )

        create_timeoff_booking_type_p, approve_timeoff_booking_type_p = get_create_timeoff("p")

        log_timeoff_p_created_success = rail.WriteLogOperator(
            task_id='log_timeoff_p_created_success',
            log="{{ dag_run.conf.log }}",
            message="Success | The time off is added successfully.",
            severity="Success",
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employee_id }}",
                "timeofftype": "{{ dag_run.conf.timeoff_type }}",
                "startdate": "{{ dag_run.conf.start_date }}",
                "action": "{{ dag_run.conf.action }}",
                "status": "Success | The time off is added successfully."
            }
        )

        if_request_type_equals_to_n = rail.IfOperator(
            task_id='if_request_type_equals_to_n',
            test='{{ dag_run.conf.type == "N" }}',
            yes_task="if_startendtime_timeoffstarttimehrs_present",
            no_task="finish",
        )

        if_startendtime_timeoffstarttimehrs_present = rail.IfOperator(
            task_id='if_startendtime_timeoffstarttimehrs_present',
            test=lambda dag_run: bool(dag_run.conf["timeoff_start_end_time"]["start_time_hrs"] and dag_run.conf["timeoff_start_end_time"]["start_time_hrs"] != "0"),
            yes_task="create_timeoffbooking_for_user_n1",
            no_task="create_timeoffbooking_for_user_n2",
        )

        create_timeoffbooking_for_user_n1 = rail.EmptyOperator(
            task_id='create_timeoffbooking_for_user_n1',
        )

        create_timeoff_booking_type_n1, approve_timeoff_booking_type_n1 = get_create_timeoff("n1")

        log_timeoff_n1_created_success = rail.WriteLogOperator(
            task_id='log_timeoff_n1_created_success',
            log="{{ dag_run.conf.log }}",
            message="Success | The time off is added successfully.",
            severity="Success",
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employee_id }}",
                "timeofftype": "{{ dag_run.conf.timeoff_type }}",
                "startdate": "{{ dag_run.conf.start_date }}",
                "action": "{{ dag_run.conf.action }}",
                "status": "Success | The time off is added successfully."
            }
        )

        create_timeoffbooking_for_user_n2 = rail.EmptyOperator(
            task_id='create_timeoffbooking_for_user_n2',
        )

        create_timeoff_booking_type_n2, approve_timeoff_booking_type_n2 = get_create_timeoff("n2")

        log_timeoff_n2_created_success = rail.WriteLogOperator(
            task_id='log_timeoff_n2_created_success',
            log="{{ dag_run.conf.log }}",
            message="Success | The time off is added successfully.",
            severity="Success",
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employee_id }}",
                "timeofftype": "{{ dag_run.conf.timeoff_type }}",
                "startdate": "{{ dag_run.conf.start_date }}",
                "action": "{{ dag_run.conf.action }}",
                "status": "Success | The time off is added successfully."
            }
        )

        log_timeoff_booking_not_allowed = rail.WriteLogOperator(
            task_id='log_timeoff_booking_not_allowed',
            log="{{ dag_run.conf.log }}",
            message="Skipped | {{ dag_run.conf.timeoff_type }} is not allowed for booking.",
            severity="Success",
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employee_id }}",
                "timeofftype": "{{ dag_run.conf.timeoff_type }}",
                "startdate": "{{ dag_run.conf.start_date }}",
                "action": "{{ dag_run.conf.action }}",
                "status": "Skipped | {{ dag_run.conf.timeoff_type }} is not allowed for booking."
            }
        )

        if_request_action_equals_to_d = rail.IfOperator(
            task_id='if_request_action_equals_to_d',
            test='{{ dag_run.conf.action == "D" }}',
            yes_task="get_time_off_details_for_user_and_date_range_2",
            no_task="catch_and_log_errors",
        )

        get_time_off_details_for_user_and_date_range_2 = rail.RepliconServiceOperator(
            task_id='get_time_off_details_for_user_and_date_range_2',
            endpoint="/services/TimeOffService1.svc/GetTimeOffDetailsForUserAndDateRange2",
            data=request_payload.get_time_off_details_for_user_and_date_range_payload
        )

        if_timeoff_booking_with_decimal_workdays_present_to_delete = rail.IfOperator(
            task_id='if_timeoff_booking_with_decimal_workdays_present_to_delete',
            test=custom_methods.check_if_timeoff_booking_with_decimal_workdays_present_to_delete,
            yes_task="delete_time_off_1",
            no_task="if_timeoff_booking_with_hours_mins_present_to_delete",
        )

        delete_time_off_1 = rail.RepliconServiceOperator(
            task_id='delete_time_off_1',
            endpoint="/services/TimeOffService1.svc/DeleteTimeOff",
            data=request_payload.delete_timeoff_payload_1
        )

        if_timeoff_booking_with_hours_mins_present_to_delete = rail.IfOperator(
            task_id='if_timeoff_booking_with_hours_mins_present_to_delete',
            test=custom_methods.check_if_timeoff_booking_with_hours_mins_present_to_delete,
            yes_task="delete_time_off_2",
            no_task="log_timeoff_booking_delete_skipped",
        )

        delete_time_off_2 = rail.RepliconServiceOperator(
            task_id='delete_time_off_2',
            endpoint="/services/TimeOffService1.svc/DeleteTimeOff",
            data=request_payload.delete_timeoff_payload_2
        )

        log_timeoff_booking_deleted = rail.WriteLogOperator(
            task_id='log_timeoff_booking_deleted',
            log="{{ dag_run.conf.log }}",
            message="Success | Time off booking deleted.",
            severity="Success",
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employee_id }}",
                "timeofftype": "{{ dag_run.conf.timeoff_type }}",
                "startdate": "{{ dag_run.conf.start_date }}",
                "action": "{{ dag_run.conf.action }}",
                "status": "Success | Time off booking deleted. "
            }
        )

        log_timeoff_booking_delete_skipped = rail.WriteLogOperator(
            task_id='log_timeoff_booking_delete_skipped',
            log="{{ dag_run.conf.log }}",
            message="Skipped | Time Off booking is not present to delete.",
            severity="Skipped",
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employee_id }}",
                "timeofftype": "{{ dag_run.conf.timeoff_type }}",
                "startdate": "{{ dag_run.conf.start_date }}",
                "action": "{{ dag_run.conf.action }}",
                "status": "Skipped | Time Off booking is not present to delete."
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log="{{ dag_run.conf.log }}",
            message="Error | {{ get_error_message() }}",
            severity="Error",
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employee_id }}",
                "timeofftype": "{{ dag_run.conf.timeoff_type }}",
                "startdate": "{{ dag_run.conf.start_date }}",
                "action": "{{ dag_run.conf.action }}",
                "status": "Error | {{ get_error_message() }}"
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        if_timeoff_hrs_less_than_0_1 >> rail.Label('Yes')  >> log_timeoff_duration_not_present_skipped >> catch_and_log_errors
        if_timeoff_hrs_less_than_0_1 >> rail.Label('No') >> if_request_useruri_not_present
        if_request_useruri_not_present >> rail.Label('Yes')  >> log_user_not_present_skipped >> catch_and_log_errors
        if_request_useruri_not_present >> rail.Label('No') >> if_request_timeoffuri_not_present
        if_request_timeoffuri_not_present >> rail.Label('Yes')  >> log_timeoffuri_not_present_skipped >> catch_and_log_errors
        if_request_timeoffuri_not_present >> rail.Label('No') >> if_request_action_not_present
        if_request_action_not_present >> rail.Label('Yes')  >> log_action_not_present_skipped >> catch_and_log_errors
        if_request_action_not_present >> rail.Label('No') >> if_request_action_equals_to_c
        if_request_action_equals_to_c >> rail.Label('Yes')  >> get_time_off_type_details >> get_time_off_type_assignments_for_user \
            >> if_request_timeoffuri_present
        if_request_timeoffuri_present >> rail.Label('Yes')  >> get_time_off_details_for_user_and_date_range_1 >> if_timeoff_booking_already_present
        if_request_timeoffuri_present >> rail.Label('No') >> log_timeoff_booking_not_allowed >> catch_and_log_errors
        if_timeoff_booking_already_present >> rail.Label('Yes')  >> log_timeoff_booking_already_present_skipped >> catch_and_log_errors
        if_timeoff_booking_already_present >> rail.Label('No') >> if_timeoff_display_formaturi_equals_to_timeoff_measurement_unit_workdays
        if_timeoff_display_formaturi_equals_to_timeoff_measurement_unit_workdays >> rail.Label('Yes')  >> if_request_type_equals_to_f
        if_timeoff_display_formaturi_equals_to_timeoff_measurement_unit_workdays >> rail.Label('No') >> if_request_type_equals_to_n
        if_request_type_equals_to_f >> rail.Label('Yes')  >> create_timeoff_booking_for_user_f >> create_timeoff_booking_type_f
        approve_timeoff_booking_type_f >> log_timeoff_f_created_success >> catch_and_log_errors
        if_request_type_equals_to_f >> rail.Label('No') >> if_request_type_equals_to_p
        if_request_type_equals_to_p >> rail.Label('Yes')  >> create_timeoff_booking_for_user_p >> create_timeoff_booking_type_p
        approve_timeoff_booking_type_p >> log_timeoff_p_created_success >> catch_and_log_errors
        if_request_type_equals_to_p >> rail.Label('No') >> catch_and_log_errors
        if_request_type_equals_to_n >> rail.Label('Yes')  >> if_startendtime_timeoffstarttimehrs_present
        if_startendtime_timeoffstarttimehrs_present >> rail.Label('Yes') >> create_timeoffbooking_for_user_n1 >> create_timeoff_booking_type_n1
        approve_timeoff_booking_type_n1 >> log_timeoff_n1_created_success >> catch_and_log_errors
        if_startendtime_timeoffstarttimehrs_present >> rail.Label('No') >> create_timeoffbooking_for_user_n2 >> create_timeoff_booking_type_n2
        approve_timeoff_booking_type_n2 >> log_timeoff_n2_created_success >> catch_and_log_errors
        if_request_type_equals_to_n >> rail.Label('No') >> finish
        if_request_action_equals_to_c >> rail.Label('No') >> if_request_action_equals_to_d
        if_request_action_equals_to_d >> rail.Label('Yes')  >> get_time_off_details_for_user_and_date_range_2 \
            >> if_timeoff_booking_with_decimal_workdays_present_to_delete
        if_timeoff_booking_with_decimal_workdays_present_to_delete >> rail.Label('Yes')  >> delete_time_off_1 >> log_timeoff_booking_deleted >> catch_and_log_errors
        if_timeoff_booking_with_decimal_workdays_present_to_delete >> rail.Label('No') >> if_timeoff_booking_with_hours_mins_present_to_delete
        if_timeoff_booking_with_hours_mins_present_to_delete >> rail.Label("Yes") >> delete_time_off_2 >> log_timeoff_booking_deleted >> catch_and_log_errors
        if_timeoff_booking_with_hours_mins_present_to_delete >> rail.Label("No") >> log_timeoff_booking_delete_skipped >> catch_and_log_errors
        if_request_action_equals_to_d >> rail.Label('No') >> catch_and_log_errors

    return dag

rail.for_each_instance(create_dag)
