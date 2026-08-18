from datetime import timedelta
from airflow.models import Variable
import rail
from rail.lib.ecid import get_dagrun_ecid
from necau.time_off_import.utils import python_callable_method
from necau.time_off_import.utils import request_payload
from necau.time_off_import.task.reopen_timesheet import get_timesheet_open_process
from necau.time_off_import.task.add_update_timeoffs import get_add_update_timeoff
from necau.time_off_import.task.add_entry_logs import add_entry_log_process
from necau.time_off_import.utils import custom_method
null = None

# pylint: disable=too-many-statements


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'necau_timeoff_import_child_{config.instance}',
        description=f'NECAU - timeoff_import_child_v2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='').lower() == 'true',
            yes_task='batch_task',
            no_task='dummy_operator_1'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='lap_form_code_validations',
            end_task='catch_and_log_errors',
        )

        lap_form_code_validations = rail.IfOperator(
            task_id='lap_form_code_validations',
            test=custom_method.lap_form_code_validation,
            yes_task='add_log_entry_1',
            no_task='dummy_operator_2'
        )

        log_entry_1 = add_entry_log_process(
            "1", "Success", "Ignored", "Form code LAP and status Approved, Days Taken and Hours Taken fields are blank")

        staff_member_validations = rail.IfOperator(
            task_id='staff_member_validations',
            test=custom_method.staff_member_validation,
            yes_task='get_all_time_off_types',
            no_task='catch_and_log_errors'
        )

        get_all_time_off_types = rail.RepliconServiceOperator(
            task_id='get_all_time_off_types',
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes"
        )

        is_timeoff_type_present = rail.IfOperator(
            task_id='is_timeoff_type_present',
            test=custom_method.check_timeoff_type_present,
            yes_task='create_new_timeoff_type_draft',
            no_task='dummy_operator_3'
        )

        create_new_timeoff_type_draft = rail.RepliconServiceOperator(
            task_id='create_new_timeoff_type_draft',
            endpoint="/services/TimeOffService1.svc/CreateNewTimeOffTypeDraft"
        )

        put_timeoff_type = rail.RepliconServiceOperator(
            task_id='put_timeoff_type',
            endpoint="/services/TimeOffService1.svc/PutTimeOffType",
            data=request_payload.get_timeoff_type_payload
        )

        publish_timeoff = rail.RepliconServiceOperator(
            task_id='publish_timeoff',
            endpoint="/services/TimeOffService1.svc/PublishTimeOffTypeDraft",
            data=request_payload.get_publish_timeoff_request
        )

        get_all_time_off_validation_scripts = rail.RepliconServiceOperator(
            task_id='get_all_time_off_validation_scripts',
            endpoint="/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts"
        )

        assign_timeoff_validation = rail.RepliconServiceOperator(
            task_id='assign_timeoff_validation',
            endpoint="/services/TimeOffPolicyService1.svc/PutTimeOffBookingPolicyForTimeOffType",
            data=request_payload.get_assing_validation_payload
        )

        is_user_enabled = rail.IfOperator(
            task_id='is_user_enabled',
            test=custom_method.get_user_status,
            yes_task='get_timesheet_for_date',
            no_task='add_log_entry_4'
        )

        log_entry_4 = add_entry_log_process(
            "4", "Success", "Ignored", "Timeoff not booked as user profile doesn't Exist or is Disabled")

        get_timesheet_for_date = rail.RepliconServiceOperator(
            task_id='get_timesheet_for_date',
            endpoint="/services/TimesheetService1.svc/GetTimesheetForDate2",
            data=request_payload.get_request_timesheetdate
        )

        is_timesheet_present = rail.IfOperator(
            task_id='is_timesheet_present',
            test="{{ result('get_timesheet_for_date') is not none  }}",
            yes_task='get_timesheet_details',
            no_task='get_timeoff_details'
        )

        get_timesheet_details = rail.RepliconServiceOperator(
            task_id='get_timesheet_details',
            endpoint="/services/TimesheetService1.svc/GetTimesheetDetails",
            data=request_payload.get_timesheet_uri
        )

        get_timeoff_details = rail.RepliconServiceOperator(
            task_id='get_timeoff_details',
            endpoint="/services/TimeOffService1.svc/GetTimeOffDetailsForUserAndDateRange2",
            data=request_payload.get_timeoff_details_request
        )

        timeoff_booking_info = rail.PythonOperator(
            task_id='timeoff_booking_info',
            python_callable=python_callable_method.get_timeoff_booking_info
        )

        process_modify_bookings = rail.TriggerDagRunForEachItemOperator(
            task_id='process_modify_bookings',
            retries=0,
            items=lambda: rail.result('timeoff_booking_info'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'necau_modify_booking_child_{config.instance}',
            conf=request_payload.get_booking_info
        )

        wait_for_modify_timeoffs = rail.WaitForDagRunsSensor(
            task_id='wait_for_modify_timeoffs',
            dag_runs='{{ result("process_modify_bookings") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        lvc_form_code_validation = rail.IfOperator(
            task_id='lvc_form_code_validation',
            test=custom_method.get_lvc_form_code_validation,
            yes_task='add_log_entry_2',
            no_task='dummy_operator_4'
        )

        log_entry_2 = add_entry_log_process(
            "2", "Success", "Ignored", "Form code LVC and status Approved, Time Off already deleted")

        lap_formcode_validation = rail.IfOperator(
            task_id='lap_formcode_validation',
            test=custom_method.get_lap_form_code_validation,
            yes_task='add_log_entry_3',
            no_task='dummy_operator_5'
        )

        log_entry_3 = add_entry_log_process(
            "3", "Success", "Ignored", "Form code LAP and status, Time Off already deleted")

        is_new_bookings = rail.IfOperator(
            task_id='is_new_bookings',
            test=custom_method.get_booking_status,
            yes_task='dummy_operator_6',
            no_task='catch_and_log_errors'
        )

        is_shift_present = rail.IfOperator(
            task_id='is_shift_present',
            test=custom_method.get_shift_status,
            yes_task='get_user_info',
            no_task='get_custom_field_groups'
        )

        get_user_info = rail.RepliconServiceOperator(
            task_id='get_user_info',
            endpoint="/services/UserService1.svc/GetUserDetails",
            data={
                "userUri": "{{ dag_run.conf.user_uri }}"
            }
        )

        get_shift_effective_dates = rail.PythonOperator(
            task_id='get_shift_effective_dates',
            python_callable=python_callable_method.get_shift_effective_dates
        )

        process_shift_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id='process_shift_assignment',
            retries=0,
            items=lambda: rail.result('get_shift_effective_dates'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'necau_shift_assignment_each_booking_child_{config.instance}',
            conf=request_payload.get_assignment_effective_info
        )

        wait_for_process_shift_assignment = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_shift_assignment',
            dag_runs='{{ result("process_shift_assignment") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_custom_field_groups = rail.RepliconServiceOperator(
            task_id='get_custom_field_groups',
            endpoint="/services/CustomFieldService1.svc/GetCustomFieldGroup",
            data={
                "objectTypeUri": "urn:replicon:object-type:time-off"
            }
        )

        get_all_custom_fields = rail.RepliconServiceOperator(
            task_id='get_all_custom_fields',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data=request_payload.get_all_custom_field_request
        )

        is_timesheet_waiting_approved = rail.IfOperator(
            task_id='is_timesheet_waiting_approved',
            test=lambda: python_callable_method.get_timesheet_status(
                'get_timesheet_details'),
            yes_task='initiate_timesheet_open',
            no_task='dummy_operator_8'
        )

        initiate_timesheet_open = rail.EmptyOperator(
            task_id="initiate_timesheet_open"
        )

        re_open_timesheet = get_timesheet_open_process('lvc', config)

        days_taken_less_than_one = rail.IfOperator(
            task_id='days_taken_less_than_one',
            test=python_callable_method.is_days_taken_and_lap,
            yes_task='initiate_booking_for_less_one',
            no_task='dummy_operator_7'
        )

        initiate_booking_for_less_one = rail.EmptyOperator(
            task_id="initiate_booking_for_less_one"
        )

        add_timeoffs_for_single_day = get_add_update_timeoff('single_day')

        is_booking_multiday = rail.IfOperator(
            task_id='is_booking_multiday',
            test=python_callable_method.is_booking_multiday_and_lap,
            yes_task='initiate_booking_for_greater_one',
            no_task='dummy_operator_9'
        )

        initiate_booking_for_greater_one = rail.EmptyOperator(
            task_id="initiate_booking_for_greater_one"
        )

        add_timeoffs_greater_than_day = get_add_update_timeoff('multi_day')

        end_booking_less_than_day = rail.IfOperator(
            task_id='end_booking_less_than_day',
            test=custom_method.get_end_booking_status,
            yes_task='initiate_booking_for_equal_one',
            no_task='catch_and_log_errors'
        )

        initiate_booking_for_equal_one = rail.EmptyOperator(
            task_id="initiate_booking_for_equal_one"
        )
        add_timeoffs_current_day = get_add_update_timeoff(
            'multi_day_with_partial1')

        add_timeoffs_next_day = get_add_update_timeoff(
            'multi_day_with_partial2')

        add_log_entry = rail.WriteLogOperator(
            task_id="add_log_entry",
            log="{{ dag_run.conf.create_file_processing_log }}",
            severity="Success",
            properties=lambda dag_run: {
                'jobid': dag_run.conf['master_ecid'] + '|' + get_dagrun_ecid(dag_run),
                'Staff Member': dag_run.conf['staff_member'],
                'Status': 'Success',
                'reason': 'Timeoff Booked',
                'Request Key': dag_run.conf['request_key']
            },
            message="Successfully Completed",
        )

        is_timeoff_present = rail.IfOperator(
            task_id='is_timeoff_present',
            trigger_rule='all_done',
            test=python_callable_method.is_timeoff_present,
            yes_task='delete_errored_timeoff',
            no_task='finish'
        )

        delete_errored_timeoff = rail.RepliconServiceOperator(
            task_id='delete_errored_timeoff',
            endpoint="/services/TimeOffService1.svc/DeleteTimeOff",
            data=request_payload.get_errored_timeoff_delete_request
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ dag_run.conf.create_file_processing_log }}",
            trigger_rule='one_failed',
            severity="Error",
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                'jobid': dag_run.conf['master_ecid'] + '|' + get_dagrun_ecid(dag_run),
                'Staff Member': dag_run.conf['staff_member'],
                'Status': "Error",
                'reason': "Error",
                'Request Key': dag_run.conf['request_key']
            },
        )

        # log_to_sumo = rail.DagRunLogToSumoOperator(
        #     task_id='log_to_sumo',
        #     sumo_conn_id='sumologic-dagrunlogger',
        #     trigger_rule='all_done',
        #     extra_info={
        #         'staff_member': '{{ dag_run.conf.staff_member }}',
        #         'request_key': '{{ dag_run.conf.request_key }}'
        #     }
        # )

        finish = rail.EmptyOperator(
            task_id="finish"
        )

        dummy_operator_1 = rail.EmptyOperator(
            task_id="dummy_operator_1"
        )

        dummy_operator_2 = rail.EmptyOperator(
            task_id="dummy_operator_2"
        )

        dummy_operator_3 = rail.EmptyOperator(
            task_id="dummy_operator_3"
        )

        dummy_operator_4 = rail.EmptyOperator(
            task_id="dummy_operator_4"
        )

        dummy_operator_5 = rail.EmptyOperator(
            task_id="dummy_operator_5"
        )

        dummy_operator_6 = rail.EmptyOperator(
            task_id="dummy_operator_6"
        )

        dummy_operator_7 = rail.EmptyOperator(
            task_id="dummy_operator_7"
        )

        dummy_operator_8 = rail.EmptyOperator(
            task_id="dummy_operator_8"
        )

        dummy_operator_9 = rail.EmptyOperator(
            task_id="dummy_operator_9"
        )

        can_run_batch_task
        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            "No") >> dummy_operator_1 >> lap_form_code_validations
        lap_form_code_validations >> rail.Label(
            "Yes") >> log_entry_1 >> catch_and_log_errors
        staff_member_validations >> rail.Label(
            "Yes") >> get_all_time_off_types >> is_timeoff_type_present
        is_timeoff_type_present >> rail.Label("Yes") >> create_new_timeoff_type_draft >> put_timeoff_type >> \
            publish_timeoff >> get_all_time_off_validation_scripts >> \
            assign_timeoff_validation >> dummy_operator_3 >> is_user_enabled
        is_user_enabled >> rail.Label(
            "Yes") >> get_timesheet_for_date >> is_timesheet_present
        is_timesheet_present >> rail.Label("Yes") >> get_timesheet_details >> get_timeoff_details >> \
            timeoff_booking_info >> process_modify_bookings >> wait_for_modify_timeoffs >> lvc_form_code_validation
        lvc_form_code_validation >> rail.Label(
            "Yes") >> log_entry_2 >> catch_and_log_errors
        lap_formcode_validation >> rail.Label(
            "Yes") >> log_entry_3 >> catch_and_log_errors
        is_new_bookings >> rail.Label(
            "Yes") >> dummy_operator_6 >> is_shift_present
        is_shift_present >> rail.Label(
            "Yes") >> get_user_info >> get_shift_effective_dates >> process_shift_assignment >> wait_for_process_shift_assignment >> \
            get_custom_field_groups >> get_all_custom_fields >> is_timesheet_waiting_approved
        is_timesheet_waiting_approved >> rail.Label(
            "Yes") >> initiate_timesheet_open >> re_open_timesheet >> dummy_operator_8 >> days_taken_less_than_one
        days_taken_less_than_one >> rail.Label(
            "Yes") >> initiate_booking_for_less_one >> add_timeoffs_for_single_day >> add_log_entry
        days_taken_less_than_one >> rail.Label(
            "No") >> dummy_operator_7 >> is_booking_multiday
        is_booking_multiday >> rail.Label(
            "Yes") >> initiate_booking_for_greater_one >> add_timeoffs_greater_than_day >> add_log_entry
        end_booking_less_than_day >> rail.Label(
            "Yes") >> initiate_booking_for_equal_one >> add_timeoffs_current_day >> add_timeoffs_next_day >> add_log_entry
        end_booking_less_than_day >> rail.Label("No") >> catch_and_log_errors
        is_timesheet_waiting_approved >> rail.Label(
            "No") >> dummy_operator_8
        is_shift_present >> rail.Label("No") >> get_custom_field_groups
        staff_member_validations >> rail.Label("No") >> catch_and_log_errors
        lap_form_code_validations >> rail.Label(
            "No") >> dummy_operator_2 >> staff_member_validations
        is_timeoff_type_present >> rail.Label("No") >> dummy_operator_3
        lvc_form_code_validation >> rail.Label(
            "No") >> dummy_operator_4 >> lap_formcode_validation
        lap_formcode_validation >> rail.Label(
            "No") >> dummy_operator_5 >> is_new_bookings
        is_user_enabled >> rail.Label(
            "No") >> log_entry_4 >> catch_and_log_errors
        is_timesheet_present >> rail.Label(
            "No") >> get_timeoff_details
        is_new_bookings >> rail.Label(
            "No") >> catch_and_log_errors
        add_log_entry >> catch_and_log_errors
        is_booking_multiday >> rail.Label(
            "No") >> dummy_operator_9 >> end_booking_less_than_day >> catch_and_log_errors
        catch_and_log_errors >> is_timeoff_present
        is_timeoff_present >> rail.Label(
            "Yes") >> delete_errored_timeoff >> finish
        is_timeoff_present >> rail.Label("No") >> finish

    return dag


rail.for_each_instance(create_dag)
