from datetime import timedelta, datetime
import rail
from darkmattertechnologiesllc.timeoff_import.utils import python_callable
from darkmattertechnologiesllc.timeoff_import.utils import request_payload
from darkmattertechnologiesllc.timeoff_import.utils import response_filter
from airflow.models import Variable

null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_timeoff_child,
        description=f'Dark Matter Timeoff Sync Process Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_process_timeoff_child
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_booking_date_is_working_day'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='if_booking_date_is_working_day',
            end_task='catch_and_log_errors',
        )

        if_booking_date_is_working_day = rail.IfOperator(
            task_id='if_booking_date_is_working_day',
            test=lambda dag_run: datetime.strptime(dag_run.conf['time_off_date'], '%m/%d/%Y').weekday() not in [5, 6],
            yes_task='is_older_than_6_months',
            no_task='log_booking_day_is_non_working_day'
        )

        log_booking_day_is_non_working_day = rail.WriteLogOperator(
            task_id='log_booking_day_is_non_working_day',
            log='{{ dag_run.conf.create_log}}',
            message="Attempting to book timeoff on non-working day.",
            severity='Exception',
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "unique_id": dag_run.conf["unique_id"],
                "time_off_date": dag_run.conf["time_off_date"],
                "status": "Exception",
                "details": "Attempting to book timeoff on non-working day.",
            }
        )

        is_older_than_6_months = rail.IfOperator(
            task_id='is_older_than_6_months',
            test=python_callable.is_older_than_6_months,
            yes_task='log_booking_day_is_older_than_6_months',
            no_task='get_user_info'
        )

        log_booking_day_is_older_than_6_months = rail.WriteLogOperator(
            task_id='log_booking_day_is_older_than_6_months',
            log='{{ dag_run.conf.create_log}}',
            message="Attempting to book timeoff older than 6 months",
            severity='Exception',
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "unique_id": dag_run.conf["unique_id"],
                "time_off_date": dag_run.conf["time_off_date"],
                "status": "Exception",
                "details": "Attempting to book timeoff older than 6 months",
            }
        )

        get_user_info = rail.RepliconServiceOperator(
            task_id='get_user_info',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data=request_payload.get_bulk_users_payload,
            data_handler=lambda res: res[0] if len(
                res) > 0 and res[0]["userDetails"]["uri"] else null
        )

        is_user_present = rail.IfOperator(
            task_id='is_user_present',
            test='{{ result("get_user_info") | is_truthy }}',
            yes_task='is_timeoff_type_present',
            no_task='log_user_not_present'
        )

        log_user_not_present = rail.WriteLogOperator(
            task_id='log_user_not_present',
            log='{{ dag_run.conf.create_log}}',
            message="User not present in Replicon",
            severity='Skipped',
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "unique_id": dag_run.conf["unique_id"],
                "time_off_date": dag_run.conf["time_off_date"],
                "status": "Skipped",
                "details": "User " + str(dag_run.conf['employee_id']) + " not present in Replicon",
            }
        )

        is_timeoff_type_present = rail.IfOperator(
            task_id='is_timeoff_type_present',
            test='{{ dag_run.conf.timeoff_type_uri | is_truthy }}',
            yes_task='is_timeoff_type_assigned_to_user',
            no_task='log_timeoff_type_not_present'
        )

        log_timeoff_type_not_present = rail.WriteLogOperator(
            task_id='log_timeoff_type_not_present',
            log='{{ dag_run.conf.create_log}}',
            message='Time Off type {{ dag_run.conf.time_off_type }} not available in Replicon',
            severity='Skipped',
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "unique_id": dag_run.conf["unique_id"],
                "time_off_date": dag_run.conf["time_off_date"],
                "status": "Skipped",
                "details": "Time Off type " + str(dag_run.conf['time_off_type']) + " not available in Replicon",
            }
        )

        is_timeoff_type_assigned_to_user = rail.IfOperator(
            task_id='is_timeoff_type_assigned_to_user',
            test=python_callable.check_timeoff_type_assigned_to_user,
            yes_task='get_time_off_details_on_booking_id',
            no_task='log_timeoff_type_not_assigned_to_user'
        )

        log_timeoff_type_not_assigned_to_user = rail.WriteLogOperator(
            task_id='log_timeoff_type_not_assigned_to_user',
            log='{{ dag_run.conf.create_log}}',
            message='Time Off type {{ dag_run.conf.time_off_type }} is not assigned to user in Replicon',
            severity='Skipped',
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "unique_id": dag_run.conf["unique_id"],
                "time_off_date": dag_run.conf["time_off_date"],
                "status": "Skipped",
                "details": "Time Off type " + str(dag_run.conf['time_off_type']) + " is not assigned to user in Replicon",
            }
        )

        get_time_off_details_on_booking_id = rail.RepliconServiceOperator(
            task_id="get_time_off_details_on_booking_id",
            endpoint="/services/TimeOffListService1.svc/GetData",
            data=request_payload.get_time_off_details_on_booking_id,
            data_handler=response_filter.get_filtered_time_off_details_on_booking_id
        )

        is_timeoff_present_in_instance = rail.IfOperator(
            task_id='is_timeoff_present_in_instance',
            test="{{ result('get_time_off_details_on_booking_id') | is_truthy }}",
            yes_task='process_timeoff_update_delete',
            no_task='process_timeoff_add'
        )

        process_timeoff_update_delete = rail.TriggerDagRunOperator(
            task_id='process_timeoff_update_delete',
            trigger_dag_id=config.timeoff_booking_update_delete_child,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            conf=lambda dag_run: {
                'user_uri': rail.result("get_user_info")["userDetails"]["uri"],
                'employee_id': dag_run.conf['employee_id'],
                'time_off_type': dag_run.conf['time_off_type'],
                'time_off_date': dag_run.conf['time_off_date'],
                'unique_id': dag_run.conf['unique_id'],
                'total_units': dag_run.conf['total_units'],
                'unit_of_time_request': dag_run.conf['unit_of_time_request'],
                'booking_id_oef_value': dag_run.conf['booking_id_oef_value'],
                'timeoff_type_uri': dag_run.conf['timeoff_type_uri'],
                'timeoff_uri': rail.result('get_time_off_details_on_booking_id')[0]['timeoff_uri'],
                'hours': rail.result('get_time_off_details_on_booking_id')[0]['hours'],
                'approval_status': rail.result('get_time_off_details_on_booking_id')[0]['approval_status'],
                'create_log': dag_run.conf['create_log']
            }
        )

        wait_for_process_update_delete = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_update_delete',
            dag_runs='{{ result("process_timeoff_update_delete") }}',
            execution_timeout=timedelta(
                days=config.child_wait_execution_timeout)
        )

        process_timeoff_add = rail.TriggerDagRunOperator(
            task_id='process_timeoff_add',
            trigger_dag_id=config.timeoff_add_child,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            conf=lambda dag_run: {
                'user_uri': rail.result("get_user_info")["userDetails"]["uri"],
                'employee_id': dag_run.conf['employee_id'],
                'time_off_type': dag_run.conf['time_off_type'],
                'time_off_date': dag_run.conf['time_off_date'],
                'unique_id': dag_run.conf['unique_id'],
                'total_units': dag_run.conf['total_units'],
                'timeoff_type_uri': dag_run.conf['timeoff_type_uri'],
                'unit_of_time_request': dag_run.conf['unit_of_time_request'],
                'booking_id_oef_value': dag_run.conf['booking_id_oef_value'],
                'create_log': dag_run.conf['create_log']
            }
        )

        wait_for_process_add = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_add',
            dag_runs='{{ result("process_timeoff_add") }}',
            execution_timeout=timedelta(
                days=config.child_wait_execution_timeout)
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf.create_log}}',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error',
            properties={
                "employee_id": '{{ dag_run.conf.employee_id }}',
                "unique_id": '{{ dag_run.conf.unique_id }}',
                "time_off_date": '{{ dag_run.conf.time_off_date }}',
                "status": "Error",
                "details": '{{ get_error_message() }}'
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> if_booking_date_is_working_day
        if_booking_date_is_working_day >> rail.Label("Yes") >> is_older_than_6_months
        if_booking_date_is_working_day >> rail.Label("No") >> log_booking_day_is_non_working_day >> catch_and_log_errors
        is_older_than_6_months >> rail.Label("Yes") >> log_booking_day_is_older_than_6_months >> catch_and_log_errors
        is_older_than_6_months >> rail.Label("No") >> get_user_info
        get_user_info >> is_user_present
        is_user_present >> rail.Label("Yes") >> is_timeoff_type_present
        is_timeoff_type_present >> rail.Label("Yes") >> is_timeoff_type_assigned_to_user
        is_timeoff_type_present >> rail.Label("No") >> log_timeoff_type_not_present >> catch_and_log_errors
        is_user_present >> rail.Label("No") >> log_user_not_present >> catch_and_log_errors

        is_timeoff_type_assigned_to_user >> rail.Label("Yes") >> get_time_off_details_on_booking_id >> \
        is_timeoff_present_in_instance >> rail.Label("Yes") >> process_timeoff_update_delete >> wait_for_process_update_delete >> catch_and_log_errors
        is_timeoff_present_in_instance >> rail.Label("No") >> process_timeoff_add >> wait_for_process_add >> catch_and_log_errors
        is_timeoff_type_assigned_to_user >> rail.Label("No") >> log_timeoff_type_not_assigned_to_user >> catch_and_log_errors

        catch_and_log_errors >> dagrun_log_to_sumo

    return dag

rail.for_each_instance(create_child_dag)
