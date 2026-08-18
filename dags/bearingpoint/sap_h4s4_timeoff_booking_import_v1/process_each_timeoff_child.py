from datetime import timedelta
import rail
from bearingpoint.sap_h4s4_timeoff_booking_import_v1.utils import request_payload, response_filter, custom_methods
from airflow.models import Variable

null = None


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_each_timeoff_booking,
        description=f'Bearingpoint Timeoff Booking Sync Add Child {config.instance}',
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
            no_task='is_timeoff_type_present'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='is_timeoff_type_present',
            end_task='catch_and_log_errors',
        )

        is_timeoff_type_present = rail.IfOperator(
            task_id='is_timeoff_type_present',
            test='{{ dag_run.conf.timeoff_uri | is_truthy }}',
            yes_task='delete_time_entries',
            no_task='log_timeoff_type_not_assigned'
        )

        log_timeoff_type_not_assigned = rail.WriteLogOperator(
            task_id='log_timeoff_type_not_assigned',
            log='{{ dag_run.conf.log }}',
            message='Time Off type {{ dag_run.conf.timeofftype }} is not assigned to user in Replicon',
            severity='Skipped',
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "timeofftype": dag_run.conf["timeofftype"],
                "startdate": dag_run.conf["startdate"],
                "enddate": dag_run.conf["enddate"],
                "hours": dag_run.conf["hours"],
                "booking_id": dag_run.conf["booking_id"],
                'action': 'Validation',
                "status": "Skipped",
                "details": "Time Off Type - " + str(dag_run.conf['timeofftype']) + " is not assigned to user - " + str(dag_run.conf[
                    'employee_id']) + " in Replicon"
            }
        )

        delete_time_entries = rail.RepliconServiceOperator(
            task_id='delete_time_entries',
            endpoint='/services/TimeEntryService3.svc/DeleteTimeEntriesForUserAndDateRange',
            data=request_payload.get_delete_time_entries_payload
        )

        get_time_off_booking_details = rail.RepliconServiceOperator(
            task_id="get_time_off_booking_details",
            endpoint="/services/TimeOffListService1.svc/GetData",
            data=request_payload.get_user_time_off_booking_details,
            data_handler=response_filter.get_filtered_time_off_details_on_sf_booking_id
        )

        is_timeoff_exists_with_booking_id = rail.IfOperator(
            task_id='is_timeoff_exists_with_booking_id',
            test=lambda: bool(rail.result('get_time_off_booking_details')['is_timeoff_uri_present']),
            yes_task='log_timeoff_uri_present',
            no_task='is_any_manual_timeoff_present'
        )

        log_timeoff_uri_present = rail.WriteLogOperator(
            task_id='log_timeoff_uri_present',
            log='{{ dag_run.conf.log }}',
            message='Time Off URI is already present in Replicon for booking ID {{ dag_run.conf.booking_id }}',
            severity='Skipped',
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "timeofftype": dag_run.conf["timeofftype"],
                "startdate": dag_run.conf["startdate"],
                "enddate": dag_run.conf["enddate"],
                "hours": dag_run.conf["hours"],
                "booking_id": dag_run.conf["booking_id"],
                'action': 'Validation',
                "status": "Skipped",
                "details": f"Time Off Booking is already present in Replicon for booking ID - {dag_run.conf['booking_id']}"
            }
        )

        is_any_manual_timeoff_present = rail.IfOperator(
            task_id='is_any_manual_timeoff_present',
            test=lambda: bool(rail.result('get_time_off_booking_details')['timeoff_details']),
            yes_task='get_timeoffs_for_update',
            no_task='put_and_submit_timeoff_booking_for_user'
        )

        get_timeoffs_for_update = rail.PythonOperator(
            task_id='get_timeoffs_for_update',
            python_callable=custom_methods.adjust_timeoff_records,
        )

        is_any_timeoffs_to_delete = rail.IfOperator(
            task_id='is_any_timeoffs_to_delete',
            test=lambda: bool(rail.result('get_timeoffs_for_update')['timeoff_to_delete']),
            yes_task='delete_time_off_bookings',
            no_task='is_any_timeoffs_to_update'
        )

        delete_time_off_bookings = rail.RepliconServiceCallForEachItemOperator(
            task_id='delete_time_off_bookings',
            endpoint="/services/TimeOffService1.svc/DeleteTimeOff",
            items=lambda: rail.result('get_timeoffs_for_update')['timeoff_to_delete'],
            data=lambda item: {
                "timeOffUri": item['timeoff_uri']
            }
        )

        is_any_timeoffs_to_update = rail.IfOperator(
            task_id='is_any_timeoffs_to_update',
            test=lambda: bool(rail.result('get_timeoffs_for_update')['timeoff_to_update']),
            yes_task='reopen_and_put_timeoff',
            no_task='put_and_submit_timeoff_booking_for_user'
        )

        reopen_and_put_timeoff = rail.RepliconServiceCallForEachItemOperator(
            task_id="reopen_and_put_timeoff",
            endpoint="/services/TimeOffApprovalService1.svc/CreateTimeOffOrApplyModifications",
            items=lambda: rail.result('get_timeoffs_for_update')['timeoff_to_update'],
            data=request_payload.get_timeoff_booking_for_user_payload
        )

        put_and_submit_timeoff_booking_for_user = rail.RepliconServiceOperator(
            task_id='put_and_submit_timeoff_booking_for_user',
            endpoint="/services/TimeOffApprovalService1.svc/PutAndSubmitTimeOff",
            data=request_payload.get_create_and_publish_timeoff_payload
        )

        get_time_off_approval_status = rail.RepliconServiceOperator(
            task_id="get_time_off_approval_status",
            endpoint="/services/TimeOffApprovalService1.svc/GetApprovalHistoryDetails",
            data={
                "timeOffUri": "{{ result('put_and_submit_timeoff_booking_for_user').uri }}"
            },
            data_handler=lambda response: response['approvalStatus']['displayText']
        )

        is_timeoff_approved = rail.IfOperator(
            task_id="is_timeoff_approved",
            test=lambda: rail.result(
                'get_time_off_approval_status') == 'Approved',
            yes_task='log_booking_add_successful',
            no_task='approve_timeoff_booking_for_user'
        )

        approve_timeoff_booking_for_user = rail.RepliconServiceOperator(
            task_id='approve_timeoff_booking_for_user',
            endpoint="/services/TimeOffApprovalService1.svc/ForceApprove",
            data=request_payload.get_approve_holiday_booking_payload
        )

        log_booking_add_successful = rail.WriteLogOperator(
            task_id='log_booking_add_successful',
            log='{{ dag_run.conf.log }}',
            message='Timeoff Synced Successfully',
            severity='Success',
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "timeofftype": dag_run.conf["timeofftype"],
                "startdate": dag_run.conf["startdate"],
                "enddate": dag_run.conf["enddate"],
                "hours": dag_run.conf["hours"],
                'action': 'Add',
                "status": "Success",
                "details": "Time Off Booking is added successfully",
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf.log }}',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error',
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "timeofftype": dag_run.conf["timeofftype"],
                "startdate": dag_run.conf["startdate"],
                "enddate": dag_run.conf["enddate"],
                "hours": dag_run.conf["hours"],
                "booking_id": dag_run.conf["booking_id"],
                'action': '{{ "Update" if result("get_time_off_booking_details")["timeoff_details"] | is_truthy else "Add" }}',
                "status": "Error",
                "details": '{{ get_error_message() }}'
            }
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors

        can_run_batch_task >> rail.Label(
            "No") >> is_timeoff_type_present

        is_timeoff_type_present >> rail.Label(
            "Yes") >> delete_time_entries >> get_time_off_booking_details >> is_timeoff_exists_with_booking_id
        
        is_timeoff_exists_with_booking_id >> rail.Label(
            "Yes") >> log_timeoff_uri_present >> catch_and_log_errors
        
        is_timeoff_exists_with_booking_id >> rail.Label(
            "No") >> is_any_manual_timeoff_present
        
        is_any_manual_timeoff_present >> rail.Label(
            "Yes") >> get_timeoffs_for_update >> is_any_timeoffs_to_delete
        
        is_any_manual_timeoff_present >> rail.Label(
            "No") >> put_and_submit_timeoff_booking_for_user
        
        is_any_timeoffs_to_delete >> rail.Label(
            "Yes") >> delete_time_off_bookings >> is_any_timeoffs_to_update
        
        is_any_timeoffs_to_delete >> rail.Label(
            "No") >> is_any_timeoffs_to_update
        
        is_any_timeoffs_to_update >> rail.Label(
            "No") >> put_and_submit_timeoff_booking_for_user
        
        is_any_timeoffs_to_update >> rail.Label(
            "Yes") >> reopen_and_put_timeoff >> put_and_submit_timeoff_booking_for_user

        is_timeoff_type_present >> rail.Label(
            "No") >> log_timeoff_type_not_assigned >> catch_and_log_errors

        put_and_submit_timeoff_booking_for_user >> get_time_off_approval_status >> is_timeoff_approved

        is_timeoff_approved >> rail.Label(
            "Yes") >> log_booking_add_successful >> catch_and_log_errors

        is_timeoff_approved >> rail.Label(
            "No") >> approve_timeoff_booking_for_user >> log_booking_add_successful

    return dag


rail.for_each_instance(create_child_dag)
