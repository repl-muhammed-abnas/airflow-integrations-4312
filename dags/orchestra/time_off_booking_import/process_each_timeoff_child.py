from datetime import timedelta
from airflow.models import Variable
import rail
from orchestra.time_off_booking_import.utils import request_payload, response_filter

null = None

# pylint: disable=too-many-statements
def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_each_timeoff_dagid,
        description="orchestra Time Off Booking Import Process Each Time off Booking",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_time_off_details_on_booking_id'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_time_off_details_on_booking_id',
            end_task='catch_and_log_errors',
        )

        get_time_off_details_on_booking_id = rail.RepliconServiceOperator(
            task_id="get_time_off_details_on_booking_id",
            endpoint="/services/TimeOffListService1.svc/GetData",
            data=request_payload.get_time_off_details_on_booking_id,
            data_handler=response_filter.get_filtered_time_off_details_on_booking_id
        )

        can_add_timeoff = rail.IfOperator(
            task_id='can_add_timeoff',
            test= '{{ dag_run.conf.action == "Add" or dag_run.conf.action == "Taken" }}',
            yes_task='if_any_existing_booking_found',
            no_task='can_delete_timeoff'
        )

        if_any_existing_booking_found = rail.IfOperator(
            task_id = 'if_any_existing_booking_found',
            test= '{{ result("get_time_off_details_on_booking_id") | is_truthy }}',
            yes_task= 'log_existing_booking_exception',
            no_task= 'create_timeoff_booking_for_user'
        )

        log_existing_booking_exception = rail.WriteLogOperator(
            task_id='log_existing_booking_exception',
            log="{{ dag_run.conf.log }}",
            severity='Success',
            message="Time off booking already exists in Replicon",
            properties={
                'booking_id': "{{ dag_run.conf.booking_id }}",
                'loginname': "{{ dag_run.conf.loginname }}",
                'time_off_type': "{{ dag_run.conf.time_off_type }}",
                'start_date': "{{ dag_run.conf.startdate }}",
                'end_date': "{{ dag_run.conf.enddate }}",
                "action": "Add",
                'status': 'Exception',
                'details': "Time off booking already exists in Replicon",
            }
        )

        can_delete_timeoff = rail.IfOperator(
            task_id='can_delete_timeoff',
            test=lambda: rail.result('get_time_off_details_on_booking_id') != [],
            yes_task='delete_timeoff',
            no_task='log_delete_timeoff'
        )

        delete_timeoff = rail.RepliconServiceOperator(
            task_id="delete_timeoff",
            endpoint="/services/TimeOffService1.svc/DeleteTimeOff",
            data={
                   "timeOffUri":'{{ result("get_time_off_details_on_booking_id").0.timeoff_uri }}'
                 }
        )

        log_delete_timeoff = rail.WriteLogOperator(
            task_id='log_delete_timeoff',
            log="{{ dag_run.conf.log }}",
            severity= "{{ 'Success' if result('get_time_off_details_on_booking_id') | is_truthy else 'Exception'}}",
            message="{{ 'Time off deleted Successfully' if result('get_time_off_details_on_booking_id'\
                ) | is_truthy else 'Booking does not exists in Replicon'}}",
            properties={
                'booking_id': "{{ dag_run.conf.booking_id }}",
                'loginname': "{{ dag_run.conf.loginname }}",
                'time_off_type': "{{ dag_run.conf.time_off_type }}",
                'start_date': "{{ dag_run.conf.startdate }}",
                'end_date': "{{ dag_run.conf.enddate }}",
                'action':'Delete',
                'status': "{{ 'Success' if result('get_time_off_details_on_booking_id') | is_truthy else 'Exception'}}",
                'details': "{{ 'Time off Booking deleted Successfully' if result('get_time_off_details_on_booking_id'\
                    ) | is_truthy else 'Booking does not exists in Replicon'}}",
            }
        )

        create_timeoff_booking_for_user = rail.RepliconServiceOperator(
            task_id='create_timeoff_booking_for_user',
            endpoint="/services/TimeOffApprovalService1.svc/PutAndSubmitTimeOff",
            data=request_payload.get_create_timeoff_payload
        )

        get_time_off_approval_status = rail.RepliconServiceOperator(
            task_id="get_time_off_approval_status",
            endpoint="/services/TimeOffApprovalService1.svc/GetApprovalHistoryDetails",
            data={
                "timeOffUri": "{{ result('create_timeoff_booking_for_user').uri }}"
            },
            data_handler = lambda response: response['approvalStatus']['displayText']
        )

        is_timeoff_approved = rail.IfOperator(
            task_id="is_timeoff_approved",
            test=lambda: rail.result('get_time_off_approval_status') == 'Approved',
            yes_task='log_timeoff_success',
            no_task='force_approve_time_off_entry'
        )

        force_approve_time_off_entry = rail.RepliconServiceOperator(
            task_id="force_approve_time_off_entry",
            endpoint="/services/TimeOffApprovalService1.svc/ForceApprove",
            data= request_payload.get_submit_time_off_entry_payload
        )

        log_timeoff_success = rail.WriteLogOperator(
            task_id='log_timeoff_success',
            log="{{ dag_run.conf.log }}",
            severity='Success',
            message="Time off Booking Added Successfully",
            properties={
                'booking_id': "{{ dag_run.conf.booking_id }}",
                'loginname': "{{ dag_run.conf.loginname }}",
                'time_off_type': "{{ dag_run.conf.time_off_type }}",
                'start_date': "{{ dag_run.conf.startdate }}",
                'end_date': "{{ dag_run.conf.enddate }}",
                "action": "Add",
                'status': 'Success',
                'details': "Time off Booking Added Successfully",
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log='{{ dag_run.conf.log }}',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'booking_id': "{{ dag_run.conf.booking_id }}",
                'loginname': "{{ dag_run.conf.loginname }}",
                'time_off_type': "{{ dag_run.conf.time_off_type }}",
                'start_date': "{{ dag_run.conf.startdate }}",
                'end_date': "{{ dag_run.conf.enddate }}",
                'action':'Add',
                'status': 'Error',
                'details': '{{ get_error_message() }}'
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> get_time_off_details_on_booking_id >> can_add_timeoff

        can_add_timeoff >> rail.Label(
            "Yes") >> if_any_existing_booking_found

        if_any_existing_booking_found >> rail.Label(
            "Yes") >> log_existing_booking_exception >> catch_and_log_errors

        if_any_existing_booking_found >> rail.Label(
            "No") >> create_timeoff_booking_for_user >> get_time_off_approval_status >> is_timeoff_approved

        is_timeoff_approved >> rail.Label(
            "Yes") >> log_timeoff_success >> catch_and_log_errors

        is_timeoff_approved >> rail.Label(
            "No") >> force_approve_time_off_entry >> log_timeoff_success

        can_add_timeoff >> rail.Label(
            "No") >> can_delete_timeoff

        can_delete_timeoff >> rail.Label(
            "Yes") >> delete_timeoff >> log_delete_timeoff

        can_delete_timeoff >> rail.Label(
            "No") >> log_delete_timeoff >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
