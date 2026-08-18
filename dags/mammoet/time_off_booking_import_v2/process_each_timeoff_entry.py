from datetime import timedelta
from uuid import uuid4
from airflow.models import Variable
import rail

from mammoet.time_off_booking_import_v2.utils import response_filter
from mammoet.time_off_booking_import_v2.utils import request_payload

null= None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_each_time_off_entry_dagid,
        description='Mammoet Time Off Booking Import Process Each Time Off Entry',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_timeoff_entry,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='is_date_valid'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            start_task='is_date_valid',
            end_task='catch_and_log_errors',
        )

        is_date_valid = rail.IfOperator(
            task_id="is_date_valid",
            test=lambda dag_run:request_payload.validate_dates(dag_run, config),
            yes_task="is_timeoff_type_assigned_for_user",
            no_task="log_invalid_dates"
        )

        log_invalid_dates = rail.WriteLogOperator(
            task_id='log_invalid_dates',
            log="{{dag_run.conf.employee_log}}",
            severity='Exception',
            message=request_payload.get_invalid_datetime_exception,
            properties=lambda dag_run:{
                'sf_booking_id': dag_run.conf['sf_booking_id'],
                'employee_id': dag_run.conf['employee_id'],
                'time_off_type_description': dag_run.conf['time_off_type_description'],
                'action':'Validation',
                'status': 'Exception',
                'details': request_payload.get_invalid_datetime_exception(dag_run)
            },
        )

        is_timeoff_type_assigned_for_user = rail.IfOperator(
            task_id="is_timeoff_type_assigned_for_user",
            test=lambda dag_run: bool(
                dag_run.conf['timeoff_uri'] in dag_run.conf['available_timeoff_uris']),
            yes_task="get_time_off_details_on_sf_booking_id",
            no_task="log_timeoff_not_assigned_to_user"
        )

        log_timeoff_not_assigned_to_user = rail.WriteLogOperator(
            task_id='log_timeoff_not_assigned_to_user',
            log="{{dag_run.conf.employee_log}}",
            severity='Exception',
            message='Time off - {{ dag_run.conf.time_off_type_description }} is not assigned/disabled for user',
            properties={
                'sf_booking_id': "{{dag_run.conf.sf_booking_id}}",
                'employee_id': "{{dag_run.conf.employee_id}}",
                'time_off_type_description': "{{dag_run.conf.time_off_type_description}}",
                'action':'Validation',
                'status': 'Exception',
                'details': 'Time off - {{ dag_run.conf.time_off_type_description }} is not assigned/disabled for user',
            }
        )

        get_time_off_details_on_sf_booking_id = rail.RepliconServiceOperator(
            task_id="get_time_off_details_on_sf_booking_id",
            endpoint="/services/TimeOffListService1.svc/GetData",
            data=request_payload.get_time_off_details_on_sf_booking_id,
            data_handler=response_filter.get_filtered_time_off_details_on_sf_booking_id
        )

        is_timeoff_for_delete = rail.IfOperator(
            task_id='is_timeoff_for_delete',
            test=lambda dag_run: dag_run.conf['time_off_booking_status'] == "Cancelled",
            yes_task='can_delete_timeoff',
            no_task='is_sf_booking_id_available'
        )

        can_delete_timeoff = rail.IfOperator(
            task_id='can_delete_timeoff',
            test=lambda: rail.result('get_time_off_details_on_sf_booking_id') != [],
            yes_task='delete_timeoff',
            no_task='log_delete_timeoff'
        )

        delete_timeoff = rail.RepliconServiceOperator(
            task_id="delete_timeoff",
            endpoint="/services/TimeOffService1.svc/DeleteTimeOff",
            data={
                   "timeOffUri":'{{ result("get_time_off_details_on_sf_booking_id").0.timeoff_uri }}'
                 }
        )

        log_delete_timeoff = rail.WriteLogOperator(
            task_id='log_delete_timeoff',
            log="{{dag_run.conf.employee_log}}",
            severity= "{{ 'Success' if result('get_time_off_details_on_sf_booking_id') | is_truthy else 'Exception'}}",
            message="{{ 'Time off deleted Successfully' if result('get_time_off_details_on_sf_booking_id'\
                ) | is_truthy else 'Booking does not exists in Replicon'}}",
            properties={
                'sf_booking_id': "{{dag_run.conf.sf_booking_id}}",
                'employee_id': "{{dag_run.conf.employee_id}}",
                'time_off_type_description': "{{dag_run.conf.time_off_type_description}}",
                'action':'Delete',
                'status': "{{ 'Success' if result('get_time_off_details_on_sf_booking_id') | is_truthy else 'Exception'}}",
                'details': "{{ 'Time off deleted Successfully' if result('get_time_off_details_on_sf_booking_id'\
                    ) | is_truthy else 'Booking does not exists in Replicon'}}",
            }
        )


        is_sf_booking_id_available = rail.IfOperator(
            task_id='is_sf_booking_id_available',
            test=lambda: rail.result('get_time_off_details_on_sf_booking_id') != [],
            yes_task='is_update_required',
            no_task='create_and_publish_timeoff'
        )

        is_update_required = rail.IfOperator(
            task_id='is_update_required',
            test=request_payload.validate_is_update_required,
            yes_task='reopen_timeoff',
            no_task='log_no_update_required'
        )

        log_no_update_required = rail.WriteLogOperator(
            task_id='log_no_update_required',
            log="{{dag_run.conf.employee_log}}",
            message='No Update Required',
            severity='Exception',
            properties={
                'sf_booking_id': "{{dag_run.conf.sf_booking_id}}",
                'employee_id': "{{dag_run.conf.employee_id}}",
                'time_off_type_description': "{{dag_run.conf.time_off_type_description}}",
                'action':'Update',
                'status': 'Exception',
                'details': 'No Update Required'
            }
        )

        reopen_timeoff = rail.RepliconServiceOperator(
            task_id = "reopen_timeoff",
            endpoint = "/services/TimeOffApprovalService1.svc/Reopen",
            data = lambda : {
                "timeOffUri": rail.result('get_time_off_details_on_sf_booking_id')[0]['timeoff_uri'],
                "unitOfWorkId": str(uuid4()),
                "comments": "Reopened by TimeOff Import Integration"
            }
        )

        put_and_submit_timeoff = rail.RepliconServiceOperator(
            task_id = "put_and_submit_timeoff",
            endpoint = "/services/TimeOffApprovalService1.svc/PutAndSubmitTimeOff",
            data=request_payload.get_reopen_and_put_timeoff_payload,
        )

        create_and_publish_timeoff = rail.RepliconServiceOperator(
            task_id="create_and_publish_timeoff",
            endpoint="services/TimeOffApprovalService1.svc/PutAndSubmitTimeOff",
            data=request_payload.get_create_and_publish_timeoff_payload,
            retries = 0
        )

        get_time_off_approval_status = rail.RepliconServiceOperator(
            task_id="get_time_off_approval_status",
            endpoint="/services/TimeOffApprovalService1.svc/GetApprovalHistoryDetails",
            data={
                "timeOffUri": "{{ result('get_time_off_details_on_sf_booking_id').0.timeoff_uri \
                    if result('get_time_off_details_on_sf_booking_id') else result('create_and_publish_timeoff').uri }}"
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
            log="{{dag_run.conf.employee_log}}",
            severity='Success',
            message="{{ 'Time off Updated Successfully' if result('get_time_off_details_on_sf_booking_id') else  'Time off Added Successfully' }}",
            properties={
                'sf_booking_id': "{{dag_run.conf.sf_booking_id}}",
                'employee_id': "{{dag_run.conf.employee_id}}",
                'time_off_type_description': "{{dag_run.conf.time_off_type_description}}",
                "action": "{{ 'Update' if result('get_time_off_details_on_sf_booking_id') else  'Add' }}",
                'status': 'Success',
                'details': "{{ 'Time off Updated Successfully' if result('get_time_off_details_on_sf_booking_id') else  'Time off Added Successfully' }}",
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{dag_run.conf.employee_log}}",
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'sf_booking_id': "{{dag_run.conf.sf_booking_id}}",
                'employee_id': "{{dag_run.conf.employee_id}}",
                'time_off_type_description': "{{dag_run.conf.time_off_type_description}}",
                'action':'Sync',
                'status': 'Error',
                'details': '{{ get_error_message() }}'
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> is_date_valid

        is_date_valid >> rail.Label('Yes') >> is_timeoff_type_assigned_for_user
        is_date_valid >> rail.Label('No') >> log_invalid_dates >> catch_and_log_errors

        is_timeoff_type_assigned_for_user >> rail.Label('No') >> log_timeoff_not_assigned_to_user >> catch_and_log_errors
        is_timeoff_type_assigned_for_user >> rail.Label('Yes') >> get_time_off_details_on_sf_booking_id
        get_time_off_details_on_sf_booking_id >> is_timeoff_for_delete >> rail.Label('No') >> is_sf_booking_id_available

        is_timeoff_for_delete >> rail.Label('Yes') >> can_delete_timeoff >> rail.Label('No') >> log_delete_timeoff >> catch_and_log_errors
        can_delete_timeoff >> rail.Label('Yes') >> delete_timeoff >> log_delete_timeoff
        is_sf_booking_id_available >> rail.Label('Yes') >> is_update_required

        is_sf_booking_id_available >> rail.Label('No') >> create_and_publish_timeoff >> get_time_off_approval_status >> is_timeoff_approved
        is_timeoff_approved >> rail.Label('Yes') >> log_timeoff_success
        is_timeoff_approved >> rail.Label('No') >> force_approve_time_off_entry
        force_approve_time_off_entry >> log_timeoff_success >> catch_and_log_errors

        is_update_required >> reopen_timeoff >> put_and_submit_timeoff >>  get_time_off_approval_status >> is_timeoff_approved
        is_update_required >> rail.Label('No') >> log_no_update_required >> catch_and_log_errors

    return dag

rail.for_each_instance(create_child_dag)
