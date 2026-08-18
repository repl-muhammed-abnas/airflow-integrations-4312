from datetime import timedelta
import pendulum
from airflow.models import Variable
import rail
from sandtechinc.timeoff_booking_import.utils import python_callable
from sandtechinc.timeoff_booking_import.utils import request_payload
from sandtechinc.timeoff_booking_import.utils import response_filters


null = None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_timeoff_booking_child_dagid,
        description=f'Sand Tech Inc Time Off Booking Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_booking_child,
        start_date=pendulum.datetime(2024, 1, 1, tz=config.time_zone),
        schedule_interval=None,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config", extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='timeoff_booking_child_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='timeoff_booking_child_log',
            end_task='catch_and_log_errors',
        )

        timeoff_booking_child_log = rail.CreateLogOperator(
            task_id="timeoff_booking_child_log"
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id='get_user_details',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data = {
                "users": [
                    {
                        "loginName": "{{ dag_run.conf.booking_data.email }}"
                    }
                ]
            },
            data_handler=lambda res: res[0] if len(
                res) > 0 and res[0]["userDetails"]["uri"] else null
        )

        is_user_present = rail.IfOperator(
            task_id='is_user_present',
            test='{{ result("get_user_details") | is_truthy }}',
            yes_task='get_specfic_time_off_type',
            no_task='log_user_not_present'
        )

        log_user_not_present = rail.WriteLogOperator(
            task_id='log_user_not_present',
            log='{{ result("timeoff_booking_child_log") }}',
            message='User not present in Polaris',
            severity='Skipped',
            properties=lambda dag_run: {
                'requestid': '{{ dag_run.conf.booking_data.requestid }}',
                'email': '{{ dag_run.conf.booking_data.email }}',
                'status': 'Skipped',
                'details': 'User not present in Polaris'
            }
        )

        get_specfic_time_off_type = rail.RepliconServiceOperator(
            task_id='get_specfic_time_off_type',
            endpoint='/services/TimeOffService1.svc/GetAllTimeOffTypes',
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf["booking_data"]["timeofftypename"], 'uri')
        )

        is_timeoff_type_present = rail.IfOperator(
            task_id='is_timeoff_type_present',
            test='{{ result("get_specfic_time_off_type") | is_truthy }}',
            yes_task='is_timeoff_type_assigned_to_user',
            no_task='log_timeoff_type_not_present'
        )

        log_timeoff_type_not_present = rail.WriteLogOperator(
            task_id='log_timeoff_type_not_present',
            log='{{ result("timeoff_booking_child_log") }}',
            message='Time Off type {{ dag_run.conf.booking_data.timeofftypename }} is not present in Polaris',
            severity='Skipped',
            properties=lambda dag_run: {
                'requestid': '{{ dag_run.conf.booking_data.requestid }}',
                'email': '{{ dag_run.conf.booking_data.email }}',
                'status': 'Skipped',
                'details': 'Time Off type "'+ dag_run.conf["booking_data"]["timeofftypename"] + '" not present in Polaris'
            }
        )

        is_timeoff_type_assigned_to_user = rail.IfOperator(
            task_id='is_timeoff_type_assigned_to_user',
            test=python_callable.check_timeoff_type_assigned_to_user,
            yes_task='get_action_to_be_performed',
            no_task='log_timeoff_type_not_assigned_to_user'
        )

        log_timeoff_type_not_assigned_to_user = rail.WriteLogOperator(
            task_id='log_timeoff_type_not_assigned_to_user',
            log='{{ result("timeoff_booking_child_log") }}',
            message='Time Off type {{ dag_run.conf.booking_data.timeofftypename }} is not assigned to user in Polaris',
            severity='Skipped',
            properties=lambda dag_run: {
                'requestid': '{{ dag_run.conf.booking_data.requestid }}',
                'email': '{{ dag_run.conf.booking_data.email }}',
                'status': 'Skipped',
                'details': 'Time Off type "'+ dag_run.conf["booking_data"]["timeofftypename"] + '" is not assigned to user in Polaris'
            }
        )

        get_action_to_be_performed =rail.PythonOperator(
            task_id='get_action_to_be_performed',
            python_callable=python_callable.get_action_to_be_performed
        )

        get_time_off_details_on_booking_id = rail.RepliconServiceOperator(
            task_id="get_time_off_details_on_booking_id",
            endpoint="/services/TimeOffListService1.svc/GetData",
            data=request_payload.get_time_off_details_on_booking_id,
            data_handler=response_filters.get_filtered_time_off_details_on_booking_id
        )

        if_timeoff_to_be_created = rail.IfOperator(
            task_id='if_timeoff_to_be_created',
            test=lambda: bool((rail.result("get_action_to_be_performed") == 'add') or (rail.result(
                "get_action_to_be_performed") in ['update', 'update_status'] and len(rail.result("get_time_off_details_on_booking_id")) == 0)),
            yes_task='add_and_submit_timeoff_booking_for_user',
            no_task='if_action_to_be_performed_update'
        )

        add_and_submit_timeoff_booking_for_user = rail.RepliconServiceOperator(
            task_id='add_and_submit_timeoff_booking_for_user',
            endpoint="/services/TimeOffApprovalService1.svc/PutAndSubmitTimeOff",
            data=lambda dag_run: request_payload.put_and_submit_timeoff_payload(dag_run, 'add')
        )

        if_action_to_be_performed_update = rail.IfOperator(
            task_id='if_action_to_be_performed_update',
            test=lambda: rail.result("get_action_to_be_performed") in ['update','update_status'],
            yes_task='update_and_submit_timeoff_booking_for_user',
            no_task='if_action_is_delete'
        )

        update_and_submit_timeoff_booking_for_user = rail.RepliconServiceOperator(
            task_id='update_and_submit_timeoff_booking_for_user',
            endpoint="/services/TimeOffApprovalService1.svc/ReopenPutAndSubmitTimeOff3",
            data=lambda dag_run: request_payload.put_and_submit_timeoff_payload(dag_run, 'update')
        )

        if_action_to_be_performed_update_status = rail.IfOperator(
            task_id='if_action_to_be_performed_update_status',
            test=lambda: rail.result("get_action_to_be_performed") == 'update_status',
            yes_task='get_timeoff_status',
            no_task='log_timeoff_process_success'
        )

        get_timeoff_status = rail.RepliconServiceOperator(
            task_id='get_timeoff_status',
            endpoint="/services/TimeOffService1.svc/GetTimeOffDetails2",
            data=request_payload.get_timeoff_status_payload,
            data_handler=lambda response: response.get('approvalStatus', {}).get('displayText', {})
        )

        if_timeoff_already_approved = rail.IfOperator(
            task_id='if_timeoff_already_approved',
            test=lambda: rail.result('get_timeoff_status') == 'Approved',
            yes_task='log_timeoff_process_success',
            no_task='approve_timeoff_booking_for_user'
        )

        approve_timeoff_booking_for_user = rail.RepliconServiceOperator(
            task_id='approve_timeoff_booking_for_user',
            endpoint="/services/TimeOffApprovalService1.svc/ForceApprove",
            data=request_payload.get_approve_holiday_booking_payload
        )

        if_action_is_delete = rail.IfOperator(
            task_id='if_action_is_delete',
            test=lambda: rail.result("get_action_to_be_performed") == 'delete',
            yes_task='check_previous_booking_present_for_delete',
            no_task='log_invalid_changetype_and_status_combination'
        )

        check_previous_booking_present_for_delete = rail.IfOperator(
            task_id='check_previous_booking_present_for_delete',
            test=lambda: len(rail.result("get_time_off_details_on_booking_id")) > 0,
            yes_task='delete_timeoff_booking_for_user',
            no_task='log_timeoff_process_success'
        )

        delete_timeoff_booking_for_user = rail.RepliconServiceOperator(
            task_id='delete_timeoff_booking_for_user',
            endpoint="/services/TimeOffService1.svc/DeleteTimeOff",
            data={
                "timeOffUri": "{{ result('get_time_off_details_on_booking_id')[0].timeoff_uri }}"
            }
        )

        log_invalid_changetype_and_status_combination = rail.WriteLogOperator(
            task_id='log_invalid_changetype_and_status_combination',
            log='{{ result("timeoff_booking_child_log") }}',
            message='Invalid ChangeType and Status combination for Time Off booking',
            severity='Info',
            properties={
                'requestid': '{{ dag_run.conf.booking_data.requestid }}',
                'email': '{{ dag_run.conf.booking_data.email }}',
                'status': 'Exception',
                'details': 'Invalid ChangeType and Status combination for Time Off booking'
            }
        )

        log_timeoff_process_success = rail.WriteLogOperator(
            task_id='log_timeoff_process_success',
            log='{{ result("timeoff_booking_child_log") }}',
            message='Time Off booking completed successfully',
            severity='Info',
            properties={
                'requestid': '{{ dag_run.conf.booking_data.requestid }}',
                'email': '{{ dag_run.conf.booking_data.email }}',
                'status': 'Success',
                'details': 'Time Off booking processed successfully'
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ result("timeoff_booking_child_log") }}',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error',
            properties={
                'requestid': '{{ dag_run.conf.booking_data.requestid }}',
                'email': '{{ dag_run.conf.booking_data.email }}',
                'status': "Error",
                'details': '{{ get_error_message() }}'
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> timeoff_booking_child_log

        timeoff_booking_child_log >> get_user_details >> is_user_present
        
        is_user_present >> rail.Label('Yes') >> get_specfic_time_off_type >> is_timeoff_type_present
        is_user_present >> rail.Label('No') >> log_user_not_present >> catch_and_log_errors
        
        is_timeoff_type_present >> rail.Label('Yes') >> is_timeoff_type_assigned_to_user
        is_timeoff_type_present >> rail.Label('No') >> log_timeoff_type_not_present >> catch_and_log_errors

        is_timeoff_type_assigned_to_user >> rail.Label('Yes') >> get_action_to_be_performed >> get_time_off_details_on_booking_id
        is_timeoff_type_assigned_to_user >> rail.Label('No') >> log_timeoff_type_not_assigned_to_user >> catch_and_log_errors

        get_time_off_details_on_booking_id >> if_timeoff_to_be_created

        if_timeoff_to_be_created >> rail.Label('Yes') >> add_and_submit_timeoff_booking_for_user >> if_action_to_be_performed_update_status
        if_timeoff_to_be_created >> rail.Label('No') >> if_action_to_be_performed_update

        if_action_to_be_performed_update >> rail.Label('Yes') >> update_and_submit_timeoff_booking_for_user >> if_action_to_be_performed_update_status
        if_action_to_be_performed_update >> rail.Label('No') >> if_action_is_delete

        if_action_to_be_performed_update_status >> rail.Label('Yes') >> get_timeoff_status >> if_timeoff_already_approved
        if_action_to_be_performed_update_status >> rail.Label('No') >> log_timeoff_process_success

        if_timeoff_already_approved >> rail.Label('Yes') >> log_timeoff_process_success
        if_timeoff_already_approved >> rail.Label('No') >> approve_timeoff_booking_for_user >> log_timeoff_process_success

        if_action_is_delete >> rail.Label('Yes') >> check_previous_booking_present_for_delete
        if_action_is_delete >> rail.Label('No') >> log_invalid_changetype_and_status_combination >> catch_and_log_errors

        check_previous_booking_present_for_delete >> rail.Label('Yes') >> delete_timeoff_booking_for_user >> log_timeoff_process_success
        check_previous_booking_present_for_delete >> rail.Label('No') >> log_timeoff_process_success

        log_timeoff_process_success >> catch_and_log_errors

        

    return dag

rail.for_each_instance(create_dag)