"""
T-Systems Germany/Iberia Time Off Import - Child DAG
Processes individual time off records with single/multi-day logic
"""

from uuid import uuid4
from airflow.models import Variable
import rail
from tsystems.timeoff_import_germany_iberia.utils import custom_methods, request_payload, response_filter

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_timeoff_child_dag_id,
        description='T-Systems Germany/Iberia Time Off Import - Process Individual Record',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_log',
            end_task='catch_and_log_errors'
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        is_timeoff_type_available_in_mapper = rail.IfOperator(
            task_id="is_timeoff_type_available_in_mapper",
            test=lambda dag_run: bool(dag_run.conf['timeoff_type_detail']),
            yes_task="get_user_on_empid",
            no_task="log_timeoff_type_not_available_in_mapper"
        )

        log_timeoff_type_not_available_in_mapper = rail.WriteLogOperator(
            task_id='log_timeoff_type_not_available_in_mapper',
            log='{{result("create_log")}}',
            message="Time off Type '{{dag_run.conf.time_off_type}}' is not available in mapper",
            severity='Exception',
            properties={
                'employee_id': "{{dag_run.conf.employee_id}}",
                'transaction_id': "{{dag_run.conf.transaction_id}}",
                'action':'Validation',
                'status': 'Exception',
                'details': "Time off Type '{{dag_run.conf.time_off_type}}' is not available in mapper",
            }
        )

        get_user_on_empid = rail.RepliconServiceOperator(
            task_id="get_user_on_empid",
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_user_on_empid_payload,
            data_handler=response_filter.get_filtered_output_empid
        )

        is_user_present = rail.IfOperator(
            task_id="is_user_present",
            test=lambda: bool(rail.result('get_user_on_empid')),
            yes_task="get_user_info",
            no_task="log_user_not_present"
        )

        log_user_not_present = rail.WriteLogOperator(
            task_id='log_user_not_present',
            log='{{result("create_log")}}',
            message="User with cid '{{dag_run.conf.employee_id}}' is not present/disabled in replicon",
            severity='Exception',
            properties={
                'employee_id': "{{dag_run.conf.employee_id}}",
                'transaction_id': "{{dag_run.conf.transaction_id}}",
                'action':'Validation',
                'status': 'Exception',
                'details': "User with cid '{{dag_run.conf.employee_id}}' is not present/disabled in replicon",
            }
        )

        get_user_info = rail.RepliconServiceOperator(
            task_id="get_user_info",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": "{{ result('get_user_on_empid').0.uri}}",
                        "loginName": None,
                        "parameterCorrelationId": None
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=response_filter.get_filtered_output_user_info
        )

        is_date_valid = rail.IfOperator(
            task_id="is_date_valid",
            test=custom_methods.validate_dates,
            yes_task="is_timeoff_template_present",
            no_task="log_invalid_dates"
        )

        log_invalid_dates = rail.WriteLogOperator(
            task_id='log_invalid_dates',
            log="{{dag_run.conf.employee_log}}",
            severity='Exception',
            message=custom_methods.get_invalid_datetime_exception,
            properties=lambda dag_run:{
                'employee_id': "{{dag_run.conf.employee_id}}",
                'transaction_id': "{{dag_run.conf.transaction_id}}",
                'action':'Validation',
                'status': 'Exception',
                'details': custom_methods.get_invalid_datetime_exception
            },
        )

        is_timeoff_template_present = rail.IfOperator(
            task_id="is_timeoff_template_present",
            test=lambda: bool(rail.result('get_user_info')[0]['timeoff_template']),
            yes_task="get_all_assigned_time_off_type_for_user",
            no_task="log_timeoff_template_not_present"
        )

        log_timeoff_template_not_present = rail.WriteLogOperator(
            task_id='log_timeoff_template_not_present',
            log='{{result("create_log")}}',
            message='Time Off Template is not assigned to the User "{{dag_run.conf.employee_id}}"',
            severity='Exception',
            properties={
                'employee_id': "{{dag_run.conf.employee_id}}",
                'transaction_id': "{{dag_run.conf.transaction_id}}",
                'action':'Validation',
                'status': 'Exception',
                'details': 'Time Off Template is not assigned to the User "{{dag_run.conf.employee_id}}"'
            }
        )

        get_all_assigned_time_off_type_for_user = rail.RepliconServiceOperator(
            task_id='get_all_assigned_time_off_type_for_user',
            endpoint='/services/TimeOffService1.svc/GetTimeOffTypeAssignmentsForUser',
            data={
                "userUri": "{{result('get_user_on_empid').0.uri}}"
            },
            data_handler=lambda response: list(map(lambda row: row['uri'], response))
        )

        is_timeoff_type_assigned_for_user = rail.IfOperator(
            task_id="is_timeoff_type_assigned_for_user",
            test=lambda dag_run: 
                dag_run.conf['timeoff_type_detail'][0]['uri'] in rail.result('get_all_assigned_time_off_type_for_user'),
            yes_task="get_time_off_details_on_transaction_id",
            no_task="log_timeoff_not_assigned_to_user"
        )

        log_timeoff_not_assigned_to_user = rail.WriteLogOperator(
            task_id='log_timeoff_not_assigned_to_user',
            log="{{result('create_log')}}",
            severity='Exception',
            message='Time off Type - {{ dag_run.conf.time_off_type }} is not assigned/disabled for user',
            properties={
                'employee_id': "{{dag_run.conf.employee_id}}",
                'transaction_id': "{{dag_run.conf.transaction_id}}",
                'action':'Validation',
                'status': 'Exception',
                'details': 'Time off Type - {{ dag_run.conf.time_off_type }} is not assigned/disabled for user',
            }
        )

        get_time_off_details_on_transaction_id = rail.RepliconServiceOperator(
            task_id="get_time_off_details_on_transaction_id",
            endpoint="/services/TimeOffListService1.svc/GetData",
            data=request_payload.get_time_off_details_on_transaction_id,
            data_handler=response_filter.get_filtered_time_off_details_on_transaction_id
        )

        is_timeoff_for_delete = rail.IfOperator(
            task_id='is_timeoff_for_delete',
            test=lambda dag_run: dag_run.conf['duration_hours'] in [0.00, 0 , "0.00", "0","0,00"],
            yes_task='can_delete_timeoff',
            no_task='is_transaction_id_available'
        )

        can_delete_timeoff = rail.IfOperator(
            task_id='can_delete_timeoff',
            test=lambda: rail.result('get_time_off_details_on_transaction_id') != [],
            yes_task='delete_timeoff',
            no_task='log_delete_timeoff'
        )

        delete_timeoff = rail.RepliconServiceOperator(
            task_id='delete_timeoff',
            endpoint='/services/TimeOffService1.svc/DeleteTimeOff',
            data={
                "timeOffUri": "{{result('get_time_off_details_on_transaction_id').0.timeoff_uri}}"
            }
        )

        log_delete_timeoff = rail.WriteLogOperator(
            task_id='log_delete_timeoff',
            log="{{result('create_log')}}",
            severity= "{{ 'Success' if result('get_time_off_details_on_transaction_id') | is_truthy else 'Exception'}}",
            message="{{ 'TimeOff Booking deleted Successfully' if result('get_time_off_details_on_transaction_id'\
                ) | is_truthy else 'Timeoff Booking does not exists in Replicon'}}",
            properties={
                'transaction_id': "{{dag_run.conf.transaction_id}}",
                'employee_id': "{{dag_run.conf.employee_id}}",
                'action':'Delete',
                'status': "{{ 'Success' if result('get_time_off_details_on_transaction_id') | is_truthy else 'Exception'}}",
                'details': "{{ 'TimeOff Booking deleted Successfully' if result('get_time_off_details_on_transaction_id'\
                    ) | is_truthy else 'Timeoff Booking does not exists in Replicon'}}",
            }
        )

        is_transaction_id_available = rail.IfOperator(
            task_id='is_transaction_id_available',
            test=lambda: rail.result('get_time_off_details_on_transaction_id') != [],
            yes_task='is_record_changed',
            no_task='create_time_off_draft'
        )

        is_record_changed = rail.IfOperator(
            task_id='is_record_changed',
            test=custom_methods.is_time_off_changed,
            yes_task='reopen_timeoff',
            no_task='log_no_update_required'
        )

        log_no_update_required = rail.WriteLogOperator(
            task_id='log_no_update_required',
            log="{{result('create_log')}}",
            message='Timeoff Booking already available, no update required',
            severity='Exception',
            properties={
                'transaction_id': "{{dag_run.conf.transaction_id}}",
                'employee_id': "{{dag_run.conf.employee_id}}",
                'action':'Update',
                'status': 'Exception',
                'details': 'Timeoff Booking already available, no update required'
            }
        )

        # Reopen timeoff for editing
        reopen_timeoff = rail.RepliconServiceOperator(
            task_id = "reopen_timeoff",
            endpoint = "/services/TimeOffApprovalService1.svc/Reopen",
            data = lambda : {
                "timeOffUri": rail.result('get_time_off_details_on_transaction_id')[0]['timeoff_uri'],
                "unitOfWorkId": str(uuid4()),
                "comments": "Reopened by TimeOff Import - India - Integration"
            }
        )

        put_reopen_timeoff = rail.RepliconServiceOperator(
            task_id="put_reopen_timeoff",
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: request_payload.get_put_timeoff_entry_payload(
                'reopen', dag_run)
        )

        create_time_off_draft = rail.RepliconServiceOperator(
            task_id="create_time_off_draft",
            endpoint="/services/TimeOffService1.svc/CreateNewTimeOffDraft",
            data={
                "ownerUri": "{{ result('get_user_on_empid').0.uri}}"
            }
        )

        put_timeoff_entry = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry",
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: request_payload.get_put_timeoff_entry_payload(
                'new', dag_run)
        )

        publish_time_off_draft = rail.RepliconServiceOperator(
            task_id="publish_time_off_draft",
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data={
                "timeOff": "{{ result('create_time_off_draft')}}"
            }
        )

        put_timeoff_transaction_id_oef_value = rail.RepliconServiceOperator(
            task_id="put_timeoff_transaction_id_oef_value",
            endpoint="/services/TimeOffService1.svc/PutTimeOffExtensionFieldValues",
            data=request_payload.get_put_timeoff_transaction_id_oef_value_payload
        )

        get_time_off_approval_status = rail.RepliconServiceOperator(
            task_id="get_time_off_approval_status",
            endpoint="/services/TimeOffApprovalService1.svc/GetApprovalHistoryDetails",
            data={
                "timeOffUri": "{{ result('get_time_off_details_on_transaction_id').0.timeoff_uri \
                    if result('get_time_off_details_on_transaction_id') else result('publish_time_off_draft').uri }}"
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
            data= lambda :{
                "timeOffUri": rail.result('get_time_off_details_on_transaction_id')[0]['timeoff_uri']
                    if rail.result('get_time_off_details_on_transaction_id') else rail.result('publish_time_off_draft')['uri'],
                "unitOfWorkId": str(uuid4()),
                "comments": "Force Approved By TimeOff Import India Integration"
            }
        )

        log_timeoff_success = rail.WriteLogOperator(
            task_id='log_timeoff_success',
            log="{{result('create_log')}}",
            severity='Success',
            message="{{ 'Time off Booking Updated Successfully' if result('get_time_off_details_on_transaction_id') else  'Time off Booking Added Successfully' }}",
            properties={
                'transaction_id': "{{dag_run.conf.transaction_id}}",
                'employee_id': "{{dag_run.conf.employee_id}}",
                "action": "{{ 'Update' if result('get_time_off_details_on_transaction_id') else  'Add' }}",
                'status': 'Success',
                'details': "{{ 'Time off Booking Updated Successfully' if result('get_time_off_details_on_transaction_id') else  'Time off Booking Added Successfully' }}",
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{result('create_log')}}",
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'transaction_id': "{{dag_run.conf.transaction_id}}",
                'employee_id': "{{dag_run.conf.employee_id}}",
                'action':'Sync',
                'status': 'Error',
                'details': '{{ get_error_message() }}'
            }
        )

        # Task dependencies
        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> create_log

        create_log >> is_timeoff_type_available_in_mapper

        # Validation failures
        is_timeoff_type_available_in_mapper >> rail.Label("No") >> log_timeoff_type_not_available_in_mapper >> catch_and_log_errors

        # User validation path
        is_timeoff_type_available_in_mapper >> rail.Label("Yes") >> get_user_on_empid >> is_user_present
        is_user_present >> rail.Label("No") >> log_user_not_present >> catch_and_log_errors
        is_user_present >> rail.Label("Yes") >> get_user_info >> is_date_valid

        # Date validation and template checks
        is_date_valid >> rail.Label("No") >> log_invalid_dates >> catch_and_log_errors
        is_date_valid >> rail.Label("Yes") >> is_timeoff_template_present
        is_timeoff_template_present >> rail.Label("No") >> log_timeoff_template_not_present >> catch_and_log_errors
        is_timeoff_template_present >> rail.Label("Yes") >> get_all_assigned_time_off_type_for_user >> is_timeoff_type_assigned_for_user

        # Time off type assignment validation
        is_timeoff_type_assigned_for_user >> rail.Label("No") >> log_timeoff_not_assigned_to_user >> catch_and_log_errors
        is_timeoff_type_assigned_for_user >> rail.Label("Yes") >> get_time_off_details_on_transaction_id >> is_timeoff_for_delete

        # Delete path
        is_timeoff_for_delete >> rail.Label("Yes") >> can_delete_timeoff
        can_delete_timeoff >> rail.Label("Yes") >> delete_timeoff >> log_delete_timeoff >> catch_and_log_errors
        can_delete_timeoff >> rail.Label("No") >> log_delete_timeoff >> catch_and_log_errors

        # Update/Create path
        is_timeoff_for_delete >> rail.Label("No") >> is_transaction_id_available

        # Update existing timeoff
        is_transaction_id_available >> rail.Label("Yes") >> is_record_changed
        is_record_changed >> rail.Label("Yes") >> reopen_timeoff >> put_reopen_timeoff >> get_time_off_approval_status >> is_timeoff_approved
        is_record_changed >> rail.Label("No") >> log_no_update_required >> catch_and_log_errors

        # Create new timeoff
        is_transaction_id_available >> rail.Label("No") >> create_time_off_draft >> put_timeoff_entry >> publish_time_off_draft >> \
            put_timeoff_transaction_id_oef_value >> get_time_off_approval_status >> is_timeoff_approved

        # Approval handling
        is_timeoff_approved >> rail.Label("Yes") >> log_timeoff_success
        is_timeoff_approved >> rail.Label("No") >> force_approve_time_off_entry >> log_timeoff_success

        # Final error handling
        log_timeoff_success >> catch_and_log_errors
    return dag

rail.for_each_instance(create_child_dag)