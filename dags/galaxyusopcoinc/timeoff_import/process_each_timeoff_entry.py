from datetime import timedelta
from airflow.models import Variable
import rail
from galaxyusopcoinc.timeoff_import.utils import response_filter
from galaxyusopcoinc.timeoff_import.utils import request_payload


def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'vialtopartners_timeoff_import_child_process_each_timeoff_entry_{config.instance}',
        description='Vialto Partners Timeoff Balance Import Process Each Time Off Entry',
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
            execution_timeout=timedelta(
                days=config.child_process_execution_timeout),
            start_task='is_date_valid',
            end_task='catch_and_log_errors',
        )

        is_date_valid = rail.IfOperator(
            task_id="is_date_valid",
            test=request_payload.test_effective_date,
            yes_task="is_reference_id_available",
            no_task="log_invalid_dates"
        )

        log_invalid_dates = rail.WriteLogOperator(
            task_id='log_invalid_dates',
            severity='Exception',
            message='Invalid timeoffstartdate/timeoffenddate',
            properties=lambda dag_run: {
                'employeeid': dag_run.conf['employeeid'],
                'referenceid':  dag_run.conf['referenceid'],
                'timeoffentryid': dag_run.conf['timeoffentryid'],
                'status': 'Exception',
            },
        )

        is_reference_id_available = rail.IfOperator(
            task_id="is_reference_id_available",
            test=lambda dag_run: bool(dag_run.conf['timeoffuri']),
            yes_task="is_timeoff_type_assigned_for_user",
            no_task="log_reference_id_not_available"
        )

        log_reference_id_not_available = rail.WriteLogOperator(
            task_id='log_reference_id_not_available',
            severity='Exception',
            message='No Time off Type with Reference ID "{{dag_run.conf.referenceid}}" in Replicon instance',
            properties=lambda dag_run: {
                'employeeid': dag_run.conf['employeeid'],
                'referenceid':  dag_run.conf['referenceid'],
                'timeoffentryid': dag_run.conf['timeoffentryid'],
                'status': 'Exception',
            },
        )

        is_timeoff_type_assigned_for_user = rail.IfOperator(
            task_id="is_timeoff_type_assigned_for_user",
            test=lambda dag_run: bool(
                dag_run.conf['timeoffuri'] in dag_run.conf['availabletimeoffuris']),
            yes_task="get_time_off_details_on_entryid",
            no_task="log_timeoff_not_assigned_to_user"
        )

        log_timeoff_not_assigned_to_user = rail.WriteLogOperator(
            task_id='log_timeoff_not_assigned_to_user',
            severity='Exception',
            message='Time off Type with Reference ID "{{dag_run.conf.referenceid}}" is not assigned/disabled for user "{{dag_run.conf.employeeid}}" ',
            properties=lambda dag_run: {
                'employeeid': dag_run.conf['employeeid'],
                'referenceid':  dag_run.conf['referenceid'],
                'timeoffentryid': dag_run.conf['timeoffentryid'],
                'status': 'Exception',
            }
        )

        get_time_off_details_on_entryid = rail.RepliconServiceOperator(
            task_id="get_time_off_details_on_entryid",
            endpoint="/services/TimeOffListService1.svc/GetData",
            data=request_payload.get_time_off_details_on_entryid,
            response_filter=response_filter.get_filtered_time_off_details_on_entryid
        )

        is_timeoff_for_delete = rail.IfOperator(
            task_id='is_timeoff_for_delete',
            test=lambda dag_run: dag_run.conf['flag'] in config.delete_timeoffs,
            yes_task='can_delete_timeoff',
            no_task='is_timeoffentryid_available'
        )

        can_delete_timeoff = rail.IfOperator(
            task_id='can_delete_timeoff',
            test=lambda: rail.result('get_time_off_details_on_entryid') != [],
            yes_task='delete_timeoff',
            no_task='log_delete_timeoff'
        )

        delete_timeoff = rail.RepliconServiceOperator(
            task_id="delete_timeoff",
            endpoint="/services/TimeOffService1.svc/DeleteTimeOff",
            data={
                   "timeOffUri":'{{ result("get_time_off_details_on_entryid").0.timeoffuri }}'
                 }
        )

        log_delete_timeoff = rail.WriteLogOperator(
            task_id='log_delete_timeoff',
            severity= "{{ 'Success' if result('get_time_off_details_on_entryid') | is_truthy else 'Exception'}}",
            message="{{ 'Time off deleted Successfully' if result('get_time_off_details_on_entryid') | is_truthy else 'Booking does not exists in Replicon'}}",
            properties=lambda dag_run: {
                'employeeid': dag_run.conf['employeeid'],
                'referenceid':  dag_run.conf['referenceid'],
                'timeoffentryid': dag_run.conf['timeoffentryid'],
                'status': "{{ 'Success' if result('get_time_off_details_on_entryid') | is_truthy else 'Exception'}}",
            }
        )


        is_timeoffentryid_available = rail.IfOperator(
            task_id='is_timeoffentryid_available',
            test=lambda: rail.result('get_time_off_details_on_entryid') != [],
            yes_task='is_update_required',
            no_task='create_time_off_draft'
        )

        is_update_required = rail.IfOperator(
            task_id='is_update_required',
            test=request_payload.is_update_required_test,
            yes_task='get_time_off_approval_status',
            no_task='log_no_update_required'
        )

        get_time_off_approval_status = rail.RepliconServiceOperator(
            task_id="get_time_off_approval_status",
            endpoint="/services/TimeOffApprovalService1.svc/GetApprovalHistoryDetails",
            data=request_payload.get_time_off_approval_status,
            response_filter=response_filter.get_filtered_time_off_approval_status
        )

        is_timeoff_rejected = rail.IfOperator(
            task_id='is_timeoff_rejected',
            test=lambda: rail.result(
                'get_time_off_approval_status') == 'Rejected',
            yes_task='put_reopen_timeoff',
            no_task='reopen_timeoff'
        )

        log_no_update_required = rail.WriteLogOperator(
            task_id='log_no_update_required',
            message='No Update Required',
            severity='Exception',
            properties=lambda dag_run: {
                'employeeid': dag_run.conf['employeeid'],
                'referenceid':  dag_run.conf['referenceid'],
                'timeoffentryid': dag_run.conf['timeoffentryid'],
                'status': 'Exception',
            }
        )

        reopen_timeoff = rail.RepliconServiceOperator(
            task_id="reopen_timeoff",
            endpoint="/services/TimeOffApprovalService1.svc/Reopen",
            data=request_payload.get_reopen_timeoff,
        )

        put_reopen_timeoff = rail.RepliconServiceOperator(
            task_id="put_reopen_timeoff",
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: request_payload.get_put_timeoff_entry_payload(
                'reopen', dag_run),
        )

        force_approve_reopened_time_off_entry = rail.RepliconServiceOperator(
            task_id="force_approve_reopened_time_off_entry",
            endpoint="/services/TimeOffApprovalService1.svc/ForceApprove",
            data=lambda: request_payload.get_submit_time_off_entry_payload(
                'reopen'),
        )

        log_update_successfully = rail.WriteLogOperator(
            task_id='log_update_successfully',
            severity='Success',
            message='Time off updated Successfully',
            properties=lambda dag_run: {
                'employeeid': dag_run.conf['employeeid'],
                'referenceid':  dag_run.conf['referenceid'],
                'timeoffentryid': dag_run.conf['timeoffentryid'],
                'status': 'Success',
            }
        )

        create_time_off_draft = rail.RepliconServiceOperator(
            task_id="create_time_off_draft",
            endpoint="/services/TimeOffService1.svc/CreateNewTimeOffDraft",
            data=request_payload.get_create_time_off_draft_payload,
        )

        put_timeoff_entry = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry",
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: request_payload.get_put_timeoff_entry_payload(
                'new', dag_run),
        )

        publish_time_off_draft = rail.RepliconServiceOperator(
            task_id="publish_time_off_draft",
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data=request_payload.get_publish_time_off_draft_payload,
        )

        put_timeoff_entry_id_oef_value = rail.RepliconServiceOperator(
            task_id="put_timeoff_entry_id_oef_value",
            endpoint="/services/TimeOffService1.svc/PutTimeOffExtensionFieldValues",
            data=request_payload.get_put_timeoff_entry_id_oef_value_payload,
        )

        force_approve_time_off_entry = rail.RepliconServiceOperator(
            task_id="force_approve_time_off_entry",
            endpoint="/services/TimeOffApprovalService1.svc/ForceApprove",
            data=lambda: request_payload.get_submit_time_off_entry_payload(
                'new'),
        )

        log_added_successfully = rail.WriteLogOperator(
            task_id='log_added_successfully',
            severity='Success',
            message='Time off Added Successfully',
            properties=lambda dag_run: {
                'employeeid': dag_run.conf['employeeid'],
                'referenceid':  dag_run.conf['referenceid'],
                'timeoffentryid': dag_run.conf['timeoffentryid'],
                'status': 'Success',
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                'employeeid': dag_run.conf['employeeid'],
                'referenceid':  dag_run.conf['referenceid'],
                'timeoffentryid': dag_run.conf['timeoffentryid'],
                'status': 'Error',
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> is_date_valid

        is_date_valid >> rail.Label('Yes') >> is_reference_id_available
        is_date_valid >> rail.Label(
            'No') >> log_invalid_dates >> catch_and_log_errors
        is_reference_id_available >> rail.Label(
            'No') >> log_reference_id_not_available >> catch_and_log_errors
        is_reference_id_available >> rail.Label(
            'Yes') >> is_timeoff_type_assigned_for_user
        is_timeoff_type_assigned_for_user >> rail.Label(
            'No') >> log_timeoff_not_assigned_to_user >> catch_and_log_errors
        is_timeoff_type_assigned_for_user >> rail.Label(
            'Yes') >> get_time_off_details_on_entryid
        get_time_off_details_on_entryid >> is_timeoff_for_delete >> rail.Label('No') >> is_timeoffentryid_available
        is_timeoff_for_delete >> rail.Label('Yes') >> can_delete_timeoff >> rail.Label('No') >> log_delete_timeoff >> catch_and_log_errors
        can_delete_timeoff >> rail.Label('Yes') >> delete_timeoff >> log_delete_timeoff
        is_timeoffentryid_available >> rail.Label(
            'Yes') >> is_update_required
        is_timeoffentryid_available >> rail.Label(
            'No') >> create_time_off_draft >> put_timeoff_entry >> publish_time_off_draft
        publish_time_off_draft >> put_timeoff_entry_id_oef_value >> force_approve_time_off_entry >> log_added_successfully >> catch_and_log_errors
        is_update_required >> rail.Label(
            'Yes') >> get_time_off_approval_status >> is_timeoff_rejected >> rail.Label('Yes') >> put_reopen_timeoff
        is_timeoff_rejected >> rail.Label('No') >> reopen_timeoff
        reopen_timeoff >> put_reopen_timeoff >> force_approve_reopened_time_off_entry >> log_update_successfully >> catch_and_log_errors
        is_update_required >> rail.Label(
            'No') >> log_no_update_required >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag_wbs)
