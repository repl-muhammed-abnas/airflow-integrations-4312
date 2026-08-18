from datetime import timedelta
import uuid
import rail
from airflow.models import Variable
from arcticwolf.timeoff_import.utils import request_payload, response_filter, python_callable_methods
null = None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.process_timeoff_records_dagid,
        description=f'Arctic Wolf Timeoff Import Process Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
    ) as dag:


        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_timeoff_import_child_logs'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_timeoff_import_child_logs',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_timeoff_import_child_logs = rail.CreateLogOperator(
            task_id='create_timeoff_import_child_logs'
        )

        if_invalid_date_format = rail.IfOperator(
            task_id='if_invalid_date_format',
            test=lambda dag_run: python_callable_methods.validate_date_format(dag_run.conf['timeoffdate']) is None,
            yes_task="logs_add_entry_invalid_date",
            no_task="if_request_timeoffuri_blank",
        )

        logs_add_entry_invalid_date = rail.WriteLogOperator(
            task_id='logs_add_entry_invalid_date',
            log="{{ result('create_timeoff_import_child_logs') }}",
            message="na",
            severity="Ignored",
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "timeofftype": "{{ dag_run.conf.timeofftype }}",
                "amount": "{{ dag_run.conf.amount }}",
                "unit": "{{dag_run.conf.unit}}",
                "timeoffdate": "{{ dag_run.conf.timeoffdate }}",
                "status": "Ignored",
                "details": "Timeoff date format is invalid",
                "timeoffaction": "{{ dag_run.conf.timeoffaction }}"
            }
        )

        if_request_timeoffuri_blank = rail.IfOperator(
            task_id='if_request_timeoffuri_blank',
            test=lambda dag_run: dag_run.conf['timeoffuri'] is None,
            yes_task="log_invalid_timeoff_entry",
            no_task="if_valid_timeoffaction",
        )

        log_invalid_timeoff_entry = rail.WriteLogOperator(
            task_id='log_invalid_timeoff_entry',
            log="{{ result('create_timeoff_import_child_logs') }}",
            message="na",
            severity="Ignored",
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "timeofftype": "{{ dag_run.conf.timeofftype }}",
                "amount": "{{ dag_run.conf.amount }}",
                "unit": "{{dag_run.conf.unit}}",
                "timeoffdate": "{{ dag_run.conf.timeoffdate }}",
                "status": "Ignored",
                "details": "Timeoff type is not available in Replicon",
                "timeoffaction": "{{ dag_run.conf.timeoffaction }}"
            }
        )

        if_valid_timeoffaction = rail.IfOperator(
            task_id='if_valid_timeoffaction',
            test=lambda dag_run: dag_run.conf['timeoffaction'] not in config.allowed_timeoffactions,
            yes_task="logs_add_entry_incorrect_timeoffaction",
            no_task="get_user_uri",
        )

        logs_add_entry_incorrect_timeoffaction = rail.WriteLogOperator(
            task_id='logs_add_entry_incorrect_timeoffaction',
            log="{{ result('create_timeoff_import_child_logs') }}",
            message="na",
            severity="Ignored",
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "timeofftype": "{{ dag_run.conf.timeofftype }}",
                "amount": "{{ dag_run.conf.amount }}",
                "unit": "{{dag_run.conf.unit}}",
                "timeoffdate": "{{ dag_run.conf.timeoffdate }}",
                "status": "Ignored",
                "details": "Incorrect catProcess value",
                "timeoffaction": "{{ dag_run.conf.timeoffaction }}"
            }
        )

        get_user_uri = rail.RepliconServiceOperator(
            task_id='get_user_uri',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda dag_run: {
                "users": [
                    {
                        "uri": None,
                        "loginName": None,
                        "employeeId": dag_run.conf["employeeid"],
                        "parameterCorrelationId": None
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda res: res[0]['userDetails']['uri'] if res else None
        )

        if_get_user_uri_blank = rail.IfOperator(
            task_id="if_get_user_uri_blank",
            test=lambda: rail.result('get_user_uri') is None,
            yes_task="logs_add_entry_no_user_replicon",
            no_task="get_user_timeoff_assignment_for_timeoff"
        )

        logs_add_entry_no_user_replicon = rail.WriteLogOperator(
            task_id='logs_add_entry_no_user_replicon',
            log="{{ result('create_timeoff_import_child_logs') }}",
            message="na",
            severity="Ignored",
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "timeofftype": "{{ dag_run.conf.timeofftype }}",
                "amount": "{{ dag_run.conf.amount }}",
                "unit": "{{dag_run.conf.unit}}",
                "timeoffdate": "{{ dag_run.conf.timeoffdate }}",
                "status": "Ignored",
                "details": "User with employeeid - {{ dag_run.conf.employeeid }} not found in replicon",
                "timeoffaction": "{{ dag_run.conf.timeoffaction }}"
            }
        )

        get_user_timeoff_assignment_for_timeoff = rail.RepliconServiceOperator(
            task_id='get_user_timeoff_assignment_for_timeoff',
            endpoint="/services/TimeOffService1.svc/GetTimeOffTypeAssignmentsForUser",
            data= lambda: {
                "userUri": rail.result('get_user_uri')
            },
            data_handler=lambda res, dag_run: rail.find_first_by_attr_and_get_attr(res, 'name', dag_run.conf['timeofftype'], 'uri')
        )

        if_get_user_timeoff_assignment_for_timeoff_blank = rail.IfOperator(
            task_id='if_get_user_timeoff_assignment_for_timeoff_blank',
            test='''{{ result('get_user_timeoff_assignment_for_timeoff') | is_falsy }}''',
            yes_task="logs_add_entry_timeofftype_assignment",
            no_task="if_request_timeoffaction_new_entry",
        )

        logs_add_entry_timeofftype_assignment = rail.WriteLogOperator(
            task_id='logs_add_entry_timeofftype_assignment',
            log="{{ result('create_timeoff_import_child_logs') }}",
            message="na",
            severity="Ignored",
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "timeofftype": "{{ dag_run.conf.timeofftype }}",
                "timeoffdate": "{{ dag_run.conf.timeoffdate }}",
                "amount": "{{ dag_run.conf.amount }}",
                "unit": "{{dag_run.conf.unit}}",
                "status": "Ignored",
                "details": "The timeoff type '{{ dag_run.conf.timeofftype }}' is not assigned or is in Disabled status for the user.",
                "timeoffaction": "{{ dag_run.conf.timeoffaction }}"
            }
        )

        if_request_timeoffaction_new_entry = rail.IfOperator(
            task_id='if_request_timeoffaction_new_entry',
            test=lambda dag_run: dag_run.conf['timeoffaction'] == 'Request Time Off',
            yes_task="create_new_time_off_draft",
            no_task="if_request_timeoffaction_update_entry",
        )

        create_new_time_off_draft = rail.RepliconServiceOperator(
            task_id='create_new_time_off_draft',
            endpoint="/services/TimeOffService1.svc/CreateNewTimeOffDraft",
            data=lambda: {
                "ownerUri": rail.result('get_user_uri')
            }
        )

        put_time_off2_partialbooking = rail.RepliconServiceOperator(
            task_id='put_time_off2_partialbooking',
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=request_payload.put_time_off2_partialbooking_payload
        )

        publish_time_off_draft = rail.RepliconServiceOperator(
            task_id='publish_time_off_draft',
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data={
                "timeOff": "{{ result('create_new_time_off_draft') }}"
            }
        )

        force_approve_new_timeoff_entry = rail.RepliconServiceOperator(
            task_id='force_approve_new_timeoff_entry',
            endpoint="/services/TimeOffApprovalService1.svc/ForceApprove",
            data={
                "timeOffUri": "{{ result('publish_time_off_draft').uri }}",
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Approved by Replicon Integration"
            }
        )

        timeoffimport_logs_add_entry_success = rail.WriteLogOperator(
            task_id='timeoffimport_logs_add_entry_success',
            log="{{ result('create_timeoff_import_child_logs') }}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "employeeid": dag_run.conf['employeeid'],
                "timeofftype": dag_run.conf['timeofftype'],
                "timeoffdate": dag_run.conf['timeoffdate'],
                "amount": dag_run.conf['amount'],
                "unit": dag_run.conf['unit'],
                "status": "Success",
                "details": "Timeoff booking added in Replicon",
                "timeoffaction": dag_run.conf['timeoffaction']
            }
        )

        if_request_timeoffaction_update_entry = rail.IfOperator(
            task_id='if_request_timeoffaction_update_entry',
            test=lambda dag_run: dag_run.conf['timeoffaction'] == 'Correct Time Off',
            yes_task="get_timeoff_booking_details",
            no_task="finish",
        )

        get_timeoff_booking_details = rail.RepliconServiceOperator(
            task_id='get_timeoff_booking_details',
            endpoint="/services/TimeOffListService1.svc/GetData",
            data=request_payload.get_timeoff_booking_details_payload,
            response_filter=response_filter.get_timeoff_booking_list
        )

        if_output_timeoffname_present = rail.IfOperator(
            task_id='if_output_timeoffname_present',
            test='''{{ result('get_timeoff_booking_details').timeoffname | is_truthy }}''',
            yes_task="if_request_amount_less_than_0",
            no_task="logs_add_entry_timeoffbooking_missing",
        )

        if_request_amount_less_than_0 = rail.IfOperator(
            task_id='if_request_amount_less_than_0',
            test=lambda dag_run: float(dag_run.conf['amount']) < 0,
            yes_task="if_existing_hours_less_than_abs_request_amount",
            no_task="log_add_hours_to_existing_in_seconds",
        )

        if_existing_hours_less_than_abs_request_amount = rail.IfOperator(
            task_id='if_existing_hours_less_than_abs_request_amount',
            test=lambda dag_run: float(rail.result('get_timeoff_booking_details')[
                'hours']) <= abs(float(dag_run.conf['amount']) * 8 if dag_run.conf['unit']=="Days" else float(dag_run.conf['amount'])),
            yes_task="delete_time_off",
            no_task="log_getthedifferencehoursin_seconds",
        )

        delete_time_off = rail.RepliconServiceOperator(
            task_id='delete_time_off',
            endpoint="/services/TimeOffService1.svc/DeleteTimeOff",
            data={
                "timeOffUri": "{{ result('get_timeoff_booking_details').timeoffbookinguri }}"
            }
        )

        logs_add_entry_booking_deleted = rail.WriteLogOperator(
            task_id='logs_add_entry_booking_deleted',
            log="{{ result('create_timeoff_import_child_logs') }}",
            message="na",
            severity="Success",
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "timeofftype": "{{ dag_run.conf.timeofftype }}",
                "timeoffdate": "{{ dag_run.conf.timeoffdate }}",
                "amount": "{{ dag_run.conf.amount }}",
                "unit": "{{ dag_run.conf.unit }}",
                "status": "Success",
                "details": "Timeoff booking deleted in Replicon",
                "timeoffaction": "{{ dag_run.conf.timeoffaction }}"
            }
        )

        log_getthedifferencehoursin_seconds = rail.PythonOperator(
            task_id='log_getthedifferencehoursin_seconds',
            python_callable=lambda dag_run: int((abs(float(rail.result('get_timeoff_booking_details')['hours'])) - abs(float(dag_run.conf['amount']) * 8
                                                        if dag_run.conf['unit']=="Days" else float(dag_run.conf['amount']))) * 3600)
        )

        if_output_timeoffapprovalstatus_not_equals_to_notsubmitted = rail.IfOperator(
            task_id='if_output_timeoffapprovalstatus_not_equals_to_notsubmitted',
            test='''{{ result('get_timeoff_booking_details').timeoffapprovalstatus != 'Not Submitted' }}''',
            yes_task="reopen_timeoff_booking",
            no_task="create_edit_time_off_draft",
        )

        reopen_timeoff_booking = rail.RepliconServiceOperator(
            task_id='reopen_timeoff_booking',
            endpoint="/services/TimeOffApprovalService1.svc/Reopen",
            data={
                "timeOffUri": "{{ result('get_timeoff_booking_details').timeoffbookinguri }}",
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Reopened by Integration"
            }
        )

        create_edit_time_off_draft = rail.RepliconServiceOperator(
            task_id='create_edit_time_off_draft',
            endpoint="/services/TimeOffService1.svc/CreateEditTimeOffDraft",
            data={
                "timeOffUri": "{{ result('get_timeoff_booking_details').timeoffbookinguri }}"
            }
        )

        put_time_off2_update = rail.RepliconServiceOperator(
            task_id='put_time_off2_update',
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=request_payload.put_time_off2_update_payload
        )

        publish_time_off_draft_update = rail.RepliconServiceOperator(
            task_id='publish_time_off_draft_update',
            endpoint="/services/TimeOffService1.svc/PublishTimeOffDraft",
            data={
                "timeOff": "{{ result('create_edit_time_off_draft') }}"
            }
        )

        force_approve_update_entry = rail.RepliconServiceOperator(
            task_id='force_approve_update_entry',
            endpoint="/services/TimeOffApprovalService1.svc/ForceApprove",
            data={
                "timeOffUri": "{{ result('publish_time_off_draft_update').uri }}",
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Approved by Replicon Integration"
            }
        )

        logs_add_entry_update_success = rail.WriteLogOperator(
            task_id='logs_add_entry_update_success',
            log="{{ result('create_timeoff_import_child_logs') }}",
            message="na",
            severity="Success",
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "timeofftype": "{{ dag_run.conf.timeofftype }}",
                "timeoffdate": "{{ dag_run.conf.timeoffdate }}",
                "amount": "{{ dag_run.conf.amount }}",
                "unit": "{{ dag_run.conf.unit }}",
                "status": "Success",
                "details": "Timeoff booking updated in Replicon",
                "timeoffaction": "{{ dag_run.conf.timeoffaction }}"
            }
        )

        log_add_hours_to_existing_in_seconds = rail.PythonOperator(
            task_id='log_add_hours_to_existing_in_seconds',
            python_callable=lambda dag_run: int((abs(float(rail.result('get_timeoff_booking_details')['hours'])) + abs(float(dag_run.conf['amount']) * 8
                                                            if dag_run.conf['unit']=="Days" else float(dag_run.conf['amount']))) * 3600)
        )

        logs_add_entry_timeoffbooking_missing = rail.WriteLogOperator(
            task_id='logs_add_entry_timeoffbooking_missing',
            log="{{ result('create_timeoff_import_child_logs') }}",
            message="na",
            severity="Ignored",
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "timeofftype": "{{ dag_run.conf.timeofftype }}",
                "timeoffdate": "{{ dag_run.conf.timeoffdate }}",
                "amount": "{{ dag_run.conf.amount }}",
                "unit": "{{ dag_run.conf.unit }}",
                "status": "Ignored",
                "details": "Timeoff booking is not available in Replicon",
                "timeoffaction": "{{ dag_run.conf.timeoffaction }}"
            }
        )
        finish = rail.EmptyOperator(
            task_id='finish',
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ result('create_timeoff_import_child_logs') }}",
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "timeofftype": "{{ dag_run.conf.timeofftype }}",
                "amount": "{{ dag_run.conf.amount }}",
                "unit": "{{dag_run.conf.unit}}",
                "timeoffdate": "{{ dag_run.conf.timeoffdate }}",
                "timeoffaction": "{{ dag_run.conf.timeoffaction }}",
                "status": "Error",
                "details":'{{ get_error_message() }}'
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> create_timeoff_import_child_logs >> if_invalid_date_format
        if_invalid_date_format >> rail.Label('Yes') >> logs_add_entry_invalid_date >> finish
        if_invalid_date_format >> rail.Label('No') >> if_request_timeoffuri_blank
        if_request_timeoffuri_blank >> rail.Label('Yes') >> log_invalid_timeoff_entry >> finish
        if_request_timeoffuri_blank >> rail.Label('No') >> if_valid_timeoffaction
        if_valid_timeoffaction >> rail.Label('Yes') >> logs_add_entry_incorrect_timeoffaction >> finish
        if_valid_timeoffaction >> rail.Label('No') >> get_user_uri >> if_get_user_uri_blank
        if_get_user_uri_blank >> rail.Label('Yes') >> logs_add_entry_no_user_replicon >> finish
        if_get_user_uri_blank >> rail.Label('No') >> get_user_timeoff_assignment_for_timeoff >> if_get_user_timeoff_assignment_for_timeoff_blank
        if_get_user_timeoff_assignment_for_timeoff_blank >> rail.Label('Yes') >> logs_add_entry_timeofftype_assignment >> finish
        if_get_user_timeoff_assignment_for_timeoff_blank >> rail.Label('No') >> if_request_timeoffaction_new_entry
        if_request_timeoffaction_new_entry >> rail.Label('Yes') >> create_new_time_off_draft >> put_time_off2_partialbooking \
        >> publish_time_off_draft >> force_approve_new_timeoff_entry >> timeoffimport_logs_add_entry_success >> finish
        if_request_timeoffaction_new_entry >> rail.Label('No') >> if_request_timeoffaction_update_entry
        if_request_timeoffaction_update_entry >> rail.Label('Yes') >>  get_timeoff_booking_details >> if_output_timeoffname_present
        if_output_timeoffname_present >> rail.Label('Yes') >> if_request_amount_less_than_0
        if_request_amount_less_than_0 >> rail.Label('Yes') >> if_existing_hours_less_than_abs_request_amount
        if_existing_hours_less_than_abs_request_amount >> rail.Label('Yes') >> delete_time_off >> logs_add_entry_booking_deleted >> finish
        if_existing_hours_less_than_abs_request_amount >> rail.Label('No') >> log_getthedifferencehoursin_seconds \
        >> if_output_timeoffapprovalstatus_not_equals_to_notsubmitted
        if_output_timeoffapprovalstatus_not_equals_to_notsubmitted >> rail.Label('Yes') >> reopen_timeoff_booking >> create_edit_time_off_draft
        if_output_timeoffapprovalstatus_not_equals_to_notsubmitted >> rail.Label('No') >> create_edit_time_off_draft \
        >> put_time_off2_update >> publish_time_off_draft_update >> force_approve_update_entry >> logs_add_entry_update_success >> finish
        if_request_amount_less_than_0 >> rail.Label('No') >> log_add_hours_to_existing_in_seconds >> if_output_timeoffapprovalstatus_not_equals_to_notsubmitted
        if_output_timeoffname_present >> rail.Label('No') >> logs_add_entry_timeoffbooking_missing >> finish
        if_request_timeoffaction_update_entry >> rail.Label('No') >> finish

        finish >> catch_and_log_errors >> log_to_sumo
    return dag


rail.for_each_instance(create_dag)
