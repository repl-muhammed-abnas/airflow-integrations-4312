
from uuid import uuid4
import rail
from airflow.models import Variable
from cohnreznick.timeentry_sync.utils import custom_methods, request_payload, response_filters

null = None


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_each_timeentry_for_user_dagid,
        description=f'Cohnreznick Time Entry Sync process child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.process_each_timesheet_max_active_run,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run_conf",
                                    extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=Variable.get(config.can_run_batch_task,
                              'true').lower() == 'true',
            yes_task="batch_task",
            no_task="is_timeentry_daterange_valid"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="is_timeentry_daterange_valid",
            end_task="catch_and_log_error"
        )

        is_timeentry_daterange_valid = rail.IfOperator(
            task_id="is_timeentry_daterange_valid",
            test="{{ dag_run.conf.is_valid_dates != '0' }}",
            yes_task="is_timesheet_present",
            no_task="log_timeentry_date_outside_user_start_end_date"
        )

        log_timeentry_date_outside_user_start_end_date = rail.WriteLogOperator(
            task_id="log_timeentry_date_outside_user_start_end_date",
            log="{{dag_run.conf.log}}",
            severity="Exception",
            message="Project Not Found",
            properties=lambda dag_run: custom_methods.get_log_message_per_item(
                item={'entry_id': dag_run.conf['entry_id'],
                      'employee_id': dag_run.conf['employee_id']},
                status="Exception",
                action="Validation",
                details=f"Entry date `{dag_run.conf['entry_date']}` is outside User's Start/End Date"
            )
        )

        is_timesheet_present = rail.IfOperator(
            task_id="is_timesheet_present",
            test="{{ dag_run.conf.timesheet_uri == 'na' }}",
            yes_task="create_timesheet_for_period",
            no_task="get_timesheet_details"
        )

        create_timesheet_for_period = rail.RepliconServiceOperator(
            task_id="create_timesheet_for_period",
            endpoint="/services/TimesheetService1.svc/GetTimesheetForDate2",
            data=lambda dag_run: {
                "userUri": dag_run.conf['user_uri'],
                "date": rail.parse_date(dag_run.conf['entry_date'], custom_methods.FEED_ENTRYDATE_DATE_FORMAT),
                "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
            }
        )

        get_timesheet_details = rail.RepliconServiceOperator(
            task_id = "get_timesheet_details",
            endpoint="/services/TimesheetService1.svc/GetTimesheetSummary",
            data={
                "timesheetUri": "{{dag_run.conf.timesheet_uri}}"
            }
        )

        search_time_entry_by_id = rail.RepliconServiceOperator(
            task_id='search_time_entry_by_id',
            endpoint='/services/TimeEntryRevisionGroupListService1.svc/GetData',
            data=request_payload.get_timeentry_id_payload,
            data_handler=response_filters.get_timeentries_list
        )

        is_timeentry_found = rail.IfOperator(
            task_id="is_timeentry_found",
            test="{{result('search_time_entry_by_id') | is_truthy}}",
            yes_task="validate_update_timeentry_details",
            no_task="validate_add_timeentry_details"
        )

        validate_update_timeentry_details = rail.IfOperator(
            task_id="validate_update_timeentry_details",
            test=custom_methods.check_timeentry_validations,
            yes_task="dummy_should_reopen_timesheet",
            no_task="log_negative_timeentry_exception"
        )

        dummy_should_reopen_timesheet = rail.EmptyOperator(
            task_id = "dummy_should_reopen_timesheet"
        )

        should_reopen_timesheet = rail.IfOperator(
            task_id="should_reopen_timesheet",
            test=custom_methods.check_timesheet_is_open,
            yes_task="reopen_timesheet",
            no_task="dummy_should_reopen_timeentry"
        )

        reopen_timesheet = rail.RepliconServiceOperator(
            task_id='reopen_timesheet',
            endpoint="/services/TimesheetApprovalService1.svc/Reopen",
            data={
                "timesheetUri": "{{ dag_run.conf.timesheet_uri }}",
                "unitOfWorkId": str(uuid4()),
                "comments": "Timesheet is reopened by Integration"
            }
        )

        dummy_should_reopen_timeentry = rail.EmptyOperator(
            task_id = "dummy_should_reopen_timeentry"
        )

        should_reopen_timeentry = rail.IfOperator(
            task_id="should_reopen_timeentry",
            test=lambda: rail.result('search_time_entry_by_id')[0]['approvalstatus'] in ['Approved', 'Waiting for Approval'],
            yes_task="reopen_timeentry",
            no_task="is_actual_hours_zero"
        )

        reopen_timeentry = rail.RepliconServiceOperator(
            task_id = "reopen_timeentry",
            endpoint="/services/TimeEntryRevisionGroupApprovalService1.svc/Reopen",
            data={
                "timeEntryRevisionGroupUri": "{{result('search_time_entry_by_id')[0].timeentryrevisiongroup}}",
                "unitOfWorkId": str(uuid4()),
                "comments": "Time Entry Reopened by Integration"
            }
        )

        log_timesheet_reopened = rail.WriteLogOperator(
            task_id="log_timesheet_reopened",
            log="{{ dag_run.conf.timesheet_reopened_log }}",
            message="TS is reopened",
            severity=custom_methods.get_timesheet_status,
            properties=lambda dag_run: {
                "ts_uri": dag_run.conf['timesheet_uri'],
                "timesheet_status_uri": dag_run.conf['timesheet_status_uri'],
                "timesheet_status": dag_run.conf['timesheet_status'],
                "user_login_name": dag_run.conf['user_login_name'],
                "user_uri": dag_run.conf["user_uri"]
            }
        )


        is_actual_hours_zero = rail.IfOperator(
            task_id = 'is_actual_hours_zero',
            test= "{{ dag_run.conf.actual_hours == 0}}",
            yes_task = "remove_timeentry",
            no_task= "update_timeentry"
        )

        remove_timeentry = rail.RepliconServiceOperator(
            task_id = "remove_timeentry",
            endpoint="/services/TimeEntryRevisionGroupService1.svc/DeleteTimeEntryRevisionGroup",
            data={
                "timeEntryRevisionGroupUri": "{{ result('search_time_entry_by_id')[0].timeentryrevisiongroup }}"
            }
        )

        log_timeentry_removed = rail.WriteLogOperator(
            task_id="log_timeentry_removed",
            log="{{dag_run.conf.log}}",
            severity="Success",
            message="Time entry removed successfully",
            properties=lambda dag_run: custom_methods.get_log_message_per_item(
                item={'entry_id': dag_run.conf['entry_id'],
                      'employee_id': dag_run.conf['employee_id']},
                status="Success",
                action="Delete",
                details="Time entry removed successfully"
            )
        )

        update_timeentry = rail.RepliconServiceOperator(
            task_id="update_timeentry",
            endpoint="/services/TimeEntryRevisionGroupService1.svc/PutTimeEntryRevisionGroup",
            data=request_payload.update_time_entry_revision_payload
        )

        validate_add_timeentry_details = rail.IfOperator(
            task_id="validate_add_timeentry_details",
            test=custom_methods.check_timeentry_validations_add,
            yes_task="is_actual_hours_zero_add",
            no_task="log_negative_timeentry_exception"
        )

        is_actual_hours_zero_add =  rail.IfOperator(
            task_id = "is_actual_hours_zero_add",
            test="{{ dag_run.conf.actual_hours == 0}}",
            yes_task="log_actual_hours_zero_for_add",
            no_task="add_time_entry"
        )

        log_actual_hours_zero_for_add = rail.WriteLogOperator(
            task_id="log_actual_hours_zero_for_add",
            log="{{dag_run.conf.log}}",
            severity="Exception",
            message="Time entry not added as hours is 0",
            properties=lambda dag_run: custom_methods.get_log_message_per_item(
                item={'entry_id': dag_run.conf['entry_id'],
                      'employee_id': dag_run.conf['employee_id']},
                status="Exception",
                action="Add",
                details="Time entry not added as hours is 0"
            )
        )

        log_negative_timeentry_exception = rail.WriteLogOperator(
            task_id="log_negative_timeentry_exception",
            log="{{dag_run.conf.log}}",
            severity="Exception",
            message="Negative Time Adjustment is less than 0",
            properties=lambda dag_run: custom_methods.get_log_message_per_item(
                item={'entry_id': dag_run.conf['entry_id'],
                      'employee_id': dag_run.conf['employee_id']},
                status="Exception",
                action="Validation",
                details="Negative Time Adjustment is less than 0"
            )
        )

        add_time_entry = rail.RepliconServiceOperator(
            task_id='add_time_entry',
            endpoint="/services/TimeEntryRevisionGroupService1.svc/PutTimeEntryRevisionGroup",
            data=request_payload.put_time_entry_revision_payload
        )

        log_success = rail.WriteLogOperator(
            task_id="log_success",
            message=lambda: f"Time entry {'updated' if bool(rail.result('search_time_entry_by_id')) else 'added'} successfully",
            log="{{dag_run.conf.log}}",
            severity="Success",
            properties=lambda dag_run: custom_methods.get_log_message_per_item(
                item={'entry_id': dag_run.conf['entry_id'],
                      'employee_id': dag_run.conf['employee_id']},
                status="Success",
                action='Updated' if bool(rail.result(
                    'search_time_entry_by_id')) else 'Added',
                details=f"Time entry {'updated' if bool(rail.result('search_time_entry_by_id')) else 'added'} successfully"
            )
        )

        log_timesheet_for_recalc = rail.WriteLogOperator(
            task_id = "log_timesheet_for_recalc",
            log="{{dag_run.conf.recalc_log}}",
            message="recalc TS",
            severity="Recalc",
            properties= lambda dag_run:{
                "ts_uri": dag_run.conf["timesheet_uri"] if (
                            dag_run.conf['timesheet_found']=="Yes") else rail.result('create_timesheet_for_period')['timesheet']['uri']
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            log="{{dag_run.conf.log}}",
            trigger_rule="one_failed",
            message="{{ get_error_message() }}",
            severity="Error",
            properties={
                "entry_id": "{{dag_run.conf.entry_id}}",
                "employee_id": "{{dag_run.conf.employee_id}}",
                "status": "Error",
                "action": "{{ 'Updated' if result('search_time_entry_by_id') else 'Added'}}",
                "details": "{{ get_error_message() }}"
            }
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> is_timeentry_daterange_valid
        is_timeentry_daterange_valid >> rail.Label(
            "Yes") >> is_timesheet_present >> rail.Label("Yes") >> get_timesheet_details >> search_time_entry_by_id
        is_timeentry_daterange_valid >> rail.Label(
            "No") >> log_timeentry_date_outside_user_start_end_date >> rail.Label("On Error") >> catch_and_log_error
        is_timesheet_present >> rail.Label("No") >> create_timesheet_for_period >> search_time_entry_by_id >> is_timeentry_found >> rail.Label("Yes")\
            >> validate_update_timeentry_details >> rail.Label("No") >> log_negative_timeentry_exception >> rail.Label("On Error") >> catch_and_log_error
        validate_add_timeentry_details >> rail.Label(
            "No") >> log_negative_timeentry_exception
        is_timeentry_found >> rail.Label("No") >> validate_add_timeentry_details >> rail.Label(
            "Yes") >> is_actual_hours_zero_add >> rail.Label("No") >> add_time_entry >> log_success \
                >> log_timesheet_for_recalc >> rail.Label("On Error") >> catch_and_log_error
        is_actual_hours_zero_add >> rail.Label("Yes") >> log_actual_hours_zero_for_add >> rail.Label("On Error") >> catch_and_log_error
        validate_update_timeentry_details >> rail.Label("Yes") >> dummy_should_reopen_timesheet\
              >> should_reopen_timesheet >> rail.Label("Yes") >> reopen_timesheet >> log_timesheet_reopened >> dummy_should_reopen_timeentry
        should_reopen_timesheet >> rail.Label("No") >> dummy_should_reopen_timeentry \
            >> should_reopen_timeentry >> rail.Label("Yes") >> reopen_timeentry >> \
                is_actual_hours_zero >> rail.Label("Yes") >> remove_timeentry >> log_timeentry_removed >> rail.Label("On Error") >> catch_and_log_error
        should_reopen_timeentry >> rail.Label("No") >> is_actual_hours_zero >> rail.Label("No") >> update_timeentry >> log_success

    return dag


rail.for_each_instance(create_child_dag)
