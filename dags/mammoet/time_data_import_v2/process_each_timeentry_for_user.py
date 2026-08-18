from uuid import uuid4
import rail
from mammoet.time_data_import_v2.utils import custom_methods, request_payload, response_filter

#pylint: disable=too-many-statements
def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_each_timeentry_dagid,
        description=f'mammoet Time Entry Sync process time entry child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_max_active_run,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run_conf",extra_config=config)

        is_mrs_entry = rail.IfOperator(
            task_id="is_mrs_entry",
            test="{{ dag_run.conf.sourcesystem == 'Non-CATS' }}",
            yes_task="dummy_add_timeentry_details",
            no_task="is_timeentry_daterange_valid"
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
                item={'counter': dag_run.conf['counter'],
                      'employeenumber': dag_run.conf['employeenumber'],
                      'workdate': dag_run.conf['workdate']},
                status="Exception",
                action="Validation",
                details=f"Entry date `{dag_run.conf['workdate']}` is outside User's Start/End Date"
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
                "date": rail.parse_date(dag_run.conf['workdate'], custom_methods.FEED_ENTRYDATE_DATE_FORMAT),
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

        is_replicon_id_available = rail.IfOperator(
            task_id='is_replicon_id_available',
            test="{{ dag_run.conf.extdocumentno | is_truthy }}",
            yes_task='search_time_entry_by_id',
            no_task='is_counter_id_available'
        )

        search_time_entry_by_id = rail.RepliconServiceOperator(
            task_id='search_time_entry_by_id',
            endpoint='/services/TimeEntryRevisionGroupListService1.svc/GetData',
            data=lambda dag_run: request_payload.get_timeentry_id_payload(dag_run,'replicon_id'),
            data_handler=response_filter.get_timeentries_list
        )

        is_counter_id_available = rail.IfOperator(
            task_id='is_counter_id_available',
            test="{{ dag_run.conf.counter | is_truthy }}",
            yes_task='search_time_entry_by_counter_id',
            no_task='is_timeentry_found'
        )

        search_time_entry_by_counter_id = rail.RepliconServiceOperator(
            task_id='search_time_entry_by_counter_id',
            endpoint='/services/TimeEntryRevisionGroupListService1.svc/GetData',
            data=lambda dag_run: request_payload.get_timeentry_id_payload(dag_run,'sap_id'),
            data_handler=response_filter.get_timeentries_list
        )

        is_timeentry_found = rail.IfOperator(
            task_id="is_timeentry_found",
            test="{{ result('search_time_entry_by_id') | is_truthy or result('search_time_entry_by_counter_id') | is_truthy }}",
            yes_task="dummy_should_reopen_timesheet",
            no_task="dummy_add_timeentry_details"
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
                "comments": "Timesheet is reopened by Integration (Time Data Import)"
            }
        )

        dummy_should_reopen_timeentry = rail.EmptyOperator(
            task_id = "dummy_should_reopen_timeentry"
        )

        def check_reopen_timeentry():
            if rail.result('search_time_entry_by_id'):
                return {
                    'status': rail.result('search_time_entry_by_id')[0]['approvalstatus'] in ['Approved', 'Waiting for Approval'],
                    'timeentryrevisiongroup': rail.result('search_time_entry_by_id')[0]['timeentryrevisiongroup']
                }
            return {
                    'status': rail.result('search_time_entry_by_counter_id')[0]['approvalstatus'] in ['Approved', 'Waiting for Approval'],
                    'timeentryrevisiongroup': rail.result('search_time_entry_by_counter_id')[0]['timeentryrevisiongroup']
                }

        should_reopen_timeentry = rail.IfOperator(
            task_id="should_reopen_timeentry",
            test=lambda: check_reopen_timeentry()['status'],
            yes_task="reopen_timeentry",
            no_task="get_time_entry_details_for_update"
        )

        reopen_timeentry = rail.RepliconServiceOperator(
            task_id = "reopen_timeentry",
            endpoint="/services/TimeEntryRevisionGroupApprovalService1.svc/Reopen",
            data=lambda: {
                "timeEntryRevisionGroupUri": check_reopen_timeentry()['timeentryrevisiongroup'],
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

        get_time_entry_details_for_update = rail.RepliconServiceOperator(
            task_id = 'get_time_entry_details_for_update',
            endpoint= '/services/TimeEntryRevisionGroupService1.svc/GetTimeEntryRevisionGroupsDetails',
            data=lambda: {
                "timeEntryRevisionGroups": [
                    {
                        "uri": check_reopen_timeentry()['timeentryrevisiongroup'],
                    }
                ]
            },
            data_handler= response_filter.get_activity_type_oef_uri
        )

        update_timeentry = rail.RepliconServiceOperator(
            task_id="update_timeentry",
            endpoint="/services/TimeEntryRevisionGroupService1.svc/PutTimeEntryRevisionGroup",
            data=lambda dag_run: request_payload.update_time_entry_revision_payload(dag_run,action='oef_update')
        )

        dummy_add_timeentry_details = rail.EmptyOperator(
            task_id="dummy_add_timeentry_details",
        )

        add_time_entry = rail.RepliconServiceOperator(
            task_id='add_time_entry',
            endpoint="/services/TimeEntryRevisionGroupService1.svc/PutTimeEntryRevisionGroup",
            data=request_payload.put_time_entry_revision_payload
        )

        get_time_entry_details = rail.RepliconServiceOperator(
            task_id = 'get_time_entry_details',
            endpoint= '/services/TimeEntryRevisionGroupService1.svc/GetTimeEntryRevisionGroupsDetails',
            data=lambda: {
                "timeEntryRevisionGroups": [
                    {
                        "uri": rail.result("add_time_entry")['uri'],
                    }
                ]
            },
            data_handler= lambda resp: resp[0]['integerId']
        )

        update_replicon_id_oef = rail.RepliconServiceOperator(
            task_id='update_replicon_id_oef',
            endpoint="/services/TimeEntryRevisionGroupService1.svc/PutTimeEntryRevisionGroup",
            data=lambda dag_run: request_payload.put_time_entry_revision_payload(dag_run,action='update')
        )

        def force_approve_payload():
            time_entry_uri = rail.result(
                'search_time_entry_by_id')[0]['timeentryrevisiongroup'] if bool(
                    rail.result('search_time_entry_by_id')) else rail.result(
                'search_time_entry_by_counter_id')[0]['timeentryrevisiongroup'] if bool(
                    rail.result('search_time_entry_by_counter_id')) else rail.result('add_time_entry')['uri']
            return {
                "timeEntryRevisionGroupUri": time_entry_uri,
                "unitOfWorkId": str(uuid4()),
                "comments": "Force Approved By Integration"
            }

        can_approve_time_entry = rail.IfOperator(
            task_id="can_approve_time_entry",
            test="{{ dag_run.conf.sourcesystem == 'CATS' }}",
            yes_task="force_approve_time_entry",
            no_task="log_success"
        )

        force_approve_time_entry = rail.RepliconServiceOperator(
            task_id='force_approve_time_entry',
            endpoint="/services/TimeEntryRevisionGroupApprovalService1.svc/ForceApprove",
            data=force_approve_payload
        )

        log_success = rail.WriteLogOperator(
            task_id="log_success",
            message=lambda: f"Time entry {'updated' if bool(rail.result('update_timeentry')) else 'added'} successfully",
            log="{{dag_run.conf.log}}",
            severity="Success",
            properties=lambda dag_run: custom_methods.get_log_message_per_item(
                item={'counter': dag_run.conf['counter'],
                      'employeenumber': dag_run.conf['employeenumber'],
                      'workdate': dag_run.conf['workdate']},
                status="Success",
                action='Update' if bool(rail.result(
                    'update_timeentry')) else 'Add',
                details=f"Time entry {'updated' if bool(rail.result('update_timeentry')) else 'added'} successfully"
            )
        )

        log_timesheet_for_recalc = rail.WriteLogOperator(
            task_id = "log_timesheet_for_recalc",
            log="{{dag_run.conf.recalc_log}}",
            message="recalc TS",
            severity="Recalc",
            properties= lambda dag_run:{
                "ts_uri": dag_run.conf["timesheet_uri"] if (
                            dag_run.conf['timesheet_found']=="Yes") else rail.result(
                                'create_timesheet_for_period')['timesheet']['uri'] if dag_run.conf['sourcesystem'] == 'CATS' else None
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            log="{{dag_run.conf.log}}",
            trigger_rule="one_failed",
            message="{{ get_error_message() }}",
            severity="Error",
            properties={
                "counter_id": "{{dag_run.conf.counter}}",
                "employee_id": "{{dag_run.conf.employeenumber}}",
                "entrydate": "{{dag_run.conf.workdate}}",
                "status": "Error",
                "action": "{{ 'Update' if result('update_timeentry') else 'Add'}}",
                "details": "{{ get_error_message() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info= lambda dag_run: {
                "employee_id": dag_run.conf['employeenumber'],
                "counter_id": dag_run.conf['counter'],
                "workdate": dag_run.conf['workdate'],
                "time_entry_type": dag_run.conf['abs_att_type'],
                "entry_id": dag_run.conf['extdocumentno'],
                "sourcesystem": dag_run.conf['sourcesystem'],
                "starttime": dag_run.conf['starttime'],
                "endtime": dag_run.conf['endtime']
            }
        )

        is_mrs_entry >> rail.Label("Yes") >> dummy_add_timeentry_details
        is_mrs_entry >> rail.Label("No") >> is_timeentry_daterange_valid
        is_timeentry_daterange_valid >> rail.Label(
            "Yes") >> is_timesheet_present >> rail.Label("Yes") >> get_timesheet_details >> is_replicon_id_available
        is_timeentry_daterange_valid >> rail.Label(
            "No") >> log_timeentry_date_outside_user_start_end_date >> rail.Label("On Error") >> catch_and_log_error
        is_timesheet_present >> rail.Label("No") >> create_timesheet_for_period >> is_replicon_id_available
        is_replicon_id_available >> rail.Label("Yes") >> search_time_entry_by_id >> is_counter_id_available
        is_replicon_id_available >> rail.Label("No") >> is_counter_id_available
        is_counter_id_available >> rail.Label("Yes") >> search_time_entry_by_counter_id >> is_timeentry_found
        is_counter_id_available >> rail.Label("No") >> is_timeentry_found
        is_timeentry_found >> rail.Label("Yes") >> dummy_should_reopen_timesheet
        is_timeentry_found >> rail.Label("No") >> dummy_add_timeentry_details >> add_time_entry >>\
            get_time_entry_details >> update_replicon_id_oef>>can_approve_time_entry
        can_approve_time_entry >> rail.Label(
            "Yes") >> force_approve_time_entry >> log_success >> log_timesheet_for_recalc >> rail.Label("On Error") >> catch_and_log_error
        dummy_should_reopen_timesheet >> should_reopen_timesheet >> rail.Label("Yes") >> reopen_timesheet >>\
            log_timesheet_reopened >> dummy_should_reopen_timeentry
        should_reopen_timesheet >> rail.Label("No") >> dummy_should_reopen_timeentry \
            >> should_reopen_timeentry >> rail.Label("Yes") >> reopen_timeentry >> get_time_entry_details_for_update
        should_reopen_timeentry >> rail.Label("No") >> get_time_entry_details_for_update >> update_timeentry >> can_approve_time_entry
        can_approve_time_entry >> rail.Label(
            "Yes") >> log_success
        catch_and_log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag)
