import uuid
import rail
from addsystems.time_entry_sync.utils import request_payload, response_filter
null = None
# pylint: disable=too-many-statements


def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f"addsystems_time_data_process_each_time_record_child_{config.instance}",
        description=f"addsystems TimeSync Child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        has_mandatory_fields = rail.IfOperator(
            task_id='has_mandatory_fields',
            test=request_payload.mandatory_fields_check,
            yes_task="search_user",
            no_task="log_madatory_fields_not_present"
        )

        log_madatory_fields_not_present = rail.WriteLogOperator(
            task_id='log_madatory_fields_not_present',
            message='\
                {%- if dag_run.conf.item.UserInitials | is_falsy -%} \
                    user is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.item.EventAddDate | is_falsy -%} \
                    entry date is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.item.CustomerCode | is_falsy -%} \
                    client code is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.item.ClienteleCallNum | is_falsy -%} \
                    Preoject Code is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.item.ProjName | is_falsy -%} \
                    Task Name is not present in payload, \
                {%- endif -%}',
            severity='Exception',
            properties={
                'employeeid': "{{dag_run.conf.item.UserInitials}}",
                'entrydate': "{{dag_run.conf.item.EventAddDate}}",
                'clientcode': "{{dag_run.conf.item.CustomerCode}}",
                'projectcode': "{{dag_run.conf.item.ClienteleCallNum}}",
                'taskname': "{{dag_run.conf.item.ProjName}}",
                'comment': "{{dag_run.conf.item.EventSummary}}",
                'InternalId': "{{dag_run.conf.item.InternalId}}"
            }
        )

        search_user = rail.RepliconServiceOperator(
            task_id="search_user",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=request_payload.get_search_user_payload
        )

        is_user_present = rail.IfOperator(
            task_id="is_user_present",
            test=lambda: bool(rail.result('search_user')),
            yes_task="user_has_license",
            no_task="log_user_not_present"
        )

        log_user_not_present = rail.WriteLogOperator(
            task_id='log_user_not_present',
            message="User not available",
            severity='Exception',
            properties={
                'employeeid': "{{dag_run.conf.item.UserInitials}}",
                'entrydate': "{{dag_run.conf.item.EventAddDate}}",
                'clientcode': "{{dag_run.conf.item.CustomerCode}}",
                'projectcode': "{{dag_run.conf.item.ClienteleCallNum}}",
                'taskname': "{{dag_run.conf.item.ProjName}}",
                'comment': "{{dag_run.conf.item.EventSummary}}",
                'InternalId': "{{dag_run.conf.item.InternalId}}"
            }
        )

        user_has_license = rail.IfOperator(
            task_id="user_has_license",
            test=lambda: bool(rail.find_first_by_attr_and_get_attr(rail.result('search_user')[0]['assignedProducts'], 'displayText', 'TimeBill Plus', 'uri')),
            yes_task="get_project_details",
            no_task="log_user_no_license"
        )

        log_user_no_license = rail.WriteLogOperator(
            task_id='log_user_no_license',
            message="User does not have license to do the time entry in Replicon",
            severity='Exception',
            properties={
                'employeeid': "{{dag_run.conf.item.UserInitials}}",
                'entrydate': "{{dag_run.conf.item.EventAddDate}}",
                'clientcode': "{{dag_run.conf.item.CustomerCode}}",
                'projectcode': "{{dag_run.conf.item.ClienteleCallNum}}",
                'taskname': "{{dag_run.conf.item.ProjName}}",
                'comment': "{{dag_run.conf.item.EventSummary}}",
                'InternalId': "{{dag_run.conf.item.InternalId}}"
            }
        )

        get_project_details = rail.RepliconServiceOperator(
            task_id='get_project_details',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data={
                "projects": [
                    {
                        "uri": null,
                        "name": null,
                        "code": '{{ dag_run.conf.item.ClienteleCallNum }}',
                        "parameterCorrelationId": null
                    }
                ]
            },
            response_filter=lambda resp: (resp.json()['d'][0:1] or [
                                          {"projectDetails": null}])[0]['projectDetails']
        )

        is_project_present = rail.IfOperator(
            task_id="is_project_present",
            test=lambda: bool(rail.result('get_project_details')),
            yes_task="get_client_data",
            no_task="log_project_not_present"
        )

        log_project_not_present = rail.WriteLogOperator(
            task_id='log_project_not_present',
            message="Project not available",
            severity='Exception',
            properties={
                'employeeid': "{{dag_run.conf.item.UserInitials}}",
                'entrydate': "{{dag_run.conf.item.EventAddDate}}",
                'clientcode': "{{dag_run.conf.item.CustomerCode}}",
                'projectcode': "{{dag_run.conf.item.ClienteleCallNum}}",
                'taskname': "{{dag_run.conf.item.ProjName}}",
                'comment': "{{dag_run.conf.item.EventSummary}}",
                'InternalId': "{{dag_run.conf.item.InternalId}}"
            }
        )

        get_client_data = rail.RepliconServiceOperator(
            task_id='get_client_data',
            endpoint='/services/ClientListService1.svc/GetData',
            data=request_payload.get_client_payload,
            response_filter=response_filter.check_client_data
        )

        is_client_present = rail.IfOperator(
            task_id="is_client_present",
            test=lambda: bool(rail.result('get_client_data')),
            yes_task="get_task_data",
            no_task="log_client_not_present"
        )

        log_client_not_present = rail.WriteLogOperator(
            task_id='log_client_not_present',
            message="client not available",
            severity='Exception',
            properties={
                'employeeid': "{{dag_run.conf.item.UserInitials}}",
                'entrydate': "{{dag_run.conf.item.EventAddDate}}",
                'clientcode': "{{dag_run.conf.item.CustomerCode}}",
                'projectcode': "{{dag_run.conf.item.ClienteleCallNum}}",
                'taskname': "{{dag_run.conf.item.ProjName}}",
                'comment': "{{dag_run.conf.item.EventSummary}}",
                'InternalId': "{{dag_run.conf.item.InternalId}}"
            }
        )

        get_task_data = rail.RepliconServiceOperator(
            task_id='get_task_data',
            endpoint='/services/TaskListService1.svc/GetData',
            data=request_payload.get_task_payload,
            response_filter=response_filter.check_task_data
        )

        has_task_data = rail.IfOperator(
            task_id='has_task_data',
            test=lambda: bool(rail.result("get_task_data")),
            yes_task='get_timeentry_oef',
            no_task='log_task_not_present'
        )

        get_timsheet_for_date = rail.RepliconServiceOperator(
            task_id='get_timsheet_for_date',
            endpoint='/services/TimesheetService1.svc/GetTimesheetDetailsForDate',
            data=request_payload.get_timesheet_for_date
        )

        get_timeentry_revision_filters = rail.RepliconServiceOperator(
            task_id="get_timeentry_revision_filters",
            endpoint='/services/TimeEntryRevisionGroupListService1.svc/GetAllFilterDefinitions',
            response_filter=response_filter.get_uuid_filter_data
        )

        search_timeentry = rail.RepliconServiceOperator(
            task_id="search_timeentry",
            endpoint='/services/TimeEntryRevisionGroupListService1.svc/GetData',
            data=request_payload.get_timeentry_details,
            response_filter=response_filter.get_timeentry_filter_data
        )

        is_timeentry_present = rail.IfOperator(
            task_id="is_timeentry_present",
            test=lambda: bool(rail.result('search_timeentry')),
            yes_task="get_time_entry_details",
            no_task="is_timesheet_submitted_create"
        )

        create_time_entry = rail.EmptyOperator(
            task_id="create_time_entry"
        )

        is_timesheet_submitted_billable_update = rail.IfOperator(
            task_id="is_timesheet_submitted_billable_update",
            test='''{{result('get_timsheet_for_date').timesheet.statusUri | ends_with('approved') or result('get_timsheet_for_date').timesheet.statusUri | ends_with('waiting') }}''',
            yes_task="reopen_timesheet_billable_update",
            no_task="update_time_entry_billable"
        )

        is_timesheet_submitted_billable_update_completed = rail.IfOperator(
            task_id="is_timesheet_submitted_billable_update_completed",
            test=lambda:rail.get_current_context()['dag_run'].get_task_instance('is_timesheet_submitted_billable_update').current_state() == "success",
            yes_task="update_time_entry_nonbillable",
            no_task="reopen_timesheet_non_billable_update"
        )

        is_timesheet_submitted_non_billable_update = rail.IfOperator(
            task_id="is_timesheet_submitted_non_billable_update",
            test='''{{result('get_timsheet_for_date').timesheet.statusUri | ends_with('approved') or result('get_timsheet_for_date').timesheet.statusUri | ends_with('waiting') }}''',
            yes_task="is_timesheet_submitted_billable_update_completed",
            no_task="update_time_entry_nonbillable"
        )

        reopen_timesheet_non_billable_update = rail.RepliconServiceOperator(
            task_id='reopen_timesheet_non_billable_update',
            endpoint="/services/TimesheetApprovalService1.svc/Reopen",
            data={
                "timesheetUri": '{{ result("get_timsheet_for_date")["timesheet"]["uri"] }}',
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Timesheet is reopened by Integration"
            }
        )

        is_timesheet_submitted_create = rail.IfOperator(
            task_id="is_timesheet_submitted_create",
            test='''{{result('get_timsheet_for_date').timesheet.statusUri | ends_with('approved') or result('get_timsheet_for_date').timesheet.statusUri | ends_with('waiting') }}''',
            yes_task="reopen_timesheet_create",
            no_task="check_nonbillable_and_billable_time"
        )

        reopen_timesheet_create = rail.RepliconServiceOperator(
            task_id='reopen_timesheet_create',
            endpoint="/services/TimesheetApprovalService1.svc/Reopen",
            data={
                "timesheetUri": '{{ result("get_timsheet_for_date")["timesheet"]["uri"] }}',
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Timesheet is reopened by Integration"
            }
        )

        get_time_entry_details = rail.RepliconServiceOperator(
            task_id="get_time_entry_details",
            endpoint='/services/TimeEntryRevisionGroupService1.svc/GetTimeEntryRevisionGroupsDetails',
            data=request_payload.get_time_entry_details,
            response_filter=response_filter.get_billable_nonbillable_timeentry_details
        )

        reopen_timesheet_billable_update = rail.RepliconServiceOperator(
            task_id='reopen_timesheet_billable_update',
            endpoint="/services/TimesheetApprovalService1.svc/Reopen",
            data={
                "timesheetUri": '{{ result("get_timsheet_for_date")["timesheet"]["uri"] }}',
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Timesheet is reopened by Integration"
            }
        )

        get_time_entries_for_user_and_user = rail.RepliconServiceOperator(
            task_id='get_time_entries_for_user_and_user',
            endpoint='/services/TimeEntryService3.svc/GetTimeEntriesForUserAndDateRange',
            data=request_payload.get_time_entries_for_user_and_user
        )

        update_time_entry = rail.EmptyOperator(
            task_id="update_time_entry"
        )

        check_not_zero_nonbillabletime = rail.IfOperator(
            task_id='check_not_zero_nonbillabletime',
            test=lambda dag_run: float(
                dag_run.conf['item']['NonBillableTime']) != 0.0,
            yes_task='is_time_entry_not_updated_manually_non_billable',
            no_task='time_entry_updated_success'
        )

        check_not_zero_billabletime = rail.IfOperator(
            task_id='check_not_zero_billabletime',
            test=lambda dag_run: float(
                dag_run.conf['item']['BillableTime']) != 0.0,
            yes_task='is_time_entry_not_updated_manually_billable',
            no_task='time_entry_updated_success'
        )

        is_time_entry_not_updated_manually_billable = rail.IfOperator(
            task_id="is_time_entry_not_updated_manually_billable",
            # pylint: disable=line-too-long
            test=lambda: (rail.find_first_by_attr_and_get_attr(rail.result('get_time_entries_for_user_and_user'),
                                                               'revisionGroupUri', rail.find_first_by_attr_and_get_attr(rail.result('get_time_entry_details'),
                                                                                                                        'billable', True, 'uri'), 'revision.openingAuthority.actingUser.loginName') == config.login_name) if (rail.find_first_by_attr_and_get_attr(rail.result('get_time_entry_details'), 'billable', True, 'uri')) else True,
            yes_task="is_timesheet_submitted_billable_update",
            no_task="time_entry_updated_success"
        )

        is_manually_updated = rail.EmptyOperator(
            task_id="is_manually_updated"
        )

        is_time_entry_not_updated_manually_non_billable = rail.IfOperator(
            task_id="is_time_entry_not_updated_manually_non_billable",
            # pylint: disable=line-too-long
            test=lambda: (rail.find_first_by_attr_and_get_attr(rail.result('get_time_entries_for_user_and_user'),
                                                               'revisionGroupUri', rail.find_first_by_attr_and_get_attr(rail.result('get_time_entry_details'),
                                                                                                                        'billable', False, 'uri'), 'revision.openingAuthority.actingUser.loginName') == config.login_name) if (rail.find_first_by_attr_and_get_attr(rail.result('get_time_entry_details'), 'billable', False, 'uri')) else True,
            yes_task="is_timesheet_submitted_non_billable_update",
            no_task="time_entry_updated_success"
        )

        check_nonbillabletime = rail.IfOperator(
            task_id='check_nonbillabletime',
            test=lambda dag_run: float(
                dag_run.conf['item']['NonBillableTime']) != 0.0,
            yes_task='process_time_entry_nonbillable',
            no_task='time_entry_success'
        )

        check_billabletime = rail.IfOperator(
            task_id='check_billabletime',
            test=lambda dag_run: float(
                dag_run.conf['item']['BillableTime']) != 0.0,
            yes_task='process_time_entry_billable',
            no_task='time_entry_success'
        )

        check_nonbillable_and_billable_time = rail.IfOperator(
            task_id='check_nonbillable_and_billable_time',
            test=lambda dag_run: (float(
                dag_run.conf['item']['NonBillableTime']) == 0.0) and (float(
                    dag_run.conf['item']['BillableTime']) == 0.0),
            yes_task='process_time_entry_billable',
            no_task='create_time_entry'
        )

        check_nonbillable_and_billable_time_is_not_zero = rail.IfOperator(
            task_id='check_nonbillable_and_billable_time_is_not_zero',
            test=lambda dag_run: ((float(
                dag_run.conf['item']['NonBillableTime']) == 0.0) and (float(
                    dag_run.conf['item']['BillableTime']) == 0.0)),
            yes_task='check_task',
            no_task='is_time_entry_not_updated_manually_non_billable_and_billable'
        )

        check_task = rail.EmptyOperator(
            task_id='check_task'
        )

        is_time_entry_not_updated_manually_non_billable_and_billable = rail.IfOperator(
            task_id="is_time_entry_not_updated_manually_non_billable_and_billable",
            # pylint: disable=line-too-long
            test=lambda: ((rail.find_first_by_attr_and_get_attr(rail.result('get_time_entries_for_user_and_user'),
                                                                'revisionGroupUri', rail.find_first_by_attr_and_get_attr(rail.result('get_time_entry_details'),
                                                                                                                         'billable', False, 'uri'), 'revision.openingAuthority.actingUser.loginName') == config.login_name) if (rail.find_first_by_attr_and_get_attr(rail.result('get_time_entry_details'), 'billable', False, 'uri')) else True) and
            # pylint: disable=line-too-long
            ((rail.find_first_by_attr_and_get_attr(rail.result('get_time_entries_for_user_and_user'),
                                                   'revisionGroupUri', rail.find_first_by_attr_and_get_attr(rail.result('get_time_entry_details'),
                                                                                                            'billable', True, 'uri'), 'revision.openingAuthority.actingUser.loginName') == config.login_name) if (rail.find_first_by_attr_and_get_attr(rail.result('get_time_entry_details'), 'billable', True, 'uri')) else True),
            yes_task="update_time_entry",
            no_task="is_manually_updated"
        )

        update_time_entry_billable = rail.RepliconServiceOperator(
            task_id='update_time_entry_billable',
            endpoint='/services/TimeEntryRevisionGroupService1.svc/PutTimeEntryRevisionGroup',
            data=request_payload.get_update_time_entry_payload_billable
        )

        update_time_entry_nonbillable = rail.RepliconServiceOperator(
            task_id='update_time_entry_nonbillable',
            endpoint='/services/TimeEntryRevisionGroupService1.svc/PutTimeEntryRevisionGroup',
            data=request_payload.get_update_time_entry_payload_nonbillable
        )

        get_timeentry_oef = rail.RepliconServiceOperator(
            task_id="get_timeentry_oef",
            endpoint='/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldBindings',
            data={"bindingContextUri": "urn:replicon:object-type:time-entry"},
            response_filter=response_filter.get_uuid_oef_data
        )

        log_task_not_present = rail.WriteLogOperator(
            task_id='log_task_not_present',
            message="Task not available",
            severity='Exception',
            properties={
                'employeeid': "{{dag_run.conf.item.UserInitials}}",
                'entrydate': "{{dag_run.conf.item.EventAddDate}}",
                'clientcode': "{{dag_run.conf.item.CustomerCode}}",
                'projectcode': "{{dag_run.conf.item.ClienteleCallNum}}",
                'taskname': "{{dag_run.conf.item.ProjName}}",
                'comment': "{{dag_run.conf.item.EventSummary}}",
                'InternalId': "{{dag_run.conf.item.InternalId}}"
            }
        )

        process_time_entry_billable = rail.RepliconServiceOperator(
            task_id='process_time_entry_billable',
            endpoint='/services/TimeEntryRevisionGroupService1.svc/PutTimeEntryRevisionGroup',
            data=request_payload.get_time_entry_payload_billable
        )

        process_time_entry_nonbillable = rail.RepliconServiceOperator(
            task_id='process_time_entry_nonbillable',
            endpoint='/services/TimeEntryRevisionGroupService1.svc/PutTimeEntryRevisionGroup',
            data=request_payload.get_time_entry_payload_nonbillable
        )

        time_entry_success = rail.WriteLogOperator(
            task_id='time_entry_success',
            message="Billable={{dag_run.conf.item.BillableTime}} is {{ get_task_state('process_time_entry_billable') }}" +
            " and Non Billable={{dag_run.conf.item.NonBillableTime}} is {{ get_task_state('process_time_entry_nonbillable') }}",
            severity=lambda: "Success" if ((rail.get_current_context()['dag_run'].get_task_instance('process_time_entry_billable').current_state() == 'success')
                                           and (rail.get_current_context()['dag_run'].get_task_instance('process_time_entry_nonbillable').current_state() ==
                                                'success')) else "Exception",
            properties={
                'employeeid': "{{dag_run.conf.item.UserInitials}}",
                'entrydate': "{{dag_run.conf.item.EventAddDate}}",
                'clientcode': "{{dag_run.conf.item.CustomerCode}}",
                'projectcode': "{{dag_run.conf.item.ClienteleCallNum}}",
                'taskname': "{{dag_run.conf.item.ProjName}}",
                'comment': "{{dag_run.conf.item.EventSummary}}",
                'InternalId': "{{dag_run.conf.item.InternalId}}"
            }
        )

        time_entry_updated_success = rail.WriteLogOperator(
            task_id='time_entry_updated_success',
            message=lambda dag_run: request_payload.get_message(dag_run,
                                                                rail.get_current_context()['dag_run'].get_task_instance(
                                                                    'update_time_entry_billable').current_state(),
                                                                rail.get_current_context()['dag_run'].get_task_instance(
                                                                    'update_time_entry_nonbillable').current_state(),
                                                                rail.get_current_context()['dag_run'].get_task_instance(
                                                                    'is_time_entry_not_updated_manually_billable').current_state(),
                                                                rail.get_current_context()['dag_run'].get_task_instance(
                                                                    'is_time_entry_not_updated_manually_non_billable').current_state()),
            severity=lambda: request_payload.get_severity(rail.get_current_context()['dag_run'].get_task_instance('update_time_entry_billable').current_state(),
                                        rail.get_current_context()['dag_run'].get_task_instance('update_time_entry_nonbillable').current_state()),
            properties={
                'employeeid': "{{dag_run.conf.item.UserInitials}}",
                'entrydate': "{{dag_run.conf.item.EventAddDate}}",
                'clientcode': "{{dag_run.conf.item.CustomerCode}}",
                'projectcode': "{{dag_run.conf.item.ClienteleCallNum}}",
                'taskname': "{{dag_run.conf.item.ProjName}}",
                'comment': "{{dag_run.conf.item.EventSummary}}",
                'InternalId': "{{dag_run.conf.item.InternalId}}"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'employeeid': "{{dag_run.conf.item.UserInitials}}",
                'entrydate': "{{dag_run.conf.item.EventAddDate}}",
                'clientcode': "{{dag_run.conf.item.CustomerCode}}",
                'projectcode': "{{dag_run.conf.item.ClienteleCallNum}}",
                'taskname': "{{dag_run.conf.item.ProjName}}",
                'comment': "{{dag_run.conf.item.EventSummary}}",
                'InternalId': "{{dag_run.conf.item.InternalId}}"
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        has_mandatory_fields >> rail.Label("Yes") >> search_user >> is_user_present >> rail.Label("Yes") >> log_user_not_present\
            >> catch_and_log_errors

        has_mandatory_fields >> rail.Label(
            "No") >> log_madatory_fields_not_present >> catch_and_log_errors

        is_user_present >> rail.Label("No") >> user_has_license >> rail.Label("Yes") >> get_project_details >> is_project_present\
            >> rail.Label("No") >> log_project_not_present >> catch_and_log_errors
        is_project_present >> rail.Label("Yes") >> get_client_data >> is_client_present >> rail.Label(
            "Yes") >> get_task_data >> has_task_data
        
        user_has_license >> rail.Label("No") >> log_user_no_license >> catch_and_log_errors

        is_client_present >> rail.Label(
            "No") >> log_client_not_present >> catch_and_log_errors

        has_task_data >> rail.Label("Yes") >> get_timeentry_oef >> get_timsheet_for_date >> get_timeentry_revision_filters\
            >> search_timeentry >> is_timeentry_present >> rail.Label(
            "No") >> is_timesheet_submitted_create >> rail.Label("Yes") >> reopen_timesheet_create >> check_nonbillable_and_billable_time >> rail.Label("No") >> create_time_entry >> [check_nonbillabletime, check_billabletime]
        
        is_timesheet_submitted_billable_update >> rail.Label("No") >> update_time_entry_billable

        is_timesheet_submitted_non_billable_update >> rail.Label("No") >> update_time_entry_nonbillable

        is_timesheet_submitted_create >> rail.Label("No") >> check_nonbillable_and_billable_time


        is_timeentry_present >> rail.Label("Yes") >> get_time_entry_details >> get_time_entries_for_user_and_user\
            >> check_nonbillable_and_billable_time_is_not_zero >> rail.Label("No")\
            >> is_time_entry_not_updated_manually_non_billable_and_billable >> rail.Label("Yes") \
             >> update_time_entry >> [check_not_zero_nonbillabletime, check_not_zero_billabletime]

        check_nonbillable_and_billable_time_is_not_zero >> rail.Label(
            "Yes") >> check_task >> is_time_entry_not_updated_manually_billable

        check_not_zero_nonbillabletime >> rail.Label("Yes") >> is_time_entry_not_updated_manually_non_billable\
            >> rail.Label("yes") >> is_timesheet_submitted_non_billable_update >> rail.Label("Yes") >> is_timesheet_submitted_billable_update_completed >> update_time_entry_nonbillable >> time_entry_updated_success\
            >> catch_and_log_errors
        
        is_timesheet_submitted_billable_update_completed >> rail.Label("Yes") >> update_time_entry_nonbillable 

        is_timesheet_submitted_billable_update_completed >> rail.Label("No") >> reopen_timesheet_non_billable_update >> update_time_entry_nonbillable

        is_time_entry_not_updated_manually_non_billable >> rail.Label(
            "No") >> time_entry_updated_success >> catch_and_log_errors

        check_not_zero_billabletime >> rail.Label("Yes") >> is_time_entry_not_updated_manually_billable\
            >> rail.Label("Yes")>> is_timesheet_submitted_billable_update >> rail.Label(
            "Yes") >> reopen_timesheet_billable_update >> update_time_entry_billable >> time_entry_updated_success\
            >> catch_and_log_errors

        is_time_entry_not_updated_manually_billable >> rail.Label(
            "No") >> time_entry_updated_success >> catch_and_log_errors

        check_nonbillabletime >> rail.Label("Yes") >>  process_time_entry_nonbillable\
            >> time_entry_success >> catch_and_log_errors >> log_to_sumo

        check_billabletime >> rail.Label("Yes") >> process_time_entry_billable\
            >> time_entry_success >> catch_and_log_errors >> log_to_sumo

        check_nonbillabletime >> rail.Label(
            "No") >> time_entry_success >> catch_and_log_errors >> log_to_sumo

        check_billabletime >> rail.Label(
            "No") >> time_entry_success >> catch_and_log_errors >> log_to_sumo

        has_task_data >> rail.Label(
            "No") >> log_task_not_present >> catch_and_log_errors

        check_nonbillable_and_billable_time >> rail.Label(
            "Yes") >> process_time_entry_billable >> time_entry_success >> catch_and_log_errors >> log_to_sumo

        is_time_entry_not_updated_manually_non_billable_and_billable >> rail.Label(
            "No") >> is_manually_updated >> time_entry_updated_success

        check_not_zero_nonbillabletime >> rail.Label(
            "No") >> time_entry_updated_success >> catch_and_log_errors

        check_not_zero_billabletime >> rail.Label(
            "No") >> time_entry_updated_success >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
