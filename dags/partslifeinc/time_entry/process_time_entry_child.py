from datetime import timedelta
import uuid
import rail
from airflow.models import Variable
from partslifeinc.time_entry.utils import python_callable_methods, request_payload
null = None
# pylint: disable=too-many-statements


def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.process_time_entry_child_dagid,
        description=f"Partslife Time entry per user Child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_child_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='search_user'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='search_user',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        search_user = rail.RepliconServiceOperator(
            task_id="search_user",
            endpoint='/services/UserListService1.svc/GetData',
            data=request_payload.get_search_user_payload,
            data_handler=lambda res: res['rows'] and res['rows'][0]['cells'][0]['uri']
        )

        if_user_present = rail.IfOperator(
            task_id="if_user_present",
            test=lambda: bool(rail.result('search_user')),
            yes_task="if_invalid_date_format",
            no_task="log_user_not_present"
        )

        log_user_not_present = rail.WriteLogOperator(
            task_id='log_user_not_present',
            message="User not available",
            log="{{dag_run.conf.create_time_entry_logs}}",
            items=lambda dag_run: dag_run.conf["data"],
            severity='Exception',
            properties=lambda dag_run, item: {
                "employeename": dag_run.conf["employee"],
                "timesheet_entry_date": dag_run.conf["timesheet_entry_date"],
                "break_type": item["break_type"],
                "project_name": item["project_name"],
                "punch_in_hr": item["punch_in_hr"],
                "punch_in_min": item["punch_in_min"],
                "punch_out_hr": item["punch_out_hr"],
                "punch_out_min": item["punch_out_min"],
                "status": "Exception",
                "details": "User not available in Replicon",
            }
        )

        # Check if valid date
        if_invalid_date_format = rail.IfOperator(
            task_id='if_invalid_date_format',
            test=lambda dag_run: python_callable_methods.validate_date_format(dag_run.conf['timesheet_entry_date']) is None,
            yes_task="logs_add_entry_invalid_date",
            no_task="get_timesheet_policysets",
        )

        logs_add_entry_invalid_date = rail.WriteLogOperator(
            task_id='logs_add_entry_invalid_date',
            log="{{dag_run.conf.create_time_entry_logs}}",
            items=lambda dag_run: dag_run.conf["data"],
            message="na",
            severity="Exception",
            properties=lambda dag_run, item:{
                "employeename": dag_run.conf["employee"],
                "timesheet_entry_date": dag_run.conf["timesheet_entry_date"],
                "break_type": item["break_type"],
                "project_name": item["project_name"],
                "punch_in_hr": item["punch_in_hr"],
                "punch_in_min": item["punch_in_min"],
                "punch_out_hr": item["punch_out_hr"],
                "punch_out_min": item["punch_out_min"],
                "status": "Exception",
                "details": "Date format is not valid",
            }
        )


        get_timesheet_policysets = rail.RepliconServiceOperator(
            task_id='get_timesheet_policysets',
            endpoint="/services/PolicySetService1.svc/GetAssignedPolicySetsForUser",
            data={
                "userUri": "{{ result('search_user') }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', 'urn:replicon:policy:timesheet', '')
        )

        if_time_punch_timesheet_assigned = rail.IfOperator(
            task_id = 'if_time_punch_timesheet_assigned',
            test =lambda: rail.result('get_timesheet_policysets')['policySet']['name'] == "Time Punches Timesheet"
                    if rail.result('get_timesheet_policysets') else False,
            yes_task='get_timsheet_for_date',
            no_task='log_exceptiom_incorrect_timesheet_assigned'
        )

        log_exceptiom_incorrect_timesheet_assigned = rail.WriteLogOperator(
            task_id='log_exceptiom_incorrect_timesheet_assigned',
            log="{{dag_run.conf.create_time_entry_logs}}",
            items=lambda dag_run: dag_run.conf["data"],
            message="na",
            severity="Exception",
            properties=lambda dag_run, item:{
                "employeename": dag_run.conf["employee"],
                "timesheet_entry_date": dag_run.conf["timesheet_entry_date"],
                "break_type": item["break_type"],
                "project_name": item["project_name"],
                "punch_in_hr": item["punch_in_hr"],
                "punch_in_min": item["punch_in_min"],
                "punch_out_hr": item["punch_out_hr"],
                "punch_out_min": item["punch_out_min"],
                "status": "Exception",
                "details": "Time Punches Timesheet not assigned to the user",
            }
        )


        get_timsheet_for_date = rail.RepliconServiceOperator(
            task_id='get_timsheet_for_date',
            endpoint='/services/TimesheetService1.svc/GetTimesheetDetailsForDate',
            data=lambda dag_run: {
                "userUri": rail.result('search_user'),
                "date": python_callable_methods.validate_date_format(dag_run.conf['timesheet_entry_date']),
                "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
            }
        )

        is_timesheet_submitted = rail.IfOperator(
            task_id="is_timesheet_submitted",
            test='''{{result('get_timsheet_for_date').timesheet.statusUri | ends_with('approved') }}''',
            yes_task="logs_add_entry_timesheet_already_approved",
            no_task="get_existing_punch_for_user"
        )

        logs_add_entry_timesheet_already_approved = rail.WriteLogOperator(
            task_id='logs_add_entry_timesheet_already_approved',
            log="{{dag_run.conf.create_time_entry_logs}}",
            items=lambda dag_run: dag_run.conf["data"],
            message="na",
            severity="Exception",
            properties=lambda dag_run, item:{
                "employeename": dag_run.conf["employee"],
                "timesheet_entry_date": dag_run.conf["timesheet_entry_date"],
                "break_type": item["break_type"],
                "project_name": item["project_name"],
                "punch_in_hr": item["punch_in_hr"],
                "punch_in_min": item["punch_in_min"],
                "punch_out_hr": item["punch_out_hr"],
                "punch_out_min": item["punch_out_min"],
                "status": "Exception",
                "details": "Timesheet already Approved",
            }
        )

        get_existing_punch_for_user = rail.RepliconServiceOperator(
            task_id="get_existing_punch_for_user",
            endpoint='/services/TimePunchService1.svc/BulkGetTimePunchDetailsForUsersAndDateRange',
            data=lambda dag_run:{
                "userUris": [
                    rail.result('search_user')
                ],
                "dateRange": {
                    "startDate": python_callable_methods.validate_date_format(dag_run.conf['timesheet_entry_date']),
                    "endDate": python_callable_methods.validate_date_format(dag_run.conf['timesheet_entry_date']),
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                },
                "timePunchTimeSegmentDateRangeFilterOption": "urn:replicon:time-punch-time-segment-date-range-filter-option:punch-user-time-zone"
                },
            data_handler=lambda res: res[0]['timePunches']
        )

        if_existing_punch_data =  rail.IfOperator(
            task_id = 'if_existing_punch_data',
            test=lambda: bool(rail.result('get_existing_punch_for_user')),
            yes_task='logs_add_entry_existing_punch_data',
            no_task='get_timeentry_oef',
        )

        logs_add_entry_existing_punch_data = rail.WriteLogOperator(
            task_id='logs_add_entry_existing_punch_data',
            log="{{dag_run.conf.create_time_entry_logs}}",
            items=lambda dag_run: dag_run.conf["data"],
            message="na",
            severity="Exception",
            properties=lambda dag_run, item:{
                "employeename": dag_run.conf["employee"],
                "timesheet_entry_date": dag_run.conf["timesheet_entry_date"],
                "break_type": item["break_type"],
                "project_name": item["project_name"],
                "punch_in_hr": item["punch_in_hr"],
                "punch_in_min": item["punch_in_min"],
                "punch_out_hr": item["punch_out_hr"],
                "punch_out_min": item["punch_out_min"],
                "status": "Exception",
                "details": "Punch data already exists",
            }
        )


        get_timeentry_oef = rail.RepliconServiceOperator(
            task_id="get_timeentry_oef",
            endpoint='/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldBindings',
            data={"bindingContextUri": "urn:replicon:object-type:time-punch"},
            data_handler=python_callable_methods.get_timeentry_oef_uri
        )

        mark_overlapping_entries = rail.PythonOperator(
            task_id = 'mark_overlapping_entries',
            python_callable=lambda dag_run: python_callable_methods.update_overlapping_entries(dag_run.conf["data"])
        )

        log_exception_for_overlapping_entries =  rail.WriteLogOperator(
            task_id='log_exception_for_overlapping_entries',
            log="{{dag_run.conf.create_time_entry_logs}}",
            items=lambda dag_run: [obj for obj in dag_run.conf["data"] if obj['overlap']],
            message="na",
            severity="Exception",
            properties=lambda dag_run, item:{
                "employeename": dag_run.conf["employee"],
                "timesheet_entry_date": dag_run.conf["timesheet_entry_date"],
                "break_type": item["break_type"],
                "project_name": item["project_name"],
                "punch_in_hr": item["punch_in_hr"],
                "punch_in_min": item["punch_in_min"],
                "punch_out_hr": item["punch_out_hr"],
                "punch_out_min": item["punch_out_min"],
                "status": "Exception",
                "details": "Overlapping entry",
            }
        )

        payload_list_variable = rail.SetVariableOperator(
            task_id='payload_list_variable',
            append=False,
            name='payload_list_variable',
            value=[]
        )

        all_valid_entries_list = rail.SetVariableOperator(
            task_id='all_valid_entries_list',
            append=False,
            name='all_valid_entries_list',
            value=[]
        )


        for_each_record_start = rail.ForEachOperator(
            task_id = 'for_each_record_start',
            items=lambda dag_run: [obj for obj in dag_run.conf["data"] if not obj['overlap']],
            start_task='if_break_punch_present',
            end_task='for_each_record_end'
        )

        if_break_punch_present = rail.IfOperator(
            task_id = 'if_break_punch_present',
            test=lambda:  bool(rail.result('for_each_record_start')["break_type"]),
            yes_task='get_break_type_uri',
            no_task='if_project_name_present'
        )

        get_break_type_uri =  rail.RepliconServiceOperator(
            task_id='get_break_type_uri',
            endpoint='/services/TimePunchService1.svc/GetPageOfBreakTypesAvailableForUserFilteredByTextSearch',
            data=lambda: {
                "page": "1",
                "pageSize": "10",
                "userUri": rail.result('search_user'),
                "textSearch": {
                    "queryText": rail.result('for_each_record_start')["break_type"],
                    "searchInDisplayText": True,
                    "searchInName": True
                }
            },
            data_handler=lambda resp: resp[0]['uri'] if resp else None
        )

        if_break_type_uri_present = rail.IfOperator(
            task_id = 'if_break_type_uri_present',
            test=lambda:  bool(rail.result('get_break_type_uri')),
            yes_task='insert_break_punch_in_to_payload',
            no_task='logs_add_entry_break_type_notfound'
        )

        insert_break_punch_in_to_payload = rail.SetVariableOperator(
            task_id='insert_break_punch_in_to_payload',
            append=True,
            name='{{ result("payload_list_variable").name }}',
            value=request_payload.get_break_punch_in_to_payload
        )

        insert_break_punch_out_to_payload = rail.SetVariableOperator(
            task_id='insert_break_punch_out_to_payload',
            append=True,
            name='{{ result("payload_list_variable").name }}',
            value=request_payload.get_break_punch_out_to_payload
        )

        logs_add_entry_break_type_notfound =  rail.WriteLogOperator(
            task_id='logs_add_entry_break_type_notfound',
            log="{{dag_run.conf.create_time_entry_logs}}",
            message="Break type not available",
            severity='Exception',
            properties=lambda dag_run:{
                "employeename": dag_run.conf["employee"],
                "timesheet_entry_date": dag_run.conf["timesheet_entry_date"],
                "break_type": rail.result('for_each_record_start')["break_type"],
                "project_name": rail.result('for_each_record_start')["project_name"],
                "punch_in_hr": rail.result('for_each_record_start')["punch_in_hr"],
                "punch_in_min": rail.result('for_each_record_start')["punch_in_min"],
                "punch_out_hr": rail.result('for_each_record_start')["punch_out_hr"],
                "punch_out_min": rail.result('for_each_record_start')["punch_out_min"],
                "status": "Exception",
                "details": "Break type not available in Replicon",
            }
        )

        if_project_name_present = rail.IfOperator(
            task_id = 'if_project_name_present',
            test=lambda: bool(rail.result('for_each_record_start')["project_name"]),
            yes_task='get_project_details',
            no_task='insert_punch_in_to_payload_no_project_task'
        )

        insert_punch_in_to_payload_no_project_task = rail.SetVariableOperator(
            task_id='insert_punch_in_to_payload_no_project_task',
            append=True,
            name='{{ result("payload_list_variable").name }}',
            value=request_payload.get_punch_in_payload
        )

        insert_punch_out_to_payload_no_project_task = rail.SetVariableOperator(
            task_id='insert_punch_out_to_payload_no_project_task',
            append=True,
            name='{{ result("payload_list_variable").name }}',
            value=request_payload.get_punch_out_payload
        )

        get_project_details = rail.RepliconServiceOperator(
            task_id='get_project_details',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data=lambda: {
                "projects": [
                    {
                        "uri": null,
                        "name": rail.result('for_each_record_start')['project_name'],
                        "code": null,
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
            yes_task="get_task_details",
            no_task="log_project_not_present"
        )

        log_project_not_present = rail.WriteLogOperator(
            task_id='log_project_not_present',
            log="{{dag_run.conf.create_time_entry_logs}}",
            message="Project not available",
            severity='Exception',
            properties=lambda dag_run:{
                "employeename": dag_run.conf["employee"],
                "timesheet_entry_date": dag_run.conf["timesheet_entry_date"],
                "break_type": rail.result('for_each_record_start')["break_type"],
                "project_name": rail.result('for_each_record_start')["project_name"],
                "punch_in_hr": rail.result('for_each_record_start')["punch_in_hr"],
                "punch_in_min": rail.result('for_each_record_start')["punch_in_min"],
                "punch_out_hr": rail.result('for_each_record_start')["punch_out_hr"],
                "punch_out_min": rail.result('for_each_record_start')["punch_out_min"],
                "status": "Exception",
                "details": "Project not available in Replicon",
            }
        )

        get_task_details=rail.RepliconServiceOperator(
            task_id='get_task_details',
            endpoint="/services/TaskService1.svc/GetChildrenTaskDetails",
            data=lambda: {
                "parentUri": rail.result('get_project_details')['uri']
            }
        )

        log_task_uri=rail.PythonOperator(
            task_id='log_task_uri',
            python_callable= lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result('get_task_details'), 'name', rail.result('for_each_record_start')['taskname'], 'uri', '')
        )

        if_config_taskname_present = rail.IfOperator(
            task_id='if_config_taskname_present',
            test=lambda: bool(rail.result('for_each_record_start')['taskname']),
            yes_task="if_task_uri_present",
            no_task="insert_to_main_payload_punch_in",
        )

        if_task_uri_present=rail.IfOperator(
            task_id='if_task_uri_present',
            test='''{{ result('log_task_uri') | is_truthy }}''',
            yes_task="get_subtask_details",
            no_task="log_exception_task_not_found",
        )

        get_subtask_details=rail.RepliconServiceOperator(
            task_id='get_subtask_details',
            endpoint="/services/TaskService1.svc/GetChildrenTaskDetails",
            data={
                "parentUri": "{{ result('log_task_uri') }}"
            }
        )

        log_subtask_uri=rail.PythonOperator(
            task_id='log_subtask_uri',
            python_callable= lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result('get_subtask_details'), 'name',
                rail.result('for_each_record_start')['subtaskname'], 'uri', '')
        )

        if_config_subtaskname_present = rail.IfOperator(
            task_id='if_config_subtaskname_present',
            test=lambda: bool(rail.result('for_each_record_start')['subtaskname']),
            yes_task="if_subtask_uri_present",
            no_task="insert_to_main_payload_punch_in",
        )

        if_subtask_uri_present=rail.IfOperator(
            task_id='if_subtask_uri_present',
            test='''{{ result('log_subtask_uri') | is_truthy }}''',
            yes_task="insert_to_main_payload_punch_in",
            no_task="log_exception_subtask_not_found",
        )

        log_exception_task_not_found = rail.WriteLogOperator(
            task_id='log_exception_task_not_found',
            log="{{dag_run.conf.create_time_entry_logs}}",
            message="Task not available",
            severity='Exception',
            properties=lambda dag_run:{
                "employeename": dag_run.conf["employee"],
                "timesheet_entry_date": dag_run.conf["timesheet_entry_date"],
                "break_type": rail.result('for_each_record_start')["break_type"],
                "project_name": rail.result('for_each_record_start')["project_name"],
                "punch_in_hr": rail.result('for_each_record_start')["punch_in_hr"],
                "punch_in_min": rail.result('for_each_record_start')["punch_in_min"],
                "punch_out_hr": rail.result('for_each_record_start')["punch_out_hr"],
                "punch_out_min": rail.result('for_each_record_start')["punch_out_min"],
                "status": "Exception",
                "details": "Task not available in Replicon",
            }
        )

        log_exception_subtask_not_found = rail.WriteLogOperator(
            task_id='log_exception_subtask_not_found',
            log="{{dag_run.conf.create_time_entry_logs}}",
            message="Sub Task not available",
            severity='Exception',
            properties=lambda dag_run:{
                "employeename": dag_run.conf["employee"],
                "timesheet_entry_date": dag_run.conf["timesheet_entry_date"],
                "break_type": rail.result('for_each_record_start')["break_type"],
                "project_name": rail.result('for_each_record_start')["project_name"],
                "punch_in_hr": rail.result('for_each_record_start')["punch_in_hr"],
                "punch_in_min": rail.result('for_each_record_start')["punch_in_min"],
                "punch_out_hr": rail.result('for_each_record_start')["punch_out_hr"],
                "punch_out_min": rail.result('for_each_record_start')["punch_out_min"],
                "status": "Exception",
                "details": "Sub Task not available in Replicon",
            }
        )

        insert_to_all_valid_entries_list = rail.SetVariableOperator(
            task_id='insert_to_all_valid_entries_list',
            append=True,
            name='{{ result("all_valid_entries_list").name }}',
            value=lambda dag_run:{
                "employeename": dag_run.conf["employee"],
                "timesheet_entry_date": dag_run.conf["timesheet_entry_date"],
                "break_type": rail.result('for_each_record_start')["break_type"],
                "project_name": rail.result('for_each_record_start')["project_name"],
                "punch_in_hr": rail.result('for_each_record_start')["punch_in_hr"],
                "punch_in_min": rail.result('for_each_record_start')["punch_in_min"],
                "punch_out_hr": rail.result('for_each_record_start')["punch_out_hr"],
                "punch_out_min": rail.result('for_each_record_start')["punch_out_min"]
            }
        )

        insert_to_main_payload_punch_in = rail.SetVariableOperator(
            task_id='insert_to_main_payload_punch_in',
            append=True,
            name='{{ result("payload_list_variable").name }}',
            value=request_payload.get_punch_in_payload_with_project_details
        )

        insert_to_main_payload_punch_out = rail.SetVariableOperator(
            task_id='insert_to_main_payload_punch_out',
            append=True,
            name='{{ result("payload_list_variable").name }}',
            value=request_payload.get_punch_out_payload_with_project_details
        )

        for_each_record_end = rail.EmptyOperator(
            task_id = 'for_each_record_end'
        )

        put_punch_entry_for_user =  rail.RepliconServiceOperator(
            task_id="put_punch_entry_for_user",
            endpoint='/services/TimePunchService1.svc/BulkPutTimePunch4',
            #pylint: disable = line-too-long
            data=lambda: {
                "timePunches": rail.get_dag_run_var(rail.result('payload_list_variable')['name']),
                "bulkPutTimePunchBehaviour": {
                    "bulkPutTimePunchBehaviourErrorHandlingOptionUri": "urn:replicon:bulk-put-time-punch-behaviour-error-handling-option:fault-and-rollback-on-error"
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        log_success_for_valid_entries = rail.WriteLogOperator(
            task_id='log_success_for_valid_entries',
            log="{{ dag_run.conf.create_time_entry_logs }}",
            items=lambda: rail.get_dag_run_var(rail.result('all_valid_entries_list')['name']),
            severity='Success',
            message='Success',
            properties=lambda dag_run, item: {
                "employeename": dag_run.conf["employee"],
                "timesheet_entry_date": dag_run.conf["timesheet_entry_date"],
                "break_type": item["break_type"],
                "project_name": item["project_name"],
                "punch_in_hr": item["punch_in_hr"],
                "punch_in_min": item["punch_in_min"],
                "punch_out_hr": item["punch_out_hr"],
                "punch_out_min": item["punch_out_min"],
                "status": "Success",
                "details": "Successfully added to Replicon",
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ dag_run.conf.create_time_entry_logs }}",
            items=lambda dag_run: dag_run.conf["data"],
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties=lambda dag_run, item: {
                "employeename": dag_run.conf["employee"],
                "timesheet_entry_date": dag_run.conf["timesheet_entry_date"],
                "break_type": item["break_type"],
                "project_name": item["project_name"],
                "punch_in_hr": item["punch_in_hr"],
                "punch_in_min": item["punch_in_min"],
                "punch_out_hr": item["punch_out_hr"],
                "punch_out_min": item["punch_out_min"],
                "status": "Error",
                "details": rail.render_template("{{get_error_message()}}"),
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> search_user >> if_user_present
        if_user_present >> rail.Label('Yes') >> if_invalid_date_format
        if_invalid_date_format >> rail.Label('Yes') >> logs_add_entry_invalid_date >> finish
        if_invalid_date_format >> rail.Label('No') >> get_timesheet_policysets >> if_time_punch_timesheet_assigned
        if_time_punch_timesheet_assigned >> rail.Label('Yes') >> get_timsheet_for_date >> is_timesheet_submitted
        if_time_punch_timesheet_assigned >> rail.Label('No') >> log_exceptiom_incorrect_timesheet_assigned >> finish
        is_timesheet_submitted >> rail.Label('Yes') >> logs_add_entry_timesheet_already_approved >> finish
        is_timesheet_submitted >> rail.Label('No') >> get_existing_punch_for_user >> if_existing_punch_data
        if_existing_punch_data >> rail.Label('Yes') >> logs_add_entry_existing_punch_data >> finish
        if_existing_punch_data >> rail.Label('No') >> get_timeentry_oef >> mark_overlapping_entries \
        >> log_exception_for_overlapping_entries >> payload_list_variable >> all_valid_entries_list \
        >> for_each_record_start >> if_break_punch_present
        if_break_punch_present  >> rail.Label('Yes') >> get_break_type_uri >> if_break_type_uri_present
        if_break_type_uri_present >> rail.Label('Yes') >> insert_break_punch_in_to_payload \
        >> insert_break_punch_out_to_payload >> insert_to_all_valid_entries_list >> for_each_record_end
        if_break_type_uri_present >> rail.Label('No') >> logs_add_entry_break_type_notfound >> for_each_record_end
        if_break_punch_present  >> rail.Label('No') >> if_project_name_present
        if_project_name_present >> rail.Label('Yes') >> get_project_details >> is_project_present
        if_project_name_present >> rail.Label('No') >> insert_punch_in_to_payload_no_project_task \
        >> insert_punch_out_to_payload_no_project_task >> insert_to_all_valid_entries_list >> for_each_record_end
        is_project_present >> rail.Label('Yes') >> get_task_details >> log_task_uri >> if_config_taskname_present
        if_config_taskname_present >> rail.Label('Yes') >> if_task_uri_present
        if_config_taskname_present >> rail.Label('No') >> insert_to_main_payload_punch_in
        if_task_uri_present >> rail.Label('Yes') >> get_subtask_details >> log_subtask_uri >> if_config_subtaskname_present
        if_config_subtaskname_present >> rail.Label('Yes') >> if_subtask_uri_present
        if_config_subtaskname_present >> rail.Label('No') >> insert_to_main_payload_punch_in
        if_subtask_uri_present >> rail.Label('Yes') >> insert_to_main_payload_punch_in >> insert_to_main_payload_punch_out \
        >>  insert_to_all_valid_entries_list >> for_each_record_end
        if_subtask_uri_present >> rail.Label('No') >> log_exception_subtask_not_found >> for_each_record_end
        if_task_uri_present >> rail.Label('No') >> log_exception_task_not_found >> for_each_record_end
        is_project_present >> rail.Label('No') >> log_project_not_present >> for_each_record_end \
        >> put_punch_entry_for_user >> log_success_for_valid_entries >> finish
        for_each_record_start >> for_each_record_end
        if_user_present >> rail.Label('No') >> log_user_not_present >> finish
        finish >> catch_and_log_errors >> log_to_sumo

    return dag

rail.for_each_instance(create_child_dag)
